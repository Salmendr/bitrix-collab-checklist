from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


# The production image installs requests. The isolated Codex verification
# runtime intentionally has no network client; a module stub is enough because
# these tests never call Yandex HTTP methods.
try:
    import requests as _requests  # noqa: F401
except ModuleNotFoundError:
    sys.modules.setdefault("requests", types.SimpleNamespace())


class PublicFolderRuntimeCase(unittest.TestCase):
    def setUp(self):
        import app.db as db
        from app.checklists import documents
        from app.checklists import public_folder_links as links
        from app.checklists import public_folder_operations as operations
        from app.checklists.storage import get_checklist, save_checklist

        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_root = self.root / "db"
        self.upload_root = self.root / "uploads"
        self.staging_root = self.root / "public_staging"
        self.db_root.mkdir()
        self.upload_root.mkdir()
        self.staging_root.mkdir()

        self.db = db
        self.documents = documents
        self.links = links
        self.operations = operations
        self.get_checklist = get_checklist
        self.save_checklist = save_checklist

        self.patches = [
            patch.object(db, "DB_PATH", str(self.db_root / "app.db")),
            patch.object(
                links,
                "PUBLIC_FOLDER_SIGNING_KEY_PATH",
                self.db_root / "public_folder_signing.key",
            ),
            patch.object(operations, "PUBLIC_FOLDER_STAGING_ROOT", self.staging_root),
            patch.object(operations, "UPLOAD_ROOT", self.upload_root),
            patch.object(operations, "BASE_DIR", self.root),
            patch.object(documents, "UPLOAD_ROOT", self.upload_root),
            patch.object(operations, "write_debug_log", Mock()),
            patch.object(operations, "enqueue_yandex_mirror_job", Mock()),
        ]
        for active_patch in self.patches:
            active_patch.start()

        links._signing_key_cache = None
        operations._item_locks.clear()
        db.init_db()

        checklist = get_checklist("chat101", "id")
        self.item_id = checklist["items"][0]["id"]
        self.item_name = checklist["items"][0]["name"]
        save_checklist("chat101", checklist, "id")
        self.link = links.get_or_create_public_folder_link(
            dialog_id="chat101",
            checklist_key="id",
            item_id=self.item_id,
            acting_user_id="18",
            acting_user_name="Сотрудник",
        )

    def tearDown(self):
        self.links._signing_key_cache = None
        self.operations._item_locks.clear()
        for active_patch in reversed(self.patches):
            active_patch.stop()
        self.temp.cleanup()

    def item(self) -> dict:
        checklist = self.get_checklist("chat101", "id")
        return next(
            item
            for item in checklist.get("items", [])
            if item.get("id") == self.item_id
        )

    def save_item_documents(self, documents: list[dict]) -> None:
        checklist = self.get_checklist("chat101", "id")
        item = next(
            value
            for value in checklist["items"]
            if value.get("id") == self.item_id
        )
        item["documents"] = documents
        self.save_checklist("chat101", checklist, "id")

    def prepare_received(
        self,
        *,
        operation_type: str = "upload",
        file_name: str = "report.pdf",
        contents: bytes = b"payload",
        expected_document_id: str = "",
    ) -> dict:
        operation = self.operations.prepare_public_folder_operation(
            link=self.link,
            operation_type=operation_type,
            original_file_name=file_name,
            first_name="Иван",
            last_name="Петров",
            expected_document_id=expected_document_id,
        )
        staging = Path(operation["staging_path"])
        staging.parent.mkdir(parents=True, exist_ok=True)
        staging.write_bytes(contents)
        return self.operations.finalize_received_public_folder_operation(
            operation["operation_id"],
            len(contents),
        )


class PublicFolderLinkTests(PublicFolderRuntimeCase):
    def test_signed_link_is_permanent_until_reissued(self):
        first_token = self.links.build_public_folder_token(
            self.link["link_id"],
            self.link["generation"],
        )
        self.assertEqual(
            self.links.build_public_folder_token(
                self.link["link_id"],
                self.link["generation"],
            ),
            first_token,
        )
        self.assertIsNotNone(self.links.resolve_public_folder_token(first_token))

        reissued = self.links.reissue_public_folder_link(
            dialog_id="chat101",
            checklist_key="id",
            item_id=self.item_id,
            acting_user_id="18",
            acting_user_name="Сотрудник",
        )
        next_token = self.links.build_public_folder_token(
            reissued["link_id"],
            reissued["generation"],
        )
        self.assertNotEqual(first_token, next_token)
        self.assertIsNone(self.links.resolve_public_folder_token(first_token))
        self.assertIsNotNone(self.links.resolve_public_folder_token(next_token))

    def test_tampered_token_is_rejected(self):
        token = self.links.build_public_folder_token(
            self.link["link_id"],
            self.link["generation"],
        )
        replacement = "A" if token[-1] != "A" else "B"
        self.assertIsNone(
            self.links.resolve_public_folder_token(token[:-1] + replacement)
        )


class PublicFolderOperationTests(PublicFolderRuntimeCase):
    def test_public_payload_hides_internal_apply_error(self):
        payload = self.operations.public_folder_operation_payload({
            "operation_id": "operation-1",
            "status": "error",
            "stage": "apply_failed",
            "error": "sqlite3.OperationalError: /private/runtime/app.db",
        })
        self.assertNotIn("/private/runtime", payload["error"])
        self.assertEqual(
            payload["error"],
            "Не удалось обработать файл. Обратитесь к владельцу папки.",
        )

    def test_name_and_surname_are_both_required(self):
        with self.assertRaisesRegex(ValueError, "Имя"):
            self.operations.prepare_public_folder_operation(
                link=self.link,
                operation_type="upload",
                original_file_name="a.txt",
                first_name="",
                last_name="Петров",
            )
        with self.assertRaisesRegex(ValueError, "Фамилия"):
            self.operations.prepare_public_folder_operation(
                link=self.link,
                operation_type="upload",
                original_file_name="a.txt",
                first_name="Иван",
                last_name="",
            )

    def test_duplicate_names_are_reserved_as_2_and_3(self):
        self.save_item_documents([{
            "id": "existing",
            "seriesId": "existing",
            "name": "Смета.pdf",
            "fileUrl": "/uploads/existing.pdf",
            "path": "/uploads/existing.pdf",
            "mirrorStatus": "synced",
        }])
        first = self.operations.prepare_public_folder_operation(
            link=self.link,
            operation_type="upload",
            original_file_name="Смета.pdf",
            first_name="Иван",
            last_name="Петров",
        )
        second = self.operations.prepare_public_folder_operation(
            link=self.link,
            operation_type="upload",
            original_file_name="Смета.pdf",
            first_name="Иван",
            last_name="Петров",
        )
        self.assertEqual(first["file_name"], "Смета (2).pdf")
        self.assertEqual(second["file_name"], "Смета (3).pdf")

    def test_upload_waits_for_edit_session_then_records_external_uploader(self):
        operation = self.prepare_received(file_name="План.pdf")
        conn = self.db.get_conn()
        conn.execute(
            "INSERT INTO edit_sessions(session_id, dialog_id, status) "
            "VALUES ('session-active', 'chat101', 'active')"
        )
        conn.commit()
        conn.close()

        waiting = self.operations.process_public_folder_operation(
            operation["operation_id"]
        )
        self.assertEqual(waiting["status"], "queued")
        self.assertEqual(waiting["stage"], "waiting_for_edit_session")
        self.assertTrue(Path(waiting["staging_path"]).is_file())

        conn = self.db.get_conn()
        conn.execute(
            "UPDATE edit_sessions SET status = 'committed' "
            "WHERE session_id = 'session-active'"
        )
        conn.commit()
        conn.close()
        completed = self.operations.process_public_folder_operation(
            operation["operation_id"]
        )
        self.assertEqual(completed["status"], "completed")
        document = self.item()["documents"][0]
        self.assertEqual(document["uploadedByName"], "Иван Петров")
        self.assertTrue(document["uploadedById"].startswith("external:"))
        self.assertEqual(document["source"], "public_folder")
        self.assertFalse(Path(completed["staging_path"]).exists())

    def test_replacement_conflicts_after_employee_replaces_expected_version(self):
        old_path = self.upload_root / "checklists/chat101/item/old.pdf"
        old_path.parent.mkdir(parents=True)
        old_path.write_bytes(b"old")
        self.save_item_documents([{
            "id": "old-document",
            "seriesId": "series-1",
            "name": "Документ.pdf",
            "fileUrl": "/uploads/checklists/chat101/item/old.pdf",
            "path": "/uploads/checklists/chat101/item/old.pdf",
            "size": 3,
            "mirrorStatus": "synced",
        }])
        operation = self.prepare_received(
            operation_type="replace",
            file_name="Новая версия.pdf",
            expected_document_id="old-document",
        )

        employee_path = self.upload_root / "checklists/chat101/item/employee.pdf"
        employee_path.write_bytes(b"employee")
        self.save_item_documents([{
            "id": "employee-document",
            "seriesId": "series-1",
            "name": "Версия сотрудника.pdf",
            "fileUrl": "/uploads/checklists/chat101/item/employee.pdf",
            "path": "/uploads/checklists/chat101/item/employee.pdf",
            "size": 8,
            "mirrorStatus": "synced",
        }])

        result = self.operations.process_public_folder_operation(
            operation["operation_id"]
        )
        self.assertEqual(result["status"], "conflict")
        self.assertIn("заменён другим пользователем", result["error"])
        self.assertEqual(self.item()["documents"][0]["id"], "employee-document")
        self.assertFalse(Path(result["staging_path"]).exists())

    def test_successful_replacement_keeps_archive_and_old_yandex_path(self):
        old_path = self.upload_root / "checklists/chat101/item/current.zip"
        old_path.parent.mkdir(parents=True)
        old_path.write_bytes(b"old archive content")
        old_yandex_path = "disk:/Project/Item/current.zip"
        self.save_item_documents([{
            "id": "old-document",
            "seriesId": "series-1",
            "name": "current.zip",
            "fileUrl": "/uploads/checklists/chat101/item/current.zip",
            "path": "/uploads/checklists/chat101/item/current.zip",
            "size": len(b"old archive content"),
            "uploadedByName": "Сотрудник",
            "mirrorStatus": "synced",
            "yandexPath": old_yandex_path,
        }])
        operation = self.prepare_received(
            operation_type="replace",
            file_name="current.zip",
            contents=b"new content",
            expected_document_id="old-document",
        )
        result = self.operations.process_public_folder_operation(
            operation["operation_id"]
        )
        self.assertEqual(result["status"], "completed")

        document = self.item()["documents"][0]
        self.assertEqual(document["uploadedByName"], "Иван Петров")
        self.assertEqual(document["seriesId"], "series-1")
        self.assertEqual(len(document["archiveVersions"]), 1)
        archive = document["archiveVersions"][0]
        self.assertEqual(archive["originalDocumentId"], "old-document")
        self.assertEqual(archive["originalYandexPath"], old_yandex_path)
        self.assertEqual(
            archive["yandexDeleteStatus"],
            "pending_after_replacement_sync",
        )
        archived_path = self.documents.get_upload_file_path_from_url(
            archive["fileUrl"]
        )
        self.assertEqual(archived_path.read_bytes(), b"old archive content")
        self.assertFalse(old_path.exists())

    def test_interrupted_receiving_upload_is_marked_error_and_cleaned(self):
        operation = self.operations.prepare_public_folder_operation(
            link=self.link,
            operation_type="upload",
            original_file_name="partial.bin",
            first_name="Иван",
            last_name="Петров",
        )
        staging = Path(operation["staging_path"])
        staging.parent.mkdir(parents=True, exist_ok=True)
        staging.write_bytes(b"partial")
        recovered = self.operations._recover_interrupted_public_folder_operations()
        failed = self.operations.get_public_folder_operation(
            operation["operation_id"]
        )
        self.assertEqual(recovered, 1)
        self.assertEqual(failed["status"], "error")
        self.assertEqual(failed["stage"], "receive_interrupted")
        self.assertFalse(staging.exists())

    def test_processing_public_operation_blocks_edit_snapshot(self):
        from app.checklists import edit_session_changes

        with (
            patch.object(
                edit_session_changes,
                "get_edit_session_for_actor",
                return_value={"status": "active"},
            ),
            patch.object(
                self.operations,
                "has_processing_public_folder_operation",
                return_value=True,
            ),
        ):
            with self.assertRaisesRegex(
                edit_session_changes.EditSessionConflictError,
                "Внешний файл",
            ):
                edit_session_changes.acquire_checklist_for_edit_session(
                    session_id="session-1",
                    dialog_id="chat101",
                    checklist_key="id",
                    user_id="18",
                    user_name="Сотрудник",
                )


class PublicFolderStaticContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parents[1]

    def read(self, relative: str) -> str:
        return (self.root / relative).read_text(encoding="utf-8")

    def test_share_button_is_between_upload_and_folder_with_label(self):
        source = self.read("app/ui/static/js/popup-document-list.js")
        upload_at = source.index('data-role="upload"')
        share_at = source.index('data-role="share-folder"')
        folder_at = source.index('data-role="view-folder"')
        self.assertLess(upload_at, share_at)
        self.assertLess(share_at, folder_at)
        self.assertIn("Поделиться папкой", source)

    def test_all_three_upload_surfaces_have_drag_and_drop(self):
        popup = self.read("app/ui/static/js/popup-document-actions.js")
        folder = self.read("app/ui/static/js/folder-uploads.js")
        public = self.read("app/ui/static/js/public-folder.js")
        for source in (popup, folder, public):
            self.assertIn("dragover", source)
            self.assertIn("drop", source)
            self.assertIn("dataTransfer", source)

    def test_public_contract_has_no_delete_endpoint_or_control(self):
        routes = self.read("app/checklists/public_folder_routes.py")
        template = self.read("app/ui/templates/public_folder.html")
        script = self.read("app/ui/static/js/public-folder.js")
        self.assertIn('"delete": False', routes)
        self.assertNotIn('/api/public-folder/{token}/delete', routes)
        self.assertNotIn('data-role="public-delete', template)
        self.assertNotIn("/delete", script)
        self.assertIn("Удаление по общей ссылке недоступно", template)

    def test_public_page_exposes_replace_and_archive(self):
        routes = self.read("app/checklists/public_folder_routes.py")
        template = self.read("app/ui/templates/public_folder.html")
        self.assertIn('/api/public-folder/{token}/replace', routes)
        self.assertIn("Архив версий", template)
        self.assertIn("publicReplaceInput", template)

    def test_main_registers_route_and_durable_worker(self):
        source = self.read("main.py")
        self.assertIn("app.include_router(public_folder_router)", source)
        self.assertIn("start_public_folder_operation_worker()", source)


if __name__ == "__main__":
    unittest.main()
