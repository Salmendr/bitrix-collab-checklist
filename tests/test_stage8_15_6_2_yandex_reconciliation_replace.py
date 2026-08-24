from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import Mock, patch


class YandexRemoteFileDiscoveryTests(unittest.TestCase):
    def test_missing_local_link_adopts_matching_remote_file(self):
        from app.checklists import yandex_file_reconciliation as discovery

        with tempfile.TemporaryDirectory() as temp_dir:
            local_file = Path(temp_dir) / "Раздел ПД.zip"
            local_bytes = b"existing yandex content"
            local_file.write_bytes(local_bytes)
            remote_path = "disk:/Проект/Пункт/Раздел ПД.zip"
            with (
                patch.object(
                    discovery,
                    "get_latest_yandex_structure_job_for_item",
                    return_value=None,
                ),
                patch.object(
                    discovery,
                    "yandex_disk_try_get_resource_meta",
                    return_value={
                        "name": "Раздел ПД.zip",
                        "path": remote_path,
                        "type": "file",
                        "size": len(local_bytes),
                        "sha256": hashlib.sha256(local_bytes).hexdigest(),
                        "md5": hashlib.md5(local_bytes).hexdigest(),
                    },
                ),
            ):
                result = discovery.find_existing_yandex_document(
                    dialog_id="chat1",
                    checklist_key="p",
                    item={
                        "id": "item-1",
                        "name": "Пункт",
                        "group": 1,
                        "yandexFolderPath": "disk:/Проект/Пункт",
                    },
                    document={
                        "id": "doc-1",
                        "name": "Раздел ПД.zip",
                        "yandexPath": "",
                    },
                    local_path=local_file,
                    context={},
                )

        self.assertEqual(result["status"], "matched")
        self.assertEqual(result["match"]["path"], remote_path)

    def test_same_name_with_different_checksum_is_conflict(self):
        from app.checklists import yandex_file_reconciliation as discovery

        with tempfile.TemporaryDirectory() as temp_dir:
            local_file = Path(temp_dir) / "file.pdf"
            local_file.write_bytes(b"abc")
            with (
                patch.object(
                    discovery,
                    "get_latest_yandex_structure_job_for_item",
                    return_value=None,
                ),
                patch.object(
                    discovery,
                    "yandex_disk_try_get_resource_meta",
                    return_value={
                        "name": "file.pdf",
                        "path": "disk:/Проект/Пункт/file.pdf",
                        "type": "file",
                        "size": 3,
                        "sha256": hashlib.sha256(b"xyz").hexdigest(),
                        "md5": hashlib.md5(b"xyz").hexdigest(),
                    },
                ),
            ):
                result = discovery.find_existing_yandex_document(
                    dialog_id="chat1",
                    checklist_key="id",
                    item={
                        "id": "item-1",
                        "name": "Пункт",
                        "group": 1,
                        "yandexFolderPath": "disk:/Проект/Пункт",
                    },
                    document={"id": "doc-1", "name": "file.pdf"},
                    local_path=local_file,
                    context={},
                )

        self.assertEqual(result["status"], "conflict")
        self.assertEqual(result["conflicts"][0]["reason"], "sha256_mismatch")

    def test_missing_remote_checksum_is_not_treated_as_a_match(self):
        from app.checklists import yandex_file_reconciliation as discovery

        with tempfile.TemporaryDirectory() as temp_dir:
            local_file = Path(temp_dir) / "file.pdf"
            local_file.write_bytes(b"abc")
            with (
                patch.object(
                    discovery,
                    "get_latest_yandex_structure_job_for_item",
                    return_value=None,
                ),
                patch.object(
                    discovery,
                    "yandex_disk_try_get_resource_meta",
                    return_value={
                        "name": "file.pdf",
                        "path": "disk:/Проект/Пункт/file.pdf",
                        "type": "file",
                        "size": 3,
                    },
                ),
            ):
                result = discovery.find_existing_yandex_document(
                    dialog_id="chat1",
                    checklist_key="id",
                    item={
                        "id": "item-1",
                        "name": "Пункт",
                        "group": 1,
                        "yandexFolderPath": "disk:/Проект/Пункт",
                    },
                    document={"id": "doc-1", "name": "file.pdf"},
                    local_path=local_file,
                    context={},
                )

        self.assertEqual(result["status"], "conflict")
        self.assertEqual(
            result["conflicts"][0]["reason"],
            "remote_checksum_missing",
        )

    def test_probe_error_blocks_recovery_upload(self):
        from app.checklists import yandex_file_reconciliation as discovery

        with tempfile.TemporaryDirectory() as temp_dir:
            local_file = Path(temp_dir) / "file.pdf"
            local_file.write_bytes(b"abc")
            with (
                patch.object(
                    discovery,
                    "get_latest_yandex_structure_job_for_item",
                    return_value=None,
                ),
                patch.object(
                    discovery,
                    "yandex_disk_try_get_resource_meta",
                    side_effect=RuntimeError("temporary Yandex error"),
                ),
            ):
                result = discovery.find_existing_yandex_document(
                    dialog_id="chat1",
                    checklist_key="id",
                    item={
                        "id": "item-1",
                        "name": "Пункт",
                        "group": 1,
                        "yandexFolderPath": "disk:/Проект/Пункт",
                    },
                    document={"id": "doc-1", "name": "file.pdf"},
                    local_path=local_file,
                    context={},
                )

        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(len(result["probeErrors"]), 1)


class YandexReconciliationIntegrationTests(unittest.TestCase):
    def _checklist_db(self, root: Path, document: dict) -> tuple[Path, object]:
        db_path = root / "checklists.db"

        def get_conn():
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            return conn

        conn = get_conn()
        conn.execute(
            "CREATE TABLE checklists(dialog_id TEXT PRIMARY KEY, data_json TEXT)"
        )
        conn.execute(
            "INSERT INTO checklists(dialog_id, data_json) VALUES (?, ?)",
            (
                "chat1::id",
                json.dumps({
                    "checklistKey": "id",
                    "items": [{
                        "id": "item-1",
                        "name": "Пункт",
                        "group": 1,
                        "documents": [document],
                    }],
                }, ensure_ascii=False),
            ),
        )
        conn.commit()
        conn.close()
        return db_path, get_conn

    def test_existing_remote_copy_stops_legacy_upload_job(self):
        from app.checklists import yandex_mirror_reconciliation as reconciliation

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            local_file = root / "file.pdf"
            local_file.write_bytes(b"same")
            _, get_conn = self._checklist_db(
                root,
                {
                    "id": "doc-1",
                    "name": "file.pdf",
                    "fileUrl": "/uploads/file.pdf",
                    "mirrorStatus": "queued",
                    "yandexPath": "",
                },
            )
            finish = Mock(return_value={
                "updated": True,
                "job": {"job_id": "legacy-job", "status": "synced"},
            })
            update = Mock(return_value=True)
            ensure = Mock()
            enqueue = Mock()
            with (
                patch.object(reconciliation, "get_conn", get_conn),
                patch.object(reconciliation, "is_yandex_disk_enabled", return_value=True),
                patch.object(
                    reconciliation,
                    "get_project_storage_context",
                    return_value={
                        "storageMode": {"mirrorTargets": ["yandex_disk"]},
                    },
                ),
                patch.object(reconciliation, "_document_local_path", return_value=local_file),
                patch.object(
                    reconciliation,
                    "build_standard_item_yandex_repair_spec",
                    return_value={"repairRequired": False},
                ),
                patch.object(reconciliation, "yandex_project_resource_guard", return_value=nullcontext()),
                patch.object(
                    reconciliation,
                    "find_existing_yandex_document",
                    return_value={
                        "status": "matched",
                        "match": {"path": "disk:/Проект/Пункт/file.pdf"},
                        "checkedPaths": ["disk:/Проект/Пункт/file.pdf"],
                    },
                ),
                patch.object(
                    reconciliation,
                    "finish_document_upload_job_from_remote_match",
                    finish,
                ),
                patch.object(reconciliation, "update_document_mirror_fields", update),
                patch.object(
                    reconciliation,
                    "ensure_yandex_upload_job_for_reconciliation",
                    ensure,
                ),
                patch.object(reconciliation, "enqueue_yandex_mirror_job", enqueue),
            ):
                result = reconciliation.reconcile_yandex_mirror_documents(
                    source="test",
                    dialog_id="chat1",
                )

        self.assertEqual(result["remoteMatchedByDiscovery"], 1)
        finish.assert_called_once()
        ensure.assert_not_called()
        enqueue.assert_not_called()
        self.assertEqual(
            update.call_args.args[4]["yandexPath"],
            "disk:/Проект/Пункт/file.pdf",
        )

    def test_remote_content_conflict_never_creates_upload(self):
        from app.checklists import yandex_mirror_reconciliation as reconciliation

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            local_file = root / "file.pdf"
            local_file.write_bytes(b"local")
            _, get_conn = self._checklist_db(
                root,
                {
                    "id": "doc-1",
                    "name": "file.pdf",
                    "fileUrl": "/uploads/file.pdf",
                    "mirrorStatus": "queued",
                },
            )
            fail = Mock(return_value={"updated": True})
            ensure = Mock()
            enqueue = Mock()
            with (
                patch.object(reconciliation, "get_conn", get_conn),
                patch.object(reconciliation, "is_yandex_disk_enabled", return_value=True),
                patch.object(
                    reconciliation,
                    "get_project_storage_context",
                    return_value={
                        "storageMode": {"mirrorTargets": ["yandex_disk"]},
                    },
                ),
                patch.object(reconciliation, "_document_local_path", return_value=local_file),
                patch.object(
                    reconciliation,
                    "build_standard_item_yandex_repair_spec",
                    return_value={"repairRequired": False},
                ),
                patch.object(reconciliation, "yandex_project_resource_guard", return_value=nullcontext()),
                patch.object(
                    reconciliation,
                    "find_existing_yandex_document",
                    return_value={
                        "status": "conflict",
                        "error": "remote conflict",
                        "checkedPaths": ["disk:/Проект/Пункт/file.pdf"],
                        "matches": [],
                        "conflicts": [{"path": "disk:/Проект/Пункт/file.pdf"}],
                    },
                ),
                patch.object(
                    reconciliation,
                    "_pending_replacement_may_overwrite_old_path",
                    return_value=False,
                ),
                patch.object(
                    reconciliation,
                    "fail_document_upload_job_for_remote_conflict",
                    fail,
                ),
                patch.object(reconciliation, "update_document_mirror_fields", return_value=True),
                patch.object(
                    reconciliation,
                    "ensure_yandex_upload_job_for_reconciliation",
                    ensure,
                ),
                patch.object(reconciliation, "enqueue_yandex_mirror_job", enqueue),
            ):
                result = reconciliation.reconcile_yandex_mirror_documents(
                    source="test",
                    dialog_id="chat1",
                )

        self.assertEqual(result["remoteFileConflicts"], 1)
        fail.assert_called_once()
        ensure.assert_not_called()
        enqueue.assert_not_called()


class YandexReplacementDeleteTests(unittest.TestCase):
    def test_delete_404_is_idempotent_success(self):
        from app.yandex_disk import client

        response = Mock(status_code=404)
        response.json.return_value = {"error": "DiskNotFoundError"}
        with patch.object(client.requests, "delete", return_value=response):
            result = client.yandex_disk_delete_path(
                "disk:/Проект/Пункт/old.pdf"
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["alreadyMissing"])

    def test_missing_old_copy_completes_delete_job_without_error(self):
        from app.checklists import yandex_mirror_queue as queue

        finish = Mock()
        delete = Mock()
        with (
            patch.object(queue, "is_yandex_disk_enabled", return_value=True),
            patch.object(queue, "update_upload_job_progress"),
            patch.object(queue, "get_document_replacement_by_delete_job", return_value={}),
            patch.object(queue, "yandex_disk_try_get_resource_meta", return_value=None),
            patch.object(queue, "yandex_disk_delete_path", delete),
            patch.object(queue, "finish_upload_job", finish),
        ):
            queue.process_delete_job({
                "job_id": "delete-1",
                "yandex_path": "disk:/Проект/Пункт/old.pdf",
                "file_name": "old.pdf",
            })

        delete.assert_not_called()
        finish.assert_called_once_with(
            "delete-1",
            status="deleted",
            stage="already_missing",
        )

    def test_delete_job_never_removes_new_file_at_same_path(self):
        from app.checklists import yandex_mirror_queue as queue

        finish = Mock()
        probe = Mock()
        delete = Mock()
        with (
            patch.object(queue, "is_yandex_disk_enabled", return_value=True),
            patch.object(queue, "update_upload_job_progress"),
            patch.object(
                queue,
                "get_document_replacement_by_delete_job",
                return_value={
                    "new_yandex_path": "disk:/Проект/Пункт/file.pdf",
                },
            ),
            patch.object(queue, "yandex_disk_try_get_resource_meta", probe),
            patch.object(queue, "yandex_disk_delete_path", delete),
            patch.object(queue, "finish_upload_job", finish),
        ):
            queue.process_delete_job({
                "job_id": "delete-1",
                "yandex_path": "disk:/Проект/Пункт/file.pdf",
                "file_name": "file.pdf",
            })

        probe.assert_not_called()
        delete.assert_not_called()
        finish.assert_called_once_with(
            "delete-1",
            status="skipped",
            stage="same_path_protected",
        )

    def test_transient_failed_delete_is_recovered_after_restart(self):
        from app.checklists import yandex_mirror_queue as queue

        replacement = {
            "operation_id": "replace-1",
            "delete_job_id": "delete-1",
            "archive_version_id": "archive-1",
        }
        update_replacement = Mock(return_value={
            **replacement,
            "status": "pending",
        })
        update_archive = Mock(return_value=True)
        with (
            patch.object(
                queue,
                "list_recoverable_failed_document_replacements",
                return_value=[replacement],
            ),
            patch.object(
                queue,
                "get_upload_job",
                return_value={
                    "job_id": "delete-1",
                    "status": "error",
                    "error": "Yandex Disk delete failed (status 404)",
                },
            ),
            patch.object(
                queue,
                "retry_failed_yandex_delete_job",
                return_value={"job_id": "delete-1", "status": "queued"},
            ),
            patch.object(queue, "update_document_replacement", update_replacement),
            patch.object(queue, "update_archive_version_yandex_state", update_archive),
        ):
            result = queue.recover_failed_document_replacement_deletes(
                source="test"
            )

        self.assertEqual(result["requeued"], 1)
        update_replacement.assert_called_once()
        update_archive.assert_called_once()


if __name__ == "__main__":
    unittest.main()
