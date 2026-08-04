from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import Mock, patch


class EditSessionFileLineageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.upload_root = self.root / "uploads"
        self.stash_root = self.root / "sessions"
        self.upload_root.mkdir()
        self.stash_root.mkdir()
        self.db_path = self.root / "test.db"

        import app.checklists.edit_session_files as files

        self.files = files

        def get_conn():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            return conn

        self.patches = [
            patch.object(files, "UPLOAD_ROOT", self.upload_root),
            patch.object(files, "EDIT_SESSION_FILE_ROOT", self.stash_root),
            patch.object(files, "get_conn", get_conn),
        ]
        for active_patch in self.patches:
            active_patch.start()
        files.ensure_edit_session_file_schema()

    def tearDown(self):
        for active_patch in reversed(self.patches):
            active_patch.stop()
        self.temp.cleanup()

    def _insert_entry(
        self,
        *,
        entry_id: str,
        session_id: str,
        sequence_no: int,
        kind: str,
        original: Path,
        checksum: str,
        staged: Path | None = None,
    ) -> None:
        conn = self.files.get_conn()
        conn.execute(
            """
            INSERT INTO edit_session_file_entries(
                entry_id, session_id, sequence_no, entry_kind,
                original_path, staged_path, sha256, status,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', '', '')
            """,
            (
                entry_id,
                session_id,
                sequence_no,
                kind,
                str(original),
                str(staged) if staged else "",
                checksum,
            ),
        )
        conn.commit()
        conn.close()

    def test_create_then_replace_can_commit_when_later_stash_consumed_file(self):
        old_bytes = b"first upload"
        new_bytes = b"replacement"
        old_path = self.upload_root / "old.jpg"
        new_path = self.upload_root / "new.jpg"
        staged_path = self.stash_root / "session" / "stash.jpg"
        staged_path.parent.mkdir(parents=True)
        staged_path.write_bytes(old_bytes)
        new_path.write_bytes(new_bytes)
        old_hash = hashlib.sha256(old_bytes).hexdigest()
        new_hash = hashlib.sha256(new_bytes).hexdigest()

        self._insert_entry(
            entry_id="created-old",
            session_id="session",
            sequence_no=1,
            kind="created",
            original=old_path,
            checksum=old_hash,
        )
        self._insert_entry(
            entry_id="created-new",
            session_id="session",
            sequence_no=2,
            kind="created",
            original=new_path,
            checksum=new_hash,
        )
        self._insert_entry(
            entry_id="stash-old",
            session_id="session",
            sequence_no=3,
            kind="stashed",
            original=old_path,
            staged=staged_path,
            checksum=old_hash,
        )

        result = self.files.prepare_edit_session_files_for_commit("session")
        self.assertTrue(result["ok"])
        self.assertEqual(result["preparedCount"], 3)

    def test_missing_created_file_without_matching_stash_still_fails(self):
        missing = self.upload_root / "missing.jpg"
        self._insert_entry(
            entry_id="created-missing",
            session_id="bad-session",
            sequence_no=1,
            kind="created",
            original=missing,
            checksum=hashlib.sha256(b"missing").hexdigest(),
        )
        with self.assertRaises(self.files.EditSessionFileConflictError):
            self.files.prepare_edit_session_files_for_commit("bad-session")


class YandexRecoveryHelperTests(unittest.TestCase):
    def test_resource_locked_error_is_bounded_and_retried(self):
        from app.checklists import yandex_resource_locks as locks

        attempts = []

        def operation():
            attempts.append(len(attempts) + 1)
            if len(attempts) < 3:
                raise RuntimeError(
                    "DiskResourceLockedError: Resource is locked."
                )
            return "ok"

        with patch.object(locks.time, "sleep") as sleep:
            result = locks.run_with_yandex_resource_retry(
                operation,
                operation_name="test",
                delays=(0.1, 0.2, 0.3),
            )
        self.assertEqual(result, "ok")
        self.assertEqual(len(attempts), 3)
        self.assertEqual(sleep.call_count, 2)

    def test_custom_prefix_is_preserved_or_allocated(self):
        from app.checklists import yandex_folders as folders

        self.assertEqual(
            folders.split_custom_folder_prefix("07_Раздел"),
            (7, "Раздел"),
        )
        with patch.object(
            folders,
            "next_free_custom_folder_prefix",
            return_value=4,
        ):
            allocated = folders.build_stable_custom_folder_target_path(
                parent_path="disk:/Проект/Раздел",
                item_name="Новый пункт",
            )
        preserved = folders.build_stable_custom_folder_target_path(
            parent_path="disk:/Проект/Раздел",
            item_name="Новое имя",
            preserve_source_name="07_Старое имя",
        )
        self.assertEqual(allocated, "disk:/Проект/Раздел/04_Новый пункт")
        self.assertEqual(preserved, "disk:/Проект/Раздел/07_Новое имя")

    def test_two_custom_folder_candidates_create_conflict_without_mutation(self):
        from app.checklists import yandex_custom_recovery as recovery

        candidates = [
            {
                "name": "01_Пункт",
                "path": "disk:/Проект/Старый/01_Пункт",
                "url": "https://example.test/old",
            },
            {
                "name": "02_Пункт",
                "path": "disk:/Проект/Новый/02_Пункт",
                "url": "https://example.test/new",
            },
        ]
        created = {}

        def create_job(**kwargs):
            created.update(kwargs)
            return {
                "job_id": "conflict-job",
                "status": kwargs["initial_status"],
                "error": kwargs["error"],
                "result": kwargs["result"],
            }

        upsert = Mock()
        with (
            patch.object(recovery, "can_create_custom_item_yandex_folder", return_value=True),
            patch.object(recovery, "resolve_custom_item_parent_yandex_path", return_value="disk:/Проект/Новый"),
            patch.object(recovery, "get_latest_yandex_structure_job_for_item", return_value=None),
            patch.object(recovery, "_collect_custom_candidates", return_value=candidates),
            patch.object(recovery, "_has_ambiguous_local_identity", return_value=False),
            patch.object(recovery, "yandex_project_resource_guard", return_value=nullcontext()),
            patch.object(recovery, "create_yandex_structure_job", side_effect=create_job),
            patch.object(recovery, "persist_item_yandex_structure_state"),
            patch.object(recovery, "upsert_item_yandex_mapping", upsert),
        ):
            result = recovery.reconcile_custom_item_yandex_folder(
                dialog_id="chat1",
                checklist_key="id",
                item_id="custom-1",
                item={
                    "id": "custom-1",
                    "name": "Пункт",
                    "group": 2,
                    "isCustom": True,
                },
            )

        self.assertTrue(result["conflict"])
        self.assertEqual(created["initial_status"], "conflict")
        self.assertEqual(len(created["result"]["conflictCandidates"]), 2)
        upsert.assert_not_called()


if __name__ == "__main__":
    unittest.main()
