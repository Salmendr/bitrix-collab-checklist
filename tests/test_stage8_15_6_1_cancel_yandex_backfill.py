from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import Mock, patch


class RollbackBusinessStateTests(unittest.TestCase):
    def _checklist(self) -> dict:
        return {
            "checklistKey": "id",
            "items": [{
                "id": "item-1",
                "name": "Документ",
                "group": 3,
                "isCustom": True,
                "status": "Есть",
                "documents": [{
                    "id": "doc-1",
                    "name": "file.pdf",
                    "fileUrl": "/uploads/file.pdf",
                    "mirrorStatus": "queued",
                    "mirrorError": "",
                    "mirrorJobId": "job-1",
                    "yandexPath": "",
                    "yandexFileUrl": "",
                }],
            }],
        }

    def test_background_yandex_fields_do_not_create_rollback_conflict(self):
        from app.checklists import edit_session_changes as changes

        expected = self._checklist()
        current = copy.deepcopy(expected)
        current["items"][0].update({
            "yandexFolderStatus": "completed",
            "yandexFolderPath": "disk:/project/item",
            "yandexFolderUrl": "https://disk.example/item",
        })
        current["items"][0]["documents"][0].update({
            "mirrorStatus": "synced",
            "mirrorJobId": "job-2",
            "yandexPath": "disk:/project/item/file.pdf",
            "yandexFileUrl": "https://disk.example/item",
        })

        self.assertEqual(
            changes.rollback_business_state_hash(expected, "id"),
            changes.rollback_business_state_hash(current, "id"),
        )

    def test_cancel_restores_business_data_and_keeps_worker_result(self):
        from app.checklists import edit_session_changes as changes

        snapshot = self._checklist()
        snapshot["items"][0]["plan"] = "01.08.2026"
        expected = self._checklist()
        expected["items"][0]["plan"] = "05.08.2026"
        current = copy.deepcopy(expected)
        current["items"][0]["documents"][0].update({
            "mirrorStatus": "synced",
            "mirrorJobId": "job-2",
            "yandexPath": "disk:/project/item/file.pdf",
            "yandexFileUrl": "https://disk.example/item",
        })

        restored = changes.merge_background_yandex_state(
            snapshot,
            current,
            expected,
            "id",
        )
        item = next(
            value
            for value in restored["items"]
            if value.get("id") == "item-1"
        )
        document = item["documents"][0]
        self.assertEqual(item["plan"], "01.08.2026")
        self.assertEqual(document["mirrorStatus"], "synced")
        self.assertEqual(
            document["yandexPath"],
            "disk:/project/item/file.pdf",
        )

    def test_restore_preflight_accepts_only_background_worker_changes(self):
        from app.checklists import edit_session_changes as changes
        from app.checklists import storage

        snapshot = self._checklist()
        snapshot["items"][0]["plan"] = "01.08.2026"
        expected = self._checklist()
        expected["items"][0]["plan"] = "05.08.2026"
        current = copy.deepcopy(expected)
        current["items"][0]["documents"][0].update({
            "mirrorStatus": "synced",
            "yandexPath": "disk:/project/item/file.pdf",
        })
        row = {
            "dialog_id": "chat1",
            "checklist_key": "id",
            "snapshot": {"data": snapshot},
            "last_state_hash": changes.checklist_state_hash(expected, "id"),
            "last_state_json": changes.stable_json_dumps(
                changes.canonical_checklist_data(expected, "id")
            ),
            "last_business_state_hash": (
                changes.rollback_business_state_hash(expected, "id")
            ),
            "mutation_count": 1,
            "rollback_restored_at": "",
        }
        with (
            patch.object(changes, "_load_stage3_snapshots", return_value=[row]),
            patch.object(storage, "get_checklist", return_value=current),
        ):
            plans = changes.prepare_edit_session_checklist_restore("session")

        self.assertEqual(len(plans), 1)
        restored_item = next(
            value
            for value in plans[0]["restoredData"]["items"]
            if value.get("id") == "item-1"
        )
        self.assertEqual(restored_item["plan"], "01.08.2026")
        self.assertEqual(
            restored_item["documents"][0]["mirrorStatus"],
            "synced",
        )

        conflicting = copy.deepcopy(current)
        conflicting["items"][0]["plan"] = "06.08.2026"
        with (
            patch.object(changes, "_load_stage3_snapshots", return_value=[row]),
            patch.object(storage, "get_checklist", return_value=conflicting),
        ):
            with self.assertRaises(changes.EditSessionConflictError):
                changes.prepare_edit_session_checklist_restore("session")


class RollbackFilePreflightTests(unittest.TestCase):
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

    def _created_entry(
        self,
        *,
        entry_id: str,
        sequence_no: int,
        path: Path,
        checksum: str,
    ) -> None:
        conn = self.files.get_conn()
        conn.execute(
            """
            INSERT INTO edit_session_file_entries(
                entry_id, session_id, sequence_no, entry_kind,
                original_path, staged_path, sha256, status,
                created_at, updated_at
            ) VALUES (?, 'session', ?, 'created', ?, '', ?, 'active', '', '')
            """,
            (entry_id, sequence_no, str(path), checksum),
        )
        conn.commit()
        conn.close()

    def test_conflict_is_detected_before_any_created_file_is_deleted(self):
        bad_path = self.upload_root / "bad.pdf"
        good_path = self.upload_root / "good.pdf"
        bad_path.write_bytes(b"changed outside session")
        good_path.write_bytes(b"valid new file")
        self._created_entry(
            entry_id="bad-first",
            sequence_no=1,
            path=bad_path,
            checksum=hashlib.sha256(b"expected bytes").hexdigest(),
        )
        self._created_entry(
            entry_id="good-later",
            sequence_no=2,
            path=good_path,
            checksum=hashlib.sha256(b"valid new file").hexdigest(),
        )

        with self.assertRaises(self.files.EditSessionFileConflictError):
            self.files.rollback_edit_session_files("session")

        self.assertTrue(bad_path.is_file())
        self.assertTrue(good_path.is_file())

    def test_missing_created_file_is_idempotent_after_interrupted_cancel(self):
        missing_path = self.upload_root / "already-removed.pdf"
        self._created_entry(
            entry_id="already-removed",
            sequence_no=1,
            path=missing_path,
            checksum=hashlib.sha256(b"removed").hexdigest(),
        )

        result = self.files.rollback_edit_session_files("session")
        self.assertTrue(result["ok"])
        self.assertEqual(result["rolledBackCount"], 1)


class RollbackCoordinatorTests(unittest.TestCase):
    def test_all_preflights_run_before_checklist_or_file_mutation(self):
        from app.checklists import edit_session_changes as changes
        from app.checklists import edit_session_files as files
        from app.checklists import edit_session_locks as locks
        from app.checklists import edit_sessions as sessions
        from app.checklists import notification_drafts as drafts

        events: list[str] = []
        fake_conn = Mock()
        fake_conn.execute.return_value.rowcount = 1
        record = {
            "session_id": "session",
            "status": "rolling_back",
        }

        with (
            patch.object(sessions, "ensure_edit_session_tables"),
            patch.object(sessions, "get_edit_session", return_value=record),
            patch.object(sessions, "get_conn", return_value=fake_conn),
            patch.object(
                changes,
                "refresh_edit_session_expected_state",
                side_effect=lambda *a, **k: events.append("refresh"),
            ),
            patch.object(
                changes,
                "prepare_edit_session_checklist_restore",
                side_effect=lambda *a, **k: events.append("checklist_preflight") or [],
            ),
            patch.object(
                files,
                "validate_edit_session_files_for_rollback",
                side_effect=lambda *a, **k: events.append("file_preflight"),
            ),
            patch.object(
                changes,
                "restore_edit_session_checklists",
                side_effect=lambda *a, **k: events.append("checklist_restore"),
            ),
            patch.object(
                files,
                "rollback_edit_session_files",
                side_effect=lambda *a, **k: events.append("file_rollback"),
            ),
            patch.object(
                changes,
                "mark_edit_session_operations_rolled_back_in_transaction",
            ),
            patch.object(
                files,
                "mark_edit_session_file_entries_rolled_back_in_transaction",
            ),
            patch.object(
                drafts,
                "mark_notification_drafts_cancelled_in_transaction",
            ),
            patch.object(locks, "release_edit_session_locks_in_transaction"),
        ):
            sessions.complete_edit_session_rollback("session")

        self.assertEqual(events, [
            "refresh",
            "checklist_preflight",
            "file_preflight",
            "checklist_restore",
            "file_rollback",
        ])

    def test_commit_is_blocked_after_interrupted_cancel(self):
        from app.checklists import edit_sessions as sessions

        record = {
            "session_id": "session",
            "status": "error",
            "rollback_started_at": "2026-08-05T00:27:08+00:00",
            "rolled_back_at": "",
        }
        with (
            patch.object(sessions, "ensure_edit_session_tables"),
            patch.object(
                sessions,
                "get_edit_session_for_actor",
                return_value=record,
            ),
        ):
            with self.assertRaises(sessions.EditSessionConflictError):
                sessions.begin_edit_session_commit("session")

    def test_save_payload_is_rejected_before_it_overwrites_checklist(self):
        from app.checklists import session_finalization as finalization

        session = {
            "session_id": "session",
            "status": "error",
            "client_session_id": "popup-1",
            "rollback_started_at": "2026-08-05T00:27:08+00:00",
            "rolled_back_at": "",
        }
        reserve = Mock()
        persist = Mock()
        with (
            patch.object(
                finalization,
                "get_edit_session_for_actor",
                return_value=session,
            ),
            patch.object(finalization, "_reserve_finalization", reserve),
            patch.object(finalization, "_persist_finalization_sessions", persist),
        ):
            with self.assertRaises(finalization.EditSessionConflictError):
                finalization.finalize_edit_session_payload({
                    "sessionId": "session",
                    "dialogId": "chat1",
                    "clientSessionId": "popup-1",
                    "sessions": [],
                })

        reserve.assert_not_called()
        persist.assert_not_called()

    def test_lost_cancel_reason_is_paused_until_user_confirms_again(self):
        from app.checklists import edit_session_recovery as recovery
        from app.checklists import edit_sessions as sessions

        candidate = {
            "session_id": "session",
            "status": "error",
            "close_reason": "",
            "rollback_started_at": "2026-08-05T00:27:08+00:00",
        }
        pause = Mock()
        complete_rollback = Mock()
        complete_commit = Mock()
        with (
            patch.object(recovery, "recover_edit_session_file_entries", return_value={"ok": True}),
            patch.object(recovery, "_recovery_candidates", return_value=[candidate]),
            patch.object(recovery, "_has_partially_applied_file_rollback", return_value=True),
            patch.object(recovery, "_record_attempt"),
            patch.object(recovery, "_pause_interrupted_rollback_for_confirmation", pause),
            patch.object(recovery, "_restart_active_session_candidates", return_value=[]),
            patch.object(recovery, "_committed_cleanup_candidates", return_value=[]),
            patch.object(sessions, "ensure_edit_session_tables"),
            patch.object(sessions, "complete_edit_session_rollback", complete_rollback),
            patch.object(sessions, "complete_edit_session_commit", complete_commit),
            patch.object(
                sessions,
                "sweep_expired_edit_sessions",
                return_value={"ok": True},
            ),
        ):
            result = recovery.recover_edit_session_lifecycle(source="test")

        self.assertEqual(result["pausedRollbackCount"], 1)
        pause.assert_called_once()
        complete_rollback.assert_not_called()
        complete_commit.assert_not_called()


class YandexBackfillTests(unittest.TestCase):
    def test_synced_job_is_requeued_when_remote_file_is_missing(self):
        from app.checklists import upload_jobs

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "jobs.db"

            def get_conn():
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                return conn

            with patch.object(upload_jobs, "get_conn", get_conn):
                upload_jobs.ensure_upload_jobs_table()
                conn = get_conn()
                conn.execute(
                    """
                    INSERT INTO upload_jobs(
                        job_id, job_type, dialog_id, checklist_key,
                        item_id, document_id, local_path, file_name,
                        file_size, yandex_path, status, stage,
                        created_at, updated_at
                    ) VALUES (
                        'job-1', 'upload', 'chat1', 'id', 'item-1',
                        'doc-1', 'old.pdf', 'old.pdf', 3,
                        'disk:/project/item/old.pdf', 'synced', 'completed',
                        '', ''
                    )
                    """
                )
                conn.commit()
                conn.close()

                result = (
                    upload_jobs.ensure_yandex_upload_job_for_reconciliation(
                        dialog_id="chat1",
                        checklist_key="id",
                        item_id="item-1",
                        document_id="doc-1",
                        local_path="new.pdf",
                        file_name="new.pdf",
                        file_size=7,
                        force_requeue_synced=True,
                    )
                )
                saved = upload_jobs.get_upload_job("job-1") or {}

            self.assertEqual(result["reconciledAction"], "requeued_remote_missing")
            self.assertEqual(saved["status"], "queued")
            self.assertEqual(saved["yandex_path"], "")
            self.assertEqual(saved["local_path"], "new.pdf")

    def test_local_file_is_queued_when_exact_remote_path_is_absent(self):
        from app.checklists import yandex_mirror_reconciliation as reconciliation

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            local_file = root / "file.pdf"
            local_file.write_bytes(b"local primary")
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
                            "name": "Документ",
                            "group": 1,
                            "documents": [{
                                "id": "doc-1",
                                "name": "file.pdf",
                                "fileUrl": "/uploads/file.pdf",
                                "mirrorStatus": "synced",
                                "yandexPath": "disk:/project/item/file.pdf",
                            }],
                        }],
                    }, ensure_ascii=False),
                ),
            )
            conn.commit()
            conn.close()

            ensure_job = Mock(return_value={
                "job_id": "job-1",
                "status": "queued",
                "reconciledAction": "requeued_remote_missing",
            })
            update = Mock(return_value=True)
            enqueue = Mock(return_value={"queued": True})
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
                        "status": "missing",
                        "checkedPaths": ["disk:/project/item/file.pdf"],
                        "matches": [],
                        "conflicts": [],
                    },
                ),
                patch.object(reconciliation, "ensure_yandex_upload_job_for_reconciliation", ensure_job),
                patch.object(reconciliation, "update_document_mirror_fields", update),
                patch.object(reconciliation, "enqueue_yandex_mirror_job", enqueue),
            ):
                result = reconciliation.reconcile_yandex_mirror_documents(
                    source="test",
                    dialog_id="chat1",
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["remoteMissing"], 1)
            self.assertEqual(result["queued"], 1)
            self.assertTrue(
                ensure_job.call_args.kwargs["force_requeue_synced"]
            )
            self.assertEqual(
                update.call_args.args[4]["yandexPath"],
                "",
            )


if __name__ == "__main__":
    unittest.main()
