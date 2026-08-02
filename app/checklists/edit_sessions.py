from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from app.db import get_conn
from app.settings import EDIT_SESSION_TTL_SECONDS

from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


ACTIVE_EDIT_SESSION_STATUSES = {
    "active",
    "committing",
    "rolling_back",
    "error",
}

TERMINAL_EDIT_SESSION_STATUSES = {
    "committed",
    "rolled_back",
}

EXPLICIT_ROLLBACK_REASONS = frozenset({
    "cancel_button",
})


class EditSessionError(RuntimeError):
    pass


class EditSessionNotFoundError(EditSessionError):
    pass


class EditSessionConflictError(EditSessionError):
    pass


class EditSessionPermissionError(EditSessionError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat(timespec="seconds")


def utc_after_seconds_iso(seconds: int) -> str:
    safe_seconds = max(1, int(seconds or 0))
    return (
        utc_now()
        + timedelta(seconds=safe_seconds)
    ).isoformat(timespec="seconds")


def parse_iso_datetime(value: str) -> datetime | None:
    raw = clean_cell_value(value)

    if not raw:
        return None

    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def json_loads(value: str, default: Any) -> Any:
    raw = clean_cell_value(value)

    if not raw:
        return default

    try:
        return json.loads(raw)
    except Exception:
        return default


def row_to_dict(row) -> dict | None:
    return dict(row) if row else None


def ensure_edit_session_tables() -> None:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS edit_sessions (
            session_id TEXT PRIMARY KEY,
            dialog_id TEXT NOT NULL,
            user_id TEXT,
            user_name TEXT,
            client_session_id TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            close_reason TEXT,
            metadata_json TEXT,
            started_at TEXT,
            heartbeat_at TEXT,
            expires_at TEXT,
            commit_started_at TEXT,
            committed_at TEXT,
            rollback_started_at TEXT,
            rolled_back_at TEXT,
            expired_at TEXT,
            error TEXT,
            recovery_attempts INTEGER DEFAULT 0,
            last_recovery_at TEXT,
            last_recovery_source TEXT,
            last_recovery_action TEXT,
            last_recovery_error TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    edit_session_columns = {
        row["name"]
        for row in cur.execute(
            "PRAGMA table_info(edit_sessions)"
        ).fetchall()
    }

    edit_session_recovery_columns = {
        "recovery_attempts": "INTEGER DEFAULT 0",
        "last_recovery_at": "TEXT",
        "last_recovery_source": "TEXT",
        "last_recovery_action": "TEXT",
        "last_recovery_error": "TEXT",
    }

    for column_name, column_sql in (
        edit_session_recovery_columns.items()
    ):
        if column_name not in edit_session_columns:
            cur.execute(
                "ALTER TABLE edit_sessions "
                f"ADD COLUMN {column_name} {column_sql}"
            )

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_sessions_status_expiry
        ON edit_sessions(status, expires_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_sessions_dialog_status
        ON edit_sessions(dialog_id, status, updated_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_sessions_client
        ON edit_sessions(
            dialog_id,
            client_session_id,
            user_id,
            status
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS edit_session_checklists (
            session_id TEXT NOT NULL,
            dialog_id TEXT NOT NULL,
            checklist_key TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            lock_id TEXT,
            snapshot_id TEXT,
            started_at TEXT,
            updated_at TEXT,
            PRIMARY KEY (
                session_id,
                dialog_id,
                checklist_key
            )
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_session_checklists_session
        ON edit_session_checklists(session_id, status)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS edit_session_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            dialog_id TEXT NOT NULL,
            checklist_key TEXT NOT NULL,
            snapshot_json TEXT NOT NULL,
            snapshot_hash TEXT,
            created_at TEXT,
            UNIQUE (
                session_id,
                dialog_id,
                checklist_key
            )
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_session_snapshots_session
        ON edit_session_snapshots(session_id, created_at)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS edit_session_operations (
            operation_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            sequence_no INTEGER NOT NULL,
            operation_type TEXT NOT NULL,
            dialog_id TEXT,
            checklist_key TEXT,
            item_id TEXT,
            series_id TEXT,
            document_id TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            before_json TEXT,
            after_json TEXT,
            payload_json TEXT,
            error TEXT,
            created_at TEXT,
            updated_at TEXT,
            committed_at TEXT,
            rolled_back_at TEXT
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_edit_session_operations_seq
        ON edit_session_operations(session_id, sequence_no)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_session_operations_status
        ON edit_session_operations(session_id, status, sequence_no)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS edit_session_file_entries (
            entry_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            sequence_no INTEGER NOT NULL,
            operation_id TEXT,
            operation_type TEXT,
            dialog_id TEXT,
            checklist_key TEXT,
            item_id TEXT,
            series_id TEXT,
            document_id TEXT,
            entry_kind TEXT NOT NULL,
            original_path TEXT NOT NULL,
            staged_path TEXT,
            file_name TEXT,
            file_size INTEGER DEFAULT 0,
            sha256 TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            metadata_json TEXT,
            error TEXT,
            created_at TEXT,
            updated_at TEXT,
            committed_at TEXT,
            rolled_back_at TEXT,
            cleanup_at TEXT
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            idx_edit_session_file_entries_seq
        ON edit_session_file_entries(session_id, sequence_no)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS
            idx_edit_session_file_entries_status
        ON edit_session_file_entries(session_id, status, sequence_no)
    """)

    conn.commit()
    conn.close()


def get_edit_session(session_id: str) -> dict | None:
    ensure_edit_session_tables()

    normalized_session_id = clean_cell_value(session_id)

    if not normalized_session_id:
        return None

    conn = get_conn()

    row = conn.execute("""
        SELECT *
        FROM edit_sessions
        WHERE session_id = ?
    """, (
        normalized_session_id,
    )).fetchone()

    conn.close()

    return row_to_dict(row)


def get_edit_session_counts(session_id: str) -> dict:
    ensure_edit_session_tables()

    normalized_session_id = clean_cell_value(session_id)

    if not normalized_session_id:
        return {
            "checklistCount": 0,
            "snapshotCount": 0,
            "operationCount": 0,
            "pendingOperationCount": 0,
        }

    conn = get_conn()

    checklist_count = conn.execute("""
        SELECT COUNT(*) AS cnt
        FROM edit_session_checklists
        WHERE session_id = ?
    """, (
        normalized_session_id,
    )).fetchone()["cnt"]

    snapshot_count = conn.execute("""
        SELECT COUNT(*) AS cnt
        FROM edit_session_snapshots
        WHERE session_id = ?
    """, (
        normalized_session_id,
    )).fetchone()["cnt"]

    operation_count = conn.execute("""
        SELECT COUNT(*) AS cnt
        FROM edit_session_operations
        WHERE session_id = ?
    """, (
        normalized_session_id,
    )).fetchone()["cnt"]

    pending_operation_count = conn.execute("""
        SELECT COUNT(*) AS cnt
        FROM edit_session_operations
        WHERE session_id = ?
          AND status NOT IN ('committed', 'rolled_back')
    """, (
        normalized_session_id,
    )).fetchone()["cnt"]

    conn.close()

    from app.checklists.edit_session_files import (
        get_edit_session_file_counts,
    )

    file_counts = get_edit_session_file_counts(
        normalized_session_id
    )

    return {
        "checklistCount": int(checklist_count or 0),
        "snapshotCount": int(snapshot_count or 0),
        "operationCount": int(operation_count or 0),
        "pendingOperationCount": int(
            pending_operation_count or 0
        ),
        **file_counts,
    }


def public_edit_session_payload(record: dict | None) -> dict:
    if not record:
        return {}

    metadata = json_loads(
        record.get("metadata_json") or "",
        {},
    )

    payload = {
        "sessionId": record.get("session_id") or "",
        "dialogId": record.get("dialog_id") or "",
        "userId": record.get("user_id") or "",
        "userName": record.get("user_name") or "",
        "clientSessionId": (
            record.get("client_session_id") or ""
        ),
        "status": record.get("status") or "",
        "closeReason": record.get("close_reason") or "",
        "metadata": metadata,
        "startedAt": record.get("started_at") or "",
        "heartbeatAt": record.get("heartbeat_at") or "",
        "expiresAt": record.get("expires_at") or "",
        "commitStartedAt": (
            record.get("commit_started_at") or ""
        ),
        "committedAt": record.get("committed_at") or "",
        "rollbackStartedAt": (
            record.get("rollback_started_at") or ""
        ),
        "rolledBackAt": (
            record.get("rolled_back_at") or ""
        ),
        "expiredAt": record.get("expired_at") or "",
        "error": record.get("error") or "",
        "recoveryAttempts": int(
            record.get("recovery_attempts") or 0
        ),
        "lastRecoveryAt": (
            record.get("last_recovery_at") or ""
        ),
        "lastRecoverySource": (
            record.get("last_recovery_source") or ""
        ),
        "lastRecoveryAction": (
            record.get("last_recovery_action") or ""
        ),
        "lastRecoveryError": (
            record.get("last_recovery_error") or ""
        ),
        "createdAt": record.get("created_at") or "",
        "updatedAt": record.get("updated_at") or "",
    }

    payload.update(
        get_edit_session_counts(
            record.get("session_id") or ""
        )
    )

    return payload


def validate_edit_session_identity(
    record: dict,
    dialog_id: str = "",
    user_id: str = "",
) -> None:
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_user_id = clean_cell_value(user_id)

    if (
        normalized_dialog_id
        and record.get("dialog_id") != normalized_dialog_id
    ):
        raise EditSessionPermissionError(
            "edit session dialog mismatch"
        )

    record_user_id = clean_cell_value(
        record.get("user_id")
    )

    if (
        normalized_user_id
        and record_user_id
        and record_user_id != normalized_user_id
    ):
        raise EditSessionPermissionError(
            "edit session user mismatch"
        )


def is_edit_session_expired(record: dict) -> bool:
    expires_at = parse_iso_datetime(
        record.get("expires_at") or ""
    )

    return bool(
        expires_at
        and expires_at <= utc_now()
    )


def start_edit_session(
    dialog_id: str,
    user_id: str = "",
    user_name: str = "",
    client_session_id: str = "",
    metadata: dict | None = None,
) -> dict:
    ensure_edit_session_tables()

    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_user_id = clean_cell_value(user_id)
    normalized_user_name = clean_cell_value(user_name)
    normalized_client_session_id = clean_cell_value(
        client_session_id
    )

    if not normalized_dialog_id:
        raise ValueError("dialogId is required")

    now = utc_now_iso()
    expires_at = utc_after_seconds_iso(
        EDIT_SESSION_TTL_SECONDS
    )

    conn = get_conn()

    try:
        conn.execute("BEGIN IMMEDIATE")

        existing = None

        if normalized_client_session_id:
            existing = conn.execute("""
                SELECT *
                FROM edit_sessions
                WHERE dialog_id = ?
                  AND client_session_id = ?
                  AND COALESCE(user_id, '') = ?
                  AND status = 'active'
                ORDER BY created_at DESC
                LIMIT 1
            """, (
                normalized_dialog_id,
                normalized_client_session_id,
                normalized_user_id,
            )).fetchone()

        if existing:
            conn.execute("""
                UPDATE edit_sessions
                SET user_name = ?,
                    heartbeat_at = ?,
                    expires_at = ?,
                    updated_at = ?,
                    error = ''
                WHERE session_id = ?
            """, (
                normalized_user_name,
                now,
                expires_at,
                now,
                existing["session_id"],
            ))

            conn.commit()

            return {
                "created": False,
                "resumed": True,
                "session": get_edit_session(
                    existing["session_id"]
                ),
            }

        session_id = uuid.uuid4().hex

        conn.execute("""
            INSERT INTO edit_sessions(
                session_id,
                dialog_id,
                user_id,
                user_name,
                client_session_id,
                status,
                close_reason,
                metadata_json,
                started_at,
                heartbeat_at,
                expires_at,
                commit_started_at,
                committed_at,
                rollback_started_at,
                rolled_back_at,
                expired_at,
                error,
                created_at,
                updated_at
            )
            VALUES (
                ?, ?, ?, ?, ?,
                'active', '', ?,
                ?, ?, ?,
                '', '', '', '', '', '', ?, ?
            )
        """, (
            session_id,
            normalized_dialog_id,
            normalized_user_id,
            normalized_user_name,
            normalized_client_session_id,
            json_dumps(metadata or {}),
            now,
            now,
            expires_at,
            now,
            now,
        ))

        conn.commit()

    finally:
        conn.close()

    return {
        "created": True,
        "resumed": False,
        "session": get_edit_session(session_id),
    }


def heartbeat_edit_session(
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
) -> dict:
    ensure_edit_session_tables()

    normalized_session_id = clean_cell_value(session_id)

    if not normalized_session_id:
        raise ValueError("sessionId is required")

    record = get_edit_session(normalized_session_id)

    if not record:
        raise EditSessionNotFoundError(
            "edit session not found"
        )

    validate_edit_session_identity(
        record,
        dialog_id,
        user_id,
    )

    if record.get("status") != "active":
        raise EditSessionConflictError(
            "edit session is not active"
        )

    if is_edit_session_expired(record):
        saved = expire_edit_session(
            normalized_session_id,
            reason="heartbeat_timeout_autosave",
        )
        raise EditSessionConflictError(
            "edit session heartbeat expired; changes were saved"
            if saved.get("status") == "committed"
            else "edit session heartbeat expired"
        )

    now = utc_now_iso()
    expires_at = utc_after_seconds_iso(
        EDIT_SESSION_TTL_SECONDS
    )

    conn = get_conn()

    cur = conn.execute("""
        UPDATE edit_sessions
        SET heartbeat_at = ?,
            expires_at = ?,
            updated_at = ?,
            error = ''
        WHERE session_id = ?
          AND status = 'active'
    """, (
        now,
        expires_at,
        now,
        normalized_session_id,
    ))

    conn.commit()
    updated = int(cur.rowcount or 0)
    conn.close()

    if not updated:
        raise EditSessionConflictError(
            "edit session heartbeat rejected"
        )

    return get_edit_session(normalized_session_id) or {}


def get_edit_session_for_actor(
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
) -> dict:
    record = get_edit_session(session_id)

    if not record:
        raise EditSessionNotFoundError(
            "edit session not found"
        )

    validate_edit_session_identity(
        record,
        dialog_id,
        user_id,
    )

    return record


def begin_edit_session_commit(
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
    reason: str = "save_and_close",
) -> dict:
    ensure_edit_session_tables()

    record = get_edit_session_for_actor(
        session_id,
        dialog_id,
        user_id,
    )

    status = clean_cell_value(
        record.get("status")
    )

    if status == "committed":
        return record

    if status in {"rolling_back", "rolled_back"}:
        raise EditSessionConflictError(
            "rolled back edit session cannot be committed"
        )

    if status == "committing":
        return record

    if status not in {"active", "error"}:
        raise EditSessionConflictError(
            f"edit session cannot be committed from {status}"
        )

    now = utc_now_iso()
    conn = get_conn()

    cur = conn.execute("""
        UPDATE edit_sessions
        SET status = 'committing',
            close_reason = ?,
            commit_started_at = ?,
            error = '',
            updated_at = ?
        WHERE session_id = ?
          AND status IN ('active', 'error')
    """, (
        clean_cell_value(reason),
        now,
        now,
        record["session_id"],
    ))

    conn.commit()
    updated = int(cur.rowcount or 0)
    conn.close()

    if not updated:
        latest = get_edit_session(record["session_id"])

        if latest and latest.get("status") == "committing":
            return latest

        raise EditSessionConflictError(
            "edit session commit transition rejected"
        )

    return get_edit_session(record["session_id"]) or {}


def complete_edit_session_commit(
    session_id: str,
) -> dict:
    ensure_edit_session_tables()

    record = get_edit_session(session_id)

    if not record:
        raise EditSessionNotFoundError(
            "edit session not found"
        )

    if record.get("status") == "committed":
        return record

    if record.get("status") != "committing":
        raise EditSessionConflictError(
            "edit session is not committing"
        )

    from app.checklists.edit_session_locks import (
        release_edit_session_locks_in_transaction,
    )
    from app.checklists.edit_session_changes import (
        mark_edit_session_operations_committed_in_transaction,
    )
    from app.checklists.edit_session_files import (
        mark_edit_session_file_entries_committed_in_transaction,
        prepare_edit_session_files_for_commit,
        purge_committed_session_files,
    )
    from app.checklists.edit_session_yandex import (
        enqueue_committed_edit_session_yandex_jobs,
        ensure_edit_session_yandex_schema,
        prepare_edit_session_yandex_jobs_in_transaction,
    )
    from app.checklists.edit_session_structure import (
        enqueue_committed_edit_session_structure_jobs,
        prepare_edit_session_structure_jobs_in_transaction,
    )
    from app.checklists.yandex_structure_jobs import (
        ensure_yandex_structure_jobs_table,
    )
    from app.checklists.notification_drafts import (
        mark_notification_drafts_committed_in_transaction,
    )

    ensure_edit_session_yandex_schema()
    ensure_yandex_structure_jobs_table()

    prepare_edit_session_files_for_commit(
        record["session_id"]
    )

    now = utc_now_iso()
    conn = get_conn()

    try:
        conn.execute("BEGIN IMMEDIATE")

        yandex_prepare_result = (
            prepare_edit_session_yandex_jobs_in_transaction(
                conn,
                session_id=record["session_id"],
                now=now,
            )
        )
        structure_prepare_result = (
            prepare_edit_session_structure_jobs_in_transaction(
                conn,
                session_id=record["session_id"],
                now=now,
            )
        )

        cur = conn.execute("""
            UPDATE edit_sessions
            SET status = 'committed',
                committed_at = ?,
                expires_at = '',
                error = '',
                updated_at = ?
            WHERE session_id = ?
              AND status = 'committing'
        """, (
            now,
            now,
            record["session_id"],
        ))

        if int(cur.rowcount or 0) != 1:
            raise EditSessionConflictError(
                "edit session commit completion rejected"
            )

        mark_edit_session_operations_committed_in_transaction(
            conn,
            session_id=record["session_id"],
            now=now,
        )

        mark_notification_drafts_committed_in_transaction(
            conn,
            session_id=record["session_id"],
            now=now,
        )

        mark_edit_session_file_entries_committed_in_transaction(
            conn,
            session_id=record["session_id"],
            now=now,
        )

        release_edit_session_locks_in_transaction(
            conn,
            session_id=record["session_id"],
            reason="session_committed",
            now=now,
        )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    purge_committed_session_files(
        record["session_id"]
    )

    try:
        enqueue_committed_edit_session_yandex_jobs(
            record["session_id"],
            source="edit_session_commit",
        )
    except Exception:
        # Jobs уже записаны в SQLite атомарно вместе с commit.
        # Startup recovery повторно поставит queued jobs в очередь.
        pass

    try:
        enqueue_committed_edit_session_structure_jobs(
            record["session_id"],
            source="edit_session_commit",
        )
    except Exception:
        # Структурные jobs также записаны в SQLite вместе с commit.
        # Startup recovery повторно поставит queued jobs в очередь.
        pass

    return get_edit_session(record["session_id"]) or {}


def commit_edit_session(
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
    reason: str = "save_and_close",
) -> dict:
    record = begin_edit_session_commit(
        session_id,
        dialog_id,
        user_id,
        reason,
    )

    if record.get("status") == "committed":
        try:
            from app.checklists.edit_session_yandex import (
                enqueue_committed_edit_session_yandex_jobs,
            )
            enqueue_committed_edit_session_yandex_jobs(
                record.get("session_id") or session_id,
                source="edit_session_commit_repeat",
            )
        except Exception:
            pass
        return record

    return complete_edit_session_commit(
        record.get("session_id") or session_id
    )


def begin_edit_session_rollback(
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
    reason: str = "cancel",
    expired: bool = False,
) -> dict:
    ensure_edit_session_tables()

    record = get_edit_session_for_actor(
        session_id,
        dialog_id,
        user_id,
    )

    status = clean_cell_value(
        record.get("status")
    )

    if status == "rolled_back":
        return record

    if status in {"committing", "committed"}:
        raise EditSessionConflictError(
            "committing or committed session cannot be rolled back"
        )

    if status == "rolling_back":
        return record

    if status not in {"active", "error", "expired"}:
        raise EditSessionConflictError(
            f"edit session cannot be rolled back from {status}"
        )

    normalized_reason = clean_cell_value(reason)

    if (
        normalized_reason not in EXPLICIT_ROLLBACK_REASONS
        or expired
    ):
        raise EditSessionConflictError(
            "rollback is allowed only for an explicit confirmed cancel"
        )

    now = utc_now_iso()
    next_expired_at = record.get("expired_at") or ""

    conn = get_conn()

    cur = conn.execute("""
        UPDATE edit_sessions
        SET status = 'rolling_back',
            close_reason = ?,
            rollback_started_at = ?,
            expired_at = ?,
            error = '',
            updated_at = ?
        WHERE session_id = ?
          AND status IN ('active', 'error', 'expired')
    """, (
        normalized_reason,
        now,
        next_expired_at,
        now,
        record["session_id"],
    ))

    conn.commit()
    updated = int(cur.rowcount or 0)
    conn.close()

    if not updated:
        latest = get_edit_session(record["session_id"])

        if latest and latest.get("status") == "rolling_back":
            return latest

        raise EditSessionConflictError(
            "edit session rollback transition rejected"
        )

    return get_edit_session(record["session_id"]) or {}


def complete_edit_session_rollback(
    session_id: str,
) -> dict:
    ensure_edit_session_tables()

    record = get_edit_session(session_id)

    if not record:
        raise EditSessionNotFoundError(
            "edit session not found"
        )

    if record.get("status") == "rolled_back":
        return record

    if record.get("status") != "rolling_back":
        raise EditSessionConflictError(
            "edit session is not rolling back"
        )

    from app.checklists.edit_session_locks import (
        release_edit_session_locks_in_transaction,
    )
    from app.checklists.edit_session_changes import (
        mark_edit_session_operations_rolled_back_in_transaction,
        restore_edit_session_checklists,
    )
    from app.checklists.edit_session_files import (
        mark_edit_session_file_entries_rolled_back_in_transaction,
        rollback_edit_session_files,
    )
    from app.checklists.notification_drafts import (
        mark_notification_drafts_cancelled_in_transaction,
    )

    try:
        rollback_edit_session_files(
            record["session_id"]
        )
        restore_edit_session_checklists(
            record["session_id"]
        )
    except Exception as exc:
        now = utc_now_iso()
        conn = get_conn()
        conn.execute("""
            UPDATE edit_sessions
            SET status = 'error',
                error = ?,
                updated_at = ?
            WHERE session_id = ?
              AND status = 'rolling_back'
        """, (
            "edit session rollback failed: " + str(exc),
            now,
            record["session_id"],
        ))
        conn.commit()
        conn.close()
        raise

    now = utc_now_iso()
    conn = get_conn()

    try:
        conn.execute("BEGIN IMMEDIATE")

        cur = conn.execute("""
            UPDATE edit_sessions
            SET status = 'rolled_back',
                rolled_back_at = ?,
                expires_at = '',
                error = '',
                updated_at = ?
            WHERE session_id = ?
              AND status = 'rolling_back'
        """, (
            now,
            now,
            record["session_id"],
        ))

        if int(cur.rowcount or 0) != 1:
            raise EditSessionConflictError(
                "edit session rollback completion rejected"
            )

        mark_edit_session_operations_rolled_back_in_transaction(
            conn,
            session_id=record["session_id"],
            now=now,
        )

        mark_notification_drafts_cancelled_in_transaction(
            conn,
            session_id=record["session_id"],
            now=now,
        )

        mark_edit_session_file_entries_rolled_back_in_transaction(
            conn,
            session_id=record["session_id"],
            now=now,
        )

        release_edit_session_locks_in_transaction(
            conn,
            session_id=record["session_id"],
            reason="session_rolled_back",
            now=now,
        )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    return get_edit_session(record["session_id"]) or {}


def rollback_edit_session(
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
    reason: str = "cancel",
    expired: bool = False,
) -> dict:
    record = begin_edit_session_rollback(
        session_id,
        dialog_id,
        user_id,
        reason,
        expired,
    )

    if record.get("status") == "rolled_back":
        return record

    return complete_edit_session_rollback(
        record.get("session_id") or session_id
    )


def expire_edit_session(
    session_id: str,
    reason: str = "heartbeat_timeout_autosave",
) -> dict:
    """Save an abandoned session instead of rolling it back.

    Session expiry, process restart, browser close and network loss are
    technical lifecycle events. They must never delete user files or restore
    an old snapshot. Only the explicit confirmed Cancel action may invoke the
    rollback path.
    """
    record = get_edit_session(session_id)

    if not record:
        raise EditSessionNotFoundError(
            "edit session not found"
        )

    if record.get("status") in TERMINAL_EDIT_SESSION_STATUSES:
        return record

    if record.get("status") == "rolling_back":
        raise EditSessionConflictError(
            "explicit rollback is already in progress"
        )

    return commit_edit_session(
        record["session_id"],
        reason=clean_cell_value(reason)
        or "heartbeat_timeout_autosave",
    )


def list_expired_edit_sessions(
    limit: int = 100,
) -> list[dict]:
    ensure_edit_session_tables()

    safe_limit = max(
        1,
        min(int(limit or 100), 1000),
    )

    now = utc_now_iso()
    conn = get_conn()

    rows = conn.execute("""
        SELECT *
        FROM edit_sessions
        WHERE status IN ('active', 'error')
          AND expires_at <> ''
          AND expires_at <= ?
        ORDER BY expires_at ASC
        LIMIT ?
    """, (
        now,
        safe_limit,
    )).fetchall()

    conn.close()

    return [dict(row) for row in rows]


def sweep_expired_edit_sessions(
    source: str = "sweeper",
    limit: int = 100,
) -> dict:
    """Commit expired sessions; never roll them back automatically."""
    expired_records = list_expired_edit_sessions(
        limit=limit
    )

    committed = []
    errors = []

    for record in expired_records:
        session_id = record.get("session_id") or ""

        try:
            result = expire_edit_session(
                session_id,
                reason=f"{source}_heartbeat_timeout_autosave",
            )
            committed.append(
                public_edit_session_payload(result)
            )
        except Exception as exc:
            errors.append({
                "sessionId": session_id,
                "error": str(exc),
            })

    return {
        "ok": not errors,
        "source": clean_cell_value(source),
        "expiredFound": len(expired_records),
        "savedCount": len(committed),
        "committedCount": len(committed),
        "rolledBackCount": 0,
        "errorCount": len(errors),
        "saved": committed,
        "committed": committed,
        "rolledBack": [],
        "errors": errors,
    }


def recover_interrupted_edit_sessions(
    source: str = "startup",
) -> dict:
    from app.checklists.edit_session_recovery import (
        recover_edit_session_lifecycle,
    )

    return recover_edit_session_lifecycle(
        source=source
    )


def create_edit_session_snapshot(
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    snapshot: dict,
) -> dict:
    ensure_edit_session_tables()

    record = get_edit_session_for_actor(
        session_id,
        dialog_id=dialog_id,
    )

    if record.get("status") != "active":
        raise EditSessionConflictError(
            "snapshot can be created only for active session"
        )

    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(
        checklist_key
    )
    snapshot_json = json_dumps(snapshot or {})
    snapshot_hash = hashlib.sha256(
        snapshot_json.encode("utf-8")
    ).hexdigest()
    snapshot_id = uuid.uuid4().hex
    now = utc_now_iso()

    conn = get_conn()

    conn.execute("""
        INSERT OR IGNORE INTO edit_session_snapshots(
            snapshot_id,
            session_id,
            dialog_id,
            checklist_key,
            snapshot_json,
            snapshot_hash,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        snapshot_id,
        record["session_id"],
        normalized_dialog_id,
        normalized_checklist_key,
        snapshot_json,
        snapshot_hash,
        now,
    ))

    conn.execute("""
        INSERT INTO edit_session_checklists(
            session_id,
            dialog_id,
            checklist_key,
            status,
            lock_id,
            snapshot_id,
            started_at,
            updated_at
        )
        VALUES (?, ?, ?, 'active', '', ?, ?, ?)
        ON CONFLICT(session_id, dialog_id, checklist_key)
        DO UPDATE SET
            snapshot_id = COALESCE(
                NULLIF(edit_session_checklists.snapshot_id, ''),
                excluded.snapshot_id
            ),
            updated_at = excluded.updated_at
    """, (
        record["session_id"],
        normalized_dialog_id,
        normalized_checklist_key,
        snapshot_id,
        now,
        now,
    ))

    conn.commit()

    saved = conn.execute("""
        SELECT *
        FROM edit_session_snapshots
        WHERE session_id = ?
          AND dialog_id = ?
          AND checklist_key = ?
    """, (
        record["session_id"],
        normalized_dialog_id,
        normalized_checklist_key,
    )).fetchone()

    conn.close()

    return dict(saved) if saved else {}
