from __future__ import annotations

import sqlite3
import uuid
from typing import Any

from app.db import get_conn

from app.checklists.edit_sessions import (
    ACTIVE_EDIT_SESSION_STATUSES,
    EditSessionConflictError,
    EditSessionNotFoundError,
    ensure_edit_session_tables,
    get_edit_session_for_actor,
    is_edit_session_expired,
    sweep_expired_edit_sessions,
    utc_now_iso,
)
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


LOCKED_CHECKLIST_STATUS = "locked"
RELEASED_CHECKLIST_STATUS = "released"

LOCK_SCHEMA_COLUMNS = {
    "lock_acquired_at": "TEXT",
    "lock_heartbeat_at": "TEXT",
    "lock_released_at": "TEXT",
    "lock_release_reason": "TEXT",
}


def ensure_edit_session_lock_schema() -> None:
    ensure_edit_session_tables()

    conn = get_conn()

    try:
        columns = {
            str(row["name"])
            for row in conn.execute(
                "PRAGMA table_info(edit_session_checklists)"
            ).fetchall()
        }

        for column_name, declaration in (
            LOCK_SCHEMA_COLUMNS.items()
        ):
            if column_name in columns:
                continue

            conn.execute(
                "ALTER TABLE edit_session_checklists "
                f"ADD COLUMN {column_name} {declaration}"
            )

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_edit_session_checklists_owner
            ON edit_session_checklists(
                dialog_id,
                checklist_key,
                status,
                lock_id
            )
        """)

        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_edit_session_checklists_active_lock
            ON edit_session_checklists(
                dialog_id,
                checklist_key
            )
            WHERE status = 'locked'
              AND COALESCE(lock_id, '') <> ''
        """)

        conn.commit()

    finally:
        conn.close()


def _public_lock_payload(row: Any) -> dict:
    if not row:
        return {}

    record = dict(row)

    return {
        "sessionId": record.get("session_id") or "",
        "dialogId": record.get("dialog_id") or "",
        "checklistKey": record.get("checklist_key") or "",
        "status": record.get("status") or "",
        "lockId": record.get("lock_id") or "",
        "userId": record.get("owner_user_id") or "",
        "userName": record.get("owner_user_name") or "",
        "acquiredAt": record.get("lock_acquired_at") or "",
        "heartbeatAt": record.get("lock_heartbeat_at") or "",
        "updatedAt": record.get("updated_at") or "",
    }


def _release_terminal_locks_in_transaction(
    conn,
    *,
    now: str,
) -> int:
    cur = conn.execute("""
        UPDATE edit_session_checklists
        SET status = 'released',
            lock_id = '',
            lock_released_at = ?,
            lock_release_reason = 'terminal_session_cleanup',
            updated_at = ?
        WHERE status = 'locked'
          AND COALESCE(lock_id, '') <> ''
          AND session_id IN (
              SELECT session_id
              FROM edit_sessions
              WHERE status IN ('committed', 'rolled_back')
          )
    """, (
        now,
        now,
    ))

    return int(cur.rowcount or 0)


def _validate_active_session(
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
) -> dict:
    record = get_edit_session_for_actor(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )

    if record.get("status") != "active":
        raise EditSessionConflictError(
            "edit session is not active"
        )

    if is_edit_session_expired(record):
        sweep_expired_edit_sessions(
            source="lock_validation",
            limit=1000,
        )
        raise EditSessionConflictError(
            "edit session expired and was saved"
        )

    return record


def list_edit_session_locks(
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
) -> list[dict]:
    ensure_edit_session_lock_schema()

    record = get_edit_session_for_actor(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )

    conn = get_conn()

    try:
        rows = conn.execute("""
            SELECT
                c.*,
                s.user_id AS owner_user_id,
                s.user_name AS owner_user_name
            FROM edit_session_checklists AS c
            JOIN edit_sessions AS s
              ON s.session_id = c.session_id
            WHERE c.session_id = ?
              AND c.status = 'locked'
              AND COALESCE(c.lock_id, '') <> ''
            ORDER BY c.lock_acquired_at ASC,
                     c.checklist_key ASC
        """, (
            record["session_id"],
        )).fetchall()

    finally:
        conn.close()

    return [
        _public_lock_payload(row)
        for row in rows
    ]


def acquire_edit_session_lock(
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    user_id: str = "",
    user_name: str = "",
    requested_lock_id: str = "",
) -> dict:
    ensure_edit_session_lock_schema()

    normalized_session_id = clean_cell_value(session_id)
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(
        checklist_key
    )
    normalized_user_id = clean_cell_value(user_id)
    normalized_user_name = clean_cell_value(user_name)
    normalized_requested_lock_id = clean_cell_value(
        requested_lock_id
    )

    if not normalized_session_id:
        raise ValueError("sessionId is required")

    if not normalized_dialog_id:
        raise ValueError("dialogId is required")

    if not normalized_checklist_key:
        raise ValueError("checklistKey is required")

    # До проверки владельца автоматически сохраняем сессии, у которых
    # истёк общий heartbeat. Commit освобождает их locks без удаления
    # пользовательских файлов и без восстановления старого snapshot.
    sweep_expired_edit_sessions(
        source="lock_acquire",
        limit=1000,
    )

    session = _validate_active_session(
        session_id=normalized_session_id,
        dialog_id=normalized_dialog_id,
        user_id=normalized_user_id,
    )

    now = utc_now_iso()
    conn = get_conn()

    try:
        conn.execute("BEGIN IMMEDIATE")
        _release_terminal_locks_in_transaction(
            conn,
            now=now,
        )

        existing = conn.execute("""
            SELECT
                c.*,
                s.user_id AS owner_user_id,
                s.user_name AS owner_user_name,
                s.status AS owner_session_status
            FROM edit_session_checklists AS c
            JOIN edit_sessions AS s
              ON s.session_id = c.session_id
            WHERE c.dialog_id = ?
              AND c.checklist_key = ?
              AND c.status = 'locked'
              AND COALESCE(c.lock_id, '') <> ''
              AND s.status IN (
                  'active',
                  'committing',
                  'rolling_back',
                  'error'
              )
            ORDER BY c.updated_at DESC
            LIMIT 1
        """, (
            normalized_dialog_id,
            normalized_checklist_key,
        )).fetchone()

        if (
            existing
            and existing["session_id"]
            != session["session_id"]
        ):
            conn.commit()

            owner = _public_lock_payload(existing)

            return {
                "ok": True,
                "owned": False,
                "lockedByOther": True,
                "sessionBound": True,
                "retainedUntilSessionEnd": True,
                "lockId": "",
                "sessionId": session["session_id"],
                "dialogId": normalized_dialog_id,
                "checklistKey": normalized_checklist_key,
                "userId": owner.get("userId") or "",
                "userName": (
                    owner.get("userName")
                    or "Другой сотрудник"
                ),
                "updatedAt": owner.get("updatedAt") or "",
            }

        effective_lock_id = (
            (
                existing["lock_id"]
                if existing
                else ""
            )
            or normalized_requested_lock_id
            or uuid.uuid4().hex
        )

        owner_user_name = (
            normalized_user_name
            or clean_cell_value(session.get("user_name"))
            or "Неизвестный пользователь"
        )

        conn.execute("""
            INSERT INTO edit_session_checklists(
                session_id,
                dialog_id,
                checklist_key,
                status,
                lock_id,
                snapshot_id,
                started_at,
                updated_at,
                lock_acquired_at,
                lock_heartbeat_at,
                lock_released_at,
                lock_release_reason
            )
            VALUES (
                ?, ?, ?, 'locked', ?, '', ?, ?, ?, ?, '', ''
            )
            ON CONFLICT(
                session_id,
                dialog_id,
                checklist_key
            )
            DO UPDATE SET
                status = 'locked',
                lock_id = excluded.lock_id,
                started_at = COALESCE(
                    NULLIF(edit_session_checklists.started_at, ''),
                    excluded.started_at
                ),
                updated_at = excluded.updated_at,
                lock_acquired_at = COALESCE(
                    NULLIF(
                        edit_session_checklists.lock_acquired_at,
                        ''
                    ),
                    excluded.lock_acquired_at
                ),
                lock_heartbeat_at = excluded.lock_heartbeat_at,
                lock_released_at = '',
                lock_release_reason = ''
        """, (
            session["session_id"],
            normalized_dialog_id,
            normalized_checklist_key,
            effective_lock_id,
            now,
            now,
            now,
            now,
        ))

        conn.commit()

    except sqlite3.IntegrityError:
        conn.rollback()

        # Защитная ветка на случай конкурирующей записи между
        # несколькими процессами приложения.
        return acquire_edit_session_lock(
            session_id=normalized_session_id,
            dialog_id=normalized_dialog_id,
            checklist_key=normalized_checklist_key,
            user_id=normalized_user_id,
            user_name=normalized_user_name,
            requested_lock_id=normalized_requested_lock_id,
        )

    finally:
        conn.close()

    return {
        "ok": True,
        "owned": True,
        "lockedByOther": False,
        "sessionBound": True,
        "retainedUntilSessionEnd": True,
        "lockId": effective_lock_id,
        "sessionId": session["session_id"],
        "dialogId": normalized_dialog_id,
        "checklistKey": normalized_checklist_key,
        "userId": clean_cell_value(
            session.get("user_id")
        ),
        "userName": owner_user_name,
        "updatedAt": now,
    }


def heartbeat_edit_session_lock(
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    user_id: str = "",
    user_name: str = "",
    lock_id: str = "",
) -> dict:
    ensure_edit_session_lock_schema()

    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(
        checklist_key
    )
    normalized_lock_id = clean_cell_value(lock_id)

    session = _validate_active_session(
        session_id=session_id,
        dialog_id=normalized_dialog_id,
        user_id=user_id,
    )

    now = utc_now_iso()
    conn = get_conn()

    try:
        row = conn.execute("""
            SELECT *
            FROM edit_session_checklists
            WHERE session_id = ?
              AND dialog_id = ?
              AND checklist_key = ?
              AND status = 'locked'
              AND COALESCE(lock_id, '') <> ''
        """, (
            session["session_id"],
            normalized_dialog_id,
            normalized_checklist_key,
        )).fetchone()

        if not row:
            return {
                "ok": True,
                "owned": False,
                "lockedByOther": False,
                "lockExpired": True,
                "sessionBound": True,
                "lockId": "",
                "sessionId": session["session_id"],
                "dialogId": normalized_dialog_id,
                "checklistKey": normalized_checklist_key,
                "userId": "",
                "userName": "",
                "updatedAt": "",
            }

        existing_lock_id = clean_cell_value(
            row["lock_id"]
        )

        if (
            normalized_lock_id
            and existing_lock_id != normalized_lock_id
        ):
            return {
                "ok": True,
                "owned": False,
                "lockedByOther": False,
                "lockExpired": True,
                "sessionBound": True,
                "lockId": "",
                "sessionId": session["session_id"],
                "dialogId": normalized_dialog_id,
                "checklistKey": normalized_checklist_key,
                "userId": clean_cell_value(
                    session.get("user_id")
                ),
                "userName": clean_cell_value(
                    session.get("user_name")
                ),
                "updatedAt": row["updated_at"] or "",
            }

        conn.execute("""
            UPDATE edit_session_checklists
            SET lock_heartbeat_at = ?,
                updated_at = ?
            WHERE session_id = ?
              AND dialog_id = ?
              AND checklist_key = ?
              AND status = 'locked'
        """, (
            now,
            now,
            session["session_id"],
            normalized_dialog_id,
            normalized_checklist_key,
        ))
        conn.commit()

    finally:
        conn.close()

    return {
        "ok": True,
        "owned": True,
        "lockedByOther": False,
        "lockExpired": False,
        "sessionBound": True,
        "retainedUntilSessionEnd": True,
        "lockId": existing_lock_id,
        "sessionId": session["session_id"],
        "dialogId": normalized_dialog_id,
        "checklistKey": normalized_checklist_key,
        "userId": clean_cell_value(
            session.get("user_id")
        ),
        "userName": (
            clean_cell_value(user_name)
            or clean_cell_value(session.get("user_name"))
        ),
        "updatedAt": now,
    }


def heartbeat_all_edit_session_locks(
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
) -> dict:
    ensure_edit_session_lock_schema()

    session = _validate_active_session(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )

    now = utc_now_iso()
    conn = get_conn()

    try:
        cur = conn.execute("""
            UPDATE edit_session_checklists
            SET lock_heartbeat_at = ?,
                updated_at = ?
            WHERE session_id = ?
              AND status = 'locked'
              AND COALESCE(lock_id, '') <> ''
        """, (
            now,
            now,
            session["session_id"],
        ))
        conn.commit()
        renewed_count = int(cur.rowcount or 0)

    finally:
        conn.close()

    return {
        "ok": True,
        "sessionId": session["session_id"],
        "renewedCount": renewed_count,
        "updatedAt": now,
    }


def release_edit_session_lock(
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    user_id: str = "",
    lock_id: str = "",
) -> dict:
    """Release a clean stage lock and retain a mutated stage lock.

    A stage that has mutations must stay locked until the global edit session
    commits or rolls back; otherwise another user could edit it and a later
    rollback could overwrite those concurrent changes.  A stage that was only
    viewed can be released immediately when the popup switches away from it.
    """
    ensure_edit_session_lock_schema()

    session = get_edit_session_for_actor(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )

    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(
        checklist_key
    )
    normalized_lock_id = clean_cell_value(lock_id)
    now = utc_now_iso()

    conn = get_conn()

    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("""
            SELECT *
            FROM edit_session_checklists
            WHERE session_id = ?
              AND dialog_id = ?
              AND checklist_key = ?
              AND status = 'locked'
              AND COALESCE(lock_id, '') <> ''
        """, (
            session["session_id"],
            normalized_dialog_id,
            normalized_checklist_key,
        )).fetchone()

        if not row:
            conn.commit()
            return {
                "ok": True,
                "released": False,
                "owned": False,
                "lockedByOther": False,
                "sessionBound": True,
                "retainedUntilSessionEnd": False,
                "lockId": "",
                "sessionId": session["session_id"],
                "dialogId": normalized_dialog_id,
                "checklistKey": normalized_checklist_key,
                "reason": "lock_not_found",
            }

        existing_lock_id = clean_cell_value(row["lock_id"])

        if (
            normalized_lock_id
            and existing_lock_id != normalized_lock_id
        ):
            conn.commit()
            return {
                "ok": True,
                "released": False,
                "owned": False,
                "lockedByOther": False,
                "sessionBound": True,
                "retainedUntilSessionEnd": False,
                "lockId": "",
                "sessionId": session["session_id"],
                "dialogId": normalized_dialog_id,
                "checklistKey": normalized_checklist_key,
                "reason": "lock_id_mismatch",
            }

        mutation_count = int(row["mutation_count"] or 0)
        has_snapshot = bool(
            clean_cell_value(row["snapshot_id"])
        )
        has_mutations = bool(
            mutation_count > 0 or has_snapshot
        )

        if has_mutations:
            conn.commit()
            return {
                "ok": True,
                "released": False,
                "owned": True,
                "lockedByOther": False,
                "sessionBound": True,
                "retainedUntilSessionEnd": True,
                "lockId": existing_lock_id,
                "sessionId": session["session_id"],
                "dialogId": normalized_dialog_id,
                "checklistKey": normalized_checklist_key,
                "mutationCount": mutation_count,
                "reason": "stage_has_session_mutations",
            }

        conn.execute("""
            UPDATE edit_session_checklists
            SET status = 'released',
                lock_id = '',
                lock_released_at = ?,
                lock_release_reason = 'clean_stage_switch',
                updated_at = ?
            WHERE session_id = ?
              AND dialog_id = ?
              AND checklist_key = ?
              AND status = 'locked'
              AND lock_id = ?
        """, (
            now,
            now,
            session["session_id"],
            normalized_dialog_id,
            normalized_checklist_key,
            existing_lock_id,
        ))
        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    return {
        "ok": True,
        "released": True,
        "owned": False,
        "lockedByOther": False,
        "sessionBound": True,
        "retainedUntilSessionEnd": False,
        "lockId": "",
        "sessionId": session["session_id"],
        "dialogId": normalized_dialog_id,
        "checklistKey": normalized_checklist_key,
        "mutationCount": 0,
        "reason": "clean_stage_released",
        "releasedAt": now,
    }


def retain_edit_session_lock_until_session_end(
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    user_id: str = "",
    lock_id: str = "",
) -> dict:
    ensure_edit_session_lock_schema()

    session = get_edit_session_for_actor(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )

    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(
        checklist_key
    )
    normalized_lock_id = clean_cell_value(lock_id)

    conn = get_conn()

    try:
        row = conn.execute("""
            SELECT *
            FROM edit_session_checklists
            WHERE session_id = ?
              AND dialog_id = ?
              AND checklist_key = ?
              AND status = 'locked'
              AND COALESCE(lock_id, '') <> ''
        """, (
            session["session_id"],
            normalized_dialog_id,
            normalized_checklist_key,
        )).fetchone()

    finally:
        conn.close()

    existing_lock_id = (
        clean_cell_value(row["lock_id"])
        if row
        else ""
    )
    owned = bool(
        row
        and (
            not normalized_lock_id
            or normalized_lock_id == existing_lock_id
        )
    )

    return {
        "ok": True,
        "released": False,
        "owned": owned,
        "lockedByOther": False,
        "sessionBound": True,
        "retainedUntilSessionEnd": owned,
        "lockId": existing_lock_id if owned else "",
        "sessionId": session["session_id"],
        "dialogId": normalized_dialog_id,
        "checklistKey": normalized_checklist_key,
        "userId": clean_cell_value(
            session.get("user_id")
        ),
        "userName": clean_cell_value(
            session.get("user_name")
        ),
    }


def release_edit_session_locks_in_transaction(
    conn,
    *,
    session_id: str,
    reason: str,
    now: str | None = None,
) -> int:
    normalized_session_id = clean_cell_value(session_id)

    if not normalized_session_id:
        raise ValueError("sessionId is required")

    effective_now = clean_cell_value(now) or utc_now_iso()
    effective_reason = (
        clean_cell_value(reason)
        or "session_finished"
    )

    cur = conn.execute("""
        UPDATE edit_session_checklists
        SET status = 'released',
            lock_id = '',
            lock_released_at = ?,
            lock_release_reason = ?,
            updated_at = ?
        WHERE session_id = ?
          AND status = 'locked'
          AND COALESCE(lock_id, '') <> ''
    """, (
        effective_now,
        effective_reason,
        effective_now,
        normalized_session_id,
    ))

    return int(cur.rowcount or 0)


def release_all_edit_session_locks(
    session_id: str,
    reason: str = "session_finished",
) -> dict:
    ensure_edit_session_lock_schema()

    normalized_session_id = clean_cell_value(session_id)

    if not normalized_session_id:
        raise ValueError("sessionId is required")

    # Проверяем существование сессии, но не требуем active:
    # функция используется при commit, rollback и восстановлении.
    from app.checklists.edit_sessions import get_edit_session

    session = get_edit_session(normalized_session_id)

    if not session:
        raise EditSessionNotFoundError(
            "edit session not found"
        )

    now = utc_now_iso()
    conn = get_conn()

    try:
        conn.execute("BEGIN IMMEDIATE")
        released_count = (
            release_edit_session_locks_in_transaction(
                conn,
                session_id=normalized_session_id,
                reason=reason,
                now=now,
            )
        )
        conn.commit()

    finally:
        conn.close()

    return {
        "ok": True,
        "sessionId": normalized_session_id,
        "releasedCount": released_count,
        "releasedAt": now,
        "reason": clean_cell_value(reason),
    }
