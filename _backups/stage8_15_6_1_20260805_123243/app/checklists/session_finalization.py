from __future__ import annotations

import hashlib
import json
import threading
import uuid
from typing import Any

from app.bitrix.client import bitrix_webhook_call
from app.db import get_conn
from app.logging_utils import write_debug_log

from app.checklists.edit_sessions import (
    EditSessionConflictError,
    commit_edit_session,
    get_edit_session_for_actor,
    utc_now_iso,
)
from app.checklists.messages import (
    build_multi_checklist_chat_message,
    build_recent_changes_sections,
)
from app.checklists.normalization import normalize_checklist_data
from app.checklists.storage import get_checklist, save_checklist
from app.checklists.session_summary import (
    build_session_summary_sessions,
)
from app.checklists.notification_delivery import (
    deliver_committed_notification_drafts,
)
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


FINALIZATION_SCHEMA_VERSION = 1
FINALIZATION_ACTIVE_STATUSES = frozenset({
    "preparing",
    "local_saved",
    "committing",
})

FINALIZATION_EXTRA_COLUMNS = {
    "payload_json": "TEXT",
    "delivery_status": "TEXT NOT NULL DEFAULT 'pending'",
    "notification_status": "TEXT NOT NULL DEFAULT 'pending'",
    "delivery_error": "TEXT",
    "delivery_claimed_at": "TEXT",
}

_FINALIZATION_DELIVERY_GUARD = threading.Lock()
_FINALIZATION_DELIVERY_RUNNING: set[str] = set()


class SessionFinalizationError(RuntimeError):
    pass


class SessionFinalizationInProgressError(SessionFinalizationError):
    pass


class SessionFinalizationPayloadConflictError(SessionFinalizationError):
    pass


def _json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _json_loads(value: str, default: Any) -> Any:
    raw = clean_cell_value(value)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def ensure_session_finalization_schema() -> None:
    conn = get_conn()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS edit_session_finalizations (
                session_id TEXT PRIMARY KEY,
                dialog_id TEXT NOT NULL,
                user_id TEXT,
                payload_hash TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'preparing',
                attempt_id TEXT,
                saved_count INTEGER DEFAULT 0,
                message_status TEXT NOT NULL DEFAULT 'pending',
                message_attempts INTEGER DEFAULT 0,
                message_error TEXT,
                message_result_json TEXT,
                response_json TEXT,
                payload_json TEXT,
                delivery_status TEXT NOT NULL DEFAULT 'pending',
                notification_status TEXT NOT NULL DEFAULT 'pending',
                delivery_error TEXT,
                delivery_claimed_at TEXT,
                error TEXT,
                started_at TEXT,
                local_saved_at TEXT,
                committed_at TEXT,
                delivery_started_at TEXT,
                completed_at TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_edit_session_finalizations_status
            ON edit_session_finalizations(status, updated_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_edit_session_finalizations_message
            ON edit_session_finalizations(message_status, updated_at)
        """)

        columns = {
            str(row["name"])
            for row in conn.execute(
                "PRAGMA table_info(edit_session_finalizations)"
            ).fetchall()
        }
        for column_name, declaration in FINALIZATION_EXTRA_COLUMNS.items():
            if column_name in columns:
                continue
            conn.execute(
                "ALTER TABLE edit_session_finalizations "
                f"ADD COLUMN {column_name} {declaration}"
            )

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_edit_session_finalizations_delivery
            ON edit_session_finalizations(delivery_status, updated_at)
        """)

        # Rows created before hotfix 8.15.1 have no durable payload for
        # background delivery.  Mark them as historical instead of treating
        # them as newly queued work; otherwise startup could resend old chat
        # messages and tasks.
        conn.execute("""
            UPDATE edit_session_finalizations
            SET delivery_status = 'legacy_completed',
                notification_status = CASE
                    WHEN COALESCE(notification_status, '') IN ('', 'pending')
                    THEN 'legacy'
                    ELSE notification_status
                END,
                updated_at = COALESCE(NULLIF(updated_at, ''), ?)
            WHERE COALESCE(payload_json, '') = ''
              AND status = 'completed'
              AND delivery_status IN ('pending', 'queued')
        """, (utc_now_iso(),))
        conn.commit()
    finally:
        conn.close()


def _canonical_session_payload(payload: dict) -> dict:
    raw_sessions = payload.get("sessions") or []
    sessions = []
    for raw_session in raw_sessions:
        if not isinstance(raw_session, dict):
            continue
        sessions.append({
            "dialogId": normalize_dialog_id(
                raw_session.get("dialogId")
                or payload.get("dialogId")
            ),
            "checklistKey": normalize_checklist_key(
                raw_session.get("checklistKey")
            ),
            "data": raw_session.get("data") or {},
            "changes": raw_session.get("changes") or [],
        })

    sessions.sort(key=lambda item: (
        item.get("dialogId") or "",
        item.get("checklistKey") or "",
    ))

    editor = payload.get("editor") or {}
    return {
        "sessionId": clean_cell_value(payload.get("sessionId")),
        "dialogId": normalize_dialog_id(payload.get("dialogId")),
        "editor": {
            "id": clean_cell_value(
                editor.get("id")
                or editor.get("userId")
                or payload.get("userId")
            ),
            "name": clean_cell_value(
                editor.get("name")
                or editor.get("userName")
            ),
        },
        "sessions": sessions,
    }


def build_finalization_payload_hash(payload: dict) -> str:
    canonical = _canonical_session_payload(payload)
    return hashlib.sha256(
        _json_dumps(canonical).encode("utf-8")
    ).hexdigest()


def _get_finalization_row(session_id: str) -> dict | None:
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT *
            FROM edit_session_finalizations
            WHERE session_id = ?
        """, (session_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _cached_response(row: dict) -> dict:
    response = _json_loads(
        row.get("response_json") or "",
        {},
    )
    if response:
        response["replayed"] = True
        return response

    return {
        "ok": True,
        "committed": row.get("status") in {"committed", "completed"},
        "replayed": True,
        "sessionId": row.get("session_id") or "",
        "savedCount": int(row.get("saved_count") or 0),
        "messageOk": row.get("message_status") in {"sent", "skipped"},
        "messageSkipped": row.get("message_status") == "skipped",
        "messageStatus": row.get("message_status") or "pending",
        "messageError": row.get("message_error") or "",
    }


def _reserve_finalization(
    *,
    session_id: str,
    dialog_id: str,
    user_id: str,
    payload_hash: str,
    payload: dict,
) -> tuple[str, dict | None]:
    ensure_session_finalization_schema()
    attempt_id = uuid.uuid4().hex
    now = utc_now_iso()
    conn = get_conn()

    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("""
            SELECT *
            FROM edit_session_finalizations
            WHERE session_id = ?
        """, (session_id,)).fetchone()

        if row:
            record = dict(row)
            status = clean_cell_value(record.get("status"))

            # Once the local durable commit has reached a terminal state, any
            # repeated close signal is an idempotent replay. This includes a
            # compact Bitrix host-cross payload arriving after the full Save
            # payload; payload-shape differences must not produce a false 409.
            if status == "completed":
                conn.commit()
                return "cached", record

            if status == "failed" and record.get("committed_at"):
                conn.commit()
                return "cached", record

            existing_hash = clean_cell_value(record.get("payload_hash"))
            if existing_hash != payload_hash:
                raise SessionFinalizationPayloadConflictError(
                    "finalization payload differs from the already reserved payload"
                )

            if status in FINALIZATION_ACTIVE_STATUSES:
                conn.commit()
                raise SessionFinalizationInProgressError(
                    "edit session finalization is already in progress"
                )

            conn.execute("""
                UPDATE edit_session_finalizations
                SET status = 'preparing',
                    attempt_id = ?,
                    payload_json = ?,
                    delivery_status = 'pending',
                    notification_status = 'pending',
                    delivery_error = '',
                    delivery_claimed_at = '',
                    error = '',
                    started_at = ?,
                    updated_at = ?
                WHERE session_id = ?
            """, (
                attempt_id,
                _json_dumps(payload),
                now,
                now,
                session_id,
            ))
        else:
            conn.execute("""
                INSERT INTO edit_session_finalizations(
                    session_id,
                    dialog_id,
                    user_id,
                    payload_hash,
                    status,
                    attempt_id,
                    saved_count,
                    message_status,
                    message_attempts,
                    message_error,
                    message_result_json,
                    response_json,
                    payload_json,
                    delivery_status,
                    notification_status,
                    delivery_error,
                    delivery_claimed_at,
                    error,
                    started_at,
                    local_saved_at,
                    committed_at,
                    delivery_started_at,
                    completed_at,
                    created_at,
                    updated_at
                )
                VALUES (
                    ?, ?, ?, ?,
                    'preparing', ?, 0,
                    'pending', 0, '', '', '',
                    ?, 'pending', 'pending', '', '', '',
                    ?, '', '', '', '', ?, ?
                )
            """, (
                session_id,
                dialog_id,
                user_id,
                payload_hash,
                attempt_id,
                _json_dumps(payload),
                now,
                now,
                now,
            ))

        conn.commit()
        return attempt_id, None
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _update_finalization(
    session_id: str,
    *,
    status: str | None = None,
    saved_count: int | None = None,
    message_status: str | None = None,
    message_attempts_delta: int = 0,
    message_error: str | None = None,
    message_result: dict | None = None,
    response: dict | None = None,
    delivery_status: str | None = None,
    notification_status: str | None = None,
    delivery_error: str | None = None,
    delivery_claimed_at: str | None = None,
    error: str | None = None,
    timestamp_column: str | None = None,
) -> None:
    allowed_timestamp_columns = {
        "local_saved_at",
        "committed_at",
        "delivery_started_at",
        "completed_at",
    }
    if timestamp_column and timestamp_column not in allowed_timestamp_columns:
        raise ValueError("invalid finalization timestamp column")

    assignments = ["updated_at = ?"]
    values: list[Any] = [utc_now_iso()]

    if status is not None:
        assignments.append("status = ?")
        values.append(status)
    if saved_count is not None:
        assignments.append("saved_count = ?")
        values.append(int(saved_count))
    if message_status is not None:
        assignments.append("message_status = ?")
        values.append(message_status)
    if message_attempts_delta:
        assignments.append(
            "message_attempts = COALESCE(message_attempts, 0) + ?"
        )
        values.append(int(message_attempts_delta))
    if message_error is not None:
        assignments.append("message_error = ?")
        values.append(message_error)
    if message_result is not None:
        assignments.append("message_result_json = ?")
        values.append(_json_dumps(message_result))
    if response is not None:
        assignments.append("response_json = ?")
        values.append(_json_dumps(response))
    if delivery_status is not None:
        assignments.append("delivery_status = ?")
        values.append(delivery_status)
    if notification_status is not None:
        assignments.append("notification_status = ?")
        values.append(notification_status)
    if delivery_error is not None:
        assignments.append("delivery_error = ?")
        values.append(delivery_error)
    if delivery_claimed_at is not None:
        assignments.append("delivery_claimed_at = ?")
        values.append(delivery_claimed_at)
    if error is not None:
        assignments.append("error = ?")
        values.append(error)
    if timestamp_column:
        assignments.append(f"{timestamp_column} = ?")
        values.append(utc_now_iso())

    values.append(session_id)
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE edit_session_finalizations SET "
            + ", ".join(assignments)
            + " WHERE session_id = ?",
            values,
        )
        conn.commit()
    finally:
        conn.close()


def _persist_finalization_sessions(
    payload: dict,
    *,
    persist_data: bool = True,
) -> list[dict]:
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    raw_sessions = payload.get("sessions") or []
    sessions: list[dict] = []

    for raw_session in raw_sessions:
        if not isinstance(raw_session, dict):
            continue

        checklist_key = normalize_checklist_key(
            raw_session.get("checklistKey")
        )
        session_dialog_id = normalize_dialog_id(
            raw_session.get("dialogId") or dialog_id
        )
        changes = raw_session.get("changes") or []
        session_data = raw_session.get("data") or {}

        if not session_dialog_id:
            continue

        if persist_data and changes and session_data:
            session_data = dict(session_data)
            session_data["checklistKey"] = checklist_key
            session_data["resolvedDialogId"] = session_dialog_id
            data = normalize_checklist_data(
                session_data,
                checklist_key,
            )
            data["resolvedDialogId"] = session_dialog_id
            save_checklist(
                session_dialog_id,
                data,
                checklist_key,
            )
        else:
            data = get_checklist(
                session_dialog_id,
                checklist_key,
            )

        sessions.append({
            "dialogId": session_dialog_id,
            "checklistKey": checklist_key,
            "changes": changes,
            "data": data,
        })

    return sessions


def _deliver_summary_once(
    *,
    session_id: str,
    dialog_id: str,
    sessions: list[dict],
    editor: dict,
) -> dict:
    visible_sessions = [
        session
        for session in sessions
        if build_recent_changes_sections(
            session.get("changes") or [],
            session.get("checklistKey") or "id",
        )
    ]

    if not visible_sessions:
        _update_finalization(
            session_id,
            message_status="skipped",
            message_error="",
        )
        return {
            "messageOk": True,
            "messageSkipped": True,
            "messageStatus": "skipped",
            "messageError": "",
            "messageResult": {},
        }

    target_dialog_id = (
        dialog_id
        or visible_sessions[0].get("dialogId")
        or ""
    )

    _update_finalization(
        session_id,
        message_status="sending",
        message_attempts_delta=1,
        message_error="",
        timestamp_column="delivery_started_at",
    )

    try:
        message = build_multi_checklist_chat_message(
            visible_sessions,
            editor,
        )
        result = bitrix_webhook_call("im.message.add", {
            "DIALOG_ID": target_dialog_id,
            "MESSAGE": message,
        })
    except Exception as exc:
        error_text = str(exc)
        _update_finalization(
            session_id,
            message_status="failed",
            message_error=error_text,
            message_result={"exception": error_text},
        )
        return {
            "messageOk": False,
            "messageSkipped": False,
            "messageStatus": "failed",
            "messageError": error_text,
            "messageResult": {"exception": error_text},
        }

    if isinstance(result, dict) and "error" in result:
        error_text = clean_cell_value(
            result.get("error_description")
            or result.get("error")
            or "message send failed"
        )
        _update_finalization(
            session_id,
            message_status="failed",
            message_error=error_text,
            message_result=result,
        )
        return {
            "messageOk": False,
            "messageSkipped": False,
            "messageStatus": "failed",
            "messageError": error_text,
            "messageResult": result,
        }

    _update_finalization(
        session_id,
        message_status="sent",
        message_error="",
        message_result=result if isinstance(result, dict) else {"result": result},
    )
    return {
        "messageOk": True,
        "messageSkipped": False,
        "messageStatus": "sent",
        "messageError": "",
        "messageResult": result,
    }


def _claim_finalization_delivery(session_id: str) -> dict | None:
    ensure_session_finalization_schema()
    normalized_session_id = clean_cell_value(session_id)
    if not normalized_session_id:
        return None

    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM edit_session_finalizations WHERE session_id = ?",
            (normalized_session_id,),
        ).fetchone()
        if not row:
            conn.commit()
            return None

        record = dict(row)
        if clean_cell_value(record.get("delivery_status")) not in {
            "pending",
            "queued",
        }:
            conn.commit()
            return None

        cur = conn.execute(
            """
            UPDATE edit_session_finalizations
            SET delivery_status = 'running',
                delivery_claimed_at = ?,
                delivery_started_at = ?,
                updated_at = ?
            WHERE session_id = ?
              AND delivery_status IN ('pending', 'queued')
            """,
            (now, now, now, normalized_session_id),
        )
        if int(cur.rowcount or 0) != 1:
            conn.commit()
            return None

        conn.commit()
        record["delivery_status"] = "running"
        record["delivery_claimed_at"] = now
        return record
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def deliver_finalized_edit_session(session_id: str) -> dict:
    claimed = _claim_finalization_delivery(session_id)
    if not claimed:
        return {
            "ok": True,
            "claimed": False,
            "sessionId": clean_cell_value(session_id),
        }

    payload = _json_loads(claimed.get("payload_json") or "", {})
    if not isinstance(payload, dict):
        payload = {}

    dialog_id = normalize_dialog_id(
        payload.get("dialogId") or claimed.get("dialog_id")
    )
    editor = payload.get("editor") or {}

    try:
        fallback_sessions = _persist_finalization_sessions(
            payload,
            persist_data=False,
        )
        summary_sessions = build_session_summary_sessions(
            session_id=clean_cell_value(session_id),
            fallback_sessions=fallback_sessions,
        )

        try:
            notification_delivery = (
                deliver_committed_notification_drafts(
                    clean_cell_value(session_id)
                )
            )
        except Exception as notification_exc:
            notification_delivery = {
                "ok": False,
                "status": "failed",
                "sessionId": clean_cell_value(session_id),
                "error": str(notification_exc),
                "taskResults": [],
                "chatResults": [],
                "crmResults": [],
            }

        delivery = _deliver_summary_once(
            session_id=clean_cell_value(session_id),
            dialog_id=dialog_id,
            sessions=summary_sessions,
            editor=editor,
        )

        existing = _get_finalization_row(
            clean_cell_value(session_id)
        ) or {}
        response = _json_loads(
            existing.get("response_json") or "",
            {},
        )
        response.update({
            "notificationDelivery": notification_delivery,
            **delivery,
            "externalDeliveryQueued": False,
            "externalDeliveryCompleted": True,
        })

        notification_status = clean_cell_value(
            notification_delivery.get("status")
        ) or ("completed" if notification_delivery.get("ok") else "failed")
        error_parts = [
            clean_cell_value(notification_delivery.get("error")),
            clean_cell_value(delivery.get("messageError")),
        ]
        delivery_error = "; ".join(
            part for part in error_parts if part
        )

        _update_finalization(
            clean_cell_value(session_id),
            status="completed",
            delivery_status="completed",
            notification_status=notification_status,
            delivery_error=delivery_error,
            response=response,
            error="",
            timestamp_column="completed_at",
        )
        write_debug_log("edit_session_external_delivery_completed", {
            "sessionId": clean_cell_value(session_id),
            "dialogId": dialog_id,
            "messageStatus": delivery.get("messageStatus"),
            "notificationDeliveryStatus": notification_status,
            "error": delivery_error,
        })
        return {
            "ok": True,
            "claimed": True,
            "sessionId": clean_cell_value(session_id),
            "message": delivery,
            "notificationDelivery": notification_delivery,
        }
    except Exception as exc:
        _update_finalization(
            clean_cell_value(session_id),
            status="completed",
            delivery_status="failed",
            delivery_error=str(exc),
            error="",
        )
        write_debug_log("edit_session_external_delivery_failed", {
            "sessionId": clean_cell_value(session_id),
            "dialogId": dialog_id,
            "error": str(exc),
        })
        return {
            "ok": False,
            "claimed": True,
            "sessionId": clean_cell_value(session_id),
            "error": str(exc),
        }


def _delivery_thread_target(session_id: str) -> None:
    try:
        deliver_finalized_edit_session(session_id)
    finally:
        with _FINALIZATION_DELIVERY_GUARD:
            _FINALIZATION_DELIVERY_RUNNING.discard(
                clean_cell_value(session_id)
            )


def enqueue_finalization_delivery(
    session_id: str,
    *,
    source: str = "session_finalize",
) -> dict:
    normalized_session_id = clean_cell_value(session_id)
    if not normalized_session_id:
        return {"ok": False, "queued": False, "error": "sessionId is required"}

    with _FINALIZATION_DELIVERY_GUARD:
        if normalized_session_id in _FINALIZATION_DELIVERY_RUNNING:
            return {
                "ok": True,
                "queued": False,
                "alreadyRunning": True,
                "sessionId": normalized_session_id,
            }
        _FINALIZATION_DELIVERY_RUNNING.add(normalized_session_id)

    # Give the HTTP response and Bitrix popup close flow a short head start.
    # All inputs are already durable in SQLite, so the timer only affects when
    # external network work begins, not whether it can be recovered.
    thread = threading.Timer(
        0.75,
        _delivery_thread_target,
        args=(normalized_session_id,),
    )
    thread.daemon = True
    thread.name = f"session-finalization-{normalized_session_id[:8]}"
    thread.start()
    write_debug_log("edit_session_external_delivery_queued", {
        "sessionId": normalized_session_id,
        "source": source,
    })
    return {
        "ok": True,
        "queued": True,
        "sessionId": normalized_session_id,
    }


def recover_pending_finalization_deliveries(
    source: str = "startup",
    limit: int = 200,
) -> dict:
    ensure_session_finalization_schema()
    conn = get_conn()
    try:
        # A process restart leaves a claimed delivery in `running`.  Do not
        # retry it automatically because an external request may have reached
        # Bitrix before the process stopped.  Mark it failed/uncertain for
        # diagnostics instead of silently creating a duplicate message.
        now = utc_now_iso()
        conn.execute("""
            UPDATE edit_session_finalizations
            SET delivery_status = 'failed',
                delivery_error = CASE
                    WHEN COALESCE(delivery_error, '') = ''
                    THEN 'Приложение было перезапущено во время внешней доставки; результат требует проверки'
                    ELSE delivery_error
                END,
                updated_at = ?
            WHERE status = 'completed'
              AND delivery_status = 'running'
              AND COALESCE(payload_json, '') <> ''
        """, (now,))
        conn.commit()

        rows = conn.execute(
            """
            SELECT session_id
            FROM edit_session_finalizations
            WHERE status = 'completed'
              AND delivery_status IN ('pending', 'queued')
              AND COALESCE(payload_json, '') <> ''
            ORDER BY committed_at ASC, updated_at ASC
            LIMIT ?
            """,
            (max(1, int(limit or 1)),),
        ).fetchall()
    finally:
        conn.close()

    queued = 0
    for row in rows:
        result = enqueue_finalization_delivery(
            row["session_id"],
            source=source,
        )
        if result.get("queued"):
            queued += 1

    return {
        "ok": True,
        "source": source,
        "found": len(rows),
        "queued": queued,
    }


def finalize_edit_session_payload(payload: dict) -> dict:
    session_id = clean_cell_value(payload.get("sessionId"))
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    user_id = clean_cell_value(payload.get("userId"))
    reason = clean_cell_value(
        payload.get("reason")
        or payload.get("closeEvent")
        or "save_and_close"
    )

    if not session_id:
        raise ValueError("sessionId is required")
    if not dialog_id:
        raise ValueError("dialogId is required")

    session = get_edit_session_for_actor(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )
    requested_client_session_id = clean_cell_value(
        payload.get("clientSessionId")
    )
    current_client_session_id = clean_cell_value(
        session.get("clientSessionId")
        or session.get("client_session_id")
    )
    if (
        requested_client_session_id
        and current_client_session_id
        and requested_client_session_id != current_client_session_id
    ):
        raise EditSessionConflictError(
            "edit session ownership moved to another popup window"
        )

    session_status = clean_cell_value(session.get("status"))
    if session_status == "rolled_back":
        raise EditSessionConflictError(
            "rolled back session cannot be finalized"
        )

    payload_hash = build_finalization_payload_hash(payload)
    reservation, cached_row = _reserve_finalization(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
        payload_hash=payload_hash,
        payload=payload,
    )
    if reservation == "cached" and cached_row:
        return _cached_response(cached_row)

    write_debug_log("edit_session_finalization_started", {
        "sessionId": session_id,
        "dialogId": dialog_id,
        "userId": user_id,
        "reason": reason,
        "payloadHash": payload_hash,
    })

    try:
        already_committed = session_status == "committed"
        sessions = _persist_finalization_sessions(
            payload,
            persist_data=not already_committed,
        )
        saved_count = len(sessions)

        if not already_committed:
            _update_finalization(
                session_id,
                status="local_saved",
                saved_count=saved_count,
                error="",
                timestamp_column="local_saved_at",
            )

            _update_finalization(
                session_id,
                status="committing",
            )
            committed_session = commit_edit_session(
                session_id=session_id,
                dialog_id=dialog_id,
                user_id=user_id,
                reason=reason,
            )
            if clean_cell_value(committed_session.get("status")) != "committed":
                raise EditSessionConflictError(
                    "edit session commit did not reach committed status"
                )
        else:
            committed_session = session

        _update_finalization(
            session_id,
            status="committed",
            saved_count=saved_count,
            error="",
            timestamp_column="committed_at",
        )

        # Local persistence, durable external jobs and lock release are already
        # completed by commit_edit_session().  Do not keep the Bitrix popup open
        # while Yandex/Bitrix network calls run.  Their input is stored in SQLite
        # and processed by a background worker after this response is returned.
        response = {
            "ok": True,
            "committed": True,
            "sessionId": session_id,
            "sessionStatus": "committed",
            "savedCount": saved_count,
            "locksReleased": True,
            "externalJobsStarted": True,
            "externalDeliveryQueued": True,
            "externalDeliveryCompleted": False,
            "summaryChecklistCount": 0,
            "summaryChangeCount": 0,
            "notificationDelivery": {
                "ok": True,
                "status": "queued",
                "sessionId": session_id,
            },
            "messageOk": True,
            "messageSkipped": False,
            "messageStatus": "queued",
            "messageError": "",
        }
        _update_finalization(
            session_id,
            status="completed",
            saved_count=saved_count,
            message_status="queued",
            notification_status="queued",
            delivery_status="queued",
            delivery_error="",
            response=response,
            error="",
            timestamp_column="completed_at",
        )

        enqueue_result = enqueue_finalization_delivery(
            session_id,
            source="save_and_close",
        )
        response["externalDeliveryWorkerStarted"] = bool(
            enqueue_result.get("queued")
            or enqueue_result.get("alreadyRunning")
        )
        _update_finalization(
            session_id,
            response=response,
        )

        write_debug_log("edit_session_finalization_completed", {
            "sessionId": session_id,
            "dialogId": dialog_id,
            "savedCount": saved_count,
            "locksReleased": True,
            "messageStatus": "queued",
            "notificationDeliveryStatus": "queued",
            "externalDeliveryWorkerStarted": response[
                "externalDeliveryWorkerStarted"
            ],
        })
        return response

    except Exception as exc:
        current = _get_finalization_row(session_id) or {}
        committed_at = clean_cell_value(current.get("committed_at"))
        _update_finalization(
            session_id,
            status="failed",
            error=str(exc),
            response={
                "ok": False,
                "committed": bool(committed_at),
                "sessionId": session_id,
                "error": str(exc),
            },
        )
        write_debug_log("edit_session_finalization_failed", {
            "sessionId": session_id,
            "dialogId": dialog_id,
            "committed": bool(committed_at),
            "error": str(exc),
        })
        raise
