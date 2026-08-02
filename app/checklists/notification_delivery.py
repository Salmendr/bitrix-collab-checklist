from __future__ import annotations

import hashlib
import json
from datetime import datetime, time, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

from app.bitrix.client import bitrix_webhook_call
from app.checklists.bitrix_companies import ensure_supplier_company_synced
from app.checklists.document_version_links import build_configured_version_file_url
from app.checklists.edit_sessions import utc_now_iso
from app.checklists.notification_drafts import public_notification_draft
from app.checklists.registry import get_checklist_title
from app.checklists.storage import get_project_storage_context
from app.checklists.utils import clean_cell_value
from app.db import get_conn
from app.settings import BITRIX_TECH_WEBHOOK_URL


DELIVERY_SCHEMA_VERSION = 1
DELIVERY_SUCCESS_STATUSES = frozenset({"sent", "succeeded", "synced"})
DELIVERY_TERMINAL_STATUSES = frozenset({
    "sent",
    "succeeded",
    "synced",
    "failed",
    "blocked",
    "disabled",
})
DELIVERY_RETRYABLE_STATUSES = frozenset({"failed", "blocked", "disabled"})
DELIVERY_IN_PROGRESS_STATUSES = frozenset({"pending", "sending", "retrying"})
DELIVERY_TYPES = frozenset({"crm", "task", "chat"})
PROJECT_TIMEZONE = timezone(timedelta(hours=10))


class NotificationDeliveryError(RuntimeError):
    pass


def _json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _json_loads(value: Any, default: Any) -> Any:
    raw = clean_cell_value(value)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _hash_key(*parts: Any) -> str:
    raw = "\x1f".join(clean_cell_value(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _escape_bbcode(value: Any) -> str:
    text = clean_cell_value(value)
    return text.replace("[", "&#91;").replace("]", "&#93;")


def _portal_base_url() -> str:
    raw = clean_cell_value(BITRIX_TECH_WEBHOOK_URL)
    if not raw:
        return ""
    parsed = urlsplit(raw)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}"


def _task_url(task_id: str, responsible_id: str) -> str:
    base = _portal_base_url()
    if not base or not clean_cell_value(task_id):
        return ""
    user_id = clean_cell_value(responsible_id) or "0"
    return (
        f"{base}/company/personal/user/{user_id}/tasks/task/view/"
        f"{clean_cell_value(task_id)}/"
    )


def _deadline_iso(deadline_date: str) -> str:
    raw = clean_cell_value(deadline_date)
    if not raw:
        return ""
    parsed = datetime.strptime(raw, "%Y-%m-%d").date()
    deadline = datetime.combine(
        parsed,
        time(hour=23, minute=59, second=0),
        tzinfo=PROJECT_TIMEZONE,
    )
    return deadline.isoformat()


def _parse_task_id(response: Any) -> str:
    if not isinstance(response, dict):
        return ""
    result = response.get("result")
    if isinstance(result, dict):
        task = result.get("task")
        if isinstance(task, dict):
            return clean_cell_value(task.get("id") or task.get("ID"))
        return clean_cell_value(result.get("id") or result.get("ID"))
    return clean_cell_value(result)


def _parse_message_id(response: Any) -> str:
    if not isinstance(response, dict):
        return ""
    result = response.get("result")
    if isinstance(result, dict):
        return clean_cell_value(
            result.get("messageId")
            or result.get("id")
            or result.get("ID")
        )
    return clean_cell_value(result)


def ensure_notification_delivery_schema() -> None:
    conn = get_conn()
    try:
        attempt_columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(notification_delivery_attempts)"
            ).fetchall()
        }
        for name, definition in {
            "session_id": "TEXT",
            "recipient_key": "TEXT",
            "draft_ids_json": "TEXT",
            "delivery_group_key": "TEXT",
        }.items():
            if name not in attempt_columns:
                conn.execute(
                    f"ALTER TABLE notification_delivery_attempts "
                    f"ADD COLUMN {name} {definition}"
                )

        history_columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(document_assignment_history)"
            ).fetchall()
        }
        for name, definition in {
            "chat_message_id": "TEXT",
            "delivery_attempt_id": "TEXT",
        }.items():
            if name not in history_columns:
                conn.execute(
                    f"ALTER TABLE document_assignment_history "
                    f"ADD COLUMN {name} {definition}"
                )

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_notification_delivery_session
            ON notification_delivery_attempts(
                session_id, status, delivery_type, created_at
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_notification_delivery_recipient
            ON notification_delivery_attempts(
                session_id, recipient_key, delivery_type, created_at
            )
        """)
        conn.execute("""
            DELETE FROM document_assignment_history
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM document_assignment_history
                WHERE COALESCE(draft_id, '') <> ''
                  AND COALESCE(document_id, '') <> ''
                GROUP BY draft_id, document_id
            )
              AND COALESCE(draft_id, '') <> ''
              AND COALESCE(document_id, '') <> ''
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_assignment_history_version
            ON document_assignment_history(draft_id, document_id)
        """)
        conn.commit()
    finally:
        conn.close()


def _load_committed_drafts(session_id: str) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT *
            FROM notification_drafts
            WHERE session_id = ?
              AND status = 'committed'
            ORDER BY created_at ASC, draft_id ASC
        """, (clean_cell_value(session_id),)).fetchall()
    finally:
        conn.close()

    return [public_notification_draft(dict(row)) for row in rows]


def _recipient_key(draft: dict) -> str:
    recipient = draft.get("recipient") or {}
    recipient_type = clean_cell_value(recipient.get("type")) or "internal"
    if recipient_type == "internal":
        identity = clean_cell_value(recipient.get("bitrixUserId"))
    else:
        identity = (
            clean_cell_value(recipient.get("companyId"))
            or clean_cell_value(recipient.get("email")).casefold()
            or "".join(
                ch for ch in clean_cell_value(recipient.get("phone"))
                if ch.isdigit()
            )
            or clean_cell_value(recipient.get("name")).casefold()
        )
    return f"{recipient_type}:{identity}"


def _recipient_label(draft: dict) -> str:
    recipient = draft.get("recipient") or {}
    recipient_type = clean_cell_value(recipient.get("type"))
    name = _escape_bbcode(recipient.get("name") or "Исполнитель")
    if recipient_type == "internal":
        user_id = clean_cell_value(recipient.get("bitrixUserId"))
        if user_id:
            return f"[user={user_id}]{name}[/user]"
    return name


def _sender_label(draft: dict) -> str:
    sender = draft.get("sender") or {}
    name = _escape_bbcode(sender.get("name") or "Постановщик")
    user_id = clean_cell_value(sender.get("userId"))
    if user_id:
        return f"[user={user_id}]{name}[/user]"
    return name


def _file_link(draft: dict, file_record: dict) -> str:
    file_name = _escape_bbcode(file_record.get("fileName") or "Документ")
    url = build_configured_version_file_url(
        draft.get("dialogId") or "",
        draft.get("checklistKey") or "id",
        draft.get("itemId") or "",
        file_record.get("documentId") or "",
    )
    if not clean_cell_value(url):
        return file_name
    return f"[url={url}]{file_name}[/url]"


def _task_responsible_id(draft: dict) -> str:
    recipient = draft.get("recipient") or {}
    if clean_cell_value(recipient.get("type")) == "external":
        return clean_cell_value(recipient.get("curatorUserId"))
    return clean_cell_value(recipient.get("bitrixUserId"))


def _build_task_payload(draft: dict, project: dict) -> dict:
    assignment_part = clean_cell_value(
        (draft.get("assignmentPart") or {}).get("text")
    )
    item_name = clean_cell_value(draft.get("itemName")) or "Пункт чек-листа"
    project_name = clean_cell_value(project.get("projectName")) or "Проект"
    checklist_title = get_checklist_title(
        clean_cell_value(draft.get("checklistKey")) or "id"
    )
    responsible_id = _task_responsible_id(draft)
    sender_id = clean_cell_value((draft.get("sender") or {}).get("userId"))
    group_id = clean_cell_value(project.get("projectId"))

    if not responsible_id:
        raise NotificationDeliveryError("task responsible userId is missing")
    if not sender_id:
        raise NotificationDeliveryError("task creator userId is missing")
    if not group_id:
        raise NotificationDeliveryError("projectId is missing in project context")

    title = f"Задание в части {assignment_part} — {item_name} — {project_name}"
    recipient = draft.get("recipient") or {}
    lines = [
        f"[B]Постановщик:[/B] {_sender_label(draft)}",
        f"[B]Исполнитель:[/B] {_recipient_label(draft)}",
        f"[B]Стадия:[/B] {_escape_bbcode(checklist_title)}",
        f"[B]Пункт:[/B] {_escape_bbcode(item_name)}",
        f"[B]В какой части:[/B] {_escape_bbcode(assignment_part)}",
        f"[B]Крайний срок:[/B] {_escape_bbcode(draft.get('deadlineDate'))}",
    ]
    if clean_cell_value(recipient.get("type")) == "external":
        contacts = " · ".join(filter(None, [
            clean_cell_value(recipient.get("phone")),
            clean_cell_value(recipient.get("email")),
            clean_cell_value(recipient.get("contactDetails")),
        ]))
        lines.extend([
            f"[B]Внешний исполнитель:[/B] {_escape_bbcode(recipient.get('name'))}",
            f"[B]Контакты:[/B] {_escape_bbcode(contacts)}",
            (
                f"[B]Внутренний куратор:[/B] "
                f"[user={responsible_id}]"
                f"{_escape_bbcode(recipient.get('curatorName') or responsible_id)}"
                f"[/user]"
            ),
        ])
    description = clean_cell_value(draft.get("description"))
    if description:
        lines.extend(["", "[B]Описание:[/B]", _escape_bbcode(description)])
    lines.extend(["", "[B]Приложенные документы:[/B]"])
    for file_record in draft.get("files") or []:
        lines.append(f"• {_file_link(draft, file_record)}")

    return {
        "fields": {
            "TITLE": title,
            "DESCRIPTION": "[BR]".join(lines),
            "DESCRIPTION_IN_BBCODE": "Y",
            "CREATED_BY": int(sender_id),
            "RESPONSIBLE_ID": int(responsible_id),
            "GROUP_ID": int(group_id),
            "DEADLINE": _deadline_iso(draft.get("deadlineDate") or ""),
        }
    }


def _build_group_message(
    drafts: list[dict],
    task_results: dict[str, dict],
    project: dict,
) -> str:
    if not drafts:
        return ""
    recipient_label = _recipient_label(drafts[0])
    project_name = _escape_bbcode(project.get("projectName") or "Проект")
    lines = [
        f"[B]Задания для {recipient_label}[/B]",
        f"Проект: {project_name}",
        "",
    ]
    for draft in drafts:
        result = task_results.get(draft.get("draftId") or "") or {}
        assignment_part = _escape_bbcode(
            (draft.get("assignmentPart") or {}).get("text")
        )
        item_name = _escape_bbcode(draft.get("itemName"))
        checklist_title = _escape_bbcode(
            get_checklist_title(draft.get("checklistKey") or "id")
        )
        lines.extend([
            f"[B]{checklist_title} / {item_name}[/B]",
            f"От: {_sender_label(draft)}",
            f"В части: {assignment_part}",
            f"Срок: {_escape_bbcode(draft.get('deadlineDate'))}",
        ])
        if result.get("status") == "sent" and result.get("externalUrl"):
            lines.append(
                f"Задача: [url={result['externalUrl']}]"
                f"№{_escape_bbcode(result.get('externalId'))}[/url]"
            )
        elif result.get("status") == "sent":
            lines.append(
                f"Задача создана: №{_escape_bbcode(result.get('externalId'))}"
            )
        else:
            lines.append(
                "Задача: не создана автоматически — "
                + _escape_bbcode(result.get("error") or result.get("status"))
            )
        lines.append("Документы:")
        for file_record in draft.get("files") or []:
            lines.append(f"• {_file_link(draft, file_record)}")
        description = clean_cell_value(draft.get("description"))
        if description:
            lines.append("Описание: " + _escape_bbcode(description))
        lines.append("")
    return "[BR]".join(lines).removesuffix("[BR]")


def _attempt_row(idempotency_key: str) -> dict | None:
    ensure_notification_delivery_schema()
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT * FROM notification_delivery_attempts
            WHERE idempotency_key = ?
        """, (clean_cell_value(idempotency_key),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _reserve_attempt(
    *,
    draft_id: str,
    session_id: str,
    delivery_type: str,
    idempotency_key: str,
    recipient_key: str = "",
    draft_ids: list[str] | None = None,
    group_key: str = "",
    request_payload: dict | None = None,
) -> tuple[dict, bool]:
    ensure_notification_delivery_schema()
    now = utc_now_iso()
    attempt_id = _hash_key(idempotency_key, "attempt")[:32]
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("""
            SELECT * FROM notification_delivery_attempts
            WHERE idempotency_key = ?
        """, (idempotency_key,)).fetchone()
        if row:
            conn.commit()
            return dict(row), False
        conn.execute("""
            INSERT INTO notification_delivery_attempts(
                attempt_id,
                draft_id,
                delivery_type,
                idempotency_key,
                status,
                attempts,
                external_id,
                external_url,
                request_json,
                response_json,
                error,
                created_at,
                updated_at,
                started_at,
                finished_at,
                session_id,
                recipient_key,
                draft_ids_json,
                delivery_group_key
            ) VALUES (?, ?, ?, ?, 'pending', 0, '', '', ?, '', '', ?, ?, '', '', ?, ?, ?, ?)
        """, (
            attempt_id,
            draft_id,
            delivery_type,
            idempotency_key,
            _json_dumps(request_payload or {}),
            now,
            now,
            session_id,
            recipient_key,
            _json_dumps(draft_ids or ([draft_id] if draft_id else [])),
            group_key,
        ))
        conn.commit()
        return _attempt_row(idempotency_key) or {}, True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _update_attempt(
    idempotency_key: str,
    *,
    status: str,
    external_id: str = "",
    external_url: str = "",
    response_payload: Any = None,
    error: str = "",
    increment_attempts: bool = False,
    started: bool = False,
    finished: bool = False,
) -> dict:
    assignments = ["status = ?", "updated_at = ?"]
    values: list[Any] = [status, utc_now_iso()]
    if increment_attempts:
        assignments.append("attempts = COALESCE(attempts, 0) + 1")
    if external_id != "":
        assignments.append("external_id = ?")
        values.append(external_id)
    if external_url != "":
        assignments.append("external_url = ?")
        values.append(external_url)
    if response_payload is not None:
        assignments.append("response_json = ?")
        values.append(_json_dumps(response_payload))
    assignments.append("error = ?")
    values.append(clean_cell_value(error))
    if started:
        assignments.append("started_at = ?")
        values.append(utc_now_iso())
    if finished:
        assignments.append("finished_at = ?")
        values.append(utc_now_iso())
    values.append(idempotency_key)
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE notification_delivery_attempts SET "
            + ", ".join(assignments)
            + " WHERE idempotency_key = ?",
            values,
        )
        conn.commit()
    finally:
        conn.close()
    return _attempt_row(idempotency_key) or {}


def _claim_retry_attempt(idempotency_key: str) -> tuple[dict, bool]:
    """Atomically claim one failed/blocked delivery for a manual retry."""
    ensure_notification_delivery_schema()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM notification_delivery_attempts "
            "WHERE idempotency_key = ?",
            (clean_cell_value(idempotency_key),),
        ).fetchone()
        if not row:
            conn.commit()
            return {}, False
        current = dict(row)
        status = clean_cell_value(current.get("status")).lower()
        if status not in DELIVERY_RETRYABLE_STATUSES:
            conn.commit()
            return current, False
        now = utc_now_iso()
        cursor = conn.execute(
            "UPDATE notification_delivery_attempts SET "
            "status = 'retrying', "
            "attempts = COALESCE(attempts, 0) + 1, "
            "updated_at = ?, started_at = ?, finished_at = '', error = '' "
            "WHERE idempotency_key = ? AND status = ?",
            (now, now, clean_cell_value(idempotency_key), status),
        )
        claimed = int(cursor.rowcount or 0) == 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _attempt_row(idempotency_key) or current, claimed


def _attempt_has_uncertain_result(row: dict | None) -> bool:
    if not row:
        return False
    response = _json_loads(row.get("response_json"), {})
    if not isinstance(response, dict):
        return False
    if clean_cell_value(response.get("exception")):
        return True
    nested = response.get("result")
    return bool(
        isinstance(nested, dict)
        and clean_cell_value(nested.get("retryRisk")) == "unknown_external_result"
    )


def _public_attempt(row: dict | None) -> dict:
    row = row or {}
    status = clean_cell_value(row.get("status")).lower()
    uncertain = _attempt_has_uncertain_result(row)
    return {
        "attemptId": row.get("attempt_id") or "",
        "draftId": row.get("draft_id") or "",
        "sessionId": row.get("session_id") or "",
        "deliveryType": row.get("delivery_type") or "",
        "idempotencyKey": row.get("idempotency_key") or "",
        "status": row.get("status") or "",
        "attempts": int(row.get("attempts") or 0),
        "retryable": status in DELIVERY_RETRYABLE_STATUSES,
        "uncertainExternalResult": uncertain,
        "retryWarning": (
            "Bitrix мог выполнить операцию, но ответ не дошёл. "
            "Повтор может создать дубль."
            if uncertain else ""
        ),
        "externalId": row.get("external_id") or "",
        "externalUrl": row.get("external_url") or "",
        "error": row.get("error") or "",
        "recipientKey": row.get("recipient_key") or "",
        "draftIds": _json_loads(row.get("draft_ids_json"), []),
        "groupKey": row.get("delivery_group_key") or "",
        "request": _json_loads(row.get("request_json"), {}),
        "response": _json_loads(row.get("response_json"), {}),
        "createdAt": row.get("created_at") or "",
        "updatedAt": row.get("updated_at") or "",
        "startedAt": row.get("started_at") or "",
        "finishedAt": row.get("finished_at") or "",
    }


def _execute_bitrix_attempt(
    *,
    draft_id: str,
    session_id: str,
    delivery_type: str,
    idempotency_key: str,
    method: str,
    payload: dict,
    recipient_key: str = "",
    draft_ids: list[str] | None = None,
    group_key: str = "",
    id_parser=None,
    url_builder=None,
    allow_retry: bool = False,
) -> dict:
    row, created = _reserve_attempt(
        draft_id=draft_id,
        session_id=session_id,
        delivery_type=delivery_type,
        idempotency_key=idempotency_key,
        recipient_key=recipient_key,
        draft_ids=draft_ids,
        group_key=group_key,
        request_payload={"method": method, "payload": payload},
    )
    status = clean_cell_value(row.get("status")).lower()

    if created:
        _update_attempt(
            idempotency_key,
            status="sending",
            increment_attempts=True,
            started=True,
            error="",
        )
    elif allow_retry and status in DELIVERY_RETRYABLE_STATUSES:
        row, claimed = _claim_retry_attempt(idempotency_key)
        if not claimed:
            return _public_attempt(row)
    else:
        return _public_attempt(row)

    if not clean_cell_value(BITRIX_TECH_WEBHOOK_URL):
        row = _update_attempt(
            idempotency_key,
            status="blocked",
            error="BITRIX_TECH_WEBHOOK_URL is empty",
            finished=True,
        )
        return _public_attempt(row)

    try:
        response = bitrix_webhook_call(method, payload)
    except Exception as exc:
        row = _update_attempt(
            idempotency_key,
            status="failed",
            response_payload={
                "exception": str(exc),
                "retryRisk": "unknown_external_result",
            },
            error=str(exc),
            finished=True,
        )
        return _public_attempt(row)

    if not isinstance(response, dict) or response.get("error"):
        error = clean_cell_value(
            (response or {}).get("error_description")
            if isinstance(response, dict)
            else ""
        ) or clean_cell_value(
            (response or {}).get("error")
            if isinstance(response, dict)
            else ""
        ) or "Bitrix delivery failed"
        row = _update_attempt(
            idempotency_key,
            status="failed",
            response_payload=response,
            error=error,
            finished=True,
        )
        return _public_attempt(row)

    external_id = clean_cell_value(id_parser(response) if id_parser else "")
    if not external_id:
        row = _update_attempt(
            idempotency_key,
            status="failed",
            response_payload=response,
            error="Bitrix returned empty external ID",
            finished=True,
        )
        return _public_attempt(row)

    external_url = clean_cell_value(
        url_builder(external_id) if url_builder else ""
    )
    row = _update_attempt(
        idempotency_key,
        status="sent",
        external_id=external_id,
        external_url=external_url,
        response_payload=response,
        error="",
        finished=True,
    )
    return _public_attempt(row)


def _ensure_history_rows(draft: dict) -> None:
    now = utc_now_iso()
    recipient = draft.get("recipient") or {}
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        for file_record in draft.get("files") or []:
            history_id = _hash_key(
                draft.get("draftId"),
                file_record.get("documentId"),
                "assignment-history",
            )[:32]
            conn.execute("""
                INSERT OR IGNORE INTO document_assignment_history(
                    history_id,
                    draft_id,
                    recipient_id,
                    session_id,
                    dialog_id,
                    checklist_key,
                    item_id,
                    series_id,
                    document_id,
                    version_name,
                    sender_user_id,
                    sender_name,
                    recipient_type,
                    recipient_user_id,
                    recipient_name,
                    assignment_part_id,
                    assignment_part_text,
                    deadline_date,
                    bitrix_task_id,
                    bitrix_task_url,
                    chat_status,
                    task_status,
                    crm_sync_status,
                    created_at,
                    updated_at,
                    chat_message_id,
                    delivery_attempt_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', 'pending', 'pending', 'pending', ?, ?, '', '')
            """, (
                history_id,
                draft.get("draftId") or "",
                recipient.get("recipientId") or "",
                draft.get("sessionId") or "",
                draft.get("dialogId") or "",
                draft.get("checklistKey") or "",
                draft.get("itemId") or "",
                file_record.get("seriesId") or "",
                file_record.get("documentId") or "",
                file_record.get("fileName") or "",
                (draft.get("sender") or {}).get("userId") or "",
                (draft.get("sender") or {}).get("name") or "",
                recipient.get("type") or "",
                (
                    recipient.get("bitrixUserId")
                    if recipient.get("type") == "internal"
                    else recipient.get("curatorUserId")
                ) or "",
                recipient.get("name") or "",
                (draft.get("assignmentPart") or {}).get("id") or "",
                (draft.get("assignmentPart") or {}).get("text") or "",
                draft.get("deadlineDate") or "",
                now,
                now,
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _update_history_for_draft(
    draft_id: str,
    *,
    task_result: dict | None = None,
    crm_result: dict | None = None,
    chat_result: dict | None = None,
) -> None:
    assignments = ["updated_at = ?"]
    values: list[Any] = [utc_now_iso()]
    if task_result is not None:
        assignments.extend([
            "task_status = ?",
            "bitrix_task_id = ?",
            "bitrix_task_url = ?",
            "delivery_attempt_id = ?",
        ])
        values.extend([
            task_result.get("status") or "failed",
            task_result.get("externalId") or "",
            task_result.get("externalUrl") or "",
            task_result.get("attemptId") or "",
        ])
    if crm_result is not None:
        assignments.append("crm_sync_status = ?")
        values.append(crm_result.get("status") or "failed")
    if chat_result is not None:
        assignments.extend([
            "chat_status = ?",
            "chat_message_id = ?",
        ])
        values.extend([
            chat_result.get("status") or "failed",
            chat_result.get("externalId") or "",
        ])
    values.append(clean_cell_value(draft_id))
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE document_assignment_history SET "
            + ", ".join(assignments)
            + " WHERE draft_id = ?",
            values,
        )
        conn.commit()
    finally:
        conn.close()


def _execute_crm_attempt(
    draft: dict,
    *,
    allow_retry: bool = False,
) -> dict:
    recipient = draft.get("recipient") or {}
    if clean_cell_value(recipient.get("type")) != "external":
        return {
            "deliveryType": "crm",
            "status": "skipped",
            "error": "",
            "company": {},
            "retryable": False,
            "uncertainExternalResult": False,
        }

    draft_id = clean_cell_value(draft.get("draftId"))
    session_id = clean_cell_value(draft.get("sessionId"))
    company_id = clean_cell_value(recipient.get("companyId"))
    idempotency_key = f"notification:{draft_id}:crm:v1"
    request_payload = {
        "operation": "ensure_supplier_company_synced",
        "companyId": company_id,
    }
    row, created = _reserve_attempt(
        draft_id=draft_id,
        session_id=session_id,
        delivery_type="crm",
        idempotency_key=idempotency_key,
        recipient_key=_recipient_key(draft),
        draft_ids=[draft_id],
        group_key=draft_id,
        request_payload=request_payload,
    )
    status = clean_cell_value(row.get("status")).lower()
    if created:
        _update_attempt(
            idempotency_key,
            status="sending",
            increment_attempts=True,
            started=True,
            error="",
        )
    elif allow_retry and status in DELIVERY_RETRYABLE_STATUSES:
        row, claimed = _claim_retry_attempt(idempotency_key)
        if not claimed:
            result = _public_attempt(row)
            result["company"] = {}
            return result
    else:
        result = _public_attempt(row)
        result["company"] = {}
        return result

    if not company_id:
        row = _update_attempt(
            idempotency_key,
            status="failed",
            error="external contractor companyId is missing",
            finished=True,
        )
        result = _public_attempt(row)
        result["company"] = {}
        return result

    if not clean_cell_value(BITRIX_TECH_WEBHOOK_URL):
        row = _update_attempt(
            idempotency_key,
            status="blocked",
            error="BITRIX_TECH_WEBHOOK_URL is empty",
            finished=True,
        )
        result = _public_attempt(row)
        result["company"] = {}
        return result

    try:
        sync_result = ensure_supplier_company_synced(company_id)
    except Exception as exc:
        sync_result = {
            "ok": False,
            "status": "error",
            "error": str(exc),
            "company": {},
            "retryRisk": "unknown_external_result",
        }

    sync_status = clean_cell_value(sync_result.get("status")).lower()
    if sync_result.get("ok") or sync_status in {"synced", "cached"}:
        final_status = "synced"
    elif sync_status in {"disabled", "blocked"}:
        final_status = "blocked"
    else:
        final_status = "failed"
    company = sync_result.get("company") or {}
    row = _update_attempt(
        idempotency_key,
        status=final_status,
        external_id=clean_cell_value(company.get("companyId")),
        response_payload={"result": sync_result},
        error=clean_cell_value(sync_result.get("error")),
        finished=True,
    )
    result = _public_attempt(row)
    result["company"] = company
    return result


def deliver_committed_notification_drafts(session_id: str) -> dict:
    ensure_notification_delivery_schema()
    normalized_session_id = clean_cell_value(session_id)
    drafts = _load_committed_drafts(normalized_session_id)
    ready_drafts = [draft for draft in drafts if draft.get("isReady")]
    invalid_drafts = [
        {
            "draftId": draft.get("draftId") or "",
            "errors": draft.get("readinessErrors") or [],
        }
        for draft in drafts
        if not draft.get("isReady")
    ]

    task_results: dict[str, dict] = {}
    crm_results: dict[str, dict] = {}
    groups: dict[tuple[str, str], list[dict]] = {}

    for draft in ready_drafts:
        _ensure_history_rows(draft)
        crm_result = _execute_crm_attempt(draft)
        crm_results[draft.get("draftId") or ""] = crm_result
        _update_history_for_draft(
            draft.get("draftId") or "",
            crm_result=crm_result,
        )

        project = get_project_storage_context(draft.get("dialogId") or "") or {}
        try:
            task_payload = _build_task_payload(draft, project)
        except Exception as exc:
            task_payload = {}
            task_result = {
                "deliveryType": "task",
                "status": "failed",
                "error": str(exc),
                "externalId": "",
                "externalUrl": "",
                "attemptId": "",
            }
        else:
            responsible_id = _task_responsible_id(draft)
            task_result = _execute_bitrix_attempt(
                draft_id=draft.get("draftId") or "",
                session_id=normalized_session_id,
                delivery_type="task",
                idempotency_key=(
                    f"notification:{draft.get('draftId')}:task:v1"
                ),
                method="tasks.task.add",
                payload=task_payload,
                recipient_key=_recipient_key(draft),
                draft_ids=[draft.get("draftId") or ""],
                group_key=draft.get("draftId") or "",
                id_parser=_parse_task_id,
                url_builder=lambda task_id, responsible_id=responsible_id: (
                    _task_url(task_id, responsible_id)
                ),
            )
        task_results[draft.get("draftId") or ""] = task_result
        _update_history_for_draft(
            draft.get("draftId") or "",
            task_result=task_result,
        )
        group_key = (
            clean_cell_value(draft.get("dialogId")),
            _recipient_key(draft),
        )
        groups.setdefault(group_key, []).append(draft)

    chat_results: list[dict] = []
    for (dialog_id, recipient_key), group_drafts in groups.items():
        group_drafts.sort(key=lambda item: (
            item.get("createdAt") or "",
            item.get("draftId") or "",
        ))
        draft_ids = [draft.get("draftId") or "" for draft in group_drafts]
        group_hash = _hash_key(
            normalized_session_id,
            dialog_id,
            recipient_key,
            *draft_ids,
        )[:24]
        project = get_project_storage_context(dialog_id) or {}
        message = _build_group_message(group_drafts, task_results, project)
        chat_result = _execute_bitrix_attempt(
            draft_id=draft_ids[0] if draft_ids else "",
            session_id=normalized_session_id,
            delivery_type="chat",
            idempotency_key=(
                f"notification:{normalized_session_id}:chat:{group_hash}:v1"
            ),
            method="im.message.add",
            payload={
                "DIALOG_ID": dialog_id,
                "MESSAGE": message,
                "SYSTEM": "N",
                "URL_PREVIEW": "N",
            },
            recipient_key=recipient_key,
            draft_ids=draft_ids,
            group_key=group_hash,
            id_parser=_parse_message_id,
        )
        chat_result["recipientKey"] = recipient_key
        chat_results.append(chat_result)
        for draft_id in draft_ids:
            _update_history_for_draft(draft_id, chat_result=chat_result)

    task_list = [task_results[key] for key in sorted(task_results)]
    crm_list = [crm_results[key] for key in sorted(crm_results)]
    overall_status = "skipped"
    all_results = task_list + chat_results
    if all_results:
        if all(result.get("status") == "sent" for result in all_results):
            overall_status = "sent"
        elif any(result.get("status") == "sent" for result in all_results):
            overall_status = "partial"
        elif any(result.get("status") == "blocked" for result in all_results):
            overall_status = "blocked"
        else:
            overall_status = "failed"

    return {
        "ok": overall_status in {"sent", "skipped"},
        "status": overall_status,
        "sessionId": normalized_session_id,
        "draftCount": len(drafts),
        "readyDraftCount": len(ready_drafts),
        "invalidDrafts": invalid_drafts,
        "taskResults": task_list,
        "chatResults": chat_results,
        "crmResults": crm_list,
    }


def _load_committed_draft_by_id(draft_id: str) -> dict:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM notification_drafts "
            "WHERE draft_id = ? AND status = 'committed'",
            (clean_cell_value(draft_id),),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        raise NotificationDeliveryError("committed notification draft not found")
    draft = public_notification_draft(dict(row))
    if not draft.get("isReady"):
        raise NotificationDeliveryError(
            "notification draft is not ready: "
            + "; ".join(draft.get("readinessErrors") or [])
        )
    return draft


def _task_attempt_key(draft_id: str) -> str:
    return f"notification:{clean_cell_value(draft_id)}:task:v1"


def _crm_attempt_key(draft_id: str) -> str:
    return f"notification:{clean_cell_value(draft_id)}:crm:v1"


def _group_drafts_for_retry(draft: dict) -> list[dict]:
    drafts = _load_committed_drafts(draft.get("sessionId") or "")
    key = (
        clean_cell_value(draft.get("dialogId")),
        _recipient_key(draft),
    )
    grouped = [
        candidate for candidate in drafts
        if candidate.get("isReady")
        and (
            clean_cell_value(candidate.get("dialogId")),
            _recipient_key(candidate),
        ) == key
    ]
    grouped.sort(key=lambda item: (
        item.get("createdAt") or "",
        item.get("draftId") or "",
    ))
    return grouped


def _chat_attempt_spec(group_drafts: list[dict]) -> dict:
    if not group_drafts:
        return {}
    session_id = clean_cell_value(group_drafts[0].get("sessionId"))
    dialog_id = clean_cell_value(group_drafts[0].get("dialogId"))
    recipient_key = _recipient_key(group_drafts[0])
    draft_ids = [clean_cell_value(item.get("draftId")) for item in group_drafts]
    group_hash = _hash_key(
        session_id,
        dialog_id,
        recipient_key,
        *draft_ids,
    )[:24]
    return {
        "sessionId": session_id,
        "dialogId": dialog_id,
        "recipientKey": recipient_key,
        "draftIds": draft_ids,
        "groupHash": group_hash,
        "idempotencyKey": f"notification:{session_id}:chat:{group_hash}:v1",
    }


def _task_result_from_storage(draft: dict) -> dict:
    row = _attempt_row(_task_attempt_key(draft.get("draftId") or ""))
    if row:
        return _public_attempt(row)
    conn = get_conn()
    try:
        history = conn.execute(
            "SELECT task_status, bitrix_task_id, bitrix_task_url "
            "FROM document_assignment_history WHERE draft_id = ? "
            "ORDER BY created_at DESC LIMIT 1",
            (clean_cell_value(draft.get("draftId")),),
        ).fetchone()
    finally:
        conn.close()
    return {
        "deliveryType": "task",
        "status": clean_cell_value(history["task_status"] if history else "pending"),
        "externalId": clean_cell_value(history["bitrix_task_id"] if history else ""),
        "externalUrl": clean_cell_value(history["bitrix_task_url"] if history else ""),
        "error": "",
        "attemptId": "",
        "retryable": False,
        "uncertainExternalResult": False,
    }


def _history_delivery_statuses(draft_id: str) -> dict[str, str]:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT chat_status, task_status, crm_sync_status "
            "FROM document_assignment_history WHERE draft_id = ? "
            "ORDER BY created_at DESC LIMIT 1",
            (clean_cell_value(draft_id),),
        ).fetchone()
    finally:
        conn.close()
    return {
        "chat": clean_cell_value(row["chat_status"] if row else ""),
        "task": clean_cell_value(row["task_status"] if row else ""),
        "crm": clean_cell_value(row["crm_sync_status"] if row else ""),
    }


def _retry_state_for_draft(draft: dict) -> dict[str, dict]:
    draft_id = clean_cell_value(draft.get("draftId"))
    history_statuses = _history_delivery_statuses(draft_id)
    task_row = _attempt_row(_task_attempt_key(draft_id))
    crm_row = _attempt_row(_crm_attempt_key(draft_id))
    chat_spec = _chat_attempt_spec(_group_drafts_for_retry(draft))
    chat_row = _attempt_row(chat_spec.get("idempotencyKey") or "") if chat_spec else None
    rows = {"task": task_row, "crm": crm_row, "chat": chat_row}
    result: dict[str, dict] = {}
    for delivery_type in ("crm", "task", "chat"):
        row = rows.get(delivery_type)
        status = clean_cell_value((row or {}).get("status")).lower()
        if not status:
            status = clean_cell_value(history_statuses.get(delivery_type)).lower()
        retryable = status in DELIVERY_RETRYABLE_STATUSES or status == "error"
        result[delivery_type] = {
            "status": status,
            "retryable": retryable,
            "uncertainExternalResult": _attempt_has_uncertain_result(row),
            "attempt": _public_attempt(row) if row else {},
        }
    if clean_cell_value((draft.get("recipient") or {}).get("type")) != "external":
        result["crm"]["retryable"] = False
    return result


def retry_failed_notification_deliveries(
    *,
    draft_id: str,
    delivery_types: list[str] | None = None,
    confirmed: bool = False,
    confirm_uncertain: bool = False,
) -> dict:
    if confirmed is not True:
        raise NotificationDeliveryError("confirmed=true is required")
    draft = _load_committed_draft_by_id(draft_id)
    requested = {
        clean_cell_value(value).lower()
        for value in (delivery_types or DELIVERY_TYPES)
        if clean_cell_value(value).lower() in DELIVERY_TYPES
    }
    if not requested:
        raise NotificationDeliveryError("no valid delivery types requested")

    state = _retry_state_for_draft(draft)
    retryable_types = [
        value for value in ("crm", "task", "chat")
        if value in requested and state[value]["retryable"]
    ]
    uncertain_types = [
        value for value in retryable_types
        if state[value]["uncertainExternalResult"]
    ]
    if uncertain_types and not confirm_uncertain:
        return {
            "ok": False,
            "status": "confirmation_required",
            "draftId": draft.get("draftId") or "",
            "retryableTypes": retryable_types,
            "uncertainTypes": uncertain_types,
            "requiresUncertainConfirmation": True,
            "warning": (
                "Bitrix мог выполнить отмеченные операции, но ответ не дошёл. "
                "Повтор может создать дубль."
            ),
            "results": [],
        }

    results: list[dict] = []
    task_result: dict | None = None

    if "crm" in retryable_types:
        crm_result = _execute_crm_attempt(draft, allow_retry=True)
        _update_history_for_draft(draft.get("draftId") or "", crm_result=crm_result)
        results.append(crm_result)

    if "task" in retryable_types:
        project = get_project_storage_context(draft.get("dialogId") or "") or {}
        try:
            task_payload = _build_task_payload(draft, project)
            responsible_id = _task_responsible_id(draft)
            task_result = _execute_bitrix_attempt(
                draft_id=draft.get("draftId") or "",
                session_id=draft.get("sessionId") or "",
                delivery_type="task",
                idempotency_key=_task_attempt_key(draft.get("draftId") or ""),
                method="tasks.task.add",
                payload=task_payload,
                recipient_key=_recipient_key(draft),
                draft_ids=[draft.get("draftId") or ""],
                group_key=draft.get("draftId") or "",
                id_parser=_parse_task_id,
                url_builder=lambda task_id, responsible_id=responsible_id: (
                    _task_url(task_id, responsible_id)
                ),
                allow_retry=True,
            )
        except Exception as exc:
            task_result = {
                "deliveryType": "task",
                "status": "failed",
                "error": str(exc),
                "externalId": "",
                "externalUrl": "",
                "attemptId": "",
                "retryable": True,
                "uncertainExternalResult": False,
            }
        _update_history_for_draft(draft.get("draftId") or "", task_result=task_result)
        results.append(task_result)

    if "chat" in retryable_types:
        group_drafts = _group_drafts_for_retry(draft)
        spec = _chat_attempt_spec(group_drafts)
        task_results = {
            item.get("draftId") or "": (
                task_result
                if item.get("draftId") == draft.get("draftId") and task_result
                else _task_result_from_storage(item)
            )
            for item in group_drafts
        }
        project = get_project_storage_context(spec.get("dialogId") or "") or {}
        message = _build_group_message(group_drafts, task_results, project)
        chat_result = _execute_bitrix_attempt(
            draft_id=(spec.get("draftIds") or [""])[0],
            session_id=spec.get("sessionId") or "",
            delivery_type="chat",
            idempotency_key=spec.get("idempotencyKey") or "",
            method="im.message.add",
            payload={
                "DIALOG_ID": spec.get("dialogId") or "",
                "MESSAGE": message,
                "SYSTEM": "N",
                "URL_PREVIEW": "N",
            },
            recipient_key=spec.get("recipientKey") or "",
            draft_ids=spec.get("draftIds") or [],
            group_key=spec.get("groupHash") or "",
            id_parser=_parse_message_id,
            allow_retry=True,
        )
        chat_result["recipientKey"] = spec.get("recipientKey") or ""
        for grouped_draft_id in spec.get("draftIds") or []:
            _update_history_for_draft(grouped_draft_id, chat_result=chat_result)
        results.append(chat_result)

    remaining = _retry_state_for_draft(draft)
    remaining_types = [
        value for value in ("crm", "task", "chat")
        if remaining[value]["retryable"]
    ]
    attempted = bool(results)
    all_successful = attempted and all(
        clean_cell_value(result.get("status")).lower()
        in DELIVERY_SUCCESS_STATUSES | {"skipped"}
        for result in results
    )
    return {
        "ok": True,
        "allSucceeded": all_successful and not remaining_types,
        "status": (
            "succeeded" if all_successful and not remaining_types
            else "partial" if attempted
            else "nothing_to_retry"
        ),
        "draftId": draft.get("draftId") or "",
        "retriedTypes": [
            clean_cell_value(result.get("deliveryType")) for result in results
        ],
        "remainingRetryableTypes": remaining_types,
        "requiresUncertainConfirmation": False,
        "results": results,
    }


def list_notification_delivery_attempts(
    *,
    session_id: str = "",
    draft_id: str = "",
) -> list[dict]:
    ensure_notification_delivery_schema()
    clauses: list[str] = []
    values: list[Any] = []
    if clean_cell_value(session_id):
        clauses.append("session_id = ?")
        values.append(clean_cell_value(session_id))
    if clean_cell_value(draft_id):
        clauses.append("(draft_id = ? OR draft_ids_json LIKE ?)")
        values.extend([
            clean_cell_value(draft_id),
            f'%"{clean_cell_value(draft_id)}"%',
        ])
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM notification_delivery_attempts"
            + where
            + " ORDER BY created_at ASC, attempt_id ASC",
            values,
        ).fetchall()
        return [_public_attempt(dict(row)) for row in rows]
    finally:
        conn.close()
