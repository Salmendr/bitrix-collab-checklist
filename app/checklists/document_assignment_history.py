from __future__ import annotations

from collections import defaultdict
import json
from typing import Any

from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)
from app.db import get_conn


DEFAULT_HISTORY_LIMIT = 10
MAX_HISTORY_LIMIT = 10


def _safe_limit(value: Any) -> int:
    try:
        parsed = int(value or DEFAULT_HISTORY_LIMIT)
    except (TypeError, ValueError):
        parsed = DEFAULT_HISTORY_LIMIT
    return max(1, min(parsed, MAX_HISTORY_LIMIT))


def get_assignment_history_count_map(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str = "",
) -> dict[str, int]:
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(checklist_key)
    normalized_item_id = clean_cell_value(item_id)

    if not normalized_dialog_id:
        return {}

    clauses = [
        "dialog_id = ?",
        "checklist_key = ?",
        "COALESCE(series_id, '') <> ''",
    ]
    values: list[Any] = [
        normalized_dialog_id,
        normalized_checklist_key,
    ]

    if normalized_item_id:
        clauses.append("item_id = ?")
        values.append(normalized_item_id)

    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT series_id, COUNT(*) AS history_count "
            "FROM document_assignment_history WHERE "
            + " AND ".join(clauses)
            + " GROUP BY series_id",
            values,
        ).fetchall()
    finally:
        conn.close()

    return {
        clean_cell_value(row["series_id"]): int(row["history_count"] or 0)
        for row in rows
        if clean_cell_value(row["series_id"])
    }


def attach_assignment_history_counts(
    data: dict,
    *,
    dialog_id: str,
    checklist_key: str,
) -> dict:
    if not isinstance(data, dict):
        return data

    count_map = get_assignment_history_count_map(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
    )

    for item in data.get("items") or []:
        if not isinstance(item, dict):
            continue

        for document in item.get("documents") or []:
            if not isinstance(document, dict):
                continue
            series_id = (
                clean_cell_value(document.get("seriesId"))
                or clean_cell_value(document.get("id"))
            )
            document["assignmentHistoryCount"] = int(
                count_map.get(series_id, 0)
            )

        for series in item.get("archivedDocumentSeries") or []:
            if not isinstance(series, dict):
                continue
            series_id = clean_cell_value(series.get("seriesId"))
            series["assignmentHistoryCount"] = int(
                count_map.get(series_id, 0)
            )

    return data


RETRYABLE_DELIVERY_STATUSES = frozenset({"failed", "blocked", "disabled", "error"})


def _json_loads(value: Any, default: Any) -> Any:
    raw = clean_cell_value(value)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _delivery_retry_metadata(draft_id: str, statuses: dict[str, str]) -> dict:
    normalized_draft_id = clean_cell_value(draft_id)
    if not normalized_draft_id:
        return {
            "canRetry": False,
            "retryableTypes": [],
            "uncertainTypes": [],
        }
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM notification_delivery_attempts "
            "WHERE draft_id = ? OR draft_ids_json LIKE ? "
            "ORDER BY created_at DESC, attempt_id DESC",
            (normalized_draft_id, f'%"{normalized_draft_id}"%'),
        ).fetchall()
    finally:
        conn.close()

    attempts: dict[str, dict] = {}
    for row in rows:
        raw = dict(row)
        delivery_type = clean_cell_value(raw.get("delivery_type")).lower()
        if delivery_type in {"crm", "task", "chat"} and delivery_type not in attempts:
            attempts[delivery_type] = raw

    retryable_types: list[str] = []
    uncertain_types: list[str] = []
    for delivery_type in ("crm", "task", "chat"):
        attempt = attempts.get(delivery_type) or {}
        status = clean_cell_value(attempt.get("status")).lower()
        if not status:
            status = clean_cell_value(statuses.get(delivery_type)).lower()
        if status in RETRYABLE_DELIVERY_STATUSES:
            retryable_types.append(delivery_type)
        response = _json_loads(attempt.get("response_json"), {})
        if isinstance(response, dict) and (
            clean_cell_value(response.get("exception"))
            or clean_cell_value(response.get("retryRisk")) == "unknown_external_result"
        ):
            uncertain_types.append(delivery_type)

    return {
        "canRetry": bool(retryable_types),
        "retryableTypes": retryable_types,
        "uncertainTypes": uncertain_types,
    }


def _public_history_row(row: Any) -> dict:
    raw = dict(row)
    recipient_type = clean_cell_value(raw.get("recipient_type")) or "internal"
    delivery_statuses = {
        "chat": clean_cell_value(raw.get("chat_status")) or "pending",
        "task": clean_cell_value(raw.get("task_status")) or "pending",
        "crm": clean_cell_value(raw.get("crm_sync_status")) or "pending",
    }
    retry_metadata = _delivery_retry_metadata(
        clean_cell_value(raw.get("draft_id")),
        delivery_statuses,
    )
    return {
        "historyId": clean_cell_value(raw.get("history_id")),
        "draftId": clean_cell_value(raw.get("draft_id")),
        "sessionId": clean_cell_value(raw.get("session_id")),
        "dialogId": clean_cell_value(raw.get("dialog_id")),
        "checklistKey": clean_cell_value(raw.get("checklist_key")),
        "itemId": clean_cell_value(raw.get("item_id")),
        "seriesId": clean_cell_value(raw.get("series_id")),
        "documentId": clean_cell_value(raw.get("document_id")),
        "versionName": clean_cell_value(raw.get("version_name")) or "Документ",
        "sender": {
            "userId": clean_cell_value(raw.get("sender_user_id")),
            "name": clean_cell_value(raw.get("sender_name")) or "—",
        },
        "recipient": {
            "type": recipient_type,
            "userId": clean_cell_value(raw.get("recipient_user_id")),
            "name": clean_cell_value(raw.get("recipient_name")) or "—",
            "isExternal": recipient_type == "external",
        },
        "assignmentPart": {
            "id": clean_cell_value(raw.get("assignment_part_id")),
            "text": clean_cell_value(raw.get("assignment_part_text")) or "—",
        },
        "deadlineDate": clean_cell_value(raw.get("deadline_date")),
        "task": {
            "id": clean_cell_value(raw.get("bitrix_task_id")),
            "url": clean_cell_value(raw.get("bitrix_task_url")),
            "status": clean_cell_value(raw.get("task_status")) or "pending",
        },
        "delivery": {
            "chatStatus": delivery_statuses["chat"],
            "taskStatus": delivery_statuses["task"],
            "crmSyncStatus": delivery_statuses["crm"],
            "chatMessageId": clean_cell_value(raw.get("chat_message_id")),
            "attemptId": clean_cell_value(raw.get("delivery_attempt_id")),
            **retry_metadata,
        },
        "createdAt": clean_cell_value(raw.get("created_at")),
        "updatedAt": clean_cell_value(raw.get("updated_at")),
    }


def list_document_assignment_history(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    series_id: str,
    limit: int = DEFAULT_HISTORY_LIMIT,
) -> dict:
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(checklist_key)
    normalized_item_id = clean_cell_value(item_id)
    normalized_series_id = clean_cell_value(series_id)
    normalized_limit = _safe_limit(limit)

    if not normalized_dialog_id:
        raise ValueError("dialogId is required")
    if not normalized_item_id:
        raise ValueError("itemId is required")
    if not normalized_series_id:
        raise ValueError("seriesId is required")

    conn = get_conn()
    try:
        total_row = conn.execute(
            "SELECT COUNT(*) AS history_count "
            "FROM document_assignment_history "
            "WHERE dialog_id = ? AND checklist_key = ? "
            "AND item_id = ? AND series_id = ?",
            (
                normalized_dialog_id,
                normalized_checklist_key,
                normalized_item_id,
                normalized_series_id,
            ),
        ).fetchone()
        rows = conn.execute(
            "SELECT * FROM document_assignment_history "
            "WHERE dialog_id = ? AND checklist_key = ? "
            "AND item_id = ? AND series_id = ? "
            "ORDER BY created_at DESC, history_id DESC LIMIT ?",
            (
                normalized_dialog_id,
                normalized_checklist_key,
                normalized_item_id,
                normalized_series_id,
                normalized_limit,
            ),
        ).fetchall()
    finally:
        conn.close()

    total_count = int(total_row["history_count"] or 0) if total_row else 0
    return {
        "dialogId": normalized_dialog_id,
        "checklistKey": normalized_checklist_key,
        "itemId": normalized_item_id,
        "seriesId": normalized_series_id,
        "historyCount": total_count,
        "returnedCount": len(rows),
        "limit": normalized_limit,
        "history": [_public_history_row(row) for row in rows],
    }
