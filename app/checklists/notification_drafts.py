from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any

from app.db import get_conn

from app.checklists.assignment_parts import (
    resolve_assignment_part_in_transaction,
)
from app.checklists.bitrix_users import resolve_cached_bitrix_user
from app.checklists.bitrix_companies import (
    ExternalContractorValidationError,
    resolve_external_contractor_in_transaction,
)
from app.checklists.project_curator import (
    resolve_curator_for_external_recipient,
)
from app.checklists.documents import (
    migrate_legacy_document_fields,
    normalize_documents_list,
)
from app.checklists.edit_session_changes import (
    acquire_checklist_for_edit_session,
)
from app.checklists.edit_sessions import (
    EditSessionConflictError,
    EditSessionNotFoundError,
    get_edit_session_for_actor,
    utc_now_iso,
)
from app.checklists.storage import get_checklist
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


DRAFT_STATUS_ACTIVE = "draft"
DRAFT_STATUS_COMMITTED = "committed"
DRAFT_STATUS_CANCELLED = "cancelled"
DRAFT_MUTABLE_STATUSES = frozenset({DRAFT_STATUS_ACTIVE})
RECIPIENT_TYPES = frozenset({"internal", "external"})


class NotificationDraftError(RuntimeError):
    pass


class NotificationDraftNotFoundError(NotificationDraftError):
    pass


class NotificationDraftConflictError(NotificationDraftError):
    pass


class NotificationDraftValidationError(NotificationDraftError):
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


def _normalize_deadline_date(value: Any) -> str:
    raw = clean_cell_value(value)
    if not raw:
        return ""

    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise NotificationDraftValidationError(
            "deadlineDate must use YYYY-MM-DD format"
        ) from exc

    normalized = parsed.isoformat()
    if normalized != raw:
        raise NotificationDraftValidationError(
            "deadlineDate must use YYYY-MM-DD format"
        )
    return normalized


def _normalize_sender(value: Any) -> dict:
    raw = value if isinstance(value, dict) else {}
    user_id = clean_cell_value(
        raw.get("userId")
        or raw.get("id")
        or raw.get("bitrixUserId")
    )
    name = clean_cell_value(
        raw.get("name")
        or raw.get("displayName")
    )
    cached = resolve_cached_bitrix_user(user_id=user_id, name=name)
    if cached:
        user_id = clean_cell_value(cached.get("userId")) or user_id
        name = clean_cell_value(cached.get("name")) or name
    return {
        "userId": user_id,
        "name": name,
    }


def _normalize_recipient(value: Any) -> dict | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise NotificationDraftValidationError(
            "recipient must be an object"
        )

    recipient_type = clean_cell_value(
        value.get("type")
        or value.get("recipientType")
    ).lower()

    if recipient_type and recipient_type not in RECIPIENT_TYPES:
        raise NotificationDraftValidationError(
            "recipient type must be internal or external"
        )

    if not recipient_type:
        recipient_type = "internal"

    bitrix_user_id = clean_cell_value(
        value.get("bitrixUserId")
        or value.get("userId")
        or value.get("id")
    )
    name = clean_cell_value(
        value.get("name")
        or value.get("displayName")
        or value.get("title")
    )
    if recipient_type == "internal":
        cached = resolve_cached_bitrix_user(
            user_id=bitrix_user_id,
            name=name,
        )
        if cached:
            bitrix_user_id = clean_cell_value(cached.get("userId")) or bitrix_user_id
            name = clean_cell_value(cached.get("name")) or name

    return {
        "type": recipient_type,
        "bitrixUserId": bitrix_user_id,
        "name": name,
        "phone": clean_cell_value(value.get("phone")),
        "email": clean_cell_value(value.get("email")),
        "contactDetails": clean_cell_value(
            value.get("contactDetails")
            or value.get("contact")
        ),
        "companyId": clean_cell_value(value.get("companyId")),
        "curatorUserId": clean_cell_value(
            value.get("curatorUserId")
        ),
        "curatorName": clean_cell_value(
            value.get("curatorName")
        ),
        "metadata": (
            value.get("metadata")
            if isinstance(value.get("metadata"), dict)
            else {}
        ),
    }


def _attach_project_curator(
    recipient: dict | None,
    *,
    dialog_id: str,
) -> dict | None:
    if recipient is None:
        return None
    if clean_cell_value(recipient.get("type")) != "external":
        return recipient

    curator = resolve_curator_for_external_recipient(
        dialog_id,
        current_user_id=recipient.get("curatorUserId") or "",
        current_name=recipient.get("curatorName") or "",
    )
    result = dict(recipient)
    metadata = dict(result.get("metadata") or {})
    metadata.update({
        "curatorResolutionStatus": curator.get("status") or "",
        "curatorResolutionSource": curator.get("source") or "",
        "curatorResolutionError": curator.get("error") or "",
    })
    result["curatorUserId"] = curator.get("userId") or ""
    result["curatorName"] = curator.get("name") or ""
    result["metadata"] = metadata
    return result


def _resolve_recipient_in_transaction(
    conn,
    recipient: dict | None,
    *,
    mark_used: bool = True,
) -> dict | None:
    if recipient is None:
        return None
    if clean_cell_value(recipient.get("type")) != "external":
        return recipient

    try:
        company = resolve_external_contractor_in_transaction(
            conn,
            company_id=recipient.get("companyId") or "",
            title=recipient.get("name") or "",
            phone=recipient.get("phone") or "",
            email=recipient.get("email") or "",
            contact_details=recipient.get("contactDetails") or "",
            mark_used=mark_used,
        )
    except ExternalContractorValidationError as exc:
        raise NotificationDraftValidationError(str(exc)) from exc

    metadata = dict(recipient.get("metadata") or {})
    metadata.update({
        "contractorSource": company.get("source") or "",
        "contractorSyncStatus": company.get("syncStatus") or "",
        "contractorSyncError": company.get("syncError") or "",
    })
    return {
        "type": "external",
        "bitrixUserId": "",
        "name": company.get("title") or recipient.get("name") or "",
        "phone": company.get("phone") or recipient.get("phone") or "",
        "email": company.get("email") or recipient.get("email") or "",
        "contactDetails": (
            company.get("contactDetails")
            or recipient.get("contactDetails")
            or ""
        ),
        "companyId": company.get("companyId") or "",
        "curatorUserId": recipient.get("curatorUserId") or "",
        "curatorName": recipient.get("curatorName") or "",
        "metadata": metadata,
    }


def _find_item(checklist_data: dict, item_id: str) -> dict:
    normalized_item_id = clean_cell_value(item_id)

    for raw_item in (checklist_data or {}).get("items", []) or []:
        if clean_cell_value(raw_item.get("id")) != normalized_item_id:
            continue
        return migrate_legacy_document_fields(raw_item)

    raise NotificationDraftValidationError("checklist item not found")


def _normalize_document_id_list(raw_items: Any) -> list[str]:
    if raw_items is None:
        return []
    if not isinstance(raw_items, list):
        raise NotificationDraftValidationError(
            "documentIds must be an array"
        )

    result: list[str] = []
    seen: set[str] = set()
    for raw_item in raw_items:
        if isinstance(raw_item, dict):
            document_id = clean_cell_value(
                raw_item.get("documentId")
                or raw_item.get("id")
            )
        else:
            document_id = clean_cell_value(raw_item)

        if not document_id or document_id in seen:
            continue
        seen.add(document_id)
        result.append(document_id)

    return result

def _requested_document_ids(payload: dict) -> list[str] | None:
    if "documentIds" in payload:
        raw_items = payload.get("documentIds")
    elif "files" in payload:
        raw_items = payload.get("files")
    else:
        return None

    return _normalize_document_id_list(raw_items)


def _resolve_current_documents(
    *,
    checklist_data: dict,
    item_id: str,
    document_ids: list[str],
) -> tuple[dict, list[dict]]:
    item = _find_item(checklist_data, item_id)
    current_documents = {
        clean_cell_value(document.get("id")): document
        for document in normalize_documents_list(
            item.get("documents")
        )
        if clean_cell_value(document.get("id"))
    }

    missing = [
        document_id
        for document_id in document_ids
        if document_id not in current_documents
    ]
    if missing:
        raise NotificationDraftValidationError(
            "only current document versions may be selected: "
            + ", ".join(missing)
        )

    selected = []
    for index, document_id in enumerate(document_ids):
        document = current_documents[document_id]
        selected.append({
            "documentId": document_id,
            "seriesId": clean_cell_value(
                document.get("seriesId")
            ),
            "fileName": clean_cell_value(
                document.get("name")
                or document.get("documentName")
            ),
            "fileSize": int(document.get("size") or 0),
            "fileUrl": clean_cell_value(
                document.get("fileUrl")
                or document.get("path")
            ),
            "position": index,
        })

    return item, selected


def _draft_row(draft_id: str) -> dict | None:
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT *
            FROM notification_drafts
            WHERE draft_id = ?
        """, (clean_cell_value(draft_id),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _draft_files(draft_id: str) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT *
            FROM notification_files
            WHERE draft_id = ?
            ORDER BY sort_order ASC, created_at ASC
        """, (clean_cell_value(draft_id),)).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _draft_recipient(draft_id: str) -> dict | None:
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT *
            FROM notification_recipients
            WHERE draft_id = ?
            ORDER BY created_at ASC
            LIMIT 1
        """, (clean_cell_value(draft_id),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _public_recipient(row: dict | None) -> dict | None:
    if not row:
        return None
    return {
        "recipientId": row.get("recipient_id") or "",
        "type": row.get("recipient_type") or "",
        "bitrixUserId": row.get("bitrix_user_id") or "",
        "name": row.get("display_name") or "",
        "phone": row.get("phone") or "",
        "email": row.get("email") or "",
        "contactDetails": row.get("contact_details") or "",
        "companyId": row.get("company_id") or "",
        "curatorUserId": row.get("curator_user_id") or "",
        "curatorName": row.get("curator_name") or "",
        "metadata": _json_loads(row.get("metadata_json") or "", {}),
    }


def _readiness_errors(
    *,
    row: dict,
    files: list[dict],
    recipient: dict | None,
) -> list[str]:
    errors: list[str] = []

    if not clean_cell_value(row.get("sender_user_id")):
        errors.append("sender is required")
    if not clean_cell_value(row.get("assignment_part_text")):
        errors.append("assignment part is required")
    if not clean_cell_value(row.get("deadline_date")):
        errors.append("deadline date is required")
    if not files:
        errors.append("at least one current file is required")

    if not recipient:
        errors.append("recipient is required")
    else:
        recipient_type = clean_cell_value(
            recipient.get("recipient_type")
        )
        if recipient_type == "internal":
            if not clean_cell_value(recipient.get("bitrix_user_id")):
                errors.append("internal recipient userId is required")
        elif recipient_type == "external":
            if not clean_cell_value(recipient.get("display_name")):
                errors.append("external recipient name is required")
            if not any([
                clean_cell_value(recipient.get("phone")),
                clean_cell_value(recipient.get("email")),
                clean_cell_value(recipient.get("contact_details")),
            ]):
                errors.append(
                    "external recipient contact details are required"
                )
            if not clean_cell_value(recipient.get("curator_user_id")):
                errors.append("external recipient curator is required")
        else:
            errors.append("recipient type is invalid")

    return errors


def public_notification_draft(row: dict) -> dict:
    files = _draft_files(row.get("draft_id") or "")
    recipient = _draft_recipient(row.get("draft_id") or "")
    readiness_errors = _readiness_errors(
        row=row,
        files=files,
        recipient=recipient,
    )

    return {
        "draftId": row.get("draft_id") or "",
        "sessionId": row.get("session_id") or "",
        "dialogId": row.get("dialog_id") or "",
        "checklistKey": row.get("checklist_key") or "",
        "itemId": row.get("item_id") or "",
        "itemName": row.get("item_name") or "",
        "groupId": int(row.get("group_id") or 0),
        "status": row.get("status") or "",
        "version": int(row.get("version") or 1),
        "sender": {
            "userId": row.get("sender_user_id") or "",
            "name": row.get("sender_name") or "",
        },
        "recipient": _public_recipient(recipient),
        "assignmentPart": {
            "id": row.get("assignment_part_id") or "",
            "text": row.get("assignment_part_text") or "",
        },
        "deadlineDate": row.get("deadline_date") or "",
        "description": row.get("description") or "",
        "files": [
            {
                "notificationFileId": file_row.get("notification_file_id") or "",
                "seriesId": file_row.get("series_id") or "",
                "documentId": file_row.get("document_id") or "",
                "fileName": file_row.get("file_name") or "",
                "fileSize": int(file_row.get("file_size") or 0),
                "fileUrl": file_row.get("file_url") or "",
                "position": int(file_row.get("sort_order") or 0),
            }
            for file_row in files
        ],
        "isReady": not readiness_errors,
        "readinessErrors": readiness_errors,
        "metadata": _json_loads(row.get("metadata_json") or "", {}),
        "createdById": row.get("created_by_id") or "",
        "createdByName": row.get("created_by_name") or "",
        "createdAt": row.get("created_at") or "",
        "updatedAt": row.get("updated_at") or "",
        "committedAt": row.get("committed_at") or "",
        "cancelledAt": row.get("cancelled_at") or "",
    }


def get_notification_draft_for_actor(
    *,
    draft_id: str,
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
) -> dict:
    session = get_edit_session_for_actor(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )
    row = _draft_row(draft_id)
    if not row:
        raise NotificationDraftNotFoundError(
            "notification draft not found"
        )
    if clean_cell_value(row.get("session_id")) != clean_cell_value(
        session.get("session_id")
    ):
        raise NotificationDraftNotFoundError(
            "notification draft not found"
        )
    return public_notification_draft(row)


def list_notification_drafts(
    *,
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
    checklist_key: str = "",
    item_id: str = "",
    include_cancelled: bool = False,
) -> list[dict]:
    session = get_edit_session_for_actor(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )

    clauses = ["session_id = ?"]
    values: list[Any] = [session.get("session_id") or session_id]

    normalized_checklist_key = (
        normalize_checklist_key(checklist_key)
        if clean_cell_value(checklist_key)
        else ""
    )
    normalized_item_id = clean_cell_value(item_id)

    if normalized_checklist_key:
        clauses.append("checklist_key = ?")
        values.append(normalized_checklist_key)
    if normalized_item_id:
        clauses.append("item_id = ?")
        values.append(normalized_item_id)
    if not include_cancelled:
        clauses.append("status <> 'cancelled'")

    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM notification_drafts WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at ASC, draft_id ASC",
            values,
        ).fetchall()
        return [public_notification_draft(dict(row)) for row in rows]
    finally:
        conn.close()


def _replace_draft_files_in_transaction(
    conn,
    *,
    draft_id: str,
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    selected_documents: list[dict],
    now: str,
) -> None:
    conn.execute(
        "DELETE FROM notification_files WHERE draft_id = ?",
        (draft_id,),
    )

    for document in selected_documents:
        conn.execute("""
            INSERT INTO notification_files(
                notification_file_id,
                draft_id,
                session_id,
                dialog_id,
                checklist_key,
                item_id,
                series_id,
                document_id,
                file_name,
                file_size,
                file_url,
                sort_order,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            uuid.uuid4().hex,
            draft_id,
            session_id,
            dialog_id,
            checklist_key,
            item_id,
            document.get("seriesId") or "",
            document.get("documentId") or "",
            document.get("fileName") or "",
            int(document.get("fileSize") or 0),
            document.get("fileUrl") or "",
            int(document.get("position") or 0),
            now,
            now,
        ))


def _replace_recipient_in_transaction(
    conn,
    *,
    draft_id: str,
    session_id: str,
    recipient: dict | None,
    now: str,
) -> None:
    conn.execute(
        "DELETE FROM notification_recipients WHERE draft_id = ?",
        (draft_id,),
    )

    if recipient is None:
        return

    conn.execute("""
        INSERT INTO notification_recipients(
            recipient_id,
            draft_id,
            session_id,
            recipient_type,
            bitrix_user_id,
            display_name,
            phone,
            email,
            contact_details,
            company_id,
            curator_user_id,
            curator_name,
            metadata_json,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        uuid.uuid4().hex,
        draft_id,
        session_id,
        recipient.get("type") or "internal",
        recipient.get("bitrixUserId") or "",
        recipient.get("name") or "",
        recipient.get("phone") or "",
        recipient.get("email") or "",
        recipient.get("contactDetails") or "",
        recipient.get("companyId") or "",
        recipient.get("curatorUserId") or "",
        recipient.get("curatorName") or "",
        _json_dumps(recipient.get("metadata") or {}),
        now,
        now,
    ))


def create_notification_draft(
    *,
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    user_id: str = "",
    user_name: str = "",
    sender: dict | None = None,
    recipient: dict | None = None,
    assignment_part_id: str = "",
    assignment_part_text: str = "",
    deadline_date: str = "",
    description: str = "",
    document_ids: list[str] | None = None,
    metadata: dict | None = None,
) -> dict:
    ownership = acquire_checklist_for_edit_session(
        session_id=session_id,
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        user_id=user_id,
        user_name=user_name,
    )

    normalized_dialog_id = ownership["dialogId"]
    normalized_checklist_key = ownership["checklistKey"]
    normalized_item_id = clean_cell_value(item_id)
    if not normalized_item_id:
        raise NotificationDraftValidationError("itemId is required")

    checklist_data = get_checklist(
        normalized_dialog_id,
        normalized_checklist_key,
    )
    item, selected_documents = _resolve_current_documents(
        checklist_data=checklist_data,
        item_id=normalized_item_id,
        document_ids=_normalize_document_id_list(document_ids),
    )

    normalized_sender = _normalize_sender(sender or {})
    normalized_recipient = _attach_project_curator(
        _normalize_recipient(recipient),
        dialog_id=normalized_dialog_id,
    )
    normalized_deadline = _normalize_deadline_date(deadline_date)
    now = utc_now_iso()
    draft_id = uuid.uuid4().hex

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        resolved_assignment_part = resolve_assignment_part_in_transaction(
            conn,
            assignment_part_id=assignment_part_id,
            assignment_part_text=assignment_part_text,
            user_id=ownership.get("userId") or user_id,
            user_name=ownership.get("userName") or user_name,
            mark_used=bool(
                clean_cell_value(assignment_part_id)
                or clean_cell_value(assignment_part_text)
            ),
        )
        normalized_recipient = _resolve_recipient_in_transaction(
            conn,
            normalized_recipient,
            mark_used=True,
        )
        conn.execute("""
            INSERT INTO notification_drafts(
                draft_id,
                session_id,
                dialog_id,
                checklist_key,
                item_id,
                item_name,
                group_id,
                status,
                version,
                sender_user_id,
                sender_name,
                assignment_part_id,
                assignment_part_text,
                deadline_date,
                description,
                metadata_json,
                created_by_id,
                created_by_name,
                created_at,
                updated_at,
                committed_at,
                cancelled_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?,
                'draft', 1,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ''
            )
        """, (
            draft_id,
            ownership["sessionId"],
            normalized_dialog_id,
            normalized_checklist_key,
            normalized_item_id,
            clean_cell_value(item.get("name")),
            int(item.get("group") or item.get("groupId") or 0),
            normalized_sender.get("userId") or "",
            normalized_sender.get("name") or "",
            resolved_assignment_part.get("id") or "",
            resolved_assignment_part.get("text") or "",
            normalized_deadline,
            clean_cell_value(description),
            _json_dumps(metadata or {}),
            ownership.get("userId") or "",
            ownership.get("userName") or "",
            now,
            now,
        ))
        _replace_draft_files_in_transaction(
            conn,
            draft_id=draft_id,
            session_id=ownership["sessionId"],
            dialog_id=normalized_dialog_id,
            checklist_key=normalized_checklist_key,
            item_id=normalized_item_id,
            selected_documents=selected_documents,
            now=now,
        )
        _replace_recipient_in_transaction(
            conn,
            draft_id=draft_id,
            session_id=ownership["sessionId"],
            recipient=normalized_recipient,
            now=now,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    row = _draft_row(draft_id)
    if not row:
        raise RuntimeError("notification draft insert failed")
    return public_notification_draft(row)


def update_notification_draft(
    *,
    draft_id: str,
    session_id: str,
    user_id: str = "",
    user_name: str = "",
    expected_version: int | None = None,
    updates: dict,
) -> dict:
    row = _draft_row(draft_id)
    if not row:
        raise NotificationDraftNotFoundError(
            "notification draft not found"
        )
    if clean_cell_value(row.get("session_id")) != clean_cell_value(session_id):
        raise NotificationDraftNotFoundError(
            "notification draft not found"
        )
    if clean_cell_value(row.get("status")) not in DRAFT_MUTABLE_STATUSES:
        raise NotificationDraftConflictError(
            "notification draft is no longer editable"
        )

    ownership = acquire_checklist_for_edit_session(
        session_id=session_id,
        dialog_id=row.get("dialog_id") or "",
        checklist_key=row.get("checklist_key") or "",
        user_id=user_id,
        user_name=user_name,
    )

    current_version = int(row.get("version") or 1)
    if expected_version is not None and int(expected_version) != current_version:
        raise NotificationDraftConflictError(
            "notification draft version conflict"
        )

    checklist_data = get_checklist(
        row.get("dialog_id") or "",
        row.get("checklist_key") or "",
    )
    item = _find_item(checklist_data, row.get("item_id") or "")

    selected_documents: list[dict] | None = None
    requested_ids = _requested_document_ids(updates)
    if requested_ids is not None:
        _, selected_documents = _resolve_current_documents(
            checklist_data=checklist_data,
            item_id=row.get("item_id") or "",
            document_ids=requested_ids,
        )

    normalized_sender = None
    if "sender" in updates:
        normalized_sender = _normalize_sender(updates.get("sender") or {})

    normalized_recipient_marker = object()
    normalized_recipient: dict | None | object = normalized_recipient_marker
    if "recipient" in updates:
        normalized_recipient = _attach_project_curator(
            _normalize_recipient(updates.get("recipient")),
            dialog_id=row.get("dialog_id") or "",
        )

    deadline_marker = object()
    normalized_deadline: str | object = deadline_marker
    if "deadlineDate" in updates or "deadline" in updates:
        normalized_deadline = _normalize_deadline_date(
            updates.get("deadlineDate")
            if "deadlineDate" in updates
            else updates.get("deadline")
        )

    assignments = [
        "item_name = ?",
        "group_id = ?",
        "version = version + 1",
        "updated_at = ?",
    ]
    values: list[Any] = [
        clean_cell_value(item.get("name")),
        int(item.get("group") or item.get("groupId") or 0),
        utc_now_iso(),
    ]

    if normalized_sender is not None:
        assignments.extend(["sender_user_id = ?", "sender_name = ?"])
        values.extend([
            normalized_sender.get("userId") or "",
            normalized_sender.get("name") or "",
        ])
    assignment_part_marker = object()
    requested_assignment_part: dict | object = assignment_part_marker
    if "assignmentPart" in updates:
        part = updates.get("assignmentPart")
        if isinstance(part, dict):
            requested_assignment_part = {
                "id": clean_cell_value(
                    part.get("id") or part.get("assignmentPartId")
                ),
                "text": clean_cell_value(
                    part.get("text") or part.get("name")
                ),
            }
        else:
            requested_assignment_part = {
                "id": "",
                "text": clean_cell_value(part),
            }
    elif "assignmentPartId" in updates or "assignmentPartText" in updates:
        requested_assignment_part = {
            "id": clean_cell_value(updates.get("assignmentPartId")),
            "text": clean_cell_value(updates.get("assignmentPartText")),
        }
    if normalized_deadline is not deadline_marker:
        assignments.append("deadline_date = ?")
        values.append(normalized_deadline)
    if "description" in updates:
        assignments.append("description = ?")
        values.append(clean_cell_value(updates.get("description")))
    if "metadata" in updates:
        metadata = updates.get("metadata")
        assignments.append("metadata_json = ?")
        values.append(_json_dumps(metadata if isinstance(metadata, dict) else {}))

    now = values[2]

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        if normalized_recipient is not normalized_recipient_marker:
            normalized_recipient = _resolve_recipient_in_transaction(
                conn,
                normalized_recipient,
                mark_used=True,
            )
        if requested_assignment_part is not assignment_part_marker:
            resolved_assignment_part = resolve_assignment_part_in_transaction(
                conn,
                assignment_part_id=requested_assignment_part.get("id") or "",
                assignment_part_text=requested_assignment_part.get("text") or "",
                user_id=ownership.get("userId") or user_id,
                user_name=ownership.get("userName") or user_name,
                mark_used=False,
            )
            if (
                resolved_assignment_part.get("id")
                and resolved_assignment_part.get("id")
                != clean_cell_value(row.get("assignment_part_id"))
            ):
                resolved_assignment_part = resolve_assignment_part_in_transaction(
                    conn,
                    assignment_part_id=resolved_assignment_part.get("id") or "",
                    assignment_part_text=resolved_assignment_part.get("text") or "",
                    user_id=ownership.get("userId") or user_id,
                    user_name=ownership.get("userName") or user_name,
                    mark_used=True,
                )
            assignments.extend([
                "assignment_part_id = ?",
                "assignment_part_text = ?",
            ])
            values.extend([
                resolved_assignment_part.get("id") or "",
                resolved_assignment_part.get("text") or "",
            ])

        statement_values = [*values, draft_id, current_version]
        cur = conn.execute(
            "UPDATE notification_drafts SET "
            + ", ".join(assignments)
            + " WHERE draft_id = ? AND version = ? AND status = 'draft'",
            statement_values,
        )
        if int(cur.rowcount or 0) != 1:
            raise NotificationDraftConflictError(
                "notification draft version conflict"
            )

        if selected_documents is not None:
            _replace_draft_files_in_transaction(
                conn,
                draft_id=draft_id,
                session_id=ownership["sessionId"],
                dialog_id=row.get("dialog_id") or "",
                checklist_key=row.get("checklist_key") or "",
                item_id=row.get("item_id") or "",
                selected_documents=selected_documents,
                now=now,
            )
        if normalized_recipient is not normalized_recipient_marker:
            _replace_recipient_in_transaction(
                conn,
                draft_id=draft_id,
                session_id=ownership["sessionId"],
                recipient=normalized_recipient,
                now=now,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    updated_row = _draft_row(draft_id)
    if not updated_row:
        raise RuntimeError("notification draft update failed")
    return public_notification_draft(updated_row)


def cancel_notification_draft(
    *,
    draft_id: str,
    session_id: str,
    user_id: str = "",
    user_name: str = "",
    expected_version: int | None = None,
) -> dict:
    row = _draft_row(draft_id)
    if not row:
        raise NotificationDraftNotFoundError(
            "notification draft not found"
        )
    if clean_cell_value(row.get("session_id")) != clean_cell_value(session_id):
        raise NotificationDraftNotFoundError(
            "notification draft not found"
        )
    if clean_cell_value(row.get("status")) == DRAFT_STATUS_CANCELLED:
        return public_notification_draft(row)
    if clean_cell_value(row.get("status")) != DRAFT_STATUS_ACTIVE:
        raise NotificationDraftConflictError(
            "notification draft is no longer editable"
        )

    acquire_checklist_for_edit_session(
        session_id=session_id,
        dialog_id=row.get("dialog_id") or "",
        checklist_key=row.get("checklist_key") or "",
        user_id=user_id,
        user_name=user_name,
    )

    current_version = int(row.get("version") or 1)
    if expected_version is not None and int(expected_version) != current_version:
        raise NotificationDraftConflictError(
            "notification draft version conflict"
        )

    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute("""
            UPDATE notification_drafts
            SET status = 'cancelled',
                version = version + 1,
                cancelled_at = ?,
                updated_at = ?
            WHERE draft_id = ?
              AND version = ?
              AND status = 'draft'
        """, (now, now, draft_id, current_version))
        if int(cur.rowcount or 0) != 1:
            raise NotificationDraftConflictError(
                "notification draft version conflict"
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    cancelled_row = _draft_row(draft_id)
    if not cancelled_row:
        raise RuntimeError("notification draft cancel failed")
    return public_notification_draft(cancelled_row)


def mark_notification_drafts_committed_in_transaction(
    conn,
    *,
    session_id: str,
    now: str,
) -> int:
    cur = conn.execute("""
        UPDATE notification_drafts
        SET status = 'committed',
            committed_at = ?,
            updated_at = ?
        WHERE session_id = ?
          AND status = 'draft'
    """, (now, now, clean_cell_value(session_id)))
    return int(cur.rowcount or 0)


def mark_notification_drafts_cancelled_in_transaction(
    conn,
    *,
    session_id: str,
    now: str,
) -> int:
    cur = conn.execute("""
        UPDATE notification_drafts
        SET status = 'cancelled',
            cancelled_at = ?,
            updated_at = ?
        WHERE session_id = ?
          AND status = 'draft'
    """, (now, now, clean_cell_value(session_id)))
    return int(cur.rowcount or 0)
