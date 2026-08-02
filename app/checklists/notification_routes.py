from __future__ import annotations

import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.checklists.edit_sessions import (
    EditSessionConflictError,
    EditSessionNotFoundError,
    EditSessionPermissionError,
)
from app.checklists.notification_drafts import (
    NotificationDraftConflictError,
    NotificationDraftNotFoundError,
    NotificationDraftValidationError,
    cancel_notification_draft,
    create_notification_draft,
    get_notification_draft_for_actor,
    list_notification_drafts,
    update_notification_draft,
)
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


router = APIRouter()


async def _read_json_payload(request: Request) -> dict:
    try:
        payload = await request.json()
    except Exception:
        raw = await request.body()
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            payload = {}
    return payload if isinstance(payload, dict) else {}


def _error_response(exc: Exception) -> JSONResponse:
    if isinstance(exc, (NotificationDraftNotFoundError, EditSessionNotFoundError)):
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=404)
    if isinstance(exc, EditSessionPermissionError):
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=403)
    if isinstance(exc, (NotificationDraftConflictError, EditSessionConflictError)):
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=409)
    if isinstance(exc, (NotificationDraftValidationError, ValueError)):
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


@router.post("/api/checklist/notification-drafts")
async def api_create_notification_draft(request: Request):
    payload = await _read_json_payload(request)
    try:
        document_ids = payload.get("documentIds")
        if document_ids is None and "files" in payload:
            document_ids = []
            for raw_file in payload.get("files") or []:
                if isinstance(raw_file, dict):
                    document_ids.append(
                        raw_file.get("documentId") or raw_file.get("id")
                    )
                else:
                    document_ids.append(raw_file)

        assignment_part = payload.get("assignmentPart")
        if isinstance(assignment_part, dict):
            assignment_part_id = assignment_part.get("id")
            assignment_part_text = assignment_part.get("text") or assignment_part.get("name")
        else:
            assignment_part_id = payload.get("assignmentPartId")
            assignment_part_text = (
                payload.get("assignmentPartText")
                if "assignmentPartText" in payload
                else assignment_part
            )

        if document_ids is not None and not isinstance(document_ids, list):
            raise NotificationDraftValidationError(
                "documentIds must be an array"
            )

        draft = create_notification_draft(
            session_id=clean_cell_value(payload.get("sessionId")),
            dialog_id=normalize_dialog_id(payload.get("dialogId")),
            checklist_key=normalize_checklist_key(payload.get("checklistKey")),
            item_id=clean_cell_value(payload.get("itemId")),
            user_id=clean_cell_value(payload.get("userId")),
            user_name=clean_cell_value(payload.get("userName")),
            sender=payload.get("sender") if isinstance(payload.get("sender"), dict) else {},
            recipient=payload.get("recipient"),
            assignment_part_id=clean_cell_value(assignment_part_id),
            assignment_part_text=clean_cell_value(assignment_part_text),
            deadline_date=clean_cell_value(
                payload.get("deadlineDate") or payload.get("deadline")
            ),
            description=clean_cell_value(payload.get("description")),
            document_ids=(
                list(document_ids or [])
                if isinstance(document_ids, list)
                else []
            ),
            metadata=(
                payload.get("metadata")
                if isinstance(payload.get("metadata"), dict)
                else {}
            ),
        )
        return JSONResponse({"ok": True, "draft": draft}, status_code=201)
    except Exception as exc:
        return _error_response(exc)


@router.get("/api/checklist/notification-drafts")
async def api_list_notification_drafts(request: Request):
    try:
        drafts = list_notification_drafts(
            session_id=clean_cell_value(request.query_params.get("sessionId")),
            dialog_id=normalize_dialog_id(request.query_params.get("dialogId")),
            user_id=clean_cell_value(request.query_params.get("userId")),
            checklist_key=clean_cell_value(request.query_params.get("checklistKey")),
            item_id=clean_cell_value(request.query_params.get("itemId")),
            include_cancelled=(
                clean_cell_value(request.query_params.get("includeCancelled")).lower()
                in {"1", "true", "yes"}
            ),
        )
        return JSONResponse({"ok": True, "draftCount": len(drafts), "drafts": drafts})
    except Exception as exc:
        return _error_response(exc)


@router.get("/api/checklist/notification-drafts/{draft_id}")
async def api_get_notification_draft(draft_id: str, request: Request):
    try:
        draft = get_notification_draft_for_actor(
            draft_id=draft_id,
            session_id=clean_cell_value(request.query_params.get("sessionId")),
            dialog_id=normalize_dialog_id(request.query_params.get("dialogId")),
            user_id=clean_cell_value(request.query_params.get("userId")),
        )
        return JSONResponse({"ok": True, "draft": draft})
    except Exception as exc:
        return _error_response(exc)


@router.patch("/api/checklist/notification-drafts/{draft_id}")
async def api_update_notification_draft(draft_id: str, request: Request):
    payload = await _read_json_payload(request)
    try:
        expected_version = payload.get("version")
        draft = update_notification_draft(
            draft_id=draft_id,
            session_id=clean_cell_value(payload.get("sessionId")),
            user_id=clean_cell_value(payload.get("userId")),
            user_name=clean_cell_value(payload.get("userName")),
            expected_version=(
                int(expected_version)
                if expected_version not in {None, ""}
                else None
            ),
            updates=payload,
        )
        return JSONResponse({"ok": True, "draft": draft})
    except Exception as exc:
        return _error_response(exc)


@router.delete("/api/checklist/notification-drafts/{draft_id}")
async def api_delete_notification_draft(draft_id: str, request: Request):
    payload = await _read_json_payload(request)
    try:
        expected_version = payload.get("version")
        draft = cancel_notification_draft(
            draft_id=draft_id,
            session_id=clean_cell_value(
                payload.get("sessionId")
                or request.query_params.get("sessionId")
            ),
            user_id=clean_cell_value(
                payload.get("userId")
                or request.query_params.get("userId")
            ),
            user_name=clean_cell_value(payload.get("userName")),
            expected_version=(
                int(expected_version)
                if expected_version not in {None, ""}
                else None
            ),
        )
        return JSONResponse({"ok": True, "draft": draft})
    except Exception as exc:
        return _error_response(exc)
