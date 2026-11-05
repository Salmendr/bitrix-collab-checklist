"""Исполнители пунктов и разделов: API of the checklist window and of the
folder page of an item (see assignments.py)."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from app.auth.identity import is_read_only
from app.checklists import assignments
from app.checklists.edit_sessions import (
    EditSessionConflictError,
    EditSessionNotFoundError,
    EditSessionPermissionError,
)
from app.checklists.utils import clean_cell_value, normalize_checklist_key, normalize_dialog_id
from app.logging_utils import write_debug_log


router = APIRouter()

LOCKED_PREFIX = "checklist is locked by another edit session: "


def _error(exc: Exception) -> JSONResponse:
    message = str(exc)
    if isinstance(exc, (assignments.AssignmentDenied, EditSessionPermissionError)):
        status = 403
    elif isinstance(exc, EditSessionNotFoundError):
        status, message = 404, "Сессия редактирования не найдена — откройте чек-лист заново"
    elif isinstance(exc, EditSessionConflictError):
        status = 409
        if message.startswith(LOCKED_PREFIX):
            message = "Чек-лист сейчас редактирует " + message[len(LOCKED_PREFIX):]
        elif message == "edit session is not active":
            message = "Сессия редактирования уже завершена"
    elif isinstance(exc, assignments.AssignmentError):
        status = 400
    else:
        raise exc
    return JSONResponse({"ok": False, "error": message}, status_code=status)


@router.get("/api/assignments")
async def api_assignments(request: Request, dialogId: str = "", checklistKey: str = "", userId: str = "",
                          sessionId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)
    user_id = "" if is_read_only(request) else clean_cell_value(userId)
    payload = await run_in_threadpool(
        assignments.state, dialog_id, normalize_checklist_key(checklistKey), user_id,
        "" if is_read_only(request) else clean_cell_value(sessionId),
    )
    return JSONResponse(payload)


@router.post("/api/assignments/change")
async def api_assignments_change(request: Request):
    payload = await request.json()
    if not isinstance(payload, dict):
        return JSONResponse({"ok": False, "error": "bad request"}, status_code=400)
    actor = {"userId": clean_cell_value(payload.get("actingUserId")),
             "userName": clean_cell_value(payload.get("actingUserName"))}
    try:
        result = await run_in_threadpool(
            assignments.change,
            clean_cell_value(payload.get("dialogId")),
            clean_cell_value(payload.get("checklistKey")),
            clean_cell_value(payload.get("sessionId")),
            actor,
            payload,
        )
    except Exception as exc:
        write_debug_log("assign_change_refused", {
            "dialogId": clean_cell_value(payload.get("dialogId")),
            "checklistKey": clean_cell_value(payload.get("checklistKey")),
            "target": clean_cell_value(payload.get("kind")) + ":" + clean_cell_value(payload.get("targetId")),
            "op": clean_cell_value(payload.get("op")), "by": actor["userId"], "error": str(exc)[:300],
        })
        return _error(exc)
    return JSONResponse(result)


@router.get("/api/assignments/journal")
async def api_assignments_journal(dialogId: str = "", checklistKey: str = "", itemId: str = ""):
    rows = await run_in_threadpool(assignments.journal, dialogId, normalize_checklist_key(checklistKey),
                                   clean_cell_value(itemId))
    return JSONResponse({"ok": True, "journal": rows})
