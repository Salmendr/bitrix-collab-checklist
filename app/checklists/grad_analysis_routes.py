"""Граданализ (ГА): API of the right column of the OPR checklist window."""
from __future__ import annotations

import mimetypes
from urllib.parse import quote

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from starlette.concurrency import run_in_threadpool

from app.auth.identity import is_read_only, verified_actor
from app.checklists import grad_analysis
from app.checklists.edit_sessions import (
    EditSessionConflictError,
    EditSessionNotFoundError,
    EditSessionPermissionError,
)
from app.checklists.utils import can_preview_in_browser, clean_cell_value, normalize_dialog_id


router = APIRouter()

LOCKED_PREFIX = "checklist is locked by another edit session: "


def _error(exc: Exception) -> JSONResponse:
    message = str(exc)
    if isinstance(exc, grad_analysis.GaDenied) or isinstance(exc, EditSessionPermissionError):
        status = 403
    elif isinstance(exc, EditSessionNotFoundError):
        status, message = 404, "Сессия редактирования не найдена — откройте чек-лист заново"
    elif isinstance(exc, EditSessionConflictError):
        status = 409
        if message.startswith(LOCKED_PREFIX):
            message = "Граданализ сейчас заполняет " + message[len(LOCKED_PREFIX):]
        elif message == "edit session is not active":
            message = "Сессия редактирования уже завершена"
    elif isinstance(exc, grad_analysis.GaError):
        status = 400
    else:
        raise exc
    return JSONResponse({"ok": False, "error": message}, status_code=status)


def _actor(payload: dict) -> dict:
    return {
        "userId": clean_cell_value(payload.get("actingUserId")),
        "userName": clean_cell_value(payload.get("actingUserName")),
    }


@router.get("/api/ga")
async def api_ga(request: Request, dialogId: str = "", userId: str = "", sessionId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)
    if is_read_only(request):
        # The platform portal sees the checklists only; the OPR progress
        # counts ГА for everybody.
        progress = await run_in_threadpool(grad_analysis.progress_counts, dialog_id)
        return JSONResponse({"ok": True, "visible": False, "progress": progress,
                             "rights": {"view": False, "edit": False}})
    payload = await run_in_threadpool(
        grad_analysis.state_payload, dialog_id, clean_cell_value(userId), clean_cell_value(sessionId),
    )
    return JSONResponse(payload)


@router.post("/api/ga/change")
async def api_ga_change(request: Request):
    payload = await request.json()
    try:
        result = await run_in_threadpool(
            grad_analysis.mutate,
            clean_cell_value(payload.get("dialogId")),
            clean_cell_value(payload.get("sessionId")),
            _actor(payload),
            clean_cell_value(payload.get("op")),
            payload,
        )
    except Exception as exc:
        return _error(exc)
    return JSONResponse(result)


@router.post("/api/ga/upload")
async def api_ga_upload(
    request: Request,
    dialogId: str = Form(...),
    sessionId: str = Form(""),
    itemId: str = Form(...),
    replaceFileId: str = Form(""),
    actingUserId: str = Form(""),
    actingUserName: str = Form(""),
    file: UploadFile = File(...),
):
    user_id, user_name = verified_actor(request, actingUserId, actingUserName)
    try:
        result = await run_in_threadpool(
            grad_analysis.store_upload,
            dialogId,
            sessionId,
            {"userId": user_id, "userName": user_name},
            itemId,
            file.filename or "",
            file.file,
            replaceFileId,
        )
    except Exception as exc:
        return _error(exc)
    finally:
        await file.close()
    return JSONResponse(result)


@router.get("/api/ga/file")
async def api_ga_file(dialogId: str = "", fileId: str = "", userId: str = "",
                      sessionId: str = "", download: int = 0):
    try:
        path, name = await run_in_threadpool(
            grad_analysis.file_for, dialogId, fileId, clean_cell_value(userId), clean_cell_value(sessionId),
        )
    except Exception as exc:
        return _error(exc)
    media_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
    disposition = "inline" if not download and can_preview_in_browser(name, media_type) else "attachment"
    response = FileResponse(path=str(path), media_type=media_type)
    response.headers["Content-Disposition"] = f"{disposition}; filename*=UTF-8''{quote(name)}"
    return response


@router.get("/api/ga/item")
async def api_ga_item(dialogId: str = "", itemId: str = "", userId: str = "", sessionId: str = ""):
    try:
        result = await run_in_threadpool(
            grad_analysis.item_payload, dialogId, itemId, clean_cell_value(userId), clean_cell_value(sessionId),
        )
    except Exception as exc:
        return _error(exc)
    return JSONResponse(result)


@router.get("/api/ga/id-files")
async def api_ga_id_files(dialogId: str = "", userId: str = ""):
    try:
        result = await run_in_threadpool(grad_analysis.id_files, dialogId, clean_cell_value(userId))
    except Exception as exc:
        return _error(exc)
    return JSONResponse(result)


@router.get("/api/ga/check")
async def api_ga_check(dialogId: str = "", sessionId: str = "", userId: str = ""):
    result = await run_in_threadpool(
        grad_analysis.session_check, dialogId, clean_cell_value(sessionId), clean_cell_value(userId),
    )
    return JSONResponse(result)


@router.post("/api/ga/architects")
async def api_ga_architects(request: Request):
    payload = await request.json()
    try:
        result = await run_in_threadpool(
            lambda: grad_analysis.change_architects(
                clean_cell_value(payload.get("dialogId")),
                clean_cell_value(payload.get("op")),
                user={"userId": payload.get("userId"), "name": payload.get("userName")},
                actor=_actor(payload),
            )
        )
    except Exception as exc:
        return _error(exc)
    return JSONResponse({"ok": True, "architects": result})
