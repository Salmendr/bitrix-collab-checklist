import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.logging_utils import write_debug_log

from app.checklists.utils import (
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.checklists.edit_session_locks import (
    acquire_edit_session_lock,
    heartbeat_edit_session_lock,
    release_edit_session_lock,
)
from app.checklists.edit_sessions import (
    EditSessionConflictError,
    EditSessionNotFoundError,
    EditSessionPermissionError,
)
from app.checklists.locks import (
    acquire_checklist_lock,
    heartbeat_checklist_lock,
    release_checklist_lock,
)


router = APIRouter()


def session_lock_error_response(
    exc: Exception,
) -> JSONResponse:
    if isinstance(exc, EditSessionNotFoundError):
        status_code = 404
    elif isinstance(exc, EditSessionPermissionError):
        status_code = 403
    elif isinstance(exc, EditSessionConflictError):
        status_code = 409
    elif isinstance(exc, ValueError):
        status_code = 400
    else:
        status_code = 500

    return JSONResponse(
        {"ok": False, "error": str(exc)},
        status_code=status_code,
    )


@router.post("/api/checklist/lock/acquire")
async def api_checklist_lock_acquire(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    user_id = str(payload.get("userId") or "").strip()
    user_name = str(payload.get("userName") or "").strip()
    lock_id = str(payload.get("lockId") or "").strip()
    session_id = str(
        payload.get("sessionId") or ""
    ).strip()

    if not dialog_id:
        return JSONResponse(
            {"ok": False, "error": "dialogId is required"},
            status_code=400,
        )

    try:
        if session_id:
            result = acquire_edit_session_lock(
                session_id=session_id,
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                user_id=user_id,
                user_name=user_name,
                requested_lock_id=lock_id,
            )
        else:
            result = acquire_checklist_lock(
                dialog_id,
                checklist_key,
                user_id,
                user_name,
                lock_id,
            )
    except Exception as exc:
        return session_lock_error_response(exc)

    write_debug_log("lock_acquire", result)
    return JSONResponse(result)


@router.post("/api/checklist/lock/heartbeat")
async def api_checklist_lock_heartbeat(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    user_id = str(payload.get("userId") or "").strip()
    user_name = str(payload.get("userName") or "").strip()
    lock_id = str(payload.get("lockId") or "").strip()
    session_id = str(
        payload.get("sessionId") or ""
    ).strip()

    if not dialog_id:
        return JSONResponse(
            {"ok": False, "error": "dialogId is required"},
            status_code=400,
        )

    try:
        if session_id:
            result = heartbeat_edit_session_lock(
                session_id=session_id,
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                user_id=user_id,
                user_name=user_name,
                lock_id=lock_id,
            )
        else:
            result = heartbeat_checklist_lock(
                dialog_id,
                checklist_key,
                user_id,
                user_name,
                lock_id,
            )
    except Exception as exc:
        return session_lock_error_response(exc)

    return JSONResponse(result)


@router.post("/api/checklist/lock/release")
async def api_checklist_lock_release(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raw = await request.body()
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            payload = {
                "raw": raw.decode("utf-8", errors="ignore")
            }

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    lock_id = str(payload.get("lockId") or "").strip()
    user_id = str(payload.get("userId") or "").strip()
    session_id = str(
        payload.get("sessionId") or ""
    ).strip()

    if not dialog_id:
        return JSONResponse(
            {"ok": False, "error": "dialogId is required"},
            status_code=400,
        )

    try:
        if session_id:
            # Чистая стадия освобождается при переключении. Стадия,
            # в которой уже есть изменения текущей edit-session,
            # сохраняет lock до commit/rollback для защиты rollback.
            result = release_edit_session_lock(
                session_id=session_id,
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                user_id=user_id,
                lock_id=lock_id,
            )
        else:
            result = release_checklist_lock(
                dialog_id,
                checklist_key,
                lock_id,
                user_id,
            )
    except Exception as exc:
        return session_lock_error_response(exc)

    write_debug_log("lock_release", result)
    return JSONResponse(result)