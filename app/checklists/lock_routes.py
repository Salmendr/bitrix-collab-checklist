import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.logging_utils import write_debug_log

from app.checklists.utils import (
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.checklists.locks import (
    acquire_checklist_lock,
    heartbeat_checklist_lock,
    release_checklist_lock,
)


router = APIRouter()


@router.post("/api/checklist/lock/acquire")
async def api_checklist_lock_acquire(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    user_id = str(payload.get("userId") or "").strip()
    user_name = str(payload.get("userName") or "").strip()
    lock_id = str(payload.get("lockId") or "").strip()

    if not dialog_id:
        return JSONResponse(
            {"ok": False, "error": "dialogId is required"},
            status_code=400,
        )

    result = acquire_checklist_lock(
        dialog_id,
        checklist_key,
        user_id,
        user_name,
        lock_id,
    )

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

    if not dialog_id:
        return JSONResponse(
            {"ok": False, "error": "dialogId is required"},
            status_code=400,
        )

    result = heartbeat_checklist_lock(
        dialog_id,
        checklist_key,
        user_id,
        user_name,
        lock_id,
    )

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

    if not dialog_id:
        return JSONResponse(
            {"ok": False, "error": "dialogId is required"},
            status_code=400,
        )

    result = release_checklist_lock(
        dialog_id,
        checklist_key,
        lock_id,
        user_id,
    )

    write_debug_log("lock_release", result)
    return JSONResponse(result)