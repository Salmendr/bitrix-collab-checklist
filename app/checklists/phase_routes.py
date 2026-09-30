"""Project stages and the GIP list: popup API."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.logging_utils import write_debug_log
from app.checklists import project_phases
from app.checklists.utils import clean_cell_value, normalize_dialog_id


router = APIRouter()


def _state(dialog_id: str, user_id: str) -> dict:
    summary = project_phases.phase_summary(dialog_id)
    return {
        "ok": True,
        **summary,
        "canManagePhases": project_phases.can_manage_phases(dialog_id, user_id),
        "canEditGips": project_phases.is_project_admin(user_id),
    }


@router.get("/api/checklist/project-phases")
def api_project_phases(dialogId: str = "", userId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)
    return JSONResponse(_state(dialog_id, clean_cell_value(userId)))


@router.post("/api/checklist/project-phases/add")
async def api_project_phase_add(request: Request):
    payload = await request.json()
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    user_id = clean_cell_value(payload.get("actingUserId"))
    user_name = clean_cell_value(payload.get("actingUserName")) or "Пользователь"
    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)
    from starlette.concurrency import run_in_threadpool
    try:
        # Moving stage 1 on Yandex Disk can take minutes: off the event loop.
        result = await run_in_threadpool(
            project_phases.add_phase,
            dialog_id,
            acting_user_id=user_id,
            acting_user_name=user_name,
        )
    except project_phases.PhaseError as exc:
        write_debug_log("project_phase_add_refused", {
            "dialogId": dialog_id,
            "actingUserId": user_id,
            "error": str(exc),
        })
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=409)
    except Exception as exc:
        write_debug_log("project_phase_add_failed", {
            "dialogId": dialog_id,
            "actingUserId": user_id,
            "error": str(exc),
        })
        return JSONResponse({"ok": False, "error": "Не удалось добавить этап: " + str(exc)}, status_code=500)
    return JSONResponse({**_state(dialog_id, user_id), "phase": result["phase"]})


@router.post("/api/checklist/project-gips/add")
async def api_project_gip_add(request: Request):
    payload = await request.json()
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    acting_user_id = clean_cell_value(payload.get("actingUserId"))
    try:
        project_phases.add_gip(
            dialog_id,
            user_id=clean_cell_value(payload.get("userId")),
            user_name=clean_cell_value(payload.get("userName")),
            acting_user_id=acting_user_id,
        )
    except project_phases.PhaseError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=403)
    return JSONResponse(_state(dialog_id, acting_user_id))


@router.post("/api/checklist/project-gips/remove")
async def api_project_gip_remove(request: Request):
    payload = await request.json()
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    acting_user_id = clean_cell_value(payload.get("actingUserId"))
    try:
        project_phases.remove_gip(
            dialog_id,
            user_id=clean_cell_value(payload.get("userId")),
            acting_user_id=acting_user_id,
        )
    except project_phases.PhaseError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=403)
    return JSONResponse(_state(dialog_id, acting_user_id))
