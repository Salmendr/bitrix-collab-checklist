"""Bitrix24 object of the project: checklist header API."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from app.logging_utils import write_debug_log
from app.checklists import project_objects
from app.checklists.utils import clean_cell_value, normalize_dialog_id


router = APIRouter()


def checklist_object_payload(dialog_id: str) -> dict:
    return project_objects.checklist_object_views(dialog_id)


@router.get("/api/checklist/project-object")
async def api_project_object(dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)
    if project_objects.needs_auto_fetch(dialog_id):
        # The object id came without its data (older projects): read it once.
        try:
            await run_in_threadpool(
                project_objects.refresh_project_objects,
                dialog_id,
                source="popup_first_open",
                if_needed=True,
            )
        except Exception as exc:
            write_debug_log("project_object_auto_fetch_failed", {"dialogId": dialog_id, "error": str(exc)})
    payload = await run_in_threadpool(checklist_object_payload, dialog_id)
    return JSONResponse({"ok": True, **payload})


@router.post("/api/checklist/project-object/choose")
async def api_project_object_choose(request: Request):
    payload = await request.json()
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    user_id = clean_cell_value(payload.get("actingUserId"))
    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)
    try:
        project_objects.choose_value(
            dialog_id,
            clean_cell_value(payload.get("field")),
            payload.get("value"),
            acting_user_id=user_id,
        )
    except project_objects.ProjectObjectError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=403)
    return JSONResponse({"ok": True, **checklist_object_payload(dialog_id)})
