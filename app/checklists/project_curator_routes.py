from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.checklists.project_curator import (
    ProjectCuratorError,
    get_project_curator,
    resolve_project_curator,
)
from app.checklists.utils import clean_cell_value, normalize_dialog_id


router = APIRouter()


def _force(value) -> bool:
    return clean_cell_value(value).lower() in {"1", "true", "yes", "force"}


@router.get("/api/checklist/project-curator")
async def api_get_project_curator(request: Request):
    dialog_id = normalize_dialog_id(request.query_params.get("dialogId"))
    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    refresh = clean_cell_value(request.query_params.get("refresh")).lower()
    try:
        if refresh in {"auto", "1", "true", "force"}:
            curator = resolve_project_curator(
                dialog_id,
                force=refresh == "force",
                allow_cached=refresh != "force",
            )
        else:
            curator = get_project_curator(dialog_id)
        return JSONResponse({"ok": True, "projectCurator": curator})
    except ProjectCuratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=404)
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


@router.post("/api/checklist/project-curator/refresh")
async def api_refresh_project_curator(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    payload = payload if isinstance(payload, dict) else {}
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)
    try:
        curator = resolve_project_curator(dialog_id, force=True, allow_cached=True)
        return JSONResponse({"ok": True, "projectCurator": curator})
    except ProjectCuratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=404)
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
