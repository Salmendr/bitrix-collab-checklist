from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.settings import N8N_SHARED_TOKEN

from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
)

from app.checklists.storage import (
    save_project_storage_context,
    get_project_storage_context,
)


router = APIRouter()


@router.post("/api/integrations/n8n/project-storage-context")
async def api_project_storage_context(request: Request):
    expected_token = N8N_SHARED_TOKEN
    provided_token = (request.headers.get("X-N8N-Token") or "").strip()

    if expected_token and provided_token != expected_token:
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    project_name = clean_cell_value(payload.get("projectName"))
    yandex_disk = payload.get("yandexDisk") or {}
    item_mappings = payload.get("itemMappings") or []

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not project_name:
        return JSONResponse({"ok": False, "error": "projectName is required"}, status_code=400)

    if not isinstance(yandex_disk, dict) or not yandex_disk:
        return JSONResponse({"ok": False, "error": "yandexDisk is required"}, status_code=400)

    if not isinstance(item_mappings, list):
        return JSONResponse({"ok": False, "error": "itemMappings must be a list"}, status_code=400)

    try:
        save_project_storage_context(dialog_id, payload)
    except Exception as e:
        return JSONResponse({
            "ok": False,
            "error": "failed to save storage context",
            "details": str(e),
        }, status_code=500)

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "projectId": str(payload.get("projectId") or "").strip(),
        "projectName": project_name,
        "provider": str(yandex_disk.get("provider") or "yandex_disk").strip(),
        "stored": True,
        "mappingCount": len(item_mappings),
    })


@router.get("/api/integrations/n8n/project-storage-context")
def api_get_project_storage_context(dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    context = get_project_storage_context(dialog_id)

    if not context:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)

    return JSONResponse({
        "ok": True,
        "context": context,
    })