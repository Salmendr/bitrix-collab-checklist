from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse


from app.settings import N8N_SHARED_TOKEN

from app.checklists.utils import clean_cell_value, normalize_dialog_id

from app.checklists.storage import (
    get_project_storage_context,
    save_project_storage_context,
)


router = APIRouter()


@router.post("/api/integrations/n8n/project-storage-context")
def api_save_project_storage_context(payload: dict):
    payload = dict(payload or {})

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    if not dialog_id:
        raise HTTPException(status_code=400, detail="dialogId is required")

    project_id = clean_cell_value(payload.get("projectId"))
    project_name = clean_cell_value(payload.get("projectName"))

    storage_mode = payload.get("storageMode") or {
        "localPrimary": True,
        "mirrorTargets": ["yandex_disk"],
    }

    if not isinstance(storage_mode, dict):
        storage_mode = {
            "localPrimary": True,
            "mirrorTargets": ["yandex_disk"],
        }

    yandex_disk = payload.get("yandexDisk") or {}
    if not isinstance(yandex_disk, dict):
        yandex_disk = {}

    yandex_disk.setdefault("provider", "yandex_disk")
    yandex_disk.setdefault("projectRootPath", "")
    yandex_disk.setdefault("idStageRootPath", "")
    yandex_disk.setdefault("projectRootUrl", "")
    yandex_disk.setdefault("idStageRootUrl", "")
    yandex_disk.setdefault("folders", {})

    if not isinstance(yandex_disk.get("folders"), dict):
        yandex_disk["folders"] = {}

    item_mappings = payload.get("itemMappings") or []
    if not isinstance(item_mappings, list):
        item_mappings = []

    normalized_payload = {
        "dialogId": dialog_id,
        "projectId": project_id,
        "projectName": project_name,
        "storageMode": storage_mode,
        "yandexDisk": yandex_disk,
        "itemMappings": item_mappings,
    }

    save_project_storage_context(dialog_id, normalized_payload)

    context = get_project_storage_context(dialog_id)

    return {
        "ok": True,
        "dialogId": dialog_id,
        "projectId": project_id,
        "projectName": project_name,
        "foldersCount": len((context.get("yandexDisk") or {}).get("folders") or {}) if context else 0,
        "itemMappingsCount": len(context.get("itemMappings") or []) if context else 0,
    }


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