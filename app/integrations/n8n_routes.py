from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse


from app.settings import N8N_SHARED_TOKEN

from app.checklists.utils import clean_cell_value, normalize_dialog_id

from app.checklists.storage import (
    get_project_storage_context,
    save_project_storage_context,
)


router = APIRouter()

def verify_n8n_token(request: Request):
    expected_token = str(N8N_SHARED_TOKEN or "").strip()

    # Если токен не задан в окружении — не блокируем локальную разработку.
    if not expected_token:
        return

    auth_header = str(request.headers.get("authorization") or "").strip()
    bearer_token = ""

    if auth_header.lower().startswith("bearer "):
        bearer_token = auth_header[7:].strip()

    provided_token = (
        str(request.headers.get("x-n8n-token") or "").strip()
        or bearer_token
        or str(request.query_params.get("token") or "").strip()
    )

    if provided_token != expected_token:
        raise HTTPException(status_code=401, detail="Invalid n8n token")


@router.post("/api/integrations/n8n/project-storage-context")
def api_save_project_storage_context(payload: dict, request: Request):
    verify_n8n_token(request)

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
def api_get_project_storage_context(request: Request, dialogId: str = ""):
    verify_n8n_token(request)

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