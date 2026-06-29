from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse


from app.settings import N8N_SHARED_TOKEN

from app.checklists.utils import clean_cell_value, normalize_dialog_id

from app.checklists.storage import (
    get_project_storage_context,
    save_project_storage_context,
)
from app.logging_utils import write_debug_log

from app.checklists.yandex_folders import (
    run_project_yandex_folder_warmup,
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
def api_save_project_storage_context(
    request: Request,
    payload: dict,
    background_tasks: BackgroundTasks,
):
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

    write_debug_log("n8n_project_context_saved", {
        "dialogId": dialog_id,
        "projectId": project_id,
        "projectName": project_name,
        "projectRootPath": clean_cell_value(yandex_disk.get("projectRootPath")),
        "mirrorTargets": storage_mode.get("mirrorTargets") if isinstance(storage_mode, dict) else [],
    })

    background_tasks.add_task(run_project_yandex_folder_warmup_safe, dialog_id)

    context = get_project_storage_context(dialog_id)

    return {
        "ok": True,
        "dialogId": dialog_id,
        "projectId": project_id,
        "projectName": project_name,
        "yandexWarmupQueued": True,
        "foldersCount": len((context.get("yandexDisk") or {}).get("folders") or {}) if context else 0,
        "itemMappingsCount": len(context.get("itemMappings") or []) if context else 0,
    }


@router.get("/api/integrations/n8n/project-storage-context")
def api_get_project_storage_context(request: Request, dialogId: str = ""):
    verify_n8n_token(request)

    dialog_id = normalize_dialog_id(dialogId)

    if not dialog_id:
        return JSONResponse(
            {"ok": False, "error": "dialogId is required"},
            status_code=400,
        )

    context = get_project_storage_context(dialog_id)

    if not context:
        return JSONResponse(
            {"ok": False, "error": "not found"},
            status_code=404,
        )

    return JSONResponse({
        "ok": True,
        "context": context,
    })


def run_project_yandex_folder_warmup_safe(dialog_id: str):
    dialog_id = normalize_dialog_id(dialog_id)

    if not dialog_id:
        write_debug_log("n8n_yandex_warmup_background_failed", {
            "dialogId": dialog_id,
            "error": "dialogId is required",
        })
        return

    try:
        result = run_project_yandex_folder_warmup(dialog_id)

        write_debug_log("n8n_yandex_warmup_background_completed", {
            "dialogId": dialog_id,
            "result": result,
        })

    except Exception as exc:
        write_debug_log("n8n_yandex_warmup_background_failed", {
            "dialogId": dialog_id,
            "error": str(exc),
        })