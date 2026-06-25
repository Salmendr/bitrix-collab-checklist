from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
)

from app.checklists.storage import (
    get_project_storage_context,
    save_project_storage_context,
)

from app.yandex_disk.client import (
    is_yandex_disk_enabled,
    normalize_yandex_disk_path,
    yandex_disk_ensure_folder,
    yandex_disk_publish_path,
    yandex_disk_get_resource_meta,
)


router = APIRouter()


@router.get("/api/project-root-folder")
def api_project_root_folder(dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    context = get_project_storage_context(dialog_id)
    if not context:
        return JSONResponse({"ok": False, "error": "project storage context not found"}, status_code=404)

    yandex_disk = context.get("yandexDisk") or {}
    project_root_path = clean_cell_value(yandex_disk.get("projectRootPath"))
    if not project_root_path:
        return JSONResponse({"ok": False, "error": "projectRootPath is empty"}, status_code=400)

    normalized_root_path = normalize_yandex_disk_path(project_root_path)
    project_root_url = clean_cell_value(yandex_disk.get("projectRootUrl"))

    if project_root_url:
        return JSONResponse({
            "ok": True,
            "path": normalized_root_path,
            "url": project_root_url,
            "fromCache": True,
        })

    if not is_yandex_disk_enabled():
        return JSONResponse({
            "ok": True,
            "path": normalized_root_path,
            "url": "",
            "fromCache": False,
            "yandexDisabled": True,
        })

    try:
        yandex_disk_ensure_folder(normalized_root_path)
        yandex_disk_publish_path(normalized_root_path)
        meta = yandex_disk_get_resource_meta(normalized_root_path)

        project_root_url = clean_cell_value(meta.get("public_url"))

        if project_root_url:
            yandex_disk["projectRootUrl"] = project_root_url

            save_project_storage_context(dialog_id, {
                "dialogId": dialog_id,
                "projectId": context.get("projectId") or "",
                "projectName": context.get("projectName") or "",
                "storageMode": context.get("storageMode") or {},
                "yandexDisk": yandex_disk,
                "itemMappings": context.get("itemMappings") or [],
            })

        return JSONResponse({
            "ok": True,
            "path": clean_cell_value(meta.get("path")) or normalized_root_path,
            "url": project_root_url,
            "fromCache": False,
        })

    except Exception as e:
        return JSONResponse({
            "ok": False,
            "error": "failed to resolve project root folder url",
            "details": str(e),
            "path": normalized_root_path,
            "url": "",
        }, status_code=500)