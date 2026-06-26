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
)

from app.checklists.yandex_folders import (
    ensure_project_standard_yandex_folder_structure,
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

    if not is_yandex_disk_enabled():
        return JSONResponse({
            "ok": True,
            "path": normalized_root_path,
            "url": clean_cell_value(yandex_disk.get("projectRootUrl")),
            "fromCache": False,
            "yandexDisabled": True,
            "standardFoldersPrepared": False,
        })

    prepare_result = ensure_project_standard_yandex_folder_structure(dialog_id)

    refreshed_context = get_project_storage_context(dialog_id) or context
    refreshed_yandex_disk = refreshed_context.get("yandexDisk") or {}

    project_root_url = clean_cell_value(
        prepare_result.get("projectRootUrl")
        or refreshed_yandex_disk.get("projectRootUrl")
        or yandex_disk.get("projectRootUrl")
    )

    if not prepare_result.get("ok"):
        return JSONResponse({
            "ok": False,
            "error": "failed to prepare yandex folder structure",
            "details": prepare_result,
            "path": normalized_root_path,
            "url": project_root_url,
            "standardFoldersPrepared": False,
        }, status_code=500)

    return JSONResponse({
        "ok": True,
        "path": clean_cell_value(prepare_result.get("projectRootPath")) or normalized_root_path,
        "url": project_root_url,
        "fromCache": bool(prepare_result.get("prepared") == 0),
        "standardFoldersPrepared": bool(prepare_result.get("standardFoldersPrepared")),
        "standardFoldersPreparedAt": prepare_result.get("standardFoldersPreparedAt"),
        "foldersCount": prepare_result.get("foldersCount", 0),
        "prepared": prepare_result.get("prepared", 0),
        "skipped": prepare_result.get("skipped", 0),
        "failed": prepare_result.get("failed", 0),
    })