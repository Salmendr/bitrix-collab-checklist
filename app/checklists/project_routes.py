from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse
from app.logging_utils import write_debug_log

from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
)

from app.checklists.storage import (
    get_project_storage_context,
)

from app.yandex_disk.client import (
    is_yandex_disk_enabled,
    normalize_yandex_disk_path,
)

from app.checklists.yandex_folders import (
    ensure_project_yandex_root_folder,
    run_project_yandex_folder_warmup,
)


router = APIRouter()


@router.get("/api/project-root-folder")
def api_project_root_folder(background_tasks: BackgroundTasks, dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)

    write_debug_log("yandex_warmup_route_received", {
        "rawDialogId": dialogId,
        "dialogId": dialog_id,
    })
    if not dialog_id:
        write_debug_log("yandex_warmup_route_rejected", {
            "rawDialogId": dialogId,
            "dialogId": dialog_id,
            "reason": "dialogId is required",
        })
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    context = get_project_storage_context(dialog_id)
    if not context:
        write_debug_log("yandex_warmup_route_rejected", {
            "rawDialogId": dialogId,
            "dialogId": dialog_id,
            "reason": "project storage context not found",
        })
        return JSONResponse({"ok": False, "error": "project storage context not found"}, status_code=404)

    yandex_disk = context.get("yandexDisk") or {}
    project_root_path = clean_cell_value(yandex_disk.get("projectRootPath"))
    if not project_root_path:
        write_debug_log("yandex_warmup_route_rejected", {
            "dialogId": dialog_id,
            "reason": "projectRootPath is empty",
            "contextExists": True,
        })
        return JSONResponse({"ok": False, "error": "projectRootPath is empty"}, status_code=400)

    normalized_root_path = normalize_yandex_disk_path(project_root_path)

    if not is_yandex_disk_enabled():
        write_debug_log("yandex_warmup_route_skipped", {
            "dialogId": dialog_id,
            "reason": "yandex disk is disabled",
            "path": normalized_root_path,
        })

        return JSONResponse({
            "ok": True,
            "path": normalized_root_path,
            "url": clean_cell_value(yandex_disk.get("projectRootUrl")),
            "fromCache": False,
            "yandexDisabled": True,
            "standardFoldersPrepared": False,
        })

    root_result = ensure_project_yandex_root_folder(dialog_id)

    if not root_result.get("ok"):
        write_debug_log("yandex_root_route_failed", {
            "dialogId": dialog_id,
            "path": normalized_root_path,
            "rootResult": root_result,
        })

        return JSONResponse({
            "ok": False,
            "error": "failed to prepare yandex project root folder",
            "details": root_result,
            "path": normalized_root_path,
            "url": clean_cell_value(yandex_disk.get("projectRootUrl")),
            "standardFoldersPrepared": False,
        }, status_code=500)

    background_tasks.add_task(run_project_yandex_folder_warmup, dialog_id)

    refreshed_context = get_project_storage_context(dialog_id) or context
    refreshed_yandex_disk = refreshed_context.get("yandexDisk") or {}

    project_root_url = clean_cell_value(
        root_result.get("url")
        or refreshed_yandex_disk.get("projectRootUrl")
        or yandex_disk.get("projectRootUrl")
    )

    write_debug_log("yandex_root_route_completed", {
        "dialogId": dialog_id,
        "path": clean_cell_value(root_result.get("path")) or normalized_root_path,
        "urlExists": bool(project_root_url),
        "standardFoldersPrepared": bool(refreshed_yandex_disk.get("standardFoldersPrepared")),
    })

    return JSONResponse({
        "ok": True,
        "path": clean_cell_value(root_result.get("path")) or normalized_root_path,
        "url": project_root_url,
        "fromCache": bool(root_result.get("fromCache")),
        "rootReady": bool(project_root_url),
        "standardFoldersPrepared": bool(refreshed_yandex_disk.get("standardFoldersPrepared")),
        "standardFoldersPreparedAt": refreshed_yandex_disk.get("standardFoldersPreparedAt"),
        "standardFoldersPreparedCount": int(refreshed_yandex_disk.get("standardFoldersPreparedCount") or 0),
        "yandexWarmupQueued": True,
    })