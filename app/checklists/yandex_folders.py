import re
from pathlib import Path
from datetime import datetime

from app.logging_utils import write_debug_log

from app.checklists.config import (
    get_checklist_config,
    get_standard_yandex_folder_specs,
)

from app.checklists.yandex_context import resolve_checklist_yandex_root_path

from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    slugify_folder_part,
)

from app.checklists.storage import (
    get_project_storage_context,
    save_project_storage_context,
    get_item_yandex_folder,
)

from app.checklists.yandex_warmup_control import is_yandex_warmup_stop_requested

from app.yandex_disk.client import (
    is_yandex_disk_enabled,
    normalize_yandex_disk_path,
    yandex_disk_upload_bytes,
    yandex_disk_upload_file,
    yandex_disk_ensure_folder,
    yandex_disk_publish_path,
    yandex_disk_get_resource_meta,
    yandex_disk_move_path,
)



ACTIVE_YANDEX_WARMUPS: set[str] = set()

def can_create_custom_item_yandex_folder(dialog_id: str, checklist_key: str) -> bool:
    config = get_checklist_config(checklist_key)

    if not config.allow_custom_item_group_ids:
        return False

    context = get_project_storage_context(dialog_id)
    if not context:
        return False

    storage_mode = context.get("storageMode") or {}
    mirror_targets = storage_mode.get("mirrorTargets") or []

    if "yandex_disk" not in mirror_targets:
        return False

    root_path = get_root_path_from_context(dialog_id, config.key)
    return bool(root_path)

def sanitize_yandex_folder_name(value: str) -> str:
    value = clean_cell_value(value)
    value = re.sub(r'[<>:"/\\\\|?*]+', "_", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value or "Новый пункт"


def get_folder_specs_for_checklist(checklist_key: str) -> dict:
    return get_standard_yandex_folder_specs(checklist_key)


def get_root_path_from_context(dialog_id: str, checklist_key: str) -> str:
    context = get_project_storage_context(dialog_id)
    if not context:
        return ""

    config = get_checklist_config(checklist_key)
    root_path = resolve_checklist_yandex_root_path(context, config)

    return normalize_yandex_disk_path(root_path) if root_path else ""

def split_yandex_disk_path_parts(target_path: str) -> list[str]:
    normalized_path = normalize_yandex_disk_path(target_path)
    if not normalized_path:
        return []

    if not normalized_path.startswith("disk:/"):
        return []

    raw_path = normalized_path[len("disk:/"):].strip("/")
    if not raw_path:
        return []

    return [
        part.strip()
        for part in raw_path.split("/")
        if part and part.strip()
    ]


def build_yandex_disk_path_from_parts(parts: list[str]) -> str:
    clean_parts = [
        str(part or "").strip().strip("/")
        for part in parts
        if str(part or "").strip().strip("/")
    ]

    if not clean_parts:
        return "disk:/"

    return "disk:/" + "/".join(clean_parts)


def iter_yandex_disk_folder_chain(target_path: str) -> list[str]:
    parts = split_yandex_disk_path_parts(target_path)
    result = []

    for index in range(1, len(parts) + 1):
        result.append(build_yandex_disk_path_from_parts(parts[:index]))

    return result


def ensure_yandex_folder_chain(
    target_path: str,
    ensured_folder_paths: set[str] | None = None,
) -> dict:
    normalized_path = normalize_yandex_disk_path(target_path)
    chain = iter_yandex_disk_folder_chain(normalized_path)

    if not chain:
        return {
            "ok": False,
            "path": normalized_path,
            "created": [],
            "reason": "empty yandex folder path",
        }

    created = []
    cache = ensured_folder_paths if ensured_folder_paths is not None else set()

    for folder_path in chain:
        folder_path = normalize_yandex_disk_path(folder_path)

        if folder_path in cache:
            write_debug_log("yandex_folder_chain_ensure_cached", {
                "targetPath": normalized_path,
                "folderPath": folder_path,
            })

            created.append({
                "path": folder_path,
                "alreadyExists": True,
                "cached": True,
            })
            continue

        write_debug_log("yandex_folder_chain_ensure_started", {
            "targetPath": normalized_path,
            "folderPath": folder_path,
        })

        result = yandex_disk_ensure_folder(folder_path)

        cache.add(folder_path)

        write_debug_log("yandex_folder_chain_ensure_completed", {
            "targetPath": normalized_path,
            "folderPath": folder_path,
            "alreadyExists": bool(result.get("alreadyExists")),
            "cached": False,
        })

        created.append({
            "path": folder_path,
            "alreadyExists": bool(result.get("alreadyExists")),
            "cached": False,
        })

    return {
        "ok": True,
        "path": normalized_path,
        "created": created,
        "cacheSize": len(cache),
    }

def ensure_folder_and_get_public_url(
    folder_path: str,
    ensured_folder_paths: set[str] | None = None,
    folder_meta_cache: dict[str, dict] | None = None,
) -> dict:
    folder_path = normalize_yandex_disk_path(folder_path)

    if not folder_path:
        return {
            "ok": False,
            "path": "",
            "url": "",
            "name": "",
            "reason": "empty yandex folder path",
        }

    meta_cache = folder_meta_cache if folder_meta_cache is not None else {}

    if folder_path in meta_cache:
        cached_meta = meta_cache[folder_path]

        write_debug_log("yandex_folder_public_url_cached", {
            "path": folder_path,
            "urlExists": bool(clean_cell_value(cached_meta.get("url"))),
        })

        return {
            **cached_meta,
            "fromCache": True,
        }

    chain_result = ensure_yandex_folder_chain(
        folder_path,
        ensured_folder_paths=ensured_folder_paths,
    )

    yandex_disk_publish_path(folder_path)

    meta = yandex_disk_get_resource_meta(folder_path)

    result = {
        "ok": True,
        "path": clean_cell_value(meta.get("path")) or folder_path,
        "url": clean_cell_value(meta.get("public_url")),
        "name": clean_cell_value(meta.get("name")) or folder_path.rstrip("/").rsplit("/", 1)[-1],
        "chain": chain_result.get("created") or [],
        "fromCache": False,
    }

    meta_cache[folder_path] = result

    return result

def ensure_project_yandex_root_folder(dialog_id: str) -> dict:
    write_debug_log("yandex_root_prepare_started", {
        "dialogId": dialog_id,
    })

    context = get_project_storage_context(dialog_id)
    if not context:
        write_debug_log("yandex_root_prepare_failed", {
            "dialogId": dialog_id,
            "reason": "project storage context not found",
        })

        return {
            "ok": False,
            "error": "project storage context not found",
            "path": "",
            "url": "",
        }

    storage_mode = context.get("storageMode") or {}
    mirror_targets = storage_mode.get("mirrorTargets") or []

    if "yandex_disk" not in mirror_targets:
        write_debug_log("yandex_root_prepare_skipped", {
            "dialogId": dialog_id,
            "reason": "yandex_disk is not in mirrorTargets",
            "storageMode": storage_mode,
        })

        return {
            "ok": True,
            "yandexDisabled": True,
            "reason": "yandex_disk is not in mirrorTargets",
            "path": "",
            "url": "",
        }

    if not is_yandex_disk_enabled():
        write_debug_log("yandex_root_prepare_skipped", {
            "dialogId": dialog_id,
            "reason": "yandex disk is disabled",
        })

        return {
            "ok": True,
            "yandexDisabled": True,
            "reason": "yandex disk is disabled",
            "path": "",
            "url": "",
        }

    yandex_disk = context.get("yandexDisk") or {}
    project_root_path = clean_cell_value(yandex_disk.get("projectRootPath"))
    project_root_url = clean_cell_value(yandex_disk.get("projectRootUrl"))

    if not project_root_path:
        write_debug_log("yandex_root_prepare_failed", {
            "dialogId": dialog_id,
            "reason": "projectRootPath is empty",
        })

        return {
            "ok": False,
            "error": "projectRootPath is empty",
            "path": "",
            "url": "",
        }

    if project_root_url:
        write_debug_log("yandex_root_prepare_cached", {
            "dialogId": dialog_id,
            "path": project_root_path,
            "urlExists": True,
        })

        return {
            "ok": True,
            "path": normalize_yandex_disk_path(project_root_path),
            "url": project_root_url,
            "fromCache": True,
        }

    try:
        root_meta = ensure_folder_and_get_public_url(project_root_path)

        yandex_disk["projectRootPath"] = root_meta.get("path") or normalize_yandex_disk_path(project_root_path)
        yandex_disk["projectRootUrl"] = root_meta.get("url") or project_root_url
        yandex_disk["projectRootPrepared"] = True
        yandex_disk["projectRootPreparedAt"] = datetime.now().isoformat()

        save_project_storage_context(dialog_id, {
            "dialogId": dialog_id,
            "projectId": context.get("projectId") or "",
            "projectName": context.get("projectName") or "",
            "storageMode": context.get("storageMode") or {},
            "yandexDisk": yandex_disk,
            "itemMappings": context.get("itemMappings") or [],
        })

        write_debug_log("yandex_root_prepare_completed", {
            "dialogId": dialog_id,
            "path": yandex_disk.get("projectRootPath"),
            "urlExists": bool(yandex_disk.get("projectRootUrl")),
        })

        return {
            "ok": True,
            "path": clean_cell_value(yandex_disk.get("projectRootPath")),
            "url": clean_cell_value(yandex_disk.get("projectRootUrl")),
            "fromCache": False,
        }

    except Exception as exc:
        write_debug_log("yandex_root_prepare_failed", {
            "dialogId": dialog_id,
            "path": project_root_path,
            "error": str(exc),
        })

        return {
            "ok": False,
            "error": str(exc),
            "path": project_root_path,
            "url": project_root_url,
        }


def run_project_yandex_folder_warmup(dialog_id: str) -> dict:
    dialog_id = clean_cell_value(dialog_id)

    if not dialog_id:
        return {
            "ok": False,
            "error": "dialogId is required",
        }

    if is_yandex_warmup_stop_requested(dialog_id):
        write_debug_log("yandex_background_warmup_cancelled_before_start", {
            "dialogId": dialog_id,
        })

        return {
            "ok": True,
            "cancelled": True,
            "dialogId": dialog_id,
            "reason": "stop requested before start",
        }    

    if dialog_id in ACTIVE_YANDEX_WARMUPS:
        write_debug_log("yandex_warmup_already_running", {
            "dialogId": dialog_id,
        })

        return {
            "ok": True,
            "alreadyRunning": True,
        }

    ACTIVE_YANDEX_WARMUPS.add(dialog_id)

    try:
        write_debug_log("yandex_background_warmup_started", {
            "dialogId": dialog_id,
        })

        root_result = ensure_project_yandex_root_folder(dialog_id)

        if is_yandex_warmup_stop_requested(dialog_id):
            write_debug_log("yandex_background_warmup_cancelled_after_root", {
                "dialogId": dialog_id,
                "rootResult": root_result,
            })

            return {
                "ok": True,
                "cancelled": True,
                "dialogId": dialog_id,
                "rootResult": root_result,
                "reason": "stop requested after root folder prepared",
            }

        if not root_result.get("ok"):
            write_debug_log("yandex_background_warmup_root_failed", {
                "dialogId": dialog_id,
                "rootResult": root_result,
            })
            return root_result

        result = ensure_project_standard_yandex_folder_structure(dialog_id)

        write_debug_log("yandex_background_warmup_finished", {
            "dialogId": dialog_id,
            "result": result,
        })

        return result

    finally:
        ACTIVE_YANDEX_WARMUPS.discard(dialog_id)

def ensure_project_standard_yandex_folder_structure(dialog_id: str) -> dict:
    write_debug_log("yandex_warmup_started", {
        "dialogId": dialog_id,
    })

    context = get_project_storage_context(dialog_id)
    if not context:
        write_debug_log("yandex_warmup_skipped", {
            "dialogId": dialog_id,
            "reason": "project storage context not found",
        })

        return {
            "ok": False,
            "error": "project storage context not found",
            "prepared": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
        }

    storage_mode = context.get("storageMode") or {}
    mirror_targets = storage_mode.get("mirrorTargets") or []

    if "yandex_disk" not in mirror_targets:
        write_debug_log("yandex_warmup_skipped", {
            "dialogId": dialog_id,
            "reason": "yandex_disk is not in mirrorTargets",
            "storageMode": storage_mode,
            "mirrorTargets": mirror_targets,
        })

        return {
            "ok": True,
            "yandexDisabled": True,
            "reason": "yandex_disk is not in mirrorTargets",
            "prepared": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
        }

    if not is_yandex_disk_enabled():
        write_debug_log("yandex_warmup_skipped", {
            "dialogId": dialog_id,
            "reason": "yandex disk is disabled",
        })

        return {
            "ok": True,
            "yandexDisabled": True,
            "reason": "yandex disk is disabled",
            "prepared": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
        }

    root_result = ensure_project_yandex_root_folder(dialog_id)
    if not root_result.get("ok"):
        return {
            "ok": False,
            "error": "failed to prepare project root folder",
            "rootResult": root_result,
            "prepared": 0,
            "skipped": 0,
            "failed": 1,
            "errors": [{
                "alias": "projectRoot",
                "path": root_result.get("path") or "",
                "error": root_result.get("error") or root_result.get("reason") or "unknown root error",
            }],
        }

    context = get_project_storage_context(dialog_id) or context    

    yandex_disk = context.get("yandexDisk") or {}
    folders = yandex_disk.get("folders") or {}
    project_root_path = clean_cell_value(yandex_disk.get("projectRootPath"))

    write_debug_log("yandex_warmup_context_loaded", {
        "dialogId": dialog_id,
        "projectId": context.get("projectId") or "",
        "projectName": context.get("projectName") or "",
        "projectRootPath": project_root_path,
        "storageMode": storage_mode,
        "mirrorTargets": mirror_targets,
        "foldersCount": len(folders),
        "itemMappingsCount": len(context.get("itemMappings") or []),
        "alreadyPrepared": bool(yandex_disk.get("standardFoldersPrepared")),
    })

    prepared = 0
    skipped = 0
    failed = 0
    errors = []
    prepared_at = datetime.now().isoformat()

    updated_folders = dict(folders)

    ensured_folder_paths: set[str] = set()
    folder_meta_cache: dict[str, dict] = {}

    def save_cancelled_warmup_progress(reason: str) -> dict:
        yandex_disk["folders"] = updated_folders
        yandex_disk["standardFoldersPrepared"] = False
        yandex_disk["standardFoldersPreparedAt"] = prepared_at
        yandex_disk["standardFoldersPreparedCount"] = len(updated_folders)

        save_project_storage_context(dialog_id, {
            "dialogId": dialog_id,
            "projectId": context.get("projectId") or "",
            "projectName": context.get("projectName") or "",
            "storageMode": context.get("storageMode") or {},
            "yandexDisk": yandex_disk,
            "itemMappings": context.get("itemMappings") or [],
        })

        write_debug_log("yandex_warmup_cancelled", {
            "dialogId": dialog_id,
            "reason": reason,
            "projectRootPath": clean_cell_value(yandex_disk.get("projectRootPath")),
            "projectRootUrlExists": bool(clean_cell_value(yandex_disk.get("projectRootUrl"))),
            "foldersCount": len(updated_folders),
            "prepared": prepared,
            "skipped": skipped,
            "failed": failed,
            "ensuredFolderPathsCount": len(ensured_folder_paths),
            "folderMetaCacheCount": len(folder_meta_cache),
            "errors": errors[:20],
        })

        return {
            "ok": True,
            "cancelled": True,
            "reason": reason,
            "projectRootPath": clean_cell_value(yandex_disk.get("projectRootPath")),
            "projectRootUrl": clean_cell_value(yandex_disk.get("projectRootUrl")),
            "standardFoldersPrepared": False,
            "standardFoldersPreparedAt": prepared_at,
            "foldersCount": len(updated_folders),
            "prepared": prepared,
            "skipped": skipped,
            "failed": failed,
            "ensuredFolderPathsCount": len(ensured_folder_paths),
            "folderMetaCacheCount": len(folder_meta_cache),
            "errors": errors[:20],
        }

    for folder_alias, raw_folder in folders.items():
        if is_yandex_warmup_stop_requested(dialog_id):
            return save_cancelled_warmup_progress("stop requested before next folder")
        folder = raw_folder if isinstance(raw_folder, dict) else {}
        folder_path = clean_cell_value(folder.get("path"))
        write_debug_log("yandex_warmup_folder_seen", {
            "dialogId": dialog_id,
            "alias": folder_alias,
            "path": folder_path,
            "hasUrl": bool(clean_cell_value(folder.get("url"))),
        })
        if not folder_path:
            skipped += 1
            continue

        if clean_cell_value(folder.get("url")):
            skipped += 1
            continue

        try:
            write_debug_log("yandex_warmup_folder_prepare_started", {
                "dialogId": dialog_id,
                "alias": folder_alias,
                "path": folder_path,
            })

            folder_meta = ensure_folder_and_get_public_url(
                folder_path,
                ensured_folder_paths=ensured_folder_paths,
                folder_meta_cache=folder_meta_cache,
            )

            write_debug_log("yandex_warmup_folder_prepare_completed", {
                "dialogId": dialog_id,
                "alias": folder_alias,
                "path": folder_meta.get("path") or folder_path,
                "urlExists": bool(folder_meta.get("url")),
            })

            updated_folders[folder_alias] = {
                **folder,
                "name": clean_cell_value(folder_meta.get("name")) or clean_cell_value(folder.get("name")),
                "path": clean_cell_value(folder_meta.get("path")) or normalize_yandex_disk_path(folder_path),
                "url": clean_cell_value(folder_meta.get("url")) or clean_cell_value(folder.get("url")),
                "preparedAt": prepared_at,
            }

            prepared += 1

        except Exception as exc:
            failed += 1

            write_debug_log("yandex_warmup_folder_prepare_failed", {
                "dialogId": dialog_id,
                "alias": folder_alias,
                "path": folder_path,
                "error": str(exc),
            })

            errors.append({
                "alias": folder_alias,
                "path": folder_path,
                "error": str(exc),
            })

    yandex_disk["folders"] = updated_folders
    yandex_disk["standardFoldersPrepared"] = failed == 0
    yandex_disk["standardFoldersPreparedAt"] = prepared_at
    yandex_disk["standardFoldersPreparedCount"] = len(updated_folders)

    save_project_storage_context(dialog_id, {
        "dialogId": dialog_id,
        "projectId": context.get("projectId") or "",
        "projectName": context.get("projectName") or "",
        "storageMode": context.get("storageMode") or {},
        "yandexDisk": yandex_disk,
        "itemMappings": context.get("itemMappings") or [],
    })
    write_debug_log("yandex_warmup_finished", {
        "dialogId": dialog_id,
        "ok": failed == 0,
        "projectRootPath": clean_cell_value(yandex_disk.get("projectRootPath")),
        "projectRootUrlExists": bool(clean_cell_value(yandex_disk.get("projectRootUrl"))),
        "foldersCount": len(updated_folders),
        "prepared": prepared,
        "skipped": skipped,
        "failed": failed,
        "ensuredFolderPathsCount": len(ensured_folder_paths),
        "folderMetaCacheCount": len(folder_meta_cache),
        "errors": errors[:20],
    })
    return {
        "ok": failed == 0,
        "projectRootPath": clean_cell_value(yandex_disk.get("projectRootPath")),
        "projectRootUrl": clean_cell_value(yandex_disk.get("projectRootUrl")),
        "standardFoldersPrepared": failed == 0,
        "standardFoldersPreparedAt": prepared_at,
        "foldersCount": len(updated_folders),
        "prepared": prepared,
        "skipped": skipped,
        "failed": failed,
        "ensuredFolderPathsCount": len(ensured_folder_paths),
        "folderMetaCacheCount": len(folder_meta_cache),
        "errors": errors[:20],
    }

def upsert_item_yandex_mapping(
    dialog_id: str,
    checklist_key: str,
    item_name: str,
    folder_alias: str,
    folder_name: str,
    folder_path: str,
    folder_url: str,
    group_id: int = 0,
):
    context = get_project_storage_context(dialog_id)
    if not context:
        return

    checklist_key = normalize_checklist_key(checklist_key)
    item_name = clean_cell_value(item_name)

    try:
        group_id = int(group_id or 0)
    except (TypeError, ValueError):
        group_id = 0

    yandex_disk = context.get("yandexDisk") or {}
    folders = yandex_disk.get("folders") or {}
    item_mappings = context.get("itemMappings") or []

    folders[folder_alias] = {
        "name": folder_name,
        "path": folder_path,
        "url": folder_url,
        "checklistKey": checklist_key,
        "groupId": group_id,
        "itemName": item_name,
    }

    yandex_disk["folders"] = folders

    updated_mappings = []
    replaced = False

    for mapping in item_mappings:
        if not isinstance(mapping, dict):
            continue

        mapping_key = normalize_checklist_key(mapping.get("checklistKey"))
        mapping_name = clean_cell_value(mapping.get("itemName"))

        try:
            mapping_group_id = int(mapping.get("groupId") or 0)
        except (TypeError, ValueError):
            mapping_group_id = 0

        same_item = (
            mapping_key == checklist_key
            and mapping_name.lower() == item_name.lower()
            and mapping_group_id == group_id
        )

        if same_item:
            updated_mappings.append({
                **mapping,
                "checklistKey": checklist_key,
                "groupId": group_id,
                "itemName": item_name,
                "folderAlias": folder_alias,
            })
            replaced = True
        else:
            updated_mappings.append(mapping)

    if not replaced:
        updated_mappings.append({
            "checklistKey": checklist_key,
            "groupId": group_id,
            "itemName": item_name,
            "folderAlias": folder_alias,
        })

    save_project_storage_context(dialog_id, {
        "dialogId": dialog_id,
        "projectId": context.get("projectId") or "",
        "projectName": context.get("projectName") or "",
        "storageMode": context.get("storageMode") or {},
        "yandexDisk": yandex_disk,
        "itemMappings": updated_mappings,
    })




def resolve_custom_item_parent_yandex_path(
    dialog_id: str,
    checklist_key: str,
    group_id: int,
) -> str:
    checklist_key = normalize_checklist_key(checklist_key)
    root_path = get_root_path_from_context(dialog_id, checklist_key)

    if not root_path:
        return ""

    try:
        group_id = int(group_id or 0)
    except (TypeError, ValueError):
        group_id = 0

    specs = get_folder_specs_for_checklist(checklist_key)

    for raw_spec in (specs or {}).values():
        spec = raw_spec or {}

        if not spec.get("customItemsRoot"):
            continue

        try:
            spec_group_id = int(spec.get("groupId") or 0)
        except (TypeError, ValueError):
            spec_group_id = 0

        if spec_group_id != group_id:
            continue

        relative_path = (
            clean_cell_value(spec.get("relativePath"))
            or clean_cell_value(spec.get("folderName"))
        )

        if relative_path:
            return normalize_yandex_disk_path(
                f"{root_path.rstrip('/')}/{relative_path.strip('/')}"
            )

    return root_path

def build_custom_item_yandex_folder_spec(
    dialog_id: str,
    checklist_key: str,
    group_id: int,
    item_name: str,
    item_id: str,
) -> dict:
    checklist_key = normalize_checklist_key(checklist_key)
    folder_name = sanitize_yandex_folder_name(item_name)
    folder_alias = f"{checklist_key}_{slugify_folder_part(item_id or item_name)}"
    parent_path = resolve_custom_item_parent_yandex_path(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        group_id=group_id,
    )
    folder_path = (
        normalize_yandex_disk_path(
            f"{parent_path.rstrip('/')}/{folder_name}"
        )
        if parent_path
        else ""
    )

    enabled = can_create_custom_item_yandex_folder(
        dialog_id,
        checklist_key,
    )
    reason = "" if enabled else "custom item Yandex folder is disabled for this project"

    return {
        "enabled": bool(enabled),
        "reason": reason,
        "dialogId": clean_cell_value(dialog_id),
        "checklistKey": checklist_key,
        "groupId": int(group_id or 0),
        "itemId": clean_cell_value(item_id),
        "itemName": clean_cell_value(item_name),
        "folderAlias": folder_alias,
        "folderName": folder_name,
        "parentPath": parent_path,
        "targetPath": folder_path,
    }


def ensure_yandex_folder_for_custom_item(
    dialog_id: str,
    checklist_key: str,
    group_id: int,
    item_name: str,
    item_id: str,
) -> dict:
    checklist_key = normalize_checklist_key(checklist_key)
    parent_path = resolve_custom_item_parent_yandex_path(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        group_id=group_id,
    )

    if not parent_path:
        raise RuntimeError("Yandex root path not found in project storage context")

    folder_name = sanitize_yandex_folder_name(item_name)
    folder_alias = f"{checklist_key}_{slugify_folder_part(item_id or item_name)}"
    folder_path = normalize_yandex_disk_path(f"{parent_path.rstrip('/')}/{folder_name}")

    folder_meta = ensure_folder_and_get_public_url(folder_path)

    upsert_item_yandex_mapping(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_name=item_name,
        folder_alias=folder_alias,
        folder_name=folder_name,
        folder_path=folder_meta["path"],
        folder_url=folder_meta["url"],
        group_id=group_id,
    )

    return {
        "ok": True,
        "folderAlias": folder_alias,
        "folderName": folder_name,
        "folderPath": folder_meta["path"],
        "folderUrl": folder_meta["url"],
    }


def _split_yandex_parent_and_name(path: str) -> tuple[str, str]:
    normalized = normalize_yandex_disk_path(path)
    if not normalized or "/" not in normalized[len("disk:/"):]:
        return normalized.rsplit("/", 1)[0], normalized.rsplit("/", 1)[-1]
    parent, name = normalized.rsplit("/", 1)
    return parent, name


def _preserve_standard_folder_prefix(source_name: str, target_name: str) -> str:
    source_name = clean_cell_value(source_name)
    target_name = sanitize_yandex_folder_name(target_name)
    match = re.match(r"^(\d{1,3}[_\-\s]+)", source_name)
    return f"{match.group(1) if match else ''}{target_name}"



def resolve_item_group_parent_yandex_path(
    *,
    dialog_id: str,
    checklist_key: str,
    group_id: int,
) -> str:
    checklist_key = normalize_checklist_key(checklist_key)
    root_path = get_root_path_from_context(dialog_id, checklist_key)
    if not root_path:
        return ""

    config = get_checklist_config(checklist_key)
    try:
        normalized_group_id = int(group_id or 0)
    except (TypeError, ValueError):
        normalized_group_id = int(config.default_group_id)

    specs = get_folder_specs_for_checklist(checklist_key) or {}
    relative_candidates: list[str] = []

    for raw_spec in specs.values():
        spec = raw_spec or {}
        try:
            spec_group_id = int(spec.get("groupId") or 0)
        except (TypeError, ValueError):
            spec_group_id = 0
        if spec_group_id != normalized_group_id:
            continue

        relative_path = (
            clean_cell_value(spec.get("relativePath"))
            or clean_cell_value(spec.get("folderName"))
        ).replace("\\", "/").strip("/")
        if not relative_path:
            continue

        if spec.get("customItemsRoot"):
            return normalize_yandex_disk_path(
                f"{root_path.rstrip('/')}/{relative_path}"
            )

        parent_relative = (
            relative_path.rsplit("/", 1)[0]
            if "/" in relative_path
            else ""
        )
        if parent_relative:
            relative_candidates.append(parent_relative)

    if relative_candidates:
        relative_candidates.sort(
            key=lambda value: (
                len([part for part in value.split("/") if part]),
                len(value),
                value,
            )
        )
        return normalize_yandex_disk_path(
            f"{root_path.rstrip('/')}/{relative_candidates[0]}"
        )

    group_title = clean_cell_value(config.get_group_title(normalized_group_id))
    if not group_title:
        return root_path

    return normalize_yandex_disk_path(
        f"{root_path.rstrip('/')}/{sanitize_yandex_folder_name(group_title)}"
    )


def build_item_yandex_relocation_spec(
    *,
    dialog_id: str,
    checklist_key: str,
    item: dict,
    source_group_id: int,
    target_group_id: int,
    old_name: str = "",
    new_name: str = "",
    source_path_override: str = "",
) -> dict:
    checklist_key = normalize_checklist_key(checklist_key)
    item = dict(item or {})
    item_id = clean_cell_value(item.get("id"))
    current_name = clean_cell_value(item.get("name"))
    source_name = clean_cell_value(old_name) or current_name
    target_name = clean_cell_value(new_name) or current_name or source_name
    is_custom = bool(item.get("isCustom", False))
    folder_alias = clean_cell_value(item.get("yandexFolderAlias"))
    source_path = clean_cell_value(
        source_path_override
        or item.get("yandexFolderPath")
        or item.get("yandexFolderTargetPath")
    )
    source_url = clean_cell_value(item.get("yandexFolderUrl"))

    if not source_path:
        folder_info = get_item_yandex_folder(
            dialog_id,
            checklist_key,
            source_name,
            group_id=int(source_group_id or 0),
        ) or {}
        folder = (
            (folder_info.get("folder") or {})
            if isinstance(folder_info, dict)
            else {}
        )
        mapping = (
            (folder_info.get("mapping") or {})
            if isinstance(folder_info, dict)
            else {}
        )
        source_path = clean_cell_value(folder.get("path"))
        source_url = source_url or clean_cell_value(folder.get("url"))
        folder_alias = folder_alias or clean_cell_value(
            mapping.get("folderAlias")
        )

    if not folder_alias:
        folder_alias = (
            f"{checklist_key}_{slugify_folder_part(item_id or source_name)}"
        )

    target_parent = resolve_item_group_parent_yandex_path(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        group_id=int(target_group_id or 0),
    )
    source_folder_name = (
        normalize_yandex_disk_path(source_path)
        .rstrip("/")
        .rsplit("/", 1)[-1]
        if source_path
        else sanitize_yandex_folder_name(source_name or target_name)
    )
    target_folder_name = (
        sanitize_yandex_folder_name(target_name)
        if is_custom
        else _preserve_standard_folder_prefix(
            source_folder_name,
            target_name,
        )
    )
    target_path = (
        normalize_yandex_disk_path(
            f"{target_parent.rstrip('/')}/{target_folder_name}"
        )
        if target_parent and target_folder_name
        else ""
    )
    normalized_source_path = (
        normalize_yandex_disk_path(source_path)
        if source_path
        else ""
    )
    enabled = bool(
        is_yandex_disk_enabled()
        and normalized_source_path
        and target_path
        and normalized_source_path != target_path
    )

    return {
        "enabled": enabled,
        "dialogId": clean_cell_value(dialog_id),
        "checklistKey": checklist_key,
        "itemId": item_id,
        "itemName": target_name,
        "oldName": source_name,
        "newName": target_name,
        "isCustom": is_custom,
        "folderAlias": folder_alias,
        "sourceGroupId": int(source_group_id or 0),
        "targetGroupId": int(target_group_id or 0),
        "sourcePath": normalized_source_path,
        "sourceUrl": source_url,
        "targetParentPath": target_parent,
        "targetFolderName": target_folder_name,
        "targetPath": target_path,
        "reason": (
            ""
            if enabled
            else (
                "yandex disk is disabled, source path is unavailable "
                "or target path is unchanged"
            )
        ),
    }


def build_item_yandex_move_spec(
    *,
    dialog_id: str,
    checklist_key: str,
    item: dict,
    source_group_id: int,
    target_group_id: int,
) -> dict:
    item_name = clean_cell_value((item or {}).get("name"))
    return build_item_yandex_relocation_spec(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item=item,
        source_group_id=source_group_id,
        target_group_id=target_group_id,
        old_name=item_name,
        new_name=item_name,
    )


def build_item_yandex_rename_spec(
    *,
    dialog_id: str,
    checklist_key: str,
    item: dict,
    old_name: str,
    new_name: str,
) -> dict:
    checklist_key = normalize_checklist_key(checklist_key)
    item = dict(item or {})
    item_id = clean_cell_value(item.get("id"))
    group_id = int(item.get("group") or 0)
    is_custom = bool(item.get("isCustom", False))

    source_path = clean_cell_value(
        item.get("yandexFolderPath")
        or item.get("yandexFolderTargetPath")
    )
    source_url = clean_cell_value(item.get("yandexFolderUrl"))
    folder_alias = clean_cell_value(item.get("yandexFolderAlias"))

    if not source_path:
        folder_info = get_item_yandex_folder(
            dialog_id,
            checklist_key,
            old_name,
            group_id=group_id,
        ) or {}
        folder = (folder_info.get("folder") or {}) if isinstance(folder_info, dict) else {}
        mapping = (folder_info.get("mapping") or {}) if isinstance(folder_info, dict) else {}
        source_path = clean_cell_value(folder.get("path"))
        source_url = source_url or clean_cell_value(folder.get("url"))
        folder_alias = folder_alias or clean_cell_value(mapping.get("folderAlias"))

    if not folder_alias:
        folder_alias = f"{checklist_key}_{slugify_folder_part(item_id or old_name)}"

    if source_path:
        parent_path, source_folder_name = _split_yandex_parent_and_name(source_path)
        target_folder_name = (
            sanitize_yandex_folder_name(new_name)
            if is_custom
            else _preserve_standard_folder_prefix(source_folder_name, new_name)
        )
        target_path = normalize_yandex_disk_path(
            f"{parent_path.rstrip('/')}/{target_folder_name}"
        )
    else:
        custom_spec = build_custom_item_yandex_folder_spec(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            group_id=group_id,
            item_name=new_name,
            item_id=item_id,
        )
        target_folder_name = clean_cell_value(custom_spec.get("folderName"))
        target_path = clean_cell_value(custom_spec.get("targetPath"))

    return {
        "enabled": bool(is_yandex_disk_enabled() and target_path),
        "dialogId": clean_cell_value(dialog_id),
        "checklistKey": checklist_key,
        "itemId": item_id,
        "groupId": group_id,
        "isCustom": is_custom,
        "oldName": clean_cell_value(old_name),
        "newName": clean_cell_value(new_name),
        "folderAlias": folder_alias,
        "sourcePath": normalize_yandex_disk_path(source_path) if source_path else "",
        "sourceUrl": source_url,
        "targetPath": normalize_yandex_disk_path(target_path) if target_path else "",
        "targetFolderName": target_folder_name,
        "reason": "" if (is_yandex_disk_enabled() and target_path) else "yandex disk is disabled or target path is unavailable",
    }


def rename_item_yandex_mapping(
    *,
    dialog_id: str,
    checklist_key: str,
    group_id: int,
    old_name: str,
    old_group_id: int | None = None,
    new_name: str,
    folder_alias: str,
    folder_name: str,
    folder_path: str,
    folder_url: str,
) -> None:
    context = get_project_storage_context(dialog_id)
    if not context:
        return

    checklist_key = normalize_checklist_key(checklist_key)
    old_key = clean_cell_value(old_name).casefold()
    source_group_id = (
        int(group_id or 0)
        if old_group_id is None
        else int(old_group_id or 0)
    )
    alias = clean_cell_value(folder_alias)
    yandex_disk = context.get("yandexDisk") or {}
    folders = dict(yandex_disk.get("folders") or {})
    mappings = []

    for mapping in context.get("itemMappings") or []:
        if not isinstance(mapping, dict):
            continue
        mapping_key = normalize_checklist_key(mapping.get("checklistKey"))
        mapping_name = clean_cell_value(mapping.get("itemName")).casefold()
        mapping_alias = clean_cell_value(mapping.get("folderAlias"))
        try:
            mapping_group = int(mapping.get("groupId") or 0)
        except (TypeError, ValueError):
            mapping_group = 0
        is_old = (
            mapping_key == checklist_key
            and mapping_group == source_group_id
            and (mapping_name == old_key or (alias and mapping_alias == alias))
        )
        if not is_old:
            mappings.append(mapping)

    mappings.append({
        "checklistKey": checklist_key,
        "groupId": int(group_id or 0),
        "itemName": clean_cell_value(new_name),
        "folderAlias": alias,
    })
    folders[alias] = {
        **(folders.get(alias) or {}),
        "name": clean_cell_value(folder_name),
        "path": clean_cell_value(folder_path),
        "url": clean_cell_value(folder_url),
        "checklistKey": checklist_key,
        "groupId": int(group_id or 0),
        "itemName": clean_cell_value(new_name),
    }
    yandex_disk["folders"] = folders
    save_project_storage_context(dialog_id, {
        "dialogId": clean_cell_value(dialog_id),
        "projectId": context.get("projectId") or "",
        "projectName": context.get("projectName") or "",
        "storageMode": context.get("storageMode") or {},
        "yandexDisk": yandex_disk,
        "itemMappings": mappings,
    })


def rename_yandex_folder_for_item(
    *,
    dialog_id: str,
    checklist_key: str,
    group_id: int,
    item_id: str,
    old_group_id: int | None = None,
    old_name: str,
    new_name: str,
    source_path: str,
    target_path: str,
    folder_alias: str,
) -> dict:
    source_path = normalize_yandex_disk_path(source_path)
    target_path = normalize_yandex_disk_path(target_path)
    if not source_path:
        raise RuntimeError("Yandex source folder path is unavailable")
    if not target_path:
        raise RuntimeError("Yandex target folder path is unavailable")

    target_parent = target_path.rsplit("/", 1)[0]
    if target_parent:
        ensure_yandex_folder_chain(target_parent)

    move_result = yandex_disk_move_path(
        source_path,
        target_path,
        overwrite=False,
    )
    yandex_disk_publish_path(target_path)
    meta = yandex_disk_get_resource_meta(target_path)
    folder_name = clean_cell_value(meta.get("name")) or target_path.rsplit("/", 1)[-1]
    folder_url = clean_cell_value(meta.get("public_url"))
    final_path = clean_cell_value(meta.get("path")) or target_path

    rename_item_yandex_mapping(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        group_id=group_id,
        old_group_id=old_group_id,
        old_name=old_name,
        new_name=new_name,
        folder_alias=folder_alias,
        folder_name=folder_name,
        folder_path=final_path,
        folder_url=folder_url,
    )
    return {
        "ok": True,
        "itemId": clean_cell_value(item_id),
        "folderAlias": clean_cell_value(folder_alias),
        "folderName": folder_name,
        "folderPath": final_path,
        "folderUrl": folder_url,
        "sourcePath": source_path,
        "targetPath": target_path,
        "move": move_result,
    }



def move_yandex_folder_for_item(
    *,
    dialog_id: str,
    checklist_key: str,
    source_group_id: int,
    target_group_id: int,
    item_id: str,
    item_name: str,
    source_path: str,
    target_path: str,
    folder_alias: str,
) -> dict:
    return rename_yandex_folder_for_item(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        group_id=int(target_group_id or 0),
        old_group_id=int(source_group_id or 0),
        item_id=item_id,
        old_name=item_name,
        new_name=item_name,
        source_path=source_path,
        target_path=target_path,
        folder_alias=folder_alias,
    )


def ensure_yandex_folder_for_custom_opr_item(
    dialog_id: str,
    checklist_key: str,
    group_id: int,
    item_name: str,
    item_id: str,
) -> dict:
    return ensure_yandex_folder_for_custom_item(
        dialog_id=dialog_id,
        checklist_key="opr",
        group_id=group_id,
        item_name=item_name,
        item_id=item_id,
    )


def resolve_standard_folder_spec_for_item(
    checklist_key: str,
    item_name: str,
    group_id: int = 0,
) -> tuple[str, dict] | None:
    specs = get_folder_specs_for_checklist(checklist_key)
    target_name = clean_cell_value(item_name).lower()

    try:
        target_group_id = int(group_id or 0)
    except (TypeError, ValueError):
        target_group_id = 0

    fallback = None
    first_name_match = None

    for spec_key, raw_spec in (specs or {}).items():
        spec = raw_spec or {}
        spec_item_name = clean_cell_value(spec.get("itemName")) or clean_cell_value(spec_key)

        if spec_item_name.lower() != target_name:
            continue

        try:
            spec_group_id = int(spec.get("groupId") or 0)
        except (TypeError, ValueError):
            spec_group_id = 0

        if first_name_match is None:
            first_name_match = (spec_key, spec)

        if target_group_id and spec_group_id == target_group_id:
            return spec_key, spec

        if spec_group_id == 0 and fallback is None:
            fallback = (spec_key, spec)

    return fallback or first_name_match


def ensure_standard_yandex_folder_for_item(
    dialog_id: str,
    checklist_key: str,
    item_name: str,
    group_id: int = 0,
):
    checklist_key = normalize_checklist_key(checklist_key)
    item_name = clean_cell_value(item_name)

    resolved_spec = resolve_standard_folder_spec_for_item(
        checklist_key=checklist_key,
        item_name=item_name,
        group_id=group_id,
    )

    if not resolved_spec:
        return None

    spec_key, spec = resolved_spec

    try:
        group_id = int(group_id or spec.get("groupId") or 0)
    except (TypeError, ValueError):
        group_id = 0

    root_path = get_root_path_from_context(dialog_id, checklist_key)
    if not root_path:
        return None

    spec_item_name = clean_cell_value(spec.get("itemName")) or item_name
    folder_alias = clean_cell_value(spec.get("alias")) or f"{checklist_key}_{slugify_folder_part(spec_key)}"
    relative_path = clean_cell_value(spec.get("relativePath")) or clean_cell_value(spec.get("folderName")) or spec_key
    folder_name = clean_cell_value(spec.get("folderName")) or sanitize_yandex_folder_name(spec_item_name)
    folder_path = normalize_yandex_disk_path(f"{root_path.rstrip('/')}/{relative_path.strip('/')}")

    folder_meta = ensure_folder_and_get_public_url(folder_path)

    upsert_item_yandex_mapping(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_name=spec_item_name,
        folder_alias=folder_alias,
        folder_name=folder_name,
        folder_path=folder_meta["path"],
        folder_url=folder_meta["url"],
        group_id=group_id,
    )

    return {
        "ok": True,
        "folderAlias": folder_alias,
        "folderName": folder_name,
        "folderPath": folder_meta["path"],
        "folderUrl": folder_meta["url"],
    }



def ensure_standard_yandex_folder_for_opr_item(dialog_id: str, checklist_key: str, item_name: str):
    return ensure_standard_yandex_folder_for_item(dialog_id, "opr", item_name)


def ensure_item_yandex_folder_for_upload(
    dialog_id: str,
    checklist_key: str,
    item_name: str,
    item_id: str = "",
    item_group: int = 0,
    is_custom: bool = False,
):
    checklist_key = normalize_checklist_key(checklist_key)
    item_name = clean_cell_value(item_name)

    try:
        item_group = int(item_group or 0)
    except (TypeError, ValueError):
        item_group = 0

    existing = get_item_yandex_folder(
        dialog_id,
        checklist_key,
        item_name,
        group_id=item_group,
    )

    if existing:
        folder = existing.get("folder") or {}
        folder_path = clean_cell_value(folder.get("path"))
        folder_url = clean_cell_value(folder.get("url"))

        if folder_path and folder_url:
            return existing

        if folder_path:
            folder_meta = ensure_folder_and_get_public_url(folder_path)

            folder_alias = clean_cell_value(
                existing.get("folderAlias")
                or (existing.get("mapping") or {}).get("folderAlias")
            )

            folder_name = (
                clean_cell_value(folder_meta.get("name"))
                or clean_cell_value(folder.get("name"))
                or folder_path.rstrip("/").rsplit("/", 1)[-1]
            )

            folder_url = (
                clean_cell_value(folder_meta.get("url"))
                or clean_cell_value(folder.get("url"))
            )

            if folder_alias:
                upsert_item_yandex_mapping(
                    dialog_id=dialog_id,
                    checklist_key=checklist_key,
                    item_name=item_name,
                    folder_alias=folder_alias,
                    folder_name=folder_name,
                    folder_path=folder_meta.get("path") or folder_path,
                    folder_url=folder_url,
                    group_id=item_group,
                )

                refreshed = get_item_yandex_folder(
                    dialog_id,
                    checklist_key,
                    item_name,
                    group_id=item_group,
                )
                if refreshed:
                    return refreshed

            existing["folder"] = {
                **folder,
                "name": folder_name,
                "path": folder_meta.get("path") or folder_path,
                "url": folder_url,
            }

        return existing

    if is_custom:
        restored = ensure_yandex_folder_for_custom_item(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            group_id=item_group,
            item_name=item_name,
            item_id=item_id,
        )
    else:
        restored = ensure_standard_yandex_folder_for_item(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_name=item_name,
            group_id=item_group,
        )

    if not restored:
        return None

    return get_item_yandex_folder(
        dialog_id,
        checklist_key,
        item_name,
        group_id=item_group,
    ) or {
        "folderAlias": restored.get("folderAlias"),
        "folder": {
            "name": restored.get("folderName"),
            "path": restored.get("folderPath"),
            "url": restored.get("folderUrl"),
        },
        "mapping": {
            "checklistKey": checklist_key,
            "groupId": item_group,
            "itemName": item_name,
            "folderAlias": restored.get("folderAlias"),
        },
        "context": get_project_storage_context(dialog_id),
    }



def build_yandex_file_target_path(folder_path: str, filename: str) -> str:
    folder_path = normalize_yandex_disk_path(folder_path).rstrip("/")
    safe_name = Path(filename or "file.bin").name
    return f"{folder_path}/{safe_name}"


def mirror_document_to_yandex(
    dialog_id: str,
    checklist_key: str,
    item_name: str,
    filename: str,
    file_bytes: bytes,
    item_id: str = "",
    item_group: int = 0,
    is_custom: bool = False,
) -> dict:
    if not is_yandex_disk_enabled():
        return {
            "ok": False,
            "reason": "yandex disk is disabled",
        }

    checklist_key = normalize_checklist_key(checklist_key)

    try:
        folder_info = ensure_item_yandex_folder_for_upload(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_name=item_name,
            item_id=item_id,
            item_group=item_group,
            is_custom=is_custom,
        )

        if not folder_info:
            return {
                "ok": False,
                "reason": "yandex folder not found",
            }

        folder = folder_info.get("folder") or {}
        folder_alias = clean_cell_value(folder_info.get("folderAlias") or (folder_info.get("mapping") or {}).get("folderAlias"))
        folder_path = clean_cell_value(folder.get("path"))
        folder_url = clean_cell_value(folder.get("url"))

        if not folder_path:
            return {
                "ok": False,
                "reason": "yandex folder path is empty",
            }

        target_path = build_yandex_file_target_path(folder_path, filename)
        upload_result = yandex_disk_upload_bytes(target_path, file_bytes)

        return {
            "ok": True,
            "folderAlias": folder_alias,
            "folderPath": normalize_yandex_disk_path(folder_path),
            "folderUrl": folder_url,
            "filePath": upload_result.get("path") or normalize_yandex_disk_path(target_path),
        }

    except Exception as e:
        write_debug_log("yandex_mirror_upload_error", {
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "itemName": item_name,
            "itemId": item_id,
            "error": str(e),
        })

        return {
            "ok": False,
            "reason": str(e),
        }
    
def mirror_document_file_to_yandex(
    dialog_id: str,
    checklist_key: str,
    item_name: str,
    filename: str,
    local_path,
    item_id: str = "",
    item_group: int = 0,
    is_custom: bool = False,
    progress_callback=None,
) -> dict:
    if not is_yandex_disk_enabled():
        return {
            "ok": False,
            "reason": "yandex disk is disabled",
        }

    checklist_key = normalize_checklist_key(checklist_key)

    try:
        folder_info = ensure_item_yandex_folder_for_upload(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_name=item_name,
            item_id=item_id,
            item_group=item_group,
            is_custom=is_custom,
        )

        if not folder_info:
            return {
                "ok": False,
                "reason": "yandex folder not found",
            }

        folder = folder_info.get("folder") or {}
        folder_alias = clean_cell_value(
            folder_info.get("folderAlias")
            or (folder_info.get("mapping") or {}).get("folderAlias")
        )
        folder_path = clean_cell_value(folder.get("path"))
        folder_url = clean_cell_value(folder.get("url"))

        if not folder_path:
            return {
                "ok": False,
                "reason": "yandex folder path is empty",
            }

        target_path = build_yandex_file_target_path(folder_path, filename)

        upload_result = yandex_disk_upload_file(
            target_path=target_path,
            local_path=local_path,
            progress_callback=progress_callback,
        )

        return {
            "ok": True,
            "folderAlias": folder_alias,
            "folderPath": normalize_yandex_disk_path(folder_path),
            "folderUrl": folder_url,
            "filePath": upload_result.get("path") or normalize_yandex_disk_path(target_path),
        }

    except Exception as e:
        write_debug_log("yandex_mirror_file_upload_error", {
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "itemName": item_name,
            "itemId": item_id,
            "localPath": str(local_path),
            "error": str(e),
        })

        return {
            "ok": False,
            "reason": str(e),
        }