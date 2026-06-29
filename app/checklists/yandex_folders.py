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

from app.yandex_disk.client import (
    is_yandex_disk_enabled,
    normalize_yandex_disk_path,
    yandex_disk_upload_bytes,
    yandex_disk_ensure_folder,
    yandex_disk_publish_path,
    yandex_disk_get_resource_meta,
)


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


def ensure_yandex_folder_chain(target_path: str) -> dict:
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

    for folder_path in chain:
        write_debug_log("yandex_folder_chain_ensure_started", {
            "targetPath": normalized_path,
            "folderPath": folder_path,
        })

        result = yandex_disk_ensure_folder(folder_path)

        write_debug_log("yandex_folder_chain_ensure_completed", {
            "targetPath": normalized_path,
            "folderPath": folder_path,
            "alreadyExists": bool(result.get("alreadyExists")),
        })
        created.append({
            "path": folder_path,
            "alreadyExists": bool(result.get("alreadyExists")),
        })

    return {
        "ok": True,
        "path": normalized_path,
        "created": created,
    }

def ensure_folder_and_get_public_url(folder_path: str) -> dict:
    folder_path = normalize_yandex_disk_path(folder_path)

    if not folder_path:
        return {
            "ok": False,
            "path": "",
            "url": "",
            "name": "",
            "reason": "empty yandex folder path",
        }

    ensure_yandex_folder_chain(folder_path)
    yandex_disk_publish_path(folder_path)

    meta = yandex_disk_get_resource_meta(folder_path)

    return {
        "ok": True,
        "path": clean_cell_value(meta.get("path")) or folder_path,
        "url": clean_cell_value(meta.get("public_url")),
        "name": clean_cell_value(meta.get("name")) or folder_path.rstrip("/").rsplit("/", 1)[-1],
    }

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

    for folder_alias, raw_folder in folders.items():
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

            folder_meta = ensure_folder_and_get_public_url(folder_path)

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
):
    context = get_project_storage_context(dialog_id)
    if not context:
        return

    checklist_key = normalize_checklist_key(checklist_key)
    item_name = clean_cell_value(item_name)

    yandex_disk = context.get("yandexDisk") or {}
    folders = yandex_disk.get("folders") or {}
    item_mappings = context.get("itemMappings") or []

    folders[folder_alias] = {
        "name": folder_name,
        "path": folder_path,
        "url": folder_url,
    }

    yandex_disk["folders"] = folders

    updated_mappings = []
    replaced = False

    for mapping in item_mappings:
        mapping_key = normalize_checklist_key(mapping.get("checklistKey"))
        mapping_name = clean_cell_value(mapping.get("itemName"))

        if mapping_key == checklist_key and mapping_name.lower() == item_name.lower():
            updated_mappings.append({
                **mapping,
                "checklistKey": checklist_key,
                "itemName": item_name,
                "folderAlias": folder_alias,
            })
            replaced = True
        else:
            updated_mappings.append(mapping)

    if not replaced:
        updated_mappings.append({
            "checklistKey": checklist_key,
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


def ensure_yandex_folder_for_custom_item(
    dialog_id: str,
    checklist_key: str,
    group_id: int,
    item_name: str,
    item_id: str,
) -> dict:
    checklist_key = normalize_checklist_key(checklist_key)
    root_path = get_root_path_from_context(dialog_id, checklist_key)

    if not root_path:
        raise RuntimeError("Yandex root path not found in project storage context")

    folder_name = sanitize_yandex_folder_name(item_name)
    folder_alias = f"{checklist_key}_{slugify_folder_part(item_id or item_name)}"
    folder_path = normalize_yandex_disk_path(f"{root_path.rstrip('/')}/{folder_name}")

    folder_meta = ensure_folder_and_get_public_url(folder_path)

    upsert_item_yandex_mapping(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_name=item_name,
        folder_alias=folder_alias,
        folder_name=folder_name,
        folder_path=folder_meta["path"],
        folder_url=folder_meta["url"],
    )

    return {
        "ok": True,
        "folderAlias": folder_alias,
        "folderName": folder_name,
        "folderPath": folder_meta["path"],
        "folderUrl": folder_meta["url"],
    }


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


def ensure_standard_yandex_folder_for_item(dialog_id: str, checklist_key: str, item_name: str):
    checklist_key = normalize_checklist_key(checklist_key)
    specs = get_folder_specs_for_checklist(checklist_key)
    item_name = clean_cell_value(item_name)

    spec = specs.get(item_name)
    if not spec:
        return None

    root_path = get_root_path_from_context(dialog_id, checklist_key)
    if not root_path:
        return None

    folder_alias = clean_cell_value(spec.get("alias")) or f"{checklist_key}_{slugify_folder_part(item_name)}"
    relative_path = clean_cell_value(spec.get("relativePath")) or clean_cell_value(spec.get("folderName")) or item_name
    folder_name = clean_cell_value(spec.get("folderName")) or sanitize_yandex_folder_name(item_name)
    folder_path = normalize_yandex_disk_path(f"{root_path.rstrip('/')}/{relative_path.strip('/')}")

    folder_meta = ensure_folder_and_get_public_url(folder_path)

    upsert_item_yandex_mapping(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_name=item_name,
        folder_alias=folder_alias,
        folder_name=folder_name,
        folder_path=folder_meta["path"],
        folder_url=folder_meta["url"],
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

    existing = get_item_yandex_folder(dialog_id, checklist_key, item_name)
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
                )

                refreshed = get_item_yandex_folder(dialog_id, checklist_key, item_name)
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
            group_id=int(item_group or 0),
            item_name=item_name,
            item_id=item_id,
        )
    else:
        restored = ensure_standard_yandex_folder_for_item(
            dialog_id,
            checklist_key,
            item_name,
        )

    if not restored:
        return None

    return get_item_yandex_folder(dialog_id, checklist_key, item_name) or {
        "folderAlias": restored.get("folderAlias"),
        "folder": {
            "name": restored.get("folderName"),
            "path": restored.get("folderPath"),
            "url": restored.get("folderUrl"),
        },
        "mapping": {
            "checklistKey": checklist_key,
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