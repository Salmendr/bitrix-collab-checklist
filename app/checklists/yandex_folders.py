import re
from pathlib import Path

from app.logging_utils import write_debug_log

from app.checklists.registry import (
    STANDARD_ID_YANDEX_FOLDER_SPECS,
    STANDARD_OPR_YANDEX_FOLDER_SPECS,
    STANDARD_CONCEPT_YANDEX_FOLDER_SPECS,
)

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
    checklist_key = normalize_checklist_key(checklist_key)

    if checklist_key not in {"id", "opr"}:
        return False

    context = get_project_storage_context(dialog_id)
    if not context:
        return False

    yandex_disk = context.get("yandexDisk") or {}
    storage_mode = context.get("storageMode") or {}

    mirror_targets = storage_mode.get("mirrorTargets") or []
    if "yandex_disk" not in mirror_targets:
        return False

    project_root_path = clean_cell_value(yandex_disk.get("projectRootPath"))
    id_stage_root_path = clean_cell_value(yandex_disk.get("idStageRootPath"))

    return bool(project_root_path or id_stage_root_path)


def sanitize_yandex_folder_name(value: str) -> str:
    value = clean_cell_value(value)
    value = re.sub(r'[<>:"/\\\\|?*]+', "_", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value or "Новый пункт"


def get_folder_specs_for_checklist(checklist_key: str) -> dict:
    checklist_key = normalize_checklist_key(checklist_key)

    if checklist_key == "opr":
        return STANDARD_OPR_YANDEX_FOLDER_SPECS

    if checklist_key == "concept":
        return STANDARD_CONCEPT_YANDEX_FOLDER_SPECS

    return STANDARD_ID_YANDEX_FOLDER_SPECS


def get_root_path_from_context(dialog_id: str, checklist_key: str) -> str:
    context = get_project_storage_context(dialog_id)
    if not context:
        return ""

    checklist_key = normalize_checklist_key(checklist_key)
    yandex_disk = context.get("yandexDisk") or {}

    if checklist_key == "id":
        return clean_cell_value(
            yandex_disk.get("idStageRootPath")
            or yandex_disk.get("projectRootPath")
        )

    if checklist_key == "opr":
        project_root = clean_cell_value(yandex_disk.get("projectRootPath"))
        if not project_root:
            return ""

        return normalize_yandex_disk_path(
            f"{project_root.rstrip('/')}/02_ОПР"
        )

    if checklist_key == "concept":
        project_root = clean_cell_value(yandex_disk.get("projectRootPath"))
        if not project_root:
            return ""

        return normalize_yandex_disk_path(
            f"{project_root.rstrip('/')}/01_Концепция"
        )

    return ""


def ensure_folder_and_get_public_url(folder_path: str) -> dict:
    folder_path = normalize_yandex_disk_path(folder_path)

    yandex_disk_ensure_folder(folder_path)
    yandex_disk_publish_path(folder_path)

    meta = yandex_disk_get_resource_meta(folder_path)
    return {
        "path": clean_cell_value(meta.get("path")) or folder_path,
        "url": clean_cell_value(meta.get("public_url")),
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
        return existing

    if is_custom:
        if checklist_key == "opr":
            restored = ensure_yandex_folder_for_custom_opr_item(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                group_id=int(item_group or 0),
                item_name=item_name,
                item_id=item_id,
            )
        else:
            restored = ensure_yandex_folder_for_custom_item(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                group_id=int(item_group or 0),
                item_name=item_name,
                item_id=item_id,
            )
    else:
        if checklist_key == "opr":
            restored = ensure_standard_yandex_folder_for_opr_item(dialog_id, checklist_key, item_name)
        else:
            restored = ensure_standard_yandex_folder_for_item(dialog_id, checklist_key, item_name)

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