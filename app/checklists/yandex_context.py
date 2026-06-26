from copy import deepcopy
from typing import Any

from app.checklists.config import list_checklist_configs
from app.checklists.models import ChecklistConfig
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    slugify_folder_part,
)


def normalize_yandex_context_path(path: str) -> str:
    value = clean_cell_value(path).replace("\\", "/")

    if not value:
        return ""

    if value == "disk:/":
        return value

    if value.startswith("disk:/"):
        rest = value[len("disk:/"):].strip("/")
        return f"disk:/{rest}" if rest else "disk:/"

    return value.rstrip("/")


def join_yandex_path(base_path: str, relative_path: str) -> str:
    base = normalize_yandex_context_path(base_path)
    relative = clean_cell_value(relative_path).replace("\\", "/").strip("/")

    if not base:
        return normalize_yandex_context_path(relative)

    if not relative:
        return base

    return normalize_yandex_context_path(f"{base.rstrip('/')}/{relative}")


def get_context_yandex_disk(context: dict) -> dict:
    context = context or {}
    yandex_disk = context.get("yandexDisk") or {}

    return yandex_disk if isinstance(yandex_disk, dict) else {}


def get_project_root_path(context: dict) -> str:
    context = context or {}
    yandex_disk = get_context_yandex_disk(context)

    project_root_path = clean_cell_value(yandex_disk.get("projectRootPath"))
    if project_root_path:
        return normalize_yandex_context_path(project_root_path)

    project_name = clean_cell_value(context.get("projectName"))
    if project_name:
        return normalize_yandex_context_path(f"disk:/ОПР/{project_name}")

    return ""


def resolve_checklist_yandex_root_path(context: dict, config: ChecklistConfig) -> str:
    context = context or {}
    yandex_disk = get_context_yandex_disk(context)

    if config.yandex_root_context_key:
        direct_value = (
            clean_cell_value(yandex_disk.get(config.yandex_root_context_key))
            or clean_cell_value(context.get(config.yandex_root_context_key))
        )

        if direct_value:
            return normalize_yandex_context_path(direct_value)

    project_root_path = get_project_root_path(context)

    if config.yandex_root_relative_path:
        return join_yandex_path(project_root_path, config.yandex_root_relative_path)

    return project_root_path


def get_existing_yandex_folders(context: dict) -> dict:
    yandex_disk = get_context_yandex_disk(context)
    folders = yandex_disk.get("folders") or {}

    return folders if isinstance(folders, dict) else {}


def get_folder_public_url(existing_folders: dict, folder_alias: str) -> str:
    folder = existing_folders.get(folder_alias) or {}
    if not isinstance(folder, dict):
        return ""

    return clean_cell_value(folder.get("url") or folder.get("public_url"))


def build_root_folder_record(
    context: dict,
    config: ChecklistConfig,
    existing_folders: dict,
) -> dict | None:
    if not config.yandex_root_alias:
        return None

    root_path = resolve_checklist_yandex_root_path(context, config)
    if not root_path:
        return None

    folder_name = (
        clean_cell_value(config.yandex_root_folder_name)
        or root_path.rstrip("/").rsplit("/", 1)[-1]
        or config.title
    )

    return {
        "alias": config.yandex_root_alias,
        "name": folder_name,
        "path": root_path,
        "url": get_folder_public_url(existing_folders, config.yandex_root_alias),
        "checklistKey": config.key,
        "isStageRoot": True,
    }


def normalize_folder_spec_alias(config: ChecklistConfig, item_name: str, spec: dict) -> str:
    alias = clean_cell_value(spec.get("alias"))

    if alias:
        return alias

    return f"{config.key}_{slugify_folder_part(item_name)}"


def normalize_folder_spec_name(item_name: str, spec: dict, folder_path: str) -> str:
    folder_name = clean_cell_value(spec.get("folderName"))

    if folder_name:
        return folder_name

    path_name = clean_cell_value(folder_path).rstrip("/").rsplit("/", 1)[-1]
    return path_name or clean_cell_value(item_name)


def build_config_yandex_folders(context: dict) -> dict:
    context = context or {}
    existing_folders = get_existing_yandex_folders(context)

    result = deepcopy(existing_folders)

    for config in list_checklist_configs():
        root_record = build_root_folder_record(context, config, existing_folders)
        if root_record:
            root_alias = root_record.pop("alias")
            result[root_alias] = {
                **result.get(root_alias, {}),
                **root_record,
                "url": get_folder_public_url(existing_folders, root_alias),
            }

        root_path = resolve_checklist_yandex_root_path(context, config)
        if not root_path:
            continue

        specs = config.standard_yandex_folder_specs or {}

        for item_name, raw_spec in specs.items():
            spec = raw_spec or {}
            folder_alias = normalize_folder_spec_alias(config, item_name, spec)

            relative_path = (
                clean_cell_value(spec.get("relativePath"))
                or clean_cell_value(spec.get("folderName"))
                or clean_cell_value(item_name)
            )

            folder_path = join_yandex_path(root_path, relative_path)
            folder_name = normalize_folder_spec_name(item_name, spec, folder_path)

            result[folder_alias] = {
                **result.get(folder_alias, {}),
                "name": folder_name,
                "path": folder_path,
                "url": get_folder_public_url(existing_folders, folder_alias),
                "checklistKey": config.key,
                "itemName": clean_cell_value(item_name),
                "isStageRoot": False,
            }

    return result


def build_config_item_mappings() -> list[dict]:
    result = []

    for config in list_checklist_configs():
        specs = config.standard_yandex_folder_specs or {}

        for item_name, raw_spec in specs.items():
            spec = raw_spec or {}
            folder_alias = normalize_folder_spec_alias(config, item_name, spec)

            result.append({
                "checklistKey": config.key,
                "itemName": clean_cell_value(item_name),
                "folderAlias": folder_alias,
            })

    return result


def mapping_identity(mapping: dict) -> tuple[str, str]:
    return (
        normalize_checklist_key(mapping.get("checklistKey")),
        clean_cell_value(mapping.get("itemName")).lower(),
    )


def merge_item_mappings(existing_mappings: list, generated_mappings: list) -> list[dict]:
    result = []
    seen = set()

    for mapping in existing_mappings or []:
        if not isinstance(mapping, dict):
            continue

        identity = mapping_identity(mapping)
        if not identity[0] or not identity[1] or identity in seen:
            continue

        result.append(dict(mapping))
        seen.add(identity)

    for mapping in generated_mappings or []:
        if not isinstance(mapping, dict):
            continue

        identity = mapping_identity(mapping)
        if not identity[0] or not identity[1] or identity in seen:
            continue

        result.append(dict(mapping))
        seen.add(identity)

    return result


def hydrate_project_storage_context_from_configs(context: dict) -> dict:
    context = deepcopy(context or {})

    yandex_disk = context.get("yandexDisk") or {}
    if not isinstance(yandex_disk, dict):
        yandex_disk = {}

    yandex_disk.setdefault("provider", "yandex_disk")
    yandex_disk["projectRootPath"] = get_project_root_path({
        **context,
        "yandexDisk": yandex_disk,
    })

    yandex_disk["folders"] = build_config_yandex_folders({
        **context,
        "yandexDisk": yandex_disk,
    })

    context["yandexDisk"] = yandex_disk

    existing_mappings = context.get("itemMappings") or []
    context["itemMappings"] = merge_item_mappings(
        existing_mappings,
        build_config_item_mappings(),
    )

    return context