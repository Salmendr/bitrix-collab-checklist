from copy import deepcopy
from typing import Any

from app.checklists.config import list_checklist_configs
from app.checklists.models import ChecklistConfig
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    slugify_folder_part,
)
from app.checklists.yandex_project_structure import PROJECT_YANDEX_STRUCTURE_RELATIVE_PATHS

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



DEPRECATED_PROJECT_ROOT_FOLDER_NAMES = {
    "\u0030\u0031\u005f\u0420\u0430\u0431\u043e\u0447\u0430\u044f\u0020\u043f\u0430\u043f\u043a\u0430",
    "\u0030\u0033\u005f\u041e\u0431\u043c\u0435\u043d",
    "\u0030\u0034\u005f\u0042\u0049\u004d\u002d\u041c\u043e\u0434\u0435\u043b\u044c",
    "\u0030\u0035\u005f\u0410\u0440\u0445\u0438\u0432",
}


def is_deprecated_project_root_folder_path(path: str, project_root_path: str = "") -> bool:
    raw_path = normalize_yandex_context_path(path)
    root_path = normalize_yandex_context_path(project_root_path)

    if not raw_path:
        return False

    relative_path = raw_path

    if root_path and raw_path.startswith(root_path.rstrip("/") + "/"):
        relative_path = raw_path[len(root_path.rstrip("/")) + 1:]

    relative_path = relative_path.lstrip("/")

    first_part = relative_path.split("/", 1)[0].strip()

    return first_part in DEPRECATED_PROJECT_ROOT_FOLDER_NAMES


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


def normalize_project_structure_folder_alias(relative_path: str) -> str:
    return "project_" + slugify_folder_part(relative_path)


def get_folder_public_url_by_path(existing_folders: dict, folder_path: str) -> str:
    target_path = normalize_yandex_context_path(folder_path)

    for folder in (existing_folders or {}).values():
        if not isinstance(folder, dict):
            continue

        existing_path = normalize_yandex_context_path(folder.get("path"))
        if existing_path == target_path:
            return get_folder_public_url({"_": folder}, "_")

    return ""


def build_project_structure_yandex_folders(
    context: dict,
    result: dict,
    existing_folders: dict,
) -> dict:
    project_root_path = get_project_root_path(context)
    if not project_root_path:
        return result

    result = deepcopy(result or {})

    path_index = {}
    for alias, folder in result.items():
        if not isinstance(folder, dict):
            continue

        folder_path = normalize_yandex_context_path(folder.get("path"))
        if folder_path:
            path_index[folder_path] = alias

    for relative_path in PROJECT_YANDEX_STRUCTURE_RELATIVE_PATHS:
        relative_path = clean_cell_value(relative_path).replace("\\", "/").strip("/")
        if not relative_path:
            continue

        folder_path = join_yandex_path(project_root_path, relative_path)
        folder_alias = normalize_project_structure_folder_alias(relative_path)
        folder_name = folder_path.rstrip("/").rsplit("/", 1)[-1]

        if folder_alias in result:
            current = result.get(folder_alias) if isinstance(result.get(folder_alias), dict) else {}
            result[folder_alias] = {
                **current,
                "name": clean_cell_value(current.get("name")) or folder_name,
                "path": clean_cell_value(current.get("path")) or folder_path,
                "url": get_folder_public_url(existing_folders, folder_alias)
                    or clean_cell_value(current.get("url"))
                    or get_folder_public_url_by_path(existing_folders, folder_path),
                "isProjectStructure": True,
            }
            continue

        if folder_path in path_index:
            continue

        result[folder_alias] = {
            "name": folder_name,
            "path": folder_path,
            "url": get_folder_public_url(existing_folders, folder_alias)
                or get_folder_public_url_by_path(existing_folders, folder_path),
            "isProjectStructure": True,
        }

        path_index[folder_path] = folder_alias

    return result

def normalize_folder_spec_item_name(item_name: str, spec: dict) -> str:
    return clean_cell_value((spec or {}).get("itemName")) or clean_cell_value(item_name)


def normalize_folder_spec_group_id(spec: dict) -> int:
    try:
        return int((spec or {}).get("groupId") or 0)
    except (TypeError, ValueError):
        return 0


def build_config_yandex_folders(context: dict) -> dict:
    context = context or {}
    project_root_path = get_project_root_path(context)
    raw_existing_folders = get_existing_yandex_folders(context)

    existing_folders = {
        alias: folder
        for alias, folder in raw_existing_folders.items()
        if isinstance(folder, dict)
        and not is_deprecated_project_root_folder_path(
            folder.get("path"),
            project_root_path=project_root_path,
        )
    }

    result = deepcopy(existing_folders)

    # A standard item keeps the configured folder alias after a user rename.
    # The stored mapping is therefore the authoritative signal that an alias
    # now belongs to a different visible item name.  Without this index the
    # config hydration below restored the original path/name on every read.
    stored_mapping_by_alias: dict[str, dict] = {}
    for raw_mapping in context.get("itemMappings") or []:
        if not isinstance(raw_mapping, dict):
            continue
        mapping_alias = clean_cell_value(raw_mapping.get("folderAlias"))
        if mapping_alias:
            stored_mapping_by_alias[mapping_alias] = dict(raw_mapping)

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
            spec_item_name = normalize_folder_spec_item_name(item_name, spec)
            spec_group_id = normalize_folder_spec_group_id(spec)

            relative_path = (
                clean_cell_value(spec.get("relativePath"))
                or clean_cell_value(spec.get("folderName"))
                or clean_cell_value(item_name)
            )

            folder_path = join_yandex_path(root_path, relative_path)
            folder_name = normalize_folder_spec_name(spec_item_name, spec, folder_path)

            existing_record = result.get(folder_alias, {}) or {}
            stored_mapping = stored_mapping_by_alias.get(folder_alias) or {}
            stored_mapping_key = normalize_checklist_key(
                stored_mapping.get("checklistKey")
            )
            stored_mapping_name = clean_cell_value(
                stored_mapping.get("itemName")
            )
            stored_mapping_group = normalize_folder_spec_group_id(
                stored_mapping
            )
            user_renamed = bool(
                existing_record
                and clean_cell_value(existing_record.get("path"))
                and stored_mapping_key == config.key
                and stored_mapping_group == spec_group_id
                and stored_mapping_name
                and stored_mapping_name.casefold() != spec_item_name.casefold()
            ) or bool(existing_record.get("userRenamed"))

            if user_renamed:
                result[folder_alias] = {
                    **existing_record,
                    "checklistKey": config.key,
                    "itemName": stored_mapping_name
                    or clean_cell_value(existing_record.get("itemName"))
                    or spec_item_name,
                    "groupId": stored_mapping_group or spec_group_id,
                    "isStageRoot": False,
                    "userRenamed": True,
                }
                continue

            result[folder_alias] = {
                **existing_record,
                "name": folder_name,
                "path": folder_path,
                "url": get_folder_public_url(existing_folders, folder_alias),
                "checklistKey": config.key,
                "itemName": spec_item_name,
                "groupId": spec_group_id,
                "isStageRoot": False,
            }

    result = build_project_structure_yandex_folders(
        context=context,
        result=result,
        existing_folders=existing_folders,
    )

    result = {
        alias: folder
        for alias, folder in (result or {}).items()
        if isinstance(folder, dict)
        and not is_deprecated_project_root_folder_path(
            folder.get("path"),
            project_root_path=project_root_path,
        )
    }

    return result


def build_config_item_mappings() -> list[dict]:
    result = []

    for config in list_checklist_configs():
        specs = config.standard_yandex_folder_specs or {}

        for item_name, raw_spec in specs.items():
            spec = raw_spec or {}
            folder_alias = normalize_folder_spec_alias(config, item_name, spec)
            spec_item_name = normalize_folder_spec_item_name(item_name, spec)
            spec_group_id = normalize_folder_spec_group_id(spec)

            result.append({
                "checklistKey": config.key,
                "groupId": spec_group_id,
                "itemName": spec_item_name,
                "folderAlias": folder_alias,
            })

    return result


def mapping_identity(mapping: dict) -> tuple[str, int, str]:
    try:
        group_id = int((mapping or {}).get("groupId") or 0)
    except (TypeError, ValueError):
        group_id = 0

    return (
        normalize_checklist_key((mapping or {}).get("checklistKey")),
        group_id,
        clean_cell_value((mapping or {}).get("itemName")).lower(),
    )


def merge_item_mappings(existing_mappings: list, generated_mappings: list) -> list[dict]:
    result = []
    seen = set()
    seen_aliases = set()

    # The newest stored record for an alias wins.  Rename operations append the
    # replacement mapping, so walking backwards also repairs older contexts
    # that accidentally contain both the original and renamed item mappings.
    stored = []
    for mapping in reversed(existing_mappings or []):
        if not isinstance(mapping, dict):
            continue

        identity = mapping_identity(mapping)
        alias = clean_cell_value(mapping.get("folderAlias"))
        if (
            not identity[0]
            or not identity[1]
            or identity in seen
            or (alias and alias in seen_aliases)
        ):
            continue

        stored.append(dict(mapping))
        seen.add(identity)
        if alias:
            seen_aliases.add(alias)

    result.extend(reversed(stored))

    for mapping in generated_mappings or []:
        if not isinstance(mapping, dict):
            continue

        identity = mapping_identity(mapping)
        alias = clean_cell_value(mapping.get("folderAlias"))
        if (
            not identity[0]
            or not identity[1]
            or identity in seen
            or (alias and alias in seen_aliases)
        ):
            continue

        result.append(dict(mapping))
        seen.add(identity)
        if alias:
            seen_aliases.add(alias)

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