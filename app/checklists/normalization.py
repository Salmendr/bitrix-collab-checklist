import re

from app.checklists.registry import (
    get_project_checklists,
)

from app.checklists.config import get_checklist_config
from app.checklists.utils import (
    clean_cell_value,
    normalize_status,
    normalize_date_string,
    slugify_folder_part,
    normalize_checklist_key,
)

from app.checklists.documents import (
    migrate_legacy_document_fields,
    normalize_documents_list,    normalize_detached_archive_series,
)

YANDEX_FOLDER_STATUSES = frozenset({
    "queued", "running", "ready", "error", "conflict", "disabled"
})


def normalize_item_yandex_structure_fields(item: dict | None) -> dict:
    normalized = dict(item or {})

    local_folder_path = clean_cell_value(normalized.get("folderPath"))
    local_folder_url = clean_cell_value(normalized.get("folderUrl"))
    yandex_folder_path = clean_cell_value(normalized.get("yandexFolderPath"))
    yandex_folder_url = clean_cell_value(normalized.get("yandexFolderUrl"))

    # Stage 8.1 temporarily reused folderPath/folderUrl for Yandex metadata.
    # Migrate those values to dedicated fields without changing the local
    # item-folder URL contract.
    if local_folder_path.lower().startswith("disk:/"):
        yandex_folder_path = yandex_folder_path or local_folder_path
        local_folder_path = ""
    if (
        yandex_folder_path
        and local_folder_url.lower().startswith(("http://", "https://"))
        and "/api/checklist/folder" not in local_folder_url.lower()
    ):
        yandex_folder_url = yandex_folder_url or local_folder_url
        local_folder_url = ""

    status = clean_cell_value(normalized.get("yandexFolderStatus")).lower()
    if status == "completed":
        status = "ready"
    if status not in YANDEX_FOLDER_STATUSES:
        status = "ready" if (yandex_folder_path or yandex_folder_url) else ""

    error = clean_cell_value(normalized.get("yandexFolderError"))
    if status in {"queued", "running", "ready"}:
        error = ""

    normalized.update({
        "folderPath": local_folder_path,
        "folderUrl": local_folder_url,
        "yandexFolderStatus": status,
        "yandexFolderError": error,
        "yandexFolderPath": yandex_folder_path,
        "yandexFolderUrl": yandex_folder_url,
        "yandexFolderTargetPath": clean_cell_value(
            normalized.get("yandexFolderTargetPath")
        ),
        "yandexStructureJobId": clean_cell_value(
            normalized.get("yandexStructureJobId")
        ),
        "yandexStructureAction": clean_cell_value(
            normalized.get("yandexStructureAction")
        ),
        "yandexStructureUpdatedAt": clean_cell_value(
            normalized.get("yandexStructureUpdatedAt")
        ),
    })
    return normalized


def build_normalized_yandex_structure_fields(item: dict | None) -> dict:
    normalized = normalize_item_yandex_structure_fields(item)
    return {
        "yandexFolderStatus": normalized.get("yandexFolderStatus", ""),
        "yandexFolderError": normalized.get("yandexFolderError", ""),
        "yandexFolderPath": normalized.get("yandexFolderPath", ""),
        "yandexFolderUrl": normalized.get("yandexFolderUrl", ""),
        "yandexFolderTargetPath": normalized.get("yandexFolderTargetPath", ""),
        "yandexStructureJobId": normalized.get("yandexStructureJobId", ""),
        "yandexStructureAction": normalized.get("yandexStructureAction", ""),
        "yandexStructureUpdatedAt": normalized.get("yandexStructureUpdatedAt", ""),
    }


def build_not_required_return_fields(item: dict | None) -> dict:
    source = dict(item or {})

    try:
        return_group_id = int(source.get("notRequiredReturnGroupId") or 0)
    except (TypeError, ValueError):
        return_group_id = 0

    try:
        return_position = int(source.get("notRequiredReturnPosition") or 0)
    except (TypeError, ValueError):
        return_position = 0

    return {
        "notRequiredReturnGroupId": return_group_id,
        "notRequiredReturnPosition": return_position,
        "notRequiredReturnStatus": normalize_status(
            source.get("notRequiredReturnStatus")
        ),
        "notRequiredReturnPriority": clean_cell_value(
            source.get("notRequiredReturnPriority")
        ),
        "notRequiredReturnPlan": normalize_date_string(
            source.get("notRequiredReturnPlan")
        ),
        "notRequiredReturnFact": normalize_date_string(
            source.get("notRequiredReturnFact")
        ),
    }


def build_default_groups(checklist_key: str = "id") -> list[dict]:
    config = get_checklist_config(checklist_key)

    return [
        {
            "id": group.id,
            "title": group.title,
        }
        for group in config.groups
    ]


def extract_group_id_from_item_id(checklist_key: str, item_id: str) -> int | None:
    checklist_key = normalize_checklist_key(checklist_key)
    item_id = str(item_id or "").strip()

    if checklist_key == "id":
        prefix = "item_g"
    else:
        prefix = f"{checklist_key}_g"

    if not item_id.startswith(prefix):
        return None

    raw_group_part = item_id[len(prefix):].split("_", 1)[0]

    try:
        return int(raw_group_part)
    except Exception:
        return None


def resolve_required_group_id_by_item_id_or_name(checklist_key: str, item: dict) -> int:
    config = get_checklist_config(checklist_key)

    item = dict(item or {})
    item_id = str(item.get("id") or "").strip()
    name = clean_cell_value(item.get("name"))

    try:
        current_group = int(item.get("group") or 0)
    except (TypeError, ValueError):
        current_group = 0

    parsed_group_id = extract_group_id_from_item_id(config.key, item_id)

    if parsed_group_id in config.active_group_ids():
        return parsed_group_id

    if current_group in config.active_group_ids():
        current_config_group = next(
            (group for group in config.groups if group.id == current_group),
            None
        )

        if current_config_group:
            for order, item_name in enumerate(current_config_group.items, start=1):
                expected_id = build_standard_item_id(config, current_config_group.id, order)
                expected_name = clean_cell_value(item_name)

                if item_id == expected_id or name == expected_name:
                    return current_group

        return current_group

    for group in config.groups:
        if group.id == config.not_required_group_id:
            continue

        for order, item_name in enumerate(group.items, start=1):
            expected_id = build_standard_item_id(config, group.id, order)
            expected_name = clean_cell_value(item_name)

            if item_id == expected_id or name == expected_name:
                return group.id

    return config.default_group_id


def resolve_group_id(name: str) -> int:
    return resolve_required_group_id_by_item_id_or_name("id", {
        "name": name,
    })


def resolve_concept_group_id_by_item_id_or_name(item: dict) -> int:
    return resolve_required_group_id_by_item_id_or_name("concept", item)


def resolve_opr_group_id_by_item_id_or_name(item: dict) -> int:
    return resolve_required_group_id_by_item_id_or_name("opr", item)


def build_item_id(group_id: int, order: int) -> str:
    return f"item_g{group_id}_{order}"

def build_standard_item_id(config, group_id: int, order: int) -> str:
    if config.key == "id":
        return build_item_id(group_id, order)

    return f"{config.key}_g{group_id}_{order}"


def iter_standard_definition_items(config):
    for group in config.groups:
        if group.id == config.not_required_group_id:
            continue
        for order, name in enumerate(group.items, start=1):
            yield {
                "groupId": int(group.id),
                "order": int(order),
                "name": clean_cell_value(name),
                "itemId": build_standard_item_id(config, group.id, order),
            }


def resolve_standard_definition_identity(config, item: dict) -> tuple[int, str]:
    item = dict(item or {})
    if bool(item.get("isCustom", False)):
        return 0, ""

    stored_name = clean_cell_value(item.get("definitionName"))
    try:
        stored_group_id = int(item.get("definitionGroupId") or 0)
    except (TypeError, ValueError):
        stored_group_id = 0

    if stored_name:
        for definition in iter_standard_definition_items(config):
            if stored_group_id and definition["groupId"] != stored_group_id:
                continue
            if clean_cell_value(definition["name"]).casefold() == stored_name.casefold():
                return definition["groupId"], definition["name"]

    item_id = clean_cell_value(item.get("id"))
    for definition in iter_standard_definition_items(config):
        expected_id = definition["itemId"]
        if item_id == expected_id or item_id.startswith(expected_id + "_migrated_"):
            return definition["groupId"], definition["name"]

    current_name = clean_cell_value(item.get("name"))
    for definition in iter_standard_definition_items(config):
        if clean_cell_value(definition["name"]).casefold() == current_name.casefold():
            return definition["groupId"], definition["name"]

    return 0, ""


def build_standard_identity_fields(config, item: dict, display_name: str) -> dict:
    if bool((item or {}).get("isCustom", False)):
        return {
            "definitionName": "",
            "definitionGroupId": 0,
            "nameOverride": "",
        }

    definition_group_id, definition_name = resolve_standard_definition_identity(
        config,
        item,
    )
    if not definition_name:
        return {
            "definitionName": "",
            "definitionGroupId": 0,
            "nameOverride": "",
        }

    display = clean_cell_value(display_name) or definition_name
    override = "" if display.casefold() == definition_name.casefold() else display
    return {
        "definitionName": definition_name,
        "definitionGroupId": int(definition_group_id or 0),
        "nameOverride": override,
    }


def iter_default_config_items(config):
    for group in config.groups:
        if group.id == config.not_required_group_id:
            continue

        for order, name in enumerate(group.items, start=1):
            yield group.id, order, name


def build_default_item_record(config, group_id: int, order: int, name: str) -> dict:
    item_id = build_standard_item_id(config, group_id, order)

    return {
        "id": item_id,
        "group": group_id,
        "order": order,
        "name": name,
        "priority": "white",
        "status": "",
        "plan": "",
        "fact": "",
        "folderKey": build_folder_key(config.key, name, item_id),
        "folderPath": "",
        "folderUrl": "",
        **build_normalized_yandex_structure_fields({}),
        **build_not_required_return_fields({}),
        "documents": [],
        "archivedDocumentSeries": [],
        "documentUrl": "",
        "documentName": "",
        "isCustom": False,
        "definitionName": clean_cell_value(name),
        "definitionGroupId": int(group_id or 0),
        "nameOverride": "",
    }

def build_project_checklists():
    return get_project_checklists()

def build_current_default_name_set(checklist_key: str = "id") -> set[str]:
    config = get_checklist_config(checklist_key)
    result = set()

    for group in config.groups:
        if group.id == config.not_required_group_id:
            continue

        for item_name in group.items:
            normalized_name = clean_cell_value(item_name).lower()
            if normalized_name:
                result.add(normalized_name)

    return result


def build_normalized_checklist_payload(
    data: dict,
    config,
    normalized_items: list,
    progress: dict,
) -> dict:
    return {
        "title": data.get("title") or config.title,
        "checklistKey": config.key,
        "collabTitle": clean_cell_value(data.get("collabTitle")),
        "contractDeadline": "",
        "startDate": "",
        "groups": build_default_groups(config.key),
        "projectChecklists": build_project_checklists(),
        "items": normalized_items,
        "notice": data.get("notice", ""),
        "activeCount": progress["activeCount"],
        "completedCount": progress["completedCount"],
        "progressPercent": progress["progressPercent"],
    }

def derive_indicator_from_status(status: str) -> str:
    status = normalize_status(status)
    if status == "Есть":
        return "green"
    if status == "Нет" or status == "Не требуется":
        return "gray"
    return "white"


def move_item_to_required_group(item: dict) -> int:
    config = get_checklist_config("id")
    status = normalize_status(item.get("status"))

    if status == "Не требуется":
        return config.not_required_group_id

    current_group = int(item.get("group") or 0)

    if current_group in config.active_group_ids():
        return current_group

    return resolve_required_group_id_by_item_id_or_name(config.key, item)


def build_folder_key(checklist_key: str, item_name: str, item_id: str = "") -> str:
    checklist_key = normalize_checklist_key(checklist_key)
    raw_name = clean_cell_value(item_name)
    raw_item_id = str(item_id or "").strip()

    special_map = {
        ("id", "Тех задание"): "id_tech_task",
    }

    special_key = (checklist_key, raw_name)
    if special_key in special_map:
        return special_map[special_key]

    if raw_item_id:
        safe_item_id = re.sub(r"[^a-zA-Z0-9_]+", "_", raw_item_id).strip("_").lower()
        if safe_item_id:
            return f"{checklist_key}_{safe_item_id}"

    return f"{checklist_key}_{slugify_folder_part(raw_name)}"

def normalize_id_builtin_name(name: str) -> str:
    value = clean_cell_value(name)

    rename_map = {
        "ТУ ТС": "ТУ Тепловые сети",
        "ТУ Свет": "ТУ Электроснабжение",
        "ТУ СС": "ТУ Сети связи",
        "ТУ Ливневка": "ТУ Ливневая канализация",
        "ТУ Газ": "ТУ Газоснабжение",
        "Аэропорт": "Согласование с Аэропортом",
        "Примыкание ОДД": "Примыкание к УДС",
        "Расположение пож гидрантов": "Расположение пожарных гидрантов",
    }

    return rename_map.get(value, value)


def build_current_id_default_name_set() -> set[str]:
    return build_current_default_name_set("id")


def build_default_checklist_template(dialog_id: str = "", checklist_key: str = "id"):
    config = get_checklist_config(checklist_key)

    items = [
        build_default_item_record(config, group_id, order, name)
        for group_id, order, name in iter_default_config_items(config)
    ]

    return normalize_checklist_data({
        "title": config.title,
        "checklistKey": config.key,
        "collabTitle": "",
        "contractDeadline": "",
        "startDate": "",
        "groups": build_default_groups(config.key),
        "items": items,
        "notice": "",
    }, config.key)


def calculate_progress(items: list) -> dict:
    items = items or []

    active_items = [x for x in items if normalize_status(x.get("status")) != "Не требуется"]
    completed_items = [x for x in active_items if normalize_status(x.get("status")) == "Есть"]

    active_count = len(active_items)
    completed_count = len(completed_items)

    progress_percent = 0
    if active_count > 0:
        progress_percent = round((completed_count / active_count) * 100)

    return {
        "activeCount": active_count,
        "completedCount": completed_count,
        "progressPercent": progress_percent,
    }


def calculate_concept_progress(items: list) -> dict:
    return calculate_progress(items)


def calculate_opr_progress(items: list) -> dict:
    items = items or []

    active_items = [item for item in items if normalize_status(item.get("status")) != "Не требуется"]
    completed_items = [item for item in active_items if normalize_status(item.get("status")) == "Есть"]

    active_count = len(active_items)
    completed_count = len(completed_items)
    progress_percent = round((completed_count / active_count) * 100) if active_count else 0

    return {
        "activeCount": active_count,
        "completedCount": completed_count,
        "progressPercent": progress_percent,
    }


def normalize_checklist_data(data: dict, checklist_key: str = "id") -> dict:
    data = dict(data or {})
    checklist_key = normalize_checklist_key(checklist_key or data.get("checklistKey") or "id")
    config = get_checklist_config(checklist_key)

    def prepare_item_common(item: dict, default_name: str = "") -> tuple[dict, list[dict], dict, str, str, str]:
        item = migrate_legacy_document_fields(dict(item or {}))
        item = normalize_item_yandex_structure_fields(item)

        name = clean_cell_value(item.get("name")) or clean_cell_value(default_name)
        documents = normalize_documents_list(item.get("documents"))
        first_doc = documents[0] if documents else {}

        folder_key = clean_cell_value(item.get("folderKey")) or build_folder_key(
            checklist_key,
            name,
            item.get("id") or ""
        )
        folder_path = clean_cell_value(item.get("folderPath"))
        folder_url = clean_cell_value(item.get("folderUrl"))

        legacy_document_url = clean_cell_value(item.get("documentUrl")) or clean_cell_value(first_doc.get("fileUrl"))
        legacy_document_name = clean_cell_value(item.get("documentName")) or clean_cell_value(first_doc.get("name"))

        return item, documents, first_doc, folder_key, folder_path, folder_url, legacy_document_url, legacy_document_name

    def normalize_config_table_items(config) -> list[dict]:
        raw_items = data.get("items", []) or []
        normalized_items = []
        default_id_order = {}

        for definition in iter_standard_definition_items(config):
            default_id_order[(
                int(definition["groupId"]),
                clean_cell_value(definition["name"]).casefold(),
            )] = int(definition["order"])

        for raw_item in raw_items:
            item, documents, first_doc, folder_key, folder_path, folder_url, legacy_document_url, legacy_document_name = prepare_item_common(raw_item)
            display_name = clean_cell_value(item.get("name"))
            if not display_name:
                continue

            is_custom = bool(item.get("isCustom", False))
            definition_group_id, definition_name = resolve_standard_definition_identity(
                config,
                item,
            )
            if not is_custom and not definition_name:
                continue

            status = normalize_status(item.get("status"))
            try:
                raw_group_id = int(item.get("group") or 0)
            except (TypeError, ValueError):
                raw_group_id = 0

            if is_custom:
                required_group_id = resolve_required_group_id_by_item_id_or_name(
                    config.key,
                    item,
                )
            else:
                required_group_id = int(definition_group_id or 0)

            if status == "Не требуется":
                group_id = config.not_required_group_id
            elif raw_group_id == config.not_required_group_id or not raw_group_id:
                group_id = required_group_id or config.default_group_id
            elif raw_group_id not in config.active_group_ids():
                group_id = required_group_id or config.default_group_id
            else:
                group_id = raw_group_id

            identity_fields = build_standard_identity_fields(
                config,
                item,
                display_name,
            )

            normalized_items.append({
                "id": str(item.get("id") or ""),
                "group": group_id,
                "order": int(item.get("order") or 0),
                "name": display_name,
                "priority": derive_indicator_from_status(status),
                "status": status,
                "plan": normalize_date_string(item.get("plan") or item.get("plannedDate")),
                "fact": normalize_date_string(item.get("fact")),
                "folderKey": folder_key,
                "folderPath": folder_path,
                "folderUrl": folder_url,
                **build_normalized_yandex_structure_fields(item),
                **build_not_required_return_fields(item),
                "documents": documents,
                "archivedDocumentSeries": normalize_detached_archive_series(
                    item.get("archivedDocumentSeries")
                ),
                "documentUrl": legacy_document_url,
                "documentName": legacy_document_name,
                "isCustom": is_custom,
                **identity_fields,
                "_requiredGroupId": required_group_id,
            })

        deduped_items = []
        seen_builtin_identities = set()
        for existing_item in normalized_items:
            if not existing_item.get("isCustom"):
                identity = (
                    int(existing_item.get("definitionGroupId") or 0),
                    clean_cell_value(existing_item.get("definitionName")).casefold(),
                )
                if identity in seen_builtin_identities:
                    continue
                seen_builtin_identities.add(identity)
            deduped_items.append(existing_item)
        normalized_items = deduped_items

        existing_default_identities = {
            (
                int(item.get("definitionGroupId") or 0),
                clean_cell_value(item.get("definitionName")).casefold(),
            )
            for item in normalized_items
            if not item.get("isCustom")
            and clean_cell_value(item.get("definitionName"))
        }

        for definition in iter_standard_definition_items(config):
            identity = (
                int(definition["groupId"]),
                clean_cell_value(definition["name"]).casefold(),
            )
            if identity in existing_default_identities:
                continue
            migrated_item_id = (
                f'{definition["itemId"]}_migrated_'
                f'{slugify_folder_part(definition["name"])}'
            )
            normalized_items.append({
                "id": migrated_item_id,
                "group": int(definition["groupId"]),
                "order": int(definition["order"]),
                "name": definition["name"],
                "priority": "white",
                "status": "",
                "plan": "",
                "fact": "",
                "folderKey": build_folder_key(
                    config.key,
                    definition["name"],
                    migrated_item_id,
                ),
                "folderPath": "",
                "folderUrl": "",
                **build_normalized_yandex_structure_fields({}),
                **build_not_required_return_fields({}),
                "documents": [],
                "archivedDocumentSeries": [],
                "documentUrl": "",
                "documentName": "",
                "isCustom": False,
                "definitionName": definition["name"],
                "definitionGroupId": int(definition["groupId"]),
                "nameOverride": "",
                "_requiredGroupId": int(definition["groupId"]),
            })
            existing_default_identities.add(identity)

        normalized_items.sort(
            key=lambda item: (
                int(item.get("group") or 0),
                int(item.get("order") or 10000),
                default_id_order.get(
                    (
                        int(item.get("definitionGroupId") or item.get("_requiredGroupId") or item.get("group") or 0),
                        clean_cell_value(item.get("definitionName") or item.get("name")).casefold(),
                    ),
                    10000,
                ),
                clean_cell_value(item.get("name")),
            )
        )

        for group in config.groups:
            group_items = [item for item in normalized_items if item["group"] == group.id]
            for order, item in enumerate(group_items, start=1):
                item["order"] = order
                if not item.get("id"):
                    item["id"] = build_standard_item_id(config, group.id, order)
                if not item.get("folderKey"):
                    item["folderKey"] = build_folder_key(
                        config.key,
                        item.get("name"),
                        item.get("id"),
                    )

        for item in normalized_items:
            item.pop("_requiredGroupId", None)
        return normalized_items


    if checklist_key != "id":
        normalized_items = normalize_config_table_items(config)
        progress = calculate_progress(normalized_items)
        return build_normalized_checklist_payload(data, config, normalized_items, progress)

    config = get_checklist_config("id")
    raw_items = data.get("items", []) or []
    normalized_items = []
    group_order_counters = {group.id: 0 for group in config.groups}
    default_id_order = {
        (
            int(definition["groupId"]),
            clean_cell_value(definition["name"]).casefold(),
        ): int(definition["order"])
        for definition in iter_standard_definition_items(config)
    }

    for raw_item in raw_items:
        item, documents, first_doc, folder_key, folder_path, folder_url, legacy_document_url, legacy_document_name = prepare_item_common(raw_item)
        is_custom = bool(item.get("isCustom", False))
        raw_name = clean_cell_value(item.get("name"))
        display_name = raw_name if is_custom else normalize_id_builtin_name(raw_name)
        if not display_name:
            continue

        definition_group_id, definition_name = resolve_standard_definition_identity(
            config,
            {**item, "name": display_name},
        )
        if not is_custom and not definition_name:
            continue

        status = normalize_status(item.get("status"))
        if is_custom:
            required_group_id = resolve_required_group_id_by_item_id_or_name(
                config.key,
                {**item, "name": display_name},
            )
        else:
            required_group_id = int(definition_group_id or config.default_group_id)

        try:
            current_group_id = int(item.get("group") or 0)
        except (TypeError, ValueError):
            current_group_id = 0

        if status == "Не требуется":
            target_group_id = config.not_required_group_id
        elif current_group_id == config.not_required_group_id or not current_group_id:
            target_group_id = required_group_id
        elif current_group_id not in config.active_group_ids():
            target_group_id = required_group_id
        else:
            target_group_id = current_group_id

        group_order_counters[target_group_id] = group_order_counters.get(target_group_id, 0) + 1
        default_order = group_order_counters[target_group_id]
        identity_fields = build_standard_identity_fields(
            config,
            {**item, "name": display_name},
            display_name,
        )
        normalized_items.append({
            "id": str(item.get("id") or build_item_id(target_group_id, default_order)),
            "group": target_group_id,
            "order": int(item.get("order") or default_order),
            "name": display_name,
            "priority": derive_indicator_from_status(status),
            "status": status,
            "plan": normalize_date_string(item.get("plan")),
            "fact": normalize_date_string(item.get("fact")),
            "folderKey": folder_key,
            "folderPath": folder_path,
            "folderUrl": folder_url,
            **build_normalized_yandex_structure_fields(item),
            **build_not_required_return_fields(item),
            "documents": documents,
            "archivedDocumentSeries": normalize_detached_archive_series(
                item.get("archivedDocumentSeries")
            ),
            "documentUrl": legacy_document_url,
            "documentName": legacy_document_name,
            "isCustom": is_custom,
            **identity_fields,
        })

    deduped_items = []
    seen_builtin_identities = set()
    for existing_item in normalized_items:
        if not existing_item.get("isCustom"):
            identity = (
                int(existing_item.get("definitionGroupId") or 0),
                clean_cell_value(existing_item.get("definitionName")).casefold(),
            )
            if identity in seen_builtin_identities:
                continue
            seen_builtin_identities.add(identity)
        deduped_items.append(existing_item)
    normalized_items = deduped_items

    existing_default_identities = {
        (
            int(item.get("definitionGroupId") or 0),
            clean_cell_value(item.get("definitionName")).casefold(),
        )
        for item in normalized_items
        if not item.get("isCustom") and clean_cell_value(item.get("definitionName"))
    }

    for definition in iter_standard_definition_items(config):
        identity = (
            int(definition["groupId"]),
            clean_cell_value(definition["name"]).casefold(),
        )
        if identity in existing_default_identities:
            continue
        migrated_item_id = (
            f'{definition["itemId"]}_migrated_'
            f'{slugify_folder_part(definition["name"])}'
        )
        normalized_items.append({
            "id": migrated_item_id,
            "group": int(definition["groupId"]),
            "order": int(definition["order"]),
            "name": definition["name"],
            "priority": "white",
            "status": "",
            "plan": "",
            "fact": "",
            "folderKey": build_folder_key(config.key, definition["name"], migrated_item_id),
            "folderPath": "",
            "folderUrl": "",
            **build_normalized_yandex_structure_fields({}),
            **build_not_required_return_fields({}),
            "documents": [],
            "archivedDocumentSeries": [],
            "documentUrl": "",
            "documentName": "",
            "isCustom": False,
            "definitionName": definition["name"],
            "definitionGroupId": int(definition["groupId"]),
            "nameOverride": "",
        })
        existing_default_identities.add(identity)

    normalized_items.sort(
        key=lambda item: (
            int(item.get("group") or 0),
            int(item.get("order") or 10000),
            default_id_order.get(
                (
                    int(item.get("definitionGroupId") or item.get("group") or 0),
                    clean_cell_value(item.get("definitionName") or item.get("name")).casefold(),
                ),
                10000,
            ),
            clean_cell_value(item.get("name")),
        )
    )

    for group in config.groups:
        group_items = [item for item in normalized_items if item["group"] == group.id]
        for order, item in enumerate(group_items, start=1):
            item["order"] = order
            if not item.get("id"):
                item["id"] = build_standard_item_id(config, group.id, order)
            if not item.get("folderKey"):
                item["folderKey"] = build_folder_key(
                    config.key,
                    item.get("name"),
                    item.get("id"),
                )
    progress = calculate_progress(normalized_items)

    return build_normalized_checklist_payload(data, config, normalized_items, progress)
