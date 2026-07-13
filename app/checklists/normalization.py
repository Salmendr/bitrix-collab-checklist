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
    normalize_documents_list,
)

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
        "documents": [],
        "documentUrl": "",
        "documentName": "",
        "isCustom": False,
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
        current_default_names = build_current_default_name_set(config.key)

        default_id_order = {}
        default_identities = set()

        for group in config.groups:
            if group.id == config.not_required_group_id:
                continue

            for default_order, default_name in enumerate(group.items, start=1):
                normalized_name = clean_cell_value(default_name).lower()
                if not normalized_name:
                    continue

                default_identity = (group.id, normalized_name)
                default_identities.add(default_identity)
                default_id_order[default_identity] = default_order

        for raw_item in raw_items:
            item, documents, first_doc, folder_key, folder_path, folder_url, legacy_document_url, legacy_document_name = prepare_item_common(raw_item)

            name = clean_cell_value(item.get("name"))
            if not name:
                continue

            is_custom = bool(item.get("isCustom", False))
            normalized_name = name.lower()

            if not is_custom and normalized_name not in current_default_names:
                continue

            status = normalize_status(item.get("status"))

            try:
                raw_group_id = int(item.get("group") or 0)
            except (TypeError, ValueError):
                raw_group_id = 0

            required_group_id = resolve_required_group_id_by_item_id_or_name(config.key, {
                **item,
                "name": name,
                "group": raw_group_id,
            })

            if status == "Не требуется":
                group_id = config.not_required_group_id
            elif raw_group_id == config.not_required_group_id or not raw_group_id:
                group_id = required_group_id
            elif raw_group_id not in config.active_group_ids():
                group_id = required_group_id
            else:
                group_id = raw_group_id

            normalized_items.append({
                "id": str(item.get("id") or ""),
                "group": group_id,
                "order": int(item.get("order") or 0),
                "name": name,
                "priority": derive_indicator_from_status(status),
                "status": status,
                "plan": normalize_date_string(item.get("plan") or item.get("plannedDate")),
                "fact": normalize_date_string(item.get("fact")),
                "folderKey": folder_key,
                "folderPath": folder_path,
                "folderUrl": folder_url,
                "documents": documents,
                "documentUrl": legacy_document_url,
                "documentName": legacy_document_name,
                "isCustom": is_custom,
                "_requiredGroupId": required_group_id,
            })

        deduped_items = []
        seen_builtin_identities = set()

        for existing_item in normalized_items:
            name_key = clean_cell_value(existing_item.get("name")).lower()

            if not existing_item.get("isCustom"):
                try:
                    identity_group_id = int(existing_item.get("_requiredGroupId") or 0)
                except (TypeError, ValueError):
                    identity_group_id = 0

                if not identity_group_id:
                    identity_group_id = resolve_required_group_id_by_item_id_or_name(config.key, existing_item)

                identity = (identity_group_id, name_key)

                if identity in seen_builtin_identities:
                    continue

                seen_builtin_identities.add(identity)

            deduped_items.append(existing_item)

        normalized_items = deduped_items

        existing_default_identities = set()

        for existing_item in normalized_items:
            if existing_item.get("isCustom"):
                continue

            name_key = clean_cell_value(existing_item.get("name")).lower()
            if not name_key:
                continue

            try:
                identity_group_id = int(existing_item.get("_requiredGroupId") or 0)
            except (TypeError, ValueError):
                identity_group_id = 0

            if not identity_group_id:
                identity_group_id = resolve_required_group_id_by_item_id_or_name(config.key, existing_item)

            existing_default_identities.add((identity_group_id, name_key))

        for group in config.groups:
            if group.id == config.not_required_group_id:
                continue

            for default_order, default_name in enumerate(group.items, start=1):
                normalized_name = clean_cell_value(default_name).lower()
                default_identity = (group.id, normalized_name)

                if not normalized_name or default_identity in existing_default_identities:
                    continue

                migrated_item_id = f"{build_standard_item_id(config, group.id, default_order)}_migrated_{slugify_folder_part(default_name)}"

                normalized_items.append({
                    "id": migrated_item_id,
                    "group": group.id,
                    "order": default_order,
                    "name": default_name,
                    "priority": "white",
                    "status": "",
                    "plan": "",
                    "fact": "",
                    "folderKey": build_folder_key(config.key, default_name, migrated_item_id),
                    "folderPath": "",
                    "folderUrl": "",
                    "documents": [],
                    "documentUrl": "",
                    "documentName": "",
                    "isCustom": False,
                    "_requiredGroupId": group.id,
                })

                existing_default_identities.add(default_identity)

        normalized_items.sort(
            key=lambda x: (
                int(x.get("group") or 0),
                default_id_order.get(
                    (
                        int(x.get("_requiredGroupId") or x.get("group") or 0),
                        clean_cell_value(x.get("name")).lower(),
                    ),
                    10000,
                ),
                int(x.get("order") or 0),
                clean_cell_value(x.get("name")),
            )
        )

        for group in config.groups:
            group_items = [x for x in normalized_items if x["group"] == group.id]
            for order, item in enumerate(group_items, start=1):
                item["order"] = order
                if not item.get("id"):
                    item["id"] = build_standard_item_id(config, group.id, order)
                if not item.get("folderKey"):
                    item["folderKey"] = build_folder_key(config.key, item.get("name"), item.get("id"))

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

    group_order_counters = {
        group.id: 0
        for group in config.groups
    }
    current_id_default_names = build_current_default_name_set(config.key)

    for raw_item in raw_items:
        item, documents, first_doc, folder_key, folder_path, folder_url, legacy_document_url, legacy_document_name = prepare_item_common(raw_item)

        item_name = normalize_id_builtin_name(item.get("name"))
        if not item_name:
            continue

        is_custom = bool(item.get("isCustom", False))
        status = normalize_status(item.get("status"))
        target_group_id = move_item_to_required_group({
            "group": item.get("group"),
            "name": item_name,
            "status": status,
        })

        if (
            not is_custom
            and target_group_id != config.not_required_group_id
            and item_name.lower() not in current_id_default_names
        ):
            continue

        group_order_counters[target_group_id] += 1
        default_order = group_order_counters[target_group_id]

        normalized_items.append({
            "id": str(item.get("id") or build_item_id(target_group_id, default_order)),
            "group": target_group_id,
            "order": int(item.get("order") or default_order),
            "name": item_name,
            "priority": derive_indicator_from_status(status),
            "status": status,
            "plan": normalize_date_string(item.get("plan")),
            "fact": normalize_date_string(item.get("fact")),
            "folderKey": folder_key,
            "folderPath": folder_path,
            "folderUrl": folder_url,
            "documents": documents,
            "documentUrl": legacy_document_url,
            "documentName": legacy_document_name,
            "isCustom": is_custom,
        })

    deduped_items = []
    seen_builtin_names = set()

    for existing_item in normalized_items:
        name_key = clean_cell_value(existing_item.get("name")).lower()

        if (
            not existing_item.get("isCustom")
            and int(existing_item.get("group") or 0) != config.not_required_group_id
        ):
            if name_key in seen_builtin_names:
                continue
            seen_builtin_names.add(name_key)

        deduped_items.append(existing_item)

    normalized_items = deduped_items

    existing_names = {
        clean_cell_value(existing_item.get("name")).lower()
        for existing_item in normalized_items
        if clean_cell_value(existing_item.get("name"))
    }

    default_id_order = {}

    for group in config.groups:
        if group.id == config.not_required_group_id:
            continue

        for default_order, default_name in enumerate(group.items, start=1):
            normalized_name = clean_cell_value(default_name).lower()
            default_id_order[(group.id, normalized_name)] = default_order

            if not normalized_name or normalized_name in existing_names:
                continue

            migrated_item_id = f"{build_standard_item_id(config, group.id, default_order)}_migrated_{slugify_folder_part(default_name)}"

            normalized_items.append({
                "id": migrated_item_id,
                "group": group.id,
                "order": default_order,
                "name": default_name,
                "priority": "white",
                "status": "",
                "plan": "",
                "fact": "",
                "folderKey": build_folder_key(config.key, default_name, migrated_item_id),
                "folderPath": "",
                "folderUrl": "",
                "documents": [],
                "documentUrl": "",
                "documentName": "",
                "isCustom": False,
            })

            existing_names.add(normalized_name)

    normalized_items.sort(
        key=lambda x: (
            x["group"],
            default_id_order.get(
                (int(x["group"]), clean_cell_value(x.get("name")).lower()),
                10000
            ),
            x["order"],
            x["name"],
        )
    )

    for group in config.groups:
        group_items = [x for x in normalized_items if x["group"] == group.id]
        for order, item in enumerate(group_items, start=1):
            item["order"] = order
            if not item.get("id"):
                item["id"] = build_standard_item_id(config, group.id, order)
            if not item.get("folderKey"):
                item["folderKey"] = build_folder_key(config.key, item.get("name"), item.get("id"))
    progress = calculate_progress(normalized_items)

    return build_normalized_checklist_payload(data, config, normalized_items, progress)
