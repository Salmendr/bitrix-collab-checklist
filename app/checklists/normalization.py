import re

from app.checklists.registry import (
    ID_GROUPS as CHECKLIST_GROUPS,
    OPR_GROUPS,
    CONCEPT_GROUPS,
    get_project_checklists,
)

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

def build_default_groups(checklist_key: str = "id"):
    if checklist_key == "concept":
        return [{"id": group["id"], "title": group["title"]} for group in CONCEPT_GROUPS]
    if checklist_key == "opr":
        return [{"id": group["id"], "title": group["title"]} for group in OPR_GROUPS]

    return [
        {"id": group_id, "title": group_data["title"]}
        for group_id, group_data in CHECKLIST_GROUPS.items()
    ]


def resolve_group_id(name: str) -> int:
    name = str(name or "").strip()

    for group_id, group_data in CHECKLIST_GROUPS.items():
        if group_id == 4:
            continue
        if name in group_data["items"]:
            return group_id

    return 3


def resolve_concept_group_id_by_item_id_or_name(item: dict) -> int:
    item_id = str(item.get("id") or "")
    name = str(item.get("name") or "").strip()

    if item_id.startswith("concept_g"):
        parts = item_id.split("_", 2)
        if len(parts) > 1 and parts[1].startswith("g"):
            try:
                group_id = int(parts[1][1:])
                if group_id != 10:
                    return group_id
            except Exception:
                pass

    for group in CONCEPT_GROUPS:
        if group["id"] == 10:
            continue

        for order, item_name in enumerate(group["items"], start=1):
            expected_id = f"concept_g{group['id']}_{order}"
            if item_id == expected_id or name == clean_cell_value(item_name):
                return group["id"]

    current_group = int(item.get("group") or 1)
    return 1 if current_group == 10 else current_group


def resolve_opr_group_id_by_item_id_or_name(item: dict) -> int:
    item_id = str(item.get("id") or "")
    name = str(item.get("name") or "").strip()

    if item_id.startswith("opr_g"):
        parts = item_id.split("_", 2)
        if len(parts) > 1 and parts[1].startswith("g"):
            try:
                group_id = int(parts[1][1:])
                if group_id == 2:
                    return 2
            except Exception:
                pass

    for group in OPR_GROUPS:
        if group["id"] == 2:
            continue

        for order, item_name in enumerate(group["items"], start=1):
            expected_id = f"opr_g{group['id']}_{order}"
            if item_id == expected_id or name == item_name:
                return group["id"]

    current_group = int(item.get("group") or 1)
    return 1 if current_group == 2 else current_group


def build_item_id(group_id: int, order: int) -> str:
    return f"item_g{group_id}_{order}"


def build_project_checklists():
    return get_project_checklists()


def derive_indicator_from_status(status: str) -> str:
    status = normalize_status(status)
    if status == "Есть":
        return "green"
    if status == "Нет" or status == "Не требуется":
        return "gray"
    return "white"


def move_item_to_required_group(item: dict) -> int:
    status = normalize_status(item.get("status"))
    if status == "Не требуется":
        return 4

    current_group = item.get("group")
    if isinstance(current_group, int) and current_group in [1, 2, 3]:
        return current_group

    return resolve_group_id(item.get("name"))


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
    result = set()

    for group_id, group_data in CHECKLIST_GROUPS.items():
        if group_id == 4:
            continue

        for item_name in group_data["items"]:
            normalized_name = clean_cell_value(item_name).lower()
            if normalized_name:
                result.add(normalized_name)

    return result


def build_default_checklist_template(dialog_id: str = "", checklist_key: str = "id"):
    items = []

    if checklist_key == "concept":
        for group in CONCEPT_GROUPS:
            if group["id"] == 10:
                continue

            for order, name in enumerate(group["items"], start=1):
                item_id = f"concept_g{group['id']}_{order}"

                items.append({
                    "id": item_id,
                    "group": group["id"],
                    "order": order,
                    "name": name,
                    "priority": "white",
                    "status": "",
                    "plan": "",
                    "fact": "",
                    "folderKey": build_folder_key("concept", name, item_id),
                    "folderPath": "",
                    "folderUrl": "",
                    "documents": [],
                    "documentUrl": "",
                    "documentName": "",
                    "isCustom": False,
                })

        return normalize_checklist_data({
            "title": "Чек-лист Концепция",
            "checklistKey": "concept",
            "collabTitle": "",
            "contractDeadline": "",
            "startDate": "",
            "groups": build_default_groups("concept"),
            "items": items,
            "notice": "",
        }, "concept")

    if checklist_key == "opr":
        for group in OPR_GROUPS:
            if group["id"] == 2:
                continue

            for order, name in enumerate(group["items"], start=1):
                item_id = f"opr_g{group['id']}_{order}"

                items.append({
                    "id": item_id,
                    "group": group["id"],
                    "order": order,
                    "name": name,
                    "priority": "white",
                    "status": "",
                    "plan": "",
                    "fact": "",
                    "folderKey": build_folder_key("opr", name, item_id),
                    "folderPath": "",
                    "folderUrl": "",
                    "documents": [],
                    "documentUrl": "",
                    "documentName": "",
                    "isCustom": False,
                })

        return normalize_checklist_data({
            "title": "Чек-лист ОПР",
            "checklistKey": "opr",
            "collabTitle": "",
            "contractDeadline": "",
            "startDate": "",
            "groups": build_default_groups("opr"),
            "items": items,
            "notice": "",
        }, "opr")
    
    for group_id, group_data in CHECKLIST_GROUPS.items():
        if group_id == 4:
            continue

        for order, name in enumerate(group_data["items"], start=1):
            item_id = build_item_id(group_id, order)

            items.append({
                "id": item_id,
                "group": group_id,
                "order": order,
                "name": name,
                "priority": "white",
                "status": "",
                "plan": "",
                "fact": "",
                "folderKey": build_folder_key("id", name, item_id),
                "folderPath": "",
                "folderUrl": "",
                "documents": [],
                "isCustom": False,
            })

    return normalize_checklist_data({
        "title": "Чек-лист ИД",
        "checklistKey": "id",
        "collabTitle": "",
        "contractDeadline": "",
        "startDate": "",
        "groups": build_default_groups("id"),
        "items": items,
        "notice": "",
    }, "id")


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
    checklist_key = str(checklist_key or data.get("checklistKey") or "id").strip() or "id"

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

    if checklist_key == "concept":
        raw_items = data.get("items", []) or []
        normalized_items = []

        current_concept_default_names = {
            clean_cell_value(item_name).lower()
            for group in CONCEPT_GROUPS
            if group["id"] != 10
            for item_name in group["items"]
            if clean_cell_value(item_name)
        }

        for raw_item in raw_items:
            item, documents, first_doc, folder_key, folder_path, folder_url, legacy_document_url, legacy_document_name = prepare_item_common(raw_item)

            name = clean_cell_value(item.get("name"))
            if not name:
                continue

            is_custom = bool(item.get("isCustom", False))
            normalized_name = name.lower()

            # После упрощения Концепции оставляем:
            # 1) текущие стандартные пункты из concept.py
            # 2) кастомные пункты, если пользователь добавил их вручную
            if not is_custom and normalized_name not in current_concept_default_names:
                continue

            status = normalize_status(item.get("status"))
            group_id = int(item.get("group") or 0)

            if status == "Не требуется":
                group_id = 10
            elif group_id == 10 or not group_id:
                group_id = resolve_concept_group_id_by_item_id_or_name(item)

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
            })

        deduped_items = []
        seen_builtin_names = set()

        for existing_item in normalized_items:
            name_key = clean_cell_value(existing_item.get("name")).lower()

            if not existing_item.get("isCustom"):
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

        for group in CONCEPT_GROUPS:
            if group["id"] == 10:
                continue

            for default_order, default_name in enumerate(group["items"], start=1):
                normalized_name = clean_cell_value(default_name).lower()
                if not normalized_name or normalized_name in existing_names:
                    continue

                migrated_item_id = f"concept_g{group['id']}_{default_order}_migrated_{slugify_folder_part(default_name)}"

                normalized_items.append({
                    "id": migrated_item_id,
                    "group": group["id"],
                    "order": default_order,
                    "name": default_name,
                    "priority": "white",
                    "status": "",
                    "plan": "",
                    "fact": "",
                    "folderKey": build_folder_key("concept", default_name, migrated_item_id),
                    "folderPath": "",
                    "folderUrl": "",
                    "documents": [],
                    "documentUrl": "",
                    "documentName": "",
                    "isCustom": False,
                })

                existing_names.add(normalized_name)

        normalized_items.sort(key=lambda x: (x["group"], x["order"], x["name"]))

        for group in CONCEPT_GROUPS:
            group_items = [x for x in normalized_items if x["group"] == group["id"]]
            for order, item in enumerate(group_items, start=1):
                item["order"] = order
                if not item.get("id"):
                    item["id"] = f"concept_g{group['id']}_{order}"
                if not item.get("folderKey"):
                    item["folderKey"] = build_folder_key("concept", item.get("name"), item.get("id"))

        progress = calculate_concept_progress(normalized_items)

        return {
            "title": data.get("title") or "Чек-лист Концепция",
            "checklistKey": "concept",
            "collabTitle": clean_cell_value(data.get("collabTitle")),
            "contractDeadline": "",
            "startDate": "",
            "groups": [{"id": group["id"], "title": group["title"]} for group in CONCEPT_GROUPS],
            "projectChecklists": build_project_checklists(),
            "items": normalized_items,
            "notice": data.get("notice", ""),
            "activeCount": progress["activeCount"],
            "completedCount": progress["completedCount"],
            "progressPercent": progress["progressPercent"],
        }

    if checklist_key == "opr":
        raw_items = data.get("items", []) or []
        normalized_items = []

        current_opr_default_names = {
            clean_cell_value(item_name).lower()
            for group in OPR_GROUPS
            if group["id"] != 2
            for item_name in group["items"]
            if clean_cell_value(item_name)
        }

        for raw_item in raw_items:
            item, documents, first_doc, folder_key, folder_path, folder_url, legacy_document_url, legacy_document_name = prepare_item_common(raw_item)

            name = clean_cell_value(item.get("name"))
            if not name:
                continue

            is_custom = bool(item.get("isCustom", False))
            normalized_name = name.lower()

            # Для OPR оставляем только:
            # 1) текущие стандартные пункты новой архитектуры
            # 2) кастомные пункты, добавленные пользователем вручную
            if not is_custom and normalized_name not in current_opr_default_names:
                continue

            status = normalize_status(item.get("status"))
            group_id = int(item.get("group") or 0)

            if status == "Не требуется":
                group_id = 2
            elif group_id == 2 or not group_id:
                group_id = resolve_opr_group_id_by_item_id_or_name(item)

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
            })

        deduped_items = []
        seen_builtin_names = set()

        for existing_item in normalized_items:
            name_key = clean_cell_value(existing_item.get("name")).lower()

            if not existing_item.get("isCustom"):
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

        for group in OPR_GROUPS:
            if group["id"] == 2:
                continue

            for default_order, default_name in enumerate(group["items"], start=1):
                normalized_name = clean_cell_value(default_name).lower()
                if not normalized_name or normalized_name in existing_names:
                    continue

                migrated_item_id = f"opr_g{group['id']}_{default_order}_migrated_{slugify_folder_part(default_name)}"

                normalized_items.append({
                    "id": migrated_item_id,
                    "group": group["id"],
                    "order": default_order,
                    "name": default_name,
                    "priority": "white",
                    "status": "",
                    "plan": "",
                    "fact": "",
                    "folderKey": build_folder_key("opr", default_name, migrated_item_id),
                    "folderPath": "",
                    "folderUrl": "",
                    "documents": [],
                    "documentUrl": "",
                    "documentName": "",
                    "isCustom": False,
                })

                existing_names.add(normalized_name)

        normalized_items.sort(key=lambda x: (x["group"], x["order"], x["name"]))

        for group in OPR_GROUPS:
            group_items = [x for x in normalized_items if x["group"] == group["id"]]
            for order, item in enumerate(group_items, start=1):
                item["order"] = order
                if not item.get("id"):
                    item["id"] = f"opr_g{group['id']}_{order}"
                if not item.get("folderKey"):
                    item["folderKey"] = build_folder_key("opr", item.get("name"), item.get("id"))

        progress = calculate_opr_progress(normalized_items)

        return {
            "title": data.get("title") or "Чек-лист ОПР",
            "checklistKey": "opr",
            "collabTitle": clean_cell_value(data.get("collabTitle")),
            "contractDeadline": "",
            "startDate": "",
            "groups": [{"id": group["id"], "title": group["title"]} for group in OPR_GROUPS],
            "projectChecklists": build_project_checklists(),
            "items": normalized_items,
            "notice": data.get("notice", ""),
            "activeCount": progress["activeCount"],
            "completedCount": progress["completedCount"],
            "progressPercent": progress["progressPercent"],
        }

    raw_items = data.get("items", []) or []
    normalized_items = []

    group_order_counters = {1: 0, 2: 0, 3: 0, 4: 0}
    current_id_default_names = build_current_id_default_name_set()

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

        if not is_custom and target_group_id != 4 and item_name.lower() not in current_id_default_names:
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

        if not existing_item.get("isCustom") and int(existing_item.get("group") or 0) != 4:
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

    for default_group_id, group_data in CHECKLIST_GROUPS.items():
        if default_group_id == 4:
            continue

        for default_order, default_name in enumerate(group_data["items"], start=1):
            normalized_name = clean_cell_value(default_name).lower()
            if not normalized_name or normalized_name in existing_names:
                continue

            migrated_item_id = f"{build_item_id(default_group_id, default_order)}_migrated_{slugify_folder_part(default_name)}"

            normalized_items.append({
                "id": migrated_item_id,
                "group": default_group_id,
                "order": default_order,
                "name": default_name,
                "priority": "white",
                "status": "",
                "plan": "",
                "fact": "",
                "folderKey": build_folder_key("id", default_name, migrated_item_id),
                "folderPath": "",
                "folderUrl": "",
                "documents": [],
                "documentUrl": "",
                "documentName": "",
                "isCustom": False,
            })

            existing_names.add(normalized_name)

    default_id_order = {}
    for default_group_id, group_data in CHECKLIST_GROUPS.items():
        if default_group_id == 4:
            continue

        for default_order, default_name in enumerate(group_data["items"], start=1):
            default_id_order[(default_group_id, clean_cell_value(default_name).lower())] = default_order

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

    for group_id in [1, 2, 3, 4]:
        group_items = [x for x in normalized_items if x["group"] == group_id]
        for order, item in enumerate(group_items, start=1):
            item["order"] = order
            if not item.get("id"):
                item["id"] = build_item_id(group_id, order)
            if not item.get("folderKey"):
                item["folderKey"] = build_folder_key("id", item.get("name"), item.get("id"))

    progress = calculate_progress(normalized_items)

    return {
        "title": data.get("title") or "Чек-лист ИД",
        "checklistKey": "id",
        "collabTitle": clean_cell_value(data.get("collabTitle")),
        "contractDeadline": "",
        "startDate": "",
        "groups": [
            {"id": 1, "title": "ИД"},
            {"id": 2, "title": "ТУ"},
            {"id": 3, "title": "Прочее"},
            {"id": 4, "title": "Не требуется"},
        ],
        "projectChecklists": build_project_checklists(),
        "items": normalized_items,
        "notice": data.get("notice", ""),
        "activeCount": progress["activeCount"],
        "completedCount": progress["completedCount"],
        "progressPercent": progress["progressPercent"],
    }
