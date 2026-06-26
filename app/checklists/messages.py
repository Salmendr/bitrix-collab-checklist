import re
from urllib.parse import quote, urlparse

from app.settings import APP_PORTAL_PATH, BITRIX_TECH_WEBHOOK_URL

from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
    normalize_status,
    normalize_checklist_key,
)

from app.checklists.config import (
    get_checklist_config,
    list_checklist_configs,
)

def status_emoji(status: str) -> str:
    status = display_status_text(status)

    if status in {"Есть", "Да"}:
        return "🟢"
    if status == "Нет":
        return "🔴"
    if status == "Не требуется":
        return "🚫"
    return "🟢" if status else "⚪️"


MESSAGE_ALIGNMENT_SPACE = " "
MESSAGE_ALIGNMENT_SPACE_FACTOR = 2
def get_message_checklist_order_map() -> dict[str, int]:
    return {
        config.key: index
        for index, config in enumerate(list_checklist_configs())
    }


def get_message_checklist_title(checklist_key: str) -> str:
    return get_checklist_config(checklist_key).title

MESSAGE_SECTION_LABELS = {
    "status": "Статусы",
    "date": "Даты",
    "extraInfo": "Доп информация",
    "source": "Нормативы",
    "name": "Пункты",
    "document": "Документы",
    "add-item": "Пункты",
}


def display_status_text(status: str) -> str:
    status = clean_cell_value(status)
    normalized = normalize_status(status)
    return normalized or status


def build_progress_text(data: dict) -> str:
    items = data.get("items") or []

    active_items = [
        item for item in items
        if display_status_text(item.get("status")) != "Не требуется"
    ]
    completed_items = [
        item for item in active_items
        if display_status_text(item.get("status")) == "Есть"
    ]

    percent = round((len(completed_items) / len(active_items)) * 100) if active_items else 0
    return f"📊Прогресс: {percent}%"


def split_changes(changes: list) -> dict:
    groups = {
        "status": [],
        "date": [],
        "extraInfo": [],
        "source": [],
        "name": [],
        "document": [],
        "add-item": [],
    }

    for change in changes or []:
        field = str(change.get("field") or "").strip()
        if field == "status":
            groups["status"].append(change)
        elif field in {"plan", "plannedDate", "fact"}:
            groups["date"].append(change)
        elif field == "extraInfo":
            groups["extraInfo"].append(change)
        elif field == "source":
            groups["source"].append(change)
        elif field == "name":
            groups["name"].append(change)
        elif field == "document":
            groups["document"].append(change)
        elif field == "add-item":
            groups["add-item"].append(change)

    return groups


def strip_message_markup(value: str) -> str:
    return re.sub(r"\[[^\]]+\]", "", str(value or ""))


def message_visible_length(value: str) -> int:
    return len(strip_message_markup(value))


def pad_message_left(value: str, target_width: int) -> str:
    missing = max(target_width - message_visible_length(value), 0)
    return value + (MESSAGE_ALIGNMENT_SPACE * (missing * MESSAGE_ALIGNMENT_SPACE_FACTOR))


def get_checklist_message_title(checklist_key: str, checklist_title: str = "") -> str:
    cleaned_title = clean_cell_value(checklist_title)
    if cleaned_title:
        return cleaned_title

    return get_message_checklist_title(checklist_key)


def get_checklist_link_caption(checklist_key: str) -> str:
    title = get_message_checklist_title(checklist_key)

    cleaned = (
        clean_cell_value(title)
        .replace("Чек-лист", "")
        .replace("чек-лист", "")
        .strip(" —-")
        .strip()
    )

    return (cleaned or title or "ЧЕК-ЛИСТ").upper()


def build_checklist_message_link(dialog_id: str, checklist_key: str) -> str:
    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)

    if not dialog_id or not BITRIX_TECH_WEBHOOK_URL:
        return ""

    parsed = urlparse(BITRIX_TECH_WEBHOOK_URL)
    if not parsed.scheme or not parsed.netloc:
        return ""

    base_url = f"{parsed.scheme}://{parsed.netloc}"
    app_path = APP_PORTAL_PATH if APP_PORTAL_PATH.endswith("/") else APP_PORTAL_PATH + "/"
    query = f"dialogId={quote(dialog_id)}&checklistKey={quote(checklist_key)}"
    return f"{base_url}{app_path}?{query}#{query}"


def build_editor_text(editor: dict) -> str:
    editor_name = str(editor.get("name") or "").strip()
    editor_id = str(editor.get("id") or "").strip()

    if editor_name and editor_id:
        return f"👤Кем изменено: {editor_name} (ID {editor_id})"
    if editor_name:
        return f"👤Кем изменено: {editor_name}"
    if editor_id:
        return f"👤Кем изменено: ID {editor_id}"
    return "👤Кем изменено: неизвестно"


def normalize_message_value(field: str, value, checklist_key: str) -> str:
    raw = clean_cell_value(value)
    if field == "status":
        return display_status_text(raw)
    return raw


def build_change_left_label(field: str, checklist_key: str) -> str:
    if field == "status":
        return "Статус"
    if field in {"date", "plan", "plannedDate"}:
        return "План"
    if field == "fact":
        return "Факт"
    if field == "extraInfo":
        return "Доп информация"
    if field == "source":
        return "Нормативы"
    if field == "name":
        return "Пункт"
    if field == "document":
        return "Документ"
    if field == "add-item":
        return "Пункт"
    return field


def build_change_emoji(field: str, new_value: str, checklist_key: str) -> str:
    if field == "status":
        return status_emoji(new_value)
    if field in {"date", "plan", "plannedDate", "fact"}:
        return "📆"
    if field == "extraInfo":
        return "📝"
    if field == "source":
        return "📚"
    if field == "name":
        return "✏️"
    if field == "document":
        return "📎"
    if field == "add-item":
        return "➕"
    return "✏️"


def should_show_empty_old_value(field: str) -> bool:
    return field in {"extraInfo", "source", "name"}


def build_change_entry(change: dict, field: str, checklist_key: str) -> dict:
    item_name = clean_cell_value(change.get("itemName")) or "Без названия"
    old_value = normalize_message_value(field, change.get("oldValue"), checklist_key)
    new_value = normalize_message_value(field, change.get("newValue"), checklist_key) or "—"
    label = build_change_left_label(field, checklist_key)
    prefix = build_change_emoji(field, new_value, checklist_key)

    if field == "add-item":
        left = f"{prefix}{item_name} / {label}: →"
        right = "добавлен"
        return {
            "field": field,
            "itemName": item_name,
            "oldValue": old_value,
            "newValue": new_value,
            "leftText": left,
            "rightText": right,
        }

    if old_value:
        left = f"{prefix}{item_name} / {label}: {old_value} →"
    elif should_show_empty_old_value(field):
        left = f"{prefix}{item_name} / {label}: — →"
    else:
        left = f"{prefix}{item_name} / {label}: →"

    return {
        "field": field,
        "itemName": item_name,
        "oldValue": old_value,
        "newValue": new_value,
        "leftText": left,
        "rightText": new_value,
    }


def collect_global_alignment_width(section_blocks: list[dict]) -> int:
    widths = []
    for block in section_blocks:
        for section in block.get("sections") or []:
            for row in section.get("rows") or []:
                widths.append(message_visible_length(row.get("leftText") or ""))

    return max(widths) if widths else 0


def format_aligned_change_line(row: dict, target_width: int) -> str:
    left = str(row.get("leftText") or "")
    right = str(row.get("rightText") or "—")
    return f"{pad_message_left(left, target_width)}|[b]{right}[/b]"


def build_aligned_section_lines(section_title: str, rows: list[dict], target_width: int) -> list[str]:
    if not rows:
        return []

    lines = [f"[b]{section_title}:[/b]", ""]
    for row in rows:
        lines.append(format_aligned_change_line(row, target_width))
    lines.append("")
    return lines


def build_recent_changes_sections(changes: list, checklist_key: str) -> list[dict]:
    grouped = split_changes(changes)
    key = normalize_checklist_key(checklist_key)

    section_order = [
        "status",
        "date",
        "document",
        "add-item",
        "name",
        "source",
        "extraInfo",
    ]

    sections = []

    for field in section_order:
        field_changes = grouped.get(field) or []
        if not field_changes:
            continue

        rows = [
            build_change_entry(change, field, key)
            for change in field_changes
        ]

        sections.append({
            "field": field,
            "title": MESSAGE_SECTION_LABELS.get(field, field),
            "rows": rows,
        })

    return sections


def build_recent_changes_text(
    changes: list,
    checklist_title: str = "Чек-лист ИД",
    checklist_key: str = "id",
    sections: list[dict] | None = None,
    alignment_width: int = 0,
) -> str:
    title = get_checklist_message_title(checklist_key, checklist_title).upper()
    sections = sections if sections is not None else build_recent_changes_sections(changes, checklist_key)
    lines = [f"✏️[b]ИЗМЕНЕНИЯ В {title}[/b]", ""]

    if sections:
        target_width = alignment_width or collect_global_alignment_width([{"sections": sections}])
        for section in sections:
            lines.extend(build_aligned_section_lines(section["title"], section["rows"], target_width))
    else:
        lines.extend(["Изменений нет", ""])

    return "\n".join(lines).strip()


def build_checklist_message_block(
    data: dict,
    changes: list,
    sections: list[dict] | None = None,
    alignment_width: int = 0,
) -> str:
    checklist_key = normalize_checklist_key(data.get("checklistKey") or "id")
    checklist_title = get_checklist_message_title(checklist_key, data.get("title") or "")
    dialog_id = normalize_dialog_id(data.get("resolvedDialogId") or data.get("dialogId") or "")
    link_url = build_checklist_message_link(dialog_id, checklist_key)
    link_caption = get_checklist_link_caption(checklist_key)

    parts = [
        build_recent_changes_text(
            changes,
            checklist_title,
            checklist_key,
            sections=sections,
            alignment_width=alignment_width,
        ),
        "",
        build_progress_text(data),
        "",
    ]

    if link_url:
        parts.append(f"[URL={link_url}]Чтобы посмотреть весь чек-лист {link_caption} нажмите на этот текст[/URL]")
    else:
        parts.append(f"Чтобы посмотреть весь чек-лист {link_caption} нажмите на этот текст")

    return "\n".join(parts).strip()


def build_multi_checklist_chat_message(sessions: list, editor: dict) -> str:
    if not sessions:
        return ""

    indexed = []
    for session in sessions:
        data = session.get("data") or {}
        changes = session.get("changes") or []
        checklist_key = normalize_checklist_key(session.get("checklistKey") or data.get("checklistKey") or "id")
        if not changes:
            continue
        indexed.append({
            "checklistKey": checklist_key,
            "data": data,
            "changes": changes,
            "sections": build_recent_changes_sections(changes, checklist_key),
        })

    if not indexed:
        return ""

    order_map = get_message_checklist_order_map()
    indexed.sort(key=lambda item: order_map.get(item["checklistKey"], 999))
    alignment_width = collect_global_alignment_width(indexed)

    parts = []
    for entry in indexed:
        block = build_checklist_message_block(
            entry["data"],
            entry["changes"],
            sections=entry["sections"],
            alignment_width=alignment_width,
        )
        if block:
            parts.append(block)

    if not parts:
        return ""

    parts.extend([
        "",
        build_editor_text(editor),
    ])
    return "\n\n".join(parts).strip()


def build_checklist_chat_message(data: dict, changes: list, editor: dict) -> str:
    checklist_key = normalize_checklist_key(data.get("checklistKey") or "id")
    sections = build_recent_changes_sections(changes, checklist_key)
    alignment_width = collect_global_alignment_width([{"sections": sections}])
    return build_multi_checklist_chat_message([
        {
            "checklistKey": checklist_key,
            "data": data,
            "changes": changes,
            "sections": sections,
            "alignmentWidth": alignment_width,
        }
    ], editor)