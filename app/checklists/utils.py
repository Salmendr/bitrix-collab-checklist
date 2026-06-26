import re
from pathlib import Path

from app.checklists.constants import PRIORITY_OPTIONS, STATUS_OPTIONS


def clean_cell_value(value):
    value = str(value or "").strip()
    if value == "—":
        return ""
    return value


def normalize_priority(value: str) -> str:
    value = str(value or "").strip().lower()
    if value in PRIORITY_OPTIONS:
        return value
    return "white"


def normalize_status(value: str) -> str:
    value = clean_cell_value(value)

    if not value:
        return ""

    normalized_map = {
        "есть": "Есть",
        "да": "Есть",
        "нет": "Нет",
        "не требуется": "Не требуется",
        "подписан": "Есть",
        "запрос опросного листа": "",
        "договор тех прис": "",
    }

    key = value.strip().lower()
    if key in normalized_map:
        return normalized_map[key]

    if value in STATUS_OPTIONS:
        return value

    return ""


def normalize_date_string(value: str) -> str:
    value = clean_cell_value(value)
    if not value:
        return ""

    # Оставляем как есть, если это уже формат ДД.ММ.ГГГГ
    parts = value.split(".")
    if len(parts) == 3 and len(parts[0]) == 2 and len(parts[1]) == 2 and len(parts[2]) == 4:
        return value

    return value


def slugify_folder_part(value: str) -> str:
    value = str(value or "").strip().lower()

    translit_map = {
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d",
        "е": "e", "ё": "e", "ж": "zh", "з": "z", "и": "i",
        "й": "y", "к": "k", "л": "l", "м": "m", "н": "n",
        "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
        "у": "u", "ф": "f", "х": "h", "ц": "ts", "ч": "ch",
        "ш": "sh", "щ": "sch", "ъ": "", "ы": "y", "ь": "",
        "э": "e", "ю": "yu", "я": "ya"
    }

    chars = []
    for ch in value:
        if ch in translit_map:
            chars.append(translit_map[ch])
        else:
            chars.append(ch)

    value = "".join(chars)
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "item"


def format_file_size(size_bytes: int) -> str:
    try:
        size = int(size_bytes or 0)
    except Exception:
        size = 0

    if size <= 0:
        return ""

    units = ["Б", "КБ", "МБ", "ГБ"]
    value = float(size)
    unit_index = 0

    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1

    if unit_index == 0:
        return f"{int(value)} {units[unit_index]}"

    if value >= 100:
        return f"{value:.0f} {units[unit_index]}"
    if value >= 10:
        return f"{value:.1f} {units[unit_index]}"
    return f"{value:.2f} {units[unit_index]}"


def can_preview_in_browser(filename: str, media_type: str = "") -> bool:
    filename = str(filename or "").strip().lower()
    media_type = str(media_type or "").strip().lower()

    if media_type.startswith("image/"):
        return True
    if media_type.startswith("text/"):
        return True

    previewable_media_types = {
        "application/pdf",
        "application/json",
        "application/xml",
        "text/csv",
    }

    previewable_extensions = {
        ".pdf", ".txt", ".md", ".csv", ".json", ".xml", ".html", ".htm"
    }

    if media_type in previewable_media_types:
        return True

    suffix = Path(filename).suffix.lower()
    return suffix in previewable_extensions

def normalize_dialog_id(value: str) -> str:
    s = str(value or "").strip()
    s = s.strip('"').strip("'")

    if not s:
        return ""

    if s.startswith("chat") and s[4:].isdigit():
        return s

    if s.isdigit():
        return f"chat{s}"

    return s


def normalize_checklist_key(value: str) -> str:
    value = str(value or "").strip().lower()
    value = value.replace(" ", "_")

    value = re.sub(r"[^a-z0-9_-]+", "", value)

    return value or "id"