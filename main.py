import os
import uuid
from collections import defaultdict
from pathlib import Path
from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
import requests
import json
import html
import sqlite3
from io import BytesIO
from datetime import datetime
import openpyxl

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent

DB_DIR = BASE_DIR / "db"
DB_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = str(DB_DIR / "app.db")

APP_PORTAL_PATH = "/marketplace/app/84/"
TECH_USER_ID = 138
BITRIX_TECH_WEBHOOK_URL = os.getenv("BITRIX_TECH_WEBHOOK_URL", "").strip()

UPLOAD_ROOT = BASE_DIR / "uploads"
UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)

CHECKLIST_UPLOAD_ROOT = UPLOAD_ROOT / "checklists"
CHECKLIST_UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)

DEBUG_DIR = BASE_DIR / "debug"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

DEBUG_LOG_PATH = DEBUG_DIR / "close_popup.log"

app.mount("/uploads", StaticFiles(directory=str(UPLOAD_ROOT)), name="uploads")

PROJECT_CHECKLISTS = [
    {"key": "id", "title": "Чек-лист ИД"},
    {"key": "opr", "title": "Чек-лист ОПР"},
    {"key": "concept", "title": "Чек-лист Концепция"},
]

CHECKLIST_GROUPS = {
    1: {
        "title": "ИД",
        "items": [
            "ППТ",
            "Сокращение ОКН",
            "Выписка ЕГРН",
            "ГПЗУ",
            "ИГДИ",
            "ИГИ",
            "ИЭИ",
            "ИГМИ",
            "ИГИ СМР",
        ],
    },
    2: {
        "title": "ТУ",
        "items": [
            "ТУ Свет",
            "ТУ Водоснабжение",
            "ТУ Бытовая канализация",
            "Расположение пож гидрантов",
            "ТУ СС",
            "ТУ ТС",
            "ТУ Газ",
            "ТУ Ливневка",
            "ТУ выносы",
            "СТУ",
        ],
    },
    3: {
        "title": "Прочее",
        "items": [
            "Задание на лифты",
            "Аэропорт",
            "Примыкание ОДД",
            "Порубочный лист",
            "Справка вывоза мусора",
        ],
    },
    4: {
        "title": "Не требуется",
        "items": [],
    },
}

CONCEPT_GROUPS = [
    {
        "id": 1,
        "title": "Общее",
        "items": [
            {"name": "Участок сформирован", "source": "ИД", "statusKind": "bool", "extraPlaceholder": "Например: 2%"},
            {"name": "Наличие ГПЗУ", "source": "ИД", "statusKind": "bool", "extraPlaceholder": "№"},
            {"name": "Наличие проекта планировки", "source": "ИД", "statusKind": "bool", "extraPlaceholder": "Например: проект межевания от 03.07.2024 №3135"},
            {"name": "Наличие ТЗ", "source": "ИД", "statusKind": "bool", "extraPlaceholder": ""},
        ],
    },
    {
        "id": 2,
        "title": "Градостроительные ограничения",
        "items": [
            {"name": "Предельная высота", "source": "ГПЗУ, ПЗЗ", "statusKind": "text", "statusPlaceholder": "___м", "extraPlaceholder": "По проекту: ___"},
            {"name": "Предельная этажность", "source": "ГПЗУ, ПЗЗ", "statusKind": "text", "statusPlaceholder": "___эт.", "extraPlaceholder": "По проекту: ___"},
            {"name": "Процент застройки", "source": "ГПЗУ, ПЗЗ", "statusKind": "text", "statusPlaceholder": "__%", "extraPlaceholder": "По проекту: ___"},
            {"name": "Плотность застройки", "source": "ГПЗУ, ПЗЗ", "statusKind": "text", "statusPlaceholder": "______м2/ГА", "extraPlaceholder": "По проекту: ___"},
            {"name": "Процент озеленения", "source": "ГПЗУ, ПЗЗ", "statusKind": "text", "statusPlaceholder": "__%", "extraPlaceholder": "По проекту: ___"},
            {"name": "Отступы от границ участка", "source": "ГПЗУ, ПЗЗ", "statusKind": "text", "statusPlaceholder": "_м", "extraPlaceholder": "Выполнено / не выполнено"},
            {
                "name": "Вид разрешенного использования",
                "source": "ГПЗУ, ПЗЗ",
                "statusKind": "select",
                "statusOptions": ["Основные", "Условно разрешенные", "Вспомогательные"],
                "extraPlaceholder": "Разрешенный"
            },
        ],
    },
    {
        "id": 3,
        "title": "Приаэродромные территории",
        "items": [
            {"name": "Макс. высота приаэродромных терр.:", "source": "ГПЗУ, Акт ПАТ аэропорта", "statusKind": "text", "statusPlaceholder": "___м", "extraPlaceholder": "По концепции: ___м"},
            {"name": "3 подзона", "source": "ГПЗУ, Акт ПАТ аэропорта", "statusKind": "text", "statusPlaceholder": "___м", "extraPlaceholder": "По концепции: ___м"},
            {"name": "4 подзона", "source": "ГПЗУ, Акт ПАТ аэропорта", "statusKind": "text", "statusPlaceholder": "___м", "extraPlaceholder": "По концепции: ___м"},
            {"name": "Согласование от военного аэропорта", "source": "", "statusKind": "bool", "extraPlaceholder": "Требуется: да/нет"},
        ],
    },
    {
        "id": 4,
        "title": "Зоны затопления и подтопления",
        "items": [
            {"name": "Участок в зоне затопления/подтопления", "source": "ГПЗУ, ПЗЗ", "statusKind": "bool", "extraPlaceholder": "Минимальная отметка земли: ___м"},
        ],
    },
    {
        "id": 5,
        "title": "Дорожно-транспортная сеть",
        "items": [
            {"name": "Требуется согласование примыканий", "source": "", "statusKind": "bool", "extraPlaceholder": ""},
        ],
    },
    {
        "id": 6,
        "title": "Охранные и санитарно-защитные зоны",
        "items": [
            {"name": "Требуется согласование размещения объекта в охранных зонах сетей", "source": "ГПЗУ / СП 124.13330 / Приказ N 197", "statusKind": "bool", "extraPlaceholder": "В т.ч.: ____________"},
            {"name": "Требуется вынос сетей", "source": "ГПЗУ / СП 124.13330 / Приказ N 197", "statusKind": "bool", "extraPlaceholder": "В т.ч.: ____________"},
            {"name": "Учтены отступы от наружных ТС до зданий, парковок, площадок", "source": "ГПЗУ / СП 124.13330 / Приказ N 197", "statusKind": "bool", "extraPlaceholder": ""},
            {"name": "Размещение в зонах ОКН", "source": "ГПЗУ / СП 124.13330 / Приказ N 197", "statusKind": "bool", "extraPlaceholder": "В т.ч.: ____________"},
            {"name": "Учтены санитарные разрывы от парковочных мест (кроме гостевых жилых домов)", "source": "СанПиН 2.2.1/2.1.1.1200-03", "statusKind": "bool", "extraPlaceholder": "До жилых домов, площадок благоустройства, школ, ДОУ, больниц и т.п."},
            {"name": "Учтены санитарно-защитные зоны существующих объектов", "source": "СанПиН 2.2.1/2.1.1.1200-03", "statusKind": "bool", "extraPlaceholder": "В т.ч.: ____________"},
            {"name": "Учтены санитарно-защитные зоны проектируемых объектов", "source": "СанПиН 2.2.1/2.1.1.1200-03", "statusKind": "bool", "extraPlaceholder": "В т.ч.: площадки ТКО, парковки, ТП, КНС, ________"},
        ],
    },
    {
        "id": 7,
        "title": "Пожарные требования",
        "items": [
            {"name": "Требуется разработка СТУ", "source": "ФЗ 123 / СП 4.13130", "statusKind": "bool", "extraPlaceholder": "При отступлениях от требований СП"},
            {"name": "Пожарные проезды в границах участка", "source": "ФЗ 123 / СП 4.13130", "statusKind": "bool", "extraPlaceholder": ""},
            {"name": "Требуется разработка ПТП по проездам", "source": "ФЗ 123 / СП 4.13130", "statusKind": "bool", "extraPlaceholder": "При отступлениях от требований СП"},
            {"name": "Пожарные разрывы от зданий и сооружений", "source": "ФЗ 123 / СП 4.13130", "statusKind": "bool", "extraPlaceholder": "Выполнено: да/нет"},
        ],
    },
    {
        "id": 8,
        "title": "Инсоляция",
        "items": [
            {"name": "Обеспечена инсоляция проектируемого объекта", "source": "СанПиН 1.2.3685-21 пп.165-168", "statusKind": "bool", "extraPlaceholder": "в т.ч. площадок"},
            {"name": "Обеспечена инсоляция окружающей застройки", "source": "СанПиН 1.2.3685-21 пп.165-168", "statusKind": "bool", "extraPlaceholder": "Запрошены тех. паспорта зданий по адресам: 1. __________"},
        ],
    },
    {
        "id": 9,
        "title": "Прочие требования",
        "items": [
            {"name": "Размещена площадка ТКО", "source": "СанПиН", "statusKind": "bool", "extraPlaceholder": "min 20м до домов и площадок (8м при раздельном сборе мусора)"},
            {"name": "К ТП, ТКО и проч. обеспечен проезд", "source": "", "statusKind": "bool", "extraPlaceholder": ""},
            {"name": "Объект обеспечен требуемым количеством машиномест в границах ЗУ*", "source": "РНГП", "statusKind": "bool", "extraPlaceholder": ""},
            {"name": "Объект обеспечен требуемым количеством площадок благоустройства в границах ЗУ*", "source": "РНГП", "statusKind": "bool", "extraPlaceholder": ""},
        ],
    },
    {
        "id": 10,
        "title": "Не требуется",
        "items": [],
    },
]

STATUS_OPTIONS = [
    "",
    "Есть",
    "Нет",
    "Не требуется",
]

PRIORITY_OPTIONS = ["white", "green", "gray"]


def build_default_groups(checklist_key: str = "id"):
    if checklist_key == "concept":
        return [{"id": group["id"], "title": group["title"]} for group in CONCEPT_GROUPS]

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

    for group in CONCEPT_GROUPS:
        if group["id"] == 10:
            continue

        for order, spec in enumerate(group["items"], start=1):
            expected_id = f"concept_g{group['id']}_{order}"
            if item_id == expected_id or name == spec["name"]:
                return group["id"]

    current_group = int(item.get("group") or 1)
    return 1 if current_group == 10 else current_group


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


def build_item_id(group_id: int, order: int) -> str:
    return f"item_g{group_id}_{order}"


def build_project_checklists():
    return [dict(x) for x in PROJECT_CHECKLISTS]


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


def build_upload_rel_path(dialog_id: str, item_id: str, filename: str) -> str:
    dialog_id = normalize_dialog_id(dialog_id)
    safe_name = Path(filename or "file.bin").name
    unique_name = f"{uuid.uuid4().hex}_{safe_name}"
    return f"checklists/{dialog_id}/{item_id}/{unique_name}"


def remove_item_document_file(item: dict):
    doc_url = str(item.get("documentUrl") or "").strip()
    if not doc_url.startswith("/uploads/"):
        return

    rel_path = doc_url.replace("/uploads/", "", 1)
    file_path = UPLOAD_ROOT / rel_path

    try:
        if file_path.exists():
            file_path.unlink()
    except Exception:
        pass


def build_default_checklist_template(dialog_id: str = "", checklist_key: str = "id"):
    items = []

    if checklist_key == "concept":
        for group in CONCEPT_GROUPS:
            if group["id"] == 10:
                continue

            for order, spec in enumerate(group["items"], start=1):
                items.append({
                    "id": f"concept_g{group['id']}_{order}",
                    "group": group["id"],
                    "order": order,
                    "name": spec["name"],
                    "source": spec.get("source", ""),
                    "statusKind": spec.get("statusKind", "bool"),
                    "statusOptions": spec.get("statusOptions", []),
                    "statusPlaceholder": spec.get("statusPlaceholder", ""),
                    "status": "",
                    "extraInfo": "",
                    "extraInfoPlaceholder": spec.get("extraPlaceholder", ""),
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

    for group_id, group_data in CHECKLIST_GROUPS.items():
        if group_id == 4:
            continue

        for order, name in enumerate(group_data["items"], start=1):
            items.append({
                "id": build_item_id(group_id, order),
                "group": group_id,
                "order": order,
                "name": name,
                "priority": "white",
                "status": "",
                "plan": "",
                "fact": "",
                "documentUrl": "",
                "documentName": "",
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


def normalize_checklist_data(data: dict, checklist_key: str = "id") -> dict:
    data = dict(data or {})
    checklist_key = str(checklist_key or data.get("checklistKey") or "id").strip() or "id"

    if checklist_key == "concept":
        raw_items = data.get("items", []) or []
        normalized_items = []

        for item in raw_items:
            item = dict(item or {})
            name = clean_cell_value(item.get("name"))
            if not name:
                continue

            normalized_items.append({
                "id": str(item.get("id") or ""),
                "group": int(item.get("group") or 0),
                "order": int(item.get("order") or 0),
                "name": clean_cell_value(item.get("name")),
                "source": clean_cell_value(item.get("source")),
                "statusKind": clean_cell_value(item.get("statusKind")) or "bool",
                "statusOptions": item.get("statusOptions") or [],
                "statusPlaceholder": clean_cell_value(item.get("statusPlaceholder")),
                "status": clean_cell_value(item.get("status")),
                "extraInfo": clean_cell_value(item.get("extraInfo")),
                "extraInfoPlaceholder": clean_cell_value(item.get("extraInfoPlaceholder")),
                "documentUrl": clean_cell_value(item.get("documentUrl")),
                "documentName": clean_cell_value(item.get("documentName")),
                "isCustom": bool(item.get("isCustom", False)),
                "priority": clean_cell_value(item.get("priority")) or "white",
            })

        normalized_items.sort(key=lambda x: (x["group"], x["order"], x["name"]))

        for group in CONCEPT_GROUPS:
            group_items = [x for x in normalized_items if x["group"] == group["id"]]
            for order, item in enumerate(group_items, start=1):
                item["order"] = order
                if not item.get("id"):
                    item["id"] = f"concept_g{group['id']}_{order}"

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
            "activeCount": 0,
            "completedCount": 0,
            "progressPercent": 0,
        }

    raw_items = data.get("items", []) or []
    normalized_items = []

    group_order_counters = {1: 0, 2: 0, 3: 0, 4: 0}

    for item in raw_items:
        item = dict(item or {})

        name = clean_cell_value(item.get("name"))
        if not name:
            continue

        status = normalize_status(item.get("status"))
        group_id = move_item_to_required_group({
            "group": item.get("group"),
            "name": name,
            "status": status,
        })

        group_order_counters[group_id] += 1
        default_order = group_order_counters[group_id]

        normalized_items.append({
            "id": str(item.get("id") or build_item_id(group_id, default_order)),
            "group": group_id,
            "order": int(item.get("order") or default_order),
            "name": name,
            "priority": derive_indicator_from_status(status),
            "status": status,
            "plan": normalize_date_string(item.get("plan")),
            "fact": normalize_date_string(item.get("fact")),
            "documentUrl": clean_cell_value(item.get("documentUrl")),
            "documentName": clean_cell_value(item.get("documentName")),
            "isCustom": bool(item.get("isCustom", False)),
        })

    normalized_items.sort(key=lambda x: (x["group"], x["order"], x["name"]))

    for group_id in [1, 2, 3, 4]:
        group_items = [x for x in normalized_items if x["group"] == group_id]
        for order, item in enumerate(group_items, start=1):
            item["order"] = order
            if not item.get("id"):
                item["id"] = build_item_id(group_id, order)

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


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS checklists (
            dialog_id TEXT PRIMARY KEY,
            title TEXT,
            data_json TEXT
        )
    """)

    conn.commit()
    conn.close()


# Страховочный вызов при импорте модуля
init_db()


@app.on_event("startup")
def startup_event():
    init_db()


def normalize_domain(value: str) -> str:
    value = (value or "").strip()
    value = value.replace("https://", "").replace("http://", "").strip("/")
    return value


def bitrix_rest_call(domain: str, method: str, access_token: str, payload: dict):
    url = f"https://{domain}/rest/{method}.json"
    response = requests.post(
        url,
        data={**payload, "auth": access_token},
        timeout=30
    )
    try:
        return response.json()
    except Exception:
        return {
            "http_status": response.status_code,
            "text": response.text
        }


def bitrix_webhook_call(method: str, payload: dict):
    if not BITRIX_TECH_WEBHOOK_URL:
        return {
            "error": "TECH_WEBHOOK_NOT_CONFIGURED",
            "error_description": "BITRIX_TECH_WEBHOOK_URL is empty"
        }

    base = BITRIX_TECH_WEBHOOK_URL.rstrip("/")
    url = f"{base}/{method}.json"

    response = requests.post(url, data=payload, timeout=30)

    try:
        return response.json()
    except Exception:
        return {
            "http_status": response.status_code,
            "text": response.text
        }


def write_debug_log(event: str, payload: dict):
    record = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "event": event,
        "payload": payload,
    }

    with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def status_emoji(status: str) -> str:
    status = normalize_status(status)

    if status == "Есть":
        return "🟢"
    if status == "Нет":
        return "🔴"
    if status == "Не требуется":
        return "🚫"
    return "⚪️"


def display_status_text(status: str) -> str:
    status = normalize_status(status)
    return status if status else ""


def format_plan_suffix(plan: str) -> str:
    plan = str(plan or "").strip()
    if not plan or plan == "—":
        return ""
    return f" | До {plan}"


def split_changes(changes: list) -> tuple[list, list]:
    status_changes = []
    date_changes = []

    for change in changes or []:
        field = str(change.get("field") or "").strip()

        if field == "status":
            status_changes.append(change)
        elif field == "plan":
            date_changes.append(change)
        # fact полностью игнорируем
        # document/add-item в верхнем блоке не показываем

    return status_changes, date_changes


def format_change_arrow(old_value: str, new_value: str) -> str:
    old_value = str(old_value or "").strip()
    new_value = str(new_value or "").strip()

    if not old_value or old_value == "—":
        return f"→ [B]{new_value}[/B]"
    return f"{old_value} → [B]{new_value}[/B]"


def format_status_change_line(change: dict) -> str:
    item_name = str(change.get("itemName") or "").strip() or "Без названия"
    new_value = normalize_status(change.get("newValue"))
    old_value = normalize_status(change.get("oldValue"))

    emoji = status_emoji(new_value)
    arrow = format_change_arrow(old_value, new_value)

    return f"{emoji}{item_name} / Статус: {arrow}"


def format_plan_change_line(change: dict) -> str:
    item_name = str(change.get("itemName") or "").strip() or "Без названия"
    old_value = str(change.get("oldValue") or "").strip()
    new_value = str(change.get("newValue") or "").strip()

    arrow = format_change_arrow(old_value, new_value)
    return f"📆{item_name} / План: {arrow}"


def collect_item_change_meta(item_id: str, changes: list) -> dict:
    meta = {
        "changed": False,
        "status_changed": False,
        "plan_changed": False,
    }

    for change in changes or []:
        if str(change.get("itemId") or "") != str(item_id):
            continue

        meta["changed"] = True

        field = str(change.get("field") or "").strip()
        if field == "status":
            meta["status_changed"] = True
        elif field == "plan":
            meta["plan_changed"] = True

    return meta


def build_item_change_flags(item: dict, changes: list) -> str:
    meta = collect_item_change_meta(str(item.get("id") or ""), changes)

    if not meta["changed"]:
        return ""

    parts = ["✏️"]

    if meta["status_changed"]:
        parts.append(status_emoji(item.get("status")))

    if meta["plan_changed"]:
        parts.append("📆")

    return "".join(parts)


def build_checklist_line(item: dict, changes: list) -> str:
    name = str(item.get("name") or "").strip()
    status = display_status_text(item.get("status"))
    plan = str(item.get("plan") or "").strip()

    emoji = status_emoji(status)
    line = f"{emoji}{name}"

    if status:
        line += f" — {status}"

    line += format_plan_suffix(plan)

    flags = build_item_change_flags(item, changes)
    if flags:
        line += f" {flags}"

    return line


def build_recent_changes_text(changes: list) -> str:
    status_changes, date_changes = split_changes(changes)

    lines = [
        "[B]✏️ИЗМЕНЕНИЯ В ЧЕК-ЛИСТ ИД[/B]",
        "",
    ]

    if status_changes:
        lines.append("[B]Статусы:[/B]")
        lines.append("")
        for change in status_changes:
            lines.append(format_status_change_line(change))
        lines.append("")

    if date_changes:
        lines.append("[B]Даты:[/B]")
        lines.append("")
        for change in date_changes:
            lines.append(format_plan_change_line(change))
        lines.append("")

    if not status_changes and not date_changes:
        lines.append("Изменений нет")
        lines.append("")

    return "\n".join(lines).strip()


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


def build_checklist_full_text(data: dict, changes: list) -> str:
    groups = data.get("groups", [])
    items = data.get("items", [])

    items_by_group = defaultdict(list)
    for item in items:
        items_by_group[int(item.get("group") or 0)].append(item)

    lines = []
    title = (data.get("title") or "Чек-лист ИД").strip()
    collab_title = (data.get("collabTitle") or "").strip()

    lines.append("_________________________________")

    if collab_title:
        lines.append(f"[B]{title} — {collab_title}[/B]")
    else:
        lines.append(f"[B]{title}[/B]")

    lines.append("")

    for group in groups:
        group_id = int(group.get("id") or 0)
        group_title = str(group.get("title") or "").strip()
        group_items = sorted(
            items_by_group.get(group_id, []),
            key=lambda x: (x.get("order", 0), x.get("name", ""))
        )

        if not group_items:
            continue

        lines.append(f"[B]{group_title}[/B]")

        for item in group_items:
            lines.append(build_checklist_line(item, changes))

        lines.append("")

    return "\n".join(lines).strip()


def build_checklist_chat_message(data: dict, changes: list, editor: dict) -> str:
    parts = [
        build_recent_changes_text(changes),
        "",
        build_editor_text(editor),
        "",
        build_checklist_full_text(data, changes),
    ]
    return "\n".join(parts).strip()


def install_finish_block():
    return """
    <script src="https://api.bitrix24.com/api/v1/"></script>
    <script>
        if (typeof BX24 !== 'undefined') {
            BX24.init(function () {
                try {
                    BX24.installFinish();
                } catch (e) {
                    console.log(e);
                }
            });
        }
    </script>
    """


def format_cell(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y")
    return str(value).strip()


def parse_xlsx_to_checklist(file_bytes: bytes):
    wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
    ws = wb[wb.sheetnames[0]]

    items = []

    for row in range(3, ws.max_row + 1):
        name = clean_cell_value(ws[f"A{row}"].value)
        status = normalize_status(ws[f"B{row}"].value)
        plan = normalize_date_string(format_cell(ws[f"C{row}"].value))
        fact = normalize_date_string(format_cell(ws[f"D{row}"].value))

        if not name:
            continue

        group_id = resolve_group_id(name)

        items.append({
            "id": build_item_id(group_id, len([x for x in items if x["group"] == group_id]) + 1),
            "group": group_id,
            "order": len([x for x in items if x["group"] == group_id]) + 1,
            "name": name,
            "priority": derive_indicator_from_status(status),
            "status": status,
            "plan": plan,
            "fact": fact,
            "documentUrl": "",
            "documentName": "",
            "isCustom": False,
        })

    data = {
        "title": "Чек-лист ИД",
        "collabTitle": ws.title,
        "contractDeadline": "",
        "startDate": "",
        "groups": build_default_groups(),
        "items": items,
    }

    return normalize_checklist_data(data)


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
    if value in {"id", "opr", "concept"}:
        return value
    return "id"


def make_storage_dialog_id(dialog_id: str, checklist_key: str = "id") -> str:
    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)
    return dialog_id if checklist_key == "id" else f"{dialog_id}::{checklist_key}"


def save_checklist(dialog_id: str, data: dict, checklist_key: str = "id"):
    dialog_id = make_storage_dialog_id(dialog_id, checklist_key)
    checklist_key = normalize_checklist_key(checklist_key)
    data = normalize_checklist_data(data, checklist_key)

    conn = get_conn()
    conn.execute("""
        INSERT INTO checklists(dialog_id, title, data_json)
        VALUES (?, ?, ?)
        ON CONFLICT(dialog_id) DO UPDATE SET
            title=excluded.title,
            data_json=excluded.data_json
    """, (dialog_id, data.get("title", "Чек-лист"), json.dumps(data, ensure_ascii=False)))
    conn.commit()
    conn.close()

def extract_dialog_id_from_form(form: dict) -> str:
    direct_candidates = [
        form.get("dialogId"),
        form.get("DIALOG_ID"),
        form.get("DIALOGID"),
        form.get("dialog_id"),
    ]

    for value in direct_candidates:
        value = normalize_dialog_id(value)
        if value:
            return value

    json_candidates = [
        form.get("PLACEMENT_OPTIONS"),
        form.get("placementOptions"),
        form.get("options"),
    ]

    for raw in json_candidates:
        if not raw:
            continue
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
            for key in ["dialogId", "DIALOG_ID", "dialog_id"]:
                value = normalize_dialog_id(data.get(key))
                if value:
                    return value
        except Exception:
            pass

    # На случай неожиданных имен полей
    for key, value in form.items():
        if "dialog" in str(key).lower():
            value = normalize_dialog_id(value)
            if value:
                return value

    return ""

def get_checklist(dialog_id: str, checklist_key: str = "id"):
    checklist_key = normalize_checklist_key(checklist_key)
    normalized_id = normalize_dialog_id(dialog_id)

    aliases = []
    for candidate in [dialog_id, normalized_id]:
        candidate = str(candidate or "").strip()
        if candidate and candidate not in aliases:
            aliases.append(candidate)

    if normalized_id.startswith("chat") and normalized_id[4:].isdigit():
        numeric_id = normalized_id[4:]
        if numeric_id not in aliases:
            aliases.append(numeric_id)

    storage_aliases = [make_storage_dialog_id(alias, checklist_key) for alias in aliases]
    storage_dialog_id = make_storage_dialog_id(normalized_id, checklist_key)

    conn = get_conn()

    row = None
    found_alias = None

    for candidate in storage_aliases:
        try:
            row = conn.execute(
                "SELECT data_json FROM checklists WHERE dialog_id = ?",
                (candidate,)
            ).fetchone()
        except sqlite3.OperationalError as e:
            if "no such table" in str(e).lower():
                conn.close()
                init_db()
                conn = get_conn()
                row = conn.execute(
                    "SELECT data_json FROM checklists WHERE dialog_id = ?",
                    (candidate,)
                ).fetchone()
            else:
                conn.close()
                raise

        if row:
            found_alias = candidate
            break

    conn.close()

    if row:
        data = json.loads(row["data_json"])
        data = normalize_checklist_data(data, checklist_key)
        data["resolvedDialogId"] = normalized_id
        data["lookupAliases"] = aliases
        data["foundAlias"] = found_alias
        return data

    data = build_default_checklist_template(normalized_id, checklist_key)
    save_checklist(normalized_id, data, checklist_key)

    data["resolvedDialogId"] = normalized_id
    data["lookupAliases"] = aliases
    data["foundAlias"] = storage_dialog_id
    return data


def app_home_html():
    return """
    <html>
    <head>
        <meta charset="utf-8">
        <title>Чек-лист ИД</title>
        <script>
            (function () {
                try {
                    const raw = localStorage.getItem('checklist_pending_dialog');
                    if (!raw) return;

                    const payload = JSON.parse(raw);
                    if (!payload || !payload.dialogId) return;

                    const age = Date.now() - (payload.ts || 0);
                    localStorage.removeItem('checklist_pending_dialog');

                    if (age < 60000) {
                        window.location.replace('/popup?dialogId=' + encodeURIComponent(payload.dialogId));
                        return;
                    }
                } catch (e) {
                    console.log('pending dialog redirect skipped:', e);
                }
            })();
        </script>
    </head>
    <body style="font-family:Arial,sans-serif;padding:40px">
        <h1>Чек-лист ИД</h1>
        <p>Открывайте приложение из Bitrix24.</p>
    </body>
    </html>
    """


def textarea_html(initial_dialog_id: str = "", initial_context_text: str = ""):
    initial_dialog_id_json = json.dumps(initial_dialog_id or "", ensure_ascii=False)
    initial_context_text_json = json.dumps(initial_context_text or "", ensure_ascii=False)

    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
        <meta charset="utf-8">
        <title>Чек-лист ИД — textarea</title>
        <script src="https://api.bitrix24.com/api/v1/"></script>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 10px 12px;
                background: #fff;
            }}
            .wrap {{
                display: flex;
                align-items: center;
                gap: 10px;
            }}
            .dot {{
                width: 28px;
                height: 28px;
                border-radius: 999px;
                background: #ef5b8d;
                color: #fff;
                display: flex;
                align-items: center;
                justify-content: center;
                font-weight: 700;
                flex: 0 0 28px;
            }}
            .main {{
                min-width: 0;
                flex: 1;
            }}
            .title {{
                font-size: 13px;
                font-weight: 700;
                margin-bottom: 3px;
            }}
            .meta {{
                font-size: 11px;
                color: #666;
                margin-bottom: 6px;
                word-break: break-word;
            }}
            .btn {{
                display: inline-block;
                padding: 6px 10px;
                border: 1px solid #d0d7de;
                border-radius: 8px;
                background: #f6f8fa;
                cursor: pointer;
                font-size: 12px;
            }}
            .btn:hover {{
                background: #eef2f7;
            }}
            .error {{
                color: #b42318;
                font-size: 11px;
                margin-top: 6px;
                word-break: break-word;
            }}
        </style>
    </head>
    <body>
        <div class="wrap">
            <div class="dot">≡</div>
            <div class="main">
                <div class="title">Чек-лист ИД</div>
                <div class="meta" id="meta">Инициализация...</div>
                <button class="btn" id="openBtn" type="button">Открыть чек-лист</button>
                <div class="error" id="error"></div>
            </div>
        </div>

        <script>
            var initialDialogId = {initial_dialog_id_json};
            var initialContextText = {initial_context_text_json};
            var autoOpened = false;

            function setMeta(text) {{
                document.getElementById('meta').textContent = text;
            }}

            function setError(text) {{
                document.getElementById('error').textContent = text || '';
            }}

            function openChecklist(dialogId) {{
                if (!dialogId) {{
                    setError('dialogId не найден');
                    return;
                }}

                try {{
                    localStorage.setItem('checklist_pending_dialog', JSON.stringify({{
                        dialogId: dialogId,
                        ts: Date.now()
                    }}));
                }} catch (e) {{
                    console.log('localStorage save error:', e);
                }}

                try {{
                    if (window.BX24 && typeof window.BX24.openApplication === 'function') {{
                        BX24.openApplication();
                        autoOpened = true;
                        setMeta('Открываем popup для ' + dialogId);
                        return;
                    }}
                }} catch (e) {{
                    setError('BX24.openApplication error: ' + String(e));
                }}

                window.open('/popup?dialogId=' + encodeURIComponent(dialogId), '_blank');
            }}

            document.getElementById('openBtn').addEventListener('click', function () {{
                try {{
                    if (window.__dialogId) {{
                        openChecklist(window.__dialogId);
                    }} else {{
                        setError('dialogId ещё не определён');
                    }}
                }} catch (e) {{
                    setError(String(e));
                }}
            }});

            function finish(dialogId, sourceText) {{
                window.__dialogId = dialogId || '';
                setMeta('dialogId: ' + (window.__dialogId || 'не передан') + ' | source: ' + sourceText);

                try {{
                    if (window.BX24 && typeof window.BX24.fitWindow === 'function') {{
                        window.BX24.fitWindow();
                    }}
                }} catch (e) {{}}

                if (window.__dialogId && !autoOpened) {{
                    setTimeout(function() {{
                        openChecklist(window.__dialogId);
                    }}, 250);
                }}
            }}

            function canUseBx24() {{
                return !!(window.BX24 && typeof window.BX24.init === 'function');
            }}

            if (initialDialogId) {{
                finish(initialDialogId, 'server-post');
            }} else if (canUseBx24()) {{
                try {{
                    window.BX24.init(function () {{
                        var dialogId = '';
                        try {{
                            var info = window.BX24.placement.info();
                            if (info && info.options && info.options.dialogId) {{
                                dialogId = info.options.dialogId;
                            }}
                        }} catch (e) {{
                            setError('placement.info error: ' + String(e));
                        }}
                        finish(dialogId, 'BX24-js');
                    }});
                }} catch (e) {{
                    setError('BX24.init error: ' + String(e));
                    finish('', 'BX24-init-failed');
                }}
            }} else {{
                setError(initialContextText || 'BX24 не найден');
                finish('', 'local');
            }}
        </script>
    </body>
    </html>
    """


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def home_get(dialogId: str = "", mode: str = ""):
    dialog_id = normalize_dialog_id(dialogId)

    if dialog_id:
        return popup_get(dialog_id)

    return app_home_html()


@app.post("/", response_class=HTMLResponse)
async def home_post(request: Request):
    return app_home_html()


@app.get("/install", response_class=HTMLResponse)
def install_get():
    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>Bitrix24 Install</title>
    </head>
    <body style="font-family:Arial,sans-serif;padding:40px">
        <h1>Установка приложения</h1>
        <p>Если эта страница открыта внутри Bitrix24, она завершит установку приложения.</p>
        {install_finish_block()}
    </body>
    </html>
    """


@app.post("/install", response_class=HTMLResponse)
async def install_post(request: Request):
    form = dict(await request.form())

    access_token = form.get("AUTH_ID") or form.get("access_token") or ""
    domain = normalize_domain(form.get("DOMAIN") or form.get("domain") or "")
    base_url = str(request.base_url).rstrip("/")

    bind_result = {
        "im_textarea": {"skipped": True}
    }

    if domain and access_token:
        bind_result["im_textarea"] = bitrix_rest_call(
            domain,
            "placement.bind",
            access_token,
            {
                "PLACEMENT": "IM_TEXTAREA",
                "HANDLER": f"{base_url}/textarea",
                "TITLE": "Чек-лист ИД",
                "OPTIONS": {
                    "iconName": "fa-bars",
                    "context": "CHAT"
                }
            }
        )

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>Bitrix24 Install Callback</title>
    </head>
    <body style="font-family:Arial,sans-serif;padding:40px">
        <h1>Install callback получен</h1>
        <p>Если bind прошёл успешно, launcher будет зарегистрирован в IM_TEXTAREA.</p>

        <h2>Что прислал Bitrix24</h2>
        <pre>{html.escape(json.dumps(form, ensure_ascii=False, indent=2))}</pre>

        <h2>Ответ placement.bind</h2>
        <pre>{html.escape(json.dumps(bind_result, ensure_ascii=False, indent=2))}</pre>

        {install_finish_block()}
    </body>
    </html>
    """


@app.get("/textarea", response_class=HTMLResponse)
def textarea_get(dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    return textarea_html(dialog_id, "GET /textarea")


@app.post("/textarea", response_class=HTMLResponse)
async def textarea_post(request: Request):
    form = dict(await request.form())
    dialog_id = extract_dialog_id_from_form(form)
    raw_context = json.dumps(form, ensure_ascii=False, indent=2)

    print("TEXTAREA POST FORM:", raw_context)
    print("TEXTAREA EXTRACTED DIALOG ID:", dialog_id)

    return textarea_html(dialog_id, raw_context)


@app.get("/popup", response_class=HTMLResponse)
def popup_get(dialogId: str = "", checklistKey: str = "id"):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = str(checklistKey or "id").strip() or "id"
    data = get_checklist(dialog_id, checklist_key)

    title_raw = data.get("title", "Чек-лист ИД")
    title = html.escape(title_raw)
    collab_title_raw = (data.get("collabTitle", "") or "").strip()
    collab_title = html.escape(collab_title_raw)
    full_title = f"{title} — {collab_title}" if collab_title_raw else title
    progress_percent = int(data.get("progressPercent", 0) or 0)

    items_json = json.dumps(data.get("items", []), ensure_ascii=False)
    groups_json = json.dumps(data.get("groups", []), ensure_ascii=False)
    project_checklists_json = json.dumps(data.get("projectChecklists", []), ensure_ascii=False)
    dialog_id_json = json.dumps(dialog_id, ensure_ascii=False)
    collab_title_json = json.dumps(collab_title_raw, ensure_ascii=False)
    checklist_key_json = json.dumps(checklist_key, ensure_ascii=False)
    checklist_title_json = json.dumps(title_raw, ensure_ascii=False)
    checklist_title_json = json.dumps(title_raw, ensure_ascii=False)

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{full_title}</title>
        <script src="https://api.bitrix24.com/api/v1/"></script>
        <style>
            * {{ box-sizing: border-box; }}
            body {{ margin:0; font-family:Arial,sans-serif; background:#f3f6fb; color:#1f2328; }}
            .shell {{ padding:14px; }}
            .modal {{ background:#fff; border:1px solid #e5e7eb; border-radius:14px; overflow:hidden; box-shadow:0 16px 40px rgba(0,0,0,.12); }}
            .header {{ padding:14px 16px 12px; border-bottom:1px solid #edf0f2; display:flex; justify-content:space-between; align-items:flex-start; gap:14px; }}
            .title {{ font-size:22px; font-weight:700; line-height:1.2; }}
            .title small {{ font-size:20px; font-weight:600; color:#344054; }}
            .header-main {{ display:flex; align-items:flex-start; gap:18px; flex:1 1 auto; min-width:0; }}
            .header-right {{ display:flex; align-items:center; gap:14px; flex:0 0 auto; }}
            .progress-box {{ min-width:150px; }}
            .progress-label {{ font-size:12px; color:#667085; margin-bottom:4px; }}
            .progress-value {{ font-size:21px; font-weight:700; margin-bottom:5px; }}
            .progress-track {{ width:100%; height:8px; background:#edf2f7; border-radius:999px; overflow:hidden; }}
            .progress-bar {{ height:100%; width:0%; background:#22c55e; transition:width .2s ease; }}
            .save-state {{ font-size:12px; font-weight:700; padding:7px 10px; border-radius:999px; background:#eef2ff; color:#3730a3; white-space:nowrap; }}
            .save-state.saving {{ background:#fff4e5; color:#b26a00; }}
            .save-state.error {{ background:#fdecec; color:#b42318; }}
            .content {{ padding:14px 16px 16px; max-height:78vh; overflow:auto; }}
            .layout {{ display:grid; grid-template-columns:minmax(0,1fr) 240px; gap:14px; align-items:start; }}
            .tables-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:14px; align-items:start; }}
            .table-panel {{ min-width:0; display:flex; }}
            .table-panel .table {{ flex:1 1 auto; }}
            .side-panel {{ border:1px solid #e5e7eb; border-radius:12px; background:#fff; overflow:hidden; position:sticky; top:0; }}
            .side-panel-title {{ padding:12px 14px; background:#fafbfc; border-bottom:1px solid #e5e7eb; font-size:13px; font-weight:700; color:#344054; }}
            .side-panel-list {{ padding:10px; display:flex; flex-direction:column; gap:8px; }}
            .side-link {{ display:block; width:100%; text-align:left; border:1px solid #d0d7de; border-radius:8px; background:#fff; padding:9px 10px; font-size:13px; cursor:pointer; }}
            .side-link.active {{ background:#eef2ff; border-color:#c7d2fe; font-weight:700; }}
            .table {{ width:100%; border:1px solid #e5e7eb; border-radius:12px; overflow:hidden; background:#fff; }}
            .thead {{ position:sticky; top:0; z-index:10; background:#f8fafc; border-bottom:1px solid #e5e7eb; }}
            .thead-top,.thead-bottom {{ min-height:38px; }}
            .thead-top,.thead-bottom,.row {{ display:grid; grid-template-columns:190px 100px 100px 136px 136px; gap:0; align-items:stretch; justify-content:start; }}
            .th,.td {{ padding:8px 9px; border-right:1px solid #edf0f2; }}
            .th:last-child,.td:last-child {{ border-right:none; }}
            .th {{ font-size:12px; font-weight:700; color:#475467; min-height:38px; display:flex; align-items:center; }}
            .thead-top .th,.thead-bottom .th {{ min-height:38px; }}
            .th.center {{ text-align:center; justify-content:center; }}
            .group-block {{ border-top:8px solid #f8fafc; }}
            .group-title {{ padding:9px 12px; min-height:40px; background:#fafbfc; border-top:1px solid #e5e7eb; border-bottom:1px solid #e5e7eb; font-size:13px; font-weight:700; color:#344054; display:flex; align-items:center; }}
            .row {{ border-top:1px solid #edf0f2; background:#fff; }}
            .row.not-required {{ background:#fafafa; }}
            .row.not-required .item-name {{ text-decoration:line-through; color:#98a2b3; }}
            .cell-name {{ display:flex; align-items:center; gap:8px; min-width:0; }}
            .status-indicator {{ width:15px; height:15px; border-radius:999px; border:1px solid #d0d7de; flex:0 0 15px; background:#fff; }}
            .status-indicator.green {{ background:#22c55e; border-color:#22c55e; }}
            .status-indicator.gray {{ background:#9ca3af; border-color:#9ca3af; }}
            .item-name {{ font-size:13px; font-weight:700; color:#1f2328; line-height:1.15; min-width:0; word-break:break-word; max-width:165px; }}
            .status-select,.date-input {{ width:100%; border:1px solid #d0d7de; border-radius:8px; padding:6px 8px; font-size:12px; background:#fff; }}
            .doc-btn,.upload-btn,.add-item-btn {{ display:inline-block; width:100%; text-align:center; padding:6px 8px; border:1px solid #d0d7de; border-radius:8px; background:#f8fafc; color:#1f2328; font-size:12px; text-decoration:none; cursor:pointer; }}
            .doc-btn:hover,.upload-btn:hover,.side-link:hover,.add-item-btn:hover {{ background:#f1f5f9; }}
            .add-item-row {{ padding:9px 12px 10px; min-height:52px; border-top:1px solid #edf0f2; background:#fcfcfd; display:flex; gap:8px; align-items:center; justify-content:flex-start; }}
            .add-item-row::after {{ content:''; flex:1 1 auto; }}
            .add-item-input {{ flex:0 0 190px; width:190px; min-width:190px; height:32px; border:1px solid #d0d7de; border-radius:8px; padding:7px 9px; font-size:12px; }}
            .add-item-btn {{ flex:0 0 100px; width:100px; min-width:100px; height:32px; border:1px solid #d0d7de; border-radius:8px; padding:6px 8px; background:#f8fafc; cursor:pointer; font-size:12px; white-space:nowrap; }}
            @media (max-width:1320px) {{ .layout {{ grid-template-columns:1fr; }} .side-panel {{ position:static; }} }}
            @media (max-width:1120px) {{ .tables-grid {{ grid-template-columns:1fr; }} }}
            @media (max-width:980px) {{
                .thead-top,.thead-bottom,.row {{ grid-template-columns:1fr; }}
                .th,.td {{ border-right:none; border-bottom:1px solid #edf0f2; }}
                .th:last-child,.td:last-child {{ border-bottom:none; }}
            }}
        </style>
    </head>
    <body>
        <div class="shell">
            <div class="modal">
                <div class="header">
                    <div class="header-main">
                        <div class="title" id="popupTitle">Чек-лист ИД</div>
                        <div class="progress-box">
                            <div class="progress-label">Прогресс</div>
                            <div class="progress-value" id="progressValue">{progress_percent}%</div>
                            <div class="progress-track"><div class="progress-bar" id="progressBar"></div></div>
                        </div>
                    </div>
                    <div class="header-right">
                        <div id="saveState" class="save-state">Сохранено</div>
                    </div>
                </div>
                <div class="content">
                    <div id="debugPanel" style="
                        margin-bottom:12px;
                        padding:10px 12px;
                        border:1px solid #e5e7eb;
                        border-radius:10px;
                        background:#fafbfc;
                        font-size:12px;
                        color:#344054;
                    ">
                        <div><b>Debug:</b> <span id="debugLastEvent">popup init</span></div>
                        <div style="margin-top:4px;">
                            <a href="/debug/logs" target="_blank">Открыть /debug/logs</a>
                        </div>
                    </div>
                    <div class="layout">
                        <div class="tables-grid">
                            <div class="table-panel">
                                <div class="table">
                            <div class="thead">
                                <div class="thead-top">
                                    <div class="th">ИД</div>
                                    <div class="th">Документ</div>
                                    <div class="th">Статус</div>
                                    <div class="th center" style="grid-column: 4 / span 2;">Дата получения</div>
                                </div>
                                <div class="thead-bottom">
                                    <div class="th"></div>
                                    <div class="th"></div>
                                    <div class="th"></div>
                                    <div class="th">План</div>
                                    <div class="th">Факт</div>
                                </div>
                            </div>
                            <div id="leftTableBody"></div>
                                </div>
                            </div>
                            <div class="table-panel">
                                <div class="table">
                                    <div class="thead">
                                        <div class="thead-top">
                                            <div class="th">ИД</div>
                                            <div class="th">Документ</div>
                                            <div class="th">Статус</div>
                                            <div class="th center" style="grid-column: 4 / span 2;">Дата получения</div>
                                        </div>
                                        <div class="thead-bottom">
                                            <div class="th"></div>
                                            <div class="th"></div>
                                            <div class="th"></div>
                                            <div class="th">План</div>
                                            <div class="th">Факт</div>
                                        </div>
                                    </div>
                                    <div id="rightTableBody"></div>
                                </div>
                            </div>
                        </div>
                        <div class="side-panel">
                            <div class="side-panel-title">Список чек-листов по проекту</div>
                            <div class="side-panel-list" id="projectChecklistList"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <script>
            const dialogId = {dialog_id_json};
            const rawGroups = {groups_json};
            const rawProjectChecklists = {project_checklists_json};
            let rawItems = {items_json};
            let collabTitle = {collab_title_json};

            const groups = Array.isArray(rawGroups) ? rawGroups : [];
            const projectChecklists = Array.isArray(rawProjectChecklists) ? rawProjectChecklists : [];
            let items = Array.isArray(rawItems) ? rawItems : [];

            let sessionChanges = [];
            let currentEditor = {
                id: "",
                name: ""
            };

            const saveStateEl = document.getElementById('saveState');
            const leftTableBodyEl = document.getElementById('leftTableBody');
            const rightTableBodyEl = document.getElementById('rightTableBody');
            const progressValueEl = document.getElementById('progressValue');
            const progressBarEl = document.getElementById('progressBar');
            const popupTitleEl = document.getElementById('popupTitle');
            const projectChecklistListEl = document.getElementById('projectChecklistList');
            const debugLastEventEl = document.getElementById('debugLastEvent');
            const APP_BASE_URL = window.location.origin;
            let closeSummarySent = false;
            let sessionDirty = false;

            function setSaveState(mode, text) {{
                saveStateEl.classList.remove('saving', 'error');
                if (mode === 'saving') saveStateEl.classList.add('saving');
                if (mode === 'error') saveStateEl.classList.add('error');
                saveStateEl.textContent = text;
            }}
            function esc(v) {{
                if (v === null || v === undefined) return '';
                return String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
            }}
            function toInputDate(value) {{
                if (!value) return '';
                const parts = value.split('.');
                if (parts.length !== 3) return '';
                return `${{parts[2]}}-${{parts[1]}}-${{parts[0]}}`;
            }}
            function fromInputDate(value) {{
                if (!value) return '';
                const parts = value.split('-');
                if (parts.length !== 3) return '';
                return `${{parts[2]}}.${{parts[1]}}.${{parts[0]}}`;
            }}
            function normalizeStatus(status) {{
                const s = String(status || '').trim();
                if (s === 'Есть') return 'Есть';
                if (s === 'Нет') return 'Нет';
                if (s === 'Не требуется') return 'Не требуется';
                return '';
            }}
            function indicatorClass(status) {{
                const s = normalizeStatus(status);
                if (s === 'Есть') return 'status-indicator green';
                if (s === 'Нет' || s === 'Не требуется') return 'status-indicator gray';
                return 'status-indicator';
            }}
            function renderTitle() {{
                if (collabTitle) {{
                    popupTitleEl.innerHTML = esc(checklistTitle) + ' <small>— ' + esc(collabTitle) + '</small>';
                }} else {{
                    popupTitleEl.textContent = checklistTitle;
                }}
            }}
            function fetchCurrentUserIfPossible() {{
                try {{
                    if (!(window.BX24 && typeof window.BX24.init === 'function')) {{
                        return;
                    }}

                    window.BX24.init(function () {{
                        try {{
                            window.BX24.callMethod('user.current', {{}}, function(result) {{
                                try {{
                                    if (result.error()) {{
                                        return;
                                    }}

                                    const data = result.data() || {{}};
                                    const fullName = [data.NAME, data.LAST_NAME].filter(Boolean).join(' ').trim();

                    currentEditor = {{
                        id: String(data.ID || ''),
                        name: fullName || String(data.NAME || '') || ''
                    }};
                }} catch (e) {{
                                    console.log('user.current parse error:', e);
                                }}
                            }});
                        }} catch (e) {{
                            console.log('user.current call error:', e);
                        }}
                    }});
                }} catch (e) {{
                    console.log('fetchCurrentUserIfPossible skipped:', e);
                }}
            }}
            function setDebugText(text) {{
                if (debugLastEventEl) {{
                    debugLastEventEl.textContent = text;
                }}
            }}
            function debugLog(event, payload = {{}}, useBeacon = false) {{
                const body = JSON.stringify({{
                    event,
                    dialogId,
                    payload,
                    href: window.location.href,
                    ts: new Date().toISOString()
                }});
                
                setDebugText(event);

                try {{
                    const url = APP_BASE_URL + '/api/debug/event';

                    if (useBeacon && navigator.sendBeacon) {{
                        const blob = new Blob([body], {{ type: 'application/json' }});
                        const ok = navigator.sendBeacon(url, blob);
                        setDebugText(event + ' | beacon=' + ok);
                        return;
                    }}

                    fetch(url, {{
                        method: 'POST',
                        headers: {{
                            'Content-Type': 'application/json'
                        }},
                        body
                    }})
                    .then(r => {{
                        setDebugText(event + ' | http=' + r.status);
                    }})
                    .catch(err => {{
                        console.log('debugLog fetch error:', err);
                        setDebugText(event + ' | fetch error');
                    }});
                }} catch (e) {{
                    console.log('debugLog error:', e);
                    setDebugText(event + ' | js error');
                }}
            }}

            function sendCloseSummaryOnce(eventName) {{
                if (closeSummarySent) {{
                    return;
                }}

                closeSummarySent = true;

                const payload = {{
                    dialogId,
                    editor: currentEditor,
                    changes: sessionChanges,
                    closeEvent: eventName,
                    ts: new Date().toISOString()
                }};

                debugLog('close_summary_start', payload, true);

                try {{
                    const closeUrl = APP_BASE_URL + '/api/checklist/close-session';
                    const debugUrl = APP_BASE_URL + '/api/debug/event';

                    const closeBlob = new Blob([JSON.stringify(payload)], {{
                        type: 'application/json'
                    }});

                    const closeOk = navigator.sendBeacon
                        ? navigator.sendBeacon(closeUrl, closeBlob)
                        : false;

                    const debugBlob = new Blob([JSON.stringify({{
                        event: 'close_summary_beacon_sent',
                        dialogId,
                        payload: {{
                            closeEvent: eventName,
                            closeOk,
                            changesCount: sessionChanges.length,
                            editor: currentEditor
                        }},
                        ts: new Date().toISOString()
                    }})], {{
                        type: 'application/json'
                    }});

                    if (navigator.sendBeacon) {{
                        navigator.sendBeacon(debugUrl, debugBlob);
                    }}

                    setDebugText('close_summary_sent | beacon=' + closeOk + ' | changes=' + sessionChanges.length);
                }} catch (e) {{
                    console.log('sendCloseSummaryOnce error:', e);
                    setDebugText('close_summary_error');
                }}
            }}

            document.addEventListener('visibilitychange', function () {{
                if (document.visibilityState === 'hidden') {{
                    sendCloseSummaryOnce('popup_hidden');
                }}
            }});

            window.addEventListener('pagehide', function () {{
                sendCloseSummaryOnce('popup_pagehide');
            }});

            window.addEventListener('beforeunload', function () {{
                sendCloseSummaryOnce('popup_beforeunload');
            }});
            async function fetchChatTitleIfMissing() {{
                if (collabTitle) {{ renderTitle(); return; }}
                try {{
                    if (!(window.BX24 && typeof window.BX24.init === 'function')) {{ renderTitle(); return; }}
                    window.BX24.init(function () {{
                        try {{
                            window.BX24.callMethod('im.dialog.get', {{ dialog_id: dialogId }}, async function(result) {{
                                try {{
                                    if (result.error()) {{ renderTitle(); return; }}
                                    const data = result.data() || {{}};
                                    let title = data.title || data.name || (data.dialog && (data.dialog.title || data.dialog.name)) || (data.chat && (data.chat.title || data.chat.name)) || '';
                                    title = String(title || '').trim();
                                    if (!title) {{ renderTitle(); return; }}
                                    collabTitle = title;
                                    renderTitle();
                                    debugLog('chat_title_loaded', {{
                                        title: title
                                    }});
                                    try {{
                                        await fetch('/api/checklist/update-meta', {{
                                            method: 'POST',
                                            headers: {{ 'Content-Type': 'application/json' }},
                                            body: JSON.stringify({{ dialogId, checklistKey: currentChecklistKey, field: 'collabTitle', value: title }})
                                        }});
                                    }} catch (e) {{
                                        console.log('save collabTitle error:', e);
                                    }}
                                }} catch (e) {{
                                    console.log('im.dialog.get parse error:', e);
                                    renderTitle();
                                }}
                            }});
                        }} catch (e) {{
                            console.log('im.dialog.get call error:', e);
                            renderTitle();
                        }}
                    }});
                }} catch (e) {{
                    console.log('BX24 init for title skipped:', e);
                    renderTitle();
                }}
            }}
            function calculateProgress() {{
                if (currentChecklistKey !== 'id') {{
                    progressValueEl.textContent = '';
                    progressBarEl.style.width = '0%';
                    return;
                }}
                const activeItems = items.filter(x => normalizeStatus(x.status) !== 'Не требуется');
                const completedItems = activeItems.filter(x => normalizeStatus(x.status) === 'Есть');
                const activeCount = activeItems.length;
                const completedCount = completedItems.length;
                const percent = activeCount ? Math.round((completedCount / activeCount) * 100) : 0;
                progressValueEl.textContent = percent + '%';
                progressBarEl.style.width = percent + '%';
            }}
            async function updateItem(itemId, field, value, checklistKey = currentChecklistKey) {{
                setSaveState('saving', 'Сохраняем...');
                const response = await fetch('/api/checklist/update-item', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ dialogId, checklistKey, itemId, field, value }})
                }});
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'save failed');
                setSaveState('', 'Сохранено');
                return result;
            }}
            async function addItem(groupId, name, checklistKey = currentChecklistKey) {{
                setSaveState('saving', 'Сохраняем...');
                const response = await fetch('/api/checklist/add-item', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ dialogId, checklistKey, groupId, name }})
                }});
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'add item failed');
                setSaveState('', 'Сохранено');
                return result;
            }}
            async function removeDocument(itemId) {{
                setSaveState('saving', 'Сохраняем...');
                const response = await fetch('/api/checklist/remove-document', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ dialogId, checklistKey: currentChecklistKey, itemId }})
                }});
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'remove document failed');
                setSaveState('', 'Сохранено');
                return result;
            }}
            async function uploadDocument(itemId, file) {{
                setSaveState('saving', 'Сохраняем...');
                const formData = new FormData();
                formData.append('dialogId', dialogId);
                formData.append('itemId', itemId);
                formData.append('file', file);
                formData.append('checklistKey', currentChecklistKey);
                const response = await fetch('/api/checklist/upload-document', {{ method: 'POST', body: formData }});
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'upload document failed');
                setSaveState('', 'Сохранено');
                return result;
            }}
            function getItemsByGroup(groupId) {{
                return items.filter(item => item.group === groupId).sort((a, b) => a.order - b.order);
            }}
            function hasItemsInGroup(groupId) {{
                return getItemsByGroup(groupId).length > 0;
            }}
            function renderProjectChecklistList() {{
                projectChecklistListEl.innerHTML = projectChecklists.map(item => {{
                    const active = item.key === currentChecklistKey ? 'side-link active' : 'side-link';
                    return `<button type="button" class="${{active}}" data-checklist-key="${{esc(item.key)}}">${{esc(item.title)}}</button>`;
                }}).join('');
                projectChecklistListEl.querySelectorAll('[data-checklist-key]').forEach(btn => {{
                    btn.addEventListener('click', function () {{
                        const key = this.dataset.checklistKey;
                        if (key === 'opr') {{
                            alert('Этот чек-лист подключим следующим этапом.');
                            return;
                        }}
                        if (key === currentChecklistKey) {{
                            return;
                        }}
                        window.location.href = '/popup?dialogId=' + encodeURIComponent(dialogId) + '&checklistKey=' + encodeURIComponent(key);
                    }});
                }});
            }}
            function conceptIndicatorClass(item) {{
                const status = String(item.status || '').trim();
                const kind = String(item.statusKind || '').trim();

                if (status === 'Не требуется') {{
                    return 'status-indicator gray';
                }}

                if (kind === 'bool') {{
                    if (status === 'Да') return 'status-indicator green';
                    if (status === 'Нет') return 'status-indicator gray';
                    return 'status-indicator';
                }}

                return status ? 'status-indicator green' : 'status-indicator';
            }}
            function buildConceptStatusCell(item) {{
                if (item.statusKind === 'bool') {{
                    return `
                        <select class="status-select" data-role="concept-status" data-item-id="${{esc(item.id)}}">
                            <option value="" ${{item.status === '' ? 'selected' : ''}}></option>
                            <option value="Да" ${{item.status === 'Да' ? 'selected' : ''}}>Да</option>
                            <option value="Нет" ${{item.status === 'Нет' ? 'selected' : ''}}>Нет</option>
                            <option value="Не требуется" ${{item.status === 'Не требуется' ? 'selected' : ''}}>Не требуется</option>
                        </select>
                    `;
                }}
                if (item.statusKind === 'select') {{
                    const options = [''].concat(item.statusOptions || [], ['Не требуется']);
                    return `
                        <select class="status-select" data-role="concept-status" data-item-id="${{esc(item.id)}}">
                            ${{options.map(option => `<option value="${{esc(option)}}" ${{item.status === option ? 'selected' : ''}}>${{esc(option)}}</option>`).join('')}}
                        </select>
                    `;
                }}
                return `<input class="status-select" type="text" data-role="concept-status" data-item-id="${{esc(item.id)}}" placeholder="${{esc(item.statusPlaceholder || '')}}" value="${{esc(item.status || '')}}">`;
            }}
            function buildConceptExtraCell(item) {{
                return `<input class="status-select" type="text" data-role="concept-extra" data-item-id="${{esc(item.id)}}" placeholder="${{esc(item.extraInfoPlaceholder || '')}}" value="${{esc(item.extraInfo || '')}}">`;
            }}
            function renderConceptTable() {{
                if (!leftTableEl || !tablesGridEl) return;
                tablesGridEl.style.gridTemplateColumns = '1fr';
                if (tablePanels[1]) {{
                    tablePanels[1].style.display = 'none';
                }}
                const visibleGroups = groups.filter(group => {{
                    if (group.id !== 10) return true;
                    return items.some(x => x.group === 10);
                }});
                leftTableEl.innerHTML = `
                    <div class="thead">
                        <div class="thead-top" style="grid-template-columns: 1.2fr 0.8fr 110px 160px 1fr;">
                            <div class="th">Пункт</div>
                            <div class="th">Нормативы</div>
                            <div class="th">Документ</div>
                            <div class="th">Статус</div>
                            <div class="th">Доп информация</div>
                        </div>
                    </div>
                    <div id="conceptTableBody">${{visibleGroups.map(group => {{
                        const groupItems = getItemsByGroup(group.id);
                        const rows = groupItems.map(item => `
                            <div class="row" style="grid-template-columns: 1.2fr 0.8fr 110px 160px 1fr;" data-item-id="${{esc(item.id)}}">
                                <div class="td">
                                    <div class="cell-name">
                                        <div class="${{conceptIndicatorClass(item)}}"></div>
                                        <div class="item-name" style="${{item.status === 'Не требуется' ? 'text-decoration:line-through;color:#98a2b3;' : ''}}">
                                            ${{esc(item.name)}}
                                        </div>
                                    </div>
                                </div>
                                <div class="td">${{esc(item.source || '')}}</div>
                                <div class="td">${{buildDocumentCell(item)}}</div>
                                <div class="td">${{buildConceptStatusCell(item)}}</div>
                                <div class="td">${{buildConceptExtraCell(item)}}</div>
                            </div>
                        `).join('');
                        return `<div class="group-block"><div class="group-title">${{esc(group.title)}}</div>${{rows}}</div>`;
                    }}) .join('')}}</div>
                `;
            }}
            function renderGroup(group) {{
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = group.id !== 4;
                const rows = groupItems.map(item => {{
                    const rowClass = normalizeStatus(item.status) === 'Не требуется' ? 'row not-required' : 'row';
                    return `
                        <div class="${{rowClass}}" data-item-id="${{esc(item.id)}}">
                            <div class="td"><div class="cell-name"><div class="${{indicatorClass(item.status)}}"></div><div class="item-name">${{esc(item.name)}}</div></div></div>
                            <div class="td">${{buildDocumentCell(item)}}</div>
                            <div class="td">
                                <select class="status-select" data-role="status" data-item-id="${{esc(item.id)}}">
                                    <option value="" ${{normalizeStatus(item.status) === '' ? 'selected' : ''}}></option>
                                    <option value="Есть" ${{normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}}>Есть</option>
                                    <option value="Нет" ${{normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}}>Нет</option>
                                    <option value="Не требуется" ${{normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}}>Не требуется</option>
                                </select>
                            </div>
                            <div class="td"><input class="date-input" type="date" data-role="plan" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.plan))}}"></div>
                            <div class="td"><input class="date-input" type="date" data-role="fact" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.fact))}}"></div>
                        </div>
                    `;
                }}).join('');
                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="addItemInput_${{group.id}}" type="text" placeholder="Новый пункт">
                        <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${{group.id}}">Добавить пункт</button>
                    </div>` : '';
                return `<div class="group-block"><div class="group-title">${{esc(group.title)}}</div>${{rows}}${{addBlock}}</div>`;
            }}
            function renderTables() {{
                if (currentChecklistKey === 'concept') {{
                    renderConceptTable();
                    return;
                }}
                if (tablesGridEl) {{
                    tablesGridEl.style.gridTemplateColumns = '1fr 1fr';
                }}
                if (tablePanels[1]) {{
                    tablePanels[1].style.display = '';
                }}
                if (leftTableEl) leftTableEl.innerHTML = idLeftTableHtml;
                if (rightTableEl) rightTableEl.innerHTML = idRightTableHtml;
                const leftBody = document.getElementById('leftTableBody');
                const rightBody = document.getElementById('rightTableBody');
                const leftGroups = groups.filter(g => g.id === 1 || g.id === 3);
                const rightGroups = groups.filter(g => g.id === 2);

                if (hasItemsInGroup(4)) {{
                    const notRequiredGroup = groups.find(g => g.id === 4);
                    if (notRequiredGroup) {{
                        rightGroups.push(notRequiredGroup);
                    }}
                }}

                if (leftBody) leftBody.innerHTML = leftGroups.map(renderGroup).join('');
                if (rightBody) rightBody.innerHTML = rightGroups.map(renderGroup).join('');
            }}
            function renderAll() {{
                renderTables();
                bindEvents();
                calculateProgress();
                renderTitle();
                renderProjectChecklistList();
            }}
            function replaceItem(updatedItem) {{
                if (!updatedItem) return;
                const idx = items.findIndex(x => x.id === updatedItem.id);
                if (idx >= 0) items[idx] = updatedItem; else items.push(updatedItem);
            }}
            function bindEvents() {{
                document.querySelectorAll('[data-role="concept-status"]').forEach(el => {{
                    const handler = async function () {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;

                        const oldItem = JSON.parse(JSON.stringify(item));
                        const newValue = this.value;

                        item.status = newValue;
                        sessionDirty = true;

                        try {{
                            const result = await updateItem(this.dataset.itemId, 'status', newValue, currentChecklistKey);
                            replaceItem(result.item);
                            renderAll();
                        }} catch (e) {{
                            replaceItem(oldItem);
                            renderAll();
                            setSaveState('error', 'Ошибка сохранения');
                        }}
                    }};

                    el.addEventListener('change', handler);
                    el.addEventListener('blur', handler);
                }});

                document.querySelectorAll('[data-role="concept-extra"]').forEach(el => {{
                    const handler = async function () {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;

                        const oldItem = JSON.parse(JSON.stringify(item));
                        const newValue = this.value;

                        item.extraInfo = newValue;
                        sessionDirty = true;

                        try {{
                            const result = await updateItem(this.dataset.itemId, 'extraInfo', newValue, currentChecklistKey);
                            replaceItem(result.item);
                            renderAll();
                        }} catch (e) {{
                            replaceItem(oldItem);
                            renderAll();
                            setSaveState('error', 'Ошибка сохранения');
                        }}
                    }};

                    el.addEventListener('change', handler);
                    el.addEventListener('blur', handler);
                }});
                document.querySelectorAll('[data-role="status"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldItem = JSON.parse(JSON.stringify(item));
                        const changeCountBefore = sessionChanges.length;
                        const newValue = this.value;

                        item.status = newValue;
                        sessionChanges.push({{
                            field: 'status',
                            itemId: item.id,
                            itemName: item.name,
                            oldValue: oldItem.status || '',
                            newValue: newValue || ''
                        }});
                        sessionDirty = true;
                        debugLog('status_changed', {{
                            itemId: item.id,
                            itemName: item.name,
                            oldValue: oldItem.status || '',
                            newValue: newValue || ''
                        }});

                        if (this.selectedIndex === 2 && oldItem.documentUrl) {{
                            sessionChanges.push({{
                                field: 'document',
                                itemId: item.id,
                                itemName: item.name,
                                oldValue: 'uploaded',
                                newValue: 'removed'
                            }});
                            sessionDirty = true;
                            debugLog('document_removed_by_status', {{
                                itemId: item.id,
                                itemName: item.name,
                                oldValue: 'uploaded',
                                newValue: 'removed'
                            }});
                        }}

                        renderAll();

                        try {{
                            const result = await updateItem(this.dataset.itemId, 'status', newValue);
                            replaceItem(result.item);
                            renderAll();
                        }} catch (e) {{
                            sessionChanges.splice(changeCountBefore);
                            replaceItem(oldItem);
                            renderAll();
                            setSaveState('error', 'Save error');
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="plan"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldItem = JSON.parse(JSON.stringify(item));
                        const changeCountBefore = sessionChanges.length;
                        const newValue = fromInputDate(this.value);

                        item.plan = newValue;
                        sessionChanges.push({{
                            field: 'plan',
                            itemId: item.id,
                            itemName: item.name,
                            oldValue: oldItem.plan || '',
                            newValue: newValue || ''
                        }});
                        sessionDirty = true;
                        debugLog('plan_changed', {{
                            itemId: item.id,
                            itemName: item.name,
                            oldValue: oldItem.plan || '',
                            newValue: newValue || ''
                        }});

                        renderAll();

                        try {{
                            const result = await updateItem(this.dataset.itemId, 'plan', newValue);
                            replaceItem(result.item);
                            renderAll();
                        }} catch (e) {{
                            sessionChanges.splice(changeCountBefore);
                            replaceItem(oldItem);
                            renderAll();
                            setSaveState('error', 'Save error');
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="fact"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldItem = JSON.parse(JSON.stringify(item));
                        const changeCountBefore = sessionChanges.length;
                        const newValue = fromInputDate(this.value);

                        item.fact = newValue;
                        sessionChanges.push({{
                            field: 'fact',
                            itemId: item.id,
                            itemName: item.name,
                            oldValue: oldItem.fact || '',
                            newValue: newValue || ''
                        }});
                        sessionDirty = true;
                        debugLog('fact_changed', {{
                            itemId: item.id,
                            itemName: item.name,
                            oldValue: oldItem.fact || '',
                            newValue: newValue || ''
                        }});

                        renderAll();

                        try {{
                            const result = await updateItem(this.dataset.itemId, 'fact', newValue);
                            replaceItem(result.item);
                            renderAll();
                        }} catch (e) {{
                            sessionChanges.splice(changeCountBefore);
                            replaceItem(oldItem);
                            renderAll();
                            setSaveState('error', 'Save error');
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="add-item"]').forEach(btn => {{
                    btn.addEventListener('click', async function() {{
                        const groupId = Number(this.dataset.groupId);
                        const input = document.getElementById('addItemInput_' + groupId);
                        if (!input) return;
                        const name = (input.value || '').trim();
                        if (!name) return;

                        try {{
                            const result = await addItem(groupId, name);
                            replaceItem(result.item);
                            if (result.item) {{
                                sessionChanges.push({{
                                    field: 'add-item',
                                    itemId: result.item.id,
                                    itemName: result.item.name,
                                    oldValue: '',
                                    newValue: result.item.name
                                }});
                                sessionDirty = true;
                                debugLog('item_added', {{
                                    itemId: result.item.id,
                                    itemName: result.item.name,
                                    groupId: groupId
                                }});
                            }}
                            input.value = '';
                            renderAll();
                        }} catch (e) {{
                            setSaveState('error', 'Add item error');
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="upload"]').forEach(btn => {{
                    btn.addEventListener('click', function() {{
                        const input = document.querySelector('[data-role="file-input"][data-item-id="' + this.dataset.itemId + '"]');
                        if (input) input.click();
                    }});
                }});

                document.querySelectorAll('[data-role="view-document"]').forEach(btn => {{
                    btn.addEventListener('click', function () {{
                        const rawUrl = this.dataset.documentUrl || '';
                        if (!rawUrl) return;
                        try {{
                            const absoluteUrl = new URL(rawUrl, window.location.origin).href;
                            window.open(absoluteUrl, '_blank', 'noopener,noreferrer');
                        }} catch (e) {{
                            console.log('open document error:', e);
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="file-input"]').forEach(input => {{
                    input.addEventListener('change', async function() {{
                        const file = this.files && this.files[0];
                        if (!file) return;
                        const item = items.find(x => x.id === this.dataset.itemId);
                        const oldStatus = item ? normalizeStatus(item.status) : '';

                        try {{
                            const result = await uploadDocument(this.dataset.itemId, file);
                            replaceItem(result.item);
                            if (result.item) {{
                                sessionChanges.push({{
                                    field: 'document',
                                    itemId: result.item.id,
                                    itemName: result.item.name,
                                    oldValue: '',
                                    newValue: 'uploaded'
                                }});
                                sessionDirty = true;
                                debugLog('document_uploaded', {{
                                    itemId: result.item.id,
                                    itemName: result.item.name,
                                    documentUrl: result.item.documentUrl || ''
                                }});

                                if ((result.item.status || '') && (result.item.status || '') !== (oldStatus || '')) {{
                                    sessionChanges.push({{
                                        field: 'status',
                                        itemId: result.item.id,
                                        itemName: result.item.name,
                                        oldValue: oldStatus || '',
                                        newValue: result.item.status || ''
                                    }});
                                    sessionDirty = true;
                                    debugLog('status_changed_by_upload', {{
                                        itemId: result.item.id,
                                        itemName: result.item.name,
                                        oldValue: oldStatus || '',
                                        newValue: result.item.status || ''
                                    }});
                                }}
                            }}
                            renderAll();
                        }} catch (e) {{
                            setSaveState('error', 'Upload error');
                        }}
                    }});
                }});
            }}
            function safeInitBx24ForPopup() {{
                try {{
                    if (window.BX24 && typeof window.BX24.init === 'function') {{
                        window.BX24.init(function () {{
                            try {{
                                if (typeof window.BX24.resizeWindow === 'function') {{
                                    window.BX24.resizeWindow(1180, 680);
                                }}
                            }} catch (e) {{
                                console.log('BX24.resizeWindow error:', e);
                            }}
                        }});
                    }}
                }} catch (e) {{
                    console.log('BX24.init skipped:', e);
                }}
            }}
            renderAll();
            debugLog('popup_loaded', {{
                href: window.location.href,
                hasDialogId: !!dialogId
            }});
            fetchChatTitleIfMissing();
            fetchCurrentUserIfPossible();
            safeInitBx24ForPopup();
        </script>
    </body>
    </html>
    """


@app.get("/view", response_class=HTMLResponse)
def view_get(dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    data = get_checklist(dialog_id)

    items_html = ""
    for item in data.get("items", []):
        items_html += f"""
        <div style="border-top:1px solid #eee;padding:12px 0;">
            <div style="font-weight:700;margin-bottom:4px;">{html.escape(item.get('name', ''))}</div>
            <div style="font-size:13px;color:#444;">Статус: {html.escape(item.get('status', '—'))}</div>
            <div style="font-size:13px;color:#666;">План: {html.escape(item.get('plan', '—'))}</div>
            <div style="font-size:13px;color:#666;">Факт: {html.escape(item.get('fact', '—'))}</div>
        </div>
        """

    if not items_html:
        items_html = '<div style="color:#666;">Нет пунктов чек-листа</div>'

    notice_html = ""
    if data.get("notice"):
        notice_html = f"""
        <div style="background:#fff8e1;border:1px solid #f3d37a;border-radius:10px;padding:12px;margin-bottom:16px;">
            {html.escape(data.get("notice", ""))}
        </div>
        """

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{html.escape(data.get("title", "Чек-лист ИД"))}</title>
    </head>
    <body style="font-family:Arial,sans-serif;padding:24px;max-width:900px;margin:0 auto;">
        <h1 style="margin-bottom:8px;">{html.escape(data.get("title", "Чек-лист ИД"))}</h1>
        <div style="color:#666;margin-bottom:6px;">dialogId: {html.escape(dialog_id or 'не передан')}</div>
        <div style="color:#666;margin-bottom:6px;">Срок по договору: {html.escape(data.get("contractDeadline", "—"))}</div>
        <div style="color:#666;margin-bottom:20px;">Начало работ: {html.escape(data.get("startDate", "—"))}</div>

        {notice_html}

        <div style="border:1px solid #e5e7eb;border-radius:12px;padding:16px;">
            {items_html}
        </div>
    </body>
    </html>
    """


@app.get("/api/checklist")
def api_checklist(dialogId: str = "", checklistKey: str = "id"):
    dialogId = normalize_dialog_id(dialogId)
    return JSONResponse(get_checklist(dialogId, checklistKey))


@app.post("/api/checklist/update-item")
async def api_checklist_update_item(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = str(payload.get("checklistKey") or "id").strip() or "id"
    item_id = str(payload.get("itemId") or "").strip()
    field = str(payload.get("field") or "").strip()
    value = payload.get("value")

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    allowed_fields = {"priority", "status", "plan", "fact", "extraInfo"}
    if field not in allowed_fields:
        return JSONResponse({"ok": False, "error": "invalid field"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", [])

    target_item = None
    for item in items:
        if str(item.get("id")) == item_id:
            target_item = item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

    if field == "priority":
        target_item["priority"] = normalize_priority(value)
    elif field == "status":
        new_value = clean_cell_value(value)

        if checklist_key == "concept":
            target_item["status"] = new_value

            status_kind = str(target_item.get("statusKind") or "").strip()

            if new_value == "Не требуется":
                target_item["group"] = 10
                target_item["priority"] = "gray"
            else:
                if target_item.get("group") == 10:
                    target_item["group"] = resolve_concept_group_id_by_item_id_or_name(target_item)

                if status_kind == "bool":
                    if new_value == "Да":
                        target_item["priority"] = "green"
                    elif new_value == "Нет":
                        target_item["priority"] = "gray"
                    else:
                        target_item["priority"] = "white"
                else:
                    target_item["priority"] = "green" if new_value else "white"
        else:
            new_status = normalize_status(value)
            target_item["status"] = new_status
            target_item["priority"] = derive_indicator_from_status(new_status)

            if new_status == "Нет":
                remove_item_document_file(target_item)
                target_item["documentUrl"] = ""
                target_item["documentName"] = ""
    elif field == "plan":
        target_item["plan"] = normalize_date_string(value)
    elif field == "fact":
        target_item["fact"] = normalize_date_string(value)
    elif field == "extraInfo":
        target_item["extraInfo"] = clean_cell_value(value)

    data["items"] = items
    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)
    updated_item = None
    for item in data.get("items", []):
        if str(item.get("id")) == item_id:
            updated_item = item
            break

    return JSONResponse({
        "ok": True,
        "item": updated_item,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "progressPercent": data.get("progressPercent", 0),
    })


@app.post("/api/checklist/update-meta")
async def api_checklist_update_meta(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = str(payload.get("checklistKey") or "id").strip() or "id"
    field = str(payload.get("field") or "").strip()
    value = payload.get("value")

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if field != "collabTitle":
        return JSONResponse({"ok": False, "error": "only collabTitle is supported now"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    data["collabTitle"] = clean_cell_value(value)

    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "field": field,
        "value": data.get("collabTitle", ""),
        "progressPercent": data.get("progressPercent", 0),
    })


@app.post("/api/checklist/add-item")
async def api_checklist_add_item(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = str(payload.get("checklistKey") or "id").strip() or "id"
    group_id = int(payload.get("groupId") or 0)
    name = clean_cell_value(payload.get("name"))

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if checklist_key != "id":
        return JSONResponse({"ok": False, "error": "add-item is only supported for id"}, status_code=400)

    if group_id not in [1, 2, 3]:
        return JSONResponse({"ok": False, "error": "invalid groupId"}, status_code=400)

    if not name:
        return JSONResponse({"ok": False, "error": "name is required"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", [])

    group_items = [x for x in items if x.get("group") == group_id]
    next_order = len(group_items) + 1

    new_item = {
        "id": build_item_id(group_id, next_order) + "_custom",
        "group": group_id,
        "order": next_order,
        "name": name,
        "priority": "white",
        "status": "",
        "plan": "",
        "fact": "",
        "documentUrl": "",
        "documentName": "",
        "isCustom": True,
    }

    items.append(new_item)
    data["items"] = items
    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)
    created_item = None
    for item in data.get("items", []):
        if str(item.get("id")) == new_item["id"]:
            created_item = item
            break

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "item": created_item or new_item,
        "progressPercent": data.get("progressPercent", 0),
    })


@app.post("/api/checklist/upload-document")
async def api_checklist_upload_document(
    dialogId: str = Form(...),
    itemId: str = Form(...),
    file: UploadFile = File(...),
    checklistKey: str = Form("id")
):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = str(checklistKey or "id").strip() or "id"
    item_id = str(itemId or "").strip()

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", [])

    target_item = None
    for item in items:
        if str(item.get("id")) == item_id:
            target_item = item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

    remove_item_document_file(target_item)

    rel_path = build_upload_rel_path(dialog_id, item_id, file.filename or "file.bin")
    abs_path = UPLOAD_ROOT / rel_path
    abs_path.parent.mkdir(parents=True, exist_ok=True)

    file_bytes = await file.read()
    with open(abs_path, "wb") as f:
        f.write(file_bytes)

    target_item["documentUrl"] = "/uploads/" + rel_path.replace("\\", "/")
    target_item["documentName"] = Path(file.filename or "file.bin").name

    if checklist_key == "id" and int(target_item.get("group") or 0) != 4:
        target_item["status"] = "Есть"
        target_item["priority"] = derive_indicator_from_status("Есть")

    data["items"] = items
    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)

    updated_item = None
    for item in data.get("items", []):
        if str(item.get("id")) == item_id:
            updated_item = item
            break

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "item": updated_item,
        "progressPercent": data.get("progressPercent", 0),
    })


@app.post("/api/checklist/remove-document")
async def api_checklist_remove_document(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = str(payload.get("checklistKey") or "id").strip() or "id"
    item_id = str(payload.get("itemId") or "").strip()

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", [])

    target_item = None
    for item in items:
        if str(item.get("id")) == item_id:
            target_item = item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

    remove_item_document_file(target_item)
    target_item["documentUrl"] = ""
    target_item["documentName"] = ""

    data["items"] = items
    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)

    updated_item = None
    for item in data.get("items", []):
        if str(item.get("id")) == item_id:
            updated_item = item
            break

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "item": updated_item,
        "progressPercent": data.get("progressPercent", 0),
    })


@app.post("/api/debug/event")
async def api_debug_event(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raw = await request.body()
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            payload = {"raw": raw.decode("utf-8", errors="ignore")}

    event = str(payload.get("event") or "unknown").strip()
    write_debug_log(event, payload)

    return JSONResponse({"ok": True})


@app.post("/api/checklist/close-session")
async def api_checklist_close_session(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raw = await request.body()
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            payload = {"raw": raw.decode("utf-8", errors="ignore")}

    write_debug_log("close_session_received", payload)

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    changes = payload.get("changes") or []
    editor = payload.get("editor") or {}

    if not dialog_id:
        write_debug_log("close_session_invalid", {
            "reason": "dialogId is required",
            "payload": payload
        })
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not changes:
        write_debug_log("close_session_skipped", {
            "dialogId": dialog_id,
            "reason": "no changes"
        })
        return JSONResponse({"ok": True, "skipped": True, "reason": "no changes"})

    data = get_checklist(dialog_id)
    message = build_checklist_chat_message(data, changes, editor)

    result = bitrix_webhook_call("im.message.add", {
        "DIALOG_ID": dialog_id,
        "MESSAGE": message,
    })

    write_debug_log("close_session_im_message_add_result", {
        "dialogId": dialog_id,
        "changesCount": len(changes),
        "editor": editor,
        "result": result
    })

    return JSONResponse({
        "ok": "error" not in result,
        "dialogId": dialog_id,
        "result": result,
    })


@app.get("/debug/logs", response_class=HTMLResponse)
def debug_logs():
    if not DEBUG_LOG_PATH.exists():
        content = "Логов пока нет"
    else:
        with open(DEBUG_LOG_PATH, "r", encoding="utf-8") as f:
            content = f.read() or "Логов пока нет"

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>Debug Logs</title>
    </head>
    <body style="font-family:Arial,sans-serif;padding:24px">
        <h1>Debug logs</h1>
        <pre style="white-space:pre-wrap;word-break:break-word;">{html.escape(content)}</pre>
    </body>
    </html>
    """

@app.get("/admin", response_class=HTMLResponse)
def admin():
    conn = get_conn()
    rows = conn.execute("SELECT dialog_id, title FROM checklists ORDER BY dialog_id").fetchall()
    conn.close()

    items = "".join(
        f"<li><b>{html.escape(row['dialog_id'])}</b> — {html.escape(row['title'])}</li>"
        for row in rows
    ) or "<li>Пока ничего не загружено</li>"

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>Загрузка чек-листа</title>
    </head>
    <body style="font-family:Arial,sans-serif;padding:40px;max-width:900px">
        <h1>Загрузка Excel для коллабы</h1>
        <p>Шаг 1: откройте коллабу и посмотрите значение <b>dialogId</b> в sidebar.</p>
        <p>Шаг 2: вставьте этот dialogId сюда и загрузите .xlsx файл.</p>

        <form action="/admin/upload" method="post" enctype="multipart/form-data" style="margin:30px 0">
            <div style="margin-bottom:16px">
                <label>dialogId</label><br>
                <input type="text" name="dialog_id" required style="width:100%;padding:10px">
            </div>

            <div style="margin-bottom:16px">
                <label>XLSX файл</label><br>
                <input type="file" name="file" accept=".xlsx" required>
            </div>

            <button type="submit" style="padding:10px 16px">Загрузить чек-лист</button>
        </form>

        <h2>Уже загруженные чек-листы</h2>
        <ul>{items}</ul>
    </body>
    </html>
    """


@app.post("/admin/upload", response_class=HTMLResponse)
async def admin_upload(dialog_id: str = Form(...), file: UploadFile = File(...)):
    dialog_id = normalize_dialog_id(dialog_id)
    file_bytes = await file.read()
    data = parse_xlsx_to_checklist(file_bytes)
    save_checklist(dialog_id, data)

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>Готово</title>
    </head>
    <body style="font-family:Arial,sans-serif;padding:40px">
        <h1>Чек-лист сохранён</h1>
        <p><b>dialogId:</b> {html.escape(dialog_id)}</p>
        <p><b>Файл:</b> {html.escape(file.filename or '')}</p>
        <p>Теперь вернитесь в коллабу и обновите sidebar.</p>
        <p><a href="/admin">Назад в /admin</a></p>
    </body>
    </html>
    """


@app.get("/api/test")
def api_test():
    return JSONResponse({
        "ok": True,
        "message": "API проекта работает"
    })
