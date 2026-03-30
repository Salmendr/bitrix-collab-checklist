import os
import uuid
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

DB_PATH = "app.db"
APP_PORTAL_PATH = "/marketplace/app/84/"
UPLOAD_ROOT = Path("uploads")
CHECKLIST_UPLOAD_ROOT = UPLOAD_ROOT / "checklists"

PROJECT_CHECKLISTS = [
    {"key": "id", "title": "Р В§Р ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ Р В�Р вЂќ"},
    {"key": "opr", "title": "Р В§Р ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ Р С›Р СџР В "},
    {"key": "concept", "title": "Р В§Р ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ Р С™Р С•Р Р…РЎвЂ Р ВµР С—РЎвЂ Р С‘РЎРЏ"},
]

UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
CHECKLIST_UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_ROOT)), name="uploads")

CHECKLIST_GROUPS = {
    1: {
        "title": "Р В�Р вЂќ",
        "items": [
            "Р СџР СџР Сћ",
            "Р РЋР С•Р С”РЎР‚Р В°РЎвЂ°Р ВµР Р…Р С‘Р Вµ Р С›Р С™Р Сњ",
            "Р вЂ™РЎвЂ№Р С—Р С‘РЎРѓР С”Р В° Р вЂўР вЂњР В Р Сњ",
            "Р вЂњР СџР вЂ”Р Р€",
            "Р В�Р вЂњР вЂќР В�",
            "Р В�Р вЂњР В�",
            "Р В�Р В­Р В�",
            "Р В�Р вЂњР СљР В�",
            "Р В�Р вЂњР В� Р РЋР СљР В ",
        ],
    },
    2: {
        "title": "Р СћР Р€",
        "items": [
            "Р СћР Р€ Р РЋР Р†Р ВµРЎвЂљ",
            "Р СћР Р€ Р вЂ™Р С•Р Т‘Р С•РЎРѓР Р…Р В°Р В±Р В¶Р ВµР Р…Р С‘Р Вµ",
            "Р СћР Р€ Р вЂ�РЎвЂ№РЎвЂљР С•Р Р†Р В°РЎРЏ Р С”Р В°Р Р…Р В°Р В»Р С‘Р В·Р В°РЎвЂ Р С‘РЎРЏ",
            "Р В Р В°РЎРѓР С—Р С•Р В»Р С•Р В¶Р ВµР Р…Р С‘Р Вµ Р С—Р С•Р В¶ Р С–Р С‘Р Т‘РЎР‚Р В°Р Р…РЎвЂљР С•Р Р†",
            "Р СћР Р€ Р РЋР РЋ",
            "Р СћР Р€ Р СћР РЋ",
            "Р СћР Р€ Р вЂњР В°Р В·",
            "Р СћР Р€ Р вЂєР С‘Р Р†Р Р…Р ВµР Р†Р С”Р В°",
            "Р СћР Р€ Р Р†РЎвЂ№Р Р…Р С•РЎРѓРЎвЂ№",
            "Р РЋР СћР Р€",
        ],
    },
    3: {
        "title": "Р СџРЎР‚Р С•РЎвЂЎР ВµР Вµ",
        "items": [
            "Р вЂ”Р В°Р Т‘Р В°Р Р…Р С‘Р Вµ Р Р…Р В° Р В»Р С‘РЎвЂћРЎвЂљРЎвЂ№",
            "Р С’РЎРЊРЎР‚Р С•Р С—Р С•РЎР‚РЎвЂљ",
            "Р СџРЎР‚Р С‘Р С�РЎвЂ№Р С”Р В°Р Р…Р С‘Р Вµ Р С›Р вЂќР вЂќ",
            "Р СџР С•РЎР‚РЎС“Р В±Р С•РЎвЂЎР Р…РЎвЂ№Р в„– Р В»Р С‘РЎРѓРЎвЂљ",
            "Р РЋР С—РЎР‚Р В°Р Р†Р С”Р В° Р Р†РЎвЂ№Р Р†Р С•Р В·Р В° Р С�РЎС“РЎРѓР С•РЎР‚Р В°",
        ],
    },
    4: {
        "title": "Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ",
        "items": [],
    },
}

STATUS_OPTIONS = [
    "",
    "Р вЂўРЎРѓРЎвЂљРЎРЉ",
    "Р СњР ВµРЎвЂљ",
    "Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ",
]

PRIORITY_OPTIONS = ["white", "green", "gray"]


def build_default_groups():
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


def clean_cell_value(value):
    value = str(value or "").strip()
    if value == "РІР‚вЂќ":
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
        "Р ВµРЎРѓРЎвЂљРЎРЉ": "Р вЂўРЎРѓРЎвЂљРЎРЉ",
        "Р Р…Р ВµРЎвЂљ": "Р СњР ВµРЎвЂљ",
        "Р Р…Р Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ": "Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ",
        "Р С—Р С•Р Т‘Р С—Р С‘РЎРѓР В°Р Р…": "Р вЂўРЎРѓРЎвЂљРЎРЉ",
        "Р В·Р В°Р С—РЎР‚Р С•РЎРѓ Р С•Р С—РЎР‚Р С•РЎРѓР Р…Р С•Р С–Р С• Р В»Р С‘РЎРѓРЎвЂљР В°": "",
        "Р Т‘Р С•Р С–Р С•Р Р†Р С•РЎР‚ РЎвЂљР ВµРЎвЂ¦ Р С—РЎР‚Р С‘РЎРѓ": "",
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

    # Р С›РЎРѓРЎвЂљР В°Р Р†Р В»РЎРЏР ВµР С� Р С”Р В°Р С” Р ВµРЎРѓРЎвЂљРЎРЉ, Р ВµРЎРѓР В»Р С‘ РЎРЊРЎвЂљР С• РЎС“Р В¶Р Вµ РЎвЂћР С•РЎР‚Р С�Р В°РЎвЂљ Р вЂќР вЂќ.Р СљР Сљ.Р вЂњР вЂњР вЂњР вЂњ
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
    if status == "Р вЂўРЎРѓРЎвЂљРЎРЉ":
        return "green"
    if status == "Р СњР ВµРЎвЂљ" or status == "Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ":
        return "gray"
    return "white"


def move_item_to_required_group(item: dict) -> int:
    status = normalize_status(item.get("status"))
    if status == "Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ":
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


def build_default_checklist_template(dialog_id: str = ""):
    items = []

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
        "title": "Р В§Р ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ Р В�Р вЂќ",
        "collabTitle": "",
        "contractDeadline": "",
        "startDate": "",
        "groups": build_default_groups(),
        "items": items,
        "notice": "",
    })


def calculate_progress(items: list) -> dict:
    items = items or []

    active_items = [x for x in items if normalize_status(x.get("status")) != "Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ"]
    completed_items = [x for x in active_items if normalize_status(x.get("status")) == "Р вЂўРЎРѓРЎвЂљРЎРЉ"]

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


def normalize_checklist_data(data: dict) -> dict:
    data = dict(data or {})

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
        "title": data.get("title") or "Р В§Р ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ Р В�Р вЂќ",
        "collabTitle": clean_cell_value(data.get("collabTitle")),
        "contractDeadline": "",
        "startDate": "",
        "groups": [
            {"id": 1, "title": "Р В�Р вЂќ"},
            {"id": 2, "title": "Р СћР Р€"},
            {"id": 3, "title": "Р СџРЎР‚Р С•РЎвЂЎР ВµР Вµ"},
            {"id": 4, "title": "Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ"},
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
        "title": "Р В§Р ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ Р В�Р вЂќ",
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

def save_checklist(dialog_id: str, data: dict):
    dialog_id = normalize_dialog_id(dialog_id)

    conn = get_conn()
    conn.execute("""
        INSERT INTO checklists(dialog_id, title, data_json)
        VALUES (?, ?, ?)
        ON CONFLICT(dialog_id) DO UPDATE SET
            title=excluded.title,
            data_json=excluded.data_json
    """, (dialog_id, data.get("title", "Р В§Р ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ"), json.dumps(data, ensure_ascii=False)))
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

    # Р СњР В° РЎРѓР В»РЎС“РЎвЂЎР В°Р в„– Р Р…Р ВµР С•Р В¶Р С‘Р Т‘Р В°Р Р…Р Р…РЎвЂ№РЎвЂ¦ Р С‘Р С�Р ВµР Р… Р С—Р С•Р В»Р ВµР в„–
    for key, value in form.items():
        if "dialog" in str(key).lower():
            value = normalize_dialog_id(value)
            if value:
                return value

    return ""

def get_checklist(dialog_id: str):
    raw_id = str(dialog_id or "").strip()
    normalized_id = normalize_dialog_id(raw_id)

    aliases = []
    for candidate in [raw_id, normalized_id]:
        if candidate and candidate not in aliases:
            aliases.append(candidate)

    if normalized_id.startswith("chat") and normalized_id[4:].isdigit():
        numeric_id = normalized_id[4:]
        if numeric_id not in aliases:
            aliases.append(numeric_id)

    conn = get_conn()

    row = None
    found_alias = None

    for candidate in aliases:
        row = conn.execute(
            "SELECT data_json FROM checklists WHERE dialog_id = ?",
            (candidate,)
        ).fetchone()
        if row:
            found_alias = candidate
            break

    conn.close()

    if row:
        data = json.loads(row["data_json"])
        data = normalize_checklist_data(data)
        data["resolvedDialogId"] = normalized_id
        data["lookupAliases"] = aliases
        data["foundAlias"] = found_alias
        return data

    data = build_default_checklist_template(normalized_id)
    save_checklist(normalized_id, data)

    data["resolvedDialogId"] = normalized_id
    data["lookupAliases"] = aliases
    data["foundAlias"] = normalized_id
    return data


def app_home_html():
    return """
    <html>
    <head>
        <meta charset="utf-8">
        <title>Р§РµРє-Р»РёСЃС‚ Р�Р”</title>
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
        <h1>Р§РµРє-Р»РёСЃС‚ Р�Р”</h1>
        <p>РћС‚РєСЂС‹РІР°Р№С‚Рµ РїСЂРёР»РѕР¶РµРЅРёРµ РёР· Bitrix24.</p>
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
        <title>Р§РµРє-Р»РёСЃС‚ Р�Р” вЂ” textarea</title>
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
            <div class="dot">в‰Ў</div>
            <div class="main">
                <div class="title">Р§РµРє-Р»РёСЃС‚ Р�Р”</div>
                <div class="meta" id="meta">Р�РЅРёС†РёР°Р»РёР·Р°С†РёСЏ...</div>
                <button class="btn" id="openBtn" type="button">РћС‚РєСЂС‹С‚СЊ С‡РµРє-Р»РёСЃС‚</button>
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
                    setError('dialogId РЅРµ РЅР°Р№РґРµРЅ');
                    return;
                }}

                try {{
                    localStorage.setItem('checklist_pending_dialog', JSON.stringify({{
                        dialogId,
                        ts: Date.now()
                    }}));
                }} catch (e) {{
                    console.log('localStorage save error:', e);
                }}

                try {{
                    if (window.BX24 && typeof window.BX24.openApplication === 'function') {{
                        BX24.openApplication();
                        autoOpened = true;
                        setMeta('РћС‚РєСЂС‹РІР°РµРј popup РґР»СЏ ' + dialogId);
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
                        setError('dialogId РµС‰С‘ РЅРµ РѕРїСЂРµРґРµР»С‘РЅ');
                    }}
                }} catch (e) {{
                    setError(String(e));
                }}
            }});

            function finish(dialogId, sourceText) {{
                window.__dialogId = dialogId || '';
                setMeta('dialogId: ' + (window.__dialogId || 'РЅРµ РїРµСЂРµРґР°РЅ') + ' | source: ' + sourceText);

                try {{
                    if (typeof BX24 !== 'undefined') {{
                        BX24.fitWindow();
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
                setError(initialContextText || 'BX24 РЅРµ РЅР°Р№РґРµРЅ');
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
        <h1>Р Р€РЎРѓРЎвЂљР В°Р Р…Р С•Р Р†Р С”Р В° Р С—РЎР‚Р С‘Р В»Р С•Р В¶Р ВµР Р…Р С‘РЎРЏ</h1>
        <p>Р вЂўРЎРѓР В»Р С‘ РЎРЊРЎвЂљР В° РЎРѓРЎвЂљРЎР‚Р В°Р Р…Р С‘РЎвЂ Р В° Р С•РЎвЂљР С”РЎР‚РЎвЂ№РЎвЂљР В° Р Р†Р Р…РЎС“РЎвЂљРЎР‚Р С‘ Bitrix24, Р С•Р Р…Р В° Р В·Р В°Р Р†Р ВµРЎР‚РЎв‚¬Р С‘РЎвЂљ РЎС“РЎРѓРЎвЂљР В°Р Р…Р С•Р Р†Р С”РЎС“ Р С—РЎР‚Р С‘Р В»Р С•Р В¶Р ВµР Р…Р С‘РЎРЏ.</p>
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
                "TITLE": "Р§РµРє-Р»РёСЃС‚ Р�Р”",
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
        <h1>Install callback РїРѕР»СѓС‡РµРЅ</h1>
        <p>Р•СЃР»Рё bind РїСЂРѕС€С‘Р» СѓСЃРїРµС€РЅРѕ, launcher Р±СѓРґРµС‚ Р·Р°СЂРµРіРёСЃС‚СЂРёСЂРѕРІР°РЅ РІ IM_TEXTAREA.</p>

        <h2>Р§С‚Рѕ РїСЂРёСЃР»Р°Р» Bitrix24</h2>
        <pre>{html.escape(json.dumps(form, ensure_ascii=False, indent=2))}</pre>

        <h2>РћС‚РІРµС‚ placement.bind</h2>
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
def popup_get(dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    data = get_checklist(dialog_id)

    title = html.escape(data.get("title", "Р В§Р ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ Р В�Р вЂќ"))
    collab_title_raw = (data.get("collabTitle", "") or "").strip()
    collab_title = html.escape(collab_title_raw)
    full_title = f"{title} РІР‚вЂќ {collab_title}" if collab_title_raw else title
    progress_percent = int(data.get("progressPercent", 0) or 0)

    items_json = json.dumps(data.get("items", []), ensure_ascii=False)
    groups_json = json.dumps(data.get("groups", []), ensure_ascii=False)
    project_checklists_json = json.dumps(data.get("projectChecklists", []), ensure_ascii=False)
    dialog_id_json = json.dumps(dialog_id, ensure_ascii=False)
    collab_title_json = json.dumps(collab_title_raw, ensure_ascii=False)

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
            .thead-top,.thead-bottom,.row {{ display:grid; grid-template-columns:190px 100px 100px 120px 120px; gap:0; align-items:stretch; justify-content:start; }}
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
            .add-item-input {{ flex:1 1 auto; min-width:0; height:32px; border:1px solid #d0d7de; border-radius:8px; padding:7px 9px; font-size:12px; }}
            .add-item-btn {{ flex:0 0 auto; min-width:118px; height:32px; border:1px solid #d0d7de; border-radius:8px; padding:0 12px; background:#fff; cursor:pointer; font-size:12px; white-space:nowrap; }}
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
                        <div class="title" id="popupTitle">Р В§Р ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ Р В�Р вЂќ</div>
                        <div class="progress-box">
                            <div class="progress-label">Р СџРЎР‚Р С•Р С–РЎР‚Р ВµРЎРѓРЎРѓ</div>
                            <div class="progress-value" id="progressValue">{progress_percent}%</div>
                            <div class="progress-track"><div class="progress-bar" id="progressBar"></div></div>
                        </div>
                    </div>
                    <div class="header-right">
                        <div id="saveState" class="save-state">Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…Р ВµР Р…Р С•</div>
                    </div>
                </div>
                <div class="content">
                    <div class="layout">
                        <div class="tables-grid">
                            <div class="table-panel">
                                <div class="table">
                            <div class="thead">
                                <div class="thead-top">
                                    <div class="th">Р В�Р вЂќ</div>
                                    <div class="th">Р вЂќР С•Р С”РЎС“Р С�Р ВµР Р…РЎвЂљ</div>
                                    <div class="th">Р РЋРЎвЂљР В°РЎвЂљРЎС“РЎРѓ</div>
                                    <div class="th center" style="grid-column: 4 / span 2;">Р вЂќР В°РЎвЂљР В° Р С—Р С•Р В»РЎС“РЎвЂЎР ВµР Р…Р С‘РЎРЏ</div>
                                </div>
                                <div class="thead-bottom">
                                    <div class="th"></div>
                                    <div class="th"></div>
                                    <div class="th"></div>
                                    <div class="th">Р СџР В»Р В°Р Р…</div>
                                    <div class="th">Р В¤Р В°Р С”РЎвЂљ</div>
                                </div>
                            </div>
                            <div id="leftTableBody"></div>
                                </div>
                            </div>
                            <div class="table-panel">
                                <div class="table">
                                    <div class="thead">
                                        <div class="thead-top">
                                            <div class="th">Р В�Р вЂќ</div>
                                            <div class="th">Р вЂќР С•Р С”РЎС“Р С�Р ВµР Р…РЎвЂљ</div>
                                            <div class="th">Р РЋРЎвЂљР В°РЎвЂљРЎС“РЎРѓ</div>
                                            <div class="th center" style="grid-column: 4 / span 2;">Р вЂќР В°РЎвЂљР В° Р С—Р С•Р В»РЎС“РЎвЂЎР ВµР Р…Р С‘РЎРЏ</div>
                                        </div>
                                        <div class="thead-bottom">
                                            <div class="th"></div>
                                            <div class="th"></div>
                                            <div class="th"></div>
                                            <div class="th">Р СџР В»Р В°Р Р…</div>
                                            <div class="th">Р В¤Р В°Р С”РЎвЂљ</div>
                                        </div>
                                    </div>
                                    <div id="rightTableBody"></div>
                                </div>
                            </div>
                        </div>
                        <div class="side-panel">
                            <div class="side-panel-title">Р РЋР С—Р С‘РЎРѓР С•Р С” РЎвЂЎР ВµР С”-Р В»Р С‘РЎРѓРЎвЂљР С•Р Р† Р С—Р С• Р С—РЎР‚Р С•Р ВµР С”РЎвЂљРЎС“</div>
                            <div class="side-panel-list" id="projectChecklistList"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <script>
            const dialogId = {dialog_id_json};
            const groups = {groups_json};
            const projectChecklists = {project_checklists_json};
            let items = {items_json};
            let collabTitle = {collab_title_json};
            const saveStateEl = document.getElementById('saveState');
            const leftTableBodyEl = document.getElementById('leftTableBody');
            const rightTableBodyEl = document.getElementById('rightTableBody');
            const progressValueEl = document.getElementById('progressValue');
            const progressBarEl = document.getElementById('progressBar');
            const popupTitleEl = document.getElementById('popupTitle');
            const projectChecklistListEl = document.getElementById('projectChecklistList');

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
                if (s === 'Р вЂўРЎРѓРЎвЂљРЎРЉ') return 'Р вЂўРЎРѓРЎвЂљРЎРЉ';
                if (s === 'Р СњР ВµРЎвЂљ') return 'Р СњР ВµРЎвЂљ';
                if (s === 'Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ') return 'Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ';
                return '';
            }}
            function indicatorClass(status) {{
                const s = normalizeStatus(status);
                if (s === 'Р вЂўРЎРѓРЎвЂљРЎРЉ') return 'status-indicator green';
                if (s === 'Р СњР ВµРЎвЂљ' || s === 'Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ') return 'status-indicator gray';
                return 'status-indicator';
            }}
            function renderTitle() {{
                if (collabTitle) {{
                    popupTitleEl.innerHTML = 'Р В§Р ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ Р В�Р вЂќ <small>РІР‚вЂќ ' + esc(collabTitle) + '</small>';
                }} else {{
                    popupTitleEl.textContent = 'Р В§Р ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ Р В�Р вЂќ';
                }}
            }}
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
                                    try {{
                                        await fetch('/api/checklist/update-meta', {{
                                            method: 'POST',
                                            headers: {{ 'Content-Type': 'application/json' }},
                                            body: JSON.stringify({{ dialogId, field: 'collabTitle', value: title }})
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
                const activeItems = items.filter(x => normalizeStatus(x.status) !== 'Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ');
                const completedItems = activeItems.filter(x => normalizeStatus(x.status) === 'Р вЂўРЎРѓРЎвЂљРЎРЉ');
                const activeCount = activeItems.length;
                const completedCount = completedItems.length;
                const percent = activeCount ? Math.round((completedCount / activeCount) * 100) : 0;
                progressValueEl.textContent = percent + '%';
                progressBarEl.style.width = percent + '%';
            }}
            async function updateItem(itemId, field, value) {{
                setSaveState('saving', 'Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµР С�...');
                const response = await fetch('/api/checklist/update-item', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ dialogId, itemId, field, value }})
                }});
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'save failed');
                setSaveState('', 'Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…Р ВµР Р…Р С•');
                return result;
            }}
            async function addItem(groupId, name) {{
                setSaveState('saving', 'Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµР С�...');
                const response = await fetch('/api/checklist/add-item', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ dialogId, groupId, name }})
                }});
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'add item failed');
                setSaveState('', 'Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…Р ВµР Р…Р С•');
                return result;
            }}
            async function removeDocument(itemId) {{
                setSaveState('saving', 'Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµР С�...');
                const response = await fetch('/api/checklist/remove-document', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ dialogId, itemId }})
                }});
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'remove document failed');
                setSaveState('', 'Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…Р ВµР Р…Р С•');
                return result;
            }}
            async function uploadDocument(itemId, file) {{
                setSaveState('saving', 'Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµР С�...');
                const formData = new FormData();
                formData.append('dialogId', dialogId);
                formData.append('itemId', itemId);
                formData.append('file', file);
                const response = await fetch('/api/checklist/upload-document', {{ method: 'POST', body: formData }});
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'upload document failed');
                setSaveState('', 'Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…Р ВµР Р…Р С•');
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
                    const active = item.key === 'id' ? 'side-link active' : 'side-link';
                    return `<button type="button" class="${{active}}" data-checklist-key="${{esc(item.key)}}">${{esc(item.title)}}</button>`;
                }}).join('');
                projectChecklistListEl.querySelectorAll('[data-checklist-key]').forEach(btn => {{
                    btn.addEventListener('click', function () {{
                        if (this.dataset.checklistKey !== 'id') alert('Р В­РЎвЂљР С•РЎвЂљ РЎвЂЎР ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ Р С—Р С•Р Т‘Р С”Р В»РЎР‹РЎвЂЎР С‘Р С� РЎРѓР В»Р ВµР Т‘РЎС“РЎР‹РЎвЂ°Р С‘Р С� РЎРЊРЎвЂљР В°Р С—Р С•Р С�.');
                    }});
                }});
            }}
            function buildDocumentCell(item) {{
                const hasDocument = !!(item.documentUrl && item.documentUrl.trim());
                if (hasDocument) {{
                    return `
                        <button
                            class="doc-btn"
                            type="button"
                            data-role="view-document"
                            data-document-url="${{esc(item.documentUrl)}}"
                        >
                            Р СџР С•РЎРѓР С�Р С•РЎвЂљРЎР‚Р ВµРЎвЂљРЎРЉ
                        </button>
                    `;
                }}
                return `
                    <button class="upload-btn" type="button" data-role="upload" data-item-id="${{esc(item.id)}}">Р вЂ”Р В°Р С–РЎР‚РЎС“Р В·Р С‘РЎвЂљРЎРЉ</button>
                    <input type="file" data-role="file-input" data-item-id="${{esc(item.id)}}" style="display:none;">
                `;
            }}
            function renderGroup(group) {{
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = group.id !== 4;
                const rows = groupItems.map(item => {{
                    const rowClass = normalizeStatus(item.status) === 'Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ' ? 'row not-required' : 'row';
                    return `
                        <div class="${{rowClass}}" data-item-id="${{esc(item.id)}}">
                            <div class="td"><div class="cell-name"><div class="${{indicatorClass(item.status)}}"></div><div class="item-name">${{esc(item.name)}}</div></div></div>
                            <div class="td">${{buildDocumentCell(item)}}</div>
                            <div class="td">
                                <select class="status-select" data-role="status" data-item-id="${{esc(item.id)}}">
                                    <option value="" ${{normalizeStatus(item.status) === '' ? 'selected' : ''}}></option>
                                    <option value="Р вЂўРЎРѓРЎвЂљРЎРЉ" ${{normalizeStatus(item.status) === 'Р вЂўРЎРѓРЎвЂљРЎРЉ' ? 'selected' : ''}}>Р вЂўРЎРѓРЎвЂљРЎРЉ</option>
                                    <option value="Р СњР ВµРЎвЂљ" ${{normalizeStatus(item.status) === 'Р СњР ВµРЎвЂљ' ? 'selected' : ''}}>Р СњР ВµРЎвЂљ</option>
                                    <option value="Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ" ${{normalizeStatus(item.status) === 'Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ' ? 'selected' : ''}}>Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ</option>
                                </select>
                            </div>
                            <div class="td"><input class="date-input" type="date" data-role="plan" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.plan))}}"></div>
                            <div class="td"><input class="date-input" type="date" data-role="fact" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.fact))}}"></div>
                        </div>
                    `;
                }}).join('');
                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="addItemInput_${{group.id}}" type="text" placeholder="Р СњР С•Р Р†РЎвЂ№Р в„– Р С—РЎС“Р Р…Р С”РЎвЂљ">
                        <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${{group.id}}">Р вЂќР С•Р В±Р В°Р Р†Р С‘РЎвЂљРЎРЉ Р С—РЎС“Р Р…Р С”РЎвЂљ</button>
                    </div>` : '';
                return `<div class="group-block"><div class="group-title">${{esc(group.title)}}</div>${{rows}}${{addBlock}}</div>`;
            }}
            function renderTables() {{
                const leftGroups = groups.filter(g => g.id === 1 || g.id === 3);
                const rightGroups = groups.filter(g => g.id === 2);

                if (hasItemsInGroup(4)) {{
                    const notRequiredGroup = groups.find(g => g.id === 4);
                    if (notRequiredGroup) {{
                        rightGroups.push(notRequiredGroup);
                    }}
                }}

                leftTableBodyEl.innerHTML = leftGroups.map(renderGroup).join('');
                rightTableBodyEl.innerHTML = rightGroups.map(renderGroup).join('');
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
                document.querySelectorAll('[data-role="status"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldItem = JSON.parse(JSON.stringify(item));
                        item.status = this.value;
                        renderAll();
                        try {{
                            const result = await updateItem(this.dataset.itemId, 'status', this.value);
                            replaceItem(result.item);
                            renderAll();
                        }} catch (e) {{
                            replaceItem(oldItem);
                            renderAll();
                            setSaveState('error', 'Р С›РЎв‚¬Р С‘Р В±Р С”Р В° РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…Р ВµР Р…Р С‘РЎРЏ');
                        }}
                    }});
                }});
                document.querySelectorAll('[data-role="plan"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldItem = JSON.parse(JSON.stringify(item));
                        item.plan = fromInputDate(this.value);
                        renderAll();
                        try {{
                            const result = await updateItem(this.dataset.itemId, 'plan', item.plan);
                            replaceItem(result.item);
                            renderAll();
                        }} catch (e) {{
                            replaceItem(oldItem);
                            renderAll();
                            setSaveState('error', 'Р С›РЎв‚¬Р С‘Р В±Р С”Р В° РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…Р ВµР Р…Р С‘РЎРЏ');
                        }}
                    }});
                }});
                document.querySelectorAll('[data-role="fact"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldItem = JSON.parse(JSON.stringify(item));
                        item.fact = fromInputDate(this.value);
                        renderAll();
                        try {{
                            const result = await updateItem(this.dataset.itemId, 'fact', item.fact);
                            replaceItem(result.item);
                            renderAll();
                        }} catch (e) {{
                            replaceItem(oldItem);
                            renderAll();
                            setSaveState('error', 'Р С›РЎв‚¬Р С‘Р В±Р С”Р В° РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…Р ВµР Р…Р С‘РЎРЏ');
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
                            input.value = '';
                            renderAll();
                        }} catch (e) {{
                            setSaveState('error', 'Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р С‘РЎРЏ Р С—РЎС“Р Р…Р С”РЎвЂљР В°');
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
                        try {{
                            const result = await uploadDocument(this.dataset.itemId, file);
                            replaceItem(result.item);
                            renderAll();
                        }} catch (e) {{
                            setSaveState('error', 'Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р В·Р В°Р С–РЎР‚РЎС“Р В·Р С”Р С‘ РЎвЂћР В°Р в„–Р В»Р В°');
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
            fetchChatTitleIfMissing();
            safeInitBx24ForPopup();
        </script>
    </body>
    </html>
    """


@app.get("/api/checklist")
def api_checklist(dialogId: str = ""):
    dialogId = normalize_dialog_id(dialogId)
    return JSONResponse(get_checklist(dialogId))


@app.post("/api/checklist/update-item")
async def api_checklist_update_item(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    item_id = str(payload.get("itemId") or "").strip()
    field = str(payload.get("field") or "").strip()
    value = payload.get("value")

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    allowed_fields = {"priority", "status", "plan", "fact"}
    if field not in allowed_fields:
        return JSONResponse({"ok": False, "error": "invalid field"}, status_code=400)

    data = get_checklist(dialog_id)
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
        new_status = normalize_status(value)
        target_item["status"] = new_status
        target_item["priority"] = derive_indicator_from_status(new_status)

        if new_status == "Р СњР ВµРЎвЂљ":
            remove_item_document_file(target_item)
            target_item["documentUrl"] = ""
            target_item["documentName"] = ""
    elif field == "plan":
        target_item["plan"] = normalize_date_string(value)
    elif field == "fact":
        target_item["fact"] = normalize_date_string(value)

    data["items"] = items
    data = normalize_checklist_data(data)
    save_checklist(dialog_id, data)
    updated_item = None
    for item in data.get("items", []):
        if str(item.get("id")) == item_id:
            updated_item = item
            break

    return JSONResponse({
        "ok": True,
        "item": updated_item,
        "dialogId": dialog_id,
        "progressPercent": data.get("progressPercent", 0),
    })


@app.post("/api/checklist/update-meta")
async def api_checklist_update_meta(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    field = str(payload.get("field") or "").strip()
    value = payload.get("value")

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if field != "collabTitle":
        return JSONResponse({"ok": False, "error": "only collabTitle is supported now"}, status_code=400)

    data = get_checklist(dialog_id)
    data["collabTitle"] = clean_cell_value(value)

    data = normalize_checklist_data(data)
    save_checklist(dialog_id, data)

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
    group_id = int(payload.get("groupId") or 0)
    name = clean_cell_value(payload.get("name"))

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if group_id not in [1, 2, 3]:
        return JSONResponse({"ok": False, "error": "invalid groupId"}, status_code=400)

    if not name:
        return JSONResponse({"ok": False, "error": "name is required"}, status_code=400)

    data = get_checklist(dialog_id)
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
    data = normalize_checklist_data(data)
    save_checklist(dialog_id, data)
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
    file: UploadFile = File(...)
):
    dialog_id = normalize_dialog_id(dialogId)
    item_id = str(itemId or "").strip()

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    data = get_checklist(dialog_id)
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

    if normalize_status(target_item.get("status")) != "Р СњР Вµ РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ":
        target_item["status"] = "Р вЂўРЎРѓРЎвЂљРЎРЉ"
        target_item["priority"] = derive_indicator_from_status("Р вЂўРЎРѓРЎвЂљРЎРЉ")

    data["items"] = items
    data = normalize_checklist_data(data)
    save_checklist(dialog_id, data)

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
    item_id = str(payload.get("itemId") or "").strip()

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    data = get_checklist(dialog_id)
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
    data = normalize_checklist_data(data)
    save_checklist(dialog_id, data)

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

@app.get("/admin", response_class=HTMLResponse)
def admin():
    conn = get_conn()
    rows = conn.execute("SELECT dialog_id, title FROM checklists ORDER BY dialog_id").fetchall()
    conn.close()

    items = "".join(
        f"<li><b>{html.escape(row['dialog_id'])}</b> РІР‚вЂќ {html.escape(row['title'])}</li>"
        for row in rows
    ) or "<li>Р СџР С•Р С”Р В° Р Р…Р С‘РЎвЂЎР ВµР С–Р С• Р Р…Р Вµ Р В·Р В°Р С–РЎР‚РЎС“Р В¶Р ВµР Р…Р С•</li>"

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>Р вЂ”Р В°Р С–РЎР‚РЎС“Р В·Р С”Р В° РЎвЂЎР ВµР С”-Р В»Р С‘РЎРѓРЎвЂљР В°</title>
    </head>
    <body style="font-family:Arial,sans-serif;padding:40px;max-width:900px">
        <h1>Р вЂ”Р В°Р С–РЎР‚РЎС“Р В·Р С”Р В° Excel Р Т‘Р В»РЎРЏ Р С”Р С•Р В»Р В»Р В°Р В±РЎвЂ№</h1>
        <p>Р РЃР В°Р С– 1: Р С•РЎвЂљР С”РЎР‚Р С•Р в„–РЎвЂљР Вµ Р С”Р С•Р В»Р В»Р В°Р В±РЎС“ Р С‘ Р С—Р С•РЎРѓР С�Р С•РЎвЂљРЎР‚Р С‘РЎвЂљР Вµ Р В·Р Р…Р В°РЎвЂЎР ВµР Р…Р С‘Р Вµ <b>dialogId</b> Р Р† sidebar.</p>
        <p>Р РЃР В°Р С– 2: Р Р†РЎРѓРЎвЂљР В°Р Р†РЎРЉРЎвЂљР Вµ РЎРЊРЎвЂљР С•РЎвЂљ dialogId РЎРѓРЎР‹Р Т‘Р В° Р С‘ Р В·Р В°Р С–РЎР‚РЎС“Р В·Р С‘РЎвЂљР Вµ .xlsx РЎвЂћР В°Р в„–Р В».</p>

        <form action="/admin/upload" method="post" enctype="multipart/form-data" style="margin:30px 0">
            <div style="margin-bottom:16px">
                <label>dialogId</label><br>
                <input type="text" name="dialog_id" required style="width:100%;padding:10px">
            </div>

            <div style="margin-bottom:16px">
                <label>XLSX РЎвЂћР В°Р в„–Р В»</label><br>
                <input type="file" name="file" accept=".xlsx" required>
            </div>

            <button type="submit" style="padding:10px 16px">Р вЂ”Р В°Р С–РЎР‚РЎС“Р В·Р С‘РЎвЂљРЎРЉ РЎвЂЎР ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ</button>
        </form>

        <h2>Р Р€Р В¶Р Вµ Р В·Р В°Р С–РЎР‚РЎС“Р В¶Р ВµР Р…Р Р…РЎвЂ№Р Вµ РЎвЂЎР ВµР С”-Р В»Р С‘РЎРѓРЎвЂљРЎвЂ№</h2>
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
        <title>Р вЂњР С•РЎвЂљР С•Р Р†Р С•</title>
    </head>
    <body style="font-family:Arial,sans-serif;padding:40px">
        <h1>Р В§Р ВµР С”-Р В»Р С‘РЎРѓРЎвЂљ РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎвЂ�Р Р…</h1>
        <p><b>dialogId:</b> {html.escape(dialog_id)}</p>
        <p><b>Р В¤Р В°Р в„–Р В»:</b> {html.escape(file.filename or '')}</p>
        <p>Р СћР ВµР С—Р ВµРЎР‚РЎРЉ Р Р†Р ВµРЎР‚Р Р…Р С‘РЎвЂљР ВµРЎРѓРЎРЉ Р Р† Р С”Р С•Р В»Р В»Р В°Р В±РЎС“ Р С‘ Р С•Р В±Р Р…Р С•Р Р†Р С‘РЎвЂљР Вµ sidebar.</p>
        <p><a href="/admin">Р СњР В°Р В·Р В°Р Т‘ Р Р† /admin</a></p>
    </body>
    </html>
    """

