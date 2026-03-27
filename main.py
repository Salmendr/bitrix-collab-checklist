from fastapi import FastAPI, Request, UploadFile, File, Form
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

STATUS_OPTIONS = [
    "",
    "Есть",
    "Нет",
    "Не требуется",
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
                "documentUrl": "#",
                "isCustom": False,
            })

    return normalize_checklist_data({
        "title": "Чек-лист ИД",
        "collabTitle": dialog_id or "",
        "contractDeadline": "",
        "startDate": "",
        "groups": build_default_groups(),
        "items": items,
        "notice": "",
    })


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
            "documentUrl": clean_cell_value(item.get("documentUrl")) or "#",
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
        "collabTitle": clean_cell_value(data.get("collabTitle")),
        "contractDeadline": clean_cell_value(data.get("contractDeadline")),
        "startDate": clean_cell_value(data.get("startDate")),
        "groups": build_default_groups(),
        "items": normalized_items,
        "notice": data.get("notice", ""),
        "activeCount": progress["activeCount"],
        "completedCount": progress["completedCount"],
        "progressPercent": progress["progressPercent"],
    }


def build_demo_checklist():
    return build_default_checklist_template()


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


def parse_xlsx_to_checklist_legacy(file_bytes: bytes):
    wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
    ws = wb[wb.sheetnames[0]]

    def format_local(value):
        if value is None:
            return ""
        if isinstance(value, datetime):
            return value.strftime("%d.%m.%Y")
        return str(value).strip()

    items = []

    # В файле:
    # строка 1 = заголовки "ИД / Статус / Дата получения"
    # строка 2 = подписи "План / Факт"
    # данные начинаются со строки 3
    for row in range(3, ws.max_row + 1):
        name = format_local(ws[f"A{row}"].value)
        status = format_local(ws[f"B{row}"].value)
        plan = format_local(ws[f"C{row}"].value)
        fact = format_local(ws[f"D{row}"].value)

        # пропускаем полностью пустые строки
        if not name:
            continue

        items.append({
            "name": name,
            "status": status or "—",
            "plan": plan or "—",
            "fact": fact or "—",
        })

    return {
        "title": f"Чек-лист ИД — {ws.title}",
        "contractDeadline": "—",
        "startDate": "—",
        "items": items
    }

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
            "documentUrl": "#",
            "isCustom": False,
        })

    data = {
        "title": f"Чек-лист ИД — {ws.title}",
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

def get_checklist_legacy(dialog_id: str):
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
    for candidate in aliases:
        row = conn.execute(
            "SELECT data_json FROM checklists WHERE dialog_id = ?",
            (candidate,)
        ).fetchone()
        if row:
            break

    conn.close()

    if row:
        data = json.loads(row["data_json"])
        data["resolvedDialogId"] = normalized_id
        data["lookupAliases"] = aliases
        return data

    return {
        "title": "Демо-чек-лист",
        "contractDeadline": "—",
        "startDate": "—",
        "items": [
            {"name": "ППТ", "status": "Есть", "plan": "—", "fact": "—"},
            {"name": "ГПЗУ", "status": "Нет", "plan": "апрель", "fact": "—"},
            {"name": "ТУ Свет", "status": "В работе", "plan": "27.03.2026", "fact": "—"},
        ],
        "notice": "Для этого dialogId пока не найден реальный Excel.",
        "resolvedDialogId": normalized_id,
        "lookupAliases": aliases
    }


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
        <title>Чек-лист ИД</title>
        <script src="https://api.bitrix24.com/api/v1/"></script>
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
                    }
                } catch (e) {
                    console.log('pending dialog redirect skipped:', e);
                }
            })();
        </script>
    </head>
    <body style="font-family:Arial,sans-serif;padding:40px">
        <h1>Чек-лист ИД</h1>

        <button id="bindBtn" style="padding:10px 16px;font-size:16px;">
            Зарегистрировать виджет в IM_SIDEBAR
        </button>
        <button id="bindTextareaBtn" style="padding:10px 16px;font-size:16px;margin-left:8px;">
            Зарегистрировать виджет в IM_TEXTAREA
        </button>

        <p style="margin-top:20px;"><a href="/sidebar" target="_blank">Открыть /sidebar</a></p>
        <p><a href="/textarea" target="_blank">Открыть /textarea</a></p>
        <p><a href="/admin" target="_blank">Открыть /admin</a></p>

        <pre id="log" style="margin-top:20px;white-space:pre-wrap;"></pre>

        <script>
            function log(text) {
                document.getElementById('log').textContent = text;
            }

            if (typeof BX24 === 'undefined') {
                log('BX24 SDK не найден. Эту страницу нужно открывать из Bitrix24 как приложение.');
            } else {
                BX24.init(function () {
                    BX24.callMethod('app.info', {}, function(infoResult) {
                        if (infoResult.error()) {
                            log('app.info error: ' + infoResult.error());
                            return;
                        }

                        log('app.info ok:\\n' + JSON.stringify(infoResult.data(), null, 2));

                        document.getElementById('bindBtn').addEventListener('click', function () {
                            BX24.callMethod('placement.bind', {
                                PLACEMENT: 'IM_SIDEBAR',
                                HANDLER: window.location.origin + '/sidebar',
                                TITLE: 'Чек-лист ИД',
                                OPTIONS: {
                                    iconName: 'fa-cloud',
                                    context: 'CHAT',
                                    role: 'USER',
                                    color: 'GREEN'
                                }
                            }, function(result) {
                                if (result.error()) {
                                    log('placement.bind error: ' + result.error());
                                } else {
                                    log('placement.bind ok:\\n' + JSON.stringify(result.data(), null, 2));
                                }
                            });
                        });

                        document.getElementById('bindTextareaBtn').addEventListener('click', function () {
                            BX24.callMethod('placement.unbind', {
                                PLACEMENT: 'IM_TEXTAREA',
                                HANDLER: window.location.origin + '/textarea'
                            }, function(unbindResult) {
                                BX24.callMethod('placement.bind', {
                                PLACEMENT: 'IM_TEXTAREA',
                                HANDLER: window.location.origin + '/textarea',
                                TITLE: 'Чек-лист ИД',
                                OPTIONS: {
                                    iconName: 'fa-bars',
                                    context: 'CHAT'
                                }
                            }, function(bindResult) {
                                if (bindResult.error()) {
                                    log('placement.bind textarea error: ' + bindResult.error());
                                } else {
                                    log('placement.bind textarea ok:\\n' + JSON.stringify(bindResult.data(), null, 2));
                                }
                            });
                            });
                        });
                    });
                });
            }
        </script>
    </body>
    </html>
    """


def sidebar_html(initial_dialog_id: str = "", initial_context_text: str = ""):
    initial_dialog_id_json = json.dumps(initial_dialog_id or "", ensure_ascii=False)
    initial_context_text_json = json.dumps(initial_context_text or "", ensure_ascii=False)

    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
        <meta charset="utf-8">
        <title>Bitrix24 Sidebar Test</title>
        <script src="https://api.bitrix24.com/api/v1/"></script>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 16px;
                background: #f7f9fc;
            }}
            .card {{
                background: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 12px;
                padding: 16px;
                margin-bottom: 12px;
            }}
            .title {{
                font-size: 20px;
                font-weight: 700;
                margin-bottom: 10px;
            }}
            .row {{
                margin-bottom: 10px;
            }}
            .label {{
                color: #666;
                font-size: 13px;
                margin-bottom: 3px;
            }}
            .value {{
                font-weight: 600;
                color: #222;
                word-break: break-word;
            }}
            .note {{
                font-size: 13px;
                color: #666;
                margin-top: 10px;
            }}
            .item {{
                border-top: 1px solid #f0f0f0;
                padding: 10px 0;
            }}
            .item:first-child {{
                border-top: none;
            }}
            .item-name {{
                font-weight: 700;
                margin-bottom: 4px;
            }}
            .muted {{
                color: #666;
                font-size: 12px;
                margin-bottom: 2px;
            }}
            .badge {{
                display: inline-block;
                border-radius: 999px;
                padding: 4px 8px;
                font-size: 12px;
                background: #eef2ff;
            }}
            .error {{
                background: #fff1f1;
                border: 1px solid #f3b3b3;
            }}
            pre {{
                white-space: pre-wrap;
                word-break: break-word;
                font-size: 12px;
            }}
        </style>
    </head>
    <body>
        <div id="app">
            <div class="card">
                <div class="title">Инициализация...</div>
                <div class="note">Ждём загрузку Bitrix24 SDK и данных чек-листа.</div>
            </div>
        </div>

        <script>
            var started = false;
            var initialDialogId = {initial_dialog_id_json};
            var initialContextText = {initial_context_text_json};

            function esc(v) {{
                if (v === null || v === undefined) return '';
                return String(v)
                    .replace(/&/g, '&amp;')
                    .replace(/</g, '&lt;')
                    .replace(/>/g, '&gt;')
                    .replace(/"/g, '&quot;');
            }}

            function showError(title, text) {{
                document.getElementById('app').innerHTML =
                    '<div class="card error">' +
                        '<div class="title">' + esc(title) + '</div>' +
                        '<div class="note">' + esc(text) + '</div>' +
                    '</div>';
            }}

            window.onerror = function(message, source, lineno, colno) {{
                showError('JavaScript error', message + ' | line: ' + lineno + ', col: ' + colno);
            }};

            function renderChecklist(dialogId, data, mode, debugInfo) {{
                var itemsHtml = '';

                if (data.items && data.items.length) {{
                    for (var i = 0; i < data.items.length; i++) {{
                        var item = data.items[i];
                        itemsHtml +=
                            '<div class="item">' +
                                '<div class="item-name">' + esc(item.name) + '</div>' +
                                '<div class="muted"><span class="badge">' + esc(item.status || '—') + '</span></div>' +
                                '<div class="muted">План: ' + esc(item.plan || '—') + '</div>' +
                                '<div class="muted">Факт: ' + esc(item.fact || '—') + '</div>' +
                            '</div>';
                    }}
                }} else {{
                    itemsHtml = '<div class="muted">Нет пунктов чек-листа</div>';
                }}

                var noticeHtml = '';
                if (data.notice) {{
                    noticeHtml =
                        '<div class="card">' +
                            '<div class="note">' + esc(data.notice) + '</div>' +
                        '</div>';
                }}

                var debugHtml =
                    '<div class="card">' +
                        '<div class="title">Диагностика</div>' +
                        '<div class="row"><div class="label">mode</div><div class="value">' + esc(mode) + '</div></div>' +
                        '<div class="row"><div class="label">dialogId из Bitrix</div><div class="value">' + esc(dialogId || 'не передан') + '</div></div>' +
                        '<div class="row"><div class="label">resolvedDialogId</div><div class="value">' + esc(data.resolvedDialogId || '') + '</div></div>' +
                        '<div class="row"><div class="label">lookupAliases</div><div class="value">' + esc((data.lookupAliases || []).join(', ')) + '</div></div>' +
                        '<div class="row"><div class="label">raw placement info</div><pre>' + esc(debugInfo || '') + '</pre></div>' +
                    '</div>';

                document.getElementById('app').innerHTML =
                    '<div class="card">' +
                        '<div class="title">' + esc(data.title || 'Чек-лист') + '</div>' +
                        '<div class="row"><div class="label">Срок по договору</div><div class="value">' + esc(data.contractDeadline || '—') + '</div></div>' +
                        '<div class="row"><div class="label">Начало работ</div><div class="value">' + esc(data.startDate || '—') + '</div></div>' +
                    '</div>' +
                    noticeHtml +
                    '<div class="card">' + itemsHtml + '</div>' +
                    debugHtml;
            }}

            function loadChecklist(dialogId, mode, placementInfoText) {{
                started = true;

                fetch('/api/checklist?dialogId=' + encodeURIComponent(dialogId || ''))
                    .then(function(response) {{
                        return response.json();
                    }})
                    .then(function(data) {{
                        renderChecklist(dialogId, data, mode, placementInfoText);
                        try {{
                            if (typeof BX24 !== 'undefined') {{
                                BX24.fitWindow();
                            }}
                        }} catch (e) {{
                            console.log(e);
                        }}
                    }})
                    .catch(function(error) {{
                        showError('Ошибка загрузки чек-листа', String(error));
                    }});
            }}

            // 1. Если Bitrix уже передал dialogId сервером — используем его сразу
            if (initialDialogId) {{
                loadChecklist(initialDialogId, 'server-post', initialContextText);
            }} else if (typeof BX24 !== 'undefined') {{
                // 2. Иначе пробуем получить через BX24 JS SDK
                try {{
                    BX24.init(function () {{
                        var dialogId = '';
                        var rawInfo = '';

                        try {{
                            var info = BX24.placement.info();
                            rawInfo = JSON.stringify(info || {{}}, null, 2);

                            if (info && info.options && info.options.dialogId) {{
                                dialogId = info.options.dialogId;
                            }}
                        }} catch (e) {{
                            rawInfo = 'placement.info error: ' + String(e);
                        }}

                        loadChecklist(dialogId, 'Bitrix24-js', rawInfo);
                    }});
                }} catch (e) {{
                    showError('BX24.init error', String(e));
                }}
            }} else {{
                // 3. Обычное открытие страницы вне Bitrix
                loadChecklist('', 'local', 'BX24 не найден');
            }}

            // 4. Аварийный fallback
            setTimeout(function() {{
                if (!started) {{
                    loadChecklist('', 'fallback', 'Ни server-post, ни BX24.init не сработали за 5 секунд');
                }}
            }}, 5000);
        </script>
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

    if mode == "popup":
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
        "im_sidebar": {"skipped": True},
        "im_textarea": {"skipped": True}
    }

    if domain and access_token:
        bind_result["im_sidebar"] = bitrix_rest_call(
            domain,
            "placement.bind",
            access_token,
            {
                "PLACEMENT": "IM_SIDEBAR",
                "HANDLER": f"{base_url}/sidebar",
                "OPTIONS": {
                    "iconName": "fa-cloud",
                    "context": "CHAT",
                    "role": "USER",
                    "color": "GREEN"
                },
                "TITLE": "Чек-лист ИД"
            }
        )
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
        <p>Если bind прошёл успешно, виджеты будут зарегистрированы в IM_SIDEBAR и IM_TEXTAREA.</p>

        <h2>Что прислал Bitrix24</h2>
        <pre>{html.escape(json.dumps(form, ensure_ascii=False, indent=2))}</pre>

        <h2>Ответ placement.bind</h2>
        <pre>{html.escape(json.dumps(bind_result, ensure_ascii=False, indent=2))}</pre>

        {install_finish_block()}
    </body>
    </html>
    """


@app.get("/sidebar", response_class=HTMLResponse)
def sidebar_get():
    return sidebar_html("", "GET /sidebar without Bitrix POST context")


@app.post("/sidebar", response_class=HTMLResponse)
async def sidebar_post(request: Request):
    form = dict(await request.form())
    dialog_id = extract_dialog_id_from_form(form)
    raw_context = json.dumps(form, ensure_ascii=False, indent=2)

    print("SIDEBAR POST FORM:", raw_context)
    print("SIDEBAR EXTRACTED DIALOG ID:", dialog_id)

    return sidebar_html(dialog_id, raw_context)


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


def popup_get_legacy(dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    data = get_checklist(dialog_id)

    items_html = ""
    for item in data.get("items", []):
        status = html.escape(item.get("status", "—"))
        plan = html.escape(item.get("plan", "—"))
        fact = html.escape(item.get("fact", "—"))
        name = html.escape(item.get("name", "—"))

        items_html += f"""
        <div class="item">
            <div class="item-name">{name}</div>
            <div class="item-meta"><b>Статус:</b> {status}</div>
            <div class="item-meta"><b>План:</b> {plan}</div>
            <div class="item-meta"><b>Факт:</b> {fact}</div>
        </div>
        """

    if not items_html:
        items_html = '<div class="empty">Нет пунктов чек-листа</div>'

    title = html.escape(data.get("title", "Чек-лист ИД"))
    contract_deadline = html.escape(data.get("contractDeadline", "—"))
    start_date = html.escape(data.get("startDate", "—"))

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{title}</title>
        <script src="https://api.bitrix24.com/api/v1/"></script>
        <style>
            * {{ box-sizing: border-box; }}
            body {{
                margin: 0;
                font-family: Arial, sans-serif;
                background: #f5f7fb;
            }}
            .shell {{
                padding: 18px;
            }}
            .modal {{
                background: #fff;
                border-radius: 16px;
                border: 1px solid #e5e7eb;
                overflow: hidden;
                box-shadow: 0 16px 40px rgba(0,0,0,0.12);
            }}
            .header {{
                padding: 20px 24px;
                border-bottom: 1px solid #edf0f2;
            }}
            .title {{
                font-size: 28px;
                font-weight: 700;
                margin-bottom: 8px;
            }}
            .meta {{
                color: #667085;
                font-size: 14px;
                display: flex;
                gap: 18px;
                flex-wrap: wrap;
            }}
            .content {{
                max-height: 560px;
                overflow: auto;
                padding: 12px 24px 24px;
            }}
            .item {{
                border-top: 1px solid #edf0f2;
                padding: 14px 0;
            }}
            .item:first-child {{
                border-top: none;
            }}
            .item-name {{
                font-size: 18px;
                font-weight: 700;
                margin-bottom: 6px;
            }}
            .item-meta {{
                color: #444;
                font-size: 14px;
                margin-bottom: 3px;
            }}
            .empty {{
                padding: 24px 0;
                color: #667085;
            }}
        </style>
    </head>
    <body>
        <div class="shell">
            <div class="modal">
                <div class="header">
                    <div class="title">{title}</div>
                    <div class="meta">
                        <div><b>dialogId:</b> {html.escape(dialog_id or "не передан")}</div>
                        <div><b>Срок по договору:</b> {contract_deadline}</div>
                        <div><b>Начало работ:</b> {start_date}</div>
                    </div>
                </div>
                <div class="content">
                    {items_html}
                </div>
            </div>
        </div>

        <script>
            if (typeof BX24 !== 'undefined') {{
                BX24.init(function () {{
                    try {{
                        BX24.resizeWindow(760, 460);
                    }} catch (e) {{
                        console.log(e);
                    }}
                }});
            }}
        </script>
    </body>
    </html>
    """


def popup_get_stage1_legacy(dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    data = get_checklist(dialog_id)

    title = html.escape(data.get("title", "Чек-лист ИД"))
    contract_deadline = html.escape(data.get("contractDeadline", "—") or "—")
    start_date = html.escape(data.get("startDate", "—") or "—")

    items_json = json.dumps(data.get("items", []), ensure_ascii=False)
    groups_json = json.dumps(data.get("groups", []), ensure_ascii=False)
    dialog_id_json = json.dumps(dialog_id, ensure_ascii=False)

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{title}</title>
        <script src="https://api.bitrix24.com/api/v1/"></script>
        <style>
            * {{
                box-sizing: border-box;
            }}

            body {{
                margin: 0;
                font-family: Arial, sans-serif;
                background: #f3f6fb;
                color: #1f2328;
            }}

            .shell {{
                padding: 18px;
            }}

            .modal {{
                background: #fff;
                border-radius: 16px;
                border: 1px solid #e5e7eb;
                overflow: hidden;
                box-shadow: 0 16px 40px rgba(0,0,0,0.12);
            }}

            .header {{
                padding: 20px 24px 16px;
                border-bottom: 1px solid #edf0f2;
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                gap: 20px;
            }}

            .title {{
                font-size: 28px;
                font-weight: 700;
                margin-bottom: 8px;
            }}

            .meta {{
                color: #667085;
                font-size: 14px;
                display: flex;
                gap: 18px;
                flex-wrap: wrap;
            }}

            .save-state {{
                font-size: 13px;
                font-weight: 700;
                padding: 8px 12px;
                border-radius: 999px;
                background: #eef2ff;
                color: #3730a3;
                white-space: nowrap;
            }}

            .save-state.saving {{
                background: #fff4e5;
                color: #b26a00;
            }}

            .save-state.error {{
                background: #fdecec;
                color: #b42318;
            }}

            .content {{
                padding: 18px 24px 24px;
                max-height: 75vh;
                overflow: auto;
            }}

            .table {{
                width: 100%;
                border: 1px solid #e5e7eb;
                border-radius: 14px;
                overflow: hidden;
            }}

            .thead {{
                background: #f8fafc;
                border-bottom: 1px solid #e5e7eb;
            }}

            .thead-top,
            .thead-bottom,
            .row {{
                display: grid;
                grid-template-columns: 1.8fr 1fr 150px 150px;
                gap: 0;
                align-items: stretch;
            }}

            .th,
            .td {{
                padding: 12px 14px;
                border-right: 1px solid #edf0f2;
            }}

            .th:last-child,
            .td:last-child {{
                border-right: none;
            }}

            .th {{
                font-size: 13px;
                font-weight: 700;
                color: #475467;
            }}

            .th.center {{
                text-align: center;
            }}

            .group-block {{
                border-top: 10px solid #f8fafc;
            }}

            .group-title {{
                padding: 14px 16px;
                background: #fafbfc;
                border-top: 1px solid #e5e7eb;
                border-bottom: 1px solid #e5e7eb;
                font-size: 14px;
                font-weight: 700;
                color: #344054;
            }}

            .row {{
                border-top: 1px solid #edf0f2;
                background: #fff;
            }}

            .cell-name {{
                display: flex;
                align-items: center;
                gap: 10px;
                min-width: 0;
            }}

            .drag {{
                color: #98a2b3;
                font-size: 18px;
                line-height: 1;
                cursor: default;
                flex: 0 0 auto;
            }}

            .priority {{
                width: 18px;
                height: 34px;
                border-radius: 6px;
                cursor: pointer;
                flex: 0 0 18px;
                border: 1px solid rgba(0,0,0,0.06);
            }}

            .priority.red {{
                background: #e74c3c;
            }}

            .priority.orange {{
                background: #f39c12;
            }}

            .priority.yellow {{
                background: #f1c40f;
            }}

            .item-name {{
                font-size: 14px;
                font-weight: 700;
                color: #1f2328;
                line-height: 1.35;
                min-width: 0;
            }}

            .status-select,
            .date-input {{
                width: 100%;
                border: 1px solid #d0d7de;
                border-radius: 8px;
                padding: 8px 10px;
                font-size: 14px;
                background: #fff;
            }}

            .date-display {{
                font-size: 12px;
                color: #667085;
                margin-top: 6px;
                min-height: 16px;
            }}

            .toolbar {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 12px;
                margin-bottom: 14px;
            }}

            .legend {{
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
                font-size: 12px;
                color: #667085;
            }}

            .legend-item {{
                display: flex;
                align-items: center;
                gap: 6px;
            }}

            .legend-color {{
                width: 12px;
                height: 12px;
                border-radius: 4px;
            }}

            .legend-color.red {{
                background: #e74c3c;
            }}

            .legend-color.orange {{
                background: #f39c12;
            }}

            .legend-color.yellow {{
                background: #f1c40f;
            }}

            .helper {{
                font-size: 12px;
                color: #667085;
            }}

            @media (max-width: 980px) {{
                .thead-top,
                .thead-bottom,
                .row {{
                    grid-template-columns: 1fr;
                }}

                .th,
                .td {{
                    border-right: none;
                    border-bottom: 1px solid #edf0f2;
                }}

                .th:last-child,
                .td:last-child {{
                    border-bottom: none;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="shell">
            <div class="modal">
                <div class="header">
                    <div>
                        <div class="title">{title}</div>
                        <div class="meta">
                            <div><b>dialogId:</b> {html.escape(dialog_id or "не передан")}</div>
                            <div><b>Срок по договору:</b> {contract_deadline}</div>
                            <div><b>Начало работ:</b> {start_date}</div>
                        </div>
                    </div>

                    <div id="saveState" class="save-state">Сохранено</div>
                </div>

                <div class="content">
                    <div class="toolbar">
                        <div class="legend">
                            <div class="legend-item"><span class="legend-color red"></span> Высокий</div>
                            <div class="legend-item"><span class="legend-color orange"></span> Средний</div>
                            <div class="legend-item"><span class="legend-color yellow"></span> Базовый</div>
                        </div>

                        <div class="helper">
                            Изменения сохраняются автоматически
                        </div>
                    </div>

                    <div class="table">
                        <div class="thead">
                            <div class="thead-top">
                                <div class="th">ИД</div>
                                <div class="th">Статус</div>
                                <div class="th center" style="grid-column: 3 / span 2;">Дата получения</div>
                            </div>
                            <div class="thead-bottom">
                                <div class="th"></div>
                                <div class="th"></div>
                                <div class="th">План</div>
                                <div class="th">Факт</div>
                            </div>
                        </div>

                        <div id="tableBody"></div>
                    </div>
                </div>
            </div>
        </div>

        <script>
            const dialogId = {dialog_id_json};
            const groups = {groups_json};
            let items = {items_json};

            const saveStateEl = document.getElementById('saveState');
            const tableBodyEl = document.getElementById('tableBody');

            function setSaveState(mode, text) {{
                saveStateEl.classList.remove('saving', 'error');

                if (mode === 'saving') {{
                    saveStateEl.classList.add('saving');
                }}
                if (mode === 'error') {{
                    saveStateEl.classList.add('error');
                }}

                saveStateEl.textContent = text;
            }}

            function esc(v) {{
                if (v === null || v === undefined) return '';
                return String(v)
                    .replaceAll('&', '&amp;')
                    .replaceAll('<', '&lt;')
                    .replaceAll('>', '&gt;')
                    .replaceAll('"', '&quot;');
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

            function nextPriority(current) {{
                if (current === 'red') return 'orange';
                if (current === 'orange') return 'yellow';
                return 'red';
            }}

            async function updateItem(itemId, field, value) {{
                setSaveState('saving', 'Сохраняем...');

                const response = await fetch('/api/checklist/update-item', {{
                    method: 'POST',
                    headers: {{
                        'Content-Type': 'application/json'
                    }},
                    body: JSON.stringify({{
                        dialogId,
                        itemId,
                        field,
                        value
                    }})
                }});

                const result = await response.json();

                if (!response.ok || !result.ok) {{
                    throw new Error(result.error || 'save failed');
                }}

                setSaveState('', 'Сохранено');
                return result;
            }}

            function getItemsByGroup(groupId) {{
                return items
                    .filter(item => item.group === groupId)
                    .sort((a, b) => a.order - b.order);
            }}

            function renderGroup(group) {{
                const groupItems = getItemsByGroup(group.id);

                if (!groupItems.length) {{
                    return '';
                }}

                const rows = groupItems.map(item => {{
                    return `
                        <div class="row" data-item-id="${{esc(item.id)}}">
                            <div class="td">
                                <div class="cell-name">
                                    <div class="drag">⋮⋮</div>
                                    <div class="priority ${{esc(item.priority)}}" data-role="priority" data-item-id="${{esc(item.id)}}"></div>
                                    <div class="item-name">${{esc(item.name)}}</div>
                                </div>
                            </div>

                            <div class="td">
                                <select class="status-select" data-role="status" data-item-id="${{esc(item.id)}}">
                                    <option value="" ${{item.status === '' ? 'selected' : ''}}></option>
                                    <option value="Есть" ${{item.status === 'Есть' ? 'selected' : ''}}>Есть</option>
                                    <option value="Нет" ${{item.status === 'Нет' ? 'selected' : ''}}>Нет</option>
                                    <option value="Подписан" ${{item.status === 'Подписан' ? 'selected' : ''}}>Подписан</option>
                                    <option value="Не требуется" ${{item.status === 'Не требуется' ? 'selected' : ''}}>Не требуется</option>
                                    <option value="Запрос опросного листа" ${{item.status === 'Запрос опросного листа' ? 'selected' : ''}}>Запрос опросного листа</option>
                                </select>
                            </div>

                            <div class="td">
                                <input class="date-input" type="date" data-role="plan" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.plan))}}">
                                <div class="date-display">${{esc(item.plan || '')}}</div>
                            </div>

                            <div class="td">
                                <input class="date-input" type="date" data-role="fact" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.fact))}}">
                                <div class="date-display">${{esc(item.fact || '')}}</div>
                            </div>
                        </div>
                    `;
                }}).join('');

                return `
                    <div class="group-block">
                        <div class="group-title">${{esc(group.title)}}</div>
                        ${{rows}}
                    </div>
                `;
            }}

            function renderTable() {{
                tableBodyEl.innerHTML = groups.map(renderGroup).join('');
                bindEvents();
            }}

            function bindEvents() {{
                document.querySelectorAll('[data-role="priority"]').forEach(el => {{
                    el.addEventListener('click', async function() {{
                        const itemId = this.dataset.itemId;
                        const item = items.find(x => x.id === itemId);
                        if (!item) return;

                        const newPriority = nextPriority(item.priority);
                        const oldPriority = item.priority;

                        item.priority = newPriority;
                        renderTable();

                        try {{
                            await updateItem(itemId, 'priority', newPriority);
                        }} catch (e) {{
                            item.priority = oldPriority;
                            renderTable();
                            setSaveState('error', 'Ошибка сохранения');
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="status"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const itemId = this.dataset.itemId;
                        const item = items.find(x => x.id === itemId);
                        if (!item) return;

                        const oldValue = item.status;
                        const newValue = this.value;

                        item.status = newValue;

                        try {{
                            await updateItem(itemId, 'status', newValue);
                        }} catch (e) {{
                            item.status = oldValue;
                            renderTable();
                            setSaveState('error', 'Ошибка сохранения');
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="plan"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const itemId = this.dataset.itemId;
                        const item = items.find(x => x.id === itemId);
                        if (!item) return;

                        const oldValue = item.plan;
                        const newValue = fromInputDate(this.value);

                        item.plan = newValue;
                        renderTable();

                        try {{
                            await updateItem(itemId, 'plan', newValue);
                        }} catch (e) {{
                            item.plan = oldValue;
                            renderTable();
                            setSaveState('error', 'Ошибка сохранения');
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="fact"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const itemId = this.dataset.itemId;
                        const item = items.find(x => x.id === itemId);
                        if (!item) return;

                        const oldValue = item.fact;
                        const newValue = fromInputDate(this.value);

                        item.fact = newValue;
                        renderTable();

                        try {{
                            await updateItem(itemId, 'fact', newValue);
                        }} catch (e) {{
                            item.fact = oldValue;
                            renderTable();
                            setSaveState('error', 'Ошибка сохранения');
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
                                    window.BX24.resizeWindow(1100, 720);
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

            renderTable();
            safeInitBx24ForPopup();
        </script>
    </body>
    </html>
    """


@app.get("/popup", response_class=HTMLResponse)
def popup_get(dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    data = get_checklist(dialog_id)

    title = html.escape(data.get("title", "Чек-лист ИД"))
    collab_title = html.escape(data.get("collabTitle", "") or dialog_id or "Без названия")
    contract_deadline = html.escape(data.get("contractDeadline", ""))
    start_date = html.escape(data.get("startDate", ""))
    progress_percent = int(data.get("progressPercent", 0) or 0)

    items_json = json.dumps(data.get("items", []), ensure_ascii=False)
    groups_json = json.dumps(data.get("groups", []), ensure_ascii=False)
    dialog_id_json = json.dumps(dialog_id, ensure_ascii=False)
    collab_title_json = json.dumps(data.get("collabTitle", "") or "", ensure_ascii=False)
    contract_deadline_json = json.dumps(data.get("contractDeadline", "") or "", ensure_ascii=False)
    start_date_json = json.dumps(data.get("startDate", "") or "", ensure_ascii=False)

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{title} — {collab_title}</title>
        <script src="https://api.bitrix24.com/api/v1/"></script>
        <style>
            * {{
                box-sizing: border-box;
            }}

            body {{
                margin: 0;
                font-family: Arial, sans-serif;
                background: #f3f6fb;
                color: #1f2328;
            }}

            .shell {{
                padding: 16px;
            }}

            .modal {{
                background: #fff;
                border-radius: 14px;
                border: 1px solid #e5e7eb;
                overflow: hidden;
                box-shadow: 0 16px 40px rgba(0,0,0,0.12);
            }}

            .header {{
                padding: 16px 18px 12px;
                border-bottom: 1px solid #edf0f2;
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                gap: 16px;
            }}

            .title {{
                font-size: 24px;
                font-weight: 700;
                margin-bottom: 8px;
            }}

            .title small {{
                font-size: 22px;
                font-weight: 600;
                color: #344054;
            }}

            .meta-grid {{
                display: flex;
                gap: 14px;
                flex-wrap: wrap;
                align-items: end;
            }}

            .meta-box {{
                min-width: 180px;
            }}

            .meta-label {{
                font-size: 12px;
                color: #667085;
                margin-bottom: 4px;
            }}

            .meta-input {{
                width: 100%;
                border: 1px solid #d0d7de;
                border-radius: 8px;
                padding: 7px 10px;
                font-size: 13px;
                background: #fff;
            }}

            .progress-box {{
                min-width: 170px;
            }}

            .progress-value {{
                font-size: 22px;
                font-weight: 700;
                margin-bottom: 6px;
            }}

            .progress-track {{
                width: 100%;
                height: 10px;
                background: #edf2f7;
                border-radius: 999px;
                overflow: hidden;
            }}

            .progress-bar {{
                height: 100%;
                width: 0%;
                background: #22c55e;
                transition: width 0.2s ease;
            }}

            .save-state {{
                font-size: 12px;
                font-weight: 700;
                padding: 7px 10px;
                border-radius: 999px;
                background: #eef2ff;
                color: #3730a3;
                white-space: nowrap;
            }}

            .save-state.saving {{
                background: #fff4e5;
                color: #b26a00;
            }}

            .save-state.error {{
                background: #fdecec;
                color: #b42318;
            }}

            .content {{
                padding: 16px 18px 18px;
                max-height: 78vh;
                overflow: auto;
            }}

            .table {{
                width: 100%;
                border: 1px solid #e5e7eb;
                border-radius: 12px;
                overflow: hidden;
                background: #fff;
            }}

            .thead {{
                position: sticky;
                top: 0;
                z-index: 10;
                background: #f8fafc;
                border-bottom: 1px solid #e5e7eb;
            }}

            .thead-top,
            .thead-bottom,
            .row {{
                display: grid;
                grid-template-columns: 1.7fr 130px 1fr 130px 130px;
                gap: 0;
                align-items: stretch;
            }}

            .th,
            .td {{
                padding: 9px 10px;
                border-right: 1px solid #edf0f2;
            }}

            .th:last-child,
            .td:last-child {{
                border-right: none;
            }}

            .th {{
                font-size: 12px;
                font-weight: 700;
                color: #475467;
            }}

            .th.center {{
                text-align: center;
            }}

            .group-block {{
                border-top: 8px solid #f8fafc;
            }}

            .group-title {{
                padding: 10px 12px;
                background: #fafbfc;
                border-top: 1px solid #e5e7eb;
                border-bottom: 1px solid #e5e7eb;
                font-size: 13px;
                font-weight: 700;
                color: #344054;
            }}

            .row {{
                border-top: 1px solid #edf0f2;
                background: #fff;
            }}

            .row.not-required {{
                background: #fafafa;
            }}

            .row.not-required .item-name {{
                text-decoration: line-through;
                color: #98a2b3;
            }}

            .cell-name {{
                display: flex;
                align-items: center;
                gap: 8px;
                min-width: 0;
            }}

            .status-indicator {{
                width: 14px;
                height: 14px;
                border-radius: 999px;
                border: 1px solid #d0d7de;
                flex: 0 0 14px;
                background: #fff;
            }}

            .status-indicator.green {{
                background: #22c55e;
                border-color: #22c55e;
            }}

            .status-indicator.gray {{
                background: #9ca3af;
                border-color: #9ca3af;
            }}

            .item-name {{
                font-size: 13px;
                font-weight: 700;
                color: #1f2328;
                line-height: 1.25;
                min-width: 0;
            }}

            .status-select,
            .date-input {{
                width: 100%;
                border: 1px solid #d0d7de;
                border-radius: 8px;
                padding: 6px 8px;
                font-size: 13px;
                background: #fff;
            }}

            .doc-btn {{
                display: inline-block;
                width: 100%;
                text-align: center;
                padding: 6px 8px;
                border: 1px solid #d0d7de;
                border-radius: 8px;
                background: #f8fafc;
                color: #1f2328;
                font-size: 12px;
                text-decoration: none;
            }}

            .add-item-row {{
                padding: 10px 12px 12px;
                border-top: 1px solid #edf0f2;
                background: #fcfcfd;
                display: flex;
                gap: 8px;
                align-items: center;
            }}

            .add-item-input {{
                flex: 1;
                border: 1px solid #d0d7de;
                border-radius: 8px;
                padding: 7px 10px;
                font-size: 13px;
            }}

            .add-item-btn {{
                border: 1px solid #d0d7de;
                border-radius: 8px;
                padding: 7px 10px;
                background: #fff;
                cursor: pointer;
                font-size: 12px;
            }}

            @media (max-width: 1080px) {{
                .thead-top,
                .thead-bottom,
                .row {{
                    grid-template-columns: 1fr;
                }}

                .th,
                .td {{
                    border-right: none;
                    border-bottom: 1px solid #edf0f2;
                }}

                .th:last-child,
                .td:last-child {{
                    border-bottom: none;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="shell">
            <div class="modal">
                <div class="header">
                    <div style="flex:1;">
                        <div class="title">Чек-лист ИД <small>— {collab_title}</small></div>

                        <div class="meta-grid">
                            <div class="meta-box">
                                <div class="meta-label">Срок по договору</div>
                                <input id="contractDeadlineInput" class="meta-input" type="date" value="">
                            </div>

                            <div class="meta-box">
                                <div class="meta-label">Начало работ</div>
                                <input id="startDateInput" class="meta-input" type="date" value="">
                            </div>

                            <div class="progress-box">
                                <div class="meta-label">Прогресс</div>
                                <div class="progress-value" id="progressValue">{progress_percent}%</div>
                                <div class="progress-track">
                                    <div class="progress-bar" id="progressBar"></div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div id="saveState" class="save-state">Сохранено</div>
                </div>

                <div class="content">
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

                        <div id="tableBody"></div>
                    </div>
                </div>
            </div>
        </div>

        <script>
            const dialogId = {dialog_id_json};
            const groups = {groups_json};
            let items = {items_json};
            let collabTitle = {collab_title_json};
            let contractDeadline = {contract_deadline_json};
            let startDate = {start_date_json};

            const saveStateEl = document.getElementById('saveState');
            const tableBodyEl = document.getElementById('tableBody');
            const progressValueEl = document.getElementById('progressValue');
            const progressBarEl = document.getElementById('progressBar');
            const contractDeadlineInput = document.getElementById('contractDeadlineInput');
            const startDateInput = document.getElementById('startDateInput');

            function setSaveState(mode, text) {{
                saveStateEl.classList.remove('saving', 'error');

                if (mode === 'saving') {{
                    saveStateEl.classList.add('saving');
                }}
                if (mode === 'error') {{
                    saveStateEl.classList.add('error');
                }}

                saveStateEl.textContent = text;
            }}

            function esc(v) {{
                if (v === null || v === undefined) return '';
                return String(v)
                    .replaceAll('&', '&amp;')
                    .replaceAll('<', '&lt;')
                    .replaceAll('>', '&gt;')
                    .replaceAll('"', '&quot;');
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

            function calculateProgress() {{
                const activeItems = items.filter(x => normalizeStatus(x.status) !== 'Не требуется');
                const completedItems = activeItems.filter(x => normalizeStatus(x.status) === 'Есть');

                const activeCount = activeItems.length;
                const completedCount = completedItems.length;
                const percent = activeCount ? Math.round((completedCount / activeCount) * 100) : 0;

                progressValueEl.textContent = percent + '%';
                progressBarEl.style.width = percent + '%';
            }}

            function replaceItemInState(updatedItem) {{
                if (!updatedItem || !updatedItem.id) {{
                    return;
                }}

                items = items.map(item => item.id === updatedItem.id ? updatedItem : item);
            }}

            async function updateItem(itemId, field, value) {{
                setSaveState('saving', 'Сохраняем...');

                const response = await fetch('/api/checklist/update-item', {{
                    method: 'POST',
                    headers: {{
                        'Content-Type': 'application/json'
                    }},
                    body: JSON.stringify({{
                        dialogId,
                        itemId,
                        field,
                        value
                    }})
                }});

                const result = await response.json();

                if (!response.ok || !result.ok) {{
                    throw new Error(result.error || 'save failed');
                }}

                if (result.item) {{
                    replaceItemInState(result.item);
                }}

                setSaveState('', 'Сохранено');
                return result;
            }}

            async function updateMeta(field, value) {{
                setSaveState('saving', 'Сохраняем...');

                const response = await fetch('/api/checklist/update-meta', {{
                    method: 'POST',
                    headers: {{
                        'Content-Type': 'application/json'
                    }},
                    body: JSON.stringify({{
                        dialogId,
                        field,
                        value
                    }})
                }});

                const result = await response.json();

                if (!response.ok || !result.ok) {{
                    throw new Error(result.error || 'save failed');
                }}

                setSaveState('', 'Сохранено');
                return result;
            }}

            async function addItem(groupId, name) {{
                setSaveState('saving', 'Сохраняем...');

                const response = await fetch('/api/checklist/add-item', {{
                    method: 'POST',
                    headers: {{
                        'Content-Type': 'application/json'
                    }},
                    body: JSON.stringify({{
                        dialogId,
                        groupId,
                        name
                    }})
                }});

                const result = await response.json();

                if (!response.ok || !result.ok) {{
                    throw new Error(result.error || 'add item failed');
                }}

                setSaveState('', 'Сохранено');
                return result;
            }}

            function getItemsByGroup(groupId) {{
                return items
                    .filter(item => item.group === groupId)
                    .sort((a, b) => a.order - b.order);
            }}

            function renderGroup(group) {{
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = group.id !== 4;

                const rows = groupItems.map(item => {{
                    const rowClass = normalizeStatus(item.status) === 'Не требуется' ? 'row not-required' : 'row';

                    return `
                        <div class="${{rowClass}}" data-item-id="${{esc(item.id)}}">
                            <div class="td">
                                <div class="cell-name">
                                    <div class="${{indicatorClass(item.status)}}"></div>
                                    <div class="item-name">${{esc(item.name)}}</div>
                                </div>
                            </div>

                            <div class="td">
                                <a class="doc-btn" href="${{esc(item.documentUrl || '#')}}" target="_blank">Посмотреть</a>
                            </div>

                            <div class="td">
                                <select class="status-select" data-role="status" data-item-id="${{esc(item.id)}}">
                                    <option value="" ${{normalizeStatus(item.status) === '' ? 'selected' : ''}}></option>
                                    <option value="Есть" ${{normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}}>Есть</option>
                                    <option value="Нет" ${{normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}}>Нет</option>
                                    <option value="Не требуется" ${{normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}}>Не требуется</option>
                                </select>
                            </div>

                            <div class="td">
                                <input class="date-input" type="date" data-role="plan" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.plan))}}">
                            </div>

                            <div class="td">
                                <input class="date-input" type="date" data-role="fact" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.fact))}}">
                            </div>
                        </div>
                    `;
                }}).join('');

                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="addItemInput_${{group.id}}" type="text" placeholder="Новый пункт">
                        <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${{group.id}}">
                            Добавить пункт
                        </button>
                    </div>
                ` : '';

                return `
                    <div class="group-block">
                        <div class="group-title">${{esc(group.title)}}</div>
                        ${{rows}}
                        ${{addBlock}}
                    </div>
                `;
            }}

            function renderTable() {{
                tableBodyEl.innerHTML = groups.map(renderGroup).join('');
                bindEvents();
                calculateProgress();
            }}

            function bindEvents() {{
                document.querySelectorAll('[data-role="status"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const itemId = this.dataset.itemId;
                        const item = items.find(x => x.id === itemId);
                        if (!item) return;

                        const oldItem = {{ ...item }};
                        const newValue = this.value;

                        item.status = newValue;
                        renderTable();

                        try {{
                            await updateItem(itemId, 'status', newValue);
                            renderTable();
                        }} catch (e) {{
                            replaceItemInState(oldItem);
                            renderTable();
                            setSaveState('error', 'Ошибка сохранения');
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="plan"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const itemId = this.dataset.itemId;
                        const item = items.find(x => x.id === itemId);
                        if (!item) return;

                        const oldItem = {{ ...item }};
                        const newValue = fromInputDate(this.value);

                        item.plan = newValue;
                        renderTable();

                        try {{
                            await updateItem(itemId, 'plan', newValue);
                            renderTable();
                        }} catch (e) {{
                            replaceItemInState(oldItem);
                            renderTable();
                            setSaveState('error', 'Ошибка сохранения');
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="fact"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const itemId = this.dataset.itemId;
                        const item = items.find(x => x.id === itemId);
                        if (!item) return;

                        const oldItem = {{ ...item }};
                        const newValue = fromInputDate(this.value);

                        item.fact = newValue;
                        renderTable();

                        try {{
                            await updateItem(itemId, 'fact', newValue);
                            renderTable();
                        }} catch (e) {{
                            replaceItemInState(oldItem);
                            renderTable();
                            setSaveState('error', 'Ошибка сохранения');
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
                            if (result.item) {{
                                items.push(result.item);
                            }}
                            input.value = '';
                            renderTable();
                        }} catch (e) {{
                            setSaveState('error', 'Ошибка добавления пункта');
                        }}
                    }});
                }});
            }}

            contractDeadlineInput.value = toInputDate(contractDeadline);
            startDateInput.value = toInputDate(startDate);

            contractDeadlineInput.addEventListener('change', async function() {{
                const oldValue = contractDeadline;
                const newValue = fromInputDate(this.value);
                contractDeadline = newValue;

                try {{
                    await updateMeta('contractDeadline', newValue);
                }} catch (e) {{
                    contractDeadline = oldValue;
                    contractDeadlineInput.value = toInputDate(oldValue);
                    setSaveState('error', 'Ошибка сохранения');
                }}
            }});

            startDateInput.addEventListener('change', async function() {{
                const oldValue = startDate;
                const newValue = fromInputDate(this.value);
                startDate = newValue;

                try {{
                    await updateMeta('startDate', newValue);
                }} catch (e) {{
                    startDate = oldValue;
                    startDateInput.value = toInputDate(oldValue);
                    setSaveState('error', 'Ошибка сохранения');
                }}
            }});

            function safeInitBx24ForPopup() {{
                try {{
                    if (window.BX24 && typeof window.BX24.init === 'function') {{
                        window.BX24.init(function () {{
                            try {{
                                if (typeof window.BX24.resizeWindow === 'function') {{
                                    window.BX24.resizeWindow(1100, 720);
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

            renderTable();
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
        target_item["status"] = normalize_status(value)
    elif field == "plan":
        target_item["plan"] = normalize_date_string(value)
    elif field == "fact":
        target_item["fact"] = normalize_date_string(value)

    data = normalize_checklist_data(data)
    save_checklist(dialog_id, data)
    updated_item = next(
        (item for item in data.get("items", []) if str(item.get("id")) == item_id),
        None,
    )

    return JSONResponse({
        "ok": True,
        "item": updated_item or target_item,
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

    allowed_fields = {"contractDeadline", "startDate", "collabTitle"}
    if field not in allowed_fields:
        return JSONResponse({"ok": False, "error": "invalid field"}, status_code=400)

    data = get_checklist(dialog_id)

    if field in {"contractDeadline", "startDate"}:
        data[field] = normalize_date_string(value)
    elif field == "collabTitle":
        data[field] = clean_cell_value(value)

    data = normalize_checklist_data(data)
    save_checklist(dialog_id, data)

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "field": field,
        "value": data.get(field, ""),
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
        "documentUrl": "#",
        "isCustom": True,
    }

    items.append(new_item)
    data["items"] = items
    data = normalize_checklist_data(data)
    save_checklist(dialog_id, data)
    saved_item = next(
        (item for item in data.get("items", []) if str(item.get("id")) == str(new_item["id"])),
        new_item,
    )

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "item": saved_item,
        "progressPercent": data.get("progressPercent", 0),
    })

def home_get_legacy_disabled():
    return """
    <html>
    <head>
        <meta charset="utf-8">
        <title>Чек-лист ИД</title>
        <script src="https://api.bitrix24.com/api/v1/"></script>
    </head>
    <body style="font-family:Arial,sans-serif;padding:40px">
        <h1>Чек-лист ИД</h1>

        <button id="bindBtn" style="padding:10px 16px;font-size:16px;">
            Привязать виджет к коллабам
        </button>

        <pre id="log" style="margin-top:20px;white-space:pre-wrap;"></pre>

        <script>
            function log(text) {
                document.getElementById('log').textContent = text;
            }

            document.getElementById('bindBtn').addEventListener('click', function () {
                BX24.init(function () {
                    BX24.callMethod('placement.bind', {
                        PLACEMENT: 'IM_SIDEBAR',
                        HANDLER: window.location.origin + '/sidebar',
                        TITLE: 'Чек-лист ИД'
                    }, function(result) {
                        if (result.error()) {
                            log('Ошибка: ' + result.error());
                        } else {
                            log('Готово. placement.bind выполнен:\\n' + JSON.stringify(result.data(), null, 2));
                        }
                    });
                });
            });
        </script>
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
