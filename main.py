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


def app_home_html():
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
            Зарегистрировать виджет в IM_SIDEBAR
        </button>

        <p style="margin-top:20px;"><a href="/sidebar" target="_blank">Открыть /sidebar</a></p>
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
                                TITLE: 'Чек-лист ИД'
                            }, function(result) {
                                if (result.error()) {
                                    log('placement.bind error: ' + result.error());
                                } else {
                                    log('placement.bind ok:\\n' + JSON.stringify(result.data(), null, 2));
                                }
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


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def home_get():
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

    bind_result = {"skipped": True}

    if domain and access_token:
        bind_result = bitrix_rest_call(
            domain,
            "placement.bind",
            access_token,
            {
                "PLACEMENT": "IM_SIDEBAR",
                "HANDLER": f"{base_url}/sidebar",
                "TITLE": "Чек-лист ИД"
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
        <p>Если bind прошёл успешно, виджет будет зарегистрирован в IM_SIDEBAR.</p>

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


@app.get("/api/checklist")
def api_checklist(dialogId: str = ""):
    dialogId = normalize_dialog_id(dialogId)
    return JSONResponse(get_checklist(dialogId))

@app.get("/", response_class=HTMLResponse)
def home_get():
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