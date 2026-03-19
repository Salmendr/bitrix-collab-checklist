from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
import requests
import json
import html

app = FastAPI()


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


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
    <head>
        <meta charset="utf-8">
        <title>Bitrix Checklist MVP</title>
    </head>
    <body style="font-family:Arial,sans-serif;padding:40px">
        <h1>Приложение работает</h1>
        <p>Это главная страница приложения.</p>
        <p><a href="/sidebar" target="_blank">Открыть /sidebar</a></p>
        <p><a href="/install" target="_blank">Открыть /install</a></p>
    </body>
    </html>
    """


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
def sidebar():
    return """
    <!doctype html>
    <html lang="ru">
    <head>
        <meta charset="utf-8">
        <title>Bitrix24 Sidebar Test</title>
        <script src="https://api.bitrix24.com/api/v1/"></script>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 16px;
                background: #f7f9fc;
            }
            .card {
                background: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 12px;
                padding: 16px;
                margin-bottom: 12px;
            }
            .title {
                font-size: 20px;
                font-weight: 700;
                margin-bottom: 10px;
            }
            .row {
                margin-bottom: 10px;
            }
            .label {
                color: #666;
                font-size: 13px;
                margin-bottom: 3px;
            }
            .value {
                font-weight: 600;
                color: #222;
            }
            .note {
                font-size: 13px;
                color: #666;
                margin-top: 10px;
            }
        </style>
    </head>
    <body>
        <div id="app"></div>

        <script>
            function esc(v) {
                if (v === null || v === undefined) return '';
                return String(v)
                    .replace(/&/g, '&amp;')
                    .replace(/</g, '&lt;')
                    .replace(/>/g, '&gt;')
                    .replace(/"/g, '&quot;');
            }

            function render(mode, dialogId, message) {
                var html =
                    '<div class="card">' +
                        '<div class="title">Тестовый чек-лист</div>' +

                        '<div class="row">' +
                            '<div class="label">Режим</div>' +
                            '<div class="value">' + esc(mode) + '</div>' +
                        '</div>' +

                        '<div class="row">' +
                            '<div class="label">dialogId</div>' +
                            '<div class="value">' + esc(dialogId || 'не передан') + '</div>' +
                        '</div>' +

                        '<div class="row">' +
                            '<div class="label">Сообщение</div>' +
                            '<div class="value">' + esc(message) + '</div>' +
                        '</div>' +

                        '<div class="note">' +
                            'Позже здесь будет ваш чек-лист по текущей коллабе.' +
                        '</div>' +
                    '</div>';

                document.getElementById('app').innerHTML = html;
            }

            render(
                'Локальный браузер',
                '',
                'Страница открыта вне Bitrix24, поэтому dialogId пока недоступен'
            );

            try {
                if (typeof BX24 !== 'undefined') {
                    BX24.init(function () {
                        var dialogId = '';

                        try {
                            var info = BX24.placement.info();
                            if (info && info.options && info.options.dialogId) {
                                dialogId = info.options.dialogId;
                            }
                        } catch (e) {
                            console.log(e);
                        }

                        render(
                            'Bitrix24',
                            dialogId,
                            'Страница открыта внутри Bitrix24'
                        );

                        try {
                            BX24.fitWindow();
                        } catch (e) {
                            console.log(e);
                        }
                    });
                }
            } catch (e) {
                console.log(e);
            }
        </script>
    </body>
    </html>
    """


@app.get("/api/test")
def api_test():
    return JSONResponse({
        "ok": True,
        "message": "API проекта работает"
    })