from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
import json
import html

app = FastAPI()


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
        <h1>Проект работает</h1>
        <p>Это локальный тест FastAPI.</p>
        <p>Следующий шаг — открыть страницу sidebar.</p>
        <p><a href="/sidebar" target="_blank">Открыть /sidebar</a></p>
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
                            'Сейчас это тестовая страница. Позже здесь будет чек-лист по текущей коллабе.' +
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


@app.post("/install", response_class=HTMLResponse)
async def install(request: Request):
    form = dict(await request.form())

    return f"""
    <html>
    <head><meta charset="utf-8"></head>
    <body style="font-family:Arial,sans-serif;padding:40px">
        <h1>Install callback получен</h1>
        <p>Это тестовая страница установки приложения из Bitrix24.</p>
        <pre>{html.escape(json.dumps(form, ensure_ascii=False, indent=2))}</pre>
    </body>
    </html>
    """


@app.get("/api/test")
def api_test():
    return JSONResponse({
        "ok": True,
        "message": "API проекта работает"
    })