import json
import html

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from app.settings import DEBUG_LOG_PATH
from app.logging_utils import write_debug_log
from app.checklists.storage import list_checklist_summaries


router = APIRouter()


@router.post("/api/debug/event")
async def api_debug_event(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raw = await request.body()
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            payload = {
                "raw": raw.decode("utf-8", errors="ignore")
            }

    event = str(payload.get("event") or "unknown").strip()
    write_debug_log(event, payload)

    return JSONResponse({"ok": True})


@router.get("/debug/logs", response_class=HTMLResponse)
def debug_logs(userId: str = ""):
    allowed_debug_user_ids = {"138", "18"}
    normalized_user_id = str(userId or "").strip()

    if normalized_user_id not in allowed_debug_user_ids:
        return HTMLResponse(
            """
            <html>
            <head>
                <meta charset="utf-8">
                <title>Access denied</title>
            </head>
            <body style="font-family:Arial,sans-serif;padding:24px">
                <h1>Доступ запрещён</h1>
                <p>Эта страница доступна только техническим пользователям.</p>
            </body>
            </html>
            """,
            status_code=403,
        )

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


@router.get("/admin", response_class=HTMLResponse)
def admin():
    rows = list_checklist_summaries()

    items = "".join(
        f"<li><b>{html.escape(row['dialog_id'])}</b> — {html.escape(row['title'])}</li>"
        for row in rows
    ) or "<li>Пока нет сохранённых чек-листов</li>"

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>Администрирование чек-листов</title>
    </head>
    <body style="font-family:Arial,sans-serif;padding:40px;max-width:900px">
        <h1>Администрирование чек-листов</h1>

        <p>
            Чек-листы больше не загружаются из XLSX.
            Они создаются автоматически по шаблонам приложения и данным,
            которые приходят через контекст проекта.
        </p>

        <h2>Сохранённые чек-листы</h2>
        <ul>{items}</ul>
    </body>
    </html>
    """