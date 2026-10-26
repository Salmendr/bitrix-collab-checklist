"""What a closed request gets."""
from __future__ import annotations

import html
import json


REASON_TEXT = {
    "expired": "Время входа истекло. Закройте окно и откройте чек-лист заново из Битрикс24.",
    "bitrix_unreachable": "Не удалось проверить вход: Битрикс24 не ответил. Попробуйте открыть чек-лист ещё раз.",
    "portal_not_configured": "Вход не настроен на сервере: не задан домен портала Битрикс24.",
    "portal_unreachable": "Не удалось проверить вход: портал не ответил. Попробуйте открыть чек-лист ещё раз.",
    "portal_rejected": "Портал не подтвердил вход. Откройте чек-лист с портала ещё раз.",
    "portal_no_bitrix_user": "Ваша учётная запись на портале не связана с Битрикс24. Обратитесь к администратору портала.",
}
DEFAULT_TEXT = "Откройте чек-лист из Битрикс24."


def reason_text(reason: str) -> str:
    return REASON_TEXT.get(str(reason or ""), DEFAULT_TEXT)


def closed_json(reason: str) -> bytes:
    return json.dumps({
        "ok": False,
        "error": "Доступ закрыт. " + reason_text(reason),
        "authRequired": True,
        "authReason": str(reason or ""),
    }, ensure_ascii=False).encode("utf-8")


def closed_html(reason: str) -> bytes:
    text = html.escape(reason_text(reason))
    code = html.escape(str(reason or ""))
    return f"""<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Доступ закрыт</title>
</head>
<body style="margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#f8fafc;font-family:Arial,sans-serif;color:#1f2328;">
<div style="max-width:440px;margin:16px;padding:24px 28px;border:1px solid #e5e7eb;border-radius:14px;background:#fff;box-shadow:0 12px 30px rgba(15,23,42,.08);">
<h1 style="margin:0 0 10px;font-size:20px;">Доступ закрыт</h1>
<p style="margin:0;font-size:14px;line-height:1.5;color:#344054;">{text}</p>
<p style="margin:14px 0 0;font-size:11px;color:#98a2b3;">{code}</p>
</div>
</body>
</html>""".encode("utf-8")
