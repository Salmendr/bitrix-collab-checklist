import json
import html

import requests
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool

from app.settings import (
    APP_PORTAL_PATH,
    APP_BASE_PATH,
    PUBLIC_APP_BASE_URL,
)

from app.bitrix.client import bitrix_rest_call

from app.checklists.utils import (
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.ui.shell import (
    normalize_domain,
    get_public_app_base_path,
    get_public_app_base_url,
    install_finish_block,
    app_home_html,
    textarea_html,
)
from app.ui.request_diagnostics import write_ui_request_diagnostic
from app.auth.bitrix import BitrixAuthError, verify_bitrix_auth
from app.auth.config import auth_mode, bitrix_portal_domains
from app.auth.identity import establish
from app.auth.pages import closed_html
from app.logging_utils import write_debug_log


router = APIRouter()

# Keys Bitrix24 posts with the window; they are never printed or put into a
# page. REFRESH_ID lives for weeks.
SECRET_FORM_KEYS = frozenset({
    "AUTH_ID",
    "REFRESH_ID",
    "APPLICATION_TOKEN",
    "access_token",
    "refresh_token",
    "auth",
})


def safe_form_context(form: dict) -> str:
    return json.dumps(
        {
            key: ("<hidden>" if key in SECRET_FORM_KEYS else value)
            for key, value in form.items()
        },
        ensure_ascii=False,
        indent=2,
    )


async def authorize_bitrix_entry(request: Request, form: dict) -> HTMLResponse | None:
    """Check the AUTH_ID Bitrix24 opened the window with.

    Returns the closed page when the window must not open; otherwise the
    request carries the checked employee (the page gets its keys).
    """
    mode = auth_mode()
    if mode == "off":
        return None
    auth_id = str(form.get("AUTH_ID") or form.get("auth") or "").strip()
    domain = str(
        request.query_params.get("DOMAIN")
        or form.get("DOMAIN")
        or ""
    ).strip()
    try:
        identity = await run_in_threadpool(verify_bitrix_auth, auth_id, domain)
    except BitrixAuthError as exc:
        write_debug_log("auth_bitrix_entry_rejected", {
            "path": request.url.path,
            "reason": exc.reason,
            "detail": exc.detail,
            "domain": domain,
            "memberId": str(form.get("member_id") or ""),
            "mode": mode,
        })
        if mode == "on":
            return HTMLResponse(closed_html(exc.reason).decode("utf-8"), status_code=401)
        return None
    establish(request, identity)
    write_debug_log("auth_bitrix_entry_accepted", {
        "path": request.url.path,
        "userId": identity["userId"],
        "domain": identity.get("domain", ""),
    })
    return None


def extract_dialog_id_from_form(form_data: dict) -> str:
    def pick(value) -> str:
        raw = str(value or "").strip()
        return normalize_dialog_id(raw) if raw else ""

    direct_candidates = [
        form_data.get("dialogId"),
        form_data.get("DIALOG_ID"),
        form_data.get("dialog_id"),
        form_data.get("chatId"),
        form_data.get("CHAT_ID"),
        form_data.get("chat_id"),
    ]

    for value in direct_candidates:
        found = pick(value)
        if found:
            return found

    def walk(obj) -> str:
        if isinstance(obj, dict):
            preferred_keys = [
                "dialogId",
                "DIALOG_ID",
                "dialog_id",
                "chatId",
                "CHAT_ID",
                "chat_id",
            ]

            for key in preferred_keys:
                found = pick(obj.get(key))
                if found:
                    return found

            for value in obj.values():
                found = walk(value)
                if found:
                    return found

        elif isinstance(obj, list):
            for value in obj:
                found = walk(value)
                if found:
                    return found

        return ""

    json_candidates = [
        form_data.get("PLACEMENT_OPTIONS"),
        form_data.get("placementOptions"),
        form_data.get("options"),
    ]

    for raw in json_candidates:
        if not raw:
            continue

        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
            found = walk(data)
            if found:
                return found
        except Exception:
            pass

    return ""


def extract_checklist_key_from_form(form_data: dict) -> str:
    def pick(value) -> str:
        raw = str(value or "").strip()
        return normalize_checklist_key(raw) if raw else ""

    direct_candidates = [
        form_data.get("checklistKey"),
        form_data.get("CHECKLIST_KEY"),
        form_data.get("checklist_key"),
    ]

    for value in direct_candidates:
        found = pick(value)
        if found:
            return found

    def walk(obj) -> str:
        if isinstance(obj, dict):
            preferred_keys = [
                "checklistKey",
                "CHECKLIST_KEY",
                "checklist_key",
            ]

            for key in preferred_keys:
                found = pick(obj.get(key))
                if found:
                    return found

            for value in obj.values():
                found = walk(value)
                if found:
                    return found

        elif isinstance(obj, list):
            for value in obj:
                found = walk(value)
                if found:
                    return found

        return ""

    json_candidates = [
        form_data.get("PLACEMENT_OPTIONS"),
        form_data.get("placementOptions"),
        form_data.get("options"),
    ]

    for raw in json_candidates:
        if not raw:
            continue

        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
            found = walk(data)
            if found:
                return found
        except Exception:
            pass

    return "id"


def extract_option_from_form(form_data: dict, key: str) -> str:
    """A value the link of a message put into the application window."""
    direct = str(form_data.get(key) or "").strip()
    if direct:
        return direct

    def walk(obj) -> str:
        if isinstance(obj, dict):
            value = str(obj.get(key) or "").strip()
            if value and not isinstance(obj.get(key), (dict, list)):
                return value
            for nested in obj.values():
                found = walk(nested)
                if found:
                    return found
        elif isinstance(obj, list):
            for nested in obj:
                found = walk(nested)
                if found:
                    return found
        return ""

    for raw in (form_data.get("PLACEMENT_OPTIONS"), form_data.get("placementOptions"), form_data.get("options")):
        if not raw:
            continue
        try:
            found = walk(json.loads(raw) if isinstance(raw, str) else raw)
        except Exception:
            continue
        if found:
            return found
    return ""


@router.get("/health")
def health(request: Request):
    return {
        "ok": True,
        "appBasePathEnv": APP_BASE_PATH,
        "publicAppBaseUrlEnv": PUBLIC_APP_BASE_URL,
        "portalPath": APP_PORTAL_PATH,
        "requestBaseUrl": str(request.base_url).rstrip("/"),
        "publicBasePathDetected": get_public_app_base_path(request),
        "publicBaseUrlDetected": get_public_app_base_url(request),
        "xForwardedPrefix": request.headers.get("x-forwarded-prefix", ""),
        "xForwardedProto": request.headers.get("x-forwarded-proto", ""),
        "xForwardedHost": request.headers.get("x-forwarded-host", ""),
        "rootPath": request.scope.get("root_path", ""),
        "launchRoute": "/launch",
        "popupRoute": "/popup",
        "textareaRoute": "/textarea",
    }


@router.get("/", response_class=HTMLResponse)
def home_get(
    request: Request,
    dialogId: str = "",
    checklistKey: str = "id",
    mode: str = "",
):
    write_ui_request_diagnostic(
        "popup_diag_http_application_get",
        request,
        extra={
            "dialogId": normalize_dialog_id(dialogId),
            "checklistKey": normalize_checklist_key(checklistKey),
            "mode": str(mode or ""),
        },
    )
    return app_home_html()


@router.post("/", response_class=HTMLResponse)
async def home_post(request: Request):
    form = dict(await request.form())
    closed = await authorize_bitrix_entry(request, form)
    if closed is not None:
        return closed

    dialog_id = extract_dialog_id_from_form(form)
    checklist_key = extract_checklist_key_from_form(form)
    raw_context = safe_form_context(form)

    print("HOME POST FORM:", raw_context)
    print("HOME EXTRACTED DIALOG ID:", dialog_id)
    print("HOME EXTRACTED CHECKLIST KEY:", checklist_key)

    write_ui_request_diagnostic(
        "popup_diag_http_application_post",
        request,
        form_keys=form.keys(),
        extra={
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
        },
    )

    return app_home_html(
        dialog_id,
        checklist_key,
        raw_context,
        initial_file_item_id=extract_option_from_form(form, "fileItemId"),
        initial_file_version_id=extract_option_from_form(form, "fileVersionId"),
    )


@router.get("/launch", response_class=HTMLResponse)
def launch_get(
    request: Request,
    dialogId: str = "",
    checklistKey: str = "id",
):
    write_ui_request_diagnostic(
        "popup_diag_http_launch_get",
        request,
        extra={
            "dialogId": normalize_dialog_id(dialogId),
            "checklistKey": normalize_checklist_key(checklistKey),
        },
    )
    return app_home_html()


@router.post("/launch", response_class=HTMLResponse)
async def launch_post(request: Request):
    write_ui_request_diagnostic(
        "popup_diag_http_launch_post",
        request,
    )
    # An application handler set to /launch is checked like the main one.
    form = dict(await request.form())
    closed = await authorize_bitrix_entry(request, form)
    if closed is not None:
        return closed
    return app_home_html()


@router.get("/install", response_class=HTMLResponse)
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


@router.post("/install", response_class=HTMLResponse)
@router.post("/install/", response_class=HTMLResponse)
async def install_post(request: Request):
    form = dict(await request.form())
    query = dict(request.query_params)
    params = {**query, **form}

    access_token = params.get("AUTH_ID") or params.get("access_token") or ""
    domain = normalize_domain(params.get("DOMAIN") or params.get("domain") or "")
    base_url = get_public_app_base_url(request)

    app_sid = params.get("APP_SID") or ""

    # The page is open to everybody (Bitrix24 calls it while installing), so
    # it talks only to the portal of the settings, never to an address from
    # the request.
    if auth_mode() != "off" and domain not in bitrix_portal_domains():
        write_debug_log("auth_install_domain_refused", {"domain": domain})
        domain = ""

    if app_sid and domain and not access_token:
        try:
            auth_response = requests.get(
                f"https://{domain}/rest/app.auth.json",
                params={"app_sid": app_sid},
                timeout=10,
            )
            auth_data = auth_response.json()
            access_token = auth_data.get("result", {}).get("access_token", "")
        except Exception:
            pass

    bind_result = {
        "im_textarea": {"skipped": True}
    }
    placement_get_result = {
        "all": {"skipped": True}
    }

    if domain and access_token:
        bind_result["im_textarea"] = bitrix_rest_call(
            domain,
            "placement.bind",
            access_token,
            {
                "PLACEMENT": "IM_TEXTAREA",
                "HANDLER": f"{base_url}/textarea",
                "TITLE": "ТЕСТ",
                "OPTIONS[iconName]": "fa-bars",
                "OPTIONS[context]": "CHAT",
                "OPTIONS[role]": "ADMIN",
            },
        )

        placement_get_result["all"] = bitrix_rest_call(
            domain,
            "placement.get",
            access_token,
            {},
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

        <h2>Что прислал Bitrix24 (query)</h2>
        <pre>{html.escape(json.dumps(query, ensure_ascii=False, indent=2))}</pre>

        <h2>Что прислал Bitrix24 (form)</h2>
        <pre>{html.escape(json.dumps(form, ensure_ascii=False, indent=2))}</pre>

        <h2>Ответ placement.bind</h2>
        <pre>{html.escape(json.dumps(bind_result, ensure_ascii=False, indent=2))}</pre>
        <h2>Ответ placement.get</h2>
        <pre>{html.escape(json.dumps(placement_get_result, ensure_ascii=False, indent=2))}</pre>

        {install_finish_block()}
    </body>
    </html>
    """


@router.get("/textarea", response_class=HTMLResponse)
def textarea_get(request: Request, dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    write_ui_request_diagnostic(
        "popup_diag_http_textarea_get",
        request,
        extra={"dialogId": dialog_id},
    )
    return textarea_html(dialog_id, "GET /textarea")


@router.post("/textarea", response_class=HTMLResponse)
async def textarea_post(request: Request):
    form = dict(await request.form())
    closed = await authorize_bitrix_entry(request, form)
    if closed is not None:
        return closed
    dialog_id = extract_dialog_id_from_form(form)
    raw_context = safe_form_context(form)

    print("TEXTAREA POST FORM:", raw_context)
    print("TEXTAREA EXTRACTED DIALOG ID:", dialog_id)

    write_ui_request_diagnostic(
        "popup_diag_http_textarea_post",
        request,
        form_keys=form.keys(),
        extra={"dialogId": dialog_id},
    )

    return textarea_html(dialog_id, raw_context)
