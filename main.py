from pathlib import Path
from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
import requests
import json
import html
import mimetypes
from io import BytesIO
from datetime import datetime
from urllib.parse import quote, urlparse
import openpyxl

from app.settings import (
    BASE_DIR,
    APP_PORTAL_PATH,
    APP_BASE_PATH,
    PUBLIC_APP_BASE_URL,
    N8N_SHARED_TOKEN,
    UPLOAD_ROOT,
    DEBUG_LOG_PATH,
    ensure_runtime_directories,
)
from app.db import init_db
from app.logging_utils import write_debug_log
from app.bitrix.client import bitrix_rest_call, bitrix_webhook_call
from app.yandex_disk.client import (
    is_yandex_disk_enabled,
    normalize_yandex_disk_path,
    yandex_disk_delete_path,
    yandex_disk_ensure_folder,
    yandex_disk_publish_path,
    yandex_disk_get_resource_meta,
)

from app.checklists.permissions import can_user_delete_files

from app.checklists.registry import (
    OPR_GROUPS,
    CONCEPT_GROUPS,
)

from app.checklists.utils import (
    clean_cell_value,
    normalize_priority,
    normalize_status,
    normalize_date_string,
    format_file_size,
    can_preview_in_browser,
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.checklists.documents import (
    build_upload_rel_path,
    remove_item_document_file,
    remove_all_item_documents,
    normalize_document_record,
    normalize_documents_list,
    migrate_legacy_document_fields,
    build_folder_view_url,
    build_document_view_url,
    get_upload_file_path_from_url,
)

from app.checklists.storage import (
    save_checklist,
    get_checklist,
    save_project_storage_context,
    get_project_storage_context,
    get_item_yandex_folder,
    list_checklist_summaries,
    get_project_root_yandex_folder_info,
)

from app.checklists.normalization import (
    build_default_groups,
    resolve_group_id,
    resolve_concept_group_id_by_item_id_or_name,
    resolve_opr_group_id_by_item_id_or_name,
    build_item_id,
    derive_indicator_from_status,
    move_item_to_required_group,
    build_folder_key,
    normalize_checklist_data,
)

from app.checklists.yandex_folders import (
    can_create_custom_item_yandex_folder,
    ensure_yandex_folder_for_custom_item,
    ensure_yandex_folder_for_custom_opr_item,
    mirror_document_to_yandex,
)

from app.checklists.messages import (
    build_recent_changes_sections,
    build_multi_checklist_chat_message,
    build_checklist_chat_message,
)

from app.checklists.locks import (
    acquire_checklist_lock,
    heartbeat_checklist_lock,
    release_checklist_lock,
)

app = FastAPI()

ensure_runtime_directories()

app.mount("/uploads", StaticFiles(directory=str(UPLOAD_ROOT)), name="uploads")


# Страховочный вызов при импорте модуля
init_db()


@app.on_event("startup")
def startup_event():
    init_db()


def normalize_domain(value: str) -> str:
    value = (value or "").strip()
    value = value.replace("https://", "").replace("http://", "").strip("/")
    return value

def normalize_base_path(value: str) -> str:
    value = str(value or "").strip()
    if not value or value == "/":
        return ""
    if not value.startswith("/"):
        value = "/" + value
    return value.rstrip("/")


def get_public_app_base_path(request: Request) -> str:
    if APP_BASE_PATH:
        return normalize_base_path(APP_BASE_PATH)

    forwarded_prefix = request.headers.get("x-forwarded-prefix", "").split(",")[0].strip()
    if forwarded_prefix:
        return normalize_base_path(forwarded_prefix)

    root_path = str(request.scope.get("root_path") or "").strip()
    return normalize_base_path(root_path)


def get_public_origin(request: Request) -> str:
    if PUBLIC_APP_BASE_URL:
        parsed = urlparse(PUBLIC_APP_BASE_URL)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"

    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "https").split(",")[0].strip()
    host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc).split(",")[0].strip()
    return f"{proto}://{host}"


def get_public_app_base_url(request: Request) -> str:
    if PUBLIC_APP_BASE_URL:
        return PUBLIC_APP_BASE_URL.rstrip("/")
    return f"{get_public_origin(request)}{get_public_app_base_path(request)}"

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

def app_home_html(
    initial_dialog_id: str = "",
    initial_checklist_key: str = "id",
    initial_context_text: str = ""
):
    initial_dialog_id_json = json.dumps(
        normalize_dialog_id(initial_dialog_id or ""),
        ensure_ascii=False
    )
    initial_checklist_key_json = json.dumps(
        normalize_checklist_key(initial_checklist_key or "id"),
        ensure_ascii=False
    )
    initial_context_text_json = json.dumps(initial_context_text or "", ensure_ascii=False)

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>Чек-листы проекта</title>
        <script src="https://api.bitrix24.com/api/v1/"></script>
        <script>
            (function () {{
                const initialDialogId = {initial_dialog_id_json};
                const initialChecklistKey = {initial_checklist_key_json};
                const initialContextText = {initial_context_text_json};

                function detectAppBasePath() {{
                    const path = String(window.location.pathname || '/').replace(/\\/+$/, '');
                    const suffixes = ['/launch', '/popup', '/textarea', '/install', '/health', '/debug/logs', '/admin', '/admin/upload'];

                    for (const suffix of suffixes) {{
                        if (path === suffix) return '';
                        if (path.endsWith(suffix)) {{
                            return path.slice(0, -suffix.length) || '';
                        }}
                    }}

                    return path === '/' ? '' : path;
                }}

                const APP_BASE_PATH = detectAppBasePath();

                function appPath(path) {{
                    return (APP_BASE_PATH || '') + '/' + String(path || '').replace(/^\\/+/, '');
                }}

                function pickValue(searchParams, hashParams, key, fallback) {{
                    return (searchParams.get(key) || hashParams.get(key) || fallback || '').trim();
                }}

                function normalizeChecklistKey(value) {{
                    const v = String(value || '').trim().toLowerCase();
                    if (v === 'concept' || v === 'opr' || v === 'id') return v;
                    return 'id';
                }}

                function extractFromBx24() {{
                    let dialogId = '';
                    let checklistKey = '';

                    try {{
                        if (!(window.BX24 && typeof window.BX24.placement === 'object' && typeof window.BX24.placement.info === 'function')) {{
                            return {{ dialogId: '', checklistKey: '' }};
                        }}

                        const info = window.BX24.placement.info() || {{}};
                        const options = info.options || {{}};

                        const dialogCandidates = [
                            options.dialogId,
                            options.DIALOG_ID,
                            options.dialog_id,
                            info.dialogId,
                            info.DIALOG_ID,
                            info.dialog_id,
                            options.chatId,
                            options.CHAT_ID,
                            options.chat_id,
                            info.chatId,
                            info.CHAT_ID,
                            info.chat_id
                        ];

                        for (let i = 0; i < dialogCandidates.length; i++) {{
                            const candidate = String(dialogCandidates[i] || '').trim();
                            if (candidate) {{
                                dialogId = candidate;
                                break;
                            }}
                        }}

                        const checklistCandidates = [
                            options.checklistKey,
                            options.CHECKLIST_KEY,
                            options.checklist_key,
                            info.checklistKey,
                            info.CHECKLIST_KEY,
                            info.checklist_key
                        ];

                        for (let i = 0; i < checklistCandidates.length; i++) {{
                            const candidate = String(checklistCandidates[i] || '').trim();
                            if (candidate) {{
                                checklistKey = normalizeChecklistKey(candidate);
                                break;
                            }}
                        }}

                        try {{
                            console.log('app_home placement.info =', info);
                        }} catch (e) {{}}
                    }} catch (e) {{
                        console.log('app_home extractFromBx24 error:', e);
                    }}

                    return {{
                        dialogId: dialogId,
                        checklistKey: checklistKey || 'id'
                    }};
                }}

                function resizeCurrentPopupFrame() {{
                    try {{
                        if (window.BX24 && typeof window.BX24.resizeWindow === 'function') {{
                            window.BX24.resizeWindow(1180, 720);
                        }}
                        if (window.BX24 && typeof window.BX24.fitWindow === 'function') {{
                            window.BX24.fitWindow();
                        }}
                    }} catch (e) {{
                        console.log('BX24 resize skipped:', e);
                    }}
                }}

                function rememberAndRedirect(dialogId, checklistKey) {{
                    if (!dialogId) return;

                    try {{
                        localStorage.setItem('checklist_pending_dialog', JSON.stringify({{
                            dialogId: dialogId,
                            checklistKey: checklistKey || 'id',
                            ts: Date.now()
                        }}));
                    }} catch (e) {{
                        console.log('pending dialog save skipped:', e);
                    }}

                    const popupUrl =
                        appPath('popup') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(checklistKey || 'id');

                    resizeCurrentPopupFrame();
                    setTimeout(resizeCurrentPopupFrame, 80);

                    setTimeout(function () {{
                        window.location.replace(popupUrl);
                    }}, 120);
                }}

                try {{
                    if (initialContextText) {{
                        console.log('HOME initial context:', initialContextText);
                    }}

                    const searchParams = new URLSearchParams(window.location.search || '');
                    const hashRaw = String(window.location.hash || '').replace(/^#/, '');
                    const hashParams = new URLSearchParams(hashRaw);

                    const raw = localStorage.getItem('checklist_pending_dialog');
                    let localPayload = null;

                    if (raw) {{
                        try {{
                            localPayload = JSON.parse(raw);
                        }} catch (e) {{
                            console.log('pending dialog parse skipped:', e);
                        }}
                    }}

                    const dialogId = pickValue(
                        searchParams,
                        hashParams,
                        'dialogId',
                        initialDialogId || (localPayload && localPayload.dialogId)
                    );

                    const checklistKey = normalizeChecklistKey(
                        pickValue(
                            searchParams,
                            hashParams,
                            'checklistKey',
                            initialChecklistKey || (localPayload && localPayload.checklistKey)
                        ) || 'id'
                    );

                    const ts = Number((localPayload && localPayload.ts) || 0);
                    const age = ts ? (Date.now() - ts) : 0;

                    if (dialogId) {{
                        rememberAndRedirect(dialogId, checklistKey);
                        return;
                    }}

                    if (window.BX24 && typeof window.BX24.init === 'function') {{
                        window.BX24.init(function () {{
                            const bxData = extractFromBx24();
                            if (bxData.dialogId) {{
                                rememberAndRedirect(bxData.dialogId, bxData.checklistKey || checklistKey || 'id');
                                return;
                            }}

                            if (localPayload && localPayload.dialogId && age < 60000) {{
                                rememberAndRedirect(
                                    localPayload.dialogId,
                                    normalizeChecklistKey(localPayload.checklistKey || 'id')
                                );
                            }}
                        }});
                        return;
                    }}

                    if (localPayload && localPayload.dialogId && age < 60000) {{
                        rememberAndRedirect(
                            localPayload.dialogId,
                            normalizeChecklistKey(localPayload.checklistKey || 'id')
                        );
                        return;
                    }}
                }} catch (e) {{
                    console.log('launcher redirect skipped:', e);
                }}
            }})();
        </script>
    </head>
    <body style="margin:0;font-family:Arial,sans-serif;background:#f8fafc;color:#344054;display:flex;align-items:center;justify-content:center;min-height:100vh;">
        <div style="padding:24px 28px;border:1px solid #e5e7eb;border-radius:14px;background:#fff;box-shadow:0 12px 30px rgba(15,23,42,0.08);font-size:14px;">
            Открываем нужный чек-лист...
        </div>
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

            function detectAppBasePath() {{
                const path = String(window.location.pathname || '/').replace(/\\/+$/, '');
                const suffixes = ['/launch', '/popup', '/textarea', '/install', '/health', '/debug/logs', '/admin', '/admin/upload'];

                for (const suffix of suffixes) {{
                    if (path === suffix) return '';
                    if (path.endsWith(suffix)) {{
                        return path.slice(0, -suffix.length) || '';
                    }}
                }}

                return path === '/' ? '' : path;
            }}

            const APP_BASE_PATH = detectAppBasePath();

            function appPath(path) {{
                return (APP_BASE_PATH || '') + '/' + String(path || '').replace(/^\\/+/, '');
            }}
            function setMeta(text) {{
                document.getElementById('meta').textContent = text;
            }}

            function setError(text) {{
                document.getElementById('error').textContent = text || '';
            }}

            function openChecklist(dialogId, checklistKey = 'id') {{
                if (!dialogId) {{
                    setError('dialogId не найден');
                    return;
                }}

                try {{
                    localStorage.setItem('checklist_pending_dialog', JSON.stringify({{
                        dialogId: dialogId,
                        checklistKey: checklistKey,
                        ts: Date.now()
                    }}));
                }} catch (e) {{
                    console.log('localStorage save error:', e);
                }}

                try {{
                    if (window.BX24 && typeof window.BX24.openApplication === 'function') {{
                        BX24.openApplication({{
                            dialogId: dialogId,
                            checklistKey: checklistKey,
                            source: 'textarea'
                        }});
                        autoOpened = true;
                        setMeta('Открываем popup для ' + dialogId);
                        return;
                    }}
                }} catch (e) {{
                    setError('BX24.openApplication error: ' + String(e));
                }}

                window.open(
                    appPath('popup') +
                    '?dialogId=' + encodeURIComponent(dialogId) +
                    '&checklistKey=' + encodeURIComponent(checklistKey),
                    '_blank'
                );
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
                            var info = window.BX24.placement.info() || {{}};
                            var options = info.options || {{}};

                            var candidates = [
                                options.dialogId,
                                options.DIALOG_ID,
                                options.dialog_id,
                                info.dialogId,
                                info.DIALOG_ID,
                                info.dialog_id,
                                options.chatId,
                                options.CHAT_ID,
                                options.chat_id,
                                info.chatId,
                                info.CHAT_ID,
                                info.chat_id
                            ];

                            for (var i = 0; i < candidates.length; i++) {{
                                var candidate = String(candidates[i] || '').trim();
                                if (candidate) {{
                                    dialogId = candidate;
                                    break;
                                }}
                            }}

                            try {{
                                console.log('placement.info =', info);
                            }} catch (e) {{}}
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


@app.get("/", response_class=HTMLResponse)
def home_get(dialogId: str = "", checklistKey: str = "id", mode: str = ""):
    return app_home_html()


@app.post("/", response_class=HTMLResponse)
async def home_post(request: Request):
    form = dict(await request.form())

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
                    "checklistKey", "CHECKLIST_KEY", "checklist_key"
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

    dialog_id = extract_dialog_id_from_form(form)
    checklist_key = extract_checklist_key_from_form(form)
    raw_context = json.dumps(form, ensure_ascii=False, indent=2)

    print("HOME POST FORM:", raw_context)
    print("HOME EXTRACTED DIALOG ID:", dialog_id)
    print("HOME EXTRACTED CHECKLIST KEY:", checklist_key)

    return app_home_html(dialog_id, checklist_key, raw_context)

@app.get("/launch", response_class=HTMLResponse)
def launch_get(dialogId: str = "", checklistKey: str = "id"):
    return app_home_html()


@app.post("/launch", response_class=HTMLResponse)
async def launch_post(request: Request):
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
@app.post("/install/", response_class=HTMLResponse)
async def install_post(request: Request):
    form = dict(await request.form())
    query = dict(request.query_params)
    params = {**query, **form}

    access_token = params.get("AUTH_ID") or params.get("access_token") or ""
    domain = normalize_domain(params.get("DOMAIN") or params.get("domain") or "")
    base_url = get_public_app_base_url(request)

    app_sid = params.get("APP_SID") or ""

    if app_sid and domain and not access_token:
        try:
            auth_response = requests.get(
                f"https://{domain}/rest/app.auth.json",
                params={"app_sid": app_sid},
                timeout=10
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
                "OPTIONS[role]": "ADMIN"
            }
        )

        placement_get_result["all"] = bitrix_rest_call(
            domain,
            "placement.get",
            access_token,
            {}
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

@app.get("/api/project-root-folder")
def api_project_root_folder(dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    context = get_project_storage_context(dialog_id)
    if not context:
        return JSONResponse({"ok": False, "error": "project storage context not found"}, status_code=404)

    yandex_disk = context.get("yandexDisk") or {}
    project_root_path = clean_cell_value(yandex_disk.get("projectRootPath"))
    if not project_root_path:
        return JSONResponse({"ok": False, "error": "projectRootPath is empty"}, status_code=400)

    normalized_root_path = normalize_yandex_disk_path(project_root_path)
    project_root_url = clean_cell_value(yandex_disk.get("projectRootUrl"))

    if project_root_url:
        return JSONResponse({
            "ok": True,
            "path": normalized_root_path,
            "url": project_root_url,
            "fromCache": True,
        })

    if not is_yandex_disk_enabled():
        return JSONResponse({
            "ok": True,
            "path": normalized_root_path,
            "url": "",
            "fromCache": False,
            "yandexDisabled": True,
        })

    try:
        yandex_disk_ensure_folder(normalized_root_path)
        yandex_disk_publish_path(normalized_root_path)
        meta = yandex_disk_get_resource_meta(normalized_root_path)

        project_root_url = clean_cell_value(meta.get("public_url"))

        if project_root_url:
            yandex_disk["projectRootUrl"] = project_root_url

            save_project_storage_context(dialog_id, {
                "dialogId": dialog_id,
                "projectId": context.get("projectId") or "",
                "projectName": context.get("projectName") or "",
                "storageMode": context.get("storageMode") or {},
                "yandexDisk": yandex_disk,
                "itemMappings": context.get("itemMappings") or [],
            })

        return JSONResponse({
            "ok": True,
            "path": clean_cell_value(meta.get("path")) or normalized_root_path,
            "url": project_root_url,
            "fromCache": False,
        })
    except Exception as e:
        return JSONResponse({
            "ok": False,
            "error": "failed to resolve project root folder url",
            "details": str(e),
            "path": normalized_root_path,
            "url": "",
        }, status_code=500)

@app.get("/popup", response_class=HTMLResponse)
def popup_get(dialogId: str = "", checklistKey: str = "id"):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    data = get_checklist(dialog_id, checklist_key)

    title_raw = data.get("title", "Чек-лист ИД")
    title = html.escape(title_raw)
    collab_title_raw = (data.get("collabTitle", "") or "").strip()
    collab_title = html.escape(collab_title_raw)
    full_title = f"{title} — {collab_title}" if collab_title_raw else title
    progress_percent = int(data.get("progressPercent", 0) or 0)

    project_root_folder_info = get_project_root_yandex_folder_info(dialog_id)
    project_root_yandex_path_json = json.dumps(
        clean_cell_value(project_root_folder_info.get("path")),
        ensure_ascii=False
    )
    project_root_yandex_url_json = json.dumps(
        clean_cell_value(project_root_folder_info.get("url")),
        ensure_ascii=False
    )

    items_json = json.dumps(data.get("items", []), ensure_ascii=False)
    groups_json = json.dumps(data.get("groups", []), ensure_ascii=False)
    project_checklists_json = json.dumps(data.get("projectChecklists", []), ensure_ascii=False)
    dialog_id_json = json.dumps(dialog_id, ensure_ascii=False)
    collab_title_json = json.dumps(collab_title_raw, ensure_ascii=False)
    checklist_key_json = json.dumps(checklist_key, ensure_ascii=False)
    checklist_title_json = json.dumps(title_raw, ensure_ascii=False)
    popup_session_enhancements_js = """
            const clientSessionId = 'popup_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
            let checklistSessionState = {};
            let currentChecklistLock = {
                owned: false,
                lockedByOther: false,
                lockId: '',
                userId: '',
                userName: '',
                checklistKey: ''
            };
            let lockHeartbeatTimer = null;
            let suppressAutoCloseSave = false;
            let activeInlineEditor = { role: '', itemId: '' };

            const headerRightEl = document.querySelector('.header-right');
            if (headerRightEl && !document.getElementById('lockNotice')) {
                const lockNode = document.createElement('div');
                lockNode.id = 'lockNotice';
                lockNode.style.fontSize = '12px';
                lockNode.style.fontWeight = '700';
                lockNode.style.padding = '7px 10px';
                lockNode.style.borderRadius = '999px';
                lockNode.style.background = '#fff4e5';
                lockNode.style.color = '#b26a00';
                lockNode.style.whiteSpace = 'nowrap';
                lockNode.style.display = 'none';
                headerRightEl.insertBefore(lockNode, saveStateEl);
            }
            const lockNoticeEl = document.getElementById('lockNotice');

            const contentEl = document.querySelector('.content');
            if (contentEl && !document.getElementById('footerActions')) {
                const footerNode = document.createElement('div');
                footerNode.id = 'footerActions';
                footerNode.style.position = 'sticky';
                footerNode.style.bottom = '0';
                footerNode.style.zIndex = '30';
                footerNode.style.marginTop = '14px';
                footerNode.style.padding = '10px 12px';
                footerNode.style.display = 'flex';
                footerNode.style.gap = '10px';
                footerNode.style.justifyContent = 'flex-end';
                footerNode.style.alignItems = 'center';
                footerNode.style.border = '1px solid #e5e7eb';
                footerNode.style.borderRadius = '12px';
                footerNode.style.background = '#fafbfc';
                footerNode.style.boxShadow = '0 -4px 18px rgba(15,23,42,.06)';
                footerNode.innerHTML = `
                    <button id="saveCloseBtn" type="button" style="min-width:150px;height:34px;border:none;border-radius:8px;background:#16a34a;color:#fff;font-size:12px;font-weight:700;cursor:pointer;">
                        Сохранить и закрыть
                    </button>
                    <button id="cancelBtn" type="button" style="min-width:100px;height:34px;border:none;border-radius:8px;background:#dc2626;color:#fff;font-size:12px;font-weight:700;cursor:pointer;">
                        Отмена
                    </button>
                `;
                contentEl.appendChild(footerNode);
            }
            const saveCloseBtn = document.getElementById('saveCloseBtn');
            const cancelBtn = document.getElementById('cancelBtn');

            function getChecklistDefaultTitle(key) {
                if (key === 'concept') return 'Чек-лист Концепция';
                if (key === 'opr') return 'Чек-лист ОПР';
                return 'Чек-лист ИД';
            }

            function getChecklistState(key) {
                const stateKey = String(key || '').trim() || 'id';
                if (!checklistSessionState[stateKey]) {
                    checklistSessionState[stateKey] = { changes: [], dirty: false };
                }
                return checklistSessionState[stateKey];
            }

            function getCurrentEditorIdentity() {
                return {
                    userId: String(currentEditor.id || clientSessionId),
                    userName: String(currentEditor.name || 'Пользователь')
                };
            }

            async function ensureCurrentEditorReady() {
                await fetchCurrentUserIfPossible();
                return getCurrentEditorIdentity();
            }

            function isChecklistLockedByOther() {
                return !!(currentChecklistLock.lockedByOther && currentChecklistLock.checklistKey === currentChecklistKey);
            }

            function isEditingAllowed() {
                return !isChecklistLockedByOther();
            }

            function disabledAttr() {
                return isEditingAllowed() ? '' : 'disabled';
            }

            function setActionButtonState() {
                if (saveCloseBtn) {
                    saveCloseBtn.disabled = !isEditingAllowed();
                    saveCloseBtn.style.opacity = saveCloseBtn.disabled ? '0.55' : '1';
                    saveCloseBtn.style.cursor = saveCloseBtn.disabled ? 'not-allowed' : 'pointer';
                }
                if (cancelBtn) {
                    cancelBtn.disabled = false;
                }
            }

            function updateSaveStateBySession() {
                if (isChecklistLockedByOther()) {
                    setSaveState('error', 'Только просмотр');
                    return;
                }
                if (sessionDirty) {
                    setSaveState('saving', 'Есть несохраненные изменения');
                    return;
                }
                setSaveState('', 'Сохранено');
            }

            function isDocumentDeletionChange(change) {
                if (!change || String(change.field || '') !== 'document') {
                    return false;
                }

                const newValue = String(change.newValue || '').trim().toLowerCase();
                return newValue === 'удален' || newValue === 'удалён' || newValue === 'removed';
            }

            function sessionStateHasDocumentDeletion(state) {
                const changes = Array.isArray(state && state.changes) ? state.changes : [];
                return changes.some(isDocumentDeletionChange);
            }

            function hasDocumentDeletionInAnyDirtyChecklist() {
                syncChecklistCache();
                const dirtyKeys = getDirtyChecklistKeys();
                return dirtyKeys.some(key => sessionStateHasDocumentDeletion(checklistSessionState[key]));
            }

            function updateLockNotice() {
                if (!lockNoticeEl) return;
                if (isChecklistLockedByOther()) {
                    const ownerName = currentChecklistLock.userName || 'другой сотрудник';
                    lockNoticeEl.textContent = 'Сейчас с этим чек-листом работает ' + ownerName + ', дождитесь завершения сессии';
                    lockNoticeEl.style.display = '';
                } else {
                    lockNoticeEl.textContent = '';
                    lockNoticeEl.style.display = 'none';
                }
                setActionButtonState();
                updateSaveStateBySession();
            }

            const previousSyncChecklistCache = syncChecklistCache;
            syncChecklistCache = function () {
                checklistCache[currentChecklistKey] = buildChecklistSnapshot();
                checklistSessionState[currentChecklistKey] = {
                    changes: deepClone(sessionChanges),
                    dirty: !!sessionDirty
                };
                if (typeof previousSyncChecklistCache === 'function') {
                    previousSyncChecklistCache();
                }
            };

            const previousApplyChecklistData = applyChecklistData;
            applyChecklistData = function (data) {
                previousApplyChecklistData(data);
                checklistTitle = String((data && data.title) || checklistTitle || getChecklistDefaultTitle(currentChecklistKey));
                const cachedState = getChecklistState(currentChecklistKey);
                sessionChanges = deepClone(cachedState.changes || []);
                sessionDirty = !!cachedState.dirty;
                closeSummarySent = false;
                activeInlineEditor = { role: '', itemId: '' };
                currentChecklistLock.checklistKey = currentChecklistKey;
                updateLockNotice();
            };

            function clearChecklistSessionState(checklistKey) {
                const targetKey = String(checklistKey || '').trim() || 'id';
                checklistSessionState[targetKey] = { changes: [], dirty: false };
                if (targetKey === currentChecklistKey) {
                    sessionChanges = [];
                    sessionDirty = false;
                    closeSummarySent = false;
                    updateLockNotice();
                }
            }

            function getDirtyChecklistKeys() {
                syncChecklistCache();
                return Object.keys(checklistSessionState).filter(key => {
                    const state = checklistSessionState[key];
                    return !!(state && state.dirty && Array.isArray(state.changes) && state.changes.length && checklistCache[key]);
                });
            }

            function buildClosePayloadForKey(checklistKey, closeEvent) {
                const key = String(checklistKey || '').trim() || 'id';
                const snapshot = key === currentChecklistKey
                    ? buildChecklistSnapshot()
                    : deepClone(checklistCache[key] || {});
                const state = key === currentChecklistKey
                    ? { changes: deepClone(sessionChanges), dirty: !!sessionDirty }
                    : deepClone(checklistSessionState[key] || { changes: [], dirty: false });

                return {
                    dialogId,
                    checklistKey: key,
                    editor: currentEditor,
                    data: snapshot,
                    changes: state.changes || [],
                    closeEvent,
                    ts: new Date().toISOString()
                };
            }

            function buildCloseBatchPayload(closeEvent) {
                const dirtyKeys = getDirtyChecklistKeys();
                return {
                    dialogId,
                    editor: currentEditor,
                    closeEvent,
                    ts: new Date().toISOString(),
                    sessions: dirtyKeys.map(key => buildClosePayloadForKey(key, closeEvent))
                };
            }

            async function persistDirtyChecklists(closeEvent, useBeacon = false) {
                const dirtyKeys = getDirtyChecklistKeys();
                if (!dirtyKeys.length) {
                    return {
                        savedCount: 0,
                        messageOk: true,
                        messageSkipped: true,
                        result: {}
                    };
                }

                const payload = buildCloseBatchPayload(closeEvent);

                if (useBeacon && navigator.sendBeacon) {
                    const blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
                    navigator.sendBeacon(APP_BASE_URL + '/api/checklist/close-session', blob);
                    dirtyKeys.forEach(clearChecklistSessionState);
                    return {
                        savedCount: dirtyKeys.length,
                        messageOk: true,
                        messageSkipped: false,
                        result: {}
                    };
                }

                const response = await fetch(appUrl('api/checklist/close-session'), {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(payload),
                    keepalive: true
                });
                const result = await response.json().catch(() => ({}));
                if (!response.ok) {
                    throw new Error(result.error || 'close-session failed');
                }

                if (result && result.messageOk === false) {
                    debugLog('close_session_message_warning', {
                        closeEvent,
                        dialogId,
                        checklistKeys: dirtyKeys,
                        result
                    });
                } else {
                    debugLog('close_session_completed', {
                        closeEvent,
                        dialogId,
                        checklistKeys: dirtyKeys,
                        result
                    });
                }

                dirtyKeys.forEach(clearChecklistSessionState);
                return {
                    savedCount: dirtyKeys.length,
                    messageOk: result.messageOk !== false,
                    messageSkipped: !!result.messageSkipped,
                    result
                };
            }

            function stopLockHeartbeat() {
                if (lockHeartbeatTimer) {
                    clearInterval(lockHeartbeatTimer);
                    lockHeartbeatTimer = null;
                }
            }

            async function acquireChecklistLock(checklistKey = currentChecklistKey, silent = false) {
                const targetKey = String(checklistKey || '').trim() || 'id';
                const editorIdentity = getCurrentEditorIdentity();
                const payload = {
                    dialogId,
                    checklistKey: targetKey,
                    userId: editorIdentity.userId,
                    userName: editorIdentity.userName,
                    lockId: (currentChecklistLock.checklistKey === targetKey && currentChecklistLock.owned)
                        ? currentChecklistLock.lockId
                        : ''
                };

                try {
                    const response = await fetch(appUrl('api/checklist/lock/acquire'), {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify(payload),
                        keepalive: true
                    });
                    const result = await response.json();
                    if (!response.ok) {
                        throw new Error(result.error || 'lock acquire failed');
                    }

                    if (targetKey !== currentChecklistKey) {
                        return result;
                    }

                    const prevOwned = currentChecklistLock.owned;
                    const prevBlocked = currentChecklistLock.lockedByOther;
                    const prevLockId = currentChecklistLock.lockId;
                    const nextOwned = !!result.owned;

                    currentChecklistLock = {
                        owned: nextOwned,
                        lockedByOther: !!result.lockedByOther,
                        lockId: nextOwned ? String(result.lockId || '') : '',
                        userId: String(result.userId || ''),
                        userName: String(result.userName || ''),
                        checklistKey: targetKey
                    };
                    updateLockNotice();

                    if (prevOwned !== currentChecklistLock.owned || prevBlocked !== currentChecklistLock.lockedByOther || prevLockId !== currentChecklistLock.lockId) {
                        renderAll();
                    }
                    return result;
                } catch (e) {
                    if (!silent) {
                        console.log('acquireChecklistLock error:', e);
                    }
                    return null;
                }
            }

            async function heartbeatChecklistLock(silent = true) {
                if (!currentChecklistLock.owned || !currentChecklistLock.lockId || currentChecklistLock.checklistKey !== currentChecklistKey) {
                    return null;
                }

                const editorIdentity = getCurrentEditorIdentity();
                try {
                    const response = await fetch(appUrl('api/checklist/lock/heartbeat'), {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            dialogId,
                            checklistKey: currentChecklistKey,
                            userId: editorIdentity.userId,
                            userName: editorIdentity.userName,
                            lockId: currentChecklistLock.lockId
                        }),
                        keepalive: true
                    });
                    const result = await response.json();
                    if (!response.ok) {
                        throw new Error(result.error || 'lock heartbeat failed');
                    }

                    const nextOwned = !!result.owned;
                    currentChecklistLock = {
                        owned: nextOwned,
                        lockedByOther: !!result.lockedByOther,
                        lockId: nextOwned ? String(result.lockId || '') : '',
                        userId: String(result.userId || ''),
                        userName: String(result.userName || ''),
                        checklistKey: currentChecklistKey
                    };
                    updateLockNotice();
                    return result;
                } catch (e) {
                    if (!silent) {
                        console.log('heartbeatChecklistLock error:', e);
                    }
                    return null;
                }
            }

            function startLockHeartbeat() {
                stopLockHeartbeat();
                lockHeartbeatTimer = setInterval(async function () {
                    if (!currentEditorReady) {
                        return;
                    }

                    if (currentChecklistLock.owned) {
                        await heartbeatChecklistLock(true);
                    } else {
                        await acquireChecklistLock(currentChecklistKey, true);
                    }
                }, 15000);
            }

            async function releaseChecklistLock(checklistKey = currentChecklistKey, useBeacon = false) {
                const targetKey = String(checklistKey || '').trim() || 'id';
                const isOwner = currentChecklistLock.checklistKey === targetKey && currentChecklistLock.owned;
                const lockId = isOwner ? currentChecklistLock.lockId : '';
                const editorIdentity = getCurrentEditorIdentity();
                stopLockHeartbeat();

                if (!isOwner || !lockId) {
                    if (currentChecklistLock.checklistKey === targetKey) {
                        currentChecklistLock = {
                            owned: false,
                            lockedByOther: false,
                            lockId: '',
                            userId: '',
                            userName: '',
                            checklistKey: targetKey
                        };
                        updateLockNotice();
                    }
                    return;
                }

                const payload = {
                    dialogId,
                    checklistKey: targetKey,
                    lockId,
                    userId: editorIdentity.userId
                };

                if (useBeacon && navigator.sendBeacon) {
                    const blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
                    navigator.sendBeacon(APP_BASE_URL + '/api/checklist/lock/release', blob);
                } else {
                    try {
                        await fetch(appUrl('api/checklist/lock/release'), {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify(payload),
                            keepalive: true
                        });
                    } catch (e) {
                        console.log('releaseChecklistLock error:', e);
                    }
                }

                if (currentChecklistLock.checklistKey === targetKey) {
                    currentChecklistLock = {
                        owned: false,
                        lockedByOther: false,
                        lockId: '',
                        userId: '',
                        userName: '',
                        checklistKey: targetKey
                    };
                    updateLockNotice();
                }
            }

            function closePopupWindow() {
                try {
                    if (window.BX24 && typeof window.BX24.closeApplication === 'function') {
                        window.BX24.closeApplication();
                        return;
                    }
                } catch (e) {
                    console.log('BX24.closeApplication error:', e);
                }

                try {
                    window.close();
                } catch (e) {
                    console.log('window.close error:', e);
                }
            }

            async function finalizePopupSession(saveChanges) {
                suppressAutoCloseSave = true;
                closeSummarySent = true;
                syncChecklistCache();

                let persistResult = {
                    savedCount: 0,
                    messageOk: true,
                    messageSkipped: true,
                    result: {}
                };

                try {
                    if (saveChanges) {
                        persistResult = await persistDirtyChecklists('save_and_close', false);
                    } else if (hasDocumentDeletionInAnyDirtyChecklist()) {
                        persistResult = await persistDirtyChecklists('cancel_with_document_deletions', false);
                    }
                } catch (e) {
                    console.log('finalizePopupSession error:', e);
                    setSaveState('error', saveChanges ? 'Ошибка сохранения' : 'Ошибка отмены');
                    return;
                }

                if (saveChanges) {
                    if (persistResult.messageOk === false) {
                        setSaveState('saving', 'Сохранено, сообщение не отправлено');
                    } else if (persistResult.savedCount > 0) {
                        setSaveState('', 'Сохранено');
                    }
                }

                await releaseChecklistLock(currentChecklistKey, false);
                closePopupWindow();
            }

            if (saveCloseBtn) {
                saveCloseBtn.addEventListener('click', async function () {
                    await finalizePopupSession(true);
                });
            }

            if (cancelBtn) {
                cancelBtn.addEventListener('click', async function () {
                    await finalizePopupSession(false);
                });
            }

            sendCloseSummaryOnce = function (eventName) {
                if (eventName === 'popup_hidden' || suppressAutoCloseSave || closeSummarySent) {
                    return;
                }

                closeSummarySent = true;
                persistDirtyChecklists(eventName, true);
                releaseChecklistLock(currentChecklistKey, true);
            };

            loadChecklistByKey = async function (checklistKey) {
                const targetKey = String(checklistKey || '').trim() || 'id';
                if (targetKey === currentChecklistKey) {
                    return;
                }

                await ensureCurrentEditorReady();
                syncChecklistCache();
                await releaseChecklistLock(currentChecklistKey, false);
                setSaveState('saving', 'Загружаем...');

                try {
                    const cachedData = checklistCache[targetKey];
                    if (cachedData) {
                        applyChecklistData(deepClone(cachedData));
                        renderAll();
                        await acquireChecklistLock(targetKey, true);
                        startLockHeartbeat();
                        return;
                    }

                    const response = await fetch(
                        appUrl('api/checklist') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(targetKey)
                    );
                    const result = await response.json();
                    if (!response.ok) {
                        throw new Error(result.error || 'load checklist failed');
                    }

                    applyChecklistData(result);
                    renderAll();
                    await acquireChecklistLock(targetKey, true);
                    startLockHeartbeat();
                } catch (e) {
                    console.log('loadChecklistByKey enhanced error:', e);
                    setSaveState('error', 'Ошибка загрузки чек-листа');
                }
            };

            buildDocumentCell = function (item) {
                if (normalizeStatus(item && item.status) === 'Не требуется') {
                    return '';
                }
                const documents = getItemDocuments(item);
                const itemId = String(item && item.id || '');
                const folderViewUrl = String(item.folderUrl || '').trim() || (documents.length ? (
                    appUrl('api/checklist/folder') +
                    '?dialogId=' + encodeURIComponent(dialogId) +
                    '&checklistKey=' + encodeURIComponent(currentChecklistKey) +
                    '&itemId=' + encodeURIComponent(itemId)
                ) : '');
                const showViewFolder = documents.length > 0 && !!folderViewUrl;

                const filesHtml = documents.map(doc => {
                    const docId = String(doc.id || '');
                    const docName = String(doc.name || 'Файл');
                    const openUrl = appUrl('api/checklist/file') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(currentChecklistKey) +
                        '&itemId=' + encodeURIComponent(itemId) +
                        '&documentId=' + encodeURIComponent(docId);
                    const sizeText = formatFileSize(doc.size || 0);

                    return `
                        <div class="doc-file-row">
                            <a
                                href="javascript:void(0)"
                                class="doc-file-link"
                                data-role="view-file"
                                data-item-id="${esc(itemId)}"
                                data-document-id="${esc(docId)}"
                                data-open-url="${esc(openUrl)}"
                                title="${esc(docName)}"
                            >
                                ${esc(docName)}
                            </a>
                            ${sizeText ? `<span class="doc-file-meta">${esc(sizeText)}</span>` : ''}
                        </div>
                    `;
                }).join('');

                return `
                    <div class="doc-cell">
                        <div class="doc-actions">
                            <button
                                class="upload-btn"
                                type="button"
                                data-role="upload"
                                data-item-id="${esc(itemId)}"
                                ${typeof disabledAttr === 'function' ? disabledAttr() : ''}
                            >
                                Загрузить
                            </button>

                            ${showViewFolder ? `
                                <button
                                    class="doc-btn"
                                    type="button"
                                    data-role="view-folder"
                                    data-item-id="${esc(itemId)}"
                                    data-folder-url="${esc(folderViewUrl)}"
                                >
                                    Посмотреть
                                </button>
                            ` : ''}
                        </div>

                        ${documents.length ? `
                            <div class="doc-files">
                                ${filesHtml}
                            </div>
                        ` : ''}

                        <input
                            type="file"
                            data-role="file-input"
                            data-item-id="${esc(itemId)}"
                            style="display:none;"
                            multiple
                            ${typeof disabledAttr === 'function' ? disabledAttr() : ''}
                        >
                    </div>
                `;
            };

            renderGroup = function (group) {
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 4;
                const rows = groupItems.map(item => {
                    const rowClass = normalizeStatus(item.status) === 'Не требуется' ? 'row not-required' : 'row';
                    return `
                        <div class="${rowClass}" data-item-id="${esc(item.id)}">
                            <div class="td"><div class="cell-name"><div class="${indicatorClass(item.status)}"></div><div class="item-name">${esc(item.name)}</div></div></div>
                            <div class="td">${buildDocumentCell(item)}</div>
                            <div class="td">
                                <select class="status-select" data-role="status" data-item-id="${esc(item.id)}" ${disabledAttr()}>
                                    <option value="" ${normalizeStatus(item.status) === '' ? 'selected' : ''}></option>
                                    <option value="Есть" ${normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}>Есть</option>
                                    <option value="Нет" ${normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}>Нет</option>
                                    <option value="Не требуется" ${normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}>Не требуется</option>
                                </select>
                            </div>
                            <div class="td"><input class="date-input" type="date" data-role="plan" data-item-id="${esc(item.id)}" value="${esc(toInputDate(item.plan))}" ${disabledAttr()}></div>
                            <div class="td"><input class="date-input" type="date" data-role="fact" data-item-id="${esc(item.id)}" value="${esc(toInputDate(item.fact))}" ${disabledAttr()}></div>
                        </div>
                    `;
                }).join('');
                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="addItemInput_${group.id}" type="text" placeholder="Новый пункт" ${disabledAttr()}>
                        <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${group.id}" ${disabledAttr()}>Добавить пункт</button>
                    </div>` : '';
                return `<div class="group-block"><div class="group-title">${esc(group.title)}</div>${rows}${addBlock}</div>`;
            };

            function getInlineEditorDomId(role, itemId) {
                return 'inline_' + String(role || '').replace(/[^a-z0-9_-]/ig, '_') + '_' + String(itemId || '').replace(/[^a-z0-9_-]/ig, '_');
            }

            function isInlineEditorActive(role, itemId) {
                return activeInlineEditor.role === role && String(activeInlineEditor.itemId || '') === String(itemId || '');
            }

            function startInlineEditor(role, itemId) {
                if (!isEditingAllowed()) {
                    return;
                }
                activeInlineEditor = {
                    role,
                    itemId: String(itemId || '')
                };
                renderAll();
            }

            function stopInlineEditor() {
                activeInlineEditor = { role: '', itemId: '' };
            }

            function focusActiveInlineEditor() {
                if (!activeInlineEditor.role || !activeInlineEditor.itemId) {
                    return;
                }
                const el = document.getElementById(getInlineEditorDomId(activeInlineEditor.role, activeInlineEditor.itemId));
                if (!el) {
                    return;
                }
                if (typeof el.focus === 'function') {
                    el.focus();
                }
                if (typeof el.select === 'function') {
                    el.select();
                }
                if (el.tagName === 'TEXTAREA') {
                    el.dataset.initialValue = String(el.value || '');
                    el.dataset.baseHeight = '32';
                    autoGrowTextarea(el);
                }
            }

            function buildConceptInlineCell(role, item, value, placeholder, extraStyle = '') {
                const itemId = String(item.id || '');
                const isActive = isInlineEditorActive(role, itemId) && isEditingAllowed();
                const displayValue = String(value || '').trim();
                const content = displayValue || String(placeholder || '').trim() || '';
                const displayClass = 'concept-inline-display' + (displayValue ? '' : ' empty') + (isEditingAllowed() ? '' : ' disabled');
                const styleAttr = extraStyle ? ` style="${extraStyle}"` : '';

                if (isActive) {
                    return `
                        <textarea
                            id="${esc(getInlineEditorDomId(role, itemId))}"
                            class="concept-inline-input"
                            data-role="${esc(role + '-edit')}"
                            data-item-id="${esc(itemId)}"
                            data-initial-value="${esc(value || '')}"
                            placeholder="${esc(placeholder || '')}"
                            ${disabledAttr()}
                        >${esc(value || '')}</textarea>
                    `;
                }

                return `
                    <div
                        class="${displayClass}"
                        data-role="${esc(role + '-display')}"
                        data-item-id="${esc(itemId)}"
                        tabindex="${isEditingAllowed() ? '0' : '-1'}"
                        ${styleAttr}
                    >${esc(content) || '&nbsp;'}</div>
                `;
            }

            function buildConceptNameCell(item) {
                const textStyle = item.status === 'Не требуется' ? 'text-decoration:line-through;color:#98a2b3;' : '';
                return buildConceptInlineCell('concept-name', item, item.name || '', 'Название пункта', textStyle);
            }

            function buildConceptSourceCell(item) {
                return buildConceptInlineCell('concept-source', item, item.source || '', 'Нормативы');
            }

            buildConceptStatusCell = function (item) {
                if (item.statusKind === 'bool') {
                    return `
                        <select class="status-select" data-role="concept-status" data-item-id="${esc(item.id)}" ${disabledAttr()}>
                            <option value="" ${item.status === '' ? 'selected' : ''}></option>
                            <option value="Да" ${item.status === 'Да' ? 'selected' : ''}>Да</option>
                            <option value="Нет" ${item.status === 'Нет' ? 'selected' : ''}>Нет</option>
                            <option value="Не требуется" ${item.status === 'Не требуется' ? 'selected' : ''}>Не требуется</option>
                        </select>
                    `;
                }
                if (item.statusKind === 'select') {
                    const options = [''].concat(item.statusOptions || [], ['Не требуется']);
                    return `
                        <select class="status-select" data-role="concept-status" data-item-id="${esc(item.id)}" ${disabledAttr()}>
                            ${options.map(option => `<option value="${esc(option)}" ${item.status === option ? 'selected' : ''}>${esc(option)}</option>`).join('')}
                        </select>
                    `;
                }
                return `<input class="status-select" type="text" data-role="concept-status" data-item-id="${esc(item.id)}" placeholder="${esc(item.statusPlaceholder || '')}" value="${esc(item.status || '')}" ${disabledAttr()}>`;
            };

            buildConceptExtraCell = function (item) {
                return `<textarea class="concept-extra-textarea" data-role="concept-extra" data-item-id="${esc(item.id)}" placeholder="${esc(item.extraInfoPlaceholder || '')}" ${disabledAttr()}>${esc(item.extraInfo || '')}</textarea>`;
            };

            renderConceptGroup = function (group) {
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 10;
                const rows = groupItems.map(item => `
                    <div class="row" style="grid-template-columns: 1.05fr 0.66fr 190px 150px 1.15fr;" data-item-id="${esc(item.id)}">
                        <div class="td"><div class="cell-name"><div class="${conceptIndicatorClass(item)}"></div>${buildConceptNameCell(item)}</div></div>
                        <div class="td">${buildConceptSourceCell(item)}</div>
                        <div class="td">${buildDocumentCell(item)}</div>
                        <div class="td">${buildConceptStatusCell(item)}</div>
                        <div class="td">${buildConceptExtraCell(item)}</div>
                    </div>
                `).join('');

                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="conceptAddItemInput_${group.id}" type="text" placeholder="Новый пункт" ${disabledAttr()}>
                        <button class="add-item-btn" type="button" data-role="concept-add-item" data-group-id="${group.id}" ${disabledAttr()}>Добавить пункт</button>
                    </div>
                ` : '';

                return `<div class="group-block"><div class="group-title">${esc(group.title)}</div>${rows}${addBlock}</div>`;
            };

            function buildConceptTableHtmlEnhanced(conceptGroups) {
                return `
                    <div class="thead">
                        <div class="thead-top" style="grid-template-columns: 1.05fr 0.66fr 190px 150px 1.15fr;">
                            <div class="th">Пункт</div>
                            <div class="th">Нормативы</div>
                            <div class="th">Документ</div>
                            <div class="th">Статус</div>
                            <div class="th">Доп информация</div>
                        </div>
                    </div>
                    <div>${conceptGroups.map(renderConceptGroup).join('')}</div>
                `;
            }

            renderConceptTable = function () {
                if (!leftTableEl || !rightTableEl || !tablesGridEl) {
                    throw new Error('concept table containers not found');
                }

                tablesGridEl.style.gridTemplateColumns = '1fr 1fr';
                if (tablePanels[1]) {
                    tablePanels[1].style.display = '';
                }

                const visibleGroups = groups.filter(group => {
                    if (Number(group.id) !== 10) return true;
                    return items.some(x => Number(x.group) === 10);
                });

                const leftGroups = visibleGroups.filter(group => [1, 3, 5, 7, 9].includes(Number(group.id)));
                const rightGroups = visibleGroups.filter(group => [2, 4, 6, 8, 10].includes(Number(group.id)));

                leftTableEl.innerHTML = buildConceptTableHtmlEnhanced(leftGroups);
                rightTableEl.innerHTML = buildConceptTableHtmlEnhanced(rightGroups);
            };

            renderProjectChecklistList = function () {
                if (!projectChecklistListEl) {
                    return;
                }

                const normalizedList = (Array.isArray(projectChecklists) ? projectChecklists : []).map(item => {
                    const key = String(item && item.key || '').trim();
                    return {
                        key,
                        title: getChecklistDefaultTitle(key)
                    };
                });

                projectChecklistListEl.innerHTML = normalizedList.map(item => {
                    const active = item.key === currentChecklistKey ? 'side-link active' : 'side-link';
                    return `<button type="button" class="${active}" data-checklist-key="${esc(item.key)}">${esc(item.title)}</button>`;
                }).join('');

                projectChecklistListEl.querySelectorAll('[data-checklist-key]').forEach(btn => {
                    btn.addEventListener('click', async function () {
                        const key = this.dataset.checklistKey;
                        await loadChecklistByKey(key);
                    });
                });
            };

            oprIndicatorClass = function (item) {
                const status = normalizeStatus(item && item.status);
                if (status === 'Есть') return 'status-indicator green';
                if (status === 'Нет' || status === 'Не требуется') return 'status-indicator gray';
                return 'status-indicator';
            };

            function resolveOprGroupIdByItemIdOrName(item) {
                const itemId = String(item && item.id || '');
                if (itemId.startsWith('opr_g')) {
                    const match = itemId.match(/^opr_g(\\d+)_/);
                    if (match) {
                        const groupId = Number(match[1]);
                        if (groupId && groupId !== 2) {
                            return groupId;
                        }
                    }
                }

                const name = String(item && item.name || '').trim();
                const matchedGroup = (Array.isArray(groups) ? groups : []).find(group => {
                    const gid = Number(group && group.id);
                    if (gid === 2) return false;

                    return Array.isArray(items) && items.some(existing =>
                        existing !== item &&
                        Number(existing.group) === gid &&
                        String(existing.name || '').trim() === name
                    );
                });

                return matchedGroup ? Number(matchedGroup.id) : 1;
            }

            function buildOprDatesToggle() {
                const active = !!oprDateVisibility[1];
                return `
                    <button
                        type="button"
                        class="id-dates-toggle"
                        data-role="opr-toggle-dates"
                        title="${active ? 'Скрыть даты' : 'Показать даты'}"
                        aria-label="${active ? 'Скрыть даты' : 'Показать даты'}"
                        ${disabledAttr()}
                    >
                        📅
                    </button>
                `;
            }

            function buildOprStatusCellUi(item) {
                return `
                    <select class="status-select" data-role="opr-status" data-item-id="${esc(item.id)}" ${disabledAttr()}>
                        <option value="" ${normalizeStatus(item.status) === '' ? 'selected' : ''}></option>
                        <option value="Есть" ${normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}>Есть</option>
                        <option value="Нет" ${normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}>Нет</option>
                        <option value="Не требуется" ${normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}>Не требуется</option>
                    </select>
                `;
            }

            function renderOprGroupUi(group) {
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 2;
                const showDates = !!oprDateVisibility[1];
                const gridClass = showDates ? 'id-grid id-grid-expanded' : 'id-grid id-grid-compact';

                const rows = groupItems.map(item => {
                    const rowClass = normalizeStatus(item.status) === 'Не требуется'
                        ? `row not-required ${gridClass}`
                        : `row ${gridClass}`;

                    return `
                        <div class="${rowClass}" data-item-id="${esc(item.id)}">
                            <div class="td">
                                <div class="cell-name">
                                    <div class="${oprIndicatorClass(item)}"></div>
                                    <div class="item-name">${esc(item.name)}</div>
                                </div>
                            </div>
                            <div class="td">${buildDocumentCell(item)}</div>
                            <div class="td">${buildOprStatusCellUi(item)}</div>
                            ${showDates ? `
                                <div class="td">
                                    <input class="date-input" type="date" data-role="opr-plan" data-item-id="${esc(item.id)}" value="${esc(toInputDate(item.plan || ''))}" ${disabledAttr()}>
                                </div>
                                <div class="td">
                                    <input class="date-input" type="date" data-role="opr-fact" data-item-id="${esc(item.id)}" value="${esc(toInputDate(item.fact || ''))}" ${disabledAttr()}>
                                </div>
                            ` : ''}
                        </div>
                    `;
                }).join('');

                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="oprAddItemInput_${group.id}" type="text" placeholder="Новый пункт" ${disabledAttr()}>
                        <button class="add-item-btn" type="button" data-role="opr-add-item" data-group-id="${group.id}" ${disabledAttr()}>Добавить пункт</button>
                    </div>
                ` : '';

                return `<div class="group-block"><div class="group-title">${esc(group.title)}</div>${rows}${addBlock}</div>`;
            }

            renderOprTables = function () {
                if (!leftTableEl || !rightTableEl || !tablesGridEl) {
                    throw new Error('opr table containers not found');
                }

                const showDates = !!oprDateVisibility[1];
                const gridClass = showDates ? 'id-grid id-grid-expanded' : 'id-grid id-grid-compact';

                tablesGridEl.style.gridTemplateColumns = 'clamp(620px, 37vw, 760px)';
                tablesGridEl.style.justifyContent = 'start';

                if (tablePanels[0]) {
                    tablePanels[0].style.display = '';
                    tablePanels[0].style.flex = '0 0 auto';
                    tablePanels[0].style.width = 'clamp(620px, 37vw, 760px)';
                    tablePanels[0].style.maxWidth = 'clamp(620px, 37vw, 760px)';
                }
                if (tablePanels[1]) {
                    tablePanels[1].style.display = 'none';
                    tablePanels[1].style.flex = '';
                    tablePanels[1].style.maxWidth = '';
                }
                if (tablePanels[2]) {
                    tablePanels[2].style.display = 'none';
                    tablePanels[2].style.flex = '';
                    tablePanels[2].style.maxWidth = '';
                }

                leftTableEl.style.width = '100%';
                leftTableEl.style.maxWidth = '100%';

                const visibleGroups = groups.filter(group => {
                    if (Number(group.id) !== 2) return true;
                    return items.some(x => Number(x.group) === 2);
                });

                leftTableEl.classList.add('id-table');
                leftTableEl.innerHTML = `
                    <div class="thead">
                        <div class="thead-top ${gridClass}">
                            <div class="th">ОПР</div>
                            <div class="th">Документ</div>
                            <div class="th">
                                <div class="th-status-with-toggle">
                                    <span>Статус</span>
                                    ${buildOprDatesToggle()}
                                </div>
                            </div>
                            ${showDates ? `<div class="th center" style="grid-column: 4 / span 2;">Даты</div>` : ''}
                        </div>
                        ${showDates ? `
                            <div class="thead-bottom ${gridClass}">
                                <div class="th"></div>
                                <div class="th"></div>
                                <div class="th"></div>
                                <div class="th">План</div>
                                <div class="th">Факт</div>
                            </div>
                        ` : ''}
                    </div>
                    <div>
                        ${visibleGroups.map(renderOprGroupUi).join('')}
                    </div>
                `;

                if (middleTableEl) middleTableEl.innerHTML = '';
                if (rightTableEl) rightTableEl.innerHTML = '';
            };

            const previousRenderAll = renderAll;
            renderAll = function () {
                previousRenderAll();
                updateLockNotice();
                focusActiveInlineEditor();
            };

            const previousBindEventsEnhanced = bindEvents;
            bindEvents = function () {
                previousBindEventsEnhanced();

                document.querySelectorAll('[data-role="concept-extra"], [data-role="concept-name-edit"], [data-role="concept-source-edit"]').forEach(el => {
                    el.dataset.initialValue = String(el.value || '');
                    el.dataset.baseHeight = '32';
                    el.style.height = '32px';
                    el.addEventListener('input', function () {
                        autoGrowTextarea(this);
                    });
                });

                document.querySelectorAll('[data-role="concept-name-display"]').forEach(el => {
                    const openEditor = function () {
                        startInlineEditor('concept-name', this.dataset.itemId);
                    };
                    el.addEventListener('click', openEditor);
                    el.addEventListener('keydown', function (e) {
                        if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            openEditor.call(this);
                        }
                    });
                });

                document.querySelectorAll('[data-role="concept-source-display"]').forEach(el => {
                    const openEditor = function () {
                        startInlineEditor('concept-source', this.dataset.itemId);
                    };
                    el.addEventListener('click', openEditor);
                    el.addEventListener('keydown', function (e) {
                        if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            openEditor.call(this);
                        }
                    });
                });

                document.querySelectorAll('[data-role="concept-name-edit"]').forEach(el => {
                    const commit = function () {
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldValue = item.name || '';
                        const newValue = String(this.value || '').trim();
                        item.name = newValue;
                        pushSessionChange(item.id, newValue || oldValue || item.id, 'name', oldValue, newValue);
                        stopInlineEditor();
                        renderAll();
                    };
                    const cancel = function () {
                        stopInlineEditor();
                        renderAll();
                    };
                    el.addEventListener('blur', commit);
                    el.addEventListener('keydown', function (e) {
                        if (e.key === 'Escape') {
                            e.preventDefault();
                            cancel();
                            return;
                        }
                        if (e.key === 'Enter' && !e.shiftKey) {
                            e.preventDefault();
                            commit.call(this);
                        }
                    });
                });

                document.querySelectorAll('[data-role="concept-source-edit"]').forEach(el => {
                    const commit = function () {
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldValue = item.source || '';
                        const newValue = String(this.value || '').trim();
                        item.source = newValue;
                        pushSessionChange(item.id, item.name || item.id, 'source', oldValue, newValue);
                        stopInlineEditor();
                        renderAll();
                    };
                    const cancel = function () {
                        stopInlineEditor();
                        renderAll();
                    };
                    el.addEventListener('blur', commit);
                    el.addEventListener('keydown', function (e) {
                        if (e.key === 'Escape') {
                            e.preventDefault();
                            cancel();
                            return;
                        }
                        if (e.key === 'Enter' && !e.shiftKey) {
                            e.preventDefault();
                            commit.call(this);
                        }
                    });
                });

                document.querySelectorAll('[data-role="opr-toggle-dates"]').forEach(btn => {
                    btn.addEventListener('click', function () {
                        oprDateVisibility[1] = !oprDateVisibility[1];
                        renderAll();
                    });
                });

                document.querySelectorAll('[data-role="opr-status"]').forEach(el => {
                    el.addEventListener('change', async function() {
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;

                        const oldItem = JSON.parse(JSON.stringify(item));
                        const oldValue = item.status || '';
                        const newValue = this.value;
                        const oldDocuments = getItemDocuments(oldItem);

                        if (newValue === 'Нет' && oldDocuments.length && !confirmStatusNoWithFiles(item.name, oldDocuments)) {
                            this.value = normalizeStatus(oldItem.status);
                            return;
                        }

                        if (newValue === 'Нет') {
                            try {
                                const result = await updateItem(item.id, 'status', newValue, 'opr');
                                if (!result || !result.item) {
                                    throw new Error('opr status save failed');
                                }

                                replaceItem(result.item);
                                pushSessionChange(item.id, item.name, 'status', oldValue, newValue);

                                if (oldDocuments.length) {
                                    const removedNames = oldDocuments.map(x => x.name || 'uploaded').join(', ');
                                    pushSessionChange(item.id, item.name, 'document', removedNames, 'Удален');
                                }

                                renderAll();
                            } catch (e) {
                                console.log('opr status save error:', e);
                                setSaveState('error', 'Ошибка сохранения статуса');
                                this.value = normalizeStatus(oldItem.status);
                            }
                            return;
                        }

                        item.status = newValue;
                        if (newValue === 'Не требуется') {
                            item.group = 2;
                        } else if (Number(item.group) === 2) {
                            item.group = resolveOprGroupIdByItemIdOrName ? resolveOprGroupIdByItemIdOrName(item) : 1;
                            if (Number(item.group) === 2) {
                                item.group = 1;
                            }
                        }

                        pushSessionChange(item.id, item.name, 'status', oldValue, newValue);
                        renderAll();
                    });
                });

                document.querySelectorAll('[data-role="opr-plan"]').forEach(el => {
                    el.addEventListener('change', function() {
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldValue = item.plan || '';
                        const newValue = fromInputDate(this.value);
                        item.plan = newValue;
                        pushSessionChange(item.id, item.name, 'plan', oldValue, newValue);
                        renderAll();
                    });
                });

                document.querySelectorAll('[data-role="opr-fact"]').forEach(el => {
                    el.addEventListener('change', function() {
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldValue = item.fact || '';
                        const newValue = fromInputDate(this.value);
                        item.fact = newValue;
                        pushSessionChange(item.id, item.name, 'fact', oldValue, newValue);
                        renderAll();
                    });
                });

                document.querySelectorAll('[data-role="opr-add-item"]').forEach(btn => {
                    btn.addEventListener('click', async function() {
                        const groupId = Number(this.dataset.groupId);
                        const input = document.getElementById('oprAddItemInput_' + groupId);
                        if (!input) return;

                        const name = (input.value || '').trim();
                        if (!name) return;

                        this.disabled = true;

                        try {
                            const result = await addItem(groupId, name, 'opr');
                            if (!result || !result.item) {
                                throw new Error('add opr item failed');
                            }

                            replaceItem(result.item);
                            pushSessionChange(result.item.id, result.item.name, 'add-item', '', result.item.name);

                            debugLog('opr_item_added', {
                                itemId: result.item.id,
                                itemName: result.item.name,
                                groupId: groupId
                            });

                            input.value = '';
                            renderAll();
                        } catch (e) {
                            console.log('opr add item error:', e);
                            setSaveState('error', 'Ошибка добавления пункта');
                        } finally {
                            this.disabled = false;
                        }
                    });
                });
            };

            setTimeout(async function () {
                await ensureCurrentEditorReady();
                await acquireChecklistLock(currentChecklistKey, true);
                startLockHeartbeat();
                updateLockNotice();

                if (String(projectRootYandexPath || '').trim() && !String(projectRootYandexUrl || '').trim()) {
                    try {
                        const response = await fetch(
                            appUrl('api/project-root-folder') +
                            '?dialogId=' + encodeURIComponent(dialogId)
                        );
                        const result = await response.json();

                        if (response.ok && result && result.ok) {
                            projectRootYandexUrl = String(result.url || '').trim();
                            renderProjectRootFolderButton();
                        }
                    } catch (e) {
                        console.log('project root folder background load error:', e);
                    }
                }
            }, 0);
    """

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
            .progress-box.id-accent {{ min-width:172px; }}
            .progress-box.id-accent .progress-label {{ font-size:13px; }}
            .progress-box.id-accent .progress-value {{ font-size:24px; }}
            .progress-box.id-accent .progress-track {{ height:9px; }}
            .save-state.saving {{ background:#fff4e5; color:#b26a00; }}
            .save-state.error {{ background:#fdecec; color:#b42318; }}
            .content {{ padding:14px 16px 16px; max-height:82vh; overflow:auto; }}
            .layout {{ display:flex; flex-direction:column; gap:12px; align-items:stretch; }}
            .tables-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:14px; align-items:start; }}
            .table-panel {{ min-width:0; display:flex; }}
            .table-panel .table {{ flex:1 1 auto; }}
            .side-panel {{ order:-1; border:1px solid #e5e7eb; border-radius:12px; background:#fff; overflow:hidden; position:static; }}
            .side-panel-title {{ padding:12px 14px; background:#fafbfc; border-bottom:1px solid #e5e7eb; font-size:13px; font-weight:700; color:#344054; }}
            .side-panel-list {{ padding:10px; display:flex; flex-wrap:wrap; gap:8px; }}
            .side-link {{ display:inline-flex; width:auto; text-align:left; border:1px solid #d0d7de; border-radius:8px; background:#fff; padding:9px 12px; font-size:13px; cursor:pointer; align-items:center; }}
            .side-link.active {{ background:#eef2ff; border-color:#c7d2fe; font-weight:700; }}
            .table {{ width:100%; border:1px solid #e5e7eb; border-radius:12px; overflow:hidden; background:#fff; }}
            .thead {{ position:sticky; top:0; z-index:10; background:#f8fafc; border-bottom:1px solid #e5e7eb; }}
            .thead-top,.thead-bottom {{ min-height:38px; }}
            .thead-top,.thead-bottom,.row {{ display:grid; grid-template-columns:190px 190px 100px 136px 136px; gap:0; align-items:stretch; justify-content:start; }}
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
            .concept-extra-textarea {{ width:100%; height:32px; min-height:32px; border:1px solid #d0d7de; border-radius:8px; padding:6px 8px; font-size:12px; background:#fff; resize:vertical; overflow:hidden; line-height:1.35; }}
            .concept-inline-display {{ min-height:32px; padding:6px 8px; border:1px solid transparent; border-radius:8px; font-size:12px; line-height:1.35; white-space:pre-wrap; word-break:break-word; cursor:text; }}
            .concept-inline-display:hover {{ background:#f8fafc; border-color:#e5e7eb; }}
            .concept-inline-display.empty {{ color:#98a2b3; }}
            .concept-inline-display.disabled {{ cursor:default; background:#f8fafc; border-color:transparent; }}
            .concept-inline-input {{ width:100%; min-height:32px; height:32px; border:1px solid #d0d7de; border-radius:8px; padding:6px 8px; font-size:12px; background:#fff; resize:vertical; overflow:hidden; line-height:1.35; }}
            .doc-btn,.upload-btn,.add-item-btn {{ display:inline-block; width:100%; text-align:center; padding:6px 8px; border:1px solid #d0d7de; border-radius:8px; background:#f8fafc; color:#1f2328; font-size:12px; text-decoration:none; cursor:pointer; }}
            .doc-btn:hover,.upload-btn:hover,.side-link:hover,.add-item-btn:hover {{ background:#f1f5f9; }}
            .doc-cell {{
                display: flex;
                flex-direction: column;
                gap: 6px;
            }}

            .doc-actions {{
                display: flex;
                gap: 6px;
            }}

            .doc-actions .upload-btn,
            .doc-actions .doc-btn {{
                flex: 1 1 0;
                width: auto;
            }}

            .doc-files {{
                display: flex;
                flex-direction: column;
                gap: 4px;
            }}

            .doc-file-row {{
                display: flex;
                align-items: center;
                gap: 6px;
                min-width: 0;
            }}

            .doc-file-link {{
                flex: 1 1 auto;
                min-width: 0;
                font-size: 12px;
                color: #175cd3;
                text-decoration: none;
                cursor: pointer;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
            }}

            .doc-file-link:hover {{
                text-decoration: underline;
            }}

            .doc-file-remove {{
                flex: 0 0 auto;
                border: none;
                background: transparent;
                color: #b42318;
                cursor: pointer;
                font-size: 14px;
                line-height: 1;
                padding: 0 2px;
                position: relative;
                z-index: 2;
                pointer-events: auto;
            }}

            .doc-file-meta {{
                flex: 0 0 auto;
                font-size: 11px;
                color: #667085;
                white-space: nowrap;
            }}
            
            .doc-file-remove:hover {{
                opacity: .8;
            }}
            .add-item-row {{ padding:9px 12px 10px; min-height:52px; border-top:1px solid #edf0f2; background:#fcfcfd; display:flex; gap:8px; align-items:center; justify-content:flex-start; }}
            .add-item-row::after {{ content:''; flex:1 1 auto; }}
            .add-item-input {{ flex:0 0 190px; width:190px; min-width:190px; height:32px; border:1px solid #d0d7de; border-radius:8px; padding:7px 9px; font-size:12px; }}
            .add-item-btn {{ flex:0 0 100px; width:100px; min-width:100px; height:32px; border:1px solid #d0d7de; border-radius:8px; padding:6px 8px; background:#f8fafc; cursor:pointer; font-size:12px; white-space:nowrap; }}
            @media (max-width:1320px) {{ .layout {{ grid-template-columns:1fr; }} .side-panel {{ position:static; }} }}
            @media (max-width:1120px) {{ .tables-grid {{ grid-template-columns:1fr; }} }}
            .tables-grid.id-three-cols {{
                grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1fr);
            }}

            .id-table .thead {{
                position: sticky;
                top: 0;
                z-index: 10;
                background: #f8fafc;
                border-bottom: 1px solid #e5e7eb;
            }}

            .id-grid {{
                display: grid;
                gap: 0;
                align-items: stretch;
                justify-content: start;
            }}

            .id-grid.id-grid-compact {{
                grid-template-columns: minmax(0, 1.18fr) minmax(0, 1.02fr) 108px;
            }}

            .id-grid.id-grid-expanded {{
                grid-template-columns: minmax(0, 1.08fr) minmax(0, 0.96fr) 108px 112px 112px;
            }}

            .id-table .thead-top,
            .id-table .thead-bottom,
            .id-table .row {{
                min-height: 38px;
            }}

            .id-table .thead-bottom {{
                border-top: 1px solid #edf0f2;
            }}

            .th-status-with-toggle {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 6px;
            }}

            .id-dates-toggle {{
                width: 22px;
                min-width: 22px;
                height: 22px;
                border: 1px solid #d0d7de;
                background: #fff;
                color: #344054;
                border-radius: 6px;
                padding: 0;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                font-size: 12px;
                line-height: 1;
                cursor: pointer;
            }}

            .id-dates-toggle:hover {{
                background: #f8fafc;
            }}

            .id-table .item-name {{
                max-width: none;
            }}

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
                        <div style="display:flex; align-items:flex-end; gap:10px; flex-wrap:wrap;">
                            <div class="progress-box">
                                <div class="progress-label">Прогресс</div>
                                <div class="progress-value" id="progressValue">{progress_percent}%</div>
                                <div class="progress-track"><div class="progress-bar" id="progressBar"></div></div>
                            </div>
                            <div id="projectRootFolderBox" style="display:none; align-self:flex-end; min-width:260px;"></div>
                        </div>
                    </div>
                    <div class="header-right">
                        <div id="saveState" class="save-state">Сохранено</div>
                    </div>
                </div>
                <div class="content">
                    <div id="debugPanel" style="
                        display:none;
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
                            <a id="debugLogsLink" href="debug/logs" target="_blank">Открыть /debug/logs</a>
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
                                            <div class="th">ТУ</div>
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
                                    <div id="middleTableBody"></div>
                                </div>
                            </div>

                            <div class="table-panel">
                                <div class="table">
                                    <div class="thead">
                                        <div class="thead-top">
                                            <div class="th">Прочее</div>
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
            const projectRootYandexPath = {project_root_yandex_path_json};
            let projectRootYandexUrl = {project_root_yandex_url_json};

            let rawGroups = {groups_json};
            let rawProjectChecklists = {project_checklists_json};
            let rawItems = {items_json};
            let collabTitle = {collab_title_json};

            let groups = Array.isArray(rawGroups) ? rawGroups : [];
            let projectChecklists = Array.isArray(rawProjectChecklists) ? rawProjectChecklists : [];
            let items = Array.isArray(rawItems) ? rawItems : [];

            let currentChecklistKey = {checklist_key_json};
            let checklistTitle = {checklist_title_json};
            let checklistCache = {{}};
            let sessionChanges = [];
            let currentEditor = {{
                id: "",
                name: ""
            }};
            window.currentEditor = currentEditor;
            let currentEditorReady = false;
            let currentEditorReadyPromise = null;
                        const saveStateEl = document.getElementById('saveState');
            const leftTableBodyEl = document.getElementById('leftTableBody');
            const middleTableBodyEl = document.getElementById('middleTableBody');
            const rightTableBodyEl = document.getElementById('rightTableBody');
            const progressValueEl = document.getElementById('progressValue');
            const progressBarEl = document.getElementById('progressBar');
            const progressBoxEl = document.querySelector('.progress-box');
            const popupTitleEl = document.getElementById('popupTitle');
            const projectRootFolderBoxEl = document.getElementById('projectRootFolderBox');
            const projectChecklistListEl = document.getElementById('projectChecklistList');
            const tablePanels = document.querySelectorAll('.table-panel');
            const tablesGridEl = document.querySelector('.tables-grid');
            const leftTableEl = tablePanels[0] ? tablePanels[0].querySelector('.table') : null;
            const middleTableEl = tablePanels[1] ? tablePanels[1].querySelector('.table') : null;
            const rightTableEl = tablePanels[2] ? tablePanels[2].querySelector('.table') : null;
            const idTableShellHtml = leftTableEl ? leftTableEl.innerHTML : '';
            const idDateVisibility = {{ 1: false, 2: false, 3: false }};
            const oprDateVisibility = {{ 1: false }};
            const conceptDateVisibility = {{ 1: false }};
            const debugLastEventEl = document.getElementById('debugLastEvent');
            const debugPanelEl = document.getElementById('debugPanel');
            const debugLogsLinkEl = document.getElementById('debugLogsLink');
            const allowedDebugUserIds = new Set(['138', '18']);
            function updateDebugPanelAccess() {{
                const currentUserId = String(currentEditor.id || '');
                if (debugPanelEl) {{
                    debugPanelEl.style.display = allowedDebugUserIds.has(currentUserId) ? '' : 'none';
                }}
                if (debugLogsLinkEl) {{
                    debugLogsLinkEl.href = 'debug/logs?userId=' + encodeURIComponent(currentUserId);
                }}
            }}
            function detectAppBasePath() {{
                const path = String(window.location.pathname || '/').replace(/\\/+$/, '');
                const suffixes = ['/popup', '/launch', '/textarea', '/install', '/health', '/debug/logs', '/admin', '/admin/upload'];

                for (const suffix of suffixes) {{
                    if (path === suffix) return '';
                    if (path.endsWith(suffix)) {{
                        return path.slice(0, -suffix.length) || '';
                    }}
                }}

                return '';
            }}

            const APP_BASE_PATH = detectAppBasePath();
            const APP_BASE_URL = window.location.origin + (APP_BASE_PATH || '');

            function appUrl(path) {{
                return APP_BASE_URL + '/' + String(path || '').replace(/^\\/+/, '');
            }}
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

            function getItemDocuments(item) {{
                const docs = Array.isArray(item && item.documents) ? item.documents : [];
                if (docs.length) {{
                    return docs;
                }}

                const legacyUrl = String(item && item.documentUrl || '').trim();
                const legacyName = String(item && item.documentName || '').trim();

                if (legacyUrl || legacyName) {{
                    return [{{
                        id: 'legacy_' + String(item && item.id || ''),
                        name: legacyName || 'Файл',
                        path: legacyUrl,
                        fileUrl: legacyUrl,
                        previewUrl: legacyUrl,
                        size: 0,
                        modifiedAt: '',
                        source: 'local'
                    }}];
                }}

                return [];
            }}

            function formatFileSize(size) {{
                const value = Number(size || 0);
                if (!value || value <= 0) return '';

                const units = ['Б', 'КБ', 'МБ', 'ГБ'];
                let current = value;
                let unitIndex = 0;

                while (current >= 1024 && unitIndex < units.length - 1) {{
                    current /= 1024;
                    unitIndex += 1;
                }}

                if (unitIndex === 0) {{
                    return Math.round(current) + ' ' + units[unitIndex];
                }}

                if (current >= 100) return current.toFixed(0) + ' ' + units[unitIndex];
                if (current >= 10) return current.toFixed(1) + ' ' + units[unitIndex];
                return current.toFixed(2) + ' ' + units[unitIndex];
            }}

            function confirmStatusNoWithFiles(itemName, documents) {{
                const docs = Array.isArray(documents) ? documents : [];
                if (!docs.length) {{
                    return true;
                }}

                const safeItemName = String(itemName || 'пункт').trim() || 'пункт';

                return window.confirm(
                    'В пункте "' + safeItemName + '" уже загружены файлы.\\n\\n' +
                    'При выборе статуса "Нет" эти файлы будут удалены.\\n\\n' +
                    'Продолжить?'
                );
            }}

            function renderTitle() {{
                if (collabTitle) {{
                    popupTitleEl.innerHTML = esc(checklistTitle) + ' <small>— ' + esc(collabTitle) + '</small>';
                }} else {{
                    popupTitleEl.textContent = checklistTitle;
                }}
            }}
            function fetchCurrentUserIfPossible() {{
                if (currentEditorReadyPromise) {{
                    return currentEditorReadyPromise;
                }}

                currentEditorReadyPromise = new Promise(function(resolve) {{
                    try {{
                        if (!(window.BX24 && typeof window.BX24.init === 'function')) {{
                            currentEditorReady = true;
                            updateDebugPanelAccess();
                            resolve(currentEditor);
                            return;
                        }}

                        let resolved = false;

                        function finish() {{
                            if (resolved) return;
                            resolved = true;
                            currentEditorReady = true;
                            updateDebugPanelAccess();
                            resolve(currentEditor);
                        }}

                        window.BX24.init(function () {{
                            try {{
                                window.BX24.callMethod('user.current', {{}}, function(result) {{
                                    try {{
                                        if (!result.error()) {{
                                            const data = result.data() || {{}};
                                            const fullName = [data.NAME, data.LAST_NAME].filter(Boolean).join(' ').trim();

                                            currentEditor = {{
                                                id: String(data.ID || ''),
                                                name: fullName || String(data.NAME || '') || ''
                                            }};
                                            window.currentEditor = currentEditor;
                                        }}
                                    }} catch (e) {{
                                        console.log('user.current parse error:', e);
                                    }} finally {{
                                        finish();
                                    }}
                                }});
                            }} catch (e) {{
                                console.log('user.current call error:', e);
                                finish();
                            }}
                        }});
                    }} catch (e) {{
                        console.log('fetchCurrentUserIfPossible skipped:', e);
                        currentEditorReady = true;
                        updateDebugPanelAccess();
                        resolve(currentEditor);
                    }}
                }});

                return currentEditorReadyPromise;
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
                    checklistKey: currentChecklistKey,
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
            function logRenderState(stage) {{
                debugLog('render_state', {{
                    stage,
                    groupsType: typeof rawGroups,
                    itemsType: typeof rawItems,
                    projectChecklistsType: typeof rawProjectChecklists,
                    groupsIsArray: Array.isArray(groups),
                    itemsIsArray: Array.isArray(items),
                    projectChecklistsIsArray: Array.isArray(projectChecklists),
                    groupsLength: groups.length,
                    itemsLength: items.length,
                    projectChecklistsLength: projectChecklists.length,
                    leftTableExists: !!leftTableBodyEl,
                    rightTableExists: !!rightTableBodyEl,
                    titleExists: !!popupTitleEl,
                    sidePanelExists: !!projectChecklistListEl
                }});
            }}

            function logRenderError(stage, error) {{
                const message = (error && error.message) ? error.message : String(error || 'unknown error');
                const stack = (error && error.stack) ? error.stack : '';

                setDebugText(stage + ' | ERROR: ' + message);

                debugLog('render_error', {{
                    stage,
                    message,
                    stack
                }});
            }}
            function deepClone(value) {{
                return JSON.parse(JSON.stringify(value));
            }}

            function buildChecklistSnapshot() {{
                return {{
                    checklistKey: currentChecklistKey,
                    title: checklistTitle,
                    collabTitle,
                    groups: deepClone(groups),
                    projectChecklists: deepClone(projectChecklists),
                    items: deepClone(items)
                }};
            }}

            function syncChecklistCache() {{
                checklistCache[currentChecklistKey] = buildChecklistSnapshot();
            }}

            function applyChecklistData(data) {{
                const nextData = data || {{}};
                const nextKey = String(nextData.checklistKey || currentChecklistKey || 'id').trim() || 'id';

                currentChecklistKey = nextKey;
                checklistTitle = String(nextData.title || getChecklistDefaultTitle(nextKey));
                collabTitle = String(nextData.collabTitle || collabTitle || '');

                rawGroups = Array.isArray(nextData.groups) ? nextData.groups : [];
                rawProjectChecklists = Array.isArray(nextData.projectChecklists)
                    ? nextData.projectChecklists
                    : rawProjectChecklists;
                rawItems = Array.isArray(nextData.items) ? nextData.items : [];

                groups = Array.isArray(rawGroups) ? rawGroups : [];
                projectChecklists = Array.isArray(rawProjectChecklists) ? rawProjectChecklists : [];
                items = Array.isArray(rawItems) ? rawItems : [];

                document.title = collabTitle
                    ? checklistTitle + ' — ' + collabTitle
                    : checklistTitle;
            }}

            async function flushCurrentChecklistSummary(reason = 'checklist_switch') {{
                syncChecklistCache();
                debugLog('close_summary_switch_skipped', {{
                    checklistKey: currentChecklistKey,
                    reason,
                    changesCount: sessionChanges.length,
                    dirty: !!sessionDirty
                }});
            }}

            async function loadChecklistByKey(checklistKey) {{
                const targetKey = String(checklistKey || '').trim() || 'id';
                if (targetKey === currentChecklistKey) {{
                    return;
                }}

                syncChecklistCache();
                await releaseChecklistLock(currentChecklistKey, false);
                setSaveState('saving', 'Загружаем...');

                try {{
                    const cachedData = checklistCache[targetKey];
                    if (cachedData) {{
                        applyChecklistData(deepClone(cachedData));
                        renderAll();
                        debugLog('checklist_switched_cached', {{
                            checklistKey: targetKey
                        }});
                        await acquireChecklistLock(targetKey, true);
                        startLockHeartbeat();
                        setSaveState('', 'Сохранено');
                        return;
                    }}

                    const response = await fetch(
                        appUrl('api/checklist') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(targetKey)
                    );
                    const result = await response.json();

                    if (!response.ok) {{
                        throw new Error(result.error || 'load checklist failed');
                    }}

                    applyChecklistData(result);
                    renderAll();
                    debugLog('checklist_switched', {{
                        checklistKey: targetKey
                    }});
                    await acquireChecklistLock(targetKey, true);
                    startLockHeartbeat();
                    setSaveState('', 'Сохранено');
                }} catch (e) {{
                    console.log('loadChecklistByKey error:', e);
                    setSaveState('error', 'Ошибка загрузки чек-листа');
                }}
            }}

            async function reloadCurrentChecklistFromServer() {{
                const response = await fetch(
                    appUrl('api/checklist') +
                    '?dialogId=' + encodeURIComponent(dialogId) +
                    '&checklistKey=' + encodeURIComponent(currentChecklistKey)
                );
                const result = await response.json();

                if (!response.ok) {{
                    throw new Error(result.error || 'reload checklist failed');
                }}

                applyChecklistData(result);
                return result;
            }}

            function sendCloseSummaryOnce(eventName) {{
                if (eventName === 'popup_hidden' || suppressAutoCloseSave || closeSummarySent) {{
                    return;
                }}

                closeSummarySent = true;
                persistDirtyChecklists(eventName, true);
                releaseChecklistLock(currentChecklistKey, true);
            }}

            window.addEventListener('message', async function (event) {{
                const data = event && event.data ? event.data : {{}};
                const messageType = String(data && data.type || '');

                if (!['checklist-document-removed', 'checklist-document-uploaded', 'checklist-document-changed'].includes(messageType)) return;
                if (String(data.dialogId || '') !== String(dialogId || '')) return;
                if (String(data.checklistKey || '') !== String(currentChecklistKey || '')) return;

                if (messageType === 'checklist-document-removed') {{
                    const itemId = String(data.itemId || '').trim();
                    const documentName = String(data.documentName || '').trim() || 'Файл';
                    const item = items.find(x => String(x.id || '') === itemId);

                    pushSessionChange(
                        itemId,
                        item ? item.name : '',
                        'document',
                        documentName,
                        'Удален'
                    );

                    checklistSessionState[currentChecklistKey] = {{
                        changes: deepClone(sessionChanges),
                        dirty: !!sessionDirty
                    }};
                }}

                try {{
                    const response = await fetch(
                        appUrl('api/checklist') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(currentChecklistKey)
                    );
                    const result = await response.json();
                    if (!response.ok) throw new Error(result.error || 'reload after folder sync failed');

                    applyChecklistData(result);
                    renderAll();
                }} catch (e) {{
                    console.log('folder sync error:', e);
                }}
            }});

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
                                        await fetch(appUrl('api/checklist/update-meta'), {{
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
                if (!progressValueEl || !progressBarEl) {{
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
                const response = await fetch(appUrl('api/checklist/update-item'), {{
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
                const response = await fetch(appUrl('api/checklist/add-item'), {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ dialogId, checklistKey, groupId, name }})
                }});
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'add item failed');
                setSaveState('', 'Сохранено');
                return result;
            }}

            async function removeDocument(itemId, documentId = '') {{
                setSaveState('saving', 'Сохраняем...');
                const response = await fetch(appUrl('api/checklist/remove-document'), {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{
                        dialogId,
                        checklistKey: currentChecklistKey,
                        itemId,
                        documentId
                    }})
                }});
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'remove document failed');
                setSaveState('', 'Сохранено');
                return result;
            }}

            async function uploadDocument(itemId, file) {{
                setSaveState('saving', 'Сохраняем...');
                const item = items.find(x => x.id === itemId);
                const formData = new FormData();
                formData.append('dialogId', dialogId);
                formData.append('itemId', itemId);
                formData.append('file', file);
                formData.append('checklistKey', currentChecklistKey);
                formData.append('itemGroup', String(item && item.group ? item.group : ''));
                const response = await fetch(appUrl('api/checklist/upload-document'), {{ method: 'POST', body: formData }})
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'upload document failed');
                setSaveState('', 'Сохранено');
                return result;
            }}
            function getItemsByGroup(groupId) {{
                return items
                    .filter(item => Number(item.group) === Number(groupId))
                    .sort((a, b) => Number(a.order || 0) - Number(b.order || 0));
            }}
            function hasItemsInGroup(groupId) {{
                return getItemsByGroup(groupId).length > 0;
            }}
            function renderProjectRootFolderButton() {{
                if (!projectRootFolderBoxEl) {{
                    return;
                }}

                const folderUrl = String(projectRootYandexUrl || '').trim();
                const folderPath = String(projectRootYandexPath || '').trim();

                if (!folderPath) {{
                    projectRootFolderBoxEl.style.display = 'none';
                    projectRootFolderBoxEl.innerHTML = '';
                    return;
                }}

                projectRootFolderBoxEl.style.display = 'flex';
                projectRootFolderBoxEl.innerHTML = `
                    <button
                        class="doc-btn"
                        type="button"
                        data-role="view-project-root-folder"
                        data-folder-url="${{esc(folderUrl)}}"
                        title="${{esc(folderPath || 'Корневая папка проекта')}}"
                        style="min-width:260px; width:260px; height:32px; white-space:nowrap;"
                    >
                        Папка проекта на Яндекс Диске
                    </button>
                `;

                const btn = projectRootFolderBoxEl.querySelector('[data-role="view-project-root-folder"]');
                if (btn) {{
                    if (!folderUrl) {{
                        btn.disabled = true;
                        btn.style.opacity = '0.65';
                        btn.style.cursor = 'default';
                        btn.textContent = 'Подготавливаем ссылку...';
                        return;
                    }}

                    btn.addEventListener('click', function () {{
                        const url = String(this.dataset.folderUrl || '').trim();
                        if (url) {{
                            window.open(url, '_blank', 'noopener');
                        }}
                    }});
                }}
            }}

            function renderProjectChecklistList() {{
                if (!projectChecklistListEl) {{
                    return;
                }}

                if (!Array.isArray(projectChecklists) || !projectChecklists.length) {{
                    projectChecklistListEl.innerHTML = '';
                    return;
                }}

                projectChecklistListEl.innerHTML = projectChecklists.map(item => {{
                    const active = item.key === currentChecklistKey ? 'side-link active' : 'side-link';
                    return `<button type="button" class="${{active}}" data-checklist-key="${{esc(item.key)}}">${{esc(item.title)}}</button>`;
                }}).join('');
                projectChecklistListEl.querySelectorAll('[data-checklist-key]').forEach(btn => {{
                    btn.addEventListener('click', async function () {{
                        const key = this.dataset.checklistKey;
                        await loadChecklistByKey(key);
                    }});
                }});
            }}
            function buildDocumentCell(item) {{
                if (normalizeStatus(item && item.status) === 'Не требуется') {{
                    return '';
                }}

                const documents = getItemDocuments(item);
                const itemId = String(item && item.id || '');
                const folderViewUrl = String(item.folderUrl || '').trim() || (documents.length ? (
                    appUrl('api/checklist/folder') +
                    '?dialogId=' + encodeURIComponent(dialogId) +
                    '&checklistKey=' + encodeURIComponent(currentChecklistKey) +
                    '&itemId=' + encodeURIComponent(itemId)
                ) : '');
                const showViewFolder = documents.length > 0 && !!folderViewUrl;

                const filesHtml = documents.map(doc => {{
                    const docId = String(doc.id || '');
                    const docName = String(doc.name || 'Файл');
                    const openUrl = appUrl('api/checklist/file') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(currentChecklistKey) +
                        '&itemId=' + encodeURIComponent(itemId) +
                        '&documentId=' + encodeURIComponent(docId);
                    const sizeText = formatFileSize(doc.size || 0);

                    return `
                        <div class="doc-file-row">
                            <a
                                href="javascript:void(0)"
                                class="doc-file-link"
                                data-role="view-file"
                                data-item-id="${{esc(itemId)}}"
                                data-document-id="${{esc(docId)}}"
                                data-open-url="${{esc(openUrl)}}"
                                title="${{esc(docName)}}"
                            >
                                ${{esc(docName)}}
                            </a>
                            <button
                                type="button"
                                class="doc-file-remove"
                                data-role="remove-file"
                                data-item-id="${{esc(itemId)}}"
                                data-document-id="${{esc(docId)}}"
                                data-document-name="${{esc(docName)}}"
                                title="Удалить файл"
                                ${{typeof disabledAttr === 'function' ? disabledAttr() : ''}}
                            >
                                ×
                            </button>
                            ${{sizeText ? `<span class="doc-file-meta">${{esc(sizeText)}}</span>` : ''}}
                        </div>
                    `;
                }}).join('');

                return `
                    <div class="doc-cell">
                        <div class="doc-actions">
                            <button
                                class="upload-btn"
                                type="button"
                                data-role="upload"
                                data-item-id="${{esc(itemId)}}"
                                ${{typeof disabledAttr === 'function' ? disabledAttr() : ''}}
                            >
                                Загрузить
                            </button>

                            ${{showViewFolder ? `
                                <button
                                    class="doc-btn"
                                    type="button"
                                    data-role="view-folder"
                                    data-item-id="${{esc(itemId)}}"
                                    data-folder-url="${{esc(folderViewUrl)}}"
                                >
                                    Посмотреть
                                </button>
                            ` : ''}}
                        </div>

                        ${{documents.length ? `
                            <div class="doc-files">
                                ${{filesHtml}}
                            </div>
                        ` : ''}}

                        <input
                            type="file"
                            data-role="file-input"
                            data-item-id="${{esc(itemId)}}"
                            style="display:none;"
                            multiple
                            ${{typeof disabledAttr === 'function' ? disabledAttr() : ''}}
                        >
                    </div>
                `;
            }}
            function pushSessionChange(itemId, itemName, field, oldValue, newValue) {{
                if (String(oldValue || '') === String(newValue || '')) {{
                    return;
                }}

                sessionChanges.push({{
                    field,
                    itemId: itemId || '',
                    itemName: itemName || '',
                    oldValue: oldValue || '',
                    newValue: newValue || ''
                }});
                sessionDirty = true;
                closeSummarySent = false;
                setSaveState('saving', 'Есть несохраненные изменения');
            }}
            function autoGrowTextarea(el) {{
                if (!el) return;

                const baseHeight = Number(el.dataset.baseHeight || 0) || 32;
                const initialValue = String(el.dataset.initialValue || '');
                el.dataset.baseHeight = String(baseHeight);
                el.style.height = baseHeight + 'px';

                if (String(el.value || '').length <= initialValue.length) {{
                    return;
                }}

                let nextHeight = baseHeight;
                while (el.scrollHeight > el.clientHeight && nextHeight < 1600) {{
                    nextHeight *= 2;
                    el.style.height = nextHeight + 'px';
                }}
            }}
            function extractConceptUnit(placeholder) {{
                const raw = String(placeholder || '').trim();
                const match = raw.match(new RegExp('^_+\\s*(.+)$'));
                return match ? match[1].trim() : '';
            }}
            function formatConceptTextStatus(value, placeholder) {{
                const rawValue = String(value || '').trim();
                if (!rawValue) {{
                    return '';
                }}

                if (rawValue === 'Не требуется') {{
                    return rawValue;
                }}

                const unit = extractConceptUnit(placeholder);
                if (!unit) {{
                    return rawValue;
                }}

                if (rawValue.toLowerCase().endsWith(unit.toLowerCase())) {{
                    return rawValue;
                }}

                if (!/^[0-9]+([.,][0-9]+)?$/.test(rawValue)) {{
                    return rawValue;
                }}

                return unit.startsWith('%') ? rawValue + unit : rawValue + ' ' + unit;
            }}
            function buildClientItemId(prefix) {{
                return prefix + '_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
            }}
            function createLocalItem(groupId, name, checklistKey) {{
                const groupItems = getItemsByGroup(groupId);
                const nextOrder = groupItems.length + 1;

                if (checklistKey === 'concept') {{
                    return {{
                        id: buildClientItemId('concept_g' + groupId + '_custom'),
                        group: groupId,
                        order: nextOrder,
                        name,
                        source: '',
                        statusKind: 'text',
                        statusOptions: [],
                        statusPlaceholder: '',
                        status: '',
                        extraInfo: '',
                        extraInfoPlaceholder: '',
                        documentUrl: '',
                        documentName: '',
                        isCustom: true,
                        priority: 'white'
                    }};
                }}

                return {{
                    id: buildClientItemId('item_g' + groupId + '_custom'),
                    group: groupId,
                    order: nextOrder,
                    name,
                    priority: 'white',
                    status: '',
                    plan: '',
                    fact: '',
                    documentUrl: '',
                    documentName: '',
                    isCustom: true
                }};
            }}
            function resolveConceptGroupId(item) {{
                const itemId = String(item && item.id || '');
                const name = String(item && item.name || '').trim();

                if (itemId.startsWith('concept_g')) {{
                    const match = itemId.match(new RegExp('^concept_g(\\d+)_'));
                    if (match) {{
                        const groupId = Number(match[1]);
                        if (groupId && groupId !== 10) {{
                            return groupId;
                        }}
                    }}
                }}

                const byName = groups.find(group => Number(group.id) !== 10 && Array.isArray(items) && items.some(existing =>
                    existing !== item &&
                    Number(existing.group) === Number(group.id) &&
                    String(existing.name || '').trim() === name
                ));
                if (byName) {{
                    return Number(byName.id);
                }}

                return 1;
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
                return `<textarea class="concept-extra-textarea" data-role="concept-extra" data-item-id="${{esc(item.id)}}" placeholder="${{esc(item.extraInfoPlaceholder || '')}}">${{esc(item.extraInfo || '')}}</textarea>`;
            }}
            function renderConceptGroup(group) {{
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 10;
                const rows = groupItems.map(item => `
                    <div class="row" style="grid-template-columns: 1.05fr 0.66fr 190px 150px 1.15fr;" data-item-id="${{esc(item.id)}}">
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

                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="conceptAddItemInput_${{group.id}}" type="text" placeholder="Новый пункт">
                        <button class="add-item-btn" type="button" data-role="concept-add-item" data-group-id="${{group.id}}">Добавить пункт</button>
                    </div>
                ` : '';

                return `<div class="group-block"><div class="group-title">${{esc(group.title)}}</div>${{rows}}${{addBlock}}</div>`;
            }}
            function buildConceptTableHtml(conceptGroups) {{
                return `
                    <div class="thead">
                        <div class="thead-top" style="grid-template-columns: 1.05fr 0.66fr 190px 150px 1.15fr;">
                            <div class="th">Пункт</div>
                            <div class="th">Нормативы</div>
                            <div class="th">Документ</div>
                            <div class="th">Статус</div>
                            <div class="th">Доп информация</div>
                        </div>
                    </div>
                    <div>${{conceptGroups.map(renderConceptGroup).join('')}}</div>
                `;
            }}
            function renderConceptTable() {{
                if (!leftTableEl || !rightTableEl || !tablesGridEl) {{
                    throw new Error('concept table containers not found');
                }}

                tablesGridEl.style.gridTemplateColumns = '1fr 1fr';
                tablesGridEl.style.justifyContent = '';

                if (tablePanels[0]) {{
                    tablePanels[0].style.display = '';
                    tablePanels[0].style.flex = '';
                    tablePanels[0].style.width = '';
                    tablePanels[0].style.maxWidth = '';
                }}
                if (tablePanels[1]) {{
                    tablePanels[1].style.display = '';
                    tablePanels[1].style.flex = '';
                    tablePanels[1].style.maxWidth = '';
                }}
                if (tablePanels[2]) {{
                    tablePanels[2].style.display = 'none';
                    tablePanels[2].style.flex = '';
                    tablePanels[2].style.maxWidth = '';
                }}

                leftTableEl.style.width = '';
                leftTableEl.style.maxWidth = '';
                rightTableEl.style.width = '';
                rightTableEl.style.maxWidth = '';

                const visibleGroups = groups.filter(group => {{
                    if (Number(group.id) !== 10) return true;
                    return items.some(x => Number(x.group) === 10);
                }});

                const leftGroups = visibleGroups.filter(group => [1, 3, 5, 7, 9].includes(Number(group.id)));
                const rightGroups = visibleGroups.filter(group => [2, 4, 6, 8, 10].includes(Number(group.id)));

                leftTableEl.innerHTML = buildConceptTableHtml(leftGroups);
                rightTableEl.innerHTML = buildConceptTableHtml(rightGroups);
            }}

            function isIdChecklist() {{
                return currentChecklistKey === 'id';
            }}

            function isIdDatesVisible(groupId) {{
                return !!idDateVisibility[Number(groupId)];
            }}

            function getIdGridClass(showDates) {{
                return showDates ? 'id-grid id-grid-expanded' : 'id-grid id-grid-compact';
            }}

            function buildIdHeader(group, showDates) {{
                const groupId = Number(group.id);
                const toggleTitle = showDates ? 'Скрыть даты' : 'Показать даты';
                const toggleIcon = '📅';

                return `
                    <div class="thead-top ${{getIdGridClass(showDates)}}">
                        <div class="th">${{esc(group.title)}}</div>
                        <div class="th">Документ</div>
                        <div class="th th-status-with-toggle">
                            <span>Статус</span>
                            <button
                                type="button"
                                class="id-dates-toggle"
                                data-role="toggle-id-dates"
                                data-group-id="${{esc(groupId)}}"
                                title="${{esc(toggleTitle)}}"
                                aria-label="${{esc(toggleTitle)}}"
                            >
                                ${{toggleIcon}}
                            </button>
                        </div>
                        ${{showDates ? `<div class="th center" style="grid-column: 4 / span 2;">Дата получения</div>` : ''}}
                    </div>
                    ${{showDates ? `
                        <div class="thead-bottom ${{getIdGridClass(showDates)}}">
                            <div class="th"></div>
                            <div class="th"></div>
                            <div class="th"></div>
                            <div class="th">План</div>
                            <div class="th">Факт</div>
                        </div>
                    ` : ''}}
                `;
            }}

            function renderIdGroup(group, showDates) {{
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 4;
                const gridClass = getIdGridClass(showDates);

                const rows = groupItems.map(item => {{
                    const rowClass = normalizeStatus(item.status) === 'Не требуется' ? 'row not-required' : 'row';

                    return `
                        <div class="${{rowClass}} ${{gridClass}}" data-item-id="${{esc(item.id)}}">
                            <div class="td">
                                <div class="cell-name">
                                    <div class="${{indicatorClass(item.status)}}"></div>
                                    <div class="item-name">${{esc(item.name)}}</div>
                                </div>
                            </div>
                            <div class="td">${{buildDocumentCell(item)}}</div>
                            <div class="td">
                                <select class="status-select" data-role="status" data-item-id="${{esc(item.id)}}" ${{disabledAttr()}}>
                                    <option value="" ${{normalizeStatus(item.status) === '' ? 'selected' : ''}}></option>
                                    <option value="Есть" ${{normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}}>Есть</option>
                                    <option value="Нет" ${{normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}}>Нет</option>
                                    <option value="Не требуется" ${{normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}}>Не требуется</option>
                                </select>
                            </div>
                            ${{showDates ? `
                                <div class="td">
                                    <input class="date-input" type="date" data-role="plan" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.plan))}}" ${{disabledAttr()}}>
                                </div>
                                <div class="td">
                                    <input class="date-input" type="date" data-role="fact" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.fact))}}" ${{disabledAttr()}}>
                                </div>
                            ` : ''}}
                        </div>
                    `;
                }}).join('');

                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="addItemInput_${{group.id}}" type="text" placeholder="Новый пункт" ${{disabledAttr()}}>
                        <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${{group.id}}" ${{disabledAttr()}}>Добавить пункт</button>
                    </div>
                ` : '';

                return `<div class="group-block"><div class="group-title">${{esc(group.title)}}</div>${{rows}}${{addBlock}}</div>`;
            }}

            function renderIdPanel(mainGroup, appendNotRequired = false) {{
                const showDates = isIdDatesVisible(mainGroup.id);
                const notRequiredGroup = appendNotRequired
                    ? groups.find(g => Number(g.id) === 4)
                    : null;

                const panelHtml = `
                    <div class="table id-table">
                        <div class="thead">
                            ${{buildIdHeader(mainGroup, showDates)}}
                        </div>
                        <div>
                            ${{renderIdGroup(mainGroup, showDates)}}
                            ${{appendNotRequired && notRequiredGroup && hasItemsInGroup(4) ? renderIdGroup(notRequiredGroup, false) : ''}}
                        </div>
                    </div>
                `;

                return panelHtml;
            }}

            function renderIdTables() {{
                if (!leftTableEl || !middleTableEl || !rightTableEl || !tablesGridEl) {{
                    throw new Error('id table containers not found');
                }}

                const idGroup = groups.find(g => Number(g.id) === 1) || {{ id: 1, title: 'ИД' }};
                const tuGroup = groups.find(g => Number(g.id) === 2) || {{ id: 2, title: 'ТУ' }};
                const otherGroup = groups.find(g => Number(g.id) === 3) || {{ id: 3, title: 'Прочее' }};

                tablesGridEl.classList.add('id-three-cols');
                tablesGridEl.style.gridTemplateColumns = 'minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1fr)';
                tablesGridEl.style.justifyContent = '';

                if (tablePanels[0]) {{
                    tablePanels[0].style.display = '';
                    tablePanels[0].style.flex = '';
                    tablePanels[0].style.width = '';
                    tablePanels[0].style.maxWidth = '';
                }}
                if (tablePanels[1]) {{
                    tablePanels[1].style.display = '';
                    tablePanels[1].style.flex = '';
                    tablePanels[1].style.maxWidth = '';
                }}
                if (tablePanels[2]) {{
                    tablePanels[2].style.display = '';
                    tablePanels[2].style.flex = '';
                    tablePanels[2].style.maxWidth = '';
                }}

                leftTableEl.style.width = '';
                leftTableEl.style.maxWidth = '';
                middleTableEl.style.width = '';
                middleTableEl.style.maxWidth = '';
                rightTableEl.style.width = '';
                rightTableEl.style.maxWidth = '';

                leftTableEl.innerHTML = renderIdPanel(idGroup, false);
                middleTableEl.innerHTML = renderIdPanel(tuGroup, false);
                rightTableEl.innerHTML = renderIdPanel(otherGroup, true);
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
                                <select class="status-select" data-role="status" data-item-id="${{esc(item.id)}}" ${{disabledAttr()}}>
                                    <option value="" ${{normalizeStatus(item.status) === '' ? 'selected' : ''}}></option>
                                    <option value="Есть" ${{normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}}>Есть</option>
                                    <option value="Нет" ${{normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}}>Нет</option>
                                    <option value="Не требуется" ${{normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}}>Не требуется</option>
                                </select>
                            </div>
                            <div class="td"><input class="date-input" type="date" data-role="plan" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.plan))}}" ${{disabledAttr()}}></div>
                            <div class="td"><input class="date-input" type="date" data-role="fact" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.fact))}}" ${{disabledAttr()}}></div>
                        </div>
                    `;
                }}).join('');
                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="addItemInput_${{group.id}}" type="text" placeholder="Новый пункт" ${{disabledAttr()}}>
                        <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${{group.id}}" ${{disabledAttr()}}>Добавить пункт</button>
                    </div>` : '';
                return `<div class="group-block"><div class="group-title">${{esc(group.title)}}</div>${{rows}}${{addBlock}}</div>`;
            }}

            function isConceptDatesVisible(groupId) {{
                return !!conceptDateVisibility[Number(groupId)];
            }}

            function getConceptGridClass(showDates) {{
                return showDates ? 'id-grid id-grid-expanded' : 'id-grid id-grid-compact';
            }}

            function buildConceptTableHeader(group, showDates) {{
                const groupId = Number(group.id);
                const toggleTitle = showDates ? 'Скрыть даты' : 'Показать даты';

                return `
                    <div class="thead-top ${{getConceptGridClass(showDates)}}">
                        <div class="th">${{esc(group.title)}}</div>
                        <div class="th">Документ</div>
                        <div class="th th-status-with-toggle">
                            <span>Статус</span>
                            <button
                                type="button"
                                class="id-dates-toggle"
                                data-role="toggle-concept-dates"
                                data-group-id="${{esc(groupId)}}"
                                title="${{esc(toggleTitle)}}"
                                aria-label="${{esc(toggleTitle)}}"
                            >
                                📅
                            </button>
                        </div>
                        ${{showDates ? `<div class="th center" style="grid-column: 4 / span 2;">Дата получения</div>` : ''}}
                    </div>
                    ${{showDates ? `
                        <div class="thead-bottom ${{getConceptGridClass(showDates)}}">
                            <div class="th"></div>
                            <div class="th"></div>
                            <div class="th"></div>
                            <div class="th">План</div>
                            <div class="th">Факт</div>
                        </div>
                    ` : ''}}
                `;
            }}

            function renderConceptTableGroup(group, showDates) {{
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 10;
                const gridClass = getConceptGridClass(showDates);

                const rows = groupItems.map(item => {{
                    const rowClass = normalizeStatus(item.status) === 'Не требуется' ? 'row not-required' : 'row';

                    return `
                        <div class="${{rowClass}} ${{gridClass}}" data-item-id="${{esc(item.id)}}">
                            <div class="td">
                                <div class="cell-name">
                                    <div class="${{indicatorClass(item.status)}}"></div>
                                    <div class="item-name">${{esc(item.name)}}</div>
                                </div>
                            </div>
                            <div class="td">${{buildDocumentCell(item)}}</div>
                            <div class="td">
                                <select class="status-select" data-role="status" data-item-id="${{esc(item.id)}}" ${{disabledAttr()}}>
                                    <option value="" ${{normalizeStatus(item.status) === '' ? 'selected' : ''}}></option>
                                    <option value="Есть" ${{normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}}>Есть</option>
                                    <option value="Нет" ${{normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}}>Нет</option>
                                    <option value="Не требуется" ${{normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}}>Не требуется</option>
                                </select>
                            </div>
                            ${{showDates ? `
                                <div class="td">
                                    <input class="date-input" type="date" data-role="plan" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.plan))}}" ${{disabledAttr()}}>
                                </div>
                                <div class="td">
                                    <input class="date-input" type="date" data-role="fact" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.fact))}}" ${{disabledAttr()}}>
                                </div>
                            ` : ''}}
                        </div>
                    `;
                }}).join('');

                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="addItemInput_${{group.id}}" type="text" placeholder="Новый пункт" ${{disabledAttr()}}>
                        <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${{group.id}}" ${{disabledAttr()}}>Добавить пункт</button>
                    </div>
                ` : '';

                return `<div class="group-block"><div class="group-title">${{esc(group.title)}}</div>${{rows}}${{addBlock}}</div>`;
            }}

            function renderConceptPanel(mainGroup, appendNotRequired = false) {{
                const showDates = isConceptDatesVisible(mainGroup.id);
                const notRequiredGroup = appendNotRequired
                    ? groups.find(g => Number(g.id) === 10)
                    : null;

                return `
                    <div class="table id-table">
                        <div class="thead">
                            ${{buildConceptTableHeader(mainGroup, showDates)}}
                        </div>
                        <div>
                            ${{renderConceptTableGroup(mainGroup, showDates)}}
                            ${{appendNotRequired && notRequiredGroup && hasItemsInGroup(10) ? renderConceptTableGroup(notRequiredGroup, false) : ''}}
                        </div>
                    </div>
                `;
            }}

            function renderConceptTables() {{
                if (!leftTableEl || !middleTableEl || !rightTableEl || !tablesGridEl) {{
                    throw new Error('concept table containers not found');
                }}

                const conceptGroup = groups.find(g => Number(g.id) === 1) || {{ id: 1, title: 'Концепция' }};

                tablesGridEl.classList.remove('id-three-cols');
                tablesGridEl.style.gridTemplateColumns = 'clamp(620px, 37vw, 760px)';
                tablesGridEl.style.justifyContent = 'start';

                if (tablePanels[0]) {{
                    tablePanels[0].style.display = '';
                    tablePanels[0].style.flex = '0 0 auto';
                    tablePanels[0].style.width = 'clamp(620px, 37vw, 760px)';
                    tablePanels[0].style.maxWidth = 'clamp(620px, 37vw, 760px)';
                }}
                if (tablePanels[1]) {{
                    tablePanels[1].style.display = 'none';
                    tablePanels[1].style.flex = '';
                    tablePanels[1].style.maxWidth = '';
                }}
                if (tablePanels[2]) {{
                    tablePanels[2].style.display = 'none';
                    tablePanels[2].style.flex = '';
                    tablePanels[2].style.maxWidth = '';
                }}

                leftTableEl.style.width = '100%';
                leftTableEl.style.maxWidth = '100%';

                leftTableEl.innerHTML = renderConceptPanel(conceptGroup, true);
                middleTableEl.innerHTML = '';
                rightTableEl.innerHTML = '';
            }}

            function renderTables() {{
                if (currentChecklistKey === 'concept') {{
                    renderConceptTables();
                    return;
                }}

                if (currentChecklistKey === 'id') {{
                    renderIdTables();
                    return;
                }}

                if (currentChecklistKey === 'opr') {{
                    renderOprTables();
                    return;
                }}

                if (tablesGridEl) {{
                    tablesGridEl.classList.remove('id-three-cols');
                    tablesGridEl.style.gridTemplateColumns = '1fr 1fr';
                }}

                if (tablePanels[0]) tablePanels[0].style.display = '';
                if (tablePanels[1]) tablePanels[1].style.display = '';
                if (tablePanels[2]) tablePanels[2].style.display = 'none';

                if (leftTableEl) leftTableEl.innerHTML = idTableShellHtml;
                if (middleTableEl) middleTableEl.innerHTML = idTableShellHtml;

                const leftBody = document.getElementById('leftTableBody');
                const middleBody = document.getElementById('middleTableBody');

                if (!leftBody || !middleBody) {{
                    throw new Error('leftTableBody or middleTableBody not found');
                }}

                const leftGroups = groups.filter(g => Number(g.id) === 1 || Number(g.id) === 3);
                const rightGroups = groups.filter(g => Number(g.id) === 2);

                if (hasItemsInGroup(4)) {{
                    const notRequiredGroup = groups.find(g => Number(g.id) === 4);
                    if (notRequiredGroup) {{
                        rightGroups.push(notRequiredGroup);
                    }}
                }}

                leftBody.innerHTML = leftGroups.map(renderGroup).join('');
                middleBody.innerHTML = rightGroups.map(renderGroup).join('');
            }}
            function renderAll() {{
                renderTables();
                bindEvents();
                calculateProgress();
                renderTitle();
                renderProjectChecklistList();
                renderProjectRootFolderButton();

                if (progressBoxEl) {{
                    progressBoxEl.classList.toggle('id-accent', currentChecklistKey === 'id' || currentChecklistKey === 'opr'|| currentChecklistKey === 'concept');
                }}

                updateDebugPanelAccess();
                syncChecklistCache();
            }}
            function replaceItem(updatedItem) {{
                if (!updatedItem) return;

                const normalizedItem = Object.assign({{
                    folderKey: '',
                    folderPath: '',
                    folderUrl: '',
                    documents: [],
                    documentUrl: '',
                    documentName: ''
                }}, updatedItem || {{}});

                normalizedItem.documents = Array.isArray(normalizedItem.documents) ? normalizedItem.documents : [];

                if (!Object.prototype.hasOwnProperty.call(normalizedItem, 'documentUrl')) {{
                    normalizedItem.documentUrl = '';
                }}
                if (!Object.prototype.hasOwnProperty.call(normalizedItem, 'documentName')) {{
                    normalizedItem.documentName = '';
                }}

                const idx = items.findIndex(x => x.id === normalizedItem.id);
                if (idx >= 0) {{
                    items[idx] = Object.assign({{}}, items[idx], normalizedItem, {{
                        folderKey: normalizedItem.folderKey || '',
                        folderPath: normalizedItem.folderPath || '',
                        folderUrl: normalizedItem.folderUrl || '',
                        documents: normalizedItem.documents || [],
                        documentUrl: normalizedItem.documentUrl || '',
                        documentName: normalizedItem.documentName || ''
                    }});
                }} else {{
                    items.push(normalizedItem);
                }}
            }}
            function bindEvents() {{
                document.querySelectorAll('[data-role="concept-status"]').forEach(el => {{
                    const commitConceptStatus = function () {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;

                        const oldValue = item.status || '';
                        let newValue = this.value;

                        if (String(item.statusKind || '') === 'text') {{
                            newValue = formatConceptTextStatus(newValue, item.statusPlaceholder || '');
                            this.value = newValue;
                        }}

                        if (newValue === 'Не требуется') {{
                            item.group = 10;
                        }} else if (Number(item.group) === 10) {{
                            item.group = resolveConceptGroupId(item);
                        }}

                        item.status = newValue;
                        pushSessionChange(item.id, item.name, 'status', oldValue, newValue);
                        renderAll();
                    }};

                    const item = items.find(x => x.id === el.dataset.itemId);
                    if (!item) return;

                    if (String(item.statusKind || '') === 'text') {{
                        el.addEventListener('blur', commitConceptStatus);
                        el.addEventListener('keydown', function (e) {{
                            if (e.key === 'Enter') {{
                                e.preventDefault();
                                commitConceptStatus.call(this);
                            }}
                        }});
                    }} else {{
                        el.addEventListener('change', commitConceptStatus);
                    }}
                }});

                document.querySelectorAll('[data-role="concept-extra"]').forEach(el => {{
                    el.dataset.initialValue = String(el.value || '');
                    el.dataset.baseHeight = '32';
                    el.style.height = '32px';
                    el.addEventListener('input', function () {{
                        autoGrowTextarea(this);
                    }});

                    const handler = function () {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;

                        const oldValue = item.extraInfo || '';
                        const newValue = this.value;
                        item.extraInfo = newValue;
                        pushSessionChange(item.id, item.name, 'extraInfo', oldValue, newValue);
                        renderAll();
                    }};

                    el.addEventListener('change', handler);
                    el.addEventListener('blur', handler);
                }});
                document.querySelectorAll('[data-role="status"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;

                        const oldItem = JSON.parse(JSON.stringify(item));
                        const newValue = this.value;
                        const oldDocuments = getItemDocuments(oldItem);

                        if (newValue === 'Нет' && oldDocuments.length && !confirmStatusNoWithFiles(item.name, oldDocuments)) {{
                            this.value = normalizeStatus(oldItem.status);
                            return;
                        }}

                        if (newValue === 'Нет') {{
                            try {{
                                const result = await updateItem(item.id, 'status', newValue);
                                if (!result || !result.item) {{
                                    throw new Error('status save failed');
                                }}

                                replaceItem(result.item);

                                pushSessionChange(item.id, item.name, 'status', oldItem.status || '', newValue || '');
                                debugLog('status_changed', {{
                                    itemId: item.id,
                                    itemName: item.name,
                                    oldValue: oldItem.status || '',
                                    newValue: newValue || ''
                                }});

                                if (oldDocuments.length) {{
                                    const removedNames = oldDocuments.map(x => x.name || 'uploaded').join(', ');
                                    pushSessionChange(item.id, item.name, 'document', removedNames, 'Удален');
                                    debugLog('document_removed_by_status', {{
                                        itemId: item.id,
                                        itemName: item.name,
                                        oldValue: removedNames,
                                        newValue: 'Удален'
                                    }});
                                }}

                                renderAll();
                            }} catch (e) {{
                                console.log('status save error:', e);
                                setSaveState('error', 'Ошибка сохранения статуса');
                                this.value = normalizeStatus(oldItem.status);
                            }}
                            return;
                        }}

                        pushSessionChange(item.id, item.name, 'status', oldItem.status || '', newValue || '');
                        debugLog('status_changed', {{
                            itemId: item.id,
                            itemName: item.name,
                            oldValue: oldItem.status || '',
                            newValue: newValue || ''
                        }});

                        item.status = newValue;
                        renderAll();
                    }});
                }});

                document.querySelectorAll('[data-role="plan"]').forEach(el => {{
                    el.addEventListener('change', function() {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldItem = JSON.parse(JSON.stringify(item));
                        const newValue = fromInputDate(this.value);

                        item.plan = newValue;
                        pushSessionChange(item.id, item.name, 'plan', oldItem.plan || '', newValue || '');
                        debugLog('plan_changed', {{
                            itemId: item.id,
                            itemName: item.name,
                            oldValue: oldItem.plan || '',
                            newValue: newValue || ''
                        }});

                        renderAll();
                    }});
                }});

                document.querySelectorAll('[data-role="fact"]').forEach(el => {{
                    el.addEventListener('change', function() {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldItem = JSON.parse(JSON.stringify(item));
                        const newValue = fromInputDate(this.value);

                        item.fact = newValue;
                        pushSessionChange(item.id, item.name, 'fact', oldItem.fact || '', newValue || '');
                        debugLog('fact_changed', {{
                            itemId: item.id,
                            itemName: item.name,
                            oldValue: oldItem.fact || '',
                            newValue: newValue || ''
                        }});

                        renderAll();
                    }});
                }});

                document.querySelectorAll('[data-role="add-item"]').forEach(btn => {{
                    btn.addEventListener('click', async function() {{
                        const groupId = Number(this.dataset.groupId);
                        const input = document.getElementById('addItemInput_' + groupId);
                        if (!input) return;

                        const name = (input.value || '').trim();
                        if (!name) return;

                        this.disabled = true;

                        try {{
                            const result = await addItem(groupId, name, currentChecklistKey);
                            if (!result || !result.item) {{
                                throw new Error('add item failed');
                            }}

                            replaceItem(result.item);
                            pushSessionChange(result.item.id, result.item.name, 'add-item', '', result.item.name);

                            debugLog('item_added', {{
                                itemId: result.item.id,
                                itemName: result.item.name,
                                groupId: groupId
                            }});

                            input.value = '';
                            renderAll();
                        }} catch (e) {{
                            console.log('add item error:', e);
                            setSaveState('error', 'Ошибка добавления пункта');
                        }} finally {{
                            this.disabled = false;
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="toggle-id-dates"]').forEach(btn => {{
                    btn.addEventListener('click', function () {{
                        const groupId = Number(this.dataset.groupId || 0);
                        if (![1, 2, 3].includes(groupId)) return;

                        idDateVisibility[groupId] = !idDateVisibility[groupId];
                        renderAll();
                    }});
                }});

                document.querySelectorAll('[data-role="toggle-concept-dates"]').forEach(btn => {{
                    btn.addEventListener('click', function () {{
                        const groupId = Number(this.dataset.groupId || 0);
                        if (groupId !== 1) return;

                        conceptDateVisibility[groupId] = !conceptDateVisibility[groupId];
                        renderAll();
                    }});
                }});

                document.querySelectorAll('[data-role="concept-add-item"]').forEach(btn => {{
                    btn.addEventListener('click', async function() {{
                        const groupId = Number(this.dataset.groupId);
                        const input = document.getElementById('conceptAddItemInput_' + groupId);
                        if (!input) return;

                        const name = (input.value || '').trim();
                        if (!name) return;

                        this.disabled = true;

                        try {{
                            const result = await addItem(groupId, name, 'concept');
                            if (!result || !result.item) {{
                                throw new Error('add concept item failed');
                            }}

                            replaceItem(result.item);
                            pushSessionChange(result.item.id, result.item.name, 'add-item', '', result.item.name);

                            debugLog('item_added', {{
                                itemId: result.item.id,
                                itemName: result.item.name,
                                groupId: groupId
                            }});

                            input.value = '';
                            renderAll();
                        }} catch (e) {{
                            console.log('concept add item error:', e);
                            setSaveState('error', 'Ошибка добавления пункта');
                        }} finally {{
                            this.disabled = false;
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="upload"]').forEach(btn => {{
                    btn.addEventListener('click', function() {{
                        const input = document.querySelector('[data-role="file-input"][data-item-id="' + this.dataset.itemId + '"]');
                        if (input) input.click();
                    }});
                }});

                document.querySelectorAll('[data-role="view-folder"]').forEach(btn => {{
                    btn.addEventListener('click', function () {{
                        const folderUrl = this.dataset.folderUrl || '';
                        if (!folderUrl) return;

                        try {{
                            window.open(folderUrl, '_blank');
                        }} catch (e) {{
                            console.log('open folder error:', e);
                            setSaveState('error', 'Ошибка открытия папки');
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="view-file"]').forEach(link => {{
                    link.addEventListener('click', function (event) {{
                        event.preventDefault();
                        event.stopPropagation();

                        const openUrl = this.dataset.openUrl || '';
                        if (!openUrl) return;

                        try {{
                            const absoluteUrl = new URL(openUrl, window.location.href).href;
                            window.open(absoluteUrl, '_blank', 'noopener,noreferrer');
                        }} catch (e) {{
                            console.log('open file error:', e);
                            setSaveState('error', 'Ошибка открытия файла');
                        }}
                    }});
                }});
                document.querySelectorAll('[data-role="file-input"]').forEach(input => {{
                    input.addEventListener('change', async function() {{
                        const itemId = this.dataset.itemId;
                        const files = Array.from(this.files || []);
                        if (!files.length) return;

                        const item = items.find(x => x.id === itemId);
                        const initialStatus = item ? normalizeStatus(item.status) : '';
                        let currentStatus = initialStatus;

                        try {{
                            for (const file of files) {{
                                const result = await uploadDocument(itemId, file);
                                replaceItem(result.item);

                                const uploadedDocs = getItemDocuments(result.item);
                                let uploadedDoc = uploadedDocs.find(x => String(x.name || '') === String(file.name || ''));

                                if (!uploadedDoc && uploadedDocs.length) {{
                                    uploadedDoc = uploadedDocs[uploadedDocs.length - 1];
                                }}

                                sessionChanges.push({{
                                    field: 'document',
                                    itemId: result.item ? result.item.id : itemId,
                                    itemName: result.item ? result.item.name : (item ? item.name : ''),
                                    oldValue: '',
                                    newValue: uploadedDoc ? (uploadedDoc.name || file.name || 'uploaded') : (file.name || 'uploaded')
                                }});
                                sessionDirty = true;

                                const newStatus = result.item ? normalizeStatus(result.item.status) : currentStatus;
                                if (newStatus !== currentStatus) {{
                                    sessionChanges.push({{
                                        field: 'status',
                                        itemId: result.item ? result.item.id : itemId,
                                        itemName: result.item ? result.item.name : (item ? item.name : ''),
                                        oldValue: currentStatus || '',
                                        newValue: newStatus || ''
                                    }});
                                    sessionDirty = true;
                                    currentStatus = newStatus;
                                }}
                            }}

                            renderAll();
                        }} catch (e) {{
                            console.log(e);
                            setSaveState('error', 'Ошибка загрузки файлов');
                        }} finally {{
                            this.value = '';
                        }}
                    }});
                }});
            }}

            const baseLoadChecklistByKey = loadChecklistByKey;
            loadChecklistByKey = async function (checklistKey) {{
                const targetKey = String(checklistKey || '').trim() || 'id';
                if (targetKey !== 'opr') {{
                    return baseLoadChecklistByKey(targetKey);
                }}
                if (targetKey === currentChecklistKey) {{
                    return;
                }}

                await flushCurrentChecklistSummary();
                setSaveState('saving', 'Загружаем...');

                try {{
                    const cachedData = checklistCache[targetKey];
                    if (cachedData) {{
                        applyChecklistData(deepClone(cachedData));
                        renderAll();
                        debugLog('checklist_switched_cached', {{
                            checklistKey: targetKey
                        }});
                        setSaveState('', 'Сохранено');
                        return;
                    }}

                    const response = await fetch(
                        appUrl('api/checklist') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(targetKey)
                    );
                    const result = await response.json();

                    if (!response.ok) {{
                        throw new Error(result.error || 'load checklist failed');
                    }}

                    applyChecklistData(result);
                    renderAll();
                    debugLog('checklist_switched', {{
                        checklistKey: targetKey
                    }});
                    setSaveState('', 'Сохранено');
                }} catch (e) {{
                    console.log('loadChecklistByKey error:', e);
                    setSaveState('error', 'Ошибка загрузки чек-листа');
                }}
            }};
            {popup_session_enhancements_js}

            function safeInitBx24ForPopup() {{
                function applyPopupWindowSize() {{
                    try {{
                        if (typeof window.BX24.resizeWindow === 'function') {{
                            window.BX24.resizeWindow(1180, 720);
                        }}
                        if (typeof window.BX24.fitWindow === 'function') {{
                            window.BX24.fitWindow();
                        }}
                    }} catch (e) {{
                        console.log('BX24 popup sizing error:', e);
                    }}
                }}

                try {{
                    if (window.BX24 && typeof window.BX24.init === 'function') {{
                        window.BX24.init(function () {{
                            applyPopupWindowSize();
                            setTimeout(applyPopupWindowSize, 80);
                            setTimeout(applyPopupWindowSize, 220);
                        }});
                    }}
                }} catch (e) {{
                    console.log('BX24.init skipped:', e);
                }}
            }}
            try {{
                logRenderState('before_renderAll');
                renderAll();
                logRenderState('after_renderAll');

                debugLog('popup_loaded', {{
                    href: window.location.href,
                    hasDialogId: !!dialogId
                }});
            }} catch (e) {{
                logRenderError('renderAll', e);
            }}

            try {{
                fetchChatTitleIfMissing();
            }} catch (e) {{
                logRenderError('fetchChatTitleIfMissing', e);
            }}

            try {{
                fetchCurrentUserIfPossible();
            }} catch (e) {{
                logRenderError('fetchCurrentUserIfPossible', e);
            }}

            try {{
                safeInitBx24ForPopup();
            }} catch (e) {{
                logRenderError('safeInitBx24ForPopup', e);
            }}
        </script>
    </body>
    </html>
    """

@app.get("/api/checklist")
def api_checklist(dialogId: str = "", checklistKey: str = "id"):
    dialogId = normalize_dialog_id(dialogId)
    checklistKey = normalize_checklist_key(checklistKey)
    return JSONResponse(get_checklist(dialogId, checklistKey))


@app.post("/api/checklist/update-item")
async def api_checklist_update_item(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    item_id = str(payload.get("itemId") or "").strip()
    field = str(payload.get("field") or "").strip()
    value = payload.get("value")

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    if checklist_key == "concept":
        allowed_fields = {"priority", "status", "plan", "fact"}
    elif checklist_key == "opr":
        allowed_fields = {"priority", "status", "plan", "fact"}
    else:
        allowed_fields = {"priority", "status", "plan", "fact"}

    if field not in allowed_fields:
        return JSONResponse({"ok": False, "error": "invalid field"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", [])

    target_item = None

    for index, item in enumerate(items):
        if str(item.get("id") or "") == item_id:
            target_item = migrate_legacy_document_fields(item)
            items[index] = target_item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

    if field == "priority":
        target_item["priority"] = normalize_priority(value)
    elif field == "status":
        if checklist_key == "concept":
            new_status = normalize_status(value)
            target_item["status"] = new_status
            target_item["priority"] = derive_indicator_from_status(new_status)

            if new_status == "Нет":
                cleared_item = remove_all_item_documents(dialog_id, checklist_key, item_id, target_item)
                target_item.clear()
                target_item.update(cleared_item)

            if new_status == "Не требуется":
                target_item["group"] = 10
            elif int(target_item.get("group") or 0) == 10:
                target_item["group"] = resolve_concept_group_id_by_item_id_or_name(target_item)

        elif checklist_key == "opr":
            new_status = normalize_status(value)
            target_item["status"] = new_status
            target_item["priority"] = derive_indicator_from_status(new_status)

            if new_status == "Нет":
                cleared_item = remove_all_item_documents(dialog_id, checklist_key, item_id, target_item)
                target_item.clear()
                target_item.update(cleared_item)

            if new_status == "Не требуется":
                target_item["group"] = 2
            elif int(target_item.get("group") or 0) == 2:
                target_item["group"] = resolve_opr_group_id_by_item_id_or_name(target_item)

        else:
            new_status = normalize_status(value)
            target_item["status"] = new_status
            target_item["priority"] = derive_indicator_from_status(new_status)

            if new_status == "Нет":
                cleared_item = remove_all_item_documents(dialog_id, checklist_key, item_id, target_item)
                target_item.clear()
                target_item.update(cleared_item)

            if new_status == "Не требуется":
                target_item["group"] = 4
            elif int(target_item.get("group") or 0) == 4:
                target_item["group"] = move_item_to_required_group(target_item)
    elif field == "plan":
        target_item["plan"] = normalize_date_string(value)
    elif field == "fact":
        target_item["fact"] = normalize_date_string(value)

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

@app.post("/api/integrations/n8n/project-storage-context")
async def api_project_storage_context(request: Request):
    expected_token = N8N_SHARED_TOKEN
    provided_token = (request.headers.get("X-N8N-Token") or "").strip()

    if expected_token and provided_token != expected_token:
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    project_name = clean_cell_value(payload.get("projectName"))
    yandex_disk = payload.get("yandexDisk") or {}
    item_mappings = payload.get("itemMappings") or []

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not project_name:
        return JSONResponse({"ok": False, "error": "projectName is required"}, status_code=400)

    if not isinstance(yandex_disk, dict) or not yandex_disk:
        return JSONResponse({"ok": False, "error": "yandexDisk is required"}, status_code=400)

    if not isinstance(item_mappings, list):
        return JSONResponse({"ok": False, "error": "itemMappings must be a list"}, status_code=400)

    try:
        save_project_storage_context(dialog_id, payload)
    except Exception as e:
        return JSONResponse({
            "ok": False,
            "error": "failed to save storage context",
            "details": str(e),
        }, status_code=500)

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "projectId": str(payload.get("projectId") or "").strip(),
        "projectName": project_name,
        "provider": str(yandex_disk.get("provider") or "yandex_disk").strip(),
        "stored": True,
        "mappingCount": len(item_mappings),
    })

@app.get("/api/integrations/n8n/project-storage-context")
def api_get_project_storage_context(dialogId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    context = get_project_storage_context(dialog_id)
    if not context:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)

    return JSONResponse({
        "ok": True,
        "context": context
    })

@app.post("/api/checklist/update-meta")
async def api_checklist_update_meta(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
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
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    group_id = int(payload.get("groupId") or 0)
    name = clean_cell_value(payload.get("name"))

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if checklist_key == "id" and group_id not in [1, 2, 3]:
        return JSONResponse({"ok": False, "error": "invalid groupId"}, status_code=400)

    if checklist_key == "concept" and group_id not in [group["id"] for group in CONCEPT_GROUPS if group["id"] != 10]:
        return JSONResponse({"ok": False, "error": "invalid groupId"}, status_code=400)

    if checklist_key == "opr" and group_id not in [group["id"] for group in OPR_GROUPS if group["id"] != 2]:
        return JSONResponse({"ok": False, "error": "invalid groupId"}, status_code=400)

    if not name:
        return JSONResponse({"ok": False, "error": "name is required"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    group_items = [x for x in items if int(x.get("group") or 0) == group_id]
    next_order = len(group_items) + 1

    yandex_folder_warning = ""

    if checklist_key == "concept":
        new_item_id = f"concept_g{group_id}_custom_{uuid.uuid4().hex[:8]}"
        new_item = {
            "id": new_item_id,
            "group": group_id,
            "order": next_order,
            "name": name,
            "priority": "white",
            "status": "",
            "plan": "",
            "fact": "",
            "folderKey": build_folder_key("concept", name, new_item_id),
            "folderPath": "",
            "folderUrl": "",
            "documents": [],
            "documentUrl": "",
            "documentName": "",
            "isCustom": True,
        }

    elif checklist_key == "opr":
        new_item_id = f"opr_g{group_id}_custom_{uuid.uuid4().hex[:8]}"
        new_item = {
            "id": new_item_id,
            "group": group_id,
            "order": next_order,
            "name": name,
            "priority": "white",
            "status": "",
            "plan": "",
            "fact": "",
            "folderKey": build_folder_key("opr", name, new_item_id),
            "folderPath": "",
            "folderUrl": "",
            "documents": [],
            "documentUrl": "",
            "documentName": "",
            "isCustom": True,
        }

        can_create_yandex_folder = False

        try:
            can_create_yandex_folder = can_create_custom_item_yandex_folder(dialog_id, checklist_key)
        except Exception as e:
            yandex_folder_warning = str(e)
            write_debug_log("custom_opr_folder_check_failed", {
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "groupId": group_id,
                "itemId": new_item_id,
                "itemName": name,
                "error": str(e),
            })

        if can_create_yandex_folder:
            try:
                ensure_yandex_folder_for_custom_opr_item(
                    dialog_id=dialog_id,
                    checklist_key=checklist_key,
                    group_id=group_id,
                    item_name=name,
                    item_id=new_item_id,
                )

                folder_info = get_item_yandex_folder(dialog_id, checklist_key, name)
                if folder_info:
                    folder = folder_info.get("folder") or {}
                    new_item["folderPath"] = clean_cell_value(folder.get("path"))
                    new_item["folderUrl"] = clean_cell_value(folder.get("url"))
            except Exception as e:
                yandex_folder_warning = str(e)
                write_debug_log("custom_opr_folder_create_failed", {
                    "dialogId": dialog_id,
                    "checklistKey": checklist_key,
                    "groupId": group_id,
                    "itemId": new_item_id,
                    "itemName": name,
                    "error": str(e),
                })

    else:
        new_item_id = f"id_g{group_id}_custom_{uuid.uuid4().hex[:8]}"
        new_item = {
            "id": new_item_id,
            "group": group_id,
            "order": next_order,
            "name": name,
            "priority": "white",
            "status": "",
            "plan": "",
            "fact": "",
            "folderKey": build_folder_key("id", name, new_item_id),
            "folderPath": "",
            "folderUrl": "",
            "documents": [],
            "documentUrl": "",
            "documentName": "",
            "isCustom": True,
        }

        can_create_yandex_folder = False

        try:
            can_create_yandex_folder = can_create_custom_item_yandex_folder(dialog_id, checklist_key)
        except Exception as e:
            yandex_folder_warning = str(e)
            write_debug_log("custom_id_folder_check_failed", {
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "groupId": group_id,
                "itemId": new_item_id,
                "itemName": name,
                "error": str(e),
            })

        if can_create_yandex_folder:
            try:
                ensure_yandex_folder_for_custom_item(
                    dialog_id=dialog_id,
                    checklist_key=checklist_key,
                    group_id=group_id,
                    item_name=name,
                    item_id=new_item_id,
                )

                folder_info = get_item_yandex_folder(dialog_id, checklist_key, name)
                if folder_info:
                    folder = folder_info.get("folder") or {}
                    new_item["folderPath"] = clean_cell_value(folder.get("path"))
                    new_item["folderUrl"] = clean_cell_value(folder.get("url"))
            except Exception as e:
                yandex_folder_warning = str(e)
                write_debug_log("custom_id_folder_create_failed", {
                    "dialogId": dialog_id,
                    "checklistKey": checklist_key,
                    "groupId": group_id,
                    "itemId": new_item_id,
                    "itemName": name,
                    "error": str(e),
                })

    items.append(new_item)
    data["items"] = items
    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)

    created_item = None
    for item in data.get("items", []):
        if str(item.get("id")) == new_item["id"]:
            created_item = item
            break

    response_payload = {
        "ok": True,
        "dialogId": dialog_id,
        "item": created_item or new_item,
        "progressPercent": data.get("progressPercent", 0),
    }

    if yandex_folder_warning:
        response_payload["yandexFolderWarning"] = yandex_folder_warning

    return JSONResponse(response_payload)

@app.post("/api/checklist/upload-document")
async def api_checklist_upload_document(
    dialogId: str = Form(...),
    itemId: str = Form(...),
    file: UploadFile = File(...),
    checklistKey: str = Form("id"),
    itemGroup: str = Form("")
):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    item_id = str(itemId or "").strip()
    item_group = int(str(itemGroup or "0").strip() or 0)

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    target_item = None
    for index, item in enumerate(items):
        if str(item.get("id") or "") == item_id:
            target_item = migrate_legacy_document_fields(item)
            items[index] = target_item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

    rel_path = build_upload_rel_path(dialog_id, item_id, file.filename or "file.bin")
    abs_path = UPLOAD_ROOT / rel_path
    abs_path.parent.mkdir(parents=True, exist_ok=True)

    file_bytes = await file.read()
    with open(abs_path, "wb") as f:
        f.write(file_bytes)

    file_url = "/uploads/" + rel_path.replace("\\", "/")
    document_id = uuid.uuid4().hex
    document_view_url = build_document_view_url(dialog_id, checklist_key, item_id, document_id)
    folder_view_url = build_folder_view_url(dialog_id, checklist_key, item_id)

    try:
        folder_path = "/" + str(abs_path.parent.relative_to(BASE_DIR)).replace("\\", "/")
    except Exception:
        folder_path = file_url.rsplit("/", 1)[0]

    uploaded_name = Path(file.filename or "file.bin").name

    document_record = normalize_document_record({
        "id": document_id,
        "name": uploaded_name,
        "path": file_url,
        "fileUrl": file_url,
        "previewUrl": document_view_url,
        "size": len(file_bytes),
        "modifiedAt": datetime.now().isoformat(timespec="seconds"),
        "source": "local",

        "mirrorStatus": "",
        "mirrorError": "",
        "yandexPath": "",
        "yandexFileUrl": "",
        "yandexFolderAlias": "",
    })

    try:
        mirror_result = mirror_document_to_yandex(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_name=clean_cell_value(target_item.get("name")),
            filename=uploaded_name,
            file_bytes=file_bytes,
            item_id=str(target_item.get("id") or ""),
            item_group=int(target_item.get("group") or 0),
            is_custom=bool(target_item.get("isCustom", False)),
        )

        if mirror_result.get("ok"):
            document_record["mirrorStatus"] = "synced"
            document_record["mirrorError"] = ""
            document_record["yandexPath"] = clean_cell_value(mirror_result.get("filePath"))
            document_record["yandexFileUrl"] = clean_cell_value(mirror_result.get("folderUrl"))
            document_record["yandexFolderAlias"] = clean_cell_value(mirror_result.get("folderAlias"))
        else:
            document_record["mirrorStatus"] = "error"
            document_record["mirrorError"] = clean_cell_value(mirror_result.get("reason")) or "mirror failed"
    except Exception as e:
        document_record["mirrorStatus"] = "error"
        document_record["mirrorError"] = str(e)

    existing_documents = normalize_documents_list(target_item.get("documents"))
    existing_documents.append(document_record)
    normalized_documents = normalize_documents_list(existing_documents)

    target_item["documents"] = normalized_documents
    target_item["folderPath"] = folder_path
    target_item["folderUrl"] = folder_view_url if normalized_documents else ""

    first_doc = normalized_documents[0] if normalized_documents else {}
    target_item["documentUrl"] = clean_cell_value(first_doc.get("fileUrl"))
    target_item["documentName"] = clean_cell_value(first_doc.get("name"))

    actual_group = int(target_item.get("group") or item_group or 0)
    if checklist_key == "id" and actual_group != 4:
        target_item["status"] = "Есть"
        target_item["priority"] = derive_indicator_from_status("Есть")
    elif checklist_key == "opr" and actual_group != 2:
        target_item["status"] = "Есть"
        target_item["priority"] = derive_indicator_from_status("Есть")
    elif checklist_key == "concept" and actual_group != 10:
        target_item["status"] = "Есть"
        target_item["priority"] = derive_indicator_from_status("Есть")

    data["items"] = items
    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)

    updated_item = None
    for item in data.get("items", []):
        if str(item.get("id") or "") == item_id:
            updated_item = item
            break

    if not updated_item:
        return JSONResponse({"ok": False, "error": "updated item not found"}, status_code=500)

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "item": updated_item,
        "progressPercent": data.get("progressPercent", 0),
    })


@app.post("/api/checklist/remove-document")
async def api_checklist_remove_document(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    item_id = str(payload.get("itemId") or "").strip()
    document_id = clean_cell_value(payload.get("documentId"))
    document_url = clean_cell_value(payload.get("documentUrl"))
    preserve_status = bool(payload.get("preserveStatus"))
    acting_user_id = clean_cell_value(payload.get("actingUserId"))
    acting_user_name = clean_cell_value(payload.get("actingUserName")) or "Пользователь"

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    if not can_user_delete_files(acting_user_id):
        return JSONResponse({
            "ok": False,
            "error": "У вас недостаточно прав на удаление файлов"
        }, status_code=403)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    target_item = None
    for index, item in enumerate(items):
        if str(item.get("id") or "") == item_id:
            target_item = migrate_legacy_document_fields(item)
            items[index] = target_item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

    documents = normalize_documents_list(target_item.get("documents"))

    doc_to_remove = None

    if document_id:
        for doc in documents:
            if str(doc.get("id") or "") == document_id:
                doc_to_remove = doc
                break

    if not doc_to_remove and document_url:
        for doc in documents:
            doc_file_url = clean_cell_value(doc.get("fileUrl"))
            doc_preview_url = clean_cell_value(doc.get("previewUrl"))
            doc_path = clean_cell_value(doc.get("path"))
            if document_url in {doc_file_url, doc_preview_url, doc_path}:
                doc_to_remove = doc
                break

    if not doc_to_remove and documents:
        doc_to_remove = documents[0]

    if doc_to_remove:
        local_document_url = (
            clean_cell_value(doc_to_remove.get("fileUrl"))
            or clean_cell_value(doc_to_remove.get("previewUrl"))
            or clean_cell_value(doc_to_remove.get("path"))
        )

        remove_item_document_file({
            "documentUrl": local_document_url
        })

        yandex_path = clean_cell_value(doc_to_remove.get("yandexPath"))
        if yandex_path and is_yandex_disk_enabled():
            try:
                yandex_disk_delete_path(yandex_path, permanently=True)
            except Exception as e:
                write_debug_log("yandex_mirror_delete_error", {
                    "dialogId": dialog_id,
                    "checklistKey": checklist_key,
                    "itemId": item_id,
                    "documentId": clean_cell_value(doc_to_remove.get("id")),
                    "yandexPath": yandex_path,
                    "error": str(e),
                })

    remaining_documents = []
    removed = False

    for doc in documents:
        same_id = document_id and str(doc.get("id") or "") == document_id
        same_url = document_url and document_url in {
            clean_cell_value(doc.get("fileUrl")),
            clean_cell_value(doc.get("previewUrl")),
            clean_cell_value(doc.get("path")),
        }

        if not removed and (same_id or same_url or (doc_to_remove and str(doc.get("id") or "") == str(doc_to_remove.get("id") or ""))):
            removed = True
            continue

        remaining_documents.append(doc)

    normalized_documents = normalize_documents_list(remaining_documents)
    target_item["documents"] = normalized_documents

    first_doc = normalized_documents[0] if normalized_documents else {}
    target_item["documentUrl"] = clean_cell_value(first_doc.get("fileUrl"))
    target_item["documentName"] = clean_cell_value(first_doc.get("name"))

    if normalized_documents:
        first_file_url = clean_cell_value(first_doc.get("fileUrl"))
        target_item["folderPath"] = first_file_url.rsplit("/", 1)[0] if first_file_url.startswith("/") else ""
        target_item["folderUrl"] = build_folder_view_url(dialog_id, checklist_key, item_id)
    else:
        target_item["folderPath"] = ""
        target_item["folderUrl"] = ""
        target_item["documentUrl"] = ""
        target_item["documentName"] = ""

        if checklist_key == "id" and int(target_item.get("group") or 0) != 4 and not preserve_status:
            target_item["status"] = ""
            target_item["priority"] = "white"
        elif checklist_key == "opr" and int(target_item.get("group") or 0) != 2 and not preserve_status:
            target_item["status"] = ""
            target_item["priority"] = "white"

    data["items"] = items
    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)

    updated_item = None
    for item in data.get("items", []):
        if str(item.get("id") or "") == item_id:
            updated_item = item
            break

    if not updated_item:
        return JSONResponse({"ok": False, "error": "updated item not found"}, status_code=500)

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "item": updated_item,
        "progressPercent": data.get("progressPercent", 0),
    })

@app.get("/api/checklist/folder", response_class=HTMLResponse)
def api_checklist_folder(dialogId: str = "", itemId: str = "", checklistKey: str = "id"):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    item_id = str(itemId or "").strip()

    if not dialog_id or not item_id:
        return HTMLResponse("<h3>Не переданы dialogId или itemId</h3>", status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    target_item = None

    for index, item in enumerate(items):
        if str(item.get("id") or "") == item_id:
            target_item = migrate_legacy_document_fields(item)
            items[index] = target_item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)
    documents = normalize_documents_list(target_item.get("documents"))
    yandex_folder_data = get_item_yandex_folder(
        dialog_id,
        checklist_key,
        clean_cell_value(target_item.get("name"))
    )
    yandex_folder = (yandex_folder_data or {}).get("folder") or {}
    yandex_folder_url = clean_cell_value(yandex_folder.get("url"))
    yandex_folder_path = clean_cell_value(yandex_folder.get("path"))
    rows = []
    for doc in documents:
        doc_id = str(doc.get("id") or "")
        doc_name = html.escape(str(doc.get("name") or "Файл"))
        doc_size = html.escape(format_file_size(doc.get("size") or 0))
        open_url = build_document_view_url(dialog_id, checklist_key, item_id, doc_id)
        download_url = open_url + "&download=1"

        rows.append(f"""
            <tr>
                <td style="padding:10px 12px;border-bottom:1px solid #edf0f2;">{doc_name}</td>
                <td style="padding:10px 12px;border-bottom:1px solid #edf0f2;white-space:nowrap;">{doc_size}</td>
                <td style="padding:10px 12px;border-bottom:1px solid #edf0f2;white-space:nowrap;">
                    <a href="{html.escape(open_url)}" target="_blank">Открыть</a>
                    &nbsp;|&nbsp;
                    <a href="{html.escape(download_url)}" target="_blank">Скачать</a>
                    &nbsp;|&nbsp;
                    <button
                        type="button"
                        data-role="folder-remove-file"
                        data-dialog-id="{html.escape(dialog_id)}"
                        data-checklist-key="{html.escape(checklist_key)}"
                        data-item-id="{html.escape(item_id)}"
                        data-document-id="{html.escape(doc_id)}"
                        data-document-name="{doc_name}"
                        style="border:none;background:transparent;color:#b42318;cursor:pointer;font-size:16px;line-height:1;padding:0 2px;"
                        title="Удалить файл"
                    >
                        ×
                    </button>
                </td>
            </tr>
        """)

    table_html = "".join(rows) if rows else """
        <tr>
            <td colspan="3" style="padding:14px 12px;color:#667085;">В папке пока нет файлов</td>
        </tr>
    """

    title = html.escape(str(target_item.get("name") or "Папка"))
    checklist_title = html.escape(str(data.get("title") or "Чек-лист"))
    remove_api_url = html.escape(f"{normalize_base_path(APP_BASE_PATH)}/api/checklist/remove-document")
    upload_api_url = html.escape(f"{normalize_base_path(APP_BASE_PATH)}/api/checklist/upload-document")
    folder_item_group = html.escape(str(target_item.get("group") or ""))

    yandex_folder_path_html = ""
    if yandex_folder_path and not yandex_folder_url:
        yandex_folder_path_html = f'''
            <div style="margin-top:12px;font-size:12px;color:#667085;">
                Папка Яндекс Диска: {html.escape(yandex_folder_path)}
            </div>
        '''

    folder_actions_html = f'''
        <div style="display:flex;gap:10px;align-items:center;justify-content:flex-end;flex-wrap:wrap;">
            <button
                type="button"
                id="folderUploadBtn"
                style="display:inline-block;padding:8px 12px;border:1px solid #d0d7de;border-radius:8px;background:#f8fafc;color:#1f2328;text-decoration:none;cursor:pointer;"
            >
                Загрузить файлы в папку пункта
            </button>
            <input type="file" id="folderUploadInput" style="display:none;" multiple>
            {f'''
                <a
                    href="{html.escape(yandex_folder_url)}"
                    target="_blank"
                    style="display:inline-block;padding:8px 12px;border:1px solid #d0d7de;border-radius:8px;background:#f8fafc;color:#1f2328;text-decoration:none;"
                >
                    Открыть папку на Яндекс Диске
                </a>
            ''' if yandex_folder_url else ''}
        </div>
    '''

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{title}</title>
    </head>
    <body style="font-family:Arial,sans-serif;background:#f8fafc;margin:0;padding:24px;color:#1f2328;">
        <div style="max-width:960px;margin:0 auto;background:#fff;border:1px solid #e5e7eb;border-radius:14px;overflow:hidden;">
            <div style="padding:16px 18px;border-bottom:1px solid #edf0f2;background:#fafbfc;">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;">
                    <div>
                        <div style="font-size:13px;color:#667085;margin-bottom:4px;">{checklist_title}</div>
                        <div style="font-size:22px;font-weight:700;">{title}</div>
                        {yandex_folder_path_html}
                    </div>
                    {folder_actions_html}
                </div>
            </div>
            <div style="padding:18px;">
                <table style="width:100%;border-collapse:collapse;">
                    <thead>
                        <tr>
                            <th style="text-align:left;padding:10px 12px;background:#f8fafc;border-bottom:1px solid #e5e7eb;">Файл</th>
                            <th style="text-align:left;padding:10px 12px;background:#f8fafc;border-bottom:1px solid #e5e7eb;">Размер</th>
                            <th style="text-align:left;padding:10px 12px;background:#f8fafc;border-bottom:1px solid #e5e7eb;">Действия</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_html}
                    </tbody>
                </table>
            </div>
        </div>
        <script>
            const folderRemoveApiUrl = "{remove_api_url}";
            const folderUploadApiUrl = "{upload_api_url}";
            const folderDialogId = "{html.escape(dialog_id)}";
            const folderChecklistKey = "{html.escape(checklist_key)}";
            const folderItemId = "{html.escape(item_id)}";
            const folderItemGroup = "{folder_item_group}";

            const folderDeleteAllowedUserIds = new Set([
                '108',
                '106',
                '114',
                '116',
                '72',
                '56',
                '26',
                '138',
                '18',
                '256',
                '140',
                '280',
                '124',
                '222'
            ]);

            function getFolderDeleteActor() {{
                try {{
                    const openerEditor = window.opener && window.opener.currentEditor
                        ? window.opener.currentEditor
                        : null;

                    return {{
                        id: String(openerEditor && openerEditor.id || '').trim(),
                        name: String(openerEditor && openerEditor.name || '').trim() || 'Пользователь'
                    }};
                }} catch (e) {{
                    return {{
                        id: '',
                        name: 'Пользователь'
                    }};
                }}
            }}

            function notifyParentChecklistDocumentChanged(messageType = 'checklist-document-changed', extraPayload = {{}}) {{
                try {{
                    if (window.opener && typeof window.opener.postMessage === 'function') {{
                        window.opener.postMessage({{
                            type: messageType,
                            dialogId: folderDialogId,
                            checklistKey: folderChecklistKey,
                            itemId: folderItemId,
                            ...extraPayload
                        }}, '*');
                    }}
                }} catch (e) {{
                    console.log('opener sync error:', e);
                }}
            }}

            document.querySelectorAll('[data-role="folder-remove-file"]').forEach(btn => {{
                btn.addEventListener('click', async function () {{
                    const documentName = this.dataset.documentName || 'файл';
                    const actor = getFolderDeleteActor();

                    if (!folderDeleteAllowedUserIds.has(String(actor.id || '').trim())) {{
                        alert('У вас недостаточно прав на удаление файлов');
                        return;
                    }}

                    if (!window.confirm('Удалить файл "' + documentName + '"?')) {{
                        return;
                    }}

                    this.disabled = true;

                    try {{
                        const response = await fetch(folderRemoveApiUrl, {{
                            method: 'POST',
                            headers: {{ 'Content-Type': 'application/json' }},
                            body: JSON.stringify({{
                                dialogId: this.dataset.dialogId,
                                checklistKey: this.dataset.checklistKey,
                                itemId: this.dataset.itemId,
                                documentId: this.dataset.documentId,
                                actingUserId: actor.id,
                                actingUserName: actor.name
                            }})
                        }});

                        const result = await response.json();
                        if (!response.ok || !result.ok) {{
                            throw new Error(result.error || 'remove document failed');
                        }}

                        notifyParentChecklistDocumentChanged('checklist-document-removed', {{
                            documentName: documentName
                        }});
                        window.location.reload();
                    }} catch (e) {{
                        console.log('folder remove error:', e);
                        alert(e && e.message ? e.message : 'Ошибка удаления файла');
                    }} finally {{
                        this.disabled = false;
                    }}
                }});
            }});

            const folderUploadBtn = document.getElementById('folderUploadBtn');
            const folderUploadInput = document.getElementById('folderUploadInput');

            if (folderUploadBtn && folderUploadInput) {{
                folderUploadBtn.addEventListener('click', function () {{
                    folderUploadInput.click();
                }});

                folderUploadInput.addEventListener('change', async function () {{
                    const files = Array.from(this.files || []);
                    if (!files.length) {{
                        return;
                    }}

                    folderUploadBtn.disabled = true;

                    try {{
                        for (const file of files) {{
                            const formData = new FormData();
                            formData.append('dialogId', folderDialogId);
                            formData.append('itemId', folderItemId);
                            formData.append('file', file);
                            formData.append('checklistKey', folderChecklistKey);
                            formData.append('itemGroup', folderItemGroup);

                            const response = await fetch(folderUploadApiUrl, {{
                                method: 'POST',
                                body: formData
                            }});

                            const result = await response.json();
                            if (!response.ok || !result.ok) {{
                                throw new Error(result.error || 'upload document failed');
                            }}
                        }}

                        notifyParentChecklistDocumentChanged('checklist-document-uploaded');
                        window.location.reload();
                    }} catch (e) {{
                        console.log('folder upload error:', e);
                        alert('Ошибка загрузки файлов');
                    }} finally {{
                        this.value = '';
                        folderUploadBtn.disabled = false;
                    }}
                }});
            }}
        </script>
    </body>
    </html>
    """

@app.get("/api/checklist/file")
def api_checklist_file(
    dialogId: str = "",
    itemId: str = "",
    documentId: str = "",
    checklistKey: str = "id",
    download: int = 0
):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    item_id = str(itemId or "").strip()
    document_id = str(documentId or "").strip()

    if not dialog_id or not item_id or not document_id:
        return JSONResponse({"ok": False, "error": "dialogId, itemId and documentId are required"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    target_item = None
    for item in items:
        if str(item.get("id") or "") == item_id:
            target_item = item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

    target_item = migrate_legacy_document_fields(target_item)
    documents = normalize_documents_list(target_item.get("documents"))

    target_doc = None
    for doc in documents:
        if str(doc.get("id") or "") == document_id:
            target_doc = doc
            break

    if not target_doc:
        return JSONResponse({"ok": False, "error": "document not found"}, status_code=404)

    file_url = clean_cell_value(target_doc.get("fileUrl")) or clean_cell_value(target_doc.get("path"))
    file_path = get_upload_file_path_from_url(file_url)

    if not file_path or not file_path.exists():
        return JSONResponse({"ok": False, "error": "file not found on disk"}, status_code=404)

    filename = clean_cell_value(target_doc.get("name")) or file_path.name
    media_type, _ = mimetypes.guess_type(str(file_path))
    media_type = media_type or "application/octet-stream"

    inline_allowed = can_preview_in_browser(filename, media_type)
    disposition = "attachment" if download else ("inline" if inline_allowed else "attachment")

    response = FileResponse(
        path=str(file_path),
        media_type=media_type
    )
    response.headers["Content-Disposition"] = f"{disposition}; filename*=UTF-8''{quote(filename)}"
    return response

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


@app.post("/api/checklist/lock/acquire")
async def api_checklist_lock_acquire(request: Request):
    payload = await request.json()
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    user_id = str(payload.get("userId") or "").strip()
    user_name = str(payload.get("userName") or "").strip()
    lock_id = str(payload.get("lockId") or "").strip()

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    result = acquire_checklist_lock(dialog_id, checklist_key, user_id, user_name, lock_id)
    write_debug_log("lock_acquire", result)
    return JSONResponse(result)


@app.post("/api/checklist/lock/heartbeat")
async def api_checklist_lock_heartbeat(request: Request):
    payload = await request.json()
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    user_id = str(payload.get("userId") or "").strip()
    user_name = str(payload.get("userName") or "").strip()
    lock_id = str(payload.get("lockId") or "").strip()

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    result = heartbeat_checklist_lock(dialog_id, checklist_key, user_id, user_name, lock_id)
    return JSONResponse(result)


@app.post("/api/checklist/lock/release")
async def api_checklist_lock_release(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raw = await request.body()
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            payload = {"raw": raw.decode("utf-8", errors="ignore")}

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    lock_id = str(payload.get("lockId") or "").strip()
    user_id = str(payload.get("userId") or "").strip()

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    result = release_checklist_lock(dialog_id, checklist_key, lock_id, user_id)
    write_debug_log("lock_release", result)
    return JSONResponse(result)


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
    editor = payload.get("editor") or {}
    raw_sessions = payload.get("sessions") or []

    def build_message_failure_response(base_dialog_id: str, extra: dict | None = None):
        response = {
            "ok": True,
            "saved": True,
            "messageOk": False,
            "dialogId": base_dialog_id,
        }
        if extra:
            response.update(extra)
        return JSONResponse(response)

    if raw_sessions:
        sessions = []
        for raw_session in raw_sessions:
            checklist_key = normalize_checklist_key(raw_session.get("checklistKey"))
            session_dialog_id = normalize_dialog_id(raw_session.get("dialogId") or dialog_id)
            changes = raw_session.get("changes") or []
            session_data = raw_session.get("data") or {}

            if not session_dialog_id:
                continue

            if changes and session_data:
                session_data = dict(session_data)
                session_data["checklistKey"] = checklist_key
                session_data["resolvedDialogId"] = session_dialog_id
                data = normalize_checklist_data(session_data, checklist_key)
                data["resolvedDialogId"] = session_dialog_id
                save_checklist(session_dialog_id, data, checklist_key)
            else:
                data = get_checklist(session_dialog_id, checklist_key)

            sessions.append({
                "dialogId": session_dialog_id,
                "checklistKey": checklist_key,
                "changes": changes,
                "data": data,
            })

        if not sessions:
            write_debug_log("close_session_skipped", {
                "dialogId": dialog_id,
                "reason": "no sessions"
            })
            return JSONResponse({"ok": True, "skipped": True, "reason": "no sessions"})

        visible_sessions = [session for session in sessions if build_recent_changes_sections(session["changes"], session["checklistKey"])]
        if not visible_sessions:
            write_debug_log("close_session_message_skipped", {
                "dialogId": dialog_id or sessions[0]["dialogId"],
                "reason": "no visible message changes",
                "sessions": [
                    {
                        "checklistKey": session["checklistKey"],
                        "changesCount": len(session["changes"]),
                    }
                    for session in sessions
                ]
            })
            return JSONResponse({
                "ok": True,
                "dialogId": dialog_id or sessions[0]["dialogId"],
                "saved": True,
                "messageSkipped": True,
            })

        target_dialog_id = dialog_id or sessions[0]["dialogId"]

        try:
            message = build_multi_checklist_chat_message(visible_sessions, editor)
            result = bitrix_webhook_call("im.message.add", {
                "DIALOG_ID": target_dialog_id,
                "MESSAGE": message,
            })
        except Exception as exc:
            write_debug_log("close_session_message_exception", {
                "dialogId": target_dialog_id,
                "editor": editor,
                "checklistKeys": [session["checklistKey"] for session in visible_sessions],
                "error": str(exc),
            })
            return build_message_failure_response(target_dialog_id, {
                "checklistKeys": [session["checklistKey"] for session in sessions],
                "messageError": str(exc),
            })

        write_debug_log("close_session_im_message_add_result", {
            "dialogId": target_dialog_id,
            "changesCount": sum(len(session["changes"]) for session in visible_sessions),
            "editor": editor,
            "result": result,
            "checklistKeys": [session["checklistKey"] for session in visible_sessions],
        })

        if "error" in result:
            return build_message_failure_response(target_dialog_id, {
                "checklistKeys": [session["checklistKey"] for session in sessions],
                "messageError": result.get("error_description") or result.get("error") or "message send failed",
                "result": result,
            })

        return JSONResponse({
            "ok": True,
            "saved": True,
            "messageOk": True,
            "dialogId": target_dialog_id,
            "result": result,
            "checklistKeys": [session["checklistKey"] for session in sessions],
        })

    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    changes = payload.get("changes") or []
    session_data = payload.get("data") or {}

    if not dialog_id:
        write_debug_log("close_session_invalid", {
            "reason": "dialogId is required",
            "payload": payload
        })
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not changes:
        write_debug_log("close_session_skipped", {
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "reason": "no changes"
        })
        return JSONResponse({"ok": True, "skipped": True, "reason": "no changes"})

    if session_data:
        session_data = dict(session_data)
        session_data["checklistKey"] = checklist_key
        session_data["resolvedDialogId"] = dialog_id
        data = normalize_checklist_data(session_data, checklist_key)
        data["resolvedDialogId"] = dialog_id
        save_checklist(dialog_id, data, checklist_key)
    else:
        data = get_checklist(dialog_id, checklist_key)

    if not build_recent_changes_sections(changes, checklist_key):
        write_debug_log("close_session_message_skipped", {
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "reason": "no visible message changes",
            "changesCount": len(changes),
        })
        return JSONResponse({
            "ok": True,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "saved": True,
            "messageSkipped": True,
        })

    try:
        message = build_checklist_chat_message(data, changes, editor)
        result = bitrix_webhook_call("im.message.add", {
            "DIALOG_ID": dialog_id,
            "MESSAGE": message,
        })
    except Exception as exc:
        write_debug_log("close_session_message_exception", {
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "editor": editor,
            "error": str(exc)
        })
        return build_message_failure_response(dialog_id, {
            "checklistKey": checklist_key,
            "messageError": str(exc),
        })

    write_debug_log("close_session_im_message_add_result", {
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "changesCount": len(changes),
        "editor": editor,
        "result": result
    })

    if "error" in result:
        return build_message_failure_response(dialog_id, {
            "checklistKey": checklist_key,
            "messageError": result.get("error_description") or result.get("error") or "message send failed",
            "result": result,
        })

    return JSONResponse({
        "ok": True,
        "saved": True,
        "messageOk": True,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "result": result,
    })


@app.get("/debug/logs", response_class=HTMLResponse)
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
            status_code=403
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

@app.get("/admin", response_class=HTMLResponse)
def admin():
    rows = list_checklist_summaries()

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
