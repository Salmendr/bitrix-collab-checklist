import json
from urllib.parse import urlparse

from fastapi import Request

from app.settings import (
    APP_BASE_PATH,
    PUBLIC_APP_BASE_URL,
)

from app.checklists.utils import (
    normalize_dialog_id,
    normalize_checklist_key,
)

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
                    const v = String(value || '').trim().toLowerCase().replace(/\\s+/g, '_');
                    const cleaned = v.replace(/[^a-z0-9_-]+/g, '');
                    return cleaned || 'id';
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