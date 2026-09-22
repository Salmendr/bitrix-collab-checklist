import json
from pathlib import Path
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


_HOST_DIAGNOSTICS_SCRIPT = (
    Path(__file__).resolve().parent
    / "static"
    / "js"
    / "checklist-host-diagnostics.js"
).read_text(encoding="utf-8")

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
            window.CHECKLIST_HOST_DIAGNOSTICS_CONFIG = Object.freeze({{
                surface: 'application_frame',
                dialogId: {initial_dialog_id_json},
                checklistKey: {initial_checklist_key_json}
            }});
        </script>
        <script>{_HOST_DIAGNOSTICS_SCRIPT}</script>
        <script>
            (function () {{
                const initialDialogId = {initial_dialog_id_json};
                const initialChecklistKey = {initial_checklist_key_json};
                const initialContextText = {initial_context_text_json};
                const hostDiagnostics = window.ChecklistHostDiagnostics;

                function recordHostDiagnostic(event, payload, useBeacon) {{
                    try {{
                        if (
                            hostDiagnostics
                            && typeof hostDiagnostics.record === 'function'
                        ) {{
                            hostDiagnostics.record(
                                event,
                                payload || {{}},
                                Boolean(useBeacon)
                            );
                        }}
                    }} catch (error) {{
                        console.log('host diagnostics skipped:', error);
                    }}
                }}

                recordHostDiagnostic('popup_diag_application_script_started', {{
                    hasInitialDialogId: Boolean(initialDialogId),
                    hasInitialContext: Boolean(initialContextText)
                }});

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
                            recordHostDiagnostic(
                                'popup_diag_application_placement_unavailable',
                                {{}}
                            );
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

                        recordHostDiagnostic(
                            'popup_diag_application_placement_resolved',
                            {{
                                dialogId: dialogId,
                                checklistKey: checklistKey || 'id',
                                placement: String(
                                    info.placement || info.PLACEMENT || ''
                                ),
                                optionKeys: Object.keys(options).sort()
                            }}
                        );
                    }} catch (e) {{
                        console.log('app_home extractFromBx24 error:', e);
                        recordHostDiagnostic(
                            'popup_diag_application_placement_error',
                            {{ error: String(e) }}
                        );
                    }}

                    return {{
                        dialogId: dialogId,
                        checklistKey: checklistKey || 'id'
                    }};
                }}

                function resizeCurrentPopupFrame() {{
                    recordHostDiagnostic(
                        'popup_diag_application_resize_before',
                        {{
                            hasResizeWindow: Boolean(
                                window.BX24
                                && typeof window.BX24.resizeWindow === 'function'
                            ),
                            hasFitWindow: Boolean(
                                window.BX24
                                && typeof window.BX24.fitWindow === 'function'
                            )
                        }}
                    );
                    try {{
                        if (window.BX24 && typeof window.BX24.resizeWindow === 'function') {{
                            window.BX24.resizeWindow(1180, 720);
                        }}
                        if (window.BX24 && typeof window.BX24.fitWindow === 'function') {{
                            window.BX24.fitWindow();
                        }}
                        recordHostDiagnostic(
                            'popup_diag_application_resize_after',
                            {{ requestedWidth: 1180, requestedHeight: 720 }}
                        );
                    }} catch (e) {{
                        console.log('BX24 resize skipped:', e);
                        recordHostDiagnostic(
                            'popup_diag_application_resize_error',
                            {{ error: String(e) }}
                        );
                    }}
                }}

                function rememberAndRedirect(dialogId, checklistKey) {{
                    if (!dialogId) return;

                    recordHostDiagnostic(
                        'popup_diag_application_redirect_preparing',
                        {{
                            dialogId: dialogId,
                            checklistKey: checklistKey || 'id'
                        }}
                    );

                    let pendingDialogStored = false;
                    try {{
                        localStorage.setItem('checklist_pending_dialog', JSON.stringify({{
                            dialogId: dialogId,
                            checklistKey: checklistKey || 'id',
                            ts: Date.now()
                        }}));
                        pendingDialogStored = true;
                    }} catch (e) {{
                        console.log('pending dialog save skipped:', e);
                    }}

                    const popupUrl =
                        appPath('popup') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(checklistKey || 'id');

                    recordHostDiagnostic(
                        'popup_diag_application_redirect_scheduled',
                        {{
                            dialogId: dialogId,
                            checklistKey: checklistKey || 'id',
                            pendingDialogStored: pendingDialogStored,
                            delayMs: 120
                        }}
                    );

                    resizeCurrentPopupFrame();
                    setTimeout(resizeCurrentPopupFrame, 80);

                    setTimeout(function () {{
                        recordHostDiagnostic(
                            'popup_diag_application_redirect_executing',
                            {{
                                dialogId: dialogId,
                                checklistKey: checklistKey || 'id'
                            }},
                            true
                        );
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

                    recordHostDiagnostic(
                        'popup_diag_application_context_resolved',
                        {{
                            dialogId: dialogId,
                            checklistKey: checklistKey,
                            hasQueryDialogId: Boolean(
                                searchParams.get('dialogId')
                            ),
                            hasHashDialogId: Boolean(
                                hashParams.get('dialogId')
                            ),
                            hasInitialDialogId: Boolean(initialDialogId),
                            hasPendingDialog: Boolean(
                                localPayload && localPayload.dialogId
                            ),
                            pendingDialogAgeMs: age
                        }}
                    );

                    if (dialogId) {{
                        if (
                            window.BX24
                            && typeof window.BX24.init === 'function'
                        ) {{
                            recordHostDiagnostic(
                                'popup_diag_application_bx24_init_started',
                                {{ source: 'resolved_server_context' }}
                            );
                            window.BX24.init(function () {{
                                recordHostDiagnostic(
                                    'popup_diag_application_bx24_init_completed',
                                    {{ source: 'resolved_server_context' }}
                                );
                                rememberAndRedirect(
                                    dialogId,
                                    checklistKey
                                );
                            }});
                            return;
                        }}

                        recordHostDiagnostic(
                            'popup_diag_application_bx24_unavailable',
                            {{ source: 'resolved_server_context' }}
                        );
                        rememberAndRedirect(dialogId, checklistKey);
                        return;
                    }}

                    if (window.BX24 && typeof window.BX24.init === 'function') {{
                        recordHostDiagnostic(
                            'popup_diag_application_bx24_init_started',
                            {{}}
                        );
                        window.BX24.init(function () {{
                            recordHostDiagnostic(
                                'popup_diag_application_bx24_init_completed',
                                {{}}
                            );
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

                    recordHostDiagnostic(
                        'popup_diag_application_context_missing',
                        {{
                            hasBx24: Boolean(window.BX24),
                            pendingDialogAgeMs: age
                        }}
                    );
                }} catch (e) {{
                    console.log('launcher redirect skipped:', e);
                    recordHostDiagnostic(
                        'popup_diag_application_script_error',
                        {{ error: String(e) }}
                    );
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
        <script>
            window.CHECKLIST_HOST_DIAGNOSTICS_CONFIG = Object.freeze({{
                surface: 'textarea_launcher',
                dialogId: {initial_dialog_id_json},
                checklistKey: 'id'
            }});
        </script>
        <script>{_HOST_DIAGNOSTICS_SCRIPT}</script>
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
            var hasOpenedOnce = false;
            var reactivationArmed = false;
            var closedBaselineHeight = 0;
            var REACTIVATION_HEIGHT_GROWTH_PX = 40;

            function currentHeightMetric() {{
                return Number(window.innerHeight || 0);
            }}
            var hostDiagnostics = window.ChecklistHostDiagnostics;

            function recordHostDiagnostic(event, payload, useBeacon) {{
                try {{
                    if (
                        hostDiagnostics
                        && typeof hostDiagnostics.record === 'function'
                    ) {{
                        hostDiagnostics.record(
                            event,
                            payload || {{}},
                            Boolean(useBeacon)
                        );
                    }}
                }} catch (error) {{
                    console.log('host diagnostics skipped:', error);
                }}
            }}

            recordHostDiagnostic('popup_diag_launcher_script_started', {{
                hasInitialDialogId: Boolean(initialDialogId),
                hasInitialContext: Boolean(initialContextText),
                autoOpened: autoOpened
            }});

            function recordLauncherRuntimeState(source, useBeacon) {{
                recordHostDiagnostic(
                    'popup_diag_launcher_runtime_state',
                    {{
                        source: source,
                        dialogId: window.__dialogId || '',
                        autoOpened: autoOpened,
                        visibilityState: String(
                            document.visibilityState || ''
                        ),
                        hasFocus: typeof document.hasFocus === 'function'
                            ? Boolean(document.hasFocus())
                            : null
                    }},
                    Boolean(useBeacon)
                );
            }}

            window.addEventListener('focus', function () {{
                recordLauncherRuntimeState('window_focus', false);
            }}, true);

            window.addEventListener('blur', function () {{
                recordLauncherRuntimeState('window_blur', false);
            }}, true);

            function checkReactivation(source) {{
                if (!reactivationArmed) {{
                    return;
                }}

                var height = currentHeightMetric();
                if (height > 0 && height < closedBaselineHeight) {{
                    closedBaselineHeight = height;
                }}

                if (document.hidden) {{
                    return;
                }}

                if (
                    window.__dialogId
                    && hasOpenedOnce
                    && !autoOpened
                    && height >= (closedBaselineHeight + REACTIVATION_HEIGHT_GROWTH_PX)
                ) {{
                    reactivationArmed = false;
                    recordHostDiagnostic(
                        'popup_diag_launcher_reopen_reactivation_detected',
                        {{
                            dialogId: window.__dialogId,
                            autoOpened: autoOpened,
                            source: source,
                            baselineHeight: closedBaselineHeight,
                            currentHeight: height
                        }}
                    );
                    window.setTimeout(function () {{
                        openChecklist(
                            window.__dialogId,
                            'id',
                            'automatic_after_reactivation'
                        );
                    }}, 100);
                }}
            }}

            document.addEventListener('visibilitychange', function () {{
                recordLauncherRuntimeState(
                    'visibilitychange',
                    Boolean(document.hidden)
                );
                checkReactivation('visibilitychange');
            }}, true);

            document.addEventListener('click', function () {{
                recordHostDiagnostic(
                    'popup_diag_launcher_document_click',
                    {{
                        dialogId: window.__dialogId || '',
                        autoOpened: autoOpened
                    }}
                );
            }}, false);

            window.addEventListener('pageshow', function (event) {{
                recordHostDiagnostic(
                    'popup_diag_launcher_pageshow_state',
                    {{
                        persisted: Boolean(event && event.persisted),
                        dialogId: window.__dialogId || '',
                        autoOpened: autoOpened
                    }}
                );
            }}, true);

            var launcherResizeDiagnosticTimer = null;
            window.addEventListener('resize', function () {{
                if (launcherResizeDiagnosticTimer !== null) {{
                    window.clearTimeout(launcherResizeDiagnosticTimer);
                }}
                launcherResizeDiagnosticTimer = window.setTimeout(function () {{
                    launcherResizeDiagnosticTimer = null;
                    recordLauncherRuntimeState('window_resize', false);
                    checkReactivation('window_resize');
                }}, 100);
            }}, true);

            if (window.visualViewport) {{
                window.visualViewport.addEventListener('resize', function () {{
                    checkReactivation('visual_viewport_resize');
                }}, true);
            }}

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

            function openChecklist(
                dialogId,
                checklistKey = 'id',
                trigger = 'unknown'
            ) {{
                if (!dialogId) {{
                    setError('dialogId не найден');
                    recordHostDiagnostic(
                        'popup_diag_launcher_open_rejected',
                        {{
                            reason: 'dialog_id_missing',
                            trigger: trigger,
                            autoOpened: autoOpened
                        }}
                    );
                    return;
                }}

                var launch = null;
                try {{
                    if (
                        hostDiagnostics
                        && typeof hostDiagnostics.startLaunch === 'function'
                    ) {{
                        launch = hostDiagnostics.startLaunch({{
                            dialogId: dialogId,
                            checklistKey: checklistKey,
                            trigger: trigger
                        }});
                    }}
                }} catch (diagnosticError) {{
                    console.log(
                        'launch diagnostics skipped:',
                        diagnosticError
                    );
                }}

                var pendingDialogStored = false;
                try {{
                    localStorage.setItem('checklist_pending_dialog', JSON.stringify({{
                        dialogId: dialogId,
                        checklistKey: checklistKey,
                        ts: Date.now()
                    }}));
                    pendingDialogStored = true;
                }} catch (e) {{
                    console.log('localStorage save error:', e);
                }}

                try {{
                    if (window.BX24 && typeof window.BX24.openApplication === 'function') {{
                        var startedAt = (
                            window.performance
                            && typeof window.performance.now === 'function'
                        )
                            ? window.performance.now()
                            : Date.now();
                        recordHostDiagnostic(
                            'popup_diag_open_application_before',
                            {{
                                launchId: launch && launch.launchId || '',
                                trigger: trigger,
                                dialogId: dialogId,
                                checklistKey: checklistKey,
                                autoOpenedBefore: autoOpened,
                                pendingDialogStored: pendingDialogStored
                            }},
                            true
                        );

                        var openResult = BX24.openApplication(
                            {{
                                dialogId: dialogId,
                                checklistKey: checklistKey,
                                source: 'textarea'
                            }},
                            function () {{
                                autoOpened = false;
                                reactivationArmed = true;
                                closedBaselineHeight = currentHeightMetric();
                                recordHostDiagnostic(
                                    'popup_diag_launcher_close_callback',
                                    {{
                                        launchId: launch && launch.launchId || '',
                                        trigger: trigger,
                                        dialogId: dialogId,
                                        checklistKey: checklistKey,
                                        closedBaselineHeight: closedBaselineHeight
                                    }},
                                    true
                                );
                            }}
                        );
                        autoOpened = true;
                        hasOpenedOnce = true;
                        var finishedAt = (
                            window.performance
                            && typeof window.performance.now === 'function'
                        )
                            ? window.performance.now()
                            : Date.now();
                        recordHostDiagnostic(
                            'popup_diag_open_application_returned',
                            {{
                                launchId: launch && launch.launchId || '',
                                trigger: trigger,
                                elapsedMs: Math.max(
                                    0,
                                    Math.round(finishedAt - startedAt)
                                ),
                                returnType: typeof openResult,
                                autoOpenedAfter: autoOpened
                            }}
                        );
                        setMeta('Открываем popup для ' + dialogId);
                        return;
                    }}
                }} catch (e) {{
                    setError('BX24.openApplication error: ' + String(e));
                    recordHostDiagnostic(
                        'popup_diag_open_application_error',
                        {{
                            launchId: launch && launch.launchId || '',
                            trigger: trigger,
                            error: String(e),
                            autoOpened: autoOpened
                        }}
                    );
                }}

                recordHostDiagnostic(
                    'popup_diag_window_open_fallback',
                    {{
                        launchId: launch && launch.launchId || '',
                        trigger: trigger,
                        hasBx24: Boolean(window.BX24),
                        hasOpenApplication: Boolean(
                            window.BX24
                            && typeof window.BX24.openApplication === 'function'
                        )
                    }}
                );

                window.open(
                    appPath('popup') +
                    '?dialogId=' + encodeURIComponent(dialogId) +
                    '&checklistKey=' + encodeURIComponent(checklistKey),
                    '_blank'
                );
            }}

            document.getElementById('openBtn').addEventListener('click', function () {{
                try {{
                    recordHostDiagnostic(
                        'popup_diag_launcher_button_clicked',
                        {{
                            dialogId: window.__dialogId || '',
                            autoOpened: autoOpened
                        }}
                    );
                    if (window.__dialogId) {{
                        openChecklist(
                            window.__dialogId,
                            'id',
                            'manual_launcher_button'
                        );
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

                recordHostDiagnostic(
                    'popup_diag_launcher_context_finished',
                    {{
                        dialogId: window.__dialogId,
                        source: sourceText,
                        autoOpened: autoOpened
                    }}
                );

                try {{
                    if (window.BX24 && typeof window.BX24.fitWindow === 'function') {{
                        recordHostDiagnostic(
                            'popup_diag_launcher_fit_window_before',
                            {{ source: sourceText }}
                        );
                        window.BX24.fitWindow();
                        recordHostDiagnostic(
                            'popup_diag_launcher_fit_window_after',
                            {{ source: sourceText }}
                        );
                    }}
                }} catch (e) {{
                    recordHostDiagnostic(
                        'popup_diag_launcher_fit_window_error',
                        {{ error: String(e), source: sourceText }}
                    );
                }}

                if (window.__dialogId && !autoOpened) {{
                    recordHostDiagnostic(
                        'popup_diag_launcher_auto_open_scheduled',
                        {{
                            dialogId: window.__dialogId,
                            delayMs: 250,
                            source: sourceText
                        }}
                    );
                    setTimeout(function() {{
                        openChecklist(
                            window.__dialogId,
                            'id',
                            'automatic_after_context'
                        );
                    }}, 250);
                }} else {{
                    recordHostDiagnostic(
                        'popup_diag_launcher_auto_open_skipped',
                        {{
                            dialogId: window.__dialogId,
                            autoOpened: autoOpened,
                            source: sourceText
                        }}
                    );
                }}
            }}

            function canUseBx24() {{
                return !!(window.BX24 && typeof window.BX24.init === 'function');
            }}

            if (initialDialogId) {{
                recordHostDiagnostic(
                    'popup_diag_launcher_server_context_used',
                    {{ dialogId: initialDialogId }}
                );

                if (canUseBx24()) {{
                    try {{
                        recordHostDiagnostic(
                            'popup_diag_launcher_bx24_init_started',
                            {{ source: 'server-post' }}
                        );
                        window.BX24.init(function () {{
                            recordHostDiagnostic(
                                'popup_diag_launcher_bx24_init_completed',
                                {{ source: 'server-post' }}
                            );
                            finish(
                                initialDialogId,
                                'server-post+BX24-js'
                            );
                        }});
                    }} catch (e) {{
                        setError('BX24.init error: ' + String(e));
                        recordHostDiagnostic(
                            'popup_diag_launcher_bx24_init_error',
                            {{
                                source: 'server-post',
                                error: String(e)
                            }}
                        );
                        finish(
                            initialDialogId,
                            'server-post-init-failed'
                        );
                    }}
                }} else {{
                    recordHostDiagnostic(
                        'popup_diag_launcher_bx24_unavailable',
                        {{ source: 'server-post' }}
                    );
                    finish(initialDialogId, 'server-post-local');
                }}
            }} else if (canUseBx24()) {{
                try {{
                    recordHostDiagnostic(
                        'popup_diag_launcher_bx24_init_started',
                        {{}}
                    );
                    window.BX24.init(function () {{
                        recordHostDiagnostic(
                            'popup_diag_launcher_bx24_init_completed',
                            {{}}
                        );
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

                            recordHostDiagnostic(
                                'popup_diag_launcher_placement_resolved',
                                {{
                                    dialogId: dialogId,
                                    placement: String(
                                        info.placement
                                        || info.PLACEMENT
                                        || ''
                                    ),
                                    optionKeys: Object.keys(options).sort()
                                }}
                            );
                        }} catch (e) {{
                            setError('placement.info error: ' + String(e));
                            recordHostDiagnostic(
                                'popup_diag_launcher_placement_error',
                                {{ error: String(e) }}
                            );
                        }}

                        finish(dialogId, 'BX24-js');
                    }});
                }} catch (e) {{
                    setError('BX24.init error: ' + String(e));
                    recordHostDiagnostic(
                        'popup_diag_launcher_bx24_init_error',
                        {{ error: String(e) }}
                    );
                    finish('', 'BX24-init-failed');
                }}
            }} else {{
                setError(initialContextText || 'BX24 не найден');
                recordHostDiagnostic(
                    'popup_diag_launcher_bx24_unavailable',
                    {{}}
                );
                finish('', 'local');
            }}
        </script>
    </body>
    </html>
    """
