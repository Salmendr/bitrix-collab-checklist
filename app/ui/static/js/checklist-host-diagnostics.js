(function (global) {
    'use strict';

    if (global.ChecklistHostDiagnostics) {
        return;
    }

    const DIAGNOSTIC_VERSION = '8.15.9.8-popup-host-diagnostics';
    const LAUNCH_STORAGE_KEY = 'checklist_popup_launch_diagnostic_v1';
    const MAX_LAUNCH_AGE_MS = 30 * 60 * 1000;
    const config = (
        global.CHECKLIST_HOST_DIAGNOSTICS_CONFIG
        && typeof global.CHECKLIST_HOST_DIAGNOSTICS_CONFIG === 'object'
    )
        ? global.CHECKLIST_HOST_DIAGNOSTICS_CONFIG
        : {};
    const surface = String(config.surface || 'unknown');
    const bootTimestamp = Date.now();
    const bootPerformance = (
        global.performance
        && typeof global.performance.now === 'function'
    )
        ? global.performance.now()
        : 0;
    let eventSequence = 0;
    let geometryTimer = null;
    let lastGeometrySignature = '';

    function createId(prefix) {
        let randomPart = '';

        try {
            randomPart = (
                global.crypto
                && typeof global.crypto.randomUUID === 'function'
            )
                ? global.crypto.randomUUID().replace(/-/g, '')
                : Math.random().toString(36).slice(2, 14);
        } catch (error) {
            randomPart = Math.random().toString(36).slice(2, 14);
        }

        return String(prefix || 'diag') + '_' + Date.now() + '_' + randomPart;
    }

    const frameInstanceId = createId(surface.replace(/[^a-z0-9]+/gi, '_'));

    function trimText(value, limit) {
        const text = String(value === undefined || value === null ? '' : value);
        const maxLength = Math.max(1, Number(limit || 500));
        return text.length > maxLength
            ? text.slice(0, maxLength) + '…'
            : text;
    }

    function isSensitiveKey(key) {
        const normalized = String(key || '').trim().toLowerCase();
        return (
            normalized === 'app_sid'
            || normalized.indexOf('auth') !== -1
            || normalized.indexOf('token') !== -1
            || normalized.indexOf('secret') !== -1
            || normalized.indexOf('password') !== -1
            || normalized.indexOf('refresh') !== -1
        );
    }

    function sanitizeUrl(value) {
        const raw = String(value || '').trim();
        if (!raw) return '';

        try {
            const url = new URL(raw, global.location.href);
            const sanitizedSearch = new URLSearchParams();

            url.searchParams.forEach(function (itemValue, itemKey) {
                sanitizedSearch.append(
                    itemKey,
                    isSensitiveKey(itemKey)
                        ? '<redacted>'
                        : trimText(itemValue, 240)
                );
            });

            url.search = sanitizedSearch.toString()
                ? '?' + sanitizedSearch.toString()
                : '';

            if (url.hash && url.hash.indexOf('=') !== -1) {
                const hashParams = new URLSearchParams(url.hash.replace(/^#/, ''));
                const sanitizedHash = new URLSearchParams();
                hashParams.forEach(function (itemValue, itemKey) {
                    sanitizedHash.append(
                        itemKey,
                        isSensitiveKey(itemKey)
                            ? '<redacted>'
                            : trimText(itemValue, 240)
                    );
                });
                url.hash = sanitizedHash.toString()
                    ? '#' + sanitizedHash.toString()
                    : '';
            }

            return url.toString();
        } catch (error) {
            return trimText(raw, 500);
        }
    }

    function detectAppBasePath() {
        const path = String(global.location.pathname || '/')
            .replace(/\/+$/, '');
        const suffixes = [
            '/launch',
            '/popup',
            '/textarea',
            '/install',
            '/health',
            '/debug/logs',
            '/admin',
            '/admin/upload'
        ];

        for (let index = 0; index < suffixes.length; index += 1) {
            const suffix = suffixes[index];
            if (path === suffix) return '';
            if (path.endsWith(suffix)) {
                return path.slice(0, -suffix.length) || '';
            }
        }

        return path === '/' ? '' : path;
    }

    const appBasePath = detectAppBasePath();

    function appPath(path) {
        return (appBasePath || '')
            + '/'
            + String(path || '').replace(/^\/+/, '');
    }

    function normalizeLaunch(value) {
        if (!value || typeof value !== 'object') return null;

        const startedAt = Number(value.startedAt || 0);
        if (
            startedAt
            && Math.abs(Date.now() - startedAt) > MAX_LAUNCH_AGE_MS
        ) {
            return null;
        }

        return {
            launchId: trimText(value.launchId, 160),
            clickSequence: Number(value.clickSequence || 0),
            dialogId: trimText(value.dialogId, 160),
            checklistKey: trimText(value.checklistKey || 'id', 80),
            trigger: trimText(value.trigger, 120),
            startedAt,
            launcherFrameInstanceId: trimText(
                value.launcherFrameInstanceId,
                160
            )
        };
    }

    function readLatestLaunch() {
        try {
            const raw = global.localStorage.getItem(LAUNCH_STORAGE_KEY);
            return raw ? normalizeLaunch(JSON.parse(raw)) : null;
        } catch (error) {
            return null;
        }
    }

    const initialLaunch = readLatestLaunch();
    let activeLaunch = initialLaunch;

    function writeLaunch(value) {
        try {
            global.localStorage.setItem(
                LAUNCH_STORAGE_KEY,
                JSON.stringify(value)
            );
            return true;
        } catch (error) {
            return false;
        }
    }

    function elapsedMilliseconds() {
        if (
            global.performance
            && typeof global.performance.now === 'function'
        ) {
            return Math.max(
                0,
                Math.round(global.performance.now() - bootPerformance)
            );
        }

        return Math.max(0, Date.now() - bootTimestamp);
    }

    function readNavigationType() {
        try {
            const entries = global.performance.getEntriesByType('navigation');
            if (entries && entries[0]) {
                return String(entries[0].type || '');
            }
        } catch (error) {
            // Navigation timing is optional in embedded Bitrix frames.
        }
        return '';
    }

    function readFrameRelationship() {
        const result = {
            isTop: false,
            hasParent: false,
            parentSameOrigin: false,
            hasOpener: false,
            frameElementTag: '',
            frameElementId: ''
        };

        try {
            result.isTop = global.top === global;
            result.hasParent = global.parent !== global;
        } catch (error) {
            result.hasParent = true;
        }

        try {
            result.parentSameOrigin = Boolean(
                global.parent
                && global.parent.location
                && global.parent.location.origin === global.location.origin
            );
        } catch (error) {
            result.parentSameOrigin = false;
        }

        try {
            result.hasOpener = Boolean(global.opener);
        } catch (error) {
            result.hasOpener = false;
        }

        try {
            if (global.frameElement) {
                result.frameElementTag = String(
                    global.frameElement.tagName || ''
                );
                result.frameElementId = trimText(
                    global.frameElement.id || '',
                    120
                );
            }
        } catch (error) {
            // Cross-origin parents intentionally hide frameElement.
        }

        return result;
    }

    function readBodyState() {
        if (!global.document.body) {
            return {
                present: false
            };
        }

        const body = global.document.body;
        const rect = body.getBoundingClientRect();
        let style = null;

        try {
            style = global.getComputedStyle(body);
        } catch (error) {
            style = null;
        }

        return {
            present: true,
            clientWidth: Number(body.clientWidth || 0),
            clientHeight: Number(body.clientHeight || 0),
            scrollWidth: Number(body.scrollWidth || 0),
            scrollHeight: Number(body.scrollHeight || 0),
            rect: {
                x: Math.round(Number(rect.x || 0)),
                y: Math.round(Number(rect.y || 0)),
                width: Math.round(Number(rect.width || 0)),
                height: Math.round(Number(rect.height || 0))
            },
            style: style
                ? {
                    display: String(style.display || ''),
                    visibility: String(style.visibility || ''),
                    opacity: String(style.opacity || ''),
                    overflow: String(style.overflow || '')
                }
                : null
        };
    }

    function readViewport() {
        const visual = global.visualViewport;
        const screen = global.screen || {};

        return {
            innerWidth: Number(global.innerWidth || 0),
            innerHeight: Number(global.innerHeight || 0),
            outerWidth: Number(global.outerWidth || 0),
            outerHeight: Number(global.outerHeight || 0),
            screenX: Number(global.screenX || 0),
            screenY: Number(global.screenY || 0),
            devicePixelRatio: Number(global.devicePixelRatio || 1),
            screen: {
                width: Number(screen.width || 0),
                height: Number(screen.height || 0),
                availWidth: Number(screen.availWidth || 0),
                availHeight: Number(screen.availHeight || 0)
            },
            visualViewport: visual
                ? {
                    width: Math.round(Number(visual.width || 0)),
                    height: Math.round(Number(visual.height || 0)),
                    offsetLeft: Math.round(Number(visual.offsetLeft || 0)),
                    offsetTop: Math.round(Number(visual.offsetTop || 0)),
                    pageLeft: Math.round(Number(visual.pageLeft || 0)),
                    pageTop: Math.round(Number(visual.pageTop || 0)),
                    scale: Number(visual.scale || 1)
                }
                : null
        };
    }

    function buildSnapshot() {
        const latestLaunch = readLatestLaunch();
        const bx24 = global.BX24;

        return {
            diagnosticVersion: DIAGNOSTIC_VERSION,
            surface,
            frameInstanceId,
            eventSequence: eventSequence,
            bootTimestamp,
            elapsedMs: elapsedMilliseconds(),
            initialLaunchId: initialLaunch && initialLaunch.launchId || '',
            activeLaunchId: activeLaunch && activeLaunch.launchId || '',
            latestLaunchId: latestLaunch && latestLaunch.launchId || '',
            latestClickSequence: latestLaunch && latestLaunch.clickSequence || 0,
            latestLaunchAgeMs: latestLaunch && latestLaunch.startedAt
                ? Math.max(0, Date.now() - latestLaunch.startedAt)
                : null,
            href: sanitizeUrl(global.location.href),
            referrer: sanitizeUrl(global.document.referrer),
            windowName: trimText(global.name || '', 160),
            document: {
                readyState: String(global.document.readyState || ''),
                visibilityState: String(global.document.visibilityState || ''),
                hidden: Boolean(global.document.hidden),
                hasFocus: typeof global.document.hasFocus === 'function'
                    ? Boolean(global.document.hasFocus())
                    : null,
                prerendering: Boolean(global.document.prerendering)
            },
            navigationType: readNavigationType(),
            historyLength: Number(global.history && global.history.length || 0),
            frame: readFrameRelationship(),
            viewport: readViewport(),
            body: readBodyState(),
            bx24: {
                exists: Boolean(bx24),
                hasInit: Boolean(bx24 && typeof bx24.init === 'function'),
                hasOpenApplication: Boolean(
                    bx24 && typeof bx24.openApplication === 'function'
                ),
                hasCloseApplication: Boolean(
                    bx24 && typeof bx24.closeApplication === 'function'
                ),
                hasResizeWindow: Boolean(
                    bx24 && typeof bx24.resizeWindow === 'function'
                ),
                hasFitWindow: Boolean(
                    bx24 && typeof bx24.fitWindow === 'function'
                )
            }
        };
    }

    function sendRecord(record, useBeacon) {
        const body = JSON.stringify(record);
        const url = appPath('api/debug/event');

        if (useBeacon) {
            try {
                if (
                    global.navigator
                    && typeof global.navigator.sendBeacon === 'function'
                ) {
                    const blob = new Blob(
                        [body],
                        { type: 'application/json' }
                    );
                    if (global.navigator.sendBeacon(url, blob)) {
                        return true;
                    }
                }
            } catch (error) {
                // Fall through to keepalive fetch for unload diagnostics.
            }
        }

        try {
            global.fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body,
                keepalive: Boolean(useBeacon),
                credentials: 'same-origin'
            }).catch(function () {});
            return true;
        } catch (error) {
            return false;
        }
    }

    function record(event, details, useBeacon) {
        eventSequence += 1;
        const snapshot = buildSnapshot();
        snapshot.eventSequence = eventSequence;

        return sendRecord({
            event: String(event || 'popup_diag_unknown'),
            surface,
            launchId: snapshot.activeLaunchId || snapshot.latestLaunchId,
            frameInstanceId,
            dialogId: trimText(
                config.dialogId
                || activeLaunch && activeLaunch.dialogId
                || new URLSearchParams(global.location.search || '')
                    .get('dialogId')
                || '',
                160
            ),
            checklistKey: trimText(
                config.checklistKey
                || activeLaunch && activeLaunch.checklistKey
                || new URLSearchParams(global.location.search || '')
                    .get('checklistKey')
                || 'id',
                80
            ),
            payload: {
                details: details && typeof details === 'object'
                    ? details
                    : {},
                snapshot
            },
            href: snapshot.href,
            ts: new Date().toISOString()
        }, Boolean(useBeacon));
    }

    function startLaunch(metadata) {
        const latest = readLatestLaunch();
        const source = metadata && typeof metadata === 'object'
            ? metadata
            : {};
        const launch = normalizeLaunch({
            launchId: createId('launch'),
            clickSequence: Number(latest && latest.clickSequence || 0) + 1,
            dialogId: source.dialogId || config.dialogId || '',
            checklistKey: source.checklistKey || config.checklistKey || 'id',
            trigger: source.trigger || 'unknown',
            startedAt: Date.now(),
            launcherFrameInstanceId: frameInstanceId
        });

        activeLaunch = launch;
        const stored = writeLaunch(launch);
        record('popup_diag_launch_created', {
            launch,
            stored
        }, true);
        return launch;
    }

    function recordGeometry(source) {
        const viewport = readViewport();
        const body = readBodyState();
        const signature = JSON.stringify({
            innerWidth: viewport.innerWidth,
            innerHeight: viewport.innerHeight,
            visualViewport: viewport.visualViewport,
            bodyWidth: body.clientWidth || 0,
            bodyHeight: body.clientHeight || 0,
            visibilityState: global.document.visibilityState
        });

        if (signature === lastGeometrySignature) return;
        lastGeometrySignature = signature;
        record('popup_diag_geometry_changed', {
            source: String(source || 'unknown')
        });
    }

    function scheduleGeometryRecord(source) {
        if (geometryTimer !== null) {
            global.clearTimeout(geometryTimer);
        }
        geometryTimer = global.setTimeout(function () {
            geometryTimer = null;
            recordGeometry(source);
        }, 80);
    }

    global.ChecklistHostDiagnostics = Object.freeze({
        version: DIAGNOSTIC_VERSION,
        surface,
        frameInstanceId,
        startLaunch,
        record,
        readLatestLaunch
    });

    record('popup_diag_surface_script_loaded', {
        configuredDialogId: trimText(config.dialogId || '', 160),
        configuredChecklistKey: trimText(config.checklistKey || '', 80)
    });

    global.document.addEventListener('DOMContentLoaded', function () {
        record('popup_diag_surface_dom_content_loaded', {});
        recordGeometry('dom_content_loaded');
    }, true);

    global.addEventListener('load', function () {
        record('popup_diag_surface_window_loaded', {});
        recordGeometry('window_load');
    }, true);

    global.addEventListener('focus', function () {
        record('popup_diag_surface_focus', {});
    }, true);

    global.addEventListener('blur', function () {
        record('popup_diag_surface_blur', {});
    }, true);

    global.document.addEventListener('visibilitychange', function () {
        record('popup_diag_surface_visibility_changed', {
            visibilityState: String(global.document.visibilityState || ''),
            hidden: Boolean(global.document.hidden)
        }, Boolean(global.document.hidden));
    }, true);

    global.addEventListener('pageshow', function (event) {
        record('popup_diag_surface_pageshow', {
            persisted: Boolean(event && event.persisted)
        });
    }, true);

    global.addEventListener('pagehide', function (event) {
        record('popup_diag_surface_pagehide', {
            persisted: Boolean(event && event.persisted)
        }, true);
    }, true);

    global.addEventListener('beforeunload', function () {
        record('popup_diag_surface_beforeunload', {}, true);
    }, true);

    global.document.addEventListener('freeze', function () {
        record('popup_diag_surface_freeze', {}, true);
    }, true);

    global.document.addEventListener('resume', function () {
        record('popup_diag_surface_resume', {});
    }, true);

    global.addEventListener('resize', function () {
        scheduleGeometryRecord('window_resize');
    }, true);

    if (global.visualViewport) {
        global.visualViewport.addEventListener('resize', function () {
            scheduleGeometryRecord('visual_viewport_resize');
        }, true);
        global.visualViewport.addEventListener('scroll', function () {
            scheduleGeometryRecord('visual_viewport_scroll');
        }, true);
    }

    global.addEventListener('error', function (event) {
        record('popup_diag_surface_javascript_error', {
            message: trimText(event && event.message || '', 500),
            filename: sanitizeUrl(event && event.filename || ''),
            line: Number(event && event.lineno || 0),
            column: Number(event && event.colno || 0)
        });
    }, true);

    global.addEventListener('unhandledrejection', function (event) {
        const reason = event && event.reason;
        record('popup_diag_surface_unhandled_rejection', {
            reason: trimText(
                reason && (reason.stack || reason.message || reason),
                800
            )
        });
    }, true);

    [250, 1500, 5000, 10000].forEach(function (delay) {
        global.setTimeout(function () {
            record('popup_diag_surface_probe', {
                delayMs: delay
            });
            recordGeometry('probe_' + delay);
        }, delay);
    });
})(window);
