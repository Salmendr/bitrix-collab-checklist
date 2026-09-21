(function (global) {
    'use strict';

    const INIT_TIMEOUT_MS = 1800;
    let initPromise = null;
    let initialized = false;

    function recordHostDiagnostic(event, payload) {
        try {
            if (
                global.ChecklistHostDiagnostics
                && typeof global.ChecklistHostDiagnostics.record === 'function'
            ) {
                global.ChecklistHostDiagnostics.record(event, payload || {});
            }
        } catch (error) {
            console.log('popup host diagnostics skipped:', error);
        }
    }

    function isAvailable() {
        return Boolean(
            global.BX24
            && typeof global.BX24.init === 'function'
        );
    }

    function init() {
        if (initialized) {
            recordHostDiagnostic('popup_diag_popup_bx24_init_reused', {
                initialized: true
            });
            return Promise.resolve(true);
        }

        if (!isAvailable()) {
            recordHostDiagnostic('popup_diag_popup_bx24_unavailable', {});
            return Promise.resolve(false);
        }

        if (initPromise) {
            recordHostDiagnostic('popup_diag_popup_bx24_init_joined', {});
            return initPromise;
        }

        recordHostDiagnostic('popup_diag_popup_bx24_init_started', {
            timeoutMs: INIT_TIMEOUT_MS
        });

        initPromise = new Promise(function (resolve) {
            let settled = false;
            let timeoutId = null;

            function finish(value, reason) {
                if (settled) return;
                settled = true;

                if (timeoutId !== null) {
                    global.clearTimeout(timeoutId);
                }

                initialized = Boolean(value);
                recordHostDiagnostic(
                    'popup_diag_popup_bx24_init_finished',
                    {
                        initialized,
                        reason: String(reason || '')
                    }
                );
                resolve(initialized);
            }

            timeoutId = global.setTimeout(function () {
                console.log(
                    'BX24.init timeout; continuing in local mode'
                );
                finish(false, 'timeout');
            }, INIT_TIMEOUT_MS);

            try {
                global.BX24.init(function () {
                    recordHostDiagnostic(
                        'popup_diag_popup_bx24_init_callback',
                        {}
                    );
                    finish(true, 'callback');
                });
            } catch (error) {
                console.log('BX24.init skipped:', error);
                recordHostDiagnostic(
                    'popup_diag_popup_bx24_init_error',
                    { error: String(error) }
                );
                finish(false, 'exception');
            }
        });

        return initPromise;
    }

    async function callMethod(method, params) {
        const initializedNow = await init();
        if (!initializedNow) {
            return null;
        }

        return new Promise(function (resolve, reject) {
            try {
                global.BX24.callMethod(
                    method,
                    params || {},
                    function (result) {
                        try {
                            if (
                                result
                                && typeof result.error === 'function'
                                && result.error()
                            ) {
                                reject(new Error(String(result.error())));
                                return;
                            }
                            resolve(
                                result
                                && typeof result.data === 'function'
                                    ? (result.data() || {})
                                    : {}
                            );
                        } catch (error) {
                            reject(error);
                        }
                    }
                );
            } catch (error) {
                reject(error);
            }
        });
    }

    function getCurrentUser() {
        return callMethod('user.current', {});
    }

    function getDialog(dialogId) {
        return callMethod('im.dialog.get', {
            dialog_id: String(dialogId || '')
        });
    }

    async function fitPopup(options) {
        recordHostDiagnostic('popup_diag_popup_fit_requested', {
            options: options && typeof options === 'object'
                ? {
                    width: Number(options.width || 0),
                    height: Number(options.height || 0),
                    delays: Array.isArray(options.delays)
                        ? options.delays.map(Number)
                        : []
                }
                : null
        });
        const initializedNow = await init();
        if (!initializedNow) {
            recordHostDiagnostic('popup_diag_popup_fit_skipped', {
                reason: 'bx24_not_initialized'
            });
            return false;
        }

        const config = options && typeof options === 'object'
            ? options
            : {};
        const width = Number(config.width || 1180);
        const height = Number(config.height || 720);
        const delays = Array.isArray(config.delays)
            ? config.delays
            : [0];

        function applySize(delay) {
            recordHostDiagnostic('popup_diag_popup_fit_before', {
                width,
                height,
                delayMs: Number(delay || 0),
                hasResizeWindow: typeof global.BX24.resizeWindow === 'function',
                hasFitWindow: typeof global.BX24.fitWindow === 'function'
            });
            try {
                if (typeof global.BX24.resizeWindow === 'function') {
                    global.BX24.resizeWindow(width, height);
                }
                if (typeof global.BX24.fitWindow === 'function') {
                    global.BX24.fitWindow();
                }
                recordHostDiagnostic('popup_diag_popup_fit_after', {
                    width,
                    height,
                    delayMs: Number(delay || 0)
                });
            } catch (error) {
                console.log('BX24 popup sizing error:', error);
                recordHostDiagnostic('popup_diag_popup_fit_error', {
                    width,
                    height,
                    delayMs: Number(delay || 0),
                    error: String(error)
                });
            }
        }

        for (const delay of delays) {
            const normalizedDelay = Math.max(0, Number(delay || 0));
            if (normalizedDelay === 0) {
                applySize(normalizedDelay);
            } else {
                global.setTimeout(function () {
                    applySize(normalizedDelay);
                }, normalizedDelay);
            }
        }

        return true;
    }

    global.ChecklistPopupBitrix = Object.freeze({
        isAvailable,
        init,
        callMethod,
        getCurrentUser,
        getDialog,
        fitPopup,
        initTimeoutMs: INIT_TIMEOUT_MS
    });
})(window);
