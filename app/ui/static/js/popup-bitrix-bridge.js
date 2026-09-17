(function (global) {
    'use strict';

    const INIT_TIMEOUT_MS = 1800;
    let initPromise = null;
    let initialized = false;

    function isAvailable() {
        return Boolean(
            global.BX24
            && typeof global.BX24.init === 'function'
        );
    }

    function init() {
        if (initialized) {
            return Promise.resolve(true);
        }

        if (!isAvailable()) {
            return Promise.resolve(false);
        }

        if (initPromise) {
            return initPromise;
        }

        initPromise = new Promise(function (resolve) {
            let settled = false;
            let timeoutId = null;

            function finish(value) {
                if (settled) return;
                settled = true;

                if (timeoutId !== null) {
                    global.clearTimeout(timeoutId);
                }

                initialized = Boolean(value);
                resolve(initialized);
            }

            timeoutId = global.setTimeout(function () {
                console.log(
                    'BX24.init timeout; continuing in local mode'
                );
                finish(false);
            }, INIT_TIMEOUT_MS);

            try {
                global.BX24.init(function () {
                    finish(true);
                });
            } catch (error) {
                console.log('BX24.init skipped:', error);
                finish(false);
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

    function getFrameGeometry() {
        const root = global.document && global.document.documentElement;
        const body = global.document && global.document.body;

        return {
            innerWidth: Number(global.innerWidth || 0),
            innerHeight: Number(global.innerHeight || 0),
            scrollWidth: Number(
                root && root.scrollWidth
                || body && body.scrollWidth
                || 0
            ),
            scrollHeight: Number(
                root && root.scrollHeight
                || body && body.scrollHeight
                || 0
            ),
            visibilityState: String(
                global.document && global.document.visibilityState
                || ''
            )
        };
    }

    async function fitPopup(options) {
        const initializedNow = await init();
        if (!initializedNow) {
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

        function applySize(stage) {
            try {
                if (typeof global.BX24.resizeWindow === 'function') {
                    if (typeof global.debugLog === 'function') {
                        global.debugLog('popup_frame_resize_requested', {
                            stage: String(stage || ''),
                            requestedWidth: width,
                            requestedHeight: height,
                            frame: getFrameGeometry()
                        });
                    }
                    global.BX24.resizeWindow(width, height);
                }
            } catch (error) {
                console.log('BX24 popup sizing error:', error);
            }
        }

        for (const delay of delays) {
            const normalizedDelay = Math.max(0, Number(delay || 0));
            if (normalizedDelay === 0) {
                applySize('initial');
            } else {
                global.setTimeout(function () {
                    applySize('delay-' + normalizedDelay);
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
        getFrameGeometry,
        fitPopup,
        initTimeoutMs: INIT_TIMEOUT_MS
    });
})(window);
