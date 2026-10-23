(function (global) {
    'use strict';

    // The server puts this script and window.CHECKLIST_AUTH into every page
    // it shows to a checked employee, before any other script. It sends the
    // key of the window with every request of the page and adds a short link
    // key to links that open a new window, so that those windows open
    // without a new login.

    const data = global.CHECKLIST_AUTH;
    if (!data || !data.token || global.ChecklistAuth) return;

    const HEADER = 'X-Checklist-Auth';
    const LINK_PARAM = 'lt';
    const BEACON_PARAM = 'at';
    const origin = global.location.origin;

    let token = String(data.token);
    let linkToken = String(data.linkToken || '');
    let beaconToken = String(data.beaconToken || '');
    let linkExpiresAt = Number(data.linkExpiresAt || 0) * 1000;
    // The life of a link key as seen by this browser (its clock may differ
    // from the server's): renew when half of it is gone.
    let linkLifeMs = Math.max(4000, linkExpiresAt - Date.now());
    let renewAt = Date.now() + linkLifeMs / 2;
    let expiredShown = false;

    function appUrl(value) {
        if (!value) return null;
        try {
            const url = new URL(String(value), global.location.href);
            if (url.origin !== origin) return null;
            if (url.pathname.indexOf('/ui-static/') !== -1) return null;
            return url;
        } catch (error) {
            return null;
        }
    }

    function withParam(value, name, param) {
        const url = appUrl(value);
        if (!url || !param) return value;
        url.searchParams.set(name, param);
        return url.href;
    }

    // A link of the application with the link key (pages and files). Right
    // after the computer wakes up the key can be out of date for a moment:
    // the renewal starts at once, the link opens with the closed page then.
    function link(value) {
        if (linkExpiresAt && Date.now() > linkExpiresAt - 5000) refresh();
        return withParam(value, LINK_PARAM, linkToken);
    }

    function showExpired(reason) {
        if (expiredShown) return;
        expiredShown = true;
        const show = function () {
            const bar = global.document.createElement('div');
            bar.setAttribute('role', 'alert');
            bar.style.cssText = 'position:fixed;left:0;right:0;top:0;z-index:100000;'
                + 'padding:10px 16px;background:#b42318;color:#fff;font:14px Arial,sans-serif;'
                + 'text-align:center;box-shadow:0 4px 12px rgba(0,0,0,.2);';
            bar.textContent = reason === 'expired'
                ? 'Время входа истекло. Закройте окно и откройте чек-лист заново из Битрикс24.'
                : 'Доступ закрыт. Закройте окно и откройте чек-лист заново из Битрикс24.';
            global.document.body.appendChild(bar);
        };
        if (global.document.body) show();
        else global.document.addEventListener('DOMContentLoaded', show, { once: true });
    }

    function inspect(response) {
        if (!response || response.status !== 401) return;
        response.clone().json().then(function (body) {
            if (body && body.authRequired) showExpired(String(body.authReason || ''));
        }).catch(function () {});
    }

    const nativeFetch = global.fetch ? global.fetch.bind(global) : null;
    if (nativeFetch) {
        global.fetch = function (input, init) {
            let options = init;
            try {
                const target = typeof input === 'string' || input instanceof URL
                    ? String(input)
                    : (input && input.url) || '';
                if (appUrl(target)) {
                    options = Object.assign({}, init || {});
                    const headers = new Headers(
                        options.headers
                        || (input && typeof input === 'object' && input.headers)
                        || undefined
                    );
                    headers.set(HEADER, token);
                    options.headers = headers;
                }
            } catch (error) {
                options = init;
            }
            return nativeFetch(input, options).then(function (response) {
                inspect(response);
                return response;
            });
        };
    }

    const xhrOpen = global.XMLHttpRequest && global.XMLHttpRequest.prototype.open;
    const xhrSend = global.XMLHttpRequest && global.XMLHttpRequest.prototype.send;
    if (xhrOpen && xhrSend) {
        global.XMLHttpRequest.prototype.open = function (method, url) {
            this.__checklistAuthUrl = url;
            return xhrOpen.apply(this, arguments);
        };
        global.XMLHttpRequest.prototype.send = function () {
            try {
                if (appUrl(this.__checklistAuthUrl)) {
                    this.setRequestHeader(HEADER, token);
                }
            } catch (error) {
                // The request goes as it is.
            }
            return xhrSend.apply(this, arguments);
        };
    }

    if (global.navigator && typeof global.navigator.sendBeacon === 'function') {
        const nativeBeacon = global.navigator.sendBeacon.bind(global.navigator);
        global.navigator.sendBeacon = function (url, body) {
            return nativeBeacon(withParam(url, BEACON_PARAM, beaconToken), body);
        };
    }

    const nativeOpen = global.open;
    if (typeof nativeOpen === 'function') {
        global.open = function (url) {
            const args = Array.prototype.slice.call(arguments);
            if (typeof url === 'string' || url instanceof URL) {
                args[0] = link(String(url));
            }
            return nativeOpen.apply(global, args);
        };
    }

    // Links of pages and files get a fresh link key at the moment they are
    // used: click, middle click, the context menu or Enter.
    function refreshAnchor(event) {
        const target = event.target;
        const anchor = target && typeof target.closest === 'function'
            ? target.closest('a[href]')
            : null;
        if (!anchor) return;
        const href = anchor.getAttribute('href') || '';
        if (!href || href.charAt(0) === '#' || /^javascript:/i.test(href)) return;
        if (!appUrl(href)) return;
        anchor.setAttribute('href', link(href));
    }
    ['mousedown', 'click', 'auxclick', 'contextmenu', 'keydown'].forEach(function (type) {
        global.document.addEventListener(type, refreshAnchor, true);
    });

    // The keys are not left in the address bar.
    try {
        const current = new URL(global.location.href);
        if (current.searchParams.has(LINK_PARAM) || current.searchParams.has(BEACON_PARAM)) {
            current.searchParams.delete(LINK_PARAM);
            current.searchParams.delete(BEACON_PARAM);
            global.history.replaceState(global.history.state, '', current.href);
        }
    } catch (error) {
        // An old browser keeps the address as it is.
    }

    let refreshing = false;
    function refresh() {
        if (refreshing || expiredShown) return;
        refreshing = true;
        global.fetch(origin + appBase() + '/api/auth/refresh', { method: 'POST', cache: 'no-store' })
            .then(function (response) { return response.json(); })
            .then(function (body) {
                if (body && body.ok && body.token) {
                    token = String(body.token);
                    linkToken = String(body.linkToken || linkToken);
                    beaconToken = String(body.beaconToken || beaconToken);
                    linkExpiresAt = Number(body.linkExpiresAt || 0) * 1000;
                    linkLifeMs = Math.max(4000, linkExpiresAt - Date.now());
                    renewAt = Date.now() + linkLifeMs / 2;
                }
            })
            .catch(function () {})
            .then(function () { refreshing = false; });
    }

    // The application can sit under a path prefix: /popup, /admin and the
    // other pages are its direct children.
    function appBase() {
        const path = String(global.location.pathname || '/');
        const marks = ['/popup', '/api/', '/admin', '/debug/', '/textarea', '/launch'];
        for (let i = 0; i < marks.length; i++) {
            const index = path.indexOf(marks[i]);
            if (index !== -1) return path.slice(0, index);
        }
        return path === '/' ? '' : path.replace(/\/+$/, '');
    }

    function refreshIfDue() {
        if (Date.now() >= renewAt) refresh();
    }

    // Timers sleep with the computer: the check also runs when the window
    // comes back.
    global.setInterval(refreshIfDue, Math.max(1000, Math.min(30000, linkLifeMs / 4)));
    global.document.addEventListener('visibilitychange', function () {
        if (global.document.visibilityState === 'visible') refreshIfDue();
    });
    global.addEventListener('focus', refreshIfDue);
    global.addEventListener('online', refreshIfDue);

    // Reloading or leaving a page inside Bitrix24 cannot count on a cookie
    // (a browser can block cookies of a frame), so the new address carries
    // a fresh link key.
    function go(value, replace) {
        const target = link(String(value || global.location.href));
        if (replace) global.location.replace(target);
        else global.location.assign(target);
    }

    function reload() {
        go(global.location.href, true);
    }

    global.ChecklistAuth = Object.freeze({
        go: go,
        reload: reload,
        user: Object.freeze({
            id: String(data.user && data.user.id || ''),
            name: String(data.user && data.user.name || '')
        }),
        source: String(data.source || ''),
        link: link,
        token: function () { return token; }
    });
})(window);
