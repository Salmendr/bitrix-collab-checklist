(function (global) {
    'use strict';

    function detectAppBasePath() {
        const path = String(global.location.pathname || '/')
            .replace(/\/+$/, '');
        const suffixes = [
            '/popup',
            '/launch',
            '/textarea',
            '/install',
            '/health',
            '/debug/logs',
            '/admin',
            '/admin/upload'
        ];

        for (const suffix of suffixes) {
            if (path === suffix) {
                return '';
            }
            if (path.endsWith(suffix)) {
                return path.slice(0, -suffix.length) || '';
            }
        }

        return '';
    }

    const basePath = detectAppBasePath();
    const baseUrl = global.location.origin + (basePath || '');

    function url(path) {
        return baseUrl + '/' + String(path || '')
            .replace(/^\/+/, '');
    }

    async function request(path, options) {
        return global.fetch(url(path), options);
    }

    async function requestJson(path, options) {
        const response = await request(path, options);
        const payload = await response.json().catch(function () {
            return {};
        });

        if (!response.ok) {
            const error = new Error(
                payload.error
                || payload.detail
                || ('HTTP ' + response.status)
            );
            error.status = response.status;
            error.payload = payload;
            throw error;
        }

        return payload;
    }

    global.ChecklistPopupApi = Object.freeze({
        basePath,
        baseUrl,
        detectAppBasePath,
        url,
        request,
        requestJson
    });
})(window);
