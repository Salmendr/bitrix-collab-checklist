(function (global) {
    'use strict';

    // Files from 1 GiB on go to the server in parts of 64 MiB: IIS in front
    // of the application takes 4 294 967 295 bytes in one request at most.
    // The file is cut by bytes (any format), the server puts the parts
    // together into the same file and adds it to the checklist like an
    // ordinary upload. A break sends again only the part in progress: a
    // repeated attempt of the upload manager goes on from what the server
    // has. The answer has the shape of the ordinary upload request:
    // { status, responseOk, result }.

    const THRESHOLD_BYTES = 1024 * 1024 * 1024;
    const CHUNK_BYTES = 64 * 1024 * 1024;
    // A part of 64 MiB on a slow line (about 0.3 Mbit/s) still fits.
    const CHUNK_TIMEOUT_MS = 30 * 60 * 1000;

    // The server upload of a file, kept between attempts of one task.
    const uploads = new Map();

    function fileOf(formData) {
        if (!formData || typeof formData.get !== 'function') return null;
        const file = formData.get('file');
        return file && typeof file === 'object' && typeof file.size === 'number'
            ? file
            : null;
    }

    function applies(formData) {
        const file = fileOf(formData);
        return !!(file && file.size >= THRESHOLD_BYTES);
    }

    function apiUrl(url, action) {
        const target = new URL(url, global.location.href);
        target.pathname = target.pathname.replace(
            /\/api\/checklist\/(upload|replace)-document\/?$/,
            '/api/checklist/chunked-upload/' + action
        );
        target.search = '';
        return target;
    }

    function kindOf(url) {
        return /replace-document\/?(\?|$)/.test(String(url || '')) ? 'replace' : 'upload';
    }

    function fieldsOf(formData) {
        const fields = {};
        formData.forEach(function (value, key) {
            if (key !== 'file' && typeof value === 'string') {
                fields[key] = value;
            }
        });
        return fields;
    }

    function keyOf(fields, file, kind) {
        return [
            kind,
            fields.sessionId || '',
            fields.checklistKey || '',
            fields.itemId || '',
            fields.documentId || '',
            fields.relativeFolder || '',
            file.name,
            file.size,
            file.lastModified || 0
        ].join('|');
    }

    function retryableError(message, httpStatus) {
        const error = new Error(message);
        error.retryable = true;
        error.httpStatus = httpStatus;
        return error;
    }

    function log(event, payload) {
        try {
            if (typeof global.debugLog === 'function') {
                global.debugLog(event, payload || {});
            }
        } catch (error) {
            // Diagnostics only.
        }
    }

    // One request; the XHR can be stopped by «Отмена» or the close.
    function request(method, url, body, options) {
        const opts = options || {};
        return new Promise(function (resolve, reject) {
            const xhr = new XMLHttpRequest();
            xhr.open(method, url.href || url, true);
            xhr.timeout = opts.timeout || 2 * 60 * 1000;
            if (opts.contentType) {
                xhr.setRequestHeader('Content-Type', opts.contentType);
            }
            if (opts.onProgress && xhr.upload) {
                xhr.upload.onprogress = function (event) {
                    opts.onProgress(event.loaded || 0);
                };
            }
            xhr.onload = function () {
                let result = {};
                try {
                    result = JSON.parse(String(xhr.responseText || '{}'));
                } catch (error) {
                    result = { ok: false, error: 'Некорректный ответ сервера' };
                }
                resolve({ status: Number(xhr.status || 0), result });
            };
            xhr.onerror = function () {
                reject(retryableError('Ошибка сети при загрузке файла', Number(xhr.status || 0)));
            };
            xhr.ontimeout = function () {
                reject(retryableError('Истекло время загрузки', 408));
            };
            xhr.onabort = function () {
                reject(new Error('upload aborted'));
            };
            if (opts.register) opts.register(xhr);
            xhr.send(body === undefined ? null : body);
        });
    }

    function postJson(url, payload, register) {
        return request('POST', url, JSON.stringify(payload || {}), {
            contentType: 'application/json',
            register
        });
    }

    function abortOnServer(baseUrl, uploadId, reason) {
        if (!uploadId) return;
        try {
            const target = apiUrl(baseUrl, 'abort');
            const body = JSON.stringify({ uploadId, reason: String(reason || '') });
            if (global.navigator && typeof global.navigator.sendBeacon === 'function') {
                global.navigator.sendBeacon(
                    target.href,
                    new Blob([body], { type: 'application/json' })
                );
                return;
            }
            global.fetch(target.href, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body,
                keepalive: true
            }).catch(function () {});
        } catch (error) {
            // The server removes forgotten parts itself.
        }
    }

    async function send(url, formData, progressControl) {
        const file = fileOf(formData);
        const fields = fieldsOf(formData);
        const kind = kindOf(url);
        const key = keyOf(fields, file, kind);
        const control = progressControl || {};
        let aborted = false;
        let currentXhr = null;
        let state = uploads.get(key) || null;

        const register = function (xhr) {
            currentXhr = xhr;
        };
        if (typeof control.setAbort === 'function') {
            control.setAbort(function () {
                aborted = true;
                if (currentXhr) {
                    try { currentXhr.abort(); } catch (error) {}
                }
                const current = uploads.get(key);
                uploads.delete(key);
                abortOnServer(url, current && current.uploadId, 'cancelled');
            });
        }
        const progress = function (bytes) {
            if (typeof control.updateProgress === 'function') {
                control.updateProgress(
                    Math.max(0, Math.min(100, Math.round(bytes / file.size * 100))),
                    bytes
                );
            }
        };

        if (!state) {
            const started = await postJson(apiUrl(url, 'start'), {
                ...fields,
                operationKind: kind,
                fileName: file.name,
                fileSize: file.size
            }, register);
            if (started.status !== 200 || !started.result || started.result.ok === false) {
                return { status: started.status, responseOk: false, result: started.result || {} };
            }
            state = { uploadId: String(started.result.uploadId || ''), receivedBytes: 0 };
            uploads.set(key, state);
            log('chunked_upload_client_started', {
                uploadId: state.uploadId,
                fileName: file.name,
                fileSize: file.size,
                operationKind: kind,
                parts: Math.ceil(file.size / CHUNK_BYTES)
            });
        } else {
            log('chunked_upload_client_resumed', {
                uploadId: state.uploadId,
                fileName: file.name,
                fromBytes: state.receivedBytes
            });
        }

        let offset = Number(state.receivedBytes || 0);
        let mismatches = 0;
        while (offset < file.size) {
            if (aborted) throw new Error('upload aborted');
            const end = Math.min(offset + CHUNK_BYTES, file.size);
            const target = apiUrl(url, 'chunk');
            target.searchParams.set('uploadId', state.uploadId);
            target.searchParams.set('offset', String(offset));
            const sentFrom = offset;
            const answer = await request('PUT', target, file.slice(offset, end), {
                contentType: 'application/octet-stream',
                timeout: CHUNK_TIMEOUT_MS,
                register,
                onProgress: function (loaded) { progress(sentFrom + loaded); }
            });
            const result = answer.result || {};
            if (answer.status === 200 && result.ok !== false) {
                offset = Number(result.receivedBytes || end);
                state.receivedBytes = offset;
                progress(offset);
                // A part came through: the breaks before it do not count.
                if (typeof control.resetBreaks === 'function') control.resetBreaks();
                continue;
            }
            if (answer.status === 409 && result.offsetMismatch) {
                mismatches += 1;
                if (mismatches > 5) {
                    throw retryableError('Не удалось продолжить загрузку файла', 409);
                }
                offset = Number(result.receivedBytes || 0);
                state.receivedBytes = offset;
                continue;
            }
            if (result.uploadMissing) {
                // The server no longer has it (the session ended or it was
                // cleaned): a new attempt starts the file again.
                uploads.delete(key);
                return { status: answer.status, responseOk: false, result };
            }
            if (answer.status >= 500 || answer.status === 400 && result.retryable) {
                throw retryableError(result.error || 'Сервер временно недоступен', answer.status || 0);
            }
            uploads.delete(key);
            abortOnServer(url, state.uploadId, 'failed');
            return { status: answer.status, responseOk: false, result };
        }

        const done = await postJson(apiUrl(url, 'complete'), { uploadId: state.uploadId }, register);
        const result = done.result || {};
        if (done.status === 409 && result.offsetMismatch) {
            state.receivedBytes = Number(result.receivedBytes || 0);
            throw retryableError('Файл получен не полностью, продолжаем', 409);
        }
        if (done.status >= 500) {
            // The server may have added it: a repeated complete answers the same.
            throw retryableError(result.error || 'Сервер временно недоступен', done.status);
        }
        uploads.delete(key);
        log('chunked_upload_client_completed', {
            uploadId: state.uploadId,
            fileName: file.name,
            fileSize: file.size,
            status: done.status
        });
        return {
            status: done.status,
            responseOk: done.status >= 200 && done.status < 300,
            result
        };
    }

    global.ChecklistChunkedUpload = Object.freeze({
        THRESHOLD_BYTES,
        CHUNK_BYTES,
        applies,
        send
    });
})(window);
