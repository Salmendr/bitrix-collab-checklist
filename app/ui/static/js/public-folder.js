(function () {
    'use strict';

    const pathMarker = '/public/folder/';
    const markerIndex = window.location.pathname.indexOf(pathMarker);
    const pageMessage = document.getElementById('publicPageMessage');

    if (markerIndex < 0) {
        if (pageMessage) {
            pageMessage.textContent = 'Не удалось определить адрес общей папки.';
            pageMessage.classList.add('is-error');
        }
        return;
    }

    const appBasePath = window.location.pathname.slice(0, markerIndex);
    const tokenTail = window.location.pathname.slice(
        markerIndex + pathMarker.length
    );
    const token = decodeURIComponent(tokenTail.split('/')[0] || '');
    const apiRoot = (
        appBasePath
        + '/api/public-folder/'
        + encodeURIComponent(token)
    );

    const firstNameInput = document.getElementById('publicFirstName');
    const lastNameInput = document.getElementById('publicLastName');
    const identityHint = document.getElementById('publicIdentityHint');
    const uploadZone = document.getElementById('publicUploadZone');
    const uploadInput = document.getElementById('publicUploadInput');
    const chooseFilesButton = document.getElementById('publicChooseFilesBtn');
    const replaceInput = document.getElementById('publicReplaceInput');
    const refreshButton = document.getElementById('publicRefreshBtn');
    const currentFilesElement = document.getElementById('publicCurrentFiles');
    const currentSummary = document.getElementById('publicCurrentSummary');
    const archiveGroupsElement = document.getElementById('publicArchiveGroups');
    const archiveSummary = document.getElementById('publicArchiveSummary');
    const operationsCard = document.getElementById('publicOperationsCard');
    const operationList = document.getElementById('publicOperationList');
    const checklistTitle = document.getElementById('publicChecklistTitle');
    const itemNameElement = document.getElementById('publicItemName');

    const operations = new Map();
    let loadingState = false;
    let replaceTarget = null;

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    function formatBytes(value) {
        const bytes = Math.max(0, Number(value) || 0);
        if (!bytes) return '0 Б';
        const units = ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ'];
        const index = Math.min(
            units.length - 1,
            Math.floor(Math.log(bytes) / Math.log(1024))
        );
        const amount = bytes / Math.pow(1024, index);
        return (index === 0 ? amount.toFixed(0) : amount.toFixed(amount >= 10 ? 1 : 2))
            .replace('.', ',') + ' ' + units[index];
    }

    function formatDate(value) {
        const normalized = String(value || '').trim();
        if (!normalized) return 'Дата не указана';
        const parsed = new Date(normalized);
        if (Number.isNaN(parsed.getTime())) return normalized;
        return new Intl.DateTimeFormat('ru-RU', {
            dateStyle: 'short',
            timeStyle: 'short'
        }).format(parsed);
    }

    function setPageMessage(message, error = false) {
        if (!pageMessage) return;
        pageMessage.textContent = String(message || '');
        pageMessage.classList.toggle('is-error', !!error);
    }

    function identity() {
        return {
            firstName: String(firstNameInput && firstNameInput.value || '').trim(),
            lastName: String(lastNameInput && lastNameInput.value || '').trim()
        };
    }

    function saveIdentity() {
        const actor = identity();
        try {
            window.sessionStorage.setItem(
                'public-folder-identity:' + token,
                JSON.stringify(actor)
            );
        } catch (error) {
            // Storage is optional; required fields remain visible on the page.
        }
    }

    function restoreIdentity() {
        try {
            const stored = JSON.parse(
                window.sessionStorage.getItem(
                    'public-folder-identity:' + token
                ) || '{}'
            );
            if (firstNameInput) firstNameInput.value = String(stored.firstName || '');
            if (lastNameInput) lastNameInput.value = String(stored.lastName || '');
        } catch (error) {
            // Ignore unavailable or malformed session storage.
        }
    }

    function requireIdentity() {
        const actor = identity();
        const firstValid = !!actor.firstName;
        const lastValid = !!actor.lastName;

        if (firstNameInput) firstNameInput.classList.toggle('is-invalid', !firstValid);
        if (lastNameInput) lastNameInput.classList.toggle('is-invalid', !lastValid);

        if (!firstValid || !lastValid) {
            if (identityHint) {
                identityHint.textContent = 'Перед отправкой обязательно заполните имя и фамилию.';
                identityHint.classList.add('is-error');
            }
            (firstValid ? lastNameInput : firstNameInput).focus();
            return null;
        }

        if (identityHint) {
            identityHint.textContent = (
                'В графе «Кем загружено» будет указано: '
                + actor.firstName + ' ' + actor.lastName + '.'
            );
            identityHint.classList.remove('is-error');
        }
        saveIdentity();
        return actor;
    }

    function operationStateText(operation) {
        const status = String(operation.status || 'queued');
        if (status === 'completed') return 'Файл принят и добавлен в папку.';
        if (status === 'conflict') {
            return operation.error || 'Операция остановлена из-за изменения текущей версии.';
        }
        if (status === 'error') return operation.error || 'Не удалось обработать файл.';
        if (operation.waitingForEditSession) {
            return 'Файл принят и ожидает завершения редактирования чек-листа.';
        }
        if (status === 'processing') return 'Файл принят, изменения применяются…';
        if (status === 'receiving') return 'Файл передаётся приложению…';
        return 'Файл принят и ожидает обработки…';
    }

    function renderOperations() {
        if (!operationsCard || !operationList) return;
        const rows = Array.from(operations.values());
        operationsCard.hidden = rows.length === 0;
        operationList.innerHTML = rows.map(operation => {
            const status = String(operation.status || 'queued');
            const operationLabel = operation.operationType === 'replace'
                ? 'Замена'
                : 'Загрузка';
            return `
                <div class="public-operation-row is-${escapeHtml(status)}">
                    <div class="public-operation-title">
                        ${escapeHtml(operationLabel)}: ${escapeHtml(operation.fileName || operation.originalFileName || 'Файл')}
                    </div>
                    <div class="public-operation-state">
                        ${escapeHtml(operationStateText(operation))}
                    </div>
                </div>
            `;
        }).join('');
    }

    async function parseResponse(response) {
        const payload = await response.json().catch(() => ({}));
        if (!response.ok && !(payload.accepted && payload.operation)) {
            throw new Error(payload.error || payload.details || 'Запрос не выполнен');
        }
        return payload;
    }

    async function pollOperation(operationId) {
        const normalizedId = String(operationId || '');
        if (!normalizedId) return;

        for (let attempt = 0; attempt < 900; attempt += 1) {
            await new Promise(resolve => window.setTimeout(resolve, 2000));
            try {
                const response = await fetch(
                    apiRoot + '/operations/' + encodeURIComponent(normalizedId),
                    { cache: 'no-store', credentials: 'omit' }
                );
                const payload = await parseResponse(response);
                const operation = payload.operation || {};
                operations.set(normalizedId, operation);
                renderOperations();
                if (['completed', 'conflict', 'error'].includes(String(operation.status || ''))) {
                    if (operation.status === 'completed') await loadState();
                    return;
                }
            } catch (error) {
                setPageMessage(error && error.message || 'Не удалось проверить операцию', true);
                return;
            }
        }
    }

    async function sendFile(file, options = {}) {
        const actor = requireIdentity();
        if (!actor || !file) return;

        const operationType = options.documentId ? 'replace' : 'upload';
        const provisionalId = 'receiving-' + Date.now() + '-' + Math.random();
        operations.set(provisionalId, {
            operationId: provisionalId,
            operationType,
            fileName: file.name,
            status: 'receiving'
        });
        renderOperations();

        const form = new FormData();
        form.append('file', file, file.name);
        form.append('firstName', actor.firstName);
        form.append('lastName', actor.lastName);
        if (options.documentId) {
            form.append('documentId', options.documentId);
            form.append('forceReplace', options.forceReplace ? '1' : '0');
        }

        try {
            const response = await fetch(
                apiRoot + (options.documentId ? '/replace' : '/upload'),
                {
                    method: 'POST',
                    body: form,
                    credentials: 'omit',
                    cache: 'no-store'
                }
            );
            const payload = await parseResponse(response);
            operations.delete(provisionalId);
            const operation = payload.operation || {};
            const operationId = String(operation.operationId || provisionalId);
            operations.set(operationId, operation);
            renderOperations();

            if (operation.status === 'completed') {
                await loadState();
            } else if (!['conflict', 'error'].includes(String(operation.status || ''))) {
                pollOperation(operationId);
            }
        } catch (error) {
            operations.set(provisionalId, {
                operationId: provisionalId,
                operationType,
                fileName: file.name,
                status: 'error',
                error: error && error.message || 'Файл не отправлен'
            });
            renderOperations();
        }
    }

    async function sendUploads(files) {
        const list = Array.from(files || []).filter(Boolean);
        if (!list.length || !requireIdentity()) return;
        await Promise.allSettled(list.map(file => sendFile(file)));
    }

    function renderCurrentFiles(files) {
        const rows = Array.isArray(files) ? files : [];
        if (currentSummary) {
            currentSummary.textContent = rows.length
                ? 'Текущих файлов: ' + rows.length
                : 'В папке пока нет текущих файлов.';
        }
        if (!currentFilesElement) return;
        if (!rows.length) {
            currentFilesElement.innerHTML = '<div class="public-empty-state">В папке пока нет файлов</div>';
            return;
        }

        currentFilesElement.innerHTML = rows.map(file => {
            const replaceTitle = file.replaceBlocked
                ? 'Замена станет доступна после завершения синхронизации'
                : 'Заменить текущую версию';
            return `
                <article class="public-file-row">
                    <div class="public-file-main">
                        <a class="public-file-name" href="${escapeHtml(file.openUrl)}" target="_blank" rel="noopener nofollow noreferrer">
                            ${escapeHtml(file.name || 'Файл')}
                        </a>
                        <div class="public-file-meta">
                            ${escapeHtml(formatBytes(file.size))} · ${escapeHtml(formatDate(file.uploadedAt))}
                        </div>
                    </div>
                    <div class="public-file-uploader">
                        Кем загружено: <strong>${escapeHtml(file.uploadedByName || '—')}</strong>
                    </div>
                    <div class="public-file-actions">
                        <a class="public-download-link" href="${escapeHtml(file.downloadUrl)}" rel="nofollow noreferrer">
                            Скачать
                        </a>
                        <button
                            class="public-replace-button"
                            type="button"
                            data-role="public-replace"
                            data-document-id="${escapeHtml(file.documentId)}"
                            data-document-name="${escapeHtml(file.name || 'Файл')}"
                            data-requires-force="${file.requiresForceReplace ? '1' : '0'}"
                            title="${escapeHtml(replaceTitle)}"
                            ${file.replaceBlocked ? 'disabled' : ''}
                        >
                            Заменить
                        </button>
                    </div>
                </article>
            `;
        }).join('');

        currentFilesElement.querySelectorAll('[data-role="public-replace"]').forEach(button => {
            button.addEventListener('click', function () {
                if (!requireIdentity()) return;
                const requiresForce = this.dataset.requiresForce === '1';
                if (
                    requiresForce
                    && !window.confirm(
                        'У текущего файла есть ошибка синхронизации. Всё равно принять новую версию?'
                    )
                ) {
                    return;
                }
                replaceTarget = {
                    documentId: String(this.dataset.documentId || ''),
                    documentName: String(this.dataset.documentName || 'Файл'),
                    forceReplace: requiresForce
                };
                replaceInput.value = '';
                replaceInput.click();
            });
        });
    }

    function renderArchive(groups) {
        const rows = Array.isArray(groups) ? groups : [];
        const versionsCount = rows.reduce(
            (total, group) => total + (Array.isArray(group.versions) ? group.versions.length : 0),
            0
        );
        if (archiveSummary) {
            archiveSummary.textContent = versionsCount
                ? 'Архивных версий: ' + versionsCount
                : 'Архивных версий пока нет.';
        }
        if (!archiveGroupsElement) return;
        if (!rows.length) {
            archiveGroupsElement.innerHTML = '<div class="public-empty-state">Архив пока пуст</div>';
            return;
        }

        archiveGroupsElement.innerHTML = rows.map(group => `
            <details class="public-archive-group">
                <summary>
                    ${escapeHtml(group.label || 'Архив версий')}
                    · ${Number(group.versions && group.versions.length || 0)}
                </summary>
                <div class="public-archive-versions">
                    ${(group.versions || []).map(version => `
                        <div class="public-archive-version">
                            <div class="public-version-label">${escapeHtml(version.versionLabel || '')}</div>
                            <div class="public-file-main">
                                <a class="public-file-name" href="${escapeHtml(version.openUrl)}" target="_blank" rel="noopener nofollow noreferrer">
                                    ${escapeHtml(version.name || 'Архивный файл')}
                                </a>
                                <div class="public-file-meta">
                                    ${escapeHtml(formatBytes(version.size))} · ${escapeHtml(formatDate(version.archivedAt || version.uploadedAt))}
                                    · ${escapeHtml(version.uploadedByName || '—')}
                                </div>
                            </div>
                            <a class="public-download-link" href="${escapeHtml(version.downloadUrl)}" rel="nofollow noreferrer">Скачать</a>
                        </div>
                    `).join('')}
                </div>
            </details>
        `).join('');
    }

    async function loadState() {
        if (loadingState) return;
        loadingState = true;
        if (refreshButton) refreshButton.disabled = true;
        try {
            const response = await fetch(apiRoot, {
                cache: 'no-store',
                credentials: 'omit'
            });
            const state = await parseResponse(response);
            if (!state.permissions || state.permissions.delete !== false) {
                throw new Error('Получен некорректный набор прав общей папки');
            }
            if (checklistTitle) checklistTitle.textContent = state.checklistTitle || 'Общая папка чек-листа';
            if (itemNameElement) itemNameElement.textContent = state.itemName || 'Папка пункта';
            document.title = (state.itemName || 'Папка пункта') + ' — общая папка';
            renderCurrentFiles(state.currentFiles);
            renderArchive(state.archiveGroups);
            setPageMessage('');
        } catch (error) {
            setPageMessage(error && error.message || 'Не удалось загрузить папку', true);
            if (currentSummary) currentSummary.textContent = 'Список недоступен';
            if (archiveSummary) archiveSummary.textContent = 'Архив недоступен';
        } finally {
            loadingState = false;
            if (refreshButton) refreshButton.disabled = false;
        }
    }

    function preventDragDefaults(event) {
        event.preventDefault();
        event.stopPropagation();
    }

    restoreIdentity();
    [firstNameInput, lastNameInput].forEach(input => {
        if (!input) return;
        input.addEventListener('input', function () {
            this.classList.remove('is-invalid');
            const actor = identity();
            if (actor.firstName && actor.lastName) {
                saveIdentity();
                if (identityHint) {
                    identityHint.textContent = (
                        'В графе «Кем загружено» будет указано: '
                        + actor.firstName + ' ' + actor.lastName + '.'
                    );
                    identityHint.classList.remove('is-error');
                }
            }
        });
    });

    if (chooseFilesButton) {
        chooseFilesButton.addEventListener('click', function (event) {
            event.stopPropagation();
            if (requireIdentity()) uploadInput.click();
        });
    }
    if (uploadZone) {
        uploadZone.addEventListener('click', function (event) {
            if (event.target === chooseFilesButton) return;
            if (requireIdentity()) uploadInput.click();
        });
        uploadZone.addEventListener('keydown', function (event) {
            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                if (requireIdentity()) uploadInput.click();
            }
        });
        ['dragenter', 'dragover'].forEach(eventName => {
            uploadZone.addEventListener(eventName, function (event) {
                preventDragDefaults(event);
                uploadZone.classList.add('is-drag-over');
            });
        });
        ['dragleave', 'drop'].forEach(eventName => {
            uploadZone.addEventListener(eventName, function (event) {
                preventDragDefaults(event);
                uploadZone.classList.remove('is-drag-over');
            });
        });
        uploadZone.addEventListener('drop', function (event) {
            const files = event.dataTransfer && event.dataTransfer.files;
            sendUploads(files);
        });
    }
    if (uploadInput) {
        uploadInput.addEventListener('change', function () {
            const files = Array.from(this.files || []);
            this.value = '';
            sendUploads(files);
        });
    }
    if (replaceInput) {
        replaceInput.addEventListener('change', function () {
            const file = this.files && this.files[0];
            this.value = '';
            if (!file || !replaceTarget) return;
            const options = replaceTarget;
            replaceTarget = null;
            sendFile(file, options);
        });
    }
    if (refreshButton) refreshButton.addEventListener('click', loadState);

    loadState();
})();
