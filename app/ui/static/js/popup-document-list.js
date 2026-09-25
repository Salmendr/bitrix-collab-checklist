'use strict';

// Stage 8.2: technical Yandex folder state is separated from the local item folder.
function normalizeYandexFolderStatus(value) {
    const normalized = String(value || '').trim().toLowerCase();
    return ['queued', 'running', 'ready', 'error', 'conflict', 'disabled'].includes(normalized)
        ? normalized
        : '';
}

function getYandexFolderStateText(status, action = '') {
    const normalizedAction = String(action || '').trim();
    if (status === 'conflict') {
        return 'Конфликт папок Яндекс.Диска — нажмите Яндекс для проверки';
    }
    if (status !== 'error') return '';
    if (normalizedAction === 'move_item_folder') {
        return 'Ошибка перемещения папки — нажмите Яндекс для повтора';
    }
    if (normalizedAction === 'rename_item_folder') {
        return 'Ошибка переименования папки — нажмите Яндекс для повтора';
    }
    return 'Ошибка синхронизации папки — нажмите Яндекс для повтора';
}

function assignmentHistoryPanelId(itemId, seriesId) {
    return 'assignment-history-popup-'
        + String(itemId || '').replace(/[^a-zA-Z0-9_-]+/g, '-')
        + '-'
        + String(seriesId || '').replace(/[^a-zA-Z0-9_-]+/g, '-');
}

function documentRelativeFolder(doc) {
    return String(doc && doc.relativeFolder || '')
        .split('/')
        .map(part => part.trim())
        .filter(Boolean)
        .join('/');
}

function pluralFiles(count) {
    const tail = count % 100;
    const last = count % 10;
    const word = (tail >= 11 && tail <= 14)
        ? 'файлов'
        : last === 1
            ? 'файл'
            : (last >= 2 && last <= 4)
                ? 'файла'
                : 'файлов';
    return count + ' ' + word;
}

// Folders of a subitem in the popup: only its top-level folders, each
// leading to its folder page; files inside folders are not listed here.
function buildSubitemFolderRows(item, documents, itemId) {
    const folderKeys = new Map();
    const addFolder = path => {
        const parts = String(path || '').split('/').map(part => part.trim()).filter(Boolean);
        for (let index = 1; index <= parts.length; index += 1) {
            const candidate = parts.slice(0, index).join('/');
            const key = candidate.toLocaleLowerCase('ru');
            if (!folderKeys.has(key)) folderKeys.set(key, candidate);
        }
    };
    (Array.isArray(item && item.subfolders) ? item.subfolders : []).forEach(addFolder);
    documents.forEach(doc => addFolder(documentRelativeFolder(doc)));

    const allFolders = Array.from(folderKeys.values());
    const topFolders = allFolders
        .filter(path => path.indexOf('/') < 0)
        .sort((left, right) => left.localeCompare(right, 'ru'));
    if (!topFolders.length) return '';

    const hasNested = allFolders.some(path => path.indexOf('/') >= 0);
    const editingAllowed = (
        typeof isEditingAllowed !== 'function'
        || isEditingAllowed()
    );
    const rows = topFolders.map(folder => {
        const key = folder.toLocaleLowerCase('ru');
        const count = documents.filter(doc => {
            const docFolder = documentRelativeFolder(doc).toLocaleLowerCase('ru');
            return docFolder === key || docFolder.startsWith(key + '/');
        }).length;
        const folderUrl = appUrl('api/checklist/folder')
            + '?dialogId=' + encodeURIComponent(dialogId)
            + '&checklistKey=' + encodeURIComponent(currentChecklistKey)
            + '&itemId=' + encodeURIComponent(itemId)
            + '&folder=' + encodeURIComponent(folder);
        return `
            <button
                class="doc-folder-row"
                type="button"
                data-role="view-folder"
                data-item-id="${esc(itemId)}"
                data-folder-url="${esc(folderUrl)}"
                title="Открыть папку «${esc(folder)}»"
                ${editingAllowed ? '' : 'disabled'}
            >
                <span class="doc-folder-row-icon" aria-hidden="true">📁</span>
                <span class="doc-folder-row-name">${esc(folder)}</span>
                <span class="doc-folder-row-meta">· ${esc(count ? pluralFiles(count) : 'пусто')}</span>
            </button>
        `;
    }).join('');

    return `
        <div class="doc-folders">
            ${rows}
            ${hasNested ? `
                <div class="doc-folders-note">
                    Есть вложенные папки — чтобы увидеть все папки и файлы,
                    перейдите в папку пункта
                </div>
            ` : ''}
        </div>
    `;
}

// Stage 7.1.1: unified document toolbar and direct per-file replacement controls.
function buildDocumentCell(item) {
    if (normalizeStatus(item && item.status) === 'Не требуется') {
        return '';
    }

    const documents = getItemDocuments(item);
    const itemId = String(item && item.id || '');
    const subitemsApi = window.ChecklistPopupSubitems || null;
    const isSubitem = !!String(item && item.parentItemId || '').trim();
    const allowsFolders = (
        isSubitem
        || !!String(item && item.notRequiredReturnParentId || '').trim()
    );
    // Files of nested folders are shown on the folder page only.
    const rootDocuments = allowsFolders
        ? documents.filter(doc => !documentRelativeFolder(doc))
        : documents;
    const folderRowsHtml = allowsFolders
        ? buildSubitemFolderRows(item, documents, itemId)
        : '';
    // A parent item opens its folder also when only its subitems have files:
    // the folder page lists the subfolders.
    const hasSubitemDocuments = !!(
        subitemsApi
        && !isSubitem
        && subitemsApi.hasSubitemDocuments(item)
    );
    const editingAllowed = (
        typeof isEditingAllowed !== 'function'
        || isEditingAllowed()
    );
    const folderViewUrl = String(item.folderUrl || '').trim() || ((documents.length || hasSubitemDocuments) ? (
        appUrl('api/checklist/folder')
        + '?dialogId=' + encodeURIComponent(dialogId)
        + '&checklistKey=' + encodeURIComponent(currentChecklistKey)
        + '&itemId=' + encodeURIComponent(itemId)
    ) : '');
    const showDocumentActions = documents.length > 0;
    const showViewFolder = (
        !isSubitem
        && (showDocumentActions || hasSubitemDocuments)
        && !!folderViewUrl
    );
    const yandexFolderStatus = normalizeYandexFolderStatus(
        item && item.yandexFolderStatus
    );
    const yandexFolderError = String(
        item && item.yandexFolderError || ''
    ).trim();
    const yandexFolderPath = String(
        item && item.yandexFolderPath || ''
    ).trim();
    const yandexFolderUrl = String(
        item && item.yandexFolderUrl || ''
    ).trim();
    const yandexStructureJobId = String(
        item && item.yandexStructureJobId || ''
    ).trim();
    const yandexStructureAction = String(
        item && item.yandexStructureAction || ''
    ).trim();
    // The item Yandex action follows the same visibility rule for
    // standard and custom items: no current documents means no button.
    let showYandexAction = showDocumentActions || hasSubitemDocuments;
    const stageYandexAvailability = (
        typeof getCurrentStageYandexAvailability === 'function'
            ? getCurrentStageYandexAvailability()
            : { disabled: false, reason: '' }
    );
    const stageYandexDisabled = !!stageYandexAvailability.disabled;
    const mirrorErrorDocuments = documents.filter(doc => (
        String(doc && doc.mirrorStatus || '').trim().toLowerCase() === 'error'
    ));
    const hasMirrorErrors = mirrorErrorDocuments.length > 0;
    const yandexPending = ['queued', 'running'].includes(
        yandexFolderStatus
    );
    const yandexDisabled = yandexFolderStatus === 'disabled';
    const yandexConflict = yandexFolderStatus === 'conflict';
    const yandexRetry = yandexFolderStatus === 'error' || hasMirrorErrors;
    if (isSubitem) {
        // A subitem has no own folder link: only a retry after an error.
        showYandexAction = yandexRetry || yandexConflict;
    }
    const yandexButtonDisabled = (
        stageYandexDisabled
        || yandexPending
        || yandexDisabled
    );
    const yandexButtonTitle = stageYandexDisabled
        ? 'Открыть папку пункта на Яндекс.Диске'
        : yandexConflict
            ? 'Показать конфликт папок Яндекс.Диска'
            : yandexRetry
            ? 'Повторить только неуспешную синхронизацию Яндекс.Диска'
            : yandexPending
                ? 'Операция с папкой Яндекс.Диска выполняется'
                : yandexDisabled
                    ? 'Открыть папку пункта на Яндекс.Диске'
                    : 'Открыть папку пункта на Яндекс.Диске';
    const yandexStateText = getYandexFolderStateText(
        yandexFolderStatus,
        yandexStructureAction
    );

    const actionIcons = window.ChecklistActionIcons;
    const iconSvg = name => (
        actionIcons
        && typeof actionIcons.svg === 'function'
            ? actionIcons.svg(name)
            : ''
    );

    const filesHtml = rootDocuments.map(doc => {
        const docId = String(doc.id || '');
        const docName = String(doc.name || 'Файл');
        const mirrorStatus = String(
            doc.mirrorStatus || ''
        ).trim().toLowerCase();
        const mirrorError = String(
            doc.mirrorError || ''
        ).trim();
        const openUrl = appUrl('api/checklist/file')
            + '?dialogId=' + encodeURIComponent(dialogId)
            + '&checklistKey=' + encodeURIComponent(currentChecklistKey)
            + '&itemId=' + encodeURIComponent(itemId)
            + '&documentId=' + encodeURIComponent(docId);
        const sizeText = formatFileSize(doc.size || 0);
        const mirrorBlocked = ['queued', 'running'].includes(
            mirrorStatus
        );
        const replaceDisabled = (
            !editingAllowed
            || mirrorBlocked
        );
        const replaceTitle = mirrorBlocked
            ? 'Файл ещё синхронизируется с Яндекс.Диском'
            : mirrorStatus === 'error'
                ? 'Заменить файл — потребуется подтверждение ошибки синхронизации'
                : 'Заменить файл';
        const seriesId = String(doc.seriesId || docId);
        const historyCount = Math.max(0, Number(doc.assignmentHistoryCount || 0));
        const historyPanelId = assignmentHistoryPanelId(itemId, seriesId);
        const historyHtml = historyCount > 0 ? `
            <div class="assignment-history-shell">
                <button
                    type="button"
                    class="assignment-history-toggle"
                    data-role="document-assignment-history-toggle"
                    data-panel-id="${esc(historyPanelId)}"
                    data-dialog-id="${esc(dialogId)}"
                    data-checklist-key="${esc(currentChecklistKey)}"
                    data-item-id="${esc(itemId)}"
                    data-series-id="${esc(seriesId)}"
                    aria-expanded="false"
                >
                    <span
                        class="assignment-history-toggle-icon"
                        data-role="assignment-history-toggle-icon"
                        aria-hidden="true"
                    >▸</span>
                    <span>История заданий: ${historyCount}</span>
                </button>
                <div
                    id="${esc(historyPanelId)}"
                    class="assignment-history-panel"
                    aria-live="polite"
                    hidden
                ></div>
            </div>
        ` : '';
        const mirrorErrorHtml = mirrorStatus === 'error' ? `
            <div
                class="doc-yandex-file-error"
                data-role="yandex-file-error"
                title="${esc(mirrorError || 'Ошибка синхронизации файла')}"
            >
                Ошибка синхронизации файла: ${esc(docName)}
            </div>
        ` : '';

        return `
            <div class="doc-file-block">
            <div class="doc-file-row">
                <a
                    href="javascript:void(0)"
                    class="doc-file-link"
                    data-role="view-file"
                    data-item-id="${esc(itemId)}"
                    data-document-id="${esc(docId)}"
                    data-open-url="${esc(openUrl)}"
                >
                    ${esc(docName)}
                </a>
                ${sizeText ? `<span class="doc-file-meta">${esc(sizeText)}</span>` : '<span class="doc-file-meta"></span>'}
                <button
                    class="doc-file-replace checklist-action-button checklist-action-button-replace"
                    type="button"
                    data-role="replace-document"
                    data-item-id="${esc(itemId)}"
                    data-document-id="${esc(docId)}"
                    data-document-name="${esc(docName)}"
                    data-series-id="${esc(String(doc.seriesId || ''))}"
                    data-mirror-status="${esc(mirrorStatus)}"
                    data-mirror-error="${esc(mirrorError)}"
                    title="${esc(replaceTitle)}"
                    aria-label="${esc(replaceTitle)}"
                    ${replaceDisabled ? 'disabled' : ''}
                >
                    ${iconSvg('replace')}
                </button>
                <a class="doc-file-download checklist-action-button checklist-action-button-download"
                   href="${esc(openUrl + '&download=1')}" title="Скачать" aria-label="Скачать ${esc(docName)}">
                    ${iconSvg('download')}
                </a>
            </div>
            ${mirrorErrorHtml}
            ${historyHtml}
            </div>
        `;
    }).join('');

    return `
        <div class="doc-cell">
            <div class="doc-actions" role="toolbar" aria-label="Действия с документами пункта">
                <button
                    class="upload-btn doc-icon-btn checklist-action-button checklist-action-button-upload"
                    type="button"
                    data-role="upload"
                    data-icon="upload-to-app"
                    data-item-id="${esc(itemId)}"
                    title="Загрузить файлы в пункт"
                    aria-label="Загрузить файлы в пункт"
                    ${typeof disabledAttr === 'function' ? disabledAttr() : ''}
                >
                    ${iconSvg('upload')}
                </button>

                ${showViewFolder ? `
                    <button
                        class="doc-btn doc-icon-btn checklist-action-button checklist-action-button-folder"
                        type="button"
                        data-role="view-folder"
                        data-icon="view-folder"
                        data-item-id="${esc(itemId)}"
                        data-folder-url="${esc(folderViewUrl)}"
                        title="Открыть папку пункта"
                        aria-label="Открыть папку пункта"
                        aria-disabled="${editingAllowed ? 'false' : 'true'}"
                        ${editingAllowed ? '' : 'disabled'}
                    >
                        ${iconSvg('folder')}
                    </button>
                ` : ''}

                ${showYandexAction ? `
                    <button
                        class="doc-icon-btn checklist-action-button checklist-action-button-yandex"
                        type="button"
                        data-role="view-yandex-folder"
                        data-item-id="${esc(itemId)}"
                        data-yandex-status="${esc(yandexFolderStatus)}"
                        data-yandex-error="${esc(yandexFolderError)}"
                        data-yandex-folder-path="${esc(yandexFolderPath)}"
                        data-yandex-folder-url="${esc(yandexFolderUrl)}"
                        data-yandex-action="${esc(yandexStructureAction)}"
                        data-structure-job-id="${esc(yandexStructureJobId)}"
                        data-has-mirror-errors="${hasMirrorErrors ? '1' : '0'}"
                        data-stage-yandex-disabled="${stageYandexDisabled ? '1' : '0'}"
                        title="${esc(yandexButtonTitle)}"
                        aria-label="${esc(yandexButtonTitle)}"
                        aria-disabled="${yandexButtonDisabled ? 'true' : 'false'}"
                        aria-busy="${yandexPending ? 'true' : 'false'}"
                        ${yandexButtonDisabled ? 'disabled' : ''}
                    >
                        ${iconSvg('yandex')}
                    </button>
                ` : ''}

                ${subitemsApi ? subitemsApi.buildToggle(item) : ''}
            </div>

            ${yandexStateText ? `
                <div
                    class="doc-yandex-structure-state"
                    data-role="yandex-structure-state"
                    data-status="${esc(yandexFolderStatus)}"
                    data-yandex-status="${esc(yandexFolderStatus)}"
                    data-item-id="${esc(itemId)}"
                    data-structure-job-id="${esc(yandexStructureJobId)}"
                    title="${esc(yandexFolderError || yandexStateText)}"
                >
                    <span class="doc-yandex-structure-dot" aria-hidden="true"></span>
                    <span>${esc(yandexStateText)}</span>
                </div>
            ` : ''}

            ${rootDocuments.length ? `
                <div class="doc-files">
                    ${filesHtml}
                </div>
            ` : ''}

            ${folderRowsHtml}

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
}

window.ChecklistPopupDocuments = Object.freeze({
    buildDocumentCell
});
