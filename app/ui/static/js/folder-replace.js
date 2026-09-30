(function (global) {
    'use strict';

    // «Заменить папку»: the whole current contents of this folder (or of the
    // subitem) go to «Архив версий», the dropped folder's contents take their
    // place. The folder keeps its name. Everything belongs to the edit
    // session: Cancel restores the old contents; Yandex Disk follows on Save.

    const core = global.ChecklistFolderCore;
    const doc = global.document;
    const button = doc.getElementById('folderReplaceFolderBtn');
    if (!core || !button) return;

    const bootstrap = core.bootstrap || {};
    const staging = global.ChecklistUploadStaging;
    const planner = global.ChecklistFolderUploadPlanner;
    const uploads = global.ChecklistFolderUploads;
    const relativeFolder = String(bootstrap.relativeFolder || '');
    const folderLabel = String(bootstrap.folderName || bootstrap.itemName || 'Папка');
    const deleteAllowedUserIds = new Set(
        Array.isArray(bootstrap.deleteAllowedUserIds)
            ? bootstrap.deleteAllowedUserIds.map(value => String(value || '').trim())
            : []
    );
    const CONFIRM_TEXT = (
        'Вы уверены что хотите заменить папку? '
        + 'Все файлы заменяемой папки переместятся в архив'
    );

    function text(value) {
        return String(value == null ? '' : value).trim();
    }

    function joinPath() {
        return Array.from(arguments).map(text).filter(Boolean).join('/');
    }

    // Replacing removes files: only for users who may delete files.
    function hideWithoutRights() {
        const actor = core.getActor();
        if (!deleteAllowedUserIds.has(text(actor && actor.id))) {
            button.remove();
        }
    }

    async function postJson(url, body) {
        const actor = core.getActor();
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                dialogId: text(bootstrap.dialogId),
                checklistKey: text(bootstrap.checklistKey),
                actingUserId: actor.id,
                actingUserName: actor.name,
                sessionId: core.requireSession('замена папки'),
                requireEditSession: true,
                ...body
            })
        });
        const result = await response.json().catch(() => ({}));
        if (!response.ok || !result.ok) {
            throw new Error(result && result.error || 'Не удалось заменить папку');
        }
        return result;
    }

    // "Новая/Раздел/файл" → the path inside the replaced folder.
    function innerFolderOf(path) {
        const parts = planner && typeof planner.splitPath === 'function'
            ? planner.splitPath(path)
            : text(path).split('/').filter(Boolean);
        return parts.slice(1).join('/');
    }

    function openDialog() {
        const overlay = doc.createElement('div');
        overlay.className = 'folder-dialog-overlay';
        overlay.innerHTML = `
            <div class="folder-dialog folder-replace-dialog" role="dialog" aria-modal="true">
                <h2 class="folder-dialog-title"></h2>
                <div class="folder-replace-drop" data-role="replace-drop" tabindex="0">
                    <strong>Перетащите сюда новую папку</strong>
                    <small>Её содержимое встанет на место текущего, имя папки останется прежним</small>
                </div>
                <div class="folder-replace-card" data-role="replace-card" hidden>
                    <svg class="upload-staging-folder-icon" viewBox="0 0 44 36" aria-hidden="true">
                        <path d="M3 6.5A2.5 2.5 0 0 1 5.5 4h11l3.5 4h18.5A2.5 2.5 0 0 1 41 10.5v19a2.5 2.5 0 0 1-2.5 2.5h-33A2.5 2.5 0 0 1 3 29.5Z"></path>
                    </svg>
                    <div>
                        <div class="folder-replace-card-name" data-role="replace-name"></div>
                        <div class="folder-replace-card-size" data-role="replace-size"></div>
                    </div>
                </div>
                <div class="folder-replace-progress" data-role="replace-progress" hidden></div>
                <div class="folder-dialog-error" role="alert" hidden></div>
                <div class="folder-dialog-actions">
                    <button type="button" class="folder-dialog-cancel" data-role="replace-close">Отмена</button>
                    <button type="button" class="folder-dialog-submit" data-role="replace-submit" disabled>Заменить</button>
                </div>
            </div>
        `;
        overlay.querySelector('.folder-dialog-title').textContent = 'Заменить папку «' + folderLabel + '»';
        const drop = overlay.querySelector('[data-role="replace-drop"]');
        const card = overlay.querySelector('[data-role="replace-card"]');
        const nameEl = overlay.querySelector('[data-role="replace-name"]');
        const sizeEl = overlay.querySelector('[data-role="replace-size"]');
        const progressEl = overlay.querySelector('[data-role="replace-progress"]');
        const errorEl = overlay.querySelector('.folder-dialog-error');
        const closeButton = overlay.querySelector('[data-role="replace-close"]');
        const submit = overlay.querySelector('[data-role="replace-submit"]');

        const state = {
            dropped: null,
            busy: false,
            replaceId: '',
            failedFiles: []
        };

        function showError(message) {
            errorEl.textContent = text(message);
            errorEl.hidden = !text(message);
        }

        function close() {
            if (state.busy) return;
            if (state.replaceId) {
                // The old contents are already archived: show the result.
                global.location.reload();
                return;
            }
            overlay.remove();
        }

        closeButton.addEventListener('click', close);
        overlay.addEventListener('click', event => {
            if (event.target === overlay) close();
        });

        ['dragenter', 'dragover'].forEach(name => {
            drop.addEventListener(name, event => {
                if (state.busy || state.replaceId) return;
                event.preventDefault();
                if (event.dataTransfer) event.dataTransfer.dropEffect = 'copy';
                drop.classList.add('is-drop-target');
            });
        });
        drop.addEventListener('dragleave', () => drop.classList.remove('is-drop-target'));
        drop.addEventListener('drop', event => {
            event.preventDefault();
            drop.classList.remove('is-drop-target');
            if (state.busy || state.replaceId || !staging) return;
            showError('');
            staging.readDrop(event.dataTransfer).then(result => {
                const folders = result.topEntries.filter(entry => entry.isDirectory);
                if (folders.length !== 1 || result.topEntries.length !== 1 || result.looseCount) {
                    showError('Перетащите одну папку целиком');
                    return;
                }
                const size = result.files.reduce((total, file) => total + Number(file.size || 0), 0);
                state.dropped = {
                    name: folders[0].name,
                    files: result.files,
                    emptyDirs: result.emptyDirs
                };
                nameEl.textContent = folders[0].name;
                sizeEl.textContent = staging.formatBytes(size);
                card.hidden = false;
                submit.disabled = false;
            }).catch(() => showError('Не удалось прочитать перетащенную папку'));
        });

        async function uploadFiles(files) {
            let done = 0;
            const failed = [];
            progressEl.hidden = false;
            progressEl.textContent = 'Загружено 0 из ' + files.length;
            await Promise.all(files.map(file => {
                const placement = {
                    itemId: text(bootstrap.itemId),
                    relativeFolder: joinPath(
                        relativeFolder,
                        innerFolderOf(staging.fileFolderPath(file))
                    ),
                    folderReplaceId: state.replaceId
                };
                return uploads.enqueueUpload(file, placement).then(() => {
                    done += 1;
                }).catch(error => {
                    failed.push({ file, error });
                }).finally(() => {
                    progressEl.textContent = 'Загружено ' + done + ' из ' + files.length;
                });
            }));
            return failed;
        }

        async function createEmptyFolders(dropped) {
            const filePaths = dropped.files.map(file => innerFolderOf(staging.fileFolderPath(file)).toLowerCase());
            for (const dir of dropped.emptyDirs) {
                const inner = innerFolderOf(dir);
                if (!inner) continue;
                const prefix = inner.toLowerCase() + '/';
                if (filePaths.some(path => (path + '/').startsWith(prefix))) continue;
                const parts = inner.split('/');
                const name = parts.pop();
                await postJson(String(bootstrap.folderCreateApiUrl || ''), {
                    itemId: text(bootstrap.itemId),
                    parentFolder: joinPath(relativeFolder, parts.join('/')),
                    name,
                    folderReplaceId: state.replaceId
                });
            }
        }

        function finish(failed) {
            state.failedFiles = failed.map(entry => entry.file);
            core.notifyParent('checklist-document-changed', {
                folderChange: {
                    oldValue: folderLabel,
                    newValue: state.dropped ? state.dropped.name : ''
                }
            });
            if (!failed.length) {
                global.location.reload();
                return;
            }
            showError(
                'Не загружено файлов: ' + failed.length + '. '
                + text(failed[0].error && failed[0].error.message)
            );
            submit.textContent = 'Повторить загрузку';
            submit.disabled = false;
            closeButton.disabled = false;
        }

        submit.addEventListener('click', async () => {
            if (state.busy || !state.dropped) return;
            showError('');
            if (state.replaceId) {
                // Retry only the files that failed; the folder is already replaced.
                state.busy = true;
                submit.disabled = true;
                closeButton.disabled = true;
                const failed = await uploadFiles(state.failedFiles);
                state.busy = false;
                finish(failed);
                return;
            }
            if (!global.confirm(CONFIRM_TEXT)) return;
            state.busy = true;
            submit.disabled = true;
            closeButton.disabled = true;
            drop.hidden = true;
            try {
                progressEl.hidden = false;
                progressEl.textContent = 'Перемещаем текущие файлы в архив…';
                const begun = await postJson(String(bootstrap.folderReplaceApiUrl || ''), {
                    itemId: text(bootstrap.itemId),
                    folder: relativeFolder,
                    newFolderName: state.dropped.name
                });
                state.replaceId = text(begun.folderReplaceId);
                await createEmptyFolders(state.dropped);
                const failed = await uploadFiles(state.dropped.files);
                state.busy = false;
                finish(failed);
            } catch (error) {
                state.busy = false;
                closeButton.disabled = false;
                if (!state.replaceId) {
                    drop.hidden = false;
                    submit.disabled = false;
                    progressEl.hidden = true;
                }
                showError(error && error.message ? error.message : 'Не удалось заменить папку');
            } finally {
                core.applySessionState();
            }
        });

        doc.body.appendChild(overlay);
        drop.focus();
    }

    button.addEventListener('click', event => {
        event.preventDefault();
        if (button.disabled) return;
        try {
            core.requireSession('замена папки');
        } catch (error) {
            alert(error.message);
            return;
        }
        openDialog();
    });

    if (doc.readyState === 'loading') {
        doc.addEventListener('DOMContentLoaded', hideWithoutRights, { once: true });
    } else {
        hideWithoutRights();
    }
})(window);
