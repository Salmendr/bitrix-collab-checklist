(function (global) {
    'use strict';

    // Folder operations of the item window: create, rename, move, delete.
    // Every change belongs to the active edit session (Cancel restores it);
    // Yandex Disk follows after Save.

    const core = global.ChecklistFolderCore;
    if (!core) return;

    const bootstrap = core.bootstrap || {};
    const doc = global.document;

    const dialogId = String(bootstrap.dialogId || '');
    const checklistKey = String(bootstrap.checklistKey || '');
    const itemId = String(bootstrap.itemId || '');
    const itemName = String(bootstrap.itemName || 'Пункт');
    const relativeFolder = String(bootstrap.relativeFolder || '');
    const isTopLevel = bootstrap.itemIsTopLevel === true;
    const moveTargets = Array.isArray(bootstrap.moveTargets)
        ? bootstrap.moveTargets.map(value => String(value || ''))
        : [];
    const childFolderNames = Array.isArray(bootstrap.childFolderNames)
        ? bootstrap.childFolderNames.map(value => String(value || ''))
        : [];
    const deleteAllowedUserIds = new Set(
        Array.isArray(bootstrap.deleteAllowedUserIds)
            ? bootstrap.deleteAllowedUserIds.map(value => String(value || '').trim())
            : []
    );

    const FORBIDDEN_NAME_RE = /[<>:"|?*\\/\u0000-\u001f\u007f]/;
    const MAX_NAME_LENGTH = 255;

    function folderName(path) {
        const parts = String(path || '').split('/').filter(Boolean);
        return parts.length ? parts[parts.length - 1] : '';
    }

    function parentFolder(path) {
        const parts = String(path || '').split('/').filter(Boolean);
        parts.pop();
        return parts.join('/');
    }

    function folderPageUrl(path) {
        const template = String(bootstrap.folderPageUrlTemplate || '');
        if (!path) {
            return template.replace(/&folder=__FOLDER__/, '');
        }
        return template.replace('__FOLDER__', encodeURIComponent(path));
    }

    function validateName(value) {
        const name = String(value || '').replace(/\s+/g, ' ').trim();
        if (!name || name === '.' || name === '..') {
            return { error: 'Введите имя папки' };
        }
        if (FORBIDDEN_NAME_RE.test(name)) {
            return { error: 'Имя не может содержать символы < > : " / \\ | ? *' };
        }
        if (name.length > MAX_NAME_LENGTH) {
            return { error: 'Имя длиннее ' + MAX_NAME_LENGTH + ' символов' };
        }
        if (/[. ]$/.test(name)) {
            return { error: 'Имя не может заканчиваться точкой или пробелом' };
        }
        return { name };
    }

    function sameName(left, right) {
        return String(left || '').toLocaleLowerCase('ru')
            === String(right || '').toLocaleLowerCase('ru');
    }

    function closeDialog(overlay) {
        if (overlay && overlay.parentNode) {
            overlay.parentNode.removeChild(overlay);
        }
    }

    // Small modal: a text field or a list of target folders.
    function openDialog(options) {
        return new Promise(resolve => {
            const overlay = doc.createElement('div');
            overlay.className = 'folder-dialog-overlay';
            overlay.innerHTML = `
                <form class="folder-dialog" role="dialog" aria-modal="true">
                    <h2 class="folder-dialog-title"></h2>
                    <div class="folder-dialog-body"></div>
                    <div class="folder-dialog-error" role="alert" hidden></div>
                    <div class="folder-dialog-actions">
                        <button type="button" class="folder-dialog-cancel">Отмена</button>
                        <button type="submit" class="folder-dialog-submit"></button>
                    </div>
                </form>
            `;
            const form = overlay.querySelector('form');
            const body = overlay.querySelector('.folder-dialog-body');
            const errorBox = overlay.querySelector('.folder-dialog-error');
            const submit = overlay.querySelector('.folder-dialog-submit');
            overlay.querySelector('.folder-dialog-title').textContent = options.title;
            submit.textContent = options.submitLabel || 'Готово';

            let input = null;
            if (options.kind === 'name') {
                input = doc.createElement('input');
                input.type = 'text';
                input.className = 'folder-dialog-input';
                input.maxLength = MAX_NAME_LENGTH;
                input.value = options.value || '';
                input.setAttribute('aria-label', options.title);
                body.appendChild(input);
            } else if (options.kind === 'targets') {
                if (options.note) {
                    const note = doc.createElement('p');
                    note.className = 'folder-dialog-note';
                    note.textContent = options.note;
                    body.appendChild(note);
                }
                const list = doc.createElement('div');
                list.className = 'folder-dialog-targets';
                const firstEnabled = options.targets.findIndex(target => !target.disabled);
                options.targets.forEach((target, index) => {
                    const label = doc.createElement('label');
                    label.className = 'folder-dialog-target'
                        + (target.disabled ? ' is-current' : '');
                    const radio = doc.createElement('input');
                    radio.type = 'radio';
                    radio.name = 'folderMoveTarget';
                    radio.value = target.value;
                    radio.checked = index === firstEnabled;
                    radio.disabled = !!target.disabled;
                    const text = doc.createElement('span');
                    text.textContent = target.label;
                    text.style.paddingLeft = (target.depth * 16) + 'px';
                    label.append(radio, text);
                    list.appendChild(label);
                });
                body.appendChild(list);
            }

            function showError(message) {
                errorBox.textContent = message;
                errorBox.hidden = !message;
            }

            function finish(value) {
                doc.removeEventListener('keydown', onKeyDown, true);
                closeDialog(overlay);
                resolve(value);
            }

            function onKeyDown(event) {
                if (event.key === 'Escape') {
                    event.preventDefault();
                    finish(null);
                }
            }

            overlay.querySelector('.folder-dialog-cancel').addEventListener(
                'click',
                () => finish(null)
            );
            overlay.addEventListener('click', event => {
                if (event.target === overlay) finish(null);
            });
            form.addEventListener('submit', async event => {
                event.preventDefault();
                let value = null;
                if (input) {
                    const checked = validateName(input.value);
                    if (checked.error) {
                        showError(checked.error);
                        input.focus();
                        return;
                    }
                    value = checked.name;
                } else {
                    const selected = form.querySelector('input[name="folderMoveTarget"]:checked');
                    if (!selected) {
                        showError('Выберите папку назначения');
                        return;
                    }
                    value = selected.value;
                }
                if (typeof options.validate === 'function') {
                    const problem = options.validate(value);
                    if (problem) {
                        showError(problem);
                        return;
                    }
                }
                submit.disabled = true;
                showError('');
                try {
                    await options.onSubmit(value);
                    finish(value);
                } catch (error) {
                    submit.disabled = false;
                    showError(error && error.message ? error.message : 'Не удалось выполнить действие');
                }
            });
            doc.addEventListener('keydown', onKeyDown, true);
            doc.body.appendChild(overlay);
            if (input) {
                input.focus();
                input.select();
            } else {
                submit.focus();
            }
        });
    }

    async function postJson(url, body) {
        const actor = core.getActor();
        const sessionId = core.requireSession('изменение папок');
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                dialogId,
                checklistKey,
                actingUserId: actor.id,
                actingUserName: actor.name,
                sessionId,
                requireEditSession: true,
                ...body
            })
        });
        let result = {};
        try {
            result = await response.json();
        } catch (error) {
            throw new Error('Некорректный ответ сервера');
        }
        if (!response.ok || !result.ok) {
            if (result.editSessionError === true) {
                try {
                    const openerApi = global.opener && global.opener.ChecklistPopupEditSession;
                    if (openerApi && typeof openerApi.invalidate === 'function') {
                        openerApi.invalidate(result.error || '');
                    }
                } catch (error) {
                    void error;
                }
            }
            throw new Error(result.error || result.details || 'Не удалось изменить папку');
        }
        return result;
    }

    function notifyChecklist(oldValue, newValue, targetItemId, structureChanged) {
        core.notifyParent('checklist-document-changed', {
            itemId: targetItemId || itemId,
            folderChange: { oldValue, newValue },
            // The popup takes the item list from the server (move, delete).
            structureChanged: structureChanged === true
        });
    }

    async function createFolder() {
        const title = isTopLevel ? 'Новая папка (подпункт)' : 'Новая папка';
        await openDialog({
            kind: 'name',
            title,
            submitLabel: 'Создать',
            validate(name) {
                if (childFolderNames.some(existing => sameName(existing, name))) {
                    return 'Папка «' + name + '» уже есть здесь';
                }
                return '';
            },
            async onSubmit(name) {
                if (isTopLevel) {
                    await postJson(String(bootstrap.addItemApiUrl || ''), {
                        parentItemId: itemId,
                        name
                    });
                } else {
                    await postJson(String(bootstrap.folderCreateApiUrl || ''), {
                        itemId,
                        parentFolder: relativeFolder,
                        name
                    });
                }
                notifyChecklist('', name);
                global.location.reload();
            }
        });
    }

    async function renameFolder() {
        if (!relativeFolder) {
            // The subitem itself: its name is the folder name.
            await openDialog({
                kind: 'name',
                title: 'Переименовать подпункт',
                value: itemName,
                submitLabel: 'Переименовать',
                async onSubmit(name) {
                    if (name === itemName) return;
                    await postJson(String(bootstrap.renameItemApiUrl || ''), {
                        itemId,
                        name
                    });
                    notifyChecklist(itemName, name);
                    global.location.reload();
                }
            });
            return;
        }
        const currentName = folderName(relativeFolder);
        await openDialog({
            kind: 'name',
            title: 'Переименовать папку',
            value: currentName,
            submitLabel: 'Переименовать',
            async onSubmit(name) {
                if (name === currentName) return;
                const result = await postJson(String(bootstrap.folderRenameApiUrl || ''), {
                    itemId,
                    folder: relativeFolder,
                    name
                });
                notifyChecklist(currentName, name);
                global.location.replace(folderPageUrl(String(result.relativeFolder || '')));
            }
        });
    }

    async function moveFolder() {
        if (!relativeFolder) return;
        const currentParent = parentFolder(relativeFolder);
        const targets = [];
        if (currentParent) {
            targets.push({ value: '', label: 'Корень подпункта', depth: 0 });
        }
        moveTargets.forEach(path => {
            const depth = path.split('/').length;
            targets.push({ value: path, label: folderName(path), depth });
        });
        if (!targets.length) {
            alert('Переместить некуда: в подпункте нет других папок');
            return;
        }
        await openDialog({
            kind: 'targets',
            title: 'Переместить «' + folderName(relativeFolder) + '» в…',
            submitLabel: 'Переместить',
            targets,
            async onSubmit(targetFolder) {
                const result = await postJson(String(bootstrap.folderMoveApiUrl || ''), {
                    itemId,
                    folder: relativeFolder,
                    targetFolder
                });
                notifyChecklist(relativeFolder, String(result.relativeFolder || ''));
                global.location.replace(folderPageUrl(String(result.relativeFolder || '')));
            }
        });
    }

    // The item (or subitem) of this window: to another section or into
    // another item. Same edit session; Yandex Disk follows after Save.
    async function moveItem() {
        const raw = Array.isArray(bootstrap.itemMoveTargets) ? bootstrap.itemMoveTargets : [];
        const isSubitem = bootstrap.isSubitem === true;
        const targets = raw.map(target => {
            const isSection = target.kind === 'section';
            return {
                value: (isSection ? 'section:' : 'item:') + String(target.groupId) + ':' + String(target.itemId || ''),
                label: isSection
                    ? 'Раздел «' + String(target.label || '') + '»'
                        + (target.current ? ' — сейчас здесь' : '')
                    : 'внутрь пункта «' + String(target.label || '') + '»'
                        + (target.current ? ' — сейчас здесь' : ''),
                depth: isSection ? 0 : 1,
                disabled: !!target.current
            };
        });
        if (!targets.some(target => !target.disabled)) {
            alert('Переместить некуда');
            return;
        }
        await openDialog({
            kind: 'targets',
            title: 'Переместить «' + itemName + '» в…',
            submitLabel: 'Переместить',
            note: bootstrap.itemHasSubitems === true
                ? 'Пункт с подпунктами можно перенести только в другой раздел.'
                : 'Раздел — пункт станет обычным пунктом раздела; пункт — станет его подпунктом.',
            targets,
            async onSubmit(value) {
                const parts = String(value || '').split(':');
                const kind = parts[0];
                const groupId = Number(parts[1] || 0);
                const hostId = parts.slice(2).join(':');
                const body = { itemId, targetGroupId: groupId };
                if (kind === 'item') {
                    body.targetParentId = hostId;
                } else if (isSubitem) {
                    // A subitem becomes an item of the section.
                    body.targetParentId = '';
                }
                await postJson(String(bootstrap.reorderItemApiUrl || ''), body);
                notifyChecklist('', itemName, itemId, true);
                global.location.reload();
            }
        });
    }

    async function deleteItem(button) {
        const actor = core.getActor();
        if (!deleteAllowedUserIds.has(String(actor.id || '').trim())) {
            alert('У вас недостаточно прав на удаление пунктов');
            return;
        }
        const isSubitem = bootstrap.isSubitem === true;
        const what = isSubitem ? 'подпункт' : 'пункт';
        const tail = isSubitem
            ? 'со всеми папками и файлами'
            : 'со всеми подпунктами, папками и файлами';
        if (!global.confirm(
            'Удалить ' + what + ' «' + itemName + '» ' + tail + '?\n\n'
            + 'До сохранения изменений удаление можно отменить кнопкой «Отмена». '
            + 'После сохранения папка на Яндекс.Диске уйдёт в корзину Яндекса.'
        )) {
            return;
        }
        button.disabled = true;
        try {
            await postJson(String(bootstrap.deleteItemApiUrl || ''), { itemId });
            notifyChecklist(itemName, 'Удалён', itemId, true);
            const backUrl = String(bootstrap.backUrl || '').trim();
            if (backUrl) {
                // A subitem: back to its parent item.
                global.location.replace(backUrl);
            } else {
                core.returnToPopup({ forcePopup: true, preventDefault() {} });
            }
        } catch (error) {
            alert(error && error.message ? error.message : 'Не удалось удалить пункт');
            button.disabled = false;
        }
    }

    async function deleteFolder(button) {
        if (!relativeFolder) return;
        const actor = core.getActor();
        if (!deleteAllowedUserIds.has(String(actor.id || '').trim())) {
            alert('У вас недостаточно прав на удаление файлов');
            return;
        }
        try {
            core.requireSession('удаление папки');
        } catch (error) {
            alert(error.message);
            return;
        }
        const name = folderName(relativeFolder);
        if (!global.confirm(
            'Удалить папку «' + name + '» со всеми вложенными папками и файлами?\n\n'
            + 'До сохранения изменений удаление можно отменить кнопкой «Отмена».'
        )) {
            return;
        }
        button.disabled = true;
        try {
            await postJson(String(bootstrap.folderDeleteApiUrl || ''), {
                itemId,
                folder: relativeFolder
            });
            notifyChecklist(name, 'Удалена');
            global.location.replace(folderPageUrl(parentFolder(relativeFolder)));
        } catch (error) {
            alert(error && error.message ? error.message : 'Не удалось удалить папку');
            button.disabled = false;
        }
    }

    function guarded(action) {
        return async function (event) {
            event.preventDefault();
            const button = event.currentTarget;
            if (button.disabled) return;
            try {
                core.requireSession('изменение папок');
            } catch (error) {
                alert(error.message);
                return;
            }
            try {
                await action(button);
            } catch (error) {
                alert(error && error.message ? error.message : 'Не удалось выполнить действие');
            } finally {
                core.applySessionState();
            }
        };
    }

    // Deleting a folder deletes its files: only for users who may delete
    // files (the server checks it again).
    function hideDeleteWithoutRights() {
        const actor = core.getActor();
        if (deleteAllowedUserIds.has(String(actor && actor.id || '').trim())) return;
        doc.querySelectorAll('[data-role="folder-delete"], [data-role="item-delete"]').forEach(button => {
            button.remove();
        });
    }

    // Links to Yandex Disk: «Перейти по ссылке» / «Скопировать ссылку».
    function openYandexLink(button, url) {
        if (global.ChecklistLinkMenu) {
            global.ChecklistLinkMenu.open(button, url);
        } else {
            global.open(url, '_blank', 'noopener,noreferrer');
        }
    }

    // Nested folders are published on Yandex Disk on the first click.
    async function openYandexFolder(button) {
        if (button.dataset.loading === '1') return;
        const cached = String(button.dataset.yandexUrl || '');
        if (cached) {
            openYandexLink(button, cached);
            return;
        }
        button.dataset.loading = '1';
        button.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch(String(bootstrap.yandexLinkApiUrl || ''), {
                cache: 'no-store'
            });
            const result = await response.json().catch(() => ({}));
            const url = String(result && result.url || '').trim();
            if (!response.ok || !result.ok || !url) {
                throw new Error(result && result.error || 'Не удалось открыть папку на Яндекс.Диске');
            }
            button.dataset.yandexUrl = url;
            openYandexLink(button, url);
        } catch (error) {
            alert(error && error.message ? error.message : 'Не удалось открыть папку на Яндекс.Диске');
        } finally {
            delete button.dataset.loading;
            button.removeAttribute('aria-busy');
        }
    }

    doc.querySelectorAll('[data-role="folder-yandex-lazy"]').forEach(button => {
        button.addEventListener('click', event => {
            event.preventDefault();
            openYandexFolder(button);
        });
    });
    doc.querySelectorAll('a.folder-yandex-link[href]').forEach(link => {
        link.addEventListener('click', event => {
            event.preventDefault();
            openYandexLink(link, link.href);
        });
    });

    if (doc.readyState === 'loading') {
        doc.addEventListener('DOMContentLoaded', hideDeleteWithoutRights, { once: true });
    } else {
        hideDeleteWithoutRights();
    }

    const handlers = {
        'folder-create': createFolder,
        'folder-rename': renameFolder,
        'folder-move': moveFolder,
        'folder-delete': deleteFolder,
        'item-move': moveItem,
        'item-delete': deleteItem
    };
    Object.keys(handlers).forEach(role => {
        doc.querySelectorAll('[data-role="' + role + '"]').forEach(button => {
            button.addEventListener('click', guarded(handlers[role]));
        });
    });

    global.ChecklistFolderTreeActions = Object.freeze({
        folderPageUrl,
        validateName
    });
})(window);
