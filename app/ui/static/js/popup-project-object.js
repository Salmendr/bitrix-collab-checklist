(function (global) {
    'use strict';

    // The Bitrix24 object of the project in the checklist header: the legal
    // name and the cipher (click copies), «Перейти к объекту» (the card of
    // the object of this checklist, else of the main object) and «выбрать»
    // for administrators and GIPs when the objects give several variants.

    const doc = global.document;
    const barEl = doc.getElementById('projectObjectBar');
    if (!barEl) return;

    const bootstrapObject = (
        popupBootstrap.projectObject && typeof popupBootstrap.projectObject === 'object'
    ) ? popupBootstrap.projectObject : {};

    const state = {
        byKey: bootstrapObject.byKey && typeof bootstrapObject.byKey === 'object'
            ? bootstrapObject.byKey
            : {},
        managerUserIds: Array.isArray(bootstrapObject.managerUserIds)
            ? bootstrapObject.managerUserIds.map(String)
            : [],
        renderedKey: '',
        menu: '',
        loading: false,
        busy: false
    };

    function text(value) {
        return String(value == null ? '' : value).trim();
    }

    function escHtml(value) {
        return text(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function currentKey() {
        return String(currentChecklistKey || 'id');
    }

    function currentView() {
        return state.byKey[currentKey()] || null;
    }

    function currentUserId() {
        return text(global.currentEditor && global.currentEditor.id);
    }

    function canChoose() {
        const userId = currentUserId();
        if (!userId) return false;
        const phases = global.ChecklistPopupProjectPhases;
        if (phases && phases.state && Array.isArray(phases.state.adminUserIds)
            && phases.state.adminUserIds.includes(userId)) {
            return true;
        }
        return state.managerUserIds.includes(userId);
    }

    function applyPayload(result) {
        if (!result || typeof result !== 'object') return;
        if (result.byKey && typeof result.byKey === 'object') state.byKey = result.byKey;
        if (Array.isArray(result.managerUserIds)) state.managerUserIds = result.managerUserIds.map(String);
        pushToGipField();
        render();
    }

    function pushToGipField() {
        const phases = global.ChecklistPopupProjectPhases;
        if (phases && typeof phases.applyObject === 'function') {
            phases.applyObject(currentView(), state.managerUserIds);
        }
    }

    async function copyText(value) {
        try {
            if (global.navigator && global.navigator.clipboard && global.isSecureContext) {
                await global.navigator.clipboard.writeText(value);
                return true;
            }
        } catch (error) {
            // Clipboard API can be blocked inside the Bitrix24 frame.
        }
        const area = doc.createElement('textarea');
        area.value = value;
        area.setAttribute('readonly', '');
        area.style.position = 'fixed';
        area.style.top = '-1000px';
        area.style.opacity = '0';
        doc.body.appendChild(area);
        area.select();
        let copied = false;
        try {
            copied = doc.execCommand('copy');
        } catch (error) {
            copied = false;
        }
        area.remove();
        return copied;
    }

    function flash(element, message) {
        if (!element) return;
        const hint = element.querySelector('.project-object-copied');
        if (hint) {
            hint.textContent = message;
            hint.hidden = false;
        }
        element.classList.add('copied');
        global.clearTimeout(element._copiedTimer);
        element._copiedTimer = global.setTimeout(function () {
            element.classList.remove('copied');
            if (hint) hint.hidden = true;
        }, 1400);
    }

    async function onCopy(element) {
        const value = text(element && element.dataset.copyValue);
        if (!value) return;
        const copied = await copyText(value);
        flash(element, copied ? 'Скопировано' : 'Не удалось скопировать');
    }

    function openObject(event, view) {
        const path = text(view && view.objectPath);
        if (!path) return;
        const bx = global.BX24;
        const bridge = global.ChecklistPopupBitrix;
        if (bx && typeof bx.openPath === 'function') {
            event.preventDefault();
            // The card opens in the Bitrix24 slider; outside Bitrix24 the
            // link opens it in a new tab.
            const open = function () {
                try {
                    bx.openPath(path, function (result) {
                        if (result && result.result === 'error') {
                            openInTab(view);
                        }
                    });
                } catch (error) {
                    openInTab(view);
                }
            };
            if (bridge && typeof bridge.init === 'function') {
                bridge.init().then(function (ready) {
                    if (ready) open();
                    else openInTab(view);
                });
            } else {
                open();
            }
            return;
        }
        if (!text(view.objectUrl)) {
            event.preventDefault();
            openInTab(view);
        }
    }

    function objectUrl(view) {
        const url = text(view && view.objectUrl);
        if (url) return url;
        const bx = global.BX24;
        try {
            const domain = bx && typeof bx.getDomain === 'function' ? text(bx.getDomain()) : '';
            if (domain) return 'https://' + domain + text(view.objectPath);
        } catch (error) {
            // No domain outside Bitrix24.
        }
        return '';
    }

    function openInTab(view) {
        const url = objectUrl(view);
        if (url) global.open(url, '_blank', 'noopener');
    }

    async function choose(field, value) {
        if (state.busy) return;
        state.busy = true;
        try {
            const identity = typeof getCurrentEditorIdentity === 'function'
                ? getCurrentEditorIdentity()
                : { userId: currentUserId() };
            const response = await fetch(appUrl('api/checklist/project-object/choose'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    dialogId,
                    field,
                    value,
                    actingUserId: identity.userId
                })
            });
            const result = await response.json().catch(() => ({}));
            if (!response.ok || !result.ok) {
                throw new Error(result && result.error || 'Не удалось сохранить выбор');
            }
            state.menu = '';
            applyPayload(result);
        } catch (error) {
            global.alert(error && error.message ? error.message : 'Не удалось сохранить выбор');
        } finally {
            state.busy = false;
        }
    }

    function menuHtml(field, variants, current) {
        const options = variants.map(function (value) {
            const selected = value === current;
            return '<button type="button" class="project-object-option' + (selected ? ' selected' : '') + '"'
                + ' data-choose-field="' + field + '" data-choose-value="' + escHtml(value) + '"'
                + (selected ? ' aria-current="true"' : '') + '>'
                + escHtml(value)
                + '</button>';
        }).join('');
        const title = field === 'cipher' ? 'Шифр для всех чек-листов проекта' : 'Наименование для всех чек-листов проекта';
        return '<div class="project-object-menu" role="menu">'
            + '<div class="project-object-menu-title">' + title + '</div>'
            + options
            + '</div>';
    }

    function fieldHtml(field, label, value, variants, view) {
        const chooser = canChoose() && variants.length > 1;
        return '<span class="project-object-field project-object-' + field + '">'
            + (label ? '<span class="project-object-label">' + label + '</span>' : '')
            + '<span class="project-object-value" role="button" tabindex="0"'
            + ' data-copy-value="' + escHtml(value) + '" title="Нажмите, чтобы скопировать">'
            + escHtml(value)
            + '<span class="project-object-copied" hidden></span>'
            + '</span>'
            + (chooser
                ? '<button type="button" class="project-object-choose" data-menu="' + field + '"'
                    + ' title="Вариантов: ' + variants.length + '">выбрать</button>'
                : '')
            + (chooser && state.menu === field ? menuHtml(field, variants, value) : '')
            + '</span>';
    }

    function render() {
        const view = currentView();
        state.renderedKey = currentKey();
        if (!view || !view.configured) {
            barEl.hidden = true;
            barEl.innerHTML = '';
            return;
        }
        const parts = [];
        if (state.loading && !view.objectDriven) {
            parts.push('<span class="project-object-note">Загружаем данные объекта…</span>');
        } else if (!view.objectDriven && text(view.error)) {
            parts.push('<span class="project-object-note error" title="' + escHtml(view.error) + '">'
                + 'Не удалось получить данные объекта: ' + escHtml(view.error) + '</span>');
        }
        const legalName = text(view.legalName);
        const cipher = text(view.cipher);
        const legalNames = Array.isArray(view.legalNames) ? view.legalNames.map(text) : [];
        const ciphers = Array.isArray(view.ciphers) ? view.ciphers.map(text) : [];
        if (legalName) parts.push(fieldHtml('legalName', '', legalName, legalNames, view));
        if (cipher) parts.push(fieldHtml('cipher', 'Шифр:', cipher, ciphers, view));
        if (text(view.objectPath)) {
            const href = objectUrl(view) || '#';
            parts.push('<a class="project-object-link" href="' + escHtml(href) + '" target="_blank" rel="noopener"'
                + ' title="Объект #' + escHtml(view.objectId) + ' в Битрикс24">Перейти к объекту</a>');
        }
        if (!parts.length) {
            barEl.hidden = true;
            barEl.innerHTML = '';
            return;
        }
        barEl.innerHTML = parts.join('');
        barEl.hidden = false;
    }

    barEl.addEventListener('click', function (event) {
        const view = currentView();
        const option = event.target.closest('[data-choose-field]');
        if (option) {
            choose(option.dataset.chooseField, option.dataset.chooseValue);
            return;
        }
        const toggle = event.target.closest('[data-menu]');
        if (toggle) {
            state.menu = state.menu === toggle.dataset.menu ? '' : toggle.dataset.menu;
            render();
            return;
        }
        const value = event.target.closest('[data-copy-value]');
        if (value) {
            onCopy(value);
            return;
        }
        const link = event.target.closest('.project-object-link');
        if (link) openObject(event, view);
    });

    barEl.addEventListener('keydown', function (event) {
        if (event.key !== 'Enter' && event.key !== ' ') return;
        const value = event.target.closest('[data-copy-value]');
        if (value) {
            event.preventDefault();
            onCopy(value);
        }
    });

    doc.addEventListener('click', function (event) {
        if (state.menu && !event.target.closest('.project-object-field')) {
            state.menu = '';
            render();
        }
    });

    doc.addEventListener('keydown', function (event) {
        if (event.key === 'Escape' && state.menu) {
            state.menu = '';
            render();
        }
    });

    // The object id is known but its cards were not read yet: the server
    // reads them once.
    async function loadIfNeeded() {
        const views = Object.values(state.byKey || {});
        if (!views.some(view => view && view.needsFetch)) return;
        state.loading = true;
        render();
        try {
            const response = await fetch(
                appUrl('api/checklist/project-object') + '?dialogId=' + encodeURIComponent(dialogId)
            );
            const result = await response.json().catch(() => ({}));
            if (response.ok && result.ok) applyPayload(result);
        } catch (error) {
            console.log('project object load skipped:', error);
        } finally {
            state.loading = false;
            render();
        }
    }

    // Called by renderAll: the popup switches checklists in place.
    function refresh() {
        if (state.renderedKey !== currentKey()) {
            state.menu = '';
            pushToGipField();
        }
        render();
    }

    render();
    pushToGipField();
    loadIfNeeded();
    // Rights («выбрать») depend on the Bitrix user, which arrives later.
    Promise.resolve(
        typeof fetchCurrentUserIfPossible === 'function'
            ? fetchCurrentUserIfPossible()
            : null
    ).catch(() => null).then(render);

    global.ChecklistPopupProjectObject = Object.freeze({
        refresh,
        render,
        state
    });
})(window);
