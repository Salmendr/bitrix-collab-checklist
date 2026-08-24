(function (global) {
    'use strict';

    function text(value) {
        return String(value == null ? '' : value).trim();
    }

    function popupContext(button) {
        if (typeof global.appUrl !== 'function') return null;
        const actor = typeof global.getCurrentEditorIdentity === 'function'
            ? global.getCurrentEditorIdentity()
            : (global.currentEditor || {});
        const sessionId = typeof global.getActiveEditSessionId === 'function'
            ? global.getActiveEditSessionId()
            : '';
        return {
            createUrl: global.appUrl('api/checklist/public-folder-link'),
            reissueUrl: global.appUrl('api/checklist/public-folder-link/reissue'),
            dialogId: text(global.dialogId),
            checklistKey: text(global.currentChecklistKey) || 'id',
            itemId: text(button && button.dataset.itemId),
            itemName: text(button && button.dataset.itemName) || 'Пункт',
            sessionId: text(sessionId),
            actingUserId: text(actor && (actor.userId || actor.id)),
            actingUserName: text(actor && (actor.userName || actor.name))
        };
    }

    function folderContext(button) {
        const core = global.ChecklistFolderCore;
        const bootstrap = core && core.bootstrap || global.CHECKLIST_FOLDER_BOOTSTRAP;
        if (!bootstrap || !bootstrap.publicFolderLinkApiUrl) return null;
        const actor = core && typeof core.getActor === 'function'
            ? core.getActor()
            : { id: bootstrap.userId, name: bootstrap.userName };
        const sessionId = core && typeof core.getSessionId === 'function'
            ? core.getSessionId()
            : bootstrap.sessionId;
        return {
            createUrl: text(bootstrap.publicFolderLinkApiUrl),
            reissueUrl: text(bootstrap.publicFolderReissueApiUrl),
            dialogId: text(bootstrap.dialogId),
            checklistKey: text(bootstrap.checklistKey) || 'id',
            itemId: text(bootstrap.itemId || button && button.dataset.itemId),
            itemName: text(bootstrap.itemName || button && button.dataset.itemName) || 'Пункт',
            sessionId: text(sessionId),
            actingUserId: text(actor && (actor.userId || actor.id)),
            actingUserName: text(actor && (actor.userName || actor.name))
        };
    }

    function resolveContext(button) {
        return folderContext(button) || popupContext(button);
    }

    function requireContext(button) {
        const context = resolveContext(button);
        if (!context || !context.createUrl || !context.reissueUrl) {
            throw new Error('Адрес управления общей ссылкой не настроен');
        }
        if (!context.dialogId || !context.itemId) {
            throw new Error('Не удалось определить папку пункта');
        }
        if (!context.sessionId) {
            throw new Error(
                'Общей ссылкой можно управлять только в активной сессии редактирования'
            );
        }
        return context;
    }

    async function requestLink(url, context) {
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            cache: 'no-store',
            body: JSON.stringify({
                dialogId: context.dialogId,
                checklistKey: context.checklistKey,
                itemId: context.itemId,
                sessionId: context.sessionId,
                actingUserId: context.actingUserId,
                actingUserName: context.actingUserName
            })
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || !payload.ok || !payload.url) {
            throw new Error(payload.error || 'Не удалось получить общую ссылку');
        }
        return payload;
    }

    async function copyText(value) {
        const normalized = text(value);
        if (!normalized) throw new Error('Ссылка ещё не получена');
        if (global.navigator.clipboard && global.isSecureContext) {
            await global.navigator.clipboard.writeText(normalized);
            return;
        }
        const helper = global.document.createElement('textarea');
        helper.value = normalized;
        helper.setAttribute('readonly', '');
        helper.style.position = 'fixed';
        helper.style.opacity = '0';
        global.document.body.appendChild(helper);
        helper.select();
        const copied = global.document.execCommand('copy');
        helper.remove();
        if (!copied) throw new Error('Не удалось скопировать ссылку');
    }

    function closeExistingDialog() {
        const existing = global.document.getElementById('publicFolderShareOverlay');
        if (existing) existing.remove();
    }

    function createDialog(context) {
        closeExistingDialog();
        const overlay = global.document.createElement('div');
        overlay.id = 'publicFolderShareOverlay';
        overlay.className = 'public-share-overlay';
        overlay.innerHTML = `
            <section class="public-share-dialog" role="dialog" aria-modal="true" aria-labelledby="publicShareTitle">
                <div class="public-share-dialog-header">
                    <div>
                        <div class="public-share-eyebrow">Общая папка</div>
                        <h3 id="publicShareTitle"></h3>
                    </div>
                    <button class="public-share-close" type="button" data-role="public-share-close" aria-label="Закрыть">×</button>
                </div>
                <p class="public-share-description">
                    Получатель сможет просматривать и скачивать файлы, видеть архив,
                    загружать новые файлы и заменять текущие. Удаление недоступно.
                </p>
                <label class="public-share-link-field">
                    <span>Постоянная ссылка</span>
                    <input type="text" readonly data-role="public-share-url" aria-label="Общая ссылка">
                </label>
                <div class="public-share-state" data-role="public-share-state" aria-live="polite">
                    Получаем ссылку…
                </div>
                <div class="public-share-actions">
                    <button class="public-share-secondary" type="button" data-role="public-share-reissue" disabled>
                        Перевыпустить ссылку
                    </button>
                    <button class="public-share-primary" type="button" data-role="public-share-copy" disabled>
                        Копировать ссылку
                    </button>
                </div>
            </section>
        `;
        const title = overlay.querySelector('#publicShareTitle');
        title.textContent = context.itemName || 'Папка пункта';
        global.document.body.appendChild(overlay);

        const dialog = overlay.querySelector('.public-share-dialog');
        const closeButton = overlay.querySelector('[data-role="public-share-close"]');
        closeButton.addEventListener('click', closeExistingDialog);
        overlay.addEventListener('click', event => {
            if (event.target === overlay) closeExistingDialog();
        });
        dialog.addEventListener('click', event => event.stopPropagation());
        global.document.addEventListener('keydown', function onKeydown(event) {
            if (event.key !== 'Escape' || !overlay.isConnected) return;
            global.document.removeEventListener('keydown', onKeydown);
            closeExistingDialog();
        });
        closeButton.focus();
        return overlay;
    }

    function setDialogState(overlay, message, error = false) {
        const state = overlay.querySelector('[data-role="public-share-state"]');
        state.textContent = text(message);
        state.classList.toggle('is-error', !!error);
    }

    function applyLinkPayload(overlay, payload) {
        const urlInput = overlay.querySelector('[data-role="public-share-url"]');
        const copyButton = overlay.querySelector('[data-role="public-share-copy"]');
        const reissueButton = overlay.querySelector('[data-role="public-share-reissue"]');
        urlInput.value = text(payload && payload.url);
        copyButton.disabled = !urlInput.value;
        reissueButton.disabled = !urlInput.value;
        setDialogState(
            overlay,
            payload && payload.reissued
                ? 'Новая ссылка готова. Предыдущая ссылка больше не работает.'
                : 'Ссылка готова.'
        );
    }

    async function open(button) {
        const context = requireContext(button);
        const overlay = createDialog(context);
        const copyButton = overlay.querySelector('[data-role="public-share-copy"]');
        const reissueButton = overlay.querySelector('[data-role="public-share-reissue"]');
        const urlInput = overlay.querySelector('[data-role="public-share-url"]');

        copyButton.addEventListener('click', async function () {
            this.disabled = true;
            try {
                await copyText(urlInput.value);
                setDialogState(overlay, 'Ссылка скопирована.');
            } catch (error) {
                setDialogState(overlay, error && error.message || 'Ошибка копирования', true);
            } finally {
                this.disabled = !urlInput.value;
            }
        });

        reissueButton.addEventListener('click', async function () {
            if (!global.confirm(
                'Перевыпустить общую ссылку? Предыдущая ссылка сразу перестанет работать.'
            )) return;

            this.disabled = true;
            copyButton.disabled = true;
            setDialogState(overlay, 'Перевыпускаем ссылку…');
            try {
                const freshContext = requireContext(button);
                const payload = await requestLink(freshContext.reissueUrl, freshContext);
                applyLinkPayload(overlay, payload);
            } catch (error) {
                setDialogState(
                    overlay,
                    error && error.message || 'Не удалось перевыпустить ссылку',
                    true
                );
                this.disabled = !urlInput.value;
                copyButton.disabled = !urlInput.value;
            }
        });

        try {
            const payload = await requestLink(context.createUrl, context);
            applyLinkPayload(overlay, payload);
        } catch (error) {
            setDialogState(
                overlay,
                error && error.message || 'Не удалось получить общую ссылку',
                true
            );
        }
    }

    function bind(root) {
        const scope = root && typeof root.querySelectorAll === 'function'
            ? root
            : global.document;
        scope.querySelectorAll('[data-role="share-folder"]').forEach(button => {
            if (button.dataset.publicShareBound === '1') return;
            button.dataset.publicShareBound = '1';
            button.addEventListener('click', async function () {
                if (this.disabled || this.dataset.loading === '1') return;
                this.dataset.loading = '1';
                this.classList.add('is-loading');
                try {
                    await open(this);
                } catch (error) {
                    global.alert(error && error.message || 'Не удалось открыть общую ссылку');
                } finally {
                    this.dataset.loading = '0';
                    this.classList.remove('is-loading');
                }
            });
        });
    }

    global.PublicFolderShare = Object.freeze({ bind, open });

    if (global.document.readyState === 'loading') {
        global.document.addEventListener('DOMContentLoaded', () => bind(global.document));
    } else {
        bind(global.document);
    }
})(window);
