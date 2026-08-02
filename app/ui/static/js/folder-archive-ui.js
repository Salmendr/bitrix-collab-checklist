(function (global) {
    'use strict';

    const bootstrap = (
        global.ChecklistFolderArchiveBootstrap
        || {}
    );

    const folderCore = (
        global.ChecklistFolderCore
        || null
    );

    const adminUserIds = new Set(
        Array.isArray(bootstrap.adminUserIds)
            ? bootstrap.adminUserIds.map(value => (
                String(value || '').trim()
            ))
            : []
    );

    function getActor() {
        try {
            const openerEditor = (
                global.opener
                && global.opener.currentEditor
                ? global.opener.currentEditor
                : null
            );

            return {
                id: String(
                    openerEditor
                    && openerEditor.id
                    || ''
                ).trim(),
                name: String(
                    openerEditor
                    && openerEditor.name
                    || ''
                ).trim() || 'Пользователь'
            };
        } catch (error) {
            console.log(
                'folder archive actor error:',
                error
            );

            return {
                id: '',
                name: 'Пользователь'
            };
        }
    }

    function requireArchiveEditSession() {
        if (
            folderCore
            && typeof folderCore.requireSession
                === 'function'
        ) {
            return folderCore.requireSession(
                'удаление архивной версии'
            );
        }

        const sessionId = String(
            bootstrap.sessionId || ''
        ).trim();

        if (!sessionId) {
            throw new Error(
                'Нельзя удалить архивную версию: '
                + 'нет активной сессии редактирования'
            );
        }

        return sessionId;
    }

    function notifyParent(extraPayload) {
        try {
            if (
                global.opener
                && typeof global.opener.postMessage
                    === 'function'
            ) {
                global.opener.postMessage({
                    type: 'checklist-document-changed',
                    dialogId: String(
                        bootstrap.dialogId || ''
                    ),
                    checklistKey: String(
                        bootstrap.checklistKey || ''
                    ),
                    itemId: String(
                        bootstrap.itemId || ''
                    ),
                    ...(extraPayload || {})
                }, '*');
            }
        } catch (error) {
            console.log(
                'folder archive opener sync error:',
                error
            );
        }
    }

    function refreshDeletePermissions() {
        const actor = getActor();
        const allowed = adminUserIds.has(
            String(actor.id || '').trim()
        );

        document.querySelectorAll(
            '[data-role="folder-delete-archive-version"]'
        ).forEach(button => {
            button.classList.toggle(
                'is-visible',
                allowed
            );
        });

        return allowed;
    }

    function setPanelExpanded(button, panel, expanded) {
        button.setAttribute(
            'aria-expanded',
            expanded ? 'true' : 'false'
        );

        panel.classList.toggle(
            'is-expanded',
            expanded
        );

        const icon = button.querySelector(
            '[data-role="folder-archive-toggle-icon"]'
        );

        if (icon) {
            icon.textContent = expanded ? '▾' : '▸';
        }
    }

    function bindArchiveToggles() {
        document.querySelectorAll(
            '[data-role="folder-archive-toggle"]'
        ).forEach(button => {
            if (button.dataset.archiveBound === '1') {
                return;
            }

            button.dataset.archiveBound = '1';

            button.addEventListener('click', function () {
                const panelId = String(
                    this.dataset.panelId || ''
                ).trim();

                if (!panelId) {
                    return;
                }

                const panel = document.getElementById(
                    panelId
                );

                if (!panel) {
                    return;
                }

                const expanded = (
                    this.getAttribute('aria-expanded')
                    === 'true'
                );

                setPanelExpanded(
                    this,
                    panel,
                    !expanded
                );
            });
        });
    }

    async function deleteArchiveVersion(button) {
        const actor = getActor();

        if (
            !adminUserIds.has(
                String(actor.id || '').trim()
            )
        ) {
            alert(
                'Постоянное удаление архивных ' +
                'версий доступно только администраторам'
            );
            return;
        }

        const archiveVersionId = String(
            button.dataset.archiveVersionId || ''
        ).trim();

        const seriesId = String(
            button.dataset.seriesId || ''
        ).trim();

        const archiveVersionName = String(
            button.dataset.archiveVersionName
            || 'архивную версию'
        ).trim();

        if (!archiveVersionId || !seriesId) {
            alert(
                'Не удалось определить архивную версию'
            );
            return;
        }

        let sessionId = '';

        try {
            sessionId = requireArchiveEditSession();
        } catch (error) {
            alert(
                error && error.message
                    ? error.message
                    : (
                        'Удаление недоступно без '
                        + 'активной сессии'
                    )
            );
            return;
        }

        const confirmed = global.confirm(
            'Удалить архивную версию "' +
            archiveVersionName +
            '"?\n\n' +
            'До сохранения чек-листа удаление '
            + 'можно отменить общей кнопкой '
            + '«Отменить».'
        );

        if (!confirmed) {
            return;
        }

        button.disabled = true;

        try {
            const response = await fetch(
                String(bootstrap.deleteApiUrl || ''),
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        dialogId: String(
                            bootstrap.dialogId || ''
                        ),
                        checklistKey: String(
                            bootstrap.checklistKey || ''
                        ),
                        itemId: String(
                            bootstrap.itemId || ''
                        ),
                        archiveVersionId,
                        seriesId,
                        sessionId,
                        requireEditSession: true,
                        actingUserId: actor.id,
                        actingUserName: actor.name
                    })
                }
            );

            const result = await response
                .json()
                .catch(() => ({}));

            if (!response.ok || !result.ok) {
                throw new Error(
                    result.error
                    || result.details
                    || 'archive version delete failed'
                );
            }

            notifyParent({
                changeKind: 'archive-version-delete',
                archiveVersionId,
                seriesId,
                archiveVersionName
            });

            global.location.reload();
        } catch (error) {
            console.log(
                'archive version delete error:',
                error
            );

            alert(
                error && error.message
                    ? error.message
                    : 'Ошибка удаления архивной версии'
            );
        } finally {
            button.disabled = false;
        }
    }

    function bindArchiveDeleteButtons() {
        document.querySelectorAll(
            '[data-role="folder-delete-archive-version"]'
        ).forEach(button => {
            if (button.dataset.archiveBound === '1') {
                return;
            }

            button.dataset.archiveBound = '1';

            button.addEventListener(
                'click',
                function () {
                    void deleteArchiveVersion(this);
                }
            );
        });
    }

    function refreshSessionState() {
        if (
            folderCore
            && typeof folderCore.applySessionState
                === 'function'
        ) {
            folderCore.applySessionState();
        }

        refreshDeletePermissions();
    }

    function init() {
        bindArchiveToggles();
        bindArchiveDeleteButtons();
        refreshSessionState();

        global.addEventListener(
            'focus',
            refreshSessionState
        );

        global.document.addEventListener(
            'visibilitychange',
            function () {
                if (!global.document.hidden) {
                    refreshSessionState();
                }
            }
        );
    }

    global.ChecklistFolderArchive = Object.freeze({
        init,
        refreshDeletePermissions,
        bindArchiveToggles,
        bindArchiveDeleteButtons
    });

    if (document.readyState === 'loading') {
        document.addEventListener(
            'DOMContentLoaded',
            init,
            { once: true }
        );
    } else {
        init();
    }
})(window);
