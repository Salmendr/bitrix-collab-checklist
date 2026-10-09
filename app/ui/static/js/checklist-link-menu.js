(function (global) {
    'use strict';

    // A small menu next to a button that leads to Yandex Disk:
    // «Перейти по ссылке» opens the link in a new window, «Скопировать
    // ссылку» copies it. Used by the checklist popup and the folder page.

    const doc = global.document;
    const STYLE_ID = 'checklistLinkMenuStyle';
    let menuEl = null;
    let anchorEl = null;
    let closeTimer = null;

    function ensureStyle() {
        if (doc.getElementById(STYLE_ID)) return;
        const style = doc.createElement('style');
        style.id = STYLE_ID;
        style.textContent = [
            '.checklist-link-menu{position:fixed;z-index:3000;display:flex;flex-direction:column;gap:2px;',
            'min-width:190px;padding:5px;border:1px solid #d0d5dd;border-radius:10px;background:#fff;',
            'box-shadow:0 12px 28px rgba(16,24,40,.18);font-family:Arial,sans-serif;font-size:13px;}',
            '.checklist-link-menu[hidden]{display:none;}',
            '.checklist-link-menu button{display:block;width:100%;padding:7px 10px;border:0;border-radius:6px;',
            'background:transparent;color:#1f2328;font:inherit;text-align:left;cursor:pointer;white-space:nowrap;}',
            '.checklist-link-menu button:hover,.checklist-link-menu button:focus-visible{background:#f2f4f7;outline:none;}',
            '.checklist-link-menu-note{padding:6px 10px;color:#027a48;font-weight:700;}',
            '.checklist-link-menu-note.error{color:#b42318;}'
        ].join('');
        (doc.head || doc.documentElement).appendChild(style);
    }

    async function copyText(value) {
        try {
            if (global.navigator && global.navigator.clipboard && global.isSecureContext) {
                await global.navigator.clipboard.writeText(value);
                return true;
            }
        } catch (error) {
            // The Clipboard API can be blocked inside the Bitrix24 frame.
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

    function close() {
        global.clearTimeout(closeTimer);
        if (menuEl) {
            menuEl.hidden = true;
            menuEl.innerHTML = '';
        }
        if (anchorEl) anchorEl.setAttribute('aria-expanded', 'false');
        anchorEl = null;
    }

    function place(anchor) {
        const rect = anchor.getBoundingClientRect();
        const width = menuEl.offsetWidth;
        const height = menuEl.offsetHeight;
        const viewportWidth = doc.documentElement.clientWidth || global.innerWidth;
        const viewportHeight = doc.documentElement.clientHeight || global.innerHeight;
        let left = rect.left;
        if (left + width > viewportWidth - 8) left = Math.max(8, viewportWidth - width - 8);
        let top = rect.bottom + 4;
        if (top + height > viewportHeight - 8 && rect.top - height - 4 > 8) top = rect.top - height - 4;
        menuEl.style.left = Math.round(left) + 'px';
        menuEl.style.top = Math.round(top) + 'px';
    }

    function open(anchor, url) {
        const link = String(url || '').trim();
        if (!anchor || !link) return;
        if (anchorEl === anchor && menuEl && !menuEl.hidden) {
            close();
            return;
        }
        ensureStyle();
        if (!menuEl) {
            menuEl = doc.createElement('div');
            menuEl.className = 'checklist-link-menu';
            menuEl.setAttribute('role', 'menu');
            menuEl.hidden = true;
            doc.body.appendChild(menuEl);
        }
        close();
        anchorEl = anchor;
        anchor.setAttribute('aria-expanded', 'true');
        menuEl.innerHTML = '';
        const go = doc.createElement('button');
        go.type = 'button';
        go.setAttribute('role', 'menuitem');
        go.textContent = 'Перейти по ссылке';
        go.addEventListener('click', function () {
            close();
            global.open(link, '_blank', 'noopener,noreferrer');
        });
        const copy = doc.createElement('button');
        copy.type = 'button';
        copy.setAttribute('role', 'menuitem');
        copy.textContent = 'Скопировать ссылку';
        copy.addEventListener('click', async function () {
            const copied = await copyText(link);
            menuEl.innerHTML = '';
            const note = doc.createElement('div');
            note.className = 'checklist-link-menu-note' + (copied ? '' : ' error');
            note.textContent = copied ? 'Ссылка скопирована' : 'Не удалось скопировать';
            menuEl.appendChild(note);
            closeTimer = global.setTimeout(close, 1100);
        });
        menuEl.appendChild(go);
        menuEl.appendChild(copy);
        menuEl.hidden = false;
        place(anchor);
        go.focus({ preventScroll: true });
    }

    doc.addEventListener('mousedown', function (event) {
        if (!menuEl || menuEl.hidden) return;
        if (menuEl.contains(event.target) || (anchorEl && anchorEl.contains(event.target))) return;
        close();
    }, true);
    doc.addEventListener('keydown', function (event) {
        if (event.key === 'Escape' && menuEl && !menuEl.hidden) close();
    });
    global.addEventListener('resize', close);
    global.addEventListener('scroll', function (event) {
        if (menuEl && !menuEl.hidden && !menuEl.contains(event.target)) close();
    }, true);

    global.ChecklistLinkMenu = Object.freeze({ open, close, copyText });
})(window);
