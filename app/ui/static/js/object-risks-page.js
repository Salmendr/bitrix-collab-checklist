(function (global) {
    'use strict';

    // The risk tables of all objects in the order of the slice; the window
    // opens at the table of its project. Every change is saved at once and
    // the server answers with the new «Итоговая оценка».

    const doc = global.document;
    const UI = global.ObjectAnalysisUI;
    const bootstrap = global.OBJECT_ANALYSIS_BOOTSTRAP || {};
    const listEl = doc.getElementById('oaRisks');
    if (!UI || !listEl) return;

    const esc = UI.esc;
    const REFRESH_PAUSE_MS = 5000;
    const PROBABILITIES = [
        { key: 'opr', label: 'ОПР' },
        { key: 'pd', label: 'ПД' },
        { key: 'rd', label: 'РД+Стройка' }
    ];
    const state = { data: null, byId: {}, loadedAt: 0, loading: null, scrolled: false, saving: 0 };

    function actor() {
        const checked = global.ChecklistAuth && global.ChecklistAuth.user;
        if (checked && checked.id) return { userId: String(checked.id), userName: String(checked.name || '') };
        return { userId: String(bootstrap.userId || ''), userName: String(bootstrap.userName || '') };
    }

    const api = UI.createApi({ baseUrl: global.location.origin + String(bootstrap.appBase || ''), actor });

    function level(number) {
        return (state.data.riskLevels || []).find(entry => Number(entry.level) === Number(number)) || null;
    }

    function totalHtml(total) {
        const tip = total.level
            ? 'Среднее трёх худших оценок: ' + (total.ratings || []).join(', ')
                + (total.probabilityLevel ? '. Из них по вероятности реализации: ' + total.probabilityLevel : '')
            : 'Нет ни одной оценки';
        return '<span class="oa-risk-total-label">Итоговая оценка риска</span>'
            + '<span class="oa-risk-pill" style="background:' + esc(total.color) + ';color:' + esc(total.text) + '" title="' + esc(tip) + '">'
            + esc(total.label) + '</span>';
    }

    function levelSelect(row, canEdit) {
        const current = level(row.level);
        const style = current ? ' style="background:' + esc(current.color) + ';color:' + esc(current.text) + '"' : '';
        if (!canEdit) {
            return '<span class="oa-risk-level-value"' + style + '>' + esc(current ? current.label : '—') + '</span>';
        }
        return '<select class="oa-risk-select" data-oa-field="level"' + style + '>'
            + '<option value="0"' + (row.level ? '' : ' selected') + '>— не оценено —</option>'
            + (state.data.riskLevels || []).map(entry => (
                '<option value="' + entry.level + '"' + (Number(row.level) === Number(entry.level) ? ' selected' : '') + '>'
                + esc(entry.label) + '</option>'
            )).join('')
            + '</select>';
    }

    function textField(row, field, canEdit, placeholder, limit) {
        if (!canEdit) {
            return '<div class="oa-risk-static">' + (row[field] ? esc(row[field]) : '<span class="oa-placeholder">—</span>') + '</div>';
        }
        return '<textarea class="oa-risk-text" rows="1" maxlength="' + limit + '" data-oa-field="' + field + '"'
            + ' placeholder="' + esc(placeholder) + '">' + esc(row[field]) + '</textarea>';
    }

    function cardHtml(table) {
        const canEdit = !!table.canEdit;
        const limits = state.data.limits || {};
        const rows = table.risks.rows.map(row => (
            '<div class="oa-risk-row" data-oa-row="' + esc(row.id) + '">'
            + '<div class="oa-risk-cell">' + textField(row, 'name', canEdit, 'Характеристика', limits.name || 300) + '</div>'
            + '<div class="oa-risk-cell">' + textField(row, 'description', canEdit, 'Описание', limits.description || 2000) + '</div>'
            + '<div class="oa-risk-cell oa-risk-cell-level">' + levelSelect(row, canEdit) + '</div>'
            + '<div class="oa-risk-cell oa-risk-cell-delete">'
            + (canEdit ? '<button type="button" class="oa-icon-btn" data-oa-delete title="Удалить строку">×</button>' : '')
            + '</div></div>'
        )).join('');
        const probabilities = table.risks.probabilities || {};
        const probabilityHtml = PROBABILITIES.map(entry => {
            const value = probabilities[entry.key];
            return '<label class="oa-risk-prob">' + esc(entry.label)
                + (canEdit
                    ? '<input type="number" min="0" max="100" step="1" inputmode="numeric" data-oa-probability="' + entry.key + '"'
                        + ' value="' + (value == null ? '' : esc(value)) + '">'
                    : '<span class="oa-risk-prob-value">' + (value == null ? '—' : esc(value)) + '</span>')
                + '<span class="oa-risk-percent">%</span></label>';
        }).join('');
        const focus = table.dialogId === (bootstrap.focusDialogId || bootstrap.dialogId);
        return '<section class="oa-risk-card' + (focus ? ' is-current' : '') + '" data-oa-risk="' + esc(table.dialogId) + '">'
            + '<header class="oa-risk-head"><h2 class="oa-risk-title">' + esc(table.name) + '</h2>'
            + '<span class="oa-risk-group">' + esc(table.group) + '</span>'
            + (canEdit ? '' : '<span class="oa-risk-readonly">только просмотр</span>')
            + '<span class="oa-risk-total" data-oa-total>' + totalHtml(table.total) + '</span></header>'
            + '<div class="oa-risk-table">'
            + '<div class="oa-risk-row oa-risk-row-head"><div>Характеристика</div><div>Описание характеристики</div><div>Оценка риска</div><div></div></div>'
            + rows
            + '</div>'
            + '<div class="oa-risk-foot">'
            + (canEdit ? '<button type="button" class="oa-btn" data-oa-add-row>+ Строка</button>' : '')
            + '<div class="oa-risk-probs"><span class="oa-risk-probs-title">Вероятность реализации</span>' + probabilityHtml + '</div>'
            + '</div></section>';
    }

    function autosize(area) {
        area.style.height = 'auto';
        area.style.height = Math.max(30, area.scrollHeight + 2) + 'px';
    }

    function render() {
        const data = state.data;
        if (!data) return;
        state.byId = {};
        data.tables.forEach(table => { state.byId[table.dialogId] = table; });
        listEl.innerHTML = data.tables.map(cardHtml).join('') || '<div class="oa-loading">Нет объектов</div>';
        listEl.setAttribute('aria-busy', 'false');
        listEl.querySelectorAll('textarea').forEach(autosize);
        if (!state.scrolled) {
            state.scrolled = true;
            const focus = listEl.querySelector('.oa-risk-card.is-current');
            if (focus) focus.scrollIntoView({ block: 'start' });
        }
    }

    function renderCard(dialogId) {
        const card = listEl.querySelector('[data-oa-risk="' + CSS.escape(dialogId) + '"]');
        const table = state.byId[dialogId];
        if (!card || !table) return;
        const holder = doc.createElement('div');
        holder.innerHTML = cardHtml(table);
        const fresh = holder.firstElementChild;
        card.replaceWith(fresh);
        fresh.querySelectorAll('textarea').forEach(autosize);
    }

    function applyResult(dialogId, result, rerender) {
        const table = state.byId[dialogId];
        if (!table) return;
        table.risks = result.risks;
        table.total = result.total;
        if (rerender) {
            renderCard(dialogId);
            return;
        }
        const card = listEl.querySelector('[data-oa-risk="' + CSS.escape(dialogId) + '"]');
        const totalEl = card && card.querySelector('[data-oa-total]');
        if (totalEl) totalEl.innerHTML = totalHtml(result.total);
    }

    async function change(dialogId, op, payload, rerender, control) {
        state.saving += 1;
        if (control) control.classList.add('is-saving');
        try {
            const result = await api.changeRisk(dialogId, op, payload);
            applyResult(dialogId, result, rerender);
            if (control) control.classList.remove('is-saving', 'is-error');
            return true;
        } catch (error) {
            if (control) control.classList.add('is-error');
            global.alert(error.message || 'Не удалось сохранить');
            return false;
        } finally {
            state.saving -= 1;
            if (control) control.classList.remove('is-saving');
        }
    }

    listEl.addEventListener('input', event => {
        if (event.target.matches('textarea')) autosize(event.target);
    });

    listEl.addEventListener('change', async event => {
        const target = event.target;
        const card = target.closest('[data-oa-risk]');
        if (!card) return;
        const dialogId = card.dataset.oaRisk;
        const table = state.byId[dialogId];
        if (!table) return;
        if (target.dataset.oaProbability) {
            const raw = String(target.value || '').trim();
            if (raw !== '' && (!/^\d+$/.test(raw) || Number(raw) > 100)) {
                global.alert('Вероятность — целое число от 0 до 100');
                const old = table.risks.probabilities[target.dataset.oaProbability];
                target.value = old == null ? '' : String(old);
                return;
            }
            await change(dialogId, 'probability', { key: target.dataset.oaProbability, value: raw }, false, target);
            return;
        }
        const rowEl = target.closest('[data-oa-row]');
        const field = target.dataset.oaField;
        if (!rowEl || !field) return;
        const row = table.risks.rows.find(entry => entry.id === rowEl.dataset.oaRow);
        let value = target.value;
        if (field === 'name' && !String(value).trim()) {
            global.alert('Введите характеристику');
            target.value = row ? row.name : '';
            autosize(target);
            return;
        }
        if (field === 'level') {
            value = Number(value);
            const current = level(value);
            target.style.background = current ? current.color : '';
            target.style.color = current ? current.text : '';
        }
        await change(dialogId, 'row-update', { rowId: rowEl.dataset.oaRow, field, value }, false, target);
    });

    listEl.addEventListener('click', async event => {
        const card = event.target.closest('[data-oa-risk]');
        if (!card) return;
        const dialogId = card.dataset.oaRisk;
        if (event.target.closest('[data-oa-add-row]')) {
            await change(dialogId, 'row-add', { name: '' }, true, null);
            const fresh = listEl.querySelector('[data-oa-risk="' + CSS.escape(dialogId) + '"]');
            const areas = fresh ? fresh.querySelectorAll('[data-oa-field="name"]') : [];
            const last = areas[areas.length - 1];
            if (last) {
                last.focus();
                last.select();
            }
            return;
        }
        const remove = event.target.closest('[data-oa-delete]');
        if (remove) {
            const rowEl = remove.closest('[data-oa-row]');
            const table = state.byId[dialogId];
            const row = table && table.risks.rows.find(entry => entry.id === rowEl.dataset.oaRow);
            if (!global.confirm('Удалить строку «' + (row ? row.name : '') + '»?')) return;
            await change(dialogId, 'row-delete', { rowId: rowEl.dataset.oaRow }, true, null);
        }
    });

    async function load(force) {
        if (!force && Date.now() - state.loadedAt < REFRESH_PAUSE_MS) return;
        // Never under the hands of the user.
        if (state.loading || state.saving || (doc.activeElement && listEl.contains(doc.activeElement))) return;
        state.loading = (async function () {
            try {
                state.data = await api.risks(bootstrap.dialogId);
                state.loadedAt = Date.now();
                render();
            } catch (error) {
                listEl.innerHTML = '<div class="oa-loading is-error">' + esc(error.message || 'Не удалось загрузить таблицы') + '</div>';
            } finally {
                state.loading = null;
            }
        })();
        return state.loading;
    }

    global.addEventListener('focus', () => load(false));
    load(true);
})(window);
