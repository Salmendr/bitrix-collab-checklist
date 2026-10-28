(function (global) {
    'use strict';

    // «Анализ состояния объекта»: the cells, the header and the editors,
    // shared by the panel of a checklist and the slice window. A change is
    // saved at once; the server answers with the whole project row.

    const doc = global.document;

    const COLUMNS = [
        { key: 'contractStatus', group: 'Договор', title: 'Статус договора', width: 'minmax(118px, 1.1fr)' },
        { key: 'contract', group: 'Договор', title: 'Договор', width: '58px' },
        { key: 'stages', group: 'Договор', title: 'Стадии по договору', width: 'minmax(92px, .9fr)' },
        { key: 'risk', group: 'Оценка рисков', title: 'Экономика', width: 'minmax(100px, .9fr)' },
        { key: 'general', group: 'Оценка рисков', title: 'Общее', width: 'minmax(120px, 1.3fr)' },
        { key: 'surveys', group: 'ИД', title: 'Изыскания', width: '68px' },
        { key: 'tu', group: 'ИД', title: 'ТУ', width: '50px' },
        { key: 'ga', group: 'Граданализ', title: 'Чек-лист ГА', width: '90px' },
        { key: 'oprSections', group: 'ОПР', title: 'Готовность ОПР', width: 'minmax(150px, 1.7fr)' },
        { key: 'oprCustomerDate', group: 'ОПР', title: 'Передача ОПР Заказчику', width: '86px' },
        { key: 'oprPdDate', group: 'ОПР', title: 'Передача ОПР в стадию ПД', width: '86px' },
        { key: 'meeting', group: 'ОПР', title: 'Совещание по объекту (внутр.)', width: '104px' },
        { key: 'vks', group: 'Для передачи', title: 'ВКС с Заказчиком', width: '104px' },
        { key: 'customerResponsible', group: 'Для передачи', title: 'Ответственный от Заказчика', width: 'minmax(110px, 1.2fr)' }
    ];
    const NAME_COLUMN = { key: 'name', group: '', title: 'Объект', width: 'minmax(150px, 1.3fr)' };
    const WEEKDAYS = ['вс', 'пн', 'вт', 'ср', 'чт', 'пт', 'сб'];
    const WEEKDAYS_LONG = ['воскресенье', 'понедельник', 'вторник', 'среду', 'четверг', 'пятницу', 'субботу'];

    function text(value) {
        return String(value == null ? '' : value).trim();
    }

    function esc(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function columns(withName) {
        return withName ? [NAME_COLUMN].concat(COLUMNS) : COLUMNS.slice();
    }

    function gridTemplate(withName) {
        return columns(withName).map(column => column.width).join(' ');
    }

    // ---- API ----

    function createApi(options) {
        const base = String(options.baseUrl || '');
        const actor = typeof options.actor === 'function' ? options.actor : () => ({ userId: '', userName: '' });

        async function request(method, path, body) {
            const init = { method, headers: {} };
            let url = base + path;
            if (method === 'GET') {
                const who = actor();
                const query = new URLSearchParams(Object.assign({}, body || {}, { userId: who.userId || '' }));
                url += (url.includes('?') ? '&' : '?') + query.toString();
            } else {
                const who = actor();
                init.headers['Content-Type'] = 'application/json';
                init.body = JSON.stringify(Object.assign({}, body || {}, {
                    actingUserId: who.userId || '',
                    actingUserName: who.userName || ''
                }));
            }
            const response = await fetch(url, init);
            const result = await response.json().catch(() => ({}));
            if (!response.ok || !result.ok) {
                throw new Error(result && result.error || 'Не удалось выполнить запрос');
            }
            return result;
        }

        return {
            panel: dialogId => request('GET', '/api/object-analysis', { dialogId }),
            slice: dialogId => request('GET', '/api/object-analysis/slice', { dialogId }),
            risks: dialogId => request('GET', '/api/object-analysis/risks', { dialogId }),
            saveField: (dialogId, field, value) => request('POST', '/api/object-analysis/field', { dialogId, field, value }),
            changeOption: (dialogId, list, action, optionId, label) => request(
                'POST', '/api/object-analysis/options', { dialogId, list, action, optionId: optionId || '', label: label || '' }
            ),
            changeRisk: (dialogId, op, payload) => request(
                'POST', '/api/object-analysis/risks', Object.assign({ dialogId, op }, payload || {})
            )
        };
    }

    // ---- values ----

    function formatDate(value) {
        const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(text(value));
        return match ? match[3] + '.' + match[2] + '.' + match[1] : '';
    }

    function parseLocal(value) {
        const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/.exec(text(value));
        if (!match) return null;
        return new Date(+match[1], +match[2] - 1, +match[3], +match[4], +match[5]);
    }

    function meetingText(meeting) {
        const at = text(meeting && meeting.at);
        if (!at) return '';
        const time = at.slice(11, 16);
        if (meeting.weekly) {
            const day = parseLocal(at);
            return 'Каждую ' + (day ? WEEKDAYS_LONG[day.getDay()] : '') + ', ' + time;
        }
        const day = parseLocal(at);
        return formatDate(at) + ' ' + time + (day ? ' (' + WEEKDAYS[day.getDay()] + ')' : '');
    }

    function optionMap(list) {
        const map = {};
        (Array.isArray(list) ? list : []).forEach(option => { map[option.id] = option; });
        return map;
    }

    function riskLevel(ctx, level) {
        return (ctx.riskLevels || []).find(entry => Number(entry.level) === Number(level)) || null;
    }

    // ---- cells ----

    function chips(ids, map, extraClass) {
        return ids.map(id => {
            const option = map[id];
            if (!option) return '';
            const removed = option.removed;
            return '<span class="oa-chip ' + extraClass + (removed ? ' is-removed' : '') + '"'
                + (removed ? ' title="Вариант убран из списка"' : '') + '>'
                + esc(option.label) + '</span>';
        }).join('');
    }

    function editable(project, key, ctx) {
        return !!(project && project.canEdit && ctx.editable !== false
            && ['contractStatus', 'stages', 'general', 'oprSections', 'oprCustomerDate',
                'oprPdDate', 'meeting', 'vks', 'customerResponsible'].includes(key));
    }

    function shareCell(share, kind, project) {
        const missing = share && share.missing;
        const percent = share && share.percent;
        let title;
        if (kind === 'surveys') {
            title = missing
                ? 'В чек-листе ИД нет пункта «Изыскания»'
                : (share.total ? 'Готово подпунктов «Изыскания»: ' + share.done + ' из ' + share.total : 'В пункте «Изыскания» нет подпунктов');
        } else if (kind === 'ga') {
            title = share && share.total ? 'Выполнено пунктов Граданализа: ' + share.done + ' из ' + share.total : 'В Граданализе нет пунктов в работе';
        } else {
            title = share && share.total ? 'Готово пунктов раздела ТУ: ' + share.done + ' из ' + share.total : 'В разделе ТУ нет пунктов';
        }
        const value = percent == null ? '—' : percent + '%';
        // Граданализ lives in the OPR checklist.
        const target = kind === 'ga' ? 'opr' : 'id';
        const link = text(kind === 'ga' ? project.oprChecklistUrl : project.idChecklistUrl);
        const hint = kind === 'ga' ? '. Нажмите, чтобы открыть чек-лист ОПР' : '. Нажмите, чтобы открыть чек-лист ИД';
        return '<span class="oa-share' + (percent == null ? ' is-empty' : '') + (link ? ' is-link' : '') + '"'
            + (link ? ' data-oa-open="' + target + '"' : '') + ' title="' + esc(title + (link ? hint : '')) + '">'
            + esc(value) + '</span>';
    }

    function cellInner(key, project, ctx) {
        const options = ctx.options || {};
        switch (key) {
            case 'name': {
                const link = text(project.idChecklistUrl);
                return '<span class="oa-name' + (link ? ' is-link' : '') + '"'
                    + (link ? ' data-oa-open="id" title="Открыть чек-лист ИД проекта"' : '') + '>'
                    + esc(project.name) + '</span>';
            }
            case 'contractStatus':
                return project.contractStatus
                    ? '<span class="oa-text">' + esc(project.contractStatus) + '</span>'
                    : '<span class="oa-placeholder">Не указан</span>';
            case 'contract': {
                const has = project.contract && project.contract.hasContract;
                const hint = text(project.contract && project.contract.hint);
                return '<span class="oa-dot ' + (has ? 'is-on' : 'is-off') + '" title="' + esc(hint || (has ? 'Договор есть' : 'Договора нет')) + '"></span>';
            }
            case 'stages': {
                const html = chips(project.stages || [], optionMap(options.stages), 'oa-chip-stage');
                return html || '<span class="oa-placeholder">—</span>';
            }
            case 'risk': {
                const risk = project.risk || {};
                const tip = risk.level
                    ? 'Итоговая оценка по худшим оценкам: ' + (risk.ratings || []).join(', ')
                    : 'Риски ещё не оценены';
                return '<button type="button" class="oa-risk" data-oa-open="risks"'
                    + ' style="background:' + esc(risk.color || '#eef1f5') + ';color:' + esc(risk.text || '#667085') + '"'
                    + ' title="' + esc(tip + '. Нажмите, чтобы открыть расчёт рисков') + '">'
                    + esc(risk.label || 'Не оценено') + '</button>';
            }
            case 'general': {
                const general = project.general || {};
                const level = riskLevel(ctx, general.color);
                const style = level ? ' style="background:' + esc(level.color) + ';color:' + esc(level.text) + '"' : '';
                return general.text
                    ? '<span class="oa-general' + (level ? ' is-filled' : '') + '"' + style + ' title="' + esc(general.text) + '">' + esc(general.text) + '</span>'
                    : '<span class="oa-general oa-placeholder' + (level ? ' is-filled' : '') + '"' + style + '>—</span>';
            }
            case 'surveys':
                return shareCell(project.id && project.id.surveys, 'surveys', project);
            case 'tu':
                return shareCell(project.id && project.id.tu, 'tu', project);
            case 'ga':
                return shareCell(project.ga, 'ga', project);
            case 'oprSections': {
                const map = optionMap(options.oprSections);
                const sections = project.oprSections || { ready: [], notReady: [] };
                const canEdit = editable(project, key, ctx);
                const half = (name, ids, cls, empty) => {
                    const html = chips(ids || [], map, cls);
                    return '<div class="oa-half oa-half-' + name + (canEdit ? ' is-editable' : '') + '"'
                        + (canEdit ? ' data-oa-edit="oprSections" data-oa-half="' + name + '" tabindex="0"' : '') + '>'
                        + (html || '<span class="oa-placeholder">' + empty + '</span>')
                        + '</div>';
                };
                return half('ready', sections.ready, 'oa-chip-ready', canEdit ? '+ готовые' : 'готовых нет')
                    + half('notReady', sections.notReady, 'oa-chip-not-ready', canEdit ? '+ не готовые' : 'не готовых нет');
            }
            case 'oprCustomerDate':
            case 'oprPdDate':
                return project[key]
                    ? '<span class="oa-text">' + esc(formatDate(project[key])) + '</span>'
                    : '<span class="oa-placeholder">—</span>';
            case 'meeting':
            case 'vks': {
                const value = meetingText(project[key]);
                return value
                    ? '<span class="oa-text' + (project[key].weekly ? ' is-weekly' : '') + '"'
                        + (project[key].weekly ? ' title="Еженедельно, с ' + esc(formatDate(project[key].at)) + '"' : '') + '>'
                        + esc(value) + '</span>'
                    : '<span class="oa-placeholder">—</span>';
            }
            case 'customerResponsible':
                return project.customerResponsible
                    ? '<span class="oa-text oa-clamp" title="' + esc(project.customerResponsible) + '">' + esc(project.customerResponsible) + '</span>'
                    : '<span class="oa-placeholder">—</span>';
            default:
                return '';
        }
    }

    function cellHtml(column, project, ctx) {
        const key = column.key;
        const canEdit = editable(project, key, ctx) && key !== 'oprSections';
        return '<div class="oa-cell oa-cell-' + key + (canEdit ? ' is-editable' : '') + '"'
            + ' data-oa-key="' + key + '"'
            + (canEdit ? ' data-oa-edit="' + key + '" tabindex="0" title="Нажмите, чтобы изменить"' : '')
            + '>' + cellInner(key, project, ctx) + '</div>';
    }

    function headerHtml(withName) {
        const list = columns(withName);
        const groups = [];
        list.forEach(column => {
            const last = groups[groups.length - 1];
            if (last && last.title === column.group) last.span += 1;
            else groups.push({ title: column.group, span: 1 });
        });
        const top = groups.map(group => (
            '<div class="oa-head oa-head-group' + (group.title ? '' : ' is-blank') + '" style="grid-column:span ' + group.span + '">'
            + esc(group.title) + '</div>'
        )).join('');
        const bottom = list.map(column => (
            '<div class="oa-head oa-head-column' + (column.key === 'name' ? ' is-name' : '') + '" title="' + esc(column.title) + '">' + esc(column.title) + '</div>'
        )).join('');
        return top + bottom;
    }

    function rowHtml(project, ctx, withName) {
        return '<div class="oa-row' + (project.current && withName ? ' is-current' : '') + '" data-oa-project="' + esc(project.dialogId) + '">'
            + columns(withName).map(column => cellHtml(column, project, ctx)).join('')
            + '</div>';
    }

    // ---- editors ----

    let popover = null;
    let popoverClose = null;

    function closeEditor() {
        if (popover) {
            popover.remove();
            popover = null;
        }
        if (popoverClose) {
            doc.removeEventListener('mousedown', popoverClose, true);
            doc.removeEventListener('keydown', popoverKey, true);
            popoverClose = null;
        }
    }

    function popoverKey(event) {
        if (event.key === 'Escape') {
            event.stopPropagation();
            closeEditor();
        }
    }

    function place(anchor, element) {
        const rect = anchor.getBoundingClientRect();
        const viewW = global.innerWidth || doc.documentElement.clientWidth;
        const viewH = global.innerHeight || doc.documentElement.clientHeight;
        const width = element.offsetWidth;
        const height = element.offsetHeight;
        let left = Math.min(rect.left, viewW - width - 8);
        left = Math.max(8, left);
        let top = rect.bottom + 4;
        if (top + height > viewH - 8 && rect.top - height - 4 > 8) top = rect.top - height - 4;
        top = Math.max(8, Math.min(top, viewH - height - 8));
        element.style.left = left + 'px';
        element.style.top = top + 'px';
    }

    function openPopover(anchor, html, setup) {
        closeEditor();
        popover = doc.createElement('div');
        popover.className = 'oa-popover';
        popover.innerHTML = html;
        doc.body.appendChild(popover);
        place(anchor, popover);
        popoverClose = function (event) {
            if (popover && !popover.contains(event.target) && !anchor.contains(event.target)) closeEditor();
        };
        doc.addEventListener('mousedown', popoverClose, true);
        doc.addEventListener('keydown', popoverKey, true);
        if (setup) setup(popover);
        const focus = popover.querySelector('[data-oa-focus]');
        if (focus) focus.focus();
        return popover;
    }

    function setBusy(element, busy) {
        element.querySelectorAll('button, input, textarea, select').forEach(control => { control.disabled = busy; });
        element.classList.toggle('is-busy', busy);
    }

    function showError(element, error) {
        const box = element.querySelector('.oa-pop-error');
        const message = error && error.message ? error.message : String(error || 'Ошибка');
        if (box) {
            box.textContent = message;
            box.hidden = false;
        } else {
            global.alert(message);
        }
    }

    async function save(element, ctx, project, field, value) {
        setBusy(element, true);
        try {
            const result = await ctx.api.saveField(project.dialogId, field, value);
            ctx.onProject(result.project);
            closeEditor();
        } catch (error) {
            setBusy(element, false);
            showError(element, error);
        }
    }

    function footer(clearLabel) {
        return '<div class="oa-pop-error" hidden></div>'
            + '<div class="oa-pop-actions">'
            + (clearLabel ? '<button type="button" class="oa-btn oa-btn-ghost" data-oa-clear>' + esc(clearLabel) + '</button>' : '')
            + '<span class="oa-pop-spacer"></span>'
            + '<button type="button" class="oa-btn oa-btn-ghost" data-oa-cancel>Отмена</button>'
            + '<button type="button" class="oa-btn oa-btn-primary" data-oa-save>Сохранить</button>'
            + '</div>';
    }

    function bindFooter(element, onSave, onClear) {
        const saveBtn = element.querySelector('[data-oa-save]');
        const cancelBtn = element.querySelector('[data-oa-cancel]');
        const clearBtn = element.querySelector('[data-oa-clear]');
        if (saveBtn) saveBtn.addEventListener('click', onSave);
        if (cancelBtn) cancelBtn.addEventListener('click', closeEditor);
        if (clearBtn && onClear) clearBtn.addEventListener('click', onClear);
    }

    function editContractStatus(anchor, project, ctx) {
        const statuses = ctx.contractStatuses || [];
        const html = '<div class="oa-pop-title">Статус договора</div>'
            + '<div class="oa-pop-list">'
            + statuses.concat(['']).map(status => (
                '<button type="button" class="oa-pop-option' + (status === project.contractStatus ? ' is-selected' : '') + '"'
                + ' data-oa-value="' + esc(status) + '">' + esc(status || 'Не указан') + '</button>'
            )).join('')
            + '</div><div class="oa-pop-error" hidden></div>';
        openPopover(anchor, html, element => {
            element.querySelectorAll('[data-oa-value]').forEach(button => {
                button.addEventListener('click', () => save(element, ctx, project, 'contractStatus', button.dataset.oaValue));
            });
        });
    }

    // A list of the common options: checkboxes, «Добавить» for leaders,
    // rename and remove for administrators.
    function optionListHtml(listKey, list, chosen, ctx, extra) {
        const otherMark = extra && extra.otherMark;
        const items = list.filter(option => !option.removed || chosen.includes(option.id));
        return '<div class="oa-pop-options' + (listKey === 'oprSections' ? ' oa-pop-options-columns' : '') + '" data-oa-options>'
            + items.map(option => {
                const checked = chosen.includes(option.id);
                const other = otherMark && otherMark.ids.includes(option.id);
                return '<label class="oa-pop-check' + (option.removed ? ' is-removed' : '') + '" data-oa-label="' + esc(option.label.toLowerCase()) + '">'
                    + '<input type="checkbox" value="' + esc(option.id) + '"' + (checked ? ' checked' : '') + '>'
                    + '<span class="oa-pop-check-label">' + esc(option.label) + '</span>'
                    + (option.removed ? '<span class="oa-pop-mark">убран из списка</span>' : '')
                    + (other ? '<span class="oa-pop-mark">' + esc(otherMark.text) + '</span>' : '')
                    + (ctx.rights && ctx.rights.canManageOptions
                        ? '<span class="oa-pop-admin">'
                            + '<button type="button" class="oa-icon-btn" data-oa-rename="' + esc(option.id) + '" title="Переименовать для всех проектов">✎</button>'
                            + (option.removed
                                ? '<button type="button" class="oa-icon-btn" data-oa-restore="' + esc(option.id) + '" title="Вернуть в список">↺</button>'
                                : '<button type="button" class="oa-icon-btn" data-oa-remove="' + esc(option.id) + '" title="Убрать из списка для всех проектов">×</button>')
                            + '</span>'
                        : '')
                    + '</label>';
            }).join('')
            + '</div>'
            + (ctx.rights && ctx.rights.canAddOption
                ? '<div class="oa-pop-add"><input type="text" maxlength="' + esc((ctx.limits && ctx.limits.option) || 40) + '"'
                    + ' placeholder="Новый вариант для всех проектов" data-oa-new>'
                    + '<button type="button" class="oa-btn" data-oa-add>Добавить</button></div>'
                : '');
    }

    function bindOptionAdmin(element, listKey, project, ctx, reopen) {
        async function change(action, optionId, label) {
            // The ticks not saved yet survive the new list; a new option
            // comes ticked.
            const checked = checkedIds(element);
            setBusy(element, true);
            try {
                const result = await ctx.api.changeOption(project.dialogId, listKey, action, optionId, label);
                ctx.onOptions(result.options);
                if (action === 'add') {
                    const added = (result.options[listKey] || []).find(
                        option => option.label.toLowerCase() === text(label).toLowerCase()
                    );
                    if (added && !checked.includes(added.id)) checked.push(added.id);
                }
                reopen(checked);
            } catch (error) {
                setBusy(element, false);
                showError(element, error);
            }
        }
        const addBtn = element.querySelector('[data-oa-add]');
        const input = element.querySelector('[data-oa-new]');
        if (addBtn && input) {
            const add = () => {
                const label = text(input.value);
                if (label) change('add', '', label);
            };
            addBtn.addEventListener('click', add);
            input.addEventListener('keydown', event => {
                if (event.key === 'Enter') {
                    event.preventDefault();
                    add();
                }
            });
        }
        element.querySelectorAll('[data-oa-rename]').forEach(button => {
            button.addEventListener('click', event => {
                event.preventDefault();
                const option = (ctx.options[listKey] || []).find(entry => entry.id === button.dataset.oaRename);
                const label = global.prompt('Новое название варианта для всех проектов', option ? option.label : '');
                if (label && text(label)) change('rename', button.dataset.oaRename, text(label));
            });
        });
        element.querySelectorAll('[data-oa-remove]').forEach(button => {
            button.addEventListener('click', event => {
                event.preventDefault();
                const option = (ctx.options[listKey] || []).find(entry => entry.id === button.dataset.oaRemove);
                if (global.confirm('Убрать «' + (option ? option.label : '') + '» из списка всех проектов? Там, где он уже выбран, он останется.')) {
                    change('remove', button.dataset.oaRemove);
                }
            });
        });
        element.querySelectorAll('[data-oa-restore]').forEach(button => {
            button.addEventListener('click', event => {
                event.preventDefault();
                change('restore', button.dataset.oaRestore);
            });
        });
    }

    function checkedIds(element) {
        return Array.from(element.querySelectorAll('[data-oa-options] input[type="checkbox"]'))
            .filter(box => box.checked)
            .map(box => box.value);
    }

    function editStages(anchor, project, ctx, preset) {
        const reopen = checked => {
            const fresh = ctx.findAnchor(project.dialogId, 'stages') || anchor;
            editStages(fresh, ctx.project(project.dialogId) || project, ctx, checked);
        };
        const html = '<div class="oa-pop-title">Стадии по договору</div>'
            + optionListHtml('stages', ctx.options.stages || [], preset || project.stages || [], ctx)
            + footer('');
        openPopover(anchor, html, element => {
            bindOptionAdmin(element, 'stages', project, ctx, reopen);
            bindFooter(element, () => save(element, ctx, project, 'stages', checkedIds(element)));
        });
    }

    function editSections(anchor, project, ctx, half, preset) {
        const sections = project.oprSections || { ready: [], notReady: [] };
        const mine = preset || (half === 'ready' ? sections.ready : sections.notReady);
        const other = half === 'ready' ? sections.notReady : sections.ready;
        const reopen = checked => {
            const fresh = ctx.findAnchor(project.dialogId, 'oprSections', half) || anchor;
            editSections(fresh, ctx.project(project.dialogId) || project, ctx, half, checked);
        };
        const html = '<div class="oa-pop-title">Готовность ОПР: '
            + (half === 'ready' ? '<span class="oa-pop-ready">готовые разделы</span>' : '<span class="oa-pop-not-ready">не готовые разделы</span>')
            + '</div>'
            + '<input type="search" class="oa-pop-search" placeholder="Найти раздел" data-oa-search data-oa-focus>'
            + optionListHtml('oprSections', ctx.options.oprSections || [], mine, ctx, {
                otherMark: { ids: other, text: half === 'ready' ? 'сейчас в «не готовых»' : 'сейчас в «готовых»' }
            })
            + footer('');
        openPopover(anchor, html, element => {
            element.classList.add('oa-popover-wide');
            place(anchor, element);
            const search = element.querySelector('[data-oa-search]');
            search.addEventListener('input', () => {
                const needle = text(search.value).toLowerCase();
                element.querySelectorAll('[data-oa-label]').forEach(label => {
                    label.hidden = !!needle && !label.dataset.oaLabel.includes(needle);
                });
            });
            bindOptionAdmin(element, 'oprSections', project, ctx, reopen);
            bindFooter(element, () => {
                const chosen = checkedIds(element);
                // A section chosen here leaves the other row.
                const rest = other.filter(id => !chosen.includes(id));
                const value = half === 'ready'
                    ? { ready: chosen, notReady: rest }
                    : { ready: rest, notReady: chosen };
                save(element, ctx, project, 'oprSections', value);
            });
        });
    }

    function editGeneral(anchor, project, ctx) {
        const general = project.general || { text: '', color: 0 };
        const limit = (ctx.limits && ctx.limits.general) || 300;
        const swatches = [{ level: 0, label: 'Без заливки', color: '#ffffff', text: '#1f2328' }].concat(ctx.riskLevels || []);
        const html = '<div class="oa-pop-title">Оценка рисков: общее</div>'
            + '<textarea class="oa-pop-text" maxlength="' + limit + '" rows="4" data-oa-text data-oa-focus>' + esc(general.text) + '</textarea>'
            + '<div class="oa-pop-counter"><span data-oa-count></span> / ' + limit + '</div>'
            + '<div class="oa-pop-subtitle">Заливка</div>'
            + '<div class="oa-swatches">'
            + swatches.map(level => (
                '<button type="button" class="oa-swatch' + (Number(general.color || 0) === Number(level.level) ? ' is-selected' : '') + '"'
                + ' data-oa-color="' + level.level + '" title="' + esc(level.label) + '"'
                + ' style="background:' + esc(level.color) + '"></button>'
            )).join('')
            + '</div>'
            + footer('');
        openPopover(anchor, html, element => {
            const area = element.querySelector('[data-oa-text]');
            const count = element.querySelector('[data-oa-count]');
            let color = Number(general.color || 0);
            const update = () => { count.textContent = String(area.value.length); };
            area.addEventListener('input', update);
            update();
            element.querySelectorAll('[data-oa-color]').forEach(button => {
                button.addEventListener('click', () => {
                    color = Number(button.dataset.oaColor);
                    element.querySelectorAll('[data-oa-color]').forEach(other => other.classList.toggle('is-selected', other === button));
                });
            });
            bindFooter(element, () => save(element, ctx, project, 'general', { text: area.value, color }));
        });
    }

    function editDate(anchor, project, ctx, key) {
        const title = key === 'oprCustomerDate' ? 'Дата передачи ОПР Заказчику' : 'Дата передачи ОПР в стадию ПД';
        const html = '<div class="oa-pop-title">' + esc(title) + '</div>'
            + '<input type="date" class="oa-pop-input" value="' + esc(project[key] || '') + '" data-oa-date data-oa-focus>'
            + footer('Очистить');
        openPopover(anchor, html, element => {
            const input = element.querySelector('[data-oa-date]');
            bindFooter(
                element,
                () => save(element, ctx, project, key, input.value),
                () => save(element, ctx, project, key, '')
            );
        });
    }

    function editMeeting(anchor, project, ctx, key) {
        const meeting = project[key] || { at: '', weekly: false };
        const title = key === 'vks' ? 'ВКС с Заказчиком' : 'Совещание по объекту (внутреннее)';
        const html = '<div class="oa-pop-title">' + esc(title) + '</div>'
            + '<input type="datetime-local" class="oa-pop-input" value="' + esc(meeting.at || '') + '" data-oa-at data-oa-focus>'
            + '<label class="oa-pop-check oa-pop-weekly"><input type="checkbox" data-oa-weekly' + (meeting.weekly ? ' checked' : '') + '>'
            + '<span>Повторять еженедельно</span></label>'
            + footer('Очистить');
        openPopover(anchor, html, element => {
            const at = element.querySelector('[data-oa-at]');
            const weekly = element.querySelector('[data-oa-weekly]');
            bindFooter(
                element,
                () => save(element, ctx, project, key, { at: at.value, weekly: weekly.checked }),
                () => save(element, ctx, project, key, { at: '', weekly: false })
            );
        });
    }

    function editResponsible(anchor, project, ctx) {
        const limit = (ctx.limits && ctx.limits.customerResponsible) || 500;
        const html = '<div class="oa-pop-title">Ответственный от Заказчика</div>'
            + '<textarea class="oa-pop-text" maxlength="' + limit + '" rows="4" data-oa-text data-oa-focus>' + esc(project.customerResponsible || '') + '</textarea>'
            + footer('');
        openPopover(anchor, html, element => {
            const area = element.querySelector('[data-oa-text]');
            bindFooter(element, () => save(element, ctx, project, 'customerResponsible', area.value));
        });
    }

    function openEditor(anchor, project, key, ctx, half) {
        if (!editable(project, key, ctx)) return;
        if (key === 'contractStatus') editContractStatus(anchor, project, ctx);
        else if (key === 'stages') editStages(anchor, project, ctx);
        else if (key === 'oprSections') editSections(anchor, project, ctx, half === 'notReady' ? 'notReady' : 'ready');
        else if (key === 'general') editGeneral(anchor, project, ctx);
        else if (key === 'oprCustomerDate' || key === 'oprPdDate') editDate(anchor, project, ctx, key);
        else if (key === 'meeting' || key === 'vks') editMeeting(anchor, project, ctx, key);
        else if (key === 'customerResponsible') editResponsible(anchor, project, ctx);
    }

    // Clicks inside a grid: editors, the risk window, the ИД checklist.
    function bindGrid(container, ctx) {
        function handle(event) {
            const target = event.target;
            const open = target.closest('[data-oa-open]');
            const rowEl = target.closest('[data-oa-project]');
            const project = rowEl ? ctx.project(rowEl.dataset.oaProject) : null;
            if (!project) return;
            if (open) {
                event.preventDefault();
                if (open.dataset.oaOpen === 'risks') ctx.openRisks(project);
                else if (open.dataset.oaOpen === 'id' && text(project.idChecklistUrl)) {
                    global.open(project.idChecklistUrl, '_blank', 'noopener');
                } else if (open.dataset.oaOpen === 'opr' && text(project.oprChecklistUrl)) {
                    global.open(project.oprChecklistUrl, '_blank', 'noopener');
                }
                return;
            }
            const edit = target.closest('[data-oa-edit]');
            if (edit) {
                event.preventDefault();
                openEditor(edit, project, edit.dataset.oaEdit, ctx, edit.dataset.oaHalf);
            }
        }
        container.addEventListener('click', handle);
        container.addEventListener('keydown', event => {
            if ((event.key === 'Enter' || event.key === ' ') && event.target.matches('[data-oa-edit]')) handle(event);
        });
    }

    function findAnchor(container, dialogId, key, half) {
        const row = Array.from(container.querySelectorAll('[data-oa-project]'))
            .find(element => element.dataset.oaProject === dialogId);
        if (!row) return null;
        if (key === 'oprSections') return row.querySelector('[data-oa-half="' + (half || 'ready') + '"]');
        return row.querySelector('[data-oa-edit="' + key + '"]');
    }

    global.ObjectAnalysisUI = Object.freeze({
        COLUMNS,
        createApi,
        gridTemplate,
        headerHtml,
        rowHtml,
        bindGrid,
        findAnchor,
        closeEditor,
        formatDate,
        meetingText,
        esc,
        text
    });
})(window);
