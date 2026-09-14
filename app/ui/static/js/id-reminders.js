(function (global) {
    'use strict';
    const host = document.getElementById('idReminderControl');
    const toggle = document.getElementById('idReminderEnabled');
    const settingsButton = document.getElementById('idReminderSettings');
    const summary = document.getElementById('idReminderSummary');
    let config = { enabled: false, days: [0,1,2,3,4,5,6], hour: 9, minute: 0, timezoneOffset: 600, recipients: [] };
    let loadedSession = '', loading = false, loaded = false, deliveries = [], isPending = false;
    const weekdays = ['ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС'];
    const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const pad = n => String(n).padStart(2, '0');
    function actor(sessionId) {
        const who = getCurrentEditorIdentity();
        return { dialogId, sessionId, userId: String(who.userId || '') };
    }
    function zoneLabel(offset) {
        return 'UTC' + (offset >= 0 ? '+' : '−') + pad(Math.floor(Math.abs(offset) / 60)) + ':' + pad(Math.abs(offset) % 60);
    }
    async function request(sessionId, value) {
        const payload = actor(sessionId);
        const url = appUrl('api/checklist/id-reminders');
        if (global.ChecklistPopupBitrix) await global.ChecklistPopupBitrix.init();
        const auth = global.BX24 && typeof global.BX24.getAuth === 'function' ? global.BX24.getAuth() : null;
        if (!auth || !auth.access_token) throw new Error('Настройте оповещения из приложения внутри Битрикс24');
        const headers = { 'X-Bitrix-Access-Token': auth.access_token };
        const response = await fetch(value ? url : url + '?' + new URLSearchParams(payload), value ? {
            method: 'PUT', headers: { ...headers, 'Content-Type': 'application/json' },
            body: JSON.stringify({ ...payload, config: value })
        } : { headers });
        const result = await response.json();
        if (!response.ok || !result.ok) throw new Error(result.error || 'Не удалось сохранить оповещения');
        config = result.config; deliveries = result.deliveries || []; loaded = true; isPending = !!result.pending;
        paint(result.pending);
        return result;
    }
    function paint(pending) {
        toggle.checked = !!config.enabled;
        settingsButton.hidden = !config.enabled;
        summary.textContent = config.enabled ? ((config.days.length === 7 ? 'Ежедневно' : config.days.map(d => weekdays[d]).join(', '))
            + ' · ' + pad(config.hour) + ':' + pad(config.minute) + ' ' + zoneLabel(config.timezoneOffset)
            + (pending ? ' · после сохранения' : '')) : (pending ? 'Отключение после сохранения' : '');
    }
    async function refresh() {
        if (!host) return;
        host.hidden = currentChecklistKey !== 'id';
        if (host.hidden || loading) return;
        const sessionId = global.ChecklistPopupEditSession && global.ChecklistPopupEditSession.getSessionId();
        if (!sessionId || loadedSession === sessionId) return;
        loading = true;
        try { await request(sessionId); loadedSession = sessionId; }
        catch (error) { summary.textContent = error.message; }
        finally { loading = false; }
    }
    async function open() {
        const sessionId = await requireEditingSession('настройка оповещений');
        if (!loaded || loadedSession !== sessionId) await request(sessionId);
        loadedSession = sessionId;
        if (document.querySelector(".id-reminder-dialog")) return;
        let recipients = config.recipients.map(u => ({ ...u }));
        const box = document.createElement('dialog');
        box.className = 'id-reminder-dialog';
        box.setAttribute('aria-labelledby', 'idReminderDialogTitle');
        const offsets = [...new Set([120,180,240,300,360,420,480,540,600,660,720,config.timezoneOffset])].sort((a,b)=>a-b);
        const issues = deliveries.filter(d => ['failed', 'uncertain'].includes(d.status));
        box.innerHTML = `<form method="dialog" class="id-reminder-form">
            <h2 id="idReminderDialogTitle">Оповещения по ИД</h2>
            <fieldset><legend>Периодичность</legend>
              <label class="id-reminder-daily"><input type="checkbox" data-daily ${config.days.length === 7 ? 'checked' : ''}> Каждый день</label>
              <div class="id-reminder-days">${weekdays.map((d,i) => `<label><input type="checkbox" data-day="${i}" ${config.days.includes(i) ? 'checked' : ''}> Каждый ${d}</label>`).join('')}</div>
            </fieldset>
            <div class="id-reminder-time"><label>Время<input type="time" name="time" step="60" required value="${pad(config.hour)}:${pad(config.minute)}"></label>
            <label>Часовой пояс<select name="timezone">${offsets.map(v=>`<option value="${v}" ${v===config.timezoneOffset?'selected':''}>${zoneLabel(v)}${v===180?' · Москва':''}</option>`).join('')}</select></label></div>
            <label for="idReminderUser">Получатели</label><div class="bitrix-user-picker"><input id="idReminderUser" type="search" placeholder="Имя или фамилия сотрудника" autocomplete="off">
            <input type="hidden" data-user-id><input type="hidden" data-user-name><div data-users hidden></div><div class="id-reminder-hint" data-picker-hint></div></div>
            <div class="id-reminder-recipients" data-recipients></div>
            <p class="id-reminder-hint">В личные сообщения от аккаунта вебхука. В список попадут пункты ИД, ТУ и «Прочее» без текущих файлов; «Не требуется» пропускается.</p>
            <p class="id-reminder-hint">Настройка вступит в силу после сохранения сеанса.</p>
            ${issues.length ? `<details class="id-reminder-errors"><summary>Есть неподтверждённые отправки (${issues.length})</summary>${issues.slice(0,5).map(d=>`<p>${esc((config.recipients.find(u=>u.userId===d.user_id)||{}).name||d.user_id)}: ${esc(d.error)}</p>`).join('')}</details>` : ''}
            <div class="id-reminder-error" data-error role="alert"></div>
            <div class="id-reminder-footer"><button type="button" data-cancel>Отмена</button><button type="submit" data-save>Сохранить настройку</button></div>
        </form>`;
        document.body.appendChild(box);
        const q = selector => box.querySelector(selector);
        function chips() {
            q('[data-recipients]').innerHTML = recipients.map(u=>`<span class="id-reminder-chip">${esc(u.name)}<button type="button" data-remove="${esc(u.userId)}" aria-label="Убрать ${esc(u.name)}">×</button></span>`).join('');
        }
        chips();
        box.addEventListener('click', event => {
            const remove = event.target.closest('[data-remove]');
            if (remove) { recipients = recipients.filter(u=>u.userId!==remove.dataset.remove); chips(); }
        });
        const picker = global.ChecklistBitrixUserPicker.create({
            input:q('#idReminderUser'), results:q('[data-users]'), hint:q('[data-picker-hint]'),
            idInput:q('[data-user-id]'), nameInput:q('[data-user-name]'), apiUrl:()=>appUrl('api/checklist/bitrix-users'),
            onSelect(user) { if(user && !recipients.some(u=>u.userId===user.userId)) recipients.push(user); chips(); }
        });
        q('[data-daily]').addEventListener('change', event=>box.querySelectorAll('[data-day]').forEach(c=>{c.checked=event.target.checked;}));
        box.querySelectorAll('[data-day]').forEach(c=>c.addEventListener('change',()=>{q('[data-daily]').checked=[...box.querySelectorAll('[data-day]')].every(e=>e.checked);}));
        let saving = false;
        function close() { if (saving) return; if (picker.destroy) picker.destroy(); box.close(); box.remove(); paint(isPending); }
        q('[data-cancel]').addEventListener('click',close);
        box.addEventListener('cancel',event=>{event.preventDefault();close();});
        box.addEventListener('submit', async event => {
            event.preventDefault(); if(saving) return;
            const days = [...box.querySelectorAll('[data-day]:checked')].map(c=>Number(c.dataset.day));
            if (!days.length || !recipients.length) { q('[data-error]').textContent='Выберите дни и хотя бы одного получателя.'; return; }
            const [hour,minute]=q('[name=time]').value.split(':').map(Number);
            saving=true; q('[data-save]').disabled=true;
            try {
                await request(sessionId,{enabled:true,days,hour,minute,timezoneOffset:Number(q('[name=timezone]').value),recipients});
                saving=false; close(); paint(true);
                if(typeof setSaveState==='function') setSaveState('pending','Настройка оповещений изменена');
            } catch(error) { q('[data-error]').textContent=error.message; saving=false; q('[data-save]').disabled=false; }
        });
        box.showModal(); q('#idReminderUser').focus();
    }
    function report(error) { paint(isPending); summary.textContent=error.message; }
    if (host) {
        toggle.addEventListener('change',async()=>{
            const requested = toggle.checked; toggle.checked=config.enabled;
            try {
                if(requested) await open();
                else { const sessionId=await requireEditingSession('отключение оповещений'); await request(sessionId,{...config,enabled:false}); }
            } catch(error) { report(error); }
        });
        settingsButton.addEventListener('click',()=>open().catch(report));
        global.addEventListener('checklist-edit-session-state',()=>refresh());
        refresh();
    }
    global.ChecklistIDReminders=Object.freeze({refresh,open});
})(window);
