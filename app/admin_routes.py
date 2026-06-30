import json
import shutil

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from app.db import get_conn
from app.settings import UPLOAD_ROOT
from app.checklists.utils import clean_cell_value, normalize_dialog_id
from app.checklists.yandex_warmup_control import request_yandex_warmup_stop


router = APIRouter()

ADMIN_USER_IDS = {"138", "18"}


def is_admin_user(user_id: str) -> bool:
    return clean_cell_value(user_id) in ADMIN_USER_IDS


def json_admin_denied():
    return JSONResponse({
        "ok": False,
        "error": "admin access denied",
    }, status_code=403)


def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row)


def placeholders(values: list[str]) -> str:
    return ",".join("?" for _ in values) or "?"


def get_dialog_variants(dialog_id: str) -> list[str]:
    raw = clean_cell_value(dialog_id)
    normalized = normalize_dialog_id(raw)

    variants = []
    for value in [raw, normalized]:
        value = clean_cell_value(value)
        if value and value not in variants:
            variants.append(value)

    if normalized.startswith("chat"):
        numeric = normalized[4:]
        if numeric and numeric not in variants:
            variants.append(numeric)

    return variants


def count_rows(conn, table_name: str, where_sql: str, params: tuple) -> int:
    if not table_exists(conn, table_name):
        return 0

    row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM {table_name} WHERE {where_sql}",
        params,
    ).fetchone()

    return int(row["cnt"] if row else 0)


def delete_rows(conn, table_name: str, where_sql: str, params: tuple) -> int:
    if not table_exists(conn, table_name):
        return 0

    cur = conn.execute(
        f"DELETE FROM {table_name} WHERE {where_sql}",
        params,
    )

    return int(cur.rowcount or 0)


def get_project_summary(dialog_id: str) -> dict:
    normalized = normalize_dialog_id(dialog_id)
    variants = get_dialog_variants(normalized)
    checklist_like = normalized + "::%"

    conn = get_conn()

    project_context_count = count_rows(
        conn,
        "project_storage_contexts",
        f"dialog_id IN ({placeholders(variants)})",
        tuple(variants),
    )

    checklist_count = count_rows(
        conn,
        "checklists",
        f"dialog_id IN ({placeholders(variants)}) OR dialog_id LIKE ?",
        tuple(variants + [checklist_like]),
    )

    upload_jobs_count = count_rows(
        conn,
        "upload_jobs",
        f"dialog_id IN ({placeholders(variants)})",
        tuple(variants),
    )

    upload_dir = UPLOAD_ROOT / "checklists" / normalized

    conn.close()

    return {
        "dialogId": normalized,
        "variants": variants,
        "projectContextCount": project_context_count,
        "checklistCount": checklist_count,
        "uploadJobsCount": upload_jobs_count,
        "uploadDir": str(upload_dir),
        "uploadDirExists": upload_dir.exists(),
    }


def load_project_context_rows() -> list[dict]:
    conn = get_conn()

    if not table_exists(conn, "project_storage_contexts"):
        conn.close()
        return []

    rows = conn.execute("""
        SELECT
            dialog_id,
            project_id,
            project_name,
            provider,
            storage_mode_json,
            yandex_json,
            item_mappings_json,
            updated_at
        FROM project_storage_contexts
        ORDER BY updated_at DESC
        LIMIT 500
    """).fetchall()

    conn.close()

    result = []

    for row in rows:
        dialog_id = normalize_dialog_id(row["dialog_id"])
        project_name = clean_cell_value(row["project_name"])
        project_id = clean_cell_value(row["project_id"])

        yandex_json = clean_cell_value(row["yandex_json"])
        yandex_disk = {}
        if yandex_json:
            try:
                yandex_disk = json.loads(yandex_json) or {}
            except Exception:
                yandex_disk = {}

        result.append({
            "dialogId": dialog_id,
            "projectId": project_id,
            "projectName": project_name,
            "provider": clean_cell_value(row["provider"]),
            "updatedAt": clean_cell_value(row["updated_at"]),
            "projectRootPath": clean_cell_value(yandex_disk.get("projectRootPath")),
            "projectRootUrl": clean_cell_value(yandex_disk.get("projectRootUrl")),
            "standardFoldersPrepared": bool(yandex_disk.get("standardFoldersPrepared")),
            "summary": get_project_summary(dialog_id),
        })

    return result


@router.get("/admin", response_class=HTMLResponse)
def admin_page(userId: str = ""):
    user_id = clean_cell_value(userId)
    user_id_json = json.dumps(user_id, ensure_ascii=False)

    if not is_admin_user(user_id):
        return HTMLResponse(
            """
            <html>
            <head><meta charset="utf-8"><title>Доступ запрещён</title></head>
            <body style="font-family:Arial,sans-serif;padding:24px;">
                <h1>Доступ запрещён</h1>
                <p>Админ-панель доступна только техническим пользователям.</p>
            </body>
            </html>
            """,
            status_code=403,
        )

    html = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>Админ-панель чек-листов</title>
    <style>
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: Arial, sans-serif;
            background: #f3f6fb;
            color: #1f2328;
        }
        .shell {
            padding: 18px;
        }
        .panel {
            background: #fff;
            border: 1px solid #e5e7eb;
            border-radius: 14px;
            padding: 16px;
            box-shadow: 0 12px 32px rgba(15,23,42,.08);
        }
        h1 {
            margin: 0 0 10px;
            font-size: 24px;
        }
        .muted {
            color: #667085;
            font-size: 13px;
            line-height: 1.35;
        }
        .toolbar {
            display: flex;
            gap: 8px;
            align-items: center;
            margin: 14px 0;
            flex-wrap: wrap;
        }
        input {
            height: 34px;
            border: 1px solid #d0d7de;
            border-radius: 8px;
            padding: 0 10px;
            font-size: 13px;
        }
        .project-name-input {
            width: 260px;
        }
        .project-id-input {
            width: 130px;
        }
        button {
            height: 34px;
            border: 1px solid #d0d7de;
            border-radius: 8px;
            background: #f8fafc;
            cursor: pointer;
            font-weight: 700;
            padding: 0 12px;
            white-space: nowrap;
        }
        button:hover {
            background: #f1f5f9;
        }
        button.primary {
            background: #eff6ff;
            border-color: #bfdbfe;
            color: #175cd3;
        }
        button.danger {
            background: #fef2f2;
            border-color: #fca5a5;
            color: #b42318;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 12px;
            font-size: 13px;
        }
        th, td {
            border-bottom: 1px solid #edf0f2;
            padding: 9px 8px;
            text-align: left;
            vertical-align: top;
        }
        th {
            background: #fafbfc;
            color: #475467;
            font-size: 12px;
        }
        .path {
            max-width: 360px;
            word-break: break-word;
            font-size: 12px;
            color: #667085;
        }
        .badge {
            display: inline-flex;
            align-items: center;
            padding: 3px 7px;
            border-radius: 999px;
            font-size: 11px;
            font-weight: 700;
            background: #eef2ff;
            color: #3730a3;
            margin-right: 4px;
            margin-bottom: 4px;
        }
        .badge.warn {
            background: #fff4e5;
            color: #b26a00;
        }
        .badge.good {
            background: #ecfdf3;
            color: #027a48;
        }
        .checks {
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
            margin: 10px 0;
            font-size: 13px;
        }
        .checks label {
            display: flex;
            align-items: center;
            gap: 5px;
        }
        .checks input {
            height: auto;
        }
        .status {
            margin-top: 12px;
            white-space: pre-wrap;
            font-family: Consolas, monospace;
            font-size: 12px;
            background: #0f172a;
            color: #e5e7eb;
            border-radius: 10px;
            padding: 12px;
            min-height: 48px;
            max-height: 360px;
            overflow: auto;
        }
        .danger-note {
            margin-top: 10px;
            padding: 10px 12px;
            border: 1px solid #fecaca;
            border-radius: 10px;
            background: #fef2f2;
            color: #b42318;
            font-size: 13px;
            line-height: 1.4;
        }
    </style>
</head>
<body>
    <div class="shell">
        <div class="panel">
            <h1>Админ-панель чек-листов</h1>
            <div class="muted">
                Здесь отображаются проекты из <b>project_storage_contexts</b>. Именно по этой таблице приложение поднимает сохранённые проекты и запускает yandex warmup.
                Текущий userId: <b id="userIdText"></b>
            </div>

            <div class="toolbar">
                <button id="reloadBtn" class="primary" type="button">Обновить список</button>
                <input id="dialogInput" type="text" placeholder="dialogId, например chat3122">
                <button id="inspectBtn" type="button">Проверить dialogId</button>
            </div>

            <div class="checks">
                <label><input id="deleteContext" type="checkbox" checked> удалить project_storage_contexts</label>
                <label><input id="deleteChecklists" type="checkbox" checked> удалить checklists</label>
                <label><input id="deleteUploadJobs" type="checkbox" checked> удалить upload_jobs</label>
                <label><input id="deleteLocalUploads" type="checkbox"> удалить локальные uploads</label>
            </div>

            <div class="danger-note">
                Для того чтобы yandex warmup перестал реагировать на старый chatId, обязательно нужно удалить <b>project_storage_contexts</b>.
                Если включить <b>checklists</b>, будут удалены все чек-листы этого chatId, включая ключи вида <b>chat3122::opr</b>, <b>chat3122::p</b>, <b>chat3122::concept</b>.
            </div>

            <table>
                <thead>
                    <tr>
                        <th>chatId</th>
                        <th>Объект / проект</th>
                        <th>Яндекс</th>
                        <th>Связанные данные</th>
                        <th>Действия</th>
                    </tr>
                </thead>
                <tbody id="projectsBody"></tbody>
            </table>

            <div id="statusBox" class="status">Готово.</div>
        </div>
    </div>

    <script>
        const USER_ID = __USER_ID_JSON__;

        const userIdText = document.getElementById('userIdText');
        const statusBox = document.getElementById('statusBox');
        const projectsBody = document.getElementById('projectsBody');
        const dialogInput = document.getElementById('dialogInput');

        userIdText.textContent = USER_ID;

        function detectAppBasePath() {
            const path = String(window.location.pathname || '').replace(/\\/+$/, '');
            if (path.endsWith('/admin')) {
                return path.slice(0, -'/admin'.length) || '';
            }
            return '';
        }

        const APP_BASE_PATH = detectAppBasePath();

        function appUrl(path) {
            return (APP_BASE_PATH || '') + '/' + String(path || '').replace(/^\\/+/, '');
        }

        function esc(value) {
            if (value === null || value === undefined) return '';
            return String(value)
                .replaceAll('&', '&amp;')
                .replaceAll('<', '&lt;')
                .replaceAll('>', '&gt;')
                .replaceAll('"', '&quot;');
        }

        function setStatus(value) {
            statusBox.textContent = typeof value === 'string'
                ? value
                : JSON.stringify(value, null, 2);
        }

        async function apiGet(path) {
            const separator = path.includes('?') ? '&' : '?';
            const response = await fetch(appUrl(path) + separator + 'userId=' + encodeURIComponent(USER_ID));
            const result = await response.json().catch(() => ({}));
            if (!response.ok || !result.ok) {
                throw new Error(result.error || 'request failed');
            }
            return result;
        }

        async function apiPost(path, payload) {
            const response = await fetch(appUrl(path), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ...payload, userId: USER_ID })
            });

            const result = await response.json().catch(() => ({}));

            if (!response.ok || !result.ok) {
                throw new Error(result.error || 'request failed');
            }

            return result;
        }

        function selectedDeleteOptions() {
            return {
                deleteContext: document.getElementById('deleteContext').checked,
                deleteChecklists: document.getElementById('deleteChecklists').checked,
                deleteUploadJobs: document.getElementById('deleteUploadJobs').checked,
                deleteLocalUploads: document.getElementById('deleteLocalUploads').checked
            };
        }

        function renderProjects(items) {
            projectsBody.innerHTML = '';

            if (!items.length) {
                projectsBody.innerHTML = `
                    <tr>
                        <td colspan="5" class="muted">Проекты в project_storage_contexts не найдены.</td>
                    </tr>
                `;
                return;
            }

            items.forEach(item => {
                const summary = item.summary || {};
                const preparedClass = item.standardFoldersPrepared ? 'badge good' : 'badge warn';
                const preparedText = item.standardFoldersPrepared ? 'folders prepared' : 'folders not prepared';

                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td>
                        <b>${esc(item.dialogId)}</b>
                        <div class="muted">projectId: ${esc(item.projectId || '')}</div>
                    </td>
                    <td>
                        <input
                            class="project-name-input"
                            data-role="project-name"
                            data-dialog-id="${esc(item.dialogId)}"
                            value="${esc(item.projectName || '')}"
                            placeholder="Название объекта"
                        >
                        <br>
                        <input
                            class="project-id-input"
                            data-role="project-id"
                            data-dialog-id="${esc(item.dialogId)}"
                            value="${esc(item.projectId || '')}"
                            placeholder="projectId"
                            style="margin-top:6px;"
                        >
                    </td>
                    <td>
                        <span class="${preparedClass}">${preparedText}</span>
                        <div class="path">${esc(item.projectRootPath || '')}</div>
                        ${item.projectRootUrl ? `<div><a href="${esc(item.projectRootUrl)}" target="_blank">Открыть папку</a></div>` : ''}
                    </td>
                    <td>
                        <span class="badge">contexts: ${summary.projectContextCount || 0}</span>
                        <span class="badge">checklists: ${summary.checklistCount || 0}</span>
                        <span class="badge">jobs: ${summary.uploadJobsCount || 0}</span>
                        <span class="${summary.uploadDirExists ? 'badge good' : 'badge warn'}">uploads: ${summary.uploadDirExists ? 'yes' : 'no'}</span>
                    </td>
                    <td>
                        <button type="button" class="primary" data-action="save" data-dialog-id="${esc(item.dialogId)}">Сохранить</button>
                        <button type="button" data-action="inspect" data-dialog-id="${esc(item.dialogId)}">Проверить</button>
                        <button type="button" class="danger" data-action="delete" data-dialog-id="${esc(item.dialogId)}">Удалить</button>
                    </td>
                `;

                projectsBody.appendChild(tr);
            });
        }

        async function reloadProjects() {
            try {
                setStatus('Загружаем проекты...');
                const result = await apiGet('api/admin/projects');
                renderProjects(result.items || []);
                setStatus(result);
            } catch (e) {
                setStatus('Ошибка: ' + String(e.message || e));
            }
        }

        async function inspectProject(dialogId) {
            try {
                const target = String(dialogId || dialogInput.value || '').trim();
                if (!target) {
                    alert('Укажи dialogId');
                    return;
                }

                const result = await apiGet('api/admin/project-summary?dialogId=' + encodeURIComponent(target));
                setStatus(result);
            } catch (e) {
                setStatus('Ошибка: ' + String(e.message || e));
            }
        }

        async function saveProject(dialogId) {
            const nameInput = document.querySelector('[data-role="project-name"][data-dialog-id="' + CSS.escape(dialogId) + '"]');
            const idInput = document.querySelector('[data-role="project-id"][data-dialog-id="' + CSS.escape(dialogId) + '"]');

            const projectName = nameInput ? nameInput.value : '';
            const projectId = idInput ? idInput.value : '';

            try {
                const result = await apiPost('api/admin/update-project', {
                    dialogId,
                    projectName,
                    projectId
                });
                setStatus(result);
                await reloadProjects();
            } catch (e) {
                setStatus('Ошибка: ' + String(e.message || e));
            }
        }

        async function deleteProject(dialogId) {
            const target = String(dialogId || dialogInput.value || '').trim();
            if (!target) {
                alert('Укажи dialogId');
                return;
            }

            const opts = selectedDeleteOptions();

            const message =
                'Удалить данные проекта ' + target + '?\\n\\n' +
                'project_storage_contexts: ' + opts.deleteContext + '\\n' +
                'checklists: ' + opts.deleteChecklists + '\\n' +
                'upload_jobs: ' + opts.deleteUploadJobs + '\\n' +
                'local uploads: ' + opts.deleteLocalUploads + '\\n\\n' +
                'После удаления project_storage_contexts yandex warmup больше не должен запускаться по этому chatId.\\n\\n' +
                'Действие необратимо.';

            if (!confirm(message)) {
                return;
            }

            try {
                const result = await apiPost('api/admin/delete-project', {
                    dialogId: target,
                    ...opts
                });
                setStatus(result);
                await reloadProjects();
            } catch (e) {
                setStatus('Ошибка: ' + String(e.message || e));
            }
        }

        document.getElementById('reloadBtn').addEventListener('click', reloadProjects);
        document.getElementById('inspectBtn').addEventListener('click', function () {
            inspectProject('');
        });

        projectsBody.addEventListener('click', function (event) {
            const btn = event.target.closest('button[data-action]');
            if (!btn) return;

            const action = btn.dataset.action;
            const dialogId = btn.dataset.dialogId;

            if (action === 'save') {
                saveProject(dialogId);
            }

            if (action === 'inspect') {
                inspectProject(dialogId);
            }

            if (action === 'delete') {
                deleteProject(dialogId);
            }
        });

        reloadProjects();
    </script>
</body>
</html>
"""

    html = html.replace("__USER_ID_JSON__", user_id_json)
    return HTMLResponse(html)


@router.get("/api/admin/projects")
def api_admin_projects(userId: str = ""):
    if not is_admin_user(userId):
        return json_admin_denied()

    return JSONResponse({
        "ok": True,
        "items": load_project_context_rows(),
    })


@router.get("/api/admin/project-summary")
def api_admin_project_summary(userId: str = "", dialogId: str = ""):
    if not is_admin_user(userId):
        return json_admin_denied()

    dialog_id = normalize_dialog_id(dialogId)
    if not dialog_id:
        return JSONResponse({
            "ok": False,
            "error": "dialogId is required",
        }, status_code=400)

    return JSONResponse({
        "ok": True,
        "summary": get_project_summary(dialog_id),
    })


@router.post("/api/admin/update-project")
async def api_admin_update_project(request: Request):
    payload = await request.json()

    user_id = clean_cell_value(payload.get("userId"))
    if not is_admin_user(user_id):
        return json_admin_denied()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    project_name = clean_cell_value(payload.get("projectName"))
    project_id = clean_cell_value(payload.get("projectId"))

    if not dialog_id:
        return JSONResponse({
            "ok": False,
            "error": "dialogId is required",
        }, status_code=400)

    variants = get_dialog_variants(dialog_id)

    conn = get_conn()

    if not table_exists(conn, "project_storage_contexts"):
        conn.close()
        return JSONResponse({
            "ok": False,
            "error": "project_storage_contexts table not found",
        }, status_code=404)

    cur = conn.execute(
        f"""
        UPDATE project_storage_contexts
        SET project_name = ?,
            project_id = ?,
            updated_at = datetime('now')
        WHERE dialog_id IN ({placeholders(variants)})
        """,
        tuple([project_name, project_id] + variants),
    )

    conn.commit()
    updated = int(cur.rowcount or 0)
    conn.close()

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "updated": updated,
        "projectName": project_name,
        "projectId": project_id,
        "summary": get_project_summary(dialog_id),
    })


@router.post("/api/admin/delete-project")
async def api_admin_delete_project(request: Request):
    payload = await request.json()

    user_id = clean_cell_value(payload.get("userId"))
    if not is_admin_user(user_id):
        return json_admin_denied()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    if not dialog_id:
        return JSONResponse({
            "ok": False,
            "error": "dialogId is required",
        }, status_code=400)

    delete_context = bool(payload.get("deleteContext", True))
    delete_checklists = bool(payload.get("deleteChecklists", True))
    delete_upload_jobs = bool(payload.get("deleteUploadJobs", True))
    delete_local_uploads = bool(payload.get("deleteLocalUploads", False))

    variants = get_dialog_variants(dialog_id)
    checklist_like = dialog_id + "::%"

    request_yandex_warmup_stop(dialog_id)

    conn = get_conn()
    deleted = {}

    try:
        if delete_context:
            deleted["project_storage_contexts"] = delete_rows(
                conn,
                "project_storage_contexts",
                f"dialog_id IN ({placeholders(variants)})",
                tuple(variants),
            )
        else:
            deleted["project_storage_contexts"] = 0

        if delete_checklists:
            deleted["checklists"] = delete_rows(
                conn,
                "checklists",
                f"dialog_id IN ({placeholders(variants)}) OR dialog_id LIKE ?",
                tuple(variants + [checklist_like]),
            )
        else:
            deleted["checklists"] = 0

        if delete_upload_jobs:
            deleted["upload_jobs"] = delete_rows(
                conn,
                "upload_jobs",
                f"dialog_id IN ({placeholders(variants)})",
                tuple(variants),
            )
        else:
            deleted["upload_jobs"] = 0

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    uploads_deleted = False
    uploads_path = UPLOAD_ROOT / "checklists" / dialog_id

    if delete_local_uploads and uploads_path.exists():
        shutil.rmtree(uploads_path)
        uploads_deleted = True

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "variants": variants,
        "stopWarmupRequested": True,
        "deleted": deleted,
        "uploadsPath": str(uploads_path),
        "uploadsDeleted": uploads_deleted,
        "summaryAfter": get_project_summary(dialog_id),
    })