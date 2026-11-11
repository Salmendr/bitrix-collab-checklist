"""A personal message after Yandex Disk received the files of a session.

When every file saved in one edit session has gone to Yandex Disk (or has
finally failed) and one of them is 1 GiB or bigger, the employee who
uploaded them gets one message in his personal chat with Bitrix24:

    Загрузка файла на Яндекс Диск завершена!
    [Чек-лист ОПР — Проект] (a link to the checklist)

    Пункт — Ссылка на папку (a link to the item folder on Yandex Disk)
    • file.dwg
    • other.pdf — ошибка загрузки на Яндекс Диск

The check runs after every Yandex job and in the periodic sweep (after a
restart too). A session gets one message at most. Sessions whose files
had all reached Yandex Disk before this feature existed get none.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone

from app.bitrix.client import bitrix_webhook_call
from app.db import get_conn
from app.logging_utils import write_debug_log
from app.settings import BITRIX_TECH_WEBHOOK_URL
from app.checklists.utils import clean_cell_value

BIG_FILE_BYTES = 1024 * 1024 * 1024
FINAL_JOB_STATUSES = {"synced", "error", "cancelled", "skipped", "done"}
HEADER = "Загрузка файла на Яндекс Диск завершена!"
FAILED_MARK = " — ошибка загрузки на Яндекс Диск"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _job_clock() -> str:
    # The clock of upload_jobs (finished_at): local time without a zone.
    return datetime.now().isoformat(timespec="seconds")


_TABLES_READY = False


def ensure_yandex_upload_notice_tables() -> None:
    global _TABLES_READY
    if _TABLES_READY:
        return
    conn = get_conn()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS yandex_upload_notices (
                session_id TEXT PRIMARY KEY,
                user_id TEXT,
                status TEXT,
                message_id TEXT,
                error TEXT,
                files_json TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS yandex_upload_notice_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        # From now on: files that reached Yandex Disk before do not notify.
        conn.execute(
            "INSERT OR IGNORE INTO yandex_upload_notice_state(key, value) VALUES ('enabled_at', ?)",
            (_job_clock(),),
        )
        conn.commit()
        _TABLES_READY = True
    finally:
        conn.close()


def _enabled_at(conn) -> str:
    row = conn.execute(
        "SELECT value FROM yandex_upload_notice_state WHERE key = 'enabled_at'"
    ).fetchone()
    return clean_cell_value(row["value"] if row else "")


def _session_jobs(conn, session_id: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT job_id, dialog_id, checklist_key, item_id, document_id,
               file_name, file_size, yandex_path, status, error,
               created_at, finished_at
        FROM upload_jobs
        WHERE source_session_id = ?
          AND job_type = 'upload'
        ORDER BY created_at ASC, rowid ASC
        """,
        (session_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _webhook_owner_id() -> str:
    match = re.search(r"/rest/(\d+)/", BITRIX_TECH_WEBHOOK_URL or "")
    return match.group(1) if match else ""


def _item_label(items: list[dict], item: dict) -> str:
    name = clean_cell_value(item.get("name")) or "Пункт"
    parent_id = clean_cell_value(item.get("parentItemId"))
    if parent_id:
        parent = next((i for i in items if clean_cell_value(i.get("id")) == parent_id), None)
        if parent and clean_cell_value(parent.get("name")):
            return f"{clean_cell_value(parent.get('name'))} / {name}"
    return name


def _item_folder_url(dialog_id: str, checklist_key: str, item: dict, job: dict) -> str:
    """The public link of the item folder, as the folder page's Yandex
    button gives it (published if it has none yet); the Yandex Disk address
    of the folder when no public link can be had."""
    from app.yandex_disk.client import yandex_disk_client_url

    if item:
        try:
            from app.checklists.folder_page import public_folder_link
            result = public_folder_link(dialog_id, checklist_key, item, "")
            if result.get("ok") and clean_cell_value(result.get("url")):
                return clean_cell_value(result.get("url"))
        except Exception as exc:
            write_debug_log("yandex_upload_notice_folder_link_failed", {
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "itemId": clean_cell_value(item.get("id")),
                "error": str(exc)[:300],
            })
        try:
            from app.checklists.folder_page import resolve_item_yandex_folder
            path = clean_cell_value(resolve_item_yandex_folder(dialog_id, checklist_key, item).get("path"))
            if path:
                return yandex_disk_client_url(path)
        except Exception:
            pass
    file_path = clean_cell_value(job.get("yandex_path"))
    if file_path and "/" in file_path.rstrip("/"):
        return yandex_disk_client_url(file_path.rstrip("/").rsplit("/", 1)[0])
    return ""


def _checklist_caption(dialog_id: str, checklist_key: str) -> str:
    from app.checklists.messages import get_message_checklist_title
    from app.checklists.project_phases import base_dialog_id, phase_label
    from app.checklists.storage import get_project_storage_context

    title = get_message_checklist_title(checklist_key)
    stage = phase_label(dialog_id)
    if stage:
        title = f"{title} · {stage}"
    project = ""
    for candidate in (dialog_id, base_dialog_id(dialog_id)):
        context = get_project_storage_context(candidate) or {}
        project = clean_cell_value(context.get("projectName"))
        if project:
            break
    return f"{title} — {project}" if project else title


def build_yandex_upload_notice(jobs: list[dict]) -> tuple[str, list[dict]]:
    """The message text and the files it lists (cancelled jobs are files
    that no longer exist: they are left out)."""
    from app.checklists.messages import build_checklist_message_link
    from app.checklists.storage import get_checklist

    files = []
    checklists: dict[tuple[str, str], dict] = {}
    for job in jobs:
        if clean_cell_value(job.get("status")) == "cancelled":
            continue
        dialog_id = clean_cell_value(job.get("dialog_id"))
        checklist_key = clean_cell_value(job.get("checklist_key")) or "id"
        key = (dialog_id, checklist_key)
        if key not in checklists:
            try:
                data = get_checklist(dialog_id, checklist_key) or {}
            except Exception:
                data = {}
            checklists[key] = {"items": data.get("items") or [], "groups": {}}
        entry = checklists[key]
        item_id = clean_cell_value(job.get("item_id"))
        if item_id not in entry["groups"]:
            item = next((i for i in entry["items"] if clean_cell_value(i.get("id")) == item_id), {}) or {}
            entry["groups"][item_id] = {
                "label": _item_label(entry["items"], item) if item else "Пункт",
                "folderUrl": _item_folder_url(dialog_id, checklist_key, item, job),
                "order": next((n for n, i in enumerate(entry["items"]) if clean_cell_value(i.get("id")) == item_id), 10 ** 6),
                "files": [],
            }
        failed = clean_cell_value(job.get("status")) != "synced"
        name = clean_cell_value(job.get("file_name")) or "Файл"
        entry["groups"][item_id]["files"].append({"name": name, "failed": failed})
        files.append({
            "jobId": clean_cell_value(job.get("job_id")),
            "name": name,
            "size": int(job.get("file_size") or 0),
            "status": clean_cell_value(job.get("status")),
            "error": clean_cell_value(job.get("error"))[:300],
        })

    lines = [HEADER]
    for (dialog_id, checklist_key), entry in checklists.items():
        if not entry["groups"]:
            continue
        caption = _checklist_caption(dialog_id, checklist_key)
        link = build_checklist_message_link(dialog_id, checklist_key)
        lines.append("")
        lines.append(f"[URL={link}]{caption}[/URL]" if link else caption)
        for group in sorted(entry["groups"].values(), key=lambda g: g["order"]):
            lines.append("")
            if group["folderUrl"]:
                lines.append(f"{group['label']} — [URL={group['folderUrl']}]Ссылка на папку[/URL]")
            else:
                lines.append(group["label"])
            for file in group["files"]:
                lines.append("• " + file["name"] + (FAILED_MARK if file["failed"] else ""))
    return "\n".join(lines), files


def _claim(session_id: str, user_id: str) -> bool:
    """One message per session, also with several workers at once."""
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT status FROM yandex_upload_notices WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if row:
            conn.commit()
            return False
        now = _now()
        conn.execute(
            """
            INSERT INTO yandex_upload_notices(session_id, user_id, status, created_at, updated_at)
            VALUES (?, ?, 'sending', ?, ?)
            """,
            (session_id, user_id, now, now),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _record(session_id: str, user_id: str, status: str, *, message_id: str = "", error: str = "", files=None) -> None:
    conn = get_conn()
    try:
        now = _now()
        conn.execute(
            """
            INSERT INTO yandex_upload_notices(session_id, user_id, status, message_id, error, files_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                status = excluded.status,
                message_id = excluded.message_id,
                error = excluded.error,
                files_json = excluded.files_json,
                updated_at = excluded.updated_at
            """,
            (session_id, user_id, status, message_id, error[:1000],
             json.dumps(files or [], ensure_ascii=False), now, now),
        )
        conn.commit()
    finally:
        conn.close()


def _result_id(response) -> str:
    result = response.get("result") if isinstance(response, dict) else None
    if type(result) is int and result > 0:
        return str(result)
    if isinstance(result, str) and result.isdecimal() and int(result) > 0:
        return result
    return ""


def _send(user_id: str, message: str) -> tuple[str, str, str, str]:
    """(status, message id, error, method). A timeout or an unknown answer
    is never sent again: the message may have arrived."""
    try:
        response = bitrix_webhook_call("im.message.add", {
            "DIALOG_ID": user_id,
            "MESSAGE": message,
            "SYSTEM": "N",
            "URL_PREVIEW": "N",
        })
    except Exception as exc:
        return "uncertain", "", "Нет подтверждения отправки: " + type(exc).__name__, "im.message.add"
    message_id = _result_id(response)
    if message_id:
        return "sent", message_id, "", "im.message.add"
    if not (isinstance(response, dict) and response.get("error")):
        return "uncertain", "", "Битрикс не подтвердил результат отправки", "im.message.add"
    first_error = f"{response.get('error')}: {clean_cell_value(response.get('error_description'))}"
    # Bitrix24 refused the chat (for example a message of the webhook owner
    # to himself): the same text goes as a personal notification.
    try:
        response = bitrix_webhook_call("im.notify.system.add", {
            "USER_ID": user_id,
            "MESSAGE": message,
        })
    except Exception as exc:
        return "uncertain", "", first_error + "; уведомление: нет подтверждения (" + type(exc).__name__ + ")", "im.notify.system.add"
    message_id = _result_id(response)
    if message_id:
        return "sent", message_id, first_error, "im.notify.system.add"
    if isinstance(response, dict) and response.get("error"):
        return "failed", "", first_error + "; " + f"{response.get('error')}: {clean_cell_value(response.get('error_description'))}", "im.notify.system.add"
    return "uncertain", "", first_error + "; уведомление: Битрикс не подтвердил результат", "im.notify.system.add"


def check_session_yandex_upload_notice(session_id: str, *, source: str = "") -> dict:
    session_id = clean_cell_value(session_id)
    if not session_id:
        return {"status": "no_session"}
    ensure_yandex_upload_notice_tables()

    conn = get_conn()
    try:
        done = conn.execute(
            "SELECT status FROM yandex_upload_notices WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if done:
            return {"status": "already_" + clean_cell_value(done["status"])}
        session = conn.execute(
            "SELECT session_id, user_id, user_name, status FROM edit_sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        jobs = _session_jobs(conn, session_id)
        enabled_at = _enabled_at(conn)
    finally:
        conn.close()

    if not session or clean_cell_value(session["status"]) != "committed" or not jobs:
        return {"status": "not_applicable"}
    pending = [job for job in jobs if clean_cell_value(job.get("status")) not in FINAL_JOB_STATUSES]
    if pending:
        return {"status": "waiting", "pending": len(pending)}
    listed = [job for job in jobs if clean_cell_value(job.get("status")) != "cancelled"]
    big = [job for job in listed if int(job.get("file_size") or 0) >= BIG_FILE_BYTES]
    if not big:
        return {"status": "no_big_file"}
    last_finished = max(clean_cell_value(job.get("finished_at")) for job in jobs)
    if enabled_at and last_finished and last_finished < enabled_at:
        return {"status": "before_feature"}

    user_id = clean_cell_value(session["user_id"])
    if not user_id or not user_id.isdecimal():
        _record(session_id, user_id, "skipped", error="Нет сотрудника Битрикса у сессии")
        return {"status": "skipped"}
    if not _claim(session_id, user_id):
        return {"status": "claimed_elsewhere"}

    try:
        message, files = build_yandex_upload_notice(jobs)
    except Exception as exc:
        _record(session_id, user_id, "failed", error="Не удалось собрать сообщение: " + str(exc))
        write_debug_log("yandex_upload_notice_failed", {"sessionId": session_id, "userId": user_id, "error": str(exc)[:500]})
        return {"status": "failed"}

    status, message_id, error, method = _send(user_id, message)
    _record(session_id, user_id, status, message_id=message_id, error=error, files=files)
    write_debug_log("yandex_upload_notice", {
        "sessionId": session_id,
        "userId": user_id,
        "userName": clean_cell_value(session["user_name"]),
        "source": source,
        "status": status,
        "method": method,
        "messageId": message_id,
        "error": error[:500],
        "webhookOwnerId": _webhook_owner_id(),
        "files": files,
        "bigFiles": [clean_cell_value(job.get("file_name")) for job in big],
    })
    return {"status": status, "messageId": message_id, "method": method}


def check_yandex_upload_notice_for_job(job_id: str, *, source: str = "yandex_job") -> dict:
    job_id = clean_cell_value(job_id)
    if not job_id:
        return {"status": "no_job"}
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT source_session_id FROM upload_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    finally:
        conn.close()
    session_id = clean_cell_value(row["source_session_id"] if row else "")
    if not session_id:
        return {"status": "no_session"}
    return check_session_yandex_upload_notice(session_id, source=source)


def sweep_yandex_upload_notices(limit: int = 20) -> int:
    """Sessions with a big file whose last file has finished and that have
    no message yet (a restart between the last file and the message)."""
    ensure_yandex_upload_notice_tables()
    conn = get_conn()
    try:
        enabled_at = _enabled_at(conn)
        rows = conn.execute(
            """
            SELECT j.source_session_id AS session_id
            FROM upload_jobs j
            LEFT JOIN yandex_upload_notices n ON n.session_id = j.source_session_id
            WHERE j.job_type = 'upload'
              AND COALESCE(j.source_session_id, '') <> ''
              AND n.session_id IS NULL
            GROUP BY j.source_session_id
            HAVING MAX(COALESCE(j.file_size, 0)) >= ?
               AND MAX(COALESCE(j.finished_at, '')) >= ?
               AND SUM(CASE WHEN j.status IN ('synced', 'error', 'cancelled', 'skipped', 'done') THEN 0 ELSE 1 END) = 0
            LIMIT ?
            """,
            (BIG_FILE_BYTES, enabled_at, int(limit)),
        ).fetchall()
    finally:
        conn.close()
    handled = 0
    for row in rows:
        try:
            result = check_session_yandex_upload_notice(row["session_id"], source="sweep")
            if result.get("status") in {"sent", "failed", "uncertain", "skipped"}:
                handled += 1
        except Exception as exc:
            write_debug_log("yandex_upload_notice_sweep_error", {"sessionId": row["session_id"], "error": str(exc)[:500]})
    return handled
