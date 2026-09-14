"""Persistent ID reminders: session drafts, clock slots and per-recipient delivery."""
from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timedelta, timezone

from app.db import get_conn
from app.bitrix.client import bitrix_webhook_call
from app.logging_utils import write_debug_log
from app.checklists.bitrix_users import get_cached_bitrix_user
from app.checklists.documents import migrate_legacy_document_fields, normalize_documents_list
from app.checklists.config import get_checklist_config
from app.checklists.utils import clean_cell_value, normalize_dialog_id

DEFAULT_CONFIG = {"enabled": False, "days": list(range(7)), "hour": 9,
                  "minute": 0, "timezoneOffset": 600, "recipients": []}
_STOP = threading.Event()
_THREAD = None
_THREAD_LOCK = threading.Lock()


def now_utc():
    return datetime.now(timezone.utc)


def iso(value):
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def ensure_schema(conn=None):
    own = conn is None
    conn = conn or get_conn()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS id_reminder_settings (
            dialog_id TEXT PRIMARY KEY, config_json TEXT NOT NULL,
            revision TEXT NOT NULL, updated_at TEXT NOT NULL, updated_by TEXT NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS id_reminder_drafts (
            session_id TEXT NOT NULL, dialog_id TEXT NOT NULL, config_json TEXT NOT NULL,
            updated_by TEXT NOT NULL, PRIMARY KEY(session_id, dialog_id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS id_reminder_slots (
            dialog_id TEXT NOT NULL, slot_at TEXT NOT NULL, revision TEXT NOT NULL,
            status TEXT NOT NULL, PRIMARY KEY(dialog_id, slot_at))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS id_reminder_deliveries (
            delivery_id TEXT PRIMARY KEY, dialog_id TEXT NOT NULL, slot_at TEXT NOT NULL,
            revision TEXT NOT NULL, user_id TEXT NOT NULL, message TEXT NOT NULL,
            status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            retry_at TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL,
            message_id TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '',
            UNIQUE(dialog_id, slot_at, user_id))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_id_reminder_queue ON id_reminder_deliveries(status,retry_at)")
        if own:
            conn.commit()
    finally:
        if own:
            conn.close()


def normalize_config(raw):
    if not isinstance(raw, dict) or type(raw.get("enabled")) is not bool:
        raise ValueError("Укажите состояние оповещений")
    def integer(name, lo, hi):
        value = raw.get(name, DEFAULT_CONFIG[name])
        if type(value) is not int or not lo <= value <= hi:
            raise ValueError("Недопустимое значение: " + name)
        return value
    days = raw.get("days", [])
    if not isinstance(days, list) or not days or any(type(v) is not int or v not in range(7) for v in days):
        raise ValueError("Выберите хотя бы один день недели")
    recipients = raw.get("recipients", [])
    if not isinstance(recipients, list) or len(recipients) > 50:
        raise ValueError("Выберите не более 50 сотрудников")
    resolved = {}
    for value in recipients:
        user_id = str(value.get("userId") if isinstance(value, dict) else value).strip()
        if not user_id.isdecimal() or int(user_id) <= 0:
            raise ValueError("Выберите сотрудника из списка Битрикса")
        user = get_cached_bitrix_user(user_id)
        if not user or not user.get("active"):
            if raw["enabled"]:
                raise ValueError("Сотрудник недоступен. Обновите список Битрикса: " + user_id)
            continue
        resolved[user_id] = {"userId": user_id, "name": user["name"]}
    if raw["enabled"] and not resolved:
        raise ValueError("Выберите хотя бы одного получателя")
    return {"enabled": raw["enabled"], "days": sorted(set(days)),
            "hour": integer("hour", 0, 23), "minute": integer("minute", 0, 59),
            "timezoneOffset": integer("timezoneOffset", -720, 840),
            "recipients": list(resolved.values())}


def get_settings(dialog_id, session_id=""):
    conn = get_conn()
    try:
        draft = conn.execute("SELECT config_json FROM id_reminder_drafts WHERE dialog_id=? AND session_id=?",
                             (dialog_id, session_id)).fetchone()
        row = conn.execute("SELECT * FROM id_reminder_settings WHERE dialog_id=?", (dialog_id,)).fetchone()
        history = conn.execute("""SELECT slot_at,user_id,status,message_id,error FROM id_reminder_deliveries
                                WHERE dialog_id=? ORDER BY updated_at DESC LIMIT 20""", (dialog_id,)).fetchall()
        return {"config": json.loads((draft or row)["config_json"]) if draft or row else dict(DEFAULT_CONFIG),
                "pending": bool(draft), "deliveries": [dict(r) for r in history]}
    finally:
        conn.close()


def save_draft(*, dialog_id, session_id, user_id, config):
    normalized = normalize_config(config)
    from app.checklists.edit_session_changes import acquire_checklist_for_edit_session
    from app.checklists.edit_sessions import EditSessionConflictError
    acquire_checklist_for_edit_session(session_id=session_id, dialog_id=dialog_id,
                                      checklist_key="id", user_id=user_id)
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        # Commit/Cancel may race a request after its initial session check.
        row = conn.execute("SELECT status FROM edit_sessions WHERE session_id=?", (session_id,)).fetchone()
        if not row or row["status"] != "active":
            raise EditSessionConflictError("Сеанс уже завершён")
        conn.execute("""INSERT INTO id_reminder_drafts VALUES(?,?,?,?)
                        ON CONFLICT(session_id,dialog_id) DO UPDATE SET
                        config_json=excluded.config_json,updated_by=excluded.updated_by""",
                     (session_id, dialog_id, json.dumps(normalized, ensure_ascii=False), user_id))
        conn.commit()
    finally:
        conn.close()
    return get_settings(dialog_id, session_id)


def finalize_drafts(conn, session_id, *, commit, now):
    ensure_schema(conn)
    if commit:
        for row in conn.execute("SELECT * FROM id_reminder_drafts WHERE session_id=?", (session_id,)).fetchall():
            conn.execute("""INSERT INTO id_reminder_settings VALUES(?,?,?,?,?)
                            ON CONFLICT(dialog_id) DO UPDATE SET config_json=excluded.config_json,
                            revision=excluded.revision,updated_at=excluded.updated_at,updated_by=excluded.updated_by""",
                         (row["dialog_id"], row["config_json"], uuid.uuid4().hex, now, row["updated_by"]))
            conn.execute("UPDATE id_reminder_deliveries SET status='cancelled',updated_at=? WHERE dialog_id=? AND status='pending'",
                         (now, row["dialog_id"]))
    conn.execute("DELETE FROM id_reminder_drafts WHERE session_id=?", (session_id,))


def due_slot(config, now, updated_at):
    """Coalesce downtime into the latest due slot within 24 hours."""
    local = now.astimezone(timezone(timedelta(minutes=config["timezoneOffset"])))
    for delta in (0, 1):
        day = local - timedelta(days=delta)
        slot = day.replace(hour=config["hour"], minute=config["minute"], second=0, microsecond=0)
        if day.weekday() not in config["days"] or not timedelta(0) <= local - slot < timedelta(days=1):
            continue
        value = iso(slot)
        if value >= updated_at:
            return value
    return None


def empty_items(data):
    nr = get_checklist_config("id").not_required_group_id
    result = []
    for raw in data.get("items", []):
        item = migrate_legacy_document_fields(dict(raw))
        if int(item.get("group") or 0) == nr or item.get("isNotRequired"):
            continue
        if normalize_documents_list(item.get("documents")):
            continue  # A sync error is not an absent document; archive alone is.
        name = clean_cell_value(item.get("name"))
        if name:
            result.append(name)
    return result


def plain_message(project, names):
    # Names are untrusted user input; prevent BBCode links/mentions in IM.
    def plain(s):
        return " ".join(str(s).replace("[", "［").replace("]", "］").split())
    return "Исходные данные по " + plain(project) + " требуют дополнения по следующим пунктам:\n" + "\n".join(
        f"{i}-{plain(name)}" for i, name in enumerate(names, 1))


def collect_due(now=None):
    now = now or now_utc()
    stamp = iso(now)
    conn = get_conn()
    count = 0
    try:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute("SELECT * FROM id_reminder_settings ORDER BY dialog_id").fetchall()
        for row in rows:
            config = json.loads(row["config_json"])
            if not config["enabled"]:
                continue
            slot = due_slot(config, now, row["updated_at"])
            if not slot:
                continue
            # Never notify about uncommitted uploads/removals or Cancel drafts.
            editing = conn.execute("""SELECT 1 FROM edit_session_checklists c JOIN edit_sessions s
                ON s.session_id=c.session_id WHERE c.dialog_id=? AND c.checklist_key='id'
                AND s.status IN ('active','committing','rolling_back','error') LIMIT 1""", (row["dialog_id"],)).fetchone()
            if editing:
                continue
            data_row = conn.execute("SELECT data_json,title FROM checklists WHERE dialog_id=?",
                                    (row["dialog_id"],)).fetchone()
            if not data_row:
                data_row = conn.execute("SELECT data_json,title FROM checklists WHERE dialog_id=?",
                                        (row["dialog_id"] + "::id",)).fetchone()
            if not data_row:
                continue
            names = empty_items(json.loads(data_row["data_json"]))
            created = conn.execute("INSERT OR IGNORE INTO id_reminder_slots VALUES(?,?,?,?)",
                                   (row["dialog_id"], slot, row["revision"], "pending" if names else "empty")).rowcount
            if not created or not names:
                continue
            context = conn.execute("SELECT project_name FROM project_storage_contexts WHERE dialog_id=?",
                                   (row["dialog_id"],)).fetchone()
            project = context["project_name"] if context and context["project_name"] else data_row["title"] or row["dialog_id"]
            message = plain_message(project, names)
            for user in config["recipients"]:
                conn.execute("""INSERT OR IGNORE INTO id_reminder_deliveries
                    (delivery_id,dialog_id,slot_at,revision,user_id,message,status,updated_at)
                    VALUES(?,?,?,?,?,?,'pending',?)""",
                    (uuid.uuid4().hex, row["dialog_id"], slot, row["revision"], user["userId"], message, stamp))
                count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def deliver_one(now=None):
    now = now or now_utc()
    stamp = iso(now)
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        # A process dying during HTTP has an unknown outcome, not permission
        # to send again. This also works with multiple application processes.
        conn.execute("""UPDATE id_reminder_deliveries SET status='uncertain',error=?,updated_at=?
                        WHERE status='sending' AND updated_at<?""",
                     ("Результат отправки не подтверждён. Автоповтор остановлен.", stamp, iso(now-timedelta(minutes=5))))
        conn.execute("UPDATE id_reminder_deliveries SET status='expired',updated_at=? WHERE status='pending' AND slot_at<?",
                     (stamp, iso(now-timedelta(days=1))))
        row = conn.execute("""SELECT d.* FROM id_reminder_deliveries d JOIN id_reminder_settings s
            ON d.dialog_id=s.dialog_id AND d.revision=s.revision
            WHERE d.status='pending' AND d.retry_at<=? ORDER BY d.slot_at,d.delivery_id LIMIT 1""", (stamp,)).fetchone()
        if not row:
            conn.commit()
            return False
        record = dict(row)
        conn.execute("UPDATE id_reminder_deliveries SET status='sending',attempts=attempts+1,updated_at=? WHERE delivery_id=?",
                     (stamp, record["delivery_id"]))
        conn.commit()
    finally:
        conn.close()
    status, message_id, error, retry_at = "failed", "", "", ""
    user = get_cached_bitrix_user(record["user_id"])
    try:
        if not user or not user.get("active"):
            error = "Получатель больше не является активным сотрудником Битрикса"
        else:
            response = bitrix_webhook_call("im.message.add", {"DIALOG_ID": record["user_id"],
                                          "MESSAGE": record["message"], "SYSTEM": "N", "URL_PREVIEW": "N"})
            result = response.get("result") if isinstance(response, dict) else None
            if type(result) is int and result > 0:
                status, message_id = "sent", str(result)
            elif isinstance(result, str) and result.isdecimal() and int(result) > 0:
                status, message_id = "sent", result
            elif isinstance(response, dict) and response.get("error"):
                code = str(response["error"])
                error = code + ": " + str(response.get("error_description") or "")
                # Only explicit rate-limit rejection proves that no message
                # was accepted. Never replay a timeout or unknown 5xx outcome.
                if code == "QUERY_LIMIT_EXCEEDED" and record["attempts"] < 2:
                    status, retry_at = "pending", iso(now + timedelta(seconds=60))
            else:
                status, error = "uncertain", "Битрикс не подтвердил результат отправки"
    except Exception as exc:
        status, error = "uncertain", "Нет подтверждения отправки: " + type(exc).__name__
    conn = get_conn()
    try:
        conn.execute("""UPDATE id_reminder_deliveries SET status=?,message_id=?,error=?,retry_at=?,updated_at=?
                        WHERE delivery_id=? AND status='sending'""",
                     (status, message_id, error[:1000], retry_at, stamp, record["delivery_id"]))
        conn.commit()
    finally:
        conn.close()
    write_debug_log("id_reminder_delivery", {"dialogId": record["dialog_id"], "slotAt": record["slot_at"],
                    "userId": record["user_id"], "status": status, "messageId": message_id, "error": error[:500]})
    return True


def _worker():
    while not _STOP.is_set():
        try:
            collect_due()
            # Bound a pass; yield promptly on shutdown and avoid starving scans.
            for _ in range(50):
                if _STOP.is_set() or not deliver_one():
                    break
        except Exception as exc:
            write_debug_log("id_reminder_worker_error", {"error": str(exc)[:500]})
        _STOP.wait(15)


def start_worker():
    global _THREAD
    with _THREAD_LOCK:
        if _THREAD and _THREAD.is_alive():
            return
        ensure_schema()
        _STOP.clear()
        _THREAD = threading.Thread(target=_worker, name="id-reminders", daemon=True)
        _THREAD.start()


def stop_worker():
    _STOP.set()
