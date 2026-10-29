"""Reminders about the dates of «Анализ состояния объекта».

A personal Bitrix message at 8:30 Khabarovsk time 7, 3 and 1 day before a
date and in the morning of the day itself; a weekly meeting gets one in the
morning of every meeting day. The message names the project and leads to
its OPR checklist.

Every date has its own recipients: «Карман Сергей» until somebody changes
the list in the editor of the date. Only the people of the list get the
reminder.

A reminder whose time had passed when the date was set is not sent; one
the application missed is sent later the same day, never after the
meeting began. A changed or cleared date, or a recipient taken off the
list, drops the reminders not sent yet.

DATE_REMINDERS_ENABLED=0 stops the sending (the test copy of the
application).
"""
from __future__ import annotations

import os
import threading
import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from app.bitrix.client import bitrix_webhook_call
from app.db import get_conn
from app.logging_utils import write_debug_log
from app.checklists.utils import clean_cell_value


# Хабаровск, UTC+10 all year.
TZ = timezone(timedelta(hours=10))
REMIND_AT = time(8, 30)
DAYS_BEFORE = (7, 3, 1, 0)
DEFAULT_RECIPIENTS = (("256", "Карман Сергей"),)
MAX_RECIPIENTS = 30

DATE_FIELDS = ("oprCustomerDate", "oprPdDate")
MEETING_FIELDS = ("meeting", "vks")
FIELDS = DATE_FIELDS + MEETING_FIELDS
SUBJECTS = {
    "oprCustomerDate": "дата передачи ОПР Заказчику",
    "oprPdDate": "дата передачи ОПР в стадию ПД",
    "meeting": "совещание по объекту (внутреннее)",
    "vks": "ВКС с Заказчиком",
}
WEEKLY_SUBJECTS = {
    "meeting": "еженедельное совещание по объекту (внутреннее)",
    "vks": "еженедельная ВКС с Заказчиком",
}
WHEN = {7: "Через 7 дней", 3: "Через 3 дня", 1: "Завтра", 0: "Сегодня"}

PASS_SECONDS = 30
HISTORY_LIMIT = 3

_STOP = threading.Event()
_THREAD = None
_THREAD_LOCK = threading.Lock()
_SCHEMA_READY = False


class RecipientError(ValueError):
    """A recipient the list cannot take; the message is for the user."""


def enabled() -> bool:
    return clean_cell_value(os.getenv("DATE_REMINDERS_ENABLED", "1")).lower() not in {"0", "false", "off", "no"}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _parse_moment(value: str) -> datetime | None:
    try:
        moment = datetime.fromisoformat(clean_cell_value(value))
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def ensure_schema(conn=None) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    own = conn is None
    conn = conn or get_conn()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS date_reminder_slots (
            base_dialog_id TEXT NOT NULL, field TEXT NOT NULL, slot_key TEXT NOT NULL,
            slot_at TEXT NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY(base_dialog_id, field, slot_key))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS date_reminder_deliveries (
            delivery_id TEXT PRIMARY KEY, base_dialog_id TEXT NOT NULL, field TEXT NOT NULL,
            slot_key TEXT NOT NULL, slot_at TEXT NOT NULL, value_key TEXT NOT NULL,
            user_id TEXT NOT NULL, user_name TEXT NOT NULL DEFAULT '', message TEXT NOT NULL,
            status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            retry_at TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL,
            message_id TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '',
            UNIQUE(base_dialog_id, field, slot_key, user_id))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_date_reminder_queue ON date_reminder_deliveries(status, retry_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_date_reminder_project ON date_reminder_deliveries(base_dialog_id, field)")
        conn.commit()
        _SCHEMA_READY = True
    finally:
        if own:
            conn.close()


# ---------------------------------------------------------------------------
# Recipients and the meta of a date
# ---------------------------------------------------------------------------

def default_recipients() -> list[dict]:
    from app.checklists.bitrix_users import get_cached_bitrix_user

    result = []
    for user_id, name in DEFAULT_RECIPIENTS:
        try:
            cached = get_cached_bitrix_user(user_id) or {}
        except Exception:
            cached = {}
        result.append({"userId": user_id, "name": clean_cell_value(cached.get("name")) or name})
    return result


def _recipient_list(value: Any) -> list[dict]:
    result: list[dict] = []
    for entry in value if isinstance(value, list) else []:
        if isinstance(entry, dict):
            user_id = clean_cell_value(entry.get("userId"))
            name = " ".join(str(entry.get("name") or "").split())[:120]
        else:
            user_id, name = clean_cell_value(entry), ""
        if not user_id.isdecimal() or int(user_id) <= 0 or any(r["userId"] == user_id for r in result):
            continue
        result.append({"userId": user_id, "name": name or "ID " + user_id})
    return result[:MAX_RECIPIENTS]


def clean_meta(raw: Any) -> dict:
    """recipients None: the list was never changed, the default goes."""
    raw = raw if isinstance(raw, dict) else {}
    return {
        "recipients": _recipient_list(raw["recipients"]) if isinstance(raw.get("recipients"), list) else None,
        "setById": clean_cell_value(raw.get("setById")),
        "setByName": clean_cell_value(raw.get("setByName")),
        "setAt": clean_cell_value(raw.get("setAt")) if _parse_moment(raw.get("setAt") or "") else "",
    }


def recipients_of(meta: dict, defaults: list[dict] | None = None) -> list[dict]:
    recipients = (meta or {}).get("recipients")
    if recipients is not None:
        return list(recipients)
    return list(defaults) if defaults is not None else default_recipients()


def validated_recipients(value: Any, previous: list[dict]) -> list[dict]:
    """Active employees from the Bitrix list; one already on the list or the
    default one stays even when the cache does not know him."""
    from app.checklists.bitrix_users import get_cached_bitrix_user

    if not isinstance(value, list):
        raise RecipientError("Неверный список получателей")
    if len(value) > MAX_RECIPIENTS:
        raise RecipientError(f"Не больше {MAX_RECIPIENTS} получателей")
    known = {entry["userId"]: entry["name"] for entry in previous}
    known.update({user_id: name for user_id, name in DEFAULT_RECIPIENTS if user_id not in known})
    result: list[dict] = []
    for raw in value:
        entry = (_recipient_list([raw]) or [None])[0]
        if entry is None:
            raise RecipientError("Выберите сотрудника из списка Битрикса")
        user_id = entry["userId"]
        if any(chosen["userId"] == user_id for chosen in result):
            continue
        cached = get_cached_bitrix_user(user_id)
        if cached and cached.get("active"):
            result.append({"userId": user_id, "name": clean_cell_value(cached.get("name")) or entry["name"]})
        elif user_id in known and not cached:
            result.append({"userId": user_id, "name": known[user_id]})
        else:
            raise RecipientError("Сотрудник недоступен. Выберите его из списка Битрикса: " + entry["name"])
    return result


def value_key(field: str, value: Any) -> str:
    """What the reminders of a date belong to: a new key drops the old ones."""
    if field in MEETING_FIELDS:
        value = value if isinstance(value, dict) else {}
        at = clean_cell_value(value.get("at"))
        return (at + ("|weekly" if value.get("weekly") else "")) if at else ""
    return clean_cell_value(value)


def _event(field: str, value: Any) -> tuple[date, time | None, bool] | None:
    try:
        if field in MEETING_FIELDS:
            value = value if isinstance(value, dict) else {}
            at = datetime.fromisoformat(clean_cell_value(value.get("at")))
            return at.date(), at.time().replace(second=0, microsecond=0), bool(value.get("weekly"))
        return date.fromisoformat(clean_cell_value(value)), None, False
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Schedule and the message
# ---------------------------------------------------------------------------

def due_slots(field: str, value: Any, set_at: str, now: datetime) -> list[dict]:
    """The reminder of this date due at `now`: only today's 8:30 can be."""
    event = _event(field, value)
    if event is None:
        return []
    event_day, event_time, weekly = event
    local_now = now.astimezone(TZ)
    today = local_now.date()
    slot = datetime.combine(today, REMIND_AT, TZ)
    if local_now < slot:
        return []
    set_moment = _parse_moment(set_at) if set_at else None
    if set_moment is not None and slot < set_moment:
        return []
    if weekly:
        if today < event_day or (today - event_day).days % 7:
            return []
        days, occurrence = 0, today
    else:
        days, occurrence = (event_day - today).days, event_day
        if days not in DAYS_BEFORE:
            return []
    if days == 0 and event_time is not None and local_now >= datetime.combine(today, event_time, TZ):
        return []
    return [{
        "slotKey": f"{value_key(field, value)}|{occurrence.isoformat()}|{days}",
        "slotAt": iso(slot),
        "daysBefore": days,
        "occurrence": occurrence,
        "time": event_time,
        "weekly": weekly,
    }]


def _plain(value: Any) -> str:
    # Names are user input: no BBCode links or mentions in the message.
    return " ".join(str(value or "").replace("[", "［").replace("]", "］").split())


def build_message(field: str, slot: dict, *, project: str, link: str, set_by_name: str) -> str:
    from app.checklists.messages import build_message_value_link

    when = WHEN[slot["daysBefore"]] + ", " + slot["occurrence"].strftime("%d.%m.%Y")
    if slot["time"] is not None:
        when += " в " + slot["time"].strftime("%H:%M")
    subject = WEEKLY_SUBJECTS[field] if slot["weekly"] else SUBJECTS[field]
    name = build_message_value_link(_plain(project), link)
    lines = ["⏰ " + when + ": " + subject + " по объекту «[B]" + name + "[/B]»."]
    if clean_cell_value(set_by_name):
        lines.append("Дату поставил: " + _plain(set_by_name))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Queue
# ---------------------------------------------------------------------------

def _records() -> dict[str, dict]:
    from app.checklists import object_analysis

    conn = object_analysis._conn()
    try:
        return object_analysis._read_records(conn)
    finally:
        conn.close()


def _slot_made(base: str, field: str, slot: dict) -> bool:
    conn = get_conn()
    try:
        ensure_schema(conn)
        return conn.execute(
            "SELECT 1 FROM date_reminder_slots WHERE base_dialog_id=? AND field=? AND slot_key=?",
            (base, field, slot["slotKey"]),
        ).fetchone() is not None
    finally:
        conn.close()


def collect_due(now: datetime | None = None) -> int:
    """Today's reminders become deliveries once per date, day and recipient."""
    from app.checklists import object_analysis

    if not enabled():
        return 0
    now = now or now_utc()
    stamp = iso(now)
    count = 0
    for base, record in sorted(_records().items()):
        for field in FIELDS:
            value = record.get(field)
            key = value_key(field, value)
            if not key:
                continue
            meta = clean_meta((record.get("reminders") or {}).get(field))
            slots = [slot for slot in due_slots(field, value, meta["setAt"], now) if not _slot_made(base, field, slot)]
            if not slots:
                continue
            recipients = recipients_of(meta)
            project = object_analysis._project_name(base, "")
            link = object_analysis.checklist_link(base, "opr")
            conn = get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                for slot in slots:
                    created = conn.execute(
                        "INSERT OR IGNORE INTO date_reminder_slots VALUES(?,?,?,?,?)",
                        (base, field, slot["slotKey"], slot["slotAt"], stamp),
                    ).rowcount
                    if not created:
                        continue
                    message = build_message(field, slot, project=project, link=link, set_by_name=meta["setByName"])
                    for user in recipients:
                        conn.execute(
                            """INSERT OR IGNORE INTO date_reminder_deliveries
                               (delivery_id, base_dialog_id, field, slot_key, slot_at, value_key,
                                user_id, user_name, message, status, updated_at)
                               VALUES(?,?,?,?,?,?,?,?,?,'pending',?)""",
                            (uuid.uuid4().hex, base, field, slot["slotKey"], slot["slotAt"], key,
                             user["userId"], user["name"], message, stamp),
                        )
                        count += 1
                    write_debug_log("date_reminder_due", {
                        "dialogId": base, "field": field, "slotKey": slot["slotKey"],
                        "recipients": [user["userId"] for user in recipients],
                    })
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
    return count


def _still_wanted(conn, record: dict) -> bool:
    """The date and the recipient as when the reminder was made."""
    from app.checklists import object_analysis

    current = object_analysis._read_record(conn, record["base_dialog_id"])
    field = record["field"]
    if value_key(field, current.get(field)) != record["value_key"]:
        return False
    meta = clean_meta((current.get("reminders") or {}).get(field))
    return any(user["userId"] == record["user_id"] for user in recipients_of(meta))


def deliver_one(now: datetime | None = None) -> bool:
    from app.checklists.bitrix_users import get_cached_bitrix_user

    if not enabled():
        return False
    now = now or now_utc()
    stamp = iso(now)
    conn = get_conn()
    try:
        ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        # A process that died during the HTTP call left an unknown outcome:
        # never send it again.
        conn.execute("""UPDATE date_reminder_deliveries SET status='uncertain', error=?, updated_at=?
                        WHERE status='sending' AND updated_at<?""",
                     ("Результат отправки не подтверждён. Автоповтор остановлен.", stamp,
                      iso(now - timedelta(minutes=5))))
        conn.execute("UPDATE date_reminder_deliveries SET status='expired', updated_at=? WHERE status='pending' AND slot_at<?",
                     (stamp, iso(now - timedelta(days=1))))
        while True:
            row = conn.execute("""SELECT * FROM date_reminder_deliveries WHERE status='pending' AND retry_at<=?
                                  ORDER BY slot_at, delivery_id LIMIT 1""", (stamp,)).fetchone()
            if not row:
                conn.commit()
                return False
            record = dict(row)
            if _still_wanted(conn, record):
                break
            conn.execute("UPDATE date_reminder_deliveries SET status='cancelled', updated_at=? WHERE delivery_id=?",
                         (stamp, record["delivery_id"]))
        conn.execute("UPDATE date_reminder_deliveries SET status='sending', attempts=attempts+1, updated_at=? WHERE delivery_id=?",
                     (stamp, record["delivery_id"]))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    status, message_id, error, retry_at = "failed", "", "", ""
    user = get_cached_bitrix_user(record["user_id"])
    try:
        if user and not user.get("active"):
            error = "Получатель больше не является активным сотрудником Битрикса"
        else:
            response = bitrix_webhook_call("im.message.add", {
                "DIALOG_ID": record["user_id"], "MESSAGE": record["message"],
                "SYSTEM": "N", "URL_PREVIEW": "N",
            })
            result = response.get("result") if isinstance(response, dict) else None
            if type(result) is int and result > 0:
                status, message_id = "sent", str(result)
            elif isinstance(result, str) and result.isdecimal() and int(result) > 0:
                status, message_id = "sent", result
            elif isinstance(response, dict) and response.get("error"):
                code = str(response["error"])
                error = code + ": " + str(response.get("error_description") or "")
                # Only a refusal for the rate proves nothing was sent.
                if code == "QUERY_LIMIT_EXCEEDED" and record["attempts"] < 2:
                    status, retry_at = "pending", iso(now + timedelta(seconds=60))
            else:
                status, error = "uncertain", "Битрикс не подтвердил результат отправки"
    except Exception as exc:
        status, error = "uncertain", "Нет подтверждения отправки: " + type(exc).__name__
    conn = get_conn()
    try:
        conn.execute("""UPDATE date_reminder_deliveries SET status=?, message_id=?, error=?, retry_at=?, updated_at=?
                        WHERE delivery_id=? AND status='sending'""",
                     (status, message_id, error[:1000], retry_at, stamp, record["delivery_id"]))
        conn.commit()
    finally:
        conn.close()
    write_debug_log("date_reminder_delivery", {
        "dialogId": record["base_dialog_id"], "field": record["field"], "slotKey": record["slot_key"],
        "userId": record["user_id"], "status": status, "messageId": message_id, "error": error[:500],
    })
    return True


def history(base: str) -> dict[str, list[dict]]:
    """The last reminders of every date of a project, for its editor."""
    result: dict[str, list[dict]] = {field: [] for field in FIELDS}
    conn = get_conn()
    try:
        ensure_schema(conn)
        rows = conn.execute(
            """SELECT field, slot_at, user_name, status, error FROM date_reminder_deliveries
               WHERE base_dialog_id=? AND status <> 'cancelled' ORDER BY slot_at DESC, user_name LIMIT 40""",
            (base,),
        ).fetchall()
    finally:
        conn.close()
    for row in rows:
        entries = result.get(row["field"])
        if entries is not None and len(entries) < HISTORY_LIMIT:
            entries.append({
                "at": row["slot_at"], "userName": row["user_name"],
                "status": row["status"], "error": row["error"][:200],
            })
    return result


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def _worker() -> None:
    while not _STOP.is_set():
        try:
            collect_due()
            for _ in range(50):
                if _STOP.is_set() or not deliver_one():
                    break
        except Exception as exc:
            write_debug_log("date_reminder_worker_error", {"error": str(exc)[:500]})
        _STOP.wait(PASS_SECONDS)


def start_worker() -> None:
    global _THREAD
    with _THREAD_LOCK:
        if _THREAD and _THREAD.is_alive():
            return
        ensure_schema()
        if not enabled():
            write_debug_log("date_reminders_disabled", {})
            return
        _STOP.clear()
        _THREAD = threading.Thread(target=_worker, name="date-reminders", daemon=True)
        _THREAD.start()


def stop_worker() -> None:
    _STOP.set()
