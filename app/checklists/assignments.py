"""Исполнители пунктов и разделов чек-листов и их задачи в Битриксе.

Who assigns: leaders, administrators and the GIPs of the project in every
checklist, the «Главный Концептолог» in Концепция, the «Главный Дизайнер» in
Дизайн. An assignee hands his own items over to others («Передать»). Only
employees of Bitrix are assigned; an item in «Не требуется» is not. ГА has
its architects instead (grad_analysis).

An item keeps its own list of assignees; a subitem without its own list has
the assignees of its item. A section has its assignees too: one task per
person with the items of the section at the moment he was assigned.

An assignment is a part of the edit session of the window: it goes to a
draft, «Сохранить» / «Сохранить и закрыть» commit it with the session,
«Отмена» drops it. The commit writes the journal of the item (who assigned
whom and when, the handovers, who set the statuses, who answers for the
item after every change) and queues the synchronization with Bitrix, run
after the commit by the delivery of the session.

The tasks: a task per person and item (or section), created by the webhook;
its creator is then made the one who assigned, so a finished task waits for
his check. «Проверить после завершения» is on, the group of the project is
set, the deadline when it was given. The first task of a person in a
checklist is his main task, the next ones are its subtasks while it is not
closed. The subitems of an item a person answers for are lines of the
checklist of his task; a subitem gets a task of its own only when the
person is not among the assignees of its item.

Only the status set by hand in the checklist moves a task: «В работе» →
«Выполняется», «Есть» → «Ждёт контроля» (all tasks of the item), «Нет» →
«Ждёт выполнения»; a status set back reopens a task already accepted. A
removed assignee, a deleted item and an item in «Не требуется» close the
task. A handover makes the first new person the responsible of the task.
A task of a section goes to control when all its items are «Есть».

Every call to Bitrix is in the debug log as «assign_bitrix_call» with the
method, what it is for, the answer or the error; every decision of the
synchronization as «assign_sync_*»: grep '"assign_' in close_popup.log.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time as _time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import requests

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.settings import BITRIX_TECH_WEBHOOK_URL
from app.checklists.utils import clean_cell_value, normalize_checklist_key, normalize_dialog_id


ITEM, GROUP = "item", "group"
ASSIGNABLE_KEYS = frozenset({"id", "concept", "opr", "p", "r", "design"})
GIP_KEYS = ("id", "opr", "p", "r")
MAX_USERS = 20
MAX_NAME = 300
STATUS_YES, STATUS_WORK, STATUS_NO, STATUS_NR = "Есть", "В работе", "Нет", "Не требуется"
STARTED = (STATUS_YES, STATUS_WORK)
PROJECT_TIMEZONE = timezone(timedelta(hours=10))
JOURNAL_LIMIT = 500

# Bitrix task statuses.
T_NEW, T_PENDING, T_PROGRESS, T_CONTROL, T_DONE, T_DEFERRED = "1", "2", "3", "4", "5", "6"
TASK_STATUS_LABELS = {
    T_NEW: "Новая", T_PENDING: "Ждёт выполнения", T_PROGRESS: "Выполняется",
    T_CONTROL: "Ждёт контроля", T_DONE: "Завершена", T_DEFERRED: "Отложена", "7": "Отклонена",
}
# What the checklist wants of a task.
S_PENDING, S_PROGRESS, S_CONTROL, S_CLOSED = "pending", "progress", "control", "closed"

SYNC_ATTEMPTS = 5
SYNC_RETRY_SECONDS = (60, 300, 900, 3600, 3600)
WORKER_PASS_SECONDS = 60

_WRITE_LOCK = threading.RLock()
_SYNC_LOCKS_GUARD = threading.Lock()
_SYNC_LOCKS: dict[tuple[str, str], threading.Lock] = {}
_STOP = threading.Event()
_THREAD: threading.Thread | None = None
_THREAD_LOCK = threading.Lock()


class AssignmentError(RuntimeError):
    """A refused change; the message is for the user."""


class AssignmentDenied(AssignmentError):
    pass


def tasks_enabled() -> bool:
    return clean_cell_value(os.getenv("ASSIGNMENT_TASKS_ENABLED", "1")).lower() not in {"0", "false", "off", "no"}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _text(value, limit: int = MAX_NAME) -> str:
    return clean_cell_value(value)[:limit]


def _status(value) -> str:
    from app.checklists.normalization import normalize_status

    return normalize_status(value)


def _json(value, default):
    try:
        result = json.loads(value or "")
    except Exception:
        return default
    return result if isinstance(result, type(default)) else default


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def _ensure_tables(conn) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS assignment_targets (
        dialog_id TEXT NOT NULL, checklist_key TEXT NOT NULL,
        target_kind TEXT NOT NULL, target_id TEXT NOT NULL,
        users_json TEXT NOT NULL DEFAULT '[]',
        updated_at TEXT, updated_by TEXT,
        PRIMARY KEY (dialog_id, checklist_key, target_kind, target_id))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS assignment_drafts (
        session_id TEXT NOT NULL, dialog_id TEXT NOT NULL, checklist_key TEXT NOT NULL,
        data_json TEXT NOT NULL, user_id TEXT, user_name TEXT, updated_at TEXT,
        PRIMARY KEY (session_id, dialog_id, checklist_key))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS assignment_journal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dialog_id TEXT NOT NULL, checklist_key TEXT NOT NULL,
        target_kind TEXT NOT NULL, target_id TEXT NOT NULL,
        at TEXT, kind TEXT, actor_id TEXT, actor_name TEXT, session_id TEXT,
        data_json TEXT)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS assignment_journal_target
        ON assignment_journal(dialog_id, checklist_key, target_kind, target_id, id)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS assignment_journal_session
        ON assignment_journal(session_id)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS assignment_item_status (
        dialog_id TEXT NOT NULL, checklist_key TEXT NOT NULL, item_id TEXT NOT NULL,
        status TEXT, updated_at TEXT,
        PRIMARY KEY (dialog_id, checklist_key, item_id))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS assignment_tasks (
        dialog_id TEXT NOT NULL, checklist_key TEXT NOT NULL,
        target_kind TEXT NOT NULL, target_id TEXT NOT NULL, user_id TEXT NOT NULL,
        task_id TEXT DEFAULT '', parent_task_id TEXT DEFAULT '',
        responsible_id TEXT DEFAULT '', creator_id TEXT DEFAULT '', deadline TEXT DEFAULT '',
        applied_state TEXT DEFAULT '', lines_json TEXT DEFAULT '{}', error TEXT DEFAULT '',
        created_at TEXT, updated_at TEXT,
        PRIMARY KEY (dialog_id, checklist_key, target_kind, target_id, user_id))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS assignment_main_tasks (
        dialog_id TEXT NOT NULL, checklist_key TEXT NOT NULL, user_id TEXT NOT NULL,
        task_id TEXT, updated_at TEXT,
        PRIMARY KEY (dialog_id, checklist_key, user_id))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS assignment_sync_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dialog_id TEXT NOT NULL, checklist_key TEXT NOT NULL, session_id TEXT,
        status TEXT DEFAULT 'pending', attempts INTEGER DEFAULT 0, error TEXT DEFAULT '',
        next_at TEXT DEFAULT '', created_at TEXT, updated_at TEXT)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS assignment_sync_queue_status
        ON assignment_sync_queue(status, next_at)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS assignment_messages (
        session_id TEXT PRIMARY KEY, status TEXT, message_id TEXT, error TEXT,
        created_at TEXT, updated_at TEXT)""")


_SCHEMA_READY = False


def ensure_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    conn = get_conn()
    try:
        _ensure_tables(conn)
        conn.commit()
        _SCHEMA_READY = True
    finally:
        conn.close()


def _conn():
    ensure_schema()
    return get_conn()


def _clean_user(raw, *, keep_items: bool = False) -> dict | None:
    if not isinstance(raw, dict):
        return None
    user_id = clean_cell_value(raw.get("userId"))
    if not user_id.isdecimal() or int(user_id) <= 0:
        return None
    entry = {
        "userId": user_id,
        "name": _text(raw.get("name")) or ("ID " + user_id),
        "assignedById": clean_cell_value(raw.get("assignedById")),
        "assignedByName": _text(raw.get("assignedByName")),
        "assignedAt": clean_cell_value(raw.get("assignedAt")),
        "deadline": _clean_date(raw.get("deadline")),
    }
    if keep_items:
        entry["items"] = [clean_cell_value(i) for i in raw.get("items") or [] if clean_cell_value(i)]
    return entry


def _clean_users(raw_list, *, keep_items: bool = False) -> list[dict]:
    result: list[dict] = []
    seen: set[str] = set()
    for raw in raw_list or []:
        entry = _clean_user(raw, keep_items=keep_items)
        if entry and entry["userId"] not in seen:
            seen.add(entry["userId"])
            result.append(entry)
    return result[:MAX_USERS]


def _clean_date(value) -> str:
    raw = clean_cell_value(value)[:10]
    try:
        datetime.strptime(raw, "%Y-%m-%d")
    except ValueError:
        return ""
    return raw


def _target_key(kind: str, target_id: str) -> str:
    return kind + ":" + target_id


def _split_key(key: str) -> tuple[str, str]:
    kind, _, target_id = key.partition(":")
    return kind, target_id


def _read_committed(conn, dialog_id: str, key: str) -> dict[str, list[dict]]:
    rows = conn.execute(
        "SELECT target_kind, target_id, users_json FROM assignment_targets WHERE dialog_id = ? AND checklist_key = ?",
        (dialog_id, key),
    ).fetchall()
    return {
        _target_key(row["target_kind"], row["target_id"]): _clean_users(
            _json(row["users_json"], []), keep_items=row["target_kind"] == GROUP)
        for row in rows
    }


def _read_draft(conn, session_id: str, dialog_id: str, key: str) -> dict | None:
    if not session_id:
        return None
    row = conn.execute(
        "SELECT data_json FROM assignment_drafts WHERE session_id = ? AND dialog_id = ? AND checklist_key = ?",
        (session_id, dialog_id, key),
    ).fetchone()
    if not row:
        return None
    draft = _json(row["data_json"], {})
    draft.setdefault("targets", {})
    draft.setdefault("actions", [])
    draft.setdefault("rekeys", [])
    return draft


def _overlay(committed: dict[str, list[dict]], draft: dict | None) -> dict[str, list[dict]]:
    """The targets as the window sees them: the draft over what is committed.
    A target of the draft set to None has no list of its own."""
    result = {key: list(users) for key, users in committed.items()}
    for key, users in ((draft or {}).get("targets") or {}).items():
        if users is None:
            result.pop(key, None)
        else:
            result[key] = _clean_users(users, keep_items=key.startswith(GROUP + ":"))
    return result


# ---------------------------------------------------------------------------
# The checklist
# ---------------------------------------------------------------------------

def _checklist(dialog_id: str, key: str) -> dict:
    from app.checklists.storage import get_checklist

    try:
        return get_checklist(dialog_id, key) or {}
    except Exception as exc:
        write_debug_log("assign_checklist_read_failed", {"dialogId": dialog_id, "checklistKey": key, "error": str(exc)})
        return {}


def _checklist_in_transaction(conn, dialog_id: str, key: str) -> dict:
    from app.checklists.storage import make_storage_dialog_id

    row = conn.execute("SELECT data_json FROM checklists WHERE dialog_id = ?",
                       (make_storage_dialog_id(dialog_id, key),)).fetchone()
    return _json(row["data_json"], {}) if row else {}


def _not_required_group(key: str) -> int:
    from app.checklists.config import get_checklist_config

    try:
        return int(get_checklist_config(key).not_required_group_id or 0)
    except Exception:
        return 0


class _Items:
    """The items of a checklist the assignments read."""

    def __init__(self, data: dict, key: str):
        self.key = key
        self.items = [i for i in data.get("items") or [] if isinstance(i, dict) and clean_cell_value(i.get("id"))]
        self.by_id = {clean_cell_value(i.get("id")): i for i in self.items}
        self.groups = {}
        for group in data.get("groups") or []:
            if isinstance(group, dict):
                try:
                    self.groups[int(group.get("id"))] = clean_cell_value(group.get("title") or group.get("name"))
                except (TypeError, ValueError):
                    continue
        self.nr_group = _not_required_group(key)

    def get(self, item_id: str) -> dict | None:
        return self.by_id.get(item_id)

    def name(self, item_id: str) -> str:
        return clean_cell_value((self.by_id.get(item_id) or {}).get("name")) or "Пункт"

    def parent_id(self, item_id: str) -> str:
        return clean_cell_value((self.by_id.get(item_id) or {}).get("parentItemId"))

    def status(self, item_id: str) -> str:
        return _status((self.by_id.get(item_id) or {}).get("status"))

    def active(self, item_id: str) -> bool:
        item = self.by_id.get(item_id)
        if item is None or _status(item.get("status")) == STATUS_NR:
            return False
        try:
            return int(item.get("group") or 0) != self.nr_group or not self.nr_group
        except (TypeError, ValueError):
            return True

    def children(self, item_id: str) -> list[str]:
        return [clean_cell_value(i.get("id")) for i in self.items if clean_cell_value(i.get("parentItemId")) == item_id]

    def group_of(self, item_id: str) -> int:
        try:
            return int((self.by_id.get(item_id) or {}).get("group") or 0)
        except (TypeError, ValueError):
            return 0

    def group_title(self, group_id: str) -> str:
        try:
            return self.groups.get(int(group_id), "") or "Раздел"
        except (TypeError, ValueError):
            return "Раздел"

    def group_items(self, group_id: str) -> list[str]:
        try:
            gid = int(group_id)
        except (TypeError, ValueError):
            return []
        return [
            clean_cell_value(i.get("id")) for i in self.items
            if not clean_cell_value(i.get("parentItemId")) and self.group_of(clean_cell_value(i.get("id"))) == gid
            and self.active(clean_cell_value(i.get("id")))
        ]


def effective(targets: dict[str, list[dict]], items: _Items) -> dict[str, dict]:
    """Assignees of every item: its own list, or the list of its item
    (one level of subitems)."""
    result: dict[str, dict] = {}
    for item_id in items.by_id:
        own = targets.get(_target_key(ITEM, item_id))
        if own is not None:
            if own:
                result[item_id] = {"users": own, "inherited": False}
            continue
        parent = items.parent_id(item_id)
        inherited = targets.get(_target_key(ITEM, parent)) if parent else None
        if inherited:
            result[item_id] = {"users": inherited, "inherited": True}
    return result


# ---------------------------------------------------------------------------
# Rights
# ---------------------------------------------------------------------------

def _project_people(base: str) -> dict[str, set[str]]:
    """GIPs (people of ИД, ОПР, П, Р), Главный Концептолог and Главный
    Дизайнер of the project; the manual GIP list without object data."""
    from app.checklists.project_phases import list_gips

    people = {"gip": set(), "concept": set(), "design": set()}
    try:
        from app.checklists.project_objects import (
            _cached_objects,
            _load_bitrix,
            _main_object_id,
            checklist_people,
        )

        bitrix = _load_bitrix(base)
        objects = _cached_objects(bitrix)
        main_id = _main_object_id(bitrix, objects)
        if main_id:
            names = bitrix.get("objectUsers") if isinstance(bitrix.get("objectUsers"), dict) else {}
            for key in GIP_KEYS:
                people["gip"].update(p["userId"] for p in checklist_people(objects, main_id, key, names))
            people["concept"].update(p["userId"] for p in checklist_people(objects, main_id, "concept", names))
            people["design"].update(p["userId"] for p in checklist_people(objects, main_id, "design", names))
            return people
    except Exception as exc:
        write_debug_log("assign_people_failed", {"base": base, "error": str(exc)})
    people["gip"].update(gip["userId"] for gip in list_gips(base))
    return people


def assign_rights(dialog_id: str, checklist_key: str, user_id: str) -> dict:
    from app.checklists.project_leaders import is_project_leader
    from app.checklists.project_phases import base_dialog_id

    user = clean_cell_value(user_id)
    key = normalize_checklist_key(checklist_key)
    if not user or key not in ASSIGNABLE_KEYS:
        return {"assign": False, "role": ""}
    base = base_dialog_id(normalize_dialog_id(dialog_id))
    if is_project_leader(base, user):
        return {"assign": True, "role": "leader"}
    people = _project_people(base)
    if user in people["gip"]:
        return {"assign": True, "role": "gip"}
    if key == "concept" and user in people["concept"]:
        return {"assign": True, "role": "concept"}
    if key == "design" and user in people["design"]:
        return {"assign": True, "role": "design"}
    return {"assign": False, "role": ""}


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

def portal_base_url() -> str:
    parsed = urlsplit(clean_cell_value(BITRIX_TECH_WEBHOOK_URL))
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}"


def task_url(task_id: str, responsible_id: str) -> str:
    base = portal_base_url()
    if not base or not clean_cell_value(task_id):
        return ""
    return f"{base}/company/personal/user/{clean_cell_value(responsible_id) or '0'}/tasks/task/view/{clean_cell_value(task_id)}/"


def _own_session(session_id: str, dialog_id: str, user_id: str) -> str:
    from app.checklists.edit_sessions import get_edit_session_for_actor

    session_id = clean_cell_value(session_id)
    if not session_id or not user_id:
        return ""
    try:
        record = get_edit_session_for_actor(session_id=session_id, dialog_id=dialog_id, user_id=user_id)
    except Exception:
        return ""
    return session_id if record.get("status") == "active" else ""


def _public_user(entry: dict) -> dict:
    result = {
        "userId": entry["userId"], "name": entry["name"],
        "assignedByName": entry.get("assignedByName") or "", "assignedAt": entry.get("assignedAt") or "",
        "deadline": entry.get("deadline") or "",
    }
    if "items" in entry:
        result["items"] = list(entry["items"])
    return result


def state(dialog_id: str, checklist_key: str, user_id: str = "", session_id: str = "") -> dict:
    dialog_id = normalize_dialog_id(dialog_id)
    key = normalize_checklist_key(checklist_key)
    user = clean_cell_value(user_id)
    if not dialog_id or key not in ASSIGNABLE_KEYS:
        return {"ok": True, "supported": False, "targets": [], "rights": {"assign": False}}
    own_session = _own_session(session_id, dialog_id, user) if user else ""
    conn = _conn()
    try:
        committed = _read_committed(conn, dialog_id, key)
        draft = _read_draft(conn, own_session, dialog_id, key)
        task_rows = conn.execute(
            """SELECT target_kind, target_id, user_id, task_id, responsible_id, applied_state
               FROM assignment_tasks WHERE dialog_id = ? AND checklist_key = ? AND COALESCE(task_id, '') <> ''""",
            (dialog_id, key),
        ).fetchall()
    finally:
        conn.close()
    targets = _overlay(committed, draft)
    tasks: dict[str, dict] = {}
    for row in task_rows:
        tasks.setdefault(_target_key(row["target_kind"], row["target_id"]), {})[row["user_id"]] = {
            "taskId": row["task_id"], "url": task_url(row["task_id"], row["responsible_id"] or row["user_id"]),
            "closed": row["applied_state"] == S_CLOSED,
        }
    return {
        "ok": True,
        "supported": True,
        "dialogId": dialog_id,
        "checklistKey": key,
        "rights": assign_rights(dialog_id, key, user),
        "me": user,
        "hasDraft": bool(draft and (draft.get("actions") or draft.get("targets"))),
        "targets": [
            {"kind": _split_key(k)[0], "id": _split_key(k)[1], "users": [_public_user(u) for u in users]}
            for k, users in sorted(targets.items())
        ],
        "tasks": tasks,
        "portalUrl": portal_base_url(),
    }


# ---------------------------------------------------------------------------
# Changes (the draft of the edit session)
# ---------------------------------------------------------------------------

def _names(users: list[dict]) -> list[str]:
    return [u["name"] for u in users]


def _resolve_users(raw_list, actor: dict, now: str, deadline: str, *, items: list[str] | None = None) -> list[dict]:
    from app.checklists.bitrix_users import get_cached_bitrix_user

    result = []
    for raw in raw_list or []:
        if not isinstance(raw, dict):
            continue
        user_id = clean_cell_value(raw.get("userId"))
        if not user_id.isdecimal() or int(user_id) <= 0:
            raise AssignmentError("Выберите сотрудника из списка Битрикса")
        cached = get_cached_bitrix_user(user_id) or {}
        if cached and cached.get("active") is False:
            raise AssignmentError("Сотрудник " + (cached.get("name") or user_id) + " уволен или отключён в Битриксе")
        entry = {
            "userId": user_id,
            "name": _text(cached.get("name")) or _text(raw.get("name")) or ("ID " + user_id),
            "assignedById": actor["userId"], "assignedByName": actor["userName"], "assignedAt": now,
            "deadline": _clean_date(raw.get("deadline")) or deadline,
            # A person already assigned keeps his deadline unless it is given.
            "deadlineSet": "deadline" in raw,
        }
        if items is not None:
            entry["items"] = list(items)
        result.append(entry)
    return result


def _merge(old: list[dict], wanted: list[dict]) -> list[dict]:
    """The new list: people kept keep their assignment, new ones are new."""
    old_by_id = {u["userId"]: u for u in old}
    result = []
    for entry in wanted:
        given = entry.pop("deadlineSet", False)
        kept = old_by_id.get(entry["userId"])
        if kept is not None:
            kept = dict(kept)
            if given:
                kept["deadline"] = entry.get("deadline") or ""
            result.append(kept)
        else:
            result.append(entry)
    seen: set[str] = set()
    unique = []
    for entry in result:
        if entry["userId"] not in seen:
            seen.add(entry["userId"])
            unique.append(entry)
    if len(unique) > MAX_USERS:
        raise AssignmentError("Слишком много исполнителей")
    return unique


def _hand_over(users: list[dict], from_id: str, receivers: list[dict]) -> tuple[list[dict], str]:
    """The list after a handover: the receivers take the place of the giver
    (those already in the list stay where they are) and the first new one
    takes his task."""
    position = [u["userId"] for u in users].index(from_id)
    others = {u["userId"] for u in users if u["userId"] != from_id}
    fresh = [dict(r) for r in receivers if r["userId"] not in others]
    heir = fresh[0]["userId"] if fresh else receivers[0]["userId"]
    return users[:position] + fresh + users[position + 1:], heir


def _seed_statuses(draft: dict, items: "_Items", kind: str, target_id: str) -> None:
    """The statuses when an item gets its first assignee: the journal tells
    who changed them from then on, also in the same session."""
    seeds = draft.setdefault("statuses", {})
    if kind == GROUP:
        ids = items.group_items(target_id)
    else:
        ids = [target_id] + items.children(target_id)
    for item_id in ids:
        if item_id in items.by_id and item_id not in seeds:
            seeds[item_id] = items.status(item_id) or STATUS_NO


def change(dialog_id: str, checklist_key: str, session_id: str, actor: dict, payload: dict) -> dict:
    """One change of the assignees of an item or a section in the draft of
    the session. The lock of the checklist is taken and kept until Save or
    Cancel."""
    from app.checklists.edit_session_changes import acquire_checklist_for_edit_session

    dialog_id = normalize_dialog_id(dialog_id)
    key = normalize_checklist_key(checklist_key)
    session_id = clean_cell_value(session_id)
    actor = {"userId": clean_cell_value(actor.get("userId")),
             "userName": _text(actor.get("userName")) or "Пользователь"}
    kind = clean_cell_value(payload.get("kind")) or ITEM
    target_id = clean_cell_value(payload.get("targetId"))
    op = clean_cell_value(payload.get("op"))
    if key not in ASSIGNABLE_KEYS:
        raise AssignmentError("В этом чек-листе исполнителей не назначают")
    if not dialog_id or not session_id:
        raise AssignmentError("Нет сессии редактирования: откройте чек-лист заново")
    if kind not in (ITEM, GROUP) or not target_id:
        raise AssignmentError("Не указан пункт")
    if op not in ("set", "delegate", "inherit"):
        raise AssignmentError("Неизвестное действие")
    if not actor["userId"]:
        raise AssignmentDenied("Не удалось определить пользователя")
    rights = assign_rights(dialog_id, key, actor["userId"])
    items = _Items(_checklist(dialog_id, key), key)
    if kind == ITEM:
        if items.get(target_id) is None:
            raise AssignmentError("Пункт не найден: обновите окно")
        target_active = items.active(target_id)
    else:
        if not target_id.isdecimal() or int(target_id) not in items.groups or (
                items.nr_group and str(items.nr_group) == target_id):
            raise AssignmentError("Раздел не найден")
        target_active = True
    if not target_active and op != "set":
        raise AssignmentError("Пункт в «Не требуется»: исполнителя не назначить")

    acquire_checklist_for_edit_session(
        session_id=session_id, dialog_id=dialog_id, checklist_key=key,
        user_id=actor["userId"], user_name=actor["userName"],
    )
    now = _now()
    deadline = _clean_date(payload.get("deadline"))
    with _WRITE_LOCK:
        conn = _conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT status FROM edit_sessions WHERE session_id = ?", (session_id,)).fetchone()
            if not row or row["status"] != "active":
                raise AssignmentError("Сессия редактирования уже завершена")
            committed = _read_committed(conn, dialog_id, key)
            draft = _read_draft(conn, session_id, dialog_id, key)
            fresh = draft is None
            draft = draft or {"targets": {}, "actions": [], "rekeys": []}
            current = _overlay(committed, draft)
            tkey = _target_key(kind, target_id)
            eff = effective(current, items)
            if kind == ITEM:
                before = list((eff.get(target_id) or {}).get("users") or [])
            else:
                before = list(current.get(tkey) or [])
            actions: list[dict] = []
            changes: dict[str, list[dict] | None] = {}

            if op == "set":
                if not rights["assign"]:
                    raise AssignmentDenied("Исполнителей назначают руководители, администраторы и ГИП проекта"
                                           + {"concept": ", в Концепции — Главный Концептолог",
                                              "design": ", в Дизайне — Главный Дизайнер"}.get(key, ""))
                snapshot = items.group_items(target_id) if kind == GROUP else None
                wanted = _resolve_users(payload.get("users"), actor, now, "", items=snapshot)
                before_ids = {b["userId"] for b in before}
                for entry in wanted:
                    if entry["userId"] not in before_ids and not entry["deadline"]:
                        entry["deadline"] = deadline
                new_list = _merge(before, wanted)
                added = [u for u in new_list if u["userId"] not in before_ids]
                removed = [u for u in before if u["userId"] not in {n["userId"] for n in new_list}]
                if added and not target_active:
                    raise AssignmentError("Пункт в «Не требуется»: исполнителя не назначить")
                if added and kind == GROUP and not snapshot:
                    raise AssignmentError("В разделе нет пунктов")
                changes[tkey] = new_list
                if added:
                    actions.append({"kind": "assign", "users": [_public_user(u) for u in added], "deadline": deadline})
                if removed:
                    actions.append({"kind": "unassign", "users": [_public_user(u) for u in removed]})
                deadline_changed = [
                    u for u in new_list
                    if u["userId"] in {b["userId"] for b in before}
                    and u.get("deadline") != next(b for b in before if b["userId"] == u["userId"]).get("deadline")
                ]
                if deadline_changed:
                    actions.append({"kind": "deadline", "users": [_public_user(u) for u in deadline_changed]})
                if not actions and kind == ITEM and (eff.get(target_id) or {}).get("inherited"):
                    # The same people as the item, now as the subitem's own list.
                    actions.append({"kind": "own", "users": [_public_user(u) for u in new_list]})
            elif op == "inherit":
                if not rights["assign"]:
                    raise AssignmentDenied("Исполнителей назначают руководители, администраторы и ГИП проекта")
                parent = items.parent_id(target_id) if kind == ITEM else ""
                if not parent:
                    raise AssignmentError("Это не подпункт")
                changes[tkey] = None
                actions.append({"kind": "inherit"})
            else:  # delegate
                from_id = clean_cell_value(payload.get("fromUserId")) or actor["userId"]
                giver = next((u for u in before if u["userId"] == from_id), None)
                if giver is None:
                    raise AssignmentError("Этот сотрудник не назначен на пункт")
                if not rights["assign"] and from_id != actor["userId"]:
                    raise AssignmentDenied("Передать можно только свой пункт")
                receivers = _resolve_users(payload.get("users"), actor, now, deadline or giver.get("deadline") or "",
                                           items=giver.get("items") if kind == GROUP else None)
                receivers = [u for u in receivers if u["userId"] != from_id]
                if not receivers:
                    raise AssignmentError("Выберите, кому передать")
                for entry in receivers:
                    entry.pop("deadlineSet", None)
                changes[tkey], heir = _hand_over(before, from_id, receivers)
                if len(changes[tkey]) > MAX_USERS:
                    raise AssignmentError("Слишком много исполнителей")
                rekeys = [{"kind": kind, "targetId": target_id, "from": from_id, "to": heir}]
                if kind == ITEM and not items.parent_id(target_id):
                    # The item is handed over entirely: its subitems too.
                    for child_id in items.children(target_id):
                        ckey = _target_key(ITEM, child_id)
                        own = current.get(ckey)
                        if own and any(u["userId"] == from_id for u in own):
                            changes[ckey], child_heir = _hand_over(own, from_id, receivers)
                            rekeys.append({"kind": ITEM, "targetId": child_id, "from": from_id, "to": child_heir})
                draft["rekeys"].extend(rekeys)
                actions.append({"kind": "delegate", "from": _public_user(giver),
                                "users": [_public_user(u) for u in receivers], "deadline": deadline})

            if not actions:
                conn.rollback()
                return state(dialog_id, key, actor["userId"], session_id)
            for ckey, value in changes.items():
                draft["targets"][ckey] = value
            _seed_statuses(draft, items, kind, target_id)
            after = _overlay(committed, draft)
            after_eff = effective(after, items)
            current_users = (after_eff.get(target_id) or {}).get("users") if kind == ITEM else after.get(tkey)
            for action in actions:
                action.update({
                    "at": now, "targetKind": kind, "targetId": target_id,
                    "actorId": actor["userId"], "actorName": actor["userName"],
                    "current": _names(current_users or []),
                })
                if kind == GROUP:
                    action["itemIds"] = items.group_items(target_id)
            draft["actions"].extend(actions)
            data_json = json.dumps(draft, ensure_ascii=False)
            if fresh:
                conn.execute(
                    """INSERT INTO assignment_drafts(session_id, dialog_id, checklist_key, data_json, user_id, user_name, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (session_id, dialog_id, key, data_json, actor["userId"], actor["userName"], now),
                )
            else:
                conn.execute(
                    "UPDATE assignment_drafts SET data_json = ?, updated_at = ? WHERE session_id = ? AND dialog_id = ? AND checklist_key = ?",
                    (data_json, now, session_id, dialog_id, key),
                )
            # The lock stays with the session until Save or Cancel.
            conn.execute(
                """UPDATE edit_session_checklists SET mutation_count = COALESCE(mutation_count, 0) + 1
                   WHERE session_id = ? AND dialog_id = ? AND checklist_key = ?""",
                (session_id, dialog_id, key),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    write_debug_log("assign_draft_changed", {
        "dialogId": dialog_id, "checklistKey": key, "sessionId": session_id, "op": op,
        "target": tkey, "by": actor["userId"],
        "actions": [{"kind": a["kind"], "users": [u["userId"] for u in a.get("users") or []],
                     "from": (a.get("from") or {}).get("userId", "")} for a in actions],
        "current": [u["userId"] for u in current_users or []],
    })
    return state(dialog_id, key, actor["userId"], session_id)


# ---------------------------------------------------------------------------
# Commit and rollback (inside the transaction of the edit session)
# ---------------------------------------------------------------------------

def _journal(conn, dialog_id, key, kind, target_id, at, entry_kind, actor_id, actor_name, session_id, data) -> None:
    conn.execute(
        """INSERT INTO assignment_journal(dialog_id, checklist_key, target_kind, target_id, at, kind,
                                          actor_id, actor_name, session_id, data_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (dialog_id, key, kind, target_id, at, entry_kind, actor_id, actor_name, session_id,
         json.dumps(data, ensure_ascii=False)),
    )


def _write_targets(conn, dialog_id: str, key: str, targets: dict, now: str, user_id: str) -> None:
    for tkey, users in targets.items():
        kind, target_id = _split_key(tkey)
        if users is None:
            conn.execute(
                "DELETE FROM assignment_targets WHERE dialog_id = ? AND checklist_key = ? AND target_kind = ? AND target_id = ?",
                (dialog_id, key, kind, target_id),
            )
            continue
        conn.execute(
            """INSERT INTO assignment_targets(dialog_id, checklist_key, target_kind, target_id, users_json, updated_at, updated_by)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(dialog_id, checklist_key, target_kind, target_id) DO UPDATE SET
                   users_json = excluded.users_json, updated_at = excluded.updated_at, updated_by = excluded.updated_by""",
            (dialog_id, key, kind, target_id,
             json.dumps(_clean_users(users, keep_items=kind == GROUP), ensure_ascii=False), now, user_id),
        )


def _apply_rekeys(conn, dialog_id: str, key: str, rekeys: list[dict], now: str) -> list[dict]:
    """A handover: the task of the one who gave the item goes to the first
    one who got it (Bitrix learns the new responsible on the sync)."""
    done = []
    for rekey in rekeys:
        kind, target_id = rekey.get("kind"), clean_cell_value(rekey.get("targetId"))
        giver, receiver = clean_cell_value(rekey.get("from")), clean_cell_value(rekey.get("to"))
        if not giver or not receiver or giver == receiver:
            continue
        row = conn.execute(
            """SELECT task_id FROM assignment_tasks WHERE dialog_id = ? AND checklist_key = ? AND target_kind = ?
               AND target_id = ? AND user_id = ?""",
            (dialog_id, key, kind, target_id, giver),
        ).fetchone()
        taken = conn.execute(
            """SELECT 1 FROM assignment_tasks WHERE dialog_id = ? AND checklist_key = ? AND target_kind = ?
               AND target_id = ? AND user_id = ?""",
            (dialog_id, key, kind, target_id, receiver),
        ).fetchone()
        if not row or taken:
            continue
        conn.execute(
            """UPDATE assignment_tasks SET user_id = ?, updated_at = ? WHERE dialog_id = ? AND checklist_key = ?
               AND target_kind = ? AND target_id = ? AND user_id = ?""",
            (receiver, now, dialog_id, key, kind, target_id, giver),
        )
        # The handed task is no more the main task of the giver.
        conn.execute(
            "DELETE FROM assignment_main_tasks WHERE dialog_id = ? AND checklist_key = ? AND user_id = ? AND task_id = ?",
            (dialog_id, key, giver, row["task_id"]),
        )
        done.append({**rekey, "taskId": row["task_id"]})
    return done


def _session_checklists(conn, session_id: str) -> list[tuple[str, str]]:
    try:
        rows = conn.execute(
            "SELECT DISTINCT dialog_id, checklist_key FROM edit_session_checklists WHERE session_id = ?",
            (session_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [(normalize_dialog_id(r["dialog_id"]), normalize_checklist_key(r["checklist_key"])) for r in rows]


def _record_statuses(conn, session_id: str, dialog_id: str, key: str, user: tuple[str, str], now: str) -> int:
    """The journal of the item tells who set its status, for every item
    that has assignees or is in the list of a section task."""
    targets = _read_committed(conn, dialog_id, key)
    if not targets:
        return 0
    items = _Items(_checklist_in_transaction(conn, dialog_id, key), key)
    eff = effective(targets, items)
    tracked = set(eff)
    for tkey, users in targets.items():
        if tkey.startswith(GROUP + ":"):
            for entry in users:
                tracked.update(entry.get("items") or [])
    stored = {
        row["item_id"]: row["status"] for row in conn.execute(
            "SELECT item_id, status FROM assignment_item_status WHERE dialog_id = ? AND checklist_key = ?",
            (dialog_id, key),
        ).fetchall()
    }
    written = 0
    for item_id in sorted(tracked):
        if item_id not in items.by_id:
            continue
        status = items.status(item_id) or STATUS_NO
        previous = stored.get(item_id)
        if previous is not None and previous != status:
            users = (eff.get(item_id) or {}).get("users") or []
            _journal(conn, dialog_id, key, ITEM, item_id, now, "status", user[0], user[1], session_id, {
                "from": previous, "to": status,
                "byAssignee": user[0] in {u["userId"] for u in users},
                "current": _names(users),
            })
            written += 1
        if previous != status:
            conn.execute(
                """INSERT INTO assignment_item_status(dialog_id, checklist_key, item_id, status, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(dialog_id, checklist_key, item_id) DO UPDATE SET status = excluded.status,
                       updated_at = excluded.updated_at""",
                (dialog_id, key, item_id, status, now),
            )
    return written


def finalize_drafts(conn, session_id: str, *, commit: bool, now: str) -> None:
    """Called inside the transaction that commits or rolls back the session."""
    try:
        _ensure_tables(conn)
        rows = conn.execute(
            "SELECT dialog_id, checklist_key, data_json, user_id, user_name FROM assignment_drafts WHERE session_id = ?",
            (session_id,),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        write_debug_log("assign_finalize_failed", {"sessionId": session_id, "error": str(exc)})
        return
    if not commit:
        conn.execute("DELETE FROM assignment_drafts WHERE session_id = ?", (session_id,))
        if rows:
            write_debug_log("assign_drafts_dropped", {"sessionId": session_id, "drafts": len(rows)})
        return
    session = conn.execute("SELECT user_id, user_name FROM edit_sessions WHERE session_id = ?", (session_id,)).fetchone()
    user = (clean_cell_value(session["user_id"]) if session else "", clean_cell_value(session["user_name"]) if session else "")
    touched: dict[tuple[str, str], dict] = {}
    for row in rows:
        dialog_id, key = normalize_dialog_id(row["dialog_id"]), normalize_checklist_key(row["checklist_key"])
        draft = _json(row["data_json"], {})
        _write_targets(conn, dialog_id, key, draft.get("targets") or {}, now, clean_cell_value(row["user_id"]))
        for action in draft.get("actions") or []:
            data = {k: v for k, v in action.items() if k not in ("at", "targetKind", "targetId", "actorId", "actorName", "kind")}
            _journal(conn, dialog_id, key, action.get("targetKind") or ITEM, clean_cell_value(action.get("targetId")),
                     clean_cell_value(action.get("at")) or now, clean_cell_value(action.get("kind")),
                     clean_cell_value(action.get("actorId")), clean_cell_value(action.get("actorName")), session_id, data)
        for item_id, status in (draft.get("statuses") or {}).items():
            conn.execute(
                """INSERT OR IGNORE INTO assignment_item_status(dialog_id, checklist_key, item_id, status, updated_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (dialog_id, key, clean_cell_value(item_id), clean_cell_value(status), now),
            )
        rekeyed = _apply_rekeys(conn, dialog_id, key, draft.get("rekeys") or [], now)
        touched[(dialog_id, key)] = {"actions": len(draft.get("actions") or []), "rekeys": rekeyed}
    for pair in _session_checklists(conn, session_id):
        touched.setdefault(pair, {"actions": 0, "rekeys": []})
    queued = []
    for (dialog_id, key), info in touched.items():
        if key not in ASSIGNABLE_KEYS:
            continue
        has_targets = conn.execute(
            "SELECT 1 FROM assignment_targets WHERE dialog_id = ? AND checklist_key = ? LIMIT 1", (dialog_id, key),
        ).fetchone()
        has_tasks = conn.execute(
            "SELECT 1 FROM assignment_tasks WHERE dialog_id = ? AND checklist_key = ? LIMIT 1", (dialog_id, key),
        ).fetchone()
        if not has_targets and not has_tasks:
            continue
        statuses = _record_statuses(conn, session_id, dialog_id, key, user, now)
        conn.execute(
            """INSERT INTO assignment_sync_queue(dialog_id, checklist_key, session_id, status, attempts, created_at, updated_at)
               VALUES (?, ?, ?, 'pending', 0, ?, ?)""",
            (dialog_id, key, session_id, now, now),
        )
        queued.append({"dialogId": dialog_id, "checklistKey": key, "actions": info["actions"],
                       "statusChanges": statuses, "handovers": [r.get("taskId") for r in info["rekeys"]]})
    conn.execute("DELETE FROM assignment_drafts WHERE session_id = ?", (session_id,))
    if rows or queued:
        write_debug_log("assign_drafts_committed", {"sessionId": session_id, "drafts": len(rows), "queued": queued})


# ---------------------------------------------------------------------------
# Journal of an item (the folder page)
# ---------------------------------------------------------------------------

_JOURNAL_TEXT = {
    "assign": "Назначил",
    "unassign": "Снял",
    "delegate": "Передал",
    "inherit": "Исполнители как у пункта",
    "own": "Свои исполнители подпункта",
    "deadline": "Крайний срок",
    "status": "Статус",
    "task": "Задача",
}


def journal(dialog_id: str, checklist_key: str, item_id: str) -> list[dict]:
    dialog_id = normalize_dialog_id(dialog_id)
    key = normalize_checklist_key(checklist_key)
    item_id = clean_cell_value(item_id)
    if not dialog_id or not item_id:
        return []
    items = _Items(_checklist(dialog_id, key), key)
    conn = _conn()
    try:
        rows = conn.execute(
            """SELECT * FROM assignment_journal WHERE dialog_id = ? AND checklist_key = ?
               AND ((target_kind = 'item' AND target_id = ?) OR target_kind = 'group')
               ORDER BY id DESC LIMIT ?""",
            (dialog_id, key, item_id, JOURNAL_LIMIT * 2),
        ).fetchall()
    finally:
        conn.close()
    result = []
    for row in rows:
        data = _json(row["data_json"], {})
        if row["target_kind"] == GROUP and item_id not in (data.get("itemIds") or []):
            # A section: the rows of its items only.
            continue
        result.append({
            "at": row["at"], "kind": row["kind"], "label": _JOURNAL_TEXT.get(row["kind"], row["kind"]),
            "actorId": row["actor_id"], "actorName": row["actor_name"],
            "section": items.group_title(row["target_id"]) if row["target_kind"] == GROUP else "",
            "data": data,
        })
        if len(result) >= JOURNAL_LIMIT:
            break
    return result


# ---------------------------------------------------------------------------
# Bitrix
# ---------------------------------------------------------------------------

def flatten(value, prefix: str = "") -> list[tuple[str, str]]:
    """PHP form fields of the REST API: fields[TITLE]=..., select[0]=..."""
    out: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            out += flatten(item, f"{prefix}[{key}]" if prefix else str(key))
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            out += flatten(item, f"{prefix}[{index}]")
    elif isinstance(value, bool):
        out.append((prefix, "Y" if value else "N"))
    else:
        out.append((prefix, "" if value is None else str(value)))
    return out


def _http_post(url: str, data, timeout: int):
    return requests.post(url, data=data, timeout=timeout)


def _short_params(params: dict) -> dict:
    def short(value):
        if isinstance(value, dict):
            return {k: (f"<{len(v)} chars>" if k in ("DESCRIPTION", "POST_MESSAGE", "MESSAGE") and isinstance(v, str)
                        else short(v)) for k, v in value.items()}
        if isinstance(value, list):
            return [short(v) for v in value[:20]]
        if isinstance(value, str) and len(value) > 160:
            return value[:160] + "…"
        return value
    return short(params)


def _short_result(body: dict):
    result = body.get("result") if isinstance(body, dict) else None
    if isinstance(result, dict) and isinstance(result.get("task"), dict):
        task = result["task"]
        return {k: task.get(k) for k in ("id", "status", "createdBy", "responsibleId", "parentId", "groupId", "deadline")
                if k in task}
    if isinstance(result, (dict, list)):
        text = json.dumps(result, ensure_ascii=False)
        return text[:300]
    return result


def bitrix_call(method: str, params: dict, *, purpose: str, context: dict | None = None) -> dict:
    """A call of the webhook with nested parameters, every call in the log."""
    if not BITRIX_TECH_WEBHOOK_URL:
        body = {"error": "TECH_WEBHOOK_NOT_CONFIGURED", "error_description": "BITRIX_TECH_WEBHOOK_URL is empty"}
    else:
        url = BITRIX_TECH_WEBHOOK_URL.rstrip("/") + "/" + method + ".json"
        started = _time.monotonic()
        try:
            response = _http_post(url, flatten(params), 30)
            try:
                body = response.json()
            except Exception:
                body = {"error": f"HTTP_{getattr(response, 'status_code', '?')}",
                        "error_description": clean_cell_value(getattr(response, "text", ""))[:300]}
            if not isinstance(body, dict):
                body = {"result": body}
        except Exception as exc:
            body = {"error": "EXCEPTION", "error_description": str(exc)[:300]}
        body.setdefault("_ms", int((_time.monotonic() - started) * 1000))
    write_debug_log("assign_bitrix_call", {
        **(context or {}),
        "method": method, "purpose": purpose,
        "ok": not body.get("error"),
        "error": clean_cell_value(body.get("error")),
        "errorDescription": clean_cell_value(body.get("error_description"))[:300],
        "ms": body.get("_ms"),
        "params": _short_params(params),
        "result": _short_result(body) if not body.get("error") else None,
    })
    return body


def _error_text(body: dict) -> str:
    if not body.get("error"):
        return ""
    return (clean_cell_value(body.get("error")) + ": " + clean_cell_value(body.get("error_description"))).strip(": ")


def _task_id_of(body: dict) -> str:
    result = body.get("result")
    if isinstance(result, dict):
        task = result.get("task")
        if isinstance(task, dict):
            return clean_cell_value(task.get("id") or task.get("ID"))
        return clean_cell_value(result.get("id") or result.get("ID"))
    return clean_cell_value(result)


def _task_get(task_id: str, ctx: dict) -> dict | None:
    """The task as Bitrix has it; None when it is not there (deleted or no
    access)."""
    body = bitrix_call("tasks.task.get", {
        "taskId": task_id,
        "select": ["ID", "STATUS", "CREATED_BY", "RESPONSIBLE_ID", "PARENT_ID", "GROUP_ID", "DEADLINE", "TASK_CONTROL"],
    }, purpose="read task", context=ctx)
    if body.get("error"):
        return None
    task = (body.get("result") or {}).get("task") if isinstance(body.get("result"), dict) else None
    if not isinstance(task, dict) or not clean_cell_value(task.get("id")):
        return None
    return {
        "id": clean_cell_value(task.get("id")),
        "status": clean_cell_value(task.get("status")),
        "createdBy": clean_cell_value(task.get("createdBy")),
        "responsibleId": clean_cell_value(task.get("responsibleId")),
    }


def _deadline_iso(value: str) -> str:
    raw = _clean_date(value)
    if not raw:
        return ""
    day = datetime.strptime(raw, "%Y-%m-%d").date()
    return datetime(day.year, day.month, day.day, 19, 0, tzinfo=PROJECT_TIMEZONE).isoformat()


# ---------------------------------------------------------------------------
# What the checklist wants of Bitrix
# ---------------------------------------------------------------------------

def _desired(targets: dict[str, list[dict]], items: _Items) -> tuple[dict, dict]:
    """The tasks the checklist wants: {(kind, target, user): want} and which
    task covers an item of a person: {(item, user): (kind, target, user)}."""
    eff = effective(targets, items)
    wants: dict[tuple, dict] = {}
    covers: dict[tuple, tuple] = {}
    for item_id, info in eff.items():
        if not items.active(item_id):
            continue
        parent = items.parent_id(item_id)
        parent_users = {u["userId"] for u in (eff.get(parent) or {}).get("users") or []} if parent else set()
        for entry in info["users"]:
            user = entry["userId"]
            if parent and user in parent_users and items.active(parent):
                continue
            lines = {}
            if not parent:
                for child in items.children(item_id):
                    if items.active(child) and user in {u["userId"] for u in (eff.get(child) or {}).get("users") or []}:
                        lines[child] = {"title": items.name(child), "done": items.status(child) == STATUS_YES}
            status = items.status(item_id)
            if status == STATUS_YES or (lines and all(line["done"] for line in lines.values())):
                want_state = S_CONTROL
            elif status == STATUS_WORK or any(items.status(c) in STARTED for c in lines):
                want_state = S_PROGRESS
            else:
                want_state = S_PENDING
            key = (ITEM, item_id, user)
            wants[key] = {"entry": entry, "lines": lines, "state": want_state, "parent": parent}
            covers[(item_id, user)] = key
            for child in lines:
                covers[(child, user)] = key
    for tkey, users in targets.items():
        kind, group_id = _split_key(tkey)
        if kind != GROUP:
            continue
        for entry in users:
            user = entry["userId"]
            lines = {
                item_id: {"title": items.name(item_id), "done": items.status(item_id) == STATUS_YES}
                for item_id in entry.get("items") or [] if items.active(item_id)
            }
            if not lines:
                continue
            if all(line["done"] for line in lines.values()):
                want_state = S_CONTROL
            elif any(items.status(i) in STARTED for i in lines):
                want_state = S_PROGRESS
            else:
                want_state = S_PENDING
            key = (GROUP, group_id, user)
            wants[key] = {"entry": entry, "lines": lines, "state": want_state, "parent": ""}
            covers[("group:" + group_id, user)] = key
    return wants, covers


class _Context:
    """What the tasks of one checklist say about it."""

    def __init__(self, dialog_id: str, key: str, data: dict):
        from app.checklists.messages import build_checklist_message_link
        from app.checklists.project_phases import base_dialog_id, phase_label
        from app.checklists.registry import get_checklist_title
        from app.checklists.storage import get_project_storage_context

        self.dialog_id = dialog_id
        self.key = key
        self.base = base_dialog_id(dialog_id)
        self.items = _Items(data, key)
        title = clean_cell_value(get_checklist_title(key)) or "Чек-лист"
        self.checklist_title = title
        self.short_title = title.replace("Чек-лист", "").strip() or title
        context = {}
        for candidate in (dialog_id, self.base):
            try:
                context = get_project_storage_context(candidate) or {}
            except Exception:
                context = {}
            if context:
                break
        self.project_name = clean_cell_value(context.get("projectName")) or clean_cell_value(data.get("collabTitle")) or self.base
        group_id = clean_cell_value(context.get("projectId"))
        self.group_id = group_id if group_id.isdecimal() and int(group_id) > 0 else ""
        try:
            self.phase = phase_label(dialog_id)
        except Exception:
            self.phase = ""
        try:
            self.link = build_checklist_message_link(dialog_id, key)
        except Exception:
            self.link = ""

    def log(self, **extra) -> dict:
        return {"dialogId": self.dialog_id, "checklistKey": self.key, **extra}

    def project(self) -> str:
        return self.project_name + (", " + self.phase if self.phase else "")

    def title(self, kind: str, target_id: str) -> str:
        if kind == GROUP:
            head = f"Раздел «{self.items.group_title(target_id)}»"
        else:
            parent = self.items.parent_id(target_id)
            head = f"Пункт «{self.items.name(target_id)}»"
            if parent:
                head = f"Подпункт «{self.items.name(target_id)}» пункта «{self.items.name(parent)}»"
        return f"{head} — {self.short_title} — {self.project()}"[:250]

    def description(self, kind: str, target_id: str, entry: dict, lines: dict) -> str:
        if kind == GROUP:
            what = f"раздел «{self.items.group_title(target_id)}»"
        elif self.items.parent_id(target_id):
            what = (f"подпункт «{self.items.name(target_id)}» пункта «{self.items.name(self.items.parent_id(target_id))}»")
        else:
            what = f"пункт «{self.items.name(target_id)}»"
        parts = [f"Вы назначены исполнителем: {what} чек-листа {self.short_title} проекта «{self.project()}»."]
        if lines:
            parts.append("Пункты и подпункты — в чек-листе этой задачи; они отмечаются сами, когда в чек-листе у них ставят «Есть».")
        if entry.get("assignedByName"):
            parts.append("Назначил: " + entry["assignedByName"] + ".")
        if self.link:
            parts.append(f"[URL={self.link}]Открыть чек-лист {self.short_title}[/URL] — файлы пункта в его папке.")
        parts.append("Статус «В работе» в чек-листе переводит задачу в «Выполняется», статус «Есть» — на проверку постановщику.")
        return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Synchronization
# ---------------------------------------------------------------------------

def _sync_lock(dialog_id: str, key: str) -> threading.Lock:
    with _SYNC_LOCKS_GUARD:
        return _SYNC_LOCKS.setdefault((dialog_id, key), threading.Lock())


def _task_rows(dialog_id: str, key: str) -> dict[tuple, dict]:
    conn = _conn()
    try:
        rows = conn.execute("SELECT * FROM assignment_tasks WHERE dialog_id = ? AND checklist_key = ?",
                            (dialog_id, key)).fetchall()
    finally:
        conn.close()
    return {(r["target_kind"], r["target_id"], r["user_id"]): dict(r) for r in rows}


def _save_task_row(dialog_id: str, key: str, task_key: tuple, fields: dict) -> None:
    now = _now()
    kind, target_id, user = task_key
    conn = _conn()
    try:
        exists = conn.execute(
            """SELECT 1 FROM assignment_tasks WHERE dialog_id = ? AND checklist_key = ? AND target_kind = ?
               AND target_id = ? AND user_id = ?""", (dialog_id, key, kind, target_id, user),
        ).fetchone()
        if not exists:
            conn.execute(
                """INSERT INTO assignment_tasks(dialog_id, checklist_key, target_kind, target_id, user_id, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""", (dialog_id, key, kind, target_id, user, now, now),
            )
        if fields:
            columns = ", ".join(f"{name} = ?" for name in fields)
            conn.execute(
                f"""UPDATE assignment_tasks SET {columns}, updated_at = ? WHERE dialog_id = ? AND checklist_key = ?
                    AND target_kind = ? AND target_id = ? AND user_id = ?""",
                (*fields.values(), now, dialog_id, key, kind, target_id, user),
            )
        conn.commit()
    finally:
        conn.close()


def _task_journal(ctx: _Context, task_key: tuple, action: str, task_id: str, *, item_ids=None, **extra) -> None:
    kind, target_id, user = task_key
    responsible = clean_cell_value(extra.pop("responsibleId", "")) or user
    name = clean_cell_value(extra.pop("userName", ""))
    if not name:
        from app.checklists.bitrix_users import get_cached_bitrix_user

        name = clean_cell_value((get_cached_bitrix_user(user) or {}).get("name")) or ("ID " + user)
    data = {"action": action, "userId": user, "userName": name, "taskId": task_id,
            "url": task_url(task_id, responsible), **extra}
    if kind == GROUP:
        data["itemIds"] = list(item_ids or [])
    conn = _conn()
    try:
        _journal(conn, ctx.dialog_id, ctx.key, kind, target_id, _now(), "task", "", "", "", data)
        conn.commit()
    finally:
        conn.close()


def _main_task(ctx: _Context, user: str) -> str:
    """The open main task of the person in this checklist, or ""."""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT task_id FROM assignment_main_tasks WHERE dialog_id = ? AND checklist_key = ? AND user_id = ?",
            (ctx.dialog_id, ctx.key, user),
        ).fetchone()
    finally:
        conn.close()
    task_id = clean_cell_value(row["task_id"]) if row else ""
    if not task_id:
        return ""
    task = _task_get(task_id, ctx.log(userId=user, taskId=task_id))
    if task is None or task["status"] == T_DONE or task["responsibleId"] != user:
        write_debug_log("assign_sync_main_task_closed", ctx.log(userId=user, taskId=task_id,
                                                                 status=(task or {}).get("status", "missing"),
                                                                 responsibleId=(task or {}).get("responsibleId", "")))
        return ""
    return task_id


def _set_main_task(ctx: _Context, user: str, task_id: str) -> None:
    conn = _conn()
    try:
        conn.execute(
            """INSERT INTO assignment_main_tasks(dialog_id, checklist_key, user_id, task_id, updated_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(dialog_id, checklist_key, user_id) DO UPDATE SET task_id = excluded.task_id,
                   updated_at = excluded.updated_at""",
            (ctx.dialog_id, ctx.key, user, task_id, _now()),
        )
        conn.commit()
    finally:
        conn.close()


def _create_task(ctx: _Context, task_key: tuple, want: dict) -> dict:
    kind, target_id, user = task_key
    entry = want["entry"]
    creator = clean_cell_value(entry.get("assignedById"))
    log = ctx.log(target=f"{kind}:{target_id}", userId=user)
    parent_task = _main_task(ctx, user)
    fields = {
        "TITLE": ctx.title(kind, target_id),
        "DESCRIPTION": ctx.description(kind, target_id, entry, want["lines"]),
        "RESPONSIBLE_ID": user,
        "TASK_CONTROL": "Y",
    }
    if creator:
        fields["CREATED_BY"] = creator
    if ctx.group_id:
        fields["GROUP_ID"] = ctx.group_id
    if entry.get("deadline"):
        fields["DEADLINE"] = _deadline_iso(entry["deadline"])
    if parent_task:
        fields["PARENT_ID"] = parent_task
    body = bitrix_call("tasks.task.add", {"fields": fields}, purpose="create task", context=log)
    attempts = [("as is", _error_text(body))]
    if body.get("error") and "GROUP_ID" in fields:
        fields.pop("GROUP_ID")
        body = bitrix_call("tasks.task.add", {"fields": fields}, purpose="create task without the project group", context=log)
        attempts.append(("no group", _error_text(body)))
    if body.get("error") and "CREATED_BY" in fields:
        fields.pop("CREATED_BY")
        body = bitrix_call("tasks.task.add", {"fields": fields}, purpose="create task as the webhook user", context=log)
        attempts.append(("no creator", _error_text(body)))
    task_id = _task_id_of(body) if not body.get("error") else ""
    if not task_id:
        error = _error_text(body) or "no task id in the answer"
        write_debug_log("assign_sync_create_failed", {**log, "attempts": attempts, "error": error})
        _save_task_row(ctx.dialog_id, ctx.key, task_key, {"error": error[:500]})
        return {"ok": False, "error": error}
    task = _task_get(task_id, {**log, "taskId": task_id})
    creator_now = (task or {}).get("createdBy", "")
    if creator and creator_now != creator:
        update = bitrix_call("tasks.task.update", {"taskId": task_id, "fields": {"CREATED_BY": creator}},
                             purpose="make the one who assigned the creator", context={**log, "taskId": task_id})
        task = _task_get(task_id, {**log, "taskId": task_id})
        creator_now = (task or {}).get("createdBy", "")
        if update.get("error") or creator_now != creator:
            write_debug_log("assign_sync_creator_not_set", {**log, "taskId": task_id, "wanted": creator,
                                                           "creator": creator_now, "error": _error_text(update)})
    _save_task_row(ctx.dialog_id, ctx.key, task_key, {
        "task_id": task_id, "parent_task_id": parent_task, "responsible_id": user, "creator_id": creator_now or creator,
        "deadline": entry.get("deadline") or "", "applied_state": S_PENDING, "lines_json": "{}", "error": "",
    })
    if not parent_task:
        _set_main_task(ctx, user, task_id)
    write_debug_log("assign_sync_task_created", {**log, "taskId": task_id, "parentTaskId": parent_task,
                                                "main": not parent_task, "creator": creator_now, "wantedCreator": creator,
                                                "groupId": fields.get("GROUP_ID", ""), "deadline": fields.get("DEADLINE", ""),
                                                "status": (task or {}).get("status", ""), "attempts": attempts})
    _task_journal(ctx, task_key, "created", task_id, parentTaskId=parent_task, item_ids=list(want["lines"]),
                  userName=entry.get("name", ""))
    return {"ok": True, "taskId": task_id, "created": True, "status": (task or {}).get("status", "")}


def _sync_lines(ctx: _Context, task_key: tuple, task_id: str, row_lines: dict, want_lines: dict) -> dict:
    """The checklist of the task follows the subitems (or the items of a
    section): added, removed, checked and unchecked."""
    log = ctx.log(target=f"{task_key[0]}:{task_key[1]}", userId=task_key[2], taskId=task_id)
    lines = dict(row_lines)
    for item_id, line in want_lines.items():
        stored = lines.get(item_id)
        if stored is None or not stored.get("id"):
            body = bitrix_call("task.checklistitem.add", {"TASKID": task_id, "FIELDS": {"TITLE": line["title"][:250]}},
                               purpose="add a line", context={**log, "itemId": item_id})
            line_id = clean_cell_value(body.get("result")) if not body.get("error") else ""
            if not line_id:
                continue
            stored = {"id": line_id, "done": False, "title": line["title"]}
            lines[item_id] = stored
        if bool(stored.get("done")) != bool(line["done"]):
            method = "task.checklistitem.complete" if line["done"] else "task.checklistitem.renew"
            body = bitrix_call(method, {"TASKID": task_id, "ITEMID": stored["id"]},
                               purpose="check a line" if line["done"] else "uncheck a line", context={**log, "itemId": item_id})
            if not body.get("error"):
                stored["done"] = bool(line["done"])
    for item_id in [i for i in lines if i not in want_lines]:
        bitrix_call("task.checklistitem.delete", {"TASKID": task_id, "ITEMID": lines[item_id].get("id")},
                    purpose="remove a line", context={**log, "itemId": item_id})
        lines.pop(item_id)
    return lines


def _move_task(ctx: _Context, task_key: tuple, task_id: str, target_state: str, *, reason: str = "") -> dict:
    """Bring the task to what the checklist wants, from its real status."""
    log = ctx.log(target=f"{task_key[0]}:{task_key[1]}", userId=task_key[2], taskId=task_id)
    task = _task_get(task_id, log)
    if task is None:
        return {"ok": False, "missing": True}
    status = task["status"]
    steps: list[tuple[str, str]] = []
    if target_state == S_PROGRESS:
        if status in (T_CONTROL, T_DONE):
            steps = [("tasks.task.renew", "reopen"), ("tasks.task.start", "start")]
        elif status in (T_NEW, T_PENDING, T_DEFERRED):
            steps = [("tasks.task.start", "start")]
    elif target_state == S_PENDING:
        if status == T_PROGRESS:
            steps = [("tasks.task.pause", "back to waiting")]
        elif status in (T_CONTROL, T_DONE, T_DEFERRED):
            steps = [("tasks.task.renew", "reopen")]
    elif target_state == S_CONTROL:
        if status not in (T_CONTROL, T_DONE):
            steps = [("tasks.task.complete", "send for control")]
    elif target_state == S_CLOSED:
        if status != T_DONE:
            if reason:
                bitrix_call("task.commentitem.add", {"TASKID": task_id, "FIELDS": {"POST_MESSAGE": reason}},
                            purpose="tell why the task is closed", context=log)
            if status != T_CONTROL:
                steps.append(("tasks.task.complete", "complete"))
            steps.append(("tasks.task.approve", "accept"))
    errors = []
    for method, purpose in steps:
        body = bitrix_call(method, {"taskId": task_id}, purpose=purpose, context={**log, "from": status, "want": target_state})
        if body.get("error"):
            errors.append(method + " " + _error_text(body))
            if method == "tasks.task.approve":
                # Closed with control: the task waits for the check of its creator.
                continue
            break
    after = _task_get(task_id, log) if steps else task
    final = (after or {}).get("status", "")
    reached = {
        S_PROGRESS: final == T_PROGRESS,
        S_PENDING: final in (T_NEW, T_PENDING),
        S_CONTROL: final in (T_CONTROL, T_DONE),
        S_CLOSED: final in (T_CONTROL, T_DONE),
    }[target_state]
    write_debug_log("assign_sync_task_moved" if reached else "assign_sync_task_move_failed", {
        **log, "want": target_state, "from": status, "fromLabel": TASK_STATUS_LABELS.get(status, status),
        "to": final, "toLabel": TASK_STATUS_LABELS.get(final, final), "steps": [m for m, _ in steps], "errors": errors,
    })
    return {"ok": reached, "from": status, "to": final, "errors": errors, "responsibleId": (after or {}).get("responsibleId", "")}


_CLOSE_REASONS = {
    "unassigned": "Исполнитель снят с пункта в чек-листе — задача завершена автоматически.",
    "not_required": "Пункт переведён в «Не требуется» — задача завершена автоматически.",
    "deleted": "Пункт удалён из чек-листа — задача завершена автоматически.",
    "covered": "Подпункт теперь в чек-листе задачи по его пункту — эта задача завершена автоматически.",
    "section_empty": "В разделе не осталось пунктов задачи — задача завершена автоматически.",
}


def _close_reason(task_key: tuple, items: _Items, targets: dict) -> str:
    kind, target_id, user = task_key
    if kind == GROUP:
        users = targets.get(_target_key(GROUP, target_id)) or []
        return "section_empty" if any(u["userId"] == user for u in users) else "unassigned"
    if items.get(target_id) is None:
        return "deleted"
    if not items.active(target_id):
        return "not_required"
    eff = effective(targets, items)
    if user in {u["userId"] for u in (eff.get(target_id) or {}).get("users") or []}:
        return "covered"
    return "unassigned"


def _sync_task(ctx: _Context, task_key: tuple, row: dict | None, want: dict | None, targets: dict) -> dict:
    kind, target_id, user = task_key
    log = ctx.log(target=f"{kind}:{target_id}", userId=user)
    task_id = clean_cell_value((row or {}).get("task_id"))
    applied = clean_cell_value((row or {}).get("applied_state"))
    if want is None:
        if not task_id or applied == S_CLOSED:
            return {"ok": True, "skipped": True}
        reason = _close_reason(task_key, ctx.items, targets)
        result = _move_task(ctx, task_key, task_id, S_CLOSED, reason=_CLOSE_REASONS[reason])
        if result.get("ok") or result.get("missing"):
            _save_task_row(ctx.dialog_id, ctx.key, task_key, {"applied_state": S_CLOSED, "error": ""})
            _task_journal(ctx, task_key, "closed", task_id, reason=reason, status=result.get("to", ""),
                          item_ids=list(_json((row or {}).get("lines_json"), {})))
        else:
            _save_task_row(ctx.dialog_id, ctx.key, task_key, {"error": "; ".join(result.get("errors") or [])[:500]})
        return result
    created = False
    if not task_id:
        result = _create_task(ctx, task_key, want)
        if not result.get("ok"):
            return result
        task_id, applied, created = result["taskId"], S_PENDING, True
        row = {"lines_json": "{}", "responsible_id": user, "deadline": want["entry"].get("deadline") or ""}
    responsible = clean_cell_value(row.get("responsible_id")) or user
    if responsible != user:
        body = bitrix_call("tasks.task.update", {"taskId": task_id, "fields": {"RESPONSIBLE_ID": user}},
                           purpose="hand the task over", context={**log, "taskId": task_id, "from": responsible})
        if body.get("error") and "not found" in _error_text(body).lower():
            _save_task_row(ctx.dialog_id, ctx.key, task_key, {"task_id": "", "applied_state": "", "lines_json": "{}"})
            return _sync_task(ctx, task_key, None, want, targets)
        if not body.get("error"):
            bitrix_call("task.commentitem.add", {"TASKID": task_id, "FIELDS": {
                "POST_MESSAGE": f"Пункт передан в чек-листе: теперь исполнитель — {want['entry']['name']}."}},
                purpose="tell about the handover", context={**log, "taskId": task_id})
            _save_task_row(ctx.dialog_id, ctx.key, task_key, {"responsible_id": user, "error": ""})
            _task_journal(ctx, task_key, "responsible", task_id, fromUserId=responsible, responsibleId=user,
                          item_ids=list(want["lines"]), userName=want["entry"].get("name", ""))
            write_debug_log("assign_sync_task_handed_over", {**log, "taskId": task_id, "from": responsible})
    deadline = want["entry"].get("deadline") or ""
    if deadline and deadline != clean_cell_value(row.get("deadline")):
        body = bitrix_call("tasks.task.update", {"taskId": task_id, "fields": {"DEADLINE": _deadline_iso(deadline)}},
                           purpose="set the deadline", context={**log, "taskId": task_id})
        if not body.get("error"):
            _save_task_row(ctx.dialog_id, ctx.key, task_key, {"deadline": deadline})
    row_lines = _json(row.get("lines_json"), {})
    if row_lines or want["lines"]:
        lines = _sync_lines(ctx, task_key, task_id, row_lines, want["lines"])
        if lines != row_lines:
            _save_task_row(ctx.dialog_id, ctx.key, task_key, {"lines_json": json.dumps(lines, ensure_ascii=False)})
    result = {"ok": True, "taskId": task_id, "created": created}
    if want["state"] != applied:
        moved = _move_task(ctx, task_key, task_id, want["state"])
        if moved.get("missing"):
            write_debug_log("assign_sync_task_missing", {**log, "taskId": task_id, "want": want["state"]})
            _save_task_row(ctx.dialog_id, ctx.key, task_key, {"task_id": "", "applied_state": "", "lines_json": "{}"})
            return _sync_task(ctx, task_key, None, want, targets)
        if moved.get("ok"):
            _save_task_row(ctx.dialog_id, ctx.key, task_key, {"applied_state": want["state"], "error": ""})
            if not created or want["state"] != S_PENDING:
                _task_journal(ctx, task_key, want["state"], task_id, status=moved.get("to", ""),
                              reopened=applied in (S_CONTROL, S_CLOSED) and want["state"] in (S_PENDING, S_PROGRESS),
                              item_ids=list(want["lines"]), userName=want["entry"].get("name", ""))
        else:
            _save_task_row(ctx.dialog_id, ctx.key, task_key, {"error": "; ".join(moved.get("errors") or [])[:500]})
        result.update(moved)
    return result


def sync_checklist(dialog_id: str, checklist_key: str, *, reason: str = "") -> dict:
    """Bring the tasks of a checklist to what it says. Only the differences
    from what was applied before go to Bitrix: a task moved by hand in
    Bitrix stays where it is until its item changes."""
    dialog_id = normalize_dialog_id(dialog_id)
    key = normalize_checklist_key(checklist_key)
    if not tasks_enabled():
        write_debug_log("assign_sync_disabled", {"dialogId": dialog_id, "checklistKey": key})
        return {"ok": True, "disabled": True}
    with _sync_lock(dialog_id, key):
        data = _checklist(dialog_id, key)
        if not data.get("items"):
            write_debug_log("assign_sync_no_checklist", {"dialogId": dialog_id, "checklistKey": key})
            return {"ok": False, "error": "no checklist"}
        ctx = _Context(dialog_id, key, data)
        conn = _conn()
        try:
            targets = _read_committed(conn, dialog_id, key)
        finally:
            conn.close()
        wants, _covers = _desired(targets, ctx.items)
        rows = _task_rows(dialog_id, key)
        write_debug_log("assign_sync_started", ctx.log(
            reason=reason, projectGroupId=ctx.group_id, project=ctx.project(),
            wanted=[{"target": f"{k[0]}:{k[1]}", "userId": k[2], "state": w["state"], "lines": len(w["lines"])}
                    for k, w in wants.items()],
            known=[{"target": f"{k[0]}:{k[1]}", "userId": k[2], "taskId": r.get("task_id"), "applied": r.get("applied_state")}
                   for k, r in rows.items()],
        ))
        results = {}
        failed = []
        position = {item_id: index for index, item_id in enumerate(ctx.items.by_id)}

        def rank(task_key):
            # Main tasks first: the items of a person in the order of the
            # checklist, then his subitems, then his sections; closing last.
            kind, target_id, _user = task_key
            want = wants.get(task_key)
            if want is None:
                return (3, 0, task_key)
            if kind == GROUP:
                return (2, 0, task_key)
            return (1 if want.get("parent") else 0, position.get(target_id, 0), task_key)

        order = sorted(set(wants) | set(rows), key=rank)
        for task_key in order:
            try:
                result = _sync_task(ctx, task_key, rows.get(task_key), wants.get(task_key), targets)
            except Exception as exc:
                result = {"ok": False, "error": str(exc)[:300]}
                write_debug_log("assign_sync_task_error", ctx.log(target=f"{task_key[0]}:{task_key[1]}",
                                                                  userId=task_key[2], error=str(exc)[:500]))
            results[task_key] = result
            if not result.get("ok") and not result.get("skipped"):
                failed.append(f"{task_key[0]}:{task_key[1]}:{task_key[2]}")
        write_debug_log("assign_sync_finished", ctx.log(reason=reason, tasks=len(results), failed=failed))
        return {"ok": not failed, "failed": failed, "tasks": len(results)}


# ---------------------------------------------------------------------------
# The queue and the chat message
# ---------------------------------------------------------------------------

def _claim(row_id: int) -> bool:
    conn = _conn()
    try:
        cur = conn.execute(
            "UPDATE assignment_sync_queue SET status = 'running', updated_at = ? WHERE id = ? AND status IN ('pending', 'failed')",
            (_now(), row_id),
        )
        conn.commit()
        return int(cur.rowcount or 0) == 1
    finally:
        conn.close()


def _finish(row_id: int, ok: bool, error: str, attempts: int) -> None:
    now = datetime.now(timezone.utc)
    if ok:
        status, next_at = "done", ""
    elif attempts >= SYNC_ATTEMPTS:
        status, next_at = "gave_up", ""
    else:
        status = "failed"
        next_at = (now + timedelta(seconds=SYNC_RETRY_SECONDS[min(attempts, len(SYNC_RETRY_SECONDS)) - 1])).replace(
            microsecond=0).isoformat().replace("+00:00", "Z")
    conn = _conn()
    try:
        conn.execute(
            "UPDATE assignment_sync_queue SET status = ?, attempts = ?, error = ?, next_at = ?, updated_at = ? WHERE id = ?",
            (status, attempts, error[:500], next_at, _now(), row_id),
        )
        conn.commit()
    finally:
        conn.close()


def _run_queue_row(row: dict) -> dict:
    if not _claim(int(row["id"])):
        return {"ok": True, "skipped": True}
    attempts = int(row.get("attempts") or 0) + 1
    try:
        result = sync_checklist(row["dialog_id"], row["checklist_key"], reason=f"session {row.get('session_id') or ''}")
        error = "; ".join(result.get("failed") or []) or clean_cell_value(result.get("error"))
        ok = bool(result.get("ok"))
    except Exception as exc:
        ok, error = False, str(exc)
        write_debug_log("assign_sync_error", {"queueId": row["id"], "error": str(exc)[:500]})
    _finish(int(row["id"]), ok, error, attempts)
    return {"ok": ok, "error": error}


def deliver_session(session_id: str) -> dict:
    """After the commit of a session: its synchronization, then the chat
    message about who was assigned."""
    session_id = clean_cell_value(session_id)
    conn = _conn()
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM assignment_sync_queue WHERE session_id = ? AND status IN ('pending', 'failed') ORDER BY id",
            (session_id,),
        ).fetchall()]
    finally:
        conn.close()
    results = [_run_queue_row(row) for row in rows]
    message = {}
    try:
        message = send_session_message(session_id)
    except Exception as exc:
        message = {"ok": False, "error": str(exc)}
        write_debug_log("assign_message_failed", {"sessionId": session_id, "error": str(exc)[:500]})
    return {"ok": all(r.get("ok") for r in results), "synced": len(results), "message": message}


def _session_actions(session_id: str) -> list[dict]:
    conn = _conn()
    try:
        rows = conn.execute(
            """SELECT * FROM assignment_journal WHERE session_id = ? AND kind IN ('assign', 'delegate')
               ORDER BY id""", (session_id,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def send_session_message(session_id: str) -> dict:
    """A message of its own to the chat of the project: everyone assigned
    in the session, with the items and the links to the tasks."""
    from app.checklists.project_phases import base_dialog_id

    actions = _session_actions(session_id)
    if not actions:
        return {"ok": True, "skipped": "nothing assigned"}
    conn = _conn()
    try:
        sent = conn.execute("SELECT status FROM assignment_messages WHERE session_id = ?", (session_id,)).fetchone()
        waiting = conn.execute(
            "SELECT COUNT(*) AS n FROM assignment_sync_queue WHERE session_id = ? AND status IN ('pending', 'running', 'failed')",
            (session_id,),
        ).fetchone()["n"]
    finally:
        conn.close()
    if sent and sent["status"] == "sent":
        return {"ok": True, "skipped": "already sent"}
    if waiting:
        # The links to the tasks first: the message goes after a retry.
        write_debug_log("assign_message_waits", {"sessionId": session_id, "queued": waiting})
        return {"ok": True, "skipped": "tasks not ready"}
    by_checklist: dict[tuple[str, str], list[dict]] = {}
    for action in actions:
        by_checklist.setdefault((action["dialog_id"], action["checklist_key"]), []).append(action)
    blocks = []
    actor_name = ""
    dialog_for_chat = ""
    for (dialog_id, key), list_ in by_checklist.items():
        data = _checklist(dialog_id, key)
        ctx = _Context(dialog_id, key, data)
        rows = _task_rows(dialog_id, key)
        conn = _conn()
        try:
            targets = _read_committed(conn, dialog_id, key)
        finally:
            conn.close()
        _wants, covers = _desired(targets, ctx.items)
        people: dict[str, dict] = {}
        for action in list_:
            data_ = _json(action["data_json"], {})
            actor_name = actor_name or clean_cell_value(action["actor_name"])
            for user in data_.get("users") or []:
                person = people.setdefault(user["userId"], {"name": user["name"], "parts": []})
                if action["target_kind"] == GROUP:
                    label = f"раздел «{ctx.items.group_title(action['target_id'])}»"
                    cover = covers.get(("group:" + action["target_id"], user["userId"]))
                else:
                    label = ctx.items.name(action["target_id"])
                    cover = covers.get((action["target_id"], user["userId"]))
                task = rows.get(cover) if cover else None
                url = task_url(task.get("task_id"), user["userId"]) if task and task.get("task_id") else ""
                if action["kind"] == "delegate":
                    label += f" (передал {clean_cell_value((data_.get('from') or {}).get('name'))})"
                text = f"[URL={url}]{label}[/URL]" if url else label
                if text not in person["parts"]:
                    person["parts"].append(text)
        lines = [f"[USER={uid}]{p['name']}[/USER] — " + ", ".join(p["parts"]) for uid, p in people.items()]
        head = f"[b]Назначены исполнители — чек-лист {ctx.short_title}[/b]" + (f" ({ctx.phase})" if ctx.phase else "")
        blocks.append(head + "\n" + "\n".join(lines))
        dialog_for_chat = dialog_for_chat or base_dialog_id(dialog_id)
    message = "\n\n".join(blocks) + (f"\n\nНазначил: {actor_name}" if actor_name else "")
    if not tasks_enabled():
        write_debug_log("assign_message_disabled", {"sessionId": session_id})
        return {"ok": True, "skipped": "disabled"}
    body = bitrix_call("im.message.add", {"DIALOG_ID": dialog_for_chat, "MESSAGE": message},
                       purpose="tell the chat who is assigned", context={"sessionId": session_id, "dialogId": dialog_for_chat})
    ok = not body.get("error")
    now = _now()
    conn = _conn()
    try:
        conn.execute(
            """INSERT INTO assignment_messages(session_id, status, message_id, error, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(session_id) DO UPDATE SET status = excluded.status, message_id = excluded.message_id,
                   error = excluded.error, updated_at = excluded.updated_at""",
            (session_id, "sent" if ok else "failed", clean_cell_value(body.get("result")), _error_text(body), now, now),
        )
        conn.commit()
    finally:
        conn.close()
    write_debug_log("assign_message_sent" if ok else "assign_message_failed", {
        "sessionId": session_id, "dialogId": dialog_for_chat, "people": sum(len(b.split("\n")) - 1 for b in blocks),
        "error": _error_text(body), "text": message[:600],
    })
    return {"ok": ok, "error": _error_text(body)}


def process_due(limit: int = 20) -> int:
    """The queue rows a delivery left behind: a restart, a failed call."""
    now = _now()
    stale = (datetime.now(timezone.utc) - timedelta(minutes=2)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    conn = _conn()
    try:
        # A row stays «running» when the process stopped in the middle.
        conn.execute("UPDATE assignment_sync_queue SET status = 'failed', next_at = '' WHERE status = 'running' AND updated_at < ?",
                     ((datetime.now(timezone.utc) - timedelta(minutes=15)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),))
        conn.commit()
        rows = [dict(r) for r in conn.execute(
            """SELECT * FROM assignment_sync_queue
               WHERE (status = 'pending' AND created_at < ?) OR (status = 'failed' AND COALESCE(next_at, '') <= ?)
               ORDER BY id LIMIT ?""", (stale, now, limit),
        ).fetchall()]
    finally:
        conn.close()
    sessions = set()
    for row in rows:
        _run_queue_row(row)
        if row.get("session_id"):
            sessions.add(row["session_id"])
    for session_id in sessions:
        try:
            send_session_message(session_id)
        except Exception as exc:
            write_debug_log("assign_message_failed", {"sessionId": session_id, "error": str(exc)[:500]})
    return len(rows)


def _worker() -> None:
    while not _STOP.is_set():
        try:
            process_due()
        except Exception as exc:
            write_debug_log("assign_worker_error", {"error": str(exc)[:500]})
        _STOP.wait(WORKER_PASS_SECONDS)


def start_worker() -> None:
    global _THREAD
    with _THREAD_LOCK:
        if _THREAD and _THREAD.is_alive():
            return
        ensure_schema()
        _STOP.clear()
        _THREAD = threading.Thread(target=_worker, name="assignment-sync", daemon=True)
        _THREAD.start()


def stop_worker() -> None:
    _STOP.set()
