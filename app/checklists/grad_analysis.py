"""Граданализ (ГА): the checklists to КОПР Генплан and КОПР АР, ПБ, ОДИ in
the right column of every OPR checklist window.

Leaders and administrators change it, so do the architects of the project
(Алексей Кузьмин unless the project names others); GIPs see it; nobody else
does. ГА has its own lock: the architect and the GIP of a project edit ГА and
OPR at the same time.

A change goes to a draft of the edit session of the window; «Сохранить и
закрыть» commits the draft together with the session, «Отмена» drops it and
the files uploaded into it. The status of an item is set by hand only: «Да»,
«Нет» or «Не требуется»; an item never checked is white like «Нет» but
needs no note. «Проверяющий» gets a record per item whose status the session
changed: the final status, who and when it was last changed.

Files of ГА live on the application server only (no Yandex Disk). A link to
a file of the ИД checklist follows the series of that file: a new version
in ИД replaces it, a file deleted from ИД stays marked so.

The data are kept apart from the table of checklists: the Yandex Disk
workers walk that table and must never take ГА for a checklist with folders.
"""
from __future__ import annotations

import copy
import json
import re
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.settings import UPLOAD_ROOT
from app.checklists.grad_analysis_template import (
    BLOCKS,
    DERIVED_ITEM_IDS,
    GROUPS,
    template_items,
)
from app.checklists.utils import clean_cell_value, normalize_dialog_id


GA_KEY = "ga"
YES, NO, NONE, NR = "yes", "no", "", "nr"
STATUS_LABELS = {YES: "Да", NO: "Нет", NONE: "Нет", NR: "Не требуется"}
DEFAULT_ARCHITECTS = ({"userId": "114", "name": "Алексей Кузьмин"},)

MAX_NAME = 1000
MAX_LINK_TEXT = 2000
MAX_NOTE = 4000
MAX_ARCHITECTS = 20
MAX_FILE_NAME = 255
FILES_ROOT = UPLOAD_ROOT / "ga"

GROUP_IDS = tuple(group["id"] for group in GROUPS)
_GROUP_RANK = {group_id: index for index, group_id in enumerate(GROUP_IDS)}
_TEMPLATE = {item["id"]: item for item in template_items()}
_DRAFT_FIELDS = ("touchedAt", "touchedById", "touchedByName")
_WRITE_LOCK = threading.Lock()
_SAFE_PART_RE = re.compile(r"[^\w.-]+")


class GaError(RuntimeError):
    """A change that cannot be made; the text is for the user."""


class GaDenied(GaError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _text(value, limit: int) -> str:
    return str(value if value is not None else "").replace("\r\n", "\n").replace("\r", "\n").strip()[:limit]


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def ensure_schema(conn=None) -> None:
    own = conn is None
    conn = conn or get_conn()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS ga_checklists (
            dialog_id TEXT PRIMARY KEY, data_json TEXT NOT NULL,
            revision INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT '', updated_by TEXT NOT NULL DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS ga_drafts (
            session_id TEXT NOT NULL, dialog_id TEXT NOT NULL, data_json TEXT NOT NULL,
            base_revision INTEGER NOT NULL DEFAULT 0,
            user_id TEXT NOT NULL DEFAULT '', user_name TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY(session_id, dialog_id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS ga_architects (
            base_dialog_id TEXT PRIMARY KEY, users_json TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT '', updated_by TEXT NOT NULL DEFAULT '')""")
        if own:
            conn.commit()
    finally:
        if own:
            conn.close()


_schema_ready = False


def _conn():
    global _schema_ready
    conn = get_conn()
    if not _schema_ready:
        ensure_schema(conn)
        conn.commit()
        _schema_ready = True
    return conn


def _read_committed(conn, dialog_id: str) -> tuple[dict, int]:
    row = conn.execute(
        "SELECT data_json, revision FROM ga_checklists WHERE dialog_id = ?", (dialog_id,)
    ).fetchone()
    if not row:
        return normalize({}), 0
    try:
        return normalize(json.loads(row["data_json"] or "{}")), _int(row["revision"])
    except Exception:
        return normalize({}), _int(row["revision"])


def _read_draft(conn, session_id: str, dialog_id: str) -> dict | None:
    if not session_id:
        return None
    row = conn.execute(
        "SELECT data_json FROM ga_drafts WHERE session_id = ? AND dialog_id = ?",
        (session_id, dialog_id),
    ).fetchone()
    if not row:
        return None
    try:
        return normalize(json.loads(row["data_json"] or "{}"))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Items
# ---------------------------------------------------------------------------

def _clean_file(raw) -> dict | None:
    if not isinstance(raw, dict) or not clean_cell_value(raw.get("id")):
        return None
    kind = "link" if clean_cell_value(raw.get("kind")) == "link" else "upload"
    record = {
        "id": clean_cell_value(raw.get("id")),
        "kind": kind,
        "name": clean_cell_value(raw.get("name"))[:MAX_FILE_NAME] or "Файл",
        "size": max(0, _int(raw.get("size"))),
        "at": clean_cell_value(raw.get("at")),
        "byId": clean_cell_value(raw.get("byId")),
        "byName": clean_cell_value(raw.get("byName")),
    }
    if kind == "upload":
        record["storedName"] = clean_cell_value(raw.get("storedName"))
    else:
        for key in ("sourceDialogId", "sourceItemId", "seriesId", "documentId"):
            record[key] = clean_cell_value(raw.get(key))
    for key in ("archivedAt", "archivedById", "archivedByName", "reason", "replacedBy"):
        if clean_cell_value(raw.get(key)):
            record[key] = clean_cell_value(raw.get(key))
    return record


def _clean_review(raw) -> dict | None:
    if not isinstance(raw, dict) or not clean_cell_value(raw.get("at")):
        return None
    status = clean_cell_value(raw.get("status"))
    return {
        "status": status if status in STATUS_LABELS else NONE,
        "userId": clean_cell_value(raw.get("userId")),
        "userName": clean_cell_value(raw.get("userName")),
        "at": clean_cell_value(raw.get("at")),
    }


def _clean_item(raw: dict, template: dict | None) -> dict:
    status = clean_cell_value(raw.get("status"))
    item = {
        "id": clean_cell_value(raw.get("id")),
        "group": clean_cell_value(raw.get("group")),
        "parentId": clean_cell_value(raw.get("parentId")),
        "order": _int(raw.get("order"), 1000),
        "name": _text(raw.get("name"), MAX_NAME),
        "isCustom": template is None,
        "status": status if status in STATUS_LABELS else NONE,
        "linkText": _text(raw.get("linkText"), MAX_LINK_TEXT),
        "note": _text(raw.get("note"), MAX_NOTE),
        "files": [f for f in map(_clean_file, raw.get("files") or []) if f],
        "archive": [f for f in map(_clean_file, raw.get("archive") or []) if f],
        "reviews": [r for r in map(_clean_review, raw.get("reviews") or []) if r],
    }
    if template is not None:
        # The file decides the place and the text of its own items.
        item.update({
            "group": template["group"],
            "parentId": template["parentId"],
            "order": template["order"],
            "name": template["name"],
        })
    if item["group"] not in GROUP_IDS:
        item["group"] = GROUP_IDS[0]
    for key in _DRAFT_FIELDS:
        if clean_cell_value(raw.get(key)):
            item[key] = clean_cell_value(raw.get(key))
    return item


def _new_template_item(template: dict) -> dict:
    return _clean_item({
        "id": template["id"],
        "linkText": template["linkText"],
        "note": template["noteTemplate"],
    }, template)


def children_of(items: list[dict], parent_id: str) -> list[dict]:
    return sorted(
        [item for item in items if item["parentId"] == parent_id],
        key=lambda item: (item["order"], item["id"]),
    )


def _apply_derived(items: list[dict]) -> list[dict]:
    """Item 1 is «Да» when all its subitems in work are «Да»; a «Да» that
    lost a subitem becomes unchecked. Returns the items that changed."""
    changed = []
    for item in items:
        if item["id"] not in DERIVED_ITEM_IDS or item["parentId"] or item["status"] == NR:
            continue
        active = [child for child in children_of(items, item["id"]) if child["status"] != NR]
        if not active:
            continue
        if all(child["status"] == YES for child in active):
            derived = YES
        elif item["status"] == YES:
            derived = NONE
        else:
            derived = item["status"]
        if derived != item["status"]:
            item["status"] = derived
            changed.append(item)
    return changed


def _sorted(items: list[dict]) -> list[dict]:
    top = sorted(
        [item for item in items if not item["parentId"]],
        key=lambda item: (_GROUP_RANK.get(item["group"], 99), item["order"], item["id"]),
    )
    result = []
    for item in top:
        result.append(item)
        result.extend(children_of(items, item["id"]))
    placed = {id(item) for item in result}
    result.extend(item for item in items if id(item) not in placed)
    return result


def normalize(data: dict | None) -> dict:
    """Every item of the file, custom items kept; one level of subitems."""
    by_id: dict[str, dict] = {}
    for raw in (data or {}).get("items") or []:
        if not isinstance(raw, dict):
            continue
        item_id = clean_cell_value(raw.get("id"))
        if not item_id or item_id in by_id:
            continue
        template = _TEMPLATE.get(item_id)
        if template is None and not raw.get("isCustom"):
            continue
        by_id[item_id] = _clean_item(raw, template)
    for item_id, template in _TEMPLATE.items():
        if item_id not in by_id:
            by_id[item_id] = _new_template_item(template)
    items = list(by_id.values())
    for item in items:
        parent = by_id.get(item["parentId"]) if item["parentId"] else None
        if item["parentId"] and (parent is None or parent["parentId"] or parent is item):
            item["parentId"] = ""
        elif parent is not None:
            item["group"] = parent["group"]
    _apply_derived(items)
    return {"version": 1, "items": _sorted(items)}


def _hidden_ids(items: list[dict]) -> set[str]:
    """Subitems of a «Не требуется» item go with it."""
    nr_ids = {item["id"] for item in items if item["status"] == NR and not item["parentId"]}
    return {item["id"] for item in items if item["parentId"] in nr_ids}


def progress_of(data: dict) -> dict:
    """Items and subitems are separate positions; «Не требуется» and what
    goes with it are not counted."""
    items = data.get("items") or []
    hidden = _hidden_ids(items)
    positions = [item for item in items if item["status"] != NR and item["id"] not in hidden]
    done = sum(1 for item in positions if item["status"] == YES)
    total = len(positions)
    return {"done": done, "total": total, "percent": round(done * 100 / total) if total else None}


def _note_missing(item: dict) -> bool:
    note = item["note"].strip()
    template = clean_cell_value((_TEMPLATE.get(item["id"]) or {}).get("noteTemplate"))
    return not note or (bool(template) and note == template)


def missing_notes(data: dict) -> list[str]:
    """«Нет» needs a note (an untouched note template is not one)."""
    items = data.get("items") or []
    hidden = _hidden_ids(items)
    return [
        item["id"] for item in items
        if item["status"] == NO and item["id"] not in hidden and _note_missing(item)
    ]


# ---------------------------------------------------------------------------
# Rights and architects
# ---------------------------------------------------------------------------

def base_of(dialog_id: str) -> str:
    from app.checklists.project_phases import base_dialog_id

    return base_dialog_id(normalize_dialog_id(dialog_id))


def architects(base: str) -> list[dict]:
    conn = _conn()
    try:
        row = conn.execute("SELECT users_json FROM ga_architects WHERE base_dialog_id = ?", (base,)).fetchone()
    finally:
        conn.close()
    if not row:
        users = [dict(user) for user in DEFAULT_ARCHITECTS]
    else:
        try:
            users = [u for u in json.loads(row["users_json"] or "[]") if isinstance(u, dict)]
        except Exception:
            users = []
    return [
        {"userId": clean_cell_value(user.get("userId")), "name": clean_cell_value(user.get("name"))}
        for user in users if clean_cell_value(user.get("userId"))
    ]


def rights(dialog_id: str, user_id: str) -> dict:
    from app.checklists.project_leaders import is_project_leader
    from app.checklists.project_phases import manager_user_ids

    user = clean_cell_value(user_id)
    base = base_of(dialog_id)
    if not user or not base:
        return {"view": False, "edit": False, "manageArchitects": False, "defersOprLock": False}
    leader = is_project_leader(base, user)
    architect = user in {entry["userId"] for entry in architects(base)}
    gip = user in set(manager_user_ids(base))
    edit = leader or architect
    return {
        "view": edit or gip,
        "edit": edit,
        "manageArchitects": leader,
        # An architect who only checks ГА does not hold the OPR checklist
        # for the GIP while the window is open.
        "defersOprLock": architect and not leader and not gip,
    }


def _require(dialog_id: str, user_id: str, right: str) -> dict:
    result = rights(dialog_id, user_id)
    if not result[right]:
        raise GaDenied({
            "view": "Граданализ видят руководители, администраторы, архитекторы и ГИП проекта",
            "edit": "Граданализ заполняют руководители, администраторы и архитекторы проекта",
            "manageArchitects": "Архитекторов назначают руководители и администраторы",
        }[right])
    return result


def change_architects(dialog_id: str, op: str, *, user: dict, actor: dict) -> list[dict]:
    from app.checklists.bitrix_users import get_cached_bitrix_user

    _require(dialog_id, actor.get("userId"), "manageArchitects")
    base = base_of(dialog_id)
    target_id = clean_cell_value((user or {}).get("userId"))
    if not target_id.isdecimal() or int(target_id) <= 0:
        raise GaError("Выберите сотрудника из списка Битрикса")
    with _WRITE_LOCK:
        current = architects(base)
        if op == "add":
            if any(entry["userId"] == target_id for entry in current):
                return current
            if len(current) >= MAX_ARCHITECTS:
                raise GaError("Слишком много архитекторов")
            cached = get_cached_bitrix_user(target_id) or {}
            name = clean_cell_value(cached.get("name")) or _text((user or {}).get("name"), 200)
            current.append({"userId": target_id, "name": name or ("ID " + target_id)})
        elif op == "remove":
            current = [entry for entry in current if entry["userId"] != target_id]
        else:
            raise GaError("Неизвестное действие")
        conn = _conn()
        try:
            conn.execute(
                """INSERT INTO ga_architects(base_dialog_id, users_json, updated_at, updated_by)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(base_dialog_id) DO UPDATE SET users_json = excluded.users_json,
                       updated_at = excluded.updated_at, updated_by = excluded.updated_by""",
                (base, json.dumps(current, ensure_ascii=False), _now(), clean_cell_value(actor.get("userId"))),
            )
            conn.commit()
        finally:
            conn.close()
    write_debug_log("ga_architects_changed", {"base": base, "op": op, "userId": target_id,
                                              "by": clean_cell_value(actor.get("userId"))})
    return architects(base)


# ---------------------------------------------------------------------------
# Locks and sessions
# ---------------------------------------------------------------------------

def _lock_holder(conn, dialog_id: str, checklist_key: str) -> dict | None:
    try:
        row = conn.execute("""
            SELECT c.session_id, s.user_id, s.user_name
            FROM edit_session_checklists AS c
            JOIN edit_sessions AS s ON s.session_id = c.session_id
            WHERE c.dialog_id = ? AND c.checklist_key = ? AND c.status = 'locked'
              AND COALESCE(c.lock_id, '') <> ''
              AND s.status IN ('active', 'committing', 'rolling_back', 'error')
            ORDER BY c.updated_at DESC LIMIT 1
        """, (dialog_id, checklist_key)).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    return {
        "sessionId": clean_cell_value(row["session_id"]),
        "userId": clean_cell_value(row["user_id"]),
        "userName": clean_cell_value(row["user_name"]) or "Другой сотрудник",
    }


def _own_session(session_id: str, dialog_id: str, user_id: str) -> str:
    """The session of the window when it belongs to this user, else ""."""
    from app.checklists.edit_sessions import get_edit_session_for_actor

    session_id = clean_cell_value(session_id)
    if not session_id or not user_id:
        return ""
    try:
        record = get_edit_session_for_actor(session_id=session_id, dialog_id=dialog_id, user_id=user_id)
    except Exception:
        return ""
    return session_id if record.get("status") == "active" else ""


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

def _short_date(value: str) -> str:
    try:
        moment = datetime.fromisoformat(clean_cell_value(value).replace("Z", "+00:00"))
    except ValueError:
        return ""
    return moment.strftime("%d.%m")


class _IdSources:
    """ИД checklists that links of one view resolve against."""

    def __init__(self):
        self._cache: dict[str, dict] = {}

    def checklist(self, dialog_id: str) -> dict:
        from app.checklists.object_analysis import _saved_checklist

        if dialog_id not in self._cache:
            self._cache[dialog_id] = _saved_checklist(dialog_id, "id") or {}
        return self._cache[dialog_id]

    def item(self, dialog_id: str, item_id: str) -> dict | None:
        return next(
            (item for item in self.checklist(dialog_id).get("items") or []
             if clean_cell_value(item.get("id")) == item_id),
            None,
        )


def _file_path(dialog_id: str, file_id: str) -> str:
    return "api/ga/file?dialogId=" + dialog_id + "&fileId=" + file_id


def _id_document_path(dialog_id: str, item_id: str, document_id: str) -> str:
    from urllib.parse import urlencode

    return "api/checklist/file?" + urlencode({
        "dialogId": dialog_id, "checklistKey": "id", "itemId": item_id, "documentId": document_id,
    })


def _file_view(record: dict, dialog_id: str, sources: _IdSources) -> dict:
    view = {
        "id": record["id"], "kind": record["kind"], "name": record["name"], "size": record["size"],
        "at": record["at"], "byName": record["byName"],
        "archivedAt": record.get("archivedAt", ""), "archivedByName": record.get("archivedByName", ""),
        "reason": record.get("reason", ""),
    }
    if record["kind"] == "upload":
        view["url"] = _file_path(dialog_id, record["id"])
        return view
    item = sources.item(record["sourceDialogId"], record["sourceItemId"])
    current = None
    if item is not None:
        current = next(
            (doc for doc in item.get("documents") or []
             if clean_cell_value(doc.get("seriesId")) == record["seriesId"]),
            None,
        )
    view["sourceItemName"] = clean_cell_value((item or {}).get("name"))
    if current is None:
        view.update({"state": "deleted", "url": "", "mark": "файл удалён из ИД"})
        return view
    view["name"] = clean_cell_value(current.get("name")) or record["name"]
    view["url"] = _id_document_path(record["sourceDialogId"], record["sourceItemId"], clean_cell_value(current.get("id")))
    if clean_cell_value(current.get("id")) != record["documentId"]:
        day = _short_date(current.get("uploadedAt") or current.get("modifiedAt") or "")
        view.update({"state": "updated", "mark": "файл обновлён в ИД" + (" " + day if day else "")})
    else:
        view["state"] = "current"
    return view


def _item_view(item: dict, items: list[dict], hidden: set[str], dialog_id: str, sources: _IdSources) -> dict:
    children = children_of(items, item["id"])
    active_children = [child for child in children if child["status"] != NR]
    template = _TEMPLATE.get(item["id"]) or {}
    last = item["reviews"][-1] if item["reviews"] else None
    return {
        "id": item["id"],
        "group": item["group"],
        "parentId": item["parentId"],
        "name": item["name"],
        "isCustom": item["isCustom"],
        "status": item["status"],
        "derived": item["id"] in DERIVED_ITEM_IDS and bool(active_children),
        "childCount": len(children),
        "hidden": item["id"] in hidden,
        "linkText": item["linkText"],
        "note": item["note"],
        "noteTemplate": clean_cell_value(template.get("noteTemplate")),
        "noteMissing": item["status"] == NO and _note_missing(item),
        "files": [_file_view(record, dialog_id, sources) for record in item["files"]],
        "archiveCount": len(item["archive"]),
        "review": last,
        "reviewCount": len(item["reviews"]),
        "touched": bool(item.get("touchedAt")),
    }


def progress_counts(dialog_id: str) -> dict:
    """The committed ГА of a checklist: for the OPR progress and the panel."""
    dialog_id = normalize_dialog_id(dialog_id)
    conn = _conn()
    try:
        data, _revision = _read_committed(conn, dialog_id)
    finally:
        conn.close()
    return progress_of(data)


def state_payload(dialog_id: str, user_id: str, session_id: str = "") -> dict:
    dialog_id = normalize_dialog_id(dialog_id)
    user_id = clean_cell_value(user_id)
    own_session = _own_session(session_id, dialog_id, user_id)
    conn = _conn()
    try:
        committed, _revision = _read_committed(conn, dialog_id)
        draft = _read_draft(conn, own_session, dialog_id) if own_session else None
        ga_lock = _lock_holder(conn, dialog_id, GA_KEY)
        opr_lock = _lock_holder(conn, dialog_id, "opr")
    finally:
        conn.close()
    data = draft or committed
    user_rights = rights(dialog_id, user_id)
    payload = {"ok": True, "dialogId": dialog_id, "visible": user_rights["view"],
               "rights": user_rights, "progress": progress_of(data)}
    if not user_rights["view"]:
        return payload
    items = data["items"]
    hidden = _hidden_ids(items)
    sources = _IdSources()

    def other(lock):
        if not lock or lock["sessionId"] == own_session:
            return None
        return {"userId": lock["userId"], "userName": lock["userName"]}

    payload.update({
        "blocks": [dict(block) for block in BLOCKS],
        "groups": [dict(group) for group in GROUPS],
        "items": [_item_view(item, items, hidden, dialog_id, sources) for item in items],
        "architects": architects(base_of(dialog_id)),
        "draft": draft is not None,
        "missingNotes": missing_notes(data),
        "lockedBy": other(ga_lock),
        "oprLockedBy": other(opr_lock),
    })
    return payload


def item_payload(dialog_id: str, item_id: str, user_id: str, session_id: str = "") -> dict:
    """The folder of an item: its files with the archive, and the history
    of its checks."""
    dialog_id = normalize_dialog_id(dialog_id)
    _require(dialog_id, user_id, "view")
    own_session = _own_session(session_id, dialog_id, clean_cell_value(user_id))
    conn = _conn()
    try:
        data = (_read_draft(conn, own_session, dialog_id) if own_session else None) \
            or _read_committed(conn, dialog_id)[0]
    finally:
        conn.close()
    item = next((entry for entry in data["items"] if entry["id"] == clean_cell_value(item_id)), None)
    if item is None:
        raise GaError("Пункт не найден")
    sources = _IdSources()
    view = _item_view(item, data["items"], _hidden_ids(data["items"]), dialog_id, sources)
    view["archive"] = [_file_view(record, dialog_id, sources) for record in reversed(item["archive"])]
    view["reviews"] = list(reversed(item["reviews"]))
    view["statusLabels"] = dict(STATUS_LABELS)
    return {"ok": True, "item": view}


def id_files(dialog_id: str, user_id: str) -> dict:
    """Files of the ИД checklists of the project for «Сослаться на файл»:
    the first stage and the stage of this window."""
    from app.checklists.project_phases import phase_label

    dialog_id = normalize_dialog_id(dialog_id)
    _require(dialog_id, user_id, "edit")
    sources = _IdSources()
    result = []
    for source in dict.fromkeys([base_of(dialog_id), dialog_id]):
        checklist = sources.checklist(source)
        items = [item for item in checklist.get("items") or [] if isinstance(item, dict)]
        by_id = {clean_cell_value(item.get("id")): item for item in items}
        entries = []
        for item in items:
            documents = [
                {
                    "id": clean_cell_value(doc.get("id")),
                    "seriesId": clean_cell_value(doc.get("seriesId")),
                    "name": clean_cell_value(doc.get("name")),
                    "at": clean_cell_value(doc.get("uploadedAt")),
                    "url": _id_document_path(source, clean_cell_value(item.get("id")), clean_cell_value(doc.get("id"))),
                }
                for doc in item.get("documents") or [] if isinstance(doc, dict) and clean_cell_value(doc.get("id"))
            ]
            if not documents:
                continue
            parent = by_id.get(clean_cell_value(item.get("parentItemId")))
            entries.append({
                "itemId": clean_cell_value(item.get("id")),
                "name": clean_cell_value(item.get("name")),
                "parentName": clean_cell_value((parent or {}).get("name")),
                "documents": documents,
            })
        result.append({"dialogId": source, "label": phase_label(source) or "ИД проекта", "items": entries})
    return {"ok": True, "sources": result}


# ---------------------------------------------------------------------------
# Changes
# ---------------------------------------------------------------------------

def _find(items: list[dict], item_id: str) -> dict:
    item = next((entry for entry in items if entry["id"] == clean_cell_value(item_id)), None)
    if item is None:
        raise GaError("Пункт не найден")
    return item


def _touch(item: dict, actor: dict, now: str) -> None:
    item["touchedAt"] = now
    item["touchedById"] = clean_cell_value(actor.get("userId"))
    item["touchedByName"] = clean_cell_value(actor.get("userName")) or "Пользователь"


def _set_status(items: list[dict], item: dict, status: str, actor: dict, now: str) -> None:
    if item["id"] in _hidden_ids(items):
        raise GaError("Пункт убран в «Не требуется» вместе со своим пунктом")
    if (
        status == YES
        and item["id"] in DERIVED_ITEM_IDS
        and any(child["status"] != NR for child in children_of(items, item["id"]))
    ):
        raise GaError("«Да» у этого пункта ставится само, когда все его подпункты отмечены «Да»")
    if item["status"] != status:
        item["status"] = status
        _touch(item, actor, now)


def _new_id(items: list[dict]) -> str:
    taken = {item["id"] for item in items}
    while True:
        candidate = "ga_" + uuid.uuid4().hex[:10]
        if candidate not in taken:
            return candidate


def _apply(data: dict, op: str, payload: dict, actor: dict, dialog_id: str, now: str) -> dict:
    items = data["items"]
    if op in {"status", "return"}:
        item = _find(items, payload.get("itemId"))
        if op == "return":
            if item["status"] != NR:
                raise GaError("Пункт не в «Не требуется»")
            status = NONE
        else:
            status = clean_cell_value(payload.get("status"))
            if status not in {YES, NO, NR}:
                raise GaError("Неизвестный статус")
        _set_status(items, item, status, actor, now)
    elif op == "note":
        _find(items, payload.get("itemId"))["note"] = _text(payload.get("text"), MAX_NOTE)
    elif op == "linkText":
        _find(items, payload.get("itemId"))["linkText"] = _text(payload.get("text"), MAX_LINK_TEXT)
    elif op == "rename":
        item = _find(items, payload.get("itemId"))
        if not item["isCustom"]:
            raise GaError("Пункты из чек-листа не переименовываются")
        name = _text(payload.get("name"), MAX_NAME)
        if not name:
            raise GaError("Введите описание требования")
        item["name"] = name
    elif op == "add":
        name = _text(payload.get("name"), MAX_NAME)
        if not name:
            raise GaError("Введите описание требования")
        parent_id = clean_cell_value(payload.get("parentId"))
        if parent_id:
            parent = _find(items, parent_id)
            if parent["parentId"]:
                raise GaError("У подпункта не бывает своих подпунктов")
            if parent["status"] == NR:
                raise GaError("Пункт в «Не требуется»: сначала верните его")
            group_id = parent["group"]
            siblings = children_of(items, parent_id)
        else:
            group_id = clean_cell_value(payload.get("groupId"))
            if group_id not in GROUP_IDS:
                raise GaError("Неизвестный раздел")
            siblings = [item for item in items if item["group"] == group_id and not item["parentId"]]
        items.append(_clean_item({
            "id": _new_id(items), "group": group_id, "parentId": parent_id,
            "order": max([entry["order"] for entry in siblings] or [0]) + 1,
            "name": name, "isCustom": True,
        }, None))
    elif op == "delete":
        item = _find(items, payload.get("itemId"))
        if not item["isCustom"]:
            raise GaError("Пункты из чек-листа не удаляются — переведите пункт в «Не требуется»")
        removed = {item["id"]} | {child["id"] for child in children_of(items, item["id"])}
        data["items"] = items = [entry for entry in items if entry["id"] not in removed]
    elif op == "link":
        item = _find(items, payload.get("itemId"))
        source = normalize_dialog_id(payload.get("sourceDialogId"))
        if source not in {base_of(dialog_id), dialog_id}:
            raise GaError("Можно сослаться только на файлы ИД этого проекта")
        sources = _IdSources()
        id_item = sources.item(source, clean_cell_value(payload.get("sourceItemId")))
        document = next(
            (doc for doc in (id_item or {}).get("documents") or []
             if clean_cell_value(doc.get("id")) == clean_cell_value(payload.get("documentId"))),
            None,
        )
        if document is None:
            raise GaError("Файл не найден в ИД — обновите список")
        series_id = clean_cell_value(document.get("seriesId")) or clean_cell_value(document.get("id"))
        if any(f["kind"] == "link" and f["sourceDialogId"] == source and f["seriesId"] == series_id
               for f in item["files"]):
            raise GaError("Ссылка на этот файл уже есть")
        item["files"].append(_clean_file({
            "id": uuid.uuid4().hex, "kind": "link", "name": clean_cell_value(document.get("name")),
            "size": document.get("size"), "at": now,
            "byId": actor.get("userId"), "byName": actor.get("userName"),
            "sourceDialogId": source, "sourceItemId": clean_cell_value(id_item.get("id")),
            "seriesId": series_id, "documentId": clean_cell_value(document.get("id")),
        }))
    elif op == "archiveFile":
        item = _find(items, payload.get("itemId"))
        record = next((f for f in item["files"] if f["id"] == clean_cell_value(payload.get("fileId"))), None)
        if record is None:
            raise GaError("Файл не найден")
        item["files"] = [f for f in item["files"] if f is not record]
        item["archive"].append({**record, "archivedAt": now, "archivedById": clean_cell_value(actor.get("userId")),
                                "archivedByName": clean_cell_value(actor.get("userName")), "reason": "archived"})
    elif op == "upload":
        item = _find(items, payload.get("itemId"))
        record = payload["record"]
        replace_id = clean_cell_value(payload.get("replaceFileId"))
        if replace_id:
            old = next((f for f in item["files"] if f["id"] == replace_id), None)
            if old is None:
                raise GaError("Заменяемый файл не найден")
            item["files"] = [record if f is old else f for f in item["files"]]
            item["archive"].append({**old, "archivedAt": now, "archivedById": clean_cell_value(actor.get("userId")),
                                    "archivedByName": clean_cell_value(actor.get("userName")),
                                    "reason": "replaced", "replacedBy": record["id"]})
        else:
            item["files"].append(record)
    else:
        raise GaError("Неизвестное действие")
    for parent in _apply_derived(items):
        _touch(parent, actor, now)
    return normalize(data)


def mutate(dialog_id: str, session_id: str, actor: dict, op: str, payload: dict) -> dict:
    """A change of the draft of the edit session (the lock of ГА is taken
    on the first change)."""
    from app.checklists.edit_session_changes import acquire_checklist_for_edit_session

    dialog_id = normalize_dialog_id(dialog_id)
    session_id = clean_cell_value(session_id)
    actor = {"userId": clean_cell_value(actor.get("userId")),
             "userName": clean_cell_value(actor.get("userName")) or "Пользователь"}
    if not dialog_id or not session_id:
        raise GaError("Нет сессии редактирования")
    _require(dialog_id, actor["userId"], "edit")
    acquire_checklist_for_edit_session(
        session_id=session_id, dialog_id=dialog_id, checklist_key=GA_KEY,
        user_id=actor["userId"], user_name=actor["userName"],
    )
    now = _now()
    with _WRITE_LOCK:
        conn = _conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT status FROM edit_sessions WHERE session_id = ?", (session_id,)).fetchone()
            if not row or row["status"] != "active":
                raise GaError("Сессия редактирования уже завершена")
            draft = _read_draft(conn, session_id, dialog_id)
            base_revision = None
            if draft is None:
                draft, base_revision = _read_committed(conn, dialog_id)
            result = _apply(copy.deepcopy(draft), op, payload, actor, dialog_id, now)
            if base_revision is None:
                conn.execute(
                    "UPDATE ga_drafts SET data_json = ?, updated_at = ? WHERE session_id = ? AND dialog_id = ?",
                    (json.dumps(result, ensure_ascii=False), now, session_id, dialog_id),
                )
            else:
                conn.execute(
                    """INSERT INTO ga_drafts(session_id, dialog_id, data_json, base_revision,
                                             user_id, user_name, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (session_id, dialog_id, json.dumps(result, ensure_ascii=False), base_revision,
                     actor["userId"], actor["userName"], now),
                )
                # A lock with changes stays with the session until Save or
                # Cancel, like the lock of a checklist.
                conn.execute(
                    """UPDATE edit_session_checklists SET mutation_count = COALESCE(mutation_count, 0) + 1
                       WHERE session_id = ? AND dialog_id = ? AND checklist_key = ?""",
                    (session_id, dialog_id, GA_KEY),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    write_debug_log("ga_changed", {"dialogId": dialog_id, "sessionId": session_id, "op": op,
                                   "itemId": clean_cell_value(payload.get("itemId")), "userId": actor["userId"]})
    return state_payload(dialog_id, actor["userId"], session_id)


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

def _dialog_dir(dialog_id: str) -> Path:
    return FILES_ROOT / (_SAFE_PART_RE.sub("_", dialog_id) or "_")


def store_upload(dialog_id: str, session_id: str, actor: dict, item_id: str,
                 file_name: str, stream, replace_file_id: str = "") -> dict:
    from app.checklists.document_names import safe_file_name

    dialog_id = normalize_dialog_id(dialog_id)
    _require(dialog_id, clean_cell_value(actor.get("userId")), "edit")
    try:
        name = safe_file_name(file_name)[:MAX_FILE_NAME]
    except ValueError as exc:
        raise GaError(str(exc)) from exc
    file_id = uuid.uuid4().hex
    stored_name = file_id + "_" + (_SAFE_PART_RE.sub("_", name) or "file")[-120:]
    folder = _dialog_dir(dialog_id)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / stored_name
    size = 0
    with path.open("wb") as target:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            target.write(chunk)
    record = _clean_file({
        "id": file_id, "kind": "upload", "name": name, "size": size, "at": _now(),
        "byId": actor.get("userId"), "byName": actor.get("userName"), "storedName": stored_name,
    })
    try:
        return mutate(dialog_id, session_id, actor, "upload", {
            "itemId": item_id, "record": record, "replaceFileId": replace_file_id,
        })
    except Exception:
        path.unlink(missing_ok=True)
        raise


def _referenced_stored_names(conn, dialog_id: str) -> set[str]:
    datas = [_read_committed(conn, dialog_id)[0]]
    for row in conn.execute("SELECT data_json FROM ga_drafts WHERE dialog_id = ?", (dialog_id,)).fetchall():
        try:
            datas.append(normalize(json.loads(row["data_json"] or "{}")))
        except Exception:
            continue
    names = set()
    for data in datas:
        for item in data["items"]:
            for record in item["files"] + item["archive"]:
                if record["kind"] == "upload" and record.get("storedName"):
                    names.add(record["storedName"])
    return names


def _purge_files(dialog_id: str, stored_names: list[str]) -> None:
    """Files of a cancelled draft that nothing refers to any more."""
    conn = _conn()
    try:
        referenced = _referenced_stored_names(conn, dialog_id)
    finally:
        conn.close()
    removed = 0
    for name in stored_names:
        if name in referenced or "/" in name or "\\" in name or name in {"", ".", ".."}:
            continue
        try:
            (_dialog_dir(dialog_id) / name).unlink(missing_ok=True)
            removed += 1
        except OSError as exc:
            write_debug_log("ga_file_purge_failed", {"dialogId": dialog_id, "file": name, "error": str(exc)})
    if removed:
        write_debug_log("ga_files_purged", {"dialogId": dialog_id, "count": removed})


def file_for(dialog_id: str, file_id: str, user_id: str, session_id: str = "") -> tuple[Path, str]:
    dialog_id = normalize_dialog_id(dialog_id)
    _require(dialog_id, user_id, "view")
    own_session = _own_session(session_id, dialog_id, clean_cell_value(user_id))
    conn = _conn()
    try:
        datas = [_read_committed(conn, dialog_id)[0]]
        draft = _read_draft(conn, own_session, dialog_id) if own_session else None
        if draft:
            datas.insert(0, draft)
    finally:
        conn.close()
    for data in datas:
        for item in data["items"]:
            for record in item["files"] + item["archive"]:
                if record["id"] == clean_cell_value(file_id) and record["kind"] == "upload":
                    path = _dialog_dir(dialog_id) / record["storedName"]
                    if record["storedName"] and path.is_file():
                        return path, record["name"]
                    raise GaError("Файл не найден на сервере")
    raise GaError("Файл не найден")


# ---------------------------------------------------------------------------
# Save and Cancel of the edit session
# ---------------------------------------------------------------------------

def _committed_form(data: dict) -> dict:
    """A draft as it is kept: a changed status becomes a record of
    «Проверяющий»."""
    for item in data["items"]:
        if item.get("touchedAt"):
            item["reviews"].append({
                "status": item["status"],
                "userId": item.get("touchedById", ""),
                "userName": item.get("touchedByName", ""),
                "at": item["touchedAt"],
            })
        for key in _DRAFT_FIELDS:
            item.pop(key, None)
    return data


def finalize_drafts(conn, session_id: str, *, commit: bool, now: str) -> None:
    """Called inside the transaction that commits or rolls back the session."""
    try:
        rows = conn.execute(
            "SELECT dialog_id, data_json, base_revision, user_id FROM ga_drafts WHERE session_id = ?",
            (session_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return
    purge: dict[str, list[str]] = {}
    for row in rows:
        dialog_id = row["dialog_id"]
        try:
            draft = normalize(json.loads(row["data_json"] or "{}"))
        except Exception:
            continue
        committed_row = conn.execute(
            "SELECT data_json, revision FROM ga_checklists WHERE dialog_id = ?", (dialog_id,)
        ).fetchone()
        revision = _int(committed_row["revision"]) if committed_row else 0
        if commit:
            if revision != _int(row["base_revision"]):
                # The lock of ГА prevents this; the draft still wins.
                write_debug_log("ga_commit_over_newer_revision", {
                    "dialogId": dialog_id, "sessionId": session_id,
                    "baseRevision": _int(row["base_revision"]), "revision": revision,
                })
            conn.execute(
                """INSERT INTO ga_checklists(dialog_id, data_json, revision, updated_at, updated_by)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(dialog_id) DO UPDATE SET data_json = excluded.data_json,
                       revision = excluded.revision, updated_at = excluded.updated_at,
                       updated_by = excluded.updated_by""",
                (dialog_id, json.dumps(_committed_form(draft), ensure_ascii=False), revision + 1,
                 now, clean_cell_value(row["user_id"])),
            )
        else:
            committed = normalize(json.loads(committed_row["data_json"])) if committed_row else normalize({})
            kept = {
                record["storedName"] for item in committed["items"]
                for record in item["files"] + item["archive"] if record["kind"] == "upload"
            }
            purge[dialog_id] = [
                record["storedName"] for item in draft["items"]
                for record in item["files"] + item["archive"]
                if record["kind"] == "upload" and record.get("storedName") and record["storedName"] not in kept
            ]
    conn.execute("DELETE FROM ga_drafts WHERE session_id = ?", (session_id,))
    for dialog_id, names in purge.items():
        if names:
            # After the transaction: the files are removed once nothing
            # refers to them.
            timer = threading.Timer(1.0, _purge_files, args=(dialog_id, names))
            timer.daemon = True
            timer.start()
    if rows:
        write_debug_log("ga_drafts_finalized", {"sessionId": session_id, "commit": commit, "drafts": len(rows)})


def session_check(dialog_id: str, session_id: str, user_id: str) -> dict:
    """Before Save: «Нет» without a note stops it."""
    dialog_id = normalize_dialog_id(dialog_id)
    own_session = _own_session(session_id, dialog_id, clean_cell_value(user_id))
    if not own_session:
        return {"ok": True, "draft": False, "missingNotes": []}
    conn = _conn()
    try:
        draft = _read_draft(conn, own_session, dialog_id)
    finally:
        conn.close()
    if draft is None:
        return {"ok": True, "draft": False, "missingNotes": []}
    return {"ok": True, "draft": True, "missingNotes": missing_notes(draft)}
