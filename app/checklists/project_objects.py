"""Bitrix24 objects of a project (smart process «Объект Рабочее название»).

The project context (``bitrix`` of the base chat dialog) keeps the object id
entered by n8n or in the admin panel (``objectItemId``). Its card and the
cards listed in its «Связанный объект» field are read once and cached in the
same context; the admin panel button «Подтянуть данные из объекта» reads them
again. Nothing here calls Bitrix while a checklist is shown.

From the cached cards:

* the main object: the earliest created object with ОПР/ПД/РД, otherwise the
  earliest created one; an administrator may pin another one;
* the object of every checklist by its «Тип Работ» and the people of its
  ГИП field («Главный Концептолог» / «Главный Дизайнер» in Концепция and
  Дизайн): Ответственный and Руководитель рабочей группы of that object;
  Концепция also gets «Ответственный за Концепцию/Эскиз» of the ОПР/ПД/РД
  objects;
* the legal name and the cipher: an administrator or a ГИП pins one of the
  variants for the whole project when the objects differ.
"""
from __future__ import annotations

import re
import threading
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.checklists.utils import clean_cell_value, normalize_checklist_key, normalize_dialog_id


OBJECT_ENTITY_TYPE_ID = 1064

FIELD_LEGAL_NAME = "ufCrm18_1771309107"
FIELD_CIPHER = "ufCrm18_1791257496231"
FIELD_WORK_GROUP_LEADER = "ufCrm18_1774403893"
FIELD_CONCEPT_RESPONSIBLE = "ufCrm18_1785981004"
FIELD_WORK_TYPES = "ufCrm18_1791268015"
FIELD_RELATED = "ufCrm18_1791267888"

WORK_TYPE_CONCEPT = 2480
WORK_TYPE_OPR = 2478
WORK_TYPE_PD = 2486
WORK_TYPE_RD = 2488
WORK_TYPE_DESIGN = 2482
WORK_TYPE_PPT = 2484
WORK_TYPE_NAMES = {
    WORK_TYPE_CONCEPT: "Концепция",
    WORK_TYPE_OPR: "ОПР",
    WORK_TYPE_PD: "ПД",
    WORK_TYPE_RD: "РД",
    WORK_TYPE_DESIGN: "Дизайн",
    WORK_TYPE_PPT: "ППТ",
}
PROJECT_WORK_TYPES = frozenset({WORK_TYPE_OPR, WORK_TYPE_PD, WORK_TYPE_RD})

# The object of a checklist: the first object having one of these work types.
CHECKLIST_WORK_TYPES = {
    "id": (WORK_TYPE_OPR, WORK_TYPE_PD, WORK_TYPE_RD),
    "opr": (WORK_TYPE_OPR,),
    "p": (WORK_TYPE_PD,),
    "r": (WORK_TYPE_RD,),
    "concept": (WORK_TYPE_CONCEPT,),
    "design": (WORK_TYPE_DESIGN,),
}
PEOPLE_FIELD_LABELS = {
    "concept": "Главный Концептолог",
    "design": "Главный Дизайнер",
}
DEFAULT_PEOPLE_FIELD_LABEL = "ГИП"

ROLE_ASSIGNED = "Ответственный"
ROLE_WORK_GROUP_LEADER = "Руководитель рабочей группы"
ROLE_CONCEPT_RESPONSIBLE = "Ответственный за Концепцию/Эскиз"

# An automatic first read that failed is retried after this many seconds.
AUTO_RETRY_SECONDS = 3600
MAX_RELATED_OBJECTS = 30

_SAVE_LOCK = threading.Lock()
_FETCH_LOCKS_GUARD = threading.Lock()
_FETCH_LOCKS: dict[str, threading.Lock] = {}


class ProjectObjectError(RuntimeError):
    """A refused request; the message is for the user."""


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_time(value: Any) -> datetime | None:
    text = clean_cell_value(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _positive_int(value: Any) -> int:
    try:
        number = int(str(value if value is not None else "").strip())
    except Exception:
        return 0
    return number if number > 0 else 0


def _base(dialog_id: str) -> str:
    from app.checklists.project_phases import base_dialog_id

    return base_dialog_id(normalize_dialog_id(dialog_id))


# ---------------------------------------------------------------------------
# Bitrix values
# ---------------------------------------------------------------------------

def _original_uf_name(camel: str) -> str:
    # ufCrm18_1771309107 -> UF_CRM_18_1771309107
    match = re.match(r"^ufCrm(\d+)_(\w+)$", camel)
    if not match:
        return camel
    return f"UF_CRM_{match.group(1)}_{match.group(2)}".upper()


def _field(item: dict, camel: str) -> Any:
    if camel in item:
        return item.get(camel)
    return item.get(_original_uf_name(camel))


def _values(value: Any) -> list:
    if value is None or value is False:
        return []
    if isinstance(value, (list, tuple, set)):
        result = []
        for entry in value:
            result.extend(_values(entry))
        return result
    if isinstance(value, dict):
        for key in ("id", "ID", "value", "VALUE"):
            if key in value:
                return _values(value.get(key))
        return []
    if isinstance(value, str):
        parts = [part.strip() for part in re.split(r"[,;\s]+", value) if part.strip()]
        return parts
    return [value]


def _text_value(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        for entry in value:
            text = _text_value(entry)
            if text:
                return text
        return ""
    if isinstance(value, dict):
        return clean_cell_value(value.get("value") or value.get("VALUE") or value.get("title"))
    return " ".join(clean_cell_value(value).split())


def parse_user_ids(value: Any) -> list[str]:
    result: list[str] = []
    for entry in _values(value):
        text = clean_cell_value(entry)
        match = re.match(r"^(?:user_|U)?(\d+)$", text, re.IGNORECASE)
        if match and int(match.group(1)) > 0 and match.group(1) not in result:
            result.append(match.group(1))
    return result


def parse_work_types(value: Any) -> list[int]:
    result: list[int] = []
    for entry in _values(value):
        number = _positive_int(entry)
        if number and number not in result:
            result.append(number)
    return result


def parse_object_ids(value: Any, entity_type_id: int = OBJECT_ENTITY_TYPE_ID) -> list[int]:
    """Ids of objects in a CRM link field.

    Plain ids, ``T<hex type>_<id>`` (the CRM field of a smart process) and
    ``DYNAMIC_<type>_<id>``; links to other entities are skipped.
    """
    result: list[int] = []
    for entry in _values(value):
        text = clean_cell_value(entry)
        number = 0
        if re.fullmatch(r"\d+", text):
            number = int(text)
        else:
            match = re.fullmatch(r"T([0-9a-fA-F]+)_(\d+)", text)
            if match and int(match.group(1), 16) == int(entity_type_id):
                number = int(match.group(2))
            match = re.fullmatch(r"DYNAMIC_(\d+)_(\d+)", text, re.IGNORECASE)
            if match and int(match.group(1)) == int(entity_type_id):
                number = int(match.group(2))
        if number > 0 and number not in result:
            result.append(number)
    return result


def _response_item(response: Any) -> dict:
    if not isinstance(response, dict):
        return {}
    result = response.get("result")
    if isinstance(result, dict) and isinstance(result.get("item"), dict):
        return dict(result.get("item") or {})
    if isinstance(response.get("item"), dict):
        return dict(response.get("item") or {})
    return {}


def _response_error(response: Any) -> str:
    if not isinstance(response, dict):
        return "Битрикс24 вернул неожиданный ответ"
    if response.get("error"):
        return clean_cell_value(
            response.get("error_description") or response.get("error")
        ) or "Ошибка Битрикс24"
    if response.get("http_status"):
        return f"Битрикс24 ответил HTTP {response.get('http_status')}"
    return ""


def normalize_object_item(item: dict, object_id: int) -> dict:
    return {
        "id": _positive_int(item.get("id") or item.get("ID")) or int(object_id),
        "title": _text_value(item.get("title") or item.get("TITLE")),
        "legalName": _text_value(_field(item, FIELD_LEGAL_NAME)),
        "cipher": _text_value(_field(item, FIELD_CIPHER)),
        "assignedById": (parse_user_ids(item.get("assignedById") or item.get("ASSIGNED_BY_ID")) or [""])[0],
        "workGroupLeaderIds": parse_user_ids(_field(item, FIELD_WORK_GROUP_LEADER)),
        "conceptResponsibleIds": parse_user_ids(_field(item, FIELD_CONCEPT_RESPONSIBLE)),
        "workTypes": parse_work_types(_field(item, FIELD_WORK_TYPES)),
        "relatedIds": parse_object_ids(_field(item, FIELD_RELATED)),
        "createdTime": clean_cell_value(item.get("createdTime") or item.get("CREATED_TIME")),
        "error": "",
    }


def fetch_object(object_id: int) -> tuple[dict | None, str]:
    from app.bitrix.client import bitrix_webhook_call

    try:
        response = bitrix_webhook_call(
            "crm.item.get",
            {
                "entityTypeId": OBJECT_ENTITY_TYPE_ID,
                "id": int(object_id),
                "useOriginalUfNames": "N",
            },
        )
    except Exception as exc:
        return None, f"Битрикс24 недоступен: {exc}"
    error = _response_error(response)
    if error:
        return None, error
    item = _response_item(response)
    if not item:
        return None, "Объект не найден"
    return normalize_object_item(item, object_id), ""


def _resolve_user_names(user_ids: list[str]) -> dict[str, str]:
    from app.bitrix.client import bitrix_webhook_call
    from app.checklists.bitrix_users import (
        get_cached_bitrix_user,
        normalize_bitrix_user,
        synchronize_bitrix_users,
    )

    names: dict[str, str] = {}
    missing: list[str] = []
    for user_id in user_ids:
        cached = get_cached_bitrix_user(user_id)
        if cached and clean_cell_value(cached.get("name")):
            names[user_id] = clean_cell_value(cached.get("name"))
        else:
            missing.append(user_id)
    if missing:
        try:
            synchronize_bitrix_users(force=True)
        except Exception as exc:
            write_debug_log("project_objects_user_sync_failed", {"error": str(exc)})
        still_missing = []
        for user_id in missing:
            cached = get_cached_bitrix_user(user_id)
            if cached and clean_cell_value(cached.get("name")):
                names[user_id] = clean_cell_value(cached.get("name"))
            else:
                still_missing.append(user_id)
        # Dismissed employees are not in the cache: ask for each one.
        for user_id in still_missing:
            try:
                response = bitrix_webhook_call("user.get", {"ID": user_id})
                rows = response.get("result") if isinstance(response, dict) else None
                user = normalize_bitrix_user(rows[0]) if isinstance(rows, list) and rows else None
                if user and clean_cell_value(user.get("name")):
                    names[user_id] = clean_cell_value(user.get("name"))
            except Exception as exc:
                write_debug_log("project_objects_user_get_failed", {"userId": user_id, "error": str(exc)})
    return names


# ---------------------------------------------------------------------------
# Context storage
# ---------------------------------------------------------------------------

def _load_bitrix(base: str) -> dict:
    from app.checklists.storage import get_project_storage_context

    context = get_project_storage_context(base) or {}
    bitrix = context.get("bitrix")
    return dict(bitrix) if isinstance(bitrix, dict) else {}


def _update_bitrix(base: str, changes: dict) -> dict:
    """Merge keys into ``bitrix`` of the base context without touching the
    rest of the row (n8n may rewrite the row at the same time)."""
    import json

    with _SAVE_LOCK:
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT bitrix_json FROM project_storage_contexts WHERE dialog_id = ?",
                (base,),
            ).fetchone()
            if not row:
                raise ProjectObjectError("Контекст проекта не найден")
            try:
                bitrix = json.loads(row["bitrix_json"] or "{}")
            except Exception:
                bitrix = {}
            if not isinstance(bitrix, dict):
                bitrix = {}
            bitrix.update(changes)
            conn.execute(
                "UPDATE project_storage_contexts SET bitrix_json = ? WHERE dialog_id = ?",
                (json.dumps(bitrix, ensure_ascii=False), base),
            )
            conn.commit()
        finally:
            conn.close()
    return bitrix


def _cached_objects(bitrix: dict) -> list[dict]:
    objects = bitrix.get("objects")
    if not isinstance(objects, list):
        return []
    return [entry for entry in objects if isinstance(entry, dict) and _positive_int(entry.get("id"))]


# ---------------------------------------------------------------------------
# Reading the objects
# ---------------------------------------------------------------------------

def _created_sort_key(entry: dict):
    created = _parse_time(entry.get("createdTime"))
    return (
        0 if created else 1,
        created or datetime.max.replace(tzinfo=timezone.utc),
        _positive_int(entry.get("id")),
    )


def auto_main_object_id(objects: list[dict]) -> int:
    """The earliest created object with ОПР/ПД/РД, else the earliest one."""
    loaded = [entry for entry in objects if not clean_cell_value(entry.get("error"))]
    if not loaded:
        return 0
    project = [
        entry for entry in loaded
        if PROJECT_WORK_TYPES.intersection(entry.get("workTypes") or [])
    ]
    pool = project or loaded
    return _positive_int(sorted(pool, key=_created_sort_key)[0].get("id"))


def _fetch_lock(base: str) -> threading.Lock:
    with _FETCH_LOCKS_GUARD:
        return _FETCH_LOCKS.setdefault(base, threading.Lock())


def refresh_project_objects(
    dialog_id: str,
    *,
    object_item_id: int | None = None,
    source: str = "",
    if_needed: bool = False,
) -> dict:
    """Read the object, its related objects and the people from Bitrix24.

    ``object_item_id`` replaces the stored object id (n8n, admin panel).
    ``if_needed``: skip when the cards were read meanwhile (an automatic read
    waiting for another one).
    """
    base = _base(dialog_id)
    if not base:
        raise ProjectObjectError("Не указан проект")
    lock = _fetch_lock(base)
    with lock:
        bitrix = _load_bitrix(base)
        if if_needed and not needs_auto_fetch(base, bitrix):
            return project_object_state(base, bitrix)
        stored_id = _positive_int(bitrix.get("objectItemId"))
        root_id = _positive_int(object_item_id) if object_item_id is not None else stored_id
        started_at = _iso_now()
        if not root_id:
            changes = {
                "objectItemId": 0,
                "objects": [],
                "objectUsers": {},
                "objectsStatus": "missing",
                "objectsError": "",
                "objectsFetchedAt": "",
                "objectsAttemptAt": started_at,
            }
            _update_bitrix(base, changes)
            return project_object_state(base)

        root, error = fetch_object(root_id)
        if not root:
            changes = {
                "objectItemId": root_id,
                "objectEntityTypeId": OBJECT_ENTITY_TYPE_ID,
                "objectsStatus": "error",
                "objectsError": error,
                "objectsAttemptAt": started_at,
            }
            if root_id != stored_id:
                # Another object: the cards of the old one no longer apply.
                changes.update({"objects": [], "objectUsers": {}, "objectsFetchedAt": ""})
            _update_bitrix(base, changes)
            write_debug_log("project_objects_fetch_failed", {
                "dialogId": base, "objectItemId": root_id, "source": source, "error": error,
            })
            return project_object_state(base)

        objects: dict[int, dict] = {root_id: {**root, "id": root_id}}
        errors: list[str] = []

        def fetch_related(ids: list[int]) -> None:
            for related_id in ids:
                if related_id in objects or len(objects) >= MAX_RELATED_OBJECTS:
                    continue
                entry, related_error = fetch_object(related_id)
                if entry:
                    objects[related_id] = {**entry, "id": related_id}
                else:
                    objects[related_id] = {"id": related_id, "error": related_error, "workTypes": []}
                    errors.append(f"#{related_id}: {related_error}")

        fetch_related(root.get("relatedIds") or [])
        # The «Связанный объект» field of the main object lists the related
        # objects; when the entered object is not the main one, read it too
        # (one level only).
        main_id = auto_main_object_id(list(objects.values()))
        if main_id and main_id != root_id:
            fetch_related(objects[main_id].get("relatedIds") or [])

        user_ids: list[str] = []
        for entry in objects.values():
            for user_id in (
                [entry.get("assignedById")]
                + list(entry.get("workGroupLeaderIds") or [])
                + list(entry.get("conceptResponsibleIds") or [])
            ):
                user_id = clean_cell_value(user_id)
                if user_id and user_id not in user_ids:
                    user_ids.append(user_id)
        names = _resolve_user_names(user_ids)

        ordered = sorted(objects.values(), key=_created_sort_key)
        main_id = auto_main_object_id(ordered) or root_id
        main = objects.get(main_id) or root
        fetched_at = _iso_now()
        changes = {
            "objectItemId": root_id,
            "objectEntityTypeId": OBJECT_ENTITY_TYPE_ID,
            "objects": ordered,
            "objectUsers": {user_id: names.get(user_id, "") for user_id in user_ids},
            "objectsStatus": "partial" if errors else "resolved",
            "objectsError": "; ".join(errors),
            "objectsFetchedAt": fetched_at,
            "objectsAttemptAt": started_at,
            "objectTitle": clean_cell_value(main.get("title")),
        }
        override = _positive_int(bitrix.get("mainObjectId"))
        if override and override not in objects:
            changes["mainObjectId"] = 0
        effective_main = override if override in objects and not objects[override].get("error") else main_id
        assigned = clean_cell_value((objects.get(effective_main) or {}).get("assignedById"))
        if assigned:
            # The project curator (external notifications) is the
            # Ответственный of the main object.
            changes["curator"] = {
                "userId": assigned,
                "name": names.get(assigned, ""),
                "resolvedAt": fetched_at,
                "source": "project_objects",
            }
            changes["resolutionStatus"] = "resolved"
            changes["resolutionError"] = ""
            changes["lastResolvedAt"] = fetched_at
        _update_bitrix(base, changes)
        write_debug_log("project_objects_fetched", {
            "dialogId": base,
            "objectItemId": root_id,
            "source": source,
            "objects": [
                {"id": entry.get("id"), "workTypes": entry.get("workTypes"), "error": entry.get("error") or ""}
                for entry in ordered
            ],
            "mainObjectId": effective_main,
        })
        return project_object_state(base)


def needs_auto_fetch(dialog_id: str, bitrix: dict | None = None) -> bool:
    """An object id without a successful read: read it once on first use."""
    if bitrix is None:
        bitrix = _load_bitrix(_base(dialog_id))
    if not _positive_int(bitrix.get("objectItemId")):
        return False
    if clean_cell_value(bitrix.get("objectsFetchedAt")) and _cached_objects(bitrix):
        return False
    attempt = _parse_time(bitrix.get("objectsAttemptAt"))
    if attempt and (datetime.now(timezone.utc) - attempt).total_seconds() < AUTO_RETRY_SECONDS:
        return False
    return True


def refresh_in_background(
    dialog_id: str,
    *,
    object_item_id: int | None = None,
    source: str = "",
    if_needed: bool = False,
) -> None:
    def run():
        try:
            refresh_project_objects(dialog_id, object_item_id=object_item_id, source=source, if_needed=if_needed)
        except Exception as exc:
            write_debug_log("project_objects_background_failed", {
                "dialogId": dialog_id, "source": source, "error": str(exc),
            })

    threading.Thread(target=run, name="project-objects-refresh", daemon=True).start()


# ---------------------------------------------------------------------------
# Derived state
# ---------------------------------------------------------------------------

def portal_origin() -> str:
    from app.settings import BITRIX_TECH_WEBHOOK_URL

    parsed = urlparse(clean_cell_value(BITRIX_TECH_WEBHOOK_URL))
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return ""


def object_card_path(object_id: int) -> str:
    return f"/crm/type/{OBJECT_ENTITY_TYPE_ID}/details/{int(object_id)}/"


def _main_object_id(bitrix: dict, objects: list[dict]) -> int:
    loaded_ids = {
        _positive_int(entry.get("id")) for entry in objects
        if not clean_cell_value(entry.get("error"))
    }
    override = _positive_int(bitrix.get("mainObjectId"))
    if override in loaded_ids:
        return override
    return auto_main_object_id(objects) or (
        _positive_int(bitrix.get("objectItemId")) if _positive_int(bitrix.get("objectItemId")) in loaded_ids else 0
    )


def _ordered_candidates(objects: list[dict], main_id: int) -> list[dict]:
    loaded = [entry for entry in objects if not clean_cell_value(entry.get("error"))]
    return sorted(
        loaded,
        key=lambda entry: (0 if _positive_int(entry.get("id")) == main_id else 1, _created_sort_key(entry)),
    )


def checklist_object(objects: list[dict], main_id: int, checklist_key: str) -> dict | None:
    work_types = CHECKLIST_WORK_TYPES.get(normalize_checklist_key(checklist_key))
    if not work_types:
        return None
    for entry in _ordered_candidates(objects, main_id):
        if set(work_types).intersection(entry.get("workTypes") or []):
            return entry
    return None


def checklist_people(objects: list[dict], main_id: int, checklist_key: str, names: dict) -> list[dict]:
    key = normalize_checklist_key(checklist_key)
    people: list[dict] = []

    def add(user_id: str, role: str, object_id: int) -> None:
        user_id = clean_cell_value(user_id)
        if not user_id:
            return
        for person in people:
            if person["userId"] == user_id:
                if role not in person["roles"]:
                    person["roles"].append(role)
                return
        people.append({
            "userId": user_id,
            "name": clean_cell_value(names.get(user_id)) or f"ID {user_id}",
            "roles": [role],
            "objectId": int(object_id),
        })

    entry = checklist_object(objects, main_id, key)
    if entry:
        object_id = _positive_int(entry.get("id"))
        add(entry.get("assignedById"), ROLE_ASSIGNED, object_id)
        for user_id in entry.get("workGroupLeaderIds") or []:
            add(user_id, ROLE_WORK_GROUP_LEADER, object_id)
    if key == "concept":
        for source in _ordered_candidates(objects, main_id):
            if not PROJECT_WORK_TYPES.intersection(source.get("workTypes") or []):
                continue
            for user_id in source.get("conceptResponsibleIds") or []:
                add(user_id, ROLE_CONCEPT_RESPONSIBLE, _positive_int(source.get("id")))
    return people


def _variants(objects: list[dict], main_id: int, field: str) -> list[dict]:
    result: list[dict] = []
    for entry in _ordered_candidates(objects, main_id):
        value = clean_cell_value(entry.get(field))
        if not value:
            continue
        existing = next((variant for variant in result if variant["value"] == value), None)
        if existing:
            existing["objectIds"].append(_positive_int(entry.get("id")))
        else:
            result.append({"value": value, "objectIds": [_positive_int(entry.get("id"))]})
    return result


def _chosen(variants: list[dict], pinned: str) -> str:
    pinned = clean_cell_value(pinned)
    if pinned and any(variant["value"] == pinned for variant in variants):
        return pinned
    return variants[0]["value"] if variants else ""


def _object_brief(entry: dict, main_id: int, names: dict) -> dict:
    object_id = _positive_int(entry.get("id"))
    assigned = clean_cell_value(entry.get("assignedById"))
    return {
        "id": object_id,
        "title": clean_cell_value(entry.get("title")),
        "legalName": clean_cell_value(entry.get("legalName")),
        "cipher": clean_cell_value(entry.get("cipher")),
        "workTypes": [
            WORK_TYPE_NAMES.get(work_type, str(work_type))
            for work_type in entry.get("workTypes") or []
        ],
        "createdTime": clean_cell_value(entry.get("createdTime")),
        "assigned": {"userId": assigned, "name": clean_cell_value(names.get(assigned))} if assigned else None,
        "workGroupLeaders": [
            {"userId": user_id, "name": clean_cell_value(names.get(user_id))}
            for user_id in entry.get("workGroupLeaderIds") or []
        ],
        "conceptResponsibles": [
            {"userId": user_id, "name": clean_cell_value(names.get(user_id))}
            for user_id in entry.get("conceptResponsibleIds") or []
        ],
        "isMain": object_id == main_id,
        "error": clean_cell_value(entry.get("error")),
        "path": object_card_path(object_id),
    }


def project_object_state(dialog_id: str, bitrix: dict | None = None) -> dict:
    """Project-wide state: objects, main object, variants and choices."""
    base = _base(dialog_id)
    if bitrix is None:
        bitrix = _load_bitrix(base)
    objects = _cached_objects(bitrix)
    names = bitrix.get("objectUsers") if isinstance(bitrix.get("objectUsers"), dict) else {}
    main_id = _main_object_id(bitrix, objects)
    legal_names = _variants(objects, main_id, "legalName")
    ciphers = _variants(objects, main_id, "cipher")
    return {
        "baseDialogId": base,
        "objectItemId": _positive_int(bitrix.get("objectItemId")),
        "mainObjectId": main_id,
        "mainObjectOverride": _positive_int(bitrix.get("mainObjectId")),
        "autoMainObjectId": auto_main_object_id(objects),
        "status": clean_cell_value(bitrix.get("objectsStatus")),
        "error": clean_cell_value(bitrix.get("objectsError")),
        "fetchedAt": clean_cell_value(bitrix.get("objectsFetchedAt")),
        "objectDriven": bool(main_id),
        "objects": [_object_brief(entry, main_id, names) for entry in objects],
        "legalNames": legal_names,
        "legalName": _chosen(legal_names, bitrix.get("legalNameChoice")),
        "legalNamePinned": clean_cell_value(bitrix.get("legalNameChoice")),
        "ciphers": ciphers,
        "cipher": _chosen(ciphers, bitrix.get("cipherChoice")),
        "cipherPinned": clean_cell_value(bitrix.get("cipherChoice")),
        "portalOrigin": portal_origin(),
    }


def object_people_user_ids(dialog_id: str) -> set[str] | None:
    """Everyone in the ГИП fields of the project, None without object data."""
    bitrix = _load_bitrix(_base(dialog_id))
    objects = _cached_objects(bitrix)
    main_id = _main_object_id(bitrix, objects)
    if not main_id:
        return None
    names = bitrix.get("objectUsers") if isinstance(bitrix.get("objectUsers"), dict) else {}
    result: set[str] = set()
    for key in CHECKLIST_WORK_TYPES:
        for person in checklist_people(objects, main_id, key, names):
            result.add(person["userId"])
    return result


def checklist_object_view(dialog_id: str, checklist_key: str, *, bitrix: dict | None = None, state: dict | None = None) -> dict:
    """What the checklist header shows."""
    key = normalize_checklist_key(checklist_key)
    base = _base(dialog_id)
    if bitrix is None:
        bitrix = _load_bitrix(base)
    if state is None:
        state = project_object_state(base, bitrix)
    objects = _cached_objects(bitrix)
    names = bitrix.get("objectUsers") if isinstance(bitrix.get("objectUsers"), dict) else {}
    main_id = state["mainObjectId"]
    own = checklist_object(objects, main_id, key) if main_id else None
    # Without loaded cards the link still leads to the entered object.
    link_id = _positive_int((own or {}).get("id")) or main_id or state["objectItemId"]
    path = object_card_path(link_id) if link_id else ""
    origin = state["portalOrigin"]
    return {
        "checklistKey": key,
        "configured": bool(state["objectItemId"]),
        "objectDriven": state["objectDriven"],
        "status": state["status"],
        "error": state["error"],
        "needsFetch": needs_auto_fetch(base, bitrix),
        "legalName": state["legalName"],
        "legalNames": [variant["value"] for variant in state["legalNames"]],
        "cipher": state["cipher"],
        "ciphers": [variant["value"] for variant in state["ciphers"]],
        "objectId": link_id,
        "checklistObjectId": _positive_int((own or {}).get("id")),
        "mainObjectId": main_id,
        "objectPath": path,
        "objectUrl": (origin + path) if (origin and path) else "",
        "peopleLabel": PEOPLE_FIELD_LABELS.get(key, DEFAULT_PEOPLE_FIELD_LABEL),
        "people": checklist_people(objects, main_id, key, names) if main_id else [],
    }


def checklist_object_views(dialog_id: str) -> dict:
    """Header data of every checklist (the popup switches checklists in
    place) and the users who may pin the legal name and the cipher."""
    from app.checklists.config import list_checklist_configs
    from app.checklists.project_phases import manager_user_ids

    base = _base(dialog_id)
    bitrix = _load_bitrix(base)
    state = project_object_state(base, bitrix)
    return {
        "byKey": {
            config.key: checklist_object_view(base, config.key, bitrix=bitrix, state=state)
            for config in list_checklist_configs()
        },
        "managerUserIds": manager_user_ids(base),
    }


# ---------------------------------------------------------------------------
# Changes from the admin panel and the checklist
# ---------------------------------------------------------------------------

def set_object_item_id(dialog_id: str, object_item_id: Any, *, source: str = "") -> dict:
    """Store a new object id and read it; "" or 0 clears the object."""
    raw = clean_cell_value(object_item_id)
    number = _positive_int(raw)
    if raw and not number:
        raise ProjectObjectError("ID объекта должен быть положительным числом")
    return refresh_project_objects(dialog_id, object_item_id=number, source=source)


def set_main_object(dialog_id: str, object_id: Any) -> dict:
    base = _base(dialog_id)
    bitrix = _load_bitrix(base)
    number = _positive_int(object_id)
    loaded = {
        _positive_int(entry.get("id")) for entry in _cached_objects(bitrix)
        if not clean_cell_value(entry.get("error"))
    }
    if number and number not in loaded:
        raise ProjectObjectError("Такого объекта нет среди подтянутых")
    changes: dict = {"mainObjectId": number}
    objects = _cached_objects(bitrix)
    names = bitrix.get("objectUsers") if isinstance(bitrix.get("objectUsers"), dict) else {}
    main_id = number or auto_main_object_id(objects)
    main = next((entry for entry in objects if _positive_int(entry.get("id")) == main_id), None)
    assigned = clean_cell_value((main or {}).get("assignedById"))
    if main:
        changes["objectTitle"] = clean_cell_value(main.get("title"))
    if assigned:
        changes["curator"] = {
            "userId": assigned,
            "name": clean_cell_value(names.get(assigned)),
            "resolvedAt": _iso_now(),
            "source": "project_objects",
        }
    _update_bitrix(base, changes)
    write_debug_log("project_main_object_set", {"dialogId": base, "mainObjectId": number})
    return project_object_state(base)


CHOICE_FIELDS = {"legalName": ("legalNames", "legalNameChoice"), "cipher": ("ciphers", "cipherChoice")}


def choose_value(dialog_id: str, field: str, value: Any, *, acting_user_id: str = "", check_rights: bool = True) -> dict:
    if field not in CHOICE_FIELDS:
        raise ProjectObjectError("Неизвестное поле")
    base = _base(dialog_id)
    if check_rights:
        from app.checklists.project_phases import can_manage_phases

        if not can_manage_phases(base, acting_user_id):
            raise ProjectObjectError("Выбирать могут только администраторы и ГИП проекта")
    state = project_object_state(base)
    variants_key, store_key = CHOICE_FIELDS[field]
    text = clean_cell_value(value)
    if text and not any(variant["value"] == text for variant in state[variants_key]):
        raise ProjectObjectError("Такого варианта нет среди подтянутых из объектов")
    _update_bitrix(base, {store_key: text})
    write_debug_log("project_object_value_chosen", {
        "dialogId": base, "field": field, "value": text, "actingUserId": acting_user_id,
    })
    return project_object_state(base)
