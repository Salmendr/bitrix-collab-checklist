"""«Анализ состояния объекта»: the panel under the checklist header, the
slice over all projects and the risk tables.

The data belong to the project (the chat of its first stage), not to a
checklist: every checklist window of the project shows the same panel.
A change is saved at once, without the edit session of a checklist, and
never goes to the project chat.

Rights: the panel, the slice and the risk tables are seen by the leaders
(administrators, permanent leaders, responsible people of the objects and
contracts) and by the GIPs. A project is edited by its leaders and GIPs;
everyone who sees the slice sees all the projects.

The lists of «Стадии по договору» and «Готовность ОПР» are common to all
projects: leaders add options, administrators rename and remove them. A
removed option stays on the projects that have it until it is taken off.
"""
from __future__ import annotations

import json
import re
import threading
import uuid
from datetime import date, datetime
from typing import Any

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.checklists import date_reminders
from app.checklists.utils import clean_cell_value, normalize_dialog_id, normalize_status


class ObjectAnalysisError(RuntimeError):
    """A refused change; the message is for the user."""


class ObjectAnalysisDenied(ObjectAnalysisError):
    """The user may not see or change this."""


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONTRACT_STATUSES = (
    "Нет",
    "Готовится",
    "На рассмотрении у Заказчика",
    "На рассмотрении у НАС",
    "Подписан Нами",
    "Подписан Заказчиком",
    "Подписан сторонами аванс НЕ получен",
    "Подписан сторонами аванс Получен",
)
NO_CONTRACT_STATUS_GROUP = "Статус договора не указан"

# From saturated green to saturated red.
RISK_LEVELS = (
    {"level": 1, "label": "Очень низкий", "color": "#1a9850", "text": "#ffffff"},
    {"level": 2, "label": "Низкий", "color": "#66bd63", "text": "#ffffff"},
    {"level": 3, "label": "Ниже среднего", "color": "#a6d96a", "text": "#1f2328"},
    {"level": 4, "label": "Средний", "color": "#fee08b", "text": "#1f2328"},
    {"level": 5, "label": "Выше среднего", "color": "#fdae61", "text": "#1f2328"},
    {"level": 6, "label": "Высокий", "color": "#f46d43", "text": "#ffffff"},
    {"level": 7, "label": "Очень высокий", "color": "#d73027", "text": "#ffffff"},
)
RISK_LEVEL_BY_NUMBER = {entry["level"]: entry for entry in RISK_LEVELS}
NOT_RATED = {"level": 0, "label": "Не оценено", "color": "#eef1f5", "text": "#667085"}

# The worst ratings that make the total.
RISK_TOTAL_WORST = 3
# Probability of realization → risk rating: (lowest percent, level).
PROBABILITY_LEVELS = ((100, 1), (90, 2), (75, 3), (60, 4), (45, 5), (30, 6), (0, 7))
PROBABILITY_KEYS = ("opr", "pd", "rd")

DEFAULT_RISK_ROWS = (
    "Цель заказчика (только заработать денег, «оставить след», развитие смежного бизнеса, исполнение обязательств)",
    "Срок реализации проекта - Цель заказчика",
    "Источник финансирования",
    "Земля - КРТ, собств, аренда",
    "Позиция города",
    "Проверка контрагента, в том числе Судебная аналитика (Юристы)",
    "Прошлые объекты",
    "Команда Заказчика (уровень профессионализма)",
)

LIST_STAGES = "stages"
LIST_OPR_SECTIONS = "oprSections"
DEFAULT_STAGE_OPTIONS = ("АГР", "ОПР", "ПД", "РД", "Дизайн", "Концепция")
# «РАЗДЕЛЫ ПРОЕКТНОЙ ДОКУМЕНТАЦИИ» without the linear objects: the sections
# of Постановление № 87 first, the rest by the alphabet.
_MAIN_OPR_SECTIONS = (
    "ПЗ", "ПЗУ", "АР", "КР",
    "ИОС 1", "ИОС 2", "ИОС 3", "ИОС 4", "ИОС 5", "ИОС 6",
    "ЭГ (ИОС)", "ЭС (ИОС)", "ЭМ (ИОС)", "ЭО (ИОС)", "ЭН (ИОС)", "ВК (ИОС)",
    "ОВ (ИОС)", "СС (ИОС)", "ГСН (ИОС)", "ГСВ (ИОС)", "ТКР (ИОС)", "АВТ (ИОС)",
    "ПОС", "ПОД", "ООС", "ПБ", "ОДИ", "ЭЭ", "СМ",
)
_OTHER_OPR_SECTIONS = (
    "АГР", "АЗ", "АИ", "АК", "АПВ", "АПТ", "АС", "АСТУЭ", "АТП", "АТХ", "БЛАГ",
    "ВОДОСТ", "ВР", "ВС", "ВТ", "ГДЗ", "ГЛД", "ГМИ", "ГП", "ГР", "ИО", "КД",
    "КЖ", "КМ", "КМД", "КОНД", "КТСО", "МО", "НВ", "НВД", "НВК", "НК", "НСС",
    "ОК", "ОМ", "ОПЗ", "ОР", "ОС", "ОЭ", "ПНО", "ПО", "ППР", "ПС", "ПСО", "ПТ",
    "ПТА", "ПУ", "Р", "РТ", "С", "СВ", "СД", "СДКУ", "СКС", "СМЕТ", "СМИС",
    "СОТ", "СП", "ССР", "ССРСС", "ТБЭ", "ТК", "ТМ", "ТР", "ТС", "ТХ", "ТЧ",
    "ХС", "ЭК",
)
DEFAULT_OPR_SECTION_OPTIONS = _MAIN_OPR_SECTIONS + _OTHER_OPR_SECTIONS
DEFAULT_OPTIONS = {
    LIST_STAGES: DEFAULT_STAGE_OPTIONS,
    LIST_OPR_SECTIONS: DEFAULT_OPR_SECTION_OPTIONS,
}
LIST_TITLES = {LIST_STAGES: "Стадии по договору", LIST_OPR_SECTIONS: "Готовность ОПР"}

MAX_GENERAL_TEXT = 300
MAX_RESPONSIBLE_TEXT = 500
MAX_RISK_NAME = 300
MAX_RISK_DESCRIPTION = 2000
MAX_OPTION_LABEL = 40
MAX_RISK_ROWS = 30

DATE_FIELDS = ("oprCustomerDate", "oprPdDate")
MEETING_FIELDS = ("meeting", "vks")
FIELDS = frozenset({
    "contractStatus", "stages", "general", "oprSections",
    "customerResponsible", *DATE_FIELDS, *MEETING_FIELDS,
})

TU_GROUP_TITLE = "ту"

_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$")
_WRITE_LOCK = threading.Lock()


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def _ensure_tables(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS object_analysis (
            base_dialog_id TEXT PRIMARY KEY,
            data_json TEXT NOT NULL,
            updated_at TEXT,
            updated_by_id TEXT,
            updated_by_name TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS object_analysis_options (
            list_key TEXT NOT NULL,
            option_id TEXT NOT NULL,
            label TEXT NOT NULL,
            position INTEGER NOT NULL,
            removed INTEGER NOT NULL DEFAULT 0,
            created_by_id TEXT,
            created_at TEXT,
            PRIMARY KEY (list_key, option_id)
        )
    """)
    seeded = {
        row["list_key"] for row in conn.execute(
            "SELECT DISTINCT list_key FROM object_analysis_options"
        ).fetchall()
    }
    now = _now()
    for list_key, labels in DEFAULT_OPTIONS.items():
        if list_key in seeded:
            continue
        # The first start fills the list once; later the users edit it.
        conn.executemany(
            "INSERT OR IGNORE INTO object_analysis_options"
            "(list_key, option_id, label, position, removed, created_by_id, created_at)"
            " VALUES (?, ?, ?, ?, 0, 'default', ?)",
            [
                (list_key, f"{list_key}-{index:03d}", label, index, now)
                for index, label in enumerate(labels, start=1)
            ],
        )
    conn.commit()


def _conn():
    conn = get_conn()
    _ensure_tables(conn)
    return conn


def base_of(dialog_id: str) -> str:
    from app.checklists.project_phases import base_dialog_id

    return base_dialog_id(normalize_dialog_id(dialog_id))


def _empty_record() -> dict:
    return {
        "contractStatus": "",
        "stages": [],
        "general": {"text": "", "color": 0},
        "oprSections": {"ready": [], "notReady": []},
        "oprCustomerDate": "",
        "oprPdDate": "",
        "meeting": {"at": "", "weekly": False},
        "vks": {"at": "", "weekly": False},
        "customerResponsible": "",
        "risks": None,
        # Recipients of the reminders of every date and who set it.
        "reminders": {},
    }


def _clean_record(raw: Any) -> dict:
    record = _empty_record()
    if not isinstance(raw, dict):
        return record
    status = clean_cell_value(raw.get("contractStatus"))
    record["contractStatus"] = status if status in CONTRACT_STATUSES else ""
    record["stages"] = _id_list(raw.get("stages"))
    general = raw.get("general") if isinstance(raw.get("general"), dict) else {}
    record["general"] = {
        "text": str(general.get("text") or "")[:MAX_GENERAL_TEXT],
        "color": _int(general.get("color")) if _int(general.get("color")) in RISK_LEVEL_BY_NUMBER else 0,
    }
    sections = raw.get("oprSections") if isinstance(raw.get("oprSections"), dict) else {}
    ready = _id_list(sections.get("ready"))
    record["oprSections"] = {
        "ready": ready,
        "notReady": [value for value in _id_list(sections.get("notReady")) if value not in ready],
    }
    for field in DATE_FIELDS:
        value = clean_cell_value(raw.get(field))
        record[field] = value if _valid_date(value) else ""
    for field in MEETING_FIELDS:
        meeting = raw.get(field) if isinstance(raw.get(field), dict) else {}
        at = clean_cell_value(meeting.get("at"))
        at = at if _valid_datetime(at) else ""
        record[field] = {"at": at, "weekly": bool(meeting.get("weekly")) and bool(at)}
    record["customerResponsible"] = str(raw.get("customerResponsible") or "")[:MAX_RESPONSIBLE_TEXT]
    record["risks"] = _clean_risks(raw.get("risks")) if raw.get("risks") is not None else None
    reminders = raw.get("reminders") if isinstance(raw.get("reminders"), dict) else {}
    record["reminders"] = {
        field: date_reminders.clean_meta(reminders[field])
        for field in date_reminders.FIELDS if isinstance(reminders.get(field), dict)
    }
    return record


def _id_list(value: Any) -> list[str]:
    result: list[str] = []
    for entry in value if isinstance(value, list) else []:
        entry = clean_cell_value(entry)
        if entry and entry not in result:
            result.append(entry)
    return result


def _valid_date(value: str) -> bool:
    if not value:
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return len(value) == 10


def _valid_datetime(value: str) -> bool:
    if not value or not _DATETIME_RE.match(value):
        return False
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _read_record(conn, base: str) -> dict:
    row = conn.execute(
        "SELECT data_json FROM object_analysis WHERE base_dialog_id = ?", (base,)
    ).fetchone()
    try:
        raw = json.loads(row["data_json"]) if row else {}
    except Exception:
        raw = {}
    return _clean_record(raw)


def _read_records(conn) -> dict[str, dict]:
    rows = conn.execute("SELECT base_dialog_id, data_json FROM object_analysis").fetchall()
    result = {}
    for row in rows:
        try:
            raw = json.loads(row["data_json"])
        except Exception:
            raw = {}
        result[row["base_dialog_id"]] = _clean_record(raw)
    return result


def _write_record(conn, base: str, record: dict, actor: dict) -> None:
    conn.execute(
        """
        INSERT INTO object_analysis(base_dialog_id, data_json, updated_at, updated_by_id, updated_by_name)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(base_dialog_id) DO UPDATE SET
            data_json = excluded.data_json,
            updated_at = excluded.updated_at,
            updated_by_id = excluded.updated_by_id,
            updated_by_name = excluded.updated_by_name
        """,
        (
            base,
            json.dumps(record, ensure_ascii=False),
            _now(),
            clean_cell_value(actor.get("userId")),
            clean_cell_value(actor.get("userName")),
        ),
    )


def _change_record(base: str, actor: dict, change) -> dict:
    """Read, change and write one project in one transaction."""
    with _WRITE_LOCK:
        conn = _conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            record = _read_record(conn, base)
            change(record)
            _write_record(conn, base, record, actor)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    return record


# ---------------------------------------------------------------------------
# Option lists
# ---------------------------------------------------------------------------

def list_options(conn=None) -> dict[str, list[dict]]:
    own = conn is None
    if own:
        conn = _conn()
    try:
        rows = conn.execute(
            "SELECT list_key, option_id, label, position, removed FROM object_analysis_options"
            " ORDER BY list_key, position, option_id"
        ).fetchall()
    finally:
        if own:
            conn.close()
    result: dict[str, list[dict]] = {key: [] for key in DEFAULT_OPTIONS}
    for row in rows:
        if row["list_key"] not in result:
            continue
        result[row["list_key"]].append({
            "id": row["option_id"],
            "label": row["label"],
            "removed": bool(row["removed"]),
        })
    return result


def _option_ids(options: dict, list_key: str, *, active_only: bool) -> set[str]:
    return {
        option["id"] for option in options.get(list_key, [])
        if not (active_only and option["removed"])
    }


def change_option(list_key: str, action: str, *, dialog_id: str, actor: dict,
                  option_id: str = "", label: str = "") -> dict[str, list[dict]]:
    """«Добавить» for leaders; «Переименовать», «Убрать из списка» and
    «Вернуть в список» for administrators."""
    from app.checklists.project_phases import is_project_admin

    if list_key not in DEFAULT_OPTIONS:
        raise ObjectAnalysisError("Неизвестный список")
    user_id = clean_cell_value(actor.get("userId"))
    base = base_of(dialog_id)
    rights = user_rights(base, user_id)
    label = " ".join(str(label or "").split())[:MAX_OPTION_LABEL]
    option_id = clean_cell_value(option_id)
    if action == "add":
        if not rights["canAddOption"]:
            raise ObjectAnalysisDenied("Добавлять варианты могут руководители и администраторы")
    elif not is_project_admin(user_id):
        raise ObjectAnalysisDenied("Менять список могут только администраторы")
    with _WRITE_LOCK:
        conn = _conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                "SELECT option_id, label, removed, position FROM object_analysis_options WHERE list_key = ?",
                (list_key,),
            ).fetchall()
            by_id = {row["option_id"]: row for row in rows}
            same_label = next(
                (row for row in rows if row["label"].lower() == label.lower() and row["option_id"] != option_id),
                None,
            )
            if action == "add":
                if not label:
                    raise ObjectAnalysisError("Введите название варианта")
                if same_label is not None:
                    if not same_label["removed"]:
                        raise ObjectAnalysisError(f"Вариант «{label}» уже есть в списке")
                    # Added again: the removed option comes back.
                    conn.execute(
                        "UPDATE object_analysis_options SET removed = 0 WHERE list_key = ? AND option_id = ?",
                        (list_key, same_label["option_id"]),
                    )
                else:
                    position = max([row["position"] for row in rows] or [0]) + 1
                    conn.execute(
                        "INSERT INTO object_analysis_options"
                        "(list_key, option_id, label, position, removed, created_by_id, created_at)"
                        " VALUES (?, ?, ?, ?, 0, ?, ?)",
                        (list_key, uuid.uuid4().hex[:12], label, position, user_id, _now()),
                    )
            elif action in {"rename", "remove", "restore"}:
                if option_id not in by_id:
                    raise ObjectAnalysisError("Такого варианта нет в списке")
                if action == "rename":
                    if not label:
                        raise ObjectAnalysisError("Введите название варианта")
                    if same_label is not None:
                        raise ObjectAnalysisError(f"Вариант «{label}» уже есть в списке")
                    conn.execute(
                        "UPDATE object_analysis_options SET label = ? WHERE list_key = ? AND option_id = ?",
                        (label, list_key, option_id),
                    )
                else:
                    conn.execute(
                        "UPDATE object_analysis_options SET removed = ? WHERE list_key = ? AND option_id = ?",
                        (1 if action == "remove" else 0, list_key, option_id),
                    )
            else:
                raise ObjectAnalysisError("Неизвестное действие")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    write_debug_log("object_analysis_option_changed", {
        "list": list_key, "action": action, "optionId": option_id, "label": label,
        "userId": user_id, "dialogId": base,
    })
    return list_options()


# ---------------------------------------------------------------------------
# Rights
# ---------------------------------------------------------------------------

def user_rights(base: str, user_id: str) -> dict:
    from app.checklists.project_leaders import is_project_leader
    from app.checklists.project_phases import is_project_admin, manager_user_ids

    user = clean_cell_value(user_id)
    if not user:
        return {"view": False, "edit": False, "canAddOption": False, "canManageOptions": False}
    leader = is_project_leader(base, user)
    gip = user in manager_user_ids(base)
    return {
        "view": leader or gip,
        "edit": leader or gip,
        "canAddOption": leader,
        "canManageOptions": is_project_admin(user),
    }


def _require(base: str, user_id: str, right: str) -> dict:
    rights = user_rights(base, user_id)
    if not rights[right]:
        raise ObjectAnalysisDenied(
            "Анализ состояния объекта доступен руководителям, администраторам и ГИП проекта"
            if right == "view" else
            "Менять данные объекта могут руководители, администраторы и ГИП этого проекта"
        )
    return rights


# ---------------------------------------------------------------------------
# Field changes
# ---------------------------------------------------------------------------

def update_field(dialog_id: str, field: str, value: Any, *, actor: dict, recipients: Any = None) -> dict:
    """recipients: the list of the reminders of a date; None keeps it."""
    base = base_of(dialog_id)
    if not base:
        raise ObjectAnalysisError("Не указан проект")
    if field not in FIELDS:
        raise ObjectAnalysisError("Неизвестное поле")
    _require(base, actor.get("userId"), "edit")
    options = list_options()

    def change(record: dict) -> None:
        before = record[field]
        record[field] = _validated(field, value, record, options)
        if field in date_reminders.FIELDS:
            _change_reminders(record, field, before, recipients, actor)

    record = _change_record(base, actor, change)
    write_debug_log("object_analysis_field_changed", {
        "dialogId": base, "field": field, "userId": clean_cell_value(actor.get("userId")),
    })
    return record


def _validated(field: str, value: Any, record: dict, options: dict) -> Any:
    if field == "contractStatus":
        status = clean_cell_value(value)
        if status and status not in CONTRACT_STATUSES:
            raise ObjectAnalysisError("Такого статуса договора нет в списке")
        return status
    if field == "stages":
        return _chosen_options(value, record["stages"], options, LIST_STAGES)
    if field == "general":
        value = value if isinstance(value, dict) else {}
        text = str(value.get("text") or "").strip()
        if len(text) > MAX_GENERAL_TEXT:
            raise ObjectAnalysisError(f"Не больше {MAX_GENERAL_TEXT} символов")
        color = _int(value.get("color"))
        if color and color not in RISK_LEVEL_BY_NUMBER:
            raise ObjectAnalysisError("Неизвестный цвет")
        return {"text": text, "color": color}
    if field == "oprSections":
        value = value if isinstance(value, dict) else {}
        current = record["oprSections"]
        known = current["ready"] + current["notReady"]
        ready = _chosen_options(value.get("ready"), known, options, LIST_OPR_SECTIONS)
        not_ready = _chosen_options(value.get("notReady"), known, options, LIST_OPR_SECTIONS)
        if set(ready) & set(not_ready):
            raise ObjectAnalysisError("Раздел не может быть одновременно готовым и не готовым")
        return {"ready": ready, "notReady": not_ready}
    if field in DATE_FIELDS:
        text = clean_cell_value(value)
        if text and not _valid_date(text):
            raise ObjectAnalysisError("Неверная дата")
        return text
    if field in MEETING_FIELDS:
        value = value if isinstance(value, dict) else {}
        at = clean_cell_value(value.get("at"))[:16]
        if at and not _valid_datetime(at):
            raise ObjectAnalysisError("Неверные дата и время")
        return {"at": at, "weekly": bool(value.get("weekly")) and bool(at)}
    if field == "customerResponsible":
        text = str(value or "").strip()
        if len(text) > MAX_RESPONSIBLE_TEXT:
            raise ObjectAnalysisError(f"Не больше {MAX_RESPONSIBLE_TEXT} символов")
        return text
    raise ObjectAnalysisError("Неизвестное поле")


def _change_reminders(record: dict, field: str, before: Any, recipients: Any, actor: dict) -> None:
    meta = date_reminders.clean_meta(record["reminders"].get(field))
    if recipients is not None:
        try:
            meta["recipients"] = date_reminders.validated_recipients(
                recipients, date_reminders.recipients_of(meta)
            )
        except date_reminders.RecipientError as exc:
            raise ObjectAnalysisError(str(exc)) from exc
    # A new date: its reminders count from now and name who set it.
    if date_reminders.value_key(field, before) != date_reminders.value_key(field, record[field]):
        meta["setById"] = clean_cell_value(actor.get("userId"))
        meta["setByName"] = clean_cell_value(actor.get("userName"))
        meta["setAt"] = date_reminders.iso(date_reminders.now_utc())
    record["reminders"][field] = meta


def _chosen_options(value: Any, current: list[str], options: dict, list_key: str) -> list[str]:
    """Options of a list in the list order; a removed option may stay only
    where it already was."""
    chosen = _id_list(value)
    active = _option_ids(options, list_key, active_only=True)
    known = _option_ids(options, list_key, active_only=False)
    for option_id in chosen:
        if option_id not in known:
            raise ObjectAnalysisError("Такого варианта нет в списке")
        if option_id not in active and option_id not in current:
            raise ObjectAnalysisError("Этот вариант убран из списка")
    order = {option["id"]: index for index, option in enumerate(options.get(list_key, []))}
    return sorted(chosen, key=lambda option_id: order.get(option_id, len(order)))


# ---------------------------------------------------------------------------
# Risks
# ---------------------------------------------------------------------------

def _default_risks() -> dict:
    return {
        "rows": [
            {"id": f"r{index}", "name": name, "description": "", "level": 0}
            for index, name in enumerate(DEFAULT_RISK_ROWS, start=1)
        ],
        "probabilities": {key: None for key in PROBABILITY_KEYS},
    }


def _probability(value: Any) -> int | None:
    if value is None or value == "":
        return None
    number = _int(value, -1)
    if number < 0 or number > 100:
        raise ObjectAnalysisError("Вероятность — целое число от 0 до 100")
    return number


def _clean_risks(raw: Any) -> dict:
    if not isinstance(raw, dict):
        return _default_risks()
    rows = []
    for row in raw.get("rows") if isinstance(raw.get("rows"), list) else []:
        if not isinstance(row, dict) or not clean_cell_value(row.get("id")):
            continue
        level = _int(row.get("level"))
        rows.append({
            "id": clean_cell_value(row.get("id")),
            "name": str(row.get("name") or "")[:MAX_RISK_NAME],
            "description": str(row.get("description") or "")[:MAX_RISK_DESCRIPTION],
            "level": level if level in RISK_LEVEL_BY_NUMBER else 0,
        })
    probabilities = raw.get("probabilities") if isinstance(raw.get("probabilities"), dict) else {}
    cleaned = {}
    for key in PROBABILITY_KEYS:
        try:
            cleaned[key] = _probability(probabilities.get(key))
        except ObjectAnalysisError:
            cleaned[key] = None
    return {"rows": rows, "probabilities": cleaned}


def probability_level(probabilities: dict) -> int:
    """The lowest probability of realization as one more risk rating."""
    values = [value for value in (probabilities or {}).values() if isinstance(value, int)]
    if not values:
        return 0
    lowest = min(values)
    for threshold, level in PROBABILITY_LEVELS:
        if lowest >= threshold:
            return level
    return 7


def risk_total(risks: dict | None) -> dict:
    """«Итоговая оценка»: the mean of the three worst ratings (the lowest
    probability of realization counts as a rating), rounded to the nearest
    level, a half to the riskier one."""
    risks = risks or _default_risks()
    ratings = [row["level"] for row in risks.get("rows") or [] if row.get("level")]
    from_probability = probability_level(risks.get("probabilities") or {})
    if from_probability:
        ratings.append(from_probability)
    if not ratings:
        return {**NOT_RATED, "ratings": []}
    worst = sorted(ratings, reverse=True)[:RISK_TOTAL_WORST]
    # Integer arithmetic: floor(mean + 1/2) without float error.
    level = (2 * sum(worst) + len(worst)) // (2 * len(worst))
    level = max(1, min(7, level))
    return {**RISK_LEVEL_BY_NUMBER[level], "ratings": worst, "probabilityLevel": from_probability}


def change_risks(dialog_id: str, op: str, payload: dict, *, actor: dict) -> dict:
    """One change of the risk table of a project."""
    base = base_of(dialog_id)
    if not base:
        raise ObjectAnalysisError("Не указан проект")
    _require(base, actor.get("userId"), "edit")
    payload = payload if isinstance(payload, dict) else {}

    def change(record: dict) -> None:
        risks = record["risks"] or _default_risks()
        rows = risks["rows"]
        row_id = clean_cell_value(payload.get("rowId"))
        row = next((entry for entry in rows if entry["id"] == row_id), None)
        if op == "row-update":
            if row is None:
                raise ObjectAnalysisError("Строка уже удалена")
            field = clean_cell_value(payload.get("field"))
            value = payload.get("value")
            if field == "name":
                name = " ".join(str(value or "").split())
                if not name:
                    raise ObjectAnalysisError("Введите характеристику")
                row["name"] = name[:MAX_RISK_NAME]
            elif field == "description":
                row["description"] = str(value or "").strip()[:MAX_RISK_DESCRIPTION]
            elif field == "level":
                level = _int(value)
                if level and level not in RISK_LEVEL_BY_NUMBER:
                    raise ObjectAnalysisError("Неизвестная оценка риска")
                row["level"] = level
            else:
                raise ObjectAnalysisError("Неизвестное поле")
        elif op == "row-add":
            if len(rows) >= MAX_RISK_ROWS:
                raise ObjectAnalysisError(f"Не больше {MAX_RISK_ROWS} строк")
            name = " ".join(str(payload.get("name") or "").split()) or "Новая характеристика"
            rows.append({"id": "r" + uuid.uuid4().hex[:10], "name": name[:MAX_RISK_NAME], "description": "", "level": 0})
        elif op == "row-delete":
            if row is not None:
                rows.remove(row)
        elif op == "probability":
            key = clean_cell_value(payload.get("key"))
            if key not in PROBABILITY_KEYS:
                raise ObjectAnalysisError("Неизвестная стадия")
            risks["probabilities"][key] = _probability(payload.get("value"))
        else:
            raise ObjectAnalysisError("Неизвестное действие")
        record["risks"] = risks

    record = _change_record(base, actor, change)
    write_debug_log("object_analysis_risks_changed", {
        "dialogId": base, "op": op, "userId": clean_cell_value(actor.get("userId")),
    })
    return risk_view(base, record)


def risk_view(base: str, record: dict) -> dict:
    risks = record.get("risks") or _default_risks()
    return {"dialogId": base, "risks": risks, "total": risk_total(risks)}


# ---------------------------------------------------------------------------
# Figures of the ИД checklist
# ---------------------------------------------------------------------------

def _saved_checklist(dialog_id: str, checklist_key: str) -> dict | None:
    """A checklist as saved; None when it was never opened (nothing is
    created here)."""
    from app.checklists.normalization import normalize_checklist_data
    from app.checklists.storage import make_storage_dialog_id

    normalized = normalize_dialog_id(dialog_id)
    aliases = [normalized]
    if normalized.startswith("chat") and normalized[4:].isdigit():
        aliases.append(normalized[4:])
    conn = get_conn()
    try:
        row = None
        for alias in aliases:
            try:
                row = conn.execute(
                    "SELECT data_json FROM checklists WHERE dialog_id = ?",
                    (make_storage_dialog_id(alias, checklist_key),),
                ).fetchone()
            except Exception:
                row = None
            if row:
                break
    finally:
        conn.close()
    if not row:
        return None
    try:
        return normalize_checklist_data(json.loads(row["data_json"]), checklist_key)
    except Exception:
        return None


def _share(done: int, total: int) -> dict:
    return {
        "percent": round(done * 100 / total) if total else None,
        "done": done,
        "total": total,
    }


def _status(item: dict) -> str:
    return normalize_status(item.get("status"))


def id_figures(items: list[dict], groups: list[dict]) -> dict:
    """«Изыскания»: the subitems of the item «Изыскания»; «ТУ»: the section
    ТУ by the rule of the checklist progress. «Не требуется» is not
    counted."""
    from app.checklists.id_surveys import find_surveys_item
    from app.checklists.subitems import NOT_REQUIRED_STATUS, children_of, progress_items

    surveys = find_surveys_item(items)
    if surveys is not None and _status(surveys) == NOT_REQUIRED_STATUS:
        surveys = None
    if surveys is None:
        survey_share = {**_share(0, 0), "missing": True}
    else:
        children = [
            child for child in children_of(items, clean_cell_value(surveys.get("id")))
            if _status(child) != NOT_REQUIRED_STATUS
        ]
        survey_share = _share(sum(1 for child in children if _status(child) == "Есть"), len(children))

    tu_ids = {
        _int(group.get("id")) for group in groups
        if clean_cell_value(group.get("title")).lower() == TU_GROUP_TITLE
    }
    tu_items = [
        item for item in progress_items(items)
        if _int(item.get("group")) in tu_ids and _status(item) != NOT_REQUIRED_STATUS
    ]
    tu_share = _share(sum(1 for item in tu_items if _status(item) == "Есть"), len(tu_items))
    return {"surveys": survey_share, "tu": tu_share}


def project_id_figures(base: str) -> dict:
    """From the ИД checklist of the first stage (the chat of the project)."""
    data = _saved_checklist(base, "id")
    if data is None:
        return {"surveys": {**_share(0, 0), "missing": True}, "tu": _share(0, 0)}
    return id_figures(data.get("items") or [], data.get("groups") or [])


def project_ga_figure(base: str) -> dict:
    """«Чек-лист ГА»: the Граданализ of the OPR checklist of the first stage."""
    from app.checklists.grad_analysis import progress_counts

    try:
        return progress_counts(base)
    except Exception as exc:
        write_debug_log("object_analysis_ga_failed", {"base": base, "error": str(exc)})
        return _share(0, 0)


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

def _project_rows() -> list[dict]:
    """Projects of the application (the admin panel list), stages aside."""
    from app.checklists.project_phases import base_dialog_id

    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT dialog_id, project_name FROM project_storage_contexts"
        ).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()
    result: dict[str, dict] = {}
    for row in rows:
        dialog_id = normalize_dialog_id(row["dialog_id"])
        if not dialog_id or base_dialog_id(dialog_id) != dialog_id or dialog_id in result:
            continue
        result[dialog_id] = {"dialogId": dialog_id, "name": clean_cell_value(row["project_name"])}
    return list(result.values())


def _project_name(base: str, name: str) -> str:
    if name:
        return name
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT project_name FROM project_storage_contexts WHERE dialog_id = ?", (base,)
        ).fetchone()
    except Exception:
        row = None
    finally:
        conn.close()
    if row and clean_cell_value(row["project_name"]):
        return clean_cell_value(row["project_name"])
    data = _saved_checklist(base, "id") or {}
    return clean_cell_value(data.get("collabTitle")) or base


def checklist_link(base: str, checklist_key: str = "id") -> str:
    from app.checklists.messages import build_checklist_message_link

    try:
        return build_checklist_message_link(base, checklist_key)
    except Exception:
        return ""


def project_view(base: str, record: dict, *, name: str = "", user_id: str = "",
                 rights: dict | None = None) -> dict:
    from app.checklists.project_objects import contract_state

    try:
        contract = contract_state(base)
    except Exception as exc:
        contract = {"hasContract": False, "hint": f"Не удалось прочитать объект: {exc}"}
    rights = rights if rights is not None else user_rights(base, user_id)
    return {
        "dialogId": base,
        "name": _project_name(base, name),
        "canEdit": bool(rights.get("edit")),
        "contractStatus": record["contractStatus"],
        "contract": contract,
        "stages": record["stages"],
        "general": record["general"],
        "risk": risk_total(record["risks"]),
        "id": project_id_figures(base),
        "ga": project_ga_figure(base),
        "oprSections": record["oprSections"],
        "oprCustomerDate": record["oprCustomerDate"],
        "oprPdDate": record["oprPdDate"],
        "meeting": record["meeting"],
        "vks": record["vks"],
        "customerResponsible": record["customerResponsible"],
        "reminders": reminders_view(base, record),
        "idChecklistUrl": checklist_link(base, "id"),
        "oprChecklistUrl": checklist_link(base, "opr"),
    }


def reminders_view(base: str, record: dict) -> dict:
    try:
        sent = date_reminders.history(base)
    except Exception as exc:
        write_debug_log("object_analysis_reminder_history_failed", {"base": base, "error": str(exc)})
        sent = {}
    defaults = date_reminders.default_recipients()
    result = {}
    for field in date_reminders.FIELDS:
        meta = date_reminders.clean_meta(record["reminders"].get(field))
        result[field] = {
            "recipients": date_reminders.recipients_of(meta, defaults),
            "setByName": meta["setByName"],
            "history": sent.get(field, []),
        }
    return result


def _common(options: dict | None = None) -> dict:
    return {
        "options": options if options is not None else list_options(),
        "contractStatuses": list(CONTRACT_STATUSES),
        "riskLevels": list(RISK_LEVELS),
        "notRated": NOT_RATED,
        "listTitles": LIST_TITLES,
        "limits": {
            "general": MAX_GENERAL_TEXT,
            "customerResponsible": MAX_RESPONSIBLE_TEXT,
            "option": MAX_OPTION_LABEL,
        },
    }


def panel_payload(dialog_id: str, user_id: str) -> dict:
    """The panel of one project; {"visible": False} for everyone else."""
    base = base_of(dialog_id)
    rights = user_rights(base, user_id) if base else {"view": False}
    if not rights.get("view"):
        return {"visible": False}
    conn = _conn()
    try:
        record = _read_record(conn, base)
        options = list_options(conn)
    finally:
        conn.close()
    return {
        "visible": True,
        "rights": rights,
        "project": project_view(base, record, rights=rights),
        **_common(options),
    }


def contract_group(status: str) -> str:
    return status if status in CONTRACT_STATUSES else NO_CONTRACT_STATUS_GROUP


GROUP_ORDER = tuple(reversed(CONTRACT_STATUSES)) + (NO_CONTRACT_STATUS_GROUP,)


def slice_payload(dialog_id: str, user_id: str) -> dict:
    """All projects grouped by the contract status (the most advanced
    first, then by name); the project the slice was opened from goes
    first of all."""
    base = base_of(dialog_id)
    rights = user_rights(base, user_id) if base else {"view": False}
    if not rights.get("view"):
        raise ObjectAnalysisDenied("Анализ состояния объекта доступен руководителям, администраторам и ГИП проекта")
    conn = _conn()
    try:
        records = _read_records(conn)
        options = list_options(conn)
    finally:
        conn.close()
    projects = _project_rows()
    if base not in {project["dialogId"] for project in projects}:
        projects.append({"dialogId": base, "name": ""})
    rows = []
    for project in projects:
        project_base = project["dialogId"]
        record = records.get(project_base) or _empty_record()
        own_rights = rights if project_base == base else user_rights(project_base, user_id)
        view = project_view(project_base, record, name=project["name"], rights=own_rights)
        view["current"] = project_base == base
        view["group"] = contract_group(record["contractStatus"])
        rows.append(view)
    group_index = {group: index for index, group in enumerate(GROUP_ORDER)}
    rows.sort(key=lambda row: (
        0 if row["current"] else 1,
        group_index.get(row["group"], len(group_index)),
        row["name"].lower(),
        row["dialogId"],
    ))
    return {
        "currentDialogId": base,
        "rights": rights,
        "groups": list(GROUP_ORDER),
        "projects": rows,
        **_common(options),
    }


def risks_payload(dialog_id: str, user_id: str) -> dict:
    """The risk tables of all projects in the order of the slice."""
    sliced = slice_payload(dialog_id, user_id)
    conn = _conn()
    try:
        records = _read_records(conn)
    finally:
        conn.close()
    tables = []
    for project in sliced["projects"]:
        record = records.get(project["dialogId"]) or _empty_record()
        tables.append({
            **risk_view(project["dialogId"], record),
            "name": project["name"],
            "current": project["current"],
            "canEdit": project["canEdit"],
            "group": project["group"],
        })
    return {
        "currentDialogId": sliced["currentDialogId"],
        "tables": tables,
        "riskLevels": list(RISK_LEVELS),
        "notRated": NOT_RATED,
        "probabilityLevels": [
            {"from": threshold, "level": level} for threshold, level in PROBABILITY_LEVELS
        ],
        "limits": {"name": MAX_RISK_NAME, "description": MAX_RISK_DESCRIPTION, "rows": MAX_RISK_ROWS},
    }
