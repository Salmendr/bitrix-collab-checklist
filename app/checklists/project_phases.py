"""Project stages («Этап 1», «Этап 2», …) inside one project chat.

A stage is a full, independent set of the project checklists. Stage 1 keeps
the chat dialog id (``chat3122``): all existing data, links and messages stay
valid. Stage N ≥ 2 lives under its own storage dialog id ``chat3122__etapN``,
so checklists, edit sessions, Yandex jobs and uploads of different stages
never mix. Bitrix only ever sees the chat id (``base_dialog_id``).

On Yandex Disk every stage has its own root inside the project root:
``<project>/Этап 1``, ``<project>/Этап 2``. Adding the first extra stage moves
the three project folders of stage 1 into ``Этап 1`` and rewrites every
stored path of the project.
"""
from __future__ import annotations

import json
import re
import threading
import time
from datetime import datetime

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.checklists.utils import clean_cell_value, normalize_dialog_id


PHASE_DIALOG_SUFFIX = "__etap"
PHASE_DIALOG_RE = re.compile(r"^(?P<base>.+)" + re.escape(PHASE_DIALOG_SUFFIX) + r"(?P<no>\d+)$")
# The three top-level folders of a project (see yandex_project_structure).
PHASE_ROOT_FOLDERS = ("00_Исходные данные", "02_Выдача документации", "03_Архив")
# Project administrators: manage the GIP list, add stages.
PROJECT_ADMIN_USER_IDS = frozenset({"18", "138"})
# Waiting for background synchronization before stage 1 is moved.
MIGRATION_WAIT_SECONDS = 90
YANDEX_MOVE_TIMEOUT_SECONDS = 900

_ADD_LOCKS_GUARD = threading.Lock()
_ADD_LOCKS: dict[str, threading.Lock] = {}


class PhaseError(RuntimeError):
    """A stage operation refused or failed; the message is for the user."""


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------

def split_phase_dialog_id(dialog_id: str) -> tuple[str, int]:
    normalized = normalize_dialog_id(dialog_id)
    match = PHASE_DIALOG_RE.match(normalized)
    if not match:
        return normalized, 1
    number = int(match.group("no"))
    if number < 2:
        return normalize_dialog_id(match.group("base")), 1
    return normalize_dialog_id(match.group("base")), number


def base_dialog_id(dialog_id: str) -> str:
    """The Bitrix chat of any stage dialog id."""
    return split_phase_dialog_id(dialog_id)[0]


def phase_number(dialog_id: str) -> int:
    return split_phase_dialog_id(dialog_id)[1]


def phase_dialog_id(base: str, number: int) -> str:
    base = base_dialog_id(base)
    return base if int(number) <= 1 else f"{base}{PHASE_DIALOG_SUFFIX}{int(number)}"


def phase_name(number: int) -> str:
    return f"Этап {int(number)}"


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def _ensure_tables(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS project_phases (
            base_dialog_id TEXT NOT NULL,
            phase_no INTEGER NOT NULL,
            dialog_id TEXT NOT NULL,
            name TEXT NOT NULL,
            yandex_path TEXT,
            created_at TEXT,
            created_by_id TEXT,
            created_by_name TEXT,
            PRIMARY KEY (base_dialog_id, phase_no)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS project_gips (
            base_dialog_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            user_name TEXT,
            added_by_id TEXT,
            added_at TEXT,
            PRIMARY KEY (base_dialog_id, user_id)
        )
    """)


def _conn():
    conn = get_conn()
    _ensure_tables(conn)
    return conn


def list_phases(dialog_id: str) -> list[dict]:
    """Stages of the project, [] while the project has only its single stage."""
    base = base_dialog_id(dialog_id)
    if not base:
        return []
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT * FROM project_phases WHERE base_dialog_id = ? ORDER BY phase_no",
            (base,),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "no": int(row["phase_no"]),
            "name": clean_cell_value(row["name"]) or phase_name(row["phase_no"]),
            "dialogId": clean_cell_value(row["dialog_id"]),
            "yandexPath": clean_cell_value(row["yandex_path"]),
        }
        for row in rows
    ]


def is_phased(dialog_id: str) -> bool:
    return bool(list_phases(dialog_id))


def phase_label(dialog_id: str) -> str:
    """«Этап N» when the project has stages, otherwise ""."""
    if not is_phased(dialog_id):
        return ""
    return phase_name(phase_number(dialog_id))


def _insert_phase(conn, base: str, number: int, yandex_path: str, user_id: str, user_name: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO project_phases(
            base_dialog_id, phase_no, dialog_id, name, yandex_path,
            created_at, created_by_id, created_by_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            base,
            int(number),
            phase_dialog_id(base, number),
            phase_name(number),
            clean_cell_value(yandex_path),
            datetime.now().isoformat(),
            clean_cell_value(user_id),
            clean_cell_value(user_name),
        ),
    )


# ---------------------------------------------------------------------------
# GIP list and rights
# ---------------------------------------------------------------------------

def list_gips(dialog_id: str) -> list[dict]:
    base = base_dialog_id(dialog_id)
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT user_id, user_name FROM project_gips WHERE base_dialog_id = ? ORDER BY added_at, user_id",
            (base,),
        ).fetchall()
    finally:
        conn.close()
    return [
        {"userId": clean_cell_value(row["user_id"]), "name": clean_cell_value(row["user_name"])}
        for row in rows
    ]


def is_project_admin(user_id: str) -> bool:
    return clean_cell_value(user_id) in PROJECT_ADMIN_USER_IDS


def object_gip_user_ids(dialog_id: str) -> set[str] | None:
    """People of the ГИП fields taken from the Bitrix24 objects; None while
    the project has no object data (the manual GIP list applies)."""
    from app.checklists.project_objects import object_people_user_ids

    try:
        return object_people_user_ids(base_dialog_id(dialog_id))
    except Exception as exc:
        write_debug_log("project_object_people_failed", {"dialogId": dialog_id, "error": str(exc)})
        return None


def manager_user_ids(dialog_id: str) -> list[str]:
    """Non-admin users who manage the project: the GIPs."""
    object_ids = object_gip_user_ids(dialog_id)
    if object_ids is not None:
        return sorted(object_ids)
    return [gip["userId"] for gip in list_gips(dialog_id)]


def can_manage_phases(dialog_id: str, user_id: str) -> bool:
    user = clean_cell_value(user_id)
    if not user:
        return False
    return is_project_admin(user) or user in manager_user_ids(dialog_id)


def _refuse_when_object_driven(dialog_id: str) -> None:
    if object_gip_user_ids(dialog_id) is not None:
        raise PhaseError(
            "ГИП подтягивается из объекта Битрикс24. Измените его в карточке объекта "
            "и нажмите «Подтянуть данные из объекта» в админ-панели."
        )


def add_gip(dialog_id: str, *, user_id: str, user_name: str, acting_user_id: str) -> list[dict]:
    if not is_project_admin(acting_user_id):
        raise PhaseError("Назначать ГИП могут только администраторы")
    _refuse_when_object_driven(dialog_id)
    user = clean_cell_value(user_id)
    if not user or not user.isdigit():
        raise PhaseError("Выберите сотрудника Битрикс24")
    base = base_dialog_id(dialog_id)
    conn = _conn()
    try:
        conn.execute(
            """
            INSERT INTO project_gips(base_dialog_id, user_id, user_name, added_by_id, added_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(base_dialog_id, user_id) DO UPDATE SET user_name = excluded.user_name
            """,
            (base, user, clean_cell_value(user_name) or f"ID {user}",
             clean_cell_value(acting_user_id), datetime.now().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()
    write_debug_log("project_gip_added", {"dialogId": base, "userId": user, "actingUserId": acting_user_id})
    return list_gips(base)


def remove_gip(dialog_id: str, *, user_id: str, acting_user_id: str) -> list[dict]:
    if not is_project_admin(acting_user_id):
        raise PhaseError("Назначать ГИП могут только администраторы")
    _refuse_when_object_driven(dialog_id)
    base = base_dialog_id(dialog_id)
    conn = _conn()
    try:
        conn.execute(
            "DELETE FROM project_gips WHERE base_dialog_id = ? AND user_id = ?",
            (base, clean_cell_value(user_id)),
        )
        conn.commit()
    finally:
        conn.close()
    write_debug_log("project_gip_removed", {"dialogId": base, "userId": user_id, "actingUserId": acting_user_id})
    return list_gips(base)


# ---------------------------------------------------------------------------
# Path rewriting of stage 1
# ---------------------------------------------------------------------------

def _path_prefixes(root: str, target_root: str) -> list[tuple[str, str]]:
    """(old, new) prefixes of the three moved folders, in every stored form."""
    from app.checklists.yandex_scope import canonical_path

    root = canonical_path(root)
    target_root = canonical_path(target_root)
    pairs = []
    for folder in PHASE_ROOT_FOLDERS:
        old = f"{root}/{folder}"
        new = f"{target_root}/{folder}"
        pairs.append((old, new))
        # Some records keep the path without the «disk:» scheme.
        pairs.append((old[len("disk:"):], new[len("disk:"):]))
    return pairs


def rebase_text(value: str, prefixes: list[tuple[str, str]]) -> str:
    for old, new in prefixes:
        if value == old or value.startswith(old + "/"):
            return new + value[len(old):]
    return value


def rebase_value(value, prefixes: list[tuple[str, str]]):
    """Rewrite every string of a JSON-like value; returns (value, changes)."""
    if isinstance(value, str):
        rebased = rebase_text(value, prefixes)
        return rebased, int(rebased != value)
    if isinstance(value, list):
        changes = 0
        result = []
        for entry in value:
            rebased, count = rebase_value(entry, prefixes)
            result.append(rebased)
            changes += count
        return result, changes
    if isinstance(value, dict):
        changes = 0
        result = {}
        for key, entry in value.items():
            rebased, count = rebase_value(entry, prefixes)
            result[key] = rebased
            changes += count
        return result, changes
    return value, 0


def _rebase_column_value(raw, prefixes):
    if not isinstance(raw, str) or not raw:
        return raw, 0
    stripped = raw.strip()
    if stripped[:1] in {"{", "["}:
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = None
        if parsed is not None:
            rebased, count = rebase_value(parsed, prefixes)
            if not count:
                return raw, 0
            return json.dumps(rebased, ensure_ascii=False), count
    rebased = rebase_text(raw, prefixes)
    return rebased, int(rebased != raw)


def rebase_project_paths_in_transaction(conn, base: str, prefixes: list[tuple[str, str]]) -> dict:
    """Rewrite stored Yandex paths of every table row of the project.

    Rows are found by ``dialog_id`` (the chat id, or the checklist storage id
    ``chat::key``). Text columns holding JSON are rewritten structurally.
    """
    tables = [
        row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        if row[0] not in {"project_phases", "project_gips"}
    ]
    report: dict[str, int] = {}
    for table in tables:
        columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
        names = [column[1] for column in columns]
        if "dialog_id" not in names:
            continue
        text_columns = [
            column[1] for column in columns
            if column[1] != "dialog_id"
            and (not column[2] or "TEXT" in str(column[2]).upper() or "CHAR" in str(column[2]).upper())
        ]
        if not text_columns:
            continue
        rows = conn.execute(
            f"SELECT rowid AS _rowid, {', '.join(text_columns)} FROM {table} "
            "WHERE dialog_id = ? OR dialog_id LIKE ?",
            (base, base + "::%"),
        ).fetchall()
        changed_rows = 0
        for row in rows:
            updates = {}
            for column in text_columns:
                value, count = _rebase_column_value(row[column], prefixes)
                if count:
                    updates[column] = value
            if updates:
                assignments = ", ".join(f"{column} = ?" for column in updates)
                conn.execute(
                    f"UPDATE {table} SET {assignments} WHERE rowid = ?",
                    (*updates.values(), row["_rowid"]),
                )
                changed_rows += 1
        if changed_rows:
            report[table] = changed_rows
    return report


# ---------------------------------------------------------------------------
# Adding a stage
# ---------------------------------------------------------------------------

BUSY_SESSION_STATUSES = ("active", "committing", "rolling_back")
BUSY_JOB_STATUSES = ("queued", "running")


def _project_busy_reason(base: str) -> str:
    from app.checklists.yandex_warmup_queue import (
        YANDEX_WARMUP_GUARD,
        YANDEX_WARMUP_QUEUED_DIALOG_IDS,
        YANDEX_WARMUP_RUNNING_DIALOG_IDS,
    )

    conn = get_conn()
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        placeholders = ", ".join("?" for _ in BUSY_SESSION_STATUSES)
        if "edit_sessions" in tables:
            rows = conn.execute(
                f"SELECT user_name, user_id FROM edit_sessions WHERE dialog_id = ? AND status IN ({placeholders})",
                (base, *BUSY_SESSION_STATUSES),
            ).fetchall()
            if rows:
                names = sorted({clean_cell_value(row["user_name"]) or f"ID {row['user_id']}" for row in rows})
                return "Чек-листы проекта сейчас редактирует: " + ", ".join(names) + "."
        job_placeholders = ", ".join("?" for _ in BUSY_JOB_STATUSES)
        if "yandex_structure_jobs" in tables:
            count = conn.execute(
                f"SELECT COUNT(*) FROM yandex_structure_jobs WHERE dialog_id = ? AND status IN ({job_placeholders})",
                (base, *BUSY_JOB_STATUSES),
            ).fetchone()[0]
            if count:
                return f"Идёт синхронизация папок с Яндекс.Диском (задач: {count})."
        if "upload_jobs" in tables:
            count = conn.execute(
                f"SELECT COUNT(*) FROM upload_jobs WHERE dialog_id = ? AND status IN ({job_placeholders})",
                (base, *BUSY_JOB_STATUSES),
            ).fetchone()[0]
            if count:
                return f"Идёт синхронизация файлов с Яндекс.Диском (задач: {count})."
    finally:
        conn.close()
    with YANDEX_WARMUP_GUARD:
        if base in YANDEX_WARMUP_QUEUED_DIALOG_IDS or base in YANDEX_WARMUP_RUNNING_DIALOG_IDS:
            return "Идёт создание папок проекта на Яндекс.Диске."
    return ""


def _wait_until_idle(base: str, seconds: int) -> str:
    deadline = time.monotonic() + max(0, seconds)
    while True:
        reason = _project_busy_reason(base)
        if not reason or time.monotonic() >= deadline:
            return reason
        time.sleep(2)


def _yandex_enabled_for(context: dict) -> bool:
    from app.yandex_disk.client import is_yandex_disk_enabled

    targets = (context.get("storageMode") or {}).get("mirrorTargets") or []
    return bool(context) and "yandex_disk" in targets and bool(is_yandex_disk_enabled())


def _publish(path: str) -> str:
    from app.yandex_disk.client import (
        yandex_disk_get_resource_meta,
        yandex_disk_publish_path,
    )
    try:
        meta = yandex_disk_get_resource_meta(path)
        url = clean_cell_value(meta.get("public_url"))
        if not url:
            yandex_disk_publish_path(path)
            url = clean_cell_value(yandex_disk_get_resource_meta(path).get("public_url"))
        return url
    except Exception as exc:
        write_debug_log("project_phase_publish_failed", {"path": path, "error": str(exc)})
        return ""


def _migrate_first_phase(base: str, acting_user_id: str, acting_user_name: str) -> dict:
    """Move stage 1 into «Этап 1» and rewrite its stored paths."""
    from app.checklists.storage import get_project_storage_context, save_project_storage_context
    from app.checklists.yandex_resource_locks import yandex_project_resource_guard
    from app.checklists.yandex_scope import canonical_path
    from app.yandex_disk.client import (
        yandex_disk_move_path,
        yandex_disk_try_get_resource_meta,
    )
    from app.checklists.yandex_folders import ensure_yandex_folder_chain

    context = get_project_storage_context(base) or {}
    yandex_disk = dict(context.get("yandexDisk") or {})
    root = canonical_path(yandex_disk.get("projectRootPath"))
    if not root or root == "disk:/":
        raise PhaseError("У проекта не задана корневая папка на Яндекс.Диске")
    stage_root = f"{root}/{phase_name(1)}"
    use_yandex = _yandex_enabled_for(context)

    with yandex_project_resource_guard(base, operation="project_phase_migration"):
        busy = _project_busy_reason(base)
        if busy:
            raise PhaseError(busy + " Повторите позже.")
        moved: list[str] = []
        stage_url = ""
        if use_yandex:
            for folder in PHASE_ROOT_FOLDERS:
                if yandex_disk_try_get_resource_meta(f"{stage_root}/{folder}"):
                    raise PhaseError(
                        f"В папке «{phase_name(1)}» на Яндекс.Диске уже есть «{folder}». "
                        "Перенос остановлен, чтобы не смешать файлы."
                    )
            ensure_yandex_folder_chain(stage_root)
            try:
                for folder in PHASE_ROOT_FOLDERS:
                    source = f"{root}/{folder}"
                    if not yandex_disk_try_get_resource_meta(source):
                        continue
                    yandex_disk_move_path(
                        source,
                        f"{stage_root}/{folder}",
                        overwrite=False,
                        wait_timeout=YANDEX_MOVE_TIMEOUT_SECONDS,
                    )
                    moved.append(folder)
            except Exception as exc:
                _rollback_moves(root, stage_root, moved)
                raise PhaseError(
                    "Не удалось перенести папки проекта в «Этап 1»: " + str(exc)
                ) from exc
            stage_url = _publish(stage_root)

        prefixes = _path_prefixes(root, stage_root)
        conn = get_conn()
        try:
            _ensure_tables(conn)
            conn.execute("BEGIN IMMEDIATE")
            report = rebase_project_paths_in_transaction(conn, base, prefixes)
            _insert_phase(conn, base, 1, stage_root, acting_user_id, acting_user_name)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if use_yandex:
                _rollback_moves(root, stage_root, moved)
            raise PhaseError("Не удалось обновить пути файлов Этапа 1: " + str(exc)) from exc
        finally:
            conn.close()

        # The stored row was rewritten above; the loaded context is hydrated
        # from the old root, so rewrite it as a whole, then switch its root.
        context, _ = rebase_value(get_project_storage_context(base) or {}, prefixes)
        yandex_disk = dict(context.get("yandexDisk") or {})
        yandex_disk["projectBaseRootPath"] = root
        yandex_disk["projectBaseRootUrl"] = clean_cell_value(yandex_disk.get("projectBaseRootUrl")) or clean_cell_value(yandex_disk.get("projectRootUrl"))
        yandex_disk["projectRootPath"] = stage_root
        yandex_disk["projectRootUrl"] = stage_url
        save_project_storage_context(base, {**context, "yandexDisk": yandex_disk})

    write_debug_log("project_phase_first_migrated", {
        "dialogId": base,
        "rootPath": root,
        "stageRootPath": stage_root,
        "movedFolders": moved,
        "rebasedRows": report,
        "yandex": use_yandex,
    })
    return {"root": root, "stageRoot": stage_root, "moved": moved, "rebased": report}


def _rollback_moves(root: str, stage_root: str, moved: list[str]) -> None:
    from app.yandex_disk.client import yandex_disk_move_path

    for folder in reversed(moved):
        try:
            yandex_disk_move_path(
                f"{stage_root}/{folder}",
                f"{root}/{folder}",
                overwrite=False,
                wait_timeout=YANDEX_MOVE_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            write_debug_log("project_phase_migration_rollback_failed", {
                "rootPath": root,
                "stageRootPath": stage_root,
                "folder": folder,
                "error": str(exc),
            })


def _create_phase(base: str, number: int, acting_user_id: str, acting_user_name: str) -> dict:
    """An empty stage: the configured checklists and their Yandex folders."""
    from app.checklists.config import list_checklist_configs
    from app.checklists.storage import (
        get_checklist,
        get_project_storage_context,
        save_checklist,
        save_project_storage_context,
    )
    from app.checklists.yandex_folders import ensure_yandex_folder_chain
    from app.checklists.yandex_resource_locks import yandex_project_resource_guard
    from app.checklists.yandex_warmup_queue import enqueue_yandex_warmup

    base_context = get_project_storage_context(base) or {}
    base_disk = base_context.get("yandexDisk") or {}
    root = clean_cell_value(base_disk.get("projectBaseRootPath"))
    if not root:
        raise PhaseError("Не найдена корневая папка проекта")
    dialog_id = phase_dialog_id(base, number)
    stage_root = f"{root}/{phase_name(number)}"
    use_yandex = _yandex_enabled_for(base_context)

    context = {
        "dialogId": dialog_id,
        "projectId": base_context.get("projectId") or "",
        "projectName": base_context.get("projectName") or "",
        "storageMode": base_context.get("storageMode") or {},
        "yandexDisk": {
            "provider": clean_cell_value(base_disk.get("provider")) or "yandex_disk",
            "projectRootPath": stage_root,
            "projectRootUrl": "",
            "projectBaseRootPath": root,
            "projectBaseRootUrl": clean_cell_value(base_disk.get("projectBaseRootUrl")),
            "folders": {},
        },
        "itemMappings": [],
        "bitrix": base_context.get("bitrix") or {},
    }
    save_project_storage_context(dialog_id, context)

    collab_title = ""
    for config in list_checklist_configs():
        source = get_checklist(base, config.key)
        collab_title = collab_title or clean_cell_value(source.get("collabTitle"))
    for config in list_checklist_configs():
        data = get_checklist(dialog_id, config.key)
        if collab_title and not clean_cell_value(data.get("collabTitle")):
            data["collabTitle"] = collab_title
            save_checklist(dialog_id, data, config.key)

    stage_url = ""
    if use_yandex:
        with yandex_project_resource_guard(dialog_id, operation="project_phase_create"):
            ensure_yandex_folder_chain(stage_root)
            stage_url = _publish(stage_root)
        saved = get_project_storage_context(dialog_id) or context
        disk = dict(saved.get("yandexDisk") or {})
        disk["projectRootUrl"] = stage_url
        save_project_storage_context(dialog_id, {**saved, "yandexDisk": disk})

    conn = _conn()
    try:
        _insert_phase(conn, base, number, stage_root, acting_user_id, acting_user_name)
        conn.commit()
    finally:
        conn.close()

    warmup = enqueue_yandex_warmup(dialog_id, source="project_phase_created") if use_yandex else {}
    write_debug_log("project_phase_created", {
        "dialogId": base,
        "phaseDialogId": dialog_id,
        "phaseNo": number,
        "stageRootPath": stage_root,
        "yandex": use_yandex,
        "warmup": warmup,
    })
    return {"no": number, "dialogId": dialog_id, "stageRoot": stage_root}


def add_phase(dialog_id: str, *, acting_user_id: str, acting_user_name: str,
              wait_seconds: int = MIGRATION_WAIT_SECONDS) -> dict:
    base = base_dialog_id(dialog_id)
    if not base:
        raise PhaseError("Не указан проект")
    if not can_manage_phases(base, acting_user_id):
        raise PhaseError("Добавлять этапы могут только администраторы и ГИП проекта")
    with _ADD_LOCKS_GUARD:
        lock = _ADD_LOCKS.setdefault(base, threading.Lock())
    if not lock.acquire(blocking=False):
        raise PhaseError("Этап уже добавляется — дождитесь окончания")
    try:
        phases = list_phases(base)
        migration = {}
        if not phases:
            busy = _wait_until_idle(base, wait_seconds)
            if busy:
                raise PhaseError(busy + " Повторите позже.")
            migration = _migrate_first_phase(base, acting_user_id, acting_user_name)
            number = 2
        else:
            number = max(phase["no"] for phase in phases) + 1
        created = _create_phase(base, number, acting_user_id, acting_user_name)
        return {
            "phase": created,
            "phases": list_phases(base),
            "migration": migration,
        }
    finally:
        lock.release()


def phase_summary(dialog_id: str, user_id: str = "") -> dict:
    """Stage data for the popup bootstrap."""
    base, number = split_phase_dialog_id(dialog_id)
    phases = list_phases(base)
    return {
        "baseDialogId": base,
        "current": number,
        "label": phase_name(number) if phases else "",
        "phases": phases,
        "gips": list_gips(base),
        "managerUserIds": manager_user_ids(base),
        "adminUserIds": sorted(PROJECT_ADMIN_USER_IDS),
    }
