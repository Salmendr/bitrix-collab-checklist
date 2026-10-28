"""Folders of custom items of the section «Концепция» put in their place.

An earlier rule made the folder of a custom item of the section «Концепция»
(the Концепция checklist) inside the folder of the standard item
«Согласованная/Утвержденная концепция». Moving that standard item into such
a custom item then had to put its folder into itself, and Yandex Disk
refused the move.

Once after the start of the application every such folder goes next to the
standard item, into the stage folder, and a move of its subitems that
failed is run again. A folder already in its place is never touched, so the
pass changes nothing the next time.
"""
from __future__ import annotations

import json
import threading
import time

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.checklists.utils import clean_cell_value, normalize_dialog_id


CHECKLIST_KEY = "concept"
STANDARD_ALIAS = "concept_approved"
START_DELAY_SECONDS = 30
JOB_WAIT_SECONDS = 300
FINISHED_STATUSES = frozenset({"completed", "error", "conflict", "cancelled", "disabled"})
BUSY_STATUSES = frozenset({"queued", "running"})

_started = False
_guard = threading.Lock()


def _path(value: str) -> str:
    from app.checklists.yandex_folders import normalize_yandex_disk_path

    value = clean_cell_value(value)
    return normalize_yandex_disk_path(value).rstrip("/") if value else ""


def _key(value: str) -> str:
    return _path(value).casefold()


def concept_dialog_ids() -> list[str]:
    suffix = "::" + CHECKLIST_KEY
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT dialog_id FROM checklists WHERE dialog_id LIKE ?",
            ("%" + suffix,),
        ).fetchall()
    finally:
        conn.close()
    return sorted({
        normalize_dialog_id(row["dialog_id"][:-len(suffix)])
        for row in rows
        if str(row["dialog_id"]).endswith(suffix)
    })


def _saved_items(dialog_id: str) -> list[dict]:
    from app.checklists.storage import make_storage_dialog_id

    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT data_json FROM checklists WHERE dialog_id = ?",
            (make_storage_dialog_id(dialog_id, CHECKLIST_KEY),),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return []
    try:
        items = json.loads(row["data_json"] or "{}").get("items") or []
    except Exception:
        return []
    return [item for item in items if isinstance(item, dict)]


def _item_folder(dialog_id: str, item: dict) -> str:
    from app.checklists.yandex_structure_jobs import (
        get_latest_completed_yandex_structure_folder_path,
    )

    return _path(
        clean_cell_value(item.get("yandexFolderPath"))
        or get_latest_completed_yandex_structure_folder_path(
            dialog_id=dialog_id,
            checklist_key=CHECKLIST_KEY,
            item_id=clean_cell_value(item.get("id")),
        )
    )


def misplaced_items(dialog_id: str) -> list[dict]:
    """Custom items (not subitems) whose folder lies right inside the folder
    of «Согласованная/Утвержденная концепция»."""
    from app.checklists.definitions.concept import STANDARD_CONCEPT_YANDEX_FOLDER_SPECS
    from app.checklists.subitems import parent_id_of
    from app.checklists.yandex_folders import get_root_path_from_context
    from app.checklists.yandex_scope import project_root
    from app.checklists.yandex_structure_jobs import get_latest_yandex_structure_job_for_item

    dialog_id = normalize_dialog_id(dialog_id)
    items = _saved_items(dialog_id)
    if not items:
        return []
    try:
        # Only a root the project really has, never a guessed one.
        project_root(dialog_id)
        root = get_root_path_from_context(dialog_id, CHECKLIST_KEY)
    except Exception:
        return []
    if not root:
        return []
    spec = STANDARD_CONCEPT_YANDEX_FOLDER_SPECS[STANDARD_ALIAS]
    standard_folders = {_key(f"{root.rstrip('/')}/{spec['relativePath']}")}
    for item in items:
        if not item.get("isCustom") and clean_cell_value(
            item.get("definitionName") or item.get("name")
        ).casefold() == clean_cell_value(spec["itemName"]).casefold():
            folder = _item_folder(dialog_id, item)
            if folder:
                standard_folders.add(folder.casefold())

    result = []
    for item in items:
        if not item.get("isCustom") or parent_id_of(item):
            continue
        folder = _item_folder(dialog_id, item)
        if "/" not in folder or folder.rsplit("/", 1)[0].casefold() not in standard_folders:
            continue
        latest = get_latest_yandex_structure_job_for_item(
            dialog_id=dialog_id,
            checklist_key=CHECKLIST_KEY,
            item_id=clean_cell_value(item.get("id")),
        ) or {}
        if clean_cell_value(latest.get("status")).lower() in BUSY_STATUSES:
            continue
        result.append({"item": item, "folder": folder})
    return result


def _run(job_id: str) -> dict:
    """Hand the job to the folder workers and wait for its end."""
    from app.checklists.yandex_structure_jobs import get_yandex_structure_job
    from app.checklists.yandex_structure_queue import enqueue_yandex_structure_job

    enqueue_yandex_structure_job(job_id, source="concept_folder_repair")
    deadline = time.monotonic() + JOB_WAIT_SECONDS
    job = get_yandex_structure_job(job_id) or {}
    while clean_cell_value(job.get("status")).lower() not in FINISHED_STATUSES:
        if time.monotonic() > deadline:
            break
        time.sleep(1)
        job = get_yandex_structure_job(job_id) or {}
    return job


def _start(job: dict) -> dict:
    from app.checklists.yandex_structure_state import persist_item_yandex_structure_state

    persist_item_yandex_structure_state(
        dialog_id=clean_cell_value(job.get("dialog_id")),
        checklist_key=CHECKLIST_KEY,
        item_id=clean_cell_value(job.get("item_id")),
        job=job,
    )
    return _run(clean_cell_value(job.get("job_id")))


def _retry_failed_subitem_moves(dialog_id: str, parent_id: str) -> int:
    """The move into this item that Yandex refused runs again."""
    from app.checklists.subitems import children_of
    from app.checklists.yandex_structure_jobs import (
        get_latest_yandex_structure_job_for_item,
        retry_yandex_structure_job,
    )

    retried = 0
    for child in children_of(_saved_items(dialog_id), parent_id):
        latest = get_latest_yandex_structure_job_for_item(
            dialog_id=dialog_id,
            checklist_key=CHECKLIST_KEY,
            item_id=clean_cell_value(child.get("id")),
        ) or {}
        if (
            clean_cell_value(latest.get("action")) != "move_item_folder"
            or clean_cell_value(latest.get("status")).lower() != "error"
        ):
            continue
        job = retry_yandex_structure_job(clean_cell_value(latest.get("job_id"))) or {}
        if clean_cell_value(job.get("status")).lower() != "queued":
            continue
        final = _start(job)
        retried += 1
        write_debug_log("concept_folder_repair_subitem_move_retried", {
            "dialogId": dialog_id,
            "itemId": clean_cell_value(child.get("id")),
            "jobId": clean_cell_value(job.get("job_id")),
            "status": clean_cell_value(final.get("status")),
            "error": clean_cell_value(final.get("error"))[:500],
            "folderPath": clean_cell_value(final.get("target_path")),
        })
    return retried


def repair_dialog(dialog_id: str) -> dict:
    from app.checklists.yandex_folders import (
        resolve_item_group_parent_yandex_path,
        sanitize_yandex_folder_name,
    )
    from app.checklists.yandex_structure_jobs import (
        create_yandex_structure_job,
        retry_yandex_structure_job,
    )
    from app.yandex_disk.client import yandex_disk_try_get_resource_meta

    dialog_id = normalize_dialog_id(dialog_id)
    moved = 0
    failed = 0
    retried = 0
    for entry in misplaced_items(dialog_id):
        item = entry["item"]
        folder = entry["folder"]
        item_id = clean_cell_value(item.get("id"))
        name = clean_cell_value(item.get("name"))
        group_id = int(item.get("group") or 0)
        # A folder already moved or removed by hand is left as it is.
        if not yandex_disk_try_get_resource_meta(folder):
            write_debug_log("concept_folder_repair_item_skipped", {
                "dialogId": dialog_id,
                "itemId": item_id,
                "folderPath": folder,
                "reason": "folder_not_on_disk",
            })
            continue
        parent = _path(resolve_item_group_parent_yandex_path(
            dialog_id=dialog_id,
            checklist_key=CHECKLIST_KEY,
            group_id=group_id,
        ))
        if not parent or not name or parent.casefold() == folder.casefold() or parent.casefold().startswith(folder.casefold() + "/"):
            continue
        job = create_yandex_structure_job(
            idempotency_key=f"concept-folder-repair:{dialog_id}:{item_id}:{folder}",
            dialog_id=dialog_id,
            checklist_key=CHECKLIST_KEY,
            item_id=item_id,
            action="move_item_folder",
            source_path=folder,
            target_path=f"{parent}/{sanitize_yandex_folder_name(name)}",
            folder_alias=clean_cell_value(item.get("yandexFolderAlias")),
            item_name=name,
            group_id=group_id,
            initial_status="queued",
            result={"sourceGroupId": group_id, "isCustom": True, "repair": "concept_custom_folder"},
        )
        status = clean_cell_value(job.get("status")).lower()
        if status in {"error", "conflict"}:
            # The pass of an earlier start failed: once more.
            job = retry_yandex_structure_job(clean_cell_value(job.get("job_id"))) or job
            status = clean_cell_value(job.get("status")).lower()
        if status != "queued":
            continue
        final = _start(job)
        ok = clean_cell_value(final.get("status")).lower() == "completed"
        write_debug_log("concept_folder_repair_item_moved" if ok else "concept_folder_repair_item_failed", {
            "dialogId": dialog_id,
            "itemId": item_id,
            "itemName": name,
            "jobId": clean_cell_value(job.get("job_id")),
            "sourcePath": folder,
            "folderPath": clean_cell_value(final.get("target_path")),
            "status": clean_cell_value(final.get("status")),
            "error": clean_cell_value(final.get("error"))[:500],
        })
        if not ok:
            failed += 1
            continue
        moved += 1
        retried += _retry_failed_subitem_moves(dialog_id, item_id)
    return {"dialogId": dialog_id, "moved": moved, "failed": failed, "retried": retried}


def repair_all() -> dict:
    from app.yandex_disk.client import is_yandex_disk_enabled

    if not is_yandex_disk_enabled():
        return {"ok": True, "skipped": "yandex_disk_disabled"}
    totals = {"projects": 0, "moved": 0, "failed": 0, "retried": 0}
    for dialog_id in concept_dialog_ids():
        try:
            result = repair_dialog(dialog_id)
        except Exception as exc:
            write_debug_log("concept_folder_repair_project_failed", {"dialogId": dialog_id, "error": str(exc)})
            continue
        if result["moved"] or result["failed"]:
            totals["projects"] += 1
        for key in ("moved", "failed", "retried"):
            totals[key] += result[key]
    if totals["projects"]:
        write_debug_log("concept_folder_repair_finished", totals)
    return {"ok": True, **totals}


def start_concept_folder_repair() -> bool:
    global _started
    with _guard:
        if _started:
            return False
        _started = True

    def run():
        time.sleep(START_DELAY_SECONDS)
        try:
            repair_all()
        except Exception as exc:
            write_debug_log("concept_folder_repair_failed", {"error": str(exc)})

    threading.Thread(target=run, name="concept-folder-repair", daemon=True).start()
    return True
