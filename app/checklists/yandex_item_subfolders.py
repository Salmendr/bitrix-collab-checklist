"""Yandex Disk side of folders inside an item (create / move / delete).

Jobs are recorded when an edit session is committed and executed by the
structure worker in order, before the item's pending file uploads (uploads
wait for them, see get_blocking_yandex_structure_job_for_item).
"""
from __future__ import annotations

import json

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.checklists.document_folders import (
    join_yandex_folder,
    normalize_relative_folder,
    parent_folder,
)
from app.checklists.storage import make_storage_dialog_id
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)
from app.checklists.yandex_structure_jobs import real_item_id


def _job_result(job: dict) -> dict:
    value = job.get("result")
    return value if isinstance(value, dict) else {}


def _resolve_item_folder(dialog_id: str, checklist_key: str, item: dict) -> str:
    from app.checklists.yandex_scope import YandexScopeError, item_folder

    try:
        return item_folder(dialog_id, checklist_key, item)
    except YandexScopeError:
        pass
    if not clean_cell_value(item.get("parentItemId")) and not bool(item.get("isCustom", False)):
        # A standard item folder is created on demand, as for an upload.
        from app.checklists.yandex_folders import ensure_item_yandex_folder_for_upload
        info = ensure_item_yandex_folder_for_upload(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_name=clean_cell_value(item.get("name")),
            item_id=clean_cell_value(item.get("id")),
            item_group=int(item.get("group") or 0),
            is_custom=False,
        ) or {}
        path = clean_cell_value((info.get("folder") or {}).get("path"))
        if path:
            return path
    raise RuntimeError("Папка пункта на Яндекс.Диске ещё не создана")


def _rebase_item_documents(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    source_path: str,
    target_path: str,
) -> int:
    from app.checklists.yandex_relocation import rebase_item_file_paths_in_transaction

    storage_dialog_id = make_storage_dialog_id(dialog_id, checklist_key)
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT data_json FROM checklists WHERE dialog_id = ?",
            (storage_dialog_id,),
        ).fetchone()
        if not row:
            conn.commit()
            return 0
        data = json.loads(row["data_json"] or "{}")
        count = 0
        for item in data.get("items") or []:
            if clean_cell_value(item.get("id")) != item_id:
                continue
            count = rebase_item_file_paths_in_transaction(
                conn,
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item=item,
                job={
                    "status": "completed",
                    "action": "move_item_folder",
                    "result": {
                        "sourcePath": source_path,
                        "folderPath": target_path,
                    },
                },
            )
            break
        conn.execute(
            "UPDATE checklists SET data_json = ? WHERE dialog_id = ?",
            (json.dumps(data, ensure_ascii=False), storage_dialog_id),
        )
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_subfolder_job(job: dict) -> dict:
    from app.checklists.yandex_upload_preflight import ensure_upload_folder
    from app.checklists.yandex_subfolders import find_item
    from app.yandex_disk.client import (
        yandex_disk_delete_path,
        yandex_disk_move_path,
        yandex_disk_try_get_resource_meta,
    )

    action = clean_cell_value(job.get("action"))
    dialog_id = normalize_dialog_id(job.get("dialog_id"))
    checklist_key = normalize_checklist_key(job.get("checklist_key"))
    item_id = real_item_id(job.get("item_id"))
    item = find_item(dialog_id, checklist_key, item_id)
    if not item:
        raise RuntimeError("Пункт папки не найден")
    base = _resolve_item_folder(dialog_id, checklist_key, item)
    result = _job_result(job)

    if action == "create_item_subfolder":
        relative = normalize_relative_folder(result.get("relativeFolder"), strict=False)
        path = join_yandex_folder(base, relative)
        ensure_upload_folder(dialog_id, path)
        return {"ok": True, "folderPath": path, "relativeFolder": relative}

    if action == "move_item_subfolder":
        source_relative = normalize_relative_folder(result.get("sourceFolder"), strict=False)
        target_relative = normalize_relative_folder(result.get("targetFolder"), strict=False)
        source = join_yandex_folder(base, source_relative)
        target = join_yandex_folder(base, target_relative)
        if not source_relative or not target_relative or source == target:
            return {"ok": True, "sourcePath": source, "folderPath": target, "unchanged": True}
        source_meta = yandex_disk_try_get_resource_meta(source)
        target_meta = yandex_disk_try_get_resource_meta(target)
        if source_meta and target_meta:
            raise RuntimeError(
                "На Яндекс.Диске уже есть папка «"
                + target_relative
                + "». Перенос остановлен, чтобы не смешать файлы."
            )
        if source_meta:
            ensure_upload_folder(dialog_id, join_yandex_folder(base, parent_folder(target_relative)))
            yandex_disk_move_path(source, target, overwrite=False)
        elif not target_meta:
            # The folder never reached Yandex (e.g. empty): create the result.
            ensure_upload_folder(dialog_id, target)
        rebased = _rebase_item_documents(dialog_id, checklist_key, item_id, source, target)
        write_debug_log("yandex_item_subfolder_moved", {
            "jobId": clean_cell_value(job.get("job_id")),
            "itemId": item_id,
            "sourcePath": source,
            "targetPath": target,
            "moved": bool(source_meta),
            "rebasedPaths": rebased,
        })
        return {"ok": True, "sourcePath": source, "folderPath": target, "rebasedPaths": rebased}

    if action == "delete_item_subfolder":
        relative = normalize_relative_folder(result.get("relativeFolder"), strict=False)
        if not relative:
            raise RuntimeError("Не указана папка для удаления")
        path = join_yandex_folder(base, relative)
        meta = yandex_disk_try_get_resource_meta(path)
        if meta and clean_cell_value(meta.get("type")) != "dir":
            raise RuntimeError("На Яндекс.Диске по пути папки находится файл. Удаление остановлено.")
        if meta:
            # Into the Yandex trash: a deleted folder can be restored there.
            yandex_disk_delete_path(path, permanently=False)
        return {"ok": True, "folderPath": path, "trashed": bool(meta)}

    raise RuntimeError(f"Unsupported subfolder action: {action}")
