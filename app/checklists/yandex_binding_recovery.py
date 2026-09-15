"""Manual repair of a folder move overwritten by a stale browser snapshot.

No name search, folder creation, upload, or deletion is performed here.
Only the exact source -> destination of the latest completed move is trusted.
"""
import copy
import json

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.checklists.storage import make_storage_dialog_id, get_project_storage_context
from app.checklists.utils import clean_cell_value, normalize_dialog_id, normalize_checklist_key
from app.checklists.yandex_resource_locks import yandex_project_resource_guard
from app.checklists.yandex_scope import canonical_path, require_project_path, item_folder, YandexScopeError
from app.checklists.yandex_structure_jobs import list_latest_yandex_structure_jobs_for_checklist
from app.checklists.yandex_structure_state import build_yandex_structure_item_fields
from app.checklists.yandex_relocation import rebase_item_file_paths_in_transaction
from app.checklists.yandex_upload_preflight import is_manual_recovery
from app.yandex_disk.client import yandex_disk_try_get_resource_meta


def restore_confirmed_item_bindings(*, dialog_id, checklist_key, item_id, source):
    result = {"restored": 0, "items": []}
    if not is_manual_recovery(source) or not clean_cell_value(item_id):
        return result
    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)
    storage_id = make_storage_dialog_id(dialog_id, checklist_key)
    with yandex_project_resource_guard(dialog_id, checklist_key=checklist_key,
                                       item_id=item_id, operation="manual_binding_restore"):
        latest = list_latest_yandex_structure_jobs_for_checklist(
            dialog_id=dialog_id, checklist_key=checklist_key,
        )
        conn = get_conn()
        try:
            row = conn.execute("SELECT data_json FROM checklists WHERE dialog_id=?", (storage_id,)).fetchone()
        finally:
            conn.close()
        if not row:
            return result
        original_json = row["data_json"]
        data = json.loads(original_json)
        items = data.get("items") or []
        requested = next((i for i in items if clean_cell_value(i.get("id")) == item_id), None)
        if requested is None:
            return result
        context = get_project_storage_context(dialog_id) or {}
        try:
            requested_path = item_folder(dialog_id, checklist_key, requested, context)
        except YandexScopeError:
            requested_path = ""
        planned = copy.deepcopy(items)
        candidates = []
        for item in planned:
            identity = clean_cell_value(item.get("id"))
            job = latest.get(identity) or {}
            # Repair the clicked item and its conflicting owners only.
            # An unrelated failure must not change another section's data.
            try:
                path = canonical_path(item.get("yandexFolderPath"))
            except YandexScopeError:
                continue  # An unrelated malformed path cannot be repaired here.
            if identity != item_id and (not requested_path or path != requested_path):
                continue
            if (job.get("status") != "completed"
                    or job.get("action") not in {"move_item_folder", "rename_item_folder"}
                    or int(job.get("group_id") or 0) != int(item.get("group") or 0)
                    or clean_cell_value(job.get("item_name")) != clean_cell_value(item.get("name"))):
                continue
            details = job.get("result") or {}
            # Require the recorded result, never an unconfirmed planned target.
            if not details.get("folderPath"):
                continue
            old = require_project_path(dialog_id, details.get("sourcePath") or job.get("source_path"), context=context)
            target = require_project_path(dialog_id, details["folderPath"], context=context)
            if old == target or path != old:
                continue
            item.update(build_yandex_structure_item_fields(item=item, job=job))
            candidates.append((item, job, target))
        if not candidates:
            return result

        # Evaluate final ownership before changing anything. Two real owners
        # of the destination remain a manual conflict, even with old jobs.
        for item, job, target in candidates:
            for other in planned:
                if other.get("id") == item.get("id"):
                    continue
                try:
                    other_path = item_folder(dialog_id, checklist_key, other, context)
                except YandexScopeError:
                    continue
                if other_path == target:
                    raise YandexScopeError("Восстановление привязки остановлено: целевая папка занята другим пунктом.")
            meta = yandex_disk_try_get_resource_meta(target)
            if (not meta or meta.get("type") != "dir"
                    or canonical_path(meta.get("path")) != target):
                raise YandexScopeError("Не подтверждена папка завершённого перемещения. Привязки не изменены.")

        conn = get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute("SELECT data_json FROM checklists WHERE dialog_id=?", (storage_id,)).fetchone()
            if not current or current["data_json"] != original_json or get_project_storage_context(dialog_id) != context:
                raise YandexScopeError("Данные изменились во время проверки. Повторите синхронизацию вручную.")
            for item, job, target in candidates:
                current_job = conn.execute(
                    "SELECT job_id,status,result_json FROM yandex_structure_jobs "
                    "WHERE dialog_id=? AND checklist_key=? AND item_id=? "
                    "ORDER BY created_at DESC,rowid DESC LIMIT 1",
                    (dialog_id, checklist_key, item["id"]),
                ).fetchone()
                if (not current_job or current_job["job_id"] != job["job_id"]
                        or current_job["status"] != "completed"
                        or json.loads(current_job["result_json"] or "{}") != job.get("result")):
                    raise YandexScopeError("Появилась новая операция с папкой. Повторите синхронизацию после её завершения.")
            for item, job, target in candidates:
                rebase_item_file_paths_in_transaction(
                    conn, dialog_id=dialog_id, checklist_key=checklist_key, item=item, job=job,
                )
                result["items"].append({"itemId": item["id"], "jobId": job["job_id"], "folderPath": target})
            data["items"] = planned
            conn.execute("UPDATE checklists SET data_json=? WHERE dialog_id=?",
                         (json.dumps(data, ensure_ascii=False), storage_id))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        result["restored"] = len(candidates)
        write_debug_log("yandex_item_bindings_restored", {
            "dialogId": dialog_id, "checklistKey": checklist_key,
            "requestedItemId": item_id, "source": source, **result,
        })
        return result
