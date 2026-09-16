"""Manual repair of a folder move overwritten by a stale browser snapshot.

No name search, folder creation, upload, or deletion is performed here.
Only the exact source -> destination of the latest completed mutation is
trusted.  A newer passive conflict report may describe the stale binding but
must not erase that confirmed history.
"""
import copy
import json

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.checklists.storage import make_storage_dialog_id, get_project_storage_context
from app.checklists.utils import clean_cell_value, normalize_dialog_id, normalize_checklist_key
from app.checklists.yandex_resource_locks import yandex_project_resource_guard
from app.checklists.yandex_scope import canonical_path, require_project_path, item_folder, YandexScopeError
from app.checklists.yandex_structure_jobs import (
    insert_yandex_structure_job_in_transaction,
    list_yandex_structure_job_history_for_checklist,
)
from app.checklists.yandex_structure_state import build_yandex_structure_item_fields
from app.checklists.yandex_relocation import rebase_item_file_paths_in_transaction
from app.checklists.yandex_upload_preflight import is_manual_recovery
from app.yandex_disk.client import yandex_disk_try_get_resource_meta


def _is_passive_conflict_record(record: dict) -> bool:
    """Return True only for a conflict produced without a remote mutation."""
    if clean_cell_value(record.get("status")).lower() != "conflict":
        return False
    if int(record.get("attempts") or 0) != 0:
        return False
    details = record.get("result") or {}
    return (
        isinstance(details, dict)
        and (
            isinstance(details.get("conflictCandidates"), list)
            or bool(details.get("identityAmbiguity"))
        )
    )


def restore_confirmed_item_bindings(*, dialog_id, checklist_key, item_id, source):
    result = {"restored": 0, "items": []}
    if not is_manual_recovery(source) or not clean_cell_value(item_id):
        return result
    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)
    storage_id = make_storage_dialog_id(dialog_id, checklist_key)
    with yandex_project_resource_guard(dialog_id, checklist_key=checklist_key,
                                       item_id=item_id, operation="manual_binding_restore"):
        history = list_yandex_structure_job_history_for_checklist(
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
            item_history = history.get(identity) or []
            job = next(
                (
                    record
                    for record in item_history
                    if clean_cell_value(record.get("status")).lower() == "completed"
                ),
                {},
            )
            newer_records = [
                record
                for record in item_history
                if int(record.get("structure_rowid") or 0)
                > int(job.get("structure_rowid") or 0)
            ]
            # Repair the clicked item and its conflicting owners only.
            # An unrelated failure must not change another section's data.
            try:
                path = canonical_path(item.get("yandexFolderPath"))
            except YandexScopeError:
                continue  # An unrelated malformed path cannot be repaired here.
            if identity != item_id and (not requested_path or path != requested_path):
                continue
            # Only a passive scan conflict is allowed to follow the confirmed
            # operation.  Failed, cancelled, pending or successful mutations
            # may represent a newer remote state and therefore block reuse.
            if any(not _is_passive_conflict_record(record) for record in newer_records):
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
            passive_conflicts_by_item = {}
            for item, job, target in candidates:
                confirmed_job = conn.execute(
                    "SELECT rowid AS structure_rowid,* FROM yandex_structure_jobs "
                    "WHERE job_id=? LIMIT 1",
                    (job["job_id"],),
                ).fetchone()
                if (not confirmed_job
                        or confirmed_job["status"] != "completed"
                        or confirmed_job["action"] not in {"move_item_folder", "rename_item_folder"}
                        or json.loads(confirmed_job["result_json"] or "{}") != job.get("result")):
                    raise YandexScopeError(
                        "Подтверждённая операция с папкой изменилась. "
                        "Привязки не обновлены."
                    )

                newer_rows = conn.execute(
                    "SELECT rowid AS structure_rowid,* FROM yandex_structure_jobs "
                    "WHERE dialog_id=? AND checklist_key=? AND item_id=? "
                    "AND rowid>? ORDER BY rowid ASC",
                    (
                        dialog_id,
                        checklist_key,
                        item["id"],
                        int(confirmed_job["structure_rowid"]),
                    ),
                ).fetchall()
                passive_conflicts = []
                for newer_row in newer_rows:
                    newer = dict(newer_row)
                    newer["result"] = json.loads(newer.get("result_json") or "{}")
                    if not _is_passive_conflict_record(newer):
                        raise YandexScopeError(
                            "После подтверждённого перемещения появилась новая "
                            "операция с папкой. Сначала завершите её вручную."
                        )
                    passive_conflicts.append(newer)
                passive_conflicts_by_item[item["id"]] = passive_conflicts
            for item, job, target in candidates:
                details = dict(job.get("result") or {})
                source_path = require_project_path(
                    dialog_id,
                    details.get("sourcePath") or job.get("source_path"),
                    context=context,
                )
                passive_conflicts = passive_conflicts_by_item.get(item["id"]) or []
                conflict_signature = ":".join(
                    clean_cell_value(record.get("job_id"))
                    for record in passive_conflicts
                    if clean_cell_value(record.get("job_id"))
                ) or "no-passive-conflict"
                recovery_job = insert_yandex_structure_job_in_transaction(
                    conn,
                    idempotency_key=(
                        "manual-binding-confirmation:v1:"
                        f"{dialog_id}:{checklist_key}:{item['id']}:"
                        f"{job['job_id']}:{conflict_signature}"
                    ),
                    dialog_id=dialog_id,
                    checklist_key=checklist_key,
                    item_id=item["id"],
                    action=job["action"],
                    source_path=source_path,
                    target_path=target,
                    folder_alias=clean_cell_value(job.get("folder_alias")),
                    item_name=clean_cell_value(item.get("name")),
                    group_id=int(item.get("group") or 0),
                    initial_status="completed",
                    result={
                        **details,
                        "sourcePath": source_path,
                        "folderPath": target,
                        "manualBindingRecovery": True,
                        "confirmedJobId": clean_cell_value(job.get("job_id")),
                        "supersededConflictJobIds": [
                            clean_cell_value(record.get("job_id"))
                            for record in passive_conflicts
                            if clean_cell_value(record.get("job_id"))
                        ],
                    },
                )
                item.update(
                    build_yandex_structure_item_fields(
                        item=item,
                        job=recovery_job,
                    )
                )
                rebase_item_file_paths_in_transaction(
                    conn,
                    dialog_id=dialog_id,
                    checklist_key=checklist_key,
                    item=item,
                    job=recovery_job,
                )
                result["items"].append({
                    "itemId": item["id"],
                    "jobId": recovery_job["job_id"],
                    "confirmedJobId": job["job_id"],
                    "folderPath": target,
                })
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
