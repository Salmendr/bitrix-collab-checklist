from __future__ import annotations

from app.checklists.yandex_upload_preflight import is_manual_recovery, mark_manual_structure_continuation

import os
import threading
from queue import Empty, Queue

from app.logging_utils import write_debug_log
from app.yandex_disk.client import (
    is_yandex_disk_enabled,
    yandex_disk_try_get_resource_meta,
)

from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)
from app.checklists.yandex_folders import (
    build_stable_custom_folder_target_path,
    can_create_custom_item_yandex_folder,
    ensure_yandex_folder_for_custom_item,
    rename_yandex_folder_for_item,
    move_yandex_folder_for_item,
    split_custom_folder_prefix,
    YandexFolderConflictError,
)
from app.checklists.storage import get_checklist
from app.checklists.yandex_resource_locks import (
    yandex_project_resource_guard,
)
from app.checklists.yandex_structure_jobs import (
    claim_yandex_structure_job,
    disable_yandex_structure_job,
    fail_yandex_structure_job,
    finish_yandex_structure_job,
    get_latest_completed_yandex_structure_folder_path,
    get_yandex_structure_job,
    list_pending_yandex_structure_job_ids,
    mark_yandex_structure_job_conflict,
    requeue_interrupted_yandex_structure_jobs,
    update_yandex_structure_job_source,
    update_yandex_structure_job_target,
)
from app.checklists.yandex_structure_state import (
    persist_item_yandex_structure_state,
)


YANDEX_STRUCTURE_QUEUE: Queue[str] = Queue()
YANDEX_STRUCTURE_GUARD = threading.Lock()
YANDEX_STRUCTURE_QUEUED_JOB_IDS: set[str] = set()
YANDEX_STRUCTURE_RUNNING_JOB_IDS: set[str] = set()
YANDEX_STRUCTURE_WORKERS_STARTED = False


def _current_item(job: dict) -> dict:
    data = get_checklist(
        job.get("dialog_id") or "",
        job.get("checklist_key") or "id",
    )
    item_id = clean_cell_value(job.get("item_id"))
    return next(
        (
            dict(item or {})
            for item in (data.get("items") or [])
            if clean_cell_value((item or {}).get("id")) == item_id
        ),
        {},
    )


SOURCE_FOLDER_UNAVAILABLE_ERROR = "Yandex source folder path is unavailable"


def _resolve_actual_source_job(job: dict, item: dict) -> dict:
    """Replace a stale move/rename source with the folder that really exists.

    Called only after the mutation reported that neither the recorded source
    nor the target exists, so a normal move costs no extra request. A move
    can be recorded while the item's create job is still running: the source
    then points to the draft path (".../КР") although the worker has created
    the prefixed folder (".../04_КР"). Errors of Yandex requests (auth,
    network) are not hidden: they propagate and fail the job as they are.
    """
    # move-name-suffix-v1
    job_id = clean_cell_value(job.get("job_id"))
    source_path = clean_cell_value(job.get("source_path"))
    target_path = clean_cell_value(job.get("target_path"))

    candidates = [
        clean_cell_value(item.get("yandexFolderPath")),
        get_latest_completed_yandex_structure_folder_path(
            dialog_id=clean_cell_value(job.get("dialog_id")),
            checklist_key=clean_cell_value(job.get("checklist_key")),
            item_id=clean_cell_value(job.get("item_id")),
            exclude_job_id=job_id,
        ),
    ]
    checked: set[str] = set()
    for candidate in candidates:
        if not candidate or candidate in {source_path, target_path} or candidate in checked:
            continue
        checked.add(candidate)
        if not yandex_disk_try_get_resource_meta(candidate):
            continue
        updated = update_yandex_structure_job_source(job_id, candidate) or {
            **job,
            "source_path": candidate,
        }
        # A rename stays inside its folder: keep the real numeric prefix.
        if (
            clean_cell_value(job.get("action")) == "rename_item_folder"
            and "/" in target_path
            and candidate.rsplit("/", 1)[0] == target_path.rsplit("/", 1)[0]
        ):
            from app.checklists.yandex_folders import (
                _preserve_standard_folder_prefix,
            )
            renamed_target = (
                candidate.rsplit("/", 1)[0]
                + "/"
                + _preserve_standard_folder_prefix(
                    candidate.rsplit("/", 1)[1],
                    clean_cell_value(job.get("item_name")),
                )
            )
            if renamed_target != target_path:
                updated = update_yandex_structure_job_target(
                    job_id,
                    renamed_target,
                ) or {**updated, "target_path": renamed_target}
        write_debug_log("yandex_structure_job_source_resolved", {
            "jobId": job_id,
            "action": clean_cell_value(job.get("action")),
            "itemId": clean_cell_value(job.get("item_id")),
            "staleSourcePath": source_path,
            "sourcePath": candidate,
            "targetPath": clean_cell_value(updated.get("target_path")),
        })
        return updated
    return job


def _prepare_custom_job_target(job: dict, item: dict) -> dict:
    result_data = job.get("result") if isinstance(job.get("result"), dict) else {}
    is_custom = bool(item.get("isCustom", False) or result_data.get("isCustom"))
    if not is_custom:
        return job

    action = clean_cell_value(job.get("action"))
    source_path = clean_cell_value(job.get("source_path"))
    target_path = clean_cell_value(job.get("target_path"))
    if not target_path or "/" not in target_path:
        return job

    target_parent, target_name = target_path.rsplit("/", 1)
    target_prefix, _ = split_custom_folder_prefix(target_name)
    preserve_name = target_name if target_prefix else ""

    if action == "rename_item_folder" and source_path:
        source_parent, source_name = source_path.rsplit("/", 1)
        if source_parent == target_parent and not preserve_name:
            preserve_name = source_name

    # The new parent (e.g. the "Не требуется" section folder) may not exist
    # yet; prefix allocation lists its children.
    from app.checklists.yandex_folders import ensure_yandex_folder_chain
    ensure_yandex_folder_chain(target_parent)

    resolved_target = build_stable_custom_folder_target_path(
        parent_path=target_parent,
        item_name=clean_cell_value(job.get("item_name")),
        preserve_source_name=preserve_name,
    )
    if resolved_target == target_path:
        return job

    updated = update_yandex_structure_job_target(
        clean_cell_value(job.get("job_id")),
        resolved_target,
    )
    return updated or {**job, "target_path": resolved_target}


def _execute_yandex_structure_mutation(job: dict) -> dict:
    action = clean_cell_value(job.get("action"))
    dialog_id = normalize_dialog_id(job.get("dialog_id"))
    checklist_key = normalize_checklist_key(job.get("checklist_key"))
    item_id = clean_cell_value(job.get("item_id"))
    from app.checklists.yandex_structure_jobs import SUBFOLDER_ACTIONS
    if action in SUBFOLDER_ACTIONS:
        from app.checklists.yandex_item_subfolders import execute_subfolder_job
        return execute_subfolder_job(job)
    item = _current_item(job)
    from app.checklists.yandex_subfolders import retarget_subitem_job
    job = retarget_subitem_job(job, item)
    job = _prepare_custom_job_target(job, item)
    try:
        return _run_yandex_structure_mutation(job)
    except RuntimeError as exc:
        if (
            action not in {"rename_item_folder", "move_item_folder"}
            or str(exc) != SOURCE_FOLDER_UNAVAILABLE_ERROR
        ):
            raise
        resolved = _resolve_actual_source_job(job, item)
        if resolved is job:
            raise
        return _run_yandex_structure_mutation(resolved)


def _run_yandex_structure_mutation(job: dict) -> dict:
    action = clean_cell_value(job.get("action"))
    dialog_id = normalize_dialog_id(job.get("dialog_id"))
    checklist_key = normalize_checklist_key(job.get("checklist_key"))
    item_id = clean_cell_value(job.get("item_id"))

    if action == "create_item_folder":
        if not can_create_custom_item_yandex_folder(dialog_id, checklist_key):
            raise PermissionError(
                "custom item Yandex folder is disabled for this project"
            )
        return ensure_yandex_folder_for_custom_item(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            group_id=int(job.get("group_id") or 0),
            item_name=clean_cell_value(job.get("item_name")),
            item_id=item_id,
            target_path=clean_cell_value(job.get("target_path")),
        )

    result_data = job.get("result") if isinstance(job.get("result"), dict) else {}
    if action == "rename_item_folder":
        return rename_yandex_folder_for_item(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            group_id=int(job.get("group_id") or 0),
            item_id=item_id,
            old_name=clean_cell_value(result_data.get("oldName")),
            new_name=clean_cell_value(job.get("item_name")),
            source_path=clean_cell_value(job.get("source_path")),
            target_path=clean_cell_value(job.get("target_path")),
            folder_alias=clean_cell_value(job.get("folder_alias")),
        )

    return move_yandex_folder_for_item(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        source_group_id=int(result_data.get("sourceGroupId") or 0),
        target_group_id=int(job.get("group_id") or 0),
        item_id=item_id,
        item_name=clean_cell_value(job.get("item_name")),
        source_path=clean_cell_value(job.get("source_path")),
        target_path=clean_cell_value(job.get("target_path")),
        folder_alias=clean_cell_value(job.get("folder_alias")),
    )


def get_yandex_structure_worker_count() -> int:
    # Один worker гарантирует последовательность create/rename/move
    # и исключает гонки структурных операций одного пункта.
    raw = clean_cell_value(os.getenv("YANDEX_STRUCTURE_WORKER_COUNT", "1"))
    try:
        value = int(raw or 1)
    except Exception:
        value = 1
    return 1 if value < 1 else min(value, 1)


def process_yandex_structure_job(job_id: str) -> dict:
    job = claim_yandex_structure_job(job_id)
    if not job:
        current = get_yandex_structure_job(job_id)
        return {
            "ok": True,
            "claimed": False,
            "job": current or {},
        }

    action = clean_cell_value(job.get("action"))
    dialog_id = normalize_dialog_id(job.get("dialog_id"))
    checklist_key = normalize_checklist_key(job.get("checklist_key"))
    item_id = clean_cell_value(job.get("item_id"))

    persist_item_yandex_structure_state(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_id=item_id,
        job=job,
    )

    write_debug_log("yandex_structure_job_started", {
        "jobId": job_id,
        "action": action,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "itemId": item_id,
        "targetPath": job.get("target_path") or "",
        "attempts": int(job.get("attempts") or 0),
    })

    try:
        from app.checklists.yandex_structure_jobs import SUPPORTED_STRUCTURE_ACTIONS
        if action not in SUPPORTED_STRUCTURE_ACTIONS:
            raise RuntimeError(
                f"Yandex structure action is not implemented: {action}"
            )

        if not is_yandex_disk_enabled():
            disabled = disable_yandex_structure_job(
                job_id,
                "yandex disk is disabled",
            )
            persist_item_yandex_structure_state(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                job=disabled,
            )
            return {
                "ok": True,
                "disabled": True,
                "job": disabled or {},
            }

        if (
            action == "create_item_folder"
            and not can_create_custom_item_yandex_folder(
                dialog_id,
                checklist_key,
            )
        ):
            disabled = disable_yandex_structure_job(
                job_id,
                "custom item Yandex folder is disabled for this project",
            )
            persist_item_yandex_structure_state(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                job=disabled,
            )
            return {
                "ok": True,
                "disabled": True,
                "job": disabled or {},
            }

        with yandex_project_resource_guard(
            dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            operation=action,
        ):
            result = _execute_yandex_structure_mutation(job)

            # Persist file addresses before marking the durable move completed.
            # A crash here leaves a retriable move; its prefix rebase is idempotent.
            persist_item_yandex_structure_state(
                dialog_id=dialog_id, checklist_key=checklist_key, item_id=item_id,
                job={**job, "status": "completed", "error": "", "result": result},
            )
            completed = finish_yandex_structure_job(job_id, result=result)

            # Subfolders moved together with the parent folder.
            try:
                from app.checklists.yandex_subfolders import (
                    rebase_subitem_folders_after_parent_job,
                )
                rebase_subitem_folders_after_parent_job(job, result)
            except Exception as rebase_exc:
                write_debug_log("yandex_subitem_rebase_failed", {
                    "jobId": job_id,
                    "itemId": item_id,
                    "error": str(rebase_exc),
                })

        # File uploads for the same item are durable but deliberately kept out
        # of the in-memory mirror queue until the folder mutation completes.
        # Previously failed files require explicit manual intent; a normal
        # structural edit must not revive those uploads in the background.
        recovery_result = {}
        pending_result = {}
        mirror_release_error = ""
        try:
            from app.checklists.yandex_mirror_queue import (
                enqueue_pending_yandex_mirror_jobs_for_item,
                requeue_current_yandex_file_failures,
            )
            if (job.get('result') or {}).get('manualFileRecovery'):
                from app.checklists.yandex_mirror_reconciliation import reconcile_yandex_mirror_documents
                recovery_result = reconcile_yandex_mirror_documents(
                    source='manual_structure_completed', dialog_id=dialog_id,
                    checklist_key=checklist_key, item_id=item_id,
                )
            else:
                from app.checklists.yandex_structure_jobs import real_item_id as _real_item_id
                recovery_result = requeue_current_yandex_file_failures(
                    dialog_id=dialog_id, checklist_key=checklist_key, item_id=_real_item_id(item_id),
                    source='yandex_structure_completed',
                )
            from app.checklists.yandex_structure_jobs import real_item_id
            pending_result = enqueue_pending_yandex_mirror_jobs_for_item(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=real_item_id(item_id),
                source="yandex_structure_completed",
            )
        except Exception as release_exc:
            # The folder mutation is already durably completed. A temporary
            # mirror-dispatch failure must not rewrite that successful
            # structure job as failed; startup recovery can dispatch it later.
            mirror_release_error = str(release_exc)
            write_debug_log("yandex_structure_mirror_release_failed", {
                "jobId": job_id,
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "itemId": item_id,
                "error": mirror_release_error,
            })

        write_debug_log("yandex_structure_job_completed", {
            "jobId": job_id,
            "action": action,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "itemId": item_id,
            "folderPath": result.get("folderPath") or "",
            "folderUrlExists": bool(result.get("folderUrl")),
            "folderResolutionRecovery": recovery_result,
            "pendingMirrorJobs": pending_result,
            "mirrorReleaseError": mirror_release_error,
        })
        return {
            "ok": True,
            "completed": True,
            "job": completed or {},
            "result": result,
            "folderResolutionRecovery": recovery_result,
            "pendingMirrorJobs": pending_result,
            "mirrorReleaseError": mirror_release_error,
        }
    except YandexFolderConflictError as exc:
        current_result = job.get("result") if isinstance(job.get("result"), dict) else {}
        conflicted = mark_yandex_structure_job_conflict(
            job_id,
            error=str(exc),
            result={
                **current_result,
                "conflictCandidates": exc.candidates,
                "isCustom": bool(current_result.get("isCustom") or _current_item(job).get("isCustom")),
            },
        )
        persist_item_yandex_structure_state(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            job=conflicted,
        )
        write_debug_log("yandex_structure_job_conflict", {
            "jobId": job_id,
            "action": action,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "itemId": item_id,
            "error": str(exc),
            "candidates": exc.candidates,
        })
        return {
            "ok": False,
            "completed": False,
            "conflict": True,
            "error": str(exc),
            "job": conflicted or {},
        }
    except Exception as exc:
        failed = fail_yandex_structure_job(job_id, str(exc))
        persist_item_yandex_structure_state(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            job=failed,
        )
        write_debug_log("yandex_structure_job_failed", {
            "jobId": job_id,
            "action": action,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "itemId": item_id,
            "error": str(exc),
        })
        return {
            "ok": False,
            "completed": False,
            "error": str(exc),
            "job": failed or {},
        }


def enqueue_yandex_structure_job(job_id: str, source: str = "") -> dict:
    normalized_job_id = clean_cell_value(job_id)
    if not normalized_job_id:
        return {"ok": False, "queued": False, "error": "jobId is required"}

    job = get_yandex_structure_job(normalized_job_id)
    if not job:
        return {"ok": False, "queued": False, "error": "job not found"}
    if clean_cell_value(job.get("status")) != "queued":
        return {
            "ok": True,
            "queued": False,
            "skipped": True,
            "reason": f"job status is {job.get('status')}",
            "jobId": normalized_job_id,
        }

    if is_manual_recovery(source):
        mark_manual_structure_continuation(normalized_job_id)

    with YANDEX_STRUCTURE_GUARD:
        if normalized_job_id in YANDEX_STRUCTURE_QUEUED_JOB_IDS:
            return {
                "ok": True,
                "queued": False,
                "alreadyQueued": True,
                "jobId": normalized_job_id,
            }
        if normalized_job_id in YANDEX_STRUCTURE_RUNNING_JOB_IDS:
            return {
                "ok": True,
                "queued": False,
                "alreadyRunning": True,
                "jobId": normalized_job_id,
            }
        YANDEX_STRUCTURE_QUEUED_JOB_IDS.add(normalized_job_id)
        YANDEX_STRUCTURE_QUEUE.put(normalized_job_id)

    write_debug_log("yandex_structure_job_queued", {
        "jobId": normalized_job_id,
        "source": source,
        "action": job.get("action") or "",
        "dialogId": job.get("dialog_id") or "",
        "checklistKey": job.get("checklist_key") or "",
        "itemId": job.get("item_id") or "",
        "queueSize": YANDEX_STRUCTURE_QUEUE.qsize(),
    })
    return {
        "ok": True,
        "queued": True,
        "jobId": normalized_job_id,
        "queueSize": YANDEX_STRUCTURE_QUEUE.qsize(),
    }


def enqueue_pending_yandex_structure_jobs(source: str = "startup") -> dict:
    job_ids = list_pending_yandex_structure_job_ids(limit=500)
    results = [
        enqueue_yandex_structure_job(job_id, source=source)
        for job_id in job_ids
    ]
    return {
        "ok": True,
        "source": source,
        "found": len(job_ids),
        "queued": sum(1 for result in results if result.get("queued")),
        "results": results,
    }


def recover_yandex_structure_state_on_startup(source: str = "startup") -> dict:
    requeued = requeue_interrupted_yandex_structure_jobs()
    queued = enqueue_pending_yandex_structure_jobs(source=source)
    result = {
        "ok": True,
        "source": source,
        "requeuedInterrupted": requeued,
        "enqueue": queued,
    }
    write_debug_log("yandex_structure_startup_recovery_completed", result)
    return result


def yandex_structure_worker(worker_index: int) -> None:
    write_debug_log("yandex_structure_worker_started", {
        "workerIndex": worker_index,
    })
    while True:
        try:
            job_id = YANDEX_STRUCTURE_QUEUE.get(timeout=1.0)
        except Empty:
            continue

        with YANDEX_STRUCTURE_GUARD:
            YANDEX_STRUCTURE_QUEUED_JOB_IDS.discard(job_id)
            YANDEX_STRUCTURE_RUNNING_JOB_IDS.add(job_id)

        try:
            process_yandex_structure_job(job_id)
        finally:
            with YANDEX_STRUCTURE_GUARD:
                YANDEX_STRUCTURE_RUNNING_JOB_IDS.discard(job_id)
            YANDEX_STRUCTURE_QUEUE.task_done()


def start_yandex_structure_workers() -> dict:
    global YANDEX_STRUCTURE_WORKERS_STARTED
    with YANDEX_STRUCTURE_GUARD:
        if YANDEX_STRUCTURE_WORKERS_STARTED:
            return {
                "ok": True,
                "alreadyStarted": True,
                "workerCount": get_yandex_structure_worker_count(),
            }
        YANDEX_STRUCTURE_WORKERS_STARTED = True

    worker_count = get_yandex_structure_worker_count()
    for worker_index in range(worker_count):
        thread = threading.Thread(
            target=yandex_structure_worker,
            args=(worker_index + 1,),
            name=f"yandex-structure-{worker_index + 1}",
            daemon=True,
        )
        thread.start()

    return {
        "ok": True,
        "started": True,
        "workerCount": worker_count,
    }


def get_yandex_structure_queue_state() -> dict:
    with YANDEX_STRUCTURE_GUARD:
        return {
            "ok": True,
            "workerCount": get_yandex_structure_worker_count(),
            "workersStarted": YANDEX_STRUCTURE_WORKERS_STARTED,
            "queueSize": YANDEX_STRUCTURE_QUEUE.qsize(),
            "queuedJobIds": sorted(YANDEX_STRUCTURE_QUEUED_JOB_IDS),
            "runningJobIds": sorted(YANDEX_STRUCTURE_RUNNING_JOB_IDS),
        }
