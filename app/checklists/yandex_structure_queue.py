from __future__ import annotations

import os
import threading
from queue import Empty, Queue

from app.logging_utils import write_debug_log
from app.yandex_disk.client import is_yandex_disk_enabled

from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)
from app.checklists.yandex_folders import (
    can_create_custom_item_yandex_folder,
    ensure_yandex_folder_for_custom_item,
    rename_yandex_folder_for_item,
    move_yandex_folder_for_item,
)
from app.checklists.yandex_structure_jobs import (
    claim_yandex_structure_job,
    disable_yandex_structure_job,
    fail_yandex_structure_job,
    finish_yandex_structure_job,
    get_yandex_structure_job,
    list_pending_yandex_structure_job_ids,
    requeue_interrupted_yandex_structure_jobs,
)
from app.checklists.yandex_structure_state import (
    persist_item_yandex_structure_state,
)


YANDEX_STRUCTURE_QUEUE: Queue[str] = Queue()
YANDEX_STRUCTURE_GUARD = threading.Lock()
YANDEX_STRUCTURE_QUEUED_JOB_IDS: set[str] = set()
YANDEX_STRUCTURE_RUNNING_JOB_IDS: set[str] = set()
YANDEX_STRUCTURE_WORKERS_STARTED = False


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
        if action not in {
            "create_item_folder",
            "rename_item_folder",
            "move_item_folder",
        }:
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

        if action == "create_item_folder":
            if not can_create_custom_item_yandex_folder(
                dialog_id,
                checklist_key,
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

            result = ensure_yandex_folder_for_custom_item(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                group_id=int(job.get("group_id") or 0),
                item_name=clean_cell_value(job.get("item_name")),
                item_id=item_id,
            )
        elif action == "rename_item_folder":
            result_data = job.get("result") if isinstance(job.get("result"), dict) else {}
            old_name = clean_cell_value(result_data.get("oldName"))
            new_name = clean_cell_value(job.get("item_name"))
            result = rename_yandex_folder_for_item(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                group_id=int(job.get("group_id") or 0),
                item_id=item_id,
                old_name=old_name,
                new_name=new_name,
                source_path=clean_cell_value(job.get("source_path")),
                target_path=clean_cell_value(job.get("target_path")),
                folder_alias=clean_cell_value(job.get("folder_alias")),
            )
        else:
            result_data = job.get("result") if isinstance(job.get("result"), dict) else {}
            result = move_yandex_folder_for_item(
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

        completed = finish_yandex_structure_job(
            job_id,
            result=result,
        )
        persist_item_yandex_structure_state(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            job=completed,
        )

        # File uploads for the same item are durable but deliberately kept out
        # of the in-memory mirror queue until the folder mutation completes.
        # A folder-not-found failure that raced with an older build is also
        # returned to the queue only after this successful structure result.
        recovery_result = {}
        pending_result = {}
        mirror_release_error = ""
        try:
            from app.checklists.yandex_mirror_queue import (
                enqueue_pending_yandex_mirror_jobs_for_item,
                requeue_yandex_folder_resolution_failures,
            )
            recovery_result = requeue_yandex_folder_resolution_failures(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                source="yandex_structure_completed",
            )
            pending_result = enqueue_pending_yandex_mirror_jobs_for_item(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
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
