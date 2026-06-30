import os
import threading
from pathlib import Path
from queue import Queue, Empty
from datetime import datetime

from app.logging_utils import write_debug_log

from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.checklists.storage import (
    get_checklist,
    save_checklist,
)

from app.checklists.normalization import (
    normalize_checklist_data,
    derive_indicator_from_status,
)

from app.checklists.documents import (
    migrate_legacy_document_fields,
    normalize_documents_list,
)

from app.checklists.upload_jobs import (
    get_upload_job,
    list_pending_yandex_job_ids,
    claim_upload_job,
    update_upload_job_progress,
    finish_upload_job,
    fail_upload_job,
    is_job_cancelled,
)

from app.checklists.yandex_folders import mirror_document_file_to_yandex
from app.yandex_disk.client import (
    is_yandex_disk_enabled,
    yandex_disk_delete_path,
)


YANDEX_MIRROR_QUEUE: Queue[str] = Queue()
YANDEX_MIRROR_GUARD = threading.Lock()

YANDEX_MIRROR_QUEUED_JOB_IDS: set[str] = set()
YANDEX_MIRROR_RUNNING_JOB_IDS: set[str] = set()
YANDEX_MIRROR_WORKERS_STARTED = False


def get_yandex_mirror_worker_count() -> int:
    raw_value = str(os.getenv("YANDEX_MIRROR_WORKER_COUNT", "1") or "1").strip()

    try:
        value = int(raw_value)
    except Exception:
        value = 1

    if value < 1:
        return 1

    if value > 2:
        return 2

    return value


def find_document_in_checklist(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str,
) -> tuple[dict, dict, dict] | None:
    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    for index, item in enumerate(items):
        if str(item.get("id") or "") != str(item_id or ""):
            continue

        item = migrate_legacy_document_fields(item)
        items[index] = item

        documents = normalize_documents_list(item.get("documents"))
        for doc in documents:
            if str(doc.get("id") or "") == str(document_id or ""):
                return data, item, doc

    return None


def update_document_mirror_fields(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str,
    updates: dict,
) -> bool:
    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    found = False

    for index, item in enumerate(items):
        if str(item.get("id") or "") != str(item_id or ""):
            continue

        item = migrate_legacy_document_fields(item)
        documents = normalize_documents_list(item.get("documents"))

        next_documents = []
        for doc in documents:
            if str(doc.get("id") or "") == str(document_id or ""):
                doc = {
                    **doc,
                    **dict(updates or {}),
                }
                found = True

            next_documents.append(doc)

        item["documents"] = normalize_documents_list(next_documents)

        first_doc = item["documents"][0] if item["documents"] else {}
        item["documentUrl"] = clean_cell_value(first_doc.get("fileUrl"))
        item["documentName"] = clean_cell_value(first_doc.get("name"))

        items[index] = item
        break

    if not found:
        return False

    data["items"] = items
    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)

    return True


def document_still_exists(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str,
) -> bool:
    return find_document_in_checklist(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_id=item_id,
        document_id=document_id,
    ) is not None


def enqueue_yandex_mirror_job(job_id: str, source: str = "") -> dict:
    job_id = clean_cell_value(job_id)

    if not job_id:
        return {
            "ok": False,
            "queued": False,
            "error": "jobId is required",
        }

    job = get_upload_job(job_id)
    if not job:
        return {
            "ok": False,
            "queued": False,
            "error": "job not found",
            "jobId": job_id,
        }

    if job.get("status") != "queued":
        return {
            "ok": True,
            "queued": False,
            "skipped": True,
            "reason": f"job status is {job.get('status')}",
            "jobId": job_id,
        }

    with YANDEX_MIRROR_GUARD:
        if job_id in YANDEX_MIRROR_QUEUED_JOB_IDS:
            return {
                "ok": True,
                "queued": False,
                "alreadyQueued": True,
                "jobId": job_id,
            }

        if job_id in YANDEX_MIRROR_RUNNING_JOB_IDS:
            return {
                "ok": True,
                "queued": False,
                "alreadyRunning": True,
                "jobId": job_id,
            }

        YANDEX_MIRROR_QUEUED_JOB_IDS.add(job_id)
        YANDEX_MIRROR_QUEUE.put(job_id)

    write_debug_log("yandex_mirror_job_queued", {
        "jobId": job_id,
        "source": source,
        "jobType": job.get("job_type"),
        "dialogId": job.get("dialog_id"),
        "checklistKey": job.get("checklist_key"),
        "itemId": job.get("item_id"),
        "documentId": job.get("document_id"),
        "fileName": job.get("file_name"),
        "queueSize": YANDEX_MIRROR_QUEUE.qsize(),
    })

    return {
        "ok": True,
        "queued": True,
        "jobId": job_id,
        "queueSize": YANDEX_MIRROR_QUEUE.qsize(),
    }


def enqueue_pending_yandex_mirror_jobs(source: str = "startup") -> dict:
    job_ids = list_pending_yandex_job_ids(limit=500)

    queued = 0
    skipped = 0
    results = []

    for job_id in job_ids:
        result = enqueue_yandex_mirror_job(job_id, source=source)
        results.append(result)

        if result.get("queued"):
            queued += 1
        else:
            skipped += 1

    write_debug_log("yandex_mirror_startup_enqueue_finished", {
        "source": source,
        "total": len(job_ids),
        "queued": queued,
        "skipped": skipped,
        "resultsSample": results[:20],
    })

    return {
        "ok": True,
        "source": source,
        "total": len(job_ids),
        "queued": queued,
        "skipped": skipped,
    }


def process_upload_job(job: dict):
    job_id = clean_cell_value(job.get("job_id"))
    dialog_id = normalize_dialog_id(job.get("dialog_id"))
    checklist_key = normalize_checklist_key(job.get("checklist_key"))
    item_id = clean_cell_value(job.get("item_id"))
    document_id = clean_cell_value(job.get("document_id"))
    local_path = clean_cell_value(job.get("local_path"))
    file_name = clean_cell_value(job.get("file_name")) or "file.bin"

    if is_job_cancelled(job_id):
        finish_upload_job(job_id, status="cancelled", stage="cancelled")
        return

    if not document_still_exists(dialog_id, checklist_key, item_id, document_id):
        finish_upload_job(job_id, status="cancelled", stage="document_removed_before_upload")
        return

    if not local_path or not Path(local_path).exists():
        error = f"local file not found: {local_path}"
        update_document_mirror_fields(
            dialog_id,
            checklist_key,
            item_id,
            document_id,
            {
                "mirrorStatus": "error",
                "mirrorError": error,
            },
        )
        fail_upload_job(job_id, error, stage="local_file_missing")
        return

    if not is_yandex_disk_enabled():
        update_document_mirror_fields(
            dialog_id,
            checklist_key,
            item_id,
            document_id,
            {
                "mirrorStatus": "disabled",
                "mirrorError": "yandex disk is disabled",
            },
        )
        finish_upload_job(job_id, status="skipped", stage="yandex_disabled")
        return

    found = find_document_in_checklist(dialog_id, checklist_key, item_id, document_id)
    if not found:
        finish_upload_job(job_id, status="cancelled", stage="document_removed_before_upload")
        return

    _, item, _ = found

    update_document_mirror_fields(
        dialog_id,
        checklist_key,
        item_id,
        document_id,
        {
            "mirrorStatus": "running",
            "mirrorError": "",
        },
    )

    update_upload_job_progress(job_id, "folder_prepare", 5)

    def on_upload_progress(uploaded_bytes: int, total_bytes: int):
        if is_job_cancelled(job_id):
            raise RuntimeError("job cancelled")

        if total_bytes:
            percent = int(uploaded_bytes * 90 / total_bytes)
            percent = max(10, min(95, percent))
        else:
            percent = 10

        update_upload_job_progress(
            job_id,
            "yandex_upload",
            percent,
            uploaded_bytes=uploaded_bytes,
            total_bytes=total_bytes,
        )

    result = mirror_document_file_to_yandex(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_name=clean_cell_value(item.get("name")),
        filename=file_name,
        local_path=local_path,
        item_id=item_id,
        item_group=int(item.get("group") or 0),
        is_custom=bool(item.get("isCustom", False)),
        progress_callback=on_upload_progress,
    )

    if not result.get("ok"):
        error = clean_cell_value(result.get("reason")) or "mirror failed"
        update_document_mirror_fields(
            dialog_id,
            checklist_key,
            item_id,
            document_id,
            {
                "mirrorStatus": "error",
                "mirrorError": error,
            },
        )
        fail_upload_job(job_id, error, stage="mirror_failed")
        return

    if not document_still_exists(dialog_id, checklist_key, item_id, document_id):
        yandex_path = clean_cell_value(result.get("filePath"))
        if yandex_path and is_yandex_disk_enabled():
            try:
                yandex_disk_delete_path(yandex_path, permanently=True)
            except Exception as exc:
                write_debug_log("yandex_mirror_orphan_delete_failed", {
                    "jobId": job_id,
                    "dialogId": dialog_id,
                    "checklistKey": checklist_key,
                    "itemId": item_id,
                    "documentId": document_id,
                    "yandexPath": yandex_path,
                    "error": str(exc),
                })

        finish_upload_job(job_id, status="cancelled", stage="document_removed_after_upload")
        return

    update_document_mirror_fields(
        dialog_id,
        checklist_key,
        item_id,
        document_id,
        {
            "mirrorStatus": "synced",
            "mirrorError": "",
            "yandexPath": clean_cell_value(result.get("filePath")),
            "yandexFileUrl": clean_cell_value(result.get("folderUrl")),
            "yandexFolderAlias": clean_cell_value(result.get("folderAlias")),
        },
    )

    finish_upload_job(job_id, status="synced", stage="done")


def process_delete_job(job: dict):
    job_id = clean_cell_value(job.get("job_id"))
    yandex_path = clean_cell_value(job.get("yandex_path"))

    if not yandex_path:
        finish_upload_job(job_id, status="skipped", stage="empty_yandex_path")
        return

    if not is_yandex_disk_enabled():
        finish_upload_job(job_id, status="skipped", stage="yandex_disabled")
        return

    update_upload_job_progress(job_id, "yandex_delete", 50)

    yandex_disk_delete_path(yandex_path, permanently=True)

    finish_upload_job(job_id, status="deleted", stage="done")


def process_yandex_mirror_job(job_id: str):
    job = claim_upload_job(job_id)

    if not job:
        return

    if job.get("status") not in {"running", "queued"}:
        return

    job_type = clean_cell_value(job.get("job_type"))

    if job_type == "upload":
        process_upload_job(job)
        return

    if job_type == "delete":
        process_delete_job(job)
        return

    fail_upload_job(
        job_id,
        f"unknown job type: {job_type}",
        stage="unknown_job_type",
    )


def yandex_mirror_worker(worker_index: int):
    write_debug_log("yandex_mirror_worker_started", {
        "workerIndex": worker_index,
    })

    while True:
        try:
            job_id = YANDEX_MIRROR_QUEUE.get(timeout=3)
        except Empty:
            continue

        job_id = clean_cell_value(job_id)

        with YANDEX_MIRROR_GUARD:
            YANDEX_MIRROR_QUEUED_JOB_IDS.discard(job_id)
            YANDEX_MIRROR_RUNNING_JOB_IDS.add(job_id)

        started_at = datetime.now().isoformat(timespec="seconds")

        write_debug_log("yandex_mirror_job_started", {
            "jobId": job_id,
            "workerIndex": worker_index,
            "startedAt": started_at,
            "queueSize": YANDEX_MIRROR_QUEUE.qsize(),
        })

        try:
            process_yandex_mirror_job(job_id)

            write_debug_log("yandex_mirror_job_finished", {
                "jobId": job_id,
                "workerIndex": worker_index,
                "startedAt": started_at,
                "finishedAt": datetime.now().isoformat(timespec="seconds"),
                "job": get_upload_job(job_id),
                "queueSize": YANDEX_MIRROR_QUEUE.qsize(),
            })

        except Exception as exc:
            fail_upload_job(job_id, str(exc), stage="exception")

            write_debug_log("yandex_mirror_job_failed", {
                "jobId": job_id,
                "workerIndex": worker_index,
                "startedAt": started_at,
                "failedAt": datetime.now().isoformat(timespec="seconds"),
                "error": str(exc),
            })

        finally:
            with YANDEX_MIRROR_GUARD:
                YANDEX_MIRROR_RUNNING_JOB_IDS.discard(job_id)

            YANDEX_MIRROR_QUEUE.task_done()


def start_yandex_mirror_workers():
    global YANDEX_MIRROR_WORKERS_STARTED

    with YANDEX_MIRROR_GUARD:
        if YANDEX_MIRROR_WORKERS_STARTED:
            return {
                "ok": True,
                "started": False,
                "alreadyStarted": True,
                "workerCount": get_yandex_mirror_worker_count(),
            }

        YANDEX_MIRROR_WORKERS_STARTED = True

    worker_count = get_yandex_mirror_worker_count()

    for index in range(worker_count):
        thread = threading.Thread(
            target=yandex_mirror_worker,
            args=(index + 1,),
            daemon=True,
            name=f"yandex-mirror-worker-{index + 1}",
        )
        thread.start()

    write_debug_log("yandex_mirror_workers_started", {
        "workerCount": worker_count,
    })

    return {
        "ok": True,
        "started": True,
        "workerCount": worker_count,
    }


def get_yandex_mirror_queue_state() -> dict:
    with YANDEX_MIRROR_GUARD:
        return {
            "ok": True,
            "workerCount": get_yandex_mirror_worker_count(),
            "workersStarted": YANDEX_MIRROR_WORKERS_STARTED,
            "queueSize": YANDEX_MIRROR_QUEUE.qsize(),
            "queuedJobIds": sorted(YANDEX_MIRROR_QUEUED_JOB_IDS),
            "runningJobIds": sorted(YANDEX_MIRROR_RUNNING_JOB_IDS),
        }