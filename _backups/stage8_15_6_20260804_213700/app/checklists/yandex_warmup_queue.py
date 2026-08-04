import os
import threading
from queue import Queue, Empty
from datetime import datetime

from app.logging_utils import write_debug_log

from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
)

from app.checklists.storage import (
    get_project_storage_context,
    list_project_storage_context_dialog_ids,
)

from app.yandex_disk.client import is_yandex_disk_enabled

from app.checklists.yandex_warmup_control import (
    request_yandex_warmup_stop,
    clear_yandex_warmup_stop,
    is_yandex_warmup_stop_requested,
    get_yandex_warmup_stop_state,
)


def get_yandex_warmup_worker_count() -> int:
    raw_value = str(os.getenv("YANDEX_WARMUP_WORKER_COUNT", "1") or "1").strip()

    try:
        value = int(raw_value)
    except Exception:
        value = 1

    if value < 1:
        return 1

    # На бою не даём случайно поставить 10 воркеров и забить Яндекс.Диск.
    if value > 2:
        return 2

    return value


YANDEX_WARMUP_QUEUE: Queue[str] = Queue()
YANDEX_WARMUP_GUARD = threading.Lock()

YANDEX_WARMUP_QUEUED_DIALOG_IDS: set[str] = set()
YANDEX_WARMUP_RUNNING_DIALOG_IDS: set[str] = set()
YANDEX_WARMUP_WORKERS_STARTED = False

def has_missing_yandex_folders(context: dict) -> bool:
    context = context or {}
    yandex_disk = context.get("yandexDisk") or {}

    project_root_path = clean_cell_value(yandex_disk.get("projectRootPath"))
    project_root_url = clean_cell_value(yandex_disk.get("projectRootUrl"))

    if project_root_path and not project_root_url:
        return True

    folders = yandex_disk.get("folders") or {}
    if not isinstance(folders, dict):
        return False

    for folder in folders.values():
        if not isinstance(folder, dict):
            continue

        folder_path = clean_cell_value(folder.get("path"))
        folder_url = clean_cell_value(folder.get("url") or folder.get("public_url"))

        if folder_path and not folder_url:
            return True

    return False

def should_skip_context_for_queue(dialog_id: str) -> tuple[bool, str]:
    context = get_project_storage_context(dialog_id)
    if not context:
        return True, "project storage context not found"

    storage_mode = context.get("storageMode") or {}
    mirror_targets = storage_mode.get("mirrorTargets") or []

    if "yandex_disk" not in mirror_targets:
        return True, "yandex_disk is not in mirrorTargets"

    if not is_yandex_disk_enabled():
        return True, "yandex disk is disabled"

    yandex_disk = context.get("yandexDisk") or {}
    project_root_path = clean_cell_value(yandex_disk.get("projectRootPath"))

    if not project_root_path:
        return True, "projectRootPath is empty"

    has_missing_folders = has_missing_yandex_folders(context)

    if bool(yandex_disk.get("standardFoldersPrepared")) and not has_missing_folders:
        return True, "standard folders already prepared"

    if not has_missing_folders and bool(yandex_disk.get("standardFoldersPrepared")):
        return True, "no missing yandex folders"

    return False, ""


def enqueue_yandex_warmup(dialog_id: str, source: str = "") -> dict:
    dialog_id = normalize_dialog_id(dialog_id)

    if not dialog_id:
        return {
            "ok": False,
            "queued": False,
            "error": "dialogId is required",
        }

    # Новый запуск по проекту отменяет прошлый stop-флаг.
    clear_yandex_warmup_stop(dialog_id)

    with YANDEX_WARMUP_GUARD:
        if dialog_id in YANDEX_WARMUP_QUEUED_DIALOG_IDS:
            write_debug_log("yandex_warmup_queue_duplicate_skipped", {
                "dialogId": dialog_id,
                "source": source,
                "reason": "already queued",
            })

            return {
                "ok": True,
                "queued": False,
                "alreadyQueued": True,
                "dialogId": dialog_id,
            }

        if dialog_id in YANDEX_WARMUP_RUNNING_DIALOG_IDS:
            write_debug_log("yandex_warmup_queue_duplicate_skipped", {
                "dialogId": dialog_id,
                "source": source,
                "reason": "already running",
            })

            return {
                "ok": True,
                "queued": False,
                "alreadyRunning": True,
                "dialogId": dialog_id,
            }

        skip, reason = should_skip_context_for_queue(dialog_id)
        if skip:
            write_debug_log("yandex_warmup_queue_skipped", {
                "dialogId": dialog_id,
                "source": source,
                "reason": reason,
            })

            return {
                "ok": True,
                "queued": False,
                "skipped": True,
                "reason": reason,
                "dialogId": dialog_id,
            }

        YANDEX_WARMUP_QUEUED_DIALOG_IDS.add(dialog_id)
        YANDEX_WARMUP_QUEUE.put(dialog_id)

    write_debug_log("yandex_warmup_queued", {
        "dialogId": dialog_id,
        "source": source,
        "queueSize": YANDEX_WARMUP_QUEUE.qsize(),
    })

    return {
        "ok": True,
        "queued": True,
        "dialogId": dialog_id,
        "queueSize": YANDEX_WARMUP_QUEUE.qsize(),
    }


def stop_yandex_warmup_for_dialog(dialog_id: str, source: str = "") -> dict:
    dialog_id = normalize_dialog_id(dialog_id)

    if not dialog_id:
        return {
            "ok": False,
            "stopRequested": False,
            "error": "dialogId is required",
        }

    stop_result = request_yandex_warmup_stop(dialog_id)

    with YANDEX_WARMUP_GUARD:
        was_queued = dialog_id in YANDEX_WARMUP_QUEUED_DIALOG_IDS
        was_running = dialog_id in YANDEX_WARMUP_RUNNING_DIALOG_IDS

        # Из Queue физически не удаляем: worker сам пропустит задачу по stop-флагу.
        YANDEX_WARMUP_QUEUED_DIALOG_IDS.discard(dialog_id)

    write_debug_log("yandex_warmup_stop_requested", {
        "dialogId": dialog_id,
        "source": source,
        "wasQueued": was_queued,
        "wasRunning": was_running,
        "stopResult": stop_result,
        "queueSize": YANDEX_WARMUP_QUEUE.qsize(),
    })

    return {
        "ok": True,
        "stopRequested": True,
        "dialogId": dialog_id,
        "wasQueued": was_queued,
        "wasRunning": was_running,
        "queueSize": YANDEX_WARMUP_QUEUE.qsize(),
    }


def yandex_warmup_worker(worker_index: int):
    from app.checklists.yandex_folders import run_project_yandex_folder_warmup

    write_debug_log("yandex_warmup_worker_started", {
        "workerIndex": worker_index,
    })

    while True:
        try:
            dialog_id = YANDEX_WARMUP_QUEUE.get(timeout=3)
        except Empty:
            continue

        dialog_id = normalize_dialog_id(dialog_id)

        if is_yandex_warmup_stop_requested(dialog_id):
            write_debug_log("yandex_warmup_queue_item_cancelled_before_start", {
                "dialogId": dialog_id,
                "workerIndex": worker_index,
                "queueSize": YANDEX_WARMUP_QUEUE.qsize(),
            })

            clear_yandex_warmup_stop(dialog_id)
            YANDEX_WARMUP_QUEUE.task_done()
            continue

        with YANDEX_WARMUP_GUARD:
            YANDEX_WARMUP_QUEUED_DIALOG_IDS.discard(dialog_id)
            YANDEX_WARMUP_RUNNING_DIALOG_IDS.add(dialog_id)

        started_at = datetime.now().isoformat()

        write_debug_log("yandex_warmup_queue_item_started", {
            "dialogId": dialog_id,
            "workerIndex": worker_index,
            "startedAt": started_at,
            "queueSize": YANDEX_WARMUP_QUEUE.qsize(),
        })

        try:
            result = run_project_yandex_folder_warmup(dialog_id)

            write_debug_log("yandex_warmup_queue_item_finished", {
                "dialogId": dialog_id,
                "workerIndex": worker_index,
                "startedAt": started_at,
                "finishedAt": datetime.now().isoformat(),
                "result": result,
                "queueSize": YANDEX_WARMUP_QUEUE.qsize(),
            })

        except Exception as exc:
            write_debug_log("yandex_warmup_queue_item_failed", {
                "dialogId": dialog_id,
                "workerIndex": worker_index,
                "startedAt": started_at,
                "failedAt": datetime.now().isoformat(),
                "error": str(exc),
            })

        finally:
            with YANDEX_WARMUP_GUARD:
                YANDEX_WARMUP_RUNNING_DIALOG_IDS.discard(dialog_id)

            clear_yandex_warmup_stop(dialog_id)
            YANDEX_WARMUP_QUEUE.task_done()


def start_yandex_warmup_workers():
    global YANDEX_WARMUP_WORKERS_STARTED

    with YANDEX_WARMUP_GUARD:
        if YANDEX_WARMUP_WORKERS_STARTED:
            return {
                "ok": True,
                "started": False,
                "alreadyStarted": True,
                "workerCount": get_yandex_warmup_worker_count(),
            }

        YANDEX_WARMUP_WORKERS_STARTED = True

    worker_count = get_yandex_warmup_worker_count()

    for index in range(worker_count):
        thread = threading.Thread(
            target=yandex_warmup_worker,
            args=(index + 1,),
            daemon=True,
            name=f"yandex-warmup-worker-{index + 1}",
        )
        thread.start()

    write_debug_log("yandex_warmup_workers_started", {
        "workerCount": worker_count,
    })

    return {
        "ok": True,
        "started": True,
        "workerCount": worker_count,
    }


def enqueue_all_saved_project_contexts(source: str = "startup") -> dict:
    dialog_ids = list_project_storage_context_dialog_ids()

    queued = 0
    skipped = 0
    results = []

    for dialog_id in dialog_ids:
        result = enqueue_yandex_warmup(dialog_id, source=source)
        results.append(result)

        if result.get("queued"):
            queued += 1
        else:
            skipped += 1

    write_debug_log("yandex_warmup_startup_enqueue_finished", {
        "source": source,
        "total": len(dialog_ids),
        "queued": queued,
        "skipped": skipped,
        "resultsSample": results[:20],
    })

    return {
        "ok": True,
        "source": source,
        "total": len(dialog_ids),
        "queued": queued,
        "skipped": skipped,
    }


def get_yandex_warmup_queue_state() -> dict:
    with YANDEX_WARMUP_GUARD:
        stop_state = get_yandex_warmup_stop_state()

        return {
            "ok": True,
            "workerCount": get_yandex_warmup_worker_count(),
            "workersStarted": YANDEX_WARMUP_WORKERS_STARTED,
            "queueSize": YANDEX_WARMUP_QUEUE.qsize(),
            "queuedDialogIds": sorted(YANDEX_WARMUP_QUEUED_DIALOG_IDS),
            "runningDialogIds": sorted(YANDEX_WARMUP_RUNNING_DIALOG_IDS),
            **stop_state,
        }