from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, TypeVar

from app.logging_utils import write_debug_log
from app.checklists.utils import clean_cell_value, normalize_dialog_id


T = TypeVar("T")

YANDEX_PROJECT_LOCKS_GUARD = threading.Lock()
YANDEX_PROJECT_LOCKS: dict[str, threading.RLock] = {}
# Who holds the lock of a project now: a long wait names it in the log.
YANDEX_PROJECT_LOCK_HOLDERS: dict[str, dict] = {}
YANDEX_PROJECT_LOCK_WAIT_LOG_SECONDS = 60.0
YANDEX_RESOURCE_RETRY_DELAYS_SECONDS = (1.0, 2.0, 4.0, 8.0)


def _project_lock_key(dialog_id: str) -> str:
    return normalize_dialog_id(dialog_id) or clean_cell_value(dialog_id) or "global"


def _project_lock(dialog_id: str) -> threading.RLock:
    key = _project_lock_key(dialog_id)
    with YANDEX_PROJECT_LOCKS_GUARD:
        return YANDEX_PROJECT_LOCKS.setdefault(key, threading.RLock())


def _error_fragments(value: Any) -> list[str]:
    fragments = [clean_cell_value(str(value or ""))]
    response = getattr(value, "response", None)
    if response is not None:
        try:
            fragments.append(clean_cell_value(str(response.json() or "")))
        except Exception:
            fragments.append(clean_cell_value(getattr(response, "text", "")))
    return [fragment.lower() for fragment in fragments if fragment]


def is_yandex_resource_locked_error(value: Any) -> bool:
    text = " ".join(_error_fragments(value))
    return any(marker in text for marker in (
        "diskresourcelockederror",
        "resource is locked",
        "ресурс заблокирован",
        "resource_locked",
    ))


def run_with_yandex_resource_retry(
    operation: Callable[[], T],
    *,
    operation_name: str,
    metadata: dict | None = None,
    delays: tuple[float, ...] = YANDEX_RESOURCE_RETRY_DELAYS_SECONDS,
) -> T:
    """Retry only a failed Yandex primitive when the resource is locked."""
    last_error: Exception | None = None
    attempts = len(delays) + 1
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            last_error = exc
            retryable = is_yandex_resource_locked_error(exc)
            if not retryable or attempt >= attempts:
                if retryable:
                    write_debug_log("yandex_resource_retry_exhausted", {
                        **(metadata or {}),
                        "operation": clean_cell_value(operation_name),
                        "attempt": attempt,
                        "maxAttempts": attempts,
                        "error": str(exc),
                    })
                raise

            delay = max(0.0, float(delays[attempt - 1]))
            write_debug_log("yandex_resource_retry_scheduled", {
                **(metadata or {}),
                "operation": clean_cell_value(operation_name),
                "attempt": attempt,
                "maxAttempts": attempts,
                "delaySeconds": delay,
                "error": str(exc),
            })
            time.sleep(delay)

    # The loop always returns or raises.  Keep the last exception intact for
    # static analyzers and for safety if delays is replaced with bad input.
    if last_error:
        raise last_error
    raise RuntimeError("Yandex operation failed without an error")


@contextmanager
def yandex_project_resource_guard(
    dialog_id: str,
    *,
    checklist_key: str = "",
    item_id: str = "",
    operation: str = "",
):
    """Serialize all mutating Yandex work inside one project.

    Structural, mirror and warmup queues are independent.  A project-scoped
    lock prevents them from mutating the same root/parent at the same time,
    while unrelated projects still run concurrently.
    """
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    lock = _project_lock(normalized_dialog_id)
    key = _project_lock_key(normalized_dialog_id)
    started = time.monotonic()
    while not lock.acquire(timeout=YANDEX_PROJECT_LOCK_WAIT_LOG_SECONDS):
        with YANDEX_PROJECT_LOCKS_GUARD:
            holder = dict(YANDEX_PROJECT_LOCK_HOLDERS.get(key) or {})
        write_debug_log("yandex_project_resource_lock_waiting", {
            "dialogId": normalized_dialog_id,
            "checklistKey": clean_cell_value(checklist_key),
            "itemId": clean_cell_value(item_id),
            "operation": clean_cell_value(operation),
            "waitedSeconds": round(time.monotonic() - started),
            "heldByOperation": holder.get("operation", ""),
            "heldByItemId": holder.get("itemId", ""),
            "heldByThread": holder.get("thread", ""),
            "heldForSeconds": round(time.monotonic() - holder["since"]) if holder else None,
        })
    with YANDEX_PROJECT_LOCKS_GUARD:
        holder = YANDEX_PROJECT_LOCK_HOLDERS.get(key)
        if holder and holder.get("threadId") == threading.get_ident():
            holder["depth"] += 1
        else:
            YANDEX_PROJECT_LOCK_HOLDERS[key] = {
                "operation": clean_cell_value(operation),
                "itemId": clean_cell_value(item_id),
                "thread": threading.current_thread().name,
                "threadId": threading.get_ident(),
                "since": time.monotonic(),
                "depth": 1,
            }
    from app.checklists.yandex_scope import active_project
    scope_token = active_project.set(normalized_dialog_id)
    waited = time.monotonic() - started
    try:
        if waited >= 0.1:
            write_debug_log("yandex_project_resource_lock_acquired", {
                "dialogId": normalized_dialog_id,
                "checklistKey": clean_cell_value(checklist_key),
                "itemId": clean_cell_value(item_id),
                "operation": clean_cell_value(operation),
                "waitedSeconds": round(waited, 3),
            })
        yield
    finally:
        active_project.reset(scope_token)
        with YANDEX_PROJECT_LOCKS_GUARD:
            holder = YANDEX_PROJECT_LOCK_HOLDERS.get(key)
            if holder and holder.get("threadId") == threading.get_ident():
                holder["depth"] -= 1
                if holder["depth"] <= 0:
                    YANDEX_PROJECT_LOCK_HOLDERS.pop(key, None)
        lock.release()
