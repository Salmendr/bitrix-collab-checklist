"""Files of an edit session the server is still writing.

A file reaches the application only after its whole body has arrived. Then
the server writes it into the folder of the item and records it in the edit
session: from a moment to tens of seconds for a big file. Save, Cancel and
the background closing of a session wait for these writes, so a file that has
fully arrived is saved or rolled back together with its session, and a write
never meets a session that has already ended (the window may be gone already,
its upload aborted, while the server still writes the received file).

The application runs in one process: the counters live in memory.
"""
from __future__ import annotations

import asyncio
import threading
import time
from contextlib import contextmanager

from app.checklists.utils import clean_cell_value
from app.logging_utils import write_debug_log


SESSION_WRITE_WAIT_SECONDS = 600

_lock = threading.Lock()
_writes: dict[str, int] = {}


@contextmanager
def session_write(session_id: str, kind: str = "upload"):
    """Mark a write of a received file of the session for its whole length."""
    key = clean_cell_value(session_id)
    if not key:
        yield
        return
    with _lock:
        _writes[key] = _writes.get(key, 0) + 1
    try:
        yield
    finally:
        with _lock:
            left = _writes.get(key, 0) - 1
            if left > 0:
                _writes[key] = left
            else:
                _writes.pop(key, None)


def session_write_count(session_id: str) -> int:
    key = clean_cell_value(session_id)
    with _lock:
        return _writes.get(key, 0) if key else 0


def has_session_writes(session_id: str) -> bool:
    return session_write_count(session_id) > 0


async def wait_for_session_writes(
    session_id: str,
    reason: str = "",
    timeout: float = SESSION_WRITE_WAIT_SECONDS,
) -> dict:
    """Wait until the server has written the received files of the session.

    It never blocks the event loop: the writes themselves run on it."""
    key = clean_cell_value(session_id)
    count = session_write_count(key)
    if not count:
        return {"waited": False, "seconds": 0, "timedOut": False}

    started = time.monotonic()
    write_debug_log("edit_session_writes_wait_started", {
        "sessionId": key,
        "reason": clean_cell_value(reason),
        "writesInProgress": count,
    })
    timed_out = False
    while session_write_count(key):
        if time.monotonic() - started >= timeout:
            timed_out = True
            break
        await asyncio.sleep(0.1)
    seconds = round(time.monotonic() - started, 2)
    write_debug_log("edit_session_writes_wait_finished", {
        "sessionId": key,
        "reason": clean_cell_value(reason),
        "seconds": seconds,
        "timedOut": timed_out,
        "writesInProgress": session_write_count(key),
    })
    return {"waited": True, "seconds": seconds, "timedOut": timed_out}
