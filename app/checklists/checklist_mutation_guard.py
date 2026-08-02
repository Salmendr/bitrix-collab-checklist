from __future__ import annotations

import asyncio
import threading
import weakref
from contextlib import asynccontextmanager

from app.checklists.utils import normalize_checklist_key, normalize_dialog_id


_registry_lock = threading.Lock()
_loop_locks: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[str, asyncio.Lock],
] = weakref.WeakKeyDictionary()


def get_checklist_mutation_lock(dialog_id: str, checklist_key: str) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    key = f'{normalize_dialog_id(dialog_id)}::{normalize_checklist_key(checklist_key)}'
    with _registry_lock:
        locks = _loop_locks.get(loop)
        if locks is None:
            locks = {}
            _loop_locks[loop] = locks
        lock = locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            locks[key] = lock
        return lock


@asynccontextmanager
async def checklist_mutation_guard(dialog_id: str, checklist_key: str):
    lock = get_checklist_mutation_lock(dialog_id, checklist_key)
    async with lock:
        yield
