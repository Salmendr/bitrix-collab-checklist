from __future__ import annotations

import asyncio
import threading
import weakref
from contextlib import asynccontextmanager

from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


_registry_lock = threading.Lock()
_loop_locks: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[str, asyncio.Lock],
] = weakref.WeakKeyDictionary()


def build_item_mutation_key(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
) -> str:
    return "::".join(
        (
            normalize_dialog_id(dialog_id),
            normalize_checklist_key(checklist_key),
            clean_cell_value(item_id),
        )
    )


def get_item_mutation_lock(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    key = build_item_mutation_key(
        dialog_id,
        checklist_key,
        item_id,
    )

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
async def item_mutation_guard(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
):
    """
    Serialize read-modify-write mutations for one checklist item.

    The application stores the whole checklist as one JSON document. Without
    this guard, concurrent uploads can read the same document list and then
    overwrite one another even when every physical file was saved correctly.
    """

    lock = get_item_mutation_lock(
        dialog_id,
        checklist_key,
        item_id,
    )

    async with lock:
        yield
