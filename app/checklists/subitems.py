"""Subitems: one level of items nested inside a checklist item.

A subitem is stored as an ordinary item with ``parentItemId``. It therefore
reuses uploads, replacements, archives and Yandex mirroring of items. Its
Yandex folder lives inside the parent's folder.

Invariants enforced by :func:`apply_item_hierarchy` on every normalization:

* only one level: a parent never has a parent itself;
* a subitem is always in its parent's group (moving a parent moves them);
* a subitem sent to "Не требуется" is detached: ``parentItemId`` is cleared
  and ``notRequiredReturnParentId`` remembers where to return it;
* ``order`` of top-level items is 1..n per group, of subitems 1..m per parent;
* a parent with active subitems is "Есть" only when all of them are "Есть".
"""
from __future__ import annotations

import re
from typing import Iterable

from app.checklists.utils import clean_cell_value


NOT_REQUIRED_STATUS = "Не требуется"
SUBITEM_ALIAS_RE = re.compile(r"_sub_[0-9a-f]{8}$")


def parent_id_of(item: dict | None) -> str:
    return clean_cell_value((item or {}).get("parentItemId"))


def is_subitem(item: dict | None) -> bool:
    return bool(parent_id_of(item))


def is_subitem_folder_alias(folder_alias: str) -> bool:
    return bool(SUBITEM_ALIAS_RE.search(clean_cell_value(folder_alias)))


def subitem_mapping_name(folder_alias: str) -> str:
    """Mapping name that can never collide with a real item name."""
    return f"subitem:{clean_cell_value(folder_alias)}"


def build_hierarchy_fields(item: dict | None) -> dict:
    source = item or {}
    return {
        "parentItemId": clean_cell_value(source.get("parentItemId")),
        "notRequiredReturnParentId": clean_cell_value(
            source.get("notRequiredReturnParentId")
        ),
    }


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def children_of(items: Iterable[dict], parent_id: str) -> list[dict]:
    target = clean_cell_value(parent_id)
    if not target:
        return []
    return sorted(
        [
            item for item in items or []
            if isinstance(item, dict) and parent_id_of(item) == target
        ],
        key=lambda item: (
            _safe_int(item.get("order"), 100000),
            clean_cell_value(item.get("id")),
        ),
    )


def apply_item_hierarchy(items: list[dict], not_required_group_id: int) -> list[dict]:
    from app.checklists.normalization import (
        derive_indicator_from_status,
        normalize_status,
    )

    by_id = {
        clean_cell_value(item.get("id")): item
        for item in items
        if clean_cell_value(item.get("id"))
    }

    for item in items:
        item.update(build_hierarchy_fields(item))

    for item in items:
        item_id = clean_cell_value(item.get("id"))
        parent_id = parent_id_of(item)
        if not parent_id:
            continue
        parent = by_id.get(parent_id)
        if (
            not parent
            or parent_id == item_id
            or parent_id_of(parent)
        ):
            item["parentItemId"] = ""
            continue
        own_status = normalize_status(item.get("status"))
        parent_status = normalize_status(parent.get("status"))
        if own_status == NOT_REQUIRED_STATUS and parent_status != NOT_REQUIRED_STATUS:
            item["parentItemId"] = ""
            item["notRequiredReturnParentId"] = parent_id
            item["group"] = int(not_required_group_id or item.get("group") or 0)
            continue
        item["group"] = _safe_int(parent.get("group"))
        item["notRequiredReturnParentId"] = ""

    # A parent that got nested itself loses its children's link above; a
    # subitem cannot have children.
    parent_ids = {parent_id_of(item) for item in items if parent_id_of(item)}
    for item in items:
        if parent_id_of(item) and clean_cell_value(item.get("id")) in parent_ids:
            item["parentItemId"] = ""

    for item in items:
        item_id = clean_cell_value(item.get("id"))
        if parent_id_of(item) or item_id not in parent_ids:
            continue
        if normalize_status(item.get("status")) == NOT_REQUIRED_STATUS:
            continue
        active_children = [
            child for child in children_of(items, item_id)
            if normalize_status(child.get("status")) != NOT_REQUIRED_STATUS
        ]
        if not active_children:
            continue
        all_done = all(
            normalize_status(child.get("status")) == "Есть"
            for child in active_children
        )
        current = normalize_status(item.get("status"))
        if all_done:
            derived = "Есть"
        elif current == "Есть":
            derived = "Нет"
        else:
            derived = current
        if derived != current:
            item["status"] = derived
            item["priority"] = derive_indicator_from_status(derived)

    return sort_items_hierarchically(items)


def sort_items_hierarchically(items: list[dict], group_rank: dict | None = None) -> list[dict]:
    """Renumber and order: each parent is followed by its subitems."""
    rank = group_rank or {}
    top_level = [item for item in items if not parent_id_of(item)]
    top_level.sort(
        key=lambda item: (
            rank.get(_safe_int(item.get("group")), _safe_int(item.get("group"))),
            _safe_int(item.get("order"), 100000),
            clean_cell_value(item.get("id")),
        )
    )
    counters: dict[int, int] = {}
    result: list[dict] = []
    placed: set[int] = set()
    for item in top_level:
        group_id = _safe_int(item.get("group"))
        counters[group_id] = counters.get(group_id, 0) + 1
        item["order"] = counters[group_id]
        result.append(item)
        placed.add(id(item))
        for position, child in enumerate(
            children_of(items, clean_cell_value(item.get("id"))),
            start=1,
        ):
            child["order"] = position
            result.append(child)
            placed.add(id(child))
    # Defensive: nothing may be dropped.
    result.extend(item for item in items if id(item) not in placed)
    return result


def progress_items(items: Iterable[dict]) -> list[dict]:
    """Items that take part in progress: subitems of a "Не требуется"
    parent are hidden with it and are not counted."""
    from app.checklists.normalization import normalize_status

    source = [item for item in items or [] if isinstance(item, dict)]
    not_required_ids = {
        clean_cell_value(item.get("id"))
        for item in source
        if normalize_status(item.get("status")) == NOT_REQUIRED_STATUS
    }
    return [
        item for item in source
        if parent_id_of(item) not in not_required_ids or not parent_id_of(item)
    ]
