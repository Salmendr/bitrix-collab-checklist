from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from app.db import get_conn
from app.checklists.config import get_checklist_config
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_checklist_item_order_table(conn=None) -> None:
    owns_conn = conn is None
    if owns_conn:
        conn = get_conn()

    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS checklist_item_order (
                dialog_id TEXT NOT NULL,
                checklist_key TEXT NOT NULL,
                item_id TEXT NOT NULL,
                group_id INTEGER NOT NULL,
                position INTEGER NOT NULL,
                order_version INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT,
                PRIMARY KEY (
                    dialog_id,
                    checklist_key,
                    item_id
                )
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_checklist_item_order_group
            ON checklist_item_order(
                dialog_id,
                checklist_key,
                group_id,
                position
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_checklist_item_order_version
            ON checklist_item_order(
                dialog_id,
                checklist_key,
                order_version
            )
        """)
        if owns_conn:
            conn.commit()
    finally:
        if owns_conn:
            conn.close()


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return int(default or 0)


def _group_sequence(data: dict, checklist_key: str) -> list[int]:
    config = get_checklist_config(checklist_key)
    configured = [int(group.id) for group in config.groups]
    from_payload = [
        _safe_int((group or {}).get("id"))
        for group in (data.get("groups") or [])
        if _safe_int((group or {}).get("id"))
    ]

    result: list[int] = []
    for group_id in [*from_payload, *configured]:
        if group_id and group_id not in result:
            result.append(group_id)
    return result


def _normalized_item_rows(data: dict, checklist_key: str) -> list[dict]:
    group_ids = _group_sequence(data, checklist_key)
    group_rank = {
        group_id: index
        for index, group_id in enumerate(group_ids)
    }

    indexed_items = []
    for index, raw_item in enumerate(data.get("items") or []):
        item = raw_item if isinstance(raw_item, dict) else {}
        item_id = clean_cell_value(item.get("id"))
        if not item_id:
            continue
        group_id = _safe_int(item.get("group"))
        indexed_items.append((index, item_id, group_id, _safe_int(item.get("order"))))

    indexed_items.sort(
        key=lambda entry: (
            group_rank.get(entry[2], 100000 + entry[2]),
            entry[2],
            entry[3] if entry[3] > 0 else 100000 + entry[0],
            entry[0],
        )
    )

    positions: dict[int, int] = {}
    result: list[dict] = []
    for _, item_id, group_id, _ in indexed_items:
        positions[group_id] = positions.get(group_id, 0) + 1
        result.append({
            "itemId": item_id,
            "groupId": group_id,
            "position": positions[group_id],
        })
    return result


def _load_rows(conn, dialog_id: str, checklist_key: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            item_id,
            group_id,
            position,
            order_version,
            updated_at
        FROM checklist_item_order
        WHERE dialog_id = ?
          AND checklist_key = ?
        ORDER BY group_id ASC, position ASC, item_id ASC
        """,
        (
            normalize_dialog_id(dialog_id),
            normalize_checklist_key(checklist_key),
        ),
    ).fetchall()
    return [dict(row) for row in rows]


def get_checklist_order_version(
    dialog_id: str,
    checklist_key: str,
    *,
    conn=None,
) -> int:
    owns_conn = conn is None
    if owns_conn:
        conn = get_conn()

    try:
        ensure_checklist_item_order_table(conn)
        row = conn.execute(
            """
            SELECT COALESCE(MAX(order_version), 0) AS version
            FROM checklist_item_order
            WHERE dialog_id = ?
              AND checklist_key = ?
            """,
            (
                normalize_dialog_id(dialog_id),
                normalize_checklist_key(checklist_key),
            ),
        ).fetchone()
        return _safe_int((dict(row) if row else {}).get("version"))
    finally:
        if owns_conn:
            conn.close()


def synchronize_checklist_item_order(
    dialog_id: str,
    checklist_key: str,
    data: dict,
    *,
    force_increment: bool = False,
    conn=None,
) -> int:
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(checklist_key)
    desired = _normalized_item_rows(data, normalized_checklist_key)

    owns_conn = conn is None
    if owns_conn:
        conn = get_conn()

    try:
        ensure_checklist_item_order_table(conn)
        existing = _load_rows(
            conn,
            normalized_dialog_id,
            normalized_checklist_key,
        )
        existing_map = {
            clean_cell_value(row.get("item_id")): (
                _safe_int(row.get("group_id")),
                _safe_int(row.get("position")),
            )
            for row in existing
        }
        desired_map = {
            row["itemId"]: (
                int(row["groupId"]),
                int(row["position"]),
            )
            for row in desired
        }
        current_version = max(
            [_safe_int(row.get("order_version")) for row in existing] or [0]
        )
        changed = existing_map != desired_map

        if not existing:
            next_version = 1
        elif force_increment or changed:
            next_version = current_version + 1
        else:
            next_version = current_version or 1

        now = utc_now_iso()
        desired_ids = [row["itemId"] for row in desired]
        if desired_ids:
            placeholders = ",".join("?" for _ in desired_ids)
            conn.execute(
                f"""
                DELETE FROM checklist_item_order
                WHERE dialog_id = ?
                  AND checklist_key = ?
                  AND item_id NOT IN ({placeholders})
                """,
                (
                    normalized_dialog_id,
                    normalized_checklist_key,
                    *desired_ids,
                ),
            )
        else:
            conn.execute(
                """
                DELETE FROM checklist_item_order
                WHERE dialog_id = ?
                  AND checklist_key = ?
                """,
                (
                    normalized_dialog_id,
                    normalized_checklist_key,
                ),
            )

        for row in desired:
            conn.execute(
                """
                INSERT INTO checklist_item_order(
                    dialog_id,
                    checklist_key,
                    item_id,
                    group_id,
                    position,
                    order_version,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(
                    dialog_id,
                    checklist_key,
                    item_id
                )
                DO UPDATE SET
                    group_id = excluded.group_id,
                    position = excluded.position,
                    order_version = excluded.order_version,
                    updated_at = excluded.updated_at
                """,
                (
                    normalized_dialog_id,
                    normalized_checklist_key,
                    row["itemId"],
                    int(row["groupId"]),
                    int(row["position"]),
                    int(next_version),
                    now,
                ),
            )

        if owns_conn:
            conn.commit()
        return int(next_version)
    except Exception:
        if owns_conn:
            conn.rollback()
        raise
    finally:
        if owns_conn:
            conn.close()


def apply_checklist_item_order(
    dialog_id: str,
    checklist_key: str,
    data: dict,
) -> dict:
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(checklist_key)
    result = dict(data or {})
    result["items"] = [
        dict(item or {})
        for item in (result.get("items") or [])
        if isinstance(item, dict)
    ]

    conn = get_conn()
    try:
        ensure_checklist_item_order_table(conn)
        rows = _load_rows(
            conn,
            normalized_dialog_id,
            normalized_checklist_key,
        )
        current_ids = {
            clean_cell_value(item.get("id"))
            for item in result["items"]
            if clean_cell_value(item.get("id"))
        }
        stored_ids = {
            clean_cell_value(row.get("item_id"))
            for row in rows
            if clean_cell_value(row.get("item_id"))
        }

        if not rows or current_ids != stored_ids:
            synchronize_checklist_item_order(
                normalized_dialog_id,
                normalized_checklist_key,
                result,
                conn=conn,
            )
            conn.commit()
            rows = _load_rows(
                conn,
                normalized_dialog_id,
                normalized_checklist_key,
            )

        row_map = {
            clean_cell_value(row.get("item_id")): row
            for row in rows
        }
        group_ids = _group_sequence(result, normalized_checklist_key)
        group_rank = {
            group_id: index
            for index, group_id in enumerate(group_ids)
        }

        for item in result["items"]:
            row = row_map.get(clean_cell_value(item.get("id")))
            if not row:
                continue
            item["group"] = _safe_int(row.get("group_id"))
            item["order"] = _safe_int(row.get("position"))

        result["items"].sort(
            key=lambda item: (
                group_rank.get(
                    _safe_int(item.get("group")),
                    100000 + _safe_int(item.get("group")),
                ),
                _safe_int(item.get("order"), 100000),
                clean_cell_value(item.get("id")),
            )
        )
        result["orderVersion"] = max(
            [_safe_int(row.get("order_version")) for row in rows] or [0]
        )
        return result
    finally:
        conn.close()


def renumber_items_by_group(
    items: Iterable[dict],
    checklist_key: str,
) -> list[dict]:
    config = get_checklist_config(checklist_key)
    valid_group_ids = set(config.group_ids())
    grouped: dict[int, list[dict]] = {
        int(group_id): []
        for group_id in config.group_ids()
    }

    for raw_item in items:
        item = dict(raw_item or {})
        group_id = _safe_int(item.get("group"))
        if group_id not in valid_group_ids:
            group_id = int(config.default_group_id)
            item["group"] = group_id
        grouped.setdefault(group_id, []).append(item)

    result: list[dict] = []
    for group in config.groups:
        group_items = grouped.get(int(group.id), [])
        group_items.sort(
            key=lambda item: (
                _safe_int(item.get("order"), 100000),
                clean_cell_value(item.get("id")),
            )
        )
        for position, item in enumerate(group_items, start=1):
            item["order"] = position
            result.append(item)
    return result
