from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from app.db import get_conn
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


SUPPORTED_STRUCTURE_ACTIONS = frozenset({
    "create_item_folder",
    "rename_item_folder",
    "move_item_folder",
})
TERMINAL_STRUCTURE_STATUSES = frozenset({
    "completed",
    "error",
    "conflict",
    "cancelled",
    "disabled",
})


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def stable_json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def stable_json_loads(value: str, default: Any) -> Any:
    raw = clean_cell_value(value)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def deterministic_structure_job_id(idempotency_key: str) -> str:
    normalized = clean_cell_value(idempotency_key)
    if not normalized:
        raise ValueError("idempotencyKey is required")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:32]


def ensure_yandex_structure_jobs_table() -> None:
    conn = get_conn()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS yandex_structure_jobs (
                job_id TEXT PRIMARY KEY,
                session_id TEXT,
                operation_id TEXT,
                dialog_id TEXT NOT NULL,
                checklist_key TEXT NOT NULL,
                item_id TEXT NOT NULL,
                action TEXT NOT NULL,
                source_path TEXT,
                target_path TEXT,
                folder_alias TEXT,
                item_name TEXT,
                group_id INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'queued',
                attempts INTEGER DEFAULT 0,
                max_attempts INTEGER DEFAULT 5,
                error TEXT,
                result_json TEXT,
                idempotency_key TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT,
                started_at TEXT,
                finished_at TEXT
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_yandex_structure_jobs_idempotency
            ON yandex_structure_jobs(idempotency_key)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_yandex_structure_jobs_status
            ON yandex_structure_jobs(status, created_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_yandex_structure_jobs_item
            ON yandex_structure_jobs(
                dialog_id,
                checklist_key,
                item_id,
                created_at
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_yandex_structure_jobs_session
            ON yandex_structure_jobs(session_id, status, created_at)
        """)
        conn.commit()
    finally:
        conn.close()


def _normalize_job_record(row) -> dict | None:
    if not row:
        return None
    record = dict(row)
    record["result"] = stable_json_loads(
        record.get("result_json") or "",
        {},
    )
    return record


def insert_yandex_structure_job_in_transaction(
    conn,
    *,
    idempotency_key: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    action: str,
    target_path: str,
    source_path: str = "",
    folder_alias: str = "",
    item_name: str = "",
    group_id: int = 0,
    session_id: str = "",
    operation_id: str = "",
    initial_status: str = "queued",
    error: str = "",
    result: dict | None = None,
    now: str = "",
) -> dict:
    normalized_action = clean_cell_value(action).lower()
    if normalized_action not in SUPPORTED_STRUCTURE_ACTIONS:
        raise ValueError("unsupported Yandex structure action")

    normalized_key = clean_cell_value(idempotency_key)
    if not normalized_key:
        raise ValueError("idempotencyKey is required")

    existing = conn.execute(
        """
        SELECT *
        FROM yandex_structure_jobs
        WHERE idempotency_key = ?
        LIMIT 1
        """,
        (normalized_key,),
    ).fetchone()
    if existing:
        return _normalize_job_record(existing) or {}

    normalized_status = clean_cell_value(initial_status).lower() or "queued"
    if normalized_status not in {
        "queued", "running", "completed", "error", "conflict", "cancelled", "disabled"
    }:
        raise ValueError("unsupported Yandex structure job status")

    timestamp = clean_cell_value(now) or utc_now_iso()
    job_id = deterministic_structure_job_id(normalized_key)

    conn.execute(
        """
        INSERT INTO yandex_structure_jobs(
            job_id,
            session_id,
            operation_id,
            dialog_id,
            checklist_key,
            item_id,
            action,
            source_path,
            target_path,
            folder_alias,
            item_name,
            group_id,
            status,
            attempts,
            max_attempts,
            error,
            result_json,
            idempotency_key,
            created_at,
            updated_at,
            started_at,
            finished_at
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            0, 5, ?, ?, ?, ?, ?, '', ''
        )
        """,
        (
            job_id,
            clean_cell_value(session_id),
            clean_cell_value(operation_id),
            normalize_dialog_id(dialog_id),
            normalize_checklist_key(checklist_key),
            clean_cell_value(item_id),
            normalized_action,
            clean_cell_value(source_path),
            clean_cell_value(target_path),
            clean_cell_value(folder_alias),
            clean_cell_value(item_name),
            int(group_id or 0),
            normalized_status,
            clean_cell_value(error),
            stable_json_dumps(result or {}),
            normalized_key,
            timestamp,
            timestamp,
        ),
    )

    row = conn.execute(
        "SELECT * FROM yandex_structure_jobs WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    return _normalize_job_record(row) or {"job_id": job_id}


def create_yandex_structure_job(**kwargs) -> dict:
    ensure_yandex_structure_jobs_table()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        job = insert_yandex_structure_job_in_transaction(
            conn,
            **kwargs,
        )
        conn.commit()
        return job
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_yandex_structure_job(job_id: str) -> dict | None:
    ensure_yandex_structure_jobs_table()
    normalized_job_id = clean_cell_value(job_id)
    if not normalized_job_id:
        return None
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM yandex_structure_jobs WHERE job_id = ?",
            (normalized_job_id,),
        ).fetchone()
        return _normalize_job_record(row)
    finally:
        conn.close()


def get_latest_yandex_structure_job_for_item(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
) -> dict | None:
    ensure_yandex_structure_jobs_table()
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT *
            FROM yandex_structure_jobs
            WHERE dialog_id = ?
              AND checklist_key = ?
              AND item_id = ?
            ORDER BY created_at DESC, rowid DESC
            LIMIT 1
            """,
            (
                normalize_dialog_id(dialog_id),
                normalize_checklist_key(checklist_key),
                clean_cell_value(item_id),
            ),
        ).fetchone()
        return _normalize_job_record(row)
    finally:
        conn.close()


def get_blocking_yandex_structure_job_for_item(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
) -> dict | None:
    """Return an unfinished folder mutation that must precede file upload."""
    ensure_yandex_structure_jobs_table()
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT *
            FROM yandex_structure_jobs
            WHERE dialog_id = ?
              AND checklist_key = ?
              AND item_id = ?
              AND action IN (
                  'create_item_folder',
                  'rename_item_folder',
                  'move_item_folder'
              )
            ORDER BY created_at DESC, rowid DESC
            LIMIT 1
            """,
            (
                normalize_dialog_id(dialog_id),
                normalize_checklist_key(checklist_key),
                clean_cell_value(item_id),
            ),
        ).fetchone()
        record = _normalize_job_record(row)
        if clean_cell_value((record or {}).get("status")) in {
            "queued", "running", "error", "conflict"
        }:
            return record
        return None
    finally:
        conn.close()


def list_latest_yandex_structure_jobs_for_checklist(
    *,
    dialog_id: str,
    checklist_key: str,
) -> dict[str, dict]:
    ensure_yandex_structure_jobs_table()
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM yandex_structure_jobs
            WHERE dialog_id = ?
              AND checklist_key = ?
            ORDER BY item_id ASC, created_at DESC, rowid DESC
            """,
            (
                normalize_dialog_id(dialog_id),
                normalize_checklist_key(checklist_key),
            ),
        ).fetchall()
    finally:
        conn.close()

    result: dict[str, dict] = {}
    for row in rows:
        record = _normalize_job_record(row) or {}
        item_id = clean_cell_value(record.get("item_id"))
        if item_id and item_id not in result:
            result[item_id] = record
    return result


def retarget_pending_create_item_folder_job_in_transaction(
    conn,
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    item_name: str,
    target_path: str,
    folder_alias: str = "",
    group_id: int | None = None,
    now: str = "",
) -> dict | None:
    timestamp = clean_cell_value(now) or utc_now_iso()
    row = conn.execute(
        """
        SELECT *
        FROM yandex_structure_jobs
        WHERE dialog_id = ?
          AND checklist_key = ?
          AND item_id = ?
          AND action = 'create_item_folder'
          AND status IN ('queued', 'error', 'disabled')
        ORDER BY created_at DESC, job_id DESC
        LIMIT 1
        """,
        (
            normalize_dialog_id(dialog_id),
            normalize_checklist_key(checklist_key),
            clean_cell_value(item_id),
        ),
    ).fetchone()
    if not row:
        return None

    normalized_group_id = (
        int(group_id)
        if group_id is not None
        else int(row["group_id"] or 0)
    )
    conn.execute(
        """
        UPDATE yandex_structure_jobs
        SET item_name = ?,
            target_path = ?,
            folder_alias = ?,
            group_id = ?,
            status = CASE
                WHEN status IN ('error', 'disabled') THEN 'queued'
                ELSE status
            END,
            error = '',
            finished_at = '',
            updated_at = ?
        WHERE job_id = ?
        """,
        (
            clean_cell_value(item_name),
            clean_cell_value(target_path),
            clean_cell_value(folder_alias) or clean_cell_value(row["folder_alias"]),
            normalized_group_id,
            timestamp,
            clean_cell_value(row["job_id"]),
        ),
    )
    updated = conn.execute(
        "SELECT * FROM yandex_structure_jobs WHERE job_id = ?",
        (clean_cell_value(row["job_id"]),),
    ).fetchone()
    return _normalize_job_record(updated)


def retarget_pending_create_item_folder_job(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    item_name: str,
    target_path: str,
    folder_alias: str = "",
    group_id: int | None = None,
) -> dict | None:
    """Update a not-yet-running create job to the final item name/path.

    The rename endpoint is introduced later. This helper is intentionally
    limited to pending create jobs so a rename never creates an obsolete
    folder first and moves it afterwards.
    """
    ensure_yandex_structure_jobs_table()
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT *
            FROM yandex_structure_jobs
            WHERE dialog_id = ?
              AND checklist_key = ?
              AND item_id = ?
              AND action = 'create_item_folder'
              AND status IN ('queued', 'error', 'disabled')
            ORDER BY created_at DESC, job_id DESC
            LIMIT 1
            """,
            (
                normalize_dialog_id(dialog_id),
                normalize_checklist_key(checklist_key),
                clean_cell_value(item_id),
            ),
        ).fetchone()
        if not row:
            conn.commit()
            return None

        normalized_group_id = (
            int(group_id)
            if group_id is not None
            else int(row["group_id"] or 0)
        )
        conn.execute(
            """
            UPDATE yandex_structure_jobs
            SET item_name = ?,
                target_path = ?,
                folder_alias = ?,
                group_id = ?,
                updated_at = ?
            WHERE job_id = ?
            """,
            (
                clean_cell_value(item_name),
                clean_cell_value(target_path),
                clean_cell_value(folder_alias) or clean_cell_value(row["folder_alias"]),
                normalized_group_id,
                now,
                clean_cell_value(row["job_id"]),
            ),
        )
        updated = conn.execute(
            "SELECT * FROM yandex_structure_jobs WHERE job_id = ?",
            (clean_cell_value(row["job_id"]),),
        ).fetchone()
        conn.commit()
        return _normalize_job_record(updated)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_pending_yandex_structure_job_ids(limit: int = 500) -> list[str]:
    ensure_yandex_structure_jobs_table()
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT job_id
            FROM yandex_structure_jobs
            WHERE status = 'queued'
            ORDER BY created_at ASC, job_id ASC
            LIMIT ?
            """,
            (max(1, int(limit or 500)),),
        ).fetchall()
        return [clean_cell_value(row["job_id"]) for row in rows]
    finally:
        conn.close()


def list_reserved_yandex_structure_target_paths(
    parent_path: str,
) -> list[str]:
    """Return durable target reservations used before remote folders exist."""
    ensure_yandex_structure_jobs_table()
    normalized_parent = clean_cell_value(parent_path).rstrip("/").casefold()
    if not normalized_parent:
        return []
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT target_path
            FROM yandex_structure_jobs
            WHERE status IN ('queued', 'running', 'error')
              AND COALESCE(target_path, '') <> ''
            """
        ).fetchall()
    finally:
        conn.close()
    result = []
    for row in rows:
        target = clean_cell_value(row["target_path"]).rstrip("/")
        if not target or "/" not in target:
            continue
        if target.rsplit("/", 1)[0].casefold() == normalized_parent:
            result.append(target)
    return result


def list_session_yandex_structure_job_ids(
    session_id: str,
    *,
    status: str = "queued",
) -> list[str]:
    ensure_yandex_structure_jobs_table()
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT job_id
            FROM yandex_structure_jobs
            WHERE session_id = ?
              AND status = ?
            ORDER BY created_at ASC, job_id ASC
            """,
            (clean_cell_value(session_id), clean_cell_value(status)),
        ).fetchall()
        return [clean_cell_value(row["job_id"]) for row in rows]
    finally:
        conn.close()


def claim_yandex_structure_job(job_id: str) -> dict | None:
    ensure_yandex_structure_jobs_table()
    normalized_job_id = clean_cell_value(job_id)
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            """
            UPDATE yandex_structure_jobs
            SET status = 'running',
                attempts = COALESCE(attempts, 0) + 1,
                error = '',
                started_at = ?,
                updated_at = ?
            WHERE job_id = ?
              AND status = 'queued'
            """,
            (now, now, normalized_job_id),
        )
        if int(cur.rowcount or 0) != 1:
            conn.commit()
            return None
        row = conn.execute(
            "SELECT * FROM yandex_structure_jobs WHERE job_id = ?",
            (normalized_job_id,),
        ).fetchone()
        conn.commit()
        return _normalize_job_record(row)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def finish_yandex_structure_job(
    job_id: str,
    *,
    result: dict | None = None,
) -> dict | None:
    ensure_yandex_structure_jobs_table()
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE yandex_structure_jobs
            SET status = 'completed',
                error = '',
                result_json = ?,
                finished_at = ?,
                updated_at = ?
            WHERE job_id = ?
            """,
            (
                stable_json_dumps(result or {}),
                now,
                now,
                clean_cell_value(job_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return get_yandex_structure_job(job_id)


def update_yandex_structure_job_target(
    job_id: str,
    target_path: str,
) -> dict | None:
    """Persist an allocated custom prefix before the remote mutation."""
    ensure_yandex_structure_jobs_table()
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE yandex_structure_jobs
            SET target_path = ?, updated_at = ?
            WHERE job_id = ?
              AND status IN ('queued', 'running')
            """,
            (
                clean_cell_value(target_path),
                now,
                clean_cell_value(job_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return get_yandex_structure_job(job_id)


def fail_yandex_structure_job(job_id: str, error: str) -> dict | None:
    ensure_yandex_structure_jobs_table()
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE yandex_structure_jobs
            SET status = 'error',
                error = ?,
                finished_at = ?,
                updated_at = ?
            WHERE job_id = ?
            """,
            (
                clean_cell_value(error),
                now,
                now,
                clean_cell_value(job_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return get_yandex_structure_job(job_id)


def mark_yandex_structure_job_conflict(
    job_id: str,
    *,
    error: str,
    result: dict,
) -> dict | None:
    ensure_yandex_structure_jobs_table()
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE yandex_structure_jobs
            SET status = 'conflict',
                error = ?,
                result_json = ?,
                finished_at = ?,
                updated_at = ?
            WHERE job_id = ?
            """,
            (
                clean_cell_value(error),
                stable_json_dumps(result or {}),
                now,
                now,
                clean_cell_value(job_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return get_yandex_structure_job(job_id)


def disable_yandex_structure_job(job_id: str, reason: str) -> dict | None:
    ensure_yandex_structure_jobs_table()
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE yandex_structure_jobs
            SET status = 'disabled',
                error = ?,
                finished_at = ?,
                updated_at = ?
            WHERE job_id = ?
            """,
            (
                clean_cell_value(reason),
                now,
                now,
                clean_cell_value(job_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return get_yandex_structure_job(job_id)


def retry_yandex_structure_job(job_id: str) -> dict | None:
    ensure_yandex_structure_jobs_table()
    now = utc_now_iso()
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            UPDATE yandex_structure_jobs
            SET status = 'queued',
                error = '',
                result_json = '',
                started_at = '',
                finished_at = '',
                updated_at = ?
            WHERE job_id = ?
              AND status IN ('error', 'disabled', 'cancelled')
            """,
            (now, clean_cell_value(job_id)),
        )
        conn.commit()
        if int(cur.rowcount or 0) != 1:
            return get_yandex_structure_job(job_id)
    finally:
        conn.close()
    return get_yandex_structure_job(job_id)


def requeue_interrupted_yandex_structure_jobs() -> int:
    ensure_yandex_structure_jobs_table()
    now = utc_now_iso()
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            UPDATE yandex_structure_jobs
            SET status = 'queued',
                error = CASE
                    WHEN COALESCE(error, '') = ''
                    THEN 'worker interrupted; queued for recovery'
                    ELSE error
                END,
                started_at = '',
                finished_at = '',
                updated_at = ?
            WHERE status = 'running'
            """,
            (now,),
        )
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()
