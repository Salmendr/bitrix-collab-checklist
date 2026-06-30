import uuid
from datetime import datetime

from app.db import get_conn
from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
    normalize_checklist_key,
)


def utc_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def row_to_dict(row) -> dict | None:
    return dict(row) if row else None


def ensure_upload_jobs_table():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS upload_jobs (
            job_id TEXT PRIMARY KEY,
            job_type TEXT,
            dialog_id TEXT,
            checklist_key TEXT,
            item_id TEXT,
            document_id TEXT,
            local_path TEXT,
            file_name TEXT,
            file_size INTEGER DEFAULT 0,
            yandex_path TEXT,
            status TEXT,
            stage TEXT,
            progress_percent INTEGER DEFAULT 0,
            uploaded_bytes INTEGER DEFAULT 0,
            total_bytes INTEGER DEFAULT 0,
            error TEXT,
            attempts INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            started_at TEXT,
            finished_at TEXT
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_upload_jobs_status
        ON upload_jobs(status, created_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_upload_jobs_document
        ON upload_jobs(dialog_id, checklist_key, item_id, document_id)
    """)

    conn.commit()
    conn.close()


def create_yandex_upload_job(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str,
    local_path: str,
    file_name: str,
    file_size: int,
) -> dict:
    ensure_upload_jobs_table()

    job_id = uuid.uuid4().hex
    now = utc_now()

    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)

    conn = get_conn()
    conn.execute("""
        INSERT INTO upload_jobs(
            job_id,
            job_type,
            dialog_id,
            checklist_key,
            item_id,
            document_id,
            local_path,
            file_name,
            file_size,
            yandex_path,
            status,
            stage,
            progress_percent,
            uploaded_bytes,
            total_bytes,
            error,
            attempts,
            created_at,
            updated_at,
            started_at,
            finished_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job_id,
        "upload",
        dialog_id,
        checklist_key,
        str(item_id or "").strip(),
        clean_cell_value(document_id),
        clean_cell_value(local_path),
        clean_cell_value(file_name),
        int(file_size or 0),
        "",
        "queued",
        "mirror_queued",
        0,
        0,
        int(file_size or 0),
        "",
        0,
        now,
        now,
        "",
        "",
    ))
    conn.commit()
    conn.close()

    return get_upload_job(job_id) or {"jobId": job_id}


def create_yandex_delete_job(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str,
    file_name: str,
    yandex_path: str,
) -> dict:
    ensure_upload_jobs_table()

    yandex_path = clean_cell_value(yandex_path)
    if not yandex_path:
        return {
            "ok": False,
            "created": False,
            "reason": "yandexPath is empty",
        }

    job_id = uuid.uuid4().hex
    now = utc_now()

    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)

    conn = get_conn()
    conn.execute("""
        INSERT INTO upload_jobs(
            job_id,
            job_type,
            dialog_id,
            checklist_key,
            item_id,
            document_id,
            local_path,
            file_name,
            file_size,
            yandex_path,
            status,
            stage,
            progress_percent,
            uploaded_bytes,
            total_bytes,
            error,
            attempts,
            created_at,
            updated_at,
            started_at,
            finished_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job_id,
        "delete",
        dialog_id,
        checklist_key,
        str(item_id or "").strip(),
        clean_cell_value(document_id),
        "",
        clean_cell_value(file_name),
        0,
        yandex_path,
        "queued",
        "delete_queued",
        0,
        0,
        0,
        "",
        0,
        now,
        now,
        "",
        "",
    ))
    conn.commit()
    conn.close()

    return get_upload_job(job_id) or {"jobId": job_id}


def get_upload_job(job_id: str) -> dict | None:
    ensure_upload_jobs_table()

    job_id = clean_cell_value(job_id)
    if not job_id:
        return None

    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM upload_jobs WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    conn.close()

    return row_to_dict(row)


def get_latest_document_job(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str,
) -> dict | None:
    ensure_upload_jobs_table()

    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)

    conn = get_conn()
    row = conn.execute("""
        SELECT *
        FROM upload_jobs
        WHERE dialog_id = ?
          AND checklist_key = ?
          AND item_id = ?
          AND document_id = ?
        ORDER BY created_at DESC
        LIMIT 1
    """, (
        dialog_id,
        checklist_key,
        str(item_id or "").strip(),
        clean_cell_value(document_id),
    )).fetchone()
    conn.close()

    return row_to_dict(row)


def list_pending_yandex_job_ids(limit: int = 100) -> list[str]:
    ensure_upload_jobs_table()

    conn = get_conn()
    rows = conn.execute("""
        SELECT job_id
        FROM upload_jobs
        WHERE status = 'queued'
        ORDER BY created_at ASC
        LIMIT ?
    """, (int(limit or 100),)).fetchall()
    conn.close()

    return [clean_cell_value(row["job_id"]) for row in rows]


def list_recent_jobs(limit: int = 20) -> list[dict]:
    ensure_upload_jobs_table()

    conn = get_conn()
    rows = conn.execute("""
        SELECT *
        FROM upload_jobs
        ORDER BY created_at DESC
        LIMIT ?
    """, (int(limit or 20),)).fetchall()
    conn.close()

    return [dict(row) for row in rows]


def claim_upload_job(job_id: str) -> dict | None:
    ensure_upload_jobs_table()

    job_id = clean_cell_value(job_id)
    now = utc_now()

    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM upload_jobs WHERE job_id = ?",
        (job_id,),
    ).fetchone()

    if not row:
        conn.close()
        return None

    current = dict(row)
    if current.get("status") != "queued":
        conn.close()
        return current

    conn.execute("""
        UPDATE upload_jobs
        SET status = 'running',
            stage = CASE
                WHEN job_type = 'delete' THEN 'yandex_delete'
                ELSE 'folder_prepare'
            END,
            progress_percent = CASE
                WHEN job_type = 'delete' THEN 10
                ELSE 5
            END,
            attempts = attempts + 1,
            started_at = CASE WHEN started_at = '' THEN ? ELSE started_at END,
            updated_at = ?
        WHERE job_id = ?
    """, (now, now, job_id))
    conn.commit()

    row = conn.execute(
        "SELECT * FROM upload_jobs WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    conn.close()

    return row_to_dict(row)


def update_upload_job_progress(
    job_id: str,
    stage: str,
    progress_percent: int,
    uploaded_bytes: int = 0,
    total_bytes: int = 0,
):
    ensure_upload_jobs_table()

    job_id = clean_cell_value(job_id)
    if not job_id:
        return

    conn = get_conn()
    conn.execute("""
        UPDATE upload_jobs
        SET stage = ?,
            progress_percent = ?,
            uploaded_bytes = CASE WHEN ? > 0 THEN ? ELSE uploaded_bytes END,
            total_bytes = CASE WHEN ? > 0 THEN ? ELSE total_bytes END,
            updated_at = ?
        WHERE job_id = ?
          AND status IN ('queued', 'running')
    """, (
        clean_cell_value(stage),
        max(0, min(100, int(progress_percent or 0))),
        int(uploaded_bytes or 0),
        int(uploaded_bytes or 0),
        int(total_bytes or 0),
        int(total_bytes or 0),
        utc_now(),
        job_id,
    ))
    conn.commit()
    conn.close()


def finish_upload_job(job_id: str, status: str = "synced", stage: str = "done"):
    ensure_upload_jobs_table()

    job_id = clean_cell_value(job_id)
    now = utc_now()

    conn = get_conn()
    conn.execute("""
        UPDATE upload_jobs
        SET status = ?,
            stage = ?,
            progress_percent = 100,
            error = '',
            updated_at = ?,
            finished_at = ?
        WHERE job_id = ?
    """, (
        clean_cell_value(status) or "synced",
        clean_cell_value(stage) or "done",
        now,
        now,
        job_id,
    ))
    conn.commit()
    conn.close()


def fail_upload_job(job_id: str, error: str, stage: str = "error"):
    ensure_upload_jobs_table()

    job_id = clean_cell_value(job_id)
    now = utc_now()

    conn = get_conn()
    conn.execute("""
        UPDATE upload_jobs
        SET status = 'error',
            stage = ?,
            error = ?,
            updated_at = ?,
            finished_at = ?
        WHERE job_id = ?
    """, (
        clean_cell_value(stage) or "error",
        clean_cell_value(error) or "unknown error",
        now,
        now,
        job_id,
    ))
    conn.commit()
    conn.close()


def cancel_upload_jobs_for_document(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str,
) -> int:
    ensure_upload_jobs_table()

    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)
    now = utc_now()

    conn = get_conn()
    cur = conn.execute("""
        UPDATE upload_jobs
        SET status = 'cancelled',
            stage = 'cancelled',
            error = 'document removed',
            updated_at = ?,
            finished_at = ?
        WHERE dialog_id = ?
          AND checklist_key = ?
          AND item_id = ?
          AND document_id = ?
          AND job_type = 'upload'
          AND status IN ('queued', 'running')
    """, (
        now,
        now,
        dialog_id,
        checklist_key,
        str(item_id or "").strip(),
        clean_cell_value(document_id),
    ))
    conn.commit()
    count = cur.rowcount or 0
    conn.close()

    return count


def is_job_cancelled(job_id: str) -> bool:
    job = get_upload_job(job_id)
    return bool(job and job.get("status") == "cancelled")


def public_job_payload(job: dict | None) -> dict:
    if not job:
        return {
            "ok": False,
            "error": "job not found",
        }

    return {
        "ok": True,
        "jobId": job.get("job_id"),
        "jobType": job.get("job_type"),
        "dialogId": job.get("dialog_id"),
        "checklistKey": job.get("checklist_key"),
        "itemId": job.get("item_id"),
        "documentId": job.get("document_id"),
        "fileName": job.get("file_name"),
        "fileSize": int(job.get("file_size") or 0),
        "yandexPath": job.get("yandex_path"),
        "status": job.get("status"),
        "stage": job.get("stage"),
        "progressPercent": int(job.get("progress_percent") or 0),
        "uploadedBytes": int(job.get("uploaded_bytes") or 0),
        "totalBytes": int(job.get("total_bytes") or 0),
        "error": job.get("error") or "",
        "createdAt": job.get("created_at") or "",
        "updatedAt": job.get("updated_at") or "",
        "startedAt": job.get("started_at") or "",
        "finishedAt": job.get("finished_at") or "",
    }