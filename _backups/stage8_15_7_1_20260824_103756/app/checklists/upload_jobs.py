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
            finished_at TEXT,
            source_session_id TEXT,
            source_operation_id TEXT,
            source_action_key TEXT
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

    upload_job_columns = {
        row["name"]
        for row in cur.execute(
            "PRAGMA table_info(upload_jobs)"
        ).fetchall()
    }

    for column_name in (
        "source_session_id",
        "source_operation_id",
        "source_action_key",
    ):
        if column_name not in upload_job_columns:
            cur.execute(
                "ALTER TABLE upload_jobs "
                f"ADD COLUMN {column_name} TEXT"
            )

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_upload_jobs_source_session
        ON upload_jobs(source_session_id, status, created_at)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_upload_jobs_source_action
        ON upload_jobs(source_action_key)
        WHERE COALESCE(source_action_key, '') <> ''
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


def ensure_yandex_upload_job_for_reconciliation(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str,
    local_path: str,
    file_name: str,
    file_size: int,
    force_requeue_synced: bool = False,
) -> dict:
    """Idempotently restore/create one upload job for a current document."""
    ensure_upload_jobs_table()
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(checklist_key)
    normalized_item_id = clean_cell_value(item_id)
    normalized_document_id = clean_cell_value(document_id)
    now = utc_now()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT *
            FROM upload_jobs
            WHERE job_type = 'upload'
              AND dialog_id = ?
              AND checklist_key = ?
              AND item_id = ?
              AND document_id = ?
            ORDER BY created_at DESC, job_id DESC
            LIMIT 1
            """,
            (
                normalized_dialog_id,
                normalized_checklist_key,
                normalized_item_id,
                normalized_document_id,
            ),
        ).fetchone()

        if row:
            record = dict(row)
            status = clean_cell_value(record.get("status")).lower()
            if force_requeue_synced and status != "running":
                conn.execute(
                    """
                    UPDATE upload_jobs
                    SET local_path = ?,
                        file_name = ?,
                        file_size = ?,
                        yandex_path = '',
                        status = 'queued',
                        stage = 'mirror_queued',
                        progress_percent = 0,
                        uploaded_bytes = 0,
                        total_bytes = ?,
                        error = '',
                        started_at = '',
                        finished_at = '',
                        updated_at = ?
                    WHERE job_id = ?
                    """,
                    (
                        clean_cell_value(local_path),
                        clean_cell_value(file_name),
                        int(file_size or 0),
                        int(file_size or 0),
                        now,
                        record["job_id"],
                    ),
                )
                conn.commit()
                restored = dict(record)
                restored.update({
                    "status": "queued",
                    "stage": "mirror_queued",
                    "local_path": clean_cell_value(local_path),
                    "file_name": clean_cell_value(file_name),
                    "file_size": int(file_size or 0),
                    "yandex_path": "",
                    "error": "",
                    "reconciledAction": "requeued_remote_missing",
                })
                return restored

            if status == "synced":
                conn.commit()
                return {**record, "reconciledAction": "existing_synced"}

            if status == "queued":
                # A legacy queued row can still point at an obsolete local path.
                # Refresh only local metadata; status/attempt history and the
                # idempotent job identity remain unchanged.
                conn.execute(
                    """
                    UPDATE upload_jobs
                    SET local_path = ?,
                        file_name = ?,
                        file_size = ?,
                        total_bytes = ?,
                        updated_at = ?
                    WHERE job_id = ?
                    """,
                    (
                        clean_cell_value(local_path),
                        clean_cell_value(file_name),
                        int(file_size or 0),
                        int(file_size or 0),
                        now,
                        record["job_id"],
                    ),
                )
                conn.commit()
                refreshed = dict(record)
                refreshed.update({
                    "local_path": clean_cell_value(local_path),
                    "file_name": clean_cell_value(file_name),
                    "file_size": int(file_size or 0),
                    "total_bytes": int(file_size or 0),
                    "updated_at": now,
                    "reconciledAction": "existing_queued",
                })
                return refreshed

            if status == "running":
                conn.commit()
                return {**record, "reconciledAction": "existing_running"}

            permanent_stage = clean_cell_value(record.get("stage")).lower()
            error_text = clean_cell_value(record.get("error")).lower()
            retry_markers = (
                "401", "unauthorized", "не авторизован", "timeout",
                "timed out", "connection", "network", "reset by peer",
                "429", "500", "502", "503", "504", "temporarily",
                "yandex folder not found", "yandex folder path is empty",
                "mirror failed", "diskresourcelockederror",
                "resource is locked", "ресурс заблокирован",
            )
            retriable_error = (
                status == "skipped" and permanent_stage == "yandex_disabled"
            ) or (
                status == "error"
                and permanent_stage in {"mirror_failed", "exception", "folder_prepare"}
                and any(marker in error_text for marker in retry_markers)
            )

            if permanent_stage in {
                "local_file_missing",
                "document_removed_before_upload",
                "document_removed_after_upload",
                "cancelled",
            }:
                row = None
            elif status in {"error", "skipped"} and not retriable_error:
                conn.commit()
                return {
                    **record,
                    "reconciledAction": "unrecoverable_" + (status or "unknown"),
                }
            else:
                conn.execute(
                    """
                    UPDATE upload_jobs
                    SET local_path = ?,
                        file_name = ?,
                        file_size = ?,
                        status = 'queued',
                        stage = 'mirror_queued',
                        progress_percent = 0,
                        uploaded_bytes = 0,
                        total_bytes = ?,
                        error = '',
                        started_at = '',
                        finished_at = '',
                        updated_at = ?
                    WHERE job_id = ?
                    """,
                    (
                        clean_cell_value(local_path),
                        clean_cell_value(file_name),
                        int(file_size or 0),
                        int(file_size or 0),
                        now,
                        record["job_id"],
                    ),
                )
                conn.commit()
                restored = dict(record)
                restored.update({
                    "status": "queued",
                    "stage": "mirror_queued",
                    "local_path": clean_cell_value(local_path),
                    "file_name": clean_cell_value(file_name),
                    "file_size": int(file_size or 0),
                    "error": "",
                    "reconciledAction": "requeued_" + (status or "unknown"),
                })
                return restored

        job_id = uuid.uuid4().hex
        action_key = ""
        conn.execute(
            """
            INSERT INTO upload_jobs(
                job_id, job_type, dialog_id, checklist_key, item_id,
                document_id, local_path, file_name, file_size, yandex_path,
                status, stage, progress_percent, uploaded_bytes, total_bytes,
                error, attempts, created_at, updated_at, started_at,
                finished_at, source_action_key
            )
            VALUES (?, 'upload', ?, ?, ?, ?, ?, ?, ?, '',
                    'queued', 'mirror_queued', 0, 0, ?, '', 0, ?, ?, '', '', ?)
            """,
            (
                job_id,
                normalized_dialog_id,
                normalized_checklist_key,
                normalized_item_id,
                normalized_document_id,
                clean_cell_value(local_path),
                clean_cell_value(file_name),
                int(file_size or 0),
                int(file_size or 0),
                now,
                now,
                action_key,
            ),
        )
        conn.commit()
        created = get_upload_job(job_id) or {"job_id": job_id}
        created["reconciledAction"] = "created_legacy_job"
        return created
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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


def list_pending_yandex_job_ids_for_item(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    limit: int = 500,
) -> list[str]:
    ensure_upload_jobs_table()
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT job_id
            FROM upload_jobs
            WHERE status = 'queued'
              AND job_type = 'upload'
              AND dialog_id = ?
              AND checklist_key = ?
              AND item_id = ?
            ORDER BY created_at ASC, job_id ASC
            LIMIT ?
            """,
            (
                normalize_dialog_id(dialog_id),
                normalize_checklist_key(checklist_key),
                clean_cell_value(item_id),
                max(1, int(limit or 500)),
            ),
        ).fetchall()
        return [clean_cell_value(row["job_id"]) for row in rows]
    finally:
        conn.close()


def list_yandex_folder_resolution_error_job_ids(
    *,
    dialog_id: str = "",
    checklist_key: str = "",
    item_id: str = "",
    limit: int = 500,
) -> list[str]:
    """List only upload failures caused by resolving an item folder."""
    ensure_upload_jobs_table()
    clauses = [
        "status = 'error'",
        "job_type = 'upload'",
        "stage = 'mirror_failed'",
        "LOWER(COALESCE(error, '')) IN (?, ?)",
    ]
    params: list[object] = [
        "yandex folder not found",
        "yandex folder path is empty",
    ]
    normalized_dialog_id = (
        normalize_dialog_id(dialog_id)
        if clean_cell_value(dialog_id)
        else ""
    )
    normalized_checklist_key = (
        normalize_checklist_key(checklist_key)
        if clean_cell_value(checklist_key)
        else ""
    )
    normalized_item_id = clean_cell_value(item_id)
    if normalized_dialog_id:
        clauses.append("dialog_id = ?")
        params.append(normalized_dialog_id)
    if normalized_checklist_key:
        clauses.append("checklist_key = ?")
        params.append(normalized_checklist_key)
    if normalized_item_id:
        clauses.append("item_id = ?")
        params.append(normalized_item_id)
    params.append(max(1, int(limit or 500)))

    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT job_id
            FROM upload_jobs
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at ASC, job_id ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        return [clean_cell_value(row["job_id"]) for row in rows]
    finally:
        conn.close()


def list_yandex_upload_error_job_ids(
    *,
    dialog_id: str = "",
    checklist_key: str = "",
    item_id: str = "",
    limit: int = 500,
) -> list[str]:
    """List recoverable current-file Yandex failures, not local data loss."""
    ensure_upload_jobs_table()
    clauses = [
        "status = 'error'",
        "job_type = 'upload'",
        "stage IN ('mirror_failed', 'exception', 'folder_prepare')",
    ]
    params: list[object] = []
    if clean_cell_value(dialog_id):
        clauses.append("dialog_id = ?")
        params.append(normalize_dialog_id(dialog_id))
    if clean_cell_value(checklist_key):
        clauses.append("checklist_key = ?")
        params.append(normalize_checklist_key(checklist_key))
    if clean_cell_value(item_id):
        clauses.append("item_id = ?")
        params.append(clean_cell_value(item_id))
    params.append(max(1, int(limit or 500)))
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT job_id
            FROM upload_jobs
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at ASC, job_id ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        return [clean_cell_value(row["job_id"]) for row in rows]
    finally:
        conn.close()


def retry_failed_yandex_upload_job(job_id: str) -> dict | None:
    """Return a safe remote upload failure to the durable queue."""
    ensure_upload_jobs_table()
    normalized_job_id = clean_cell_value(job_id)
    now = utc_now()
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            UPDATE upload_jobs
            SET status = 'queued',
                stage = 'mirror_queued',
                progress_percent = 0,
                uploaded_bytes = 0,
                error = '',
                started_at = '',
                finished_at = '',
                updated_at = ?
            WHERE job_id = ?
              AND status = 'error'
              AND job_type = 'upload'
              AND stage IN ('mirror_failed', 'exception', 'folder_prepare')
            """,
            (
                now,
                normalized_job_id,
            ),
        )
        conn.commit()
        if int(cur.rowcount or 0) != 1:
            return get_upload_job(normalized_job_id)
    finally:
        conn.close()
    return get_upload_job(normalized_job_id)


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


def requeue_interrupted_yandex_jobs() -> int:
    ensure_upload_jobs_table()

    now = utc_now()

    conn = get_conn()

    cur = conn.execute("""
        UPDATE upload_jobs
        SET status = 'queued',
            stage = 'recovered_after_restart',
            progress_percent = 0,
            uploaded_bytes = 0,
            error = '',
            updated_at = ?,
            started_at = '',
            finished_at = ''
        WHERE status = 'running'
    """, (
        now,
    ))

    conn.commit()

    recovered_count = int(
        cur.rowcount or 0
    )

    conn.close()

    return recovered_count


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


def resolve_document_mirror_status(document: dict | None) -> dict:
    """Resolve stale document mirror fields against the actual job row.

    A frontend snapshot can persist ``queued`` after the worker has already
    finished the job as skipped/error/disabled. Replacement decisions must
    use the job table as the source of truth, not that stale snapshot.
    """
    document = document if isinstance(document, dict) else {}
    stored_status = clean_cell_value(
        document.get("mirrorStatus")
    ).lower()
    stored_error = clean_cell_value(
        document.get("mirrorError")
    )
    job_id = clean_cell_value(
        document.get("mirrorJobId")
    )
    job = get_upload_job(job_id) if job_id else None

    if not job:
        return {
            "status": stored_status,
            "error": stored_error,
            "job": None,
            "reconciled": False,
        }

    job_status = clean_cell_value(job.get("status")).lower()
    job_stage = clean_cell_value(job.get("stage")).lower()
    job_error = clean_cell_value(job.get("error"))

    if job_status in {"queued", "running"}:
        effective_status = job_status
    elif job_status == "synced":
        effective_status = "synced"
    elif job_status == "skipped" and job_stage == "yandex_disabled":
        effective_status = "disabled"
    elif job_status == "skipped":
        effective_status = "disabled"
    elif job_status in {"error", "failed"}:
        effective_status = "error"
    elif job_status in {"cancelled", "canceled"}:
        effective_status = "cancelled"
    else:
        effective_status = stored_status

    effective_error = job_error or stored_error
    if effective_status == "disabled" and not effective_error:
        effective_error = "yandex disk is disabled"

    return {
        "status": effective_status,
        "error": effective_error,
        "job": job,
        "reconciled": (
            effective_status != stored_status
            or effective_error != stored_error
        ),
    }
