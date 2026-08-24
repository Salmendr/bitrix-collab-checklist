from datetime import datetime, timezone

from app.db import get_conn

from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
    normalize_checklist_key,
)


TERMINAL_REPLACEMENT_STATUSES = {
    "completed",
    "cancelled",
    "error",
}

ALLOWED_REPLACEMENT_UPDATE_FIELDS = {
    "new_yandex_path",
    "delete_job_id",
    "status",
    "stage",
    "error",
    "finished_at",
}


def utc_now_iso() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat(timespec="seconds")


def row_to_dict(row) -> dict | None:
    return dict(row) if row else None


def ensure_document_replacements_table():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS document_replacements (
            operation_id TEXT PRIMARY KEY,
            dialog_id TEXT,
            checklist_key TEXT,
            item_id TEXT,
            series_id TEXT,
            archive_version_id TEXT,
            old_document_id TEXT,
            new_document_id TEXT,
            new_upload_job_id TEXT,
            old_file_name TEXT,
            new_file_name TEXT,
            old_yandex_path TEXT,
            new_yandex_path TEXT,
            delete_job_id TEXT,
            status TEXT,
            stage TEXT,
            error TEXT,
            created_at TEXT,
            updated_at TEXT,
            finished_at TEXT
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_document_replacements_status
        ON document_replacements(status, created_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_document_replacements_upload_job
        ON document_replacements(new_upload_job_id)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_document_replacements_delete_job
        ON document_replacements(delete_job_id)
    """)

    conn.commit()
    conn.close()


def create_document_replacement(
    operation_id: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    series_id: str,
    archive_version_id: str,
    old_document_id: str,
    new_document_id: str,
    new_upload_job_id: str,
    old_file_name: str,
    new_file_name: str,
    old_yandex_path: str,
) -> dict:
    ensure_document_replacements_table()

    operation_id = clean_cell_value(operation_id)
    new_upload_job_id = clean_cell_value(
        new_upload_job_id
    )

    if not operation_id:
        raise ValueError(
            "operationId is required"
        )

    if not new_upload_job_id:
        raise ValueError(
            "newUploadJobId is required"
        )

    now = utc_now_iso()

    conn = get_conn()

    conn.execute("""
        INSERT INTO document_replacements(
            operation_id,
            dialog_id,
            checklist_key,
            item_id,
            series_id,
            archive_version_id,
            old_document_id,
            new_document_id,
            new_upload_job_id,
            old_file_name,
            new_file_name,
            old_yandex_path,
            new_yandex_path,
            delete_job_id,
            status,
            stage,
            error,
            created_at,
            updated_at,
            finished_at
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            '', '', ?, ?, '', ?, ?, ''
        )
    """, (
        operation_id,
        normalize_dialog_id(dialog_id),
        normalize_checklist_key(checklist_key),
        clean_cell_value(item_id),
        clean_cell_value(series_id),
        clean_cell_value(archive_version_id),
        clean_cell_value(old_document_id),
        clean_cell_value(new_document_id),
        new_upload_job_id,
        clean_cell_value(old_file_name),
        clean_cell_value(new_file_name),
        clean_cell_value(old_yandex_path),
        "pending",
        "awaiting_new_upload",
        now,
        now,
    ))

    conn.commit()
    conn.close()

    return (
        get_document_replacement(operation_id)
        or {
            "operation_id": operation_id,
        }
    )


def get_document_replacement(
    operation_id: str,
) -> dict | None:
    ensure_document_replacements_table()

    operation_id = clean_cell_value(operation_id)

    if not operation_id:
        return None

    conn = get_conn()

    row = conn.execute(
        """
        SELECT *
        FROM document_replacements
        WHERE operation_id = ?
        """,
        (operation_id,),
    ).fetchone()

    conn.close()

    return row_to_dict(row)


def get_document_replacement_by_upload_job(
    upload_job_id: str,
) -> dict | None:
    ensure_document_replacements_table()

    upload_job_id = clean_cell_value(
        upload_job_id
    )

    if not upload_job_id:
        return None

    conn = get_conn()

    row = conn.execute("""
        SELECT *
        FROM document_replacements
        WHERE new_upload_job_id = ?
        ORDER BY created_at DESC
        LIMIT 1
    """, (
        upload_job_id,
    )).fetchone()

    conn.close()

    return row_to_dict(row)


def get_document_replacement_by_delete_job(
    delete_job_id: str,
) -> dict | None:
    ensure_document_replacements_table()

    delete_job_id = clean_cell_value(
        delete_job_id
    )

    if not delete_job_id:
        return None

    conn = get_conn()

    row = conn.execute("""
        SELECT *
        FROM document_replacements
        WHERE delete_job_id = ?
        ORDER BY created_at DESC
        LIMIT 1
    """, (
        delete_job_id,
    )).fetchone()

    conn.close()

    return row_to_dict(row)


def list_pending_document_replacements(
    limit: int = 500,
) -> list[dict]:
    ensure_document_replacements_table()

    try:
        limit = int(limit or 500)
    except (TypeError, ValueError):
        limit = 500

    limit = max(1, min(limit, 5000))

    conn = get_conn()

    rows = conn.execute("""
        SELECT *
        FROM document_replacements
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT ?
    """, (
        limit,
    )).fetchall()

    conn.close()

    return [
        dict(row)
        for row in rows
    ]


def list_recoverable_failed_document_replacements(
    limit: int = 500,
) -> list[dict]:
    """Find failed old-file deletes that can safely be retried after restart."""
    ensure_document_replacements_table()
    from app.checklists.upload_jobs import ensure_upload_jobs_table

    ensure_upload_jobs_table()
    safe_limit = max(1, min(int(limit or 500), 5000))
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT replacement.*
            FROM document_replacements AS replacement
            JOIN upload_jobs AS job
              ON job.job_id = replacement.delete_job_id
            WHERE replacement.status = 'error'
              AND COALESCE(replacement.delete_job_id, '') <> ''
              AND job.job_type = 'delete'
              AND job.status = 'error'
              AND COALESCE(job.attempts, 0) < 3
            ORDER BY replacement.created_at ASC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def update_document_replacement(
    operation_id: str,
    **updates,
) -> dict | None:
    ensure_document_replacements_table()

    operation_id = clean_cell_value(operation_id)

    if not operation_id:
        return None

    filtered = {
        key: clean_cell_value(value)
        for key, value in updates.items()
        if key in ALLOWED_REPLACEMENT_UPDATE_FIELDS
    }

    if not filtered:
        return get_document_replacement(
            operation_id
        )

    now = utc_now_iso()
    filtered["updated_at"] = now

    status = clean_cell_value(
        filtered.get("status")
    ).lower()

    if status in TERMINAL_REPLACEMENT_STATUSES:
        filtered["finished_at"] = (
            clean_cell_value(
                filtered.get("finished_at")
            )
            or now
        )

    assignments = ", ".join(
        f"{column} = ?"
        for column in filtered
    )

    values = list(filtered.values())
    values.append(operation_id)

    conn = get_conn()

    conn.execute(
        f"""
        UPDATE document_replacements
        SET {assignments}
        WHERE operation_id = ?
        """,
        values,
    )

    conn.commit()
    conn.close()

    return get_document_replacement(
        operation_id
    )


def mark_document_replacement_failed(
    operation_id: str,
    error: str,
    stage: str = "replacement_failed",
) -> dict | None:
    return update_document_replacement(
        operation_id,
        status="error",
        stage=stage,
        error=(
            error
            or "unknown replacement error"
        ),
    )


def public_document_replacement_payload(
    record: dict | None,
) -> dict:
    if not record:
        return {}

    return {
        "operationId": (
            record.get("operation_id") or ""
        ),
        "dialogId": (
            record.get("dialog_id") or ""
        ),
        "checklistKey": (
            record.get("checklist_key") or ""
        ),
        "itemId": (
            record.get("item_id") or ""
        ),
        "seriesId": (
            record.get("series_id") or ""
        ),
        "archiveVersionId": (
            record.get("archive_version_id") or ""
        ),
        "oldDocumentId": (
            record.get("old_document_id") or ""
        ),
        "newDocumentId": (
            record.get("new_document_id") or ""
        ),
        "newUploadJobId": (
            record.get("new_upload_job_id") or ""
        ),
        "oldFileName": (
            record.get("old_file_name") or ""
        ),
        "newFileName": (
            record.get("new_file_name") or ""
        ),
        "oldYandexPath": (
            record.get("old_yandex_path") or ""
        ),
        "newYandexPath": (
            record.get("new_yandex_path") or ""
        ),
        "deleteJobId": (
            record.get("delete_job_id") or ""
        ),
        "status": (
            record.get("status") or ""
        ),
        "stage": (
            record.get("stage") or ""
        ),
        "error": (
            record.get("error") or ""
        ),
        "createdAt": (
            record.get("created_at") or ""
        ),
        "updatedAt": (
            record.get("updated_at") or ""
        ),
        "finishedAt": (
            record.get("finished_at") or ""
        ),
    }
