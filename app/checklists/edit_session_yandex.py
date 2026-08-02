from __future__ import annotations

import hashlib
import json
from typing import Any

from app.db import get_conn
from app.logging_utils import write_debug_log

from app.checklists.document_replacements import (
    ensure_document_replacements_table,
)
from app.checklists.upload_jobs import (
    ensure_upload_jobs_table,
)
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


SUPPORTED_DEFERRED_OPERATION_TYPES = {
    "document_upload",
    "document_replace",
    "document_remove",
    "checklist_item_update",
}


def stable_json_loads(value: str, default: Any) -> Any:
    raw = clean_cell_value(value)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def stable_json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def deterministic_job_id(action_key: str) -> str:
    return hashlib.sha256(
        clean_cell_value(action_key).encode("utf-8")
    ).hexdigest()[:32]


def ensure_edit_session_yandex_schema() -> None:
    ensure_upload_jobs_table()
    ensure_document_replacements_table()


def _storage_dialog_id(dialog_id: str, checklist_key: str) -> str:
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(checklist_key)
    return (
        normalized_dialog_id
        if normalized_checklist_key == "id"
        else f"{normalized_dialog_id}::{normalized_checklist_key}"
    )


def _insert_job_in_transaction(
    conn,
    *,
    action_key: str,
    session_id: str,
    operation_id: str,
    job_type: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str,
    local_path: str = "",
    file_name: str = "",
    file_size: int = 0,
    yandex_path: str = "",
    now: str,
) -> dict:
    normalized_action_key = clean_cell_value(action_key)
    if not normalized_action_key:
        raise ValueError("source action key is required")

    existing = conn.execute(
        """
        SELECT *
        FROM upload_jobs
        WHERE source_action_key = ?
        LIMIT 1
        """,
        (normalized_action_key,),
    ).fetchone()

    if existing:
        return dict(existing)

    normalized_job_type = clean_cell_value(job_type).lower()
    if normalized_job_type not in {"upload", "delete"}:
        raise ValueError("unsupported Yandex job type")

    job_id = deterministic_job_id(normalized_action_key)
    initial_stage = (
        "mirror_queued"
        if normalized_job_type == "upload"
        else "delete_queued"
    )
    safe_size = int(file_size or 0)

    conn.execute(
        """
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
            finished_at,
            source_session_id,
            source_operation_id,
            source_action_key
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            'queued', ?, 0, 0, ?, '', 0, ?, ?, '', '',
            ?, ?, ?
        )
        """,
        (
            job_id,
            normalized_job_type,
            normalize_dialog_id(dialog_id),
            normalize_checklist_key(checklist_key),
            clean_cell_value(item_id),
            clean_cell_value(document_id),
            clean_cell_value(local_path),
            clean_cell_value(file_name),
            safe_size,
            clean_cell_value(yandex_path),
            initial_stage,
            safe_size if normalized_job_type == "upload" else 0,
            now,
            now,
            clean_cell_value(session_id),
            clean_cell_value(operation_id),
            normalized_action_key,
        ),
    )

    row = conn.execute(
        "SELECT * FROM upload_jobs WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    return dict(row) if row else {"job_id": job_id}


def _mark_document_queued_in_transaction(
    conn,
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str,
    job_id: str,
) -> bool:
    storage_id = _storage_dialog_id(dialog_id, checklist_key)
    row = conn.execute(
        "SELECT title, data_json FROM checklists WHERE dialog_id = ?",
        (storage_id,),
    ).fetchone()

    if not row:
        return False

    data = stable_json_loads(row["data_json"] or "", {})
    if not isinstance(data, dict):
        return False

    found = False
    items = data.get("items") or []

    for item in items:
        if clean_cell_value(item.get("id")) != clean_cell_value(item_id):
            continue

        documents = item.get("documents") or []
        for document in documents:
            if clean_cell_value(document.get("id")) != clean_cell_value(document_id):
                continue

            document["mirrorStatus"] = "queued"
            document["mirrorStage"] = "mirror_queued"
            document["mirrorError"] = ""
            document["mirrorJobId"] = clean_cell_value(job_id)
            found = True
            break

        if found:
            item["documents"] = documents
            break

    if not found:
        return False

    data["items"] = items
    conn.execute(
        """
        UPDATE checklists
        SET data_json = ?
        WHERE dialog_id = ?
        """,
        (
            stable_json_dumps(data),
            storage_id,
        ),
    )
    return True


def _insert_replacement_in_transaction(
    conn,
    *,
    operation: dict,
    payload: dict,
    upload_job_id: str,
    now: str,
) -> dict:
    operation_id = clean_cell_value(operation.get("operation_id"))
    existing = conn.execute(
        """
        SELECT *
        FROM document_replacements
        WHERE operation_id = ?
        """,
        (operation_id,),
    ).fetchone()

    if existing:
        existing_record = dict(existing)
        existing_job_id = clean_cell_value(
            existing_record.get("new_upload_job_id")
        )
        if existing_job_id and existing_job_id != clean_cell_value(upload_job_id):
            raise RuntimeError(
                "replacement operation already points to another upload job"
            )
        return existing_record

    conn.execute(
        """
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
            '', '', 'pending', 'awaiting_new_upload', '', ?, ?, ''
        )
        """,
        (
            operation_id,
            normalize_dialog_id(operation.get("dialog_id")),
            normalize_checklist_key(operation.get("checklist_key")),
            clean_cell_value(operation.get("item_id")),
            clean_cell_value(operation.get("series_id")),
            clean_cell_value(payload.get("archiveVersionId")),
            clean_cell_value(payload.get("oldDocumentId")),
            clean_cell_value(payload.get("newDocumentId"))
            or clean_cell_value(operation.get("document_id")),
            clean_cell_value(upload_job_id),
            clean_cell_value(payload.get("oldFileName")),
            clean_cell_value(payload.get("newFileName")),
            clean_cell_value(payload.get("oldYandexPath")),
            now,
            now,
        ),
    )

    row = conn.execute(
        "SELECT * FROM document_replacements WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()
    return dict(row) if row else {"operation_id": operation_id}


def prepare_edit_session_yandex_jobs_in_transaction(
    conn,
    *,
    session_id: str,
    now: str,
) -> dict:
    normalized_session_id = clean_cell_value(session_id)

    rows = conn.execute(
        """
        SELECT *
        FROM edit_session_operations
        WHERE session_id = ?
          AND status = 'applied'
          AND operation_type IN (
              'document_upload',
              'document_replace',
              'document_remove',
              'checklist_item_update'
          )
        ORDER BY sequence_no ASC
        """,
        (normalized_session_id,),
    ).fetchall()

    jobs: list[dict] = []
    replacements: list[dict] = []
    skipped: list[dict] = []

    for raw_row in rows:
        operation = dict(raw_row)
        operation_id = clean_cell_value(operation.get("operation_id"))
        operation_type = clean_cell_value(operation.get("operation_type"))
        payload = stable_json_loads(operation.get("payload_json") or "", {})
        after = stable_json_loads(operation.get("after_json") or "", {})

        if not isinstance(payload, dict):
            payload = {}
        if not isinstance(after, dict):
            after = {}

        dialog_id = normalize_dialog_id(operation.get("dialog_id"))
        checklist_key = normalize_checklist_key(operation.get("checklist_key"))
        item_id = clean_cell_value(operation.get("item_id"))
        document_id = clean_cell_value(operation.get("document_id"))

        if operation_type == "document_upload":
            if not payload.get("deferredYandexUpload"):
                skipped.append({
                    "operationId": operation_id,
                    "reason": "upload_not_deferred",
                })
                continue

            document = after.get("document") or {}
            if not isinstance(document, dict):
                document = {}

            action_key = (
                f"edit-session:{normalized_session_id}:"
                f"operation:{operation_id}:upload"
            )
            job = _insert_job_in_transaction(
                conn,
                action_key=action_key,
                session_id=normalized_session_id,
                operation_id=operation_id,
                job_type="upload",
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                document_id=document_id,
                local_path=clean_cell_value(payload.get("localPath")),
                file_name=(
                    clean_cell_value(payload.get("fileName"))
                    or clean_cell_value(document.get("name"))
                ),
                file_size=int(document.get("size") or 0),
                now=now,
            )
            _mark_document_queued_in_transaction(
                conn,
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                document_id=document_id,
                job_id=job.get("job_id") or "",
            )
            jobs.append(job)
            continue

        if operation_type == "document_replace":
            if not payload.get("deferredYandexUpload"):
                skipped.append({
                    "operationId": operation_id,
                    "reason": "replacement_upload_not_deferred",
                })
                continue

            document = after.get("document") or {}
            if not isinstance(document, dict):
                document = {}

            action_key = (
                f"edit-session:{normalized_session_id}:"
                f"operation:{operation_id}:replacement-upload"
            )
            job = _insert_job_in_transaction(
                conn,
                action_key=action_key,
                session_id=normalized_session_id,
                operation_id=operation_id,
                job_type="upload",
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                document_id=document_id,
                local_path=clean_cell_value(payload.get("newLocalPath")),
                file_name=(
                    clean_cell_value(payload.get("newFileName"))
                    or clean_cell_value(document.get("name"))
                ),
                file_size=int(document.get("size") or 0),
                now=now,
            )
            _mark_document_queued_in_transaction(
                conn,
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                document_id=document_id,
                job_id=job.get("job_id") or "",
            )
            replacement = _insert_replacement_in_transaction(
                conn,
                operation=operation,
                payload=payload,
                upload_job_id=job.get("job_id") or "",
                now=now,
            )
            jobs.append(job)
            replacements.append(replacement)
            continue

        if operation_type == "document_remove":
            yandex_path = clean_cell_value(payload.get("oldYandexPath"))
            if not payload.get("deferredYandexDelete") or not yandex_path:
                skipped.append({
                    "operationId": operation_id,
                    "reason": "delete_not_required",
                })
                continue

            action_key = (
                f"edit-session:{normalized_session_id}:"
                f"operation:{operation_id}:delete:{document_id}"
            )
            job = _insert_job_in_transaction(
                conn,
                action_key=action_key,
                session_id=normalized_session_id,
                operation_id=operation_id,
                job_type="delete",
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                document_id=document_id,
                file_name=clean_cell_value(payload.get("fileName")),
                yandex_path=yandex_path,
                now=now,
            )
            jobs.append(job)
            continue

        if operation_type == "checklist_item_update":
            deferred_deletes = payload.get("deferredYandexDeletes") or []
            if not isinstance(deferred_deletes, list):
                deferred_deletes = []

            for index, raw_delete in enumerate(deferred_deletes):
                if not isinstance(raw_delete, dict):
                    continue

                yandex_path = clean_cell_value(raw_delete.get("yandexPath"))
                if not yandex_path:
                    continue

                delete_document_id = clean_cell_value(
                    raw_delete.get("documentId")
                )
                path_hash = hashlib.sha256(
                    yandex_path.encode("utf-8")
                ).hexdigest()[:12]
                action_key = (
                    f"edit-session:{normalized_session_id}:"
                    f"operation:{operation_id}:status-no-delete:"
                    f"{index}:{delete_document_id}:{path_hash}"
                )
                job = _insert_job_in_transaction(
                    conn,
                    action_key=action_key,
                    session_id=normalized_session_id,
                    operation_id=operation_id,
                    job_type="delete",
                    dialog_id=dialog_id,
                    checklist_key=checklist_key,
                    item_id=item_id,
                    document_id=delete_document_id,
                    file_name=clean_cell_value(raw_delete.get("fileName")),
                    yandex_path=yandex_path,
                    now=now,
                )
                jobs.append(job)

    return {
        "ok": True,
        "sessionId": normalized_session_id,
        "jobCount": len(jobs),
        "replacementCount": len(replacements),
        "skippedCount": len(skipped),
        "jobIds": [clean_cell_value(job.get("job_id")) for job in jobs],
        "jobs": jobs,
        "replacements": replacements,
        "skipped": skipped,
    }


def list_edit_session_yandex_jobs(
    *,
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
) -> list[dict]:
    from app.checklists.edit_sessions import get_edit_session_for_actor

    session = get_edit_session_for_actor(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )

    ensure_edit_session_yandex_schema()
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM upload_jobs
            WHERE source_session_id = ?
            ORDER BY created_at ASC, job_id ASC
            """,
            (session["session_id"],),
        ).fetchall()
    finally:
        conn.close()

    return [
        {
            "jobId": row["job_id"] or "",
            "jobType": row["job_type"] or "",
            "dialogId": row["dialog_id"] or "",
            "checklistKey": row["checklist_key"] or "",
            "itemId": row["item_id"] or "",
            "documentId": row["document_id"] or "",
            "localPath": row["local_path"] or "",
            "fileName": row["file_name"] or "",
            "fileSize": int(row["file_size"] or 0),
            "yandexPath": row["yandex_path"] or "",
            "status": row["status"] or "",
            "stage": row["stage"] or "",
            "error": row["error"] or "",
            "sourceSessionId": row["source_session_id"] or "",
            "sourceOperationId": row["source_operation_id"] or "",
            "sourceActionKey": row["source_action_key"] or "",
            "createdAt": row["created_at"] or "",
            "updatedAt": row["updated_at"] or "",
        }
        for row in rows
    ]


def enqueue_committed_edit_session_yandex_jobs(
    session_id: str,
    *,
    source: str = "edit_session_commit",
) -> dict:
    normalized_session_id = clean_cell_value(session_id)
    if not normalized_session_id:
        return {
            "ok": False,
            "sessionId": "",
            "error": "sessionId is required",
        }

    ensure_edit_session_yandex_schema()
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT job_id
            FROM upload_jobs
            WHERE source_session_id = ?
              AND status = 'queued'
            ORDER BY created_at ASC, job_id ASC
            """,
            (normalized_session_id,),
        ).fetchall()
    finally:
        conn.close()

    from app.checklists.yandex_mirror_queue import (
        enqueue_yandex_mirror_job,
    )

    queued = 0
    already_queued = 0
    skipped = 0
    errors = []
    results = []

    for row in rows:
        job_id = clean_cell_value(row["job_id"])
        try:
            result = enqueue_yandex_mirror_job(
                job_id,
                source=source,
            )
            results.append(result)
            if result.get("queued"):
                queued += 1
            elif result.get("alreadyQueued") or result.get("alreadyRunning"):
                already_queued += 1
            else:
                skipped += 1
        except Exception as exc:
            errors.append({
                "jobId": job_id,
                "error": str(exc),
            })

    payload = {
        "ok": not errors,
        "sessionId": normalized_session_id,
        "found": len(rows),
        "queued": queued,
        "alreadyQueued": already_queued,
        "skipped": skipped,
        "errorCount": len(errors),
        "errors": errors,
        "results": results,
    }

    write_debug_log(
        "edit_session_yandex_jobs_enqueued",
        payload,
    )
    return payload
