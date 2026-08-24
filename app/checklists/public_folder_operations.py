from __future__ import annotations

import os
import re
import shutil
import threading
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from app.db import get_conn, init_db
from app.logging_utils import write_debug_log
from app.settings import (
    BASE_DIR,
    PUBLIC_FOLDER_STAGING_ROOT,
    UPLOAD_ROOT,
)

from app.checklists.config import get_checklist_config
from app.checklists.document_replacements import (
    create_document_replacement,
    get_document_replacement,
    mark_document_replacement_failed,
)
from app.checklists.documents import (
    build_archive_rel_path,
    build_document_view_url,
    build_folder_view_url,
    build_versioned_archive_filename,
    get_next_archive_version_number,
    get_upload_file_path_from_url,
    migrate_legacy_document_fields,
    normalize_archive_version_record,
    normalize_document_record,
    normalize_documents_list,
)
from app.checklists.edit_sessions import ACTIVE_EDIT_SESSION_STATUSES
from app.checklists.normalization import (
    derive_indicator_from_status,
    normalize_checklist_data,
)
from app.checklists.storage import get_checklist, save_checklist
from app.checklists.upload_jobs import (
    cancel_upload_jobs_for_document,
    ensure_yandex_upload_job_for_reconciliation,
    get_latest_document_job,
    resolve_document_mirror_status,
)
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)
from app.checklists.yandex_mirror_queue import enqueue_yandex_mirror_job


PUBLIC_OPERATION_UPLOAD = "upload"
PUBLIC_OPERATION_REPLACE = "replace"
PUBLIC_OPERATION_PENDING_STATUSES = {
    "receiving",
    "queued",
    "processing",
}
PUBLIC_OPERATION_TERMINAL_STATUSES = {
    "completed",
    "conflict",
    "error",
}

_worker_lock = threading.Lock()
_worker_started = False
_worker_event = threading.Event()
_item_locks_guard = threading.Lock()
_item_locks: dict[str, threading.Lock] = {}


class PublicFolderOperationError(RuntimeError):
    pass


class PublicFolderOperationConflict(PublicFolderOperationError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_public_folder_operation_schema() -> None:
    init_db()


def normalize_external_identity_part(value: str, field_label: str) -> str:
    normalized = " ".join(clean_cell_value(value).split())
    if not normalized:
        raise ValueError(f"Поле «{field_label}» обязательно")
    if len(normalized) > 80:
        raise ValueError(f"Поле «{field_label}» слишком длинное")
    if any(ord(character) < 32 for character in normalized):
        raise ValueError(f"Поле «{field_label}» содержит недопустимые символы")
    return normalized


def _safe_suffix(file_name: str) -> str:
    suffix = Path(file_name or "").suffix
    return re.sub(r"[^A-Za-z0-9.]+", "", suffix)[:16]


def _staging_path(link_id: str, operation_id: str, file_name: str) -> Path:
    return (
        PUBLIC_FOLDER_STAGING_ROOT
        / clean_cell_value(link_id)
        / clean_cell_value(operation_id)
        / ("payload" + _safe_suffix(file_name))
    )


def require_public_staging_path(value: str | Path) -> Path:
    path = Path(value).resolve()
    root = PUBLIC_FOLDER_STAGING_ROOT.resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise PublicFolderOperationError(
            "public upload staging path escaped runtime root"
        ) from exc
    return path


def _name_parts(file_name: str) -> tuple[str, str, int]:
    safe_name = Path(file_name or "file.bin").name or "file.bin"
    suffix = Path(safe_name).suffix
    stem = safe_name[:-len(suffix)] if suffix else safe_name
    match = re.match(r"^(.*) \((\d+)\)$", stem)
    if match and int(match.group(2)) >= 2:
        return match.group(1) or "file", suffix, int(match.group(2))
    return stem or "file", suffix, 1


def unique_file_name(file_name: str, occupied_names) -> str:
    original = Path(file_name or "file.bin").name or "file.bin"
    occupied = {
        clean_cell_value(value).casefold()
        for value in occupied_names or []
        if clean_cell_value(value)
    }
    if original.casefold() not in occupied:
        return original

    base, suffix, current_number = _name_parts(original)
    number = max(2, current_number + 1 if current_number >= 2 else 2)
    while True:
        candidate = f"{base} ({number}){suffix}"
        if candidate.casefold() not in occupied:
            return candidate
        number += 1


def _find_item(data: dict, item_id: str) -> tuple[int, dict]:
    for index, raw_item in enumerate(data.get("items", []) or []):
        if clean_cell_value(raw_item.get("id")) != clean_cell_value(item_id):
            continue
        item = migrate_legacy_document_fields(raw_item)
        data["items"][index] = item
        return index, item
    raise KeyError("item not found")


def _find_document(documents: list[dict], document_id: str) -> tuple[int, dict | None]:
    normalized_id = clean_cell_value(document_id)
    for index, document in enumerate(documents):
        if clean_cell_value(document.get("id")) == normalized_id:
            return index, document
    return -1, None


def _row_to_dict(row) -> dict | None:
    return dict(row) if row else None


def get_public_folder_operation(operation_id: str) -> dict | None:
    ensure_public_folder_operation_schema()
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM public_folder_operations WHERE operation_id = ?",
            (clean_cell_value(operation_id),),
        ).fetchone()
    finally:
        conn.close()
    return _row_to_dict(row)


def public_folder_operation_payload(record: dict | None) -> dict:
    if not record:
        return {}
    status = clean_cell_value(record.get("status"))
    stage = clean_cell_value(record.get("stage"))
    error = clean_cell_value(record.get("error"))
    if status == "error" and stage not in {"receive_interrupted"}:
        # The durable row keeps the original exception for diagnostics, but a
        # secret public link must not disclose local paths, SQL details or
        # infrastructure errors to an external visitor.
        error = "Не удалось обработать файл. Обратитесь к владельцу папки."
    return {
        "operationId": record.get("operation_id") or "",
        "operationType": record.get("operation_type") or "",
        "fileName": record.get("file_name") or "",
        "originalFileName": record.get("original_file_name") or "",
        "fileSize": int(record.get("file_size") or 0),
        "uploaderName": record.get("uploader_name") or "",
        "status": status,
        "stage": stage,
        "error": error,
        "documentId": record.get("document_id") or "",
        "uploadJobId": record.get("upload_job_id") or "",
        "createdAt": record.get("created_at") or "",
        "updatedAt": record.get("updated_at") or "",
        "finishedAt": record.get("finished_at") or "",
        "waitingForEditSession": (
            clean_cell_value(record.get("stage"))
            == "waiting_for_edit_session"
        ),
    }


def _current_documents_for_link(link: dict) -> tuple[dict, dict, list[dict]]:
    data = get_checklist(
        link.get("dialog_id") or "",
        link.get("checklist_key") or "id",
    )
    _, item = _find_item(data, link.get("item_id") or "")
    return data, item, normalize_documents_list(item.get("documents"))


def prepare_public_folder_operation(
    *,
    link: dict,
    operation_type: str,
    original_file_name: str,
    first_name: str,
    last_name: str,
    expected_document_id: str = "",
    force_replace: bool = False,
) -> dict:
    ensure_public_folder_operation_schema()
    normalized_type = clean_cell_value(operation_type).lower()
    if normalized_type not in {
        PUBLIC_OPERATION_UPLOAD,
        PUBLIC_OPERATION_REPLACE,
    }:
        raise ValueError("unsupported public folder operation")

    first = normalize_external_identity_part(first_name, "Имя")
    last = normalize_external_identity_part(last_name, "Фамилия")
    uploader_name = f"{first} {last}"
    safe_original_name = Path(original_file_name or "file.bin").name
    if not safe_original_name:
        raise ValueError("Имя файла обязательно")

    _, _, documents = _current_documents_for_link(link)
    expected_id = clean_cell_value(expected_document_id)
    expected_series_id = ""
    excluded_document_id = ""

    if normalized_type == PUBLIC_OPERATION_REPLACE:
        if not expected_id:
            raise ValueError("documentId is required for replacement")
        _, target = _find_document(documents, expected_id)
        if not target:
            raise PublicFolderOperationConflict(
                "Выбранная версия файла уже недоступна. Обновите страницу."
            )
        mirror = resolve_document_mirror_status(target)
        mirror_status = clean_cell_value(mirror.get("status")).lower()
        if mirror_status in {"queued", "running"}:
            raise PublicFolderOperationConflict(
                "Файл ещё синхронизируется с Яндекс.Диском"
            )
        if mirror_status == "error" and not force_replace:
            raise PublicFolderOperationConflict(
                "У файла есть ошибка синхронизации; подтвердите замену"
            )
        expected_series_id = (
            clean_cell_value(target.get("seriesId")) or expected_id
        )
        excluded_document_id = expected_id

    occupied_names = [
        document.get("name") or ""
        for document in documents
        if clean_cell_value(document.get("id")) != excluded_document_id
    ]

    operation_id = uuid.uuid4().hex
    document_id = uuid.uuid4().hex
    replacement_operation_id = (
        uuid.uuid4().hex
        if normalized_type == PUBLIC_OPERATION_REPLACE
        else ""
    )
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        reserved_rows = conn.execute(
            """
            SELECT file_name
            FROM public_folder_operations
            WHERE dialog_id = ?
              AND checklist_key = ?
              AND item_id = ?
              AND status IN ('receiving', 'queued', 'processing')
            """,
            (
                normalize_dialog_id(link.get("dialog_id")),
                normalize_checklist_key(link.get("checklist_key")),
                clean_cell_value(link.get("item_id")),
            ),
        ).fetchall()
        occupied_names.extend(row["file_name"] for row in reserved_rows)
        resolved_name = unique_file_name(safe_original_name, occupied_names)
        staging_path = _staging_path(
            link.get("link_id") or "",
            operation_id,
            safe_original_name,
        )
        conn.execute(
            """
            INSERT INTO public_folder_operations(
                operation_id, link_id, link_generation,
                dialog_id, checklist_key, item_id, operation_type,
                expected_document_id, expected_series_id,
                document_id, replacement_operation_id, upload_job_id,
                original_file_name, file_name, staging_path, file_size,
                first_name, last_name, uploader_name, force_replace,
                status, stage, error, attempts,
                created_at, updated_at, started_at, finished_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, 0,
                ?, ?, ?, ?, 'receiving', 'receiving', '', 0,
                ?, ?, '', ''
            )
            """,
            (
                operation_id,
                clean_cell_value(link.get("link_id")),
                int(link.get("generation") or 0),
                normalize_dialog_id(link.get("dialog_id")),
                normalize_checklist_key(link.get("checklist_key")),
                clean_cell_value(link.get("item_id")),
                normalized_type,
                expected_id,
                expected_series_id,
                document_id,
                replacement_operation_id,
                safe_original_name,
                resolved_name,
                str(staging_path),
                first,
                last,
                uploader_name,
                int(bool(force_replace)),
                now,
                now,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return get_public_folder_operation(operation_id) or {}


def finalize_received_public_folder_operation(
    operation_id: str,
    file_size: int,
) -> dict:
    record = get_public_folder_operation(operation_id)
    if not record:
        raise PublicFolderOperationError("public folder operation not found")
    staging_path = require_public_staging_path(record.get("staging_path") or "")
    actual_size = staging_path.stat().st_size if staging_path.is_file() else 0
    normalized_size = int(file_size or actual_size or 0)
    if normalized_size <= 0 or actual_size <= 0:
        raise ValueError("Загружен пустой файл")
    if normalized_size != actual_size:
        normalized_size = actual_size

    now = utc_now_iso()
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            UPDATE public_folder_operations
            SET file_size = ?, status = 'queued', stage = 'accepted',
                error = '', updated_at = ?
            WHERE operation_id = ? AND status = 'receiving'
            """,
            (normalized_size, now, clean_cell_value(operation_id)),
        )
        conn.commit()
        if int(cur.rowcount or 0) != 1:
            raise PublicFolderOperationConflict(
                "public folder operation is no longer receiving"
            )
    finally:
        conn.close()

    _worker_event.set()
    return get_public_folder_operation(operation_id) or {}


def fail_public_folder_operation_receiving(
    operation_id: str,
    error: str,
) -> dict:
    record = get_public_folder_operation(operation_id)
    if not record:
        return {}
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE public_folder_operations
            SET status = 'error', stage = 'receive_failed', error = ?,
                updated_at = ?, finished_at = ?
            WHERE operation_id = ? AND status = 'receiving'
            """,
            (clean_cell_value(error), now, now, clean_cell_value(operation_id)),
        )
        conn.commit()
    finally:
        conn.close()
    try:
        staging = require_public_staging_path(record.get("staging_path") or "")
        if staging.exists():
            staging.unlink()
    except Exception:
        pass
    return get_public_folder_operation(operation_id) or {}


def has_processing_public_folder_operation(
    dialog_id: str,
    checklist_key: str,
) -> bool:
    ensure_public_folder_operation_schema()
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT 1
            FROM public_folder_operations
            WHERE dialog_id = ?
              AND checklist_key = ?
              AND status = 'processing'
            LIMIT 1
            """,
            (
                normalize_dialog_id(dialog_id),
                normalize_checklist_key(checklist_key),
            ),
        ).fetchone()
    finally:
        conn.close()
    return bool(row)


def _claim_public_folder_operation(operation_id: str) -> dict | None:
    ensure_public_folder_operation_schema()
    normalized_operation_id = clean_cell_value(operation_id)
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM public_folder_operations WHERE operation_id = ?",
            (normalized_operation_id,),
        ).fetchone()
        if not row or row["status"] != "queued":
            conn.commit()
            return None

        active_placeholders = ",".join(
            "?" for _ in ACTIVE_EDIT_SESSION_STATUSES
        )
        active = conn.execute(
            f"""
            SELECT 1 FROM edit_sessions
            WHERE dialog_id = ?
              AND status IN ({active_placeholders})
            LIMIT 1
            """,
            (
                row["dialog_id"],
                *sorted(ACTIVE_EDIT_SESSION_STATUSES),
            ),
        ).fetchone()
        if active:
            conn.execute(
                """
                UPDATE public_folder_operations
                SET stage = 'waiting_for_edit_session', updated_at = ?
                WHERE operation_id = ? AND status = 'queued'
                """,
                (now, normalized_operation_id),
            )
            conn.commit()
            return None

        other_processing = conn.execute(
            """
            SELECT 1 FROM public_folder_operations
            WHERE dialog_id = ?
              AND checklist_key = ?
              AND item_id = ?
              AND status = 'processing'
              AND operation_id <> ?
            LIMIT 1
            """,
            (
                row["dialog_id"],
                row["checklist_key"],
                row["item_id"],
                normalized_operation_id,
            ),
        ).fetchone()
        if other_processing:
            conn.commit()
            return None

        cur = conn.execute(
            """
            UPDATE public_folder_operations
            SET status = 'processing', stage = 'applying', error = '',
                attempts = attempts + 1, started_at = ?, updated_at = ?
            WHERE operation_id = ? AND status = 'queued'
            """,
            (now, now, normalized_operation_id),
        )
        conn.commit()
        if int(cur.rowcount or 0) != 1:
            return None
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return get_public_folder_operation(normalized_operation_id)


def _item_lock(record: dict) -> threading.Lock:
    key = "::".join((
        normalize_dialog_id(record.get("dialog_id")),
        normalize_checklist_key(record.get("checklist_key")),
        clean_cell_value(record.get("item_id")),
    ))
    with _item_locks_guard:
        lock = _item_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _item_locks[key] = lock
        return lock


def _existing_operation_document(
    data: dict,
    item_id: str,
    document_id: str,
) -> dict | None:
    try:
        _, item = _find_item(data, item_id)
    except KeyError:
        return None
    _, document = _find_document(
        normalize_documents_list(item.get("documents")),
        document_id,
    )
    return document


def _updated_item(saved: dict, item_id: str) -> dict:
    _, item = _find_item(saved, item_id)
    return item


def _resolve_final_file_name(
    record: dict,
    documents: list[dict],
    *,
    excluded_document_id: str = "",
) -> str:
    occupied = [
        document.get("name") or ""
        for document in documents
        if clean_cell_value(document.get("id"))
        != clean_cell_value(excluded_document_id)
    ]
    return unique_file_name(record.get("file_name") or "file.bin", occupied)


def _job_id(job: dict | None) -> str:
    return clean_cell_value(
        (job or {}).get("job_id") or (job or {}).get("jobId")
    )


def _public_document_rel_path(
    dialog_id: str,
    item_id: str,
    document_id: str,
    file_name: str,
) -> str:
    safe_name = Path(file_name or "file.bin").name or "file.bin"
    return "/".join((
        "checklists",
        normalize_dialog_id(dialog_id),
        clean_cell_value(item_id),
        f"{clean_cell_value(document_id)}_{safe_name}",
    ))


def _copy_staging_to_current(staging: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.name}.{uuid.uuid4().hex}.public_tmp"
    )
    try:
        shutil.copy2(staging, temporary)
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()


def _copy_current_to_public_archive(
    *,
    dialog_id: str,
    item_id: str,
    document: dict,
    archive_version_id: str,
    archived_by_id: str,
    archived_by_name: str,
    archived_at: str,
) -> tuple[dict, Path]:
    current = normalize_document_record(document)
    series_id = (
        clean_cell_value(current.get("seriesId"))
        or clean_cell_value(current.get("id"))
        or uuid.uuid4().hex
    )
    source_url = (
        clean_cell_value(current.get("fileUrl"))
        or clean_cell_value(current.get("path"))
    )
    source_path = get_upload_file_path_from_url(source_url)
    if not source_path or not source_path.is_file():
        raise FileNotFoundError("Текущий локальный файл не найден")

    version = get_next_archive_version_number(current)
    archive_filename = build_versioned_archive_filename(
        current.get("name") or source_path.name,
        version,
    )
    archive_rel_path = build_archive_rel_path(
        dialog_id=dialog_id,
        item_id=item_id,
        series_id=series_id,
        archive_filename=archive_filename,
    )
    archive_path = UPLOAD_ROOT / archive_rel_path
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = archive_path.with_name(
        f".{archive_path.name}.{archive_version_id}.public_tmp"
    )
    try:
        shutil.copy2(source_path, temporary)
        os.replace(temporary, archive_path)
    finally:
        if temporary.exists():
            temporary.unlink()

    archive_url = "/uploads/" + archive_rel_path.replace("\\", "/")
    original_yandex_path = clean_cell_value(current.get("yandexPath"))
    version_record = normalize_archive_version_record({
        "id": clean_cell_value(archive_version_id) or uuid.uuid4().hex,
        "seriesId": series_id,
        "originalDocumentId": clean_cell_value(current.get("id")),
        "version": version,
        "versionLabel": f"v{version}",
        "name": archive_filename,
        "originalName": clean_cell_value(current.get("name")) or source_path.name,
        "path": archive_url,
        "fileUrl": archive_url,
        "previewUrl": archive_url,
        "size": int(archive_path.stat().st_size),
        "uploadedAt": clean_cell_value(
            current.get("uploadedAt") or current.get("modifiedAt")
        ),
        "uploadedById": clean_cell_value(current.get("uploadedById")),
        "uploadedByName": clean_cell_value(current.get("uploadedByName")),
        "archivedAt": clean_cell_value(archived_at) or utc_now_iso(),
        "archivedById": clean_cell_value(archived_by_id),
        "archivedByName": clean_cell_value(archived_by_name),
        "source": "local_archive",
        "originalYandexPath": original_yandex_path,
        "yandexDeleteStatus": (
            "pending_after_replacement_sync"
            if original_yandex_path
            else "not_required"
        ),
        "yandexDeleteJobId": "",
        "yandexDeleteError": "",
    }, series_id=series_id)
    return version_record, archive_path


def _cleanup_operation_staging(record: dict | None) -> None:
    if not record:
        return
    try:
        staging = require_public_staging_path(record.get("staging_path") or "")
        if staging.exists():
            staging.unlink()
        parent = staging.parent
        root = PUBLIC_FOLDER_STAGING_ROOT.resolve()
        while parent != root:
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent
    except Exception:
        pass


def _apply_public_upload(record: dict) -> dict:
    dialog_id = normalize_dialog_id(record.get("dialog_id"))
    checklist_key = normalize_checklist_key(record.get("checklist_key"))
    item_id = clean_cell_value(record.get("item_id"))
    document_id = clean_cell_value(record.get("document_id"))
    data = get_checklist(dialog_id, checklist_key)

    existing = _existing_operation_document(data, item_id, document_id)
    if existing:
        existing_job = get_latest_document_job(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            document_id=document_id,
        )
        existing_job_id = _job_id(existing_job)
        if existing_job_id:
            enqueue_yandex_mirror_job(
                existing_job_id,
                source="public_folder_upload_recovery",
            )
        return {
            "documentId": document_id,
            "uploadJobId": existing_job_id,
            "alreadyApplied": True,
        }

    config = get_checklist_config(checklist_key)
    item_index, target_item = _find_item(data, item_id)
    documents = normalize_documents_list(target_item.get("documents"))
    final_name = _resolve_final_file_name(record, documents)
    staging = require_public_staging_path(record.get("staging_path") or "")
    if not staging.is_file():
        raise FileNotFoundError("Принятый файл не найден во временном хранилище")

    rel_path = _public_document_rel_path(
        dialog_id,
        item_id,
        document_id,
        final_name,
    )
    destination = UPLOAD_ROOT / rel_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    file_size = int(staging.stat().st_size)
    moved = False
    checklist_saved = False
    upload_job_id = ""

    try:
        _copy_staging_to_current(staging, destination)
        moved = True
        upload_job = ensure_yandex_upload_job_for_reconciliation(
            dialog_id=dialog_id,
            checklist_key=config.key,
            item_id=item_id,
            document_id=document_id,
            local_path=str(destination),
            file_name=final_name,
            file_size=file_size,
        )
        upload_job_id = _job_id(upload_job)
        if not upload_job_id:
            raise RuntimeError("Не удалось создать задание синхронизации")

        uploaded_at = utc_now_iso()
        file_url = "/uploads/" + rel_path.replace("\\", "/")
        document = normalize_document_record({
            "id": document_id,
            "seriesId": document_id,
            "name": final_name,
            "path": file_url,
            "fileUrl": file_url,
            "previewUrl": build_document_view_url(
                dialog_id, config.key, item_id, document_id
            ),
            "size": file_size,
            "modifiedAt": uploaded_at,
            "uploadedAt": uploaded_at,
            "uploadedById": "external:" + clean_cell_value(record.get("link_id")),
            "uploadedByName": clean_cell_value(record.get("uploader_name")),
            "source": "public_folder",
            "mirrorStatus": clean_cell_value(upload_job.get("status")) or "queued",
            "mirrorError": "",
            "mirrorJobId": upload_job_id,
            "yandexPath": clean_cell_value(upload_job.get("yandex_path")),
            "yandexFileUrl": "",
            "yandexFolderAlias": "",
        })
        documents.append(document)
        documents = normalize_documents_list(documents)
        target_item["documents"] = documents
        target_item["folderPath"] = (
            "/" + str(destination.parent.relative_to(BASE_DIR)).replace("\\", "/")
        )
        target_item["folderUrl"] = build_folder_view_url(
            dialog_id, config.key, item_id
        )
        first_document = documents[0] if documents else {}
        target_item["documentUrl"] = clean_cell_value(
            first_document.get("fileUrl")
        )
        target_item["documentName"] = clean_cell_value(
            first_document.get("name")
        )
        if config.is_active_group(target_item.get("group")):
            target_item["status"] = "Есть"
            target_item["priority"] = derive_indicator_from_status("Есть")
        data["items"][item_index] = target_item
        saved = save_checklist(
            dialog_id,
            normalize_checklist_data(data, config.key),
            config.key,
        )
        checklist_saved = True
        _updated_item(saved, item_id)
    except Exception:
        if not checklist_saved:
            if upload_job_id:
                cancel_upload_jobs_for_document(
                    dialog_id=dialog_id,
                    checklist_key=config.key,
                    item_id=item_id,
                    document_id=document_id,
                )
            if moved and destination.exists():
                destination.unlink()
        raise

    enqueue_yandex_mirror_job(
        upload_job_id,
        source="public_folder_upload",
    )
    return {
        "documentId": document_id,
        "uploadJobId": upload_job_id,
        "fileName": final_name,
        "alreadyApplied": False,
    }


def _apply_public_replacement(record: dict) -> dict:
    dialog_id = normalize_dialog_id(record.get("dialog_id"))
    checklist_key = normalize_checklist_key(record.get("checklist_key"))
    item_id = clean_cell_value(record.get("item_id"))
    expected_document_id = clean_cell_value(record.get("expected_document_id"))
    expected_series_id = clean_cell_value(record.get("expected_series_id"))
    new_document_id = clean_cell_value(record.get("document_id"))
    replacement_operation_id = clean_cell_value(
        record.get("replacement_operation_id")
    )
    data = get_checklist(dialog_id, checklist_key)

    existing = _existing_operation_document(data, item_id, new_document_id)
    if existing:
        existing_job = get_latest_document_job(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            document_id=new_document_id,
        )
        existing_job_id = _job_id(existing_job)
        if existing_job_id:
            enqueue_yandex_mirror_job(
                existing_job_id,
                source="public_folder_replace_recovery",
            )
        return {
            "documentId": new_document_id,
            "uploadJobId": existing_job_id,
            "replacementOperationId": replacement_operation_id,
            "alreadyApplied": True,
        }

    config = get_checklist_config(checklist_key)
    item_index, target_item = _find_item(data, item_id)
    documents = normalize_documents_list(target_item.get("documents"))
    old_index, old_document = _find_document(documents, expected_document_id)
    if not old_document:
        current_same_series = next(
            (
                document
                for document in documents
                if (
                    clean_cell_value(document.get("seriesId"))
                    == expected_series_id
                )
            ),
            None,
        )
        if current_same_series:
            raise PublicFolderOperationConflict(
                "Файл уже был заменён другим пользователем. Обновите страницу и повторите действие."
            )
        raise PublicFolderOperationConflict(
            "Выбранный файл больше не является текущим"
        )

    mirror = resolve_document_mirror_status(old_document)
    mirror_status = clean_cell_value(mirror.get("status")).lower()
    if mirror_status in {"queued", "running"}:
        raise PublicFolderOperationConflict(
            "Файл ещё синхронизируется с Яндекс.Диском"
        )
    if mirror_status == "error" and not bool(record.get("force_replace")):
        raise PublicFolderOperationConflict(
            "У файла есть ошибка синхронизации; требуется подтверждение"
        )

    final_name = _resolve_final_file_name(
        record,
        documents,
        excluded_document_id=expected_document_id,
    )
    staging = require_public_staging_path(record.get("staging_path") or "")
    if not staging.is_file():
        raise FileNotFoundError("Принятый файл не найден во временном хранилище")

    old_file_url = (
        clean_cell_value(old_document.get("fileUrl"))
        or clean_cell_value(old_document.get("path"))
    )
    old_local_path = get_upload_file_path_from_url(old_file_url)
    if not old_local_path or not old_local_path.is_file():
        raise FileNotFoundError("Текущий локальный файл не найден")

    new_rel_path = _public_document_rel_path(
        dialog_id,
        item_id,
        new_document_id,
        final_name,
    )
    new_abs_path = UPLOAD_ROOT / new_rel_path
    new_abs_path.parent.mkdir(parents=True, exist_ok=True)
    file_size = int(staging.stat().st_size)
    replacement_at = utc_now_iso()
    archived_version: dict | None = None
    archived_path: Path | None = None
    upload_job_id = ""
    replacement_registered = False
    checklist_saved = False
    moved_new = False

    try:
        archived_version, archived_path = _copy_current_to_public_archive(
            dialog_id=dialog_id,
            item_id=item_id,
            document=old_document,
            archive_version_id=replacement_operation_id,
            archived_by_id="external:" + clean_cell_value(record.get("link_id")),
            archived_by_name=clean_cell_value(record.get("uploader_name")),
            archived_at=replacement_at,
        )
        _copy_staging_to_current(staging, new_abs_path)
        moved_new = True

        upload_job = ensure_yandex_upload_job_for_reconciliation(
            dialog_id=dialog_id,
            checklist_key=config.key,
            item_id=item_id,
            document_id=new_document_id,
            local_path=str(new_abs_path),
            file_name=final_name,
            file_size=file_size,
        )
        upload_job_id = _job_id(upload_job)
        if not upload_job_id:
            raise RuntimeError("Не удалось создать задание синхронизации")

        if not get_document_replacement(replacement_operation_id):
            create_document_replacement(
                operation_id=replacement_operation_id,
                dialog_id=dialog_id,
                checklist_key=config.key,
                item_id=item_id,
                series_id=expected_series_id or expected_document_id,
                archive_version_id=clean_cell_value(archived_version.get("id")),
                old_document_id=expected_document_id,
                new_document_id=new_document_id,
                new_upload_job_id=upload_job_id,
                old_file_name=clean_cell_value(old_document.get("name")),
                new_file_name=final_name,
                old_yandex_path=clean_cell_value(old_document.get("yandexPath")),
            )
        replacement_registered = True

        new_file_url = "/uploads/" + new_rel_path.replace("\\", "/")
        new_document = normalize_document_record({
            "id": new_document_id,
            "seriesId": expected_series_id or expected_document_id,
            "name": final_name,
            "path": new_file_url,
            "fileUrl": new_file_url,
            "previewUrl": build_document_view_url(
                dialog_id, config.key, item_id, new_document_id
            ),
            "size": file_size,
            "modifiedAt": replacement_at,
            "uploadedAt": replacement_at,
            "uploadedById": "external:" + clean_cell_value(record.get("link_id")),
            "uploadedByName": clean_cell_value(record.get("uploader_name")),
            "source": "public_folder",
            "archiveVersions": (
                list(old_document.get("archiveVersions") or [])
                + [archived_version]
            ),
            "lastReplacedAt": replacement_at,
            "lastReplacedById": "external:" + clean_cell_value(record.get("link_id")),
            "lastReplacedByName": clean_cell_value(record.get("uploader_name")),
            "replacementOperationId": replacement_operation_id,
            "mirrorStatus": clean_cell_value(upload_job.get("status")) or "queued",
            "mirrorError": "",
            "mirrorJobId": upload_job_id,
            "yandexPath": clean_cell_value(upload_job.get("yandex_path")),
            "yandexFileUrl": "",
            "yandexFolderAlias": clean_cell_value(
                old_document.get("yandexFolderAlias")
            ),
        })
        documents[old_index] = new_document
        documents = normalize_documents_list(documents)
        target_item["documents"] = documents
        target_item["folderPath"] = (
            "/" + str(new_abs_path.parent.relative_to(BASE_DIR)).replace("\\", "/")
        )
        target_item["folderUrl"] = build_folder_view_url(
            dialog_id, config.key, item_id
        )
        first_document = documents[0] if documents else {}
        target_item["documentUrl"] = clean_cell_value(
            first_document.get("fileUrl")
        )
        target_item["documentName"] = clean_cell_value(
            first_document.get("name")
        )
        if config.is_active_group(target_item.get("group")):
            target_item["status"] = "Есть"
            target_item["priority"] = derive_indicator_from_status("Есть")
        data["items"][item_index] = target_item
        save_checklist(
            dialog_id,
            normalize_checklist_data(data, config.key),
            config.key,
        )
        checklist_saved = True
        try:
            old_local_path.unlink()
        except OSError:
            # The checklist already points to the new deterministic path.
            # A leftover old local copy is harmless and must not roll back a
            # committed external replacement.
            pass
    except Exception as exc:
        if not checklist_saved:
            if upload_job_id:
                cancel_upload_jobs_for_document(
                    dialog_id=dialog_id,
                    checklist_key=config.key,
                    item_id=item_id,
                    document_id=new_document_id,
                )
            if moved_new and new_abs_path.exists():
                new_abs_path.unlink()
            if archived_path and archived_path.exists():
                archived_path.unlink()
        if replacement_registered:
            try:
                mark_document_replacement_failed(
                    operation_id=replacement_operation_id,
                    error=str(exc),
                    stage="public_folder_replace_failed",
                )
            except Exception:
                pass
        raise

    enqueue_yandex_mirror_job(
        upload_job_id,
        source="public_folder_replace",
    )
    return {
        "documentId": new_document_id,
        "uploadJobId": upload_job_id,
        "replacementOperationId": replacement_operation_id,
        "fileName": final_name,
        "alreadyApplied": False,
    }


def _mark_operation_terminal(
    operation_id: str,
    *,
    status: str,
    stage: str,
    error: str = "",
    result: dict | None = None,
) -> dict:
    result = result or {}
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE public_folder_operations
            SET status = ?, stage = ?, error = ?,
                document_id = COALESCE(NULLIF(?, ''), document_id),
                upload_job_id = COALESCE(NULLIF(?, ''), upload_job_id),
                replacement_operation_id = COALESCE(
                    NULLIF(?, ''), replacement_operation_id
                ),
                file_name = COALESCE(NULLIF(?, ''), file_name),
                updated_at = ?, finished_at = ?
            WHERE operation_id = ? AND status = 'processing'
            """,
            (
                status,
                stage,
                clean_cell_value(error),
                clean_cell_value(result.get("documentId")),
                clean_cell_value(result.get("uploadJobId")),
                clean_cell_value(result.get("replacementOperationId")),
                clean_cell_value(result.get("fileName")),
                now,
                now,
                clean_cell_value(operation_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    record = get_public_folder_operation(operation_id) or {}
    if status in PUBLIC_OPERATION_TERMINAL_STATUSES:
        _cleanup_operation_staging(record)
    return record


def process_public_folder_operation(operation_id: str) -> dict:
    claimed = _claim_public_folder_operation(operation_id)
    if not claimed:
        return get_public_folder_operation(operation_id) or {}

    with _item_lock(claimed):
        try:
            if claimed.get("operation_type") == PUBLIC_OPERATION_UPLOAD:
                result = _apply_public_upload(claimed)
            elif claimed.get("operation_type") == PUBLIC_OPERATION_REPLACE:
                result = _apply_public_replacement(claimed)
            else:
                raise PublicFolderOperationError(
                    "unknown public folder operation"
                )
        except PublicFolderOperationConflict as exc:
            record = _mark_operation_terminal(
                operation_id,
                status="conflict",
                stage="business_conflict",
                error=str(exc),
            )
            write_debug_log("public_folder_operation_conflict", {
                "operationId": operation_id,
                "operationType": claimed.get("operation_type"),
                "dialogId": claimed.get("dialog_id"),
                "checklistKey": claimed.get("checklist_key"),
                "itemId": claimed.get("item_id"),
                "error": str(exc),
            })
            return record
        except Exception as exc:
            record = _mark_operation_terminal(
                operation_id,
                status="error",
                stage="apply_failed",
                error=str(exc),
            )
            write_debug_log("public_folder_operation_failed", {
                "operationId": operation_id,
                "operationType": claimed.get("operation_type"),
                "dialogId": claimed.get("dialog_id"),
                "checklistKey": claimed.get("checklist_key"),
                "itemId": claimed.get("item_id"),
                "error": str(exc),
                "traceback": traceback.format_exc()[-6000:],
            })
            return record

        record = _mark_operation_terminal(
            operation_id,
            status="completed",
            stage="local_commit_completed",
            result=result,
        )
        write_debug_log("public_folder_operation_completed", {
            "operationId": operation_id,
            "operationType": claimed.get("operation_type"),
            "dialogId": claimed.get("dialog_id"),
            "checklistKey": claimed.get("checklist_key"),
            "itemId": claimed.get("item_id"),
            "documentId": result.get("documentId"),
            "uploadJobId": result.get("uploadJobId"),
        })
        return record


def _recover_interrupted_public_folder_operations() -> int:
    ensure_public_folder_operation_schema()
    now = utc_now_iso()
    conn = get_conn()
    try:
        interrupted_receiving = conn.execute(
            """
            SELECT * FROM public_folder_operations
            WHERE status = 'receiving'
            """
        ).fetchall()
        receiving_cur = conn.execute(
            """
            UPDATE public_folder_operations
            SET status = 'error', stage = 'receive_interrupted',
                error = 'Передача файла была прервана перезапуском приложения',
                updated_at = ?, finished_at = ?
            WHERE status = 'receiving'
            """,
            (now, now),
        )
        processing_cur = conn.execute(
            """
            UPDATE public_folder_operations
            SET status = 'queued', stage = 'recovered_after_restart',
                error = '', updated_at = ?, started_at = ''
            WHERE status = 'processing'
            """,
            (now,),
        )
        conn.commit()
        recovered = int(processing_cur.rowcount or 0)
        interrupted = int(receiving_cur.rowcount or 0)
    finally:
        conn.close()
    for row in interrupted_receiving:
        _cleanup_operation_staging(dict(row))
    return recovered + interrupted


def _queued_operation_ids(limit: int = 50) -> list[str]:
    ensure_public_folder_operation_schema()
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT operation_id
            FROM public_folder_operations
            WHERE status = 'queued'
            ORDER BY created_at ASC, operation_id ASC
            LIMIT ?
            """,
            (max(1, int(limit or 50)),),
        ).fetchall()
    finally:
        conn.close()
    return [row["operation_id"] for row in rows]


def process_pending_public_folder_operations(limit: int = 50) -> dict:
    processed = 0
    completed = 0
    waiting = 0
    failed = 0
    for operation_id in _queued_operation_ids(limit):
        before = get_public_folder_operation(operation_id) or {}
        result = process_public_folder_operation(operation_id)
        status = clean_cell_value(result.get("status"))
        if status == "queued":
            if result.get("stage") == "waiting_for_edit_session":
                waiting += 1
            continue
        if status != clean_cell_value(before.get("status")):
            processed += 1
        if status == "completed":
            completed += 1
        elif status in {"conflict", "error"}:
            failed += 1
    return {
        "processed": processed,
        "completed": completed,
        "waiting": waiting,
        "failed": failed,
    }


def _worker_loop() -> None:
    while True:
        try:
            process_pending_public_folder_operations()
        except Exception as exc:
            write_debug_log("public_folder_worker_iteration_failed", {
                "error": str(exc),
                "traceback": traceback.format_exc()[-6000:],
            })
        _worker_event.wait(timeout=2.0)
        _worker_event.clear()


def start_public_folder_operation_worker() -> dict:
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return {"ok": True, "started": False}
        recovered = _recover_interrupted_public_folder_operations()
        worker = threading.Thread(
            target=_worker_loop,
            name="public-folder-operations",
            daemon=True,
        )
        worker.start()
        _worker_started = True
        _worker_event.set()
        return {
            "ok": True,
            "started": True,
            "recovered": recovered,
        }


def wake_public_folder_operation_worker() -> None:
    _worker_event.set()
