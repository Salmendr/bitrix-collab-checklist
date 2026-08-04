import html
import json
from logging import config
import mimetypes
import uuid
from datetime import datetime, timezone, timedelta
import time
import traceback
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, Request, UploadFile, File, Form
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from app.settings import (
    BASE_DIR,
    APP_BASE_PATH,
    UPLOAD_ROOT,
)

from app.logging_utils import write_debug_log
from app.checklists.config import get_checklist_config

from app.checklists.utils import (
    can_preview_in_browser,
    clean_cell_value,
    format_file_size,
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.checklists.permissions import (
    can_user_delete_files,
    get_file_delete_allowed_user_ids,
    get_archive_permanent_delete_admin_user_ids,
)

from app.checklists.storage import (
    get_checklist,
    get_item_yandex_folder,
    save_checklist,
)

from app.checklists.normalization import (
    normalize_checklist_data,
    derive_indicator_from_status,
)

from app.checklists.documents import (
    build_upload_rel_path,
    build_document_view_url,
    build_folder_view_url,
    get_upload_file_path_from_url,
    normalize_document_record,
    normalize_documents_list,
    migrate_legacy_document_fields,
    remove_item_document_file,
    archive_current_document_local_file,
    build_detached_archive_series,
    merge_detached_archive_series,
)

from app.checklists.upload_jobs import (
    create_yandex_upload_job,
    create_yandex_delete_job,
    cancel_upload_jobs_for_document,
    get_upload_job,
    get_latest_document_job,
    public_job_payload,
    resolve_document_mirror_status,
)

from app.checklists.document_replacements import (
    create_document_replacement,
    get_document_replacement,
    mark_document_replacement_failed,
    public_document_replacement_payload,
)

from app.checklists.archive_ui import (
    build_archive_series_rows_html,
    build_detached_archive_rows_html,
)

from app.checklists.yandex_mirror_queue import (
    enqueue_yandex_mirror_job,
    get_yandex_mirror_queue_state,
)

from app.checklists.edit_session_documents import (
    transactional_remove_document,
    transactional_replace_document,
    transactional_upload_document,
)
from app.checklists.edit_sessions import (
    EditSessionConflictError,
    EditSessionNotFoundError,
    EditSessionPermissionError,
)

from app.ui.shell import normalize_base_path
from app.ui.template_engine import render_ui_template

from app.yandex_disk.client import (
    is_yandex_disk_enabled,
    yandex_disk_delete_path,
)


from app.checklists.document_assignment_history import (
    get_assignment_history_count_map,
)

router = APIRouter()


def document_edit_session_error_response(exc: Exception) -> JSONResponse:
    if isinstance(exc, EditSessionNotFoundError):
        status_code = 404
    elif isinstance(exc, EditSessionPermissionError):
        status_code = 403
    elif isinstance(exc, EditSessionConflictError):
        status_code = 409
    elif isinstance(exc, KeyError):
        status_code = 404
    elif isinstance(exc, (ValueError, FileNotFoundError)):
        status_code = 400
    else:
        status_code = 500

    return JSONResponse(
        {
            "ok": False,
            "error": str(exc).strip("'"),
            "editSessionError": True,
        },
        status_code=status_code,
    )


def parse_bool_form_value(value) -> bool:
    return str(value or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "да",
    }


def enforce_required_edit_session(
    session_id: str,
    require_edit_session,
) -> str:
    normalized_session_id = clean_cell_value(session_id)

    if (
        parse_bool_form_value(require_edit_session)
        and not normalized_session_id
    ):
        raise EditSessionConflictError(
            "Активная сессия редактирования не готова"
        )

    return normalized_session_id


def get_document_replacement_guard(
    document: dict,
    force_replace: bool = False,
) -> dict:
    mirror_resolution = resolve_document_mirror_status(
        document
    )
    mirror_status = clean_cell_value(
        mirror_resolution.get("status")
    ).lower()

    if mirror_status in {"queued", "running"}:
        return {
            "blocked": True,
            "requiresForceReplace": False,
            "mirrorStatus": mirror_status,
            "error": (
                "Файл ещё синхронизируется с Яндекс.Диском. "
                "Дождитесь завершения синхронизации."
            ),
        }

    if mirror_status == "error" and not force_replace:
        return {
            "blocked": True,
            "requiresForceReplace": True,
            "mirrorStatus": mirror_status,
            "error": (
                "У текущего файла есть ошибка синхронизации. "
                "Для замены требуется подтверждение."
            ),
        }

    return {
        "blocked": False,
        "requiresForceReplace": False,
        "mirrorStatus": mirror_status,
        "error": "",
    }



async def save_upload_file_stream(
    file: UploadFile,
    abs_path: Path,
    upload_id: str = "",
    log_payload: dict | None = None,
) -> int:
    total_size = 0
    chunk_size = 1024 * 1024
    next_progress_log = 5 * 1024 * 1024
    started_at = time.monotonic()

    base_payload = dict(log_payload or {})
    base_payload["uploadId"] = upload_id
    base_payload["absPath"] = str(abs_path)
    base_payload["chunkSize"] = chunk_size

    write_debug_log("upload_stream_write_started", base_payload)

    try:
        with open(abs_path, "wb") as f:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break

                total_size += len(chunk)
                f.write(chunk)

                if total_size >= next_progress_log:
                    write_debug_log("upload_stream_write_progress", {
                        **base_payload,
                        "writtenBytes": total_size,
                        "durationMs": int((time.monotonic() - started_at) * 1000),
                    })
                    next_progress_log += 5 * 1024 * 1024

        write_debug_log("upload_stream_write_completed", {
            **base_payload,
            "writtenBytes": total_size,
            "durationMs": int((time.monotonic() - started_at) * 1000),
            "fileExists": abs_path.exists(),
            "fileSizeOnDisk": abs_path.stat().st_size if abs_path.exists() else 0,
        })

        return total_size

    except Exception as exc:
        write_debug_log("upload_stream_write_failed", {
            **base_payload,
            "writtenBytes": total_size,
            "durationMs": int((time.monotonic() - started_at) * 1000),
            "error": str(exc),
            "errorType": type(exc).__name__,
            "traceback": traceback.format_exc()[-4000:],
            "fileExists": abs_path.exists(),
            "fileSizeOnDisk": abs_path.stat().st_size if abs_path.exists() else 0,
        })
        raise


def format_document_uploaded_at(value: str) -> str:
    value = clean_cell_value(value)

    if not value:
        return "—"

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%d.%m.%Y %H:%M")
    except Exception:
        if "T" in value:
            return value.replace("T", " ")[:16]
        return value


def format_document_uploaded_at(value: str) -> str:
    value = clean_cell_value(value)

    if not value:
        return "—"

    utc_plus_10 = timezone(timedelta(hours=10))

    try:
        raw_value = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(raw_value)

        # Старые значения могли быть сохранены без timezone.
        # Для единого отображения считаем такие значения UTC и переводим в UTC+10.
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.astimezone(utc_plus_10).strftime("%d.%m.%Y %H:%M")
    except Exception:
        if "T" in value:
            return value.replace("T", " ")[:16]
        return value


@router.post("/api/checklist/upload-document")
async def api_checklist_upload_document(
    dialogId: str = Form(...),
    itemId: str = Form(...),
    file: UploadFile = File(...),
    checklistKey: str = Form("id"),
    itemGroup: str = Form(""),
    actingUserId: str = Form(""),
    actingUserName: str = Form(""),
    sessionId: str = Form(""),
    requireEditSession: str = Form(""),
):
    upload_id = uuid.uuid4().hex
    started_at = time.monotonic()

    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    item_id = str(itemId or "").strip()
    uploaded_name = Path(file.filename or "file.bin").name
    uploaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    acting_user_id = clean_cell_value(actingUserId)
    acting_user_name = clean_cell_value(actingUserName) or "Пользователь"

    try:
        session_id = enforce_required_edit_session(
            sessionId,
            requireEditSession,
        )
    except Exception as exc:
        return document_edit_session_error_response(exc)

    log_base = {
        "uploadId": upload_id,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "itemId": item_id,
        "itemGroup": str(itemGroup or ""),
        "fileName": uploaded_name,
        "contentType": clean_cell_value(file.content_type),
        "uploadedById": acting_user_id,
        "uploadedByName": acting_user_name,
        "sessionId": session_id,
    }

    write_debug_log("upload_document_endpoint_entered", log_base)

    if session_id:
        try:
            result = await transactional_upload_document(
                session_id=session_id,
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                item_group=int(str(itemGroup or "0").strip() or 0),
                file=file,
                acting_user_id=acting_user_id,
                acting_user_name=acting_user_name,
            )
            return JSONResponse(result)
        except Exception as exc:
            write_debug_log("transactional_upload_document_failed", {
                **log_base,
                "error": str(exc),
                "traceback": traceback.format_exc()[-6000:],
            })
            return document_edit_session_error_response(exc)

    try:
        config = get_checklist_config(checklist_key)
        item_group = int(str(itemGroup or "0").strip() or 0)

        if not dialog_id:
            write_debug_log("upload_document_rejected", {
                **log_base,
                "reason": "dialogId is required",
            })
            return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

        if not item_id:
            write_debug_log("upload_document_rejected", {
                **log_base,
                "reason": "itemId is required",
            })
            return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

        write_debug_log("upload_document_checklist_load_started", log_base)

        data = get_checklist(dialog_id, config.key)
        items = data.get("items", []) or []

        write_debug_log("upload_document_checklist_loaded", {
            **log_base,
            "itemsCount": len(items),
        })

        target_item = None
        for index, item in enumerate(items):
            if str(item.get("id") or "") == item_id:
                target_item = migrate_legacy_document_fields(item)
                items[index] = target_item
                break

        if not target_item:
            write_debug_log("upload_document_rejected", {
                **log_base,
                "reason": "item not found",
            })
            return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

        write_debug_log("upload_document_target_item_found", {
            **log_base,
            "itemName": clean_cell_value(target_item.get("name")),
            "itemGroup": int(target_item.get("group") or 0),
            "existingDocumentsCount": len(normalize_documents_list(target_item.get("documents"))),
        })

        rel_path = build_upload_rel_path(dialog_id, item_id, uploaded_name)
        abs_path = UPLOAD_ROOT / rel_path

        write_debug_log("upload_document_local_path_built", {
            **log_base,
            "relPath": rel_path,
            "absPath": str(abs_path),
            "parent": str(abs_path.parent),
        })

        abs_path.parent.mkdir(parents=True, exist_ok=True)

        write_debug_log("upload_document_local_dir_ready", {
            **log_base,
            "parent": str(abs_path.parent),
            "parentExists": abs_path.parent.exists(),
        })

        file_size = await save_upload_file_stream(
            file=file,
            abs_path=abs_path,
            upload_id=upload_id,
            log_payload={
                **log_base,
                "relPath": rel_path,
            },
        )

        file_url = "/uploads/" + rel_path.replace("\\", "/")
        document_id = uuid.uuid4().hex
        document_view_url = build_document_view_url(dialog_id, config.key, item_id, document_id)
        folder_view_url = build_folder_view_url(dialog_id, config.key, item_id)

        try:
            folder_path = "/" + str(abs_path.parent.relative_to(BASE_DIR)).replace("\\", "/")
        except Exception:
            folder_path = file_url.rsplit("/", 1)[0]

        write_debug_log("upload_document_record_build_started", {
            **log_base,
            "documentId": document_id,
            "fileSize": file_size,
            "fileUrl": file_url,
            "folderPath": folder_path,
        })

        upload_job = create_yandex_upload_job(
            dialog_id=dialog_id,
            checklist_key=config.key,
            item_id=item_id,
            document_id=document_id,
            local_path=str(abs_path),
            file_name=uploaded_name,
            file_size=file_size,
        )

        job_id = clean_cell_value(upload_job.get("job_id") or upload_job.get("jobId"))

        write_debug_log("upload_document_job_created", {
            **log_base,
            "documentId": document_id,
            "jobId": job_id,
            "job": upload_job,
        })

        document_record = normalize_document_record({
            "id": document_id,
            "name": uploaded_name,
            "path": file_url,
            "fileUrl": file_url,
            "previewUrl": document_view_url,
            "size": file_size,
            "modifiedAt": uploaded_at,
            "uploadedAt": uploaded_at,
            "uploadedById": acting_user_id,
            "uploadedByName": acting_user_name,
            "source": "local",

            "mirrorStatus": "queued",
            "mirrorError": "",
            "mirrorJobId": job_id,
            "yandexPath": "",
            "yandexFileUrl": "",
            "yandexFolderAlias": "",
        })

        existing_documents = normalize_documents_list(target_item.get("documents"))
        existing_documents.append(document_record)
        normalized_documents = normalize_documents_list(existing_documents)

        target_item["documents"] = normalized_documents
        target_item["folderPath"] = folder_path
        target_item["folderUrl"] = folder_view_url if normalized_documents else ""

        first_doc = normalized_documents[0] if normalized_documents else {}
        target_item["documentUrl"] = clean_cell_value(first_doc.get("fileUrl"))
        target_item["documentName"] = clean_cell_value(first_doc.get("name"))

        actual_group = int(target_item.get("group") or item_group or 0)
        if config.is_active_group(actual_group):
            target_item["status"] = "Есть"
            target_item["priority"] = derive_indicator_from_status("Есть")

        data["items"] = items

        write_debug_log("upload_document_checklist_save_started", {
            **log_base,
            "documentId": document_id,
            "jobId": job_id,
            "documentsCount": len(normalized_documents),
        })

        data = normalize_checklist_data(data, config.key)
        save_checklist(dialog_id, data, config.key)

        write_debug_log("upload_document_checklist_saved", {
            **log_base,
            "documentId": document_id,
            "jobId": job_id,
            "progressPercent": data.get("progressPercent", 0),
        })

        enqueue_result = enqueue_yandex_mirror_job(
            job_id,
            source="upload_document",
        ) if job_id else {
            "ok": False,
            "queued": False,
            "error": "upload job id is empty",
        }

        write_debug_log("upload_document_mirror_enqueue_completed", {
            **log_base,
            "documentId": document_id,
            "jobId": job_id,
            "enqueueResult": enqueue_result,
        })

        updated_item = None
        for item in data.get("items", []):
            if str(item.get("id") or "") == item_id:
                updated_item = item
                break

        if not updated_item:
            write_debug_log("upload_document_failed", {
                **log_base,
                "documentId": document_id,
                "jobId": job_id,
                "reason": "updated item not found",
            })
            return JSONResponse({"ok": False, "error": "updated item not found"}, status_code=500)

        response_payload = {
            "ok": True,
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "item": updated_item,
            "document": document_record,
            "uploadJobId": job_id,
            "uploadJob": public_job_payload(get_upload_job(job_id)) if job_id else {},
            "yandexMirrorQueued": bool(enqueue_result.get("queued")),
            "yandexMirrorQueue": enqueue_result,
            "progressPercent": data.get("progressPercent", 0),
        }

        write_debug_log("upload_document_completed", {
            **log_base,
            "documentId": document_id,
            "jobId": job_id,
            "fileSize": file_size,
            "durationMs": int((time.monotonic() - started_at) * 1000),
            "yandexMirrorQueued": bool(enqueue_result.get("queued")),
        })

        return JSONResponse(response_payload)

    except Exception as exc:
        write_debug_log("upload_document_exception", {
            **log_base,
            "durationMs": int((time.monotonic() - started_at) * 1000),
            "error": str(exc),
            "errorType": type(exc).__name__,
            "traceback": traceback.format_exc()[-6000:],
        })

        return JSONResponse({
            "ok": False,
            "error": "upload document failed",
            "details": str(exc),
            "uploadId": upload_id,
        }, status_code=500)


@router.post("/api/checklist/replace-document")
async def api_checklist_replace_document(
    dialogId: str = Form(...),
    itemId: str = Form(...),
    documentId: str = Form(...),
    file: UploadFile = File(...),
    checklistKey: str = Form("id"),
    actingUserId: str = Form(""),
    actingUserName: str = Form(""),
    forceReplace: str = Form(""),
    sessionId: str = Form(""),
    requireEditSession: str = Form(""),
):
    operation_id = uuid.uuid4().hex
    started_at = time.monotonic()

    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    item_id = clean_cell_value(itemId)
    document_id = clean_cell_value(documentId)

    acting_user_id = clean_cell_value(actingUserId)
    acting_user_name = (
        clean_cell_value(actingUserName)
        or "Пользователь"
    )

    force_replace = parse_bool_form_value(forceReplace)

    try:
        session_id = enforce_required_edit_session(
            sessionId,
            requireEditSession,
        )
    except Exception as exc:
        return document_edit_session_error_response(exc)

    uploaded_name = Path(
        file.filename or "file.bin"
    ).name

    log_base = {
        "operationId": operation_id,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "itemId": item_id,
        "oldDocumentId": document_id,
        "newFileName": uploaded_name,
        "actingUserId": acting_user_id,
        "actingUserName": acting_user_name,
        "forceReplace": force_replace,
        "sessionId": session_id,
    }

    write_debug_log(
        "replace_document_endpoint_entered",
        log_base,
    )

    if not dialog_id:
        return JSONResponse(
            {"ok": False, "error": "dialogId is required"},
            status_code=400,
        )

    if not item_id:
        return JSONResponse(
            {"ok": False, "error": "itemId is required"},
            status_code=400,
        )

    if not document_id:
        return JSONResponse(
            {"ok": False, "error": "documentId is required"},
            status_code=400,
        )

    if not uploaded_name:
        return JSONResponse(
            {"ok": False, "error": "file name is required"},
            status_code=400,
        )

    if session_id:
        try:
            result = await transactional_replace_document(
                session_id=session_id,
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                document_id=document_id,
                file=file,
                acting_user_id=acting_user_id,
                acting_user_name=acting_user_name,
                force_replace=force_replace,
            )
            return JSONResponse(result)
        except Exception as exc:
            write_debug_log("transactional_replace_document_failed", {
                **log_base,
                "error": str(exc),
                "traceback": traceback.format_exc()[-6000:],
            })
            return document_edit_session_error_response(exc)

    config = get_checklist_config(checklist_key)
    data = get_checklist(dialog_id, config.key)
    items = data.get("items", []) or []

    target_item = None
    target_item_index = -1

    for index, raw_item in enumerate(items):
        if str(raw_item.get("id") or "") != item_id:
            continue

        target_item = migrate_legacy_document_fields(
            raw_item
        )
        target_item_index = index
        items[index] = target_item
        break

    if not target_item:
        return JSONResponse(
            {"ok": False, "error": "item not found"},
            status_code=404,
        )

    documents = normalize_documents_list(
        target_item.get("documents")
    )
    assignment_history_counts = get_assignment_history_count_map(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_id=item_id,
    )
    for document in documents:
        series_id = (
            clean_cell_value(document.get("seriesId"))
            or clean_cell_value(document.get("id"))
        )
        document["assignmentHistoryCount"] = int(
            assignment_history_counts.get(series_id, 0)
        )

    old_document = None
    old_document_index = -1

    for index, document in enumerate(documents):
        if str(document.get("id") or "") == document_id:
            old_document = normalize_document_record(
                document
            )
            old_document_index = index
            break

    if not old_document:
        return JSONResponse(
            {"ok": False, "error": "document not found"},
            status_code=404,
        )

    guard = get_document_replacement_guard(
        old_document,
        force_replace=force_replace,
    )

    if guard.get("blocked"):
        return JSONResponse({
            "ok": False,
            "error": guard.get("error"),
            "replacementBlocked": True,
            "requiresForceReplace": bool(
                guard.get("requiresForceReplace")
            ),
            "mirrorStatus": guard.get("mirrorStatus"),
            "documentId": document_id,
        }, status_code=409)

    old_file_url = (
        clean_cell_value(old_document.get("fileUrl"))
        or clean_cell_value(old_document.get("path"))
    )

    old_local_path = get_upload_file_path_from_url(
        old_file_url
    )

    if not old_local_path:
        return JSONResponse({
            "ok": False,
            "error": (
                "Текущий файл не расположен "
                "в локальном хранилище"
            ),
        }, status_code=409)

    if not old_local_path.exists():
        return JSONResponse({
            "ok": False,
            "error": "Текущий локальный файл не найден",
            "localPath": str(old_local_path),
        }, status_code=404)

    new_rel_path = build_upload_rel_path(
        dialog_id,
        item_id,
        uploaded_name,
    )

    new_abs_path = UPLOAD_ROOT / new_rel_path

    temp_abs_path = new_abs_path.with_name(
        f".{new_abs_path.name}."
        f"{operation_id}.replace_tmp"
    )

    new_abs_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    try:
        file_size = await save_upload_file_stream(
            file=file,
            abs_path=temp_abs_path,
            upload_id=operation_id,
            log_payload={
                **log_base,
                "replaceTemporaryPath": str(
                    temp_abs_path
                ),
            },
        )
    except Exception as exc:
        if temp_abs_path.exists():
            temp_abs_path.unlink()

        write_debug_log(
            "replace_document_new_file_write_failed",
            {
                **log_base,
                "error": str(exc),
                "traceback": traceback.format_exc()[-4000:],
            },
        )

        return JSONResponse({
            "ok": False,
            "error": "Не удалось сохранить новый файл",
            "details": str(exc),
        }, status_code=500)

    if file_size <= 0:
        if temp_abs_path.exists():
            temp_abs_path.unlink()

        return JSONResponse({
            "ok": False,
            "error": "Загружен пустой файл",
        }, status_code=400)

    replacement_at = datetime.now(
        timezone.utc
    ).isoformat(timespec="seconds")

    new_document_id = uuid.uuid4().hex
    series_id = (
        clean_cell_value(old_document.get("seriesId"))
        or clean_cell_value(old_document.get("id"))
        or uuid.uuid4().hex
    )

    archived_version = None
    new_upload_job_id = ""
    checklist_saved = False
    replacement_registered = False

    try:
        # 1. Старый локальный файл уходит в архив.
        archived_version = (
            archive_current_document_local_file(
                dialog_id=dialog_id,
                item_id=item_id,
                document=old_document,
                archived_by_id=acting_user_id,
                archived_by_name=acting_user_name,
                archived_at=replacement_at,
            )
        )

        # 2. Новый временный файл становится текущим.
        temp_abs_path.replace(new_abs_path)

        new_file_url = (
            "/uploads/"
            + new_rel_path.replace("\\", "/")
        )

        new_document_view_url = build_document_view_url(
            dialog_id,
            config.key,
            item_id,
            new_document_id,
        )

        # 3. Создаём upload-job новой версии.
        upload_job = create_yandex_upload_job(
            dialog_id=dialog_id,
            checklist_key=config.key,
            item_id=item_id,
            document_id=new_document_id,
            local_path=str(new_abs_path),
            file_name=uploaded_name,
            file_size=file_size,
        )

        new_upload_job_id = clean_cell_value(
            upload_job.get("job_id")
            or upload_job.get("jobId")
        )

        if not new_upload_job_id:
            raise RuntimeError(
                "Не удалось создать upload-job "
                "для новой версии"
            )

        create_document_replacement(
            operation_id=operation_id,
            dialog_id=dialog_id,
            checklist_key=config.key,
            item_id=item_id,
            series_id=series_id,
            archive_version_id=clean_cell_value(
                archived_version.get("id")
            ),
            old_document_id=document_id,
            new_document_id=new_document_id,
            new_upload_job_id=new_upload_job_id,
            old_file_name=clean_cell_value(
                old_document.get("name")
            ),
            new_file_name=uploaded_name,
            old_yandex_path=clean_cell_value(
                old_document.get("yandexPath")
            ),
        )

        replacement_registered = True

        previous_archive_versions = list(
            old_document.get("archiveVersions")
            or []
        )

        new_document = normalize_document_record({
            "id": new_document_id,
            "seriesId": series_id,
            "name": uploaded_name,
            "path": new_file_url,
            "fileUrl": new_file_url,
            "previewUrl": new_document_view_url,
            "size": file_size,
            "modifiedAt": replacement_at,
            "uploadedAt": replacement_at,
            "uploadedById": acting_user_id,
            "uploadedByName": acting_user_name,
            "source": "local",

            "archiveVersions": (
                previous_archive_versions
                + [archived_version]
            ),

            "lastReplacedAt": replacement_at,
            "lastReplacedById": acting_user_id,
            "lastReplacedByName": acting_user_name,
            "replacementOperationId": operation_id,

            "mirrorStatus": "queued",
            "mirrorError": "",
            "mirrorJobId": new_upload_job_id,
            "yandexPath": "",
            "yandexFileUrl": "",
            "yandexFolderAlias": clean_cell_value(
                old_document.get(
                    "yandexFolderAlias"
                )
            ),
        })

        next_documents = list(documents)
        next_documents[old_document_index] = (
            new_document
        )

        normalized_documents = (
            normalize_documents_list(next_documents)
        )

        target_item["documents"] = (
            normalized_documents
        )

        target_item["folderPath"] = (
            "/" + str(
                new_abs_path.parent.relative_to(
                    BASE_DIR
                )
            ).replace("\\", "/")
        )

        target_item["folderUrl"] = (
            build_folder_view_url(
                dialog_id,
                config.key,
                item_id,
            )
        )

        first_document = (
            normalized_documents[0]
            if normalized_documents
            else {}
        )

        target_item["documentUrl"] = (
            clean_cell_value(
                first_document.get("fileUrl")
            )
        )

        target_item["documentName"] = (
            clean_cell_value(
                first_document.get("name")
            )
        )

        if config.is_active_group(
            target_item.get("group")
        ):
            target_item["status"] = "Есть"
            target_item["priority"] = (
                derive_indicator_from_status("Есть")
            )

        items[target_item_index] = target_item
        data["items"] = items

        data = normalize_checklist_data(
            data,
            config.key,
        )

        save_checklist(
            dialog_id,
            data,
            config.key,
        )

        checklist_saved = True

    except Exception as exc:
        # Rollback выполняем только пока новое состояние
        # не сохранено в checklist JSON.
        if not checklist_saved:
            if new_upload_job_id:
                cancel_upload_jobs_for_document(
                    dialog_id=dialog_id,
                    checklist_key=config.key,
                    item_id=item_id,
                    document_id=new_document_id,
                )

            if new_abs_path.exists():
                try:
                    new_abs_path.unlink()
                except Exception:
                    pass

            if archived_version:
                archived_file_path = (
                    get_upload_file_path_from_url(
                        archived_version.get(
                            "fileUrl"
                        )
                    )
                )

                if (
                    archived_file_path
                    and archived_file_path.exists()
                    and not old_local_path.exists()
                ):
                    old_local_path.parent.mkdir(
                        parents=True,
                        exist_ok=True,
                    )

                    archived_file_path.replace(
                        old_local_path
                    )

        if temp_abs_path.exists():
            try:
                temp_abs_path.unlink()
            except Exception:
                pass

        if replacement_registered:
            try:
                mark_document_replacement_failed(
                    operation_id=operation_id,
                    error=str(exc),
                    stage=(
                        "replace_route_failed_after_save"
                        if checklist_saved
                        else "replace_route_rollback"
                    ),
                )
            except Exception as replacement_exc:
                write_debug_log(
                    "replace_document_transaction_status_failed",
                    {
                        **log_base,
                        "error": str(
                            replacement_exc
                        ),
                    },
                )

        write_debug_log(
            "replace_document_transaction_failed",
            {
                **log_base,
                "newDocumentId": new_document_id,
                "newUploadJobId": new_upload_job_id,
                "checklistSaved": checklist_saved,
                "error": str(exc),
                "traceback": traceback.format_exc()[-6000:],
            },
        )

        return JSONResponse({
            "ok": False,
            "error": "Замена файла не выполнена",
            "details": str(exc),
            "operationId": operation_id,
        }, status_code=500)

    finally:
        if temp_abs_path.exists():
            try:
                temp_abs_path.unlink()
            except Exception:
                pass

    # После сохранения ставим новую версию
    # в существующую очередь.
    enqueue_result = enqueue_yandex_mirror_job(
        new_upload_job_id,
        source="replace_document",
    )

    updated_item = None
    updated_document = None

    for item in data.get("items", []):
        if str(item.get("id") or "") != item_id:
            continue

        updated_item = item

        for document in normalize_documents_list(
            item.get("documents")
        ):
            if str(document.get("id") or "") == (
                new_document_id
            ):
                updated_document = document
                break

        break

    write_debug_log(
        "replace_document_completed",
        {
            **log_base,
            "newDocumentId": new_document_id,
            "seriesId": series_id,
            "archiveVersionId": clean_cell_value(
                archived_version.get("id")
                if archived_version
                else ""
            ),
            "archiveVersion": int(
                archived_version.get("version")
                if archived_version
                else 0
            ),
            "newUploadJobId": new_upload_job_id,
            "yandexMirrorQueued": bool(
                enqueue_result.get("queued")
            ),
            "durationMs": int(
                (time.monotonic() - started_at)
                * 1000
            ),
        },
    )

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "checklistKey": config.key,
        "item": updated_item,
        "document": updated_document,
        "replacement": {
            "operationId": operation_id,
            "seriesId": series_id,
            "oldDocumentId": document_id,
            "newDocumentId": new_document_id,
            "oldFileName": clean_cell_value(
                old_document.get("name")
            ),
            "newFileName": uploaded_name,
            "archiveVersion": archived_version,
        },
        "uploadJobId": new_upload_job_id,
        "uploadJob": public_job_payload(
            get_upload_job(new_upload_job_id)
        ),
        "yandexMirrorQueued": bool(
            enqueue_result.get("queued")
        ),
        "yandexMirrorQueue": enqueue_result,
        "oldYandexDeleteDeferred": bool(
            clean_cell_value(
                old_document.get("yandexPath")
            )
        ),
        "replacementTransaction": (
            public_document_replacement_payload(
                get_document_replacement(
                    operation_id
                )
            )
        ),
        "progressPercent": data.get(
            "progressPercent",
            0,
        ),
    })


@router.post("/api/checklist/remove-document")
async def api_checklist_remove_document(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    config = get_checklist_config(checklist_key)

    item_id = str(payload.get("itemId") or "").strip()
    document_id = clean_cell_value(payload.get("documentId"))
    document_url = clean_cell_value(payload.get("documentUrl"))
    preserve_status = bool(payload.get("preserveStatus"))
    acting_user_id = clean_cell_value(payload.get("actingUserId"))
    acting_user_name = clean_cell_value(payload.get("actingUserName")) or "Пользователь"

    try:
        session_id = enforce_required_edit_session(
            payload.get("sessionId"),
            payload.get("requireEditSession"),
        )
    except Exception as exc:
        return document_edit_session_error_response(exc)

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    if not can_user_delete_files(acting_user_id):
        return JSONResponse({
            "ok": False,
            "error": "У вас недостаточно прав на удаление файлов"
        }, status_code=403)

    if session_id:
        try:
            result = transactional_remove_document(
                session_id=session_id,
                dialog_id=dialog_id,
                checklist_key=config.key,
                item_id=item_id,
                document_id=document_id,
                document_url=document_url,
                preserve_status=preserve_status,
                acting_user_id=acting_user_id,
                acting_user_name=acting_user_name,
            )
            return JSONResponse(result)
        except Exception as exc:
            write_debug_log("transactional_remove_document_failed", {
                "sessionId": session_id,
                "dialogId": dialog_id,
                "checklistKey": config.key,
                "itemId": item_id,
                "documentId": document_id,
                "error": str(exc),
                "traceback": traceback.format_exc()[-6000:],
            })
            return document_edit_session_error_response(exc)

    data = get_checklist(dialog_id, config.key)
    items = data.get("items", []) or []

    target_item = None
    for index, item in enumerate(items):
        if str(item.get("id") or "") == item_id:
            target_item = migrate_legacy_document_fields(item)
            items[index] = target_item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

    documents = normalize_documents_list(target_item.get("documents"))

    doc_to_remove = None

    if document_id:
        for doc in documents:
            if str(doc.get("id") or "") == document_id:
                doc_to_remove = doc
                break

    if not doc_to_remove and document_url:
        for doc in documents:
            doc_file_url = clean_cell_value(doc.get("fileUrl"))
            doc_preview_url = clean_cell_value(doc.get("previewUrl"))
            doc_path = clean_cell_value(doc.get("path"))
            if document_url in {doc_file_url, doc_preview_url, doc_path}:
                doc_to_remove = doc
                break

    if not doc_to_remove and documents:
        doc_to_remove = documents[0]

    delete_job_id = ""
    delete_enqueue_result = {}

    if doc_to_remove:
        removed_document_id = clean_cell_value(doc_to_remove.get("id"))

        cancel_upload_jobs_for_document(
            dialog_id=dialog_id,
            checklist_key=config.key,
            item_id=item_id,
            document_id=removed_document_id,
        )

        detached_series = build_detached_archive_series(
            doc_to_remove,
            removed_by_id=acting_user_id,
            removed_by_name=acting_user_name,
        )

        if detached_series:
            target_item["archivedDocumentSeries"] = (
                merge_detached_archive_series(
                    target_item.get(
                        "archivedDocumentSeries"
                    ),
                    [detached_series],
                )
            )

        local_document_url = (
            clean_cell_value(doc_to_remove.get("fileUrl"))
            or clean_cell_value(doc_to_remove.get("previewUrl"))
            or clean_cell_value(doc_to_remove.get("path"))
        )

        remove_item_document_file({
            "documentUrl": local_document_url
        })

        yandex_path = clean_cell_value(doc_to_remove.get("yandexPath"))
        if yandex_path:
            delete_job = create_yandex_delete_job(
                dialog_id=dialog_id,
                checklist_key=config.key,
                item_id=item_id,
                document_id=removed_document_id,
                file_name=clean_cell_value(doc_to_remove.get("name")),
                yandex_path=yandex_path,
            )

            delete_job_id = clean_cell_value(delete_job.get("job_id") or delete_job.get("jobId"))
            if delete_job_id:
                delete_enqueue_result = enqueue_yandex_mirror_job(
                    delete_job_id,
                    source="remove_document",
                )

    remaining_documents = []
    removed = False

    for doc in documents:
        same_id = document_id and str(doc.get("id") or "") == document_id
        same_url = document_url and document_url in {
            clean_cell_value(doc.get("fileUrl")),
            clean_cell_value(doc.get("previewUrl")),
            clean_cell_value(doc.get("path")),
        }

        if not removed and (
            same_id
            or same_url
            or (doc_to_remove and str(doc.get("id") or "") == str(doc_to_remove.get("id") or ""))
        ):
            removed = True
            continue

        remaining_documents.append(doc)

    normalized_documents = normalize_documents_list(remaining_documents)
    target_item["documents"] = normalized_documents

    first_doc = normalized_documents[0] if normalized_documents else {}
    target_item["documentUrl"] = clean_cell_value(first_doc.get("fileUrl"))
    target_item["documentName"] = clean_cell_value(first_doc.get("name"))

    if normalized_documents:
        first_file_url = clean_cell_value(first_doc.get("fileUrl"))
        target_item["folderPath"] = first_file_url.rsplit("/", 1)[0] if first_file_url.startswith("/") else ""
        target_item["folderUrl"] = build_folder_view_url(dialog_id, config.key, item_id)
    else:
        target_item["folderPath"] = ""
        target_item["folderUrl"] = ""
        target_item["documentUrl"] = ""
        target_item["documentName"] = ""

        if (
            config.reset_status_on_last_document_removed
            and config.is_active_group(target_item.get("group"))
            and not preserve_status
        ):
            target_item["status"] = ""
            target_item["priority"] = "white"

    data["items"] = items
    data = normalize_checklist_data(data, config.key)
    save_checklist(dialog_id, data, config.key)

    updated_item = None
    for item in data.get("items", []):
        if str(item.get("id") or "") == item_id:
            updated_item = item
            break

    if not updated_item:
        return JSONResponse({"ok": False, "error": "updated item not found"}, status_code=500)

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "checklistKey": config.key,
        "item": updated_item,
        "deleteJobId": delete_job_id,
        "yandexDeleteQueued": bool(delete_enqueue_result.get("queued")),
        "yandexDeleteQueue": delete_enqueue_result,
        "progressPercent": data.get("progressPercent", 0),
    })

@router.get("/api/checklist/item-yandex-folder")
def api_checklist_item_yandex_folder(
    dialogId: str = "",
    checklistKey: str = "id",
    itemId: str = "",
):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(
        checklistKey
    )
    item_id = clean_cell_value(itemId)

    if not dialog_id:
        return JSONResponse(
            {"ok": False, "error": "dialogId is required"},
            status_code=400,
        )

    if not item_id:
        return JSONResponse(
            {"ok": False, "error": "itemId is required"},
            status_code=400,
        )

    data = get_checklist(dialog_id, checklist_key)
    target_item = None

    for item in data.get("items", []) or []:
        if clean_cell_value(item.get("id")) == item_id:
            target_item = item
            break

    if not target_item:
        return JSONResponse(
            {"ok": False, "error": "item not found"},
            status_code=404,
        )

    stored_yandex_url = clean_cell_value(
        target_item.get("yandexFolderUrl")
    )
    stored_yandex_path = clean_cell_value(
        target_item.get("yandexFolderPath")
    )
    stored_yandex_status = clean_cell_value(
        target_item.get("yandexFolderStatus")
    )

    if stored_yandex_url or stored_yandex_path:
        return JSONResponse({
            "ok": True,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "itemId": item_id,
            "available": True,
            "url": stored_yandex_url,
            "path": stored_yandex_path,
            "status": stored_yandex_status or "ready",
            "yandexEnabled": bool(is_yandex_disk_enabled()),
        })

    try:
        folder_data = get_item_yandex_folder(
            dialog_id,
            checklist_key,
            clean_cell_value(target_item.get("name")),
            group_id=int(target_item.get("group") or 0),
        )
        folder = (folder_data or {}).get("folder") or {}
        folder_url = clean_cell_value(folder.get("url"))
        folder_path = clean_cell_value(folder.get("path"))

        return JSONResponse({
            "ok": True,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "itemId": item_id,
            "available": bool(folder_url or folder_path),
            "url": folder_url,
            "path": folder_path,
            "yandexEnabled": bool(is_yandex_disk_enabled()),
        })

    except Exception as exc:
        write_debug_log(
            "item_yandex_folder_lookup_failed",
            {
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "itemId": item_id,
                "itemName": clean_cell_value(
                    target_item.get("name")
                ),
                "error": str(exc),
            },
        )

        return JSONResponse({
            "ok": True,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "itemId": item_id,
            "available": False,
            "url": "",
            "path": "",
            "yandexEnabled": bool(is_yandex_disk_enabled()),
            "warning": str(exc),
        })


@router.get("/api/checklist/upload-job-status")
def api_checklist_upload_job_status(jobId: str = ""):
    job = get_upload_job(jobId)
    return JSONResponse(public_job_payload(job))


@router.get("/api/checklist/document-mirror-status")
def api_checklist_document_mirror_status(
    dialogId: str = "",
    checklistKey: str = "id",
    itemId: str = "",
    documentId: str = "",
):
    job = get_latest_document_job(
        dialog_id=dialogId,
        checklist_key=checklistKey,
        item_id=itemId,
        document_id=documentId,
    )

    return JSONResponse(public_job_payload(job))


@router.get("/api/checklist/yandex-mirror-queue-state")
def api_checklist_yandex_mirror_queue_state():
    return JSONResponse(get_yandex_mirror_queue_state())

def _folder_mirror_status_presentation(
    mirror_status: str,
) -> tuple[str, str, str]:
    normalized_status = clean_cell_value(
        mirror_status
    ).lower()

    presentations = {
        "error": (
            "Ошибка Яндекса",
            (
                "При синхронизации произошла ошибка. "
                "Замена доступна после подтверждения"
            ),
            "error",
        ),
    }

    return presentations.get(
        normalized_status,
        (
            "",
            "",
            "unknown",
        ),
    )


def _build_folder_document_rows_html(
    *,
    documents: list[dict],
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    target_item: dict,
) -> str:
    rows: list[str] = []

    for doc in documents:
        doc_id = str(doc.get("id") or "")
        escaped_doc_id = html.escape(doc_id)
        doc_name = html.escape(
            str(doc.get("name") or "Файл")
        )
        doc_size = html.escape(
            format_file_size(doc.get("size") or 0)
        )
        uploaded_at_text = html.escape(
            format_document_uploaded_at(
                doc.get("uploadedAt")
                or doc.get("modifiedAt")
            )
        )
        uploaded_by_text = html.escape(
            clean_cell_value(doc.get("uploadedByName"))
            or "—"
        )
        open_url = build_document_view_url(
            dialog_id,
            checklist_key,
            item_id,
            doc_id,
        )
        download_url = open_url + "&download=1"

        mirror_resolution = resolve_document_mirror_status(
            doc
        )
        mirror_status = clean_cell_value(
            mirror_resolution.get("status")
        ).lower()
        replacement_blocked = mirror_status in {
            "queued",
            "running",
        }
        replacement_disabled_attr = (
            "disabled"
            if replacement_blocked
            else ""
        )
        replacement_title = (
            "Дождитесь завершения синхронизации "
            "с Яндекс.Диском"
            if replacement_blocked
            else "Загрузить новую версию файла"
        )
        (
            mirror_status_text,
            mirror_status_title,
            mirror_status_class,
        ) = _folder_mirror_status_presentation(
            mirror_status
        )
        mirror_status_html = ""
        if mirror_status_text:
            mirror_status_html = f"""
                        <span
                            class="folder-mirror-status folder-mirror-status--{mirror_status_class}"
                            data-role="folder-mirror-status"
                            data-document-id="{escaped_doc_id}"
                            data-mirror-status="{html.escape(mirror_status)}"
                            title="{html.escape(mirror_status_title)}"
                        >
                            {html.escape(mirror_status_text)}
                        </span>
            """

        assignment_history_count = int(
            doc.get("assignmentHistoryCount") or 0
        )
        current_series_id = (
            clean_cell_value(doc.get("seriesId"))
            or doc_id
        )
        assignment_history_panel_id = (
            "assignment-history-panel-"
            + uuid.uuid5(
                uuid.NAMESPACE_URL,
                (
                    f"{dialog_id}|{checklist_key}|{item_id}|"
                    f"{current_series_id}|assignment-history"
                ),
            ).hex
        )

        rows.append(
            f'''
            <tr
                class="folder-document-row"
                data-document-row-id="{escaped_doc_id}"
            >
                <td>
                    <div class="folder-document-name-layout">

                        <a
                            class="folder-document-name"
                            href="{html.escape(open_url)}"
                            target="_blank"
                            rel="noopener noreferrer"
                            title="Открыть файл: {doc_name}"
                            aria-label="Открыть файл {doc_name}"
                        >
                            {doc_name}
                        </a>

                        {mirror_status_html}

                        <button
                            class="folder-replace-upload-button checklist-action-button checklist-action-button-replace"
                            type="button"
                            data-role="folder-replace-upload"
                            data-document-id="{escaped_doc_id}"
                            data-document-name="{doc_name}"
                            data-mirror-status="{html.escape(mirror_status)}"
                            {replacement_disabled_attr}
                            title="{html.escape(replacement_title)}"
                            aria-label="Заменить файл"
                        >
                            <span data-checklist-icon="replace"></span>
                        </button>
                    </div>
                </td>
                <td>{doc_size}</td>
                <td>{uploaded_at_text}</td>
                <td>{uploaded_by_text}</td>
                <td class="folder-document-actions-cell">
                    <div class="folder-document-actions">
                        <button
                            class="folder-remove-button checklist-action-button checklist-action-button-remove"
                            type="button"
                            data-role="folder-remove-file"
                            data-dialog-id="{html.escape(dialog_id)}"
                            data-checklist-key="{html.escape(checklist_key)}"
                            data-item-id="{html.escape(item_id)}"
                            data-document-id="{escaped_doc_id}"
                            data-document-name="{doc_name}"
                            title="Удалить файл"
                            aria-label="Удалить файл"
                        >
                            <span data-checklist-icon="remove"></span>
                        </button>
                    </div>
                </td>
            </tr>
            '''
        )

        if assignment_history_count > 0:
            rows.append(f"""
                <tr class="folder-assignment-history-summary-row">
                    <td
                        class="folder-assignment-history-summary-cell"
                        colspan="5"
                    >
                        <button
                            type="button"
                            class="assignment-history-toggle"
                            data-role="document-assignment-history-toggle"
                            data-panel-id="{html.escape(assignment_history_panel_id)}"
                            data-dialog-id="{html.escape(dialog_id)}"
                            data-checklist-key="{html.escape(checklist_key)}"
                            data-item-id="{html.escape(item_id)}"
                            data-series-id="{html.escape(current_series_id)}"
                            aria-expanded="false"
                        >
                            <span
                                class="assignment-history-toggle-icon"
                                data-role="assignment-history-toggle-icon"
                                aria-hidden="true"
                            >▸</span>
                            <span>История заданий: {assignment_history_count}</span>
                        </button>
                    </td>
                </tr>
                <tr
                    id="{html.escape(assignment_history_panel_id)}"
                    class="folder-assignment-history-panel-row"
                    hidden
                >
                    <td
                        class="folder-assignment-history-panel-cell"
                        colspan="5"
                    >
                        <div
                            class="assignment-history-panel"
                            aria-live="polite"
                        ></div>
                    </td>
                </tr>
            """)

        current_archive_rows_html = (
            build_archive_series_rows_html(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                series_id=current_series_id,
                archive_versions=doc.get(
                    "archiveVersions"
                ),
                panel_title="Архив версий",
                panel_id=(
                    "archive-panel-"
                    + uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        (
                            f"{dialog_id}|"
                            f"{checklist_key}|"
                            f"{item_id}|"
                            f"{current_series_id}"
                        ),
                    ).hex
                ),
                format_datetime=(
                    format_document_uploaded_at
                ),
                detached=False,
            )
        )

        if current_archive_rows_html:
            rows.append(
                current_archive_rows_html
            )

    detached_archive_rows_html = (
        build_detached_archive_rows_html(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            archived_document_series=(
                target_item.get(
                    "archivedDocumentSeries"
                )
            ),
            format_datetime=(
                format_document_uploaded_at
            ),
        )
    )

    if detached_archive_rows_html:
        rows.append(
            detached_archive_rows_html
        )

    if rows:
        return "".join(rows)

    return '''
        <tr class="folder-empty-row">
            <td colspan="5">В папке пока нет файлов</td>
        </tr>
    '''


def _build_folder_actions_html(
    *,
    documents: list[dict],
    yandex_folder_url: str,
    yandex_available: bool,
) -> str:
    replace_controls_html = ""

    if documents:
        replace_controls_html = '''
            <input
                class="folder-hidden-input"
                type="file"
                id="folderReplaceInput"
                aria-label="Выбрать новую версию файла"
            >
        '''

    yandex_link_html = ""

    if documents and yandex_folder_url and yandex_available:
        yandex_link_html = f'''
            <a
                class="folder-yandex-link checklist-action-button checklist-action-button-yandex"
                href="{html.escape(yandex_folder_url)}"
                target="_blank"
                title="Открыть папку пункта на Яндекс.Диске"
                aria-label="Открыть папку пункта на Яндекс.Диске"
            >
                <span data-checklist-icon="yandex"></span>
            </a>
        '''
    elif documents:
        yandex_link_html = '''
            <button
                class="folder-yandex-link checklist-action-button checklist-action-button-yandex"
                type="button"
                title="Открыть папку пункта на Яндекс.Диске"
                aria-label="Открыть папку пункта на Яндекс.Диске"
                aria-disabled="true"
                disabled
            >
                <span data-checklist-icon="yandex"></span>
            </button>
        '''

    return f'''
        <div class="folder-actions" role="toolbar" aria-label="Действия с документами пункта">
            {replace_controls_html}
            <button
                class="folder-action-button checklist-action-button checklist-action-button-upload"
                type="button"
                id="folderUploadBtn"
                title="Загрузить файлы в папку пункта"
                aria-label="Загрузить файлы в папку пункта"
            >
                <span data-checklist-icon="upload"></span>
            </button>
            <input
                class="folder-hidden-input"
                type="file"
                id="folderUploadInput"
                multiple
            >
            <button
                class="folder-action-button checklist-action-button checklist-action-button-bell"
                type="button"
                id="folderNotificationBtn"
                data-role="notify-documents-disabled"
                title="Оповещения временно недоступны"
                aria-label="Оповещения временно недоступны"
                aria-disabled="true"
                disabled
            >
                <span data-checklist-icon="bell"></span>
            </button>
            {yandex_link_html}
        </div>
    '''


def _safe_json_for_inline_script(value) -> str:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
        )
        .replace("</", "<\\/")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


@router.get(
    "/api/checklist/folder",
    response_class=HTMLResponse,
)
def api_checklist_folder(
    dialogId: str = "",
    itemId: str = "",
    checklistKey: str = "id",
    sessionId: str = "",
    userId: str = "",
    userName: str = "",
):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(
        checklistKey
    )
    item_id = str(itemId or "").strip()

    if not dialog_id or not item_id:
        return HTMLResponse(
            "<h3>Не переданы dialogId или itemId</h3>",
            status_code=400,
        )

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []
    target_item = None

    for index, item in enumerate(items):
        if str(item.get("id") or "") == item_id:
            target_item = migrate_legacy_document_fields(
                item
            )
            items[index] = target_item
            break

    if not target_item:
        return JSONResponse(
            {
                "ok": False,
                "error": "item not found",
            },
            status_code=404,
        )

    documents = normalize_documents_list(
        target_item.get("documents")
    )
    yandex_folder_data = get_item_yandex_folder(
        dialog_id,
        checklist_key,
        clean_cell_value(target_item.get("name")),
        group_id=int(target_item.get("group") or 0),
    )
    yandex_folder = (
        yandex_folder_data or {}
    ).get("folder") or {}
    yandex_folder_url = clean_cell_value(
        yandex_folder.get("url")
    )
    yandex_folder_path = clean_cell_value(
        yandex_folder.get("path")
    )
    yandex_context = (
        yandex_folder_data or {}
    ).get("context") or {}
    yandex_mirror_targets = (
        yandex_context.get("storageMode") or {}
    ).get("mirrorTargets") or []
    yandex_available = (
        bool(yandex_context)
        and "yandex_disk" in yandex_mirror_targets
        and is_yandex_disk_enabled()
    )

    table_html = _build_folder_document_rows_html(
        documents=documents,
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_id=item_id,
        target_item=target_item,
    )
    folder_actions_html = _build_folder_actions_html(
        documents=documents,
        yandex_folder_url=yandex_folder_url,
        yandex_available=yandex_available,
    )

    yandex_folder_path_html = ""

    app_base_path = normalize_base_path(
        APP_BASE_PATH
    )
    ui_static_base_url = (
        f"{app_base_path}/ui-static"
    )
    ui_asset_version = "8.15.5.1-folder-return-close"
    popup_url = (
        f"{app_base_path}/popup"
        f"?dialogId={quote(dialog_id, safe='')}"
        f"&checklistKey={quote(checklist_key, safe='')}"
        f"&focusItemId={quote(item_id, safe='')}"
    )

    bootstrap_payload = {
        "removeApiUrl": (
            f"{app_base_path}"
            "/api/checklist/remove-document"
        ),
        "uploadApiUrl": (
            f"{app_base_path}"
            "/api/checklist/upload-document"
        ),
        "replaceApiUrl": (
            f"{app_base_path}"
            "/api/checklist/replace-document"
        ),
        "documentMirrorStatusApiUrl": (
            f"{app_base_path}"
            "/api/checklist/document-mirror-status"
        ),
        "archiveDeleteApiUrl": (
            f"{app_base_path}"
            "/api/checklist/delete-archive-version"
        ),
        "notificationDraftsApiUrl": (
            f"{app_base_path}"
            "/api/checklist/notification-drafts"
        ),
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "itemId": item_id,
        "itemGroup": str(
            target_item.get("group") or ""
        ),
        "itemName": (
            clean_cell_value(target_item.get("name"))
            or "Пункт"
        ),
        "sessionId": clean_cell_value(sessionId),
        "userId": clean_cell_value(userId),
        "userName": clean_cell_value(userName),
        "popupUrl": popup_url,
        "deleteAllowedUserIds": sorted(
            get_file_delete_allowed_user_ids()
        ),
        "archiveDeleteAdminUserIds": sorted(
            get_archive_permanent_delete_admin_user_ids()
        ),
    }

    return render_ui_template(
        "folder.html",
        {
            "FOLDER_TITLE": html.escape(
                str(target_item.get("name") or "Папка")
            ),
            "FOLDER_CHECKLIST_TITLE": html.escape(
                str(data.get("title") or "Чек-лист")
            ),
            "FOLDER_YANDEX_PATH_HTML": (
                yandex_folder_path_html
            ),
            "FOLDER_ACTIONS_HTML": folder_actions_html,
            "FOLDER_TABLE_HTML": table_html,
            "FOLDER_BOOTSTRAP_JSON": (
                _safe_json_for_inline_script(
                    bootstrap_payload
                )
            ),
            "FOLDER_CSS_URL": html.escape(
                f"{ui_static_base_url}/css/folder.css?v={ui_asset_version}"
            ),
            "FOLDER_UPLOADS_CSS_URL": html.escape(
                f"{ui_static_base_url}/css/uploads.css?v={ui_asset_version}"
            ),
            "FOLDER_ACTIONS_CSS_URL": html.escape(
                f"{ui_static_base_url}/css/action-controls.css?v={ui_asset_version}"
            ),
            "FOLDER_ARCHIVE_CSS_URL": html.escape(
                f"{ui_static_base_url}/css/archive.css?v={ui_asset_version}"
            ),
            "FOLDER_NOTIFICATIONS_CSS_URL": html.escape(
                f"{ui_static_base_url}/css/notifications.css?v={ui_asset_version}"
            ),
            "FOLDER_ASSIGNMENT_HISTORY_CSS_URL": html.escape(
                f"{ui_static_base_url}/css/assignment-history.css?v={ui_asset_version}"
            ),
            "FOLDER_WINDOW_CHANNEL_JS_URL": html.escape(
                f"{ui_static_base_url}/js/checklist-window-channel.js?v={ui_asset_version}"
            ),
            "FOLDER_ACTION_ICONS_JS_URL": html.escape(
                f"{ui_static_base_url}/js/checklist-action-icons.js?v={ui_asset_version}"
            ),
            "FOLDER_CORE_JS_URL": html.escape(
                f"{ui_static_base_url}/js/folder-core.js?v={ui_asset_version}"
            ),
            "FOLDER_BITRIX_USER_PICKER_JS_URL": html.escape(
                f"{ui_static_base_url}/js/bitrix-user-picker.js?v={ui_asset_version}"
            ),
            "FOLDER_BITRIX_COMPANY_PICKER_JS_URL": html.escape(
                f"{ui_static_base_url}/js/bitrix-company-picker.js?v={ui_asset_version}"
            ),
            "FOLDER_NOTIFICATION_UI_JS_URL": html.escape(
                f"{ui_static_base_url}/js/notification-draft-ui.js?v={ui_asset_version}"
            ),
            "FOLDER_NOTIFICATION_DRAFTS_JS_URL": html.escape(
                f"{ui_static_base_url}/js/folder-notification-drafts.js?v={ui_asset_version}"
            ),
            "FOLDER_ASSIGNMENT_HISTORY_JS_URL": html.escape(
                f"{ui_static_base_url}/js/document-assignment-history.js?v={ui_asset_version}"
            ),
            "FOLDER_UPLOAD_PROGRESS_JS_URL": html.escape(
                f"{ui_static_base_url}/js/folder-upload-progress.js?v={ui_asset_version}"
            ),
            "FOLDER_UPLOADS_JS_URL": html.escape(
                f"{ui_static_base_url}/js/folder-uploads.js?v={ui_asset_version}"
            ),
            "FOLDER_REPLACEMENT_JS_URL": html.escape(
                f"{ui_static_base_url}/js/folder-replacement.js?v={ui_asset_version}"
            ),
            "FOLDER_ARCHIVE_JS_URL": html.escape(
                f"{ui_static_base_url}/js/folder-archive-ui.js?v={ui_asset_version}"
            ),
        },
    )

@router.get("/api/checklist/file")
def api_checklist_file(
    dialogId: str = "",
    itemId: str = "",
    documentId: str = "",
    checklistKey: str = "id",
    download: int = 0
):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    item_id = str(itemId or "").strip()
    document_id = str(documentId or "").strip()

    if not dialog_id or not item_id or not document_id:
        return JSONResponse({"ok": False, "error": "dialogId, itemId and documentId are required"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    target_item = None
    for item in items:
        if str(item.get("id") or "") == item_id:
            target_item = item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

    target_item = migrate_legacy_document_fields(target_item)
    documents = normalize_documents_list(target_item.get("documents"))

    target_doc = None
    for doc in documents:
        if str(doc.get("id") or "") == document_id:
            target_doc = doc
            break

    if not target_doc:
        return JSONResponse({"ok": False, "error": "document not found"}, status_code=404)

    file_url = clean_cell_value(target_doc.get("fileUrl")) or clean_cell_value(target_doc.get("path"))
    file_path = get_upload_file_path_from_url(file_url)

    if not file_path or not file_path.exists():
        return JSONResponse({"ok": False, "error": "file not found on disk"}, status_code=404)

    filename = clean_cell_value(target_doc.get("name")) or file_path.name
    media_type, _ = mimetypes.guess_type(str(file_path))
    media_type = media_type or "application/octet-stream"

    inline_allowed = can_preview_in_browser(filename, media_type)
    disposition = "attachment" if download else ("inline" if inline_allowed else "attachment")

    response = FileResponse(
        path=str(file_path),
        media_type=media_type
    )
    response.headers["Content-Disposition"] = f"{disposition}; filename*=UTF-8''{quote(filename)}"
    return response
