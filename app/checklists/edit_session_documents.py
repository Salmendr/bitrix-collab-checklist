from __future__ import annotations

import copy
import shutil
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import UploadFile

from app.logging_utils import write_debug_log
from app.settings import BASE_DIR, UPLOAD_ROOT

from app.checklists.config import get_checklist_config
from app.checklists.documents import (
    archive_current_document_local_file,
    build_detached_archive_series,
    build_document_view_url,
    build_folder_view_url,
    build_upload_rel_path,
    get_upload_file_path_from_url,
    merge_detached_archive_series,
    migrate_legacy_document_fields,
    normalize_document_record,
    normalize_documents_list,
)
from app.checklists.edit_session_changes import (
    acquire_checklist_for_edit_session,
    ensure_checklist_snapshot,
    record_checklist_operation,
)
from app.checklists.edit_session_files import (
    public_file_entry,
    register_created_file,
    rollback_edit_session_file_operation,
    stash_existing_file,
)
from app.checklists.edit_sessions import EditSessionConflictError
from app.checklists.item_mutation_guard import item_mutation_guard
from app.checklists.normalization import (
    derive_indicator_from_status,
    normalize_checklist_data,
)
from app.checklists.storage import get_checklist, save_checklist
from app.checklists.upload_jobs import resolve_document_mirror_status
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


DEFERRED_MIRROR_STATUS = "deferred"
DEFERRED_MIRROR_STAGE = "awaiting_session_commit"


async def _save_upload_stream(
    file: UploadFile,
    abs_path: Path,
    *,
    operation_id: str,
    log_payload: dict | None = None,
) -> int:
    total_size = 0
    chunk_size = 1024 * 1024
    started_at = time.monotonic()
    base_payload = {
        **dict(log_payload or {}),
        "operationId": operation_id,
        "absPath": str(abs_path),
        "chunkSize": chunk_size,
    }

    write_debug_log(
        "edit_session_document_stream_started",
        base_payload,
    )

    try:
        abs_path.parent.mkdir(parents=True, exist_ok=True)

        with abs_path.open("wb") as output:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                output.write(chunk)
                total_size += len(chunk)

        if total_size <= 0:
            raise ValueError("Загружен пустой файл")

        write_debug_log(
            "edit_session_document_stream_completed",
            {
                **base_payload,
                "writtenBytes": total_size,
                "durationMs": int(
                    (time.monotonic() - started_at) * 1000
                ),
            },
        )
        return total_size

    except Exception as exc:
        write_debug_log(
            "edit_session_document_stream_failed",
            {
                **base_payload,
                "writtenBytes": total_size,
                "error": str(exc),
                "traceback": traceback.format_exc()[-4000:],
            },
        )
        raise


def _find_item(data: dict, item_id: str) -> tuple[int, dict]:
    items = data.get("items", []) or []

    for index, raw_item in enumerate(items):
        if clean_cell_value(raw_item.get("id")) != clean_cell_value(item_id):
            continue

        item = migrate_legacy_document_fields(raw_item)
        items[index] = item
        data["items"] = items
        return index, item

    raise KeyError("item not found")


def _document_local_path(document: dict) -> Path:
    local_url = (
        clean_cell_value(document.get("fileUrl"))
        or clean_cell_value(document.get("path"))
        or clean_cell_value(document.get("previewUrl"))
    )
    path = get_upload_file_path_from_url(local_url)

    if path is None:
        raise RuntimeError(
            "Документ не расположен в локальном хранилище /uploads"
        )

    if not path.is_file():
        raise FileNotFoundError(
            "Локальный файл документа не найден: " + str(path)
        )

    return path


def _deferred_mirror_payload() -> dict:
    return {
        "mirrorStatus": DEFERRED_MIRROR_STATUS,
        "mirrorStage": DEFERRED_MIRROR_STAGE,
        "mirrorError": "",
        "mirrorJobId": "",
        "yandexPath": "",
        "yandexFileUrl": "",
    }


def _updated_item(saved: dict, item_id: str) -> dict:
    for item in saved.get("items", []) or []:
        if clean_cell_value(item.get("id")) == clean_cell_value(item_id):
            return item
    raise RuntimeError("updated item not found")


def _restore_after_failed_operation(
    *,
    session_id: str,
    operation_id: str,
    dialog_id: str,
    checklist_key: str,
    before_checklist: dict | None,
) -> None:
    if before_checklist is not None:
        try:
            save_checklist(
                dialog_id,
                before_checklist,
                checklist_key,
            )
        except Exception as exc:
            write_debug_log(
                "edit_session_document_checklist_restore_failed",
                {
                    "sessionId": session_id,
                    "operationId": operation_id,
                    "dialogId": dialog_id,
                    "checklistKey": checklist_key,
                    "error": str(exc),
                },
            )

    try:
        rollback_edit_session_file_operation(
            session_id=session_id,
            operation_id=operation_id,
        )
    except Exception as exc:
        write_debug_log(
            "edit_session_document_file_restore_failed",
            {
                "sessionId": session_id,
                "operationId": operation_id,
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "error": str(exc),
            },
        )
        raise


async def transactional_upload_document(
    *,
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    item_group: int,
    file: UploadFile,
    acting_user_id: str = "",
    acting_user_name: str = "",
) -> dict:
    async with item_mutation_guard(
        dialog_id,
        checklist_key,
        item_id,
    ):
        return await _transactional_upload_document_inner(
            session_id=session_id,
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            item_group=item_group,
            file=file,
            acting_user_id=acting_user_id,
            acting_user_name=acting_user_name,
        )


async def _transactional_upload_document_inner(
    *,
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    item_group: int,
    file: UploadFile,
    acting_user_id: str = "",
    acting_user_name: str = "",
) -> dict:
    normalized_session_id = clean_cell_value(session_id)
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(checklist_key)
    normalized_item_id = clean_cell_value(item_id)
    normalized_user_id = clean_cell_value(acting_user_id)
    normalized_user_name = clean_cell_value(acting_user_name) or "Пользователь"
    uploaded_name = Path(file.filename or "file.bin").name
    operation_id = uuid.uuid4().hex
    created_path: Path | None = None
    before_checklist: dict | None = None

    transaction = acquire_checklist_for_edit_session(
        session_id=normalized_session_id,
        dialog_id=normalized_dialog_id,
        checklist_key=normalized_checklist_key,
        user_id=normalized_user_id,
        user_name=normalized_user_name,
    )

    try:
        config = get_checklist_config(normalized_checklist_key)
        data = get_checklist(normalized_dialog_id, config.key)
        before_checklist = copy.deepcopy(data)
        ensure_checklist_snapshot(
            session_id=transaction["sessionId"],
            dialog_id=normalized_dialog_id,
            checklist_key=config.key,
            data=before_checklist,
        )

        _, target_item = _find_item(data, normalized_item_id)
        rel_path = build_upload_rel_path(
            normalized_dialog_id,
            normalized_item_id,
            uploaded_name,
        )
        created_path = UPLOAD_ROOT / rel_path
        file_size = await _save_upload_stream(
            file,
            created_path,
            operation_id=operation_id,
            log_payload={
                "sessionId": normalized_session_id,
                "dialogId": normalized_dialog_id,
                "checklistKey": config.key,
                "itemId": normalized_item_id,
                "fileName": uploaded_name,
            },
        )

        document_id = uuid.uuid4().hex
        uploaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        file_url = "/uploads/" + rel_path.replace("\\", "/")
        document_record = normalize_document_record({
            "id": document_id,
            "seriesId": document_id,
            "name": uploaded_name,
            "path": file_url,
            "fileUrl": file_url,
            "previewUrl": build_document_view_url(
                normalized_dialog_id,
                config.key,
                normalized_item_id,
                document_id,
            ),
            "size": file_size,
            "modifiedAt": uploaded_at,
            "uploadedAt": uploaded_at,
            "uploadedById": normalized_user_id,
            "uploadedByName": normalized_user_name,
            "source": "local",
            **_deferred_mirror_payload(),
            "yandexFolderAlias": "",
        })

        file_entry = register_created_file(
            session_id=normalized_session_id,
            operation_id=operation_id,
            operation_type="document_upload",
            dialog_id=normalized_dialog_id,
            checklist_key=config.key,
            item_id=normalized_item_id,
            series_id=clean_cell_value(document_record.get("seriesId")),
            document_id=document_id,
            file_path=created_path,
            user_id=normalized_user_id,
            metadata={
                "fileName": uploaded_name,
                "deferredYandexUpload": True,
            },
        )

        documents = normalize_documents_list(target_item.get("documents"))
        documents.append(document_record)
        documents = normalize_documents_list(documents)
        target_item["documents"] = documents
        target_item["folderPath"] = (
            "/" + str(created_path.parent.relative_to(BASE_DIR)).replace("\\", "/")
        )
        target_item["folderUrl"] = build_folder_view_url(
            normalized_dialog_id,
            config.key,
            normalized_item_id,
        )
        first_doc = documents[0] if documents else {}
        target_item["documentUrl"] = clean_cell_value(first_doc.get("fileUrl"))
        target_item["documentName"] = clean_cell_value(first_doc.get("name"))

        actual_group = int(target_item.get("group") or item_group or 0)
        if config.is_active_group(actual_group):
            target_item["status"] = "Есть"
            target_item["priority"] = derive_indicator_from_status("Есть")

        saved = save_checklist(
            normalized_dialog_id,
            normalize_checklist_data(data, config.key),
            config.key,
        )
        saved_item = _updated_item(saved, normalized_item_id)

        operation = record_checklist_operation(
            session_id=normalized_session_id,
            dialog_id=normalized_dialog_id,
            checklist_key=config.key,
            operation_type="document_upload",
            before={"item": _find_item(copy.deepcopy(before_checklist), normalized_item_id)[1]},
            after={"item": saved_item, "document": document_record},
            final_checklist_data=saved,
            item_id=normalized_item_id,
            series_id=clean_cell_value(document_record.get("seriesId")),
            document_id=document_id,
            operation_id=operation_id,
            payload={
                "fileName": uploaded_name,
                "localPath": str(created_path),
                "deferredYandexUpload": True,
            },
        )

        return {
            "ok": True,
            "transactional": True,
            "sessionId": normalized_session_id,
            "dialogId": normalized_dialog_id,
            "checklistKey": config.key,
            "item": saved_item,
            "document": document_record,
            "operation": operation,
            "fileEntries": [public_file_entry(file_entry)],
            "uploadJobId": "",
            "uploadJob": {},
            "yandexMirrorDeferred": True,
            "yandexMirrorQueued": False,
            "progressPercent": saved.get("progressPercent", 0),
        }

    except Exception:
        if created_path is not None and created_path.exists():
            try:
                rollback_edit_session_file_operation(
                    session_id=normalized_session_id,
                    operation_id=operation_id,
                )
            except Exception:
                try:
                    created_path.unlink()
                except Exception:
                    pass

        _restore_after_failed_operation(
            session_id=normalized_session_id,
            operation_id=operation_id,
            dialog_id=normalized_dialog_id,
            checklist_key=normalized_checklist_key,
            before_checklist=before_checklist,
        )
        raise


async def transactional_replace_document(
    *,
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str,
    file: UploadFile,
    acting_user_id: str = "",
    acting_user_name: str = "",
    force_replace: bool = False,
) -> dict:
    normalized_session_id = clean_cell_value(session_id)
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(checklist_key)
    normalized_item_id = clean_cell_value(item_id)
    normalized_document_id = clean_cell_value(document_id)
    normalized_user_id = clean_cell_value(acting_user_id)
    normalized_user_name = clean_cell_value(acting_user_name) or "Пользователь"
    uploaded_name = Path(file.filename or "file.bin").name
    operation_id = uuid.uuid4().hex
    before_checklist: dict | None = None
    archive_path: Path | None = None
    archive_entry: dict | None = None

    transaction = acquire_checklist_for_edit_session(
        session_id=normalized_session_id,
        dialog_id=normalized_dialog_id,
        checklist_key=normalized_checklist_key,
        user_id=normalized_user_id,
        user_name=normalized_user_name,
    )

    try:
        config = get_checklist_config(normalized_checklist_key)
        data = get_checklist(normalized_dialog_id, config.key)
        before_checklist = copy.deepcopy(data)
        ensure_checklist_snapshot(
            session_id=normalized_session_id,
            dialog_id=normalized_dialog_id,
            checklist_key=config.key,
            data=before_checklist,
        )

        item_index, target_item = _find_item(data, normalized_item_id)
        documents = normalize_documents_list(target_item.get("documents"))
        old_index = -1
        old_document: dict | None = None

        for index, document in enumerate(documents):
            if clean_cell_value(document.get("id")) == normalized_document_id:
                old_index = index
                old_document = document
                break

        if old_document is None:
            raise KeyError("document not found")

        mirror_resolution = resolve_document_mirror_status(
            old_document
        )
        mirror_status = clean_cell_value(
            mirror_resolution.get("status")
        ).lower()
        if mirror_status in {"queued", "running"}:
            raise EditSessionConflictError(
                "Файл ещё синхронизируется с Яндекс.Диском"
            )
        if mirror_status == "error" and not force_replace:
            raise EditSessionConflictError(
                "У текущего файла есть ошибка синхронизации; требуется подтверждение"
            )

        old_local_path = _document_local_path(old_document)
        new_rel_path = build_upload_rel_path(
            normalized_dialog_id,
            normalized_item_id,
            uploaded_name,
        )
        new_abs_path = UPLOAD_ROOT / new_rel_path
        file_size = await _save_upload_stream(
            file,
            new_abs_path,
            operation_id=operation_id,
            log_payload={
                "sessionId": normalized_session_id,
                "dialogId": normalized_dialog_id,
                "checklistKey": config.key,
                "itemId": normalized_item_id,
                "oldDocumentId": normalized_document_id,
                "fileName": uploaded_name,
            },
        )

        new_document_id = uuid.uuid4().hex
        series_id = (
            clean_cell_value(old_document.get("seriesId"))
            or normalized_document_id
            or uuid.uuid4().hex
        )
        new_entry = register_created_file(
            session_id=normalized_session_id,
            operation_id=operation_id,
            operation_type="document_replace",
            dialog_id=normalized_dialog_id,
            checklist_key=config.key,
            item_id=normalized_item_id,
            series_id=series_id,
            document_id=new_document_id,
            file_path=new_abs_path,
            user_id=normalized_user_id,
            metadata={
                "fileName": uploaded_name,
                "deferredYandexUpload": True,
            },
        )

        stashed_entry = stash_existing_file(
            session_id=normalized_session_id,
            operation_id=operation_id,
            operation_type="document_replace",
            dialog_id=normalized_dialog_id,
            checklist_key=config.key,
            item_id=normalized_item_id,
            series_id=series_id,
            document_id=normalized_document_id,
            file_path=old_local_path,
            user_id=normalized_user_id,
            metadata={
                "oldFileName": clean_cell_value(old_document.get("name")),
                "oldYandexPath": clean_cell_value(old_document.get("yandexPath")),
            },
        )

        staged_old_path = Path(stashed_entry["staged_path"])
        old_local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(staged_old_path, old_local_path)
        replaced_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        archived_version = archive_current_document_local_file(
            dialog_id=normalized_dialog_id,
            item_id=normalized_item_id,
            document=old_document,
            archived_by_id=normalized_user_id,
            archived_by_name=normalized_user_name,
            archived_at=replaced_at,
        )
        archive_path = get_upload_file_path_from_url(
            archived_version.get("fileUrl")
        )
        if archive_path is None or not archive_path.is_file():
            raise RuntimeError("Архивная версия не создана")

        archive_entry = register_created_file(
            session_id=normalized_session_id,
            operation_id=operation_id,
            operation_type="document_replace_archive",
            dialog_id=normalized_dialog_id,
            checklist_key=config.key,
            item_id=normalized_item_id,
            series_id=series_id,
            document_id=clean_cell_value(archived_version.get("id")),
            file_path=archive_path,
            user_id=normalized_user_id,
            metadata={
                "archiveVersion": int(archived_version.get("version") or 0),
                "originalDocumentId": normalized_document_id,
            },
        )

        new_file_url = "/uploads/" + new_rel_path.replace("\\", "/")
        new_document = normalize_document_record({
            "id": new_document_id,
            "seriesId": series_id,
            "name": uploaded_name,
            "path": new_file_url,
            "fileUrl": new_file_url,
            "previewUrl": build_document_view_url(
                normalized_dialog_id,
                config.key,
                normalized_item_id,
                new_document_id,
            ),
            "size": file_size,
            "modifiedAt": replaced_at,
            "uploadedAt": replaced_at,
            "uploadedById": normalized_user_id,
            "uploadedByName": normalized_user_name,
            "source": "local",
            "archiveVersions": list(old_document.get("archiveVersions") or []) + [archived_version],
            "lastReplacedAt": replaced_at,
            "lastReplacedById": normalized_user_id,
            "lastReplacedByName": normalized_user_name,
            "replacementOperationId": operation_id,
            **_deferred_mirror_payload(),
            "yandexFolderAlias": clean_cell_value(old_document.get("yandexFolderAlias")),
        })

        documents[old_index] = new_document
        documents = normalize_documents_list(documents)
        target_item["documents"] = documents
        target_item["folderPath"] = (
            "/" + str(new_abs_path.parent.relative_to(BASE_DIR)).replace("\\", "/")
        )
        target_item["folderUrl"] = build_folder_view_url(
            normalized_dialog_id,
            config.key,
            normalized_item_id,
        )
        first_doc = documents[0] if documents else {}
        target_item["documentUrl"] = clean_cell_value(first_doc.get("fileUrl"))
        target_item["documentName"] = clean_cell_value(first_doc.get("name"))
        if config.is_active_group(target_item.get("group")):
            target_item["status"] = "Есть"
            target_item["priority"] = derive_indicator_from_status("Есть")

        data["items"][item_index] = target_item
        saved = save_checklist(
            normalized_dialog_id,
            normalize_checklist_data(data, config.key),
            config.key,
        )
        saved_item = _updated_item(saved, normalized_item_id)
        saved_document = next(
            document
            for document in normalize_documents_list(saved_item.get("documents"))
            if clean_cell_value(document.get("id")) == new_document_id
        )

        operation = record_checklist_operation(
            session_id=normalized_session_id,
            dialog_id=normalized_dialog_id,
            checklist_key=config.key,
            operation_type="document_replace",
            before={"item": _find_item(copy.deepcopy(before_checklist), normalized_item_id)[1], "document": old_document},
            after={"item": saved_item, "document": saved_document, "archiveVersion": archived_version},
            final_checklist_data=saved,
            item_id=normalized_item_id,
            series_id=series_id,
            document_id=new_document_id,
            operation_id=operation_id,
            payload={
                "oldDocumentId": normalized_document_id,
                "newDocumentId": new_document_id,
                "oldFileName": clean_cell_value(old_document.get("name")),
                "newFileName": uploaded_name,
                "oldYandexPath": clean_cell_value(old_document.get("yandexPath")),
                "newLocalPath": str(new_abs_path),
                "archiveLocalPath": str(archive_path),
                "archiveVersionId": clean_cell_value(archived_version.get("id")),
                "deferredYandexUpload": True,
                "deferredOldYandexDelete": bool(clean_cell_value(old_document.get("yandexPath"))),
            },
        )

        return {
            "ok": True,
            "transactional": True,
            "sessionId": normalized_session_id,
            "dialogId": normalized_dialog_id,
            "checklistKey": config.key,
            "item": saved_item,
            "document": saved_document,
            "replacement": {
                "operationId": operation_id,
                "seriesId": series_id,
                "oldDocumentId": normalized_document_id,
                "newDocumentId": new_document_id,
                "oldFileName": clean_cell_value(old_document.get("name")),
                "newFileName": uploaded_name,
                "archiveVersion": archived_version,
            },
            "operation": operation,
            "fileEntries": [
                public_file_entry(new_entry),
                public_file_entry(stashed_entry),
                public_file_entry(archive_entry),
            ],
            "uploadJobId": "",
            "uploadJob": {},
            "yandexMirrorDeferred": True,
            "yandexMirrorQueued": False,
            "oldYandexDeleteDeferred": bool(clean_cell_value(old_document.get("yandexPath"))),
            "replacementTransaction": {
                "operationId": operation_id,
                "status": "deferred_until_commit",
            },
            "progressPercent": saved.get("progressPercent", 0),
        }

    except Exception:
        if (
            archive_path is not None
            and archive_path.exists()
            and archive_entry is None
        ):
            try:
                archive_path.unlink()
            except Exception:
                pass
        _restore_after_failed_operation(
            session_id=normalized_session_id,
            operation_id=operation_id,
            dialog_id=normalized_dialog_id,
            checklist_key=normalized_checklist_key,
            before_checklist=before_checklist,
        )
        raise


def transactional_remove_document(
    *,
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str = "",
    document_url: str = "",
    preserve_status: bool = False,
    acting_user_id: str = "",
    acting_user_name: str = "",
) -> dict:
    normalized_session_id = clean_cell_value(session_id)
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(checklist_key)
    normalized_item_id = clean_cell_value(item_id)
    normalized_document_id = clean_cell_value(document_id)
    normalized_document_url = clean_cell_value(document_url)
    normalized_user_id = clean_cell_value(acting_user_id)
    normalized_user_name = clean_cell_value(acting_user_name) or "Пользователь"
    operation_id = uuid.uuid4().hex
    before_checklist: dict | None = None

    transaction = acquire_checklist_for_edit_session(
        session_id=normalized_session_id,
        dialog_id=normalized_dialog_id,
        checklist_key=normalized_checklist_key,
        user_id=normalized_user_id,
        user_name=normalized_user_name,
    )

    try:
        config = get_checklist_config(normalized_checklist_key)
        data = get_checklist(normalized_dialog_id, config.key)
        before_checklist = copy.deepcopy(data)
        ensure_checklist_snapshot(
            session_id=normalized_session_id,
            dialog_id=normalized_dialog_id,
            checklist_key=config.key,
            data=before_checklist,
        )
        item_index, target_item = _find_item(data, normalized_item_id)
        documents = normalize_documents_list(target_item.get("documents"))
        doc_to_remove = None

        for document in documents:
            same_id = normalized_document_id and clean_cell_value(document.get("id")) == normalized_document_id
            same_url = normalized_document_url and normalized_document_url in {
                clean_cell_value(document.get("fileUrl")),
                clean_cell_value(document.get("previewUrl")),
                clean_cell_value(document.get("path")),
            }
            if same_id or same_url:
                doc_to_remove = document
                break

        if doc_to_remove is None and documents:
            doc_to_remove = documents[0]
        if doc_to_remove is None:
            raise KeyError("document not found")

        mirror_status = clean_cell_value(doc_to_remove.get("mirrorStatus")).lower()
        if mirror_status in {"queued", "running"}:
            raise EditSessionConflictError(
                "Файл ещё синхронизируется с Яндекс.Диском"
            )

        removed_document_id = clean_cell_value(doc_to_remove.get("id"))
        local_path = _document_local_path(doc_to_remove)
        file_entry = stash_existing_file(
            session_id=normalized_session_id,
            operation_id=operation_id,
            operation_type="document_remove",
            dialog_id=normalized_dialog_id,
            checklist_key=config.key,
            item_id=normalized_item_id,
            series_id=clean_cell_value(doc_to_remove.get("seriesId")),
            document_id=removed_document_id,
            file_path=local_path,
            user_id=normalized_user_id,
            metadata={
                "fileName": clean_cell_value(doc_to_remove.get("name")),
                "oldYandexPath": clean_cell_value(doc_to_remove.get("yandexPath")),
                "deferredYandexDelete": bool(clean_cell_value(doc_to_remove.get("yandexPath"))),
            },
        )

        detached_series = build_detached_archive_series(
            doc_to_remove,
            removed_by_id=normalized_user_id,
            removed_by_name=normalized_user_name,
        )
        if detached_series:
            target_item["archivedDocumentSeries"] = merge_detached_archive_series(
                target_item.get("archivedDocumentSeries"),
                [detached_series],
            )

        remaining = [
            document
            for document in documents
            if clean_cell_value(document.get("id")) != removed_document_id
        ]
        remaining = normalize_documents_list(remaining)
        target_item["documents"] = remaining
        first_doc = remaining[0] if remaining else {}
        target_item["documentUrl"] = clean_cell_value(first_doc.get("fileUrl"))
        target_item["documentName"] = clean_cell_value(first_doc.get("name"))

        if remaining:
            first_url = clean_cell_value(first_doc.get("fileUrl"))
            target_item["folderPath"] = first_url.rsplit("/", 1)[0] if first_url.startswith("/") else ""
            target_item["folderUrl"] = build_folder_view_url(
                normalized_dialog_id,
                config.key,
                normalized_item_id,
            )
        else:
            target_item["folderPath"] = ""
            target_item["folderUrl"] = ""
            if (
                config.reset_status_on_last_document_removed
                and config.is_active_group(target_item.get("group"))
                and not preserve_status
            ):
                target_item["status"] = ""
                target_item["priority"] = "white"

        data["items"][item_index] = target_item
        saved = save_checklist(
            normalized_dialog_id,
            normalize_checklist_data(data, config.key),
            config.key,
        )
        saved_item = _updated_item(saved, normalized_item_id)
        operation = record_checklist_operation(
            session_id=normalized_session_id,
            dialog_id=normalized_dialog_id,
            checklist_key=config.key,
            operation_type="document_remove",
            before={"item": _find_item(copy.deepcopy(before_checklist), normalized_item_id)[1], "document": doc_to_remove},
            after={"item": saved_item, "document": None},
            final_checklist_data=saved,
            item_id=normalized_item_id,
            series_id=clean_cell_value(doc_to_remove.get("seriesId")),
            document_id=removed_document_id,
            operation_id=operation_id,
            payload={
                "fileName": clean_cell_value(doc_to_remove.get("name")),
                "oldYandexPath": clean_cell_value(doc_to_remove.get("yandexPath")),
                "deferredYandexDelete": bool(clean_cell_value(doc_to_remove.get("yandexPath"))),
            },
        )

        return {
            "ok": True,
            "transactional": True,
            "sessionId": normalized_session_id,
            "dialogId": normalized_dialog_id,
            "checklistKey": config.key,
            "item": saved_item,
            "operation": operation,
            "fileEntries": [public_file_entry(file_entry)],
            "deleteJobId": "",
            "yandexDeleteDeferred": bool(clean_cell_value(doc_to_remove.get("yandexPath"))),
            "yandexDeleteQueued": False,
            "progressPercent": saved.get("progressPercent", 0),
        }

    except Exception:
        _restore_after_failed_operation(
            session_id=normalized_session_id,
            operation_id=operation_id,
            dialog_id=normalized_dialog_id,
            checklist_key=normalized_checklist_key,
            before_checklist=before_checklist,
        )
        raise


def stage_status_no_documents(
    *,
    session_id: str,
    operation_id: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    item: dict,
    acting_user_id: str = "",
    acting_user_name: str = "",
) -> dict:
    normalized_user_id = clean_cell_value(acting_user_id)
    normalized_user_name = clean_cell_value(acting_user_name) or "Пользователь"
    cleaned_item = migrate_legacy_document_fields(copy.deepcopy(item))
    documents = normalize_documents_list(cleaned_item.get("documents"))
    file_entries = []
    detached_additions = []
    deferred_yandex = []

    try:
        for document in documents:
            document_id = clean_cell_value(document.get("id"))
            mirror_status = clean_cell_value(document.get("mirrorStatus")).lower()
            if mirror_status in {"queued", "running"}:
                raise EditSessionConflictError(
                    "Один из файлов ещё синхронизируется с Яндекс.Диском"
                )

            local_path = _document_local_path(document)
            entry = stash_existing_file(
                session_id=session_id,
                operation_id=operation_id,
                operation_type="status_no_document_remove",
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                series_id=clean_cell_value(document.get("seriesId")),
                document_id=document_id,
                file_path=local_path,
                user_id=normalized_user_id,
                metadata={
                    "fileName": clean_cell_value(document.get("name")),
                    "oldYandexPath": clean_cell_value(document.get("yandexPath")),
                    "deferredYandexDelete": bool(clean_cell_value(document.get("yandexPath"))),
                },
            )
            file_entries.append(public_file_entry(entry))

            detached = build_detached_archive_series(
                document,
                removed_by_id=normalized_user_id,
                removed_by_name=normalized_user_name,
            )
            if detached:
                detached_additions.append(detached)

            yandex_path = clean_cell_value(document.get("yandexPath"))
            if yandex_path:
                deferred_yandex.append({
                    "documentId": document_id,
                    "fileName": clean_cell_value(document.get("name")),
                    "yandexPath": yandex_path,
                })

        cleaned_item["archivedDocumentSeries"] = merge_detached_archive_series(
            cleaned_item.get("archivedDocumentSeries"),
            detached_additions,
        )
        cleaned_item["documents"] = []
        cleaned_item["documentUrl"] = ""
        cleaned_item["documentName"] = ""
        cleaned_item["folderPath"] = ""
        cleaned_item["folderUrl"] = ""

        return {
            "item": cleaned_item,
            "fileEntries": file_entries,
            "deferredYandexDeletes": deferred_yandex,
        }

    except Exception:
        rollback_edit_session_file_operation(
            session_id=session_id,
            operation_id=operation_id,
        )
        raise
