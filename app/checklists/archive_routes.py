import json
import mimetypes
import uuid
from copy import deepcopy
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import (
    FileResponse,
    JSONResponse,
)

from app.logging_utils import write_debug_log

from app.checklists.utils import (
    can_preview_in_browser,
    clean_cell_value,
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.checklists.permissions import (
    can_user_permanently_delete_archive,
)

from app.checklists.storage import (
    get_checklist,
    save_checklist,
)

from app.checklists.normalization import (
    normalize_checklist_data,
)

from app.checklists.edit_session_changes import (
    acquire_checklist_for_edit_session,
    ensure_checklist_snapshot,
    record_checklist_operation,
)

from app.checklists.edit_session_files import (
    public_file_entry,
    rollback_edit_session_file_operation,
    stash_existing_file,
)

from app.checklists.edit_sessions import (
    EditSessionConflictError,
    EditSessionNotFoundError,
    EditSessionPermissionError,
)

from app.checklists.documents import (
    get_upload_file_path_from_url,
    migrate_legacy_document_fields,
    normalize_documents_list,
    normalize_archive_versions,
    normalize_detached_archive_series,
)


router = APIRouter()


ACTIVE_ARCHIVE_YANDEX_DELETE_STATUSES = {
    "pending_after_replacement_sync",
    "queued",
    "running",
}


def archive_version_matches(
    version: dict,
    archive_version_id: str,
    series_id: str = "",
) -> bool:
    version_id = clean_cell_value(
        version.get("id")
    )

    version_series_id = clean_cell_value(
        version.get("seriesId")
    )

    return (
        version_id == archive_version_id
        and (
            not series_id
            or version_series_id == series_id
        )
    )


def find_archive_version_in_item(
    raw_item: dict,
    archive_version_id: str,
    series_id: str = "",
) -> dict | None:
    archive_version_id = clean_cell_value(
        archive_version_id
    )

    series_id = clean_cell_value(series_id)

    if not archive_version_id:
        return None

    item = migrate_legacy_document_fields(
        deepcopy(raw_item or {})
    )

    for document in normalize_documents_list(
        item.get("documents")
    ):
        document_series_id = clean_cell_value(
            document.get("seriesId")
        )

        if (
            series_id
            and document_series_id != series_id
        ):
            continue

        versions = normalize_archive_versions(
            document.get("archiveVersions"),
            series_id=document_series_id,
        )

        for version in versions:
            if archive_version_matches(
                version,
                archive_version_id,
                series_id,
            ):
                return {
                    "version": version,
                    "location": "current_document",
                    "seriesId": document_series_id,
                    "documentId": clean_cell_value(
                        document.get("id")
                    ),
                }

    for series in normalize_detached_archive_series(
        item.get("archivedDocumentSeries")
    ):
        detached_series_id = clean_cell_value(
            series.get("seriesId")
        )

        if (
            series_id
            and detached_series_id != series_id
        ):
            continue

        versions = normalize_archive_versions(
            series.get("archiveVersions"),
            series_id=detached_series_id,
        )

        for version in versions:
            if archive_version_matches(
                version,
                archive_version_id,
                series_id,
            ):
                return {
                    "version": version,
                    "location": "detached_series",
                    "seriesId": detached_series_id,
                    "documentId": clean_cell_value(
                        series.get(
                            "lastCurrentDocumentId"
                        )
                    ),
                }

    return None


def remove_archive_version_from_item(
    raw_item: dict,
    archive_version_id: str,
    series_id: str = "",
) -> tuple[dict, dict | None, list[str]]:
    archive_version_id = clean_cell_value(
        archive_version_id
    )

    series_id = clean_cell_value(series_id)

    item = migrate_legacy_document_fields(
        deepcopy(raw_item or {})
    )

    removed_version = None
    removed_locations = []

    next_documents = []

    for raw_document in normalize_documents_list(
        item.get("documents")
    ):
        document = dict(raw_document)

        document_series_id = clean_cell_value(
            document.get("seriesId")
        )

        versions = normalize_archive_versions(
            document.get("archiveVersions"),
            series_id=document_series_id,
        )

        next_versions = []

        for version in versions:
            is_match = archive_version_matches(
                version,
                archive_version_id,
                series_id,
            )

            if is_match:
                if removed_version is None:
                    removed_version = dict(version)

                removed_locations.append(
                    "current_document"
                )
                continue

            next_versions.append(version)

        document["archiveVersions"] = (
            normalize_archive_versions(
                next_versions,
                series_id=document_series_id,
            )
        )

        next_documents.append(document)

    item["documents"] = normalize_documents_list(
        next_documents
    )

    next_detached_series = []

    for raw_series in normalize_detached_archive_series(
        item.get("archivedDocumentSeries")
    ):
        series = dict(raw_series)

        detached_series_id = clean_cell_value(
            series.get("seriesId")
        )

        versions = normalize_archive_versions(
            series.get("archiveVersions"),
            series_id=detached_series_id,
        )

        next_versions = []

        for version in versions:
            is_match = archive_version_matches(
                version,
                archive_version_id,
                series_id,
            )

            if is_match:
                if removed_version is None:
                    removed_version = dict(version)

                removed_locations.append(
                    "detached_series"
                )
                continue

            next_versions.append(version)

        normalized_next_versions = (
            normalize_archive_versions(
                next_versions,
                series_id=detached_series_id,
            )
        )

        # Пустая отсоединённая серия больше
        # не несёт полезных данных.
        if not normalized_next_versions:
            continue

        series["archiveVersions"] = (
            normalized_next_versions
        )

        next_detached_series.append(series)

    item["archivedDocumentSeries"] = (
        normalize_detached_archive_series(
            next_detached_series
        )
    )

    return (
        item,
        removed_version,
        sorted(set(removed_locations)),
    )


def resolve_archive_local_file(
    archive_version: dict,
):
    file_url = (
        clean_cell_value(
            archive_version.get("fileUrl")
        )
        or clean_cell_value(
            archive_version.get("path")
        )
        or clean_cell_value(
            archive_version.get("previewUrl")
        )
    )

    file_path = get_upload_file_path_from_url(
        file_url
    )

    return file_url, file_path


@router.get("/api/checklist/archive-file")
def api_checklist_archive_file(
    dialogId: str = "",
    itemId: str = "",
    archiveVersionId: str = "",
    seriesId: str = "",
    checklistKey: str = "id",
    download: int = 0,
):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(
        checklistKey
    )
    item_id = clean_cell_value(itemId)
    archive_version_id = clean_cell_value(
        archiveVersionId
    )
    series_id = clean_cell_value(seriesId)

    if (
        not dialog_id
        or not item_id
        or not archive_version_id
    ):
        return JSONResponse({
            "ok": False,
            "error": (
                "dialogId, itemId and "
                "archiveVersionId are required"
            ),
        }, status_code=400)

    data = get_checklist(
        dialog_id,
        checklist_key,
    )

    target_item = None

    for item in data.get("items", []) or []:
        if clean_cell_value(item.get("id")) == item_id:
            target_item = item
            break

    if not target_item:
        return JSONResponse({
            "ok": False,
            "error": "item not found",
        }, status_code=404)

    archive_entry = find_archive_version_in_item(
        target_item,
        archive_version_id,
        series_id,
    )

    if not archive_entry:
        return JSONResponse({
            "ok": False,
            "error": "archive version not found",
        }, status_code=404)

    archive_version = archive_entry["version"]

    file_url, file_path = resolve_archive_local_file(
        archive_version
    )

    if not file_path or not file_path.exists():
        return JSONResponse({
            "ok": False,
            "error": "archive file not found on disk",
            "fileUrl": file_url,
        }, status_code=404)

    filename = (
        clean_cell_value(
            archive_version.get("name")
        )
        or clean_cell_value(
            archive_version.get("originalName")
        )
        or file_path.name
    )

    media_type, _ = mimetypes.guess_type(
        str(file_path)
    )

    media_type = (
        media_type
        or "application/octet-stream"
    )

    inline_allowed = can_preview_in_browser(
        filename,
        media_type,
    )

    disposition = (
        "attachment"
        if int(download or 0)
        else (
            "inline"
            if inline_allowed
            else "attachment"
        )
    )

    response = FileResponse(
        path=str(file_path),
        media_type=media_type,
    )

    response.headers["Content-Disposition"] = (
        f"{disposition}; "
        f"filename*=UTF-8''{quote(filename)}"
    )

    return response


def archive_edit_session_error_response(
    exc: Exception,
) -> JSONResponse:
    if isinstance(exc, EditSessionNotFoundError):
        status_code = 404
    elif isinstance(exc, EditSessionPermissionError):
        status_code = 403
    elif isinstance(exc, EditSessionConflictError):
        status_code = 409
    elif isinstance(exc, KeyError):
        status_code = 404
    elif isinstance(
        exc,
        (
            ValueError,
            FileNotFoundError,
        ),
    ):
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


@router.post("/api/checklist/delete-archive-version")
async def api_checklist_delete_archive_version(
    request: Request,
):
    try:
        payload = await request.json()
    except Exception:
        raw = await request.body()

        try:
            payload = json.loads(
                raw.decode("utf-8") or "{}"
            )
        except Exception:
            payload = {}

    dialog_id = normalize_dialog_id(
        payload.get("dialogId")
    )

    checklist_key = normalize_checklist_key(
        payload.get("checklistKey")
    )

    item_id = clean_cell_value(
        payload.get("itemId")
    )

    archive_version_id = clean_cell_value(
        payload.get("archiveVersionId")
    )

    series_id = clean_cell_value(
        payload.get("seriesId")
    )

    session_id = clean_cell_value(
        payload.get("sessionId")
    )

    acting_user_id = clean_cell_value(
        payload.get("actingUserId")
    )

    acting_user_name = (
        clean_cell_value(
            payload.get("actingUserName")
        )
        or "Пользователь"
    )

    if (
        not dialog_id
        or not item_id
        or not archive_version_id
    ):
        return JSONResponse({
            "ok": False,
            "error": (
                "dialogId, itemId and "
                "archiveVersionId are required"
            ),
        }, status_code=400)

    if not session_id:
        return JSONResponse(
            {
                "ok": False,
                "error": (
                    "Удаление архивной версии "
                    "доступно только в активной "
                    "сессии редактирования"
                ),
                "editSessionError": True,
                "editSessionRequired": True,
            },
            status_code=409,
        )

    if not can_user_permanently_delete_archive(
        acting_user_id
    ):
        write_debug_log(
            "archive_permanent_delete_forbidden",
            {
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "itemId": item_id,
                "archiveVersionId": (
                    archive_version_id
                ),
                "seriesId": series_id,
                "sessionId": session_id,
                "actingUserId": acting_user_id,
                "actingUserName": acting_user_name,
            },
        )

        return JSONResponse({
            "ok": False,
            "error": (
                "Постоянное удаление архивных "
                "версий доступно только "
                "администраторам"
            ),
        }, status_code=403)

    operation_id = uuid.uuid4().hex
    original_data = None
    file_entry = None

    try:
        acquire_checklist_for_edit_session(
            session_id=session_id,
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            user_id=acting_user_id,
            user_name=acting_user_name,
        )

        original_data = get_checklist(
            dialog_id,
            checklist_key,
        )

        data = deepcopy(original_data)

        ensure_checklist_snapshot(
            session_id=session_id,
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            data=original_data,
        )

        items = data.get("items", []) or []

        target_item = None
        target_item_index = -1

        for index, item in enumerate(items):
            if clean_cell_value(item.get("id")) == item_id:
                target_item = item
                target_item_index = index
                break

        if not target_item:
            raise KeyError("item not found")

        before_item = deepcopy(target_item)

        archive_entry = find_archive_version_in_item(
            target_item,
            archive_version_id,
            series_id,
        )

        if not archive_entry:
            raise KeyError("archive version not found")

        archive_version = archive_entry["version"]

        yandex_delete_status = clean_cell_value(
            archive_version.get(
                "yandexDeleteStatus"
            )
        ).lower()

        if (
            yandex_delete_status
            in ACTIVE_ARCHIVE_YANDEX_DELETE_STATUSES
        ):
            raise EditSessionConflictError(
                "Архивная версия участвует "
                "в незавершённой синхронизации "
                "с Яндекс.Диском"
            )

        file_url, file_path = resolve_archive_local_file(
            archive_version
        )

        local_file_existed = bool(
            file_path
            and file_path.is_file()
        )

        if local_file_existed:
            file_entry = stash_existing_file(
                session_id=session_id,
                operation_id=operation_id,
                operation_type=(
                    "archive_version_delete"
                ),
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                series_id=(
                    clean_cell_value(
                        archive_version.get(
                            "seriesId"
                        )
                    )
                    or series_id
                ),
                document_id=archive_version_id,
                file_path=file_path,
                user_id=acting_user_id,
                metadata={
                    "archiveVersionId": (
                        archive_version_id
                    ),
                    "fileName": clean_cell_value(
                        archive_version.get("name")
                    ),
                    "fileUrl": file_url,
                    "location": clean_cell_value(
                        archive_entry.get("location")
                    ),
                    "deferredPermanentDelete": True,
                },
            )

        (
            updated_item,
            removed_version,
            removed_locations,
        ) = remove_archive_version_from_item(
            target_item,
            archive_version_id,
            series_id,
        )

        if not removed_version:
            raise RuntimeError(
                "archive version disappeared "
                "during deletion"
            )

        items[target_item_index] = updated_item
        data["items"] = items

        normalized_data = normalize_checklist_data(
            data,
            checklist_key,
        )

        saved_data = save_checklist(
            dialog_id,
            normalized_data,
            checklist_key,
        )

        response_item = None

        for item in saved_data.get(
            "items",
            [],
        ):
            if clean_cell_value(item.get("id")) == item_id:
                response_item = item
                break

        operation = record_checklist_operation(
            session_id=session_id,
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            operation_type="archive_version_delete",
            before={
                "item": before_item,
                "archiveVersion": removed_version,
            },
            after={
                "item": response_item,
                "archiveVersion": None,
            },
            final_checklist_data=saved_data,
            item_id=item_id,
            series_id=clean_cell_value(
                removed_version.get("seriesId")
            ),
            document_id=archive_version_id,
            operation_id=operation_id,
            payload={
                "archiveVersionId": (
                    archive_version_id
                ),
                "fileName": clean_cell_value(
                    removed_version.get("name")
                ),
                "fileUrl": file_url,
                "removedLocations": (
                    removed_locations
                ),
                "localFileStashed": bool(
                    file_entry
                ),
                "deferredPermanentDelete": True,
            },
        )

    except Exception as exc:
        if original_data is not None:
            try:
                save_checklist(
                    dialog_id,
                    original_data,
                    checklist_key,
                )
            except Exception as restore_exc:
                write_debug_log(
                    "archive_metadata_restore_failed",
                    {
                        "dialogId": dialog_id,
                        "checklistKey": checklist_key,
                        "itemId": item_id,
                        "archiveVersionId": (
                            archive_version_id
                        ),
                        "sessionId": session_id,
                        "operationId": operation_id,
                        "error": str(restore_exc),
                    },
                )

        try:
            rollback_edit_session_file_operation(
                session_id=session_id,
                operation_id=operation_id,
            )
        except Exception as restore_exc:
            write_debug_log(
                "archive_file_restore_failed",
                {
                    "dialogId": dialog_id,
                    "checklistKey": checklist_key,
                    "itemId": item_id,
                    "archiveVersionId": (
                        archive_version_id
                    ),
                    "sessionId": session_id,
                    "operationId": operation_id,
                    "error": str(restore_exc),
                },
            )

        write_debug_log(
            "archive_transactional_delete_failed",
            {
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "itemId": item_id,
                "archiveVersionId": (
                    archive_version_id
                ),
                "seriesId": series_id,
                "sessionId": session_id,
                "operationId": operation_id,
                "actingUserId": acting_user_id,
                "actingUserName": acting_user_name,
                "error": str(exc),
            },
        )

        return archive_edit_session_error_response(
            exc
        )

    write_debug_log(
        "archive_transactional_delete_staged",
        {
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "itemId": item_id,
            "archiveVersionId": (
                archive_version_id
            ),
            "seriesId": clean_cell_value(
                removed_version.get("seriesId")
            ),
            "sessionId": session_id,
            "operationId": operation_id,
            "actingUserId": acting_user_id,
            "actingUserName": acting_user_name,
            "fileUrl": file_url,
            "localFileExisted": local_file_existed,
            "localFileStashed": bool(file_entry),
            "removedLocations": removed_locations,
        },
    )

    return JSONResponse({
        "ok": True,
        "transactional": True,
        "committed": False,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "itemId": item_id,
        "archiveVersionId": archive_version_id,
        "seriesId": clean_cell_value(
            removed_version.get("seriesId")
        ),
        "sessionId": session_id,
        "operationId": operation_id,
        "operation": operation,
        "removedArchiveVersion": removed_version,
        "removedLocations": removed_locations,
        "localFileExisted": local_file_existed,
        "localFileStashed": bool(file_entry),
        "localFileDeleted": False,
        "fileEntries": (
            [public_file_entry(file_entry)]
            if file_entry
            else []
        ),
        "item": response_item,
        "progressPercent": saved_data.get(
            "progressPercent",
            0,
        ),
    })
