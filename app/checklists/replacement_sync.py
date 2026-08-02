from copy import deepcopy

from app.logging_utils import write_debug_log

from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.checklists.storage import (
    get_checklist,
    save_checklist,
)

from app.checklists.documents import (
    migrate_legacy_document_fields,
    normalize_documents_list,
    normalize_archive_versions,
    normalize_detached_archive_series,
)

from app.checklists.upload_jobs import (
    create_yandex_delete_job,
    get_upload_job,
)

from app.checklists.document_replacements import (
    get_document_replacement_by_upload_job,
    get_document_replacement_by_delete_job,
    update_document_replacement,
    mark_document_replacement_failed,
)

from app.yandex_disk.client import (
    normalize_yandex_disk_path,
)


def normalize_replacement_yandex_path(
    value: str,
) -> str:
    value = clean_cell_value(value)

    if not value:
        return ""

    return normalize_yandex_disk_path(value)


def find_replacement_current_document(
    replacement: dict,
) -> dict | None:
    dialog_id = normalize_dialog_id(
        replacement.get("dialog_id")
    )
    checklist_key = normalize_checklist_key(
        replacement.get("checklist_key")
    )
    item_id = clean_cell_value(
        replacement.get("item_id")
    )
    document_id = clean_cell_value(
        replacement.get("new_document_id")
    )

    if not dialog_id or not item_id or not document_id:
        return None

    data = get_checklist(
        dialog_id,
        checklist_key,
    )

    for raw_item in data.get("items", []) or []:
        if clean_cell_value(raw_item.get("id")) != item_id:
            continue

        item = migrate_legacy_document_fields(
            raw_item
        )

        for document in normalize_documents_list(
            item.get("documents")
        ):
            if (
                clean_cell_value(document.get("id"))
                == document_id
            ):
                return document

    return None


def update_archive_version_yandex_state(
    replacement: dict,
    delete_status: str,
    delete_job_id: str = "",
    delete_error: str = "",
) -> bool:
    dialog_id = normalize_dialog_id(
        replacement.get("dialog_id")
    )
    checklist_key = normalize_checklist_key(
        replacement.get("checklist_key")
    )
    item_id = clean_cell_value(
        replacement.get("item_id")
    )
    archive_version_id = clean_cell_value(
        replacement.get("archive_version_id")
    )

    if (
        not dialog_id
        or not item_id
        or not archive_version_id
    ):
        return False

    data = get_checklist(
        dialog_id,
        checklist_key,
    )

    items = data.get("items", []) or []
    changed = False

    for item_index, raw_item in enumerate(items):
        if clean_cell_value(raw_item.get("id")) != item_id:
            continue

        item = migrate_legacy_document_fields(
            deepcopy(raw_item)
        )

        next_documents = []

        for raw_document in normalize_documents_list(
            item.get("documents")
        ):
            document = dict(raw_document)

            versions = normalize_archive_versions(
                document.get("archiveVersions"),
                series_id=clean_cell_value(
                    document.get("seriesId")
                ),
            )

            next_versions = []

            for raw_version in versions:
                version = dict(raw_version)

                if (
                    clean_cell_value(version.get("id"))
                    == archive_version_id
                ):
                    version["yandexDeleteStatus"] = (
                        clean_cell_value(delete_status)
                    )
                    version["yandexDeleteJobId"] = (
                        clean_cell_value(delete_job_id)
                    )
                    version["yandexDeleteError"] = (
                        clean_cell_value(delete_error)
                    )
                    changed = True

                next_versions.append(version)

            document["archiveVersions"] = (
                normalize_archive_versions(
                    next_versions,
                    series_id=clean_cell_value(
                        document.get("seriesId")
                    ),
                )
            )

            next_documents.append(document)

        item["documents"] = normalize_documents_list(
            next_documents
        )

        detached_series = (
            normalize_detached_archive_series(
                item.get("archivedDocumentSeries")
            )
        )

        next_detached_series = []

        for raw_series in detached_series:
            series = dict(raw_series)

            versions = normalize_archive_versions(
                series.get("archiveVersions"),
                series_id=clean_cell_value(
                    series.get("seriesId")
                ),
            )

            next_versions = []

            for raw_version in versions:
                version = dict(raw_version)

                if (
                    clean_cell_value(version.get("id"))
                    == archive_version_id
                ):
                    version["yandexDeleteStatus"] = (
                        clean_cell_value(delete_status)
                    )
                    version["yandexDeleteJobId"] = (
                        clean_cell_value(delete_job_id)
                    )
                    version["yandexDeleteError"] = (
                        clean_cell_value(delete_error)
                    )
                    changed = True

                next_versions.append(version)

            series["archiveVersions"] = (
                normalize_archive_versions(
                    next_versions,
                    series_id=clean_cell_value(
                        series.get("seriesId")
                    ),
                )
            )

            next_detached_series.append(series)

        item["archivedDocumentSeries"] = (
            normalize_detached_archive_series(
                next_detached_series
            )
        )

        items[item_index] = item
        break

    if not changed:
        write_debug_log(
            "replacement_archive_version_not_found",
            {
                "operationId": replacement.get(
                    "operation_id"
                ),
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "itemId": item_id,
                "archiveVersionId": archive_version_id,
                "deleteStatus": delete_status,
            },
        )
        return False

    data["items"] = items

    save_checklist(
        dialog_id,
        data,
        checklist_key,
    )

    return True


def complete_replacement_without_delete(
    replacement: dict,
    new_yandex_path: str,
    stage: str,
    archive_status: str,
) -> dict:
    update_archive_version_yandex_state(
        replacement=replacement,
        delete_status=archive_status,
        delete_job_id="",
        delete_error="",
    )

    updated = update_document_replacement(
        replacement.get("operation_id"),
        new_yandex_path=new_yandex_path,
        status="completed",
        stage=stage,
        error="",
    )

    return {
        "ok": True,
        "handled": True,
        "action": stage,
        "operationId": replacement.get(
            "operation_id"
        ),
        "deleteJobId": "",
        "replacement": updated or {},
    }


def handle_replacement_upload_job(
    job: dict,
) -> dict:
    job_id = clean_cell_value(job.get("job_id"))

    replacement = (
        get_document_replacement_by_upload_job(
            job_id
        )
    )

    if not replacement:
        return {
            "ok": True,
            "handled": False,
            "jobId": job_id,
        }

    replacement_status = clean_cell_value(
        replacement.get("status")
    ).lower()

    if replacement_status in {
        "completed",
        "cancelled",
    }:
        return {
            "ok": True,
            "handled": True,
            "action": "replacement_already_terminal",
            "operationId": replacement.get(
                "operation_id"
            ),
            "deleteJobId": clean_cell_value(
                replacement.get("delete_job_id")
            ),
        }

    job_status = clean_cell_value(
        job.get("status")
    ).lower()

    job_stage = clean_cell_value(
        job.get("stage")
    )

    job_error = clean_cell_value(
        job.get("error")
    )

    if job_status == "synced":
        current_document = (
            find_replacement_current_document(
                replacement
            )
        )

        if not current_document:
            error = (
                "Новая версия синхронизирована, "
                "но её запись не найдена в чек-листе"
            )

            update_archive_version_yandex_state(
                replacement=replacement,
                delete_status=(
                    "preserved_after_new_document_missing"
                ),
                delete_error=error,
            )

            failed = mark_document_replacement_failed(
                operation_id=replacement.get(
                    "operation_id"
                ),
                error=error,
                stage=(
                    "new_document_missing_after_sync"
                ),
            )

            return {
                "ok": False,
                "handled": True,
                "action": (
                    "new_document_missing_after_sync"
                ),
                "operationId": replacement.get(
                    "operation_id"
                ),
                "deleteJobId": "",
                "replacement": failed or {},
            }

        new_yandex_path = (
            normalize_replacement_yandex_path(
                current_document.get("yandexPath")
            )
        )

        old_yandex_path = (
            normalize_replacement_yandex_path(
                replacement.get("old_yandex_path")
            )
        )

        if not new_yandex_path:
            error = (
                "Upload-job завершён как synced, "
                "но новый yandexPath пуст"
            )

            update_archive_version_yandex_state(
                replacement=replacement,
                delete_status=(
                    "preserved_after_empty_new_yandex_path"
                ),
                delete_error=error,
            )

            failed = mark_document_replacement_failed(
                operation_id=replacement.get(
                    "operation_id"
                ),
                error=error,
                stage="new_yandex_path_empty",
            )

            return {
                "ok": False,
                "handled": True,
                "action": "new_yandex_path_empty",
                "operationId": replacement.get(
                    "operation_id"
                ),
                "deleteJobId": "",
                "replacement": failed or {},
            }

        if not old_yandex_path:
            return complete_replacement_without_delete(
                replacement=replacement,
                new_yandex_path=new_yandex_path,
                stage="old_yandex_path_not_present",
                archive_status="not_required",
            )

        if old_yandex_path == new_yandex_path:
            return complete_replacement_without_delete(
                replacement=replacement,
                new_yandex_path=new_yandex_path,
                stage="same_yandex_path_overwritten",
                archive_status=(
                    "not_required_same_path"
                ),
            )

        existing_delete_job_id = clean_cell_value(
            replacement.get("delete_job_id")
        )

        if existing_delete_job_id:
            update_document_replacement(
                replacement.get("operation_id"),
                new_yandex_path=new_yandex_path,
                status="pending",
                stage="old_yandex_delete_queued",
                error="",
            )

            return {
                "ok": True,
                "handled": True,
                "action": (
                    "existing_old_yandex_delete_job"
                ),
                "operationId": replacement.get(
                    "operation_id"
                ),
                "deleteJobId": existing_delete_job_id,
            }

        delete_job = create_yandex_delete_job(
            dialog_id=replacement.get(
                "dialog_id"
            ),
            checklist_key=replacement.get(
                "checklist_key"
            ),
            item_id=replacement.get("item_id"),
            document_id=replacement.get(
                "old_document_id"
            ),
            file_name=replacement.get(
                "old_file_name"
            ),
            yandex_path=old_yandex_path,
        )

        delete_job_id = clean_cell_value(
            delete_job.get("job_id")
            or delete_job.get("jobId")
        )

        if not delete_job_id:
            error = (
                clean_cell_value(
                    delete_job.get("reason")
                    or delete_job.get("error")
                )
                or (
                    "Не удалось создать delete-job "
                    "старой версии"
                )
            )

            update_archive_version_yandex_state(
                replacement=replacement,
                delete_status="delete_job_create_error",
                delete_error=error,
            )

            failed = mark_document_replacement_failed(
                operation_id=replacement.get(
                    "operation_id"
                ),
                error=error,
                stage="old_delete_job_create_failed",
            )

            return {
                "ok": False,
                "handled": True,
                "action": (
                    "old_delete_job_create_failed"
                ),
                "operationId": replacement.get(
                    "operation_id"
                ),
                "deleteJobId": "",
                "replacement": failed or {},
            }

        updated = update_document_replacement(
            replacement.get("operation_id"),
            new_yandex_path=new_yandex_path,
            delete_job_id=delete_job_id,
            status="pending",
            stage="old_yandex_delete_queued",
            error="",
            finished_at="",
        )

        update_archive_version_yandex_state(
            replacement={
                **replacement,
                "delete_job_id": delete_job_id,
            },
            delete_status="queued",
            delete_job_id=delete_job_id,
            delete_error="",
        )

        return {
            "ok": True,
            "handled": True,
            "action": "old_yandex_delete_created",
            "operationId": replacement.get(
                "operation_id"
            ),
            "deleteJobId": delete_job_id,
            "replacement": updated or {},
        }

    if job_status == "error":
        error = (
            job_error
            or "Ошибка синхронизации новой версии"
        )

        update_archive_version_yandex_state(
            replacement=replacement,
            delete_status=(
                "preserved_after_new_upload_error"
            ),
            delete_error=error,
        )

        failed = mark_document_replacement_failed(
            operation_id=replacement.get(
                "operation_id"
            ),
            error=error,
            stage=(
                job_stage
                or "new_upload_failed"
            ),
        )

        return {
            "ok": False,
            "handled": True,
            "action": "new_upload_failed",
            "operationId": replacement.get(
                "operation_id"
            ),
            "deleteJobId": "",
            "replacement": failed or {},
        }

    if job_status == "cancelled":
        error = (
            job_error
            or "Upload-job новой версии отменён"
        )

        update_archive_version_yandex_state(
            replacement=replacement,
            delete_status=(
                "preserved_after_new_upload_cancelled"
            ),
            delete_error=error,
        )

        updated = update_document_replacement(
            replacement.get("operation_id"),
            status="cancelled",
            stage=(
                job_stage
                or "new_upload_cancelled"
            ),
            error=error,
        )

        return {
            "ok": False,
            "handled": True,
            "action": "new_upload_cancelled",
            "operationId": replacement.get(
                "operation_id"
            ),
            "deleteJobId": "",
            "replacement": updated or {},
        }

    if job_status == "skipped":
        error = (
            job_error
            or (
                "Новая версия не синхронизирована: "
                f"{job_stage or 'upload skipped'}"
            )
        )

        update_archive_version_yandex_state(
            replacement=replacement,
            delete_status=(
                "preserved_after_new_upload_skipped"
            ),
            delete_error=error,
        )

        failed = mark_document_replacement_failed(
            operation_id=replacement.get(
                "operation_id"
            ),
            error=error,
            stage=(
                job_stage
                or "new_upload_skipped"
            ),
        )

        return {
            "ok": False,
            "handled": True,
            "action": "new_upload_skipped",
            "operationId": replacement.get(
                "operation_id"
            ),
            "deleteJobId": "",
            "replacement": failed or {},
        }

    return {
        "ok": True,
        "handled": True,
        "action": "new_upload_not_terminal",
        "operationId": replacement.get(
            "operation_id"
        ),
        "deleteJobId": "",
    }


def handle_replacement_delete_job(
    job: dict,
) -> dict:
    job_id = clean_cell_value(job.get("job_id"))

    replacement = (
        get_document_replacement_by_delete_job(
            job_id
        )
    )

    if not replacement:
        return {
            "ok": True,
            "handled": False,
            "jobId": job_id,
        }

    job_status = clean_cell_value(
        job.get("status")
    ).lower()

    job_stage = clean_cell_value(
        job.get("stage")
    )

    job_error = clean_cell_value(
        job.get("error")
    )

    if job_status == "deleted":
        update_archive_version_yandex_state(
            replacement=replacement,
            delete_status="deleted",
            delete_job_id=job_id,
            delete_error="",
        )

        updated = update_document_replacement(
            replacement.get("operation_id"),
            status="completed",
            stage="old_yandex_deleted",
            error="",
        )

        return {
            "ok": True,
            "handled": True,
            "action": "old_yandex_deleted",
            "operationId": replacement.get(
                "operation_id"
            ),
            "deleteJobId": job_id,
            "replacement": updated or {},
        }

    if job_status == "error":
        error = (
            job_error
            or "Ошибка удаления старой версии"
        )

        update_archive_version_yandex_state(
            replacement=replacement,
            delete_status="error",
            delete_job_id=job_id,
            delete_error=error,
        )

        failed = mark_document_replacement_failed(
            operation_id=replacement.get(
                "operation_id"
            ),
            error=error,
            stage=(
                job_stage
                or "old_yandex_delete_failed"
            ),
        )

        return {
            "ok": False,
            "handled": True,
            "action": "old_yandex_delete_failed",
            "operationId": replacement.get(
                "operation_id"
            ),
            "deleteJobId": job_id,
            "replacement": failed or {},
        }

    if job_status in {
        "skipped",
        "cancelled",
    }:
        error = (
            job_error
            or (
                "Delete-job старой версии "
                f"завершён со статусом {job_status}"
            )
        )

        update_archive_version_yandex_state(
            replacement=replacement,
            delete_status=job_status,
            delete_job_id=job_id,
            delete_error=error,
        )

        failed = mark_document_replacement_failed(
            operation_id=replacement.get(
                "operation_id"
            ),
            error=error,
            stage=(
                job_stage
                or f"old_yandex_delete_{job_status}"
            ),
        )

        return {
            "ok": False,
            "handled": True,
            "action": (
                f"old_yandex_delete_{job_status}"
            ),
            "operationId": replacement.get(
                "operation_id"
            ),
            "deleteJobId": job_id,
            "replacement": failed or {},
        }

    return {
        "ok": True,
        "handled": True,
        "action": "old_delete_not_terminal",
        "operationId": replacement.get(
            "operation_id"
        ),
        "deleteJobId": job_id,
    }


def handle_document_replacement_after_job(
    job_id: str,
) -> dict:
    job_id = clean_cell_value(job_id)

    if not job_id:
        return {
            "ok": False,
            "handled": False,
            "error": "jobId is required",
        }

    job = get_upload_job(job_id)

    if not job:
        return {
            "ok": False,
            "handled": False,
            "error": "job not found",
            "jobId": job_id,
        }

    job_type = clean_cell_value(
        job.get("job_type")
    ).lower()

    try:
        if job_type == "upload":
            return handle_replacement_upload_job(
                job
            )

        if job_type == "delete":
            return handle_replacement_delete_job(
                job
            )

        return {
            "ok": True,
            "handled": False,
            "jobId": job_id,
            "jobType": job_type,
        }

    except Exception as exc:
        replacement = None

        if job_type == "upload":
            replacement = (
                get_document_replacement_by_upload_job(
                    job_id
                )
            )
        elif job_type == "delete":
            replacement = (
                get_document_replacement_by_delete_job(
                    job_id
                )
            )

        if replacement:
            try:
                mark_document_replacement_failed(
                    operation_id=replacement.get(
                        "operation_id"
                    ),
                    error=str(exc),
                    stage=(
                        "replacement_job_callback_error"
                    ),
                )
            except Exception:
                pass

        write_debug_log(
            "replacement_job_callback_failed",
            {
                "jobId": job_id,
                "jobType": job_type,
                "operationId": (
                    replacement.get("operation_id")
                    if replacement
                    else ""
                ),
                "error": str(exc),
            },
        )

        return {
            "ok": False,
            "handled": bool(replacement),
            "jobId": job_id,
            "jobType": job_type,
            "operationId": (
                replacement.get("operation_id")
                if replacement
                else ""
            ),
            "error": str(exc),
        }
