from __future__ import annotations

import json
import threading
from pathlib import Path

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.yandex_disk.client import (
    is_yandex_disk_enabled,
    normalize_yandex_disk_path,
)
from app.checklists.documents import (
    get_upload_file_path_from_url,
    migrate_legacy_document_fields,
    normalize_documents_list,
)
from app.checklists.storage import get_project_storage_context
from app.checklists.upload_jobs import (
    ensure_yandex_upload_job_for_reconciliation,
    fail_document_upload_job_for_remote_conflict,
    finish_document_upload_job_from_remote_match,
    get_latest_document_job,
    requeue_interrupted_yandex_jobs,
)
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)
from app.checklists.yandex_mirror_queue import (
    enqueue_yandex_mirror_job,
    update_document_mirror_fields,
)
from app.checklists.yandex_folders import (
    build_standard_item_yandex_repair_spec,
)
from app.checklists.yandex_structure_jobs import (
    create_yandex_structure_job,
    retry_yandex_structure_job,
)
from app.checklists.yandex_resource_locks import (
    is_yandex_resource_locked_error,
    yandex_project_resource_guard,
)
from app.checklists.yandex_structure_queue import (
    enqueue_yandex_structure_job,
)
from app.checklists.yandex_custom_recovery import (
    reconcile_custom_item_yandex_folder,
)
from app.checklists.yandex_file_reconciliation import (
    find_existing_yandex_document,
)
from app.checklists.document_replacements import (
    get_document_replacement_by_upload_job,
)

_RECONCILIATION_GUARD = threading.Lock()
_RECONCILIATION_RUNNING = False
_PROJECT_RECONCILIATION_GUARD = threading.Lock()
_PROJECT_RECONCILIATION_RUNNING: set[str] = set()


def _split_storage_key(storage_id: str, data: dict) -> tuple[str, str]:
    raw = clean_cell_value(storage_id)
    if "::" in raw:
        dialog_id, checklist_key = raw.rsplit("::", 1)
        return normalize_dialog_id(dialog_id), normalize_checklist_key(checklist_key)
    return (
        normalize_dialog_id(raw),
        normalize_checklist_key(data.get("checklistKey") or "id"),
    )


def _project_uses_yandex_mirror(context: dict) -> bool:
    mirror_targets = (
        (context.get("storageMode") or {}).get("mirrorTargets")
        or []
    )
    return any(
        clean_cell_value(value).lower() == "yandex_disk"
        for value in mirror_targets
    )


def _document_local_path(document: dict) -> Path | None:
    local_url = clean_cell_value(
        document.get("fileUrl")
        or document.get("path")
        or document.get("previewUrl")
    )
    local_path = get_upload_file_path_from_url(local_url)
    if not local_path:
        return None
    try:
        path = Path(local_path)
    except Exception:
        return None
    return path if path.is_file() else None


def _record_error(
    stats: dict,
    *,
    storage_id: str,
    item_id: str = "",
    document_id: str = "",
    error: Exception | str,
) -> None:
    stats["ok"] = False
    stats["errors"].append({
        "storageId": clean_cell_value(storage_id),
        "itemId": clean_cell_value(item_id),
        "documentId": clean_cell_value(document_id),
        "error": str(error),
    })


def _pending_replacement_may_overwrite_old_path(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    document_id: str,
    remote_result: dict,
) -> bool:
    """Allow only the explicit same-name replacement of its own old path."""
    job = get_latest_document_job(
        dialog_id,
        checklist_key,
        item_id,
        document_id,
    ) or {}
    if clean_cell_value(job.get("job_type")).lower() != "upload":
        return False
    replacement = get_document_replacement_by_upload_job(
        clean_cell_value(job.get("job_id"))
    ) or {}
    if clean_cell_value(replacement.get("status")).lower() != "pending":
        return False
    if (
        clean_cell_value(replacement.get("old_file_name")).casefold()
        != clean_cell_value(replacement.get("new_file_name")).casefold()
    ):
        return False
    old_path = normalize_yandex_disk_path(
        clean_cell_value(replacement.get("old_yandex_path"))
    ).rstrip("/").casefold()
    if not old_path:
        return False
    remote_paths = {
        normalize_yandex_disk_path(candidate.get("path") or "")
        .rstrip("/")
        .casefold()
        for candidate in (
            list(remote_result.get("matches") or [])
            + list(remote_result.get("conflicts") or [])
        )
        if isinstance(candidate, dict)
    }
    return remote_paths == {old_path}


def reconcile_yandex_mirror_documents(
    source: str = "startup",
    *,
    dialog_id: str = "",
    checklist_key: str = "",
    item_id: str = "",
) -> dict:
    """Verify and restore current-document mirrors from local primary files.

    The pass never deletes or blindly overwrites remote Yandex content. It
    probes every current/admissible legacy path linked to the checklist item,
    compares name, size and checksum, and queues upload only after confirmed
    absence from all known locations.
    """
    requested_dialog_id = normalize_dialog_id(dialog_id)
    requested_checklist_key = (
        normalize_checklist_key(checklist_key)
        if clean_cell_value(checklist_key)
        else ""
    )
    requested_item_id = clean_cell_value(item_id)
    if not is_yandex_disk_enabled():
        return {
            "ok": True,
            "source": source,
            "skipped": True,
            "reason": "yandex_disk_disabled",
        }

    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT dialog_id, data_json FROM checklists ORDER BY dialog_id"
        ).fetchall()
    finally:
        conn.close()

    stats = {
        "ok": True,
        "source": source,
        "checklists": 0,
        "documents": 0,
        "queued": 0,
        "existing": 0,
        "remoteVerified": 0,
        "remoteMatchedByDiscovery": 0,
        "remoteMissing": 0,
        "remotePathMissing": 0,
        "remoteFileConflicts": 0,
        "remoteCandidatesChecked": 0,
        "remoteSearchUnavailable": 0,
        "explicitReplacementUploads": 0,
        "missingLocal": 0,
        "skippedProjects": 0,
        "unrecoverable": 0,
        "structureRepairsQueued": 0,
        "structureRepairsExisting": 0,
        "customRepairsQueued": 0,
        "customRepairsCompleted": 0,
        "customConflicts": 0,
        "errors": [],
    }
    context_cache: dict[str, dict] = {}

    for row in rows:
        storage_id = clean_cell_value(row["dialog_id"])
        try:
            raw_data = json.loads(row["data_json"] or "{}")
            if not isinstance(raw_data, dict):
                continue
            dialog_id, checklist_key = _split_storage_key(
                storage_id,
                raw_data,
            )
            if not dialog_id:
                continue
            if requested_dialog_id and dialog_id != requested_dialog_id:
                continue
            if (
                requested_checklist_key
                and checklist_key != requested_checklist_key
            ):
                continue

            if dialog_id not in context_cache:
                context_cache[dialog_id] = (
                    get_project_storage_context(dialog_id) or {}
                )
            context = context_cache[dialog_id]
            if not _project_uses_yandex_mirror(context):
                stats["skippedProjects"] += 1
                continue

            # Scan the persisted record directly. Normalization is intentionally
            # avoided here so legacy/custom current documents cannot disappear
            # from a recovery pass merely because their definition changed.
            stats["checklists"] += 1
            for raw_item in raw_data.get("items") or []:
                item = migrate_legacy_document_fields(raw_item)
                item_id = clean_cell_value(item.get("id"))
                if requested_item_id and item_id != requested_item_id:
                    continue

                custom_recovery: dict = {}
                if bool(item.get("isCustom", False)):
                    try:
                        custom_recovery = reconcile_custom_item_yandex_folder(
                            dialog_id=dialog_id,
                            checklist_key=checklist_key,
                            item_id=item_id,
                            item=item,
                            source=source,
                            enqueue=True,
                        )
                        if custom_recovery.get("conflict"):
                            stats["customConflicts"] += 1
                        elif custom_recovery.get("completed"):
                            stats["customRepairsCompleted"] += 1
                        elif custom_recovery.get("queued"):
                            stats["customRepairsQueued"] += 1
                    except Exception as exc:
                        _record_error(
                            stats,
                            storage_id=storage_id,
                            item_id=item_id,
                            error=exc,
                        )

                repair_spec = build_standard_item_yandex_repair_spec(
                    dialog_id=dialog_id,
                    checklist_key=checklist_key,
                    item=item,
                )
                if repair_spec.get("repairRequired"):
                    repair_action = clean_cell_value(
                        repair_spec.get("repairAction")
                    ) or "rename_item_folder"
                    repair_key = (
                        "startup-standard-item-repair:v1:"
                        f"{dialog_id}:{checklist_key}:{item_id}:"
                        f"{repair_action}:"
                        f"{clean_cell_value(repair_spec.get('targetPath'))}"
                    )
                    repair_job = create_yandex_structure_job(
                        idempotency_key=repair_key,
                        dialog_id=dialog_id,
                        checklist_key=checklist_key,
                        item_id=item_id,
                        action=repair_action,
                        source_path=clean_cell_value(
                            repair_spec.get("sourcePath")
                        ),
                        target_path=clean_cell_value(
                            repair_spec.get("targetPath")
                        ),
                        folder_alias=clean_cell_value(
                            repair_spec.get("folderAlias")
                        ),
                        item_name=clean_cell_value(
                            repair_spec.get("itemName")
                        ),
                        group_id=int(
                            repair_spec.get("targetGroupId")
                            or item.get("group")
                            or 0
                        ),
                        initial_status="queued",
                        result={
                            "sourceGroupId": int(
                                repair_spec.get("sourceGroupId") or 0
                            ),
                            "targetGroupId": int(
                                repair_spec.get("targetGroupId") or 0
                            ),
                            "oldName": clean_cell_value(
                                repair_spec.get("definitionName")
                            ),
                            "finalName": clean_cell_value(
                                repair_spec.get("itemName")
                            ),
                            "startupRepair": True,
                        },
                    )
                    repair_status = clean_cell_value(
                        repair_job.get("status")
                    ).lower()
                    if (
                        repair_status == "error"
                        and is_yandex_resource_locked_error(
                            repair_job.get("error")
                        )
                    ):
                        repair_job = (
                            retry_yandex_structure_job(
                                repair_job.get("job_id") or ""
                            )
                            or repair_job
                        )
                        repair_status = clean_cell_value(
                            repair_job.get("status")
                        ).lower()
                    if repair_status == "queued":
                        enqueue_result = enqueue_yandex_structure_job(
                            clean_cell_value(repair_job.get("job_id")),
                            source="startup_standard_item_repair",
                        )
                        if (
                            enqueue_result.get("queued")
                            or enqueue_result.get("alreadyQueued")
                            or enqueue_result.get("alreadyRunning")
                        ):
                            stats["structureRepairsQueued"] += 1
                    else:
                        stats["structureRepairsExisting"] += 1

                for document in normalize_documents_list(item.get("documents")):
                    stats["documents"] += 1
                    document_id = clean_cell_value(document.get("id"))
                    try:
                        local_path = _document_local_path(document)
                        if local_path is None:
                            stats["missingLocal"] += 1
                            continue

                        stored_yandex_path = clean_cell_value(
                            document.get("yandexPath")
                        )
                        if not stored_yandex_path:
                            stats["remotePathMissing"] += 1

                        with yandex_project_resource_guard(
                            dialog_id,
                            checklist_key=checklist_key,
                            item_id=item_id,
                            operation="reconcile_file_discovery",
                        ):
                            remote_result = find_existing_yandex_document(
                                dialog_id=dialog_id,
                                checklist_key=checklist_key,
                                item=item,
                                document=document,
                                local_path=local_path,
                                context=context,
                                repair_spec=repair_spec,
                                custom_recovery=custom_recovery,
                            )

                        checked_paths = list(
                            remote_result.get("checkedPaths") or []
                        )
                        stats["remoteCandidatesChecked"] += len(checked_paths)
                        remote_status = clean_cell_value(
                            remote_result.get("status")
                        ).lower()
                        explicit_replacement_override = False

                        if remote_status == "unavailable":
                            error = clean_cell_value(
                                remote_result.get("error")
                            ) or "Yandex remote verification failed"
                            fail_document_upload_job_for_remote_conflict(
                                dialog_id=dialog_id,
                                checklist_key=checklist_key,
                                item_id=item_id,
                                document_id=document_id,
                                error=error,
                            )
                            update_document_mirror_fields(
                                dialog_id,
                                checklist_key,
                                item_id,
                                document_id,
                                {
                                    "mirrorStatus": "error",
                                    "mirrorError": error,
                                },
                            )
                            stats["remoteSearchUnavailable"] += 1
                            write_debug_log(
                                "yandex_file_reconciliation_unavailable",
                                {
                                    "dialogId": dialog_id,
                                    "checklistKey": checklist_key,
                                    "itemId": item_id,
                                    "documentId": document_id,
                                    "fileName": clean_cell_value(
                                        document.get("name")
                                    ),
                                    "probeErrors": (
                                        remote_result.get("probeErrors") or []
                                    ),
                                },
                            )
                            continue

                        if remote_status == "matched":
                            match = remote_result.get("match") or {}
                            matched_path = clean_cell_value(match.get("path"))
                            job_update = finish_document_upload_job_from_remote_match(
                                dialog_id=dialog_id,
                                checklist_key=checklist_key,
                                item_id=item_id,
                                document_id=document_id,
                                yandex_path=matched_path,
                            )
                            matched_job = job_update.get("job") or {}
                            update_document_mirror_fields(
                                dialog_id,
                                checklist_key,
                                item_id,
                                document_id,
                                {
                                    "mirrorStatus": "synced",
                                    "mirrorError": "",
                                    "mirrorJobId": clean_cell_value(
                                        matched_job.get("job_id")
                                    ) or clean_cell_value(
                                        document.get("mirrorJobId")
                                    ),
                                    "yandexPath": matched_path,
                                },
                            )
                            stats["existing"] += 1
                            stats["remoteVerified"] += 1
                            if (
                                normalize_yandex_disk_path(stored_yandex_path)
                                != normalize_yandex_disk_path(matched_path)
                            ):
                                stats["remoteMatchedByDiscovery"] += 1
                            continue

                        if remote_status == "conflict":
                            if _pending_replacement_may_overwrite_old_path(
                                dialog_id=dialog_id,
                                checklist_key=checklist_key,
                                item_id=item_id,
                                document_id=document_id,
                                remote_result=remote_result,
                            ):
                                stats["explicitReplacementUploads"] += 1
                                explicit_replacement_override = True
                            else:
                                error = clean_cell_value(
                                    remote_result.get("error")
                                ) or "Yandex remote file conflict"
                                fail_document_upload_job_for_remote_conflict(
                                    dialog_id=dialog_id,
                                    checklist_key=checklist_key,
                                    item_id=item_id,
                                    document_id=document_id,
                                    error=error,
                                )
                                update_document_mirror_fields(
                                    dialog_id,
                                    checklist_key,
                                    item_id,
                                    document_id,
                                    {
                                        "mirrorStatus": "error",
                                        "mirrorError": error,
                                    },
                                )
                                stats["remoteFileConflicts"] += 1
                                write_debug_log(
                                    "yandex_file_reconciliation_conflict",
                                    {
                                        "dialogId": dialog_id,
                                        "checklistKey": checklist_key,
                                        "itemId": item_id,
                                        "documentId": document_id,
                                        "fileName": clean_cell_value(
                                            document.get("name")
                                        ),
                                        "matches": remote_result.get("matches") or [],
                                        "conflicts": remote_result.get("conflicts") or [],
                                    },
                                )
                                continue

                        if remote_status not in {"missing", "conflict"}:
                            error = (
                                "Не удалось подтвердить отсутствие файла на "
                                "Яндекс.Диске. Автоматическая загрузка остановлена."
                            )
                            fail_document_upload_job_for_remote_conflict(
                                dialog_id=dialog_id,
                                checklist_key=checklist_key,
                                item_id=item_id,
                                document_id=document_id,
                                error=error,
                            )
                            update_document_mirror_fields(
                                dialog_id,
                                checklist_key,
                                item_id,
                                document_id,
                                {
                                    "mirrorStatus": "error",
                                    "mirrorError": error,
                                },
                            )
                            stats["remoteSearchUnavailable"] += 1
                            continue

                        if not checked_paths:
                            error = (
                                "Не удалось определить ни одной допустимой "
                                "папки Яндекс.Диска для проверки файла. "
                                "Автоматическая загрузка остановлена."
                            )
                            fail_document_upload_job_for_remote_conflict(
                                dialog_id=dialog_id,
                                checklist_key=checklist_key,
                                item_id=item_id,
                                document_id=document_id,
                                error=error,
                            )
                            update_document_mirror_fields(
                                dialog_id,
                                checklist_key,
                                item_id,
                                document_id,
                                {
                                    "mirrorStatus": "error",
                                    "mirrorError": error,
                                },
                            )
                            stats["remoteSearchUnavailable"] += 1
                            continue

                        # Every known current/legacy path was probed and none
                        # contains this local file. Only now may recovery upload.
                        force_requeue = True
                        if not explicit_replacement_override:
                            stats["remoteMissing"] += 1

                        job = ensure_yandex_upload_job_for_reconciliation(
                            dialog_id=dialog_id,
                            checklist_key=checklist_key,
                            item_id=item_id,
                            document_id=document_id,
                            local_path=str(local_path),
                            file_name=(
                                clean_cell_value(document.get("name"))
                                or local_path.name
                            ),
                            file_size=int(local_path.stat().st_size),
                            force_requeue_synced=force_requeue,
                        )
                        status = clean_cell_value(job.get("status")).lower()
                        action = clean_cell_value(job.get("reconciledAction"))
                        job_id = clean_cell_value(
                            job.get("job_id") or job.get("jobId")
                        )

                        if status == "synced":
                            stats["existing"] += 1
                            if (
                                clean_cell_value(document.get("mirrorStatus")).lower()
                                != "synced"
                            ):
                                update_document_mirror_fields(
                                    dialog_id,
                                    checklist_key,
                                    item_id,
                                    document_id,
                                    {
                                        "mirrorStatus": "synced",
                                        "mirrorError": "",
                                        "mirrorJobId": job_id,
                                        "yandexPath": (
                                            clean_cell_value(job.get("yandex_path"))
                                            or clean_cell_value(
                                                document.get("yandexPath")
                                            )
                                        ),
                                    },
                                )
                            continue

                        if action.startswith("unrecoverable_"):
                            stats["unrecoverable"] += 1
                            continue

                        if status not in {"queued", "running"}:
                            stats["unrecoverable"] += 1
                            continue

                        if action.startswith("existing_"):
                            stats["existing"] += 1

                        update_document_mirror_fields(
                            dialog_id,
                            checklist_key,
                            item_id,
                            document_id,
                            {
                                "mirrorStatus": status,
                                "mirrorError": "",
                                "mirrorJobId": job_id,
                                **({
                                    "yandexPath": "",
                                    "yandexFileUrl": "",
                                } if force_requeue else {}),
                            },
                        )

                        if status == "queued":
                            enqueue_result = enqueue_yandex_mirror_job(
                                job_id,
                                source=f"{source}_reconciliation",
                            )
                            if enqueue_result.get("queued"):
                                stats["queued"] += 1
                    except Exception as exc:
                        _record_error(
                            stats,
                            storage_id=storage_id,
                            item_id=item_id,
                            document_id=document_id,
                            error=exc,
                        )
        except Exception as exc:
            _record_error(
                stats,
                storage_id=storage_id,
                error=exc,
            )

    write_debug_log("yandex_mirror_reconciliation_finished", stats)
    return stats


def _run_reconciliation(source: str) -> None:
    global _RECONCILIATION_RUNNING
    try:
        requeue_interrupted_yandex_jobs()
        reconcile_yandex_mirror_documents(source=source)
    finally:
        # Dispatch legacy deletes/replacements and any still-queued uploads
        # only after folder dependencies have been reconstructed.
        try:
            from app.checklists.yandex_mirror_queue import (
                recover_yandex_mirror_state_on_startup,
            )
            recover_yandex_mirror_state_on_startup(
                source=f"{source}_after_reconciliation"
            )
        except Exception as exc:
            write_debug_log(
                "yandex_mirror_post_reconciliation_recovery_failed",
                {"source": source, "error": str(exc)},
            )
        with _RECONCILIATION_GUARD:
            _RECONCILIATION_RUNNING = False


def start_yandex_mirror_reconciliation(source: str = "startup") -> dict:
    global _RECONCILIATION_RUNNING
    with _RECONCILIATION_GUARD:
        if _RECONCILIATION_RUNNING:
            return {"ok": True, "started": False, "alreadyRunning": True}
        _RECONCILIATION_RUNNING = True

    thread = threading.Thread(
        target=_run_reconciliation,
        args=(source,),
        daemon=True,
        name="yandex-mirror-reconciliation",
    )
    thread.start()
    return {"ok": True, "started": True, "source": source}


def _run_project_file_reconciliation(
    dialog_id: str,
    source: str,
) -> None:
    try:
        reconcile_yandex_mirror_documents(
            source=source,
            dialog_id=dialog_id,
        )
    except Exception as exc:
        write_debug_log("yandex_project_file_reconciliation_failed", {
            "dialogId": dialog_id,
            "source": source,
            "error": str(exc),
        })
    finally:
        with _PROJECT_RECONCILIATION_GUARD:
            _PROJECT_RECONCILIATION_RUNNING.discard(dialog_id)


def start_yandex_project_file_reconciliation(
    dialog_id: str,
    *,
    source: str = "project_context_ready",
) -> dict:
    """Start one idempotent local-to-Yandex backfill for a ready project."""
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    if not normalized_dialog_id:
        return {
            "ok": False,
            "started": False,
            "error": "dialogId is required",
        }

    with _PROJECT_RECONCILIATION_GUARD:
        if normalized_dialog_id in _PROJECT_RECONCILIATION_RUNNING:
            return {
                "ok": True,
                "started": False,
                "alreadyRunning": True,
                "dialogId": normalized_dialog_id,
            }
        _PROJECT_RECONCILIATION_RUNNING.add(normalized_dialog_id)

    thread = threading.Thread(
        target=_run_project_file_reconciliation,
        args=(normalized_dialog_id, clean_cell_value(source) or "project_ready"),
        daemon=True,
        name=f"yandex-file-reconcile-{normalized_dialog_id[-20:]}",
    )
    thread.start()
    return {
        "ok": True,
        "started": True,
        "dialogId": normalized_dialog_id,
        "source": clean_cell_value(source) or "project_ready",
    }
