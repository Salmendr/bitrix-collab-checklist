from __future__ import annotations

import json
import threading
from pathlib import Path

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.yandex_disk.client import is_yandex_disk_enabled
from app.checklists.documents import (
    get_upload_file_path_from_url,
    migrate_legacy_document_fields,
    normalize_documents_list,
)
from app.checklists.storage import get_project_storage_context
from app.checklists.upload_jobs import (
    ensure_yandex_upload_job_for_reconciliation,
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
)
from app.checklists.yandex_structure_queue import (
    enqueue_yandex_structure_job,
)

_RECONCILIATION_GUARD = threading.Lock()
_RECONCILIATION_RUNNING = False


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


def reconcile_yandex_mirror_documents(source: str = "startup") -> dict:
    """Restore only missing/retriable current-document mirror jobs.

    The pass is intentionally local-first: it reads SQLite and local files,
    never scans or deletes remote Yandex content, and never touches archives.
    """
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
        "missingLocal": 0,
        "skippedProjects": 0,
        "unrecoverable": 0,
        "structureRepairsQueued": 0,
        "structureRepairsExisting": 0,
        "errors": [],
    }
    context_cache: dict[str, bool] = {}

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

            if dialog_id not in context_cache:
                context = get_project_storage_context(dialog_id) or {}
                context_cache[dialog_id] = _project_uses_yandex_mirror(context)
            if not context_cache[dialog_id]:
                stats["skippedProjects"] += 1
                continue

            # Scan the persisted record directly. Normalization is intentionally
            # avoided here so legacy/custom current documents cannot disappear
            # from a recovery pass merely because their definition changed.
            stats["checklists"] += 1
            for raw_item in raw_data.get("items") or []:
                item = migrate_legacy_document_fields(raw_item)
                item_id = clean_cell_value(item.get("id"))

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
                        # A persisted successful path is sufficient evidence for
                        # local startup reconciliation. This pass does not query
                        # or overwrite Yandex merely to verify remote existence.
                        if (
                            clean_cell_value(document.get("mirrorStatus")).lower()
                            == "synced"
                            and clean_cell_value(document.get("yandexPath"))
                        ):
                            stats["existing"] += 1
                            continue

                        local_path = _document_local_path(document)
                        if local_path is None:
                            stats["missingLocal"] += 1
                            continue

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
                            },
                        )

                        if status == "queued":
                            enqueue_result = enqueue_yandex_mirror_job(
                                job_id,
                                source="startup_reconciliation",
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

    write_debug_log("yandex_mirror_startup_reconciliation_finished", stats)
    return stats


def _run_reconciliation(source: str) -> None:
    global _RECONCILIATION_RUNNING
    try:
        reconcile_yandex_mirror_documents(source=source)
    finally:
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
