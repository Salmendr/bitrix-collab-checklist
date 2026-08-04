from __future__ import annotations

import json
from typing import Any

from app.db import get_conn
from app.checklists.storage import make_storage_dialog_id
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)
from app.checklists.yandex_structure_jobs import (
    list_latest_yandex_structure_jobs_for_checklist,
)


PUBLIC_YANDEX_FOLDER_STATUSES = frozenset({
    "queued",
    "running",
    "ready",
    "error",
    "conflict",
    "disabled",
})


def public_yandex_folder_status(job_status: str) -> str:
    normalized = clean_cell_value(job_status).lower()
    if normalized == "completed":
        return "ready"
    if normalized == "cancelled":
        return "error"
    if normalized in PUBLIC_YANDEX_FOLDER_STATUSES:
        return normalized
    return ""


def _job_result(job: dict | None) -> dict:
    value = (job or {}).get("result")
    return value if isinstance(value, dict) else {}


def build_yandex_structure_item_fields(
    *,
    item: dict | None = None,
    job: dict | None = None,
) -> dict:
    source_item = dict(item or {})
    source_job = dict(job or {})
    result = _job_result(source_job)

    job_status = public_yandex_folder_status(
        source_job.get("status") or source_item.get("yandexFolderStatus")
    )
    folder_path = clean_cell_value(
        result.get("folderPath")
        or result.get("path")
        or source_item.get("yandexFolderPath")
    )
    folder_url = clean_cell_value(
        result.get("folderUrl")
        or result.get("url")
        or source_item.get("yandexFolderUrl")
    )

    if not job_status and (folder_path or folder_url):
        job_status = "ready"

    error = clean_cell_value(
        source_job.get("error")
        or source_item.get("yandexFolderError")
    )
    if job_status in {"queued", "running", "ready"}:
        error = ""

    return {
        "yandexFolderStatus": job_status,
        "yandexFolderError": error,
        "yandexFolderPath": folder_path,
        "yandexFolderUrl": folder_url,
        "yandexFolderTargetPath": clean_cell_value(
            source_job.get("target_path")
            or source_item.get("yandexFolderTargetPath")
        ),
        "yandexStructureJobId": clean_cell_value(
            source_job.get("job_id")
            or source_item.get("yandexStructureJobId")
        ),
        "yandexStructureAction": clean_cell_value(
            source_job.get("action")
            or source_item.get("yandexStructureAction")
        ),
        "yandexStructureUpdatedAt": clean_cell_value(
            source_job.get("updated_at")
            or source_item.get("yandexStructureUpdatedAt")
        ),
    }


def apply_yandex_structure_job_to_item(
    item: dict,
    job: dict | None,
) -> dict:
    updated = dict(item or {})
    updated.update(
        build_yandex_structure_item_fields(item=updated, job=job)
    )
    return updated


def attach_latest_yandex_structure_states(
    data: dict,
    *,
    dialog_id: str,
    checklist_key: str,
) -> dict:
    enriched = dict(data or {})
    items = [dict(item or {}) for item in (enriched.get("items") or [])]
    latest = list_latest_yandex_structure_jobs_for_checklist(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
    )

    for index, item in enumerate(items):
        item_id = clean_cell_value(item.get("id"))
        items[index] = apply_yandex_structure_job_to_item(
            item,
            latest.get(item_id),
        )

    enriched["items"] = items
    return enriched


def persist_item_yandex_structure_state(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    job: dict | None = None,
    extra_fields: dict[str, Any] | None = None,
) -> bool:
    storage_dialog_id = make_storage_dialog_id(dialog_id, checklist_key)
    normalized_item_id = clean_cell_value(item_id)
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT data_json FROM checklists WHERE dialog_id = ?",
            (storage_dialog_id,),
        ).fetchone()
        if not row:
            conn.commit()
            return False

        try:
            data = json.loads(row["data_json"] or "{}")
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}

        found = False
        items = data.get("items") or []
        for item in items:
            if clean_cell_value(item.get("id")) != normalized_item_id:
                continue
            item.update(build_yandex_structure_item_fields(item=item, job=job))
            if isinstance(extra_fields, dict):
                item.update(extra_fields)
            found = True
            break

        if found:
            data["items"] = items
            conn.execute(
                "UPDATE checklists SET data_json = ? WHERE dialog_id = ?",
                (
                    json.dumps(data, ensure_ascii=False),
                    storage_dialog_id,
                ),
            )
        conn.commit()
        return found
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
