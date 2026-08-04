from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)
from app.checklists.yandex_structure_jobs import (
    get_latest_yandex_structure_job_for_item,
    get_yandex_structure_job,
    retry_yandex_structure_job,
)
from app.checklists.yandex_structure_queue import (
    enqueue_yandex_structure_job,
)
from app.checklists.yandex_structure_state import (
    build_yandex_structure_item_fields,
    persist_item_yandex_structure_state,
)


router = APIRouter()


def _public_job(job: dict | None) -> dict:
    job = job or {}
    return {
        "jobId": job.get("job_id") or "",
        "sessionId": job.get("session_id") or "",
        "operationId": job.get("operation_id") or "",
        "dialogId": job.get("dialog_id") or "",
        "checklistKey": job.get("checklist_key") or "",
        "itemId": job.get("item_id") or "",
        "action": job.get("action") or "",
        "sourcePath": job.get("source_path") or "",
        "targetPath": job.get("target_path") or "",
        "folderAlias": job.get("folder_alias") or "",
        "itemName": job.get("item_name") or "",
        "groupId": int(job.get("group_id") or 0),
        "status": job.get("status") or "",
        "attempts": int(job.get("attempts") or 0),
        "maxAttempts": int(job.get("max_attempts") or 0),
        "error": job.get("error") or "",
        "result": job.get("result") or {},
        "createdAt": job.get("created_at") or "",
        "updatedAt": job.get("updated_at") or "",
        "startedAt": job.get("started_at") or "",
        "finishedAt": job.get("finished_at") or "",
        "folderState": build_yandex_structure_item_fields(job=job),
    }


@router.get("/api/checklist/yandex-structure-job")
async def api_get_yandex_structure_job(
    jobId: str = "",
    dialogId: str = "",
    checklistKey: str = "id",
    itemId: str = "",
):
    job_id = clean_cell_value(jobId)
    if job_id:
        job = get_yandex_structure_job(job_id)
    else:
        dialog_id = normalize_dialog_id(dialogId)
        item_id = clean_cell_value(itemId)
        if not dialog_id or not item_id:
            return JSONResponse(
                {
                    "ok": False,
                    "error": "jobId or dialogId+itemId is required",
                },
                status_code=400,
            )
        job = get_latest_yandex_structure_job_for_item(
            dialog_id=dialog_id,
            checklist_key=normalize_checklist_key(checklistKey),
            item_id=item_id,
        )

    if not job:
        return JSONResponse(
            {"ok": False, "error": "job not found"},
            status_code=404,
        )
    return JSONResponse({"ok": True, "job": _public_job(job)})


@router.post("/api/checklist/yandex-structure-job/retry")
async def api_retry_yandex_structure_job(request: Request):
    payload = await request.json()
    job_id = clean_cell_value(payload.get("jobId"))
    if not job_id:
        return JSONResponse(
            {"ok": False, "error": "jobId is required"},
            status_code=400,
        )

    existing = get_yandex_structure_job(job_id)
    if not existing:
        return JSONResponse(
            {"ok": False, "error": "job not found"},
            status_code=404,
        )

    status = clean_cell_value(existing.get("status"))
    if status not in {"error", "disabled", "cancelled"}:
        return JSONResponse(
            {
                "ok": False,
                "error": f"job cannot be retried from status {status}",
                "job": _public_job(existing),
            },
            status_code=409,
        )

    job = retry_yandex_structure_job(job_id)
    persist_item_yandex_structure_state(
        dialog_id=job.get("dialog_id") or "",
        checklist_key=job.get("checklist_key") or "id",
        item_id=job.get("item_id") or "",
        job=job,
    )
    enqueue_result = enqueue_yandex_structure_job(
        job_id,
        source="manual_retry",
    )
    return JSONResponse({
        "ok": True,
        "job": _public_job(job),
        "enqueue": enqueue_result,
    })
