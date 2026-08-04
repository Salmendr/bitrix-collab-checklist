from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

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
from app.checklists.yandex_custom_recovery import (
    reconcile_custom_item_yandex_folder,
)
from app.checklists.yandex_mirror_queue import (
    requeue_current_yandex_file_failures,
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


@router.post("/api/checklist/yandex-recovery/recheck")
async def api_recheck_yandex_recovery(request: Request):
    payload = await request.json()
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    item_id = clean_cell_value(payload.get("itemId"))
    if not dialog_id or not item_id:
        return JSONResponse(
            {"ok": False, "error": "dialogId and itemId are required"},
            status_code=400,
        )

    result = await run_in_threadpool(
        reconcile_custom_item_yandex_folder,
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_id=item_id,
        source="manual_conflict_recheck",
        enqueue=True,
    )
    job = result.get("job") or {}
    return JSONResponse({
        "ok": not bool(result.get("conflict")),
        "conflict": bool(result.get("conflict")),
        "job": _public_job(job) if job else {},
        "recovery": result,
    }, status_code=409 if result.get("conflict") else 200)


@router.post("/api/checklist/yandex-recovery/retry")
async def api_retry_yandex_recovery(request: Request):
    payload = await request.json()
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    item_id = clean_cell_value(payload.get("itemId"))
    if not dialog_id or not item_id:
        return JSONResponse(
            {"ok": False, "error": "dialogId and itemId are required"},
            status_code=400,
        )

    latest = get_latest_yandex_structure_job_for_item(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_id=item_id,
    ) or {}
    status = clean_cell_value(latest.get("status")).lower()
    if status == "conflict":
        return JSONResponse({
            "ok": False,
            "conflict": True,
            "error": latest.get("error") or "Yandex folder conflict",
            "job": _public_job(latest),
        }, status_code=409)

    if status in {"error", "disabled", "cancelled"}:
        custom_recovery = await run_in_threadpool(
            reconcile_custom_item_yandex_folder,
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            source="manual_structure_error_recovery",
            enqueue=True,
        )
        custom_job = custom_recovery.get("job") or {}
        if custom_recovery.get("conflict"):
            return JSONResponse({
                "ok": False,
                "conflict": True,
                "job": _public_job(custom_job),
                "recovery": custom_recovery,
            }, status_code=409)
        if custom_job and clean_cell_value(custom_job.get("job_id")) != clean_cell_value(latest.get("job_id")):
            return JSONResponse({
                "ok": True,
                "job": _public_job(custom_job),
                "recovery": custom_recovery,
                "files": {},
            })

        job = retry_yandex_structure_job(latest.get("job_id") or "")
        persist_item_yandex_structure_state(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            job=job,
        )
        enqueue_result = enqueue_yandex_structure_job(
            job.get("job_id") or "",
            source="manual_combined_recovery",
        )
        return JSONResponse({
            "ok": True,
            "job": _public_job(job),
            "enqueue": enqueue_result,
            "files": {},
        })

    if status in {"queued", "running"}:
        return JSONResponse({
            "ok": True,
            "job": _public_job(latest),
            "files": {},
        })

    custom_result = await run_in_threadpool(
        reconcile_custom_item_yandex_folder,
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_id=item_id,
        source="manual_combined_recovery",
        enqueue=True,
    )
    custom_job = custom_result.get("job") or {}
    if custom_result.get("conflict"):
        return JSONResponse({
            "ok": False,
            "conflict": True,
            "job": _public_job(custom_job),
            "recovery": custom_result,
        }, status_code=409)
    if clean_cell_value(custom_job.get("status")) in {"queued", "running"}:
        return JSONResponse({
            "ok": True,
            "job": _public_job(custom_job),
            "recovery": custom_result,
            "files": {},
        })

    files = await run_in_threadpool(
        requeue_current_yandex_file_failures,
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_id=item_id,
        source="manual_combined_recovery",
    )
    latest = (
        get_latest_yandex_structure_job_for_item(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
        )
        or latest
    )
    return JSONResponse({
        "ok": True,
        "job": _public_job(latest) if latest else {},
        "recovery": custom_result,
        "files": files,
    })
