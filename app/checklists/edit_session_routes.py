import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.logging_utils import write_debug_log

from app.checklists.edit_sessions import (
    EditSessionConflictError,
    EditSessionInactivityExpiredError,
    EditSessionNotFoundError,
    EditSessionPermissionError,
    commit_edit_session,
    get_edit_session_for_actor,
    heartbeat_edit_session,
    public_edit_session_payload,
    rollback_edit_session,
    start_edit_session,
)

from app.checklists.edit_session_locks import (
    heartbeat_all_edit_session_locks,
    list_edit_session_locks,
)
from app.checklists.edit_session_changes import (
    list_edit_session_operations,
)
from app.checklists.edit_session_files import (
    list_edit_session_file_entries,
)
from app.checklists.edit_session_yandex import (
    list_edit_session_yandex_jobs,
)
from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
)


router = APIRouter()


def public_edit_session_with_locks(
    session: dict | None,
) -> dict:
    payload = public_edit_session_payload(session)
    session_id = payload.get("sessionId") or ""

    locks = (
        list_edit_session_locks(session_id)
        if session_id
        else []
    )

    payload["locks"] = locks
    payload["lockCount"] = len(locks)

    return payload


async def read_json_payload(request: Request) -> dict:
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

    return payload if isinstance(payload, dict) else {}


def session_error_response(exc: Exception) -> JSONResponse:
    if isinstance(exc, EditSessionNotFoundError):
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=404,
        )

    if isinstance(exc, EditSessionPermissionError):
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=403,
        )

    if isinstance(exc, EditSessionConflictError):
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=409,
        )

    if isinstance(exc, ValueError):
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=400,
        )

    return JSONResponse(
        {"ok": False, "error": str(exc)},
        status_code=500,
    )


@router.post("/api/checklist/session/start")
async def api_checklist_edit_session_start(
    request: Request,
):
    payload = await read_json_payload(request)

    dialog_id = normalize_dialog_id(
        payload.get("dialogId")
    )
    user_id = clean_cell_value(
        payload.get("userId")
    )
    user_name = clean_cell_value(
        payload.get("userName")
    )
    client_session_id = clean_cell_value(
        payload.get("clientSessionId")
    )
    metadata = payload.get("metadata")

    if not isinstance(metadata, dict):
        metadata = {}

    try:
        result = start_edit_session(
            dialog_id=dialog_id,
            user_id=user_id,
            user_name=user_name,
            client_session_id=client_session_id,
            metadata=metadata,
        )

        response = {
            "ok": True,
            "created": bool(result.get("created")),
            "resumed": bool(result.get("resumed")),
            "recovered": bool(result.get("recovered")),
            "session": public_edit_session_with_locks(
                result.get("session")
            ),
        }

        write_debug_log(
            "edit_session_started",
            response,
        )

        return JSONResponse(response)

    except Exception as exc:
        write_debug_log(
            "edit_session_start_failed",
            {
                "dialogId": dialog_id,
                "userId": user_id,
                "clientSessionId": client_session_id,
                "error": str(exc),
            },
        )
        return session_error_response(exc)


@router.post("/api/checklist/session/heartbeat")
async def api_checklist_edit_session_heartbeat(
    request: Request,
):
    payload = await read_json_payload(request)

    session_id = clean_cell_value(
        payload.get("sessionId")
    )
    dialog_id = normalize_dialog_id(
        payload.get("dialogId")
    )
    user_id = clean_cell_value(
        payload.get("userId")
    )
    client_session_id = clean_cell_value(
        payload.get("clientSessionId")
    )
    last_activity_at = clean_cell_value(
        payload.get("lastActivityAt")
    )

    try:
        existing_session = get_edit_session_for_actor(
            session_id=session_id,
            dialog_id=dialog_id,
            user_id=user_id,
        )
        if (
            clean_cell_value(existing_session.get("status")) == "committed"
            and clean_cell_value(existing_session.get("close_reason"))
                == "inactivity_timeout"
        ):
            return JSONResponse({
                "ok": True,
                "session": public_edit_session_with_locks(existing_session),
                "inactivityFinalized": True,
            })

        session = heartbeat_edit_session(
            session_id=session_id,
            dialog_id=dialog_id,
            user_id=user_id,
            client_session_id=client_session_id,
            last_activity_at=last_activity_at,
        )

        heartbeat_all_edit_session_locks(
            session_id=session_id,
            dialog_id=dialog_id,
            user_id=user_id,
        )

        return JSONResponse({
            "ok": True,
            "session": public_edit_session_with_locks(
                session
            ),
            "inactivityFinalized": False,
        })

    except EditSessionInactivityExpiredError:
        # A live browser must run the normal Save-and-Close flow so its upload
        # manager can reach idle first. The background sweeper finalizes only
        # when this heartbeat itself has gone stale (frozen/closed browser).
        current = get_edit_session_for_actor(
            session_id=session_id,
            dialog_id=dialog_id,
            user_id=user_id,
        )
        return JSONResponse({
            "ok": True,
            "session": public_edit_session_with_locks(current),
            "inactivityDue": True,
            "inactivityFinalized": False,
        })

    except Exception as exc:
        write_debug_log(
            "edit_session_heartbeat_failed",
            {
                "sessionId": session_id,
                "dialogId": dialog_id,
                "userId": user_id,
                "error": str(exc),
            },
        )
        return session_error_response(exc)


@router.get("/api/checklist/session/status")
async def api_checklist_edit_session_status(
    request: Request,
):
    session_id = clean_cell_value(
        request.query_params.get("sessionId")
    )
    dialog_id = normalize_dialog_id(
        request.query_params.get("dialogId")
    )
    user_id = clean_cell_value(
        request.query_params.get("userId")
    )

    try:
        session = get_edit_session_for_actor(
            session_id=session_id,
            dialog_id=dialog_id,
            user_id=user_id,
        )

        return JSONResponse({
            "ok": True,
            "session": public_edit_session_with_locks(
                session
            ),
        })

    except Exception as exc:
        return session_error_response(exc)


@router.get("/api/checklist/session/locks")
async def api_checklist_edit_session_locks(
    request: Request,
):
    session_id = clean_cell_value(
        request.query_params.get("sessionId")
    )
    dialog_id = normalize_dialog_id(
        request.query_params.get("dialogId")
    )
    user_id = clean_cell_value(
        request.query_params.get("userId")
    )

    try:
        locks = list_edit_session_locks(
            session_id=session_id,
            dialog_id=dialog_id,
            user_id=user_id,
        )

        return JSONResponse({
            "ok": True,
            "sessionId": session_id,
            "lockCount": len(locks),
            "locks": locks,
        })

    except Exception as exc:
        return session_error_response(exc)


@router.get("/api/checklist/session/changes")
async def api_checklist_edit_session_changes(
    request: Request,
):
    session_id = clean_cell_value(
        request.query_params.get("sessionId")
    )
    dialog_id = normalize_dialog_id(
        request.query_params.get("dialogId")
    )
    user_id = clean_cell_value(
        request.query_params.get("userId")
    )

    try:
        operations = list_edit_session_operations(
            session_id=session_id,
            dialog_id=dialog_id,
            user_id=user_id,
        )

        return JSONResponse({
            "ok": True,
            "sessionId": session_id,
            "operationCount": len(operations),
            "operations": operations,
        })

    except Exception as exc:
        return session_error_response(exc)


@router.get("/api/checklist/session/files")
async def api_checklist_edit_session_files(
    request: Request,
):
    session_id = clean_cell_value(
        request.query_params.get("sessionId")
    )
    dialog_id = normalize_dialog_id(
        request.query_params.get("dialogId")
    )
    user_id = clean_cell_value(
        request.query_params.get("userId")
    )

    try:
        files = list_edit_session_file_entries(
            session_id=session_id,
            dialog_id=dialog_id,
            user_id=user_id,
        )
        return JSONResponse({
            "ok": True,
            "sessionId": session_id,
            "fileEntryCount": len(files),
            "files": files,
        })
    except Exception as exc:
        return session_error_response(exc)


@router.get("/api/checklist/session/yandex-jobs")
async def api_checklist_edit_session_yandex_jobs(
    request: Request,
):
    session_id = clean_cell_value(
        request.query_params.get("sessionId")
    )
    dialog_id = normalize_dialog_id(
        request.query_params.get("dialogId")
    )
    user_id = clean_cell_value(
        request.query_params.get("userId")
    )

    try:
        jobs = list_edit_session_yandex_jobs(
            session_id=session_id,
            dialog_id=dialog_id,
            user_id=user_id,
        )
        return JSONResponse({
            "ok": True,
            "sessionId": session_id,
            "jobCount": len(jobs),
            "jobs": jobs,
        })
    except Exception as exc:
        return session_error_response(exc)


@router.post("/api/checklist/session/commit")
async def api_checklist_edit_session_commit(
    request: Request,
):
    payload = await read_json_payload(request)

    session_id = clean_cell_value(
        payload.get("sessionId")
    )
    dialog_id = normalize_dialog_id(
        payload.get("dialogId")
    )
    user_id = clean_cell_value(
        payload.get("userId")
    )
    reason = clean_cell_value(
        payload.get("reason")
    ) or "save_and_close"

    try:
        session = commit_edit_session(
            session_id=session_id,
            dialog_id=dialog_id,
            user_id=user_id,
            reason=reason,
        )

        response = {
            "ok": True,
            "committed": (
                session.get("status") == "committed"
            ),
            "session": public_edit_session_with_locks(
                session
            ),
        }

        write_debug_log(
            "edit_session_committed",
            response,
        )

        return JSONResponse(response)

    except Exception as exc:
        write_debug_log(
            "edit_session_commit_failed",
            {
                "sessionId": session_id,
                "dialogId": dialog_id,
                "userId": user_id,
                "error": str(exc),
            },
        )
        return session_error_response(exc)


@router.post("/api/checklist/session/rollback")
async def api_checklist_edit_session_rollback(
    request: Request,
):
    payload = await read_json_payload(request)

    session_id = clean_cell_value(
        payload.get("sessionId")
    )
    dialog_id = normalize_dialog_id(
        payload.get("dialogId")
    )
    user_id = clean_cell_value(
        payload.get("userId")
    )
    reason = clean_cell_value(
        payload.get("reason")
    )
    confirmed = payload.get("confirmed") is True

    try:
        if reason != "cancel_button" or not confirmed:
            raise EditSessionConflictError(
                "rollback requires an explicit confirmed Cancel action"
            )

        session = rollback_edit_session(
            session_id=session_id,
            dialog_id=dialog_id,
            user_id=user_id,
            reason=reason,
        )

        response = {
            "ok": True,
            "rolledBack": (
                session.get("status") == "rolled_back"
            ),
            "session": public_edit_session_with_locks(
                session
            ),
        }

        write_debug_log(
            "edit_session_rolled_back",
            response,
        )

        return JSONResponse(response)

    except Exception as exc:
        write_debug_log(
            "edit_session_rollback_failed",
            {
                "sessionId": session_id,
                "dialogId": dialog_id,
                "userId": user_id,
                "error": str(exc),
            },
        )
        return session_error_response(exc)
