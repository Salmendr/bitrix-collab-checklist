import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.bitrix.client import bitrix_webhook_call
from app.logging_utils import write_debug_log

from app.checklists.utils import (
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.checklists.storage import (
    save_checklist,
    get_checklist,
)

from app.checklists.normalization import normalize_checklist_data

from app.checklists.messages import (
    build_recent_changes_sections,
    build_multi_checklist_chat_message,
    build_checklist_chat_message,
)

from app.checklists.edit_sessions import (
    EditSessionConflictError,
    EditSessionNotFoundError,
    EditSessionPermissionError,
)
from app.checklists.session_finalization import (
    SessionFinalizationInProgressError,
    SessionFinalizationPayloadConflictError,
    finalize_edit_session_payload,
)


router = APIRouter()

@router.post("/api/checklist/close-session")
async def api_checklist_close_session(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raw = await request.body()
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            payload = {"raw": raw.decode("utf-8", errors="ignore")}

    write_debug_log("close_session_received", payload)

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    editor = payload.get("editor") or {}
    raw_sessions = payload.get("sessions") or []

    def build_message_failure_response(base_dialog_id: str, extra: dict | None = None):
        response = {
            "ok": True,
            "saved": True,
            "messageOk": False,
            "dialogId": base_dialog_id,
        }
        if extra:
            response.update(extra)
        return JSONResponse(response)

    if raw_sessions:
        sessions = []
        for raw_session in raw_sessions:
            checklist_key = normalize_checklist_key(raw_session.get("checklistKey"))
            session_dialog_id = normalize_dialog_id(raw_session.get("dialogId") or dialog_id)
            changes = raw_session.get("changes") or []
            session_data = raw_session.get("data") or {}

            if not session_dialog_id:
                continue

            if changes and session_data:
                session_data = dict(session_data)
                session_data["checklistKey"] = checklist_key
                session_data["resolvedDialogId"] = session_dialog_id
                data = normalize_checklist_data(session_data, checklist_key)
                data["resolvedDialogId"] = session_dialog_id
                save_checklist(session_dialog_id, data, checklist_key)
            else:
                data = get_checklist(session_dialog_id, checklist_key)

            sessions.append({
                "dialogId": session_dialog_id,
                "checklistKey": checklist_key,
                "changes": changes,
                "data": data,
            })

        if not sessions:
            write_debug_log("close_session_skipped", {
                "dialogId": dialog_id,
                "reason": "no sessions"
            })
            return JSONResponse({"ok": True, "skipped": True, "reason": "no sessions"})

        visible_sessions = [session for session in sessions if build_recent_changes_sections(session["changes"], session["checklistKey"])]
        if not visible_sessions:
            write_debug_log("close_session_message_skipped", {
                "dialogId": dialog_id or sessions[0]["dialogId"],
                "reason": "no visible message changes",
                "sessions": [
                    {
                        "checklistKey": session["checklistKey"],
                        "changesCount": len(session["changes"]),
                    }
                    for session in sessions
                ]
            })
            return JSONResponse({
                "ok": True,
                "dialogId": dialog_id or sessions[0]["dialogId"],
                "saved": True,
                "messageSkipped": True,
            })

        target_dialog_id = dialog_id or sessions[0]["dialogId"]

        try:
            message = build_multi_checklist_chat_message(visible_sessions, editor)
            result = bitrix_webhook_call("im.message.add", {
                "DIALOG_ID": target_dialog_id,
                "MESSAGE": message,
            })
        except Exception as exc:
            write_debug_log("close_session_message_exception", {
                "dialogId": target_dialog_id,
                "editor": editor,
                "checklistKeys": [session["checklistKey"] for session in visible_sessions],
                "error": str(exc),
            })
            return build_message_failure_response(target_dialog_id, {
                "checklistKeys": [session["checklistKey"] for session in sessions],
                "messageError": str(exc),
            })

        write_debug_log("close_session_im_message_add_result", {
            "dialogId": target_dialog_id,
            "changesCount": sum(len(session["changes"]) for session in visible_sessions),
            "editor": editor,
            "result": result,
            "checklistKeys": [session["checklistKey"] for session in visible_sessions],
        })

        if "error" in result:
            return build_message_failure_response(target_dialog_id, {
                "checklistKeys": [session["checklistKey"] for session in sessions],
                "messageError": result.get("error_description") or result.get("error") or "message send failed",
                "result": result,
            })

        return JSONResponse({
            "ok": True,
            "saved": True,
            "messageOk": True,
            "dialogId": target_dialog_id,
            "result": result,
            "checklistKeys": [session["checklistKey"] for session in sessions],
        })

    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    changes = payload.get("changes") or []
    session_data = payload.get("data") or {}

    if not dialog_id:
        write_debug_log("close_session_invalid", {
            "reason": "dialogId is required",
            "payload": payload
        })
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not changes:
        write_debug_log("close_session_skipped", {
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "reason": "no changes"
        })
        return JSONResponse({"ok": True, "skipped": True, "reason": "no changes"})

    if session_data:
        session_data = dict(session_data)
        session_data["checklistKey"] = checklist_key
        session_data["resolvedDialogId"] = dialog_id
        data = normalize_checklist_data(session_data, checklist_key)
        data["resolvedDialogId"] = dialog_id
        save_checklist(dialog_id, data, checklist_key)
    else:
        data = get_checklist(dialog_id, checklist_key)

    if not build_recent_changes_sections(changes, checklist_key):
        write_debug_log("close_session_message_skipped", {
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "reason": "no visible message changes",
            "changesCount": len(changes),
        })
        return JSONResponse({
            "ok": True,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "saved": True,
            "messageSkipped": True,
        })

    try:
        message = build_checklist_chat_message(data, changes, editor)
        result = bitrix_webhook_call("im.message.add", {
            "DIALOG_ID": dialog_id,
            "MESSAGE": message,
        })
    except Exception as exc:
        write_debug_log("close_session_message_exception", {
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "editor": editor,
            "error": str(exc)
        })
        return build_message_failure_response(dialog_id, {
            "checklistKey": checklist_key,
            "messageError": str(exc),
        })

    write_debug_log("close_session_im_message_add_result", {
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "changesCount": len(changes),
        "editor": editor,
        "result": result
    })

    if "error" in result:
        return build_message_failure_response(dialog_id, {
            "checklistKey": checklist_key,
            "messageError": result.get("error_description") or result.get("error") or "message send failed",
            "result": result,
        })

    return JSONResponse({
        "ok": True,
        "saved": True,
        "messageOk": True,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "result": result,
    })

@router.post("/api/checklist/session/finalize")
async def api_checklist_finalize_session(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raw = await request.body()
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            payload = {}

    try:
        result = finalize_edit_session_payload(payload)
        return JSONResponse(result)

    except ValueError as exc:
        write_debug_log("edit_session_finalize_invalid", {
            "error": str(exc),
            "payload": payload,
        })
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=400,
        )

    except EditSessionNotFoundError as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=404,
        )

    except EditSessionPermissionError as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=403,
        )

    except (
        EditSessionConflictError,
        SessionFinalizationInProgressError,
        SessionFinalizationPayloadConflictError,
    ) as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=409,
        )

    except Exception as exc:
        write_debug_log("edit_session_finalize_exception", {
            "sessionId": payload.get("sessionId") or "",
            "dialogId": payload.get("dialogId") or "",
            "error": str(exc),
        })
        return JSONResponse(
            {
                "ok": False,
                "error": str(exc),
            },
            status_code=500,
        )

