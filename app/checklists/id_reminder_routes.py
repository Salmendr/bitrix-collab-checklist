from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from app.checklists.id_reminders import get_settings, save_draft, can_manage_id_reminders
from app.checklists.edit_sessions import get_edit_session_for_actor, EditSessionPermissionError, EditSessionConflictError
from app.checklists.notification_routes import _error_response
from app.checklists.permissions import can_user_access_checklists
from app.checklists.utils import clean_cell_value, normalize_dialog_id

from app.checklists.bitrix_actor_auth import verify_bitrix_actor

router = APIRouter()


def actor(payload):
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    session_id = clean_cell_value(payload.get("sessionId"))
    user_id = clean_cell_value(payload.get("userId"))
    if not dialog_id or not session_id or not user_id or not can_user_access_checklists(user_id):
        raise EditSessionPermissionError("Нет доступа к настройке оповещений")
    session = get_edit_session_for_actor(session_id, dialog_id, user_id)
    if session.get("status") != "active":
        raise EditSessionConflictError("Сеанс редактирования уже завершён")
    return dialog_id, session_id, user_id


@router.get("/api/checklist/id-reminders")
def read_settings(request: Request):
    try:
        dialog_id, session_id, user_id = actor(request.query_params)
        verify_bitrix_actor(request, user_id)
        can_manage = can_manage_id_reminders(user_id)
        result = get_settings(dialog_id, session_id if can_manage else "")
        if not can_manage:
            # Readers receive the published schedule only, not recipients,
            # delivery history or another user's pending changes.
            result = {"config": {key: value for key, value in result["config"].items()
                                 if key in {"enabled", "days", "hour", "minute", "timezoneOffset"}},
                      "pending": False}
        return JSONResponse({"ok": True, "canManage": can_manage, **result}, headers={"Cache-Control": "no-store"})
    except Exception as exc:
        return _error_response(exc)


@router.put("/api/checklist/id-reminders")
async def update_settings(request: Request):
    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            raise ValueError("Ожидается объект настроек")
        dialog_id, session_id, user_id = await run_in_threadpool(actor, payload)
        await run_in_threadpool(verify_bitrix_actor, request, user_id)
        if not can_manage_id_reminders(user_id):
            raise EditSessionPermissionError("Настройка оповещений доступна только администраторам")
        result = await run_in_threadpool(save_draft, dialog_id=dialog_id, session_id=session_id,
                                        user_id=user_id, config=payload.get("config"))
        return {"ok": True, "canManage": True, **result}
    except Exception as exc:
        return _error_response(exc)
