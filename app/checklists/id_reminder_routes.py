from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from app.checklists.id_reminders import get_settings, save_draft
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
        dialog_id, session_id, _ = actor(request.query_params)
        verify_bitrix_actor(request, clean_cell_value(request.query_params.get("userId")))
        return JSONResponse({"ok": True, **get_settings(dialog_id, session_id)}, headers={"Cache-Control": "no-store"})
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
        result = await run_in_threadpool(save_draft, dialog_id=dialog_id, session_id=session_id,
                                        user_id=user_id, config=payload.get("config"))
        return {"ok": True, **result}
    except Exception as exc:
        return _error_response(exc)
