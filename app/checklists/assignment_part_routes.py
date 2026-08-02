from __future__ import annotations

import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.checklists.assignment_parts import (
    AssignmentPartNotFoundError,
    AssignmentPartValidationError,
    create_or_get_assignment_part,
    list_assignment_parts,
)
from app.checklists.edit_sessions import (
    EditSessionConflictError,
    EditSessionNotFoundError,
    EditSessionPermissionError,
    get_edit_session_for_actor,
)
from app.checklists.utils import clean_cell_value, normalize_dialog_id


router = APIRouter()


async def _read_json_payload(request: Request) -> dict:
    try:
        payload = await request.json()
    except Exception:
        raw = await request.body()
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            payload = {}
    return payload if isinstance(payload, dict) else {}


def _error_response(exc: Exception) -> JSONResponse:
    if isinstance(exc, (AssignmentPartNotFoundError, EditSessionNotFoundError)):
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=404)
    if isinstance(exc, EditSessionPermissionError):
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=403)
    if isinstance(exc, EditSessionConflictError):
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=409)
    if isinstance(exc, (AssignmentPartValidationError, ValueError)):
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


@router.get("/api/checklist/assignment-parts")
async def api_list_assignment_parts(request: Request):
    try:
        parts = list_assignment_parts(
            query=clean_cell_value(request.query_params.get("q")),
            limit=int(request.query_params.get("limit") or 50),
        )
        return JSONResponse({
            "ok": True,
            "assignmentPartCount": len(parts),
            "assignmentParts": parts,
        })
    except Exception as exc:
        return _error_response(exc)


@router.post("/api/checklist/assignment-parts")
async def api_create_assignment_part(request: Request):
    payload = await _read_json_payload(request)
    try:
        get_edit_session_for_actor(
            session_id=clean_cell_value(payload.get("sessionId")),
            dialog_id=normalize_dialog_id(payload.get("dialogId")),
            user_id=clean_cell_value(payload.get("userId")),
        )
        part = create_or_get_assignment_part(
            text=clean_cell_value(
                payload.get("text")
                or payload.get("name")
                or payload.get("assignmentPartText")
            ),
            user_id=clean_cell_value(payload.get("userId")),
            user_name=clean_cell_value(payload.get("userName")),
        )
        return JSONResponse({"ok": True, "assignmentPart": part}, status_code=201)
    except Exception as exc:
        return _error_response(exc)
