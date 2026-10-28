"""«Анализ состояния объекта»: API of the panel, the slice window and the
risk window."""
from __future__ import annotations

import html
import json

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.concurrency import run_in_threadpool

from app.auth.identity import is_read_only
from app.checklists import object_analysis
from app.checklists.utils import clean_cell_value, normalize_dialog_id
from app.ui.shell import get_public_app_base_path
from app.ui.template_engine import render_ui_template


router = APIRouter()

ASSET_VERSION = "8.30-grad-analysis"


def _actor(payload: dict) -> dict:
    return {
        "userId": clean_cell_value(payload.get("actingUserId")),
        "userName": clean_cell_value(payload.get("actingUserName")),
    }


def _error(exc: Exception) -> JSONResponse:
    status = 403 if isinstance(exc, object_analysis.ObjectAnalysisDenied) else 400
    return JSONResponse({"ok": False, "error": str(exc)}, status_code=status)


@router.get("/api/object-analysis")
async def api_object_analysis(request: Request, dialogId: str = "", userId: str = ""):
    dialog_id = normalize_dialog_id(dialogId)
    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)
    if is_read_only(request):
        # The platform portal sees the checklists only.
        return JSONResponse({"ok": True, "visible": False})
    payload = await run_in_threadpool(object_analysis.panel_payload, dialog_id, clean_cell_value(userId))
    return JSONResponse({"ok": True, **payload})


@router.post("/api/object-analysis/field")
async def api_object_analysis_field(request: Request):
    payload = await request.json()
    try:
        record = await run_in_threadpool(
            object_analysis.update_field,
            clean_cell_value(payload.get("dialogId")),
            clean_cell_value(payload.get("field")),
            payload.get("value"),
            actor=_actor(payload),
        )
    except object_analysis.ObjectAnalysisError as exc:
        return _error(exc)
    base = object_analysis.base_of(payload.get("dialogId"))
    view = await run_in_threadpool(
        object_analysis.project_view, base, record, user_id=_actor(payload)["userId"],
    )
    return JSONResponse({"ok": True, "project": view})


@router.post("/api/object-analysis/options")
async def api_object_analysis_options(request: Request):
    payload = await request.json()
    try:
        options = await run_in_threadpool(
            lambda: object_analysis.change_option(
                clean_cell_value(payload.get("list")),
                clean_cell_value(payload.get("action")),
                dialog_id=clean_cell_value(payload.get("dialogId")),
                actor=_actor(payload),
                option_id=clean_cell_value(payload.get("optionId")),
                label=payload.get("label") or "",
            )
        )
    except object_analysis.ObjectAnalysisError as exc:
        return _error(exc)
    return JSONResponse({"ok": True, "options": options})


@router.get("/api/object-analysis/slice")
async def api_object_analysis_slice(request: Request, dialogId: str = "", userId: str = ""):
    if is_read_only(request):
        return JSONResponse({"ok": False, "error": "Нет доступа"}, status_code=403)
    try:
        payload = await run_in_threadpool(object_analysis.slice_payload, dialogId, clean_cell_value(userId))
    except object_analysis.ObjectAnalysisError as exc:
        return _error(exc)
    return JSONResponse({"ok": True, **payload})


@router.get("/api/object-analysis/risks")
async def api_object_analysis_risks(request: Request, dialogId: str = "", userId: str = ""):
    if is_read_only(request):
        return JSONResponse({"ok": False, "error": "Нет доступа"}, status_code=403)
    try:
        payload = await run_in_threadpool(object_analysis.risks_payload, dialogId, clean_cell_value(userId))
    except object_analysis.ObjectAnalysisError as exc:
        return _error(exc)
    return JSONResponse({"ok": True, **payload})


@router.post("/api/object-analysis/risks")
async def api_object_analysis_risks_change(request: Request):
    payload = await request.json()
    try:
        view = await run_in_threadpool(
            lambda: object_analysis.change_risks(
                clean_cell_value(payload.get("dialogId")),
                clean_cell_value(payload.get("op")),
                payload,
                actor=_actor(payload),
            )
        )
    except object_analysis.ObjectAnalysisError as exc:
        return _error(exc)
    return JSONResponse({"ok": True, **view})


def _page(request: Request, template: str, title: str, dialog_id: str, user_id: str, user_name: str,
          focus: str = "") -> HTMLResponse:
    base = object_analysis.base_of(dialog_id)
    rights = (
        {"view": False} if is_read_only(request) or not base
        else object_analysis.user_rights(base, clean_cell_value(user_id))
    )
    if not rights.get("view"):
        return HTMLResponse(
            "<!doctype html><meta charset='utf-8'><title>Нет доступа</title>"
            "<p style='font-family:Arial,sans-serif;padding:24px'>"
            "Анализ состояния объекта доступен руководителям, администраторам и ГИП проекта.</p>",
            status_code=403,
        )
    app_base = get_public_app_base_path(request)
    bootstrap = {
        "dialogId": base,
        "userId": clean_cell_value(user_id),
        "userName": clean_cell_value(user_name),
        "appBase": app_base,
        # The risk window opened from a row of the slice: its table.
        "focusDialogId": object_analysis.base_of(focus) if clean_cell_value(focus) else base,
    }
    bootstrap_json = (
        json.dumps(bootstrap, ensure_ascii=False)
        .replace("</", "<\\/")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )
    static = f"{app_base}/ui-static"
    return HTMLResponse(render_ui_template(template, {
        "PAGE_TITLE": html.escape(title),
        "PAGE_BOOTSTRAP_JSON": bootstrap_json,
        "OBJECT_ANALYSIS_CSS_URL": html.escape(f"{static}/css/object-analysis.css?v={ASSET_VERSION}"),
        "OBJECT_ANALYSIS_UI_JS_URL": html.escape(f"{static}/js/object-analysis-ui.js?v={ASSET_VERSION}"),
        "PAGE_JS_URL": html.escape(
            f"{static}/js/"
            + ("object-analysis-page.js" if template == "object_analysis.html" else "object-risks-page.js")
            + f"?v={ASSET_VERSION}"
        ),
    }))


@router.get("/object-analysis", response_class=HTMLResponse)
def object_analysis_page(request: Request, dialogId: str = "", userId: str = "", userName: str = ""):
    return _page(request, "object_analysis.html", "Анализ состояния объектов", dialogId, userId, userName)


@router.get("/object-analysis/risks", response_class=HTMLResponse)
def object_risks_page(request: Request, dialogId: str = "", userId: str = "", userName: str = "", focus: str = ""):
    return _page(request, "object_risks.html", "Оценка рисков", dialogId, userId, userName, focus)
