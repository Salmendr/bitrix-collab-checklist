from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.checklists.document_assignment_history import (
    list_document_assignment_history,
)
from app.checklists.utils import clean_cell_value


router = APIRouter()


@router.get("/api/checklist/document-assignment-history")
def api_document_assignment_history(request: Request):
    try:
        payload = list_document_assignment_history(
            dialog_id=clean_cell_value(request.query_params.get("dialogId")),
            checklist_key=clean_cell_value(
                request.query_params.get("checklistKey")
            ) or "id",
            item_id=clean_cell_value(request.query_params.get("itemId")),
            series_id=clean_cell_value(request.query_params.get("seriesId")),
            limit=request.query_params.get("limit") or 10,
        )
        return JSONResponse({"ok": True, **payload})
    except ValueError as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=400,
        )
    except Exception as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=500,
        )
