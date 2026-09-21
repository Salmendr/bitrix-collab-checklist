from time import perf_counter

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.ui.popup import popup_html
from app.ui.request_diagnostics import (
    new_ui_request_id,
    write_ui_request_diagnostic,
)


router = APIRouter()


@router.get("/popup", response_class=HTMLResponse)
def popup_get(
    request: Request,
    dialogId: str = "",
    checklistKey: str = "id",
):
    request_id = new_ui_request_id()
    started_at = perf_counter()
    write_ui_request_diagnostic(
        "popup_diag_http_popup_requested",
        request,
        request_id=request_id,
        extra={
            "dialogId": str(dialogId or "")[:160],
            "checklistKey": str(checklistKey or "id")[:80],
        },
    )

    try:
        response_body = popup_html(dialogId, checklistKey)
    except Exception as error:
        write_ui_request_diagnostic(
            "popup_diag_http_popup_render_failed",
            request,
            request_id=request_id,
            extra={
                "dialogId": str(dialogId or "")[:160],
                "checklistKey": str(checklistKey or "id")[:80],
                "elapsedMs": round(
                    (perf_counter() - started_at) * 1000,
                    3,
                ),
                "errorType": type(error).__name__,
                "error": str(error)[:500],
            },
        )
        raise

    write_ui_request_diagnostic(
        "popup_diag_http_popup_rendered",
        request,
        request_id=request_id,
        extra={
            "dialogId": str(dialogId or "")[:160],
            "checklistKey": str(checklistKey or "id")[:80],
            "elapsedMs": round((perf_counter() - started_at) * 1000, 3),
            "responseBytes": len(response_body.encode("utf-8")),
        },
    )

    return HTMLResponse(
        response_body,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )
