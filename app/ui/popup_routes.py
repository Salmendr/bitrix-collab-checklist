from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from app.ui.popup import popup_html


router = APIRouter()


@router.get("/popup", response_class=HTMLResponse)
def popup_get(dialogId: str = "", checklistKey: str = "id"):
    return HTMLResponse(
        popup_html(dialogId, checklistKey),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )
