from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.checklists.notification_delivery import (
    NotificationDeliveryError,
    list_notification_delivery_attempts,
    retry_failed_notification_deliveries,
)
from app.checklists.utils import clean_cell_value


router = APIRouter()


@router.get("/api/checklist/notification-deliveries")
def api_notification_deliveries(request: Request):
    try:
        attempts = list_notification_delivery_attempts(
            session_id=clean_cell_value(
                request.query_params.get("sessionId")
            ),
            draft_id=clean_cell_value(
                request.query_params.get("draftId")
            ),
        )
        return JSONResponse({
            "ok": True,
            "attemptCount": len(attempts),
            "attempts": attempts,
        })
    except Exception as exc:
        return JSONResponse({
            "ok": False,
            "error": str(exc),
        }, status_code=500)


@router.post("/api/checklist/notification-deliveries/retry")
async def api_retry_notification_deliveries(request: Request):
    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            payload = {}
        delivery_types = payload.get("deliveryTypes")
        if not isinstance(delivery_types, list):
            delivery_types = None
        result = retry_failed_notification_deliveries(
            draft_id=clean_cell_value(payload.get("draftId")),
            delivery_types=delivery_types,
            confirmed=payload.get("confirmed") is True,
            confirm_uncertain=payload.get("confirmUncertain") is True,
        )
        status_code = (
            409
            if result.get("requiresUncertainConfirmation")
            else 200
        )
        return JSONResponse(result, status_code=status_code)
    except NotificationDeliveryError as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=400,
        )
    except Exception as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=500,
        )
