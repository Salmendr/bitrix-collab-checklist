from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.checklists.bitrix_users import (
    list_cached_bitrix_users,
    synchronize_bitrix_users,
)
from app.checklists.utils import clean_cell_value


router = APIRouter()


def _bool_param(value) -> bool:
    return clean_cell_value(value).lower() in {"1", "true", "yes", "y"}


def _limit_param(value, default: int = 50) -> int:
    try:
        return max(1, min(int(value or default), 200))
    except Exception:
        return default


@router.get("/api/checklist/bitrix-users")
async def api_list_bitrix_users(request: Request):
    query = clean_cell_value(request.query_params.get("q"))
    limit = _limit_param(request.query_params.get("limit"), 50)
    include_inactive = _bool_param(request.query_params.get("includeInactive"))
    refresh_mode = clean_cell_value(request.query_params.get("refresh")).lower()

    sync = None
    if refresh_mode in {"auto", "1", "true", "force"}:
        sync = synchronize_bitrix_users(force=refresh_mode == "force")

    users = list_cached_bitrix_users(
        query=query,
        limit=limit,
        include_inactive=include_inactive,
    )
    return JSONResponse({
        "ok": True,
        "userCount": len(users),
        "users": users,
        "sync": sync,
    })


@router.post("/api/checklist/bitrix-users/refresh")
async def api_refresh_bitrix_users(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    payload = payload if isinstance(payload, dict) else {}

    sync = synchronize_bitrix_users(force=True)
    query = clean_cell_value(payload.get("q"))
    limit = _limit_param(payload.get("limit"), 50)
    users = list_cached_bitrix_users(query=query, limit=limit)

    status_code = 200
    return JSONResponse({
        "ok": True,
        "userCount": len(users),
        "users": users,
        "sync": sync,
    }, status_code=status_code)
