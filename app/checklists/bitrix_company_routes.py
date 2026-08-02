from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.checklists.bitrix_companies import (
    list_cached_bitrix_companies,
    synchronize_bitrix_companies,
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


@router.get("/api/checklist/bitrix-companies")
async def api_list_bitrix_companies(request: Request):
    query = clean_cell_value(request.query_params.get("q"))
    limit = _limit_param(request.query_params.get("limit"), 50)
    include_inactive = _bool_param(request.query_params.get("includeInactive"))
    refresh_mode = clean_cell_value(request.query_params.get("refresh")).lower()

    sync = None
    if refresh_mode in {"auto", "1", "true", "force"}:
        sync = synchronize_bitrix_companies(force=refresh_mode == "force")

    companies = list_cached_bitrix_companies(
        query=query,
        limit=limit,
        include_inactive=include_inactive,
    )
    return JSONResponse({
        "ok": True,
        "companyCount": len(companies),
        "companies": companies,
        "sync": sync,
    })


@router.post("/api/checklist/bitrix-companies/refresh")
async def api_refresh_bitrix_companies(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    payload = payload if isinstance(payload, dict) else {}

    sync = synchronize_bitrix_companies(force=True)
    query = clean_cell_value(payload.get("q"))
    limit = _limit_param(payload.get("limit"), 50)
    companies = list_cached_bitrix_companies(query=query, limit=limit)
    return JSONResponse({
        "ok": True,
        "companyCount": len(companies),
        "companies": companies,
        "sync": sync,
    })
