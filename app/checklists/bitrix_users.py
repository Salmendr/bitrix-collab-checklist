from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import app.settings as settings
from app.bitrix.client import bitrix_webhook_call
from app.db import get_conn
from app.checklists.utils import clean_cell_value


BITRIX_USER_CACHE_TTL_SECONDS = 3600
BITRIX_USER_PAGE_SIZE = 50
BITRIX_USER_MAX_PAGES = 200

_sync_lock = threading.Lock()


class BitrixUserSyncError(RuntimeError):
    pass


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat(timespec="seconds")


def _parse_datetime(value: Any) -> datetime | None:
    raw = clean_cell_value(value)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_bool(value: Any, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    raw = clean_cell_value(value).lower()
    if raw in {"n", "no", "false", "0", "inactive"}:
        return False
    if raw in {"y", "yes", "true", "1", "active"}:
        return True
    return bool(default)


def _normalize_lookup(value: Any) -> str:
    return " ".join(clean_cell_value(value).split()).casefold()


def _build_user_name(raw: dict) -> str:
    first_name = clean_cell_value(raw.get("NAME"))
    last_name = clean_cell_value(raw.get("LAST_NAME"))
    second_name = clean_cell_value(raw.get("SECOND_NAME"))
    name = " ".join(
        part for part in (last_name, first_name, second_name) if part
    )
    return name or clean_cell_value(raw.get("FULL_NAME")) or clean_cell_value(raw.get("ID"))


def normalize_bitrix_user(raw: Any) -> dict | None:
    if not isinstance(raw, dict):
        return None
    user_id = clean_cell_value(raw.get("ID") or raw.get("id"))
    if not user_id:
        return None

    first_name = clean_cell_value(raw.get("NAME") or raw.get("firstName"))
    last_name = clean_cell_value(raw.get("LAST_NAME") or raw.get("lastName"))
    normalized = {
        "userId": user_id,
        "name": _build_user_name(raw),
        "firstName": first_name,
        "lastName": last_name,
        "active": _normalize_bool(raw.get("ACTIVE"), True),
        "email": clean_cell_value(raw.get("EMAIL") or raw.get("email")),
        "workPosition": clean_cell_value(
            raw.get("WORK_POSITION") or raw.get("workPosition")
        ),
        "departmentIds": (
            list(raw.get("UF_DEPARTMENT") or [])
            if isinstance(raw.get("UF_DEPARTMENT"), list)
            else []
        ),
        "raw": raw,
    }
    return normalized


def _public_user(row: dict) -> dict:
    raw = {}
    try:
        raw = json.loads(row.get("raw_json") or "{}")
    except Exception:
        raw = {}
    return {
        "userId": row.get("user_id") or "",
        "name": row.get("name") or "",
        "firstName": row.get("first_name") or "",
        "lastName": row.get("last_name") or "",
        "active": bool(int(row.get("active") or 0)),
        "email": row.get("email") or "",
        "workPosition": clean_cell_value(raw.get("WORK_POSITION")),
        "departmentIds": (
            list(raw.get("UF_DEPARTMENT") or [])
            if isinstance(raw.get("UF_DEPARTMENT"), list)
            else []
        ),
        "fetchedAt": row.get("fetched_at") or "",
        "updatedAt": row.get("updated_at") or "",
    }


def _cache_stats() -> dict:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) AS active_count,
                MAX(fetched_at) AS last_fetched_at
            FROM bitrix_users_cache
            """
        ).fetchone()
        return {
            "totalCount": int(row["total_count"] or 0),
            "activeCount": int(row["active_count"] or 0),
            "lastFetchedAt": row["last_fetched_at"] or "",
        }
    finally:
        conn.close()


def _cache_is_fresh(stats: dict, *, now: datetime | None = None) -> bool:
    last_fetched = _parse_datetime(stats.get("lastFetchedAt"))
    if not last_fetched or int(stats.get("activeCount") or 0) <= 0:
        return False
    current = now or _utc_now()
    return current - last_fetched < timedelta(seconds=BITRIX_USER_CACHE_TTL_SECONDS)


def list_cached_bitrix_users(
    *,
    query: str = "",
    limit: int = 50,
    include_inactive: bool = False,
) -> list[dict]:
    safe_limit = max(1, min(int(limit or 50), 200))
    conn = get_conn()
    try:
        if include_inactive:
            rows = conn.execute(
                "SELECT * FROM bitrix_users_cache ORDER BY name ASC, user_id ASC"
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM bitrix_users_cache
                WHERE active = 1
                ORDER BY name ASC, user_id ASC
                """
            ).fetchall()
    finally:
        conn.close()

    needle = _normalize_lookup(query)
    result: list[dict] = []
    for row in rows:
        public = _public_user(dict(row))
        if needle:
            haystack = " ".join([
                _normalize_lookup(public.get("userId")),
                _normalize_lookup(public.get("name")),
                _normalize_lookup(public.get("email")),
                _normalize_lookup(public.get("workPosition")),
            ])
            if needle not in haystack:
                continue
        result.append(public)
        if len(result) >= safe_limit:
            break
    return result


def get_cached_bitrix_user(user_id: str) -> dict | None:
    normalized_id = clean_cell_value(user_id)
    if not normalized_id:
        return None
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM bitrix_users_cache WHERE user_id = ?",
            (normalized_id,),
        ).fetchone()
        return _public_user(dict(row)) if row else None
    finally:
        conn.close()


def resolve_cached_bitrix_user(*, user_id: str = "", name: str = "") -> dict | None:
    normalized_id = clean_cell_value(user_id)
    if normalized_id:
        cached = get_cached_bitrix_user(normalized_id)
        if cached:
            return cached

    normalized_name = _normalize_lookup(name)
    if not normalized_name:
        return None
    matches = [
        user
        for user in list_cached_bitrix_users(
            query=name,
            limit=200,
            include_inactive=False,
        )
        if _normalize_lookup(user.get("name")) == normalized_name
    ]
    return matches[0] if len(matches) == 1 else None


def _replace_cache(users: list[dict], *, fetched_at: str) -> None:
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE bitrix_users_cache SET active = 0, updated_at = ?", (fetched_at,))
        for user in users:
            conn.execute(
                """
                INSERT INTO bitrix_users_cache(
                    user_id, name, first_name, last_name, active,
                    email, raw_json, fetched_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    name = excluded.name,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    active = excluded.active,
                    email = excluded.email,
                    raw_json = excluded.raw_json,
                    fetched_at = excluded.fetched_at,
                    updated_at = excluded.updated_at
                """,
                (
                    user.get("userId") or "",
                    user.get("name") or "",
                    user.get("firstName") or "",
                    user.get("lastName") or "",
                    1 if user.get("active") else 0,
                    user.get("email") or "",
                    json.dumps(user.get("raw") or {}, ensure_ascii=False),
                    fetched_at,
                    fetched_at,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def synchronize_bitrix_users(*, force: bool = False) -> dict:
    stats_before = _cache_stats()
    configured = bool(clean_cell_value(settings.BITRIX_TECH_WEBHOOK_URL))
    if not configured:
        return {
            "ok": True,
            "status": "disabled",
            "configured": False,
            "refreshed": False,
            "error": "BITRIX_TECH_WEBHOOK_URL is empty",
            **stats_before,
        }

    if not force and _cache_is_fresh(stats_before):
        return {
            "ok": True,
            "status": "cached",
            "configured": True,
            "refreshed": False,
            "error": "",
            **stats_before,
        }

    with _sync_lock:
        stats_locked = _cache_stats()
        if not force and _cache_is_fresh(stats_locked):
            return {
                "ok": True,
                "status": "cached",
                "configured": True,
                "refreshed": False,
                "error": "",
                **stats_locked,
            }

        users: list[dict] = []
        seen: set[str] = set()
        start = 0
        total = None

        try:
            for _page in range(BITRIX_USER_MAX_PAGES):
                response = bitrix_webhook_call(
                    "user.get",
                    {
                        "SORT": "ID",
                        "ORDER": "ASC",
                        "FILTER[ACTIVE]": "Y",
                        "FILTER[USER_TYPE]": "employee",
                        "start": start,
                    },
                )
                if not isinstance(response, dict):
                    raise BitrixUserSyncError("invalid Bitrix user.get response")
                if response.get("error"):
                    raise BitrixUserSyncError(
                        clean_cell_value(response.get("error_description"))
                        or clean_cell_value(response.get("error"))
                    )
                raw_users = response.get("result")
                if not isinstance(raw_users, list):
                    raise BitrixUserSyncError("Bitrix user.get result is not an array")

                for raw_user in raw_users:
                    normalized = normalize_bitrix_user(raw_user)
                    if not normalized:
                        continue
                    user_id = normalized["userId"]
                    if user_id in seen:
                        continue
                    seen.add(user_id)
                    users.append(normalized)

                try:
                    total = int(response.get("total"))
                except Exception:
                    total = None

                next_value = response.get("next")
                if next_value not in {None, ""}:
                    try:
                        next_start = int(next_value)
                    except Exception:
                        next_start = start + BITRIX_USER_PAGE_SIZE
                else:
                    next_start = start + BITRIX_USER_PAGE_SIZE

                if not raw_users:
                    break
                if total is not None and next_start >= total:
                    break
                if len(raw_users) < BITRIX_USER_PAGE_SIZE and next_value in {None, ""}:
                    break
                start = next_start
            else:
                raise BitrixUserSyncError("Bitrix user pagination safety limit reached")

            fetched_at = _iso_now()
            _replace_cache(users, fetched_at=fetched_at)
            stats_after = _cache_stats()
            return {
                "ok": True,
                "status": "synced",
                "configured": True,
                "refreshed": True,
                "error": "",
                "receivedCount": len(users),
                **stats_after,
            }
        except Exception as exc:
            stats_after_error = _cache_stats()
            return {
                "ok": False,
                "status": "error",
                "configured": True,
                "refreshed": False,
                "error": str(exc),
                **stats_after_error,
            }
