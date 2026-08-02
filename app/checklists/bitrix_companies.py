from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import app.settings as settings
from app.bitrix.client import bitrix_webhook_call
from app.db import get_conn
from app.checklists.edit_sessions import utc_now_iso
from app.checklists.utils import clean_cell_value


BITRIX_COMPANY_CACHE_TTL_SECONDS = 3600
BITRIX_COMPANY_PAGE_SIZE = 50
BITRIX_COMPANY_MAX_PAGES = 200
SUPPLIER_COMPANY_TYPE = "SUPPLIER"

_sync_lock = threading.Lock()


class BitrixCompanySyncError(RuntimeError):
    pass


class ExternalContractorValidationError(RuntimeError):
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


def normalize_company_title(value: Any) -> str:
    return " ".join(clean_cell_value(value).split())


def normalize_company_title_key(value: Any) -> str:
    return normalize_company_title(value).casefold()


def normalize_company_phone(value: Any) -> str:
    return "".join(ch for ch in clean_cell_value(value) if ch.isdigit())


def normalize_company_email(value: Any) -> str:
    return clean_cell_value(value).casefold()


def _first_multifield_value(value: Any) -> str:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                candidate = clean_cell_value(item.get("VALUE") or item.get("value"))
            else:
                candidate = clean_cell_value(item)
            if candidate:
                return candidate
        return ""
    if isinstance(value, dict):
        return clean_cell_value(value.get("VALUE") or value.get("value"))
    return clean_cell_value(value)


def normalize_bitrix_company(raw: Any) -> dict | None:
    if not isinstance(raw, dict):
        return None
    company_id = clean_cell_value(raw.get("ID") or raw.get("id"))
    if not company_id:
        return None
    title = normalize_company_title(raw.get("TITLE") or raw.get("title"))
    phone = _first_multifield_value(raw.get("PHONE") or raw.get("phone"))
    email = _first_multifield_value(raw.get("EMAIL") or raw.get("email"))
    company_type = clean_cell_value(
        raw.get("COMPANY_TYPE") or raw.get("companyType")
    ) or SUPPLIER_COMPANY_TYPE
    return {
        "companyId": company_id,
        "title": title or company_id,
        "companyType": company_type,
        "phone": phone,
        "email": email,
        "contactDetails": "",
        "source": "bitrix",
        "syncStatus": "synced",
        "syncError": "",
        "active": True,
        "raw": raw,
    }


def _json_loads(value: Any) -> dict:
    raw = clean_cell_value(value)
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _public_company(row: dict) -> dict:
    raw = _json_loads(row.get("raw_json"))
    return {
        "companyId": row.get("company_id") or "",
        "title": row.get("title") or "",
        "companyType": row.get("company_type") or SUPPLIER_COMPANY_TYPE,
        "phone": row.get("phone") or "",
        "email": row.get("email") or "",
        "contactDetails": row.get("contact_details") or "",
        "source": row.get("source") or "bitrix",
        "syncStatus": row.get("sync_status") or "synced",
        "syncError": row.get("sync_error") or "",
        "active": bool(int(row.get("active") or 0)),
        "usageCount": int(row.get("usage_count") or 0),
        "lastUsedAt": row.get("last_used_at") or "",
        "fetchedAt": row.get("fetched_at") or "",
        "updatedAt": row.get("updated_at") or "",
        "raw": raw,
    }


def _cache_stats() -> dict:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) AS active_count,
                SUM(CASE WHEN active = 1 AND source = 'local' THEN 1 ELSE 0 END) AS local_count,
                SUM(CASE WHEN active = 1 AND source = 'bitrix' THEN 1 ELSE 0 END) AS bitrix_count,
                MAX(CASE WHEN source = 'bitrix' THEN fetched_at ELSE '' END) AS last_fetched_at
            FROM bitrix_companies_cache
            WHERE company_type = ?
            """,
            (SUPPLIER_COMPANY_TYPE,),
        ).fetchone()
        return {
            "totalCount": int(row["total_count"] or 0),
            "activeCount": int(row["active_count"] or 0),
            "localCount": int(row["local_count"] or 0),
            "bitrixCount": int(row["bitrix_count"] or 0),
            "lastFetchedAt": row["last_fetched_at"] or "",
        }
    finally:
        conn.close()


def _cache_is_fresh(stats: dict, *, now: datetime | None = None) -> bool:
    last_fetched = _parse_datetime(stats.get("lastFetchedAt"))
    if not last_fetched or int(stats.get("bitrixCount") or 0) <= 0:
        return False
    current = now or _utc_now()
    return current - last_fetched < timedelta(
        seconds=BITRIX_COMPANY_CACHE_TTL_SECONDS
    )


def _sort_company_key(company: dict) -> tuple:
    source_rank = 0 if company.get("source") == "local" else 1
    return (
        -int(company.get("usageCount") or 0),
        source_rank,
        str(company.get("lastUsedAt") or "") and "0" or "1",
        str(company.get("lastUsedAt") or ""),
        normalize_company_title_key(company.get("title")),
        str(company.get("companyId") or ""),
    )


def list_cached_bitrix_companies(
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
                """
                SELECT * FROM bitrix_companies_cache
                WHERE company_type = ?
                """,
                (SUPPLIER_COMPANY_TYPE,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM bitrix_companies_cache
                WHERE company_type = ? AND active = 1
                """,
                (SUPPLIER_COMPANY_TYPE,),
            ).fetchall()
    finally:
        conn.close()

    needle_text = normalize_company_title_key(query)
    needle_phone = normalize_company_phone(query)
    needle_email = normalize_company_email(query)
    result: list[dict] = []
    for raw_row in rows:
        company = _public_company(dict(raw_row))
        if needle_text or needle_phone or needle_email:
            title_match = needle_text and needle_text in normalize_company_title_key(
                company.get("title")
            )
            id_match = needle_text and needle_text in normalize_company_title_key(
                company.get("companyId")
            )
            phone_match = needle_phone and needle_phone in normalize_company_phone(
                company.get("phone")
            )
            email_match = needle_email and needle_email in normalize_company_email(
                company.get("email")
            )
            contact_match = needle_text and needle_text in normalize_company_title_key(
                company.get("contactDetails")
            )
            if not any((title_match, id_match, phone_match, email_match, contact_match)):
                continue
        result.append(company)

    result.sort(key=_sort_company_key)
    return result[:safe_limit]


def get_cached_bitrix_company(company_id: str, *, conn=None) -> dict | None:
    normalized_id = clean_cell_value(company_id)
    if not normalized_id:
        return None
    owns_conn = conn is None
    connection = conn or get_conn()
    try:
        row = connection.execute(
            "SELECT * FROM bitrix_companies_cache WHERE company_id = ?",
            (normalized_id,),
        ).fetchone()
        return _public_company(dict(row)) if row else None
    finally:
        if owns_conn:
            connection.close()


def _find_match_row(
    conn,
    *,
    phone_key: str,
    email_key: str,
    title_key: str,
) -> dict | None:
    if phone_key:
        row = conn.execute(
            """
            SELECT * FROM bitrix_companies_cache
            WHERE active = 1 AND company_type = ? AND normalized_phone = ?
            ORDER BY CASE WHEN source = 'bitrix' THEN 0 ELSE 1 END, updated_at DESC
            LIMIT 1
            """,
            (SUPPLIER_COMPANY_TYPE, phone_key),
        ).fetchone()
        if row:
            return dict(row)
    if email_key:
        row = conn.execute(
            """
            SELECT * FROM bitrix_companies_cache
            WHERE active = 1 AND company_type = ? AND normalized_email = ?
            ORDER BY CASE WHEN source = 'bitrix' THEN 0 ELSE 1 END, updated_at DESC
            LIMIT 1
            """,
            (SUPPLIER_COMPANY_TYPE, email_key),
        ).fetchone()
        if row:
            return dict(row)
    if title_key:
        row = conn.execute(
            """
            SELECT * FROM bitrix_companies_cache
            WHERE active = 1 AND company_type = ? AND normalized_title = ?
            ORDER BY CASE WHEN source = 'bitrix' THEN 0 ELSE 1 END, updated_at DESC
            LIMIT 1
            """,
            (SUPPLIER_COMPANY_TYPE, title_key),
        ).fetchone()
        if row:
            return dict(row)
    return None


def resolve_external_contractor_in_transaction(
    conn,
    *,
    company_id: str = "",
    title: str = "",
    phone: str = "",
    email: str = "",
    contact_details: str = "",
    mark_used: bool = True,
) -> dict:
    normalized_id = clean_cell_value(company_id)
    normalized_title = normalize_company_title(title)
    normalized_phone = clean_cell_value(phone)
    normalized_email_value = clean_cell_value(email)
    normalized_contact = clean_cell_value(contact_details)
    phone_key = normalize_company_phone(normalized_phone)
    email_key = normalize_company_email(normalized_email_value)
    title_key = normalize_company_title_key(normalized_title)

    if not normalized_title:
        raise ExternalContractorValidationError(
            "external contractor name is required"
        )
    if not any((phone_key, email_key, normalized_contact)):
        raise ExternalContractorValidationError(
            "external contractor contact details are required"
        )

    row = None
    if normalized_id:
        selected = conn.execute(
            "SELECT * FROM bitrix_companies_cache WHERE company_id = ? AND active = 1",
            (normalized_id,),
        ).fetchone()
        if selected:
            row = dict(selected)
    if row is None:
        row = _find_match_row(
            conn,
            phone_key=phone_key,
            email_key=email_key,
            title_key=title_key,
        )

    now = utc_now_iso()
    if row:
        existing_title = normalize_company_title(row.get("title"))
        existing_phone = clean_cell_value(row.get("phone"))
        existing_email = clean_cell_value(row.get("email"))
        existing_contact = clean_cell_value(row.get("contact_details"))
        source = clean_cell_value(row.get("source")) or "bitrix"
        sync_status = clean_cell_value(row.get("sync_status")) or (
            "synced" if source == "bitrix" else "pending_create"
        )
        changed = False

        if not existing_title and normalized_title:
            existing_title = normalized_title
            changed = True
        if not existing_phone and normalized_phone:
            existing_phone = normalized_phone
            changed = True
        if not existing_email and normalized_email_value:
            existing_email = normalized_email_value
            changed = True
        if not existing_contact and normalized_contact:
            existing_contact = normalized_contact
            changed = True

        if changed:
            if source == "bitrix":
                sync_status = "pending_update"
            elif sync_status not in {"pending_create", "local_only"}:
                sync_status = "pending_create"

        conn.execute(
            """
            UPDATE bitrix_companies_cache
            SET title = ?, normalized_title = ?, phone = ?, normalized_phone = ?,
                email = ?, normalized_email = ?, contact_details = ?,
                sync_status = ?, sync_error = '',
                usage_count = usage_count + ?, last_used_at = ?, updated_at = ?
            WHERE company_id = ?
            """,
            (
                existing_title,
                normalize_company_title_key(existing_title),
                existing_phone,
                normalize_company_phone(existing_phone),
                existing_email,
                normalize_company_email(existing_email),
                existing_contact,
                sync_status,
                1 if mark_used else 0,
                now if mark_used else clean_cell_value(row.get("last_used_at")),
                now,
                row.get("company_id"),
            ),
        )
        resolved = get_cached_bitrix_company(row.get("company_id") or "", conn=conn)
        if not resolved:
            raise RuntimeError("external contractor cache update failed")
        return resolved

    local_id = "local-" + uuid.uuid4().hex
    configured = bool(clean_cell_value(settings.BITRIX_TECH_WEBHOOK_URL))
    sync_status = "pending_create" if configured else "local_only"
    conn.execute(
        """
        INSERT INTO bitrix_companies_cache(
            company_id, title, normalized_title, company_type,
            phone, normalized_phone, email, normalized_email,
            contact_details, source, sync_status, sync_error,
            usage_count, last_used_at, active, raw_json,
            fetched_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'local', ?, '', ?, ?, 1, '{}', '', ?, ?)
        """,
        (
            local_id,
            normalized_title,
            title_key,
            SUPPLIER_COMPANY_TYPE,
            normalized_phone,
            phone_key,
            normalized_email_value,
            email_key,
            normalized_contact,
            sync_status,
            1 if mark_used else 0,
            now if mark_used else "",
            now,
            now,
        ),
    )
    resolved = get_cached_bitrix_company(local_id, conn=conn)
    if not resolved:
        raise RuntimeError("external contractor cache insert failed")
    return resolved


def _upsert_remote_company_in_transaction(
    conn,
    company: dict,
    *,
    fetched_at: str,
) -> None:
    company_id = clean_cell_value(company.get("companyId"))
    if not company_id:
        return
    title = normalize_company_title(company.get("title"))
    phone = clean_cell_value(company.get("phone"))
    email = clean_cell_value(company.get("email"))
    raw = company.get("raw") if isinstance(company.get("raw"), dict) else {}
    conn.execute(
        """
        INSERT INTO bitrix_companies_cache(
            company_id, title, normalized_title, company_type,
            phone, normalized_phone, email, normalized_email,
            contact_details, source, sync_status, sync_error,
            usage_count, last_used_at, active, raw_json,
            fetched_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', 'bitrix', 'synced', '', 0, '', 1, ?, ?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET
            title = excluded.title,
            normalized_title = excluded.normalized_title,
            company_type = excluded.company_type,
            phone = excluded.phone,
            normalized_phone = excluded.normalized_phone,
            email = excluded.email,
            normalized_email = excluded.normalized_email,
            source = 'bitrix',
            sync_status = 'synced',
            sync_error = '',
            active = 1,
            raw_json = excluded.raw_json,
            fetched_at = excluded.fetched_at,
            updated_at = excluded.updated_at
        """,
        (
            company_id,
            title,
            normalize_company_title_key(title),
            clean_cell_value(company.get("companyType")) or SUPPLIER_COMPANY_TYPE,
            phone,
            normalize_company_phone(phone),
            email,
            normalize_company_email(email),
            json.dumps(raw, ensure_ascii=False),
            fetched_at,
            fetched_at,
            fetched_at,
        ),
    )


def _replace_remote_cache(companies: list[dict], *, fetched_at: str) -> None:
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            UPDATE bitrix_companies_cache
            SET active = 0, updated_at = ?
            WHERE source = 'bitrix' AND company_type = ?
            """,
            (fetched_at, SUPPLIER_COMPANY_TYPE),
        )
        for company in companies:
            _upsert_remote_company_in_transaction(
                conn,
                company,
                fetched_at=fetched_at,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _company_select_payload(start: int) -> dict:
    fields = ["ID", "TITLE", "COMPANY_TYPE", "PHONE", "EMAIL"]
    payload = {
        "filter[COMPANY_TYPE]": SUPPLIER_COMPANY_TYPE,
        "order[ID]": "ASC",
        "start": start,
    }
    for index, field in enumerate(fields):
        payload[f"select[{index}]"] = field
    return payload


def synchronize_bitrix_companies(*, force: bool = False) -> dict:
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

        companies: list[dict] = []
        seen: set[str] = set()
        start = 0
        total = None
        try:
            for _page in range(BITRIX_COMPANY_MAX_PAGES):
                response = bitrix_webhook_call(
                    "crm.company.list",
                    _company_select_payload(start),
                )
                if not isinstance(response, dict):
                    raise BitrixCompanySyncError("invalid crm.company.list response")
                if response.get("error"):
                    raise BitrixCompanySyncError(
                        clean_cell_value(response.get("error_description"))
                        or clean_cell_value(response.get("error"))
                    )
                raw_companies = response.get("result")
                if not isinstance(raw_companies, list):
                    raise BitrixCompanySyncError(
                        "crm.company.list result is not an array"
                    )
                for raw_company in raw_companies:
                    normalized = normalize_bitrix_company(raw_company)
                    if not normalized:
                        continue
                    if normalized.get("companyType") != SUPPLIER_COMPANY_TYPE:
                        continue
                    company_id = normalized["companyId"]
                    if company_id in seen:
                        continue
                    seen.add(company_id)
                    companies.append(normalized)

                try:
                    total = int(response.get("total"))
                except Exception:
                    total = None
                next_value = response.get("next")
                if next_value not in {None, ""}:
                    try:
                        next_start = int(next_value)
                    except Exception:
                        next_start = start + BITRIX_COMPANY_PAGE_SIZE
                else:
                    next_start = start + BITRIX_COMPANY_PAGE_SIZE

                if not raw_companies:
                    break
                if total is not None and next_start >= total:
                    break
                if len(raw_companies) < BITRIX_COMPANY_PAGE_SIZE and next_value in {None, ""}:
                    break
                start = next_start
            else:
                raise BitrixCompanySyncError(
                    "Bitrix company pagination safety limit reached"
                )

            fetched_at = _iso_now()
            _replace_remote_cache(companies, fetched_at=fetched_at)
            stats_after = _cache_stats()
            return {
                "ok": True,
                "status": "synced",
                "configured": True,
                "refreshed": True,
                "error": "",
                "receivedCount": len(companies),
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


def _remote_list_exact(field: str, value: str) -> dict | None:
    if not clean_cell_value(value):
        return None
    payload = _company_select_payload(0)
    payload[f"filter[{field}]"] = value
    response = bitrix_webhook_call("crm.company.list", payload)
    if not isinstance(response, dict):
        raise BitrixCompanySyncError("invalid crm.company.list response")
    if response.get("error"):
        raise BitrixCompanySyncError(
            clean_cell_value(response.get("error_description"))
            or clean_cell_value(response.get("error"))
        )
    result = response.get("result")
    if not isinstance(result, list):
        raise BitrixCompanySyncError("crm.company.list result is not an array")
    for raw in result:
        normalized = normalize_bitrix_company(raw)
        if normalized and normalized.get("companyType") == SUPPLIER_COMPANY_TYPE:
            return normalized
    return None


def _find_remote_supplier(*, title: str, phone: str, email: str) -> dict | None:
    if phone:
        match = _remote_list_exact("PHONE", phone)
        if match:
            return match
    if email:
        match = _remote_list_exact("EMAIL", email)
        if match:
            return match
    if title:
        match = _remote_list_exact("TITLE", title)
        if match:
            return match
    return None


def _company_fields_payload(
    *,
    title: str,
    phone: str,
    email: str,
) -> dict:
    payload = {
        "fields[TITLE]": title,
        "fields[COMPANY_TYPE]": SUPPLIER_COMPANY_TYPE,
        "fields[OPENED]": "Y",
    }
    if phone:
        payload["fields[PHONE][0][VALUE]"] = phone
        payload["fields[PHONE][0][VALUE_TYPE]"] = "WORK"
    if email:
        payload["fields[EMAIL][0][VALUE]"] = email
        payload["fields[EMAIL][0][VALUE_TYPE]"] = "WORK"
    return payload


def _get_remote_company(company_id: str) -> dict | None:
    response = bitrix_webhook_call(
        "crm.company.get",
        {"id": clean_cell_value(company_id)},
    )
    if not isinstance(response, dict):
        raise BitrixCompanySyncError("invalid crm.company.get response")
    if response.get("error"):
        raise BitrixCompanySyncError(
            clean_cell_value(response.get("error_description"))
            or clean_cell_value(response.get("error"))
        )
    return normalize_bitrix_company(response.get("result"))


def _mark_sync_error(company_id: str, error: str) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE bitrix_companies_cache
            SET sync_status = 'error', sync_error = ?, updated_at = ?
            WHERE company_id = ?
            """,
            (clean_cell_value(error), utc_now_iso(), clean_cell_value(company_id)),
        )
        conn.commit()
    finally:
        conn.close()


def _promote_local_company(
    *,
    local_company_id: str,
    remote_company: dict,
) -> dict:
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        local_row = conn.execute(
            "SELECT * FROM bitrix_companies_cache WHERE company_id = ?",
            (local_company_id,),
        ).fetchone()
        local = dict(local_row) if local_row else {}
        remote = dict(remote_company)
        if not clean_cell_value(remote.get("phone")):
            remote["phone"] = clean_cell_value(local.get("phone"))
        if not clean_cell_value(remote.get("email")):
            remote["email"] = clean_cell_value(local.get("email"))
        _upsert_remote_company_in_transaction(conn, remote, fetched_at=now)
        conn.execute(
            """
            UPDATE bitrix_companies_cache
            SET contact_details = ?, usage_count = ?, last_used_at = ?, updated_at = ?
            WHERE company_id = ?
            """,
            (
                clean_cell_value(local.get("contact_details")),
                int(local.get("usage_count") or 0),
                clean_cell_value(local.get("last_used_at")),
                now,
                remote.get("companyId"),
            ),
        )
        conn.execute(
            "UPDATE notification_recipients SET company_id = ? WHERE company_id = ?",
            (remote.get("companyId"), local_company_id),
        )
        if local_company_id != remote.get("companyId"):
            conn.execute(
                "DELETE FROM bitrix_companies_cache WHERE company_id = ?",
                (local_company_id,),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    resolved = get_cached_bitrix_company(remote.get("companyId") or "")
    if not resolved:
        raise RuntimeError("remote contractor cache promotion failed")
    return resolved


def ensure_supplier_company_synced(company_id: str) -> dict:
    company = get_cached_bitrix_company(company_id)
    if not company:
        raise ExternalContractorValidationError("external contractor not found")
    if not clean_cell_value(settings.BITRIX_TECH_WEBHOOK_URL):
        return {
            "ok": False,
            "status": "disabled",
            "error": "BITRIX_TECH_WEBHOOK_URL is empty",
            "company": company,
        }
    if company.get("source") == "bitrix" and company.get("syncStatus") == "synced":
        return {"ok": True, "status": "synced", "error": "", "company": company}

    try:
        remote = _find_remote_supplier(
            title=company.get("title") or "",
            phone=company.get("phone") or "",
            email=company.get("email") or "",
        )
        if remote:
            update_payload = {"id": remote.get("companyId")}
            needs_update = False
            if not clean_cell_value(remote.get("phone")) and clean_cell_value(company.get("phone")):
                update_payload["fields[PHONE][0][VALUE]"] = company.get("phone")
                update_payload["fields[PHONE][0][VALUE_TYPE]"] = "WORK"
                needs_update = True
            if not clean_cell_value(remote.get("email")) and clean_cell_value(company.get("email")):
                update_payload["fields[EMAIL][0][VALUE]"] = company.get("email")
                update_payload["fields[EMAIL][0][VALUE_TYPE]"] = "WORK"
                needs_update = True
            if needs_update:
                response = bitrix_webhook_call("crm.company.update", update_payload)
                if not isinstance(response, dict) or response.get("error"):
                    raise BitrixCompanySyncError(
                        clean_cell_value((response or {}).get("error_description"))
                        or clean_cell_value((response or {}).get("error"))
                        or "crm.company.update failed"
                    )
                remote = _get_remote_company(remote.get("companyId") or "") or remote
        else:
            response = bitrix_webhook_call(
                "crm.company.add",
                _company_fields_payload(
                    title=company.get("title") or "",
                    phone=company.get("phone") or "",
                    email=company.get("email") or "",
                ),
            )
            if not isinstance(response, dict) or response.get("error"):
                raise BitrixCompanySyncError(
                    clean_cell_value((response or {}).get("error_description"))
                    or clean_cell_value((response or {}).get("error"))
                    or "crm.company.add failed"
                )
            remote_id = clean_cell_value(response.get("result"))
            if not remote_id:
                raise BitrixCompanySyncError("crm.company.add returned empty ID")
            remote = _get_remote_company(remote_id) or {
                "companyId": remote_id,
                "title": company.get("title") or "",
                "companyType": SUPPLIER_COMPANY_TYPE,
                "phone": company.get("phone") or "",
                "email": company.get("email") or "",
                "source": "bitrix",
                "syncStatus": "synced",
                "syncError": "",
                "active": True,
                "raw": {"ID": remote_id},
            }

        synced = _promote_local_company(
            local_company_id=company.get("companyId") or "",
            remote_company=remote,
        )
        return {"ok": True, "status": "synced", "error": "", "company": synced}
    except Exception as exc:
        _mark_sync_error(company.get("companyId") or "", str(exc))
        failed = get_cached_bitrix_company(company.get("companyId") or "") or company
        return {"ok": False, "status": "error", "error": str(exc), "company": failed}
