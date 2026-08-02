from __future__ import annotations

import unicodedata
import uuid
from typing import Any

from app.db import get_conn
from app.checklists.edit_sessions import utc_now_iso
from app.checklists.utils import clean_cell_value


MAX_ASSIGNMENT_PART_LENGTH = 200
DEFAULT_LIST_LIMIT = 50
MAX_LIST_LIMIT = 200


class AssignmentPartError(RuntimeError):
    pass


class AssignmentPartNotFoundError(AssignmentPartError):
    pass


class AssignmentPartValidationError(AssignmentPartError):
    pass


def normalize_assignment_part_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", clean_cell_value(value))
    text = " ".join(text.split())
    if len(text) > MAX_ASSIGNMENT_PART_LENGTH:
        raise AssignmentPartValidationError(
            f"assignment part must not exceed {MAX_ASSIGNMENT_PART_LENGTH} characters"
        )
    return text


def normalize_assignment_part_key(value: Any) -> str:
    return normalize_assignment_part_text(value).casefold()


def _public_assignment_part(row: dict) -> dict:
    return {
        "assignmentPartId": row.get("assignment_part_id") or "",
        "id": row.get("assignment_part_id") or "",
        "text": row.get("name") or "",
        "name": row.get("name") or "",
        "active": bool(int(row.get("active") or 0)),
        "usageCount": int(row.get("usage_count") or 0),
        "createdById": row.get("created_by_id") or "",
        "createdByName": row.get("created_by_name") or "",
        "createdAt": row.get("created_at") or "",
        "updatedAt": row.get("updated_at") or "",
        "lastUsedAt": row.get("last_used_at") or "",
    }


def _row_by_id(conn, assignment_part_id: str) -> dict | None:
    row = conn.execute(
        """
        SELECT *
        FROM assignment_parts
        WHERE assignment_part_id = ?
        """,
        (clean_cell_value(assignment_part_id),),
    ).fetchone()
    return dict(row) if row else None


def _row_by_key(conn, normalized_name: str) -> dict | None:
    row = conn.execute(
        """
        SELECT *
        FROM assignment_parts
        WHERE normalized_name = ?
        """,
        (normalized_name,),
    ).fetchone()
    return dict(row) if row else None


def resolve_assignment_part_in_transaction(
    conn,
    *,
    assignment_part_id: str = "",
    assignment_part_text: str = "",
    user_id: str = "",
    user_name: str = "",
    mark_used: bool = False,
) -> dict:
    normalized_id = clean_cell_value(assignment_part_id)
    normalized_text = normalize_assignment_part_text(assignment_part_text)
    now = utc_now_iso()

    row = _row_by_id(conn, normalized_id) if normalized_id else None
    if row:
        if not bool(int(row.get("active") or 0)):
            raise AssignmentPartValidationError("assignment part is inactive")
        if normalized_text:
            expected_key = normalize_assignment_part_key(row.get("name") or "")
            supplied_key = normalize_assignment_part_key(normalized_text)
            if expected_key != supplied_key:
                raise AssignmentPartValidationError(
                    "assignmentPartId does not match assignmentPartText"
                )
    elif normalized_id and not normalized_text:
        raise AssignmentPartNotFoundError("assignment part not found")

    if not row and normalized_text:
        normalized_key = normalize_assignment_part_key(normalized_text)
        row = _row_by_key(conn, normalized_key)
        if row and not bool(int(row.get("active") or 0)):
            conn.execute(
                """
                UPDATE assignment_parts
                SET active = 1,
                    name = ?,
                    updated_at = ?
                WHERE assignment_part_id = ?
                """,
                (normalized_text, now, row.get("assignment_part_id") or ""),
            )
            row = _row_by_id(conn, row.get("assignment_part_id") or "")

    if not row and normalized_text:
        assignment_part_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO assignment_parts(
                assignment_part_id,
                name,
                normalized_name,
                active,
                usage_count,
                created_by_id,
                created_by_name,
                created_at,
                updated_at,
                last_used_at
            )
            VALUES (?, ?, ?, 1, 0, ?, ?, ?, ?, '')
            """,
            (
                assignment_part_id,
                normalized_text,
                normalize_assignment_part_key(normalized_text),
                clean_cell_value(user_id),
                clean_cell_value(user_name),
                now,
                now,
            ),
        )
        row = _row_by_id(conn, assignment_part_id)

    if not row:
        return {
            "assignmentPartId": "",
            "id": "",
            "text": "",
            "name": "",
            "active": False,
            "usageCount": 0,
            "createdById": "",
            "createdByName": "",
            "createdAt": "",
            "updatedAt": "",
            "lastUsedAt": "",
        }

    if mark_used:
        conn.execute(
            """
            UPDATE assignment_parts
            SET usage_count = COALESCE(usage_count, 0) + 1,
                last_used_at = ?,
                updated_at = ?
            WHERE assignment_part_id = ?
            """,
            (now, now, row.get("assignment_part_id") or ""),
        )
        row = _row_by_id(conn, row.get("assignment_part_id") or "")

    return _public_assignment_part(row or {})


def create_or_get_assignment_part(
    *,
    text: str,
    user_id: str = "",
    user_name: str = "",
) -> dict:
    normalized_text = normalize_assignment_part_text(text)
    if not normalized_text:
        raise AssignmentPartValidationError("assignment part text is required")

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        result = resolve_assignment_part_in_transaction(
            conn,
            assignment_part_text=normalized_text,
            user_id=user_id,
            user_name=user_name,
            mark_used=False,
        )
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_assignment_parts(
    *,
    query: str = "",
    limit: int = DEFAULT_LIST_LIMIT,
) -> list[dict]:
    try:
        normalized_limit = int(limit)
    except (TypeError, ValueError):
        normalized_limit = DEFAULT_LIST_LIMIT
    normalized_limit = max(1, min(normalized_limit, MAX_LIST_LIMIT))
    normalized_query = normalize_assignment_part_key(query) if clean_cell_value(query) else ""

    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM assignment_parts
            WHERE active = 1
            ORDER BY
                CASE WHEN COALESCE(last_used_at, '') = '' THEN 1 ELSE 0 END,
                last_used_at DESC,
                usage_count DESC,
                name COLLATE NOCASE ASC,
                assignment_part_id ASC
            """
        ).fetchall()
        result = []
        for raw_row in rows:
            row = dict(raw_row)
            if normalized_query and normalized_query not in clean_cell_value(
                row.get("normalized_name")
            ):
                continue
            result.append(_public_assignment_part(row))
            if len(result) >= normalized_limit:
                break
        return result
    finally:
        conn.close()


def backfill_assignment_parts_from_drafts() -> int:
    conn = get_conn()
    updated = 0
    try:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            """
            SELECT draft_id,
                   assignment_part_id,
                   assignment_part_text,
                   created_by_id,
                   created_by_name
            FROM notification_drafts
            WHERE COALESCE(TRIM(assignment_part_text), '') <> ''
            ORDER BY created_at ASC, draft_id ASC
            """
        ).fetchall()
        for raw_row in rows:
            row = dict(raw_row)
            resolved = resolve_assignment_part_in_transaction(
                conn,
                assignment_part_id=row.get("assignment_part_id") or "",
                assignment_part_text=row.get("assignment_part_text") or "",
                user_id=row.get("created_by_id") or "",
                user_name=row.get("created_by_name") or "",
                mark_used=False,
            )
            resolved_id = resolved.get("id") or ""
            resolved_text = resolved.get("text") or ""
            if (
                clean_cell_value(row.get("assignment_part_id")) != resolved_id
                or clean_cell_value(row.get("assignment_part_text")) != resolved_text
            ):
                conn.execute(
                    """
                    UPDATE notification_drafts
                    SET assignment_part_id = ?,
                        assignment_part_text = ?
                    WHERE draft_id = ?
                    """,
                    (resolved_id, resolved_text, row.get("draft_id") or ""),
                )
                updated += 1
        conn.commit()
        return updated
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
