from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
import threading
import uuid
from datetime import datetime, timezone

from app.db import get_conn, init_db
from app.settings import PUBLIC_FOLDER_SIGNING_KEY_PATH

from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


PUBLIC_FOLDER_TOKEN_VERSION = "v1"
_signing_key_lock = threading.Lock()
_signing_key_cache: tuple[str, bytes] | None = None


class PublicFolderLinkError(RuntimeError):
    pass


class PublicFolderLinkNotFoundError(PublicFolderLinkError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_public_folder_link_schema() -> None:
    # init_db is idempotent and owns the canonical schema migration.
    init_db()


def _urlsafe(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _signing_key() -> bytes:
    global _signing_key_cache

    key_path = PUBLIC_FOLDER_SIGNING_KEY_PATH.resolve()
    cache_key = str(key_path)
    cached = _signing_key_cache
    if cached and cached[0] == cache_key:
        return cached[1]

    with _signing_key_lock:
        cached = _signing_key_cache
        if cached and cached[0] == cache_key:
            return cached[1]

        key_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(
                str(key_path),
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
        except FileExistsError:
            descriptor = -1
        else:
            with os.fdopen(descriptor, "wb") as output:
                output.write(secrets.token_bytes(48))

        key = key_path.read_bytes()
        if len(key) < 32:
            raise PublicFolderLinkError(
                "public folder signing key is invalid"
            )

        try:
            os.chmod(key_path, 0o600)
        except OSError:
            # Windows may ignore POSIX modes. The file remains under Volume/db.
            pass

        _signing_key_cache = (cache_key, key)
        return key


def _token_signature(link_id: str, generation: int) -> str:
    message = "\n".join((
        PUBLIC_FOLDER_TOKEN_VERSION,
        clean_cell_value(link_id),
        str(int(generation or 0)),
    )).encode("utf-8")
    return _urlsafe(
        hmac.new(_signing_key(), message, hashlib.sha256).digest()
    )


def build_public_folder_token(link_id: str, generation: int) -> str:
    normalized_link_id = clean_cell_value(link_id)
    normalized_generation = int(generation or 0)
    if not normalized_link_id or normalized_generation <= 0:
        raise ValueError("linkId and generation are required")
    return ".".join((
        PUBLIC_FOLDER_TOKEN_VERSION,
        normalized_link_id,
        str(normalized_generation),
        _token_signature(normalized_link_id, normalized_generation),
    ))


def parse_public_folder_token(token: str) -> tuple[str, int] | None:
    parts = clean_cell_value(token).split(".")
    if len(parts) != 4 or parts[0] != PUBLIC_FOLDER_TOKEN_VERSION:
        return None

    link_id = clean_cell_value(parts[1])
    try:
        generation = int(parts[2])
    except (TypeError, ValueError):
        return None

    if (
        len(link_id) != 32
        or generation <= 0
        or not all(character in "0123456789abcdef" for character in link_id)
    ):
        return None

    expected = _token_signature(link_id, generation)
    if not hmac.compare_digest(expected, clean_cell_value(parts[3])):
        return None

    return link_id, generation


def _row_to_dict(row) -> dict | None:
    return dict(row) if row else None


def get_public_folder_link_by_item(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
) -> dict | None:
    ensure_public_folder_link_schema()
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT * FROM public_folder_links
            WHERE dialog_id = ?
              AND checklist_key = ?
              AND item_id = ?
            LIMIT 1
            """,
            (
                normalize_dialog_id(dialog_id),
                normalize_checklist_key(checklist_key),
                clean_cell_value(item_id),
            ),
        ).fetchone()
    finally:
        conn.close()
    return _row_to_dict(row)


def get_or_create_public_folder_link(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    acting_user_id: str = "",
    acting_user_name: str = "",
) -> dict:
    ensure_public_folder_link_schema()
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(checklist_key)
    normalized_item_id = clean_cell_value(item_id)
    if not normalized_dialog_id or not normalized_item_id:
        raise ValueError("dialogId and itemId are required")

    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT * FROM public_folder_links
            WHERE dialog_id = ?
              AND checklist_key = ?
              AND item_id = ?
            LIMIT 1
            """,
            (
                normalized_dialog_id,
                normalized_checklist_key,
                normalized_item_id,
            ),
        ).fetchone()

        created = False
        if row:
            record = dict(row)
            if clean_cell_value(record.get("status")) != "active":
                next_generation = max(1, int(record.get("generation") or 0) + 1)
                conn.execute(
                    """
                    UPDATE public_folder_links
                    SET generation = ?, status = 'active',
                        last_reissued_by_id = ?,
                        last_reissued_by_name = ?,
                        updated_at = ?, revoked_at = ''
                    WHERE link_id = ?
                    """,
                    (
                        next_generation,
                        clean_cell_value(acting_user_id),
                        clean_cell_value(acting_user_name),
                        now,
                        record["link_id"],
                    ),
                )
        else:
            created = True
            link_id = uuid.uuid4().hex
            conn.execute(
                """
                INSERT INTO public_folder_links(
                    link_id, dialog_id, checklist_key, item_id,
                    generation, status, created_by_id, created_by_name,
                    last_reissued_by_id, last_reissued_by_name,
                    created_at, updated_at, revoked_at
                )
                VALUES (?, ?, ?, ?, 1, 'active', ?, ?, '', '', ?, ?, '')
                """,
                (
                    link_id,
                    normalized_dialog_id,
                    normalized_checklist_key,
                    normalized_item_id,
                    clean_cell_value(acting_user_id),
                    clean_cell_value(acting_user_name),
                    now,
                    now,
                ),
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    record = get_public_folder_link_by_item(
        normalized_dialog_id,
        normalized_checklist_key,
        normalized_item_id,
    )
    if not record:
        raise PublicFolderLinkError("public folder link was not saved")
    record["created"] = created
    return record


def reissue_public_folder_link(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    acting_user_id: str = "",
    acting_user_name: str = "",
) -> dict:
    current = get_or_create_public_folder_link(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_id=item_id,
        acting_user_id=acting_user_id,
        acting_user_name=acting_user_name,
    )
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM public_folder_links WHERE link_id = ?",
            (current["link_id"],),
        ).fetchone()
        if not row:
            raise PublicFolderLinkNotFoundError(
                "public folder link not found"
            )
        next_generation = max(1, int(row["generation"] or 0) + 1)
        conn.execute(
            """
            UPDATE public_folder_links
            SET generation = ?, status = 'active',
                last_reissued_by_id = ?,
                last_reissued_by_name = ?,
                updated_at = ?, revoked_at = ''
            WHERE link_id = ?
            """,
            (
                next_generation,
                clean_cell_value(acting_user_id),
                clean_cell_value(acting_user_name),
                now,
                current["link_id"],
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    refreshed = get_public_folder_link_by_item(
        dialog_id,
        checklist_key,
        item_id,
    )
    if not refreshed:
        raise PublicFolderLinkError("public folder link was not reissued")
    refreshed["reissued"] = True
    return refreshed


def resolve_public_folder_token(token: str) -> dict | None:
    parsed = parse_public_folder_token(token)
    if not parsed:
        return None

    link_id, generation = parsed
    ensure_public_folder_link_schema()
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT * FROM public_folder_links
            WHERE link_id = ?
              AND generation = ?
              AND status = 'active'
            LIMIT 1
            """,
            (link_id, generation),
        ).fetchone()
    finally:
        conn.close()
    return _row_to_dict(row)


def public_folder_link_path(record: dict) -> str:
    token = build_public_folder_token(
        record.get("link_id") or "",
        int(record.get("generation") or 0),
    )
    return "/public/folder/" + token


def public_folder_link_payload(record: dict | None) -> dict:
    if not record:
        return {}
    return {
        "linkId": record.get("link_id") or "",
        "dialogId": record.get("dialog_id") or "",
        "checklistKey": record.get("checklist_key") or "",
        "itemId": record.get("item_id") or "",
        "generation": int(record.get("generation") or 0),
        "status": record.get("status") or "",
        "createdAt": record.get("created_at") or "",
        "updatedAt": record.get("updated_at") or "",
    }
