from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from pathlib import Path
from typing import Any

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.settings import EDIT_SESSION_FILE_ROOT, UPLOAD_ROOT
from app.checklists.edit_sessions import (
    EditSessionConflictError,
    get_edit_session_for_actor,
    utc_now_iso,
)
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


FILE_ENTRY_KIND_CREATED = "created"
FILE_ENTRY_KIND_STASHED = "stashed"

FILE_ENTRY_STATUS_STAGING = "staging"
FILE_ENTRY_STATUS_ACTIVE = "active"
FILE_ENTRY_STATUS_COMMITTING = "committing"
FILE_ENTRY_STATUS_COMMITTED = "committed"
FILE_ENTRY_STATUS_ROLLING_BACK = "rolling_back"
FILE_ENTRY_STATUS_ROLLED_BACK = "rolled_back"
FILE_ENTRY_STATUS_ERROR = "error"


class EditSessionFileError(RuntimeError):
    pass


class EditSessionFilePathError(EditSessionFileError):
    pass


class EditSessionFileConflictError(EditSessionFileError):
    pass


def stable_json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def json_loads(value: str, default: Any) -> Any:
    raw = clean_cell_value(value)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def safe_path_component(value: str, fallback: str) -> str:
    normalized = re.sub(
        r"[^A-Za-z0-9_.-]+",
        "_",
        clean_cell_value(value),
    ).strip("._")
    return normalized or fallback


def normalized_absolute_path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve(strict=False)


def is_path_within(path: Path, root: Path) -> bool:
    normalized_path = os.path.normcase(
        str(normalized_absolute_path(path))
    )
    normalized_root = os.path.normcase(
        str(normalized_absolute_path(root))
    )
    try:
        common = os.path.commonpath([
            normalized_path,
            normalized_root,
        ])
    except ValueError:
        return False
    return common == normalized_root


def require_upload_path(value: str | Path) -> Path:
    path = normalized_absolute_path(value)
    root = normalized_absolute_path(UPLOAD_ROOT)
    if not is_path_within(path, root):
        raise EditSessionFilePathError(
            "file path is outside checklist upload root"
        )
    return path


def require_session_file_path(value: str | Path) -> Path:
    path = normalized_absolute_path(value)
    root = normalized_absolute_path(EDIT_SESSION_FILE_ROOT)
    if not is_path_within(path, root):
        raise EditSessionFilePathError(
            "staged path is outside edit session file root"
        )
    return path


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(
            lambda: source.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_edit_session_file_schema() -> None:
    EDIT_SESSION_FILE_ROOT.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS edit_session_file_entries (
                entry_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                sequence_no INTEGER NOT NULL,
                operation_id TEXT,
                operation_type TEXT,
                dialog_id TEXT,
                checklist_key TEXT,
                item_id TEXT,
                series_id TEXT,
                document_id TEXT,
                entry_kind TEXT NOT NULL,
                original_path TEXT NOT NULL,
                staged_path TEXT,
                file_name TEXT,
                file_size INTEGER DEFAULT 0,
                sha256 TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                metadata_json TEXT,
                error TEXT,
                created_at TEXT,
                updated_at TEXT,
                committed_at TEXT,
                rolled_back_at TEXT,
                cleanup_at TEXT
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_edit_session_file_entries_seq
            ON edit_session_file_entries(session_id, sequence_no)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_edit_session_file_entries_status
            ON edit_session_file_entries(session_id, status, sequence_no)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_edit_session_file_entries_operation
            ON edit_session_file_entries(operation_id, entry_kind, status)
        """)
        conn.commit()
    finally:
        conn.close()


def next_file_entry_sequence(conn, session_id: str) -> int:
    row = conn.execute("""
        SELECT COALESCE(MAX(sequence_no), 0) AS max_sequence
        FROM edit_session_file_entries
        WHERE session_id = ?
    """, (clean_cell_value(session_id),)).fetchone()
    return int(row["max_sequence"] or 0) + 1


def session_stash_path(
    *,
    session_id: str,
    operation_id: str,
    entry_id: str,
    original_path: Path,
) -> Path:
    # Не повторяем полный исходный относительный путь внутри корзины.
    # На Windows прежняя структура могла превышать MAX_PATH (260 символов).
    # Исходный путь уже хранится отдельно в original_path таблицы,
    # поэтому для временного файла достаточно уникального entry_id.
    del operation_id

    session_component = safe_path_component(
        session_id,
        "session",
    )[:16]
    entry_component = safe_path_component(
        entry_id,
        "entry",
    )[:32]

    suffix = re.sub(
        r"[^A-Za-z0-9.]+",
        "",
        original_path.suffix,
    )[:16]

    staged_name = "stash" + suffix

    return (
        normalized_absolute_path(EDIT_SESSION_FILE_ROOT)
        / session_component
        / entry_component
        / staged_name
    )


def _validate_active_session(
    *,
    session_id: str,
    dialog_id: str,
    user_id: str = "",
) -> dict:
    session = get_edit_session_for_actor(
        session_id=clean_cell_value(session_id),
        dialog_id=normalize_dialog_id(dialog_id),
        user_id=clean_cell_value(user_id),
    )
    if session.get("status") != "active":
        raise EditSessionConflictError(
            "file operation requires active edit session"
        )
    return session


def _insert_file_entry(
    *,
    session_id: str,
    operation_id: str,
    operation_type: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    series_id: str,
    document_id: str,
    entry_kind: str,
    original_path: Path,
    staged_path: Path | None,
    file_name: str,
    file_size: int,
    checksum: str,
    status: str,
    metadata: dict | None,
) -> dict:
    ensure_edit_session_file_schema()
    entry_id = uuid.uuid4().hex
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        sequence_no = next_file_entry_sequence(conn, session_id)
        conn.execute("""
            INSERT INTO edit_session_file_entries(
                entry_id, session_id, sequence_no, operation_id,
                operation_type, dialog_id, checklist_key, item_id,
                series_id, document_id, entry_kind, original_path,
                staged_path, file_name, file_size, sha256, status,
                metadata_json, error, created_at, updated_at,
                committed_at, rolled_back_at, cleanup_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                '', ?, ?, '', '', ''
            )
        """, (
            entry_id,
            clean_cell_value(session_id),
            sequence_no,
            clean_cell_value(operation_id),
            clean_cell_value(operation_type),
            normalize_dialog_id(dialog_id),
            normalize_checklist_key(checklist_key),
            clean_cell_value(item_id),
            clean_cell_value(series_id),
            clean_cell_value(document_id),
            clean_cell_value(entry_kind),
            str(original_path),
            str(staged_path) if staged_path else "",
            clean_cell_value(file_name),
            int(file_size or 0),
            clean_cell_value(checksum),
            clean_cell_value(status),
            stable_json_dumps(metadata or {}),
            now,
            now,
        ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return get_file_entry(entry_id) or {}


def get_file_entry(entry_id: str) -> dict | None:
    ensure_edit_session_file_schema()
    normalized_entry_id = clean_cell_value(entry_id)
    if not normalized_entry_id:
        return None
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT * FROM edit_session_file_entries
            WHERE entry_id = ?
        """, (normalized_entry_id,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def register_created_file(
    *,
    session_id: str,
    operation_id: str,
    operation_type: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    file_path: str | Path,
    user_id: str = "",
    series_id: str = "",
    document_id: str = "",
    metadata: dict | None = None,
) -> dict:
    _validate_active_session(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )
    path = require_upload_path(file_path)
    if not path.is_file():
        raise FileNotFoundError(
            "created session file does not exist: " + str(path)
        )
    return _insert_file_entry(
        session_id=session_id,
        operation_id=operation_id,
        operation_type=operation_type,
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_id=item_id,
        series_id=series_id,
        document_id=document_id,
        entry_kind=FILE_ENTRY_KIND_CREATED,
        original_path=path,
        staged_path=None,
        file_name=path.name,
        file_size=path.stat().st_size,
        checksum=file_sha256(path),
        status=FILE_ENTRY_STATUS_ACTIVE,
        metadata=metadata,
    )


def stash_existing_file(
    *,
    session_id: str,
    operation_id: str,
    operation_type: str,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    file_path: str | Path,
    user_id: str = "",
    series_id: str = "",
    document_id: str = "",
    metadata: dict | None = None,
) -> dict:
    _validate_active_session(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )
    source = require_upload_path(file_path)
    if not source.is_file():
        raise FileNotFoundError(
            "file to stash does not exist: " + str(source)
        )

    ensure_edit_session_file_schema()
    entry_id = uuid.uuid4().hex
    staged = session_stash_path(
        session_id=session_id,
        operation_id=operation_id,
        entry_id=entry_id,
        original_path=source,
    )
    require_session_file_path(staged)
    size = source.stat().st_size
    checksum = file_sha256(source)
    now = utc_now_iso()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        sequence_no = next_file_entry_sequence(conn, session_id)
        conn.execute("""
            INSERT INTO edit_session_file_entries(
                entry_id, session_id, sequence_no, operation_id,
                operation_type, dialog_id, checklist_key, item_id,
                series_id, document_id, entry_kind, original_path,
                staged_path, file_name, file_size, sha256, status,
                metadata_json, error, created_at, updated_at,
                committed_at, rolled_back_at, cleanup_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                '', ?, ?, '', '', ''
            )
        """, (
            entry_id,
            clean_cell_value(session_id),
            sequence_no,
            clean_cell_value(operation_id),
            clean_cell_value(operation_type),
            normalize_dialog_id(dialog_id),
            normalize_checklist_key(checklist_key),
            clean_cell_value(item_id),
            clean_cell_value(series_id),
            clean_cell_value(document_id),
            FILE_ENTRY_KIND_STASHED,
            str(source),
            str(staged),
            source.name,
            int(size),
            checksum,
            FILE_ENTRY_STATUS_STAGING,
            stable_json_dumps(metadata or {}),
            now,
            now,
        ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    try:
        staged.parent.mkdir(parents=True, exist_ok=True)
        os.replace(source, staged)
        conn = get_conn()
        try:
            conn.execute("""
                UPDATE edit_session_file_entries
                SET status = 'active', error = '', updated_at = ?
                WHERE entry_id = ? AND status = 'staging'
            """, (utc_now_iso(), entry_id))
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        conn = get_conn()
        try:
            conn.execute("""
                UPDATE edit_session_file_entries
                SET status = 'error', error = ?, updated_at = ?
                WHERE entry_id = ?
            """, (str(exc), utc_now_iso(), entry_id))
            conn.commit()
        finally:
            conn.close()
        raise

    return get_file_entry(entry_id) or {}


def _relative_for_public(value: str, root: Path) -> str:
    raw = clean_cell_value(value)
    if not raw:
        return ""
    path = normalized_absolute_path(raw)
    if not is_path_within(path, root):
        return path.name
    return str(
        path.relative_to(normalized_absolute_path(root))
    ).replace("\\", "/")


def public_file_entry(record: dict | None) -> dict:
    if not record:
        return {}
    return {
        "entryId": record.get("entry_id") or "",
        "sessionId": record.get("session_id") or "",
        "sequenceNo": int(record.get("sequence_no") or 0),
        "operationId": record.get("operation_id") or "",
        "operationType": record.get("operation_type") or "",
        "dialogId": record.get("dialog_id") or "",
        "checklistKey": record.get("checklist_key") or "",
        "itemId": record.get("item_id") or "",
        "seriesId": record.get("series_id") or "",
        "documentId": record.get("document_id") or "",
        "entryKind": record.get("entry_kind") or "",
        "originalRelativePath": _relative_for_public(
            record.get("original_path") or "", UPLOAD_ROOT
        ),
        "stagedRelativePath": _relative_for_public(
            record.get("staged_path") or "", EDIT_SESSION_FILE_ROOT
        ),
        "fileName": record.get("file_name") or "",
        "fileSize": int(record.get("file_size") or 0),
        "sha256": record.get("sha256") or "",
        "status": record.get("status") or "",
        "metadata": json_loads(record.get("metadata_json") or "", {}),
        "error": record.get("error") or "",
        "createdAt": record.get("created_at") or "",
        "updatedAt": record.get("updated_at") or "",
        "committedAt": record.get("committed_at") or "",
        "rolledBackAt": record.get("rolled_back_at") or "",
        "cleanupAt": record.get("cleanup_at") or "",
    }


def list_edit_session_file_entries(
    *,
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
) -> list[dict]:
    ensure_edit_session_file_schema()
    session = get_edit_session_for_actor(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM edit_session_file_entries
            WHERE session_id = ? ORDER BY sequence_no ASC
        """, (session["session_id"],)).fetchall()
    finally:
        conn.close()
    return [public_file_entry(dict(row)) for row in rows]


def get_edit_session_file_counts(session_id: str) -> dict:
    ensure_edit_session_file_schema()
    normalized_session_id = clean_cell_value(session_id)
    if not normalized_session_id:
        return {
            "fileEntryCount": 0,
            "pendingFileEntryCount": 0,
            "stashedFileCount": 0,
            "createdFileCount": 0,
        }
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN status NOT IN ('committed', 'rolled_back')
                    THEN 1 ELSE 0 END) AS pending_count,
                SUM(CASE WHEN entry_kind = 'stashed'
                    THEN 1 ELSE 0 END) AS stashed_count,
                SUM(CASE WHEN entry_kind = 'created'
                    THEN 1 ELSE 0 END) AS created_count
            FROM edit_session_file_entries WHERE session_id = ?
        """, (normalized_session_id,)).fetchone()
    finally:
        conn.close()
    return {
        "fileEntryCount": int(row["total_count"] or 0),
        "pendingFileEntryCount": int(row["pending_count"] or 0),
        "stashedFileCount": int(row["stashed_count"] or 0),
        "createdFileCount": int(row["created_count"] or 0),
    }


def prepare_edit_session_files_for_commit(session_id: str) -> dict:
    ensure_edit_session_file_schema()
    normalized_session_id = clean_cell_value(session_id)
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM edit_session_file_entries
            WHERE session_id = ?
              AND status NOT IN ('committed', 'rolled_back')
            ORDER BY sequence_no ASC
        """, (normalized_session_id,)).fetchall()
    finally:
        conn.close()

    records = [dict(row) for row in rows]

    def created_file_was_consumed_by_later_stash(record: dict) -> bool:
        """Recognize a valid create -> replace/remove lineage.

        A file uploaded during the current edit session can be replaced or
        removed before that same session is committed.  The later operation
        stashes the just-created file and removes it from its original path.
        The old commit check treated that expected absence as external data
        loss and left the edit session stuck forever.

        We accept the missing created path only when a later active stash in
        the same session refers to the exact path and checksum and its staged
        copy is still present and intact.  Any other missing file remains a
        hard conflict.
        """
        original_value = clean_cell_value(record.get("original_path"))
        expected_hash = clean_cell_value(record.get("sha256"))
        sequence_no = int(record.get("sequence_no") or 0)
        if not original_value or not expected_hash:
            return False

        normalized_original = os.path.normcase(
            str(normalized_absolute_path(original_value))
        )
        for candidate in records:
            if int(candidate.get("sequence_no") or 0) <= sequence_no:
                continue
            if clean_cell_value(candidate.get("entry_kind")) != FILE_ENTRY_KIND_STASHED:
                continue
            candidate_original = clean_cell_value(
                candidate.get("original_path")
            )
            if not candidate_original:
                continue
            if os.path.normcase(
                str(normalized_absolute_path(candidate_original))
            ) != normalized_original:
                continue
            if clean_cell_value(candidate.get("sha256")) != expected_hash:
                continue

            staged = require_session_file_path(
                candidate.get("staged_path") or ""
            )
            if staged.is_file() and file_sha256(staged) == expected_hash:
                return True
        return False

    prepared = []
    for record in records:
        kind = clean_cell_value(record.get("entry_kind"))
        original = require_upload_path(record.get("original_path") or "")
        if kind == FILE_ENTRY_KIND_CREATED:
            if not original.is_file():
                if created_file_was_consumed_by_later_stash(record):
                    write_debug_log(
                        "edit_session_created_file_consumed",
                        {
                            "sessionId": normalized_session_id,
                            "entryId": record.get("entry_id") or "",
                            "sequenceNo": int(record.get("sequence_no") or 0),
                            "path": str(original),
                        },
                    )
                else:
                    raise EditSessionFileConflictError(
                        "created file is missing before commit: " + str(original)
                    )
            if (
                original.is_file()
                and
                clean_cell_value(record.get("sha256"))
                and file_sha256(original) != record.get("sha256")
            ):
                raise EditSessionFileConflictError(
                    "created file changed outside edit session: " + str(original)
                )
        elif kind == FILE_ENTRY_KIND_STASHED:
            staged = require_session_file_path(
                record.get("staged_path") or ""
            )
            if not staged.is_file():
                raise EditSessionFileConflictError(
                    "stashed file is missing before commit: " + str(staged)
                )
            if (
                clean_cell_value(record.get("sha256"))
                and file_sha256(staged) != record.get("sha256")
            ):
                raise EditSessionFileConflictError(
                    "stashed file changed outside edit session: " + str(staged)
                )
        else:
            raise EditSessionFileConflictError(
                "unknown edit session file entry kind: " + kind
            )
        prepared.append(record.get("entry_id") or "")

    conn = get_conn()
    try:
        conn.execute("""
            UPDATE edit_session_file_entries
            SET status = 'committing', error = '', updated_at = ?
            WHERE session_id = ?
              AND status NOT IN ('committed', 'rolled_back')
        """, (utc_now_iso(), normalized_session_id))
        conn.commit()
    finally:
        conn.close()
    return {
        "ok": True,
        "sessionId": normalized_session_id,
        "preparedCount": len(prepared),
        "entryIds": prepared,
    }


def mark_edit_session_file_entries_committed_in_transaction(
    conn,
    *,
    session_id: str,
    now: str,
) -> int:
    cur = conn.execute("""
        UPDATE edit_session_file_entries
        SET status = 'committed', committed_at = ?, error = '', updated_at = ?
        WHERE session_id = ? AND status <> 'rolled_back'
    """, (now, now, clean_cell_value(session_id)))
    return int(cur.rowcount or 0)


def _remove_file(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        raise EditSessionFileConflictError(
            "expected file but found directory: " + str(path)
        )
    path.unlink()


def _restore_stashed_file(record: dict) -> None:
    original = require_upload_path(record.get("original_path") or "")
    staged = require_session_file_path(record.get("staged_path") or "")
    expected_hash = clean_cell_value(record.get("sha256"))

    if staged.is_file():
        if original.exists():
            if not original.is_file():
                raise EditSessionFileConflictError(
                    "restore destination is not a file: " + str(original)
                )
            if expected_hash and file_sha256(original) == expected_hash:
                _remove_file(staged)
                return
            raise EditSessionFileConflictError(
                "restore destination already contains another file: "
                + str(original)
            )
        original.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged, original)
    elif original.is_file():
        if expected_hash and file_sha256(original) != expected_hash:
            raise EditSessionFileConflictError(
                "restored file checksum mismatch: " + str(original)
            )
    else:
        raise EditSessionFileConflictError(
            "both staged and original files are missing: " + str(original)
        )

    if expected_hash and file_sha256(original) != expected_hash:
        raise EditSessionFileConflictError(
            "restored file checksum mismatch: " + str(original)
        )


def rollback_edit_session_files(session_id: str) -> dict:
    ensure_edit_session_file_schema()
    normalized_session_id = clean_cell_value(session_id)
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM edit_session_file_entries
            WHERE session_id = ? AND status <> 'committed'
            ORDER BY sequence_no DESC
        """, (normalized_session_id,)).fetchall()
    finally:
        conn.close()

    rolled_back = []
    for row in rows:
        record = dict(row)
        if record.get("status") == FILE_ENTRY_STATUS_ROLLED_BACK:
            continue
        entry_id = record.get("entry_id") or ""
        conn = get_conn()
        try:
            conn.execute("""
                UPDATE edit_session_file_entries
                SET status = 'rolling_back', error = '', updated_at = ?
                WHERE entry_id = ? AND status <> 'committed'
            """, (utc_now_iso(), entry_id))
            conn.commit()
        finally:
            conn.close()

        try:
            kind = clean_cell_value(record.get("entry_kind"))
            if kind == FILE_ENTRY_KIND_CREATED:
                _remove_file(
                    require_upload_path(record.get("original_path") or "")
                )
            elif kind == FILE_ENTRY_KIND_STASHED:
                _restore_stashed_file(record)
            else:
                raise EditSessionFileConflictError(
                    "unknown edit session file entry kind: " + kind
                )
            rolled_back.append(entry_id)
        except Exception as exc:
            conn = get_conn()
            try:
                conn.execute("""
                    UPDATE edit_session_file_entries
                    SET status = 'error', error = ?, updated_at = ?
                    WHERE entry_id = ?
                """, (str(exc), utc_now_iso(), entry_id))
                conn.commit()
            finally:
                conn.close()
            raise

    return {
        "ok": True,
        "sessionId": normalized_session_id,
        "rolledBackCount": len(rolled_back),
        "entryIds": rolled_back,
    }


def rollback_edit_session_file_operation(
    *,
    session_id: str,
    operation_id: str,
) -> dict:
    ensure_edit_session_file_schema()
    normalized_session_id = clean_cell_value(session_id)
    normalized_operation_id = clean_cell_value(operation_id)

    if not normalized_session_id:
        raise ValueError("sessionId is required")
    if not normalized_operation_id:
        raise ValueError("operationId is required")

    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT *
            FROM edit_session_file_entries
            WHERE session_id = ?
              AND operation_id = ?
              AND status <> 'committed'
            ORDER BY sequence_no DESC
        """, (
            normalized_session_id,
            normalized_operation_id,
        )).fetchall()
    finally:
        conn.close()

    rolled_back = []

    for row in rows:
        record = dict(row)
        entry_id = clean_cell_value(record.get("entry_id"))

        if record.get("status") == FILE_ENTRY_STATUS_ROLLED_BACK:
            continue

        kind = clean_cell_value(record.get("entry_kind"))

        if kind == FILE_ENTRY_KIND_CREATED:
            _remove_file(
                require_upload_path(record.get("original_path") or "")
            )
        elif kind == FILE_ENTRY_KIND_STASHED:
            _restore_stashed_file(record)
        else:
            raise EditSessionFileConflictError(
                "unknown edit session file entry kind: " + kind
            )

        now = utc_now_iso()
        conn = get_conn()
        try:
            conn.execute("""
                UPDATE edit_session_file_entries
                SET status = 'rolled_back',
                    rolled_back_at = ?,
                    error = '',
                    updated_at = ?
                WHERE entry_id = ?
                  AND status <> 'committed'
            """, (now, now, entry_id))
            conn.commit()
        finally:
            conn.close()

        rolled_back.append(entry_id)

    return {
        "ok": True,
        "sessionId": normalized_session_id,
        "operationId": normalized_operation_id,
        "rolledBackCount": len(rolled_back),
        "entryIds": rolled_back,
    }


def mark_edit_session_file_entries_rolled_back_in_transaction(
    conn,
    *,
    session_id: str,
    now: str,
) -> int:
    cur = conn.execute("""
        UPDATE edit_session_file_entries
        SET status = 'rolled_back', rolled_back_at = ?, error = '', updated_at = ?
        WHERE session_id = ? AND status <> 'committed'
    """, (now, now, clean_cell_value(session_id)))
    return int(cur.rowcount or 0)


def _remove_empty_parents(start: Path, stop: Path) -> None:
    current = start
    stop = normalized_absolute_path(stop)
    while is_path_within(current, stop):
        if current == stop:
            break
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def purge_committed_session_files(session_id: str) -> dict:
    ensure_edit_session_file_schema()
    normalized_session_id = clean_cell_value(session_id)
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM edit_session_file_entries
            WHERE session_id = ? AND status = 'committed'
              AND entry_kind = 'stashed'
              AND COALESCE(cleanup_at, '') = ''
            ORDER BY sequence_no ASC
        """, (normalized_session_id,)).fetchall()
    finally:
        conn.close()

    cleaned = []
    errors = []
    for row in rows:
        record = dict(row)
        entry_id = record.get("entry_id") or ""
        try:
            staged = require_session_file_path(
                record.get("staged_path") or ""
            )
            _remove_file(staged)
            _remove_empty_parents(
                staged.parent,
                normalized_absolute_path(EDIT_SESSION_FILE_ROOT),
            )
            now = utc_now_iso()
            conn = get_conn()
            try:
                conn.execute("""
                    UPDATE edit_session_file_entries
                    SET cleanup_at = ?, error = '', updated_at = ?
                    WHERE entry_id = ? AND status = 'committed'
                """, (now, now, entry_id))
                conn.commit()
            finally:
                conn.close()
            cleaned.append(entry_id)
        except Exception as exc:
            errors.append({"entryId": entry_id, "error": str(exc)})
            write_debug_log(
                "edit_session_file_cleanup_failed",
                {
                    "sessionId": normalized_session_id,
                    "entryId": entry_id,
                    "error": str(exc),
                },
            )
    return {
        "ok": not errors,
        "sessionId": normalized_session_id,
        "cleanedCount": len(cleaned),
        "errorCount": len(errors),
        "errors": errors,
    }


def recover_edit_session_file_entries(source: str = "startup") -> dict:
    ensure_edit_session_file_schema()
    conn = get_conn()
    try:
        staging_rows = conn.execute("""
            SELECT * FROM edit_session_file_entries
            WHERE status = 'staging' ORDER BY created_at ASC
        """).fetchall()
        committed_sessions = conn.execute("""
            SELECT DISTINCT session_id FROM edit_session_file_entries
            WHERE status = 'committed' AND entry_kind = 'stashed'
              AND COALESCE(cleanup_at, '') = ''
        """).fetchall()
    finally:
        conn.close()

    recovered = []
    errors = []
    for row in staging_rows:
        record = dict(row)
        entry_id = record.get("entry_id") or ""
        try:
            original = require_upload_path(record.get("original_path") or "")
            staged = require_session_file_path(record.get("staged_path") or "")
            if staged.is_file() and not original.exists():
                next_status = FILE_ENTRY_STATUS_ACTIVE
                recovery_note = "stash_move_completed"
            elif original.is_file() and not staged.exists():
                # Сбой произошёл до os.replace(). Файл остаётся
                # на исходном месте, поэтому rollback может безопасно
                # обработать запись как активную.
                next_status = FILE_ENTRY_STATUS_ACTIVE
                recovery_note = "stash_move_not_started"
            elif staged.is_file() and original.is_file():
                expected_hash = clean_cell_value(
                    record.get("sha256")
                )
                if (
                    expected_hash
                    and file_sha256(staged) == expected_hash
                    and file_sha256(original) == expected_hash
                ):
                    _remove_file(staged)
                    next_status = FILE_ENTRY_STATUS_ACTIVE
                    recovery_note = "duplicate_stash_removed"
                else:
                    raise EditSessionFileConflictError(
                        "ambiguous interrupted stash state: "
                        "both paths contain different files"
                    )
            else:
                raise EditSessionFileConflictError(
                    "ambiguous interrupted stash state: "
                    "both paths are missing"
                )
            conn = get_conn()
            try:
                conn.execute("""
                    UPDATE edit_session_file_entries
                    SET status = ?, error = ?, updated_at = ?
                    WHERE entry_id = ? AND status = 'staging'
                """, (
                    next_status,
                    "" if next_status == FILE_ENTRY_STATUS_ACTIVE
                    else recovery_note,
                    utc_now_iso(),
                    entry_id,
                ))
                conn.commit()
            finally:
                conn.close()
            recovered.append({
                "entryId": entry_id,
                "status": next_status,
                "recovery": recovery_note,
            })
        except Exception as exc:
            errors.append({"entryId": entry_id, "error": str(exc)})

    cleanup_results = [
        purge_committed_session_files(row["session_id"])
        for row in committed_sessions
    ]
    return {
        "ok": not errors,
        "source": clean_cell_value(source),
        "stagingFound": len(staging_rows),
        "recoveredCount": len(recovered),
        "errorCount": len(errors),
        "recovered": recovered,
        "errors": errors,
        "cleanupResults": cleanup_results,
    }
