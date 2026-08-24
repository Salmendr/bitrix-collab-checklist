from __future__ import annotations

import copy
import hashlib
import json
from typing import Any

from app.db import get_conn
from app.logging_utils import write_debug_log

from app.checklists.edit_session_locks import acquire_edit_session_lock
from app.checklists.edit_sessions import (
    EditSessionConflictError,
    EditSessionNotFoundError,
    create_edit_session_snapshot,
    get_edit_session_for_actor,
    json_loads,
    utc_now_iso,
)
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


SNAPSHOT_SCOPE = "checklist_state_v1"
APPLIED_OPERATION_STATUS = "applied"
COMMITTED_OPERATION_STATUS = "committed"
ROLLED_BACK_OPERATION_STATUS = "rolled_back"

CHANGE_SCHEMA_COLUMNS = {
    "last_state_hash": "TEXT",
    "last_state_json": "TEXT",
    "last_business_state_hash": "TEXT",
    "last_operation_id": "TEXT",
    "mutation_count": "INTEGER DEFAULT 0",
    "rollback_restored_at": "TEXT",
    "commit_finalized_at": "TEXT",
}


BACKGROUND_ITEM_FIELDS = frozenset({
    "yandexFolderStatus",
    "yandexFolderError",
    "yandexFolderPath",
    "yandexFolderUrl",
    "yandexFolderTargetPath",
    "yandexStructureJobId",
    "yandexStructureAction",
    "yandexStructureUpdatedAt",
})

BACKGROUND_DOCUMENT_FIELDS = frozenset({
    "mirrorStatus",
    "mirrorError",
    "mirrorJobId",
    "yandexPath",
    "yandexFileUrl",
    "yandexFolderAlias",
})

BACKGROUND_ARCHIVE_FIELDS = frozenset({
    "yandexDeleteStatus",
    "yandexDeleteJobId",
    "yandexDeleteError",
})


def ensure_edit_session_change_schema() -> None:
    conn = get_conn()

    try:
        columns = {
            str(row["name"])
            for row in conn.execute(
                "PRAGMA table_info(edit_session_checklists)"
            ).fetchall()
        }

        for column_name, declaration in CHANGE_SCHEMA_COLUMNS.items():
            if column_name in columns:
                continue

            conn.execute(
                "ALTER TABLE edit_session_checklists "
                f"ADD COLUMN {column_name} {declaration}"
            )

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_edit_session_operations_type
            ON edit_session_operations(
                session_id,
                operation_type,
                status,
                sequence_no
            )
        """)

        conn.commit()

    finally:
        conn.close()


def canonical_checklist_data(
    data: dict,
    checklist_key: str,
) -> dict:
    from app.checklists.normalization import normalize_checklist_data

    return normalize_checklist_data(
        copy.deepcopy(data or {}),
        normalize_checklist_key(checklist_key),
    )


def stable_json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def checklist_state_hash(
    data: dict,
    checklist_key: str,
) -> str:
    canonical = canonical_checklist_data(
        data,
        checklist_key,
    )
    return hashlib.sha256(
        stable_json_dumps(canonical).encode("utf-8")
    ).hexdigest()


def rollback_business_checklist_data(
    data: dict,
    checklist_key: str,
) -> dict:
    """Return checklist state without background Yandex bookkeeping.

    Mirror and structure workers are allowed to finish while an edit session is
    open. Their status/path fields are not user edits and therefore must not
    create a false concurrent-edit conflict when the user explicitly cancels.
    """
    canonical = canonical_checklist_data(data, checklist_key)

    for item in canonical.get("items", []) or []:
        for field in BACKGROUND_ITEM_FIELDS:
            item.pop(field, None)

        for document in item.get("documents", []) or []:
            for field in BACKGROUND_DOCUMENT_FIELDS:
                document.pop(field, None)
            for version in document.get("archiveVersions", []) or []:
                for field in BACKGROUND_ARCHIVE_FIELDS:
                    version.pop(field, None)

        for series in item.get("archivedDocumentSeries", []) or []:
            for version in series.get("archiveVersions", []) or []:
                for field in BACKGROUND_ARCHIVE_FIELDS:
                    version.pop(field, None)

    return canonical


def rollback_business_state_hash(
    data: dict,
    checklist_key: str,
) -> str:
    return hashlib.sha256(
        stable_json_dumps(
            rollback_business_checklist_data(data, checklist_key)
        ).encode("utf-8")
    ).hexdigest()


def _copy_background_fields(
    target: dict,
    current: dict,
    expected: dict | None,
    fields: frozenset[str],
) -> None:
    expected_record = expected if isinstance(expected, dict) else None
    for field in fields:
        # With a v2 expected-state snapshot, preserve only values changed by a
        # worker after the user's last operation. For a legacy live session the
        # exact expected JSON was never recorded, so retaining current Yandex
        # bookkeeping is the safest non-destructive fallback.
        if (
            expected_record is None
            or current.get(field) != expected_record.get(field)
        ):
            target[field] = copy.deepcopy(current.get(field, ""))


def _merge_archive_background_state(
    target_versions: list,
    current_versions: list,
    expected_versions: list | None,
) -> None:
    current_by_id = {
        clean_cell_value(version.get("id")): version
        for version in current_versions or []
        if isinstance(version, dict)
        and clean_cell_value(version.get("id"))
    }
    expected_by_id = {
        clean_cell_value(version.get("id")): version
        for version in expected_versions or []
        if isinstance(version, dict)
        and clean_cell_value(version.get("id"))
    }

    for target in target_versions or []:
        version_id = clean_cell_value(target.get("id"))
        current = current_by_id.get(version_id)
        if not current:
            continue
        _copy_background_fields(
            target,
            current,
            expected_by_id.get(version_id),
            BACKGROUND_ARCHIVE_FIELDS,
        )


def merge_background_yandex_state(
    snapshot_data: dict,
    current_data: dict,
    expected_data: dict | None,
    checklist_key: str,
) -> dict:
    """Restore user data while retaining worker changes made after it."""
    restored = canonical_checklist_data(snapshot_data, checklist_key)
    current = canonical_checklist_data(current_data, checklist_key)
    expected = (
        canonical_checklist_data(expected_data, checklist_key)
        if isinstance(expected_data, dict)
        else None
    )

    current_items = {
        clean_cell_value(item.get("id")): item
        for item in current.get("items", []) or []
        if clean_cell_value(item.get("id"))
    }
    expected_items = {
        clean_cell_value(item.get("id")): item
        for item in (expected or {}).get("items", []) or []
        if clean_cell_value(item.get("id"))
    }

    for target_item in restored.get("items", []) or []:
        item_id = clean_cell_value(target_item.get("id"))
        current_item = current_items.get(item_id)
        if not current_item:
            continue
        expected_item = expected_items.get(item_id)

        _copy_background_fields(
            target_item,
            current_item,
            expected_item,
            BACKGROUND_ITEM_FIELDS,
        )

        current_documents = {
            clean_cell_value(document.get("id")): document
            for document in current_item.get("documents", []) or []
            if clean_cell_value(document.get("id"))
        }
        expected_documents = {
            clean_cell_value(document.get("id")): document
            for document in (expected_item or {}).get("documents", []) or []
            if clean_cell_value(document.get("id"))
        }

        for target_document in target_item.get("documents", []) or []:
            document_id = clean_cell_value(target_document.get("id"))
            current_document = current_documents.get(document_id)
            if not current_document:
                continue
            expected_document = expected_documents.get(document_id)
            _copy_background_fields(
                target_document,
                current_document,
                expected_document,
                BACKGROUND_DOCUMENT_FIELDS,
            )
            _merge_archive_background_state(
                target_document.get("archiveVersions", []) or [],
                current_document.get("archiveVersions", []) or [],
                (expected_document or {}).get("archiveVersions", []) or [],
            )

        current_series = {
            clean_cell_value(series.get("seriesId")): series
            for series in current_item.get("archivedDocumentSeries", []) or []
            if clean_cell_value(series.get("seriesId"))
        }
        expected_series = {
            clean_cell_value(series.get("seriesId")): series
            for series in (expected_item or {}).get(
                "archivedDocumentSeries", []
            ) or []
            if clean_cell_value(series.get("seriesId"))
        }
        for target_series in target_item.get("archivedDocumentSeries", []) or []:
            series_id = clean_cell_value(target_series.get("seriesId"))
            current_record = current_series.get(series_id)
            if not current_record:
                continue
            _merge_archive_background_state(
                target_series.get("archiveVersions", []) or [],
                current_record.get("archiveVersions", []) or [],
                (expected_series.get(series_id) or {}).get(
                    "archiveVersions", []
                ) or [],
            )

    return canonical_checklist_data(restored, checklist_key)


def acquire_checklist_for_edit_session(
    *,
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    user_id: str = "",
    user_name: str = "",
) -> dict:
    normalized_session_id = clean_cell_value(session_id)
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(
        checklist_key
    )
    normalized_user_id = clean_cell_value(user_id)
    normalized_user_name = clean_cell_value(user_name)

    if not normalized_session_id:
        raise ValueError("sessionId is required")

    session = get_edit_session_for_actor(
        session_id=normalized_session_id,
        dialog_id=normalized_dialog_id,
        user_id=normalized_user_id,
    )

    if session.get("status") != "active":
        raise EditSessionConflictError(
            "edit session is not active"
        )

    lock_result = acquire_edit_session_lock(
        session_id=normalized_session_id,
        dialog_id=normalized_dialog_id,
        checklist_key=normalized_checklist_key,
        user_id=normalized_user_id,
        user_name=normalized_user_name,
    )

    if not lock_result.get("owned"):
        owner_name = clean_cell_value(
            lock_result.get("userName")
        ) or "Другой сотрудник"
        raise EditSessionConflictError(
            "checklist is locked by another edit session: "
            + owner_name
        )

    return {
        "session": session,
        "lock": lock_result,
        "sessionId": normalized_session_id,
        "dialogId": normalized_dialog_id,
        "checklistKey": normalized_checklist_key,
        "userId": normalized_user_id,
        "userName": normalized_user_name,
    }


def ensure_checklist_snapshot(
    *,
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    data: dict,
) -> dict:
    ensure_edit_session_change_schema()

    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(
        checklist_key
    )
    canonical = canonical_checklist_data(
        data,
        normalized_checklist_key,
    )

    snapshot = create_edit_session_snapshot(
        session_id=session_id,
        dialog_id=normalized_dialog_id,
        checklist_key=normalized_checklist_key,
        snapshot={
            "snapshotVersion": 1,
            "scope": SNAPSHOT_SCOPE,
            "dialogId": normalized_dialog_id,
            "checklistKey": normalized_checklist_key,
            "data": canonical,
        },
    )

    return snapshot


def _next_operation_sequence(
    conn,
    session_id: str,
) -> int:
    row = conn.execute("""
        SELECT COALESCE(MAX(sequence_no), 0) AS max_sequence
        FROM edit_session_operations
        WHERE session_id = ?
    """, (
        session_id,
    )).fetchone()

    return int(row["max_sequence"] or 0) + 1


def record_checklist_operation(
    *,
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    operation_type: str,
    before: Any,
    after: Any,
    final_checklist_data: dict,
    item_id: str = "",
    series_id: str = "",
    document_id: str = "",
    payload: dict | None = None,
    operation_id: str = "",
) -> dict:
    ensure_edit_session_change_schema()

    normalized_session_id = clean_cell_value(session_id)
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    normalized_checklist_key = normalize_checklist_key(
        checklist_key
    )
    normalized_operation_type = clean_cell_value(
        operation_type
    )
    normalized_item_id = clean_cell_value(item_id)
    normalized_series_id = clean_cell_value(series_id)
    normalized_document_id = clean_cell_value(document_id)
    effective_operation_id = (
        clean_cell_value(operation_id)
        or __import__("uuid").uuid4().hex
    )

    if not normalized_session_id:
        raise ValueError("sessionId is required")

    if not normalized_operation_type:
        raise ValueError("operationType is required")

    session = get_edit_session_for_actor(
        normalized_session_id,
        dialog_id=normalized_dialog_id,
    )

    if session.get("status") != "active":
        raise EditSessionConflictError(
            "operation can be recorded only for active session"
        )
    if (
        clean_cell_value(session.get("rollback_started_at"))
        and not clean_cell_value(session.get("rolled_back_at"))
    ):
        raise EditSessionConflictError(
            "edit session has an interrupted cancel; repeat the confirmed "
            "Cancel action before editing"
        )

    canonical_final = canonical_checklist_data(
        final_checklist_data,
        normalized_checklist_key,
    )
    final_hash = checklist_state_hash(
        canonical_final,
        normalized_checklist_key,
    )
    final_state_json = stable_json_dumps(canonical_final)
    final_business_hash = rollback_business_state_hash(
        canonical_final,
        normalized_checklist_key,
    )
    now = utc_now_iso()
    conn = get_conn()

    try:
        conn.execute("BEGIN IMMEDIATE")

        checklist_row = conn.execute("""
            SELECT *
            FROM edit_session_checklists
            WHERE session_id = ?
              AND dialog_id = ?
              AND checklist_key = ?
              AND status = 'locked'
              AND COALESCE(lock_id, '') <> ''
        """, (
            normalized_session_id,
            normalized_dialog_id,
            normalized_checklist_key,
        )).fetchone()

        if not checklist_row:
            raise EditSessionConflictError(
                "edit session does not own checklist lock"
            )

        sequence_no = _next_operation_sequence(
            conn,
            normalized_session_id,
        )

        conn.execute("""
            INSERT INTO edit_session_operations(
                operation_id,
                session_id,
                sequence_no,
                operation_type,
                dialog_id,
                checklist_key,
                item_id,
                series_id,
                document_id,
                status,
                before_json,
                after_json,
                payload_json,
                error,
                created_at,
                updated_at,
                committed_at,
                rolled_back_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, '', ''
            )
        """, (
            effective_operation_id,
            normalized_session_id,
            sequence_no,
            normalized_operation_type,
            normalized_dialog_id,
            normalized_checklist_key,
            normalized_item_id,
            normalized_series_id,
            normalized_document_id,
            APPLIED_OPERATION_STATUS,
            stable_json_dumps(before),
            stable_json_dumps(after),
            stable_json_dumps(payload or {}),
            now,
            now,
        ))

        conn.execute("""
            UPDATE edit_session_checklists
            SET last_state_hash = ?,
                last_state_json = ?,
                last_business_state_hash = ?,
                last_operation_id = ?,
                mutation_count = COALESCE(mutation_count, 0) + 1,
                updated_at = ?
            WHERE session_id = ?
              AND dialog_id = ?
              AND checklist_key = ?
        """, (
            final_hash,
            final_state_json,
            final_business_hash,
            effective_operation_id,
            now,
            normalized_session_id,
            normalized_dialog_id,
            normalized_checklist_key,
        ))

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    return {
        "operationId": effective_operation_id,
        "sessionId": normalized_session_id,
        "sequenceNo": sequence_no,
        "operationType": normalized_operation_type,
        "dialogId": normalized_dialog_id,
        "checklistKey": normalized_checklist_key,
        "itemId": normalized_item_id,
        "seriesId": normalized_series_id,
        "documentId": normalized_document_id,
        "status": APPLIED_OPERATION_STATUS,
        "stateHash": final_hash,
    }


def list_edit_session_operations(
    *,
    session_id: str,
    dialog_id: str = "",
    user_id: str = "",
) -> list[dict]:
    ensure_edit_session_change_schema()

    session = get_edit_session_for_actor(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )

    conn = get_conn()

    try:
        rows = conn.execute("""
            SELECT *
            FROM edit_session_operations
            WHERE session_id = ?
            ORDER BY sequence_no ASC
        """, (
            session["session_id"],
        )).fetchall()

    finally:
        conn.close()

    result = []

    for row in rows:
        record = dict(row)
        result.append({
            "operationId": record.get("operation_id") or "",
            "sessionId": record.get("session_id") or "",
            "sequenceNo": int(record.get("sequence_no") or 0),
            "operationType": record.get("operation_type") or "",
            "dialogId": record.get("dialog_id") or "",
            "checklistKey": record.get("checklist_key") or "",
            "itemId": record.get("item_id") or "",
            "seriesId": record.get("series_id") or "",
            "documentId": record.get("document_id") or "",
            "status": record.get("status") or "",
            "before": json_loads(record.get("before_json") or "", None),
            "after": json_loads(record.get("after_json") or "", None),
            "payload": json_loads(record.get("payload_json") or "", {}),
            "error": record.get("error") or "",
            "createdAt": record.get("created_at") or "",
            "updatedAt": record.get("updated_at") or "",
            "committedAt": record.get("committed_at") or "",
            "rolledBackAt": record.get("rolled_back_at") or "",
        })

    return result


def _load_stage3_snapshots(session_id: str) -> list[dict]:
    ensure_edit_session_change_schema()
    conn = get_conn()

    try:
        rows = conn.execute("""
            SELECT
                s.snapshot_id,
                s.session_id,
                s.dialog_id,
                s.checklist_key,
                s.snapshot_json,
                s.snapshot_hash,
                c.last_state_hash,
                c.last_state_json,
                c.last_business_state_hash,
                c.last_operation_id,
                c.mutation_count,
                c.rollback_restored_at
            FROM edit_session_snapshots AS s
            JOIN edit_session_checklists AS c
              ON c.session_id = s.session_id
             AND c.dialog_id = s.dialog_id
             AND c.checklist_key = s.checklist_key
            WHERE s.session_id = ?
            ORDER BY s.created_at ASC,
                     s.checklist_key ASC
        """, (
            clean_cell_value(session_id),
        )).fetchall()

    finally:
        conn.close()

    result = []

    for row in rows:
        record = dict(row)
        snapshot = json_loads(
            record.get("snapshot_json") or "",
            {},
        )

        if not isinstance(snapshot, dict):
            continue

        if snapshot.get("scope") != SNAPSHOT_SCOPE:
            continue

        if not isinstance(snapshot.get("data"), dict):
            continue

        record["snapshot"] = snapshot
        result.append(record)

    return result


def refresh_edit_session_expected_state(
    session_id: str,
    *,
    only_missing: bool = True,
) -> dict:
    """Backfill the exact expected state for live pre-hotfix sessions.

    Stage 8.15.6 stored only a full hash. A background Yandex worker could
    legitimately change mirror fields afterwards, leaving no JSON with which
    to prove that the business data itself was untouched. On the first resume
    after this hotfix we capture that missing baseline. Rows already written by
    the new operation recorder are never replaced.
    """
    ensure_edit_session_change_schema()
    normalized_session_id = clean_cell_value(session_id)
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT session_id, dialog_id, checklist_key,
                   last_state_json, last_business_state_hash
            FROM edit_session_checklists
            WHERE session_id = ?
              AND status IN ('active', 'locked')
            ORDER BY checklist_key ASC
        """, (normalized_session_id,)).fetchall()
    finally:
        conn.close()

    from app.checklists.storage import get_checklist

    refreshed: list[dict] = []
    for source_row in rows:
        row = dict(source_row)
        existing_json = clean_cell_value(row.get("last_state_json"))
        existing_business_hash = clean_cell_value(
            row.get("last_business_state_hash")
        )
        if only_missing and existing_json and existing_business_hash:
            continue

        dialog_id = normalize_dialog_id(row.get("dialog_id"))
        checklist_key = normalize_checklist_key(row.get("checklist_key"))
        expected_data = json_loads(existing_json, None)
        if not isinstance(expected_data, dict):
            expected_data = get_checklist(dialog_id, checklist_key)
        canonical = canonical_checklist_data(expected_data, checklist_key)
        state_json = stable_json_dumps(canonical)
        state_hash = checklist_state_hash(canonical, checklist_key)
        business_hash = rollback_business_state_hash(
            canonical,
            checklist_key,
        )
        now = utc_now_iso()
        conn = get_conn()
        try:
            if only_missing:
                conn.execute("""
                    UPDATE edit_session_checklists
                    SET last_state_hash = ?,
                        last_state_json = ?,
                        last_business_state_hash = ?,
                        updated_at = ?
                    WHERE session_id = ?
                      AND dialog_id = ?
                      AND checklist_key = ?
                      AND (
                          COALESCE(last_state_json, '') = ''
                          OR COALESCE(last_business_state_hash, '') = ''
                      )
                """, (
                    state_hash,
                    state_json,
                    business_hash,
                    now,
                    normalized_session_id,
                    dialog_id,
                    checklist_key,
                ))
            else:
                conn.execute("""
                    UPDATE edit_session_checklists
                    SET last_state_hash = ?,
                        last_state_json = ?,
                        last_business_state_hash = ?,
                        updated_at = ?
                    WHERE session_id = ?
                      AND dialog_id = ?
                      AND checklist_key = ?
                """, (
                    state_hash,
                    state_json,
                    business_hash,
                    now,
                    normalized_session_id,
                    dialog_id,
                    checklist_key,
                ))
            conn.commit()
        finally:
            conn.close()

        refreshed.append({
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
        })

    return {
        "ok": True,
        "sessionId": normalized_session_id,
        "refreshedCount": len(refreshed),
        "refreshed": refreshed,
    }


def prepare_edit_session_checklist_restore(
    session_id: str,
) -> list[dict]:
    """Validate every checklist before rollback mutates any durable state."""
    snapshots = _load_stage3_snapshots(session_id)
    if not snapshots:
        return []

    from app.checklists.storage import get_checklist

    prepared: list[dict] = []
    for row in snapshots:
        dialog_id = normalize_dialog_id(row.get("dialog_id"))
        checklist_key = normalize_checklist_key(row.get("checklist_key"))
        snapshot_data = canonical_checklist_data(
            row["snapshot"]["data"],
            checklist_key,
        )
        current_data = canonical_checklist_data(
            get_checklist(dialog_id, checklist_key),
            checklist_key,
        )
        snapshot_business_hash = rollback_business_state_hash(
            snapshot_data,
            checklist_key,
        )
        current_state_hash = checklist_state_hash(
            current_data,
            checklist_key,
        )
        current_business_hash = rollback_business_state_hash(
            current_data,
            checklist_key,
        )
        expected_state_hash = clean_cell_value(row.get("last_state_hash"))
        expected_business_hash = clean_cell_value(
            row.get("last_business_state_hash")
        )
        expected_data = json_loads(
            row.get("last_state_json") or "",
            None,
        )
        if not isinstance(expected_data, dict):
            expected_data = None

        if (
            row.get("rollback_restored_at")
            and current_business_hash == snapshot_business_hash
        ):
            prepared.append({
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "alreadyRestored": True,
                "restoredData": current_data,
            })
            continue

        if expected_business_hash:
            has_conflict = current_business_hash != expected_business_hash
        elif expected_state_hash:
            has_conflict = current_state_hash != expected_state_hash
        else:
            has_conflict = bool(int(row.get("mutation_count") or 0))

        if has_conflict:
            raise EditSessionConflictError(
                "checklist business data changed outside edit session; "
                "automatic rollback was stopped: "
                f"{dialog_id}/{checklist_key}"
            )

        restored_data = merge_background_yandex_state(
            snapshot_data,
            current_data,
            expected_data,
            checklist_key,
        )
        prepared.append({
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "alreadyRestored": False,
            "restoredData": restored_data,
        })

    return prepared


def restore_edit_session_checklists(
    session_id: str,
    prepared: list[dict] | None = None,
) -> dict:
    plans = (
        prepared
        if prepared is not None
        else prepare_edit_session_checklist_restore(session_id)
    )
    restored = []

    if not plans:
        return {
            "ok": True,
            "sessionId": clean_cell_value(session_id),
            "restoredCount": 0,
            "restored": [],
        }

    from app.checklists.storage import save_checklist

    for plan in plans:
        dialog_id = normalize_dialog_id(plan.get("dialogId"))
        checklist_key = normalize_checklist_key(plan.get("checklistKey"))

        if plan.get("alreadyRestored"):
            restored.append({
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "alreadyRestored": True,
            })
            continue

        saved = save_checklist(
            dialog_id,
            plan.get("restoredData") or {},
            checklist_key,
        )
        canonical_saved = canonical_checklist_data(saved, checklist_key)
        restored_hash = checklist_state_hash(
            canonical_saved,
            checklist_key,
        )
        restored_json = stable_json_dumps(canonical_saved)
        restored_business_hash = rollback_business_state_hash(
            canonical_saved,
            checklist_key,
        )
        now = utc_now_iso()
        conn = get_conn()

        try:
            conn.execute("""
                UPDATE edit_session_checklists
                SET last_state_hash = ?,
                    last_state_json = ?,
                    last_business_state_hash = ?,
                    rollback_restored_at = ?,
                    updated_at = ?
                WHERE session_id = ?
                  AND dialog_id = ?
                  AND checklist_key = ?
            """, (
                restored_hash,
                restored_json,
                restored_business_hash,
                now,
                now,
                clean_cell_value(session_id),
                dialog_id,
                checklist_key,
            ))
            conn.commit()

        finally:
            conn.close()

        restored.append({
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "alreadyRestored": False,
        })

    return {
        "ok": True,
        "sessionId": clean_cell_value(session_id),
        "restoredCount": len(restored),
        "restored": restored,
    }


def _update_operation_payload(
    operation_id: str,
    payload: dict,
    error: str = "",
) -> None:
    now = utc_now_iso()
    conn = get_conn()

    try:
        conn.execute("""
            UPDATE edit_session_operations
            SET payload_json = ?,
                error = ?,
                updated_at = ?
            WHERE operation_id = ?
        """, (
            stable_json_dumps(payload),
            clean_cell_value(error),
            now,
            clean_cell_value(operation_id),
        ))
        conn.commit()

    finally:
        conn.close()


def finalize_deferred_checklist_changes(
    session_id: str,
) -> dict:
    operations = list_edit_session_operations(
        session_id=session_id,
    )
    candidate_operations = [
        operation
        for operation in operations
        if operation.get("status") == APPLIED_OPERATION_STATUS
        and operation.get("operationType") == "checklist_item_add"
    ]
    finalized = []
    warnings = []

    if not candidate_operations:
        return {
            "ok": True,
            "sessionId": clean_cell_value(session_id),
            "finalizedCount": 0,
            "warningCount": 0,
            "warnings": [],
        }

    from app.checklists.storage import (
        get_checklist,
        get_item_yandex_folder,
        save_checklist,
    )
    from app.checklists.yandex_folders import (
        can_create_custom_item_yandex_folder,
        ensure_yandex_folder_for_custom_item,
    )

    for operation in candidate_operations:

        payload = dict(operation.get("payload") or {})

        if payload.get("yandexFolderFinalized"):
            continue

        dialog_id = normalize_dialog_id(
            operation.get("dialogId")
        )
        checklist_key = normalize_checklist_key(
            operation.get("checklistKey")
        )
        item_id = clean_cell_value(operation.get("itemId"))
        after = operation.get("after") or {}
        item = after.get("item") if isinstance(after, dict) else {}
        item = item if isinstance(item, dict) else {}
        item_name = clean_cell_value(item.get("name"))
        group_id = int(item.get("group") or 0)

        try:
            if can_create_custom_item_yandex_folder(
                dialog_id,
                checklist_key,
            ):
                ensure_yandex_folder_for_custom_item(
                    dialog_id=dialog_id,
                    checklist_key=checklist_key,
                    group_id=group_id,
                    item_name=item_name,
                    item_id=item_id,
                )

                folder_info = get_item_yandex_folder(
                    dialog_id,
                    checklist_key,
                    item_name,
                    group_id=group_id,
                )
                folder = (
                    (folder_info or {}).get("folder")
                    if isinstance(folder_info, dict)
                    else {}
                ) or {}

                data = get_checklist(
                    dialog_id,
                    checklist_key,
                )
                changed = False

                for current_item in data.get("items", []) or []:
                    if clean_cell_value(current_item.get("id")) != item_id:
                        continue

                    current_item["folderPath"] = clean_cell_value(
                        folder.get("path")
                    )
                    current_item["folderUrl"] = clean_cell_value(
                        folder.get("url")
                    )
                    changed = True
                    break

                if changed:
                    saved = save_checklist(
                        dialog_id,
                        data,
                        checklist_key,
                    )
                    canonical_saved = canonical_checklist_data(
                        saved,
                        checklist_key,
                    )
                    final_hash = checklist_state_hash(
                        canonical_saved,
                        checklist_key,
                    )
                    final_state_json = stable_json_dumps(canonical_saved)
                    final_business_hash = rollback_business_state_hash(
                        canonical_saved,
                        checklist_key,
                    )
                    conn = get_conn()

                    try:
                        conn.execute("""
                            UPDATE edit_session_checklists
                            SET last_state_hash = ?,
                                last_state_json = ?,
                                last_business_state_hash = ?,
                                commit_finalized_at = ?,
                                updated_at = ?
                            WHERE session_id = ?
                              AND dialog_id = ?
                              AND checklist_key = ?
                        """, (
                            final_hash,
                            final_state_json,
                            final_business_hash,
                            utc_now_iso(),
                            utc_now_iso(),
                            clean_cell_value(session_id),
                            dialog_id,
                            checklist_key,
                        ))
                        conn.commit()

                    finally:
                        conn.close()

            payload["yandexFolderFinalized"] = True
            _update_operation_payload(
                operation.get("operationId") or "",
                payload,
            )
            finalized.append(operation.get("operationId") or "")

        except Exception as exc:
            warning = {
                "operationId": operation.get("operationId") or "",
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "itemId": item_id,
                "error": str(exc),
            }
            warnings.append(warning)
            payload["yandexFolderFinalized"] = False
            payload["yandexFolderWarning"] = str(exc)
            _update_operation_payload(
                operation.get("operationId") or "",
                payload,
                error=str(exc),
            )
            write_debug_log(
                "edit_session_custom_folder_commit_failed",
                warning,
            )

    return {
        "ok": True,
        "sessionId": clean_cell_value(session_id),
        "finalizedCount": len(finalized),
        "warningCount": len(warnings),
        "warnings": warnings,
    }


def mark_edit_session_operations_committed_in_transaction(
    conn,
    *,
    session_id: str,
    now: str,
) -> int:
    cur = conn.execute("""
        UPDATE edit_session_operations
        SET status = 'committed',
            committed_at = ?,
            updated_at = ?
        WHERE session_id = ?
          AND status NOT IN ('committed', 'rolled_back')
    """, (
        now,
        now,
        clean_cell_value(session_id),
    ))

    return int(cur.rowcount or 0)


def mark_edit_session_operations_rolled_back_in_transaction(
    conn,
    *,
    session_id: str,
    now: str,
) -> int:
    cur = conn.execute("""
        UPDATE edit_session_operations
        SET status = 'rolled_back',
            rolled_back_at = ?,
            updated_at = ?
        WHERE session_id = ?
          AND status <> 'committed'
    """, (
        now,
        now,
        clean_cell_value(session_id),
    ))

    return int(cur.rowcount or 0)
