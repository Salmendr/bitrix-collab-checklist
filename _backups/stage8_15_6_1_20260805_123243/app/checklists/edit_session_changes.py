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
    "last_operation_id": "TEXT",
    "mutation_count": "INTEGER DEFAULT 0",
    "rollback_restored_at": "TEXT",
    "commit_finalized_at": "TEXT",
}


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

    final_hash = checklist_state_hash(
        final_checklist_data,
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
                last_operation_id = ?,
                mutation_count = COALESCE(mutation_count, 0) + 1,
                updated_at = ?
            WHERE session_id = ?
              AND dialog_id = ?
              AND checklist_key = ?
        """, (
            final_hash,
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


def restore_edit_session_checklists(
    session_id: str,
) -> dict:
    snapshots = _load_stage3_snapshots(session_id)
    restored = []

    if not snapshots:
        return {
            "ok": True,
            "sessionId": clean_cell_value(session_id),
            "restoredCount": 0,
            "restored": [],
        }

    from app.checklists.storage import get_checklist, save_checklist

    for row in snapshots:
        dialog_id = normalize_dialog_id(row.get("dialog_id"))
        checklist_key = normalize_checklist_key(
            row.get("checklist_key")
        )
        snapshot_data = canonical_checklist_data(
            row["snapshot"]["data"],
            checklist_key,
        )
        snapshot_state_hash = checklist_state_hash(
            snapshot_data,
            checklist_key,
        )
        current_data = get_checklist(
            dialog_id,
            checklist_key,
        )
        current_state_hash = checklist_state_hash(
            current_data,
            checklist_key,
        )
        expected_state_hash = clean_cell_value(
            row.get("last_state_hash")
        )

        if (
            row.get("rollback_restored_at")
            and current_state_hash == snapshot_state_hash
        ):
            restored.append({
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "alreadyRestored": True,
            })
            continue

        if (
            expected_state_hash
            and current_state_hash != expected_state_hash
        ):
            raise EditSessionConflictError(
                "checklist changed outside edit session; "
                "automatic rollback was stopped: "
                f"{dialog_id}/{checklist_key}"
            )

        saved = save_checklist(
            dialog_id,
            snapshot_data,
            checklist_key,
        )
        restored_hash = checklist_state_hash(
            saved,
            checklist_key,
        )
        now = utc_now_iso()
        conn = get_conn()

        try:
            conn.execute("""
                UPDATE edit_session_checklists
                SET last_state_hash = ?,
                    rollback_restored_at = ?,
                    updated_at = ?
                WHERE session_id = ?
                  AND dialog_id = ?
                  AND checklist_key = ?
            """, (
                restored_hash,
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
                    final_hash = checklist_state_hash(
                        saved,
                        checklist_key,
                    )
                    conn = get_conn()

                    try:
                        conn.execute("""
                            UPDATE edit_session_checklists
                            SET last_state_hash = ?,
                                commit_finalized_at = ?,
                                updated_at = ?
                            WHERE session_id = ?
                              AND dialog_id = ?
                              AND checklist_key = ?
                        """, (
                            final_hash,
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
