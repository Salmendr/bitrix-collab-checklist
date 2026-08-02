from __future__ import annotations

import json

from app.db import get_conn
from app.logging_utils import write_debug_log

from app.checklists.storage import make_storage_dialog_id
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)
from app.checklists.yandex_folders import (
    build_custom_item_yandex_folder_spec,
    build_item_yandex_relocation_spec,
)
from app.checklists.yandex_structure_jobs import (
    ensure_yandex_structure_jobs_table,
    insert_yandex_structure_job_in_transaction,
    list_session_yandex_structure_job_ids,
    retarget_pending_create_item_folder_job_in_transaction,
    stable_json_loads,
)


def _load_current_item_in_transaction(
    conn,
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
) -> dict:
    storage_id = make_storage_dialog_id(dialog_id, checklist_key)
    row = conn.execute(
        "SELECT data_json FROM checklists WHERE dialog_id = ?",
        (storage_id,),
    ).fetchone()
    if not row:
        return {}
    try:
        data = json.loads(row["data_json"] or "{}")
    except Exception:
        return {}
    for item in data.get("items") or []:
        if clean_cell_value((item or {}).get("id")) == clean_cell_value(item_id):
            return dict(item or {})
    return {}


def prepare_edit_session_structure_jobs_in_transaction(
    conn,
    *,
    session_id: str,
    now: str,
) -> dict:
    normalized_session_id = clean_cell_value(session_id)
    rows = conn.execute(
        """
        SELECT *
        FROM edit_session_operations
        WHERE session_id = ?
          AND status = 'applied'
          AND operation_type IN (
              'checklist_item_add',
              'checklist_item_rename',
              'checklist_item_reorder'
          )
        ORDER BY sequence_no ASC
        """,
        (normalized_session_id,),
    ).fetchall()

    operations = [dict(row) for row in rows]
    operations_by_item: dict[tuple[str, str, str], list[dict]] = {}
    key_sequence: list[tuple[str, str, str]] = []

    for operation in operations:
        key = (
            normalize_dialog_id(operation.get("dialog_id")),
            normalize_checklist_key(operation.get("checklist_key")),
            clean_cell_value(operation.get("item_id")),
        )
        if not key[0] or not key[2]:
            continue
        if key not in operations_by_item:
            operations_by_item[key] = []
            key_sequence.append(key)
        operations_by_item[key].append(operation)

    jobs = []
    skipped = []

    for dialog_id, checklist_key, item_id in key_sequence:
        item_operations = operations_by_item[
            (dialog_id, checklist_key, item_id)
        ]
        current_item = _load_current_item_in_transaction(
            conn,
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
        )
        add_operations = [
            operation
            for operation in item_operations
            if clean_cell_value(operation.get("operation_type"))
            == "checklist_item_add"
        ]
        rename_operations = [
            operation
            for operation in item_operations
            if clean_cell_value(operation.get("operation_type"))
            == "checklist_item_rename"
        ]
        reorder_operations = [
            operation
            for operation in item_operations
            if clean_cell_value(operation.get("operation_type"))
            == "checklist_item_reorder"
        ]

        if add_operations:
            add_operation = add_operations[0]
            operation_id = clean_cell_value(
                add_operation.get("operation_id")
            )
            payload = stable_json_loads(
                add_operation.get("payload_json") or "",
                {},
            )
            payload = payload if isinstance(payload, dict) else {}

            if not payload.get("deferredYandexFolder"):
                skipped.append({
                    "operationId": operation_id,
                    "reason": "Yandex folder is not deferred",
                })
                continue
            if not current_item:
                skipped.append({
                    "operationId": operation_id,
                    "reason": (
                        "added item is absent from final checklist state"
                    ),
                })
                continue

            item_name = clean_cell_value(current_item.get("name"))
            group_id = int(
                current_item.get("group")
                or payload.get("groupId")
                or 0
            )
            spec = build_custom_item_yandex_folder_spec(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                group_id=group_id,
                item_name=item_name,
                item_id=item_id,
            )
            initial_status = (
                "queued" if spec.get("enabled") else "disabled"
            )
            job = insert_yandex_structure_job_in_transaction(
                conn,
                idempotency_key=(
                    f"edit-session:{normalized_session_id}:"
                    f"item:{item_id}:create-item-folder"
                ),
                session_id=normalized_session_id,
                operation_id=operation_id,
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                action="create_item_folder",
                target_path=clean_cell_value(spec.get("targetPath")),
                folder_alias=clean_cell_value(
                    spec.get("folderAlias")
                ),
                item_name=item_name,
                group_id=group_id,
                initial_status=initial_status,
                error=(
                    ""
                    if initial_status == "queued"
                    else clean_cell_value(spec.get("reason"))
                ),
                result={
                    "finalGroupId": group_id,
                    "finalName": item_name,
                },
                now=now,
            )
            jobs.append(job)
            continue

        if not current_item:
            first_operation = item_operations[0]
            skipped.append({
                "operationId": clean_cell_value(
                    first_operation.get("operation_id")
                ),
                "reason": (
                    "changed item is absent from final checklist state"
                ),
            })
            continue

        if not rename_operations and not reorder_operations:
            continue

        first_operation = item_operations[0]
        first_payload = stable_json_loads(
            first_operation.get("payload_json") or "",
            {},
        )
        first_payload = (
            first_payload if isinstance(first_payload, dict) else {}
        )
        rename_payloads = []
        for operation in rename_operations:
            payload = stable_json_loads(
                operation.get("payload_json") or "",
                {},
            )
            rename_payloads.append(
                payload if isinstance(payload, dict) else {}
            )
        reorder_payloads = []
        for operation in reorder_operations:
            payload = stable_json_loads(
                operation.get("payload_json") or "",
                {},
            )
            reorder_payloads.append(
                payload if isinstance(payload, dict) else {}
            )

        old_name = clean_cell_value(
            next(
                (
                    payload.get("oldName")
                    for payload in rename_payloads
                    if clean_cell_value(payload.get("oldName"))
                ),
                current_item.get("name"),
            )
        )
        final_name = clean_cell_value(current_item.get("name"))
        source_group_id = int(
            next(
                (
                    payload.get("sourceGroupId")
                    for payload in reorder_payloads
                    if int(payload.get("sourceGroupId") or 0)
                ),
                current_item.get("group") or 0,
            )
        )
        target_group_id = int(current_item.get("group") or 0)
        source_path = clean_cell_value(
            next(
                (
                    payload.get("sourcePath")
                    for payload in [
                        *rename_payloads,
                        *reorder_payloads,
                    ]
                    if clean_cell_value(payload.get("sourcePath"))
                ),
                current_item.get("yandexFolderPath"),
            )
        )

        relocation_spec = build_item_yandex_relocation_spec(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item=current_item,
            source_group_id=source_group_id,
            target_group_id=target_group_id,
            old_name=old_name,
            new_name=final_name,
            source_path_override=source_path,
        )

        retargeted = (
            retarget_pending_create_item_folder_job_in_transaction(
                conn,
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                item_name=final_name,
                target_path=clean_cell_value(
                    relocation_spec.get("targetPath")
                ),
                folder_alias=clean_cell_value(
                    relocation_spec.get("folderAlias")
                ),
                group_id=target_group_id,
                now=now,
            )
        )
        if retargeted:
            jobs.append(retargeted)
            continue

        moved = source_group_id != target_group_id
        renamed = old_name != final_name
        if not moved and not renamed:
            skipped.append({
                "operationId": clean_cell_value(
                    first_operation.get("operation_id")
                ),
                "reason": "final folder location is unchanged",
            })
            continue

        enabled = bool(
            relocation_spec.get("enabled")
            and clean_cell_value(
                relocation_spec.get("sourcePath")
            )
            and clean_cell_value(
                relocation_spec.get("targetPath")
            )
        )
        action = (
            "move_item_folder"
            if moved
            else "rename_item_folder"
        )
        operation_id = clean_cell_value(
            (
                reorder_operations[0]
                if moved and reorder_operations
                else rename_operations[0]
                if rename_operations
                else first_operation
            ).get("operation_id")
        )
        job = insert_yandex_structure_job_in_transaction(
            conn,
            idempotency_key=(
                f"edit-session:{normalized_session_id}:"
                f"item:{item_id}:relocate-item-folder"
            ),
            session_id=normalized_session_id,
            operation_id=operation_id,
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            action=action,
            source_path=clean_cell_value(
                relocation_spec.get("sourcePath")
            ),
            target_path=clean_cell_value(
                relocation_spec.get("targetPath")
            ),
            folder_alias=clean_cell_value(
                relocation_spec.get("folderAlias")
            ),
            item_name=final_name,
            group_id=target_group_id,
            initial_status="queued" if enabled else "disabled",
            error=(
                ""
                if enabled
                else clean_cell_value(
                    relocation_spec.get("reason")
                )
            ),
            result={
                "sourceGroupId": source_group_id,
                "targetGroupId": target_group_id,
                "oldName": old_name,
                "finalName": final_name,
            },
            now=now,
        )
        jobs.append(job)

    return {
        "ok": True,
        "sessionId": normalized_session_id,
        "jobCount": len(jobs),
        "queuedCount": sum(
            1
            for job in jobs
            if clean_cell_value(job.get("status")) == "queued"
        ),
        "disabledCount": sum(
            1
            for job in jobs
            if clean_cell_value(job.get("status")) == "disabled"
        ),
        "skippedCount": len(skipped),
        "jobIds": [
            clean_cell_value(job.get("job_id"))
            for job in jobs
        ],
        "jobs": jobs,
        "skipped": skipped,
    }


def enqueue_committed_edit_session_structure_jobs(
    session_id: str,
    *,
    source: str = "edit_session_commit",
) -> dict:
    normalized_session_id = clean_cell_value(session_id)
    if not normalized_session_id:
        return {
            "ok": False,
            "sessionId": "",
            "error": "sessionId is required",
        }

    ensure_yandex_structure_jobs_table()
    job_ids = list_session_yandex_structure_job_ids(
        normalized_session_id,
        status="queued",
    )

    from app.checklists.yandex_structure_queue import enqueue_yandex_structure_job

    results = []
    errors = []
    for job_id in job_ids:
        try:
            results.append(enqueue_yandex_structure_job(job_id, source=source))
        except Exception as exc:
            errors.append({"jobId": job_id, "error": str(exc)})

    payload = {
        "ok": not errors,
        "sessionId": normalized_session_id,
        "found": len(job_ids),
        "queued": sum(1 for result in results if result.get("queued")),
        "alreadyQueued": sum(
            1 for result in results
            if result.get("alreadyQueued") or result.get("alreadyRunning")
        ),
        "errorCount": len(errors),
        "errors": errors,
        "results": results,
    }
    write_debug_log("edit_session_structure_jobs_enqueued", payload)
    return payload
