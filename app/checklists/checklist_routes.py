import copy
import uuid
from datetime import datetime
from app.checklists.config import get_checklist_config
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.logging_utils import write_debug_log

from app.checklists.utils import (
    clean_cell_value,
    normalize_date_string,
    normalize_dialog_id,
    normalize_checklist_key,
    normalize_priority,
    normalize_status,
)

from app.checklists.storage import (
    save_checklist,
    get_checklist,
    get_project_storage_context,
    save_project_storage_context,
)

from app.checklists.normalization import (
    normalize_checklist_data,
    build_folder_key,
    derive_indicator_from_status,
    resolve_required_group_id_by_item_id_or_name,
)

from app.checklists.documents import (
    migrate_legacy_document_fields,
    normalize_documents_list,
    remove_all_item_documents,
)

from app.checklists.permissions import can_user_delete_files

from app.checklists.upload_jobs import (
    cancel_upload_jobs_for_document,
    create_yandex_delete_job,
)

from app.checklists.yandex_mirror_queue import enqueue_yandex_mirror_job

from app.checklists.yandex_folders import (
    build_custom_item_yandex_folder_spec,
    build_item_yandex_move_spec,
    build_item_yandex_rename_spec,
    ensure_folder_and_get_public_url,
)

from app.checklists.yandex_structure_jobs import (
    create_yandex_structure_job,
    retarget_pending_create_item_folder_job,
)
from app.checklists.yandex_structure_queue import (
    enqueue_yandex_structure_job,
)
from app.checklists.yandex_structure_state import (
    attach_latest_yandex_structure_states,
    apply_yandex_structure_job_to_item,
)

from app.checklists.yandex_context import resolve_checklist_yandex_root_path
from app.yandex_disk.client import is_yandex_disk_enabled

from app.checklists.checklist_mutation_guard import checklist_mutation_guard
from app.checklists.item_names import choose_available_item_name
from app.checklists.item_order import (
    get_checklist_order_version,
    renumber_items_by_group,
)

from app.checklists.edit_sessions import (
    EditSessionConflictError,
    EditSessionNotFoundError,
    EditSessionPermissionError,
)
from app.checklists.edit_session_changes import (
    acquire_checklist_for_edit_session,
    ensure_checklist_snapshot,
    record_checklist_operation,
)

from app.checklists.edit_session_documents import (
    stage_status_no_documents,
)
from app.checklists.edit_session_files import (
    rollback_edit_session_file_operation,
)
from app.checklists.document_assignment_history import (
    attach_assignment_history_counts,
)

router = APIRouter()

def resolve_required_group_for_item(checklist_key: str, item: dict) -> int:
    config = get_checklist_config(checklist_key)
    return resolve_required_group_id_by_item_id_or_name(config.key, item)

def enqueue_delete_jobs_for_documents(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    documents: list[dict],
    source: str = "status_no",
) -> list[str]:
    job_ids = []

    for doc in normalize_documents_list(documents):
        document_id = clean_cell_value(doc.get("id"))

        if document_id:
            cancel_upload_jobs_for_document(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                document_id=document_id,
            )

        yandex_path = clean_cell_value(doc.get("yandexPath"))
        if not yandex_path:
            continue

        delete_job = create_yandex_delete_job(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            document_id=document_id,
            file_name=clean_cell_value(doc.get("name")),
            yandex_path=yandex_path,
        )

        job_id = clean_cell_value(delete_job.get("job_id") or delete_job.get("jobId"))
        if job_id:
            enqueue_yandex_mirror_job(job_id, source=source)
            job_ids.append(job_id)

    return job_ids

@router.get("/api/checklist")
def api_get_checklist(dialogId: str = "", checklistKey: str = "id"):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    data = attach_latest_yandex_structure_states(
        data,
        dialog_id=dialog_id,
        checklist_key=checklist_key,
    )
    data = attach_assignment_history_counts(
        data,
        dialog_id=dialog_id,
        checklist_key=checklist_key,
    )
    return JSONResponse(data)



@router.get("/api/checklist/stage-yandex-folder")
def api_checklist_stage_yandex_folder(
    dialogId: str = "",
    checklistKey: str = "id",
    prepare: int = 1,
):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    config = get_checklist_config(checklist_key)

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    context = get_project_storage_context(dialog_id)
    if not context:
        # Локальный запуск без n8n-контекста является штатным
        # local-primary режимом. Не создаём бесконечные 404 в Console.
        return JSONResponse({
            "ok": True,
            "yandexDisabled": True,
            "reason": "project storage context not found",
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "alias": "",
            "path": "",
            "url": "",
            "preparedNow": False,
        })

    storage_mode = context.get("storageMode") or {}
    mirror_targets = storage_mode.get("mirrorTargets") or []

    if "yandex_disk" not in mirror_targets:
        return JSONResponse({
            "ok": True,
            "yandexDisabled": True,
            "reason": "yandex_disk is not in mirrorTargets",
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "alias": "",
            "path": "",
            "url": "",
        })

    if not is_yandex_disk_enabled():
        return JSONResponse({
            "ok": True,
            "yandexDisabled": True,
            "reason": "Yandex Disk OAuth token is not configured",
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "alias": "",
            "path": "",
            "url": "",
        })

    yandex_disk = context.get("yandexDisk") or {}
    folders = yandex_disk.get("folders") or {}

    stage_alias = (
        clean_cell_value(config.stage_yandex_folder_alias)
        or clean_cell_value(config.yandex_root_alias)
    )

    folder = folders.get(stage_alias) if stage_alias else {}
    folder = folder if isinstance(folder, dict) else {}

    folder_path = (
        clean_cell_value(folder.get("path"))
        or clean_cell_value(resolve_checklist_yandex_root_path(context, config))
    )

    folder_url = clean_cell_value(folder.get("url") or folder.get("public_url"))
    prepared_now = False

    if int(prepare or 0) and folder_path and not folder_url:
        try:
            folder_meta = ensure_folder_and_get_public_url(folder_path)

            folder_path = clean_cell_value(folder_meta.get("path")) or folder_path
            folder_url = clean_cell_value(folder_meta.get("url")) or folder_url
            folder_name = (
                clean_cell_value(folder_meta.get("name"))
                or clean_cell_value(folder.get("name"))
                or folder_path.rstrip("/").rsplit("/", 1)[-1]
            )

            if stage_alias:
                folders[stage_alias] = {
                    **folder,
                    "name": folder_name,
                    "path": folder_path,
                    "url": folder_url,
                    "checklistKey": config.key,
                    "isStageRoot": True,
                    "preparedAt": datetime.now().isoformat(),
                }

                yandex_disk["folders"] = folders

                save_project_storage_context(dialog_id, {
                    "dialogId": dialog_id,
                    "projectId": context.get("projectId") or "",
                    "projectName": context.get("projectName") or "",
                    "storageMode": context.get("storageMode") or {},
                    "yandexDisk": yandex_disk,
                    "itemMappings": context.get("itemMappings") or [],
                })

            prepared_now = True

        except Exception as exc:
            write_debug_log("stage_yandex_folder_prepare_failed", {
                "dialogId": dialog_id,
                "checklistKey": config.key,
                "alias": stage_alias,
                "path": folder_path,
                "error": str(exc),
            })

            return JSONResponse({
                "ok": False,
                "error": str(exc),
                "dialogId": dialog_id,
                "checklistKey": config.key,
                "alias": stage_alias,
                "path": folder_path,
                "url": folder_url,
            }, status_code=500)

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "checklistKey": config.key,
        "alias": stage_alias,
        "path": folder_path,
        "url": folder_url,
        "preparedNow": prepared_now,
    })


def checklist_edit_session_error_response(exc: Exception) -> JSONResponse:
    if isinstance(exc, EditSessionNotFoundError):
        status_code = 404
    elif isinstance(exc, EditSessionPermissionError):
        status_code = 403
    elif isinstance(exc, EditSessionConflictError):
        status_code = 409
    elif isinstance(exc, ValueError):
        status_code = 400
    else:
        status_code = 500

    return JSONResponse(
        {
            "ok": False,
            "error": str(exc),
            "editSessionError": True,
        },
        status_code=status_code,
    )


def payload_requires_edit_session(payload: dict) -> bool:
    value = payload.get("requireEditSession")

    if isinstance(value, bool):
        return value

    return str(value or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "да",
    }


def begin_optional_edit_session_change(
    payload: dict,
    *,
    dialog_id: str,
    checklist_key: str,
) -> dict | None:
    session_id = clean_cell_value(
        payload.get("sessionId")
    )

    if not session_id:
        if payload_requires_edit_session(payload):
            raise EditSessionConflictError(
                "Активная сессия редактирования не готова"
            )
        return None

    acting_user_id = clean_cell_value(
        payload.get("actingUserId")
        or payload.get("userId")
    )
    acting_user_name = clean_cell_value(
        payload.get("actingUserName")
        or payload.get("userName")
    ) or "Пользователь"

    return acquire_checklist_for_edit_session(
        session_id=session_id,
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        user_id=acting_user_id,
        user_name=acting_user_name,
    )


def record_or_restore_checklist_change(
    *,
    transaction: dict | None,
    dialog_id: str,
    checklist_key: str,
    operation_type: str,
    before: object,
    after: object,
    before_checklist: dict,
    final_checklist: dict,
    item_id: str = "",
    payload: dict | None = None,
    operation_id: str = "",
) -> dict | None:
    if not transaction:
        return None

    try:
        return record_checklist_operation(
            session_id=transaction["sessionId"],
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            operation_type=operation_type,
            before=before,
            after=after,
            final_checklist_data=final_checklist,
            item_id=item_id,
            payload=payload or {},
            operation_id=operation_id,
        )
    except Exception:
        save_checklist(
            dialog_id,
            before_checklist,
            checklist_key,
        )
        raise


@router.post("/api/checklist/update-meta")
async def api_checklist_update_meta(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    field = str(payload.get("field") or "").strip()
    value = payload.get("value")

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if field != "collabTitle":
        return JSONResponse({"ok": False, "error": "only collabTitle is supported now"}, status_code=400)

    try:
        transaction = begin_optional_edit_session_change(
            payload,
            dialog_id=dialog_id,
            checklist_key=checklist_key,
        )

        data = get_checklist(dialog_id, checklist_key)
        before_checklist = copy.deepcopy(data)

        if transaction:
            ensure_checklist_snapshot(
                session_id=transaction["sessionId"],
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                data=before_checklist,
            )

        before_value = clean_cell_value(
            data.get("collabTitle")
        )
        data["collabTitle"] = clean_cell_value(value)

        data = normalize_checklist_data(data, checklist_key)
        saved = save_checklist(dialog_id, data, checklist_key)

        operation = record_or_restore_checklist_change(
            transaction=transaction,
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            operation_type="checklist_meta_update",
            before={
                "field": field,
                "value": before_value,
            },
            after={
                "field": field,
                "value": saved.get("collabTitle", ""),
            },
            before_checklist=before_checklist,
            final_checklist=saved,
            payload={
                "field": field,
            },
        )

        return JSONResponse({
            "ok": True,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "field": field,
            "value": saved.get("collabTitle", ""),
            "progressPercent": saved.get("progressPercent", 0),
            "orderVersion": int(saved.get("orderVersion") or 0),
            "transactional": bool(transaction),
            "operation": operation,
        })

    except Exception as exc:
        write_debug_log("checklist_meta_update_failed", {
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "field": field,
            "sessionId": clean_cell_value(payload.get("sessionId")),
            "error": str(exc),
        })
        return checklist_edit_session_error_response(exc)


@router.post("/api/checklist/add-item")
async def api_checklist_add_item(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    config = get_checklist_config(checklist_key)
    group_id = int(payload.get("groupId") or 0)
    requested_name = clean_cell_value(payload.get("name"))

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)
    if not config.has_custom_item_group(group_id):
        return JSONResponse({"ok": False, "error": "invalid groupId"}, status_code=400)

    try:
        transaction = begin_optional_edit_session_change(
            payload,
            dialog_id=dialog_id,
            checklist_key=config.key,
        )

        async with checklist_mutation_guard(dialog_id, config.key):
            data = get_checklist(dialog_id, config.key)
            before_checklist = copy.deepcopy(data)
            if transaction:
                ensure_checklist_snapshot(
                    session_id=transaction["sessionId"],
                    dialog_id=dialog_id,
                    checklist_key=config.key,
                    data=before_checklist,
                )

            items = data.get("items", []) or []
            name_resolution = choose_available_item_name(
                requested_name,
                items,
                group_id=group_id,
            )
            name = name_resolution["name"]
            group_items = [
                item for item in items
                if int(item.get("group") or 0) == group_id
            ]
            next_order = len(group_items) + 1
            new_item_id = f"{config.key}_g{group_id}_custom_{uuid.uuid4().hex[:8]}"
            yandex_structure_spec = build_custom_item_yandex_folder_spec(
                dialog_id=dialog_id,
                checklist_key=config.key,
                group_id=group_id,
                item_name=name,
                item_id=new_item_id,
            )
            initial_yandex_status = (
                "queued" if yandex_structure_spec.get("enabled") else "disabled"
            )
            new_item = {
                "id": new_item_id,
                "group": group_id,
                "order": next_order,
                "name": name,
                "priority": "white",
                "status": "",
                "plan": "",
                "fact": "",
                "folderKey": build_folder_key(config.key, name, new_item_id),
                "folderPath": "",
                "folderUrl": "",
                "yandexFolderStatus": initial_yandex_status,
                "yandexFolderError": (
                    "" if initial_yandex_status == "queued"
                    else clean_cell_value(yandex_structure_spec.get("reason"))
                ),
                "yandexFolderPath": "",
                "yandexFolderUrl": "",
                "yandexFolderTargetPath": clean_cell_value(
                    yandex_structure_spec.get("targetPath")
                ),
                "yandexStructureJobId": "",
                "yandexStructureAction": "create_item_folder",
                "yandexStructureUpdatedAt": "",
                "documents": [],
                "documentUrl": "",
                "documentName": "",
                "isCustom": True,
                "definitionName": "",
                "definitionGroupId": 0,
                "nameOverride": "",
            }

            items.append(new_item)
            data["items"] = items
            saved = save_checklist(
                dialog_id,
                normalize_checklist_data(data, config.key),
                config.key,
            )
            created_item = next(
                (
                    item for item in saved.get("items", [])
                    if clean_cell_value(item.get("id")) == new_item_id
                ),
                new_item,
            )
            operation = record_or_restore_checklist_change(
                transaction=transaction,
                dialog_id=dialog_id,
                checklist_key=config.key,
                operation_type="checklist_item_add",
                before={"item": None},
                after={"item": created_item},
                before_checklist=before_checklist,
                final_checklist=saved,
                item_id=new_item_id,
                payload={
                    "groupId": group_id,
                    "name": name,
                    "requestedName": name_resolution["requestedName"],
                    "nameAdjusted": bool(name_resolution["adjusted"]),
                    "deferredYandexFolder": bool(transaction),
                },
            )

        yandex_folder_warning = ""
        yandex_structure_job = None
        if not transaction:
            try:
                initial_status = (
                    "queued" if yandex_structure_spec.get("enabled") else "disabled"
                )
                yandex_structure_job = create_yandex_structure_job(
                    idempotency_key=(
                        f"legacy:create-item-folder:{dialog_id}:"
                        f"{config.key}:{new_item_id}"
                    ),
                    dialog_id=dialog_id,
                    checklist_key=config.key,
                    item_id=new_item_id,
                    action="create_item_folder",
                    target_path=clean_cell_value(yandex_structure_spec.get("targetPath")),
                    folder_alias=clean_cell_value(yandex_structure_spec.get("folderAlias")),
                    item_name=name,
                    group_id=group_id,
                    initial_status=initial_status,
                    error=(
                        "" if initial_status == "queued"
                        else clean_cell_value(yandex_structure_spec.get("reason"))
                    ),
                )
                if clean_cell_value(yandex_structure_job.get("status")) == "queued":
                    enqueue_yandex_structure_job(
                        yandex_structure_job.get("job_id") or "",
                        source="legacy_add_item",
                    )
            except Exception as exc:
                yandex_folder_warning = str(exc)
                write_debug_log("custom_item_structure_job_create_failed", {
                    "dialogId": dialog_id,
                    "checklistKey": config.key,
                    "groupId": group_id,
                    "itemId": new_item_id,
                    "itemName": name,
                    "error": str(exc),
                })

        response_item = created_item
        if yandex_structure_job:
            response_item = apply_yandex_structure_job_to_item(
                response_item,
                yandex_structure_job,
            )
        response_payload = {
            "ok": True,
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "item": response_item,
            "requestedName": name_resolution["requestedName"],
            "finalName": name,
            "nameAdjusted": bool(name_resolution["adjusted"]),
            "progressPercent": saved.get("progressPercent", 0),
            "transactional": bool(transaction),
            "deferredYandexFolder": bool(transaction),
            "yandexStructureJob": yandex_structure_job or {},
            "operation": operation,
        }
        if yandex_folder_warning:
            response_payload["yandexFolderWarning"] = yandex_folder_warning
        return JSONResponse(response_payload)

    except Exception as exc:
        write_debug_log("checklist_item_add_failed", {
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "groupId": group_id,
            "name": requested_name,
            "sessionId": clean_cell_value(payload.get("sessionId")),
            "error": str(exc),
        })
        return checklist_edit_session_error_response(exc)


@router.post("/api/checklist/rename-item")
async def api_checklist_rename_item(request: Request):
    payload = await request.json()
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    config = get_checklist_config(checklist_key)
    item_id = clean_cell_value(payload.get("itemId"))
    requested_name = clean_cell_value(payload.get("name"))

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)
    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    try:
        transaction = begin_optional_edit_session_change(
            payload,
            dialog_id=dialog_id,
            checklist_key=config.key,
        )

        async with checklist_mutation_guard(dialog_id, config.key):
            data = get_checklist(dialog_id, config.key)
            before_checklist = copy.deepcopy(data)
            items = data.get("items", []) or []
            target_item = next(
                (
                    item for item in items
                    if clean_cell_value(item.get("id")) == item_id
                ),
                None,
            )
            if not target_item:
                return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

            before_item = copy.deepcopy(target_item)
            if transaction:
                ensure_checklist_snapshot(
                    session_id=transaction["sessionId"],
                    dialog_id=dialog_id,
                    checklist_key=config.key,
                    data=before_checklist,
                )

            group_id = int(target_item.get("group") or 0)
            name_resolution = choose_available_item_name(
                requested_name,
                items,
                group_id=group_id,
                exclude_item_id=item_id,
            )
            old_name = clean_cell_value(target_item.get("name"))
            new_name = name_resolution["name"]
            if old_name == new_name:
                return JSONResponse({
                    "ok": True,
                    "unchanged": True,
                    "item": target_item,
                    "requestedName": name_resolution["requestedName"],
                    "finalName": new_name,
                    "nameAdjusted": bool(name_resolution["adjusted"]),
                    "transactional": bool(transaction),
                })

            rename_spec = build_item_yandex_rename_spec(
                dialog_id=dialog_id,
                checklist_key=config.key,
                item=target_item,
                old_name=old_name,
                new_name=new_name,
            )
            target_item["name"] = new_name
            if not bool(target_item.get("isCustom", False)):
                definition_name = clean_cell_value(target_item.get("definitionName"))
                target_item["nameOverride"] = (
                    "" if definition_name and new_name.casefold() == definition_name.casefold()
                    else new_name
                )
            target_item["yandexFolderTargetPath"] = clean_cell_value(
                rename_spec.get("targetPath")
            )
            target_item["yandexStructureAction"] = (
                "create_item_folder"
                if not clean_cell_value(rename_spec.get("sourcePath"))
                else "rename_item_folder"
            )
            if rename_spec.get("enabled"):
                target_item["yandexFolderStatus"] = "queued"
                target_item["yandexFolderError"] = ""
            else:
                target_item["yandexFolderStatus"] = "disabled"
                target_item["yandexFolderError"] = clean_cell_value(
                    rename_spec.get("reason")
                )

            saved = save_checklist(
                dialog_id,
                normalize_checklist_data(data, config.key),
                config.key,
            )
            updated_item = next(
                (
                    item for item in saved.get("items", [])
                    if clean_cell_value(item.get("id")) == item_id
                ),
                target_item,
            )
            operation = record_or_restore_checklist_change(
                transaction=transaction,
                dialog_id=dialog_id,
                checklist_key=config.key,
                operation_type="checklist_item_rename",
                before={"item": before_item, "name": old_name},
                after={"item": updated_item, "name": new_name},
                before_checklist=before_checklist,
                final_checklist=saved,
                item_id=item_id,
                payload={
                    "oldName": old_name,
                    "newName": new_name,
                    "requestedName": name_resolution["requestedName"],
                    "nameAdjusted": bool(name_resolution["adjusted"]),
                    "sourcePath": clean_cell_value(rename_spec.get("sourcePath")),
                    "targetPath": clean_cell_value(rename_spec.get("targetPath")),
                    "folderAlias": clean_cell_value(rename_spec.get("folderAlias")),
                    "deferredYandexRename": bool(transaction),
                },
            )

        structure_job = None
        structure_warning = ""
        if not transaction:
            try:
                structure_job = retarget_pending_create_item_folder_job(
                    dialog_id=dialog_id,
                    checklist_key=config.key,
                    item_id=item_id,
                    item_name=new_name,
                    target_path=clean_cell_value(rename_spec.get("targetPath")),
                    folder_alias=clean_cell_value(rename_spec.get("folderAlias")),
                    group_id=group_id,
                )
                if not structure_job:
                    enabled = bool(
                        rename_spec.get("enabled")
                        and clean_cell_value(rename_spec.get("sourcePath"))
                        and clean_cell_value(rename_spec.get("targetPath"))
                    )
                    structure_job = create_yandex_structure_job(
                        idempotency_key=(
                            f"legacy:rename-item-folder:{dialog_id}:"
                            f"{config.key}:{item_id}:{uuid.uuid4().hex}"
                        ),
                        dialog_id=dialog_id,
                        checklist_key=config.key,
                        item_id=item_id,
                        action="rename_item_folder",
                        source_path=clean_cell_value(rename_spec.get("sourcePath")),
                        target_path=clean_cell_value(rename_spec.get("targetPath")),
                        folder_alias=clean_cell_value(rename_spec.get("folderAlias")),
                        item_name=new_name,
                        group_id=group_id,
                        initial_status="queued" if enabled else "disabled",
                        error="" if enabled else clean_cell_value(rename_spec.get("reason")),
                        result={"oldName": old_name},
                    )
                if clean_cell_value(structure_job.get("status")) == "queued":
                    enqueue_yandex_structure_job(
                        structure_job.get("job_id") or "",
                        source="legacy_rename_item",
                    )
            except Exception as exc:
                structure_warning = str(exc)
                write_debug_log("checklist_item_rename_structure_job_failed", {
                    "dialogId": dialog_id,
                    "checklistKey": config.key,
                    "itemId": item_id,
                    "oldName": old_name,
                    "newName": new_name,
                    "error": str(exc),
                })

        response_item = updated_item
        if structure_job:
            response_item = apply_yandex_structure_job_to_item(
                response_item,
                structure_job,
            )
        response = {
            "ok": True,
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "item": response_item,
            "oldName": old_name,
            "requestedName": name_resolution["requestedName"],
            "finalName": new_name,
            "orderVersion": int(saved.get("orderVersion") or 0),
            "nameAdjusted": bool(name_resolution["adjusted"]),
            "transactional": bool(transaction),
            "deferredYandexRename": bool(transaction),
            "yandexStructureJob": structure_job or {},
            "operation": operation,
        }
        if structure_warning:
            response["yandexFolderWarning"] = structure_warning
        return JSONResponse(response)

    except Exception as exc:
        write_debug_log("checklist_item_rename_failed", {
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "itemId": item_id,
            "name": requested_name,
            "sessionId": clean_cell_value(payload.get("sessionId")),
            "error": str(exc),
        })
        return checklist_edit_session_error_response(exc)


@router.post("/api/checklist/reorder-items")
async def api_checklist_reorder_items(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    config = get_checklist_config(checklist_key)
    item_id = clean_cell_value(payload.get("itemId"))

    try:
        target_group_id = int(payload.get("targetGroupId") or 0)
    except (TypeError, ValueError):
        target_group_id = 0

    try:
        target_position = int(payload.get("targetPosition") or 0)
    except (TypeError, ValueError):
        target_position = 0

    try:
        expected_order_version = int(payload.get("orderVersion") or 0)
    except (TypeError, ValueError):
        expected_order_version = 0

    restore_from_not_required = bool(
        payload.get("restoreFromNotRequired")
    )
    delete_documents_on_restore = bool(
        payload.get("deleteDocumentsOnRestore")
    )
    acting_user_id = clean_cell_value(
        payload.get("actingUserId")
        or payload.get("userId")
    )
    acting_user_name = clean_cell_value(
        payload.get("actingUserName")
        or payload.get("userName")
    ) or "Пользователь"

    transaction = None
    session_document_operation_id = ""
    session_document_details = {}

    if not dialog_id:
        return JSONResponse(
            {"ok": False, "error": "dialogId is required"},
            status_code=400,
        )
    if not item_id:
        return JSONResponse(
            {"ok": False, "error": "itemId is required"},
            status_code=400,
        )
    if target_group_id not in set(config.group_ids()):
        return JSONResponse(
            {"ok": False, "error": "invalid targetGroupId"},
            status_code=400,
        )

    try:
        transaction = begin_optional_edit_session_change(
            payload,
            dialog_id=dialog_id,
            checklist_key=config.key,
        )

        async with checklist_mutation_guard(dialog_id, config.key):
            data = get_checklist(dialog_id, config.key)
            before_checklist = copy.deepcopy(data)
            current_order_version = int(
                data.get("orderVersion")
                or get_checklist_order_version(dialog_id, config.key)
                or 0
            )

            if (
                expected_order_version
                and current_order_version
                and expected_order_version != current_order_version
            ):
                return JSONResponse(
                    {
                        "ok": False,
                        "error": (
                            "Порядок пунктов уже изменён в другом окне. "
                            "Обновите чек-лист и повторите перенос."
                        ),
                        "orderConflict": True,
                        "expectedOrderVersion": expected_order_version,
                        "currentOrderVersion": current_order_version,
                    },
                    status_code=409,
                )

            items = [
                dict(item or {})
                for item in (data.get("items") or [])
                if isinstance(item, dict)
            ]

            client_item_states = payload.get("itemsState")
            if isinstance(client_item_states, list):
                state_map = {
                    clean_cell_value(state.get("id")): state
                    for state in client_item_states
                    if isinstance(state, dict)
                    and clean_cell_value(state.get("id"))
                }
                allowed_state_fields = {
                    "group",
                    "order",
                    "name",
                    "nameOverride",
                    "priority",
                    "status",
                    "plan",
                    "fact",
                    "notRequiredReturnGroupId",
                    "notRequiredReturnPosition",
                    "notRequiredReturnStatus",
                    "notRequiredReturnPriority",
                    "notRequiredReturnPlan",
                    "notRequiredReturnFact",
                }
                for item in items:
                    state = state_map.get(
                        clean_cell_value(item.get("id"))
                    )
                    if not state:
                        continue
                    for field_name in allowed_state_fields:
                        if field_name in state:
                            item[field_name] = state.get(field_name)

            target_item = next(
                (
                    item
                    for item in items
                    if clean_cell_value(item.get("id")) == item_id
                ),
                None,
            )
            if not target_item:
                return JSONResponse(
                    {"ok": False, "error": "item not found"},
                    status_code=404,
                )

            if transaction:
                ensure_checklist_snapshot(
                    session_id=transaction["sessionId"],
                    dialog_id=dialog_id,
                    checklist_key=config.key,
                    data=before_checklist,
                )

            source_group_id = int(target_item.get("group") or 0)
            source_position = int(target_item.get("order") or 0)
            before_item = copy.deepcopy(target_item)

            source_items = sorted(
                [
                    item
                    for item in items
                    if int(item.get("group") or 0) == source_group_id
                    and clean_cell_value(item.get("id")) != item_id
                ],
                key=lambda item: (
                    int(item.get("order") or 100000),
                    clean_cell_value(item.get("id")),
                ),
            )
            target_items = (
                source_items
                if source_group_id == target_group_id
                else sorted(
                    [
                        item
                        for item in items
                        if int(item.get("group") or 0) == target_group_id
                        and clean_cell_value(item.get("id")) != item_id
                    ],
                    key=lambda item: (
                        int(item.get("order") or 100000),
                        clean_cell_value(item.get("id")),
                    ),
                )
            )

            insertion_position = max(
                1,
                min(
                    target_position or (len(target_items) + 1),
                    len(target_items) + 1,
                ),
            )

            yandex_move_spec = {}
            if source_group_id != target_group_id:
                yandex_move_spec = build_item_yandex_move_spec(
                    dialog_id=dialog_id,
                    checklist_key=config.key,
                    item=before_item,
                    source_group_id=source_group_id,
                    target_group_id=target_group_id,
                )

            target_item["group"] = target_group_id

            not_required_group_id = int(
                config.not_required_group_id
            )

            if (
                target_group_id == not_required_group_id
                and source_group_id != not_required_group_id
            ):
                target_item["notRequiredReturnGroupId"] = (
                    source_group_id
                )
                target_item["notRequiredReturnPosition"] = (
                    source_position
                )
                target_item["notRequiredReturnStatus"] = (
                    normalize_status(before_item.get("status"))
                )
                target_item["notRequiredReturnPriority"] = (
                    clean_cell_value(before_item.get("priority"))
                )
                target_item["notRequiredReturnPlan"] = (
                    normalize_date_string(before_item.get("plan"))
                )
                target_item["notRequiredReturnFact"] = (
                    normalize_date_string(before_item.get("fact"))
                )
                target_item["status"] = "Не требуется"
                target_item["priority"] = derive_indicator_from_status(
                    "Не требуется"
                )

            elif (
                source_group_id == not_required_group_id
                and target_group_id != not_required_group_id
            ):
                documents_before_restore = normalize_documents_list(
                    target_item.get("documents")
                )

                if (
                    delete_documents_on_restore
                    and documents_before_restore
                    and not can_user_delete_files(acting_user_id)
                ):
                    return JSONResponse(
                        {
                            "ok": False,
                            "error": (
                                "У вас недостаточно прав "
                                "на удаление файлов"
                            ),
                        },
                        status_code=403,
                    )

                if delete_documents_on_restore and documents_before_restore:
                    if transaction:
                        session_document_operation_id = uuid.uuid4().hex
                        session_document_details = stage_status_no_documents(
                            session_id=transaction["sessionId"],
                            operation_id=session_document_operation_id,
                            dialog_id=dialog_id,
                            checklist_key=config.key,
                            item_id=item_id,
                            item=target_item,
                            acting_user_id=acting_user_id,
                            acting_user_name=acting_user_name,
                        )
                        target_item.clear()
                        target_item.update(
                            session_document_details.get("item") or {}
                        )
                    else:
                        enqueue_delete_jobs_for_documents(
                            dialog_id=dialog_id,
                            checklist_key=config.key,
                            item_id=item_id,
                            documents=documents_before_restore,
                            source="not_required_restore_status_no",
                        )
                        cleared_item = remove_all_item_documents(
                            dialog_id,
                            config.key,
                            item_id,
                            target_item,
                        )
                        target_item.clear()
                        target_item.update(cleared_item)

                remaining_documents = normalize_documents_list(
                    target_item.get("documents")
                )
                restored_status = (
                    "Есть" if remaining_documents else "Нет"
                )
                target_item["status"] = restored_status
                target_item["priority"] = derive_indicator_from_status(
                    restored_status
                )
                target_item["plan"] = normalize_date_string(
                    target_item.get("notRequiredReturnPlan")
                    or target_item.get("plan")
                )
                target_item["fact"] = normalize_date_string(
                    target_item.get("notRequiredReturnFact")
                    or target_item.get("fact")
                )
                target_item["notRequiredReturnGroupId"] = 0
                target_item["notRequiredReturnPosition"] = 0
                target_item["notRequiredReturnStatus"] = ""
                target_item["notRequiredReturnPriority"] = ""
                target_item["notRequiredReturnPlan"] = ""
                target_item["notRequiredReturnFact"] = ""

            if source_group_id != target_group_id:
                target_item["yandexFolderTargetPath"] = clean_cell_value(
                    yandex_move_spec.get("targetPath")
                )
                target_item["yandexStructureAction"] = "move_item_folder"
                if yandex_move_spec.get("enabled"):
                    target_item["yandexFolderStatus"] = "queued"
                    target_item["yandexFolderError"] = ""
                else:
                    target_item["yandexFolderStatus"] = "disabled"
                    target_item["yandexFolderError"] = clean_cell_value(
                        yandex_move_spec.get("reason")
                    )

            target_items.insert(insertion_position - 1, target_item)

            for position, item in enumerate(source_items, start=1):
                item["order"] = position
            for position, item in enumerate(target_items, start=1):
                item["order"] = position

            reordered_items = []
            target_ids = {
                clean_cell_value(item.get("id"))
                for item in target_items
            }
            source_ids = {
                clean_cell_value(item.get("id"))
                for item in source_items
            }

            for item in items:
                current_id = clean_cell_value(item.get("id"))
                if current_id == item_id:
                    continue
                if (
                    int(item.get("group") or 0) == target_group_id
                    and current_id in target_ids
                ):
                    continue
                if (
                    source_group_id != target_group_id
                    and int(item.get("group") or 0) == source_group_id
                    and current_id in source_ids
                ):
                    continue
                reordered_items.append(item)

            if source_group_id != target_group_id:
                reordered_items.extend(source_items)
            reordered_items.extend(target_items)
            reordered_items = renumber_items_by_group(
                reordered_items,
                config.key,
            )

            unchanged = (
                source_group_id == target_group_id
                and source_position == insertion_position
            )
            if unchanged:
                return JSONResponse(
                    {
                        "ok": True,
                        "unchanged": True,
                        "dialogId": dialog_id,
                        "checklistKey": config.key,
                        "item": before_item,
                        "items": before_checklist.get("items") or [],
                        "orderVersion": current_order_version,
                        "transactional": bool(transaction),
                    }
                )

            data["items"] = reordered_items
            saved = save_checklist(
                dialog_id,
                normalize_checklist_data(data, config.key),
                config.key,
            )
            updated_item = next(
                (
                    item
                    for item in (saved.get("items") or [])
                    if clean_cell_value(item.get("id")) == item_id
                ),
                target_item,
            )
            saved_order_version = int(saved.get("orderVersion") or 0)

            operation = record_or_restore_checklist_change(
                transaction=transaction,
                dialog_id=dialog_id,
                checklist_key=config.key,
                operation_type="checklist_item_reorder",
                before={
                    "item": before_item,
                    "groupId": source_group_id,
                    "position": source_position,
                    "orderVersion": current_order_version,
                },
                after={
                    "item": updated_item,
                    "groupId": target_group_id,
                    "position": int(updated_item.get("order") or insertion_position),
                    "orderVersion": saved_order_version,
                },
                before_checklist=before_checklist,
                final_checklist=saved,
                item_id=item_id,
                payload={
                    "sourceGroupId": source_group_id,
                    "targetGroupId": target_group_id,
                    "sourcePosition": source_position,
                    "targetPosition": insertion_position,
                    "sourcePath": clean_cell_value(
                        yandex_move_spec.get("sourcePath")
                    ),
                    "targetPath": clean_cell_value(
                        yandex_move_spec.get("targetPath")
                    ),
                    "folderAlias": clean_cell_value(
                        yandex_move_spec.get("folderAlias")
                    ),
                    "deferredYandexMove": bool(
                        transaction and source_group_id != target_group_id
                    ),
                    "restoreFromNotRequired": bool(
                        restore_from_not_required
                    ),
                    "deleteDocumentsOnRestore": bool(
                        delete_documents_on_restore
                    ),
                    "transactionalDocuments": bool(
                        session_document_details
                    ),
                    "fileEntries": session_document_details.get(
                        "fileEntries",
                        [],
                    ),
                    "deferredYandexDeletes": (
                        session_document_details.get(
                            "deferredYandexDeletes",
                            [],
                        )
                    ),
                },
                operation_id=session_document_operation_id,
            )

        structure_job = None
        structure_warning = ""
        if not transaction and source_group_id != target_group_id:
            try:
                pending_create = retarget_pending_create_item_folder_job(
                    dialog_id=dialog_id,
                    checklist_key=config.key,
                    item_id=item_id,
                    item_name=clean_cell_value(updated_item.get("name")),
                    target_path=clean_cell_value(
                        yandex_move_spec.get("targetPath")
                    ),
                    folder_alias=clean_cell_value(
                        yandex_move_spec.get("folderAlias")
                    ),
                    group_id=target_group_id,
                )
                if pending_create:
                    structure_job = pending_create
                else:
                    enabled = bool(
                        yandex_move_spec.get("enabled")
                        and clean_cell_value(yandex_move_spec.get("sourcePath"))
                        and clean_cell_value(yandex_move_spec.get("targetPath"))
                    )
                    structure_job = create_yandex_structure_job(
                        idempotency_key=(
                            f"legacy:move-item-folder:{dialog_id}:"
                            f"{config.key}:{item_id}:{uuid.uuid4().hex}"
                        ),
                        dialog_id=dialog_id,
                        checklist_key=config.key,
                        item_id=item_id,
                        action="move_item_folder",
                        source_path=clean_cell_value(
                            yandex_move_spec.get("sourcePath")
                        ),
                        target_path=clean_cell_value(
                            yandex_move_spec.get("targetPath")
                        ),
                        folder_alias=clean_cell_value(
                            yandex_move_spec.get("folderAlias")
                        ),
                        item_name=clean_cell_value(updated_item.get("name")),
                        group_id=target_group_id,
                        initial_status="queued" if enabled else "disabled",
                        error=(
                            ""
                            if enabled
                            else clean_cell_value(
                                yandex_move_spec.get("reason")
                            )
                        ),
                        result={"sourceGroupId": source_group_id},
                    )
                if clean_cell_value(
                    (structure_job or {}).get("status")
                ) == "queued":
                    enqueue_yandex_structure_job(
                        clean_cell_value(
                            structure_job.get("job_id")
                            or structure_job.get("jobId")
                        ),
                        source="legacy_reorder_item",
                    )
            except Exception as exc:
                structure_warning = str(exc)
                write_debug_log(
                    "checklist_item_reorder_structure_job_failed",
                    {
                        "dialogId": dialog_id,
                        "checklistKey": config.key,
                        "itemId": item_id,
                        "sourceGroupId": source_group_id,
                        "targetGroupId": target_group_id,
                        "error": str(exc),
                    },
                )

        response_item = updated_item
        if structure_job:
            response_item = apply_yandex_structure_job_to_item(
                response_item,
                structure_job,
            )
        response = {
            "ok": True,
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "item": response_item,
            "items": saved.get("items") or [],
            "sourceGroupId": source_group_id,
            "targetGroupId": target_group_id,
            "targetPosition": int(updated_item.get("order") or insertion_position),
            "orderVersion": int(saved.get("orderVersion") or 0),
            "transactional": bool(transaction),
            "operation": operation,
            "yandexStructureJob": structure_job or {},
            "documentsDeleted": bool(
                delete_documents_on_restore
                and session_document_details
            ),
            "transactionalDocuments": bool(
                session_document_details
            ),
            "fileEntries": session_document_details.get(
                "fileEntries",
                [],
            ),
            "deferredYandexDeletes": (
                session_document_details.get(
                    "deferredYandexDeletes",
                    [],
                )
            ),
        }
        if structure_warning:
            response["yandexFolderWarning"] = structure_warning
        return JSONResponse(response)

    except Exception as exc:
        if transaction and session_document_operation_id:
            try:
                rollback_edit_session_file_operation(
                    session_id=transaction["sessionId"],
                    operation_id=session_document_operation_id,
                )
            except Exception as rollback_exc:
                write_debug_log(
                    "not_required_restore_file_operation_rollback_failed",
                    {
                        "sessionId": transaction.get("sessionId"),
                        "operationId": session_document_operation_id,
                        "dialogId": dialog_id,
                        "checklistKey": config.key,
                        "itemId": item_id,
                        "error": str(rollback_exc),
                    },
                )

        write_debug_log(
            "checklist_item_reorder_failed",
            {
                "dialogId": dialog_id,
                "checklistKey": config.key,
                "itemId": item_id,
                "targetGroupId": target_group_id,
                "targetPosition": target_position,
                "sessionId": clean_cell_value(payload.get("sessionId")),
                "error": str(exc),
            },
        )
        return checklist_edit_session_error_response(exc)


@router.post("/api/checklist/update-item")
async def api_checklist_update_item(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    config = get_checklist_config(checklist_key)

    item_id = str(payload.get("itemId") or "").strip()
    field = str(payload.get("field") or "").strip()
    value = payload.get("value")
    acting_user_id = clean_cell_value(payload.get("actingUserId"))
    acting_user_name = clean_cell_value(payload.get("actingUserName")) or "Пользователь"

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    allowed_fields = {"priority", "status", "plan", "fact"}

    if field not in allowed_fields:
        return JSONResponse({"ok": False, "error": "invalid field"}, status_code=400)

    transaction = None
    session_document_operation_id = ""
    session_document_details = {}

    try:
        transaction = begin_optional_edit_session_change(
            payload,
            dialog_id=dialog_id,
            checklist_key=config.key,
        )

        data = get_checklist(dialog_id, config.key)
        before_checklist = copy.deepcopy(data)
        items = data.get("items", [])

        target_item = None

        for index, item in enumerate(items):
            if str(item.get("id") or "") == item_id:
                target_item = migrate_legacy_document_fields(item)
                items[index] = target_item
                break

        if not target_item:
            return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

        before_item = copy.deepcopy(target_item)

        if transaction:
            ensure_checklist_snapshot(
                session_id=transaction["sessionId"],
                dialog_id=dialog_id,
                checklist_key=config.key,
                data=before_checklist,
            )

        if field == "priority":
            target_item["priority"] = normalize_priority(value)

        elif field == "status":
            new_status = normalize_status(value)
            documents_before_status_change = normalize_documents_list(target_item.get("documents"))

            if (
                new_status == "Нет"
                and documents_before_status_change
                and not can_user_delete_files(acting_user_id)
            ):
                write_debug_log("status_no_delete_forbidden", {
                    "dialogId": dialog_id,
                    "checklistKey": config.key,
                    "itemId": item_id,
                    "itemName": clean_cell_value(target_item.get("name")),
                    "actingUserId": acting_user_id,
                    "actingUserName": acting_user_name,
                    "documentsCount": len(documents_before_status_change),
                })

                return JSONResponse({
                    "ok": False,
                    "error": "У вас недостаточно прав на удаление файлов"
                }, status_code=403)

            target_item["status"] = new_status
            target_item["priority"] = derive_indicator_from_status(new_status)

            if new_status == "Нет":
                if transaction:
                    session_document_operation_id = uuid.uuid4().hex
                    session_document_details = stage_status_no_documents(
                        session_id=transaction["sessionId"],
                        operation_id=session_document_operation_id,
                        dialog_id=dialog_id,
                        checklist_key=config.key,
                        item_id=item_id,
                        item=target_item,
                        acting_user_id=acting_user_id,
                        acting_user_name=acting_user_name,
                    )
                    target_item.clear()
                    target_item.update(
                        session_document_details.get("item") or {}
                    )
                    target_item["status"] = new_status
                    target_item["priority"] = derive_indicator_from_status(new_status)
                else:
                    enqueue_delete_jobs_for_documents(
                        dialog_id=dialog_id,
                        checklist_key=config.key,
                        item_id=item_id,
                        documents=documents_before_status_change,
                        source="status_no",
                    )

                    cleared_item = remove_all_item_documents(dialog_id, config.key, item_id, target_item)
                    target_item.clear()
                    target_item.update(cleared_item)

            if new_status == "Не требуется":
                target_item["group"] = config.not_required_group_id
            elif int(target_item.get("group") or 0) == config.not_required_group_id:
                target_item["group"] = resolve_required_group_for_item(config.key, target_item)

        elif field == "plan":
            target_item["plan"] = normalize_date_string(value)

        elif field == "fact":
            target_item["fact"] = normalize_date_string(value)

        data["items"] = items
        data = normalize_checklist_data(data, config.key)
        saved = save_checklist(dialog_id, data, config.key)

        updated_item = None
        for item in saved.get("items", []):
            if str(item.get("id")) == item_id:
                updated_item = item
                break

        operation = record_or_restore_checklist_change(
            transaction=transaction,
            dialog_id=dialog_id,
            checklist_key=config.key,
            operation_type="checklist_item_update",
            before={
                "field": field,
                "item": before_item,
            },
            after={
                "field": field,
                "item": updated_item,
            },
            before_checklist=before_checklist,
            final_checklist=saved,
            item_id=item_id,
            payload={
                "field": field,
                "requestedValue": value,
                "transactionalDocuments": bool(session_document_details),
                "fileEntries": session_document_details.get("fileEntries", []),
                "deferredYandexDeletes": session_document_details.get(
                    "deferredYandexDeletes",
                    [],
                ),
            },
            operation_id=session_document_operation_id,
        )

        return JSONResponse({
            "ok": True,
            "item": updated_item,
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "progressPercent": saved.get("progressPercent", 0),
            "orderVersion": int(saved.get("orderVersion") or 0),
            "transactional": bool(transaction),
            "operation": operation,
            "transactionalDocuments": bool(session_document_details),
            "fileEntries": session_document_details.get("fileEntries", []),
            "deferredYandexDeletes": session_document_details.get(
                "deferredYandexDeletes",
                [],
            ),
        })

    except Exception as exc:
        if transaction and session_document_operation_id:
            try:
                rollback_edit_session_file_operation(
                    session_id=transaction["sessionId"],
                    operation_id=session_document_operation_id,
                )
            except Exception as rollback_exc:
                write_debug_log("status_no_file_operation_rollback_failed", {
                    "sessionId": transaction.get("sessionId"),
                    "operationId": session_document_operation_id,
                    "dialogId": dialog_id,
                    "checklistKey": config.key,
                    "itemId": item_id,
                    "error": str(rollback_exc),
                })

        write_debug_log("checklist_item_update_failed", {
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "itemId": item_id,
            "field": field,
            "sessionId": clean_cell_value(payload.get("sessionId")),
            "error": str(exc),
        })
        return checklist_edit_session_error_response(exc)
