import uuid
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
    get_item_yandex_folder,
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
    can_create_custom_item_yandex_folder,
    ensure_yandex_folder_for_custom_item,
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
    return JSONResponse(data)


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

    data = get_checklist(dialog_id, checklist_key)
    data["collabTitle"] = clean_cell_value(value)

    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "field": field,
        "value": data.get("collabTitle", ""),
        "progressPercent": data.get("progressPercent", 0),
    })

@router.post("/api/checklist/add-item")
async def api_checklist_add_item(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    config = get_checklist_config(checklist_key)

    group_id = int(payload.get("groupId") or 0)
    name = clean_cell_value(payload.get("name"))

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not config.has_custom_item_group(group_id):
        return JSONResponse({"ok": False, "error": "invalid groupId"}, status_code=400)

    if not name:
        return JSONResponse({"ok": False, "error": "name is required"}, status_code=400)

    data = get_checklist(dialog_id, config.key)
    items = data.get("items", []) or []

    group_items = [
        item
        for item in items
        if int(item.get("group") or 0) == group_id
    ]
    next_order = len(group_items) + 1

    new_item_id = f"{config.key}_g{group_id}_custom_{uuid.uuid4().hex[:8]}"
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
        "documents": [],
        "documentUrl": "",
        "documentName": "",
        "isCustom": True,
    }

    yandex_folder_warning = ""

    can_create_yandex_folder = False

    try:
        can_create_yandex_folder = can_create_custom_item_yandex_folder(dialog_id, config.key)
    except Exception as e:
        yandex_folder_warning = str(e)
        write_debug_log(f"custom_{config.key}_folder_check_failed", {
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "groupId": group_id,
            "itemId": new_item_id,
            "itemName": name,
            "error": str(e),
        })

    if can_create_yandex_folder:
        try:
            ensure_yandex_folder_for_custom_item(
                dialog_id=dialog_id,
                checklist_key=config.key,
                group_id=group_id,
                item_name=name,
                item_id=new_item_id,
            )

            folder_info = get_item_yandex_folder(dialog_id, config.key, name)
            if folder_info:
                folder = folder_info.get("folder") or {}
                new_item["folderPath"] = clean_cell_value(folder.get("path"))
                new_item["folderUrl"] = clean_cell_value(folder.get("url"))

        except Exception as e:
            yandex_folder_warning = str(e)
            write_debug_log(f"custom_{config.key}_folder_create_failed", {
                "dialogId": dialog_id,
                "checklistKey": config.key,
                "groupId": group_id,
                "itemId": new_item_id,
                "itemName": name,
                "error": str(e),
            })

    items.append(new_item)
    data["items"] = items
    data = normalize_checklist_data(data, config.key)
    save_checklist(dialog_id, data, config.key)

    created_item = None
    for item in data.get("items", []):
        if str(item.get("id")) == new_item["id"]:
            created_item = item
            break

    response_payload = {
        "ok": True,
        "dialogId": dialog_id,
        "checklistKey": config.key,
        "item": created_item or new_item,
        "progressPercent": data.get("progressPercent", 0),
    }

    if yandex_folder_warning:
        response_payload["yandexFolderWarning"] = yandex_folder_warning

    return JSONResponse(response_payload)

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

    data = get_checklist(dialog_id, config.key)
    items = data.get("items", [])

    target_item = None

    for index, item in enumerate(items):
        if str(item.get("id") or "") == item_id:
            target_item = migrate_legacy_document_fields(item)
            items[index] = target_item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

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
    save_checklist(dialog_id, data, config.key)

    updated_item = None
    for item in data.get("items", []):
        if str(item.get("id")) == item_id:
            updated_item = item
            break

    return JSONResponse({
        "ok": True,
        "item": updated_item,
        "dialogId": dialog_id,
        "checklistKey": config.key,
        "progressPercent": data.get("progressPercent", 0),
    })