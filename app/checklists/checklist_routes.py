import uuid

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
    move_item_to_required_group,
    resolve_concept_group_id_by_item_id_or_name,
    resolve_opr_group_id_by_item_id_or_name,
)

from app.checklists.documents import (
    migrate_legacy_document_fields,
    remove_all_item_documents,
)

from app.checklists.definitions.concept import CONCEPT_GROUPS
from app.checklists.definitions.opr import OPR_GROUPS

from app.checklists.yandex_folders import (
    can_create_custom_item_yandex_folder,
    ensure_yandex_folder_for_custom_item,
    ensure_yandex_folder_for_custom_opr_item,
)

router = APIRouter()


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
    group_id = int(payload.get("groupId") or 0)
    name = clean_cell_value(payload.get("name"))

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if checklist_key == "id" and group_id not in [1, 2, 3]:
        return JSONResponse({"ok": False, "error": "invalid groupId"}, status_code=400)

    if checklist_key == "concept" and group_id not in [group["id"] for group in CONCEPT_GROUPS if group["id"] != 10]:
        return JSONResponse({"ok": False, "error": "invalid groupId"}, status_code=400)

    if checklist_key == "opr" and group_id not in [group["id"] for group in OPR_GROUPS if group["id"] != 2]:
        return JSONResponse({"ok": False, "error": "invalid groupId"}, status_code=400)

    if not name:
        return JSONResponse({"ok": False, "error": "name is required"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    group_items = [x for x in items if int(x.get("group") or 0) == group_id]
    next_order = len(group_items) + 1

    yandex_folder_warning = ""

    if checklist_key == "concept":
        new_item_id = f"concept_g{group_id}_custom_{uuid.uuid4().hex[:8]}"
        new_item = {
            "id": new_item_id,
            "group": group_id,
            "order": next_order,
            "name": name,
            "priority": "white",
            "status": "",
            "plan": "",
            "fact": "",
            "folderKey": build_folder_key("concept", name, new_item_id),
            "folderPath": "",
            "folderUrl": "",
            "documents": [],
            "documentUrl": "",
            "documentName": "",
            "isCustom": True,
        }

    elif checklist_key == "opr":
        new_item_id = f"opr_g{group_id}_custom_{uuid.uuid4().hex[:8]}"
        new_item = {
            "id": new_item_id,
            "group": group_id,
            "order": next_order,
            "name": name,
            "priority": "white",
            "status": "",
            "plan": "",
            "fact": "",
            "folderKey": build_folder_key("opr", name, new_item_id),
            "folderPath": "",
            "folderUrl": "",
            "documents": [],
            "documentUrl": "",
            "documentName": "",
            "isCustom": True,
        }

        can_create_yandex_folder = False

        try:
            can_create_yandex_folder = can_create_custom_item_yandex_folder(dialog_id, checklist_key)
        except Exception as e:
            yandex_folder_warning = str(e)
            write_debug_log("custom_opr_folder_check_failed", {
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "groupId": group_id,
                "itemId": new_item_id,
                "itemName": name,
                "error": str(e),
            })

        if can_create_yandex_folder:
            try:
                ensure_yandex_folder_for_custom_opr_item(
                    dialog_id=dialog_id,
                    checklist_key=checklist_key,
                    group_id=group_id,
                    item_name=name,
                    item_id=new_item_id,
                )

                folder_info = get_item_yandex_folder(dialog_id, checklist_key, name)
                if folder_info:
                    folder = folder_info.get("folder") or {}
                    new_item["folderPath"] = clean_cell_value(folder.get("path"))
                    new_item["folderUrl"] = clean_cell_value(folder.get("url"))
            except Exception as e:
                yandex_folder_warning = str(e)
                write_debug_log("custom_opr_folder_create_failed", {
                    "dialogId": dialog_id,
                    "checklistKey": checklist_key,
                    "groupId": group_id,
                    "itemId": new_item_id,
                    "itemName": name,
                    "error": str(e),
                })

    else:
        new_item_id = f"id_g{group_id}_custom_{uuid.uuid4().hex[:8]}"
        new_item = {
            "id": new_item_id,
            "group": group_id,
            "order": next_order,
            "name": name,
            "priority": "white",
            "status": "",
            "plan": "",
            "fact": "",
            "folderKey": build_folder_key("id", name, new_item_id),
            "folderPath": "",
            "folderUrl": "",
            "documents": [],
            "documentUrl": "",
            "documentName": "",
            "isCustom": True,
        }

        can_create_yandex_folder = False

        try:
            can_create_yandex_folder = can_create_custom_item_yandex_folder(dialog_id, checklist_key)
        except Exception as e:
            yandex_folder_warning = str(e)
            write_debug_log("custom_id_folder_check_failed", {
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "groupId": group_id,
                "itemId": new_item_id,
                "itemName": name,
                "error": str(e),
            })

        if can_create_yandex_folder:
            try:
                ensure_yandex_folder_for_custom_item(
                    dialog_id=dialog_id,
                    checklist_key=checklist_key,
                    group_id=group_id,
                    item_name=name,
                    item_id=new_item_id,
                )

                folder_info = get_item_yandex_folder(dialog_id, checklist_key, name)
                if folder_info:
                    folder = folder_info.get("folder") or {}
                    new_item["folderPath"] = clean_cell_value(folder.get("path"))
                    new_item["folderUrl"] = clean_cell_value(folder.get("url"))
            except Exception as e:
                yandex_folder_warning = str(e)
                write_debug_log("custom_id_folder_create_failed", {
                    "dialogId": dialog_id,
                    "checklistKey": checklist_key,
                    "groupId": group_id,
                    "itemId": new_item_id,
                    "itemName": name,
                    "error": str(e),
                })

    items.append(new_item)
    data["items"] = items
    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)

    created_item = None
    for item in data.get("items", []):
        if str(item.get("id")) == new_item["id"]:
            created_item = item
            break

    response_payload = {
        "ok": True,
        "dialogId": dialog_id,
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
    item_id = str(payload.get("itemId") or "").strip()
    field = str(payload.get("field") or "").strip()
    value = payload.get("value")

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    if checklist_key == "concept":
        allowed_fields = {"priority", "status", "plan", "fact"}
    elif checklist_key == "opr":
        allowed_fields = {"priority", "status", "plan", "fact"}
    else:
        allowed_fields = {"priority", "status", "plan", "fact"}

    if field not in allowed_fields:
        return JSONResponse({"ok": False, "error": "invalid field"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
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
        if checklist_key == "concept":
            new_status = normalize_status(value)
            target_item["status"] = new_status
            target_item["priority"] = derive_indicator_from_status(new_status)

            if new_status == "Нет":
                cleared_item = remove_all_item_documents(dialog_id, checklist_key, item_id, target_item)
                target_item.clear()
                target_item.update(cleared_item)

            if new_status == "Не требуется":
                target_item["group"] = 10
            elif int(target_item.get("group") or 0) == 10:
                target_item["group"] = resolve_concept_group_id_by_item_id_or_name(target_item)

        elif checklist_key == "opr":
            new_status = normalize_status(value)
            target_item["status"] = new_status
            target_item["priority"] = derive_indicator_from_status(new_status)

            if new_status == "Нет":
                cleared_item = remove_all_item_documents(dialog_id, checklist_key, item_id, target_item)
                target_item.clear()
                target_item.update(cleared_item)

            if new_status == "Не требуется":
                target_item["group"] = 2
            elif int(target_item.get("group") or 0) == 2:
                target_item["group"] = resolve_opr_group_id_by_item_id_or_name(target_item)

        else:
            new_status = normalize_status(value)
            target_item["status"] = new_status
            target_item["priority"] = derive_indicator_from_status(new_status)

            if new_status == "Нет":
                cleared_item = remove_all_item_documents(dialog_id, checklist_key, item_id, target_item)
                target_item.clear()
                target_item.update(cleared_item)

            if new_status == "Не требуется":
                target_item["group"] = 4
            elif int(target_item.get("group") or 0) == 4:
                target_item["group"] = move_item_to_required_group(target_item)

    elif field == "plan":
        target_item["plan"] = normalize_date_string(value)

    elif field == "fact":
        target_item["fact"] = normalize_date_string(value)

    data["items"] = items
    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)

    updated_item = None
    for item in data.get("items", []):
        if str(item.get("id")) == item_id:
            updated_item = item
            break

    return JSONResponse({
        "ok": True,
        "item": updated_item,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "progressPercent": data.get("progressPercent", 0),
    })