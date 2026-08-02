import json
import html

from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.checklists.storage import (
    get_checklist,
    get_project_storage_context,
    get_project_root_yandex_folder_info,
)

from app.checklists.permissions import get_file_delete_allowed_user_ids
from app.checklists.config import list_checklist_configs
from app.checklists.yandex_context import resolve_checklist_yandex_root_path, get_project_root_path
from app.checklists.document_assignment_history import (
    attach_assignment_history_counts,
)
from app.yandex_disk.client import is_yandex_disk_enabled
from app.ui.template_engine import render_ui_template

def popup_html(dialogId: str = "", checklistKey: str = "id") -> str:
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    data = get_checklist(dialog_id, checklist_key)
    data = attach_assignment_history_counts(
        data,
        dialog_id=dialog_id,
        checklist_key=checklist_key,
    )
    project_context = get_project_storage_context(dialog_id) or {}

    title_raw = clean_cell_value(data.get("title")) or "Чек-лист"
    title = html.escape(title_raw)

    project_name_raw = clean_cell_value(project_context.get("projectName"))
    collab_title_raw = clean_cell_value(data.get("collabTitle")) or project_name_raw

    collab_title = html.escape(collab_title_raw)
    full_title = f"{title} — {collab_title}" if collab_title_raw else title
    progress_percent = int(data.get("progressPercent", 0) or 0)

    project_root_folder_info = get_project_root_yandex_folder_info(dialog_id)

    project_root_yandex_path_value = (
        clean_cell_value(project_root_folder_info.get("path"))
        or clean_cell_value((project_context.get("yandexDisk") or {}).get("projectRootPath"))
        or clean_cell_value(get_project_root_path(project_context))
    )

    project_root_yandex_url_value = (
        clean_cell_value(project_root_folder_info.get("url"))
        or clean_cell_value((project_context.get("yandexDisk") or {}).get("projectRootUrl"))
    )

    project_root_yandex_path_json = json.dumps(
        project_root_yandex_path_value,
        ensure_ascii=False
    )
    project_root_yandex_url_json = json.dumps(
        project_root_yandex_url_value,
        ensure_ascii=False
    )

    project_root_yandex_prepared_json = json.dumps(
        bool(project_root_folder_info.get("standardFoldersPrepared")),
        ensure_ascii=False
    )

    project_yandex_disk = project_context.get("yandexDisk") or {}
    project_yandex_folders = project_yandex_disk.get("folders") or {}
    project_storage_mode = project_context.get("storageMode") or {}
    project_mirror_targets = project_storage_mode.get("mirrorTargets") or []
    project_yandex_disabled = (
        not project_context
        or "yandex_disk" not in project_mirror_targets
        or not is_yandex_disk_enabled()
    )
    project_yandex_disabled_reason = (
        "project storage context not found"
        if not project_context
        else "yandex_disk is not in mirrorTargets"
        if "yandex_disk" not in project_mirror_targets
        else "Yandex Disk OAuth token is not configured"
        if not is_yandex_disk_enabled()
        else ""
    )

    stage_yandex_folders_by_key = {}

    for checklist_config in list_checklist_configs():
        stage_alias = (
            clean_cell_value(checklist_config.stage_yandex_folder_alias)
            or clean_cell_value(checklist_config.yandex_root_alias)
        )

        folder = project_yandex_folders.get(stage_alias) or {}

        stage_path = (
            clean_cell_value(folder.get("path"))
            or clean_cell_value(resolve_checklist_yandex_root_path(project_context, checklist_config))
        )

        stage_yandex_folders_by_key[checklist_config.key] = {
            "alias": stage_alias,
            "path": stage_path,
            "url": clean_cell_value(folder.get("url") or folder.get("public_url")),
            "yandexDisabled": project_yandex_disabled,
            "reason": project_yandex_disabled_reason,
        }

    stage_yandex_folders_json = json.dumps(
        stage_yandex_folders_by_key,
        ensure_ascii=False
    )

    checklist_layout_meta_by_key = {}

    for checklist_config in list_checklist_configs():
        checklist_layout_meta_by_key[checklist_config.key] = {
            "key": checklist_config.key,
            "title": checklist_config.title,
            "notRequiredGroupId": checklist_config.not_required_group_id,
            "defaultGroupId": checklist_config.default_group_id,
            "allowCustomItemGroupIds": list(checklist_config.allow_custom_item_group_ids),
            "stageYandexFolderAlias": checklist_config.stage_yandex_folder_alias,
            "layoutMode": checklist_config.layout_mode,
            "bimGroupId": checklist_config.bim_group_id,
            "bimPlacement": checklist_config.bim_placement,
        }

    checklist_layout_meta_json = json.dumps(
        checklist_layout_meta_by_key,
        ensure_ascii=False
    )

    items_json = json.dumps(data.get("items", []), ensure_ascii=False)
    order_version_json = json.dumps(int(data.get("orderVersion") or 0))
    groups_json = json.dumps(data.get("groups", []), ensure_ascii=False)
    project_checklists_json = json.dumps(data.get("projectChecklists", []), ensure_ascii=False)
    dialog_id_json = json.dumps(dialog_id, ensure_ascii=False)
    collab_title_json = json.dumps(collab_title_raw, ensure_ascii=False)
    checklist_key_json = json.dumps(checklist_key, ensure_ascii=False)
    checklist_title_json = json.dumps(title_raw, ensure_ascii=False)
    file_delete_allowed_user_ids_json = json.dumps(
        sorted(get_file_delete_allowed_user_ids()),
        ensure_ascii=False
    )

    return render_ui_template(
        "popup.html",
        {
            "POPUP_FULL_TITLE": full_title,
            "POPUP_PROGRESS_PERCENT": progress_percent,
            "POPUP_DIALOG_ID_JSON": dialog_id_json,
            "POPUP_PROJECT_ROOT_YANDEX_PATH_JSON": project_root_yandex_path_json,
            "POPUP_PROJECT_ROOT_YANDEX_URL_JSON": project_root_yandex_url_json,
            "POPUP_PROJECT_ROOT_YANDEX_PREPARED_JSON": project_root_yandex_prepared_json,
            "POPUP_STAGE_YANDEX_FOLDERS_JSON": stage_yandex_folders_json,
            "POPUP_CHECKLIST_LAYOUT_META_JSON": checklist_layout_meta_json,
            "POPUP_GROUPS_JSON": groups_json,
            "POPUP_PROJECT_CHECKLISTS_JSON": project_checklists_json,
            "POPUP_ITEMS_JSON": items_json,
            "POPUP_ORDER_VERSION_JSON": order_version_json,
            "POPUP_COLLAB_TITLE_JSON": collab_title_json,
            "POPUP_CHECKLIST_KEY_JSON": checklist_key_json,
            "POPUP_CHECKLIST_TITLE_JSON": checklist_title_json,
            "POPUP_FILE_DELETE_ALLOWED_USER_IDS_JSON": file_delete_allowed_user_ids_json,
        },
    )
