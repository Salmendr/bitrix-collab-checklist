# Definitions for checklist "ОПР".

from app.checklists.models import ChecklistConfig
from app.checklists.definitions.helpers import groups_from_list

OPR_GROUPS = [
    {
        "id": 1,
        "title": "ОПР",
        "items": [
            "ОПР.ГП",
            "ОПР.АР",
            "ИОС 1",
            "ИОС 2",
            "ИОС 3",
            "ИОС 4",
            "ИОС 5",
        ],
    },
    {
        "id": 2,
        "title": "Не требуется",
        "items": [],
    },
]

STANDARD_OPR_YANDEX_FOLDER_SPECS = {
    "ОПР.ГП": {
        "alias": "opr_gp",
        "folderName": "01_ОПР.ГП",
        "relativePath": "01_ОПР.ГП",
    },
    "ОПР.АР": {
        "alias": "opr_ar",
        "folderName": "02_ОПР.АР",
        "relativePath": "02_ОПР.АР",
    },
    "ИОС 1": {
        "alias": "opr_ios_1",
        "folderName": "ИОС_1",
        "relativePath": "03_ИОС/ИОС_1",
    },
    "ИОС 2": {
        "alias": "opr_ios_2",
        "folderName": "ИОС_2",
        "relativePath": "03_ИОС/ИОС_2",
    },
    "ИОС 3": {
        "alias": "opr_ios_3",
        "folderName": "ИОС_3",
        "relativePath": "03_ИОС/ИОС_3",
    },
    "ИОС 4": {
        "alias": "opr_ios_4",
        "folderName": "ИОС_4",
        "relativePath": "03_ИОС/ИОС_4",
    },
    "ИОС 5": {
        "alias": "opr_ios_5",
        "folderName": "ИОС_5",
        "relativePath": "03_ИОС/ИОС_5",
    },
}

CHECKLIST_CONFIG = ChecklistConfig(
    key="opr",
    title="Чек-лист ОПР",
    groups=groups_from_list(OPR_GROUPS),
    not_required_group_id=2,
    default_group_id=1,
    allow_custom_item_group_ids=(1,),
    order=20,
    yandex_root_relative_path="02_Выдача документации/02_ОПР",
    yandex_root_alias="opr_stage_root",
    yandex_root_folder_name="02_ОПР",
    reset_status_on_last_document_removed=True,
    standard_yandex_folder_specs=STANDARD_OPR_YANDEX_FOLDER_SPECS,
)