# Definitions for checklist "Концепция".

from app.checklists.models import ChecklistConfig
from app.checklists.definitions.helpers import groups_from_list


CONCEPT_GROUPS = [
    {
        "id": 1,
        "title": "Концепция",
        "items": [
            "Согласованная/Утвержденная концепция",
        ],
    },
    {
        "id": 90,
        "title": "BIM-Модель",
        "items": [
            "BIM",
            "ГП",
            "БЛ",
            "АР",
            "КР",
            "ФР",
        ],
    },
    {
        "id": 10,
        "title": "Не требуется",
        "items": [],
    },
]


STANDARD_CONCEPT_YANDEX_FOLDER_SPECS = {
    "concept_approved": {
        "groupId": 1,
        "itemName": "Согласованная/Утвержденная концепция",
        "alias": "concept_approved",
        "folderName": "01_Согласованная_Утвержденная концепция",
        "relativePath": "01_Согласованная_Утвержденная концепция",
    },

    "concept_bim_root": {
        "groupId": 90,
        "itemName": "BIM-Модель",
        "alias": "concept_bim_root",
        "folderName": "02_BIM-Модель",
        "relativePath": "02_BIM-Модель",
    },
    "concept_bim_bim": {
        "groupId": 90,
        "itemName": "BIM",
        "alias": "concept_bim_bim",
        "folderName": "00_BIM",
        "relativePath": "02_BIM-Модель/00_BIM",
    },
    "concept_bim_gp": {
        "groupId": 90,
        "itemName": "ГП",
        "alias": "concept_bim_gp",
        "folderName": "01_ГП",
        "relativePath": "02_BIM-Модель/01_ГП",
    },
    "concept_bim_bl": {
        "groupId": 90,
        "itemName": "БЛ",
        "alias": "concept_bim_bl",
        "folderName": "02_БЛ",
        "relativePath": "02_BIM-Модель/02_БЛ",
    },
    "concept_bim_ar": {
        "groupId": 90,
        "itemName": "АР",
        "alias": "concept_bim_ar",
        "folderName": "03_АР",
        "relativePath": "02_BIM-Модель/03_АР",
    },
    "concept_bim_kr": {
        "groupId": 90,
        "itemName": "КР",
        "alias": "concept_bim_kr",
        "folderName": "04_КР",
        "relativePath": "02_BIM-Модель/04_КР",
    },
    "concept_bim_fr": {
        "groupId": 90,
        "itemName": "ФР",
        "alias": "concept_bim_fr",
        "folderName": "05_ФР",
        "relativePath": "02_BIM-Модель/05_ФР",
    },
}


CHECKLIST_CONFIG = ChecklistConfig(
    key="concept",
    title="Чек-лист Концепция",
    groups=groups_from_list(CONCEPT_GROUPS),
    not_required_group_id=10,
    default_group_id=1,
    allow_custom_item_group_ids=(1,),
    order=20,
    yandex_root_relative_path="02_Выдача документации/01_Концепция",
    yandex_root_alias="concept_stage_root",
    yandex_root_folder_name="01_Концепция",
    reset_status_on_last_document_removed=False,
    standard_yandex_folder_specs=STANDARD_CONCEPT_YANDEX_FOLDER_SPECS,
    stage_yandex_folder_alias="concept_stage_root",
    layout_mode="concept_with_bim",
    bim_group_id=90,
    bim_placement="right",
)
