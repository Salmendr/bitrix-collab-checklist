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
        "id": 90,
        "title": "BIM-Модель",
        "items": [
            "BIM",
            "ГП",
            "БЛ",
            "НИС",
            "АР",
            "АИ",
            "ФР",
            "КР",
            "ОВ",
            "ВК",
            "ЭОМ",
            "СС",
            "ТХ",
        ],
    },
    {
        "id": 2,
        "title": "Не требуется",
        "items": [],
    },
]


STANDARD_OPR_YANDEX_FOLDER_SPECS = {
    "opr_gp": {
        "groupId": 1,
        "itemName": "ОПР.ГП",
        "alias": "opr_gp",
        "folderName": "01_ОПР.ГП",
        "relativePath": "01_ОПР.ГП",
    },
    "opr_ar": {
        "groupId": 1,
        "itemName": "ОПР.АР",
        "alias": "opr_ar",
        "folderName": "02_ОПР.АР",
        "relativePath": "02_ОПР.АР",
    },
    "opr_ios_1": {
        "groupId": 1,
        "itemName": "ИОС 1",
        "alias": "opr_ios_1",
        "folderName": "ИОС_1",
        "relativePath": "03_ИОС/ИОС_1",
    },
    "opr_ios_2": {
        "groupId": 1,
        "itemName": "ИОС 2",
        "alias": "opr_ios_2",
        "folderName": "ИОС_2",
        "relativePath": "03_ИОС/ИОС_2",
    },
    "opr_ios_3": {
        "groupId": 1,
        "itemName": "ИОС 3",
        "alias": "opr_ios_3",
        "folderName": "ИОС_3",
        "relativePath": "03_ИОС/ИОС_3",
    },
    "opr_ios_4": {
        "groupId": 1,
        "itemName": "ИОС 4",
        "alias": "opr_ios_4",
        "folderName": "ИОС_4",
        "relativePath": "03_ИОС/ИОС_4",
    },
    "opr_ios_5": {
        "groupId": 1,
        "itemName": "ИОС 5",
        "alias": "opr_ios_5",
        "folderName": "ИОС_5",
        "relativePath": "03_ИОС/ИОС_5",
    },

    "opr_bim_root": {
        "groupId": 90,
        "itemName": "BIM-Модель",
        "alias": "opr_bim_root",
        "folderName": "04_BIM-Модель",
        "relativePath": "04_BIM-Модель",
    },
    "opr_bim_bim": {
        "groupId": 90,
        "itemName": "BIM",
        "alias": "opr_bim_bim",
        "folderName": "00_BIM",
        "relativePath": "04_BIM-Модель/00_BIM",
    },
    "opr_bim_gp": {
        "groupId": 90,
        "itemName": "ГП",
        "alias": "opr_bim_gp",
        "folderName": "01_ГП",
        "relativePath": "04_BIM-Модель/01_ГП",
    },
    "opr_bim_bl": {
        "groupId": 90,
        "itemName": "БЛ",
        "alias": "opr_bim_bl",
        "folderName": "02_БЛ",
        "relativePath": "04_BIM-Модель/02_БЛ",
    },
    "opr_bim_nis": {
        "groupId": 90,
        "itemName": "НИС",
        "alias": "opr_bim_nis",
        "folderName": "03_НИС",
        "relativePath": "04_BIM-Модель/03_НИС",
    },
    "opr_bim_ar": {
        "groupId": 90,
        "itemName": "АР",
        "alias": "opr_bim_ar",
        "folderName": "04_АР",
        "relativePath": "04_BIM-Модель/04_АР",
    },
    "opr_bim_ai": {
        "groupId": 90,
        "itemName": "АИ",
        "alias": "opr_bim_ai",
        "folderName": "05_АИ",
        "relativePath": "04_BIM-Модель/05_АИ",
    },
    "opr_bim_fr": {
        "groupId": 90,
        "itemName": "ФР",
        "alias": "opr_bim_fr",
        "folderName": "06_ФР",
        "relativePath": "04_BIM-Модель/06_ФР",
    },
    "opr_bim_kr": {
        "groupId": 90,
        "itemName": "КР",
        "alias": "opr_bim_kr",
        "folderName": "07_КР",
        "relativePath": "04_BIM-Модель/07_КР",
    },
    "opr_bim_ov": {
        "groupId": 90,
        "itemName": "ОВ",
        "alias": "opr_bim_ov",
        "folderName": "08_ОВ",
        "relativePath": "04_BIM-Модель/08_ОВ",
    },
    "opr_bim_vk": {
        "groupId": 90,
        "itemName": "ВК",
        "alias": "opr_bim_vk",
        "folderName": "09_ВК",
        "relativePath": "04_BIM-Модель/09_ВК",
    },
    "opr_bim_eom": {
        "groupId": 90,
        "itemName": "ЭОМ",
        "alias": "opr_bim_eom",
        "folderName": "10_ЭОМ",
        "relativePath": "04_BIM-Модель/10_ЭОМ",
    },
    "opr_bim_ss": {
        "groupId": 90,
        "itemName": "СС",
        "alias": "opr_bim_ss",
        "folderName": "11_СС",
        "relativePath": "04_BIM-Модель/11_СС",
    },
    "opr_bim_tx": {
        "groupId": 90,
        "itemName": "ТХ",
        "alias": "opr_bim_tx",
        "folderName": "12_ТХ",
        "relativePath": "04_BIM-Модель/12_ТХ",
    },
}


CHECKLIST_CONFIG = ChecklistConfig(
    key="opr",
    title="Чек-лист ОПР",
    groups=groups_from_list(OPR_GROUPS),
    not_required_group_id=2,
    default_group_id=1,
    allow_custom_item_group_ids=(1,),
    order=30,
    yandex_root_relative_path="02_Выдача документации/02_ОПР",
    yandex_root_alias="opr_stage_root",
    yandex_root_folder_name="02_ОПР",
    reset_status_on_last_document_removed=True,
    standard_yandex_folder_specs=STANDARD_OPR_YANDEX_FOLDER_SPECS,
    stage_yandex_folder_alias="opr_stage_root",
    layout_mode="opr_with_bim",
    bim_group_id=90,
    bim_placement="right",
)
