# Definitions for checklist "Дизайн".

from app.checklists.models import ChecklistConfig
from app.checklists.definitions.helpers import groups_from_list


DESIGN_GROUPS = [
    {
        "id": 1,
        "title": "Исходная документация",
        "items": [
            "ТЗ",
            "Брендбук",
            "Концепция фасадов",
            "АР",
            "ОВ",
            "ВК",
            "СС",
        ],
    },
    {
        "id": 2,
        "title": "Концепция",
        "items": [],
    },
    {
        "id": 3,
        "title": "Рабочая документация",
        "items": [],
    },
    {
        "id": 5,
        "title": "Задания смежным специалистам",
        "items": [
            "АР",
            "ОВ",
            "ВК",
            "ЭОМ",
        ],
    },
    {
        "id": 4,
        "title": "Не требуется",
        "items": [],
    },
]


def _spec(alias: str, group_id: int, item_name: str, folder: str, parent: str) -> dict:
    return {
        "groupId": group_id,
        "itemName": item_name,
        "alias": alias,
        "folderName": folder,
        "relativePath": f"{parent}/{folder}",
    }


STANDARD_DESIGN_YANDEX_FOLDER_SPECS = {
    "design_source_tz": _spec("design_source_tz", 1, "ТЗ", "01_ТЗ", "01_Исходная документация"),
    "design_source_brandbook": _spec("design_source_brandbook", 1, "Брендбук", "02_Брендбук", "01_Исходная документация"),
    "design_source_facades": _spec("design_source_facades", 1, "Концепция фасадов", "03_Концепция фасадов", "01_Исходная документация"),
    "design_source_ar": _spec("design_source_ar", 1, "АР", "04_АР", "01_Исходная документация"),
    "design_source_ov": _spec("design_source_ov", 1, "ОВ", "05_ОВ", "01_Исходная документация"),
    "design_source_vk": _spec("design_source_vk", 1, "ВК", "06_ВК", "01_Исходная документация"),
    "design_source_ss": _spec("design_source_ss", 1, "СС", "07_СС", "01_Исходная документация"),

    # Sections without predefined items: custom items are created here.
    "design_concept_root": {
        "groupId": 2,
        "itemName": "Концепция",
        "alias": "design_concept_root",
        "folderName": "02_Концепция",
        "relativePath": "02_Концепция",
        "customItemsRoot": True,
    },
    "design_working_root": {
        "groupId": 3,
        "itemName": "Рабочая документация",
        "alias": "design_working_root",
        "folderName": "03_Рабочая документация",
        "relativePath": "03_Рабочая документация",
        "customItemsRoot": True,
    },

    "design_tasks_ar": _spec("design_tasks_ar", 5, "АР", "01_АР", "04_Задания смежникам"),
    "design_tasks_ov": _spec("design_tasks_ov", 5, "ОВ", "02_ОВ", "04_Задания смежникам"),
    "design_tasks_vk": _spec("design_tasks_vk", 5, "ВК", "03_ВК", "04_Задания смежникам"),
    "design_tasks_eom": _spec("design_tasks_eom", 5, "ЭОМ", "04_ЭОМ", "04_Задания смежникам"),
}


CHECKLIST_CONFIG = ChecklistConfig(
    key="design",
    title="Чек-лист Дизайн",
    groups=groups_from_list(DESIGN_GROUPS),
    not_required_group_id=4,
    default_group_id=1,
    allow_custom_item_group_ids=(1, 2, 3, 5),
    order=60,
    yandex_root_relative_path="02_Выдача документации/05_Дизайн",
    yandex_root_alias="design_stage_root",
    yandex_root_folder_name="05_Дизайн",
    reset_status_on_last_document_removed=True,
    standard_yandex_folder_specs=STANDARD_DESIGN_YANDEX_FOLDER_SPECS,
    stage_yandex_folder_alias="design_stage_root",
    layout_mode="generic",
    # Left: source documents; middle: concept; right: working documentation
    # with the adjacent-specialist tasks below it.
    panel_group_ids=((1,), (2,), (3, 5)),
)
