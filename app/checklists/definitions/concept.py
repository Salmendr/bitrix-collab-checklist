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
        "id": 10,
        "title": "Не требуется",
        "items": [],
    },
]

STANDARD_CONCEPT_YANDEX_FOLDER_SPECS = {
    "Согласованная/Утвержденная концепция": {
        "alias": "concept_approved",
        "folderName": "01_Согласованная_Утвержденная концепция",
        "relativePath": "01_Согласованная_Утвержденная концепция",
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
)