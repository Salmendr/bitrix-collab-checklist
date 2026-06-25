from app.checklists.models import (
    ChecklistConfig,
    ChecklistGroupConfig,
)

from app.checklists.definitions.id import (
    ID_GROUPS,
    STANDARD_ID_YANDEX_FOLDER_SPECS,
)

from app.checklists.definitions.opr import (
    OPR_GROUPS,
    STANDARD_OPR_YANDEX_FOLDER_SPECS,
)

from app.checklists.definitions.concept import (
    CONCEPT_GROUPS,
    STANDARD_CONCEPT_YANDEX_FOLDER_SPECS,
)


def groups_from_id_dict(raw_groups: dict) -> tuple[ChecklistGroupConfig, ...]:
    return tuple(
        ChecklistGroupConfig(
            id=int(group_id),
            title=str(group_data.get("title") or ""),
            items=tuple(group_data.get("items") or ()),
        )
        for group_id, group_data in raw_groups.items()
    )


def groups_from_list(raw_groups: list[dict]) -> tuple[ChecklistGroupConfig, ...]:
    return tuple(
        ChecklistGroupConfig(
            id=int(group.get("id") or 0),
            title=str(group.get("title") or ""),
            items=tuple(group.get("items") or ()),
        )
        for group in raw_groups
    )


CHECKLIST_CONFIGS: dict[str, ChecklistConfig] = {
    "id": ChecklistConfig(
        key="id",
        title="Чек-лист ИД",
        groups=groups_from_id_dict(ID_GROUPS),
        not_required_group_id=4,
        default_group_id=3,
        allow_custom_item_group_ids=(1, 2, 3),
        reset_status_on_last_document_removed=True,
        standard_yandex_folder_specs=STANDARD_ID_YANDEX_FOLDER_SPECS,
    ),
    "opr": ChecklistConfig(
        key="opr",
        title="Чек-лист ОПР",
        groups=groups_from_list(OPR_GROUPS),
        not_required_group_id=2,
        default_group_id=1,
        allow_custom_item_group_ids=(1,),
        reset_status_on_last_document_removed=True,
        standard_yandex_folder_specs=STANDARD_OPR_YANDEX_FOLDER_SPECS,
    ),
    "concept": ChecklistConfig(
        key="concept",
        title="Чек-лист Концепция",
        groups=groups_from_list(CONCEPT_GROUPS),
        not_required_group_id=10,
        default_group_id=1,
        allow_custom_item_group_ids=(1,),
        reset_status_on_last_document_removed=False,
        standard_yandex_folder_specs=STANDARD_CONCEPT_YANDEX_FOLDER_SPECS,
    ),
}


def normalize_checklist_config_key(value: str) -> str:
    key = str(value or "").strip().lower()
    if key in CHECKLIST_CONFIGS:
        return key
    return "id"


def get_checklist_config(checklist_key: str) -> ChecklistConfig:
    return CHECKLIST_CONFIGS[normalize_checklist_config_key(checklist_key)]


def list_checklist_configs() -> list[ChecklistConfig]:
    return list(CHECKLIST_CONFIGS.values())


def get_project_checklists_from_config() -> list[dict]:
    return [
        {
            "key": config.key,
            "title": config.title,
        }
        for config in list_checklist_configs()
    ]


def get_checklist_title_from_config(checklist_key: str) -> str:
    return get_checklist_config(checklist_key).title


def get_standard_yandex_folder_specs(checklist_key: str) -> dict:
    return dict(get_checklist_config(checklist_key).standard_yandex_folder_specs or {})