from app.checklists.definitions import load_checklist_configs
from app.checklists.models import ChecklistConfig
from app.checklists.utils import normalize_checklist_key


CHECKLIST_CONFIGS: dict[str, ChecklistConfig] = load_checklist_configs()


def normalize_checklist_config_key(value: str) -> str:
    key = normalize_checklist_key(value)
    if key in CHECKLIST_CONFIGS:
        return key
    return "id"


def get_checklist_config(checklist_key: str) -> ChecklistConfig:
    return CHECKLIST_CONFIGS[normalize_checklist_config_key(checklist_key)]


def list_checklist_configs() -> list[ChecklistConfig]:
    return sorted(
        CHECKLIST_CONFIGS.values(),
        key=lambda config: (config.order, config.key),
    )


def get_project_checklists_from_config() -> list[dict]:
    return [
        {
            "key": config.key,
            "title": config.title,
            "order": config.order,
            "notRequiredGroupId": config.not_required_group_id,
            "defaultGroupId": config.default_group_id,
            "allowCustomItemGroupIds": list(config.allow_custom_item_group_ids),
        }
        for config in list_checklist_configs()
    ]


def get_checklist_title_from_config(checklist_key: str) -> str:
    return get_checklist_config(checklist_key).title


def get_standard_yandex_folder_specs(checklist_key: str) -> dict:
    return dict(get_checklist_config(checklist_key).standard_yandex_folder_specs or {})


def get_checklist_yandex_root_config(checklist_key: str) -> dict:
    config = get_checklist_config(checklist_key)

    return {
        "key": config.key,
        "title": config.title,
        "order": config.order,
        "yandexRootContextKey": config.yandex_root_context_key,
        "yandexRootRelativePath": config.yandex_root_relative_path,
        "yandexRootAlias": config.yandex_root_alias,
        "yandexRootFolderName": config.yandex_root_folder_name,
    }