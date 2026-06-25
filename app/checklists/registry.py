from app.checklists.constants import (
    STATUS_OPTIONS,
    PRIORITY_OPTIONS,
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

from app.checklists.config import (
    CHECKLIST_CONFIGS,
    get_project_checklists_from_config,
    get_checklist_title_from_config,
)


PROJECT_CHECKLISTS = get_project_checklists_from_config()

CHECKLIST_TITLES = {
    key: config.title
    for key, config in CHECKLIST_CONFIGS.items()
}


def get_project_checklists():
    return get_project_checklists_from_config()


def get_checklist_title(checklist_key: str) -> str:
    return get_checklist_title_from_config(checklist_key)