from importlib import import_module
from pkgutil import iter_modules

from app.checklists.models import ChecklistConfig


SKIP_DEFINITION_MODULES = {
    "helpers",
}


def iter_definition_module_names() -> list[str]:
    return sorted(
        module_info.name
        for module_info in iter_modules(__path__)
        if not module_info.ispkg
        and not module_info.name.startswith("_")
        and module_info.name not in SKIP_DEFINITION_MODULES
    )


def load_checklist_configs() -> dict[str, ChecklistConfig]:
    result: dict[str, ChecklistConfig] = {}

    for module_name in iter_definition_module_names():
        module = import_module(f"{__name__}.{module_name}")
        config = getattr(module, "CHECKLIST_CONFIG", None)

        if config is None:
            continue

        if not isinstance(config, ChecklistConfig):
            raise TypeError(
                f"{module.__name__}.CHECKLIST_CONFIG must be ChecklistConfig"
            )

        if not config.key:
            raise ValueError(
                f"{module.__name__}.CHECKLIST_CONFIG.key is empty"
            )

        if config.key in result:
            raise ValueError(
                f"Duplicate checklist config key: {config.key}"
            )

        result[config.key] = config

    return result