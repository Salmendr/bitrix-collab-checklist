from __future__ import annotations

import re
from pathlib import Path
from typing import Mapping, Any


UI_ROOT = Path(__file__).resolve().parent
UI_TEMPLATE_ROOT = UI_ROOT / "templates"
UI_STATIC_ROOT = UI_ROOT / "static"

_PLACEHOLDER_PATTERN = re.compile(r"\[\[([A-Z0-9_]+)\]\]")


def render_ui_template(
    template_name: str,
    context: Mapping[str, Any],
) -> str:
    """Render a trusted internal UI template using explicit placeholders.

    Values are prepared by the caller. This renderer intentionally performs
    no HTML escaping because popup.py already distinguishes escaped HTML values
    from JSON and JavaScript fragments.
    """
    normalized_name = str(template_name or "").replace("\\", "/").strip("/")

    if not normalized_name or ".." in normalized_name.split("/"):
        raise ValueError("Invalid UI template name")

    template_path = (UI_TEMPLATE_ROOT / normalized_name).resolve()
    template_root = UI_TEMPLATE_ROOT.resolve()

    try:
        template_path.relative_to(template_root)
    except ValueError as exc:
        raise ValueError("UI template path escapes template root") from exc

    text = template_path.read_text(encoding="utf-8")
    required = set(_PLACEHOLDER_PATTERN.findall(text))
    supplied = {str(key) for key in context.keys()}

    missing = sorted(required - supplied)
    if missing:
        raise KeyError(
            "Missing UI template values: " + ", ".join(missing)
        )

    unexpected = sorted(supplied - required)
    if unexpected:
        raise KeyError(
            "Unexpected UI template values: " + ", ".join(unexpected)
        )

    def replace(match: re.Match[str]) -> str:
        return str(context[match.group(1)])

    return _PLACEHOLDER_PATTERN.sub(replace, text)
