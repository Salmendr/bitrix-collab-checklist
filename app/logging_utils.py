import json
import os
from datetime import datetime

from app.settings import DEBUG_DIR, DEBUG_LOG_PATH


# Popup-open diagnostics produced hundreds of records per session. Only the
# events that describe an actual open/reopen/close decision and any error are
# kept. Set POPUP_DIAGNOSTICS_VERBOSE=1 to record everything again.
ESSENTIAL_POPUP_DIAG_EVENTS = frozenset({
    "popup_diag_launch_created",
    "popup_diag_launcher_reopen_click_detected",
    "popup_diag_launcher_close_callback",
    "popup_diag_close_requested",
    "popup_diag_surface_unhandled_rejection",
})
NOISY_DEBUG_EVENTS = frozenset({
    "render_state",
    "yandex_folder_chain_ensure_started",
})


def is_debug_event_recorded(event: str) -> bool:
    name = str(event or "")
    if os.getenv("POPUP_DIAGNOSTICS_VERBOSE", "").strip() == "1":
        return True
    if name in NOISY_DEBUG_EVENTS:
        return False
    if not name.startswith("popup_diag_"):
        return True
    if name in ESSENTIAL_POPUP_DIAG_EVENTS:
        return True
    return name.endswith("_error") or name.endswith("_failed")


def write_debug_log(event: str, payload: dict):
    if not is_debug_event_recorded(event):
        return
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    record = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "event": event,
        "payload": payload,
    }

    with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")