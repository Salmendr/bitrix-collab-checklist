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

# The windows ask these every few seconds while a file goes to Yandex or a
# session lives; their successful answers filled the docker log. Errors of
# them and every other request are still logged.
# ACCESS_LOG_QUIET_POLLING=0 logs them all again.
QUIET_ACCESS_LOG_PATHS = (
    "/api/checklist/document-mirror-status",
    "/api/checklist/upload-job-status",
    "/api/checklist/yandex-structure-job",
    "/api/checklist/yandex-mirror-queue-state",
    "/api/checklist/session/heartbeat",
    "/api/checklist/session/status",
    "/api/checklist/lock/heartbeat",
    "/api/debug/event",
)


class QuietPollingAccessLog:
    """A filter of the uvicorn access log: drops successful polling."""

    def filter(self, record) -> bool:
        try:
            args = record.args
            path = str(args[2]).split("?", 1)[0].rstrip("/")
            status = int(args[4])
        except Exception:
            return True
        return not (status < 400 and path.endswith(QUIET_ACCESS_LOG_PATHS))


def install_quiet_access_log() -> bool:
    import logging

    if os.getenv("ACCESS_LOG_QUIET_POLLING", "1").strip().lower() in {"0", "false", "off", "no"}:
        return False
    access = logging.getLogger("uvicorn.access")
    if not any(isinstance(entry, QuietPollingAccessLog) for entry in access.filters):
        access.addFilter(QuietPollingAccessLog())
    return True
