import threading

from app.logging_utils import write_debug_log
from app.settings import EDIT_SESSION_SWEEP_SECONDS

from app.checklists.edit_sessions import (
    recover_interrupted_edit_sessions,
    sweep_expired_edit_sessions,
)


EDIT_SESSION_SWEEPER_GUARD = threading.Lock()
EDIT_SESSION_SWEEPER_STARTED = False
EDIT_SESSION_SWEEPER_STOP_EVENT = threading.Event()


def run_edit_session_sweep(
    source: str = "sweeper",
) -> dict:
    """Auto-save abandoned sessions; never roll them back."""
    result = sweep_expired_edit_sessions(
        source=source,
        limit=1000,
    )

    if (
        result.get("expiredFound")
        or result.get("errorCount")
    ):
        write_debug_log(
            "edit_session_sweep_finished",
            result,
        )

    return result


def edit_session_sweeper_worker() -> None:
    write_debug_log(
        "edit_session_sweeper_started",
        {
            "intervalSeconds": (
                EDIT_SESSION_SWEEP_SECONDS
            ),
        },
    )

    while not EDIT_SESSION_SWEEPER_STOP_EVENT.wait(
        EDIT_SESSION_SWEEP_SECONDS
    ):
        try:
            run_edit_session_sweep(
                source="background_sweeper"
            )
        except Exception as exc:
            write_debug_log(
                "edit_session_sweeper_failed",
                {
                    "error": str(exc),
                },
            )


def start_edit_session_sweeper() -> dict:
    global EDIT_SESSION_SWEEPER_STARTED

    with EDIT_SESSION_SWEEPER_GUARD:
        if EDIT_SESSION_SWEEPER_STARTED:
            return {
                "ok": True,
                "started": False,
                "alreadyStarted": True,
                "intervalSeconds": (
                    EDIT_SESSION_SWEEP_SECONDS
                ),
            }

        EDIT_SESSION_SWEEPER_STARTED = True
        EDIT_SESSION_SWEEPER_STOP_EVENT.clear()

    thread = threading.Thread(
        target=edit_session_sweeper_worker,
        daemon=True,
        name="edit-session-sweeper",
    )
    thread.start()

    return {
        "ok": True,
        "started": True,
        "intervalSeconds": (
            EDIT_SESSION_SWEEP_SECONDS
        ),
    }


def recover_edit_sessions_on_startup(
    source: str = "startup",
) -> dict:
    result = recover_interrupted_edit_sessions(
        source=source
    )

    write_debug_log(
        "edit_session_startup_recovery_finished",
        result,
    )

    return result


def get_edit_session_sweeper_state() -> dict:
    with EDIT_SESSION_SWEEPER_GUARD:
        return {
            "ok": True,
            "started": EDIT_SESSION_SWEEPER_STARTED,
            "intervalSeconds": (
                EDIT_SESSION_SWEEP_SECONDS
            ),
        }
