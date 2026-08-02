from __future__ import annotations

from typing import Any

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.settings import EDIT_SESSION_TTL_SECONDS

from app.checklists.edit_session_files import (
    purge_committed_session_files,
    recover_edit_session_file_entries,
)
from app.checklists.edit_session_yandex import (
    enqueue_committed_edit_session_yandex_jobs,
)
from app.checklists.utils import clean_cell_value


def _row_to_dict(row) -> dict | None:
    return dict(row) if row else None


def _recovery_candidates() -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM edit_sessions
            WHERE status IN ('committing', 'rolling_back')
               OR (
                    status = 'error'
                    AND (
                        (
                            COALESCE(rollback_started_at, '') <> ''
                            AND COALESCE(rolled_back_at, '') = ''
                        )
                        OR (
                            COALESCE(commit_started_at, '') <> ''
                            AND COALESCE(committed_at, '') = ''
                            AND COALESCE(rollback_started_at, '') = ''
                        )
                    )
               )
            ORDER BY updated_at ASC, created_at ASC
            """
        ).fetchall()
    finally:
        conn.close()

    return [dict(row) for row in rows]


def _restart_active_session_candidates() -> list[dict]:
    """Return active sessions that must be auto-saved after restart.

    Browser heartbeats are process-bound. A process restart must never roll
    back user changes. Active rows are committed so their files and checklist
    data remain intact and their locks are released normally.
    """
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM edit_sessions
            WHERE status = 'active'
            ORDER BY updated_at ASC, created_at ASC
            """
        ).fetchall()
    finally:
        conn.close()

    return [dict(row) for row in rows]


def _committed_cleanup_candidates() -> list[str]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT s.session_id
            FROM edit_sessions AS s
            WHERE s.status = 'committed'
              AND (
                    EXISTS (
                        SELECT 1
                        FROM edit_session_file_entries AS f
                        WHERE f.session_id = s.session_id
                          AND f.status = 'committed'
                          AND f.entry_kind = 'stashed'
                          AND COALESCE(f.cleanup_at, '') = ''
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM upload_jobs AS j
                        WHERE j.source_session_id = s.session_id
                          AND j.status = 'queued'
                    )
              )
            ORDER BY s.updated_at ASC, s.created_at ASC
            """
        ).fetchall()
    finally:
        conn.close()

    return [clean_cell_value(row["session_id"]) for row in rows]


def _record_attempt(
    *,
    session_id: str,
    source: str,
    action: str,
    now: str,
) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE edit_sessions
            SET recovery_attempts = COALESCE(recovery_attempts, 0) + 1,
                last_recovery_at = ?,
                last_recovery_source = ?,
                last_recovery_action = ?,
                last_recovery_error = '',
                updated_at = ?
            WHERE session_id = ?
            """,
            (
                now,
                clean_cell_value(source),
                clean_cell_value(action),
                now,
                clean_cell_value(session_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _record_success(
    *,
    session_id: str,
    source: str,
    action: str,
    now: str,
) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE edit_sessions
            SET last_recovery_at = ?,
                last_recovery_source = ?,
                last_recovery_action = ?,
                last_recovery_error = '',
                updated_at = ?
            WHERE session_id = ?
            """,
            (
                now,
                clean_cell_value(source),
                clean_cell_value(action),
                now,
                clean_cell_value(session_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _record_transition_failure(
    *,
    session_id: str,
    source: str,
    action: str,
    error: Exception,
    now: str,
    expires_at: str,
) -> None:
    message = f"{action} failed during {source}: {error}"
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE edit_sessions
            SET status = 'error',
                error = ?,
                expires_at = ?,
                last_recovery_at = ?,
                last_recovery_source = ?,
                last_recovery_action = ?,
                last_recovery_error = ?,
                updated_at = ?
            WHERE session_id = ?
              AND status NOT IN ('committed', 'rolled_back')
            """,
            (
                message,
                expires_at,
                now,
                clean_cell_value(source),
                clean_cell_value(action),
                str(error),
                now,
                clean_cell_value(session_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _convert_nonexplicit_rollback_to_commit(
    *,
    session_id: str,
    now: str,
) -> None:
    """Stop an old automatic rollback before it can delete more data."""
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE edit_sessions
            SET status = 'committing',
                close_reason = 'automatic_recovery_autosave',
                commit_started_at = CASE
                    WHEN COALESCE(commit_started_at, '') = '' THEN ?
                    ELSE commit_started_at
                END,
                rollback_started_at = '',
                expired_at = '',
                error = '',
                updated_at = ?
            WHERE session_id = ?
              AND status IN ('rolling_back', 'error')
              AND COALESCE(close_reason, '') <> 'cancel_button'
            """,
            (
                now,
                now,
                clean_cell_value(session_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _record_terminal_cleanup_result(
    *,
    session_id: str,
    source: str,
    action: str,
    errors: list[dict[str, Any]],
    now: str,
) -> None:
    error_text = "; ".join(
        clean_cell_value(item.get("error"))
        for item in errors
        if clean_cell_value(item.get("error"))
    )

    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE edit_sessions
            SET recovery_attempts = COALESCE(recovery_attempts, 0) + 1,
                last_recovery_at = ?,
                last_recovery_source = ?,
                last_recovery_action = ?,
                last_recovery_error = ?,
                updated_at = ?
            WHERE session_id = ?
              AND status = 'committed'
            """,
            (
                now,
                clean_cell_value(source),
                clean_cell_value(action),
                error_text,
                now,
                clean_cell_value(session_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _resume_transition_status(
    *,
    session_id: str,
    target_status: str,
    now: str,
) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE edit_sessions
            SET status = ?,
                error = '',
                updated_at = ?
            WHERE session_id = ?
              AND status = 'error'
            """,
            (
                clean_cell_value(target_status),
                now,
                clean_cell_value(session_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def recover_edit_session_lifecycle(
    *,
    source: str = "startup",
) -> dict:
    # Import inside the function to avoid a module-level cycle:
    # edit_sessions -> edit_session_recovery -> edit_sessions.
    from app.checklists.edit_sessions import (
        commit_edit_session,
        complete_edit_session_commit,
        complete_edit_session_rollback,
        ensure_edit_session_tables,
        get_edit_session,
        public_edit_session_payload,
        sweep_expired_edit_sessions,
        utc_after_seconds_iso,
        utc_now_iso,
    )

    ensure_edit_session_tables()

    normalized_source = clean_cell_value(source) or "startup"
    file_recovery = recover_edit_session_file_entries(
        source=normalized_source
    )

    completed_commits: list[dict] = []
    completed_rollbacks: list[dict] = []
    transition_errors: list[dict] = []

    candidates = _recovery_candidates()

    for candidate in candidates:
        session_id = clean_cell_value(candidate.get("session_id"))
        rollback_started = clean_cell_value(
            candidate.get("rollback_started_at")
        )
        rollback_reason = clean_cell_value(
            candidate.get("close_reason")
        )
        explicit_cancel = bool(
            rollback_reason == "cancel_button"
            and (
                rollback_started
                or candidate.get("status") == "rolling_back"
            )
        )
        action = (
            "resume_rollback"
            if explicit_cancel
            else "resume_commit"
        )
        target_status = (
            "rolling_back"
            if action == "resume_rollback"
            else "committing"
        )
        now = utc_now_iso()

        _record_attempt(
            session_id=session_id,
            source=normalized_source,
            action=action,
            now=now,
        )

        try:
            if (
                action == "resume_commit"
                and (
                    candidate.get("status") == "rolling_back"
                    or rollback_started
                )
            ):
                _convert_nonexplicit_rollback_to_commit(
                    session_id=session_id,
                    now=now,
                )
            else:
                _resume_transition_status(
                    session_id=session_id,
                    target_status=target_status,
                    now=now,
                )

            if action == "resume_rollback":
                result = complete_edit_session_rollback(
                    session_id
                )
                completed_rollbacks.append(
                    public_edit_session_payload(result)
                )
            else:
                result = complete_edit_session_commit(
                    session_id
                )
                completed_commits.append(
                    public_edit_session_payload(result)
                )

            _record_success(
                session_id=session_id,
                source=normalized_source,
                action=action,
                now=utc_now_iso(),
            )

        except Exception as exc:
            _record_transition_failure(
                session_id=session_id,
                source=normalized_source,
                action=action,
                error=exc,
                now=utc_now_iso(),
                expires_at=utc_after_seconds_iso(
                    EDIT_SESSION_TTL_SECONDS
                ),
            )
            transition_errors.append({
                "sessionId": session_id,
                "action": action,
                "error": str(exc),
            })
            write_debug_log(
                "edit_session_transition_recovery_failed",
                transition_errors[-1],
            )

    restart_commit_results: list[dict] = []
    restart_commit_errors: list[dict] = []

    if normalized_source == "startup":
        for candidate in _restart_active_session_candidates():
            session_id = clean_cell_value(
                candidate.get("session_id")
            )

            if not session_id:
                continue

            try:
                result = commit_edit_session(
                    session_id,
                    reason="application_restart_autosave",
                )
                public_result = public_edit_session_payload(result)
                restart_commit_results.append(public_result)
                completed_commits.append(public_result)
            except Exception as exc:
                error = {
                    "sessionId": session_id,
                    "action": "commit_active_after_restart",
                    "error": str(exc),
                }
                restart_commit_errors.append(error)
                transition_errors.append(error)
                write_debug_log(
                    "edit_session_restart_autosave_failed",
                    error,
                )

    committed_cleanup_results: list[dict] = []
    committed_cleanup_errors: list[dict] = []

    for session_id in _committed_cleanup_candidates():
        cleanup = purge_committed_session_files(session_id)
        enqueue = enqueue_committed_edit_session_yandex_jobs(
            session_id,
            source=f"{normalized_source}_committed_recovery",
        )

        errors: list[dict[str, Any]] = []
        errors.extend(cleanup.get("errors") or [])
        errors.extend(enqueue.get("errors") or [])

        _record_terminal_cleanup_result(
            session_id=session_id,
            source=normalized_source,
            action="recover_committed_side_effects",
            errors=errors,
            now=utc_now_iso(),
        )

        result = {
            "sessionId": session_id,
            "cleanup": cleanup,
            "enqueue": enqueue,
            "ok": not errors,
        }
        committed_cleanup_results.append(result)

        if errors:
            committed_cleanup_errors.append({
                "sessionId": session_id,
                "errors": errors,
            })

    expired_result = sweep_expired_edit_sessions(
        source=f"{normalized_source}_recovery",
        limit=1000,
    )

    result = {
        "ok": (
            not transition_errors
            and not committed_cleanup_errors
            and file_recovery.get("ok") is not False
            and expired_result.get("ok") is not False
        ),
        "source": normalized_source,
        "candidateCount": len(candidates),
        "completedCommitCount": len(completed_commits),
        "completedRollbackCount": len(completed_rollbacks),
        "transitionErrorCount": len(transition_errors),
        "restartCommitCount": len(
            restart_commit_results
        ),
        "restartCommitErrorCount": len(
            restart_commit_errors
        ),
        "restartRollbackCount": 0,
        "restartRollbackErrorCount": 0,
        "committedCleanupCount": len(
            committed_cleanup_results
        ),
        "committedCleanupErrorCount": len(
            committed_cleanup_errors
        ),
        "fileRecovery": file_recovery,
        "expiredResult": expired_result,
        "completedCommits": completed_commits,
        "completedRollbacks": completed_rollbacks,
        "transitionErrors": transition_errors,
        "restartCommitResults": restart_commit_results,
        "restartCommitErrors": restart_commit_errors,
        "restartRollbackResults": [],
        "restartRollbackErrors": [],
        "committedCleanupResults": committed_cleanup_results,
        "committedCleanupErrors": committed_cleanup_errors,
    }

    write_debug_log(
        "edit_session_lifecycle_recovery_finished",
        result,
    )

    return result
