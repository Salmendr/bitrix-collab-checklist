import sqlite3

from app.settings import DB_PATH


def get_conn():
    conn = sqlite3.connect(
        DB_PATH,
        timeout=30.0,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS checklists (
            dialog_id TEXT PRIMARY KEY,
            title TEXT,
            data_json TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS project_storage_contexts (
            dialog_id TEXT PRIMARY KEY,
            project_id TEXT,
            project_name TEXT,
            provider TEXT,
            storage_mode_json TEXT,
            yandex_json TEXT,
            item_mappings_json TEXT,
            bitrix_json TEXT,
            updated_at TEXT
        )
    """)

    project_context_columns = {
        row["name"]
        for row in cur.execute(
            "PRAGMA table_info(project_storage_contexts)"
        ).fetchall()
    }
    if "bitrix_json" not in project_context_columns:
        cur.execute(
            "ALTER TABLE project_storage_contexts "
            "ADD COLUMN bitrix_json TEXT"
        )

    cur.execute("""
        CREATE TABLE IF NOT EXISTS upload_jobs (
            job_id TEXT PRIMARY KEY,
            job_type TEXT,
            dialog_id TEXT,
            checklist_key TEXT,
            item_id TEXT,
            document_id TEXT,
            local_path TEXT,
            file_name TEXT,
            file_size INTEGER DEFAULT 0,
            yandex_path TEXT,
            status TEXT,
            stage TEXT,
            progress_percent INTEGER DEFAULT 0,
            uploaded_bytes INTEGER DEFAULT 0,
            total_bytes INTEGER DEFAULT 0,
            error TEXT,
            attempts INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            started_at TEXT,
            finished_at TEXT,
            source_session_id TEXT,
            source_operation_id TEXT,
            source_action_key TEXT
        )
    """)

    upload_job_columns = {
        row["name"]
        for row in cur.execute(
            "PRAGMA table_info(upload_jobs)"
        ).fetchall()
    }

    for column_name in (
        "source_session_id",
        "source_operation_id",
        "source_action_key",
    ):
        if column_name not in upload_job_columns:
            cur.execute(
                "ALTER TABLE upload_jobs "
                f"ADD COLUMN {column_name} TEXT"
            )

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_upload_jobs_status
        ON upload_jobs(status, created_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_upload_jobs_document
        ON upload_jobs(dialog_id, checklist_key, item_id, document_id)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_upload_jobs_source_session
        ON upload_jobs(source_session_id, status, created_at)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_upload_jobs_source_action
        ON upload_jobs(source_action_key)
        WHERE COALESCE(source_action_key, '') <> ''
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS document_replacements (
            operation_id TEXT PRIMARY KEY,
            dialog_id TEXT,
            checklist_key TEXT,
            item_id TEXT,
            series_id TEXT,
            archive_version_id TEXT,
            old_document_id TEXT,
            new_document_id TEXT,
            new_upload_job_id TEXT,
            old_file_name TEXT,
            new_file_name TEXT,
            old_yandex_path TEXT,
            new_yandex_path TEXT,
            delete_job_id TEXT,
            status TEXT,
            stage TEXT,
            error TEXT,
            created_at TEXT,
            updated_at TEXT,
            finished_at TEXT,
            session_id TEXT,
            recipient_key TEXT,
            draft_ids_json TEXT,
            delivery_group_key TEXT
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_document_replacements_status
        ON document_replacements(status, created_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_document_replacements_upload_job
        ON document_replacements(new_upload_job_id)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_document_replacements_delete_job
        ON document_replacements(delete_job_id)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS edit_sessions (
            session_id TEXT PRIMARY KEY,
            dialog_id TEXT NOT NULL,
            user_id TEXT,
            user_name TEXT,
            client_session_id TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            close_reason TEXT,
            metadata_json TEXT,
            started_at TEXT,
            heartbeat_at TEXT,
            expires_at TEXT,
            last_activity_at TEXT,
            idle_expires_at TEXT,
            commit_started_at TEXT,
            committed_at TEXT,
            rollback_started_at TEXT,
            rolled_back_at TEXT,
            expired_at TEXT,
            error TEXT,
            recovery_attempts INTEGER DEFAULT 0,
            last_recovery_at TEXT,
            last_recovery_source TEXT,
            last_recovery_action TEXT,
            last_recovery_error TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    edit_session_columns = {
        row["name"]
        for row in cur.execute(
            "PRAGMA table_info(edit_sessions)"
        ).fetchall()
    }

    edit_session_recovery_columns = {
        "last_activity_at": "TEXT",
        "idle_expires_at": "TEXT",
        "recovery_attempts": "INTEGER DEFAULT 0",
        "last_recovery_at": "TEXT",
        "last_recovery_source": "TEXT",
        "last_recovery_action": "TEXT",
        "last_recovery_error": "TEXT",
    }

    for column_name, column_sql in (
        edit_session_recovery_columns.items()
    ):
        if column_name not in edit_session_columns:
            cur.execute(
                "ALTER TABLE edit_sessions "
                f"ADD COLUMN {column_name} {column_sql}"
            )

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_sessions_status_expiry
        ON edit_sessions(status, expires_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_sessions_status_idle_expiry
        ON edit_sessions(status, idle_expires_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_sessions_dialog_status
        ON edit_sessions(dialog_id, status, updated_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_sessions_client
        ON edit_sessions(
            dialog_id,
            client_session_id,
            user_id,
            status
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS edit_session_checklists (
            session_id TEXT NOT NULL,
            dialog_id TEXT NOT NULL,
            checklist_key TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            lock_id TEXT,
            snapshot_id TEXT,
            started_at TEXT,
            updated_at TEXT,
            lock_acquired_at TEXT,
            lock_heartbeat_at TEXT,
            lock_released_at TEXT,
            lock_release_reason TEXT,
            PRIMARY KEY (
                session_id,
                dialog_id,
                checklist_key
            )
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_session_checklists_session
        ON edit_session_checklists(session_id, status)
    """)

    checklist_lock_columns = {
        row["name"]
        for row in cur.execute(
            "PRAGMA table_info(edit_session_checklists)"
        ).fetchall()
    }

    for column_name, declaration in {
        "lock_acquired_at": "TEXT",
        "lock_heartbeat_at": "TEXT",
        "lock_released_at": "TEXT",
        "lock_release_reason": "TEXT",
        "last_state_hash": "TEXT",
        "last_operation_id": "TEXT",
        "mutation_count": "INTEGER DEFAULT 0",
        "rollback_restored_at": "TEXT",
        "commit_finalized_at": "TEXT",
    }.items():
        if column_name not in checklist_lock_columns:
            cur.execute(
                "ALTER TABLE edit_session_checklists "
                f"ADD COLUMN {column_name} {declaration}"
            )

    cur.execute("""
        CREATE INDEX IF NOT EXISTS
            idx_edit_session_checklists_owner
        ON edit_session_checklists(
            dialog_id,
            checklist_key,
            status,
            lock_id
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            idx_edit_session_checklists_active_lock
        ON edit_session_checklists(
            dialog_id,
            checklist_key
        )
        WHERE status = 'locked'
          AND COALESCE(lock_id, '') <> ''
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS edit_session_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            dialog_id TEXT NOT NULL,
            checklist_key TEXT NOT NULL,
            snapshot_json TEXT NOT NULL,
            snapshot_hash TEXT,
            created_at TEXT,
            UNIQUE (
                session_id,
                dialog_id,
                checklist_key
            )
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_session_snapshots_session
        ON edit_session_snapshots(session_id, created_at)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS edit_session_operations (
            operation_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            sequence_no INTEGER NOT NULL,
            operation_type TEXT NOT NULL,
            dialog_id TEXT,
            checklist_key TEXT,
            item_id TEXT,
            series_id TEXT,
            document_id TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            before_json TEXT,
            after_json TEXT,
            payload_json TEXT,
            error TEXT,
            created_at TEXT,
            updated_at TEXT,
            committed_at TEXT,
            rolled_back_at TEXT
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_edit_session_operations_seq
        ON edit_session_operations(session_id, sequence_no)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_session_operations_status
        ON edit_session_operations(session_id, status, sequence_no)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_edit_session_operations_type
        ON edit_session_operations(
            session_id,
            operation_type,
            status,
            sequence_no
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS edit_session_file_entries (
            entry_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            sequence_no INTEGER NOT NULL,
            operation_id TEXT,
            operation_type TEXT,
            dialog_id TEXT,
            checklist_key TEXT,
            item_id TEXT,
            series_id TEXT,
            document_id TEXT,
            entry_kind TEXT NOT NULL,
            original_path TEXT NOT NULL,
            staged_path TEXT,
            file_name TEXT,
            file_size INTEGER DEFAULT 0,
            sha256 TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            metadata_json TEXT,
            error TEXT,
            created_at TEXT,
            updated_at TEXT,
            committed_at TEXT,
            rolled_back_at TEXT,
            cleanup_at TEXT
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            idx_edit_session_file_entries_seq
        ON edit_session_file_entries(session_id, sequence_no)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS
            idx_edit_session_file_entries_status
        ON edit_session_file_entries(session_id, status, sequence_no)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS
            idx_edit_session_file_entries_operation
        ON edit_session_file_entries(operation_id, entry_kind, status)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS edit_session_finalizations (
            session_id TEXT PRIMARY KEY,
            dialog_id TEXT NOT NULL,
            user_id TEXT,
            payload_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'preparing',
            attempt_id TEXT,
            saved_count INTEGER DEFAULT 0,
            message_status TEXT NOT NULL DEFAULT 'pending',
            message_attempts INTEGER DEFAULT 0,
            message_error TEXT,
            message_result_json TEXT,
            response_json TEXT,
            payload_json TEXT,
            delivery_status TEXT NOT NULL DEFAULT 'pending',
            notification_status TEXT NOT NULL DEFAULT 'pending',
            delivery_error TEXT,
            delivery_claimed_at TEXT,
            error TEXT,
            started_at TEXT,
            local_saved_at TEXT,
            committed_at TEXT,
            delivery_started_at TEXT,
            completed_at TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS
            idx_edit_session_finalizations_status
        ON edit_session_finalizations(status, updated_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS
            idx_edit_session_finalizations_message
        ON edit_session_finalizations(message_status, updated_at)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS yandex_structure_jobs (
            job_id TEXT PRIMARY KEY,
            session_id TEXT,
            operation_id TEXT,
            dialog_id TEXT NOT NULL,
            checklist_key TEXT NOT NULL,
            item_id TEXT NOT NULL,
            action TEXT NOT NULL,
            source_path TEXT,
            target_path TEXT,
            folder_alias TEXT,
            item_name TEXT,
            group_id INTEGER DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'queued',
            attempts INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 5,
            error TEXT,
            result_json TEXT,
            idempotency_key TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT,
            started_at TEXT,
            finished_at TEXT
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            idx_yandex_structure_jobs_idempotency
        ON yandex_structure_jobs(idempotency_key)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS
            idx_yandex_structure_jobs_status
        ON yandex_structure_jobs(status, created_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS
            idx_yandex_structure_jobs_item
        ON yandex_structure_jobs(
            dialog_id, checklist_key, item_id, created_at
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS
            idx_yandex_structure_jobs_session
        ON yandex_structure_jobs(session_id, status, created_at)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS checklist_item_order (
            dialog_id TEXT NOT NULL,
            checklist_key TEXT NOT NULL,
            item_id TEXT NOT NULL,
            group_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            order_version INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT,
            PRIMARY KEY (
                dialog_id,
                checklist_key,
                item_id
            )
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_checklist_item_order_group
        ON checklist_item_order(
            dialog_id,
            checklist_key,
            group_id,
            position
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_checklist_item_order_version
        ON checklist_item_order(
            dialog_id,
            checklist_key,
            order_version
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS notification_drafts (
            draft_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            dialog_id TEXT NOT NULL,
            checklist_key TEXT NOT NULL,
            item_id TEXT NOT NULL,
            item_name TEXT,
            group_id INTEGER DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'draft',
            version INTEGER NOT NULL DEFAULT 1,
            sender_user_id TEXT,
            sender_name TEXT,
            assignment_part_id TEXT,
            assignment_part_text TEXT,
            deadline_date TEXT,
            description TEXT,
            metadata_json TEXT,
            created_by_id TEXT,
            created_by_name TEXT,
            created_at TEXT,
            updated_at TEXT,
            committed_at TEXT,
            cancelled_at TEXT
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_notification_drafts_session
        ON notification_drafts(session_id, status, created_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_notification_drafts_item
        ON notification_drafts(
            dialog_id, checklist_key, item_id, status, created_at
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS notification_files (
            notification_file_id TEXT PRIMARY KEY,
            draft_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            dialog_id TEXT NOT NULL,
            checklist_key TEXT NOT NULL,
            item_id TEXT NOT NULL,
            series_id TEXT,
            document_id TEXT NOT NULL,
            file_name TEXT,
            file_size INTEGER DEFAULT 0,
            file_url TEXT,
            sort_order INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_files_unique
        ON notification_files(draft_id, document_id)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_notification_files_series
        ON notification_files(series_id, document_id, created_at)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS notification_recipients (
            recipient_id TEXT PRIMARY KEY,
            draft_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            recipient_type TEXT NOT NULL,
            bitrix_user_id TEXT,
            display_name TEXT,
            phone TEXT,
            email TEXT,
            contact_details TEXT,
            company_id TEXT,
            curator_user_id TEXT,
            curator_name TEXT,
            metadata_json TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_recipients_draft
        ON notification_recipients(draft_id)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_notification_recipients_identity
        ON notification_recipients(
            recipient_type, bitrix_user_id, company_id, created_at
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS notification_delivery_attempts (
            attempt_id TEXT PRIMARY KEY,
            draft_id TEXT NOT NULL,
            delivery_type TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            external_id TEXT,
            external_url TEXT,
            request_json TEXT,
            response_json TEXT,
            error TEXT,
            created_at TEXT,
            updated_at TEXT,
            started_at TEXT,
            finished_at TEXT
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_delivery_idempotency
        ON notification_delivery_attempts(idempotency_key)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_notification_delivery_status
        ON notification_delivery_attempts(status, delivery_type, created_at)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS document_assignment_history (
            history_id TEXT PRIMARY KEY,
            draft_id TEXT,
            recipient_id TEXT,
            session_id TEXT,
            dialog_id TEXT NOT NULL,
            checklist_key TEXT NOT NULL,
            item_id TEXT NOT NULL,
            series_id TEXT NOT NULL,
            document_id TEXT NOT NULL,
            version_name TEXT,
            sender_user_id TEXT,
            sender_name TEXT,
            recipient_type TEXT,
            recipient_user_id TEXT,
            recipient_name TEXT,
            assignment_part_id TEXT,
            assignment_part_text TEXT,
            deadline_date TEXT,
            bitrix_task_id TEXT,
            bitrix_task_url TEXT,
            chat_status TEXT,
            task_status TEXT,
            crm_sync_status TEXT,
            created_at TEXT,
            updated_at TEXT,
            chat_message_id TEXT,
            delivery_attempt_id TEXT
        )
    """)

    notification_delivery_columns = {
        row["name"]
        for row in cur.execute(
            "PRAGMA table_info(notification_delivery_attempts)"
        ).fetchall()
    }
    for column_name, column_type in {
        "session_id": "TEXT",
        "recipient_key": "TEXT",
        "draft_ids_json": "TEXT",
        "delivery_group_key": "TEXT",
    }.items():
        if column_name not in notification_delivery_columns:
            cur.execute(
                f"ALTER TABLE notification_delivery_attempts "
                f"ADD COLUMN {column_name} {column_type}"
            )

    assignment_history_columns = {
        row["name"]
        for row in cur.execute(
            "PRAGMA table_info(document_assignment_history)"
        ).fetchall()
    }
    for column_name, column_type in {
        "chat_message_id": "TEXT",
        "delivery_attempt_id": "TEXT",
    }.items():
        if column_name not in assignment_history_columns:
            cur.execute(
                f"ALTER TABLE document_assignment_history "
                f"ADD COLUMN {column_name} {column_type}"
            )

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_notification_delivery_session
        ON notification_delivery_attempts(
            session_id, status, delivery_type, created_at
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_notification_delivery_recipient
        ON notification_delivery_attempts(
            session_id, recipient_key, delivery_type, created_at
        )
    """)

    cur.execute("""
        DELETE FROM document_assignment_history
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM document_assignment_history
            WHERE COALESCE(draft_id, '') <> ''
              AND COALESCE(document_id, '') <> ''
            GROUP BY draft_id, document_id
        )
          AND COALESCE(draft_id, '') <> ''
          AND COALESCE(document_id, '') <> ''
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_assignment_history_version
        ON document_assignment_history(draft_id, document_id)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_document_assignment_history_series
        ON document_assignment_history(series_id, created_at DESC)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_document_assignment_history_draft
        ON document_assignment_history(draft_id, created_at)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bitrix_users_cache (
            user_id TEXT PRIMARY KEY,
            name TEXT,
            first_name TEXT,
            last_name TEXT,
            active INTEGER DEFAULT 1,
            email TEXT,
            raw_json TEXT,
            fetched_at TEXT,
            updated_at TEXT
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_bitrix_users_cache_active
        ON bitrix_users_cache(active, name)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bitrix_companies_cache (
            company_id TEXT PRIMARY KEY,
            title TEXT,
            normalized_title TEXT,
            company_type TEXT,
            phone TEXT,
            normalized_phone TEXT,
            email TEXT,
            normalized_email TEXT,
            contact_details TEXT,
            source TEXT DEFAULT 'bitrix',
            sync_status TEXT DEFAULT 'synced',
            sync_error TEXT,
            usage_count INTEGER DEFAULT 0,
            last_used_at TEXT,
            active INTEGER DEFAULT 1,
            raw_json TEXT,
            fetched_at TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    bitrix_company_columns = {
        row["name"]
        for row in cur.execute(
            "PRAGMA table_info(bitrix_companies_cache)"
        ).fetchall()
    }
    bitrix_company_column_defs = {
        "normalized_title": "TEXT",
        "normalized_phone": "TEXT",
        "normalized_email": "TEXT",
        "contact_details": "TEXT",
        "source": "TEXT DEFAULT 'bitrix'",
        "sync_status": "TEXT DEFAULT 'synced'",
        "sync_error": "TEXT",
        "usage_count": "INTEGER DEFAULT 0",
        "last_used_at": "TEXT",
        "active": "INTEGER DEFAULT 1",
        "created_at": "TEXT",
    }
    for column_name, column_sql in bitrix_company_column_defs.items():
        if column_name not in bitrix_company_columns:
            cur.execute(
                "ALTER TABLE bitrix_companies_cache "
                f"ADD COLUMN {column_name} {column_sql}"
            )

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_bitrix_companies_cache_supplier
        ON bitrix_companies_cache(company_type, title)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_bitrix_companies_cache_phone
        ON bitrix_companies_cache(company_type, normalized_phone, active)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_bitrix_companies_cache_email
        ON bitrix_companies_cache(company_type, normalized_email, active)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_bitrix_companies_cache_title
        ON bitrix_companies_cache(company_type, normalized_title, active)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS assignment_parts (
            assignment_part_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            usage_count INTEGER NOT NULL DEFAULT 0,
            created_by_id TEXT,
            created_by_name TEXT,
            created_at TEXT,
            updated_at TEXT,
            last_used_at TEXT
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_assignment_parts_normalized_name
        ON assignment_parts(normalized_name)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_assignment_parts_active_usage
        ON assignment_parts(active, last_used_at DESC, usage_count DESC, name)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_permissions (
            user_id TEXT PRIMARY KEY,
            user_name TEXT,
            can_access_checklists INTEGER DEFAULT 1,
            can_delete_files INTEGER DEFAULT 0,
            source TEXT,
            updated_at TEXT
        )
    """)

    permission_rows_count = cur.execute(
        "SELECT COUNT(*) AS cnt FROM user_permissions"
    ).fetchone()["cnt"]

    if int(permission_rows_count or 0) == 0:
        default_delete_user_ids = [
            18,
            26,
            56,
            72,
            100,
            106,
            108,
            114,
            116,
            124,
            138,
            140,
            222,
            256,
            280,
        ]

        for user_id in default_delete_user_ids:
            cur.execute("""
                INSERT OR IGNORE INTO user_permissions(
                    user_id,
                    user_name,
                    can_access_checklists,
                    can_delete_files,
                    source,
                    updated_at
                )
                VALUES (?, ?, 1, 1, 'seed', datetime('now'))
            """, (
                str(user_id),
                "",
            ))

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_folder_links (
            link_id TEXT PRIMARY KEY,
            dialog_id TEXT NOT NULL,
            checklist_key TEXT NOT NULL,
            item_id TEXT NOT NULL,
            generation INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'active',
            created_by_id TEXT,
            created_by_name TEXT,
            last_reissued_by_id TEXT,
            last_reissued_by_name TEXT,
            created_at TEXT,
            updated_at TEXT,
            revoked_at TEXT,
            UNIQUE(dialog_id, checklist_key, item_id)
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_public_folder_links_status
        ON public_folder_links(status, dialog_id, checklist_key, item_id)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_folder_operations (
            operation_id TEXT PRIMARY KEY,
            link_id TEXT NOT NULL,
            link_generation INTEGER NOT NULL,
            dialog_id TEXT NOT NULL,
            checklist_key TEXT NOT NULL,
            item_id TEXT NOT NULL,
            operation_type TEXT NOT NULL,
            expected_document_id TEXT,
            expected_series_id TEXT,
            document_id TEXT,
            replacement_operation_id TEXT,
            upload_job_id TEXT,
            original_file_name TEXT,
            file_name TEXT,
            staging_path TEXT NOT NULL,
            file_size INTEGER NOT NULL DEFAULT 0,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            uploader_name TEXT NOT NULL,
            force_replace INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'queued',
            stage TEXT NOT NULL DEFAULT 'accepted',
            error TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            started_at TEXT,
            finished_at TEXT,
            FOREIGN KEY(link_id) REFERENCES public_folder_links(link_id)
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_public_folder_operations_queue
        ON public_folder_operations(status, created_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_public_folder_operations_item
        ON public_folder_operations(
            dialog_id,
            checklist_key,
            item_id,
            status,
            created_at
        )
    """)


    conn.commit()
    conn.close()
