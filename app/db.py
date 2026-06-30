import sqlite3

from app.settings import DB_PATH


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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
            updated_at TEXT
        )
    """)

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
            finished_at TEXT
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_upload_jobs_status
        ON upload_jobs(status, created_at)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_upload_jobs_document
        ON upload_jobs(dialog_id, checklist_key, item_id, document_id)
    """)

    conn.commit()
    conn.close()