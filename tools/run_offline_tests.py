"""Run regression tests in a disposable database with all HTTP blocked."""
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
for key in ('BITRIX_TECH_WEBHOOK_URL', 'YANDEX_DISK_OAUTH_TOKEN', 'N8N_SHARED_TOKEN'):
    os.environ[key] = ''
import app.settings as settings
import requests

def blocked(*args, **kwargs):
    raise AssertionError('Unexpected HTTP request: use an integration mock')

with tempfile.TemporaryDirectory(prefix='checklist-tests-') as temp, patch.object(requests.sessions.Session, 'request', blocked):
    runtime = Path(temp)
    settings.BASE_DIR = runtime
    settings.VOLUME_DIR = runtime / 'Volume'
    settings.DB_DIR = settings.VOLUME_DIR / 'db'
    settings.DB_PATH = settings.DB_DIR / 'app.db'
    settings.UPLOAD_ROOT = settings.VOLUME_DIR / 'uploads'
    settings.CHECKLIST_UPLOAD_ROOT = settings.UPLOAD_ROOT / 'checklists'
    settings.EDIT_SESSION_FILE_ROOT = settings.VOLUME_DIR / 'edit_sessions'
    settings.PUBLIC_FOLDER_STAGING_ROOT = settings.VOLUME_DIR / 'public_folder_staging'
    settings.PUBLIC_FOLDER_SIGNING_KEY_PATH = settings.DB_DIR / 'public_folder_signing.key'
    settings.DEBUG_DIR = runtime / 'debug'
    settings.DEBUG_LOG_PATH = settings.DEBUG_DIR / 'close_popup.log'
    settings.ensure_runtime_directories()
    settings.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    settings.UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    settings.DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    from app.db import init_db
    init_db()
    from app.checklists.edit_sessions import ensure_edit_session_tables
    ensure_edit_session_tables()
    suite = unittest.defaultTestLoader.discover(str(ROOT / 'tests'), pattern=sys.argv[1] if len(sys.argv)>1 else 'test*.py')
    assert suite.countTestCases() > 0, "No tests discovered"
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
