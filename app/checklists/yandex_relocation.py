"""Rebase runtime file addresses after a confirmed item-folder relocation."""
import json

from app.checklists.utils import clean_cell_value
from app.checklists.yandex_scope import canonical_path, require_project_path
from app.yandex_disk.client import yandex_disk_client_url

PATH_FIELDS = frozenset({'yandexPath', 'originalYandexPath', 'oldYandexPath', 'newYandexPath'})


def _rebase(value, source, target):
    if not value:
        return value
    path = canonical_path(value)
    if path.startswith(source + '/'):
        return target + path[len(source):]
    return value


def _rebase_json(value, source, target):
    changed = 0
    if isinstance(value, list):
        for child in value:
            changed += _rebase_json(child, source, target)
    elif isinstance(value, dict):
        for key, child in list(value.items()):
            if key in PATH_FIELDS and isinstance(child, str):
                new_path = _rebase(child, source, target)
                if new_path != child:
                    value[key] = new_path
                    changed += 1
                    if key == 'yandexPath' and value.get('yandexFileUrl'):
                        value['yandexFileUrl'] = yandex_disk_client_url(new_path)
            elif isinstance(child, (dict, list)):
                changed += _rebase_json(child, source, target)
    return changed


def rebase_item_file_paths_in_transaction(conn, *, dialog_id, checklist_key, item, job):
    """Caller holds the project lock and the checklist's SQLite transaction.

    Change only addresses below the successfully moved source directory, only
    for this stable item identity. Upload authors, dates and local paths stay
    intact. No uploads/deletes are scheduled here.
    """
    if (job.get('status') != 'completed'
            or job.get('action') not in {'move_item_folder', 'rename_item_folder'}):
        return 0
    result = job.get('result') or {}
    source = require_project_path(dialog_id, result.get('sourcePath') or job.get('source_path'))
    target = require_project_path(dialog_id, result.get('folderPath') or job.get('target_path'))
    if source == target:
        return 0
    identity = (dialog_id, checklist_key, clean_cell_value(item.get('id')))
    count = _rebase_json(item, source, target)
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'document_replacements' in tables:
        for row in conn.execute('SELECT operation_id,old_yandex_path,new_yandex_path FROM document_replacements '
                                'WHERE dialog_id=? AND checklist_key=? AND item_id=?', identity).fetchall():
            old = _rebase(row['old_yandex_path'], source, target)
            new = _rebase(row['new_yandex_path'], source, target)
            if (old, new) != (row['old_yandex_path'], row['new_yandex_path']):
                conn.execute('UPDATE document_replacements SET old_yandex_path=?,new_yandex_path=? WHERE operation_id=?',
                             (old, new, row['operation_id']))
                count += 1
    if 'upload_jobs' in tables:
        for row in conn.execute('SELECT job_id,yandex_path FROM upload_jobs '
                                'WHERE dialog_id=? AND checklist_key=? AND item_id=?', identity).fetchall():
            path = _rebase(row['yandex_path'], source, target)
            if path != row['yandex_path']:
                conn.execute('UPDATE upload_jobs SET yandex_path=? WHERE job_id=?', (path, row['job_id']))
                count += 1
    # A still-open edit session may not have materialized its replacement job
    # yet. Rebase its deferred action too, so commit cannot restore a stale URL.
    if 'edit_session_operations' in tables:
        for row in conn.execute("SELECT operation_id,payload_json,before_json,after_json FROM edit_session_operations "
                                "WHERE dialog_id=? AND checklist_key=? AND item_id=? "
                                "AND operation_type IN ('document_replace','document_remove','documents_clear') "
                                "AND status NOT IN ('rolled_back','cancelled')", identity).fetchall():
            for field in ('payload_json', 'before_json', 'after_json'):
                value = json.loads(row[field] or '{}')
                changes = _rebase_json(value, source, target)
                if changes:
                    conn.execute(f'UPDATE edit_session_operations SET {field}=? WHERE operation_id=?',
                                 (json.dumps(value, ensure_ascii=False), row['operation_id']))
                    count += changes
    return count
