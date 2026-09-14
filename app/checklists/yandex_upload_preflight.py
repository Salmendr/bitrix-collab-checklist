"""Folder existence checks and explicit recovery intent for mirror uploads."""
import json
from app.checklists.utils import clean_cell_value
from app.checklists.yandex_scope import canonical_path, project_root, require_project_path, item_folder, YandexScopeError
from app.logging_utils import write_debug_log


def is_manual_recovery(source):
    return clean_cell_value(source).startswith('manual_')


def requires_manual_recovery(item, jobs=()):
    return (clean_cell_value(item.get('yandexStructureStatus')).lower() in {'error', 'conflict'}
            or any(clean_cell_value(d.get('mirrorStatus')).lower() in {'error', 'conflict'}
                   for d in item.get('documents', []))
            or any(clean_cell_value(j.get('status')).lower() in {'error', 'conflict'} for j in jobs))


def require_exclusive_item_folder(dialog_id, checklist_key, item, items, context=None):
    """Different item IDs must never silently adopt the same bound directory."""
    target = item_folder(dialog_id, checklist_key, item, context)
    for other in items:
        if clean_cell_value(other.get('id')) == clean_cell_value(item.get('id')):
            continue
        try:
            other_path = item_folder(dialog_id, checklist_key, other, context)
        except YandexScopeError:
            continue
        if other_path == target:
            raise YandexScopeError('Конфликт привязки: одна папка Яндекса назначена нескольким пунктам. '
                                   'Проверьте привязки пунктов и повторите синхронизацию вручную.')
    return target


def ensure_upload_folder(dialog_id, folder_path):
    """Create only confirmed-missing directories below an existing project root."""
    from app.yandex_disk.client import yandex_disk_try_get_resource_meta, yandex_disk_ensure_folder
    target = require_project_path(dialog_id, folder_path)
    root = project_root(dialog_id)

    def directory(path, *, required=False):
        meta = yandex_disk_try_get_resource_meta(path)  # HTTP/network errors must propagate.
        if meta is None:
            if required:
                if path == root:
                    raise YandexScopeError('Папка объекта не найдена на Яндексе. Проверьте привязку проекта; '
                                           'создание нового корня объекта остановлено.')
                raise YandexScopeError('Не удалось подтвердить создание папки Яндекса. '
                                       'Повторите синхронизацию вручную.')
            return None
        if meta.get('type') != 'dir' or canonical_path(meta.get('path') or '') != path:
            raise YandexScopeError('Конфликт пути Яндекса: ожидалась папка по заданному адресу. '
                                   'Автоматическая замена ресурса остановлена.')
        return meta

    meta = directory(target)
    if meta is not None:
        return {'meta': meta, 'created': []}
    directory(root, required=True)
    created = []
    path = root
    for part in target[len(root)+1:].split('/'):
        path += '/' + part
        meta = directory(path)
        if meta is None:
            write_debug_log('yandex_upload_folder_create_started', {'dialogId': dialog_id, 'folderPath': path})
            yandex_disk_ensure_folder(path)
            meta = directory(path, required=True)
            created.append(path)
    write_debug_log('yandex_upload_folder_verified', {
        'dialogId': dialog_id, 'folderPath': target, 'createdPaths': created,
    })
    return {'meta': meta, 'created': created}


def mark_manual_structure_continuation(job_id):
    """Persist one manual request on a queued structure job, before dispatch."""
    from app.db import get_conn
    conn = get_conn()
    try:
        conn.execute('BEGIN IMMEDIATE')
        row = conn.execute('SELECT status,result_json FROM yandex_structure_jobs WHERE job_id=?', (job_id,)).fetchone()
        if row and row['status'] == 'queued':
            result = json.loads(row['result_json'] or '{}')
            result['manualFileRecovery'] = True
            conn.execute('UPDATE yandex_structure_jobs SET result_json=? WHERE job_id=?',
                         (json.dumps(result, ensure_ascii=False), job_id))
        conn.commit()
    finally:
        conn.close()
