"""Resolve replacement cleanup in the owning item's current directory only."""
import json
from pathlib import Path

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.checklists.storage import make_storage_dialog_id
from app.checklists.utils import clean_cell_value, normalize_dialog_id, normalize_checklist_key
from app.checklists.yandex_scope import canonical_path, require_project_path, YandexScopeError
from app.checklists.yandex_upload_preflight import require_exclusive_item_folder
from app.checklists.yandex_file_reconciliation import _remote_identity_result


class ReplacementCleanupConflict(YandexScopeError):
    pass


def archive_versions(item):
    for record in list(item.get('documents') or []) + list(item.get('archivedDocumentSeries') or []):
        for version in record.get('archiveVersions') or []:
            if isinstance(version, dict):
                yield version


def load_item(replacement):
    conn = get_conn()
    try:
        row = conn.execute('SELECT data_json FROM checklists WHERE dialog_id=?',
                           (make_storage_dialog_id(replacement['dialog_id'], replacement['checklist_key']),)).fetchone()
    finally:
        conn.close()
    data = json.loads(row['data_json']) if row else {}
    matches = [i for i in data.get('items', []) if i.get('id') == replacement.get('item_id')]
    if len(matches) != 1:
        raise ReplacementCleanupConflict('Не найден однозначный пункт для завершения замены.')
    return data, matches[0]


def _file_name(value):
    value = clean_cell_value(value)
    if not value or '/' in value or '\\' in value or value in {'.', '..'}:
        raise ReplacementCleanupConflict('Не удалось подтвердить имя файла для завершения замены.')
    return value


def _local_file(record):
    from app.checklists.documents import get_upload_file_path_from_url, UPLOAD_ROOT
    path = get_upload_file_path_from_url(record.get('fileUrl') or record.get('path') or '')
    if path is None or not Path(path).resolve().is_relative_to(Path(UPLOAD_ROOT).resolve()) or not Path(path).is_file():
        raise ReplacementCleanupConflict('Не найдена локальная копия для проверки замены. Удаление остановлено.')
    return Path(path)


def _verify_file(path, name, record, meta):
    if not meta or meta.get('type') != 'file' or canonical_path(meta.get('path') or '') != path:
        raise ReplacementCleanupConflict('Не подтверждён файл по актуальному адресу Яндекса: ' + path)
    matched, reason = _remote_identity_result(local_path=_local_file(record), expected_name=name,
                                             remote_meta=meta, hash_cache={})
    if not matched:
        raise ReplacementCleanupConflict('Содержимое файла Яндекса не совпадает с локальной версией '
                                         '(' + reason + '). Удаление остановлено: ' + path)


def path_used_by_current_document(dialog_id, path):
    """Protect every live reference in this project, including other checklists."""
    # Storage IDs are exact project ID or project ID + checklist suffix.
    conn = get_conn()
    try:
        rows = conn.execute('SELECT dialog_id,data_json FROM checklists').fetchall()
    finally:
        conn.close()
    dialog_id = normalize_dialog_id(dialog_id)
    for row in rows:
        storage_id = row['dialog_id']
        if storage_id != dialog_id and not storage_id.startswith(dialog_id + '::'):
            continue
        data = json.loads(row['data_json'] or '{}')
        for item in data.get('items', []):
            for document in item.get('documents', []):
                stored = document.get('yandexPath') or ''
                if stored and canonical_path(stored) == path:
                    return True
    return False


def resolve_replacement_delete(replacement, probe):
    """Return a verified delete target or a reason for doing nothing.

    A missing stale URL is never accepted as completion. Probe the exact current
    folder, verify the live successor first, then compare the old remote file
    against its immutable local archive. Never search other items/projects.
    """
    dialog_id = normalize_dialog_id(replacement.get('dialog_id'))
    require_project_path(dialog_id, replacement.get('old_yandex_path') or '')
    data, item = load_item(replacement)
    folder = require_exclusive_item_folder(dialog_id, replacement['checklist_key'], item, data.get('items', []))
    folder_meta = probe(folder)
    if (not folder_meta or folder_meta.get('type') != 'dir'
            or canonical_path(folder_meta.get('path') or '') != folder):
        raise ReplacementCleanupConflict('Актуальная папка пункта не подтверждена. Удаление старой версии остановлено.')
    documents = item.get('documents') or []
    successors = [d for d in documents if d.get('id') == replacement.get('new_document_id')]
    if not successors:
        # Several replacements may finish out of order. The surviving current
        # version of the same series must be present and verified on Yandex.
        series = replacement.get('series_id')
        successors = [d for d in documents if series and (d.get('seriesId') or d.get('id')) == series]
    if len(successors) != 1:
        raise ReplacementCleanupConflict('Не найдена текущая версия серии. Удаление старого файла остановлено.')
    successor = successors[0]
    # Both versions live in the folder of the series inside the item.
    from app.checklists.document_folders import document_yandex_folder, documents_in_folder, document_relative_folder
    file_folder = document_yandex_folder(folder, successor)
    new_name = _file_name(successor.get('name'))
    new_path = require_project_path(dialog_id, file_folder + '/' + new_name)
    _verify_file(new_path, new_name, successor, probe(new_path))
    old_name = _file_name(replacement.get('old_file_name'))
    old_path = require_project_path(dialog_id, file_folder + '/' + old_name)
    if old_path == new_path:
        return {'path': old_path, 'action': 'same_path_protected'}
    same_folder_documents = documents_in_folder(documents, document_relative_folder(successor))
    if any(clean_cell_value(d.get('name')).casefold() == old_name.casefold() for d in same_folder_documents):
        raise ReplacementCleanupConflict('Имя старой версии уже используется текущим файлом пункта. Удаление остановлено.')
    if path_used_by_current_document(dialog_id, old_path):
        raise ReplacementCleanupConflict('На старую версию ссылается текущий документ. Удаление остановлено.')
    versions = [v for v in archive_versions(item)
                if (v.get('id') or v.get('versionId')) == replacement.get('archive_version_id')]
    if len(versions) != 1:
        raise ReplacementCleanupConflict('Не найдена архивная версия для проверки старого файла.')
    archive = versions[0]
    if (archive.get('originalDocumentId') and archive['originalDocumentId'] != replacement.get('old_document_id')):
        raise ReplacementCleanupConflict('Архив относится к другому документу. Удаление остановлено.')
    _local_file(archive)  # Preserve a readable local archive even if remote is absent.
    old_meta = probe(old_path)
    if old_meta is None:
        return {'path': old_path, 'action': 'confirmed_absent'}
    _verify_file(old_path, old_name, archive, old_meta)
    return {'path': old_path, 'action': 'delete'}


def persist_delete_target(replacement, job_id, path):
    """Keep queue, replacement and archive addresses consistent atomically."""
    conn = get_conn()
    try:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('UPDATE upload_jobs SET yandex_path=? WHERE job_id=?', (path, job_id))
        conn.execute('UPDATE document_replacements SET old_yandex_path=? WHERE operation_id=?',
                     (path, replacement['operation_id']))
        storage_id = make_storage_dialog_id(replacement['dialog_id'], replacement['checklist_key'])
        row = conn.execute('SELECT data_json FROM checklists WHERE dialog_id=?', (storage_id,)).fetchone()
        if row:
            data = json.loads(row['data_json'])
            for item in data.get('items', []):
                if item.get('id') != replacement.get('item_id'):
                    continue
                for version in archive_versions(item):
                    if (version.get('id') or version.get('versionId')) == replacement.get('archive_version_id'):
                        version['originalYandexPath'] = path
            conn.execute('UPDATE checklists SET data_json=? WHERE dialog_id=?',
                         (json.dumps(data, ensure_ascii=False), storage_id))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def retry_item_replacement_cleanup(*, dialog_id, checklist_key, item_id):
    """Explicit manual recovery only; never revive a cancelled or real delete."""
    from app.checklists.document_replacements import ensure_document_replacements_table
    from app.checklists.upload_jobs import get_upload_job
    from app.checklists.yandex_mirror_queue import enqueue_yandex_mirror_job
    from app.checklists.yandex_resource_locks import yandex_project_resource_guard
    from app.checklists.replacement_sync import update_archive_version_yandex_state
    ensure_document_replacements_table()
    stats = {'queued': 0, 'deferred': 0, 'errors': []}
    identity = (normalize_dialog_id(dialog_id), normalize_checklist_key(checklist_key), clean_cell_value(item_id))
    with yandex_project_resource_guard(dialog_id, operation='manual_replacement_recovery'):
        conn = get_conn()
        try:
            rows = conn.execute('SELECT r.*, j.status AS delete_status, j.stage AS delete_stage '
                                'FROM document_replacements r JOIN upload_jobs j ON j.job_id=r.delete_job_id '
                                'WHERE r.dialog_id=? AND r.checklist_key=? AND r.item_id=? '
                                "AND r.status != 'cancelled' AND j.job_type='delete' "
                                "AND (j.status='error' OR (j.status='deleted' AND j.stage='already_missing'))", identity).fetchall()
        finally:
            conn.close()
        from app.checklists.yandex_structure_jobs import get_latest_yandex_structure_job_for_item
        structure = get_latest_yandex_structure_job_for_item(dialog_id=identity[0], checklist_key=identity[1], item_id=identity[2]) or {}
        if structure.get('status') in {'queued', 'running', 'error', 'conflict'}:
            stats['deferred'] = len(rows)
            return stats
        for raw in rows:
            replacement = dict(raw)
            upload = get_upload_job(replacement['new_upload_job_id']) or {}
            if upload.get('status') != 'synced':
                stats['deferred'] += 1
                continue
            # The worker performs all remote and local checks under the same
            # project lock. Reuse the durable job so repeated clicks are safe.
            conn = get_conn()
            try:
                conn.execute('BEGIN IMMEDIATE')
                changed = conn.execute("UPDATE upload_jobs SET status='queued',stage='manual_replacement_verify',"
                                       "error='',finished_at='' WHERE job_id=? AND "
                                       "(status='error' OR (status='deleted' AND stage='already_missing'))",
                                       (replacement['delete_job_id'],)).rowcount
                if changed:
                    conn.execute("UPDATE document_replacements SET status='pending',stage='old_yandex_delete_queued',"
                                 "error='',finished_at='' WHERE operation_id=?", (replacement['operation_id'],))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
            if changed:
                update_archive_version_yandex_state(replacement, 'pending_after_replacement_sync', replacement['delete_job_id'], '')
                enqueue_yandex_mirror_job(replacement['delete_job_id'], source='manual_replacement_recovery')
                stats['queued'] += 1
    write_debug_log('yandex_replacement_manual_recovery', {**stats, 'dialogId': dialog_id,
                                                         'checklistKey': checklist_key, 'itemId': item_id})
    return stats


def may_overwrite_relocated_version(replacement, item, folder, probe):
    """A same-name replacement may overwrite only its verified archived bytes.

    Also covers items moved before this patch, whose stored old path is stale.
    A previous error alone grants no overwrite: the archive must match remote.
    """
    if replacement.get('status') not in {'pending', 'error'}:
        return False
    old_name = clean_cell_value(replacement.get('old_file_name'))
    new_name = clean_cell_value(replacement.get('new_file_name'))
    if not old_name or old_name != new_name:
        return False
    require_project_path(replacement['dialog_id'], replacement.get('old_yandex_path') or '')
    from app.checklists.document_folders import document_yandex_folder
    successor = next((d for d in (item.get('documents') or [])
                      if d.get('id') == replacement.get('new_document_id')), {})
    path = require_project_path(replacement['dialog_id'],
                                document_yandex_folder(folder, successor) + '/' + _file_name(old_name))
    meta = probe(path)
    if meta is None:
        return False
    versions = [v for v in archive_versions(item)
                if (v.get('id') or v.get('versionId')) == replacement.get('archive_version_id')]
    if len(versions) != 1:
        return False
    try:
        _verify_file(path, old_name, versions[0], meta)
    except ReplacementCleanupConflict:
        return False
    persist_delete_target(replacement, replacement.get('delete_job_id') or '', path)
    return True
