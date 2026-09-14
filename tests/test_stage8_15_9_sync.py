import asyncio
import hashlib
import io
import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

from app.checklists import yandex_scope as scope
from app.checklists import yandex_file_reconciliation as discovery
from app.checklists import yandex_folders as folders
from app.checklists import yandex_mirror_queue as queue
from app.checklists.document_names import unique_file_name, safe_file_name

ROOT = 'disk:/ОПР/ТОЛК - ОБЪЕКТ НА ПАВЛЕНКО'
FOREIGN = 'disk:/ОПР/ТОЛК - ЖК ДОС'
FOLDER = ROOT + '/00_ИД/01_ТЗ на проектирование'
CONTEXT = {'yandexDisk': {'projectRootPath': ROOT}}
ITEM = {'id':'id_g1_4', 'name':'Тех задание', 'group':1, 'yandexFolderPath':FOLDER}


class DatabaseCase(unittest.TestCase):
    def setUp(self):
        import app.db as db
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.db_patch = patch.object(db, 'DB_PATH', self.path/'app.db')
        self.db_patch.start(); self.addCleanup(self.db_patch.stop)
        db.init_db()
        from app.checklists.edit_sessions import ensure_edit_session_tables
        ensure_edit_session_tables()
        self.db = db

    def context(self):
        from app.checklists.storage import save_project_storage_context
        save_project_storage_context('chat1', CONTEXT)


class ScopeTests(DatabaseCase):
    def probe(self, response, **extra):
        local=self.path/'file.pdf'; local.write_bytes(b'abc')
        doc={'id':'doc','name':'file.pdf','yandexPath':FOREIGN+'/Other/file.pdf'}
        with patch.object(discovery,'yandex_disk_try_get_resource_meta',side_effect=response) as get:
            result=discovery.find_existing_yandex_document(dialog_id='chat1',checklist_key='id',item=ITEM,
                    document=doc, local_path=local,context=CONTEXT,**extra)
        return result,get

    @staticmethod
    def meta(path, content=b'abc'):
        return {'type':'file','name':'file.pdf','path':path,'size':len(content),'sha256':hashlib.sha256(content).hexdigest()}

    def test_matching_file_in_current_project_ignores_identical_foreign_copy(self):
        result,get=self.probe(lambda p:self.meta(p))
        self.assertEqual(result['status'],'matched')
        self.assertEqual(result['match']['path'],FOLDER+'/file.pdf')
        get.assert_called_once_with(FOLDER+'/file.pdf')

    def test_only_foreign_copy_cannot_mark_current_project_synced(self):
        result,get=self.probe(lambda p:self.meta(p) if p.startswith(FOREIGN) else None)
        self.assertEqual(result['status'],'missing')
        get.assert_called_once_with(FOLDER+'/file.pdf')

    def test_old_structure_job_and_recovery_candidate_do_not_expand_scope(self):
        with patch.object(discovery,'get_latest_yandex_structure_job_for_item',return_value={'source_path':FOREIGN+'/Other'}):
            result,get=self.probe(lambda p:None,repair_spec={'sourcePath':FOREIGN+'/Other'},
                                 custom_recovery={'candidates':[{'path':FOREIGN+'/Other'}]})
        self.assertEqual(result['checkedPaths'],[FOLDER+'/file.pdf'])

    def test_copy_in_another_item_of_same_project_is_not_adopted(self):
        item=dict(ITEM)
        result,get=self.probe(lambda p:None)
        self.assertNotIn(ROOT+'/Wrong/file.pdf',result['checkedPaths'])

    def test_same_size_different_content_stops_automatic_upload(self):
        result,_=self.probe(lambda p:self.meta(p,b'xyz'))
        self.assertEqual(result['status'],'conflict')

    def test_missing_hash_cannot_confirm_identity(self):
        result,_=self.probe(lambda p:{'type':'file','path':p,'name':'file.pdf','size':3})
        self.assertEqual(result['status'],'conflict')

    def test_network_error_does_not_mean_file_is_absent(self):
        result,_=self.probe(Mock(side_effect=TimeoutError()))
        self.assertEqual(result['status'],'unavailable')

    def test_api_response_cannot_change_project_binding(self):
        result,_=self.probe(lambda p:self.meta(FOREIGN+'/Other/file.pdf'))
        self.assertEqual(result['status'],'unavailable')

    def test_missing_explicit_root_is_rejected_even_with_project_name(self):
        from app.checklists.yandex_context import hydrate_project_storage_context_from_configs
        context=hydrate_project_storage_context_from_configs({'projectName':'Known name'})
        with self.assertRaises(scope.YandexScopeError):scope.project_root('chat1',context)

    def test_path_component_boundary_traversal_and_root_protected(self):
        for path in [ROOT+'2/file.pdf',FOREIGN+'/file.pdf',ROOT+'/../Other/file.pdf',ROOT]:
            with self.subTest(path=path),self.assertRaises(scope.YandexScopeError):
                scope.require_project_path('chat1',path,context=CONTEXT)

    def test_foreign_item_path_falls_back_to_current_object_mapping(self):
        context={'yandexDisk':{'projectRootPath':ROOT,'folders':{'tz':{'path':FOLDER}}},
                 'itemMappings':[{'checklistKey':'id','groupId':1,'itemName':'Тех задание','folderAlias':'tz'}]}
        self.assertEqual(scope.item_folder('chat1','id',{**ITEM,'yandexFolderPath':FOREIGN+'/Wrong'},context),FOLDER)

    def test_ambiguous_current_item_mapping_stops(self):
        context={'yandexDisk':{'projectRootPath':ROOT,'folders':{'a':{'path':FOLDER},'b':{'path':ROOT+'/Other'}}},
                 'itemMappings':[{'checklistKey':'id','groupId':1,'itemName':'Тех задание','folderAlias':v} for v in ['a','b']]}
        with self.assertRaises(scope.YandexScopeError):scope.item_folder('chat1','id',{**ITEM,'yandexFolderPath':''},context)

    def test_foreign_delete_stops_before_remote_lookup_or_delete(self):
        self.context()
        with patch.object(queue,'is_yandex_disk_enabled',return_value=True),patch.object(queue,'yandex_disk_delete_path') as delete,patch.object(queue,'yandex_disk_try_get_resource_meta') as get:
            with self.assertRaises(scope.YandexScopeError):queue.process_delete_job({'job_id':'x','dialog_id':'chat1','yandex_path':FOREIGN+'/file.pdf'})
        delete.assert_not_called();get.assert_not_called()

    def test_folder_move_cannot_cross_objects(self):
        self.context()
        with patch.object(folders,'yandex_disk_move_path') as move,patch.object(folders,'ensure_yandex_folder_chain') as ensure:
            with self.assertRaises(scope.YandexScopeError):folders.rename_yandex_folder_for_item(dialog_id='chat1',checklist_key='id',group_id=1,item_id='x',old_name='a',new_name='b',source_path=FOREIGN+'/A',target_path=ROOT+'/B',folder_alias='x')
        move.assert_not_called();ensure.assert_not_called()

    def test_low_level_guard_prevents_foreign_upload_and_delete(self):
        self.context()
        from app.checklists.yandex_resource_locks import yandex_project_resource_guard
        from app.yandex_disk import client
        with yandex_project_resource_guard('chat1'):
            with self.assertRaises(scope.YandexScopeError):client.yandex_disk_get_upload_href(FOREIGN+'/file.pdf')
            with self.assertRaises(scope.YandexScopeError):client.yandex_disk_delete_path(FOREIGN+'/file.pdf')
        self.assertEqual(scope.active_project.get(),'')


class UploadIdempotencyTests(DatabaseCase):
    def run_upload(self, remote, allow=False, folder=FOLDER):
        self.context(); local=self.path/'file.pdf';local.write_bytes(b'abc')
        upload=Mock(return_value={'path':FOLDER+'/file.pdf'})
        with patch.object(folders,'is_yandex_disk_enabled',return_value=True),patch.object(folders,'yandex_disk_try_get_resource_meta',return_value=remote),patch.object(folders,'yandex_disk_upload_file',upload):
            result=folders.mirror_document_file_to_yandex('chat1','id','Тех задание','file.pdf',local,item_folder_path=folder,allow_replace=allow)
        return result,upload

    def test_already_uploaded_bytes_are_verified_without_second_put(self):
        result,upload=self.run_upload(ScopeTests.meta(FOLDER+'/file.pdf'))
        self.assertTrue(result['ok']);self.assertTrue(result['reused']);upload.assert_not_called()

    def test_normal_upload_never_overwrites_different_remote_bytes(self):
        result,upload=self.run_upload(ScopeTests.meta(FOLDER+'/file.pdf',b'xyz'))
        self.assertFalse(result['ok']);upload.assert_not_called()

    def test_new_upload_requests_no_overwrite(self):
        result,upload=self.run_upload(None)
        self.assertTrue(result['ok']);self.assertFalse(upload.call_args.kwargs['overwrite'])

    def test_explicit_replacement_can_overwrite_owned_path(self):
        result,upload=self.run_upload(ScopeTests.meta(FOLDER+'/file.pdf',b'xyz'),True)
        self.assertTrue(result['ok']);self.assertTrue(upload.call_args.kwargs['overwrite'])

    def test_foreign_explicit_folder_blocks_upload(self):
        result,upload=self.run_upload(None,folder=FOREIGN+'/Wrong')
        self.assertFalse(result['ok']);upload.assert_not_called()


class NamingAndConcurrentUploadTests(DatabaseCase):
    def test_duplicate_names_increment_inside_item(self):
        names=[]
        for _ in range(3):names.append(unique_file_name('Файл.pdf',names))
        self.assertEqual(names,['Файл.pdf','Файл (2).pdf','Файл (3).pdf'])
        self.assertEqual(unique_file_name('файл.PDF',names),'файл (4).PDF')
        self.assertEqual(unique_file_name('Файл.pdf',[]),'Файл.pdf')

    def test_windows_paths_cannot_become_yandex_subdirectories(self):
        self.assertEqual(safe_file_name(r'C:\fakepath\Файл.pdf'),'Файл.pdf')
        with self.assertRaises(ValueError):safe_file_name('..')

    def test_concurrent_uploads_to_two_items_keep_both_files(self):
        from app.checklists.edit_sessions import start_edit_session
        from app.checklists.storage import get_checklist
        from app.checklists.edit_session_documents import transactional_upload_document
        from fastapi import UploadFile
        sid=start_edit_session('chat1','18','Tester')['session']['session_id']
        data=get_checklist('chat1','id');ids=[i['id'] for i in data['items'][:2]]
        class SlowFile(UploadFile):
            async def read(self,size=-1):
                await asyncio.sleep(.015)
                return await super().read(size)
        async def run():
            return await asyncio.gather(*(transactional_upload_document(session_id=sid,dialog_id='chat1',checklist_key='id',item_id=item_id,item_group=1,
                file=SlowFile(io.BytesIO(b'one'),filename='same.pdf'),acting_user_id='18') for item_id in ids))
        result=asyncio.run(run())
        self.assertTrue(all(r['ok'] for r in result))
        data=get_checklist('chat1','id')
        actual={i['id']:i.get('documents',[]) for i in data['items']}
        self.assertEqual([len(actual[i]) for i in ids],[1,1])
        self.assertEqual([actual[i][0]['name'] for i in ids],['same.pdf','same.pdf'])

    def test_concurrent_uploads_inside_one_item_keep_three_unique_names(self):
        from app.checklists.edit_sessions import start_edit_session
        from app.checklists.storage import get_checklist
        from app.checklists.edit_session_documents import transactional_upload_document
        from fastapi import UploadFile
        sid=start_edit_session('chat1','18','Tester')['session']['session_id']
        item_id=get_checklist('chat1','id')['items'][0]['id']
        async def run():
            await asyncio.gather(*(transactional_upload_document(session_id=sid,dialog_id='chat1',checklist_key='id',item_id=item_id,item_group=1,
                file=UploadFile(io.BytesIO(str(i).encode()),filename='same.pdf'),acting_user_id='18') for i in range(3)))
        asyncio.run(run())
        names=[d['name'] for d in get_checklist('chat1','id')['items'][0]['documents']]
        self.assertEqual(names,['same.pdf','same (2).pdf','same (3).pdf'])

class DeleteReferenceProtectionTests(DatabaseCase):
    def test_delete_never_removes_path_still_used_by_current_file(self):
        self.context()
        data={'items':[{'id':'a','name':'A','group':1,'documents':[{'id':'live','name':'file.pdf','fileUrl':'/uploads/file.pdf','yandexPath':FOLDER+'/file.pdf'}]}]}
        conn=self.db.get_conn();conn.execute('INSERT INTO checklists VALUES(?,?,?)',('chat1','Project',json.dumps(data)));conn.commit();conn.close()
        with patch.object(queue,'is_yandex_disk_enabled',return_value=True),patch.object(queue,'get_document_replacement_by_delete_job',return_value={}),patch.object(queue,'update_upload_job_progress'),patch.object(queue,'finish_upload_job') as finish,patch.object(queue,'yandex_disk_delete_path') as delete,patch.object(queue,'yandex_disk_try_get_resource_meta') as probe:
            queue.process_delete_job({'job_id':'old-delete','dialog_id':'chat1','yandex_path':FOLDER+'/file.pdf'})
        delete.assert_not_called();probe.assert_not_called();self.assertEqual(finish.call_args.kwargs['stage'],'same_path_protected')
