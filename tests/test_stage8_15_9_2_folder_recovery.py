import hashlib
import json
from pathlib import Path
from unittest.mock import patch, Mock
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
from test_stage8_15_9_sync import DatabaseCase
from app.checklists import yandex_upload_preflight as preflight
from app.checklists import yandex_folders as folders
from app.checklists import yandex_mirror_reconciliation as reconciliation
from app.checklists import yandex_mirror_queue as queue
from app.checklists import upload_jobs as jobs
from app.checklists import yandex_scope as scope
from app.yandex_disk import client

ROOT = 'disk:/ОПР/ТОЛК - ЖК Памятник Партизанам'
FOLDER = ROOT + '/00_Исходные данные/01_ИРД/05_Справки/02_Согласование с Аэропортом'
OTHER = ROOT + '/00_Исходные данные/01_ИРД/Не требуется/02_Согласование с Аэропортом1'
NAME = 'Аэропорт ответ 29-154.pdf'
ITEM = {'id': 'item_g3_2', 'name': 'Согласование с Аэропортом', 'group': 3, 'yandexFolderPath': FOLDER}
CONTEXT = {'storageMode': {'mirrorTargets': ['yandex_disk']}, 'yandexDisk': {'projectRootPath': ROOT}}


class DiskModel:
    def __init__(self):
        self.resources = {ROOT: {'type': 'dir', 'path': ROOT}, OTHER: {'type': 'dir', 'path': OTHER}}
        self.creates=[]; self.uploads=[]; self.probes=[]
    def meta(self, path):
        self.probes.append(path)
        return self.resources.get(path)
    def mkdir(self, path):
        self.creates.append(path)
        assert path.rsplit('/', 1)[0] in self.resources
        self.resources.setdefault(path, {'type': 'dir', 'path': path})
        return {'ok': True, 'path': path}
    def upload(self, *, target_path, local_path, overwrite=False, **kwargs):
        assert self.resources[target_path.rsplit('/',1)[0]]['type']=='dir'
        assert overwrite or target_path not in self.resources
        self.uploads.append(target_path)
        content=Path(local_path).read_bytes()
        self.resources[target_path]={'type':'file','path':target_path,'name':target_path.rsplit('/',1)[-1],
            'size':len(content),'sha256':hashlib.sha256(content).hexdigest()}
        return {'ok': True, 'path': target_path}


class FolderRecoveryTests(DatabaseCase):
    def setUp(self):
        super().setUp()
        from app.checklists.storage import save_project_storage_context
        save_project_storage_context('chat1', CONTEXT)
        self.disk=DiskModel()
        self.local=self.path/NAME;self.local.write_bytes(b'airport reply')
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(client,'yandex_disk_try_get_resource_meta',side_effect=self.disk.meta))
        self.stack.enter_context(patch.object(client,'yandex_disk_ensure_folder',side_effect=self.disk.mkdir))
        self.stack.enter_context(patch.object(folders,'yandex_disk_try_get_resource_meta',side_effect=self.disk.meta))
        self.stack.enter_context(patch.object(folders,'yandex_disk_upload_file',side_effect=self.disk.upload))
        self.stack.enter_context(patch.object(folders,'is_yandex_disk_enabled',return_value=True))

    def mirror(self):
        return folders.mirror_document_file_to_yandex('chat1','id',ITEM['name'],NAME,self.local,
            item_id=ITEM['id'],item_group=3,item_folder_path=FOLDER,item_folder_url='https://stale.example/')

    def test_missing_folder_and_parents_created_before_upload(self):
        result=self.mirror()
        self.assertTrue(result['ok'],result)
        self.assertEqual(self.disk.creates[-1],FOLDER)
        self.assertEqual(self.disk.uploads,[FOLDER+'/'+NAME])
        self.assertTrue(all(p.startswith(ROOT+'/') for p in self.disk.creates))
        self.assertNotIn(OTHER,self.disk.creates)

    def test_second_attempt_verifies_without_second_upload(self):
        self.assertTrue(self.mirror()['ok']);creates=list(self.disk.creates)
        self.assertTrue(self.mirror()['reused'])
        self.assertEqual(self.disk.uploads,[FOLDER+'/'+NAME]);self.assertEqual(self.disk.creates,creates)

    def test_persisted_mapping_with_dead_url_also_repairs_folder(self):
        with patch.object(folders,'get_item_yandex_folder',return_value={'folder':{'path':FOLDER,'url':'https://stale.example/'}}):
            result=folders.mirror_document_file_to_yandex('chat1','id',ITEM['name'],NAME,self.local,item_group=3)
        self.assertTrue(result['ok'],result);self.assertEqual(self.disk.creates[-1],FOLDER)

    def test_missing_project_root_never_recreates_object(self):
        del self.disk.resources[ROOT]
        result=self.mirror();self.assertFalse(result['ok'])
        self.assertEqual(self.disk.creates,[]);self.assertEqual(self.disk.uploads,[])

    def test_file_at_folder_path_is_conflict_without_mutation(self):
        self.disk.resources[FOLDER]={'type':'file','path':FOLDER}
        self.assertFalse(self.mirror()['ok']);self.assertEqual(self.disk.creates,[]);self.assertEqual(self.disk.uploads,[])

    def test_network_failure_is_not_absence(self):
        with patch.object(client,'yandex_disk_try_get_resource_meta',side_effect=TimeoutError('API timeout')):
            self.assertFalse(self.mirror()['ok'])
        self.assertEqual(self.disk.creates,[]);self.assertEqual(self.disk.uploads,[])

    def test_mismatched_folder_api_path_rejected(self):
        self.disk.resources[FOLDER]={'type':'dir','path':OTHER}
        self.assertFalse(self.mirror()['ok']);self.assertEqual(self.disk.uploads,[])

    def test_different_remote_file_is_not_overwritten(self):
        self.disk.resources[FOLDER]={'type':'dir','path':FOLDER}
        self.disk.resources[FOLDER+'/'+NAME]={'type':'file','path':FOLDER+'/'+NAME,'name':NAME,'size':13,'sha256':'0'*64}
        self.assertFalse(self.mirror()['ok']);self.assertEqual(self.disk.uploads,[])

    def test_missing_post_upload_confirmation_stays_error(self):
        with patch.object(folders,'yandex_disk_try_get_resource_meta',return_value=None):
            self.assertFalse(self.mirror()['ok'])
        self.assertEqual(len(self.disk.uploads),1)
        self.assertTrue(self.mirror()['reused']);self.assertEqual(len(self.disk.uploads),1)

    def test_same_names_in_different_groups_do_not_conflict(self):
        other={'id':'old','name':ITEM['name'],'group':4,'yandexFolderPath':OTHER}
        self.assertEqual(preflight.require_exclusive_item_folder('chat1','id',ITEM,[ITEM,other],CONTEXT),FOLDER)

    def test_shared_directory_between_distinct_items_is_blocked(self):
        other={'id':'old','name':'Other','group':4,'yandexFolderPath':FOLDER}
        with self.assertRaises(scope.YandexScopeError):
            preflight.require_exclusive_item_folder('chat1','id',ITEM,[ITEM,other],CONTEXT)

    def test_alias_for_other_section_is_not_fallback(self):
        context={'yandexDisk':{'projectRootPath':ROOT,'folders':{'old':{'path':OTHER}}},
                 'itemMappings':[{'checklistKey':'id','groupId':4,'itemName':ITEM['name'],'folderAlias':'old'}]}
        with self.assertRaises(scope.YandexScopeError):
            scope.item_folder('chat1','id',{**ITEM,'yandexFolderPath':'','yandexFolderAlias':'old'},context)

    def setup_failed_document(self):
        doc={'id':'doc1','name':NAME,'fileUrl':'/uploads/test.pdf','mirrorStatus':'error'}
        data={'checklistKey':'id','items':[{**ITEM,'documents':[doc]},
              {'id':'old','name':ITEM['name']+'1','group':4,'yandexFolderPath':OTHER,'documents':[]}]}
        conn=self.db.get_conn();conn.execute('INSERT INTO checklists VALUES(?,?,?)',('chat1','Project',json.dumps(data)));conn.commit();conn.close()
        job=jobs.create_yandex_upload_job(dialog_id='chat1',checklist_key='id',item_id=ITEM['id'],document_id='doc1',local_path=str(self.local),file_name=NAME,file_size=self.local.stat().st_size)
        jid=job['job_id'];jobs.fail_upload_job(jid,'409 DiskPathDoesntExistsError',stage='mirror_failed')
        self.stack.enter_context(patch.object(reconciliation,'is_yandex_disk_enabled',return_value=True))
        self.stack.enter_context(patch.object(reconciliation,'_document_local_path',return_value=self.local))
        from app.checklists import yandex_file_reconciliation as discovery
        self.stack.enter_context(patch.object(discovery,'yandex_disk_try_get_resource_meta',side_effect=self.disk.meta))
        return jid

    def test_failed_upload_remains_held_on_startup(self):
        jid=self.setup_failed_document()
        with patch.object(reconciliation,'enqueue_yandex_mirror_job') as enqueue:
            result=reconciliation.reconcile_yandex_mirror_documents(source='startup')
        self.assertTrue(result['ok'],result);self.assertEqual(result['manualRequired'],1)
        enqueue.assert_not_called();self.assertEqual(self.disk.probes,[]);self.assertEqual(jobs.get_upload_job(jid)['status'],'error')

    def test_manual_click_requeues_missing_file_once_and_worker_creates_folder(self):
        jid=self.setup_failed_document()
        with patch.object(reconciliation,'enqueue_yandex_mirror_job',return_value={'queued':True}):
            result=reconciliation.reconcile_yandex_mirror_documents(source='manual_combined_recovery',dialog_id='chat1',checklist_key='id',item_id=ITEM['id'])
        self.assertTrue(result['ok'],result);self.assertEqual(result['queued'],1)
        with patch.object(queue,'is_yandex_disk_enabled',return_value=True):
            queue.process_yandex_mirror_job(jid)
        self.assertEqual(jobs.get_upload_job(jid)['status'],'synced',jobs.get_upload_job(jid))
        self.assertEqual(self.disk.uploads,[FOLDER+'/'+NAME])

    def test_manual_existing_identical_copy_clears_error_without_put(self):
        jid=self.setup_failed_document();self.disk.resources[FOLDER]={'type':'dir','path':FOLDER}
        self.disk.upload(target_path=FOLDER+'/'+NAME,local_path=self.local);self.disk.uploads.clear()
        with patch.object(reconciliation,'enqueue_yandex_mirror_job') as enqueue:
            result=reconciliation.reconcile_yandex_mirror_documents(source='manual_combined_recovery',dialog_id='chat1',item_id=ITEM['id'])
        self.assertTrue(result['ok'],result);self.assertEqual(result['remoteVerified'],1)
        enqueue.assert_not_called();self.assertEqual(jobs.get_upload_job(jid)['status'],'synced');self.assertEqual(self.disk.uploads,[])

    def test_manual_real_conflict_remains_error(self):
        jid=self.setup_failed_document();self.disk.resources[FOLDER+'/'+NAME]={'type':'file','path':FOLDER+'/'+NAME,'name':NAME,'size':13,'sha256':'0'*64}
        with patch.object(reconciliation,'enqueue_yandex_mirror_job') as enqueue:
            result=reconciliation.reconcile_yandex_mirror_documents(source='manual_combined_recovery',dialog_id='chat1',item_id=ITEM['id'])
        self.assertEqual(result['remoteFileConflicts'],1,result);enqueue.assert_not_called()
        self.assertEqual(jobs.get_upload_job(jid)['status'],'error')

    def test_db_force_flag_does_not_bypass_manual_hold(self):
        jid=self.setup_failed_document()
        result=jobs.ensure_yandex_upload_job_for_reconciliation(dialog_id='chat1',checklist_key='id',item_id=ITEM['id'],document_id='doc1',local_path=str(self.local),file_name=NAME,file_size=13,force_requeue_synced=True)
        self.assertEqual(result['status'],'error');self.assertEqual(jobs.get_upload_job(jid)['status'],'error')

    def test_automatic_structure_completion_does_not_retry_failed_files(self):
        jid=self.setup_failed_document()
        result=queue.requeue_current_yandex_file_failures(dialog_id='chat1',source='yandex_structure_completed')
        self.assertEqual(result['requeued'],0);self.assertEqual(result['manualRequired'],1)
        self.assertEqual(jobs.get_upload_job(jid)['status'],'error')

    def test_manual_retry_double_click_preserves_one_job(self):
        jid=self.setup_failed_document()
        with patch.object(reconciliation,'enqueue_yandex_mirror_job',return_value={'queued':True}):
            for _ in range(2):
                reconciliation.reconcile_yandex_mirror_documents(source='manual_combined_recovery',dialog_id='chat1',item_id=ITEM['id'])
        conn=self.db.get_conn();count=conn.execute('SELECT COUNT(*) FROM upload_jobs').fetchone()[0];conn.close()
        self.assertEqual(count,1);self.assertEqual(jobs.get_upload_job(jid)['status'],'queued')

    def test_two_workers_cannot_upload_same_job_twice(self):
        jid=self.setup_failed_document()
        with patch.object(reconciliation,'enqueue_yandex_mirror_job',return_value={'queued':True}):
            reconciliation.reconcile_yandex_mirror_documents(source='manual_combined_recovery',dialog_id='chat1',item_id=ITEM['id'])
        with patch.object(queue,'is_yandex_disk_enabled',return_value=True):
            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(lambda _:queue.process_yandex_mirror_job(jid),range(2)))
        self.assertEqual(jobs.get_upload_job(jid)['status'],'synced')
        self.assertEqual(self.disk.uploads,[FOLDER+'/'+NAME])

    def structure_job(self, identity):
        from app.checklists.yandex_structure_jobs import create_yandex_structure_job
        return create_yandex_structure_job(idempotency_key=identity,dialog_id='chat1',checklist_key='id',
            item_id=ITEM['id'],action='rename_item_folder',source_path=FOLDER,target_path=FOLDER+'1',
            item_name=ITEM['name'],group_id=3,result={'oldName':ITEM['name'],'sourceGroupId':3})

    def test_manual_structure_continuation_survives_reloading_job(self):
        from app.checklists import yandex_structure_jobs as sj
        job=self.structure_job('manual-persist')
        preflight.mark_manual_structure_continuation(job['job_id'])
        loaded=sj.get_yandex_structure_job(job['job_id'])
        self.assertTrue(loaded['result']['manualFileRecovery'])
        self.assertEqual(loaded['result']['oldName'],ITEM['name'])
        self.assertTrue(sj.claim_yandex_structure_job(job['job_id'])['result']['manualFileRecovery'])

    def test_manual_structure_completion_runs_file_verification_once(self):
        from app.checklists import yandex_structure_queue as sq
        from app.checklists import yandex_structure_jobs as sj
        job=self.structure_job('manual-chain');preflight.mark_manual_structure_continuation(job['job_id'])
        with patch.object(sq,'is_yandex_disk_enabled',return_value=True),patch.object(sq,'_execute_yandex_structure_mutation',return_value={'folderPath':FOLDER+'1'}),patch.object(sq,'persist_item_yandex_structure_state'),patch.object(queue,'enqueue_pending_yandex_mirror_jobs_for_item',return_value={}),patch.object(reconciliation,'reconcile_yandex_mirror_documents',return_value={'ok':True}) as verify:
            result=sq.process_yandex_structure_job(job['job_id'])
            self.assertTrue(result['completed'],result)
            sq.process_yandex_structure_job(job['job_id'])
        verify.assert_called_once_with(source='manual_structure_completed',dialog_id='chat1',checklist_key='id',item_id=ITEM['id'])
        self.assertNotIn('manualFileRecovery',sj.get_yandex_structure_job(job['job_id'])['result'])

    def test_automatic_structure_completion_has_no_manual_intent(self):
        from app.checklists import yandex_structure_queue as sq
        job=self.structure_job('automatic-chain')
        with patch.object(sq,'is_yandex_disk_enabled',return_value=True),patch.object(sq,'_execute_yandex_structure_mutation',return_value={'folderPath':FOLDER+'1'}),patch.object(sq,'persist_item_yandex_structure_state'),patch.object(queue,'enqueue_pending_yandex_mirror_jobs_for_item',return_value={}),patch.object(reconciliation,'reconcile_yandex_mirror_documents') as verify:
            result=sq.process_yandex_structure_job(job['job_id'])
        self.assertTrue(result['completed'],result);verify.assert_not_called()

    def test_custom_recorded_conflict_not_resolved_by_background_pass(self):
        from app.checklists import yandex_custom_recovery as custom
        with patch.object(custom,'can_create_custom_item_yandex_folder',return_value=True),patch.object(custom,'resolve_custom_item_parent_yandex_path',return_value=ROOT),patch.object(custom,'get_latest_yandex_structure_job_for_item',return_value={'status':'conflict'}),patch.object(custom,'_collect_custom_candidates') as scan:
            result=custom.reconcile_custom_item_yandex_folder(dialog_id='chat1',checklist_key='id',item_id='c',item={'isCustom':True,'name':'C','group':3},source='startup')
        self.assertTrue(result['manualRequired']);scan.assert_not_called()

    def test_ambiguous_binding_updates_visible_error_and_waits_for_user(self):
        jid=self.setup_failed_document()
        conn=self.db.get_conn();data=json.loads(conn.execute("SELECT data_json FROM checklists WHERE dialog_id='chat1'").fetchone()[0])
        data['items'][1]['yandexFolderPath']=FOLDER
        conn.execute("UPDATE checklists SET data_json=? WHERE dialog_id='chat1'",(json.dumps(data),));conn.commit();conn.close()
        with patch.object(reconciliation,'enqueue_yandex_mirror_job') as enqueue:
            result=reconciliation.reconcile_yandex_mirror_documents(source='manual_combined_recovery',dialog_id='chat1',item_id=ITEM['id'])
        self.assertFalse(result['ok']);enqueue.assert_not_called()
        self.assertIn('нескольким пунктам',jobs.get_upload_job(jid)['error'])
        self.assertEqual(self.disk.creates,[]);self.assertEqual(self.disk.uploads,[])
