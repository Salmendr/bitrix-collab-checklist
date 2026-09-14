import asyncio
import io
from unittest.mock import patch
from fastapi import FastAPI, UploadFile
from fastapi.testclient import TestClient
from test_stage8_15_9_sync import DatabaseCase


class HttpRegressionTests(DatabaseCase):
    def setUp(self):
        super().setUp()
        from app.checklists.edit_sessions import start_edit_session
        from app.checklists.storage import get_checklist
        from app.checklists.edit_session_documents import transactional_upload_document
        from app.checklists.document_routes import router as docs
        from app.checklists.archive_routes import router as archive
        from app.ui.popup_routes import router as popup
        self.app=FastAPI();self.app.include_router(docs);self.app.include_router(archive);self.app.include_router(popup)
        self.client=TestClient(self.app)
        self.sid=start_edit_session('chat1','18','Сергей')['session']['session_id']
        self.item=get_checklist('chat1','id')['items'][0]['id']
        result=asyncio.run(transactional_upload_document(session_id=self.sid,dialog_id='chat1',checklist_key='id',item_id=self.item,item_group=1,
                          file=UploadFile(io.BytesIO(b'%PDF-test'),filename='Документ.pdf'),acting_user_id='18',acting_user_name='Сергей'))
        self.doc=get_checklist('chat1','id')['items'][0]['documents'][0]['id']

    def test_popup_all_checklist_types_render_without_missing_placeholders(self):
        for key in ['id','concept','opr','p','r']:
            with self.subTest(key=key):
                response=self.client.get('/popup',params={'dialogId':'chat1','checklistKey':key})
                self.assertEqual(response.status_code,200)
                self.assertNotIn('[[POPUP_',response.text)
                self.assertIn('id-reminders.js?v=8.15.9',response.text)

    def test_folder_renders_staging_download_and_current_icon_version(self):
        response=self.client.get('/api/checklist/folder',params={'dialogId':'chat1','itemId':self.item,'checklistKey':'id','sessionId':self.sid,'userId':'18'})
        self.assertEqual(response.status_code,200)
        self.assertIn('folder-download-file',response.text)
        self.assertIn('checklist-action-icons.js?v=8.15.9',response.text)
        self.assertNotIn('[[FOLDER_',response.text)

    def test_download_is_an_attachment_with_original_unicode_name_and_bytes(self):
        response=self.client.get('/api/checklist/file',params={'dialogId':'chat1','itemId':self.item,'documentId':self.doc,'download':1})
        self.assertEqual(response.status_code,200);self.assertEqual(response.content,b'%PDF-test')
        self.assertTrue(response.headers['content-disposition'].startswith('attachment;'))
        self.assertIn("filename*=UTF-8''",response.headers['content-disposition'])

    def test_preview_remains_inline(self):
        response=self.client.get('/api/checklist/file',params={'dialogId':'chat1','itemId':self.item,'documentId':self.doc})
        self.assertEqual(response.status_code,200);self.assertTrue(response.headers['content-disposition'].startswith('inline;'))

    def test_wrong_project_cannot_resolve_document_id(self):
        response=self.client.get('/api/checklist/file',params={'dialogId':'chat2','itemId':self.item,'documentId':self.doc,'download':1})
        self.assertEqual(response.status_code,404)

    def test_api_upload_route_retains_form_signature_after_lock_decorator(self):
        response=self.client.post('/api/checklist/upload-document',data={'dialogId':'chat1','itemId':self.item,'checklistKey':'id',
            'sessionId':self.sid,'actingUserId':'18','requireEditSession':'1'}, files={'file':('Документ.pdf',b'two','application/pdf')})
        self.assertEqual(response.status_code,200);self.assertTrue(response.json()['ok'])
        from app.checklists.storage import get_checklist
        names=[d['name'] for d in get_checklist('chat1','id')['items'][0]['documents']]
        self.assertEqual(names,['Документ.pdf','Документ (2).pdf'])
