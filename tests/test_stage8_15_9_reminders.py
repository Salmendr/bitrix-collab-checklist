import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, Mock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from test_stage8_15_9_sync import DatabaseCase
from app.checklists import id_reminders as reminders

NOW = datetime(2026,9,10,3,0,tzinfo=timezone.utc)  # Thursday 13:00 UTC+10
CONFIG = {'enabled':True,'days':[0,1,2,3,4,5,6],'hour':13,'minute':0,'timezoneOffset':600,
          'recipients':[{'userId':'18','name':'untrusted'},{'userId':'26','name':'untrusted'}]}


class ReminderTests(DatabaseCase):
    def setUp(self):
        super().setUp()
        from app.checklists.bitrix_users import _replace_cache, normalize_bitrix_user
        _replace_cache([normalize_bitrix_user({'ID':'18','NAME':'Сергей','LAST_NAME':'Жигарь','ACTIVE':True}),
                        normalize_bitrix_user({'ID':'26','NAME':'Технический','LAST_NAME':'аккаунт','ACTIVE':True})],fetched_at=reminders.iso(NOW))
        self.conn=self.db.get_conn();self.addCleanup(self.conn.close)
        self.data={'items':[{'id':'a','name':'ППТ','group':1,'documents':[]},
                            {'id':'b','name':'ТУ','group':2,'documents':[]},
                            {'id':'c','name':'Не нужно','group':4,'documents':[]},
                            {'id':'d','name':'Согласование с аэропортом','group':3,'documents':[]}]}
        self.conn.execute('INSERT INTO checklists VALUES(?,?,?)',('chat1','ОБЪЕКТ НА ПАВЛЕНКО',json.dumps(self.data)))
        self.conn.commit()

    def publish(self,config=None,updated=None):
        config=reminders.normalize_config(config or CONFIG)
        self.conn.execute('INSERT OR REPLACE INTO id_reminder_settings VALUES(?,?,?,?,?)',
                          ('chat1',json.dumps(config),'revision-1',reminders.iso(updated or NOW-timedelta(days=1)),'18'))
        self.conn.commit()

    def rows(self):
        return [dict(r) for r in self.conn.execute('SELECT * FROM id_reminder_deliveries ORDER BY user_id')]

    def session(self):
        from app.checklists.edit_sessions import start_edit_session
        return start_edit_session('chat1','18','Сергей Жигарь')['session']['session_id']

    def test_names_are_resolved_from_real_cached_ids(self):
        normalized=reminders.normalize_config(CONFIG)
        self.assertEqual(normalized['recipients'][0]['name'],'Жигарь Сергей')

    def test_unknown_and_inactive_users_are_rejected(self):
        for who in ['99999','chat18','-1','']:
            with self.subTest(who=who),self.assertRaises(ValueError):
                reminders.normalize_config({**CONFIG,'recipients':[{'userId':who}]})
        self.conn.execute("UPDATE bitrix_users_cache SET active=0 WHERE user_id='18'");self.conn.commit()
        with self.assertRaises(ValueError):reminders.normalize_config(CONFIG)

    def test_duplicate_recipients_are_deduplicated_by_id(self):
        config=reminders.normalize_config({**CONFIG,'recipients':[{'userId':'18'}]*3})
        self.assertEqual(len(config['recipients']),1)

    def test_invalid_dates_times_and_weekdays_are_rejected(self):
        for field,value in [('days',[]),('days',[7]),('days',['1']),('hour',24),('minute',60),('hour',True),('timezoneOffset',841),('enabled','true')]:
            with self.subTest(field=field,value=value),self.assertRaises(ValueError):
                reminders.normalize_config({**CONFIG,field:value})

    def test_daily_respects_selected_timezone(self):
        self.assertEqual(reminders.due_slot(CONFIG,NOW,reminders.iso(NOW-timedelta(days=1))),reminders.iso(NOW))
        self.assertIsNone(reminders.due_slot(CONFIG,NOW-timedelta(seconds=1),reminders.iso(NOW-timedelta(hours=1))))

    def test_multiple_selected_weekdays(self):
        for day in range(7):
            now=datetime(2026,9,7+day,3,0,tzinfo=timezone.utc)
            config={**CONFIG,'days':[0,2,4]}
            actual=reminders.due_slot(config,now,reminders.iso(now-timedelta(hours=2)))
            self.assertEqual(bool(actual),day in [0,2,4])

    def test_activation_after_time_does_not_send_retroactively(self):
        self.publish(updated=NOW+timedelta(minutes=1))
        self.assertEqual(reminders.collect_due(NOW+timedelta(minutes=2)),0)

    def test_restart_coalesces_missed_time_without_historical_spam(self):
        self.publish()
        self.assertEqual(reminders.collect_due(NOW+timedelta(hours=2)),2)
        self.assertEqual(reminders.collect_due(NOW+timedelta(hours=3)),0)
        self.assertEqual(len(self.rows()),2)

    def test_default_is_disabled_and_produces_no_messages(self):
        self.assertFalse(reminders.get_settings('chat1')['config']['enabled'])
        self.assertEqual(reminders.collect_due(NOW),0)

    def test_empty_item_message_includes_id_tu_other_but_not_not_required(self):
        self.publish();reminders.collect_due(NOW)
        self.assertEqual(self.rows()[0]['message'], 'Исходные данные по ОБЪЕКТ НА ПАВЛЕНКО требуют дополнения по следующим пунктам:\n1-ППТ\n2-ТУ\n3-Согласование с аэропортом')

    def test_present_document_with_sync_error_is_not_empty(self):
        data={'items':[{'id':'a','name':'ППТ','group':1,'documents':[{'id':'d','name':'a.pdf','fileUrl':'/uploads/a.pdf','mirrorStatus':'error'}]}]}
        self.assertEqual(reminders.empty_items(data),[])

    def test_archive_only_does_not_fill_current_item(self):
        data={'items':[{'id':'a','name':'ППТ','group':1,'documents':[],'archiveSeries':[{'id':'old'}]}]}
        self.assertEqual(reminders.empty_items(data),['ППТ'])

    def test_all_filled_or_not_required_sends_nothing(self):
        self.conn.execute('UPDATE checklists SET data_json=?',(json.dumps({'items':[self.data['items'][2]]}),));self.conn.commit()
        self.publish();self.assertEqual(reminders.collect_due(NOW),0);self.assertEqual(self.rows(),[])

    def test_message_escapes_bbcode_in_project_and_item_names(self):
        text=reminders.plain_message('[USER=1]Admin[/USER]',['[URL=x]Link[/URL]'])
        self.assertNotIn('[USER',text);self.assertNotIn('[URL',text)

    def test_settings_stay_draft_until_real_session_commit(self):
        from app.checklists.edit_sessions import commit_edit_session
        sid=self.session()
        reminders.save_draft(dialog_id='chat1',session_id=sid,user_id='18',config=CONFIG)
        self.assertFalse(reminders.get_settings('chat1')['config']['enabled'])
        self.assertTrue(reminders.get_settings('chat1',sid)['pending'])
        with patch('app.checklists.edit_sessions._schedule_committed_edit_session_jobs'):
            commit_edit_session(sid,'chat1','18',reason='save_and_close')
        self.assertTrue(reminders.get_settings('chat1')['config']['enabled'])
        self.assertFalse(reminders.get_settings('chat1',sid)['pending'])

    def test_explicit_cancel_does_not_publish_draft(self):
        from app.checklists.edit_sessions import rollback_edit_session
        sid=self.session()
        reminders.save_draft(dialog_id='chat1',session_id=sid,user_id='18',config=CONFIG)
        rollback_edit_session(sid,'chat1','18',reason='cancel_button')
        self.assertFalse(reminders.get_settings('chat1')['config']['enabled'])
        self.assertFalse(reminders.get_settings('chat1',sid)['pending'])

    def test_scheduler_waits_for_active_checklist_edits(self):
        from app.checklists.edit_session_changes import acquire_checklist_for_edit_session
        self.publish();sid=self.session()
        acquire_checklist_for_edit_session(session_id=sid,dialog_id='chat1',checklist_key='id',user_id='18')
        self.assertEqual(reminders.collect_due(NOW),0)

    def test_two_collectors_create_one_slot_and_two_recipients(self):
        self.publish()
        with ThreadPoolExecutor(max_workers=2) as pool:counts=list(pool.map(lambda _:reminders.collect_due(NOW),range(2)))
        self.assertEqual(sum(counts),2);self.assertEqual(len(self.rows()),2)

    def test_two_workers_deliver_once_per_recipient_in_personal_chat(self):
        self.publish();reminders.collect_due(NOW)
        with patch.object(reminders,'bitrix_webhook_call',return_value={'result':123}) as send:
            with ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(lambda _:reminders.deliver_one(NOW),range(4)))
            self.assertEqual(send.call_count,2)
            self.assertEqual({c.args[1]['DIALOG_ID'] for c in send.call_args_list},{'18','26'})
            self.assertTrue(all(c.args[0]=='im.message.add' for c in send.call_args_list))
            self.assertFalse(reminders.deliver_one(NOW+timedelta(minutes=1)))
        self.assertEqual([r['status'] for r in self.rows()],['sent','sent'])

    def test_ambiguous_network_timeout_is_not_retried(self):
        self.publish();reminders.collect_due(NOW)
        with patch.object(reminders,'bitrix_webhook_call',side_effect=TimeoutError()) as send:
            for _ in range(4):reminders.deliver_one(NOW)
        self.assertEqual(send.call_count,2)
        self.assertEqual([r['status'] for r in self.rows()],['uncertain','uncertain'])

    def test_interrupted_sending_is_uncertain_after_restart(self):
        self.publish();reminders.collect_due(NOW)
        self.conn.execute("UPDATE id_reminder_deliveries SET status='sending'");self.conn.commit()
        with patch.object(reminders,'bitrix_webhook_call') as send:
            self.assertFalse(reminders.deliver_one(NOW+timedelta(minutes=6)))
        send.assert_not_called();self.assertEqual([r['status'] for r in self.rows()],['uncertain','uncertain'])

    def test_explicit_rate_limit_rejection_is_bounded_to_three_attempts(self):
        self.publish({**CONFIG,'recipients':[{'userId':'18'}]});reminders.collect_due(NOW)
        with patch.object(reminders,'bitrix_webhook_call',return_value={'error':'QUERY_LIMIT_EXCEEDED'}) as send:
            for i in range(5):reminders.deliver_one(NOW+timedelta(minutes=i))
        self.assertEqual(send.call_count,3);self.assertEqual(self.rows()[0]['status'],'failed')

    def test_other_bitrix_rejection_is_visible_without_repeat(self):
        self.publish({**CONFIG,'recipients':[{'userId':'18'}]});reminders.collect_due(NOW)
        with patch.object(reminders,'bitrix_webhook_call',return_value={'error':'ACCESS_DENIED'}) as send:
            reminders.deliver_one(NOW);reminders.deliver_one(NOW+timedelta(minutes=10))
        self.assertEqual(send.call_count,1);self.assertIn('ACCESS_DENIED',self.rows()[0]['error'])

    def test_disabling_config_cancels_pending_messages_on_commit(self):
        self.publish();reminders.collect_due(NOW)
        sid=self.session();reminders.save_draft(dialog_id='chat1',session_id=sid,user_id='18',config={**CONFIG,'enabled':False})
        self.conn.execute('BEGIN IMMEDIATE');reminders.finalize_drafts(self.conn,sid,commit=True,now=reminders.iso(NOW));self.conn.commit()
        with patch.object(reminders,'bitrix_webhook_call') as send:self.assertFalse(reminders.deliver_one(NOW))
        send.assert_not_called();self.assertEqual([r['status'] for r in self.rows()],['cancelled','cancelled'])

    def test_api_requires_real_active_session_owner(self):
        from app.checklists.id_reminder_routes import router
        app=FastAPI();app.include_router(router);client=TestClient(app)
        response=client.get('/api/checklist/id-reminders',params={'dialogId':'chat1','userId':'18'})
        self.assertEqual(response.status_code,403)
        sid=self.session()
        with patch('app.checklists.id_reminder_routes.can_user_access_checklists',return_value=True), patch('app.checklists.id_reminder_routes.verify_bitrix_actor',return_value='18'):
            denied=client.put('/api/checklist/id-reminders',json={'dialogId':'chat1','userId':'26','sessionId':sid,'config':CONFIG})
            allowed=client.put('/api/checklist/id-reminders',json={'dialogId':'chat1','userId':'18','sessionId':sid,'config':CONFIG})
        self.assertEqual(denied.status_code,403);self.assertEqual(allowed.status_code,200);self.assertTrue(allowed.json()['pending'])

class BitrixActorAuthTests(unittest.TestCase):
    def request(self,token='real-test-token'):
        from starlette.requests import Request
        return Request({'type':'http','headers':[(b'x-bitrix-access-token',token.encode())]})

    def test_forged_actor_is_rejected(self):
        from app.checklists.bitrix_actor_auth import verify_bitrix_actor
        from app.checklists.edit_sessions import EditSessionPermissionError
        with patch('app.settings.BITRIX_TECH_WEBHOOK_URL','https://trusted.bitrix24.ru/rest/138/fixture/'),patch('requests.post',return_value=Mock(status_code=200,json=lambda:{'result':{'ID':'26'}})):
            with self.assertRaises(EditSessionPermissionError):verify_bitrix_actor(self.request(),'18')

    def test_missing_token_does_not_call_bitrix(self):
        from app.checklists.bitrix_actor_auth import verify_bitrix_actor
        from app.checklists.edit_sessions import EditSessionPermissionError
        with patch('requests.post') as http:
            with self.assertRaises(EditSessionPermissionError):verify_bitrix_actor(self.request(''),'18')
        http.assert_not_called()

    def test_verification_uses_configured_portal_and_disables_redirects(self):
        from app.checklists.bitrix_actor_auth import verify_bitrix_actor
        with patch('app.settings.BITRIX_TECH_WEBHOOK_URL','https://trusted.bitrix24.ru/rest/138/fixture/'),patch('requests.post',return_value=Mock(status_code=200,json=lambda:{'result':{'ID':'18','ACTIVE':True}})) as http:
            self.assertEqual(verify_bitrix_actor(self.request(),'18'),'18')
        self.assertEqual(http.call_args.args[0],'https://trusted.bitrix24.ru/rest/user.current.json')
        self.assertFalse(http.call_args.kwargs['allow_redirects'])

    def test_redirect_or_connection_error_never_authenticates(self):
        from app.checklists.bitrix_actor_auth import verify_bitrix_actor
        from app.checklists.edit_sessions import EditSessionPermissionError
        with patch('app.settings.BITRIX_TECH_WEBHOOK_URL','https://trusted.bitrix24.ru/rest/138/fixture/'),patch('requests.post',return_value=Mock(status_code=302)):
            with self.assertRaises(EditSessionPermissionError):verify_bitrix_actor(self.request(),'18')
