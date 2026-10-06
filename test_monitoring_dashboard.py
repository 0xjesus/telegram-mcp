import sqlite3
import tempfile
import unittest
from pathlib import Path
from monitoring_dashboard import Dashboard


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name)
        self.paths={p:root/(p+'.db') for p in ('whatsapp','telegram')}
        for platform,path in self.paths.items():
            col='chat_jid TEXT' if platform=='whatsapp' else 'chat_id INTEGER'
            key=col.split()[0]
            with sqlite3.connect(path) as db:
                db.executescript(f'CREATE TABLE group_monitoring_inventory({col} PRIMARY KEY,name TEXT,member_count INTEGER,updated_at TEXT); CREATE TABLE group_monitoring_consent({col} PRIMARY KEY,allowed INTEGER,evidence TEXT,updated_at TEXT); CREATE TABLE group_monitoring_audit(seq INTEGER PRIMARY KEY,{col},allowed INTEGER,evidence TEXT,updated_at TEXT); CREATE TABLE memory_group_rescan(chat_id TEXT PRIMARY KEY,cursor TEXT);')
                db.execute('INSERT INTO group_monitoring_inventory VALUES(?,?,?,?)',('1@g.us' if platform=='whatsapp' else -1,'Synthetic',99,'2026-01-01'))
                if platform=='telegram':db.executescript('CREATE TABLE chat_sync(chat_id INTEGER PRIMARY KEY,backfill_id INTEGER,backfill_status TEXT,last_backfill_at REAL,last_incremental_at REAL,incremental_pending INTEGER); CREATE TABLE chats(id INTEGER PRIMARY KEY,backfill_done INTEGER); INSERT INTO chats VALUES(-1,1); INSERT INTO chat_sync VALUES(-1,20,"done",1,1,0);')
        self.app=Dashboard(self.paths,root/'dashboard.db')

    def test_enable_all_and_explicit_disable_survives_discovery(self):
        self.app.enable_all()
        self.assertTrue(all(r['enabled'] for r in self.app.inventory()))
        self.app.toggle('telegram','-1',False)
        self.app.reconcile()
        self.assertFalse(next(r['enabled'] for r in self.app.inventory() if r['platform']=='telegram'))

    def test_new_group_defaults_on_without_member_count(self):
        self.app.enable_all()
        with sqlite3.connect(self.paths['whatsapp']) as db:db.execute("INSERT INTO group_monitoring_inventory VALUES('2@g.us','New',NULL,'2026-01-01')")
        self.app.reconcile()
        self.assertTrue(next(r['enabled'] for r in self.app.inventory() if r['id']=='2@g.us'))

    def test_unknown_group_is_rejected(self):
        with self.assertRaises(ValueError):self.app.toggle('telegram','-99',True)

    def test_recovery_is_durable_and_resets_telegram_cursors(self):
        self.app.enable_all()
        with sqlite3.connect(self.paths['telegram']) as db:
            self.assertEqual(db.execute('SELECT backfill_id,backfill_status,incremental_pending FROM chat_sync').fetchone(),(0,'pending',1))
        restarted=Dashboard(self.paths,self.app.state)
        self.assertEqual(len(restarted.recovery_status()),2)

    def test_disabled_group_is_not_recovered(self):
        self.app.enable_all();self.app.toggle('whatsapp','1@g.us',False)
        self.assertIsNone(self.app.next_whatsapp_recovery())

    def test_explicit_telegram_recovery_schedules_attachment_rescan(self):
        self.app.enable_all()
        with sqlite3.connect(self.paths['telegram']) as c:
            c.execute("ALTER TABLE chats ADD COLUMN type TEXT DEFAULT 'group'")
            c.execute('CREATE TABLE attachment_group_rescan(chat_id INTEGER PRIMARY KEY,cursor INTEGER NOT NULL DEFAULT 0)')
            c.execute('INSERT INTO attachment_group_rescan VALUES(-1,999)')
        self.app.recover('telegram','-1')
        with sqlite3.connect(self.paths['telegram']) as c:
            self.assertEqual(c.execute('SELECT cursor FROM attachment_group_rescan WHERE chat_id=-1').fetchone()[0],0)


class RecoveryPaginationTests(DashboardTests):
    def setUp(self):
        super().setUp()
        from unittest.mock import patch
        self.now=1800000000.0
        self.clock=patch('monitoring_dashboard.time.time',side_effect=lambda:self.now)
        self.clock.start();self.addCleanup(self.clock.stop)
        self.calls=[]
        self.transport=patch('monitoring_dashboard.mcp',side_effect=self.rpc)
        self.transport.start();self.addCleanup(self.transport.stop)
        with sqlite3.connect(self.paths['whatsapp']) as c:
            c.execute('CREATE TABLE messages(id TEXT,chat_jid TEXT,timestamp TEXT,PRIMARY KEY(chat_jid,id))')
        self.app.enable_all()

    def rpc(self,name,args):
        import json
        if name=='get_status':return {'content':[{'type':'text','text':json.dumps({'connected':True,'health':{'state':'ok'}})}]}
        self.calls.append(dict(args))
        return {'content':[{'type':'text','text':'requested'}]}

    def job(self,chat='1@g.us'):
        return next(r for r in self.app.recovery_status() if r['platform']=='whatsapp' and r['chat_id']==chat)

    def insert(self,ident,seconds,chat='1@g.us'):
        import datetime as dt
        stamp=dt.datetime.fromtimestamp(seconds,dt.timezone.utc).isoformat()
        with sqlite3.connect(self.paths['whatsapp']) as c:
            c.execute('INSERT INTO messages VALUES(?,?,?)',(ident,chat,stamp))
        return stamp

    def test_rpc_acceptance_waits_then_delayed_arrivals_advance_after_restart(self):
        self.app.recover_one()
        self.assertEqual(self.job()['status'],'waiting')
        self.assertEqual(self.app._timestamp(self.calls[0]['from_timestamp']),self.now)
        self.now+=119;self.app.recover_one();self.assertEqual(len(self.calls),1)
        older=self.insert('older',self.now-1000)
        self.app=Dashboard(self.paths,self.app.state)
        self.now+=1;self.app.recover_one()
        self.assertEqual(self.job()['status'],'queued')
        self.assertEqual(self.job()['anchor'],older)
        self.now+=30;self.app.recover_one()
        self.assertEqual(self.calls[-1]['from_timestamp'],older)
        self.assertEqual(len(self.calls),2)

    def test_no_new_messages_does_not_jump_to_old_cached_message(self):
        self.insert('ancient',self.now-100000)
        self.app.recover_one();self.now+=120;self.app.recover_one()
        self.assertEqual(self.job()['status'],'no_progress')
        self.now+=10000;self.app.recover_one();self.assertEqual(len(self.calls),1)

    def test_network_timeout_observes_once_without_resending(self):
        from unittest.mock import patch
        def uncertain(name,args):
            if name=='get_status':return self.rpc(name,args)
            self.calls.append(dict(args));raise TimeoutError()
        with patch('monitoring_dashboard.mcp',side_effect=uncertain):self.app.recover_one()
        self.assertEqual(self.job()['status'],'uncertain')
        self.app=Dashboard(self.paths,self.app.state)
        self.now+=120;self.app.recover_one()
        self.assertEqual(self.job()['status'],'no_progress')
        self.now+=1000;self.app.recover_one();self.assertEqual(len(self.calls),1)

    def test_unknown_outcome_can_advance_if_arrivals_prove_progress(self):
        from unittest.mock import patch
        def uncertain(name,args):
            if name=='get_status':return self.rpc(name,args)
            self.calls.append(dict(args));raise TimeoutError()
        with patch('monitoring_dashboard.mcp',side_effect=uncertain):self.app.recover_one()
        older=self.insert('late',self.now-50)
        self.now+=120;self.app.recover_one()
        self.assertEqual((self.job()['status'],self.job()['anchor']),('queued',older))

    def test_global_gate_survives_restart_between_groups(self):
        with sqlite3.connect(self.paths['whatsapp']) as c:
            c.execute("INSERT INTO group_monitoring_inventory VALUES('2@g.us','Other',NULL,'2026-01-01')")
        self.app.reconcile();self.app.recover_one()
        self.app=Dashboard(self.paths,self.app.state)
        self.app.recover_one();self.assertEqual(len(self.calls),1)
        self.now+=29;self.app.recover_one();self.assertEqual(len(self.calls),1)
        self.now+=1;self.app.recover_one();self.assertEqual(len(self.calls),2)
        self.assertNotEqual(self.calls[0]['chat_jid'],self.calls[1]['chat_jid'])

    def test_manual_off_cancels_waiting_and_recover_rejects_disabled(self):
        self.app.recover_one();self.app.toggle('whatsapp','1@g.us',False)
        self.insert('late',self.now-50);self.now+=120;self.app.recover_one()
        self.assertEqual(self.job()['status'],'paused')
        with self.assertRaises(ValueError):self.app.recover('whatsapp','1@g.us')
        self.app.reconcile();self.assertFalse(next(r['enabled'] for r in self.app.inventory() if r['platform']=='whatsapp'))
        self.assertEqual(len(self.calls),1)

    def test_live_message_newer_than_anchor_does_not_prove_history_progress(self):
        self.app.recover_one();self.insert('live',self.now+1)
        self.now+=120;self.app.recover_one()
        self.assertEqual(self.job()['status'],'no_progress')

    def test_health_failures_are_bounded_and_never_send_history(self):
        from unittest.mock import patch
        with patch('monitoring_dashboard.mcp',side_effect=ConnectionError()):
            for _ in range(4):self.app.recover_one();self.now+=301
        self.assertEqual(self.job()['status'],'unavailable')
        self.assertEqual(self.job()['attempts'],3)
        self.assertEqual(self.calls,[])

    def test_large_arrival_batch_scans_durably_before_advancing(self):
        self.app.recover_one()
        for i in range(205):self.insert(str(i),self.now-1000-i)
        self.now+=120;self.app.recover_one()
        self.assertEqual(self.job()['status'],'waiting')
        self.app=Dashboard(self.paths,self.app.state)
        self.now+=30;self.app.recover_one()
        self.assertEqual(self.job()['status'],'queued')
        import datetime as dt
        oldest=dt.datetime.fromtimestamp(self.now-150-1204,dt.timezone.utc).isoformat()
        self.assertEqual(self.job()['anchor'],oldest)
        self.assertEqual(len(self.calls),1)

    def test_cancel_during_transport_cannot_be_overwritten_by_completion(self):
        from unittest.mock import patch
        def cancel(name,args):
            result=self.rpc(name,args)
            if name=='request_sync':self.app.toggle('whatsapp','1@g.us',False)
            return result
        with patch('monitoring_dashboard.mcp',side_effect=cancel):self.app.recover_one()
        self.assertEqual(self.job()['status'],'paused')

    def test_single_worker_lock_blocks_another_dashboard_instance(self):
        from unittest.mock import patch
        other=Dashboard(self.paths,self.app.state)
        def overlap(name,args):
            result=self.rpc(name,args)
            if name=='request_sync':other.recover_one()
            return result
        with patch('monitoring_dashboard.mcp',side_effect=overlap):self.app.recover_one()
        self.assertEqual(len(self.calls),1)

    def test_legacy_requested_migration_never_replays_unknown_request(self):
        with self.app.state_db() as c:
            c.execute("UPDATE recovery SET status='requested' WHERE platform='whatsapp'")
        self.app=Dashboard(self.paths,self.app.state)
        self.app.recover_one()
        self.assertEqual(self.job()['status'],'no_progress')
        self.assertEqual(self.calls,[])


class DashboardHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from aiohttp.test_utils import TestClient,TestServer
        from unittest.mock import Mock
        from monitoring_dashboard import application
        self.dashboard=Mock()
        self.dashboard.inventory.return_value=[]
        self.client=TestClient(TestServer(application(self.dashboard,run_background=False)))
        await self.client.start_server()
        response=await self.client.get('/')
        import re
        self.token=re.search("const csrf='([^']+)'",await response.text()).group(1)

    async def asyncTearDown(self):
        await self.client.close()

    async def test_host_csrf_origin_and_valid_toggle(self):
        bad=await self.client.get('/api/groups',headers={'Host':'attacker.example'})
        self.assertEqual(bad.status,403)
        bad=await self.client.post('/api/toggle',json={'platform':'telegram','id':'-1','enabled':False})
        self.assertEqual(bad.status,403)
        bad=await self.client.post('/api/toggle',json={},headers={'X-CSRF-Token':self.token,'Origin':'https://attacker.example'})
        self.assertEqual(bad.status,403)
        good=await self.client.post('/api/toggle',json={'platform':'telegram','id':'-1','enabled':False},headers={'X-CSRF-Token':self.token})
        self.assertEqual(good.status,200)
        self.dashboard.toggle.assert_called_once_with('telegram','-1',False)
        self.dashboard.recover_one.assert_not_called()

    async def test_disabled_recover_returns_bad_request(self):
        self.dashboard.recover.side_effect=ValueError('monitoring_disabled')
        response=await self.client.post('/api/recover',json={'platform':'whatsapp','id':'1@g.us'},headers={'X-CSRF-Token':self.token})
        self.assertEqual(response.status,400)
        self.dashboard.toggle.assert_not_called()
