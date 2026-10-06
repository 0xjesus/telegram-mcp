"""Consent transitions preserve recovery progress; explicit recovery still restarts it."""
import sqlite3
import threading
import time
import unittest
from unittest.mock import patch

import consent
import test_monitoring_dashboard as dashboard_tests


class ManualConsentTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.executescript('''CREATE TABLE chats(id INTEGER PRIMARY KEY,type TEXT,backfill_done INTEGER);
            CREATE TABLE chat_sync(chat_id INTEGER PRIMARY KEY,backfill_id INTEGER,backfill_status TEXT,
                last_backfill_at REAL,last_incremental_at REAL,incremental_pending INTEGER);
            INSERT INTO chats VALUES(-1,'group',1);
            INSERT INTO chat_sync VALUES(-1,777,'done',123,456,0);''')
        consent.install(self.db)

    def progress(self):
        return (self.db.execute('SELECT * FROM chat_sync').fetchall(),
                self.db.execute('SELECT backfill_done FROM chats').fetchall())

    def seed(self, allowed, evidence='manual approval', stamp='2000-01-01T00:00:00Z'):
        self.db.execute('INSERT INTO group_monitoring_consent VALUES(-1,?,?,?)',(allowed,evidence,stamp))
        self.db.commit()

    def test_repeated_manual_allow_preserves_cursor_and_records_new_evidence(self):
        self.seed(1)
        before=self.progress()
        consent.set_consent(self.db,-1,True,'another explicit approval',True)
        self.assertEqual(self.progress(),before)
        self.assertEqual(self.db.execute('SELECT evidence FROM group_monitoring_audit').fetchall(),[('another explicit approval',)])

    def test_fresh_automatic_to_manual_preserves_cursor(self):
        self.seed(1,'auto:max-members:10',self.db.execute("SELECT datetime('now')").fetchone()[0])
        before=self.progress()
        consent.set_consent(self.db,-1,True,'manual override',True)
        self.assertEqual(self.progress(),before)
        self.assertEqual(self.db.execute('SELECT evidence FROM group_monitoring_consent').fetchone()[0],'manual override')

    def test_first_reenabled_expired_or_invalid_automatic_permission_recovers(self):
        for previous in (None,(0,'manual off','now'),(1,'auto:max-members:10','2000-01-01T00:00:00Z'),(1,'auto:max-members:10','invalid')):
            with self.subTest(previous=previous):
                self.db.execute('DELETE FROM group_monitoring_consent')
                self.db.execute("UPDATE chat_sync SET backfill_id=777,backfill_status='done',last_backfill_at=123,last_incremental_at=456,incremental_pending=0")
                self.db.execute('UPDATE chats SET backfill_done=1')
                if previous:self.seed(*previous)
                self.db.commit()
                consent.set_consent(self.db,-1,True,'manual override',True)
                self.assertEqual(self.db.execute('SELECT backfill_id,backfill_status,incremental_pending FROM chat_sync').fetchone(),(0,'pending',1))
                self.assertEqual(self.db.execute('SELECT backfill_done FROM chats').fetchone()[0],0)

    def test_repeated_revoke_does_not_reset_progress_or_grant_access(self):
        self.seed(0)
        before=self.progress()
        consent.set_consent(self.db,-1,False,'still revoked')
        self.assertEqual(self.progress(),before)
        self.assertFalse(consent.allowed(self.db,-1))


class DashboardConsentTests(unittest.TestCase):
    def setUp(self):
        dashboard_tests.DashboardTests.setUp(self)
        with sqlite3.connect(self.paths['telegram']) as c:
            c.execute("ALTER TABLE chats ADD COLUMN type TEXT DEFAULT 'group'")

    def mark_progress(self):
        for platform,path in self.paths.items():
            with sqlite3.connect(path) as c:
                c.execute("UPDATE memory_group_rescan SET cursor='777'")
                if platform=='telegram':
                    c.execute("UPDATE chat_sync SET backfill_id=777,backfill_status='done',last_backfill_at=123,last_incremental_at=456,incremental_pending=0")
                    c.execute('UPDATE chats SET backfill_done=1')
                    c.execute('CREATE TABLE IF NOT EXISTS attachment_group_rescan(chat_id INTEGER PRIMARY KEY,cursor INTEGER NOT NULL DEFAULT 0)')
                    c.execute('INSERT OR REPLACE INTO attachment_group_rescan VALUES(-1,777)')
        with self.app.state_db() as c:
            c.execute("UPDATE recovery SET status='waiting',attempts=2,retry_at=123,error='synthetic',anchor='2026-01-01',watermark=17,scan_cursor=19,candidate_anchor='2025-01-01',request_token='keep-this-token',pages=8")

    def progress(self):
        result={'jobs':self.app.recovery_status()}
        for platform,path in self.paths.items():
            with sqlite3.connect(path) as c:
                result[platform]=c.execute('SELECT * FROM memory_group_rescan ORDER BY chat_id').fetchall()
                if platform=='telegram':
                    result['sync']=c.execute('SELECT * FROM chat_sync').fetchall()
                    result['chats']=c.execute('SELECT * FROM chats').fetchall()
                    result['attachments']=c.execute('SELECT * FROM attachment_group_rescan').fetchall()
        return result

    def test_repeated_enable_all_batch_or_toggle_keeps_all_active_progress(self):
        self.app.enable_all()
        self.mark_progress()
        before=self.progress()
        for operation in (self.app.enable_all,
                          lambda:self.app.enable_batch([dict(platform='telegram',id='-1')]*2),
                          lambda:self.app.toggle('telegram','-1',True,initial=True),
                          lambda:self.app.toggle('whatsapp','1@g.us',True)):
            operation()
            self.assertEqual(self.progress(),before)

    def test_expired_automatic_toggle_recovers_and_publishes_manual_permission(self):
        self.app.enable_all()
        self.mark_progress()
        for platform,key in (('whatsapp','1@g.us'),('telegram','-1')):
            with sqlite3.connect(self.paths[platform]) as c:
                c.execute("UPDATE group_monitoring_consent SET evidence='auto:max-members:10',updated_at='2000-01-01T00:00:00Z'")
            self.app.toggle(platform,key,True)
            with sqlite3.connect(self.paths[platform]) as c:
                self.assertEqual(c.execute('SELECT cursor FROM memory_group_rescan').fetchone()[0],'')
                self.assertEqual(c.execute('SELECT evidence FROM group_monitoring_consent').fetchone()[0],'dashboard:manual-toggle')
            job=next(r for r in self.app.recovery_status() if r['platform']==platform)
            self.assertEqual((job['status'],job['attempts'],job['pages']),('queued',0,0))

    def test_fresh_automatic_enable_all_preserves_progress_but_expired_batch_recovers(self):
        self.app.enable_all()
        self.mark_progress()
        for path in self.paths.values():
            with sqlite3.connect(path) as c:
                c.execute("UPDATE group_monitoring_consent SET evidence='auto:max-members:10',updated_at=datetime('now')")
        before=self.progress()
        self.app.enable_all()
        self.assertEqual(self.progress(),before)
        with sqlite3.connect(self.paths['telegram']) as c:
            c.execute("UPDATE group_monitoring_consent SET evidence='auto:max-members:10',updated_at='invalid'")
        self.app.enable_batch([dict(platform='telegram',id='-1')])
        with sqlite3.connect(self.paths['telegram']) as c:
            self.assertEqual(c.execute('SELECT backfill_id FROM chat_sync').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT cursor FROM attachment_group_rescan').fetchone()[0],0)

    def test_explicit_recover_still_resets_all_recovery_cursors_and_job(self):
        self.app.enable_all()
        self.mark_progress()
        self.app.recover('telegram','-1')
        with sqlite3.connect(self.paths['telegram']) as c:
            self.assertEqual(c.execute('SELECT backfill_id FROM chat_sync').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT cursor FROM memory_group_rescan').fetchone()[0],'')
            self.assertEqual(c.execute('SELECT cursor FROM attachment_group_rescan').fetchone()[0],0)
        job=next(r for r in self.app.recovery_status() if r['platform']=='telegram')
        self.assertEqual((job['status'],job['attempts'],job['pages']),('queued',0,0))
        self.assertNotEqual(job['request_token'],'keep-this-token')

    def test_simultaneous_enable_requests_schedule_recovery_only_once(self):
        barrier=threading.Barrier(2)
        calls,errors=[],[]
        original=self.app._recover
        def recover(c,platform,key):
            calls.append((platform,key))
            time.sleep(.03)
            return original(c,platform,key)
        def enable():
            try:
                barrier.wait(timeout=5)
                self.app.toggle('telegram','-1',True)
            except Exception as error:errors.append(type(error).__name__)
        with patch.object(self.app,'_recover',side_effect=recover):
            threads=[threading.Thread(target=enable) for _ in range(2)]
            for thread in threads:thread.start()
            for thread in threads:thread.join(timeout=10)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors,[])
        self.assertEqual(calls,[('telegram',-1)])
