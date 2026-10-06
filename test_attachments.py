import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch


class Attachments(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'messages.db'
        self.db=sqlite3.connect(self.path); self.addCleanup(self.db.close)
        self.db.executescript('''CREATE TABLE chats(id INTEGER PRIMARY KEY,type TEXT,title TEXT);
        INSERT INTO chats VALUES(1,'user','test');
        CREATE TABLE messages(chat_id INTEGER,id INTEGER,date TEXT,text TEXT,sender_id INTEGER,
            media_type TEXT,media_name TEXT,deleted INTEGER DEFAULT 0,PRIMARY KEY(chat_id,id));''')
        from tools.attachments import worker
        self.w=worker; worker.initialize(self.db)
    def add(self,mid=1,chat=1,hash='document:42',kind='document'):
        self.db.execute("INSERT INTO messages(chat_id,id,date,media_type,media_name,media_hash) VALUES(?,?,datetime('now'),?,'note.txt',?)",(chat,mid,kind,hash));self.db.commit()
    def test_durable_bounded_discovery(self):
        self.db.executemany("INSERT INTO messages(chat_id,id,date,text) VALUES(1,?,datetime('now'),'old')",[(n,) for n in range(1,2001)])
        self.db.commit()
        for _ in range(3): self.assertEqual(self.w.pending(self.db),[])
        cursor=int(self.db.execute("SELECT value FROM attachment_meta WHERE key='discovery_cursor'").fetchone()[0])
        self.assertLessEqual(cursor,600)
        self.add(3000)
        self.assertEqual(len(self.w.pending(self.db,99)),1)
        self.db.close(); self.db=sqlite3.connect(self.path);self.addCleanup(self.db.close);self.w.initialize(self.db)
        self.assertEqual(len(self.w.pending(self.db)),1)
    def test_save_refuses_deleted_replaced_revoked_and_unknown(self):
        for change in ('deleted=1',"media_hash='other'"):
            self.add(); row=self.w.pending(self.db)[0]
            self.db.execute('UPDATE messages SET '+change+' WHERE id=1');self.db.commit()
            self.assertFalse(self.w.save(self.db,row,dict(text='secret',status='done')))
            self.db.execute('DELETE FROM messages');self.db.commit()
        self.add(hash='');row=self.w.pending(self.db)[0]
        self.assertFalse(self.w.save(self.db,row,dict(text='unknown',status='done')))
    def test_edit_invalidates_and_delete_cleans(self):
        self.add();row=self.w.pending(self.db)[0]
        self.assertTrue(self.w.save(self.db,row,dict(text='old',status='done')))
        self.db.execute("UPDATE messages SET media_hash='document:43' WHERE id=1");self.db.commit()
        self.assertEqual(self.db.execute('SELECT count(*) FROM attachment_analysis').fetchone()[0],0)
        self.assertEqual(self.w.pending(self.db)[0]['media_hash'],'document:43')
        self.db.execute('DELETE FROM messages');self.db.commit()
        self.assertEqual(self.w.pending(self.db),[])
    def test_local_content_survives_cloud_failure_and_budget_wait(self):
        self.add();row=self.w.pending(self.db)[0]
        self.w.save(self.db,row,dict(text='local',status='partial',cloud_pending=True,cloud_next=8,retry_at=9999999999))
        self.assertEqual(self.w.pending(self.db),[])
        self.w.save(self.db,row,dict(text='',status='failed',reason='CloudError'))
        r=self.db.execute('SELECT * FROM attachment_analysis').fetchone()
        self.assertEqual(r['text'],'local');self.assertEqual(r['status'],'partial')
        self.assertEqual(json.loads(r['metadata'])['cloud_next'],8)
    def test_completed_before_discovery_does_not_repeat(self):
        self.db.executemany("INSERT INTO messages(chat_id,id,date,text) VALUES(1,?,datetime('now'),'old')",[(n,) for n in range(1,601)])
        self.db.commit();self.add(700)
        row=self.w.pending(self.db)[0];self.w.save(self.db,row,dict(text='finished',status='done'))
        for _ in range(5):self.assertEqual(self.w.pending(self.db),[])
    def test_stale_automatic_consent_excluded(self):
        self.db.execute("INSERT INTO chats VALUES(-1,'group','group')")
        self.db.execute("INSERT INTO group_monitoring_consent VALUES(-1,1,'auto:max-members:10',datetime('now','-901 seconds'))")
        # Old caches can contain messages written while automatic consent was fresh.
        self.db.execute('DROP TRIGGER monitoring_guard_messages_insert')
        self.add(chat=-1)
        self.assertEqual(self.w.pending(self.db),[])
    def test_batch_clamped_to_four(self):
        for mid in range(1,10):self.add(mid)
        self.assertEqual(len(self.w.pending(self.db,100)),4)
    def test_cooldown_does_not_consume_attempts(self):
        self.add()
        class MCP:
            def call(inner,*args):raise self.w.DownloadDeferred('platform cooldown')
        self.w.run_once(self.db,MCP(),Path(self.tmp.name))
        self.assertEqual(self.db.execute('SELECT count(*) FROM attachment_analysis').fetchone()[0],0)
        self.assertEqual(self.w.pending(self.db),[])
    def test_restart_resumes_cloud_checkpoint_without_losing_local_text(self):
        self.add();row=self.w.pending(self.db)[0]
        self.w.save(self.db,row,dict(text='local\nprior cloud',status='partial',cloud_pending=True,
            cloud_version='openai-v1',cloud_next=8,cloud_text='prior cloud',retry_at=0))
        self.db.close();self.db=sqlite3.connect(self.path);self.addCleanup(self.db.close);self.w.initialize(self.db)
        class MCP:
            def call(inner,name,args):
                Path(args['output_path']).write_text('local');return {'media_hash':'document:42'}
        def enrich(path,filename,kind,local,client,previous):
            self.assertEqual(previous['cloud_next'],8)
            self.assertEqual(previous['cloud_text'],'prior cloud')
            self.assertEqual(local['text'],'local')
            return dict(text='local\nprior cloud\nnew cloud',status='done',cloud_pending=False,cloud_next=9,cloud_version='openai-v1')
        with patch.dict('os.environ',TG_ATTACHMENT_BACKEND='openai',TG_OPENAI_KEY_FILE='/nonexistent',TG_ATTACHMENT_CLOUD_DB=str(Path(self.tmp.name)/'cloud.db')),patch.object(self.w,'analyze',return_value=dict(text='local',status='done')),patch('tools.attachments.cloud.enrich',side_effect=enrich):
            self.w.run_once(self.db,MCP(),Path(self.tmp.name))
        result=self.db.execute('SELECT * FROM attachment_analysis').fetchone()
        self.assertEqual(result['status'],'done')
        self.assertIn('prior cloud',result['text']);self.assertIn('new cloud',result['text'])
    def test_run_local_and_cleanup(self):
        self.add()
        class MCP:
            def call(inner,name,args):
                Path(args['output_path']).write_text('synthetic local evidence')
                return {'path':args['output_path'],'media_hash':'document:42'}
        with patch.object(self.w,'analyze',return_value=dict(text='synthetic local evidence',status='done')):
            self.w.run_once(self.db,MCP(),Path(self.tmp.name))
        self.assertEqual(self.db.execute('SELECT text FROM attachment_analysis').fetchone()[0],'synthetic local evidence')
        self.assertEqual(list(Path(self.tmp.name).glob('tmp*')),[])
    def test_cloud_revocation_drops_checkpoint(self):
        self.db.execute("INSERT INTO chats VALUES(-1,'group','group')")
        self.db.execute("INSERT INTO group_monitoring_consent VALUES(-1,1,'manual',datetime('now'))")
        self.add(chat=-1)
        class MCP:
            def call(inner,name,args):
                Path(args['output_path']).write_text('synthetic');return {'path':args['output_path'],'media_hash':'document:42'}
        def enrich(*args,**kwargs):
            self.db.execute('UPDATE group_monitoring_consent SET allowed=0');self.db.commit()
            self.assertFalse(args[4].authorize())
            return dict(text='private',status='done')
        with patch.dict('os.environ',TG_ATTACHMENT_BACKEND='openai',TG_OPENAI_KEY_FILE='/nonexistent',TG_ATTACHMENT_CLOUD_DB=str(Path(self.tmp.name)/'cloud.db')),patch.object(self.w,'analyze',return_value=dict(text='local',status='done')),patch('tools.attachments.cloud.enrich',side_effect=enrich):
            self.w.run_once(self.db,MCP(),Path(self.tmp.name))
        self.assertEqual(self.db.execute('SELECT count(*) FROM attachment_analysis').fetchone()[0],0)

if __name__=='__main__': unittest.main()
