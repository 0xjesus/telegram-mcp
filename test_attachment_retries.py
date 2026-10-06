import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch
import test_attachments as fixture

class AttachmentRetries(unittest.TestCase):
    setUp=fixture.Attachments.setUp
    add=fixture.Attachments.add
    def force_due(self):
        self.db.execute('UPDATE attachment_queue SET retry_at=0');self.db.commit()
    def test_unknown_download_failures_stop_after_three_and_restart(self):
        self.add(hash='');calls=[]
        class MCP:
            def call(inner,*args):calls.append(1);raise RuntimeError('inaccessible')
        for _ in range(5):
            self.force_due();self.w.run_once(self.db,MCP(),Path(self.tmp.name))
            self.db.close();self.db=sqlite3.connect(self.path);self.addCleanup(self.db.close);self.w.initialize(self.db)
        self.assertEqual(len(calls),3)
        self.assertEqual(self.db.execute('SELECT count(*) FROM attachment_analysis').fetchone()[0],0)
    def test_unknown_oversized_media_is_terminal_without_download(self):
        self.add(hash='');self.db.execute('UPDATE messages SET media_size=?',(self.w.MAX_FILE+1,));self.db.commit()
        self.w.run_once(self.db,object(),Path(self.tmp.name));self.force_due()
        self.assertEqual(self.w.pending(self.db),[])
    def test_failure_after_checkpoint_preserves_text_but_stops(self):
        self.add();row=self.w.pending(self.db)[0]
        self.w.save(self.db,row,dict(text='prior evidence',status='partial',cloud_pending=True,cloud_next=8))
        for _ in range(5):
            self.w.save(self.db,row,dict(text='',status='failed',reason='missing_configuration'))
        result=self.db.execute('SELECT * FROM attachment_analysis').fetchone()
        self.assertEqual(result['text'],'prior evidence');self.assertEqual(result['status'],'partial')
        self.assertFalse(json.loads(result['metadata'])['cloud_pending'])
        self.force_due();self.assertEqual(self.w.pending(self.db),[])
    def test_cloud_errors_are_bounded_but_budget_wait_is_not_failure(self):
        self.add();row=self.w.pending(self.db)[0]
        for _ in range(4):self.w.save(self.db,row,dict(text='local',status='partial',cloud_pending=True,reason='cloud_budget'))
        self.assertEqual(self.db.execute('SELECT attempts FROM attachment_analysis').fetchone()[0],0)
        for _ in range(3):self.w.save(self.db,row,dict(text='local',status='partial',cloud_pending=True,reason='cloud_preparation_error'))
        self.force_due();self.assertEqual(self.w.pending(self.db),[])
    def test_missing_cloud_configuration_stops_after_three_cycles(self):
        self.add();calls=[]
        class MCP:
            def call(inner,name,args):
                calls.append(1);Path(args['output_path']).write_text('local evidence')
                return {'media_hash':'document:42'}
        with patch.dict('os.environ',{'TG_ATTACHMENT_BACKEND':'openai'},clear=True),patch.object(self.w,'analyze',return_value=dict(text='local evidence',status='done')):
            for _ in range(5):self.force_due();self.w.run_once(self.db,MCP(),Path(self.tmp.name))
        self.assertEqual(len(calls),3)
        result=self.db.execute('SELECT * FROM attachment_analysis').fetchone()
        self.assertEqual(result['text'],'local evidence');self.assertEqual(result['attempts'],3)
        self.assertFalse(json.loads(result['metadata'])['cloud_pending'])
    def test_reenabled_group_rediscovers_completed_attachments_in_bounded_pages(self):
        self.db.execute("INSERT INTO chats VALUES(-1,'group','group')")
        self.db.execute("INSERT INTO group_monitoring_consent VALUES(-1,1,'manual',datetime('now'))");self.db.commit()
        self.add(chat=-1);row=self.w.pending(self.db)[0]
        self.w.save(self.db,row,dict(text='completed',status='done'))
        self.db.execute('UPDATE group_monitoring_consent SET allowed=0');self.db.commit()
        self.assertEqual(self.w.pending(self.db),[])
        self.db.execute('UPDATE group_monitoring_consent SET allowed=1');self.db.commit()
        self.assertEqual(len(self.w.pending(self.db)),1)
    def test_explicit_group_recovery_has_durable_bounded_cursor(self):
        self.db.execute("INSERT INTO chats VALUES(-1,'group','group')")
        self.db.execute("INSERT INTO group_monitoring_consent VALUES(-1,1,'manual',datetime('now'))")
        self.db.executemany("INSERT INTO messages(chat_id,id,date,text) VALUES(-1,?,datetime('now'),'old')",[(n,) for n in range(1,1001)]);self.db.commit()
        self.w.request_rescan(self.db,-1);self.db.commit()
        self.w.pending(self.db)
        self.assertLessEqual(self.db.execute('SELECT cursor FROM attachment_group_rescan').fetchone()[0],200)

if __name__=='__main__':unittest.main()
