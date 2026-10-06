"""Synthetic Telegram consent regressions; no authenticated client or user data."""
import unittest
from unittest.mock import AsyncMock
from test_sync_history import SyncHistoryTests, message, FakeClient

class GroupConsentTests(unittest.IsolatedAsyncioTestCase):
    setUp = SyncHistoryTests.setUp

    async def test_group_capture_denied_without_approval(self):
        c=self.daemon.db()
        c.execute("INSERT INTO chats(id,type,title) VALUES(-7,'group','Synthetic group')")
        self.daemon.upsert_message(c,-7,message(1));c.commit()
        self.assertEqual(c.execute('SELECT count(*) FROM messages WHERE chat_id=-7').fetchone()[0],0)
        c.close()

    async def test_group_sync_does_not_fetch_without_approval(self):
        c=self.daemon.db();c.execute("INSERT INTO chats(id,type,title) VALUES(-7,'group','Synthetic group')");c.commit();c.close()
        self.daemon.S.client=FakeClient([1])
        await self.daemon.sync_chat(-7,10,False)
        self.assertEqual(self.daemon.S.client.calls,[])

    async def test_approval_revoke_and_direct_chat_channel_isolation(self):
        from consent import set_consent
        c=self.daemon.db()
        c.executemany('INSERT INTO chats(id,type,title) VALUES(?,?,?)',[(-7,'group','Synthetic group'),(-8,'supergroup','Other group'),(-9,'channel','Channel')]);c.commit()
        with self.assertRaises(ValueError):set_consent(c,-7,True,'test evidence')
        set_consent(c,-7,True,'explicit synthetic approval',True)
        for cid in (-7,-8,-9,7,-999):self.daemon.upsert_message(c,cid,message(1))
        c.commit()
        self.assertEqual({r[0] for r in c.execute('SELECT chat_id FROM messages')},{-7,-9,7})
        # Insert and update are blocked atomically even outside the daemon.
        c.execute("INSERT INTO messages(chat_id,id,date,text) VALUES(-8,2,'2026-01-01','blocked')")
        self.assertEqual(c.execute('SELECT count(*) FROM messages WHERE chat_id=-8').fetchone()[0],0)
        set_consent(c,-7,False,'revoked')
        c.execute("UPDATE messages SET text='blocked edit' WHERE chat_id=-7")
        c.execute("INSERT INTO transcripts(chat_id,id,text,status) VALUES(-7,1,'blocked','done')");c.commit()
        self.assertEqual(c.execute('SELECT text FROM messages WHERE chat_id=-7').fetchone()[0],'synthetic 1')
        self.assertEqual(c.execute('SELECT count(*) FROM transcripts WHERE chat_id=-7').fetchone()[0],0)
        c.close()
        self.assertEqual({x['chat_id'] for x in (await self.daemon.t_search({'query':'synthetic'}))['messages']},{-9,7})
        for fn,args in [(self.daemon.t_get_messages,{'chat':'-7'}),(self.daemon.t_context,{'chat':'-7','message_id':1}),(self.daemon.t_download,{'chat':'-7','message_id':1}),(self.daemon.t_info,{'chat':'-7'})]:
            with self.assertRaisesRegex(RuntimeError,'monitoring_not_authorized'):await fn(args)
        listed=await self.daemon.t_list_chats({})
        self.assertIsNone(next(r for r in listed['chats'] if r['chat_id']==-7)['last_message'])
        c=self.daemon.db();set_consent(c,-8,True,'different group approval',True);c.close()
        listed=await self.daemon.t_list_chats({})
        self.assertIsNone(next(r for r in listed['chats'] if r['chat_id']==-7)['last_message'])

    async def test_revocation_during_sync_discards_page_without_cursor_advance(self):
        from consent import set_consent
        c=self.daemon.db();c.execute("INSERT INTO chats(id,type,title) VALUES(-7,'group','Synthetic group')");c.commit()
        set_consent(c,-7,True,'synthetic',True)
        self.daemon.ensure_chat_sync(c,-7);c.commit()
        set_consent(c,-7,False,'revoked')
        self.daemon.commit_sync_page(-7,[message(3)],False,10)
        self.assertEqual(c.execute('SELECT backfill_id FROM chat_sync WHERE chat_id=-7').fetchone()[0],0)
        self.assertEqual(c.execute('SELECT count(*) FROM messages WHERE chat_id=-7').fetchone()[0],0);c.close()
        self.assertNotIn(-7,[r['id'] for r in self.daemon.sync_chat_inventory(True)])
        self.assertNotIn(-7,[r['id'] for r in self.daemon.sync_chat_inventory(False)])

    async def test_voice_bridge_denies_queued_and_inflight_group_audio(self):
        from consent import set_consent
        from voice_bridge import Bridge
        c=self.daemon.db();c.execute("INSERT INTO chats(id,type,title) VALUES(-7,'group','Synthetic group')");c.commit()
        set_consent(c,-7,True,'synthetic',True)
        self.daemon.upsert_message(c,-7,message(1))
        c.execute("UPDATE messages SET media_type='voice',text='' WHERE chat_id=-7");c.commit()
        bridge=Bridge(wa=self.tmp.name+'/absent',tg=self.tmp.name)
        self.assertEqual(len(bridge.pending()['jobs']),1)
        job=bridge.pending()['jobs'][0]
        set_consent(c,-7,False,'revoked')
        self.assertEqual(bridge.pending()['jobs'],[])
        for op in ('complete','audio'):
            request={'op':op,'job':job}
            if op=='complete':request['text']='forbidden result'
            with self.assertRaisesRegex(ValueError,'monitoring_not_authorized'):bridge.handle(request)
        self.assertEqual(c.execute('SELECT count(*) FROM transcripts').fetchone()[0],0);c.close()

    async def test_revoke_during_download_removes_new_media(self):
        from consent import set_consent
        from pathlib import Path
        from types import SimpleNamespace
        c=self.daemon.db();c.execute("INSERT INTO chats(id,type,title) VALUES(-7,'group','Synthetic group')");c.commit()
        set_consent(c,-7,True,'synthetic',True)
        from telethon import types
        m=message(1)
        m.media=types.MessageMediaDocument(document=types.Document(id=44,access_hash=1,file_reference=b'',date=m.date,mime_type='text/plain',size=4,dc_id=1,attributes=[]))
        self.daemon.S.client.get_messages=AsyncMock(return_value=m)
        async def download(m,file,progress_callback):
            Path(file).write_bytes(b'synthetic private attachment')
            set_consent(c,-7,False,'revoked during transfer')
            return file
        self.daemon.S.client.download_media=download
        with self.assertRaisesRegex(RuntimeError,'monitoring_not_authorized'):
            await self.daemon.t_download({'chat':'-7','message_id':1})
        self.assertEqual([p for p in (Path(self.tmp.name)/'media').rglob('*') if p.is_file()],[])
        c.close()

    async def test_legacy_transcriber_filters_unapproved_groups(self):
        import importlib.util
        from pathlib import Path
        from consent import set_consent
        spec=importlib.util.spec_from_file_location('synthetic_tg_asr',Path(__file__).parent/'tools/transcribe_tg.py')
        transcriber=importlib.util.module_from_spec(spec);spec.loader.exec_module(transcriber)
        c=self.daemon.db();c.execute("INSERT INTO chats(id,type,title) VALUES(-7,'group','Synthetic group')");c.commit()
        set_consent(c,-7,True,'synthetic',True)
        for cid in (7,-7):
            self.daemon.upsert_message(c,cid,message(1))
            c.execute("UPDATE messages SET media_type='voice',media_path='synthetic' WHERE chat_id=?",(cid,))
        c.commit();set_consent(c,-7,False,'revoked')
        self.assertEqual([r['chat_id'] for r in transcriber.pending(c)],[7]);c.close()
