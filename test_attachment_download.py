import asyncio
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
from test_sync_history import SyncHistoryTests, message

class AttachmentDownload(unittest.IsolatedAsyncioTestCase):
    setUp=SyncHistoryTests.setUp
    def setup_media(self,size=5):
        from telethon import types
        m=message(1)
        m.media=types.MessageMediaDocument(document=types.Document(id=42,access_hash=123,file_reference=b'',date=m.date,mime_type='text/plain',size=size,dc_id=1,attributes=[types.DocumentAttributeFilename('note.txt')]))
        c=self.daemon.db();self.daemon.upsert_message(c,7,m);c.commit();c.close()
        self.daemon.S.client.get_messages=AsyncMock(return_value=m)
        return m
    async def test_unknown_identity_resolved_and_size_bounded(self):
        m=self.setup_media()
        c=self.daemon.db();c.execute("UPDATE messages SET media_hash='' WHERE id=1");c.commit();c.close()
        async def chunks(*args,**kwargs):yield b'hello'
        self.daemon.S.client.iter_download=chunks
        root=Path(self.tmp.name)/'attachment-tmp';root.mkdir(exist_ok=True)
        result=await self.daemon.t_download_attachment(dict(chat='7',message_id=1,expected_media_hash='',output_path=str(root/'out')))
        self.assertEqual(result['media_hash'],'document:42');self.assertEqual((root/'out').read_bytes(),b'hello')
        c=self.daemon.db();self.assertEqual(c.execute('SELECT media_hash FROM messages').fetchone()[0],'document:42');c.close()
    async def test_replacement_and_tombstone_refuse_download(self):
        self.setup_media()
        root=Path(self.tmp.name)/'attachment-tmp';root.mkdir(exist_ok=True)
        with self.assertRaisesRegex(RuntimeError,'media_changed'):
            await self.daemon.t_download_attachment(dict(chat='7',message_id=1,expected_media_hash='document:old',output_path=str(root/'out')))
        c=self.daemon.db();c.execute('UPDATE messages SET deleted=1');c.commit();c.close()
        with self.assertRaisesRegex(RuntimeError,'message_unavailable'):
            await self.daemon.t_download_attachment(dict(chat='7',message_id=1,expected_media_hash='document:42',output_path=str(root/'out')))
    async def test_stream_limit_cleans_output(self):
        self.setup_media()
        async def chunks(*args,**kwargs):
            for _ in range(51):yield b'x'*1024*1024
        self.daemon.S.client.iter_download=chunks
        root=Path(self.tmp.name)/'attachment-tmp';root.mkdir(exist_ok=True)
        with self.assertRaisesRegex(RuntimeError,'file_size_limit'):
            await self.daemon.t_download_attachment(dict(chat='7',message_id=1,expected_media_hash='document:42',output_path=str(root/'out')))
        self.assertFalse((root/'out').exists())

    async def test_manual_download_refuses_oversized_metadata(self):
        self.setup_media(size=51*1024*1024)
        with self.assertRaisesRegex(RuntimeError,'file_size_limit'):
            await self.daemon.t_download({'chat':'7','message_id':1})

    async def test_manual_download_aborts_oversized_stream(self):
        self.setup_media(size=5)
        async def download(m,file,progress_callback):
            Path(file).write_bytes(b'small')
            await progress_callback(51*1024*1024,51*1024*1024)
            return file
        self.daemon.S.client.download_media=download
        with self.assertRaisesRegex(RuntimeError,'file_size_limit'):
            await self.daemon.t_download({'chat':'7','message_id':1})
        self.assertFalse(any((Path(self.tmp.name)/'media').rglob('attachment')))

    async def test_manual_oversized_replacement_updates_identity_first(self):
        m=self.setup_media()
        m.media.document.id=43;m.media.document.size=51*1024*1024
        with self.assertRaisesRegex(RuntimeError,'file_size_limit'):
            await self.daemon.t_download({'chat':'7','message_id':1})
        c=self.daemon.db()
        self.assertEqual(c.execute('SELECT media_hash FROM messages WHERE chat_id=7 AND id=1').fetchone()[0],'document:43')
        c.close()

    async def test_manual_missing_message_records_tombstone(self):
        self.setup_media();self.daemon.S.client.get_messages=AsyncMock(return_value=None)
        with self.assertRaisesRegex(RuntimeError,'message_has_no_attachment'):
            await self.daemon.t_download({'chat':'7','message_id':1})
        from message_extras import is_deleted
        c=self.daemon.db();self.assertTrue(is_deleted(c,7,1));c.close()

if __name__=='__main__': unittest.main()
