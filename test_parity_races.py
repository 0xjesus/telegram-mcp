"""Synthetic race regressions; imports the daemon with an isolated temporary store."""
import datetime as dt
import unittest
from telethon import types
from message_extras import capture_reactions
import test_sync_history as history


def media_message(doc_id, edited=None):
    m=history.message(1)
    m.message=''
    m.edit_date=edited
    m.media=types.MessageMediaDocument(document=types.Document(id=doc_id,access_hash=123,file_reference=b'',date=m.date,mime_type='audio/ogg',size=5,dc_id=1,attributes=[types.DocumentAttributeAudio(duration=1,voice=True)]))
    return m


class ParityRaces(unittest.IsolatedAsyncioTestCase):
    setUp=history.SyncHistoryTests.setUp

    def save(self,m):
        c=self.daemon.db()
        try:
            self.daemon.upsert_message(c,7,m)
            c.commit()
        finally:c.close()

    async def test_live_reaction_removal_wins_over_inflight_history(self):
        old=history.message(1)
        old.reactions=types.MessageReactions(results=[types.ReactionCount(types.ReactionEmoji('👍'),1)])
        self.save(old)
        async def history_with_live_update(*args,**kwargs):
            yield old
            fresh=history.message(1)
            c=self.daemon.db()
            try:
                capture_reactions(c,7,fresh)
                c.commit()
            finally:c.close()
        self.daemon.S.client.iter_messages=history_with_live_update
        await self.daemon.sync_chat(7,100,incremental=False)
        c=self.daemon.db()
        self.assertEqual(c.execute('SELECT count(*) FROM message_reactions').fetchone()[0],0)
        c.close()

    async def test_replacement_clears_transcript_and_inline_text_atomically(self):
        self.save(media_message(42))
        c=self.daemon.db()
        c.execute("UPDATE messages SET text='old transcription',media_path='/tmp/old.oga' WHERE id=1")
        c.execute("INSERT INTO transcripts(chat_id,id,text,status) VALUES(7,1,'old transcription','done')")
        c.commit()
        replacement=media_message(43,dt.datetime(2026,2,1,tzinfo=dt.timezone.utc))
        self.daemon.upsert_message(c,7,replacement)
        self.assertEqual(c.execute('SELECT count(*) FROM transcripts').fetchone()[0],0)
        self.assertEqual(tuple(c.execute('SELECT text,media_path,media_hash FROM messages').fetchone()),('',None,'document:43'))
        c.rollback()
        self.assertEqual(c.execute('SELECT text FROM transcripts').fetchone()[0],'old transcription')
        self.assertEqual(c.execute('SELECT media_hash FROM messages').fetchone()[0],'document:42')
        c.close()

    async def test_older_edited_message_cannot_restore_replaced_media(self):
        old=media_message(42,dt.datetime(2026,1,2,tzinfo=dt.timezone.utc))
        self.save(old)
        self.save(media_message(43,dt.datetime(2026,2,1,tzinfo=dt.timezone.utc)))
        self.save(old)
        c=self.daemon.db()
        self.assertEqual(c.execute('SELECT media_hash FROM messages').fetchone()[0],'document:43')
        c.close()

    async def test_same_second_live_replacement_wins_over_inflight_history(self):
        stamp=dt.datetime(2026,1,2,tzinfo=dt.timezone.utc)
        old=media_message(42,stamp)
        self.save(old)
        async def history_with_live_edit(*args,**kwargs):
            yield old
            self.save(media_message(43,stamp))
        self.daemon.S.client.iter_messages=history_with_live_edit
        await self.daemon.sync_chat(7,100,incremental=False)
        c=self.daemon.db()
        self.assertEqual(c.execute('SELECT media_hash FROM messages').fetchone()[0],'document:43')
        c.close()

    async def test_same_second_stale_redelivery_cannot_restore_retired_media_after_restart(self):
        stamp=dt.datetime(2026,1,2,tzinfo=dt.timezone.utc)
        old=media_message(42,stamp)
        self.save(old)
        self.save(media_message(43,stamp))
        self.daemon.init_db()
        self.save(old)
        c=self.daemon.db()
        self.assertEqual(c.execute('SELECT media_hash FROM messages').fetchone()[0],'document:43')
        c.close()
        # Reusing that document in a genuinely later edit is allowed.
        self.save(media_message(42,stamp+dt.timedelta(seconds=1)))
        c=self.daemon.db()
        self.assertEqual(c.execute('SELECT media_hash FROM messages').fetchone()[0],'document:42')
        c.close()

    async def test_download_discovered_replacement_invalidates_transcript(self):
        from unittest.mock import AsyncMock
        from pathlib import Path
        self.save(media_message(42))
        c=self.daemon.db()
        c.execute("UPDATE messages SET text='old transcription' WHERE id=1")
        c.execute("INSERT INTO transcripts(chat_id,id,text,status) VALUES(7,1,'old transcription','done')")
        c.commit();c.close()
        self.daemon.S.client.get_messages=AsyncMock(return_value=media_message(43,dt.datetime(2026,2,1,tzinfo=dt.timezone.utc)))
        root=Path(self.tmp.name)/'attachment-tmp';root.mkdir(exist_ok=True)
        with self.assertRaisesRegex(RuntimeError,'media_changed'):
            await self.daemon.t_download_attachment(dict(chat='7',message_id=1,expected_media_hash='document:42',output_path=str(root/'out')))
        c=self.daemon.db()
        self.assertEqual(c.execute('SELECT count(*) FROM transcripts').fetchone()[0],0)
        self.assertEqual(tuple(c.execute('SELECT text,media_hash FROM messages').fetchone()),('','document:43'))
        c.close()

    async def test_download_media_removed_keeps_the_existing_text_message(self):
        from unittest.mock import AsyncMock
        from pathlib import Path
        self.save(media_message(42))
        changed=history.message(1)
        changed.message='media removed, text remains'
        changed.edit_date=dt.datetime(2026,2,1,tzinfo=dt.timezone.utc)
        self.daemon.S.client.get_messages=AsyncMock(return_value=changed)
        root=Path(self.tmp.name)/'attachment-tmp';root.mkdir(exist_ok=True)
        with self.assertRaisesRegex(RuntimeError,'media_changed'):
            await self.daemon.t_download_attachment(dict(chat='7',message_id=1,expected_media_hash='document:42',output_path=str(root/'out')))
        c=self.daemon.db()
        self.assertEqual(tuple(c.execute('SELECT text,deleted,media_hash FROM messages').fetchone()),('media removed, text remains',0,''))
        c.close()
