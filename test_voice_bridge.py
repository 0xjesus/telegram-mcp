import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from voice_bridge import Bridge


class VoiceBridgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);(self.root/'media').mkdir()
        with sqlite3.connect(self.root/'messages.db') as c:
            c.executescript("CREATE TABLE messages(chat_id INTEGER,id INTEGER,text TEXT,date TEXT,media_type TEXT,media_path TEXT,media_hash TEXT,deleted INTEGER DEFAULT 0,PRIMARY KEY(chat_id,id)); CREATE TABLE transcripts(chat_id INTEGER,id INTEGER,text TEXT,status TEXT,attempts INTEGER,error TEXT,updated_at TEXT,PRIMARY KEY(chat_id,id)); CREATE TABLE chats(id INTEGER,type TEXT); CREATE TABLE group_monitoring_consent(chat_id INTEGER,allowed INTEGER,evidence TEXT,updated_at TEXT);")
        self.bridge=Bridge(wa=self.root/'missing',tg=self.root)

    def save(self,kind='video'):
        p=self.root/'media'/'clip';p.write_bytes(b'synthetic')
        with sqlite3.connect(self.root/'messages.db') as c:c.execute("INSERT INTO messages VALUES(7,1,'Caption','2026-10-06T00:00:00Z',?,?, 'document:1',0)",(kind,str(p)))
        return p

    def test_video_and_audio_are_pending(self):
        for kind in ('video','audio','video_note'):
            with self.subTest(kind=kind):
                with sqlite3.connect(self.root/'messages.db') as c:c.execute('DELETE FROM messages')
                self.save(kind)
                jobs=self.bridge.pending()['jobs']
                self.assertEqual(len(jobs),1)
                self.assertEqual(jobs[0]['media_hash'],'document:1')

    def test_caption_survives_video_transcript(self):
        self.save()
        job=self.bridge.pending()['jobs'][0]
        self.bridge.handle({'op':'complete','job':job,'text':'Spoken words'})
        with sqlite3.connect(self.root/'messages.db') as c:
            text=c.execute('SELECT text FROM messages').fetchone()[0]
        self.assertEqual(text,'Caption\n[Audio del video] Spoken words')

    def test_replaced_media_rejects_stale_transcription(self):
        self.save();job=self.bridge.pending()['jobs'][0]
        with sqlite3.connect(self.root/'messages.db') as c:c.execute("UPDATE messages SET media_hash='document:2'")
        with self.assertRaises(ValueError):self.bridge.handle({'op':'complete','job':job,'text':'stale'})
        with sqlite3.connect(self.root/'messages.db') as c:self.assertEqual(c.execute('SELECT count(*) FROM transcripts').fetchone()[0],0)

    def test_audio_operation_extracts_real_video_audio_and_cleans_temp(self):
        import shutil,subprocess
        if not shutil.which('ffmpeg'):self.skipTest('ffmpeg required')
        path=self.save()
        subprocess.run(['ffmpeg','-v','error','-y','-f','lavfi','-i','color=c=red:s=64x64:d=1','-f','lavfi','-i','sine=frequency=440:duration=1','-c:v','mpeg4','-c:a','aac','-f','mp4',str(path)],check=True)
        job=self.bridge.pending()['jobs'][0]
        output=self.bridge.handle({'op':'audio','job':job})
        self.assertTrue(output.startswith(b'OggS'))
        self.assertFalse(list((self.root/'media').glob('transcribe-*.ogg')))
