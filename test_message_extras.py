import sqlite3
import unittest
from types import SimpleNamespace
from telethon import types
from message_extras import install, capture_reactions, is_deleted, record_deleted


class ExtrasTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.executescript("CREATE TABLE messages(chat_id INTEGER,id INTEGER,deleted INTEGER DEFAULT 0, PRIMARY KEY(chat_id,id)); CREATE TABLE chats(id INTEGER PRIMARY KEY,type TEXT); CREATE TABLE group_monitoring_consent(chat_id INTEGER PRIMARY KEY,allowed INTEGER,evidence TEXT,updated_at TEXT); INSERT INTO chats VALUES(7,'user'); INSERT INTO messages VALUES(7,1,0);")
        install(self.db)

    def test_reaction_snapshot_replaces_and_removes(self):
        def msg(emoji):
            return SimpleNamespace(id=1,reactions=SimpleNamespace(results=[types.ReactionCount(types.ReactionEmoji(emoji),2)],recent_reactions=[]))
        capture_reactions(self.db,7,msg('👍'))
        self.assertIn('👍',self.db.execute('SELECT text FROM message_reactions').fetchone()[0])
        capture_reactions(self.db,7,msg('❤️'))
        self.assertNotIn('👍',self.db.execute('SELECT text FROM message_reactions').fetchone()[0])
        capture_reactions(self.db,7,SimpleNamespace(id=1,reactions=None))
        self.assertEqual(self.db.execute('SELECT count(*) FROM message_reactions').fetchone()[0],0)

    def test_tombstones_exist_without_cached_message(self):
        record_deleted(self.db,7,[1,99])
        self.assertTrue(is_deleted(self.db,7,99))
        self.assertEqual(self.db.execute('SELECT deleted FROM messages').fetchone()[0],1)

    def test_revoked_group_reactions_are_not_captured(self):
        self.db.execute("INSERT INTO chats VALUES(-7,'group')")
        self.db.execute('INSERT INTO messages VALUES(-7,1,0)')
        capture_reactions(self.db,-7,SimpleNamespace(id=1,reactions=SimpleNamespace(results=[types.ReactionCount(types.ReactionEmoji('👍'),1)],recent_reactions=[])))
        self.assertEqual(self.db.execute('SELECT count(*) FROM message_reactions').fetchone()[0],0)

    def test_deleted_message_has_no_reactions(self):
        record_deleted(self.db,7,[1])
        capture_reactions(self.db,7,SimpleNamespace(id=1,reactions=SimpleNamespace(results=[types.ReactionCount(types.ReactionEmoji('👍'),1)],recent_reactions=[])))
        self.assertEqual(self.db.execute('SELECT count(*) FROM message_reactions').fetchone()[0],0)

    def test_peerless_deletion_matches_private_and_basic_groups_but_not_channels(self):
        channel=-1000000000007
        self.db.execute("INSERT INTO chats VALUES(-7,'group')")
        self.db.execute("INSERT INTO chats VALUES(?,'supergroup')",(channel,))
        self.db.execute('INSERT INTO messages VALUES(-7,1,0)')
        self.db.execute('INSERT INTO messages VALUES(?,1,0)',(channel,))
        record_deleted(self.db,None,[1,99])
        self.assertTrue(is_deleted(self.db,7,1))
        self.assertTrue(is_deleted(self.db,-7,1))
        self.assertTrue(is_deleted(self.db,8,99))
        self.assertFalse(is_deleted(self.db,channel,1))
        self.assertFalse(is_deleted(self.db,channel,99))
        self.assertEqual(self.db.execute('SELECT deleted FROM messages WHERE chat_id=7').fetchone()[0],1)

    def test_revoked_group_deletion_removes_visible_cache(self):
        self.db.execute("INSERT INTO chats VALUES(-7,'group')")
        self.db.execute('INSERT INTO messages VALUES(-7,1,0)')
        self.db.execute("CREATE TRIGGER deny_revoked BEFORE UPDATE ON messages WHEN new.chat_id=-7 BEGIN SELECT RAISE(IGNORE); END")
        record_deleted(self.db,-7,[1])
        self.assertTrue(is_deleted(self.db,-7,1))
        self.assertIsNone(self.db.execute('SELECT 1 FROM messages WHERE chat_id=-7 AND deleted=0').fetchone())
