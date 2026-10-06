"""Synthetic Telegram history only. Never construct an authenticated client."""
import asyncio
import datetime as dt
import importlib.util
import json
import os
import threading
from pathlib import Path
from contextlib import asynccontextmanager
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch, AsyncMock

from telethon import types
from telethon.errors import FloodWaitError


def message(ident):
    return types.Message(id=ident, peer_id=types.PeerUser(7),
                         date=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
                         message=f"synthetic {ident}")


def dialog(head, archived=False):
    return SimpleNamespace(id=7, entity=types.User(id=7, first_name="Synthetic"),
                           name="synthetic chat", unread_count=0, date=message(head or 1).date,
                           pinned=False, archived=archived, message=message(head) if head else None)


class FakeClient:
    def __init__(self, ids=(), failure=None):
        self.messages = [message(ident) for ident in ids]
        self.failure = failure
        self.calls = []
        self.dialogs = []
        self.failure_after = None

    async def get_input_entity(self, chat_id):
        return chat_id

    async def iter_messages(self, entity, **kwargs):
        self.calls.append({"entity": entity, **kwargs})
        if self.failure:
            raise self.failure
        values = [m for m in self.messages if m.id > kwargs.get("min_id", 0)
                  and (not kwargs.get("offset_id") or m.id < kwargs["offset_id"])]
        values.sort(key=lambda m: m.id, reverse=not kwargs.get("reverse", False))
        for index, m in enumerate(values[:kwargs["limit"]]):
            if index == self.failure_after:
                raise OSError("synthetic partial page failure")
            yield m

    async def iter_dialogs(self):
        for dialog in self.dialogs:
            yield dialog


class SyncHistoryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        with patch.dict(os.environ, {"TG_STORE": self.tmp.name, "TG_BATCH_PAUSE": "0", "TG_BACKFILL_CAP": "20000"}):
            spec = importlib.util.spec_from_file_location("synthetic_telegram_daemon", Path(__file__).with_name("daemon.py"))
            self.daemon = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.daemon)
        self.daemon.init_db()
        self.daemon.S.stage = "authorized"
        self.daemon.S.client = FakeClient()
        c = self.daemon.db()
        c.execute("INSERT INTO chats(id,title) VALUES(7,'synthetic chat')")
        c.commit()
        c.close()

    def saved_ids(self):
        c = self.daemon.db()
        ids = [row["id"] for row in c.execute("SELECT id FROM messages WHERE chat_id=7 ORDER BY id")]
        c.close()
        return ids

    def save(self, ident):
        c = self.daemon.db()
        self.daemon.upsert_message(c, 7, message(ident))
        c.commit()
        c.close()

    async def test_empty_voice_redelivery_keeps_manual_transcript(self):
        self.save(1)
        c=self.daemon.db()
        c.execute("UPDATE messages SET text='Transcripción corregida manualmente',media_type='voice' WHERE chat_id=7 AND id=1")
        incoming=message(1);incoming.message=''
        with patch.object(self.daemon,'media_info',return_value=('voice',None)):
            self.daemon.upsert_message(c,7,incoming)
        self.assertEqual(c.execute('SELECT text FROM messages WHERE chat_id=7 AND id=1').fetchone()[0],
                         'Transcripción corregida manualmente')
        incoming.message='Nueva leyenda explícita'
        with patch.object(self.daemon,'media_info',return_value=('voice',None)):
            self.daemon.upsert_message(c,7,incoming)
        self.assertEqual(c.execute('SELECT text FROM messages WHERE chat_id=7 AND id=1').fetchone()[0],
                         'Nueva leyenda explícita')
        c.close()

    async def test_dialog_preview_does_not_skip_offline_messages(self):
        self.save(1)
        client = FakeClient([1, 2, 3])
        client.dialogs = [SimpleNamespace(id=7, entity=types.User(id=7, first_name="Synthetic"),
                                         name="synthetic chat", unread_count=0,
                                         date=message(3).date, pinned=False, archived=False,
                                         message=message(3))]
        self.daemon.S.client = client
        await self.daemon.upsert_dialogs()
        await self.daemon.sync_chat(7, 100, incremental=True)
        self.assertEqual(self.saved_ids(), [1, 2, 3])

    async def test_failed_fetch_is_not_a_successful_empty_history(self):
        self.daemon.S.client = FakeClient(failure=OSError("synthetic connection loss"))
        with self.assertRaises(RuntimeError):
            await self.daemon.sync_chat(7, 100, incremental=False)
        self.assertEqual(self.saved_ids(), [])

    async def test_incremental_pages_keep_oldest_unseen_messages(self):
        self.save(1)
        self.daemon.S.client = FakeClient(range(1, 8))
        await self.daemon.sync_chat(7, 2, incremental=True)
        await self.daemon.sync_chat(7, 2, incremental=True)
        await self.daemon.sync_chat(7, 2, incremental=True)
        self.assertEqual(self.saved_ids(), list(range(1, 8)))

    async def test_sync_request_is_bounded_to_one_page(self):
        self.daemon.BATCH = 3
        self.daemon.S.client = FakeClient(range(1, 20))
        got = await self.daemon.sync_chat(7, 1000, incremental=False)
        self.assertEqual(got, 3)
        self.assertEqual(self.daemon.S.client.calls[0]["limit"], 3)

    async def test_live_arrival_does_not_move_durable_incremental_cursor(self):
        self.save(1)
        await self.daemon.sync_chat(7, 2, incremental=True)
        self.save(9)
        self.daemon.S.client = FakeClient(range(1, 10))
        for _ in range(4):
            await self.daemon.sync_chat(7, 2, incremental=True)
        self.assertEqual(self.saved_ids(), list(range(1, 10)))

    async def test_large_page_writes_do_not_run_on_event_loop_thread(self):
        self.daemon.S.client = FakeClient([1, 2, 3])
        main_thread = threading.get_ident()
        writes = []
        original = self.daemon.upsert_message
        def record_write(*args, **kwargs):
            writes.append(threading.get_ident())
            return original(*args, **kwargs)
        with patch.object(self.daemon, "upsert_message", record_write):
            await self.daemon.sync_chat(7, 3, incremental=False)
        self.assertTrue(writes)
        self.assertNotIn(main_thread, writes)

    async def test_full_history_requeues_legacy_done_chats_and_repairs_holes(self):
        self.save(1)
        self.save(8)
        c = self.daemon.db()
        c.execute("UPDATE chats SET first_pass_done=1, backfill_done=1 WHERE id=7")
        c.commit(); c.close()
        self.daemon.BACKFILL_CAP = 0
        self.daemon.init_db()
        self.daemon.S.client = FakeClient(range(1, 9))
        for _ in range(5):
            await self.daemon.sync_chat(7, 2, incremental=False)
        self.assertEqual(self.saved_ids(), list(range(1, 9)))
        c = self.daemon.db()
        self.assertEqual(c.execute("SELECT backfill_done FROM chats WHERE id=7").fetchone()[0], 1)
        c.close()

    async def test_full_history_migration_preserves_cursor_on_restart(self):
        self.save(1)
        self.save(8)
        self.daemon.BACKFILL_CAP = 0
        self.daemon.init_db()
        self.daemon.S.client = FakeClient(range(1, 9))
        await self.daemon.sync_chat(7, 2, incremental=False)
        self.daemon.init_db()
        await self.daemon.sync_chat(7, 2, incremental=False)
        self.assertEqual(self.daemon.S.client.calls[-1].get("offset_id"), 7)

    async def test_cap_is_not_reported_as_exhausted_history(self):
        self.daemon.BACKFILL_CAP = 2
        self.daemon.S.client = FakeClient(range(1, 9))
        await self.daemon.sync_chat(7, 10, incremental=False)
        await self.daemon.sync_chat(7, 10, incremental=False)
        self.assertEqual(self.saved_ids(), [7, 8])
        c = self.daemon.db()
        self.assertEqual(c.execute("SELECT backfill_done FROM chats WHERE id=7").fetchone()[0], 0)
        c.close()

    async def test_flood_wait_survives_restart_and_prevents_another_request(self):
        self.daemon.S.client = FakeClient(failure=FloodWaitError(request=None, capture=120))
        with self.assertRaises(RuntimeError):
            await self.daemon.sync_chat(7, 2, incremental=False)
        self.daemon.S = self.daemon.State()
        self.daemon.S.client = FakeClient([1, 2])
        self.daemon.init_db()
        with self.assertRaises(RuntimeError):
            await self.daemon.sync_chat(7, 2, incremental=False)
        self.assertEqual(self.daemon.S.client.calls, [])

    async def test_failed_first_pass_is_retried_without_marking_done(self):
        self.assertTrue(hasattr(self.daemon, "sync_cycle"), "A bounded cycle must be independently runnable")
        self.daemon.S.client = FakeClient(failure=OSError("synthetic outage"))
        await self.daemon.sync_cycle()
        c = self.daemon.db()
        self.assertEqual(tuple(c.execute("SELECT first_pass_done,backfill_done FROM chats WHERE id=7").fetchone()), (0, 0))
        c.close()

    async def test_partial_page_failure_keeps_cursor_and_page_atomic(self):
        self.daemon.S.client = FakeClient(range(1, 9))
        self.daemon.S.client.failure_after = 1
        with self.assertRaises(RuntimeError):
            await self.daemon.sync_chat(7, 3, incremental=False)
        self.assertEqual(self.saved_ids(), [])
        c = self.daemon.db()
        row = c.execute("SELECT backfill_id,backfill_status,retry_after FROM chat_sync WHERE chat_id=7").fetchone()
        self.assertEqual((row[0], row[1]), (0, "pending"))
        self.assertGreater(row[2], 0)
        c.close()

    async def test_concurrent_requests_advance_separate_pages(self):
        self.daemon.S.client = FakeClient(range(1, 9))
        await asyncio.gather(self.daemon.sync_chat(7, 2, incremental=False),
                             self.daemon.sync_chat(7, 2, incremental=False))
        self.assertEqual(self.saved_ids(), [5, 6, 7, 8])

    async def test_round_robin_bounds_backfill_and_reaches_archived_chats(self):
        self.daemon.BACKFILL_CAP = 0
        self.daemon.BACKFILL_CHATS_PER_CYCLE = 2
        self.daemon.BATCH = 2
        c = self.daemon.db()
        c.executemany("INSERT INTO chats(id,title,first_pass_done,archived) VALUES(?,?,1,1)",
                      [(n, f"synthetic {n}") for n in range(8, 12)])
        for chat_id in range(7, 12):
            self.daemon.upsert_message(c, chat_id, message(8))
        c.execute("UPDATE chats SET first_pass_done=1")
        c.commit(); c.close()
        self.daemon.init_db()
        self.daemon.S.client = FakeClient(range(1, 9))
        await self.daemon.sync_cycle()
        old_calls = [call for call in self.daemon.S.client.calls if not call.get("reverse")]
        self.assertEqual(len(old_calls), 2)
        await self.daemon.sync_cycle()
        old_calls = [call for call in self.daemon.S.client.calls if not call.get("reverse")]
        self.assertEqual(len(old_calls), 4)
        self.assertEqual(len({call["entity"] for call in old_calls}), 4)

    async def test_database_write_failure_rolls_back_messages_and_checkpoint(self):
        self.daemon.S.client = FakeClient([1, 2, 3])
        original = self.daemon.upsert_message
        def fail_second(c, chat_id, m, **kwargs):
            if m.id == 2:
                raise OSError("synthetic disk failure")
            return original(c, chat_id, m, **kwargs)
        with patch.object(self.daemon, "upsert_message", fail_second), self.assertRaises(RuntimeError):
            await self.daemon.sync_chat(7, 3, incremental=False)
        self.assertEqual(self.saved_ids(), [])
        c = self.daemon.db()
        self.assertEqual(c.execute("SELECT backfill_id FROM chat_sync WHERE chat_id=7").fetchone()[0], 0)
        self.assertEqual(c.execute("SELECT first_pass_done FROM chats WHERE id=7").fetchone()[0], 0)
        c.close()

    def test_client_does_not_hide_short_flood_waits(self):
        cfg = Path(self.tmp.name) / "synthetic-app.json"
        cfg.write_text(json.dumps({"api_id": 123, "api_hash": "synthetic-not-a-credential"}))
        with patch.object(self.daemon, "CFG", cfg), patch.object(self.daemon, "TelegramClient") as constructor:
            self.daemon.make_client()
        self.assertEqual(constructor.call_args.kwargs["flood_sleep_threshold"], 0)

    async def test_mcp_read_flood_is_persisted_and_next_read_is_blocked(self):
        calls = []
        async def get_entity(chat_id):
            calls.append(chat_id)
            raise FloodWaitError(request=None, capture=30)
        self.daemon.S.client.get_entity = get_entity
        class Request:
            method = "POST"
            async def json(self):
                return {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
                    "name": "get_chat_info", "arguments": {"chat": "7"}}}
        first = json.loads((await self.daemon.mcp(Request())).text)
        self.assertTrue(first["result"]["isError"])
        self.assertEqual(self.daemon.S.health["state"], "flood_wait")
        self.daemon.S = self.daemon.State()
        self.daemon.S.stage = "authorized"
        self.daemon.S.client = FakeClient()
        self.daemon.S.client.get_entity = get_entity
        self.daemon.init_db()
        second = json.loads((await self.daemon.mcp(Request())).text)
        self.assertTrue(second["result"]["isError"])
        self.assertEqual(calls, [7])

    async def test_wrapped_send_flood_is_reported_without_retry(self):
        calls = []
        @asynccontextmanager
        async def action(*args):
            yield
        async def reject_send(*args, **kwargs):
            calls.append("synthetic call rejected before delivery")
            raise FloodWaitError(request=None, capture=30)
        self.daemon.S.client.action = action
        self.daemon.S.client.send_message = reject_send
        class Request:
            method = "POST"
            async def json(self):
                return {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
                    "name": "send_message", "arguments": {"chat": "7", "text": "synthetic"}}}
        with patch.object(self.daemon.asyncio, "sleep", new_callable=AsyncMock):
            first = json.loads((await self.daemon.mcp(Request())).text)
            second = json.loads((await self.daemon.mcp(Request())).text)
        self.assertTrue(first["result"]["isError"])
        self.assertTrue(second["result"]["isError"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.daemon.S.health["state"], "flood_wait")
        c = self.daemon.db()
        self.assertEqual(c.execute("SELECT COUNT(*) FROM sends").fetchone()[0], 0)
        self.assertGreater(float(c.execute("SELECT value FROM sync_meta WHERE key='read_retry_after'").fetchone()[0]), 0)
        c.close()

    async def settle_history(self):
        for ident in [1, 2, 3]:
            self.save(ident)
        self.daemon.S.client = FakeClient([1, 2, 3])
        await self.daemon.sync_chat(7, 100, incremental=False)
        self.daemon.S.client.calls.clear()

    async def test_unchanged_dialogs_do_not_request_incremental_history(self):
        await self.settle_history()
        self.daemon.S.client.dialogs = [dialog(3)]
        await self.daemon.sync_cycle()
        await self.daemon.sync_cycle()
        self.assertEqual(self.daemon.S.client.calls, [])

    async def test_new_preview_schedules_gap_fill_without_inserting_its_body(self):
        await self.settle_history()
        self.daemon.S.client = FakeClient(range(1, 8))
        self.daemon.S.client.dialogs = [dialog(7)]
        await self.daemon.upsert_dialogs()
        self.assertEqual(self.saved_ids(), [1, 2, 3])
        self.assertEqual([row["id"] for row in self.daemon.sync_chat_inventory()], [7])
        await self.daemon.sync_cycle()
        self.assertEqual(self.saved_ids(), list(range(1, 8)))
        self.daemon.S.client.calls.clear()
        await self.daemon.sync_cycle()
        self.assertEqual(self.daemon.S.client.calls, [])

    async def test_preview_and_later_live_event_preserve_gap_fill_in_archived_chat(self):
        await self.settle_history()
        self.daemon.S.client = FakeClient(range(1, 10))
        self.daemon.S.client.dialogs = [dialog(8, archived=True)]
        await self.daemon.upsert_dialogs()
        self.save(9)
        await self.daemon.sync_cycle()
        self.assertEqual(self.saved_ids(), list(range(1, 10)))

    async def test_unknown_preview_gets_periodic_check_and_continues_full_pages(self):
        await self.settle_history()
        self.daemon.S.client = FakeClient(range(1, 8))
        self.daemon.S.client.dialogs = [dialog(None)]
        await self.daemon.sync_cycle()
        self.assertEqual(self.daemon.S.client.calls, [])
        c = self.daemon.db()
        c.execute("UPDATE chat_sync SET last_incremental_at=0 WHERE chat_id=7")
        c.commit(); c.close()
        self.daemon.BATCH = 2
        for _ in range(4):
            await self.daemon.sync_cycle()
            self.daemon.init_db()
        self.assertEqual(self.saved_ids(), list(range(1, 8)))
        self.assertEqual(len(self.daemon.S.client.calls), 3)

    async def test_unavailable_preview_head_is_not_retried_every_cycle(self):
        await self.settle_history()
        self.daemon.S.client.dialogs = [dialog(99)]
        await self.daemon.sync_cycle()
        await self.daemon.sync_cycle()
        self.assertEqual(len(self.daemon.S.client.calls), 1)

    async def test_periodic_verification_is_bounded_across_427_archived_chats(self):
        c = self.daemon.db()
        c.executemany("INSERT INTO chats(id,title) VALUES(?,?)",
                      [(chat_id, f"synthetic {chat_id}") for chat_id in range(8, 434)])
        c.execute("UPDATE chats SET first_pass_done=1,archived=1")
        c.commit(); c.close()
        self.daemon.init_db()
        c = self.daemon.db()
        c.execute("UPDATE chat_sync SET backfill_status='done'")
        c.commit(); c.close()
        self.daemon.S.client = FakeClient()
        for chat_id in range(7, 434):
            item = dialog(None, archived=True)
            item.id = chat_id
            item.entity = types.User(id=chat_id, first_name="Synthetic")
            self.daemon.S.client.dialogs.append(item)
        await self.daemon.sync_cycle()
        self.assertEqual(len(self.daemon.S.client.calls), 10)
        await self.daemon.sync_cycle()
        self.assertEqual(len(self.daemon.S.client.calls), 20)
        self.assertEqual(len({call["entity"] for call in self.daemon.S.client.calls}), 20)

    def test_older_sync_schema_gains_preview_fields_without_resetting_cursors(self):
        c = self.daemon.db()
        c.executescript("""DROP TABLE chat_sync;
            CREATE TABLE chat_sync(chat_id INTEGER PRIMARY KEY,
                incremental_id INTEGER NOT NULL DEFAULT 0, backfill_id INTEGER NOT NULL DEFAULT 0,
                backfill_status TEXT NOT NULL DEFAULT 'pending', last_backfill_at REAL NOT NULL DEFAULT 0,
                retry_after REAL NOT NULL DEFAULT 0, last_error TEXT, updated_at TEXT);
            INSERT INTO chat_sync(chat_id,incremental_id,backfill_id) VALUES(7,9,3);""")
        c.commit(); c.close()
        self.daemon.init_db()
        self.daemon.init_db()
        c = self.daemon.db()
        row = c.execute("SELECT incremental_id,backfill_id,dialog_head_id,last_incremental_at,incremental_pending FROM chat_sync WHERE chat_id=7").fetchone()
        self.assertEqual(tuple(row), (9, 3, None, 0, 0))
        c.close()


if __name__ == "__main__":
    unittest.main()
