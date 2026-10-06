import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from telethon import types, functions
from telethon.errors import FloodWaitError
from advanced_tools import register


class AdvancedTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tools = {}
        def tool(name, desc, schema):
            def wrap(fn): self.tools[name] = fn; return fn
            return wrap
        self.client = AsyncMock()
        self.client.get_entity.return_value = types.Channel(id=7, title='Synthetic', photo=types.ChatPhotoEmpty(), date=None, megagroup=True, default_banned_rights=types.ChatBannedRights(None, invite_users=True))
        self.client.get_input_entity.return_value = types.InputPeerChannel(7, 1)
        self.api = dict(S=SimpleNamespace(client=self.client, send_lock=asyncio.Lock()), need_auth=Mock(), health_ok=lambda:True, sync_guard=AsyncMock(), resolve=AsyncMock(return_value=-1007), require_monitoring=Mock(), check_send_limits=Mock(return_value=None), record_send=Mock(return_value=11), release_rejected_send=Mock(), set_flood=Mock(), record_sync_error=Mock(), t_info=AsyncMock(), t_list=AsyncMock())
        register(tool, self.api)

    async def test_permission_change_preserves_other_restrictions(self):
        await self.tools['set_group_locked']({'chat':'-1007','locked':True})
        request=self.client.call_args.args[0]
        self.assertIsInstance(request, functions.messages.EditChatDefaultBannedRightsRequest)
        self.assertTrue(request.banned_rights.change_info)
        self.assertTrue(request.banned_rights.invite_users)

    async def test_poll_preserves_multiple_choice_and_validates_before_sending(self):
        self.client.send_file.return_value=SimpleNamespace(id=22)
        await self.tools['send_poll']({'chat':'7','question':'Q','options':['A','B'],'multiple_choice':True})
        media=self.client.send_file.call_args.args[1]
        self.assertTrue(media.poll.multiple_choice)
        self.assertEqual([a.option for a in media.poll.answers],[b'0',b'1'])
        self.client.send_file.reset_mock()
        with self.assertRaises(ValueError):await self.tools['send_poll']({'chat':'7','question':'Q','options':['A','A']})
        self.client.send_file.assert_not_called()

    async def test_flood_wait_is_durable_and_not_retried(self):
        self.client.side_effect=FloodWaitError(request=None,capture=90)
        with self.assertRaises(FloodWaitError):await self.tools['block_contact']({'chat':'7'})
        self.api['record_sync_error'].assert_called_once()
        self.api['release_rejected_send'].assert_called_once_with(11)
        self.assertEqual(self.client.call_count,1)

    async def test_rate_limit_blocks_mutation(self):
        self.api['check_send_limits'].return_value='limited'
        with self.assertRaises(RuntimeError):await self.tools['block_contact']({'chat':'7'})
        self.client.assert_not_called()

    async def test_join_link_rejects_external_url(self):
        with self.assertRaises(ValueError):await self.tools['join_group_with_link']({'link':'https://evil.example/+fake'})
        self.client.assert_not_called()

    async def test_privacy_keys_are_explicit(self):
        with self.assertRaises(ValueError):await self.tools['set_privacy_setting']({'name':'unknown','value':'all'})
        self.client.assert_not_called()

    async def test_bulk_participants_rejected(self):
        with self.assertRaises(ValueError):await self.tools['update_group_participants']({'chat':'7','participants':[str(i) for i in range(11)],'action':'add'})
        self.client.assert_not_called()

    async def test_poll_results_are_read_only_and_consent_checked(self):
        self.client.get_messages.return_value=SimpleNamespace(media=types.MessageMediaPoll(poll=types.Poll(id=1,question=types.TextWithEntities('Q',[]),answers=[types.PollAnswer(types.TextWithEntities('A',[]),b'0')],hash=0),results=types.PollResults(results=[types.PollAnswerVoters(b'0',voters=2)],total_voters=2)))
        result=await self.tools['get_poll_results']({'chat':'7','message_id':1})
        self.assertEqual(result['options'][0]['votes'],2)
        self.api['require_monitoring'].assert_called()
        self.api['record_send'].assert_not_called()

    async def test_valid_batch_reports_completed_and_unattempted_members_at_limit(self):
        self.api['check_send_limits'].side_effect=[None,'minimum_gap']
        result=await self.tools['update_group_participants']({'chat':'7','participants':['@a','@b','@c'],'action':'add'})
        self.assertFalse(result['ok'])
        self.assertEqual(result['updated'],['@a'])
        self.assertEqual(result['failed_member'],'@b')
        self.assertEqual(result['remaining'],['@c'])
        self.assertEqual(result['failed_outcome'],'not_attempted')
        self.assertEqual(self.client.call_count,1)

    async def test_partial_batch_flood_is_persisted_without_replaying_success(self):
        self.client.side_effect=[None,FloodWaitError(request=None,capture=90)]
        result=await self.tools['update_group_participants']({'chat':'7','participants':['@a','@b','@c'],'action':'add'})
        self.assertFalse(result['ok'])
        self.assertEqual(result['updated'],['@a'])
        self.assertEqual(result['remaining'],['@c'])
        self.api['set_flood'].assert_called_once()
        self.api['record_sync_error'].assert_called_once()
        self.assertEqual(self.client.call_count,2)
