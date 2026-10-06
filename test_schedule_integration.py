import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock
from test_sync_history import SyncHistoryTests
from scheduler import RetryLater

class ScheduleIntegrationTests(SyncHistoryTests):
 async def test_mcp_schedule_cancel_and_timezone(self):
  due=(dt.datetime.now(dt.timezone.utc)+dt.timedelta(hours=1)).isoformat()
  result=await self.daemon.t_schedule({'chat':'7','text':'synthetic scheduled','send_at':due,'idempotency_key':'one'})
  listing=await self.daemon.t_scheduled({})
  self.assertEqual(listing['messages'][0]['id'],result['id'])
  await self.daemon.t_cancel_scheduled({'job_id':result['id']})
  self.assertEqual((await self.daemon.t_scheduled({}))['messages'][0]['status'],'cancelled')
  with self.assertRaises(ValueError):
   await self.daemon.t_schedule({'chat':'7','text':'synthetic','send_at':'2026-12-01T12:00:00'})
 async def test_flood_during_resolution_prevents_send(self):
  async def entity(cid):
   self.daemon.set_flood(60,'synthetic background read')
   self.daemon.record_sync_error(None,self.daemon.FloodWaitError(request=None,capture=60))
   return cid
  self.daemon.S.client.get_input_entity=entity
  self.daemon.S.client.send_message=AsyncMock()
  marked=[]
  with self.assertRaises(RetryLater):
   await self.daemon.t_send({'chat':'7','text':'synthetic'},before_send=lambda:marked.append(True))
  self.assertFalse(marked)
  self.daemon.S.client.send_message.assert_not_called()
 async def test_old_membership_snapshot_cannot_regrant(self):
  from consent import configure_policy,allowed
  c=self.daemon.db();configure_policy(c,True,10,'synthetic authorization');c.close()
  ent=self.daemon.types.Chat(id=7,title='Group',photo=self.daemon.types.ChatPhotoEmpty(),participants_count=10,date=dt.datetime.now(dt.timezone.utc),version=1)
  dialog=SimpleNamespace(id=-7,entity=ent,name='Group',unread_count=0,date=None,pinned=False,archived=False,message=None,monitoring_count=10)
  self.daemon.S.monitoring_epoch=1
  self.daemon.write_dialog_page([dialog],expected_epoch=0)
  c=self.daemon.db();self.assertFalse(allowed(c,-7));c.close()
  self.daemon.write_dialog_page([dialog],expected_epoch=1)
  c=self.daemon.db();self.assertTrue(allowed(c,-7));c.close()

 async def test_resolution_flood_is_durably_deferred(self):
  from unittest.mock import Mock
  self.daemon.S.client.is_connected=Mock(return_value=True)
  self.daemon.S.client.get_input_entity=AsyncMock(side_effect=self.daemon.FloodWaitError(request=None,capture=120))
  row={'chat_id':7,'text':'synthetic','reply_to':None,'silent':False}
  with self.assertRaises(RetryLater):await self.daemon.dispatch_scheduled(row,lambda:None)
  c=self.daemon.db()
  self.assertGreater(float(c.execute("SELECT value FROM sync_meta WHERE key='read_retry_after'").fetchone()[0]),0)
  c.close()

 async def test_membership_change_does_not_invalidate_other_group_snapshot(self):
  from consent import configure_policy,allowed
  c=self.daemon.db();configure_policy(c,True,10,'synthetic authorization');c.close()
  stamp=dt.datetime.now(dt.timezone.utc).isoformat()
  dialogs=[SimpleNamespace(id=cid,name='Synthetic',monitoring_count=3,monitoring_verified_at=stamp,monitoring_attempted_at=None,monitoring_epoch=0) for cid in (-7,-8)]
  self.daemon.S.monitoring_epoch=1
  self.daemon.S.monitoring_epochs[-7]=1
  self.daemon.write_group_metadata(dialogs,0)
  c=self.daemon.db()
  self.assertFalse(allowed(c,-7));self.assertTrue(allowed(c,-8));c.close()
