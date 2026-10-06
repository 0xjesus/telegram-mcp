import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock
from test_sync_history import SyncHistoryTests
from consent import configure_policy

class MetadataTests(SyncHistoryTests):
 def dialog(self,index):
  ent=self.daemon.types.Channel(id=index,title='Synthetic',photo=self.daemon.types.ChatPhotoEmpty(),date=dt.datetime.now(dt.timezone.utc),megagroup=True,access_hash=index)
  return SimpleNamespace(id=-1000000000000-index,entity=ent,name='Synthetic',unread_count=0,date=None,pinned=False,archived=False,message=None)
 def setup_client(self,dialogs,responses):
  class Client:
   async def iter_dialogs(inner):
    for item in dialogs:yield item
   async def __call__(inner,request):return await responses(request)
  self.daemon.S.client=Client();self.daemon.PAUSE=0
 async def test_policy_disabled_has_no_extra_rpc(self):
  rpc=AsyncMock();self.setup_client([self.dialog(1)],rpc)
  await self.daemon.upsert_dialogs();rpc.assert_not_called()
 async def test_metadata_published_before_flood_and_attempt_retained(self):
  c=self.daemon.db();configure_policy(c,True,10,'synthetic');c.close()
  groups=[self.dialog(i) for i in range(1,23)]
  calls=[]
  async def rpc(request):
   calls.append(request)
   if len(calls)==21:
    c=self.daemon.db();self.assertEqual(c.execute('SELECT count(*) FROM group_monitoring_inventory').fetchone()[0],20);c.close()
   if len(calls)==22:raise self.daemon.FloodWaitError(request=None,capture=30)
   return SimpleNamespace(full_chat=SimpleNamespace(participants_count=20))
  self.setup_client(groups,rpc)
  with self.assertRaises(self.daemon.SyncDeferred):await self.daemon.upsert_dialogs()
  c=self.daemon.db()
  self.assertEqual(c.execute('SELECT count(*) FROM group_monitoring_inventory').fetchone()[0],22)
  self.assertEqual(c.execute('SELECT count(*) FROM group_metadata_attempts').fetchone()[0],22)
  c.close()
 async def test_cached_large_count_avoids_extra_rpc(self):
  c=self.daemon.db();configure_policy(c,True,10,'synthetic');c.close()
  rpc=AsyncMock(return_value=SimpleNamespace(full_chat=SimpleNamespace(participants_count=20)))
  self.setup_client([self.dialog(1)],rpc);await self.daemon.upsert_dialogs()
  self.setup_client([self.dialog(1)],rpc);await self.daemon.upsert_dialogs()
  self.assertEqual(rpc.await_count,1)

 async def test_unknown_failure_reuses_durable_attempt(self):
  c=self.daemon.db();configure_policy(c,True,10,'synthetic');c.close()
  rpc=AsyncMock(side_effect=OSError('synthetic transport failure'))
  self.setup_client([self.dialog(1)],rpc);await self.daemon.upsert_dialogs()
  self.setup_client([self.dialog(1)],rpc);await self.daemon.upsert_dialogs()
  self.assertEqual(rpc.await_count,1)
