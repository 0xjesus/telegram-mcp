import tempfile,time,unittest,sqlite3
from pathlib import Path
from scheduler import Queue,RetryLater

class SchedulerTests(unittest.IsolatedAsyncioTestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  self.path=Path(self.tmp.name)/'queue.db';self.now=1000000000
  self.q=Queue(self.path,clock=lambda:self.now)
 def add(self,**kwargs):return self.q.enqueue(7,'synthetic',self.now+60,**kwargs)
 async def test_due_and_restart_no_early_send(self):
  job=self.add(idempotency_key='unique')
  self.assertEqual(self.add(idempotency_key='unique')['id'],job['id'])
  calls=[]
  async def send(row,mark):mark();calls.append(row['id']);return {'message_id':3}
  self.assertFalse(await self.q.dispatch_one(send))
  self.now+=60
  self.assertTrue(await self.q.dispatch_one(send))
  self.assertEqual(calls,[job['id']]);self.assertEqual(self.q.list()[0]['status'],'sent')
  self.assertFalse(await Queue(self.path,clock=lambda:self.now).dispatch_one(send))
 async def test_cancel_reschedule_and_limits(self):
  job=self.add();self.q.reschedule(job['id'],self.now+120)
  self.q.cancel(job['id'])
  self.assertEqual(self.q.list()[0]['status'],'cancelled')
  with self.assertRaises(ValueError):self.q.reschedule(job['id'],self.now+120)
  with self.assertRaises(ValueError):self.q.enqueue(7,'x'*5000,self.now+60)
  with self.assertRaises(ValueError):self.q.enqueue(7,'x',self.now-1)
 async def test_rate_wait_then_success(self):
  self.add();self.now+=60
  async def deferred(row,mark):raise RetryLater(120)
  await self.q.dispatch_one(deferred)
  self.assertEqual(self.q.list()[0]['status'],'pending')
  self.assertEqual(self.q.list()[0]['next_attempt'],self.now+120)
 async def test_ambiguous_send_and_crash_never_auto_retry(self):
  self.add();self.now+=60
  async def fail(row,mark):mark();raise TimeoutError('sensitive error')
  await self.q.dispatch_one(fail)
  self.assertEqual(self.q.list()[0]['status'],'uncertain')
  self.assertNotIn('sensitive',str(self.q.list()))
  other=self.add()
  with sqlite3.connect(self.path) as db:db.execute("UPDATE scheduled_messages SET status='dispatching' WHERE id=?",(other['id'],))
  self.q.recover()
  self.assertEqual({r['status'] for r in self.q.list()},{'uncertain'})
 async def test_expiration_and_pending_bound(self):
  self.add(expires_at=self.now+70);self.now+=80
  async def forbidden(row,mark):self.fail('expired sent')
  await self.q.dispatch_one(forbidden)
  self.assertEqual(self.q.list()[0]['status'],'expired')
  self.q.max_pending=2
  self.add();self.add()
  with self.assertRaises(ValueError):self.add()

 async def test_idempotency_replay_after_due(self):
  due=self.now+60
  first=self.q.enqueue(7,'synthetic',due,idempotency_key='retry')
  self.now+=120
  self.assertEqual(self.q.enqueue(7,'synthetic',due,idempotency_key='retry')['id'],first['id'])
  with self.assertRaises(ValueError):self.q.enqueue(7,'changed',due,idempotency_key='retry')

 async def test_slow_disk_poll_does_not_block_event_loop(self):
  import asyncio,threading
  entered=threading.Event();release=threading.Event()
  def blocked_claim():
   entered.set();release.wait(2);return False
  self.q.claim=blocked_claim
  task=asyncio.create_task(self.q.dispatch_one(None))
  try:
   for _ in range(100):
    if entered.is_set():break
    await asyncio.sleep(.001)
   self.assertTrue(entered.is_set())
   self.assertFalse(task.done())
  finally:release.set()
  self.assertFalse(await task)
