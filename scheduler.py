"""Bounded durable text-message queue. One dispatcher, no task per message."""
from contextlib import contextmanager
import datetime as dt
import asyncio,json,math,os,sqlite3,time,uuid
from pathlib import Path

class RetryLater(RuntimeError):
    def __init__(self, seconds, message='send deferred'):
        super().__init__(message)
        self.seconds=max(1,float(seconds))


def parse_time(value):
    try:
        parsed=dt.datetime.fromisoformat(value.replace('Z','+00:00'))
        if parsed.tzinfo is None:raise ValueError()
        return parsed.timestamp()
    except (ValueError,TypeError,AttributeError):
        raise ValueError('timestamp_requires_RFC3339_timezone') from None

class Queue:
    def __init__(self,path,clock=time.time,max_pending=1000):
        self.path=str(path);self.clock=clock;self.max_pending=max_pending
        Path(path).parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        with self.db() as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS scheduled_messages(
                id TEXT PRIMARY KEY,chat_id INTEGER NOT NULL,text TEXT NOT NULL,
                send_at REAL NOT NULL,expires_at REAL NOT NULL,next_attempt REAL NOT NULL,
                status TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
                idempotency_key TEXT UNIQUE,reply_to INTEGER,silent INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL,updated_at REAL NOT NULL,message_id INTEGER,error TEXT);
                CREATE INDEX IF NOT EXISTS scheduled_due ON scheduled_messages(status,next_attempt);
                CREATE INDEX IF NOT EXISTS scheduled_updated ON scheduled_messages(updated_at);
                CREATE TABLE IF NOT EXISTS scheduler_meta(key TEXT PRIMARY KEY,value REAL NOT NULL);''')
        os.chmod(self.path,0o600)
    @contextmanager
    def db(self):
        db=sqlite3.connect(self.path,timeout=15);db.row_factory=sqlite3.Row
        try:
            with db:yield db
        finally:db.close()
    def dates(self,send_at,expires_at):
        now=self.clock()
        if not isinstance(send_at,(int,float)) or not math.isfinite(send_at) or not now+1<=send_at<=now+366*86400:
            raise ValueError('send_at_must_be_future_within_one_year')
        expires_at=send_at+86400 if expires_at is None else expires_at
        if not math.isfinite(expires_at) or not send_at<expires_at<=send_at+7*86400:
            raise ValueError('expiration_must_follow_due_within_seven_days')
        return float(send_at),float(expires_at)
    def enqueue(self,chat_id,text,send_at,expires_at=None,idempotency_key=None,reply_to=None,silent=False):
        if not isinstance(send_at,(int,float)) or not math.isfinite(send_at):raise ValueError('invalid_send_at')
        expires_at=send_at+86400 if expires_at is None else expires_at
        if not isinstance(text,str) or not text.strip() or len(text.encode('utf-16-le'))//2>4096:
            raise ValueError('text_required_max_4096_UTF16_units')
        if idempotency_key is not None and (not isinstance(idempotency_key,str) or not 1<=len(idempotency_key)<=128):
            raise ValueError('invalid_idempotency_key')
        now=self.clock()
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            if idempotency_key:
                existing=db.execute('SELECT * FROM scheduled_messages WHERE idempotency_key=?',(idempotency_key,)).fetchone()
                if existing:
                    if (existing['chat_id'],existing['text'],existing['send_at'],existing['expires_at'],existing['reply_to'],existing['silent'])!=(int(chat_id),text,send_at,expires_at,reply_to,int(silent)):
                        raise ValueError('idempotency_key_conflict')
                    return dict(existing,job_id=existing['id'])
            send_at,expires_at=self.dates(send_at,expires_at)
            if db.execute("SELECT count(*) FROM scheduled_messages").fetchone()[0]>=10000:
                raise ValueError('queue_history_limit_review_required')
            if db.execute("SELECT count(*) FROM scheduled_messages WHERE status IN ('pending','claimed','dispatching')").fetchone()[0]>=self.max_pending:
                raise ValueError('pending_queue_limit')
            ident=uuid.uuid4().hex
            db.execute("INSERT INTO scheduled_messages(id,chat_id,text,send_at,expires_at,next_attempt,status,idempotency_key,reply_to,silent,created_at,updated_at) VALUES(?,?,?,?,?,?,'pending',?,?,?,?,?)",(ident,int(chat_id),text,send_at,expires_at,send_at,idempotency_key,reply_to,int(silent),now,now))
            return dict(db.execute('SELECT * FROM scheduled_messages WHERE id=?',(ident,)).fetchone(),job_id=ident)
    def list(self,limit=50,cursor='',status=None):
        if type(limit) is not int or not 1<=limit<=100:raise ValueError('limit_1_to_100')
        with self.db() as db:
            return [dict(r,job_id=r['id']) for r in db.execute('SELECT * FROM scheduled_messages WHERE id>? AND (? IS NULL OR status=?) ORDER BY id LIMIT ?',(cursor,status,status,limit))]
    def cancel(self,ident):
        with self.db() as db:
            changed=db.execute("UPDATE scheduled_messages SET status='cancelled',updated_at=? WHERE id=? AND status='pending'",(self.clock(),ident)).rowcount
            if not changed:raise ValueError('job_not_pending')
        return {'id':ident,'status':'cancelled'}
    def reschedule(self,ident,send_at,expires_at=None):
        send_at,expires_at=self.dates(send_at,expires_at)
        with self.db() as db:
            changed=db.execute("UPDATE scheduled_messages SET send_at=?,expires_at=?,next_attempt=?,updated_at=?,error=NULL WHERE id=? AND status='pending'",(send_at,expires_at,send_at,self.clock(),ident)).rowcount
            if not changed:raise ValueError('job_not_pending')
        return {'id':ident,'status':'pending','send_at':send_at,'expires_at':expires_at}
    def recover(self):
        with self.db() as db:
            db.execute("UPDATE scheduled_messages SET status='uncertain',error='restart_during_send',updated_at=? WHERE status='dispatching'",(self.clock(),))
            db.execute("UPDATE scheduled_messages SET status='pending' WHERE status='claimed'")
    def mark_dispatching(self,ident):
        with self.db() as db:
            if db.execute("UPDATE scheduled_messages SET status='dispatching',attempts=attempts+1,updated_at=? WHERE id=? AND status='claimed' AND expires_at>?",(self.clock(),ident,self.clock())).rowcount!=1:
                raise ValueError('claim_no_longer_dispatchable')
    def finish(self,ident,status,error=None,message_id=None,next_attempt=None):
        with self.db() as db:
            db.execute('UPDATE scheduled_messages SET status=?,error=?,message_id=?,next_attempt=COALESCE(?,next_attempt),updated_at=? WHERE id=?',(status,error,message_id,next_attempt,self.clock(),ident))
    def claim(self):
        now=self.clock()
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute("UPDATE scheduled_messages SET status='expired',updated_at=? WHERE status='pending' AND expires_at<=?",(now,now))
            db.execute("DELETE FROM scheduled_messages WHERE id IN (SELECT id FROM scheduled_messages WHERE status IN ('sent','cancelled','expired','failed') AND updated_at<? LIMIT 100)",(now-7*86400,))
            if db.execute("SELECT 1 FROM scheduler_meta WHERE key='cooldown_until' AND value>?",(now,)).fetchone():return False
            row=db.execute("SELECT * FROM scheduled_messages WHERE status='pending' AND next_attempt<=? ORDER BY next_attempt,id LIMIT 1",(now,)).fetchone()
            if not row:return False
            if row['attempts']>=5:
                db.execute("UPDATE scheduled_messages SET status='failed',error='attempt_limit' WHERE id=?",(row['id'],));return True
            db.execute("UPDATE scheduled_messages SET status='claimed',updated_at=? WHERE id=?",(now,row['id']))
            return dict(row)

    def current_status(self,ident):
        with self.db() as db:return db.execute('SELECT status FROM scheduled_messages WHERE id=?',(ident,)).fetchone()[0]

    def defer(self,ident,retry):
        with self.db() as db:
            db.execute("INSERT INTO scheduler_meta VALUES('cooldown_until',?) ON CONFLICT(key) DO UPDATE SET value=MAX(value,excluded.value)",(retry,))
        self.finish(ident,'pending',error='rate_deferred',next_attempt=retry)

    async def dispatch_one(self,send):
        # Disk pressure must not block the MCP event loop while polling an empty queue.
        row=await asyncio.to_thread(self.claim)
        if not isinstance(row,dict):return bool(row)
        try:
            result=await asyncio.wait_for(send(row,lambda:self.mark_dispatching(row['id'])),timeout=max(.01,min(60,row['expires_at']-self.clock())))
            await asyncio.to_thread(self.finish,row['id'],'sent',message_id=result['message_id'])
        except RetryLater as error:
            retry=self.clock()+max(5,error.seconds)
            await asyncio.to_thread(self.defer,row['id'],retry)
        except Exception as error:
            current=await asyncio.to_thread(self.current_status,row['id'])
            await asyncio.to_thread(self.finish,row['id'],'uncertain' if current=='dispatching' else 'failed',error=type(error).__name__)
        return True
