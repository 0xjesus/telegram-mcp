"""Private loopback monitoring controls and paced WhatsApp history requests."""
from contextlib import contextmanager
import argparse
import asyncio
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import secrets
import sqlite3
import time
from urllib.parse import urlparse
import urllib.request
from aiohttp import web
from runtime_monitor import RuntimeMonitor, add_runtime_panel

PLATFORMS={'whatsapp':'chat_jid','telegram':'chat_id'}
REQUEST_INTERVAL=30
ARRIVAL_WAIT=120
OBSERVATION_BATCH=200


class Dashboard:
    def __init__(self,paths,state):
        self.paths=paths;self.state=Path(state)
        self.state.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        with self.state_db() as c:
            c.execute('PRAGMA journal_mode=WAL')
            c.executescript('''CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value INTEGER);
            CREATE TABLE IF NOT EXISTS recovery(platform TEXT,chat_id TEXT,status TEXT,attempts INTEGER DEFAULT 0,retry_at REAL DEFAULT 0,error TEXT,PRIMARY KEY(platform,chat_id));''')
            columns={r[1] for r in c.execute('PRAGMA table_info(recovery)')}
            for name,kind in [('anchor','TEXT'),('watermark','INTEGER NOT NULL DEFAULT 0'),
                              ('scan_cursor','INTEGER NOT NULL DEFAULT 0'),('candidate_anchor','TEXT'),
                              ('request_token','TEXT'),('pages','INTEGER NOT NULL DEFAULT 0')]:
                if name not in columns:c.execute(f'ALTER TABLE recovery ADD COLUMN {name} {kind}')
            # Old accepted requests have no observation watermark. Never replay them silently.
            c.execute("UPDATE recovery SET status='no_progress',error='legacy_request_outcome_unknown' WHERE platform='whatsapp' AND status='requested'")
        self.state.chmod(0o600)

    @contextmanager
    def state_db(self):
        c=sqlite3.connect(self.state,timeout=15)
        c.execute('PRAGMA synchronous=NORMAL')
        try:
            with c:yield c
        finally:c.close()

    def connect(self,platform):
        if platform not in self.paths:raise ValueError('invalid_platform')
        c=sqlite3.connect(Path(self.paths[platform]).resolve().as_uri()+'?mode=rw',uri=True,timeout=10)
        c.row_factory=sqlite3.Row
        return c

    def inventory(self):
        result=[]
        recovery={(r['platform'],r['chat_id']):r for r in self.recovery_status()}
        for platform,col in PLATFORMS.items():
            c=self.connect(platform)
            try:
                if platform=='telegram':
                    tables={r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    sync={str(r['chat_id']):dict(r) for r in c.execute('SELECT * FROM chat_sync')} if 'chat_sync' in tables else {}
                    rescans={str(r[0]) for r in c.execute('SELECT chat_id FROM memory_group_rescan')} if 'memory_group_rescan' in tables else None
                    retry=c.execute("SELECT CAST(value AS REAL) FROM sync_meta WHERE key='read_retry_after'").fetchone() if 'sync_meta' in tables else None
                    retry_after=retry[0] if retry else 0
                rows=c.execute(f'''SELECT i.{col} id,i.name,i.member_count,c.allowed,c.evidence,c.updated_at
                FROM group_monitoring_inventory i LEFT JOIN group_monitoring_consent c ON c.{col}=i.{col} ORDER BY i.name''').fetchall()
                for r in rows:
                    valid=True
                    if (r['evidence'] or '').startswith('auto:max-members:'):
                        try:valid=dt.datetime.fromisoformat(r['updated_at'].replace('Z','+00:00')).timestamp()>time.time()-900
                        except (ValueError,TypeError,AttributeError):valid=False
                    job=recovery.get((platform,str(r['id'])),{})
                    item=dict(platform=platform,id=str(r['id']),name=r['name'] or str(r['id']),members=r['member_count'],enabled=bool(r['allowed'] and valid),recovery=job.get('status','none'),recovery_error=job.get('error'))
                    if platform=='telegram':
                        item.update(self._telegram_recovery(sync.get(str(r['id'])),str(r['id']) in rescans if rescans is not None else None,item['enabled'],retry_after))
                    result.append(item)
            finally:c.close()
        return result

    @staticmethod
    def _telegram_recovery(state,rescan,enabled,retry_after=0):
        # These small durable tables describe source copying and local replay only.
        # Neither a finished source cursor nor an empty replay queue proves that
        # embeddings, attachment analysis or audio transcripts are complete.
        state=state or {}
        recent_pending=bool(state.get('incremental_pending')) or (state.get('dialog_head_id') or 0)>max(state.get('incremental_id') or 0,state.get('incremental_checked_head_id') or 0)
        if not state:upstream='state_unknown'
        elif max(state.get('retry_after') or 0,retry_after or 0)>time.time():upstream='retry_wait'
        elif state.get('last_error'):upstream='sync_error'
        elif recent_pending:upstream='recent_pending'
        elif state.get('backfill_status')=='capped':upstream='history_limited'
        elif state.get('backfill_status')!='done':upstream='history_pending'
        else:upstream='history_ready'
        recovery='source_'+upstream
        if not enabled:recovery='paused'
        elif upstream=='history_ready' and rescan:recovery='index_rescan_pending'
        return dict(recovery=recovery,recovery_error='source_sync_error' if enabled and state.get('last_error') else None,
                    sync=dict(upstream=upstream,history_status=state.get('backfill_status'),
                              recent_pending=recent_pending,index_rescan_pending=rescan,
                              source_updated_at=state.get('updated_at'),full_content_verified=False))

    def toggle(self,platform,ident,enabled,initial=False):
        if type(enabled)is not bool:raise ValueError('invalid_enabled')
        col=PLATFORMS.get(platform)
        if col is None:raise ValueError('invalid_platform')
        ident=str(ident);key=int(ident) if platform=='telegram' else ident
        c=self.connect(platform)
        try:
            with c:
                c.execute('BEGIN IMMEDIATE')
                if not c.execute(f'SELECT 1 FROM group_monitoring_inventory WHERE {col}=?',(key,)).fetchone():raise ValueError('unknown_group')
                needs_recovery=enabled and not self._enabled(c,col,key)
                evidence='dashboard:enable-all-explicit-user-request' if initial else 'dashboard:manual-toggle'
                now=dt.datetime.now(dt.timezone.utc).isoformat()
                c.execute(f'''INSERT INTO group_monitoring_consent({col},allowed,evidence,updated_at) VALUES(?,?,?,?)
                ON CONFLICT({col}) DO UPDATE SET allowed=excluded.allowed,evidence=excluded.evidence,updated_at=excluded.updated_at''',(key,int(enabled),evidence,now))
                c.execute(f'INSERT INTO group_monitoring_audit({col},allowed,evidence,updated_at) VALUES(?,?,?,?)',(key,int(enabled),evidence,now))
                if needs_recovery:self._recover(c,platform,key)
            with self.state_db() as jobs:
                if needs_recovery:jobs.execute("INSERT INTO recovery(platform,chat_id,status) VALUES(?,?,'queued') ON CONFLICT(platform,chat_id) DO UPDATE SET status='queued',attempts=0,retry_at=0,error=NULL,anchor=NULL,watermark=0,scan_cursor=0,candidate_anchor=NULL,request_token=lower(hex(randomblob(16))),pages=0",(platform,ident))
                if not enabled:jobs.execute("UPDATE recovery SET status='paused' WHERE platform=? AND chat_id=?",(platform,ident))
        finally:c.close()

    @staticmethod
    def _recover(c,platform,key):
        if c.execute("SELECT 1 FROM sqlite_master WHERE name='memory_group_rescan'").fetchone():
            c.execute("INSERT INTO memory_group_rescan(chat_id,cursor) VALUES(?,'') ON CONFLICT(chat_id) DO UPDATE SET cursor=''",(str(key),))
        if platform=='telegram':
            if c.execute("SELECT 1 FROM sqlite_master WHERE name='attachment_group_rescan'").fetchone():
                from tools.attachments.worker import request_rescan
                request_rescan(c,key)
            c.execute("UPDATE chat_sync SET backfill_id=0,backfill_status='pending',last_backfill_at=0,last_incremental_at=0,incremental_pending=1 WHERE chat_id=?",(key,))
            c.execute('UPDATE chats SET backfill_done=0 WHERE id=?',(key,))

    def enable_all(self):
        with self.state_db() as c:c.execute("INSERT INTO settings VALUES('default_enabled',1) ON CONFLICT(key) DO UPDATE SET value=1")
        self.enable_batch(self.inventory())

    def reconcile(self):
        with self.state_db() as c:default=c.execute("SELECT value FROM settings WHERE key='default_enabled'").fetchone()
        if not default or not default[0]:return
        for platform,col in PLATFORMS.items():
            c=self.connect(platform)
            try:rows=c.execute(f"SELECT i.{col} FROM group_monitoring_inventory i LEFT JOIN group_monitoring_consent g ON g.{col}=i.{col} WHERE g.{col} IS NULL OR g.evidence LIKE 'auto:max-members:%'").fetchall()
            finally:c.close()
            self.enable_batch([dict(platform=platform,id=str(row[0])) for row in rows])

    def enable_batch(self, rows):
        # One commit per platform avoids hundreds of fsyncs on the message disks.
        for platform,col in PLATFORMS.items():
            keys=[str(r['id']) for r in rows if r['platform']==platform]
            if not keys:continue
            c=self.connect(platform)
            try:
                recover_keys=[]
                with c:
                    c.execute('BEGIN IMMEDIATE')
                    now=dt.datetime.now(dt.timezone.utc).isoformat()
                    for ident in keys:
                        key=int(ident) if platform=='telegram' else ident
                        needs_recovery=not self._enabled(c,col,key)
                        evidence='dashboard:enable-all-explicit-user-request'
                        c.execute(f'INSERT INTO group_monitoring_consent({col},allowed,evidence,updated_at) VALUES(?,1,?,?) ON CONFLICT({col}) DO UPDATE SET allowed=1,evidence=excluded.evidence,updated_at=excluded.updated_at',(key,evidence,now))
                        c.execute(f'INSERT INTO group_monitoring_audit({col},allowed,evidence,updated_at) VALUES(?,1,?,?)',(key,evidence,now))
                        if needs_recovery:
                            self._recover(c,platform,key)
                            recover_keys.append(ident)
                with self.state_db() as jobs:
                    jobs.executemany("INSERT INTO recovery(platform,chat_id,status) VALUES(?,?,'queued') ON CONFLICT(platform,chat_id) DO UPDATE SET status='queued',attempts=0,retry_at=0,error=NULL,anchor=NULL,watermark=0,scan_cursor=0,candidate_anchor=NULL,request_token=lower(hex(randomblob(16))),pages=0",[(platform,k) for k in recover_keys])
            finally:c.close()

    def recovery_status(self):
        with self.state_db() as c:
            c.row_factory=sqlite3.Row
            return [dict(r) for r in c.execute('SELECT * FROM recovery')]

    def recover(self,platform,ident):
        col=PLATFORMS.get(platform)
        if col is None:raise ValueError('invalid_platform')
        key=int(ident) if platform=='telegram' else str(ident)
        c=self.connect(platform)
        try:
            with c:
                if not self._enabled(c,col,key):raise ValueError('monitoring_disabled')
                self._recover(c,platform,key)
            with self.state_db() as jobs:
                jobs.execute("""INSERT INTO recovery(platform,chat_id,status) VALUES(?,?,'queued')
                    ON CONFLICT(platform,chat_id) DO UPDATE SET status='queued',attempts=0,
                    retry_at=0,error=NULL,anchor=NULL,watermark=0,scan_cursor=0,
                    candidate_anchor=NULL,request_token=lower(hex(randomblob(16))),pages=0""",(platform,str(ident)))
        finally:c.close()

    @staticmethod
    def _enabled(c,col,key):
        return bool(c.execute(f"""SELECT 1 FROM group_monitoring_consent WHERE {col}=? AND allowed=1
            AND (evidence NOT LIKE 'auto:max-members:%' OR
            CAST(strftime('%s',updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900)""",(key,)).fetchone())

    def next_whatsapp_recovery(self):
        with self.state_db() as c:
            c.row_factory=sqlite3.Row
            row=c.execute("""SELECT * FROM recovery WHERE platform='whatsapp'
                AND status IN ('queued','waiting','uncertain') AND retry_at<=?
                ORDER BY retry_at,chat_id LIMIT 1""",(time.time(),)).fetchone()
            return dict(row) if row else None

    @staticmethod
    def _timestamp(value):
        if isinstance(value,(int,float)):return float(value)
        parsed=dt.datetime.fromisoformat(str(value).replace('Z','+00:00'))
        if parsed.tzinfo is None:parsed=parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.timestamp()

    @staticmethod
    def _iso(epoch):
        return dt.datetime.fromtimestamp(epoch,dt.timezone.utc).isoformat()

    def _observe(self,row,c):
        # Only arrivals after THIS request can move the cursor. Old cached history
        # may contain gaps, and RPC acceptance is not evidence that a page arrived.
        arrivals=c.execute("""SELECT rowid,timestamp FROM messages
            WHERE chat_jid=? AND rowid>? ORDER BY rowid LIMIT ?""",
            (row['chat_id'],row['scan_cursor'],OBSERVATION_BATCH+1)).fetchall()
        candidate=row['candidate_anchor']
        for arrival in arrivals[:OBSERVATION_BATCH]:
            try:stamp=self._timestamp(arrival['timestamp'])
            except (TypeError,ValueError,OverflowError):continue
            if 0<stamp<self._timestamp(row['anchor']):
                if not candidate or stamp<self._timestamp(candidate):candidate=self._iso(stamp)
        more=len(arrivals)>OBSERVATION_BATCH
        cursor=arrivals[min(len(arrivals),OBSERVATION_BATCH)-1]['rowid'] if arrivals else row['scan_cursor']
        status=row['status'] if more else ('queued' if candidate else 'no_progress')
        with self.state_db() as jobs:
            jobs.execute("""UPDATE recovery SET status=?,scan_cursor=?,candidate_anchor=?,
                anchor=?,retry_at=?,attempts=?,pages=pages+?,error=?
                WHERE platform='whatsapp' AND chat_id=? AND request_token=?
                AND status IN ('waiting','uncertain')""",
                (status,cursor,candidate,candidate if not more and candidate else row['anchor'],
                 time.time()+REQUEST_INTERVAL if more else time.time(),
                 0 if candidate and not more else row['attempts'],int(bool(candidate and not more)),
                 row['error'] if more else (None if candidate else 'no_new_older_messages'),
                 row['chat_id'],row['request_token']))

    def recover_one(self):
        # A process lock also covers two dashboard instances sharing the state DB.
        fd=os.open(str(self.state)+'.recovery.lock',os.O_CREAT|os.O_RDWR,0o600)
        try:
            try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:return
            self._recover_one()
        finally:os.close(fd)

    def _recover_one(self):
        row=self.next_whatsapp_recovery()
        if not row:return
        c=self.connect('whatsapp')
        try:
            if not self._enabled(c,'chat_jid',row['chat_id']):
                with self.state_db() as jobs:jobs.execute("UPDATE recovery SET status='paused' WHERE platform='whatsapp' AND chat_id=?",(row['chat_id'],))
                return
            if row['status'] in ('waiting','uncertain'):
                self._observe(row,c);return
            with self.state_db() as jobs:
                gate=jobs.execute("SELECT value FROM settings WHERE key='next_history_request'").fetchone()
            if gate and gate[0]>time.time():return
            # Health failures are known pre-dispatch failures, so retry them at most three times.
            try:
                health=mcp('get_status',{})
                state=json.loads(next(b['text'] for b in health.get('content',[]) if b.get('type')=='text'))
                if health.get('isError') or not state.get('connected') or state.get('health',{}).get('state','ok')!='ok':
                    raise RuntimeError('platform_not_ready')
            except Exception as error:
                with self.state_db() as jobs:
                    jobs.execute("""UPDATE recovery SET status=?,attempts=attempts+1,retry_at=?,error=?
                        WHERE platform='whatsapp' AND chat_id=? AND status='queued' AND request_token IS ?""",
                        ('unavailable' if row['attempts']>=2 else 'queued',time.time()+300,
                         type(error).__name__,row['chat_id'],row['request_token']))
                return
            if not self._enabled(c,'chat_jid',row['chat_id']):return
            anchor=row['anchor'] or self._iso(time.time())
            watermark=c.execute('SELECT COALESCE(MAX(rowid),0) FROM messages').fetchone()[0]
            token=secrets.token_hex(16)
            # Persist the uncertain dispatch boundary BEFORE making the request.
            # A crash here is observed once after restart, never blindly replayed.
            with self.state_db() as jobs:
                jobs.execute('BEGIN IMMEDIATE')
                changed=jobs.execute("""UPDATE recovery SET status='waiting',anchor=?,watermark=?,
                    scan_cursor=?,candidate_anchor=NULL,request_token=?,attempts=attempts+1,retry_at=?,error=NULL
                    WHERE platform='whatsapp' AND chat_id=? AND status='queued' AND request_token IS ?""",
                    (anchor,watermark,watermark,token,time.time()+ARRIVAL_WAIT,row['chat_id'],row['request_token'])).rowcount
                if not changed:return
                jobs.execute("INSERT INTO settings VALUES('next_history_request',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(time.time()+REQUEST_INTERVAL,))
            status='waiting';error=None
            try:
                if not self._enabled(c,'chat_jid',row['chat_id']):
                    status='paused'
                else:
                    result=mcp('request_sync',{'chat_jid':row['chat_id'],'from_timestamp':anchor})
                    if result.get('isError'):raise RuntimeError('history_request_outcome_unknown')
            except Exception as exc:
                status='uncertain';error=type(exc).__name__
            finally:
                with self.state_db() as jobs:
                    jobs.execute("""UPDATE recovery SET status=?,retry_at=?,error=? WHERE platform='whatsapp'
                        AND chat_id=? AND request_token=? AND status='waiting'""",
                        (status,time.time()+ARRIVAL_WAIT,error,row['chat_id'],token))
                    jobs.execute("INSERT INTO settings VALUES('next_history_request',?) ON CONFLICT(key) DO UPDATE SET value=MAX(value,excluded.value)",(time.time()+REQUEST_INTERVAL,))
        finally:c.close()


def mcp(name,args):
    headers={'Content-Type':'application/json','Accept':'application/json, text/event-stream'}
    def rpc(method,params):
        req=urllib.request.Request('http://127.0.0.1:7343/mcp',data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode(),headers=headers)
        with urllib.request.urlopen(req,timeout=20) as reply:
            session=reply.headers.get('Mcp-Session-Id')
            if session:headers['Mcp-Session-Id']=session
            data=reply.read(2*1024*1024+1)
        if len(data)>2*1024*1024:raise ValueError('response_limit')
        data=json.loads(data)
        if 'error' in data:raise RuntimeError('mcp_error')
        return data['result']
    rpc('initialize',{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'monitoring-dashboard','version':'1'}})
    return rpc('tools/call',{'name':name,'arguments':args})


def application(dashboard,run_background=True,runtime_monitor=None):
    token=secrets.token_urlsafe(32)
    lock=asyncio.Lock()
    @web.middleware
    async def private(request,handler):
        host=request.host.split(':')[0]
        if host not in ('127.0.0.1','localhost','monitoreo.localhost'):raise web.HTTPForbidden()
        if request.method=='POST':
            if request.headers.get('X-CSRF-Token')!=token:raise web.HTTPForbidden()
            origin=request.headers.get('Origin')
            if origin and urlparse(origin).netloc!=request.host:raise web.HTTPForbidden()
        response=await handler(request)
        response.headers.update({'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','X-Frame-Options':'DENY','Content-Security-Policy':"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'; connect-src 'self'"})
        return response
    app=web.Application(middlewares=[private],client_max_size=4096)
    observer=runtime_monitor if runtime_monitor is not None else RuntimeMonitor()
    async def page(request):return web.Response(text=add_runtime_panel(Path(__file__).with_name('monitoring_dashboard.html').read_text().replace('__CSRF__',token)),content_type='text/html')
    async def runtime(request):return web.json_response(observer.snapshot(),headers={'Cache-Control':'no-store'})
    async def inventory(request):
        rows=await asyncio.to_thread(dashboard.inventory)
        return web.json_response({'groups':rows,'default_enabled':True})
    async def toggle(request):
        a=await request.json()
        try:
            async with lock:await asyncio.to_thread(dashboard.toggle,a['platform'],a['id'],a['enabled'])
        except (KeyError,ValueError):raise web.HTTPBadRequest(text='Selección inválida')
        return web.json_response({'ok':True})
    async def recover(request):
        a=await request.json()
        try:
            async with lock:await asyncio.to_thread(dashboard.recover,a['platform'],a['id'])
        except (KeyError,ValueError):raise web.HTTPBadRequest(text='Selección inválida')
        return web.json_response({'ok':True})
    async def background(app):
        async def loop():
            while True:
                try:
                    async with lock:await asyncio.to_thread(dashboard.reconcile)
                    await asyncio.to_thread(dashboard.recover_one)
                except Exception as error:print('dashboard:',type(error).__name__,flush=True)
                await asyncio.sleep(30)
        task=asyncio.create_task(loop())
        yield
        task.cancel()
        try:await task
        except asyncio.CancelledError:pass
    if run_background:
        app.cleanup_ctx.append(background)
        app.cleanup_ctx.append(observer.background)
    app.router.add_get('/',page);app.router.add_get('/api/groups',inventory)
    app.router.add_get('/api/runtime',runtime)
    app.router.add_post('/api/toggle',toggle);app.router.add_post('/api/recover',recover)
    return app


def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser()
    parser.add_argument('--enable-all',action='store_true')
    parser.add_argument('--port',type=int,default=7257)
    args=parser.parse_args()
    home=Path.home()
    dashboard=Dashboard({'whatsapp':Path(os.environ.get('WA_STORE',home/'.local/share/whatsapp-mcp/store'))/'messages.db','telegram':Path(os.environ.get('TG_STORE',home/'.local/share/telegram-mcp'))/'messages.db'},Path(os.environ.get('MONITORING_STATE',home/'.local/share/messaging-monitoring/dashboard.db')))
    if args.enable_all:
        dashboard.enable_all();print('Enabled all groups',len(dashboard.inventory()),flush=True);return
    web.run_app(application(dashboard),host='127.0.0.1',port=args.port,access_log=None)


if __name__=='__main__':main()
