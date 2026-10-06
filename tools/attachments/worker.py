"""Durable, bounded Telegram attachment worker; downloads through the daemon only."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request

from consent import allowed, install, predicate

KINDS = "('photo','image','sticker','document')"
MAX_FILE = 50 * 1024 * 1024


def initialize(db):
    db.row_factory = sqlite3.Row
    columns = {r[1] for r in db.execute('PRAGMA table_info(messages)')}
    for name, kind in [('media_hash', "TEXT NOT NULL DEFAULT ''"), ('media_size','INTEGER')]:
        if name not in columns:
            db.execute(f'ALTER TABLE messages ADD COLUMN {name} {kind}')
    install(db)
    db.executescript('''CREATE TABLE IF NOT EXISTS attachment_analysis(
        chat_id INTEGER NOT NULL,message_id INTEGER NOT NULL,media_hash TEXT NOT NULL,
        text TEXT NOT NULL DEFAULT '',status TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
        error TEXT,metadata TEXT NOT NULL DEFAULT '{}',updated_at TEXT NOT NULL,
        retry_at REAL NOT NULL DEFAULT 0,PRIMARY KEY(chat_id,message_id));
        CREATE TABLE IF NOT EXISTS attachment_queue(chat_id INTEGER,message_id INTEGER,
        retry_at REAL NOT NULL DEFAULT 0,PRIMARY KEY(chat_id,message_id));
        CREATE INDEX IF NOT EXISTS attachment_queue_retry ON attachment_queue(retry_at);
        CREATE TABLE IF NOT EXISTS attachment_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        INSERT OR IGNORE INTO attachment_meta VALUES('discovery_cursor','0');''')
    queue_columns={r[1] for r in db.execute('PRAGMA table_info(attachment_queue)')}
    for name, definition in [('attempts','INTEGER NOT NULL DEFAULT 0'),('terminal','INTEGER NOT NULL DEFAULT 0'),('error','TEXT')]:
        if name not in queue_columns:db.execute(f'ALTER TABLE attachment_queue ADD COLUMN {name} {definition}')
    db.execute('CREATE INDEX IF NOT EXISTS attachment_queue_due ON attachment_queue(terminal,retry_at)')
    db.execute('CREATE TABLE IF NOT EXISTS attachment_group_rescan(chat_id INTEGER PRIMARY KEY,cursor INTEGER NOT NULL DEFAULT 0)')
    for event in ('INSERT','UPDATE'):
        transition='' if event=='INSERT' else " AND (old.allowed!=1 OR (old.evidence LIKE 'auto:max-members:%' AND COALESCE(CAST(strftime('%s',old.updated_at) AS INTEGER),0)<=CAST(strftime('%s','now') AS INTEGER)-900))"
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS attachment_consent_rescan_{event.lower()}
            AFTER {event} ON group_monitoring_consent WHEN new.allowed=1 AND {predicate('new.chat_id')}{transition}
            BEGIN INSERT INTO attachment_group_rescan(chat_id,cursor) VALUES(new.chat_id,0)
            ON CONFLICT(chat_id) DO UPDATE SET cursor=0; END''')
    for event in ('INSERT','UPDATE'):
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS attachment_guard_{event.lower()}
            BEFORE {event} ON attachment_analysis WHEN NOT ({predicate('new.chat_id')})
            OR NOT EXISTS(SELECT 1 FROM messages m WHERE m.chat_id=new.chat_id AND m.id=new.message_id
                AND m.deleted=0 AND m.media_hash!='' AND m.media_hash=new.media_hash)
            BEGIN SELECT RAISE(IGNORE); END''')
    db.executescript(f'''CREATE TRIGGER IF NOT EXISTS attachment_message_insert AFTER INSERT ON messages
        WHEN new.deleted=0 AND new.media_type IN {KINDS} BEGIN
        INSERT OR IGNORE INTO attachment_queue(chat_id,message_id) VALUES(new.chat_id,new.id); END;
        CREATE TRIGGER IF NOT EXISTS attachment_message_update AFTER UPDATE OF media_hash,media_type,deleted ON messages
        WHEN old.media_hash IS NOT new.media_hash OR old.media_type IS NOT new.media_type OR old.deleted IS NOT new.deleted BEGIN
        DELETE FROM attachment_analysis WHERE chat_id=old.chat_id AND message_id=old.id;
        DELETE FROM attachment_queue WHERE chat_id=old.chat_id AND message_id=old.id;
        INSERT OR IGNORE INTO attachment_queue(chat_id,message_id)
            SELECT new.chat_id,new.id WHERE new.deleted=0 AND new.media_type IN {KINDS}; END;
        CREATE TRIGGER IF NOT EXISTS attachment_message_delete AFTER DELETE ON messages BEGIN
        DELETE FROM attachment_analysis WHERE chat_id=old.chat_id AND message_id=old.id;
        DELETE FROM attachment_queue WHERE chat_id=old.chat_id AND message_id=old.id; END;''')
    for event in ('INSERT','UPDATE','DELETE'):
        ref='old' if event=='DELETE' else 'new'
        condition='' if event=='DELETE' else f' WHEN NOT ({predicate("new.chat_id")})'
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS attachment_consent_{event.lower()}
            AFTER {event} ON group_monitoring_consent{condition} BEGIN
            DELETE FROM attachment_analysis WHERE chat_id={ref}.chat_id; END''')
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='memory_changes'").fetchone():
        for event,ref in [('INSERT','new'),('UPDATE','new'),('DELETE','old')]:
            db.execute(f'''CREATE TRIGGER IF NOT EXISTS memory_attachment_{event.lower()}
                AFTER {event} ON attachment_analysis BEGIN
                INSERT INTO memory_changes(chat_id,message_id,op)
                VALUES({ref}.chat_id,{ref}.message_id,'upsert'); END''')
    backend=os.environ.get('TG_ATTACHMENT_BACKEND','local')
    previous=db.execute("SELECT value FROM attachment_meta WHERE key='backend'").fetchone()
    if previous and previous[0]!=backend:
        db.execute("UPDATE attachment_meta SET value='0' WHERE key='discovery_cursor'")
    db.execute("INSERT INTO attachment_meta VALUES('backend',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(backend,))
    db.commit()


def discover(db, limit=200):
    cursor=int(db.execute("SELECT value FROM attachment_meta WHERE key='discovery_cursor'").fetchone()[0])
    rows=db.execute('SELECT rowid,chat_id,id,media_type,deleted FROM messages WHERE rowid>? ORDER BY rowid LIMIT ?', (cursor,min(200,max(1,limit)))).fetchall()
    for row in rows:
        if not row['deleted'] and row['media_type'] in ('photo','image','sticker','document'):
            prior=db.execute('SELECT status,metadata,retry_at FROM attachment_analysis WHERE chat_id=? AND message_id=?',(row['chat_id'],row['id'])).fetchone()
            retry=0
            if prior:
                metadata=json.loads(prior['metadata'])
                upgrading=(os.environ.get('TG_ATTACHMENT_BACKEND')=='openai' and prior['status'] in ('done','partial') and metadata.get('cloud_version')!='openai-v1')
                if metadata.get('retry_exhausted'):continue
                if prior['status']!='failed' and not metadata.get('cloud_pending') and not upgrading:continue
                retry=0 if upgrading else prior['retry_at']
            db.execute('INSERT OR IGNORE INTO attachment_queue(chat_id,message_id,retry_at) VALUES(?,?,?)',(row['chat_id'],row['id'],retry))
    if rows: db.execute("UPDATE attachment_meta SET value=? WHERE key='discovery_cursor'",(str(rows[-1]['rowid']),))
    db.commit()


def request_rescan(db, chat_id):
    """Explicit recovery: enqueue a per-group cursor, with no history work here."""
    if not allowed(db,chat_id):raise ValueError('monitoring_not_authorized')
    db.execute('INSERT INTO attachment_group_rescan(chat_id,cursor) VALUES(?,0) ON CONFLICT(chat_id) DO UPDATE SET cursor=0',(int(chat_id),))


def drain_rescan(db,limit=200):
    group=db.execute('SELECT chat_id,cursor FROM attachment_group_rescan ORDER BY rowid LIMIT 1').fetchone()
    if not group:return
    if not allowed(db,group['chat_id']):
        db.execute('DELETE FROM attachment_group_rescan WHERE chat_id=?',(group['chat_id'],));db.commit();return
    rows=db.execute('SELECT id,deleted,media_type FROM messages WHERE chat_id=? AND id>? ORDER BY id LIMIT ?',
                    (group['chat_id'],group['cursor'],min(200,max(1,limit)))).fetchall()
    for row in rows:
        if not row['deleted'] and row['media_type'] in ('photo','image','sticker','document'):
            db.execute('''INSERT INTO attachment_queue(chat_id,message_id) VALUES(?,?)
                ON CONFLICT(chat_id,message_id) DO UPDATE SET retry_at=0,attempts=0,terminal=0,error=NULL''',(group['chat_id'],row['id']))
    if rows:db.execute('UPDATE attachment_group_rescan SET cursor=? WHERE chat_id=?',(rows[-1]['id'],group['chat_id']))
    else:db.execute('DELETE FROM attachment_group_rescan WHERE chat_id=?',(group['chat_id'],))
    db.commit()


def record_failure(db,row,reason,permanent=False):
    """Failed unknown media has no validated analysis, but still has a durable job."""
    if not db.in_transaction:db.execute('BEGIN IMMEDIATE')
    message=db.execute('SELECT deleted,media_hash FROM messages WHERE chat_id=? AND id=?',(row['chat_id'],row['message_id'])).fetchone()
    if message and not message['deleted'] and message['media_hash']==row['media_hash'] and allowed(db,row['chat_id']):
        db.execute('''UPDATE attachment_queue SET attempts=min(3,attempts+1),error=?,
            terminal=CASE WHEN ? OR attempts+1>=3 THEN 1 ELSE terminal END,retry_at=?
            WHERE chat_id=? AND message_id=?''',(reason,permanent,time.time()+300,row['chat_id'],row['message_id']))
    db.commit()


def current(db,row):
    message=db.execute('SELECT * FROM messages WHERE chat_id=? AND id=?',(row['chat_id'],row['message_id'])).fetchone()
    return bool(message and not message['deleted'] and message['media_hash'] and message['media_hash']==row['media_hash'] and allowed(db,row['chat_id']))


def pending(db,limit=4):
    discover(db)
    drain_rescan(db)
    # Take bounded queue entries first; excluded/expired groups cannot force a history scan.
    candidates=db.execute('''SELECT m.chat_id,m.id AS message_id,m.media_hash,m.media_size,
        m.media_type,m.media_name AS filename,m.deleted,a.status,a.retry_at,
        q.attempts AS attempts FROM attachment_queue q
        JOIN messages m ON m.chat_id=q.chat_id AND m.id=q.message_id
        LEFT JOIN attachment_analysis a ON a.chat_id=m.chat_id AND a.message_id=m.id
        WHERE q.terminal=0 AND q.retry_at<=? ORDER BY q.retry_at LIMIT 32''',(time.time(),)).fetchall()
    result=[]
    for candidate in candidates:
        row=dict(candidate)
        if not allowed(db,row['chat_id']):
            db.execute('UPDATE attachment_queue SET retry_at=? WHERE chat_id=? AND message_id=?',(time.time()+300,row['chat_id'],row['message_id']))
        elif not row['deleted']:
            result.append(row)
            if len(result)>=max(1,min(4,limit)): break
    db.commit()
    return result


def save(db,row,result):
    if not db.in_transaction: db.execute('BEGIN IMMEDIATE')
    if not current(db,row):
        db.commit();return False
    prior=db.execute('SELECT * FROM attachment_analysis WHERE chat_id=? AND message_id=? AND media_hash=?',(row['chat_id'],row['message_id'],row['media_hash'])).fetchone()
    result=dict(result)
    metadata=json.loads(prior['metadata']) if prior else {}
    queue=db.execute('SELECT attempts,terminal FROM attachment_queue WHERE chat_id=? AND message_id=?',(row['chat_id'],row['message_id'])).fetchone()
    failures=queue['attempts'] if queue else (prior['attempts'] if prior else 0)
    progressed=result.get('cloud_next',0)>metadata.get('cloud_next',0)
    ordinary_failure=(result['status']=='failed' or result.get('reason') in ('cloud_error','cloud_preparation_error','cloud_retry_exhausted')) and not progressed
    if ordinary_failure:failures=min(3,failures+1)
    if result['status']=='failed' and prior and prior['text']:
        result=dict(metadata,**result)
        result.update(text=prior['text'],status='partial',cloud_pending=True,retry_at=time.time()+3600)
    exhausted=failures>=3 or bool(queue and queue['terminal'])
    if exhausted:
        result.update(cloud_pending=False,retry_exhausted=True)
        result['status']='partial' if result.get('text') else 'failed_permanent'
    else:result.pop('retry_exhausted',None)
    attempts=failures
    status=result['status']
    retry=result.get('retry_at',time.time()+min(3600,60*2**min(attempts,8)))
    db.execute('''INSERT INTO attachment_analysis VALUES(?,?,?,?,?,?,?,?,datetime('now'),?)
        ON CONFLICT(chat_id,message_id) DO UPDATE SET media_hash=excluded.media_hash,text=excluded.text,
        status=excluded.status,attempts=excluded.attempts,error=excluded.error,metadata=excluded.metadata,
        updated_at=excluded.updated_at,retry_at=excluded.retry_at''',
        (row['chat_id'],row['message_id'],row['media_hash'],result.get('text',''),status,attempts,
        result.get('reason'),json.dumps({k:v for k,v in result.items() if k not in ('text','status')}),retry))
    if status=='failed' or result.get('cloud_pending'):
        db.execute('UPDATE attachment_queue SET retry_at=?,attempts=?,error=? WHERE chat_id=? AND message_id=?',(retry,attempts,result.get('reason'),row['chat_id'],row['message_id']))
    elif exhausted:
        db.execute('UPDATE attachment_queue SET terminal=1,attempts=?,error=? WHERE chat_id=? AND message_id=?',(attempts,result.get('reason'),row['chat_id'],row['message_id']))
    else: db.execute('DELETE FROM attachment_queue WHERE chat_id=? AND message_id=?',(row['chat_id'],row['message_id']))
    db.commit();return True


def analyze(path,filename,media_type):
    child=subprocess.run([sys.executable,'-m','tools.attachments.worker','--extract',str(path),
        '--filename',filename or '', '--media-type',media_type],capture_output=True,timeout=600,
        env=dict(os.environ,OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',TOKENIZERS_PARALLELISM='false'))
    if child.returncode: raise RuntimeError('extractor_failed')
    return json.loads(child.stdout)


class DownloadDeferred(Exception):
    pass


class Mcp:
    def call(self,name,args):
        request=urllib.request.Request('http://127.0.0.1:'+os.environ.get('TG_PORT','7255')+'/mcp',
            data=json.dumps({'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':name,'arguments':args}}).encode(),headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=180) as reply: data=json.loads(reply.read(1024*1024))
        result=data.get('result',{})
        if result.get('isError') or 'error' in data:
            detail=' '.join(part.get('text','') for part in result.get('content',[]))
            if 'FloodWait' in detail or 'SyncDeferred' in detail:raise DownloadDeferred('platform cooldown')
            raise RuntimeError('download_unavailable')
        return json.loads(result['content'][0]['text'])


def run_once(db,mcp,temp_root):
    for row in pending(db):
        try:
            if not allowed(db,row['chat_id']):continue
            if row['media_size'] and row['media_size']>MAX_FILE:
                if not save(db,row,dict(text='',status='failed_permanent',reason='file_size_limit')):
                    record_failure(db,row,'file_size_limit',permanent=True)
                continue
            with tempfile.TemporaryDirectory(dir=temp_root) as tmp:
                path=Path(tmp)/'attachment'
                reply=mcp.call('download_attachment',{'chat':str(row['chat_id']),'message_id':row['message_id'],
                    'expected_media_hash':row['media_hash'],'output_path':str(path)})
                if not path.is_file() or path.stat().st_size>MAX_FILE: raise RuntimeError('download_unavailable')
                # Old caches have no media identity; the daemon resolves it from the live message.
                if not row['media_hash']: row['media_hash']=reply.get('media_hash','')
                if not current(db,row):continue
                row['filename']=reply.get('filename',row['filename'])
                row['media_type']=reply.get('media_type',row['media_type'])
                outcome=analyze(path,row['filename'],'image' if row['media_type']=='photo' else row['media_type'])
                if outcome['status'] not in ('done','partial'):
                    save(db,row,outcome);continue
                if os.environ.get('TG_ATTACHMENT_BACKEND')=='openai':
                    from tools.attachments.cloud import enrich,SEPARATOR
                    from tools.attachments.cloud_client import CloudClient
                    previous=db.execute('SELECT metadata FROM attachment_analysis WHERE chat_id=? AND message_id=? AND media_hash=?',(row['chat_id'],row['message_id'],row['media_hash'])).fetchone()
                    previous=json.loads(previous[0]) if previous else {}
                    checkpoint=dict(previous,**outcome)
                    checkpoint.update(status='partial',cloud_pending=True,retry_at=time.time()+60,reason=outcome.get('reason',''))
                    if previous.get('cloud_text'):checkpoint['text']+=SEPARATOR+previous['cloud_text'].strip()
                    if not save(db,row,checkpoint):continue
                    # Both explicit paths are required: never silently create an independent budget.
                    client=CloudClient(Path(os.environ['TG_ATTACHMENT_CLOUD_DB']),Path(os.environ['TG_OPENAI_KEY_FILE']),
                        monthly_budget=float(os.environ.get('TG_ATTACHMENT_MONTHLY_USD','10')),
                        model=os.environ.get('TG_OPENAI_MODEL','gpt-5.4-mini-2026-03-17'),authorize=lambda:current(db,row))
                    outcome=enrich(path,row['filename'],'image' if row['media_type']=='photo' else row['media_type'],outcome,client,previous=previous)
                save(db,row,outcome)
        except DownloadDeferred:
            db.execute('UPDATE attachment_queue SET retry_at=? WHERE chat_id=? AND message_id=?',(time.time()+300,row['chat_id'],row['message_id']));db.commit()
            break
        except Exception as error:
            if not save(db,row,dict(text='',status='failed',reason=type(error).__name__)):
                record_failure(db,row,type(error).__name__)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--extract',type=Path);parser.add_argument('--filename',default='')
    parser.add_argument('--media-type',default='');parser.add_argument('--once',action='store_true')
    args=parser.parse_args();os.umask(0o077)
    if args.extract:
        import resource
        resource.setrlimit(resource.RLIMIT_AS,(6*1024**3,6*1024**3))
        resource.setrlimit(resource.RLIMIT_CPU,(540,540))
        from tools.attachments.extract import extract
        print(json.dumps(extract(args.extract,args.filename,args.media_type)));return
    store=Path(os.environ.get('TG_STORE',str(Path.home()/'.local/share/telegram-mcp'))).resolve()
    temp_root=store/'attachment-tmp';temp_root.mkdir(exist_ok=True,mode=0o700)
    with (temp_root/'worker.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        for entry in temp_root.glob('tmp*'):
            if entry.is_dir() and not entry.is_symlink() and entry.stat().st_mtime<time.time()-86400:
                shutil.rmtree(entry)
        db=sqlite3.connect((store/'messages.db').as_uri()+'?mode=rw',uri=True,timeout=30)
        initialize(db)
        try:
            while True:
                run_once(db,Mcp(),temp_root)
                if args.once:break
                time.sleep(30)
        finally:db.close()

if __name__=='__main__':main()
