"""Manual group overrides and optional size-based automatic monitoring."""
import argparse
import datetime
import json
import os
from pathlib import Path
import sqlite3

SCHEMA='''CREATE TABLE IF NOT EXISTS group_monitoring_consent(
 chat_id INTEGER PRIMARY KEY,allowed INTEGER NOT NULL DEFAULT 0,
 evidence TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS group_monitoring_audit(
 seq INTEGER PRIMARY KEY AUTOINCREMENT,chat_id INTEGER NOT NULL,allowed INTEGER NOT NULL,
 evidence TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS group_monitoring_policy(singleton INTEGER PRIMARY KEY CHECK(singleton=1),enabled INTEGER NOT NULL DEFAULT 0,max_members INTEGER NOT NULL DEFAULT 10);
CREATE TABLE IF NOT EXISTS group_monitoring_inventory(chat_id INTEGER PRIMARY KEY,name TEXT NOT NULL,member_count INTEGER,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS group_policy_audit(seq INTEGER PRIMARY KEY,enabled INTEGER,max_members INTEGER,evidence TEXT,updated_at TEXT);'''


def predicate(column):
    return f"({column}>0 OR EXISTS(SELECT 1 FROM chats policy_chat WHERE policy_chat.id={column} AND policy_chat.type='channel') OR EXISTS(SELECT 1 FROM group_monitoring_consent policy_consent WHERE policy_consent.chat_id={column} AND policy_consent.allowed=1 AND (policy_consent.evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',policy_consent.updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900)))"


def allowed(db, chat_id):
    chat_id=int(chat_id)
    if chat_id>0:return True
    try:
        return bool(db.execute('SELECT '+predicate('?'),(chat_id,chat_id,chat_id)).fetchone()[0])
    except sqlite3.OperationalError:
        return False


def install(db):
    db.executescript(SCHEMA)
    for table in ('messages','transcripts'):
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone():continue
        for event in ('INSERT','UPDATE'):
            db.execute(f'DROP TRIGGER IF EXISTS monitoring_guard_{table}_{event.lower()}')
            db.execute(f'''CREATE TRIGGER IF NOT EXISTS monitoring_guard_{table}_{event.lower()}
                BEFORE {event} ON {table} WHEN NOT {predicate('new.chat_id')}
                BEGIN SELECT RAISE(IGNORE); END''')


def set_consent(db, chat_id, permit, evidence, confirmed=False):
    chat_id=int(chat_id)
    row=db.execute('SELECT type FROM chats WHERE id=?',(chat_id,)).fetchone()
    if chat_id>=0 or not row or row[0] not in ('group','supergroup'):
        raise ValueError('exact_known_group_id_required')
    if evidence.startswith('auto:max-members:'):
        raise ValueError('reserved_automatic_evidence_prefix')
    if not evidence.strip() or permit and not confirmed:
        raise ValueError('explicit_confirmation_and_evidence_required')
    install(db)
    now=datetime.datetime.now(datetime.timezone.utc).isoformat()
    with db:
        db.execute('''INSERT INTO group_monitoring_consent VALUES(?,?,?,?)
            ON CONFLICT(chat_id) DO UPDATE SET allowed=excluded.allowed,evidence=excluded.evidence,updated_at=excluded.updated_at''',
            (chat_id,int(permit),evidence.strip(),now))
        db.execute('INSERT INTO group_monitoring_audit(chat_id,allowed,evidence,updated_at) VALUES(?,?,?,?)',
            (chat_id,int(permit),evidence.strip(),now))
        if permit and db.execute("SELECT 1 FROM sqlite_master WHERE name='chat_sync'").fetchone():
            # Revisit the gap while monitoring was disabled; never infer a completed backfill.
            db.execute("UPDATE chat_sync SET backfill_id=0,backfill_status='pending',last_backfill_at=0,last_incremental_at=0,incremental_pending=1 WHERE chat_id=?",(chat_id,))
            db.execute('UPDATE chats SET backfill_done=0 WHERE id=?',(chat_id,))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('allow','revoke','list','auto-enable','auto-disable','inventory'))
    parser.add_argument('--group',type=int)
    parser.add_argument('--evidence',default='')
    parser.add_argument('--confirm',action='store_true')
    parser.add_argument('--max-members',type=int,default=10)
    args=parser.parse_args()
    path=Path(os.environ.get('TG_STORE',str(Path.home()/'.local/share/telegram-mcp'))).resolve()/'messages.db'
    with sqlite3.connect(path.as_uri()+'?mode=rw',uri=True,timeout=30) as db:
        if args.action in ('auto-enable','auto-disable'):
            if not args.confirm:parser.error('--confirm required for policy changes')
            configure_policy(db,args.action=='auto-enable',args.max_members,args.evidence)
            print(json.dumps({'automatic':args.action=='auto-enable','max_members':args.max_members}))
        elif args.action=='inventory':
            install(db);db.row_factory=sqlite3.Row
            print(json.dumps([dict(r) for r in db.execute('SELECT * FROM group_monitoring_inventory ORDER BY name')]))
        elif args.action=='list':
            install(db);db.row_factory=sqlite3.Row
            print(json.dumps([dict(r) for r in db.execute('SELECT * FROM group_monitoring_consent')]))
        else:
            if args.group is None:parser.error('--group is required')
            set_consent(db,args.group,args.action=='allow',args.evidence,args.confirm)
            print(json.dumps({'group':args.group,'monitoring':args.action=='allow'}))



AUTO_PREFIX='auto:max-members:'


def configure_policy(db, enabled, max_members=10, evidence=''):
    if type(max_members) is not int or not 1<=max_members<=1000 or not evidence.strip():
        raise ValueError('invalid_policy_or_missing_authorization')
    install(db)
    with db:
        db.execute('INSERT INTO group_monitoring_policy VALUES(1,?,?) ON CONFLICT(singleton) DO UPDATE SET enabled=excluded.enabled,max_members=excluded.max_members',(int(enabled),max_members))
        db.execute('INSERT INTO group_policy_audit(enabled,max_members,evidence,updated_at) VALUES(?,?,?,?)',(int(enabled),max_members,evidence,datetime.datetime.now(datetime.timezone.utc).isoformat()))
        # Changed rules require fresh membership counts before automatic admission.
        db.execute("UPDATE group_monitoring_consent SET allowed=0 WHERE evidence LIKE 'auto:max-members:%'")


def refresh_group(db, chat_id, name, member_count):
    now=datetime.datetime.now(datetime.timezone.utc).isoformat()
    valid_count=member_count if type(member_count) is int and member_count>0 else None
    db.execute('INSERT INTO group_monitoring_inventory VALUES(?,?,?,?) ON CONFLICT(chat_id) DO UPDATE SET name=excluded.name,member_count=excluded.member_count,updated_at=excluded.updated_at',(int(chat_id),name,valid_count,now))
    config=db.execute('SELECT enabled,max_members FROM group_monitoring_policy WHERE singleton=1').fetchone()
    if not config or not config[0]: return
    existing=db.execute('SELECT allowed,evidence FROM group_monitoring_consent WHERE chat_id=?',(chat_id,)).fetchone()
    if existing and not existing[1].startswith(AUTO_PREFIX):return
    permit=valid_count is not None and valid_count<=config[1]
    db.execute('INSERT INTO group_monitoring_consent VALUES(?,?,?,?) ON CONFLICT(chat_id) DO UPDATE SET allowed=excluded.allowed,evidence=excluded.evidence,updated_at=excluded.updated_at',(chat_id,int(permit),AUTO_PREFIX+str(config[1]),now))
    if permit and (not existing or not existing[0]):
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='chat_sync'").fetchone():
            db.execute("UPDATE chat_sync SET backfill_id=0,backfill_status='pending',last_backfill_at=0,last_incremental_at=0,incremental_pending=1 WHERE chat_id=?",(chat_id,))
            db.execute('UPDATE chats SET backfill_done=0 WHERE id=?',(chat_id,))


def finish_inventory(db, seen):
    seen=set(seen)
    for row in db.execute("SELECT chat_id FROM group_monitoring_consent WHERE evidence LIKE 'auto:max-members:%'").fetchall():
        if row[0] not in seen:
            db.execute('UPDATE group_monitoring_consent SET allowed=0 WHERE chat_id=?',(row[0],))
            db.execute('UPDATE group_monitoring_inventory SET member_count=NULL WHERE chat_id=?',(row[0],))


def invalidate_group(db, chat_id):
    db.execute("UPDATE group_monitoring_consent SET allowed=0 WHERE chat_id=? AND evidence LIKE 'auto:max-members:%'",(chat_id,))

if __name__=='__main__':main()
