"""Reaction snapshots and deletion tombstones; message content stays unmodified."""
import json
from types import SimpleNamespace
from telethon import events, types, utils
from consent import allowed, predicate


def install(db):
    db.executescript('''CREATE TABLE IF NOT EXISTS message_reactions(
        chat_id INTEGER NOT NULL,message_id INTEGER NOT NULL,text TEXT NOT NULL,
        PRIMARY KEY(chat_id,message_id));
        CREATE TABLE IF NOT EXISTS message_tombstones(
        chat_id INTEGER NOT NULL,message_id INTEGER NOT NULL,PRIMARY KEY(chat_id,message_id));
        CREATE TABLE IF NOT EXISTS nonchannel_tombstones(message_id INTEGER PRIMARY KEY);
        CREATE INDEX IF NOT EXISTS messages_deletion_id ON messages(id);
        CREATE TABLE IF NOT EXISTS message_event_clock(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),generation INTEGER NOT NULL);
        INSERT OR IGNORE INTO message_event_clock VALUES(1,0);
        CREATE TABLE IF NOT EXISTS message_versions(
        chat_id INTEGER NOT NULL,message_id INTEGER NOT NULL,
        content_generation INTEGER NOT NULL DEFAULT 0,reaction_generation INTEGER NOT NULL DEFAULT 0,
        edit_timestamp REAL NOT NULL DEFAULT 0,PRIMARY KEY(chat_id,message_id));
        CREATE TABLE IF NOT EXISTS retired_message_media(
        chat_id INTEGER NOT NULL,message_id INTEGER NOT NULL,media_hash TEXT NOT NULL,
        edit_timestamp REAL NOT NULL,PRIMARY KEY(chat_id,message_id,media_hash));
        CREATE TRIGGER IF NOT EXISTS reactions_message_deleted AFTER UPDATE OF deleted ON messages
        WHEN new.deleted=1 BEGIN
        DELETE FROM message_reactions WHERE chat_id=new.chat_id AND message_id=new.id;
        INSERT OR IGNORE INTO message_tombstones VALUES(new.chat_id,new.id); END;
        CREATE TRIGGER IF NOT EXISTS reactions_message_removed AFTER DELETE ON messages BEGIN
        DELETE FROM message_reactions WHERE chat_id=old.chat_id AND message_id=old.id; END;''')
    for event in ('INSERT','UPDATE'):
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS reactions_consent_{event.lower()}
            BEFORE {event} ON message_reactions WHEN NOT {predicate('new.chat_id')}
            BEGIN SELECT RAISE(IGNORE); END''')


def is_deleted(db, chat_id, message_id):
    return db.execute('SELECT 1 FROM message_tombstones WHERE chat_id=? AND message_id=?',(chat_id,message_id)).fetchone() is not None or db.execute('SELECT 1 FROM messages WHERE chat_id=? AND id=? AND deleted=1',(chat_id,message_id)).fetchone() is not None or (nonchannel(db,chat_id) and db.execute('SELECT 1 FROM nonchannel_tombstones WHERE message_id=?',(message_id,)).fetchone() is not None)


def nonchannel(db, chat_id):
    if utils.resolve_id(chat_id)[1] is types.PeerChannel:return False
    row=db.execute('SELECT type FROM chats WHERE id=?',(chat_id,)).fetchone()
    return not row or row[0] not in ('channel','supergroup')


def record_deleted(db, chat_id, ids):
    if not db.in_transaction:db.execute('BEGIN IMMEDIATE')
    for mid in ids:
        if chat_id is None:
            # User/basic-group IDs share an account-wide namespace; channel IDs do not.
            db.execute('INSERT OR IGNORE INTO nonchannel_tombstones VALUES(?)',(mid,))
            known=db.execute('SELECT chat_id FROM messages WHERE id=?',(mid,)).fetchall()
            for row in known:
                if nonchannel(db,row[0]):record_deleted(db,row[0],[mid])
            continue
        db.execute('INSERT OR IGNORE INTO message_tombstones VALUES(?,?)',(chat_id,mid))
        db.execute('UPDATE messages SET deleted=1 WHERE chat_id=? AND id=?',(chat_id,mid))
        # Consent guards may ignore updates while disabled. A received deletion still applies.
        db.execute('DELETE FROM messages WHERE chat_id=? AND id=? AND deleted=0',(chat_id,mid))
        db.execute('DELETE FROM message_reactions WHERE chat_id=? AND message_id=?',(chat_id,mid))


def reaction_name(reaction):
    if isinstance(reaction,types.ReactionEmoji):return reaction.emoticon
    if isinstance(reaction,types.ReactionCustomEmoji):return 'custom:'+str(reaction.document_id)
    return type(reaction).__name__


def generation(db):
    return db.execute('SELECT generation FROM message_event_clock WHERE singleton=1').fetchone()[0]


def advance_generation(db):
    db.execute('UPDATE message_event_clock SET generation=generation+1 WHERE singleton=1')
    return generation(db)


def accept_content(db, chat_id, message, snapshot_generation=None, media_hash=None):
    """Called in the same write transaction as the message and its attachments."""
    row=db.execute('SELECT content_generation,edit_timestamp FROM message_versions WHERE chat_id=? AND message_id=?',(chat_id,message.id)).fetchone()
    stamp=getattr(message,'edit_date',None)
    edited=stamp.timestamp() if stamp else 0
    if row and (edited<row[1] or snapshot_generation is not None and row[0]>snapshot_generation):return False
    if media_hash is not None:
        retired=db.execute('SELECT edit_timestamp FROM retired_message_media WHERE chat_id=? AND message_id=? AND media_hash=?',(chat_id,message.id,media_hash)).fetchone()
        if retired and edited<=retired[0]:return False
        previous=db.execute('SELECT media_hash FROM messages WHERE chat_id=? AND id=?',(chat_id,message.id)).fetchone()
        if previous and previous[0] and previous[0]!=media_hash:
            db.execute('''INSERT INTO retired_message_media VALUES(?,?,?,?)
                ON CONFLICT(chat_id,message_id,media_hash) DO UPDATE
                SET edit_timestamp=MAX(edit_timestamp,excluded.edit_timestamp)''',(chat_id,message.id,previous[0],edited))
    gen=advance_generation(db) if snapshot_generation is None else snapshot_generation
    db.execute('''INSERT INTO message_versions(chat_id,message_id,content_generation,edit_timestamp)
        VALUES(?,?,?,?) ON CONFLICT(chat_id,message_id) DO UPDATE
        SET content_generation=excluded.content_generation,edit_timestamp=excluded.edit_timestamp''',(chat_id,message.id,gen,edited))
    return True


def capture_reactions(db, chat_id, message, snapshot_generation=None):
    if not db.in_transaction:db.execute('BEGIN IMMEDIATE')
    if not allowed(db,chat_id) or is_deleted(db,chat_id,message.id):return
    row=db.execute('SELECT reaction_generation FROM message_versions WHERE chat_id=? AND message_id=?',(chat_id,message.id)).fetchone()
    if snapshot_generation is not None and row and row[0]>snapshot_generation:return
    gen=advance_generation(db) if snapshot_generation is None else snapshot_generation
    # Keep the generation even when the visible snapshot becomes empty.
    db.execute('''INSERT INTO message_versions(chat_id,message_id,reaction_generation) VALUES(?,?,?)
        ON CONFLICT(chat_id,message_id) DO UPDATE SET reaction_generation=excluded.reaction_generation''',(chat_id,message.id,gen))
    reactions=getattr(message,'reactions',None)
    if not reactions or not reactions.results:
        db.execute('DELETE FROM message_reactions WHERE chat_id=? AND message_id=?',(chat_id,message.id))
        return
    # Telegram may expose totals only, or a partial recent reactor list.
    text=json.dumps({'counts':[{'reaction':reaction_name(r.reaction),'count':r.count} for r in reactions.results[:100]],
        'recent':[{'peer_id':utils.get_peer_id(r.peer_id),'reaction':reaction_name(r.reaction)} for r in (getattr(reactions,'recent_reactions',None) or [])[:100]],'complete_reactor_list':False},ensure_ascii=False)
    db.execute('''INSERT INTO message_reactions VALUES(?,?,?) ON CONFLICT(chat_id,message_id)
        DO UPDATE SET text=excluded.text WHERE text IS NOT excluded.text''',(chat_id,message.id,text))


def register_events(api):
    @api['S'].client.on(events.Raw(types=types.UpdateMessageReactions))
    async def on_reactions(event):
        cid=utils.get_peer_id(event.peer)
        c=api['db']()
        try:
            with c:capture_reactions(c,cid,SimpleNamespace(id=event.msg_id,reactions=event.reactions))
        finally:c.close()
