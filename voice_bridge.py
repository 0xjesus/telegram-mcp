#!/usr/bin/python3
"""One SSH request, then exit. No model, daemon, shell or account credentials."""
import contextlib
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shutil
import sqlite3
import subprocess
import sys
import urllib.request

MAX_AUDIO = 25 * 1024 * 1024
PREFIX = '[Nota de voz] '
FIELDS = {'wa': ('chat_jid', 'message_id', 'content', 'timestamp', ('audio', 'video')),
          'tg': ('chat_id', 'id', 'text', 'date', ('voice','audio','video','video_note'))}
MAX_SECONDS = 600
FFMPEG = Path(os.environ.get('VOICE_FFMPEG') or shutil.which('ffmpeg') or Path.home()/'.local/share/transcriptrealtime/voice-notes/ffmpeg/ffmpeg')


class Bridge:
    def __init__(self, wa=None, tg=None):
        self.stores = {'wa': Path(wa or Path.home()/'.local/share/whatsapp-mcp/store'),
                       'tg': Path(tg or Path.home()/'.local/share/telegram-mcp')}

    @contextlib.contextmanager
    def db(self, platform, write=False):
        path = self.stores[platform] / 'messages.db'
        c = sqlite3.connect(path.as_uri() + ('?mode=rw' if write else '?mode=ro'), uri=True, timeout=5)
        c.row_factory = sqlite3.Row
        try:
            if write: c.execute('BEGIN IMMEDIATE')
            yield c
            if write: c.commit()
        finally:
            c.close()

    def pending(self):
        jobs = []
        for platform, (chat, ident, body, date, media) in FIELDS.items():
            extra = "AND m.chat_jid!='status@broadcast' AND (m.chat_jid NOT LIKE '%@g.us' OR EXISTS(SELECT 1 FROM group_monitoring_consent g WHERE g.chat_jid=m.chat_jid AND g.allowed=1 AND (g.evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',g.updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900)))" if platform == 'wa' else "AND m.deleted=0 AND (m.chat_id>0 OR EXISTS(SELECT 1 FROM chats pc WHERE pc.id=m.chat_id AND pc.type='channel') OR EXISTS(SELECT 1 FROM group_monitoring_consent pg WHERE pg.chat_id=m.chat_id AND pg.allowed=1 AND (pg.evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',pg.updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900)))"  # stories are not conversation
            try:
                with self.db(platform) as c:
                    identity = "m.media_hash" if platform=='tg' and 'media_hash' in {r[1] for r in c.execute('PRAGMA table_info(messages)')} else "''"
                    rows = c.execute(f'''SELECT {identity} AS media_hash,m.{chat} AS chat,m.id,CAST(strftime('%s',m.{date}) AS INTEGER) AS date,t.status
                        FROM messages m LEFT JOIN transcripts t ON t.{chat}=m.{chat} AND t.{ident}=m.id
                        WHERE m.media_type IN ({','.join('?'*len(media))}) {extra} AND (t.status IS NULL OR
                        (t.status IN ('failed','error') AND COALESCE(t.attempts,0)<3 AND
                         (COALESCE(t.error,'')!='unavailable' OR julianday(t.updated_at)<julianday('now','-5 minutes'))) OR
                        (t.status='done' AND (m.{body} IS NULL OR m.{body}='' OR
                         (m.media_type='video' AND m.{body} NOT LIKE '[Audio del video] %'
                          AND instr(m.{body},char(10)||'[Audio del video] ')=0))))
                        ORDER BY m.{date} DESC LIMIT 1''', tuple(media)).fetchall()
            except (sqlite3.Error,OSError):
                continue
            jobs.extend(dict(platform=platform, chat=str(r['chat']), id=str(r['id']),
                             date=r['date'], repair=r['status']=='done',**({'media_hash':r['media_hash']} if platform=='tg' else {})) for r in rows)
        return {'jobs': sorted(jobs, key=lambda j: j['date'])}

    @staticmethod
    def extract_audio(src, dst):
        """Video -> mono 16 kHz Opus/Ogg. Writes to a .part file so a killed ffmpeg never leaves a
        truncated .ogg that a later pass would transcribe as complete. No audio stream is permanent."""
        src, dst = Path(src), Path(dst)
        part = dst.with_suffix(dst.suffix + '.part')
        try:
            result = subprocess.run([str(FFMPEG),'-nostdin','-v','error','-y','-i',str(src),'-vn','-ac','1','-ar','16000',
                                     '-t',str(MAX_SECONDS+1),'-c:a','libopus','-b:a','24k','-f','ogg',str(part)],
                                    capture_output=True, timeout=120)
        except subprocess.TimeoutExpired:
            part.unlink(missing_ok=True)
            raise ValueError('invalid_audio') from None
        if result.returncode or not part.exists() or part.stat().st_size == 0:
            part.unlink(missing_ok=True)
            stderr = result.stderr or b''
            raise ValueError('no_audio' if (b'does not contain any stream' in stderr or b'Output file is empty' in stderr or not result.returncode) else 'invalid_audio')
        part.replace(dst)

    def wa_path(self, key):
        return self.stores['wa']/'uploads/_transcribe'/(hashlib.sha256(('\0'.join(key)).encode()).hexdigest()+'.ogg')

    @staticmethod
    def _mcp_client(port):
        session = None
        def call(method, params):
            nonlocal session
            headers = {'Content-Type':'application/json', 'Accept':'application/json, text/event-stream'}
            if session: headers['Mcp-Session-Id'] = session
            body = json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode()
            req = urllib.request.Request(f'http://127.0.0.1:{port}/mcp', data=body, headers=headers)
            with urllib.request.urlopen(req, timeout=45) as response:
                session = response.headers.get('Mcp-Session-Id') or session
                data = response.read(8*1024*1024+1)
            if len(data)>8*1024*1024: raise RuntimeError('unavailable')
            result = json.loads(data).get('result', {})
            if result.get('isError'): raise RuntimeError('unavailable')
            return result
        call('initialize', {'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'voice-notes-bridge','version':'1'}})
        return call

    @staticmethod
    def download_whatsapp(mid, chat, path):
        call = Bridge._mcp_client(7343)
        status = call('tools/call', {'name':'get_status','arguments':{}})
        state = json.loads(next(x['text'] for x in status['content'] if x.get('type')=='text'))
        if not state.get('connected') or state.get('health',{}).get('state')!='ok':
            raise RuntimeError('unavailable')
        result = call('tools/call', {'name':'download_media','arguments':{'chat_jid':chat,'message_id':mid,'output_path':str(path)}})
        result = json.loads(next(x['text'] for x in result['content'] if x.get('type')=='text'))
        if not result.get('Success'):
            message = result.get('Message','')
            if re.search(r'\b(404|410)\b',message) or 'incomplete media information' in message:
                raise ValueError('invalid_audio')
            raise RuntimeError('unavailable')

    @staticmethod
    def download_telegram(mid, chat):
        """Fetch only the selected voice through the existing account daemon."""
        call = Bridge._mcp_client(7255)
        status = call('tools/call', {'name':'get_status','arguments':{}})
        state = json.loads(next(x['text'] for x in status['content'] if x.get('type')=='text'))
        if not state.get('connected') or not state.get('paired') or state.get('health',{}).get('state')!='ok':
            raise RuntimeError('unavailable')
        result = call('tools/call', {'name':'download_media','arguments':{'chat':chat,'message_id':int(mid)}})
        result = json.loads(next(x['text'] for x in result['content'] if x.get('type')=='text'))
        if not isinstance(result.get('path'),str) or not result['path']:
            raise ValueError('invalid_audio')
        return Path(result['path'])

    @staticmethod
    def monitoring_allowed(c, platform, chat_id):
        if platform=='wa':
            if not chat_id.endswith('@g.us'):return True
            try:return bool(c.execute("SELECT 1 FROM group_monitoring_consent WHERE chat_jid=? AND allowed=1 AND (evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900)",(chat_id,)).fetchone())
            except sqlite3.OperationalError:return False
        if int(chat_id)>0:return True
        if c.execute("SELECT 1 FROM chats WHERE id=? AND type='channel'",(chat_id,)).fetchone():return True
        try:return bool(c.execute("SELECT 1 FROM group_monitoring_consent WHERE chat_id=? AND allowed=1 AND (evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900)",(chat_id,)).fetchone())
        except sqlite3.OperationalError:return False

    def handle(self, request):
        allowed = {'pending':{'op'}, 'audio':{'op','job'},
                   'complete':{'op','job','text'}, 'fail':{'op','job','reason'}}
        op = request.get('op')
        if op not in allowed or set(request)-allowed[op]: raise ValueError('invalid_request')
        if op == 'pending': return self.pending()
        job = request.get('job', {})
        if not isinstance(job,dict) or set(job)-{'platform','chat','id','date','repair','media_hash'}: raise ValueError('invalid_job')
        platform = job.get('platform')
        if platform not in FIELDS: raise ValueError('invalid_job')
        if any(not isinstance(job.get(k),str) or not 0<len(job[k])<=200 for k in ['chat','id']): raise ValueError('invalid_job')
        chat, ident, body, date, media = FIELDS[platform]
        key = (job['chat'], job['id'])
        with self.db(platform, op in ['complete','fail']) as c:
            if not self.monitoring_allowed(c,platform,key[0]):raise ValueError('monitoring_not_authorized')
            extra = '' if platform=='wa' else 'AND deleted=0'
            row = c.execute(f"SELECT * FROM messages WHERE {chat}=? AND id=? AND media_type IN ({','.join('?'*len(media))}) {extra}", (*key,*media)).fetchone()
            if row is None: raise ValueError('invalid_job')
            if platform=='tg' and 'media_hash' in row.keys() and job.get('media_hash')!=row['media_hash']:raise ValueError('invalid_job')
            previous = c.execute(f'SELECT * FROM transcripts WHERE {chat}=? AND {ident}=?', key).fetchone()
            if previous and previous['status']=='failed_permanent':
                if op=='audio': raise ValueError('invalid_job')
                return {'ok':True}
            if op == 'audio':
                if previous and (previous['status']=='done' or (previous['attempts'] or 0)>=3): raise ValueError('invalid_job')
                root = self.stores[platform] / ('uploads/_transcribe' if platform=='wa' else 'media')
                if platform == 'wa':
                    path = self.wa_path(key)
                    root.mkdir(parents=True,exist_ok=True,mode=0o700)
                    if not path.exists():
                        if row['media_type'] == 'video':
                            temp = path.with_suffix('.mp4')
                            try:
                                self.download_whatsapp(job['id'],job['chat'],temp)
                                self.extract_audio(temp,path)
                            finally:
                                temp.unlink(missing_ok=True)
                        else:
                            self.download_whatsapp(job['id'],job['chat'],path)
                else:
                    path = Path(row['media_path']) if row['media_path'] else self.download_telegram(job['id'],job['chat'])
                    if not job.get('media_hash'):
                        # The daemon fills identity for legacy rows; retry with a fresh job.
                        raise RuntimeError('unavailable')
                    if row['media_type'] in ('video','video_note','audio'):
                        if not path.resolve().is_relative_to(root.resolve()) or path.stat().st_size>50*1024*1024:raise ValueError('invalid_audio')
                        converted=root/('transcribe-'+hashlib.sha256(('\0'.join(key)+job['media_hash']).encode()).hexdigest()+'.ogg')
                        self.extract_audio(path,converted)
                        path=converted
                try: path = path.resolve(strict=True)
                except FileNotFoundError: raise ValueError('invalid_audio') from None
                if not path.is_relative_to(root.resolve()) or not path.is_file(): raise ValueError('invalid_audio')
                with path.open('rb') as source: audio = source.read(MAX_AUDIO+1)
                if not audio or len(audio)>MAX_AUDIO: raise ValueError('invalid_audio')
                if not self.monitoring_allowed(c,platform,key[0]):raise ValueError("monitoring_not_authorized")
                if platform=='tg' and row['media_type'] in ('video','video_note','audio'):path.unlink(missing_ok=True)
                return audio
            if op == 'fail':
                if request.get('reason') not in ['unavailable','invalid_audio','no_audio']: raise ValueError('invalid_request')
                if previous and previous['status']=='done': return {'ok':True}
                text, status, error = None, ('failed' if platform=='wa' else 'error'), request['reason']
                if error == 'no_audio': status = 'failed_permanent'  # a clip with no audio stream never becomes transcribable
            else:
                text = previous['text'] if previous and previous['status']=='done' else request.get('text')
                if not isinstance(text,str) or len(text)>32768: raise ValueError('invalid_text')
                status, error = 'done', None
            attempts = (previous['attempts'] or 0) if previous else 0
            attempts = min(3, attempts + (0 if error=='unavailable' or previous and previous['status']=='done' else 1))
            now = dt.datetime.now(dt.timezone.utc).isoformat()
            columns = [chat,ident,'text','status','attempts','error','updated_at']
            values = [*key,text,status,attempts,error,now]
            if platform=='wa': columns += ['model','created_at']; values += ['nemotron-mac-es-US',now]
            updates = ','.join(f'{x}=excluded.{x}' for x in columns[2:] if x!='created_at')
            c.execute(f"INSERT INTO transcripts({','.join(columns)}) VALUES({','.join('?' for _ in columns)}) ON CONFLICT({chat},{ident}) DO UPDATE SET {updates}", values)
            if op=='complete':
                prefix = '[Audio del video] ' if row['media_type'] in ('video','video_note') else '[Audio] ' if row['media_type']=='audio' and platform=='tg' else PREFIX
                content = row[body] or ''
                if row['media_type'] in ('video','video_note','audio'):
                    # The message body is also used by clients without a transcripts
                    # join. Keep captions and manual transcript corrections intact.
                    if not any(line.startswith(prefix) for line in content.split('\n')):
                        content = (content + '\n' if content else '') + prefix + (text or '(inaudible)')
                elif not content or content.startswith(prefix):
                    content = prefix + (text or '(inaudible)')
                if content != (row[body] or ''):
                    c.execute(f"UPDATE messages SET {body}=? WHERE {chat}=? AND id=?", (content,*key))
        if op=='complete' and platform=='wa':
            with contextlib.suppress(OSError): self.wa_path(key).unlink()
        return {'ok':True}


def main():
    os.umask(0o077)
    signal.alarm(150)
    try:
        if os.environ.get('SSH_ORIGINAL_COMMAND','voice-notes')!='voice-notes': raise ValueError('invalid_request')
        data = sys.stdin.buffer.read(256*1024+1)
        if len(data)>256*1024: raise ValueError('invalid_request')
        result = Bridge().handle(json.loads(data))
        if isinstance(result,bytes):
            sys.stdout.buffer.write(json.dumps({'ok':True,'bytes':len(result)}).encode()+b'\n'+result)
        else:
            print(json.dumps({'ok':True,**result}))
    except ValueError as exc:
        print(json.dumps({'ok':False,'error':'invalid_audio' if str(exc)=='invalid_audio' else 'invalid_request'}))
    except Exception:
        print(json.dumps({'ok':False,'error':'unavailable'}))


if __name__=='__main__': main()
