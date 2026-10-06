"""Bounded images and sampled video frames returned as MCP image content."""
import asyncio
import base64
import io
import json
import math
import os
from pathlib import Path
import subprocess

FORMATS='mov,matroska,webm,avi,ogg,mpeg,mpegts'


def preview(path,media_type):
    if media_type not in ('photo','image','sticker','video','video_note','gif'):raise ValueError('preview_not_supported')
    if path.stat().st_size>50*1024*1024:raise ValueError('file_size_limit')
    if media_type in ('photo','image','sticker'):
        from PIL import Image,ImageOps
        with Image.open(path) as image:
            if image.width*image.height>20_000_000:raise ValueError('image_size_limit')
            image=ImageOps.exif_transpose(image).convert('RGB');image.thumbnail((1280,1280))
            output=io.BytesIO();image.save(output,format='JPEG',quality=85)
            return [{'type':'image','mimeType':'image/jpeg','data':base64.b64encode(output.getvalue()).decode()}]
    ffmpeg=os.environ.get('TG_FFMPEG','ffmpeg');ffprobe=os.environ.get('TG_FFPROBE','ffprobe')
    probe=subprocess.run([ffprobe,'-v','error','-protocol_whitelist','file,pipe','-format_whitelist',FORMATS,'-show_entries','format=duration','-of','json',str(path)],capture_output=True,timeout=15,check=True)
    duration=float(json.loads(probe.stdout)['format']['duration'])
    if not math.isfinite(duration) or duration<=0:raise ValueError('invalid_duration')
    result=[]
    for i in range(6):
        frame=subprocess.run([ffmpeg,'-nostdin','-v','error','-threads','1','-protocol_whitelist','file,pipe','-format_whitelist',FORMATS,'-ss',str(duration*(i+.5)/6),'-i',str(path),'-an','-frames:v','1','-vf','scale=640:640:force_original_aspect_ratio=decrease','-threads','1','-f','image2pipe','-vcodec','mjpeg','pipe:1'],capture_output=True,timeout=20,check=True)
        if not frame.stdout or len(frame.stdout)>1024*1024:raise ValueError('invalid_frame')
        result.append({'type':'image','mimeType':'image/jpeg','data':base64.b64encode(frame.stdout).decode()})
    return result


def register(tool,api):
    lock=asyncio.Lock()
    @tool('get_media_preview','Muestra una imagen o hasta seis fotogramas de un video. Es una muestra visual, no un análisis completo del video.',{'type':'object','properties':{'chat':{'type':'string'},'message_id':{'type':'integer'}},'required':['chat','message_id']})
    async def get_preview(a):
        async with lock:
            result=await api['t_download'](a)
            cid=await api['resolve'](a['chat']);api['require_monitoring'](cid)
            content=await asyncio.to_thread(preview,Path(result['path']),result['type'])
            api['require_monitoring'](cid)
            from message_extras import is_deleted
            c=api['db']()
            try:
                row=c.execute('SELECT media_hash FROM messages WHERE chat_id=? AND id=?',(cid,a['message_id'])).fetchone()
                if not row or row[0]!=result['media_hash'] or is_deleted(c,cid,a['message_id']):raise RuntimeError('media_changed_or_deleted')
            finally:c.close()
            return {'_mcp_content':[{'type':'text','text':json.dumps({'frames':len(content),'sampled':result['type'] in ('video','video_note','gif')})},*content]}
