"""Resumable cloud enrichment; retain the complete supported local extraction."""
import base64
from datetime import datetime, timezone
import io
from pathlib import Path
import time
import zipfile

from tools.attachments.extract import IMAGE_SUFFIXES, MAX_EXPANDED, MAX_PAGES, natural

SEPARATOR = '\n\n[Análisis generado por IA — OpenAI]\n'
VERSION = 'openai-v1'


def image_content(image):
    from PIL import ImageOps
    prepared = ImageOps.exif_transpose(image).convert('RGB')
    try:
        prepared.thumbnail((2048, 2048))
        buffer = io.BytesIO()
        prepared.save(buffer, format='JPEG', quality=90)
        return {'type':'input_image', 'detail':'high',
                'image_url':'data:image/jpeg;base64,'+base64.b64encode(buffer.getvalue()).decode('ascii')}
    finally:
        prepared.close()


def units(path, filename, media_type, text):
    """Yield stable units, covering every supported text block and raster visual."""
    from PIL import Image
    suffix = Path(filename or path.name).suffix.lower()
    for start in range(0, len(text), 12000):
        yield f'Texto {start//12000+1}', [{'type':'input_text','text':text[start:start+12000]}]
    with path.open('rb') as stream:
        is_pdf = stream.read(5) == b'%PDF-'
    if suffix == '.pdf' or is_pdf:
        import pypdfium2 as pdfium
        with pdfium.PdfDocument(path) as document:
            for index in range(min(len(document), MAX_PAGES)):
                page = document[index]
                try:
                    bitmap = page.render(scale=min(2.5,2048/max(page.get_size())))
                    try:
                        image = bitmap.to_pil()
                        try:
                            content = image_content(image)
                        finally:
                            image.close()
                    finally:
                        bitmap.close()
                finally:
                    page.close()
                yield f'Página {index+1}', [content]
    elif suffix in IMAGE_SUFFIXES or media_type in ('image','sticker'):
        with Image.open(path) as image:
            content = image_content(image)
        yield 'Imagen', [content]
    elif suffix in ('.docx','.xlsx','.pptx'):
        with zipfile.ZipFile(path) as archive:
            entries = archive.infolist()
            if len(entries)>10000 or sum(e.file_size for e in entries)>MAX_EXPANDED:
                raise ValueError('archive_limit')
            for name in sorted(archive.namelist(),key=natural):
                if name.startswith(('word/media/','xl/media/','ppt/media/')) and Path(name).suffix.lower() in IMAGE_SUFFIXES:
                    with Image.open(io.BytesIO(archive.read(name))) as image:
                        content = image_content(image)
                    yield name, [content]


def next_month():
    now=datetime.now(timezone.utc)
    return datetime(now.year+(now.month==12), 1 if now.month==12 else now.month+1, 1, tzinfo=timezone.utc).timestamp()


def enrich(path, filename, media_type, local, client, previous=None, max_units=8):
    from tools.attachments.cloud_client import BudgetExceeded, CloudError, RequestDeferred, AuthorizationDenied, RetryExhausted
    if local['status'] not in ('done','partial'):
        return local
    previous = previous or {}
    if previous.get('cloud_version') != VERSION:
        previous = {}
    cursor = previous.get('cloud_next',0)
    analysis = previous.get('cloud_text','')
    incomplete = previous.get('cloud_incomplete',False)
    outcome = dict(local, backend='openai', cloud_version=VERSION, cloud_pending=True)
    reason = 'cloud_pending'
    retry_at = time.time()+60
    processed = 0
    try:
        for index, (label, content) in enumerate(units(Path(path),filename,media_type,local.get('text',''))):
            if index < cursor:
                continue
            if processed >= max_units:
                break
            answer = client.analyze(content)
            analysis += f'\n[{label}]\n'+answer['text']
            incomplete = incomplete or not answer['complete']
            cursor = index+1
            processed += 1
        else:
            outcome['cloud_pending'] = False
            reason = 'cloud_incomplete' if incomplete else local.get('reason','')
            if reason == 'vision_not_configured':
                reason = ''
    except AuthorizationDenied:
        raise
    except BudgetExceeded:
        reason, retry_at = 'cloud_budget', next_month()
    except RequestDeferred:
        reason = 'cloud_deferred'
        retry_at = time.time()+300
    except RetryExhausted:
        outcome['cloud_pending'] = False
        reason = 'cloud_retry_exhausted'
    except CloudError:
        reason = 'cloud_error'
        retry_at = time.time()+3600
    except Exception:
        reason = 'cloud_preparation_error'
        retry_at = time.time()+3600
    # Bound generated text separately without dropping original extracted evidence.
    if len(analysis)>1_000_000:
        analysis=analysis[:1_000_000]
        incomplete=True
    outcome.update(cloud_next=cursor, cloud_text=analysis, cloud_incomplete=incomplete,
                   text=local.get('text','')+(SEPARATOR+analysis.strip() if analysis else ''),
                   reason=reason, retry_at=retry_at)
    outcome['status'] = 'partial' if outcome['cloud_pending'] or incomplete or reason else 'done'
    if local['status']=='partial' and local.get('reason')!='vision_not_configured':
        outcome['status']='partial'
    return outcome
