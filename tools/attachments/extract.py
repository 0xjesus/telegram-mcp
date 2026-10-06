"""Bounded, local attachment text extraction. Nothing is executed from documents."""
import io
import json
import os
from pathlib import Path
import re
import zipfile
from xml.etree import ElementTree as ET

MAX_FILE = 50 * 1024 * 1024
MAX_EXPANDED = 100 * 1024 * 1024
MAX_TEXT = 2_000_000
MAX_PAGES = 500
_OCR = None


def result(text='', status='done', **metadata):
    if len(text) > MAX_TEXT:
        return dict(text=text[:MAX_TEXT], status='partial', reason='text_limit', **metadata)
    return dict(text=text.strip(), status=status, **metadata)


def tag(node):
    return node.tag.rsplit('}', 1)[-1]


def xml(data):
    # Do not expand entities or accept DTDs from untrusted attachments.
    if b'<!DOCTYPE' in data.upper() or b'<!ENTITY' in data.upper():
        raise ValueError('xml_entities_not_allowed')
    return ET.fromstring(data)


def paragraphs(root):
    return '\n'.join(' '.join(n.text or '' for n in p.iter() if tag(n) == 't')
                     for p in root.iter() if tag(p) == 'p')


def natural(value):
    return [int(x) if x.isdigit() else x for x in re.split('(\\d+)', value)]


def office(path, suffix):
    parts = []
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        if len(entries) > 10000 or sum(e.file_size for e in entries) > MAX_EXPANDED:
            return result(status='failed_permanent', reason='archive_limit')
        names = archive.namelist()
        shared = []
        if suffix == '.xlsx' and 'xl/sharedStrings.xml' in names:
            shared = [''.join(n.text or '' for n in item.iter() if tag(n) == 't')
                      for item in xml(archive.read('xl/sharedStrings.xml'))]
        for name in sorted(names, key=natural):
            selected = ((suffix == '.docx' and re.fullmatch(r'word/(document|header\d+|footer\d+|footnotes|endnotes)\.xml', name))
                        or (suffix == '.pptx' and re.fullmatch(r'ppt/(slides/slide\d+|notesSlides/notesSlide\d+)\.xml', name))
                        or (suffix == '.xlsx' and re.fullmatch(r'xl/worksheets/sheet\d+\.xml', name)))
            if not selected:
                continue
            root = xml(archive.read(name))
            if suffix == '.xlsx':
                cells = []
                for cell in root.iter():
                    if tag(cell) != 'c':
                        continue
                    values = [n.text or '' for n in cell.iter() if tag(n) in ('v', 't')]
                    if cell.get('t') == 's':
                        values = [shared[int(v)] for v in values]
                    formulas = [n.text or '' for n in cell.iter() if tag(n) == 'f']
                    cells.append(cell.get('r', '') + ': ' + ' '.join(values) +
                                 (' [formula: ' + ' '.join(formulas) + ']' if formulas else ''))
                text = '\n'.join(cells)
            else:
                text = paragraphs(root)
            parts.append('[' + name + ']\n' + text)
        # Pictures embedded in Office files also need OCR; do not silently ignore them.
        for name in sorted(names, key=natural):
            if re.match(r'(word|ppt|xl)/media/', name) and Path(name).suffix.lower() in IMAGE_SUFFIXES:
                from PIL import Image
                with Image.open(io.BytesIO(archive.read(name))) as image:
                    parts.append('[' + name + ' OCR]\n' + ocr(image))
    if not parts:
        return result(status='unsupported', reason='empty_office_container')
    return result('\n\n'.join(parts), method='office_xml_and_ocr')


def ocr(image):
    global _OCR
    from PIL import ImageOps
    import numpy as np
    from rapidocr_onnxruntime import RapidOCR
    if _OCR is None:
        _OCR = RapidOCR(intra_op_num_threads=1, inter_op_num_threads=1)
    prepared = ImageOps.exif_transpose(image).convert('RGB')
    prepared.thumbnail((4096, 4096))
    rows, _ = _OCR(np.asarray(prepared)[:, :, ::-1])
    return '\n'.join(row[1] for row in (rows or []))


def pdf(path):
    import pypdfium2 as pdfium
    parts = []
    with pdfium.PdfDocument(path) as document:
        total = len(document)
        for number in range(min(total, MAX_PAGES)):
            page = document[number]
            try:
                textpage = page.get_textpage()
                try:
                    text = textpage.get_text_range()
                finally:
                    textpage.close()
                # OCR every page: mixed text/scanned pages otherwise lose embedded text.
                scale = min(2.5, 4096 / max(page.get_size()))
                bitmap = page.render(scale=scale)
                try:
                    image = bitmap.to_pil()
                    try:
                        scanned = ocr(image)
                    finally:
                        image.close()
                finally:
                    bitmap.close()
                if scanned and re.sub(r'\s+', '', scanned) != re.sub(r'\s+', '', text):
                    text += '\n[OCR]\n' + scanned
                parts.append(f'[Página {number + 1}]\n{text}')
            finally:
                page.close()
        return result('\n\n'.join(parts), status='partial' if total > MAX_PAGES else 'done',
                      pages_total=total, pages_processed=min(total, MAX_PAGES), method='pdf_text_and_ocr')


IMAGE_SUFFIXES = {'.png', '.jpg', '.jpeg', '.webp', '.gif', '.bmp', '.tiff', '.tif'}
TEXT_SUFFIXES = {'.txt', '.md', '.csv', '.tsv', '.json', '.xml', '.log', '.yaml', '.yml'}


def extract(path, filename='', media_type='', vision=None):
    path = Path(path)
    if path.stat().st_size > MAX_FILE:
        return result(status='failed_permanent', reason='file_size_limit')
    suffix = Path(filename or path.name).suffix.lower()
    if suffix in TEXT_SUFFIXES:
        raw = path.read_bytes()
        text = raw.decode('utf-16') if raw.startswith((b'\xff\xfe', b'\xfe\xff')) else raw.decode('utf-8-sig')
        return result(text, method='text')
    if suffix in ('.docx', '.xlsx', '.pptx'):
        return office(path, suffix)
    with path.open('rb') as header:
        is_pdf = header.read(5) == b'%PDF-'
    if suffix == '.pdf' or is_pdf:
        return pdf(path)
    if suffix in IMAGE_SUFFIXES or media_type in ('image', 'sticker'):
        from PIL import Image
        with Image.open(path) as image:
            text = ocr(image)
            parts = ['[Texto de imagen]\n' + text] if text else []
            if vision:
                parts.append('[Descripción visual generada por IA]\n' + vision(image))
            frames = getattr(image, 'n_frames', 1)
        return result('\n'.join(parts), status='done' if vision and frames == 1 else 'partial',
                      method='image_ocr_and_vision' if vision else 'image_ocr',
                      reason='animation_first_frame' if frames > 1 else ('' if vision else 'vision_not_configured'))
    return result(status='unsupported', reason='unsupported_format')
