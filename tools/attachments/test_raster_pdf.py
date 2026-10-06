"""Synthetic raster/PDF fixtures; OCR and cloud model calls are stubbed."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from PIL import Image
from tools.attachments.extract import extract
from tools.attachments.cloud import enrich

class RasterPdfTests(unittest.TestCase):
    def test_pdf_renders_every_page_and_keeps_ocr(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'synthetic.pdf'
            with Image.new('RGB',(300,200),'white') as page:
                page.save(path,save_all=True,append_images=[page])
            with patch('tools.attachments.extract.ocr',return_value='synthetic contract total 123') as ocr:
                result=extract(path,'synthetic.pdf','document')
            self.assertEqual(ocr.call_count,2)
            self.assertEqual(result['pages_processed'],2)
            self.assertEqual(result['status'],'done')
            self.assertIn('[Página 2]',result['text'])
            self.assertIn('total 123',result['text'])
    def test_image_local_text_and_mock_cloud_visual_description(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'synthetic.png'
            with Image.new('RGB',(40,30),'blue') as image:image.save(path)
            with patch('tools.attachments.extract.ocr',return_value='synthetic invoice'):
                local=extract(path,'synthetic.png','image')
            class Client:
                def analyze(self,content):return {'text':'synthetic description','complete':True}
            result=enrich(path,'synthetic.png','image',local,Client())
            self.assertEqual(result['status'],'done')
            self.assertIn('synthetic invoice',result['text'])
            self.assertIn('synthetic description',result['text'])

if __name__=='__main__':unittest.main()
