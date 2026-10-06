import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

class EnrichmentTests(unittest.TestCase):
    def test_preserves_all_text_and_resumes_units(self):
        from tools.attachments.cloud import enrich
        class Client:
            def analyze(self, content):
                return {'text':'analysis '+content[0]['text'][-3:], 'complete':True,'cached':False}
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'long.txt'; path.write_text('a'*25000)
            original={'text':path.read_text(),'status':'done','method':'text'}
            first=enrich(path,'long.txt','document',original,Client(),max_units=1)
            self.assertTrue(first['cloud_pending'])
            self.assertEqual(first['cloud_next'],1)
            self.assertTrue(first['text'].startswith(original['text']))
            final=enrich(path,'long.txt','document',original,Client(),previous=first,max_units=8)
            self.assertFalse(final['cloud_pending'])
            self.assertEqual(final['status'],'done')
            self.assertEqual(final['cloud_next'],3)

    def test_budget_retains_local_text_without_failure(self):
        from tools.attachments.cloud import enrich
        from tools.attachments.cloud_client import BudgetExceeded
        class Client:
            def analyze(self,content): raise BudgetExceeded('budget')
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'test.txt'; path.write_text('full evidence')
            got=enrich(path,'test.txt','document',{'text':'full evidence','status':'done'},Client())
            self.assertEqual(got['status'],'partial')
            self.assertTrue(got['cloud_pending'])
            self.assertEqual(got['text'],'full evidence')
            self.assertEqual(got['reason'],'cloud_budget')

    def test_image_has_high_detail_and_bounded_dimensions(self):
        from tools.attachments.cloud import image_content
        from PIL import Image
        import base64,io
        with Image.new('RGB',(4000,1000),'red') as im:
            content=image_content(im)
        self.assertEqual(content['detail'],'high')
        with Image.open(io.BytesIO(base64.b64decode(content['image_url'].split(',')[1]))) as got:
            self.assertEqual(got.size,(2048,512))

    def test_render_failure_preserves_first_extraction(self):
        from tools.attachments.cloud import enrich
        with patch('tools.attachments.cloud.units',side_effect=ValueError('malformed')):
            got=enrich(Path('/synthetic'),'bad.pdf','document',{'text':'preserved original','status':'done'},object())
        self.assertEqual(got['text'],'preserved original')
        self.assertEqual(got['reason'],'cloud_preparation_error')
        self.assertEqual(got['status'],'partial')

    def test_retry_exhaustion_is_terminal_partial_and_preserves_evidence(self):
        from tools.attachments.cloud import enrich, VERSION
        from tools.attachments.cloud_client import RetryExhausted
        class Client:
            def analyze(self,content):raise RetryExhausted('attempts exhausted')
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'synthetic.txt';path.write_text('local evidence '*1000)
            local={'text':path.read_text(),'status':'done'}
            prior={'cloud_version':VERSION,'cloud_next':1,'cloud_text':'previous cloud evidence'}
            got=enrich(path,'synthetic.txt','document',local,Client(),previous=prior)
            self.assertEqual(got['status'],'partial')
            self.assertFalse(got['cloud_pending'])
            self.assertEqual(got['reason'],'cloud_retry_exhausted')
            self.assertTrue(got['text'].startswith(local['text']))
            self.assertIn('previous cloud evidence',got['text'])
            self.assertEqual(got['cloud_next'],1)
