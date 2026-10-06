import importlib.util
from pathlib import Path
import tempfile
import unittest
import zipfile

MODULE = Path(__file__).with_name('extract.py')

class ExtractionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def extract(self, name, data):
        self.assertTrue(MODULE.exists(), 'automatic attachment extractor is missing')
        spec = importlib.util.spec_from_file_location('extract', MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        path = self.root / name
        path.write_bytes(data)
        return module.extract(path, filename=name)

    def test_plaintext_keeps_tail_instead_of_truncating(self):
        text = 'first\n' * 2000 + 'sentinel final agreement'
        result = self.extract('notes.txt', text.encode())
        self.assertEqual(result['status'], 'done')
        self.assertIn('sentinel final agreement', result['text'])

    def archive(self, entries):
        path = self.root / 'fixture.zip'
        with zipfile.ZipFile(path, 'w') as archive:
            for name, data in entries.items():
                archive.writestr(name, data)
        return path.read_bytes()

    def test_docx_paragraphs_tables_and_footer(self):
        data = self.archive({
            'word/document.xml': '<w:document xmlns:w="urn:w"><w:p><w:r><w:t>Agreement</w:t></w:r></w:p><w:tbl><w:tr><w:tc><w:p><w:r><w:t>Final table value</w:t></w:r></w:p></w:tc></w:tr></w:tbl></w:document>',
            'word/footer1.xml': '<w:ftr xmlns:w="urn:w"><w:p><w:r><w:t>Footer evidence</w:t></w:r></w:p></w:ftr>'})
        result = self.extract('contract.docx', data)
        self.assertIn('Final table value', result['text'])
        self.assertIn('Footer evidence', result['text'])

    def test_presentation_includes_last_slide_and_notes(self):
        data = self.archive({f'ppt/slides/slide{i}.xml': f'<a:p xmlns:a="urn:a"><a:t>slide {i}</a:t></a:p>' for i in range(1,15)} | {'ppt/notesSlides/notesSlide14.xml':'<a:p xmlns:a="urn:a"><a:t>last speaker notes</a:t></a:p>'})
        result = self.extract('pitch.pptx', data)
        self.assertIn('slide 14', result['text'])
        self.assertIn('last speaker notes', result['text'])
        self.assertEqual(result['status'], 'done')

    def test_spreadsheet_includes_all_sheets_and_resolves_shared_strings(self):
        data = self.archive({'xl/sharedStrings.xml':'<sst><si><t>Final cell evidence</t></si></sst>', 'xl/worksheets/sheet1.xml':'<worksheet><row><c r="A1"><v>12</v></c></row></worksheet>', 'xl/worksheets/sheet2.xml':'<worksheet><row><c r="Z99" t="s"><v>0</v></c></row></worksheet>'})
        result = self.extract('budget.xlsx', data)
        self.assertIn('Final cell evidence', result['text'])
        self.assertIn('Z99', result['text'])

    def test_unsupported_never_marked_complete(self):
        result = self.extract('archive.bin', b'\x00\xff\x00')
        self.assertEqual(result['status'], 'unsupported')
        self.assertEqual(result['text'], '')

if __name__ == '__main__':
    unittest.main()
