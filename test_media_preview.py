import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from media_preview import preview


@unittest.skipUnless(shutil.which('ffmpeg') and shutil.which('ffprobe'),'ffmpeg required')
class PreviewTests(unittest.TestCase):
    def test_synthetic_video_has_bounded_actual_frames(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'synthetic.mp4'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=red:s=96x64:d=2','-c:v','mpeg4',str(path)],check=True)
            result=preview(path,'video')
            self.assertEqual(len(result),6)
            self.assertTrue(all(r['type']=='image' and r['mimeType']=='image/jpeg' for r in result))
            self.assertLess(sum(len(r['data']) for r in result),100000)

    def test_unsupported_file_does_not_execute(self):
        with self.assertRaises(ValueError):preview(Path('/tmp/no-file'),'document')
