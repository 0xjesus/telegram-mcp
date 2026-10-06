"""Local image captions. Download model files explicitly before starting the worker."""
import os
from pathlib import Path

_CACHE = None


def describe(image):
    global _CACHE
    path = Path(os.environ['WA_VISION_MODEL']).expanduser()
    if not path.is_dir():
        raise ValueError('vision_model_missing')
    import torch
    from transformers import BlipProcessor, BlipForConditionalGeneration
    torch.set_num_threads(2)
    if _CACHE is None:
        processor = BlipProcessor.from_pretrained(path, local_files_only=True)
        model = BlipForConditionalGeneration.from_pretrained(path, local_files_only=True,
                    weights_only=True, dtype=torch.float32).eval()
        _CACHE = processor, model
    processor, model = _CACHE
    prepared = image.convert('RGB')
    inputs = processor(images=prepared, return_tensors='pt')
    with torch.inference_mode():
        generated = model.generate(**inputs, max_new_tokens=64, do_sample=False, num_beams=3)
    text = processor.decode(generated[0], skip_special_tokens=True).strip()
    if not text:
        raise RuntimeError('empty_visual_description')
    return text
