from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

from PIL import Image


def load_image(path: str, max_size: Optional[Tuple[int, int]] = (512, 512)) -> Image.Image:
    p = Path(path)
    img = Image.open(p).convert("RGB")
    if max_size is not None:
        img.thumbnail(max_size)
    return img
