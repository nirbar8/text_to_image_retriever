from __future__ import annotations

from typing import List, Protocol

import torch
from PIL import Image


class Embedder(Protocol):
    name: str
    device: torch.device
    embed_dim: int

    def embed_pil_images(self, images: List[Image.Image]) -> torch.Tensor:
        ...

    def embed_texts(self, texts: List[str]) -> torch.Tensor:
        ...
