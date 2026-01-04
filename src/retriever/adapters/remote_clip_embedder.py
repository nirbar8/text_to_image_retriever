from __future__ import annotations

import base64
import io
from dataclasses import dataclass
from typing import List, Optional

import httpx
import torch
from PIL import Image


@dataclass
class RemoteClipEmbedder:
    base_url: str
    timeout_s: float = 60.0
    image_format: str = "png"
    name: str = "remoteclip"
    device: torch.device = torch.device("cpu")
    embed_dim: int = 0

    def __post_init__(self) -> None:
        self._client = httpx.Client(timeout=self.timeout_s)

    def _encode_images(self, images: List[Image.Image]) -> List[str]:
        encoded: List[str] = []
        for im in images:
            buf = io.BytesIO()
            fmt = self.image_format.lower()
            pil_format = "JPEG" if fmt in {"jpg", "jpeg"} else fmt.upper()
            im.save(buf, format=pil_format)
            encoded.append(base64.b64encode(buf.getvalue()).decode("ascii"))
        return encoded

    def _to_tensor(self, embeddings: List[List[float]]) -> torch.Tensor:
        if embeddings and self.embed_dim == 0:
            self.embed_dim = len(embeddings[0])
        return torch.tensor(embeddings, dtype=torch.float32)

    def embed_pil_images(self, images: List[Image.Image]) -> torch.Tensor:
        payload = {"images": self._encode_images(images)}
        resp = self._client.post(f"{self.base_url.rstrip('/')}/embed/images", json=payload)
        resp.raise_for_status()
        data = resp.json()
        embeddings = data.get("embeddings", [])
        return self._to_tensor(embeddings)

    def embed_texts(self, texts: List[str]) -> torch.Tensor:
        payload = {"texts": texts}
        resp = self._client.post(f"{self.base_url.rstrip('/')}/embed/texts", json=payload)
        resp.raise_for_status()
        data = resp.json()
        embeddings = data.get("embeddings", [])
        return self._to_tensor(embeddings)
