from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import torch
from PIL import Image

try:
    from transformers import Siglip2Model, Siglip2Processor
except Exception as exc:  # pragma: no cover - optional dependency
    raise ModuleNotFoundError(
        "SigLip2 embedder requires transformers with Siglip2 support. "
        "Install with `uv pip install transformers` and ensure a SigLip2 model is available."
    ) from exc


@dataclass
class SigLip2Embedder:
    model_name: str = "google/siglip2-base-patch16-224"
    device: Optional[torch.device] = None
    name: str = "siglip2"
    embed_dim: int = 0

    def __post_init__(self) -> None:
        if self.device is None:
            if torch.cuda.is_available():
                self.device = torch.device("cuda")
            elif torch.backends.mps.is_available():
                self.device = torch.device("mps")
            else:
                self.device = torch.device("cpu")

        self.processor = Siglip2Processor.from_pretrained(self.model_name)
        self.model = Siglip2Model.from_pretrained(self.model_name).to(self.device)
        self.model.eval()
        self.embed_dim = int(getattr(self.model.config, "projection_dim", 0) or 0)

    @torch.inference_mode()
    def embed_pil_images(self, images: List[Image.Image]) -> torch.Tensor:
        inputs = self.processor(images=images, return_tensors="pt").to(self.device)
        if hasattr(self.model, "get_image_features"):
            feats = self.model.get_image_features(**inputs)
        else:
            outputs = self.model(**inputs)
            feats = getattr(outputs, "image_embeds", outputs[0])
        feats = feats / feats.norm(dim=-1, keepdim=True)
        return feats.float().cpu()

    @torch.inference_mode()
    def embed_texts(self, texts: List[str]) -> torch.Tensor:
        inputs = self.processor(text=texts, return_tensors="pt", padding=True).to(self.device)
        if hasattr(self.model, "get_text_features"):
            feats = self.model.get_text_features(**inputs)
        else:
            outputs = self.model(**inputs)
            feats = getattr(outputs, "text_embeds", outputs[0])
        feats = feats / feats.norm(dim=-1, keepdim=True)
        return feats.float().cpu()
