from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import torch
from PIL import Image

try:
    import open_clip
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "CLIP embedder requires open_clip_torch. Install it with "
        "`uv pip install open_clip_torch` or add it to your model dependency group."
    ) from exc


@dataclass
class ClipEmbedder:
    model_name: str = "ViT-B-32"
    pretrained: str = "openai"
    device: Optional[torch.device] = None
    name: str = "clip"
    embed_dim: int = 0

    def __post_init__(self) -> None:
        if self.device is None:
            if torch.cuda.is_available():
                self.device = torch.device("cuda")
            elif torch.backends.mps.is_available():
                self.device = torch.device("mps")
            else:
                self.device = torch.device("cpu")

        model, _, preprocess = open_clip.create_model_and_transforms(
            self.model_name, pretrained=self.pretrained
        )
        model = model.to(self.device)
        model.eval()

        self.model = model
        self.preprocess = preprocess
        self.tokenizer = open_clip.get_tokenizer(self.model_name)
        self.embed_dim = int(
            getattr(self.model, "embed_dim", 0)
            or getattr(getattr(self.model, "text_projection", None), "shape", [0, 0])[1]
            or 0
        )

    @torch.inference_mode()
    def embed_pil_images(self, images: List[Image.Image]) -> torch.Tensor:
        batch = torch.stack([self.preprocess(im) for im in images], dim=0).to(self.device)
        with torch.autocast(device_type=self.device.type, enabled=(self.device.type != "cpu")):
            feats = self.model.encode_image(batch)
        feats = feats / feats.norm(dim=-1, keepdim=True)
        return feats.float().cpu()

    @torch.inference_mode()
    def embed_texts(self, texts: List[str]) -> torch.Tensor:
        tokens = self.tokenizer(texts).to(self.device)
        with torch.autocast(device_type=self.device.type, enabled=(self.device.type != "cpu")):
            feats = self.model.encode_text(tokens)
        feats = feats / feats.norm(dim=-1, keepdim=True)
        return feats.float().cpu()
