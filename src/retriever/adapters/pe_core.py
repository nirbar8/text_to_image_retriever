from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import torch
from PIL import Image

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PM_DIR = _REPO_ROOT / "third_party" / "perception_models"
if _PM_DIR.exists() and str(_PM_DIR) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(_PM_DIR))

try:
    import core.vision_encoder.pe as pe
    import core.vision_encoder.transforms as transforms
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "PE-Core not installed. Clone perception_models into third_party/ and install it, "
        "or add it to PYTHONPATH. See README for setup steps."
    ) from exc


@dataclass
class PECoreEmbedder:
    config_name: str = "PE-Core-B16-224"
    device: torch.device | None = None

    def __post_init__(self) -> None:
        if self.device is None:
            if torch.cuda.is_available():
                self.device = torch.device("cuda")
            elif torch.backends.mps.is_available():
                self.device = torch.device("mps")
            else:
                self.device = torch.device("cpu")

        self.model = pe.CLIP.from_config(self.config_name, pretrained=True).to(self.device)
        self.model.eval()

        self.preprocess = transforms.get_image_transform(self.model.image_size)
        self.tokenizer = transforms.get_text_tokenizer(self.model.context_length)
        self.embed_dim = int(getattr(self.model, "clip_dim", 1024))

    @torch.inference_mode()
    def embed_images(self, image_paths: List[str]) -> torch.Tensor:
        imgs = []
        for p in image_paths:
            im = Image.open(p).convert("RGB")
            imgs.append(self.preprocess(im))
        batch = torch.stack(imgs, dim=0).to(self.device)

        with torch.autocast(device_type=self.device.type, enabled=(self.device.type != "cpu")):
            image_features, _, _ = self.model(batch, None)

        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        return image_features.float().cpu()

    @torch.inference_mode()
    def embed_texts(self, texts: List[str]) -> torch.Tensor:
        tokens = self.tokenizer(texts).to(self.device)

        with torch.autocast(device_type=self.device.type, enabled=(self.device.type != "cpu")):
            _, text_features, _ = self.model(None, tokens)

        text_features = text_features / text_features.norm(dim=-1, keepdim=True)
        return text_features.float().cpu()

    @torch.inference_mode()
    def embed_pil_images(self, images: List[Image.Image]) -> torch.Tensor:
        batch = torch.stack([self.preprocess(im) for im in images], dim=0).to(self.device)

        with torch.autocast(device_type=self.device.type, enabled=(self.device.type != "cpu")):
            image_features, _, _ = self.model(batch, None)

        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        return image_features.float().cpu()
