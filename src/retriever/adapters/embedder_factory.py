from __future__ import annotations

from typing import Optional

from retriever.adapters.embedder_base import Embedder


def build_embedder(
    backend: str,
    model_name: str,
    *,
    clip_pretrained: Optional[str] = None,
    remote_clip_url: Optional[str] = None,
    remote_clip_timeout_s: float = 60.0,
    remote_clip_image_format: str = "png",
) -> Embedder:
    backend_key = backend.strip().lower()

    if backend_key in {"pe", "pe-core", "pe_core"}:
        from retriever.adapters.pe_core import PECoreEmbedder
        return PECoreEmbedder(model_name)

    if backend_key in {"clip"}:
        from retriever.adapters.clip_embedder import ClipEmbedder

        pretrained = clip_pretrained or "openai"
        return ClipEmbedder(model_name=model_name, pretrained=pretrained)

    if backend_key in {"siglip2", "siglip"}:
        from retriever.adapters.siglip2_embedder import SigLip2Embedder

        return SigLip2Embedder(model_name=model_name)

    if backend_key in {"remoteclip", "remote_clip", "remote"}:
        if not remote_clip_url:
            raise ValueError("REMOTECLIP backend requires remote_clip_url")
        from retriever.adapters.remote_clip_embedder import RemoteClipEmbedder

        return RemoteClipEmbedder(
            base_url=remote_clip_url,
            timeout_s=remote_clip_timeout_s,
            image_format=remote_clip_image_format,
        )

    raise ValueError(f"Unknown embedder backend: {backend}")
