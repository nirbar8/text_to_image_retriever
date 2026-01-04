from __future__ import annotations

from pathlib import Path

from retriever.core.interfaces import TileStore


class LocalFileTileStore(TileStore):
    def get_tile_bytes(self, image_path: str) -> bytes:
        path = Path(image_path)
        return path.read_bytes()
