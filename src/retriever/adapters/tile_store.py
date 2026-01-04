from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
from PIL import Image

from retriever.core.schemas import IndexRequest
from retriever.core.interfaces import TileStore


@dataclass(frozen=True)
class LocalFileTileStore(TileStore):
    def get_tile_image(self, request: IndexRequest) -> Image.Image:
        if not request.image_path:
            raise ValueError("image_path is required for LocalFileTileStore")
        return Image.open(request.image_path).convert("RGB")


@dataclass(frozen=True)
class OrthophotoTileStore(TileStore):
    default_raster_path: Optional[str] = None

    def get_tile_image(self, request: IndexRequest) -> Image.Image:
        if request.bbox is None:
            raise ValueError("bbox is required for OrthophotoTileStore")
        raster_path = request.raster_path or self.default_raster_path
        if not raster_path:
            raise ValueError("raster_path is required for OrthophotoTileStore")

        import rasterio
        from rasterio.windows import from_bounds
        from rasterio.warp import transform_bounds
        from rasterio.enums import Resampling

        bbox = request.bbox
        bands = tuple(request.bands) if request.bands else None
        out_w = request.out_width or request.width
        out_h = request.out_height or request.height

        with rasterio.open(raster_path) as src:
            if src.crs is None:
                raise ValueError("Raster has no CRS")

            if bands is None:
                bands = tuple(range(1, min(3, src.count) + 1))
            if len(bands) == 0:
                raise ValueError("No bands selected")

            if bbox.crs != str(src.crs):
                left, bottom, right, top = transform_bounds(
                    bbox.crs, src.crs, bbox.minx, bbox.miny, bbox.maxx, bbox.maxy, densify_pts=21
                )
            else:
                left, bottom, right, top = bbox.minx, bbox.miny, bbox.maxx, bbox.maxy

            window = from_bounds(left, bottom, right, top, transform=src.transform)
            window = window.round_offsets().round_lengths()
            full = rasterio.windows.Window(0, 0, src.width, src.height)
            window = window.intersection(full)
            if window.width <= 0 or window.height <= 0:
                raise ValueError("Requested bbox is outside raster extent")

            if out_w and out_h:
                data = src.read(
                    list(bands),
                    window=window,
                    out_shape=(len(bands), int(out_h), int(out_w)),
                    resampling=Resampling.bilinear,
                )
            else:
                data = src.read(list(bands), window=window)

        img = np.transpose(data, (1, 2, 0))
        if img.dtype != np.uint8:
            img = np.clip(img, 0, 255).astype(np.uint8)
        return Image.fromarray(img, mode="RGB")


@dataclass(frozen=True)
class SyntheticSatelliteTileStore(TileStore):
    channels: int = 3

    def get_tile_image(self, request: IndexRequest) -> Image.Image:
        if request.bbox is None:
            raise ValueError("bbox is required for SyntheticSatelliteTileStore")
        width = int(request.out_width or request.width)
        height = int(request.out_height or request.height)
        gid = int(request.gid or request.image_id)

        key = (gid, request.bbox.minx, request.bbox.miny, request.bbox.maxx, request.bbox.maxy)
        seed = (hash(key) & 0xFFFFFFFF)
        rng = np.random.default_rng(seed)

        yy, xx = np.mgrid[0:height, 0:width]
        base = (xx / max(width - 1, 1) + yy / max(height - 1, 1)) / 2.0
        base = base[..., None]
        noise = rng.random((height, width, self.channels), dtype=np.float32) * 0.2
        img = np.clip(base + noise, 0.0, 1.0)
        img = (img * 255.0 + 0.5).astype(np.uint8)
        return Image.fromarray(img, mode="RGB")
