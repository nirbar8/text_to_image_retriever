from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Optional

import numpy as np
from PIL import Image

from retriever.core.geometry import polygon_from_wkt
from retriever.core.schemas import IndexRequest
from retriever.core.interfaces import TileStore


@dataclass(frozen=True)
class LocalFileTileStore(TileStore):
    def get_tile_image(self, request: IndexRequest) -> Image.Image:
        if not request.image_path:
            raise ValueError("image_path is required for LocalFileTileStore")
        image_path = request.image_path
        if image_path.startswith(("http://", "https://")):
            import httpx

            resp = httpx.get(image_path, timeout=30.0)
            resp.raise_for_status()
            return Image.open(BytesIO(resp.content)).convert("RGB")
        return Image.open(image_path).convert("RGB")


@dataclass(frozen=True)
class OrthophotoTileStore(TileStore):
    default_raster_path: Optional[str] = None

    def get_tile_image(self, request: IndexRequest) -> Image.Image:
        if not request.pixel_polygon:
            raise ValueError("pixel_polygon is required for OrthophotoTileStore")
        raster_path = request.raster_path or self.default_raster_path
        if not raster_path:
            raise ValueError("raster_path is required for OrthophotoTileStore")

        import rasterio
        from rasterio.enums import Resampling

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

            geom = polygon_from_wkt(str(request.pixel_polygon))
            minx, miny, maxx, maxy = geom.bounds
            window = rasterio.windows.Window(
                col_off=float(minx),
                row_off=float(miny),
                width=float(maxx - minx),
                height=float(maxy - miny),
            )
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
        if img.ndim == 3:
            if img.shape[2] == 1:
                img = img[:, :, 0]
            elif img.shape[2] == 2:
                pad = img[:, :, :1]
                img = np.concatenate([img, pad], axis=2)
            elif img.shape[2] > 3:
                img = img[:, :, :3]
        return Image.fromarray(img).convert("RGB")


@dataclass(frozen=True)
class SyntheticSatelliteTileStore(TileStore):
    channels: int = 3

    def get_tile_image(self, request: IndexRequest) -> Image.Image:
        if not request.pixel_polygon:
            raise ValueError("pixel_polygon is required for SyntheticSatelliteTileStore")
        width = int(request.out_width or request.width)
        height = int(request.out_height or request.height)
        gid = int(request.gid or request.image_id)

        geom = polygon_from_wkt(str(request.pixel_polygon))
        minx, miny, maxx, maxy = geom.bounds
        key = (gid, minx, miny, maxx, maxy)
        seed = (hash(key) & 0xFFFFFFFF)
        rng = np.random.default_rng(seed)

        yy, xx = np.mgrid[0:height, 0:width]
        base = (xx / max(width - 1, 1) + yy / max(height - 1, 1)) / 2.0
        base = base[..., None]
        noise = rng.random((height, width, self.channels), dtype=np.float32) * 0.2
        img = np.clip(base + noise, 0.0, 1.0)
        img = (img * 255.0 + 0.5).astype(np.uint8)
        if img.ndim == 3:
            if img.shape[2] == 1:
                img = img[:, :, 0]
            elif img.shape[2] == 2:
                pad = img[:, :, :1]
                img = np.concatenate([img, pad], axis=2)
            elif img.shape[2] > 3:
                img = img[:, :, :3]
        return Image.fromarray(img).convert("RGB")
