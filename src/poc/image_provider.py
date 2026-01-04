# image_provider.py
from __future__ import annotations

from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Iterable, List, Optional, Sequence, Tuple, Union
import numpy as np


@dataclass(frozen=True)
class BBox:
    """
    Bounding box in EPSG:4326 by default (lon/lat).
    """
    minx: float
    miny: float
    maxx: float
    maxy: float
    crs: str = "EPSG:4326"

    def as_tuple(self) -> Tuple[float, float, float, float]:
        return (self.minx, self.miny, self.maxx, self.maxy)


@dataclass(frozen=True)
class ImageRequest:
    bbox: BBox
    out_size: Optional[Tuple[int, int]] = None  # (width, height) in pixels
    bands: Optional[Sequence[int]] = None       # 1-indexed band IDs, e.g. (1,2,3)


class ImageProvider(ABC):
    """
    Interface you can implement for different backends:
    - local GeoTIFF/COG
    - HTTP COG
    - WMS/WMTS/XYZ
    - custom cache, etc.
    """

    @abstractmethod
    def get(self, req: ImageRequest) -> np.ndarray:
        """
        Returns image as numpy array with shape (H, W, C), dtype typically uint8/uint16/float32.
        """
        raise NotImplementedError

    def get_batch(self, reqs: Sequence[ImageRequest]) -> List[np.ndarray]:
        """
        Default batch implementation; override for optimized batching.
        """
        return [self.get(r) for r in reqs]


class RasterioCOGProvider(ImageProvider):
    """
    Reads chips from a GeoTIFF/COG (local path or HTTP URL) using rasterio.
    Supports EPSG:4326 requests by transforming bounds into dataset CRS.
    """

    def __init__(self, raster_path_or_url: str):
        self._path = raster_path_or_url

    def get(self, req: ImageRequest) -> np.ndarray:
        # Local import so the interface stays lightweight for other implementations
        import rasterio
        from rasterio.windows import from_bounds
        from rasterio.warp import transform_bounds
        from rasterio.enums import Resampling

        with rasterio.open(self._path) as src:
            if src.crs is None:
                raise ValueError("Raster has no CRS; cannot interpret bbox.")

            bands = tuple(req.bands) if req.bands is not None else tuple(range(1, min(3, src.count) + 1))
            if len(bands) == 0:
                raise ValueError("No bands selected.")

            # Transform bbox to dataset CRS if needed (assume req bbox CRS known)
            if req.bbox.crs != str(src.crs):
                left, bottom, right, top = transform_bounds(
                    req.bbox.crs, src.crs, *req.bbox.as_tuple(), densify_pts=21
                )
            else:
                left, bottom, right, top = req.bbox.as_tuple()

            window = from_bounds(left, bottom, right, top, transform=src.transform)
            # Clamp to dataset bounds
            window = window.round_offsets().round_lengths()
            full = rasterio.windows.Window(0, 0, src.width, src.height)
            window = window.intersection(full)
            if window.width <= 0 or window.height <= 0:
                raise ValueError("Requested bbox is outside raster extent.")

            if req.out_size is None:
                data = src.read(list(bands), window=window)
            else:
                out_w, out_h = req.out_size
                if out_w <= 0 or out_h <= 0:
                    raise ValueError("out_size must be positive (width, height).")
                data = src.read(
                    list(bands),
                    window=window,
                    out_shape=(len(bands), out_h, out_w),
                    resampling=Resampling.bilinear,
                )

            # Convert from (C,H,W) -> (H,W,C)
            img = np.transpose(data, (1, 2, 0))
            return img


class SyntheticGridProvider(ImageProvider):
    """
    Simple provider for debugging:
    Produces deterministic imagery from bbox and out_size without any external data.
    Useful for unit tests and pipeline checks.
    """

    def __init__(self, channels: int = 3, dtype: np.dtype = np.uint8):
        self.channels = int(channels)
        self.dtype = dtype

    def get(self, req: ImageRequest) -> np.ndarray:
        w_h = req.out_size or (256, 256)
        w, h = w_h
        if w <= 0 or h <= 0:
            raise ValueError("out_size must be positive.")

        # Deterministic seed based on bbox values
        key = (req.bbox.minx, req.bbox.miny, req.bbox.maxx, req.bbox.maxy, req.bbox.crs)
        seed = (hash(key) & 0xFFFFFFFF)
        rng = np.random.default_rng(seed)

        # Generate a smooth-ish field + noise so it's not constant
        yy, xx = np.mgrid[0:h, 0:w]
        base = (xx / max(w - 1, 1) + yy / max(h - 1, 1)) / 2.0  # [0,1]
        base = base[..., None]  # (H,W,1)
        noise = rng.random((h, w, self.channels), dtype=np.float32) * 0.15
        img = np.clip(base + noise, 0.0, 1.0)

        if self.dtype == np.uint8:
            return (img * 255.0 + 0.5).astype(np.uint8)
        if self.dtype == np.float32:
            return img.astype(np.float32)
        return img.astype(self.dtype)
