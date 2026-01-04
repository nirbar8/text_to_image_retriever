# demo_save_png_scales.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

import numpy as np
from PIL import Image
import rasterio

# Import from the module we wrote earlier
from poc.image_provider import BBox, ImageRequest, ImageProvider, RasterioCOGProvider, SyntheticGridProvider


@dataclass(frozen=True)
class Scale:
    """
    Scale defined as expanding the same center bbox by a multiplier.
    multiplier=1.0 keeps the bbox as-is
    multiplier=2.0 doubles width and height around the same center
    """
    multiplier: float
    name: str


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def save_png(img_hwc: np.ndarray, path: str) -> None:
    """
    img_hwc: (H, W, C) uint8 recommended
    """
    if img_hwc.dtype != np.uint8:
        # Simple normalization for non-uint8 arrays
        x = img_hwc.astype(np.float32)
        lo = np.percentile(x, 2.0)
        hi = np.percentile(x, 98.0)
        if hi <= lo + 1e-6:
            hi = lo + 1.0
        x = np.clip((x - lo) / (hi - lo), 0.0, 1.0)
        img_hwc = (x * 255.0 + 0.5).astype(np.uint8)

    if img_hwc.ndim != 3 or img_hwc.shape[2] not in (1, 3, 4):
        raise ValueError(f"Expected HWC with C=1/3/4, got {img_hwc.shape}")

    if img_hwc.shape[2] == 1:
        img_hwc = img_hwc[:, :, 0]

    Image.fromarray(img_hwc).save(path)


def expand_bbox(b: BBox, multiplier: float) -> BBox:
    if multiplier <= 0:
        raise ValueError("multiplier must be positive")

    cx = 0.5 * (b.minx + b.maxx)
    cy = 0.5 * (b.miny + b.maxy)
    w = (b.maxx - b.minx) * multiplier
    h = (b.maxy - b.miny) * multiplier

    return BBox(
        minx=cx - 0.5 * w,
        miny=cy - 0.5 * h,
        maxx=cx + 0.5 * w,
        maxy=cy + 0.5 * h,
        crs=b.crs,
    )


def run_demo(
    provider: ImageProvider,
    base_bbox: BBox,
    out_dir: str,
    out_size: Tuple[int, int] = (512, 512),
    bands: Optional[Sequence[int]] = None,
    scales: Optional[List[Scale]] = None,
) -> None:
    ensure_dir(out_dir)

    if scales is None:
        scales = [
            Scale(1.0, "x1"),
            Scale(1.5, "x1_5"),
            Scale(2.0, "x2"),
            Scale(3.0, "x3"),
            Scale(4.0, "x4"),
        ]

    for s in scales:
        bbox_s = expand_bbox(base_bbox, s.multiplier)
        req = ImageRequest(bbox=bbox_s, out_size=out_size, bands=bands)
        img = provider.get(req)  # (H,W,C)
        out_path = os.path.join(out_dir, f"chip_{s.name}.png")
        save_png(img, out_path)
        print(f"Saved {out_path}  shape={img.shape} dtype={img.dtype} bbox={bbox_s.as_tuple()} {bbox_s.crs}")


def run():
    # Option A: Synthetic (always works, no data needed)
    provider: ImageProvider = SyntheticGridProvider(channels=3, dtype=np.uint8)

    # Option B: Rasterio GeoTIFF/COG (uncomment and set your path/URL)
    # provider = RasterioCOGProvider("/path/to/your_ortho_or_cog.tif")
    # If the raster is not RGB in bands 1,2,3 you can pass bands=(4,3,2) for example.
    RASTER = "rasters/earthsearch_s2_chip.tif"  # or an https://... .tif COG URL
    RASTER = "rasters/planetarycomputer_s2_chip.tif"  # or an https://... .tif COG URL
    provider = RasterioCOGProvider(RASTER)


    with rasterio.open(RASTER) as src:
        print("Raster:", RASTER)
        print("CRS:", src.crs)
        print("Bounds:", src.bounds)
        print("Count:", src.count)
        print("Dtype:", src.dtypes)
        print("Res:", src.res)


    # Pick "the same place" as a base bbox (EPSG:4326 lon/lat)
    # Replace these numbers with your actual AOI bbox.
    base_bbox = BBox(
        minx=34.760,  # lon
        miny=31.990,  # lat
        maxx=34.780,  # lon
        maxy=32.010,  # lat
        crs="EPSG:4326",
    )

    run_demo(
        provider=provider,
        base_bbox=base_bbox,
        out_dir="out_scales",
        out_size=(512, 512),
        bands=None,  # or e.g. (4,3,2)
        scales=[
            Scale(1.0, "x1"),
            Scale(2.0, "x2"),
            Scale(4.0, "x4"),
            Scale(8.0, "x8"),
        ],
    )
