# test_image_provider.py
from __future__ import annotations

import numpy as np
import pytest

from poc.image_provider import BBox, ImageRequest, ImageProvider, RasterioCOGProvider, SyntheticGridProvider


def test_synthetic_provider_single_and_batch():
    provider: ImageProvider = SyntheticGridProvider(channels=3, dtype=np.uint8)

    req1 = ImageRequest(bbox=BBox(0, 0, 1, 1), out_size=(128, 64))
    img1 = provider.get(req1)

    assert img1.shape == (64, 128, 3)
    assert img1.dtype == np.uint8

    # Deterministic for same bbox + out_size
    img1b = provider.get(req1)
    assert np.array_equal(img1, img1b)

    # Batch
    req2 = ImageRequest(bbox=BBox(10, 10, 11, 11), out_size=(32, 32))
    batch = provider.get_batch([req1, req2])
    assert len(batch) == 2
    assert batch[0].shape == (64, 128, 3)
    assert batch[1].shape == (32, 32, 3)


def _write_test_geotiff(path: str):
    """
    Create a small RGB GeoTIFF in EPSG:4326 with known content.
    """
    import rasterio
    from rasterio.transform import from_bounds

    width, height = 200, 100
    # bounds in EPSG:4326
    left, bottom, right, top = 0.0, 0.0, 2.0, 1.0
    transform = from_bounds(left, bottom, right, top, width=width, height=height)

    # Make simple gradients for R,G,B
    yy, xx = np.mgrid[0:height, 0:width]
    r = (xx / (width - 1) * 255).astype(np.uint8)
    g = (yy / (height - 1) * 255).astype(np.uint8)
    b = ((r.astype(np.int32) + g.astype(np.int32)) // 2).astype(np.uint8)
    data = np.stack([r, g, b], axis=0)  # (3,H,W)

    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": 3,
        "dtype": "uint8",
        "crs": "EPSG:4326",
        "transform": transform,
        "tiled": True,
        "compress": "deflate",
    }

    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data)


def test_rasterio_provider_reads_expected_shape(tmp_path):
    tif = tmp_path / "test_rgb.tif"
    _write_test_geotiff(str(tif))

    provider = RasterioCOGProvider(str(tif))

    # Request a bbox inside the raster
    req = ImageRequest(
        bbox=BBox(0.5, 0.25, 1.5, 0.75, crs="EPSG:4326"),
        out_size=(64, 64),
        bands=(1, 2, 3),
    )
    img = provider.get(req)
    assert img.shape == (64, 64, 3)
    assert img.dtype == np.uint8

    # Sanity: values should not be all equal (gradient)
    assert img[..., 0].std() > 0
    assert img[..., 1].std() > 0


def test_rasterio_provider_outside_extent_raises(tmp_path):
    tif = tmp_path / "test_rgb.tif"
    _write_test_geotiff(str(tif))
    provider = RasterioCOGProvider(str(tif))

    req = ImageRequest(
        bbox=BBox(10.0, 10.0, 11.0, 11.0, crs="EPSG:4326"),
        out_size=(32, 32),
    )
    with pytest.raises(ValueError):
        _ = provider.get(req)


