from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from PIL import Image

from retriever.adapters.tile_store import OrthophotoTileStore
from retriever.core.schemas import IndexRequest, TileBBox


def load_image(path: str, max_size: Optional[Tuple[int, int]] = (512, 512)) -> Image.Image:
    p = Path(path)
    img = Image.open(p).convert("RGB")
    if max_size is not None:
        img.thumbnail(max_size)
    return img


def _parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _hit_bbox(hit: Dict[str, Any]) -> Optional[TileBBox]:
    minx = hit.get("bbox_minx")
    miny = hit.get("bbox_miny")
    maxx = hit.get("bbox_maxx")
    maxy = hit.get("bbox_maxy")
    if minx is None or miny is None or maxx is None or maxy is None:
        return None
    return TileBBox(
        minx=float(minx),
        miny=float(miny),
        maxx=float(maxx),
        maxy=float(maxy),
        crs=str(hit.get("bbox_crs") or "EPSG:4326"),
    )


def load_hit_image(
    hit: Dict[str, Any],
    max_size: Optional[Tuple[int, int]] = (512, 512),
) -> Image.Image:
    image_path = hit.get("image_path")
    if image_path:
        return load_image(image_path, max_size=max_size)

    bbox = _hit_bbox(hit)
    if bbox is None:
        raise ValueError("Missing bbox fields for raster-backed tile")

    raster_path = hit.get("raster_path")
    if not raster_path:
        raise ValueError("Missing raster_path for raster-backed tile")

    width = _parse_int(hit.get("width"))
    height = _parse_int(hit.get("height"))
    if not width or not height:
        raise ValueError("Missing width/height for raster-backed tile")

    out_width = _parse_int(hit.get("out_width")) or width
    out_height = _parse_int(hit.get("out_height")) or height

    req = IndexRequest(
        image_id=_parse_int(hit.get("image_id")) or 0,
        image_path=None,
        width=width,
        height=height,
        tile_id=hit.get("tile_id"),
        gid=_parse_int(hit.get("gid")),
        raster_path=str(raster_path),
        bbox=bbox,
        out_width=out_width,
        out_height=out_height,
        lat=hit.get("lat"),
        lon=hit.get("lon"),
        utm_zone=hit.get("utm_zone"),
    )

    store = OrthophotoTileStore(default_raster_path=str(raster_path))
    img = store.get_tile_image(req)
    if max_size is not None:
        img.thumbnail(max_size)
    return img
