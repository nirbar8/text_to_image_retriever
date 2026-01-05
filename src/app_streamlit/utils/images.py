from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from PIL import Image

from retriever.adapters.tile_store import OrthophotoTileStore
from retriever.core.geometry import dedup_key, filter_polygons_by_query
from retriever.core.schemas import IndexRequest


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


def _hit_pixel_polygon(hit: Dict[str, Any]) -> Optional[str]:
    pixel_polygon = hit.get("pixel_polygon")
    if pixel_polygon:
        return str(pixel_polygon)
    return None


def dedup_hits_by_polygon(
    hits: list[Dict[str, Any]],
    extra_fields: Tuple[str, ...] = ("source", "tile_store", "width", "height"),
) -> list[Dict[str, Any]]:
    seen: set[str] = set()
    kept: list[Dict[str, Any]] = []
    for hit in hits:
        pixel_polygon = _hit_pixel_polygon(hit)
        if not pixel_polygon:
            key = tuple(hit.get(k) for k in extra_fields)
            token = f"fallback:{key}"
        else:
            token = dedup_key(pixel_polygon, *(hit.get(k) for k in extra_fields))
        if token in seen:
            continue
        seen.add(token)
        kept.append(hit)
    return kept


def filter_hits_by_polygon(
    hits: list[Dict[str, Any]],
    query_wkt: str,
    mode: str = "intersects",
) -> list[Dict[str, Any]]:
    for hit in hits:
        pixel_polygon = _hit_pixel_polygon(hit)
        if pixel_polygon:
            hit["pixel_polygon"] = pixel_polygon
    return filter_polygons_by_query(hits, query_wkt, mode=mode, wkt_key="pixel_polygon")


def load_hit_image(
    hit: Dict[str, Any],
    max_size: Optional[Tuple[int, int]] = (512, 512),
) -> Image.Image:
    image_path = hit.get("image_path")
    if image_path:
        return load_image(image_path, max_size=max_size)

    pixel_polygon = _hit_pixel_polygon(hit)
    if pixel_polygon is None:
        raise ValueError("Missing pixel_polygon fields for raster-backed tile")

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
        pixel_polygon=pixel_polygon,
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
