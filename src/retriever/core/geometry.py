from __future__ import annotations

import hashlib
from typing import Iterable

from shapely import wkt
from shapely.geometry import Polygon
from shapely.geometry.base import BaseGeometry


def polygon_from_wkt(wkt_str: str) -> BaseGeometry:
    geom = wkt.loads(wkt_str)
    if geom.is_empty:
        raise ValueError("WKT geometry is empty")
    if geom.geom_type not in {"Polygon", "MultiPolygon"}:
        raise ValueError(f"Expected Polygon or MultiPolygon WKT, got {geom.geom_type}")
    return geom


def bbox_to_wkt(minx: float, miny: float, maxx: float, maxy: float) -> str:
    coords = [(minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny)]
    return Polygon(coords).wkt


def normalize_polygon_wkt(wkt_str: str) -> str:
    geom = polygon_from_wkt(wkt_str).buffer(0)
    if hasattr(geom, "normalize"):
        geom = geom.normalize()
    return wkt.dumps(geom, rounding_precision=6, trim=True)


def dedup_key(pixel_polygon_wkt: str, *parts: object) -> str:
    normalized = normalize_polygon_wkt(pixel_polygon_wkt)
    extras = "|".join("" if part is None else str(part) for part in parts)
    payload = f"{normalized}|{extras}" if extras else normalized
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def filter_polygons_by_query(
    rows: Iterable[dict],
    query_wkt: str,
    mode: str = "intersects",
    wkt_key: str = "pixel_polygon",
) -> list[dict]:
    query_geom = polygon_from_wkt(query_wkt)
    filtered: list[dict] = []
    for row in rows:
        wkt_str = row.get(wkt_key)
        if not wkt_str:
            continue
        geom = polygon_from_wkt(str(wkt_str))
        if mode == "within":
            if geom.within(query_geom):
                filtered.append(row)
        else:
            if geom.intersects(query_geom):
                filtered.append(row)
    return filtered
