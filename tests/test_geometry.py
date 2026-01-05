from retriever.core.geometry import bbox_to_wkt, dedup_key, filter_polygons_by_query, polygon_from_wkt


def test_polygon_from_wkt_parses() -> None:
    geom = polygon_from_wkt("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")
    assert geom.bounds == (0.0, 0.0, 1.0, 1.0)


def test_bbox_to_wkt_round_trip_bounds() -> None:
    wkt_str = bbox_to_wkt(0, 0, 2, 3)
    geom = polygon_from_wkt(wkt_str)
    assert geom.bounds == (0.0, 0.0, 2.0, 3.0)


def test_dedup_key_is_stable() -> None:
    wkt_str = bbox_to_wkt(0, 0, 1, 1)
    assert dedup_key(wkt_str, "source", 512) == dedup_key(wkt_str, "source", 512)


def test_filter_polygons_by_query_intersects() -> None:
    rows = [
        {"pixel_polygon": bbox_to_wkt(0, 0, 1, 1)},
        {"pixel_polygon": bbox_to_wkt(2, 2, 3, 3)},
    ]
    query = bbox_to_wkt(-0.5, -0.5, 1.5, 1.5)
    filtered = filter_polygons_by_query(rows, query_wkt=query, mode="intersects")
    assert len(filtered) == 1
