from __future__ import annotations

import argparse
import json

from retriever.components.tyler.coco import CocoTyler, CocoTylerConfig
from retriever.components.tyler.orthophoto import OrthophotoTyler, OrthophotoTylerConfig
from retriever.components.tyler.satellite import SatelliteBoundsTyler, SatelliteTylerConfig
from retriever.components.tyler.settings import TylerSettings


def run() -> None:
    parser = argparse.ArgumentParser(description="Generate tiles from orthophoto or satellite bounds.")
    parser.add_argument("--mode", choices=["orthophoto", "satellite", "coco"], default=None)
    args = parser.parse_args()

    s = TylerSettings()
    mode = args.mode or s.mode

    if mode == "orthophoto":
        tyler = OrthophotoTyler(
            OrthophotoTylerConfig(
                raster_path=s.raster_path,
                tile_size_px=s.tile_size_px,
                stride_px=s.stride_px,
            )
        )
    elif mode == "satellite":
        tyler = SatelliteBoundsTyler(
            SatelliteTylerConfig(
                bounds=(s.bounds_minx, s.bounds_miny, s.bounds_maxx, s.bounds_maxy),
                tile_size_deg=s.tile_size_deg,
                tile_size_px=s.tile_size_px,
                image_count=s.sat_image_count,
                image_size_deg=s.sat_image_size_deg,
                rotation_deg_max=s.sat_rotation_deg_max,
                seed=s.sat_seed,
            )
        )
    else:
        tyler = CocoTyler(
            CocoTylerConfig(
                instances_json=s.coco_instances_json,
                images_dir=s.coco_images_dir,
                max_items=s.coco_max_items,
                seed=s.coco_seed,
                lat_range=(s.coco_lat_min, s.coco_lat_max),
                lon_range=(s.coco_lon_min, s.coco_lon_max),
            )
        )

    tiles = tyler.generate_tiles()
    s.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with s.output_jsonl.open("w", encoding="utf-8") as f:
        for t in tiles:
            record = {
                "image_id": t.image_id,
                "image_path": getattr(t, "image_path", ""),
                "width": t.width,
                "height": t.height,
                "tile_id": t.tile_id,
                "gid": getattr(t, "gid", None),
                "raster_path": getattr(t, "raster_path", None),
                "bbox": {
                    "minx": t.minx,
                    "miny": t.miny,
                    "maxx": t.maxx,
                    "maxy": t.maxy,
                    "crs": t.crs,
                },
                "out_width": t.width,
                "out_height": t.height,
                "lat": getattr(t, "lat", None),
                "lon": getattr(t, "lon", None),
                "utm_zone": getattr(t, "utm_zone", None),
            }
            f.write(json.dumps(record, ensure_ascii=False))
            f.write("\n")

    print(f"Wrote {len(tiles)} tiles to {s.output_jsonl}")


if __name__ == "__main__":
    run()
