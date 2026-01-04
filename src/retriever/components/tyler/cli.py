from __future__ import annotations

import argparse
import json

from retriever.components.tyler.factory import TylerFactory
from retriever.components.tyler.settings import TylerMode, TylerSettings


def _build_bbox(tile: object) -> dict[str, float | str] | None:
    if all(hasattr(tile, attr) for attr in ("minx", "miny", "maxx", "maxy", "crs")):
        return {
            "minx": tile.minx,
            "miny": tile.miny,
            "maxx": tile.maxx,
            "maxy": tile.maxy,
            "crs": tile.crs,
        }
    return None


def run() -> None:
    parser = argparse.ArgumentParser(description="Generate tiles from orthophoto or satellite bounds.")
    parser.add_argument("--mode", choices=["orthophoto", "satellite", "coco"], default=None)
    args = parser.parse_args()

    s = TylerSettings()
    if args.mode:
        s.mode = TylerMode(args.mode)
    tyler = TylerFactory(s).build()

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
                "bbox": _build_bbox(t),
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
