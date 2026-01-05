from __future__ import annotations

import argparse
import json

from retriever.components.tyler.factory import TylerFactory
from retriever.components.tyler.settings import TylerMode, TylerSettings


def run() -> None:
    parser = argparse.ArgumentParser(description="Generate tiles from orthophoto or satellite bounds.")
    parser.add_argument("--mode", choices=["orthophoto", "satellite", "coco", "dota"], default=None)
    args = parser.parse_args()

    s = TylerSettings()
    if args.mode:
        s.mode = TylerMode(args.mode)
    tyler = TylerFactory(s).build()

    tiles = tyler.generate_tiles()
    tile_store = {
        TylerMode.ORTHOPHOTO: "orthophoto",
        TylerMode.SATELLITE: "synthetic",
        TylerMode.COCO: "local",
        TylerMode.DOTA: "local",
    }.get(s.mode, "orthophoto")
    source = {
        TylerMode.ORTHOPHOTO: "orthophoto",
        TylerMode.SATELLITE: "satellite",
        TylerMode.COCO: "coco",
        TylerMode.DOTA: "dota",
    }.get(s.mode, "orthophoto")
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
                "pixel_polygon": getattr(t, "pixel_polygon", None),
                "out_width": t.width,
                "out_height": t.height,
                "lat": getattr(t, "lat", None),
                "lon": getattr(t, "lon", None),
                "utm_zone": getattr(t, "utm_zone", None),
                "tile_store": tile_store,
                "source": source,
            }
            f.write(json.dumps(record, ensure_ascii=False))
            f.write("\n")

    print(f"Wrote {len(tiles)} tiles to {s.output_jsonl}")


if __name__ == "__main__":
    run()
