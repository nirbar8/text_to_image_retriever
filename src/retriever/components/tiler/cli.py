from __future__ import annotations

import argparse
import json

from retriever.components.tiler.orthophoto import OrthophotoTiler, OrthophotoTilerConfig
from retriever.components.tiler.satellite import SatelliteBoundsTiler, SatelliteTilerConfig
from retriever.components.tiler.settings import TilerSettings


def run() -> None:
    parser = argparse.ArgumentParser(description="Generate tiles from orthophoto or satellite bounds.")
    parser.add_argument("--mode", choices=["orthophoto", "satellite"], default=None)
    args = parser.parse_args()

    s = TilerSettings()
    mode = args.mode or s.mode

    if mode == "orthophoto":
        tiler = OrthophotoTiler(
            OrthophotoTilerConfig(
                raster_path=s.raster_path,
                tile_size_px=s.tile_size_px,
                stride_px=s.stride_px,
            )
        )
    else:
        tiler = SatelliteBoundsTiler(
            SatelliteTilerConfig(
                bounds=(s.bounds_minx, s.bounds_miny, s.bounds_maxx, s.bounds_maxy),
                tile_size_deg=s.tile_size_deg,
            )
        )

    tiles = tiler.generate_tiles()
    s.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with s.output_jsonl.open("w", encoding="utf-8") as f:
        for t in tiles:
            f.write(json.dumps(t.__dict__, ensure_ascii=False))
            f.write("\n")

    print(f"Wrote {len(tiles)} tiles to {s.output_jsonl}")


if __name__ == "__main__":
    run()
