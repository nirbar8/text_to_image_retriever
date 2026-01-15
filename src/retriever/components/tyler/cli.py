from __future__ import annotations

import argparse
import json
from pathlib import Path

from retriever.components.tyler.factories.tyler_factory import TylerFactory
from retriever.components.tyler.settings.tyler_mode import TylerMode
from retriever.components.tyler.settings.tyler_settings import TylerSettings
from retriever.components.tyler.tylers.coco import CocoTyler
from retriever.components.tyler.tylers.dota import DotaTyler
from retriever.components.tyler.tylers.orthophoto import OrthophotoTyler
from retriever.components.tyler.tylers.satellite import SatelliteBoundsTyler
from retriever.components.tyler.tylers.strip import StripTyler
from retriever.core.interfaces import Tyler
from retriever.core.schemas import TileSpec


def _tile_to_record(tile: TileSpec, tyler: Tyler) -> dict:
    """Convert a TileSpec to a JSON record dictionary."""
    return {
        "image_id": tile.image_id,
        "image_path": tile.image_path or "",
        "width": tile.width,
        "height": tile.height,
        "tile_id": tile.tile_id,
        "gid": tile.gid,
        "raster_path": tile.raster_path,
        "pixel_polygon": tile.pixel_polygon,
        "out_width": tile.width,
        "out_height": tile.height,
        "lat": tile.lat,
        "lon": tile.lon,
        "utm_zone": tile.utm_zone,
        "tile_store": tyler.tile_store,
        "source": tyler.source,
        "tyler_mode": tile.tyler_mode,
    }


def _write_tiles_jsonl(tiles: list[TileSpec], tyler: Tyler, output_path: Path) -> None:
    """Write tiles to a JSONL file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for tile in tiles:
            record = _tile_to_record(tile, tyler)
            f.write(json.dumps(record, ensure_ascii=False))
            f.write("\n")


def run() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Generate tiles from orthophoto or satellite bounds.")
    parser.add_argument("--mode", choices=["orthophoto", "satellite", "coco", "dota", "strip"], default=None)
    args = parser.parse_args()

    settings = TylerSettings()
    if args.mode:
        settings.mode = TylerMode(args.mode)
    
    available_tylers = [
        OrthophotoTyler,
        SatelliteBoundsTyler,
        CocoTyler,
        DotaTyler,
        StripTyler,
    ]
    
    tyler = TylerFactory(settings, available_tylers).build()
    tiles = tyler.generate_tiles()
    
    _write_tiles_jsonl(tiles, tyler, settings.output_jsonl)
    
    print(f"Wrote {len(tiles)} tiles to {settings.output_jsonl}")


if __name__ == "__main__":
    run()
