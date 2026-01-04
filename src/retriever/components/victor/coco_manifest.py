from __future__ import annotations

from pathlib import Path

import orjson
from tqdm import tqdm

from retriever.components.victor.settings import VictorSettings


def build_coco_manifest(
    instances_json: Path,
    images_dir: Path,
    out_jsonl: Path,
    max_items: int,
) -> None:
    data = orjson.loads(instances_json.read_bytes())
    images = data["images"]

    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    n = min(max_items, len(images))

    with out_jsonl.open("wb") as f:
        for img in tqdm(images[:n], desc="building manifest"):
            file_name = img["file_name"]
            record = {
                "image_id": int(img["id"]),
                "image_path": str((images_dir / file_name).resolve()),
                "width": int(img["width"]),
                "height": int(img["height"]),
                "coco_file_name": file_name,
                "tile_id": None,
                "lat": None,
                "lon": None,
                "utm_zone": None,
            }
            f.write(orjson.dumps(record))
            f.write(b"\n")


def run() -> None:
    s = VictorSettings()
    build_coco_manifest(
        instances_json=s.coco_instances_json,
        images_dir=s.coco_images_dir,
        out_jsonl=s.manifest_path,
        max_items=s.max_items,
    )
    print(f"Wrote {s.manifest_path}")


if __name__ == "__main__":
    run()
