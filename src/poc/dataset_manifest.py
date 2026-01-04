import orjson
from pathlib import Path
from tqdm import tqdm

from poc.config import Settings

def build_coco_manifest(
    instances_json: Path,
    images_dir: Path,
    out_jsonl: Path,
    max_items: int,
) -> None:
    """
    Writes JSONL lines, each is a single image job:
    {
      "image_id": int,
      "image_path": str,
      "width": int,
      "height": int,
      "coco_file_name": str,
      "tile_id": null,
      "lat": null,
      "lon": null
    }
    """
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

                # geo-ish placeholders for later infra
                "tile_id": None,
                "lat": None,
                "lon": None,
                "utm_zone": None,
            }
            f.write(orjson.dumps(record))
            f.write(b"\n")


def build_entry() -> None:
    s = Settings()
    build_coco_manifest(
        instances_json=s.coco_instances_json,
        images_dir=s.coco_images_dir,
        out_jsonl=s.manifest_path,
        max_items=s.max_items,
    )
    print(f"Wrote {s.manifest_path}")
