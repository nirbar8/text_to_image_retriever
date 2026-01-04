from __future__ import annotations

import argparse
from pathlib import Path

import httpx


DEFAULT_URL = "https://download.osgeo.org/geotiff/samples/usgs/f41078a1.tif"


def download(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with httpx.stream("GET", url, timeout=60.0) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", "0"))
        downloaded = 0
        with out_path.open("wb") as f:
            for chunk in r.iter_bytes(chunk_size=1 << 20):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = int(downloaded / total * 100)
                    print(f"\rDownloading... {pct}%", end="")
    if total:
        print("\nDone.")
    else:
        print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download a sample open-source orthophoto raster.")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--out", default="data/rasters/orthophoto.tif")
    args = parser.parse_args()

    download(args.url, Path(args.out))
    print(f"Saved to {args.out}")


if __name__ == "__main__":
    main()
