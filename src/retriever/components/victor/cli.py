from __future__ import annotations

import argparse

from retriever.components.victor.coco_manifest import run as build_manifest
from retriever.components.victor.manager import run as run_manager


def main() -> None:
    parser = argparse.ArgumentParser(description="Vector manager (tile registry + publish index requests).")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("build-manifest")
    sub.add_parser("publish")
    args = parser.parse_args()

    if args.command == "build-manifest":
        build_manifest()
    elif args.command == "publish":
        run_manager()


if __name__ == "__main__":
    main()
