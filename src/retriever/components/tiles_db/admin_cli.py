"""CLI for managing TilesDB without running the service (maintenance mode)."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List

from retriever.components.tiles_db.models import TileStatus
from retriever.components.tiles_db.sqlite_adapter import (
    SqliteTilesConfig,
    SqliteTilesRepositoryService,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="TilesDB maintenance CLI - inspect and manage the tiles database.",
        prog="tiles-db-admin",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/tiles.db"),
        help="Path to tiles.db (default: data/tiles.db).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("summary", help="Print total count and counts by status.")

    list_parser = sub.add_parser("list", help="List tiles (optionally filtered by status).")
    list_parser.add_argument("--status", default=None, help="Filter by status value.")
    list_parser.add_argument("--limit", type=int, default=50, help="Limit number of rows.")
    list_parser.add_argument("--offset", type=int, default=0, help="Offset for pagination.")

    show_parser = sub.add_parser("show", help="Show a single tile by tile_id.")
    show_parser.add_argument("tile_id", help="Tile ID to display.")

    set_status_parser = sub.add_parser("set-status", help="Update status for tile IDs.")
    set_status_parser.add_argument("status", help="New status value.")
    set_status_parser.add_argument("tile_ids", nargs="+", help="Tile IDs to update.")

    delete_parser = sub.add_parser("delete", help="Delete tiles by tile ID.")
    delete_parser.add_argument("tile_ids", nargs="+", help="Tile IDs to delete.")

    return parser.parse_args()


def _format_counts(counts: dict[str, int]) -> List[str]:
    preferred = [
        TileStatus.READY_FOR_INDEXING.value,
        TileStatus.IN_PROCESS.value,
        TileStatus.INDEXED.value,
        TileStatus.FAILED.value,
        "",
    ]
    lines: List[str] = []
    for status in preferred:
        if status in counts:
            label = status or "<empty>"
            lines.append(f"{label}: {counts[status]}")
    for status in sorted(k for k in counts.keys() if k not in preferred):
        lines.append(f"{status}: {counts[status]}")
    return lines


def _print_tile(tile: dict) -> None:
    for key, value in tile.items():
        print(f"{key}: {value}")


def _print_tiles(tiles: List) -> None:
    if not tiles:
        print("No tiles found.")
        return

    # Get all keys from first tile
    keys = list(tiles[0].model_dump().keys()) if hasattr(tiles[0], 'model_dump') else list(tiles[0].keys())
    print("\t".join(keys))
    for tile in tiles:
        tile_dict = tile.model_dump() if hasattr(tile, 'model_dump') else tile
        print(
            "\t".join(
                str(tile_dict.get(field, ""))
                for field in keys
            )
        )


def main() -> None:
    """Run TilesDB maintenance commands."""
    args = _parse_args()
    repo = SqliteTilesRepositoryService(SqliteTilesConfig(args.db_path))

    if args.command == "summary":
        counts = repo.status_counts()
        total = sum(counts.values())
        print(f"TilesDB: {args.db_path}")
        print(f"Total tiles: {total}")
        for line in _format_counts(counts):
            print(f"  {line}")
        return

    if args.command == "list":
        status = None
        if args.status:
            try:
                status = TileStatus(args.status)
            except ValueError:
                print(f"Invalid status: {args.status}")
                return

        tiles, total = repo.list_tiles(status=status, limit=args.limit, offset=args.offset)
        print(f"Showing {len(tiles)} of {total} tiles (offset: {args.offset}, limit: {args.limit})")
        print()
        _print_tiles(tiles)
        return

    if args.command == "show":
        tile = repo.get_tile(args.tile_id)
        if tile is None:
            print(f"Tile not found: {args.tile_id}")
            return
        _print_tile(tile.model_dump())
        return

    if args.command == "set-status":
        try:
            status = TileStatus(args.status)
        except ValueError:
            print(f"Invalid status: {args.status}")
            print(f"Valid values: {', '.join(s.value for s in TileStatus)}")
            return

        affected = repo.batch_update_status(args.tile_ids, status=status)
        print(f"Updated {affected} tiles to status='{status.value}'.")
        return

    if args.command == "delete":
        affected = repo.batch_delete(args.tile_ids)
        print(f"Deleted {affected} tiles.")
        return


if __name__ == "__main__":
    main()
