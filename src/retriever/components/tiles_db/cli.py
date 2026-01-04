from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List

from retriever.adapters.tiles_repo_sqlite import SqliteTilesConfig, SqliteTilesRepository
from retriever.core.schemas import TILE_DB_COLUMNS


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect and maintain the TilesDB (SQLite).")
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
        "waiting for embedding",
        "waiting for index",
        "indexed",
        "failed",
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


def _print_tiles(rows: Iterable[dict]) -> None:
    header = list(TILE_DB_COLUMNS)
    print("\t".join(header))
    for row in rows:
        print(
            "\t".join(
                str(row.get(field, ""))
                for field in TILE_DB_COLUMNS
            )
        )


def main() -> None:
    args = _parse_args()
    repo = SqliteTilesRepository(SqliteTilesConfig(args.db_path))

    if args.command == "summary":
        counts = repo.status_counts()
        total = sum(counts.values())
        print(f"TilesDB: {args.db_path}")
        print(f"Total tiles: {total}")
        for line in _format_counts(counts):
            print(f"  {line}")
        return

    if args.command == "list":
        rows = repo.list_tiles(limit=args.limit, status=args.status)
        _print_tiles(rows)
        return

    if args.command == "show":
        tile = repo.get_tile(args.tile_id)
        if tile is None:
            print(f"Tile not found: {args.tile_id}")
            return
        _print_tile(tile)
        return

    if args.command == "set-status":
        repo.update_status(args.tile_ids, status=args.status)
        print(f"Updated {len(args.tile_ids)} tiles to status='{args.status}'.")
        return

    if args.command == "delete":
        repo.delete_tiles(args.tile_ids)
        print(f"Deleted {len(args.tile_ids)} tiles.")
        return


if __name__ == "__main__":
    main()
