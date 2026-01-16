"""CLI entry point for TilesDB service."""

from __future__ import annotations

import uvicorn

from retriever.components.tiles_db.service import create_app
from retriever.components.tiles_db.settings import TilesDBSettings


def main() -> None:
    """Start the TilesDB FastAPI service."""
    settings = TilesDBSettings()

    print(f"Starting TilesDB service on {settings.host}:{settings.port}")
    print(f"Database: {settings.database.db_path}")

    app = create_app(settings)

    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
