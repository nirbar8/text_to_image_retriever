from __future__ import annotations

import argparse

import uvicorn

from retriever.services.retriever.settings import RetrieverSettings


def main() -> None:
    parser = argparse.ArgumentParser(description="Retriever FastAPI service")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()

    s = RetrieverSettings()
    host = args.host or s.host
    port = args.port or s.port

    uvicorn.run("retriever.services.retriever.app:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
