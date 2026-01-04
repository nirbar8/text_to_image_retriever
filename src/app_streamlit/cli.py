from __future__ import annotations

import argparse
import sys

from streamlit.web import cli as stcli


def main() -> None:
    parser = argparse.ArgumentParser(description="Streamlit app runner")
    parser.add_argument("--app", default="src/app_streamlit/app.py")
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()

    sys.argv = ["streamlit", "run", args.app]
    if args.port is not None:
        sys.argv.extend(["--server.port", str(args.port)])
    sys.exit(stcli.main())


if __name__ == "__main__":
    main()
