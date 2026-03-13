"""
CLI entry point for running the Durable Streams server standalone.

Usage:
    python -m rakaia [--host HOST] [--port PORT]
"""

from __future__ import annotations

import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="Durable Streams Protocol Server")
    parser.add_argument(
        "--host", default="127.0.0.1", help="Host to bind to (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", type=int, default=4437, help="Port to listen on (default: 4437)"
    )
    args = parser.parse_args()

    try:
        import uvicorn
    except ImportError:
        print(
            "uvicorn is required to run the server standalone.\n"
            "Install it with: pip install uvicorn",
            file=sys.stderr,
        )
        sys.exit(1)

    uvicorn.run(
        "rakaia:app",
        host=args.host,
        port=args.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
