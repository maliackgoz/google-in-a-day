#!/usr/bin/env python3
"""Google in a Day — start the web server.

Usage:
  python3 run.py                        # default: http://localhost:8080
  python3 run.py --port 9090            # custom port
  python3 run.py --data-dir /tmp/crawl  # custom data directory
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from crawler.indexer import CrawlerManager
from search.searcher import Searcher
from web.server import start_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Google in a Day — Web Crawler & Search Engine",
    )
    parser.add_argument(
        "--port", "-p", type=int, default=8080,
        help="HTTP server port (default: 8080)",
    )
    parser.add_argument(
        "--data-dir", "-d", type=str, default="data",
        help="Data directory for storage files (default: data)",
    )
    args = parser.parse_args()

    data_dir = os.path.join(PROJECT_ROOT, args.data_dir)
    os.makedirs(data_dir, exist_ok=True)

    manager = CrawlerManager(data_dir)
    searcher = Searcher(manager.word_store)

    print("=" * 52)
    print("  Google in a Day — Web Crawler & Search Engine")
    print("=" * 52)
    print(f"  Data directory: {data_dir}")
    print()

    try:
        start_server(manager, searcher, port=args.port)
    finally:
        print("\nShutting down...")
        manager.shutdown()
        print("Done.")


if __name__ == "__main__":
    main()
