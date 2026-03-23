#!/usr/bin/env python3
"""Google in a Day — start the web server.

Usage:
  python3 run.py      # http://localhost:8080
"""

from __future__ import annotations

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

PORT = 8080
DATA_DIR = os.path.join(PROJECT_ROOT, "data")


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)

    manager = CrawlerManager(DATA_DIR)
    searcher = Searcher(manager.word_store)

    print("=" * 52)
    print("  Google in a Day — Web Crawler & Search Engine")
    print("=" * 52)
    print(f"  Data directory: {DATA_DIR}")
    print()

    try:
        start_server(manager, searcher, port=PORT)
    finally:
        print("\nShutting down...")
        manager.shutdown()
        print("Done.")


if __name__ == "__main__":
    main()
