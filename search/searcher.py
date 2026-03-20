"""File-based searcher: reads ``[letter].data`` files and returns paginated results.

Each query word is looked up by its initial letter.  Results are aggregated
by URL, scored by total frequency, and returned with pagination metadata.
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils import tokenize

if TYPE_CHECKING:
    from storage.file_store import WordStore


class Searcher:
    """Query engine backed by the per-letter word index files."""

    def __init__(self, word_store: "WordStore") -> None:
        self._word_store = word_store

    def search(
        self,
        query: str,
        page: int = 1,
        per_page: int = 10,
    ) -> dict:
        """Search for *query* and return a paginated result dict.

        The returned dict contains:
        - ``results``: list of ``{url, origin_url, depth, total_frequency}``
        - ``total``: total number of matching URLs
        - ``page``, ``per_page``, ``total_pages``
        """
        terms = tokenize(query)
        if not terms:
            return {
                "results": [], "total": 0,
                "page": page, "per_page": per_page, "total_pages": 0,
            }

        url_data: dict[str, dict] = {}
        for term in terms:
            for entry in self._word_store.search(term):
                url = entry["url"]
                if url not in url_data:
                    url_data[url] = {
                        "url": url,
                        "origin_url": entry["origin_url"],
                        "depth": entry["depth"],
                        "total_frequency": 0,
                    }
                url_data[url]["total_frequency"] += entry.get("frequency", 0)

        ordered = sorted(
            url_data.values(),
            key=lambda x: (-x["total_frequency"], x["url"]),
        )
        total = len(ordered)
        start = (page - 1) * per_page
        page_results = ordered[start: start + per_page]
        total_pages = (total + per_page - 1) // per_page if total else 0
        return {
            "results": page_results,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
        }


__all__ = ["Searcher"]
