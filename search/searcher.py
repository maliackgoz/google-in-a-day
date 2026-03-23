"""File-based searcher: reads ``[letter].data`` files and returns paginated results.

Each query term is resolved with optional prefix fallback (longest matching indexed
word).  Results are aggregated per URL and ranked by a composite relevance score
(frequency, exact vs prefix match, depth), with optional sort modes.
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Literal

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils import tokenize

if TYPE_CHECKING:
    from storage.file_store import WordStore

SortBy = Literal["relevance", "frequency", "depth"]


def _term_relevance_score(query_term: str, matched_key: str, entry: dict) -> int:
    """Score one hit: frequency weight, exact-match bonus, prefix ratio, depth penalty."""
    freq = int(entry.get("frequency", 0))
    depth = int(entry.get("depth", 0))
    score = freq * 10
    qt = query_term.lower()
    mk = matched_key.lower()
    if qt == mk:
        score += 1000
    elif len(qt) > 0:
        score += int(500 * len(mk) / len(qt))
    score -= depth * 5
    return max(score, 0)


class Searcher:
    """Query engine backed by the per-letter word index files."""

    def __init__(self, word_store: "WordStore") -> None:
        self._word_store = word_store

    def search(
        self,
        query: str,
        page: int = 1,
        per_page: int = 10,
        sort_by: SortBy = "relevance",
    ) -> dict:
        """Search for *query* and return a paginated result dict.

        Terms shorter than two characters are skipped (noisy single-letter tokens).

        The returned dict contains:
        - ``results``: list of ``{url, origin_url, depth, total_frequency, relevance_score}``
        - ``total``, ``page``, ``per_page``, ``total_pages``
        - ``sort_by``: echo of the sort mode used
        """
        if sort_by not in ("relevance", "frequency", "depth"):
            sort_by = "relevance"

        terms = [t for t in tokenize(query) if len(t) >= 2]
        if not terms:
            return {
                "results": [],
                "total": 0,
                "page": page,
                "per_page": per_page,
                "total_pages": 0,
                "sort_by": sort_by,
            }

        url_data: dict[str, dict] = {}
        for term in terms:
            entries, matched_key = self._word_store.search_with_prefix_fallback(term)
            if not matched_key:
                continue
            for entry in entries:
                url = entry["url"]
                sc = _term_relevance_score(term, matched_key, entry)
                if url not in url_data:
                    url_data[url] = {
                        "url": url,
                        "origin_url": entry["origin_url"],
                        "depth": entry["depth"],
                        "total_frequency": 0,
                        "relevance_score": 0,
                    }
                url_data[url]["total_frequency"] += int(entry.get("frequency", 0))
                url_data[url]["relevance_score"] += sc

        if sort_by == "relevance":
            ordered = sorted(
                url_data.values(),
                key=lambda x: (-x["relevance_score"], x["url"]),
            )
        elif sort_by == "frequency":
            ordered = sorted(
                url_data.values(),
                key=lambda x: (-x["total_frequency"], x["url"]),
            )
        else:
            ordered = sorted(
                url_data.values(),
                key=lambda x: (x["depth"], x["url"]),
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
            "sort_by": sort_by,
        }


__all__ = ["Searcher"]
