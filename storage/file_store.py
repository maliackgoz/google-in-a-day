"""Thread-safe flat-file storage for the Google-in-a-Day project.

Three storage components:
  VisitedUrlsStore  – data/visited_urls.data   (one URL per line)
  WordStore         – data/storage/[letter].data (JSON word->entries index)
  CrawlerDataStore  – data/[crawlerId].data    (JSON crawler state)
"""

from __future__ import annotations

import json
import logging
import os
import threading
from collections import Counter
from typing import Any, Optional

logger = logging.getLogger(__name__)


class VisitedUrlsStore:
    """Thread-safe set of visited URLs backed by a flat file.

    On init the file is read into memory.  All membership checks are O(1)
    against the in-memory set.  The file is flushed to disk explicitly via
    ``save()`` (called at crawler shutdown and on each job completion).
    """

    def __init__(self, data_dir: str) -> None:
        self._path = os.path.join(data_dir, "visited_urls.data")
        self._lock = threading.Lock()
        self._urls: set[str] = set()
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self._path):
            open(self._path, "a", encoding="utf-8").close()
            return
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                for line in f:
                    stripped = line.strip()
                    if stripped:
                        self._urls.add(stripped)
            logger.info("Loaded %d visited URLs", len(self._urls))
        except OSError as exc:
            logger.warning("Could not read visited_urls.data: %s", exc)

    def add_if_new(self, url: str) -> bool:
        """Return True if *url* was new and added, False if already seen."""
        with self._lock:
            if url in self._urls:
                return False
            self._urls.add(url)
            return True

    def __contains__(self, url: str) -> bool:
        with self._lock:
            return url in self._urls

    def filter_unvisited(self, urls: list[str]) -> list[str]:
        """URLs not yet visited, preserving order, deduped — **one** lock hold.

        Per-link ``url in store`` in a tight loop would acquire the lock tens of
        thousands of times on pages like Wikipedia (massive slowdown / contention).
        """
        with self._lock:
            out: list[str] = []
            batch_seen: set[str] = set()
            for u in urls:
                if u in self._urls or u in batch_seen:
                    continue
                batch_seen.add(u)
                out.append(u)
            return out

    def __len__(self) -> int:
        with self._lock:
            return len(self._urls)

    def get_all(self) -> set[str]:
        with self._lock:
            return set(self._urls)

    def save(self) -> None:
        """Flush current state to disk."""
        with self._lock:
            os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
            with open(self._path, "w", encoding="utf-8") as f:
                for url in sorted(self._urls):
                    f.write(url + "\n")
            logger.info("Saved %d visited URLs", len(self._urls))

    def clear(self) -> None:
        """Remove all visited URLs from memory and disk."""
        with self._lock:
            self._urls.clear()
            try:
                os.remove(self._path)
            except FileNotFoundError:
                pass


class WordStore:
    """Thread-safe word index using one JSON file per initial letter.

    Each file maps *word* -> list of ``{url, origin_url, depth, frequency}``
    dicts.  Per-letter locks minimize contention across crawler workers.
    """

    def __init__(self, data_dir: str) -> None:
        self._storage_dir = os.path.join(data_dir, "storage")
        os.makedirs(self._storage_dir, exist_ok=True)
        self._locks: dict[str, threading.Lock] = {}
        self._meta_lock = threading.Lock()

    def _get_lock(self, letter: str) -> threading.Lock:
        with self._meta_lock:
            if letter not in self._locks:
                self._locks[letter] = threading.Lock()
            return self._locks[letter]

    @staticmethod
    def _letter_key(word: str) -> str:
        if not word:
            return "_"
        ch = word[0].lower()
        return ch if ch.isalpha() else "_"

    def _file_path(self, letter: str) -> str:
        return os.path.join(self._storage_dir, f"{letter}.data")

    def _read_file(self, letter: str) -> dict[str, list[dict]]:
        path = self._file_path(letter)
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, ValueError, OSError):
            return {}

    def _write_file(self, letter: str, data: dict[str, list[dict]]) -> None:
        with open(self._file_path(letter), "w", encoding="utf-8") as f:
            try:
                json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            except UnicodeEncodeError:
                f.seek(0)
                f.truncate()
                json.dump(data, f, ensure_ascii=True, separators=(",", ":"))

    def add_words(
        self,
        word_counts: Counter,
        url: str,
        origin_url: str,
        depth: int,
    ) -> None:
        """Store word frequency data for a crawled page.  Thread-safe."""
        by_letter: dict[str, list[tuple[str, int]]] = {}
        for word, freq in word_counts.items():
            letter = self._letter_key(word)
            by_letter.setdefault(letter, []).append((word, freq))

        for letter, pairs in by_letter.items():
            lock = self._get_lock(letter)
            with lock:
                file_data = self._read_file(letter)
                for word, freq in pairs:
                    entries = file_data.setdefault(word, [])
                    updated = False
                    for entry in entries:
                        if entry["url"] == url:
                            entry.update(
                                frequency=freq,
                                origin_url=origin_url,
                                depth=depth,
                            )
                            updated = True
                            break
                    if not updated:
                        entries.append({
                            "url": url,
                            "origin_url": origin_url,
                            "depth": depth,
                            "frequency": freq,
                        })
                self._write_file(letter, file_data)

    def search(self, word: str) -> list[dict]:
        """Return entries for *word*, sorted by frequency descending."""
        word = word.lower()
        letter = self._letter_key(word)
        lock = self._get_lock(letter)
        with lock:
            data = self._read_file(letter)
        return sorted(
            data.get(word, []),
            key=lambda e: e.get("frequency", 0),
            reverse=True,
        )

    def search_with_prefix_fallback(self, word: str) -> tuple[list[dict], str]:
        """Resolve *word* to index entries: exact key first, else longest prefix (length ≥ 3).

        Returns ``(entries, matched_key)``.  If nothing matches, ``([], "")``.
        Mirrors a lightweight “longest indexed prefix” strategy without a stemmer.
        """
        word = word.lower()
        if not word:
            return [], ""
        rows = self.search(word)
        if rows:
            return rows, word
        if len(word) < 3:
            return [], ""
        for i in range(len(word) - 1, 2, -1):
            prefix = word[:i]
            rows = self.search(prefix)
            if rows:
                return rows, prefix
        return [], ""

    def total_words(self) -> int:
        """Count distinct words across all letter files."""
        total = 0
        if not os.path.isdir(self._storage_dir):
            return 0
        for name in os.listdir(self._storage_dir):
            if not name.endswith(".data"):
                continue
            letter = name[:-5]
            lock = self._get_lock(letter)
            with lock:
                data = self._read_file(letter)
            total += len(data)
        return total

    def clear(self) -> None:
        """Remove all word index files."""
        if not os.path.isdir(self._storage_dir):
            return
        for name in os.listdir(self._storage_dir):
            if name.endswith(".data"):
                try:
                    os.remove(os.path.join(self._storage_dir, name))
                except FileNotFoundError:
                    pass


class CrawlerDataStore:
    """Per-crawler ``[crawlerId].data`` JSON files."""

    def __init__(self, data_dir: str) -> None:
        self._data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    @property
    def data_dir(self) -> str:
        return self._data_dir

    def _file_path(self, crawler_id: str) -> str:
        return os.path.join(self._data_dir, f"{crawler_id}.data")

    def save(self, crawler_id: str, data: dict) -> None:
        with open(self._file_path(crawler_id), "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def read(self, crawler_id: str) -> Optional[dict]:
        path = self._file_path(crawler_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, ValueError, OSError):
            return None

    def list_all(self) -> list[dict]:
        """List all crawler state files, ordered by creation time descending."""
        results: list[dict] = []
        if not os.path.isdir(self._data_dir):
            return results
        for name in os.listdir(self._data_dir):
            if not name.endswith(".data"):
                continue
            if name == "visited_urls.data":
                continue
            full_path = os.path.join(self._data_dir, name)
            if not os.path.isfile(full_path):
                continue
            data = self.read(name[:-5])
            if data and "id" in data:
                results.append(data)
        return sorted(
            results, key=lambda d: d.get("created_at", 0), reverse=True
        )

    def clear_all(self) -> int:
        """Remove all crawler state files.  Returns count of files removed."""
        removed = 0
        if not os.path.isdir(self._data_dir):
            return removed
        for name in os.listdir(self._data_dir):
            if not name.endswith(".data"):
                continue
            if name == "visited_urls.data":
                continue
            try:
                os.remove(os.path.join(self._data_dir, name))
                removed += 1
            except FileNotFoundError:
                pass
        return removed


# --- Durable crawler queue & append-only logs (NDJSON + plain text) ---------

_LOG_APPEND_LOCK = threading.Lock()


def crawler_queue_path(data_dir: str, crawler_id: str) -> str:
    return os.path.join(data_dir, f"{crawler_id}.queue")


def crawler_log_path(data_dir: str, crawler_id: str) -> str:
    return os.path.join(data_dir, f"{crawler_id}.logs")


def save_crawler_queue_snapshot(
    data_dir: str, crawler_id: str, tasks: list[dict[str, Any]],
) -> None:
    """Write pending crawl tasks as one JSON object per line."""
    path = crawler_queue_path(data_dir, crawler_id)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in tasks:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_crawler_queue_snapshot(data_dir: str, crawler_id: str) -> list[dict[str, Any]]:
    path = crawler_queue_path(data_dir, crawler_id)
    if not os.path.exists(path):
        return []
    out: list[dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError:
        return []
    return out


def delete_crawler_queue_file(data_dir: str, crawler_id: str) -> None:
    try:
        os.remove(crawler_queue_path(data_dir, crawler_id))
    except FileNotFoundError:
        pass


def count_crawler_queue_lines(data_dir: str, crawler_id: str) -> int:
    path = crawler_queue_path(data_dir, crawler_id)
    if not os.path.exists(path):
        return 0
    n = 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    n += 1
    except OSError:
        return 0
    return n


def peek_crawler_queue_snapshot(
    data_dir: str, crawler_id: str, limit: int = 40,
) -> list[dict[str, Any]]:
    """Return up to *limit* parsed task dicts from the queue file."""
    rows = load_crawler_queue_snapshot(data_dir, crawler_id)
    return rows[:limit]


def append_crawler_log_line(
    data_dir: str, crawler_id: str, timestamp: int, message: str,
) -> None:
    """Append one log line (tab-separated timestamp and message)."""
    path = crawler_log_path(data_dir, crawler_id)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    safe = message.replace("\n", " ").replace("\r", "")
    line = f"{timestamp}\t{safe}\n"
    with _LOG_APPEND_LOCK:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)


def delete_crawler_log_file(data_dir: str, crawler_id: str) -> None:
    try:
        os.remove(crawler_log_path(data_dir, crawler_id))
    except FileNotFoundError:
        pass


def clear_crawler_auxiliary_files(data_dir: str) -> int:
    """Remove all ``*.queue`` and ``*.logs`` under *data_dir*. Returns count removed."""
    removed = 0
    if not os.path.isdir(data_dir):
        return removed
    for name in os.listdir(data_dir):
        if name.endswith(".queue") or name.endswith(".logs"):
            try:
                os.remove(os.path.join(data_dir, name))
                removed += 1
            except FileNotFoundError:
                pass
    return removed
