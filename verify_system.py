#!/usr/bin/env python3
"""verify_system.py — End-to-end verification of the Google-in-a-Day system.

Validates:
  1. Required deliverable files exist.
  2. File-based storage (visited_urls.data, [letter].data, [crawlerId].data).
  3. Indexer: depth limit (k=2), visited set, no duplicate visits, scheme filter.
  4. Back-pressure: tiny bounded queue completes without deadlock.
  5. Search with pagination against file-based word index.
  6. Web API endpoints (Crawler page, Status page, Search page).
  7. Unit-level checks for core primitives.

Usage (from the project root):
    python3 verify_system.py
"""

from __future__ import annotations

import http.server
import json
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
from collections import Counter
from urllib import error, parse, request

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from crawler.indexer import CrawlerManager, CrawlTask, UrlQueue
from search.searcher import Searcher
from storage.file_store import CrawlerDataStore, VisitedUrlsStore, WordStore
from utils import extract_title_and_content, normalize_url, tokenize
from web.server import CrawlerHTTPServer, CrawlerHandler

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ---------------------------------------------------------------------------
# Dummy HTTP server — 7 interconnected pages across depths 0-3
# ---------------------------------------------------------------------------
#
#   /               (depth 0)  →  /alpha, /beta, mailto:…, javascript:…
#   /alpha          (depth 1)  →  /alpha/one, /alpha/two
#   /beta           (depth 1)  →  /beta/one, / (back-link cycle)
#   /alpha/one      (depth 2)  →  /alpha/one/deep
#   /alpha/two      (depth 2)  —  (no outlinks)
#   /beta/one       (depth 2)  →  /alpha (cross-link, already visited)
#   /alpha/one/deep (depth 3)  —  MUST NOT be reached when k=2

PAGES: dict[str, str] = {
    "/": (
        "<html><head><title>Quantum Home</title></head><body>"
        "<p>Welcome to the quantum computing portal.</p>"
        '<a href="/alpha">Alpha Section</a> '
        '<a href="/beta">Beta Section</a> '
        '<a href="mailto:admin@example.com">Contact</a> '
        '<a href="javascript:void(0)">NoOp</a>'
        "</body></html>"
    ),
    "/alpha": (
        "<html><head><title>Alpha Page</title></head><body>"
        "<p>Alpha discusses quantum entanglement and algorithms.</p>"
        '<a href="/alpha/one">Alpha One</a> '
        '<a href="/alpha/two">Alpha Two</a>'
        "</body></html>"
    ),
    "/beta": (
        "<html><head><title>Beta Page</title></head><body>"
        "<p>Beta covers photon interference patterns.</p>"
        '<a href="/beta/one">Beta One</a> '
        '<a href="/">Back to Home</a>'
        "</body></html>"
    ),
    "/alpha/one": (
        "<html><head><title>Deep Alpha One</title></head><body>"
        "<p>Photosynthesis research at the quantum scale.</p>"
        '<a href="/alpha/one/deep">Go Deeper</a>'
        "</body></html>"
    ),
    "/alpha/two": (
        "<html><head><title>Alpha Two</title></head><body>"
        "<p>Quantum cryptography and security protocols.</p>"
        "</body></html>"
    ),
    "/beta/one": (
        "<html><head><title>Beta One</title></head><body>"
        "<p>Quantum networking and teleportation theories.</p>"
        '<a href="/alpha">Cross link to Alpha</a>'
        "</body></html>"
    ),
    "/alpha/one/deep": (
        "<html><head><title>Too Deep</title></head><body>"
        "<p>This page contains the word unreachable and must not appear at k=2.</p>"
        "</body></html>"
    ),
}


class _RequestCounter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}

    def increment(self, path: str) -> None:
        with self._lock:
            self._counts[path] = self._counts.get(path, 0) + 1

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()


request_counter = _RequestCounter()


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        path = self.path.split("?")[0]
        request_counter.increment(path)
        if path in ("/alpha", "/beta", "/alpha/one", "/alpha/two", "/beta/one"):
            time.sleep(0.15)
        if path in PAGES:
            body = PAGES[path].encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def log_message(self, fmt: str, *args: object) -> None:
        pass


# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------
_passed = 0
_failed = 0
_failures: list[str] = []


def check(condition: bool, label: str, detail: str = "") -> None:
    global _passed, _failed
    if condition:
        _passed += 1
        print(f"  [PASS] {label}")
    else:
        _failed += 1
        line = f"  [FAIL] {label}"
        if detail:
            line += f" -- {detail}"
        print(line)
        _failures.append(label)


def _wait_for_job(manager: CrawlerManager, crawler_id: str, timeout: float = 20) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = manager.get_job_status(crawler_id)
        if status and status.get("status") in ("finished", "interrupted"):
            return True
        time.sleep(0.1)
    return False


# ---------------------------------------------------------------------------
# Section 1 — Deliverables
# ---------------------------------------------------------------------------
def section_deliverables() -> None:
    print("\n--- Section 1: Deliverables ---")
    for name in ("readme.md", "product_prd.md", "recommendation.md"):
        check(os.path.isfile(os.path.join(PROJECT_ROOT, name)), f"{name} exists")


# ---------------------------------------------------------------------------
# Section 2 — File-based storage
# ---------------------------------------------------------------------------
def section_storage(data_dir: str) -> None:
    print("\n--- Section 2: File-based Storage ---")

    vs = VisitedUrlsStore(data_dir)
    check(vs.add_if_new("http://example.com"), "VisitedUrlsStore: add new → True")
    check(not vs.add_if_new("http://example.com"), "VisitedUrlsStore: duplicate → False")
    check(len(vs) == 1, f"VisitedUrlsStore: len == 1 (got {len(vs)})")
    vs.save()
    vpath = os.path.join(data_dir, "visited_urls.data")
    check(os.path.isfile(vpath), "visited_urls.data file created")
    with open(vpath) as f:
        lines = [l.strip() for l in f if l.strip()]
    check("http://example.com" in lines, "visited_urls.data contains saved URL")

    ws = WordStore(data_dir)
    ws.add_words(Counter({"quantum": 3, "alpha": 1}), "http://ex.com/1", "http://ex.com", 1)
    entries = ws.search("quantum")
    check(len(entries) == 1, f"WordStore search → 1 entry (got {len(entries)})")
    if entries:
        check(entries[0]["frequency"] == 3, f"frequency == 3 (got {entries[0].get('frequency')})")
    spath = os.path.join(data_dir, "storage", "q.data")
    check(os.path.isfile(spath), "storage/q.data created for word 'quantum'")

    cs = CrawlerDataStore(data_dir)
    cs.save("test_123", {"id": "test_123", "status": "running", "created_at": 100})
    read = cs.read("test_123")
    check(read is not None and read["id"] == "test_123", "CrawlerDataStore round-trip")
    all_c = cs.list_all()
    check(any(d["id"] == "test_123" for d in all_c), "CrawlerDataStore list_all works")
    os.remove(os.path.join(data_dir, "test_123.data"))


# ---------------------------------------------------------------------------
# Section 3 — Indexer: depth, visited set, duplicates, scheme filter
# ---------------------------------------------------------------------------
def section_indexer(base: str, data_dir: str) -> None:
    print("\n--- Section 3: Indexer (depth, visited, duplicates) ---")
    request_counter.reset()

    manager = CrawlerManager(data_dir)
    origin = base + "/"
    cid = manager.create_job(
        origin_url=origin,
        max_depth=2,
        max_workers=2,
        max_queue_size=100,
        max_pages=50,
        http_timeout=5.0,
    )
    check(bool(cid), f"Crawler ID created: {cid}")

    parts = cid.split("_")
    check(len(parts) == 2, f"ID has two parts separated by _ (got {len(parts)} parts)")
    if len(parts) == 2:
        check(parts[0].isdigit(), "First part is epoch timestamp")
        check(
            len(parts[1]) >= 8 and all(c in "0123456789abcdef" for c in parts[1]),
            "Second part is random hex suffix",
        )

    finished = _wait_for_job(manager, cid, timeout=20)
    check(finished, "Crawler completes without hanging")

    status = manager.get_job_status(cid)
    check(status is not None, "Job status available")
    if status:
        check(status.get("status") == "finished", f"Status is 'finished' (got {status.get('status')})")
        pages = status.get("pages_processed", 0)
        check(pages >= 6, f"Processed >= 6 pages (got {pages})")

    cdata_path = os.path.join(data_dir, f"{cid}.data")
    check(os.path.isfile(cdata_path), f"{cid}.data file exists")

    vpath = os.path.join(data_dir, "visited_urls.data")
    check(os.path.isfile(vpath), "visited_urls.data updated after crawl")

    with open(vpath) as f:
        visited = {l.strip() for l in f if l.strip()}
    deep = normalize_url(base + "/alpha/one/deep")
    check(deep not in visited, "Depth-3 page NOT in visited_urls.data")

    expected = {normalize_url(base + p) for p in
                ("/", "/alpha", "/beta", "/alpha/one", "/alpha/two", "/beta/one")}
    for eu in sorted(expected):
        path = eu.replace(base, "")
        check(eu in visited, f"Expected page visited: {path}")

    counts = request_counter.snapshot()
    dups = {p: n for p, n in counts.items() if n > 1 and p in PAGES}
    check(len(dups) == 0, "No duplicate HTTP requests", str(dups) if dups else "")

    check(counts.get("mailto:admin@example.com") is None,
          "mailto: link was NOT requested (scheme filter)")
    check(counts.get("javascript:void(0)") is None,
          "javascript: link was NOT requested (scheme filter)")

    storage_dir = os.path.join(data_dir, "storage")
    check(os.path.isdir(storage_dir), "storage/ directory exists")
    q_file = os.path.join(storage_dir, "q.data")
    check(os.path.isfile(q_file), "storage/q.data exists (word 'quantum')")
    if os.path.isfile(q_file):
        with open(q_file) as f:
            q_data = json.load(f)
        check("quantum" in q_data, "'quantum' key in q.data")

    manager.shutdown()


# ---------------------------------------------------------------------------
# Section 4 — Back-pressure / throttling
# ---------------------------------------------------------------------------
def section_backpressure(base: str, data_dir: str) -> None:
    print("\n--- Section 4: Back-pressure ---")
    request_counter.reset()

    bp_dir = os.path.join(data_dir, "_bp_test")
    os.makedirs(os.path.join(bp_dir, "storage"), exist_ok=True)

    manager = CrawlerManager(bp_dir)
    cid = manager.create_job(
        origin_url=base + "/",
        max_depth=2,
        max_workers=2,
        max_queue_size=2,
        max_pages=50,
        http_timeout=5.0,
    )
    finished = _wait_for_job(manager, cid, timeout=30)
    manager.shutdown()
    check(finished, "Crawler with maxsize=2 completes without deadlock")

    q = UrlQueue(maxsize=3)
    for i in range(3):
        q.put(CrawlTask(url=str(i), origin_url="x", depth=0))
    check(q.is_at_capacity(), "is_at_capacity() True when full")
    q.get()
    check(not q.is_at_capacity(), "is_at_capacity() False after dequeue")

    q2 = UrlQueue(maxsize=1)
    q2.put(CrawlTask(url="block", origin_url="x", depth=0))
    stop = threading.Event()
    t0 = time.monotonic()
    ok = q2.put_with_backpressure(
        CrawlTask(url="overflow", origin_url="x", depth=0),
        stop_event=stop,
        poll_interval=0.1,
        max_wait=0.5,
    )
    elapsed = time.monotonic() - t0
    check(not ok, "put_with_backpressure returns False after max_wait")
    check(0.3 <= elapsed <= 2.0,
          f"Blocked ~0.5s before giving up (elapsed {elapsed:.2f}s)")

    shutil.rmtree(bp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 5 — Search with pagination
# ---------------------------------------------------------------------------
def section_search(data_dir: str) -> None:
    print("\n--- Section 5: Search with Pagination ---")

    ws = WordStore(data_dir)
    searcher = Searcher(ws)

    result = searcher.search("quantum", page=1, per_page=5)
    check(result["total"] > 0, f"Search 'quantum' has results (total={result['total']})")
    check(len(result["results"]) > 0, "Results list is non-empty")
    if result["results"]:
        r = result["results"][0]
        check("url" in r, "Result has 'url'")
        check("origin_url" in r, "Result has 'origin_url'")
        check("depth" in r, "Result has 'depth'")

    small = searcher.search("quantum", page=1, per_page=2)
    check(small["per_page"] == 2, "per_page=2 respected")
    check(len(small["results"]) <= 2, f"At most 2 results (got {len(small['results'])})")

    unr = searcher.search("unreachable")
    check(unr["total"] == 0, "'unreachable' NOT found (depth-3 beyond k=2)")

    photo = searcher.search("photosynthesis")
    check(photo["total"] >= 1, "'photosynthesis' found (depth-2 page)")


# ---------------------------------------------------------------------------
# Section 6 — Web API endpoints
# ---------------------------------------------------------------------------
def section_web_api(base: str, data_dir: str) -> None:
    print("\n--- Section 6: Web API ---")

    web_dir = os.path.join(data_dir, "_web_test")
    os.makedirs(os.path.join(web_dir, "storage"), exist_ok=True)

    manager = CrawlerManager(web_dir)
    searcher = Searcher(manager.word_store)

    server = CrawlerHTTPServer(("127.0.0.1", 0), CrawlerHandler, manager, searcher)
    port = server.server_address[1]
    srv = threading.Thread(target=server.serve_forever, daemon=True)
    srv.start()
    wb = f"http://127.0.0.1:{port}"

    try:
        with request.urlopen(f"{wb}/") as resp:
            check(resp.status == 200, "GET / returns 200")
            body = resp.read().decode()
            check("Google in a Day" in body, "Crawler page contains title")
    except Exception as exc:
        check(False, f"GET / failed: {exc}")

    try:
        form = parse.urlencode({
            "origin_url": base + "/",
            "max_depth": "2",
            "max_workers": "2",
            "queue_capacity": "100",
            "max_pages": "50",
        }).encode()
        req = request.Request(f"{wb}/", data=form, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with request.urlopen(req) as resp:
            check("/status/" in (resp.url or ""), "POST / redirects to /status/<id>")
    except error.HTTPError as exc:
        loc = exc.headers.get("Location", "")
        check("/status/" in loc, f"POST / 303 redirect Location: {loc}")
    except Exception as exc:
        check(False, f"POST / failed: {exc}")

    jobs = manager.list_jobs()
    if jobs:
        cid = jobs[0]["id"]
        _wait_for_job(manager, cid, timeout=20)

        try:
            with request.urlopen(f"{wb}/api/status/{cid}") as resp:
                check(resp.status == 200, "GET /api/status/<id> returns 200")
                data = json.loads(resp.read())
                check(data.get("id") == cid, "API status has correct ID")
                check("pages_processed" in data, "API status has pages_processed")
                check("logs" in data, "API status has logs")
        except Exception as exc:
            check(False, f"GET /api/status failed: {exc}")

    try:
        with request.urlopen(f"{wb}/search?q=test") as resp:
            check(resp.status == 200, "GET /search returns 200")
    except Exception as exc:
        check(False, f"GET /search failed: {exc}")

    try:
        with request.urlopen(f"{wb}/api/search?q=quantum&page=1&per_page=5") as resp:
            check(resp.status == 200, "GET /api/search returns 200")
            data = json.loads(resp.read())
            check("results" in data, "API search has 'results'")
            check("total" in data, "API search has 'total'")
            check("page" in data, "API search has 'page'")
    except Exception as exc:
        check(False, f"GET /api/search failed: {exc}")

    server.shutdown()
    manager.shutdown()
    shutil.rmtree(web_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 7 — Unit checks
# ---------------------------------------------------------------------------
def section_unit_checks() -> None:
    print("\n--- Section 7: Unit Checks ---")

    check(normalize_url("HTTP://Example.COM/path/") == "http://example.com/path",
          "normalize_url: lowercase + strip trailing slash")
    check(normalize_url("http://example.com/path#frag") == "http://example.com/path",
          "normalize_url: strip fragment")
    check(normalize_url("http://example.com/") == "http://example.com/",
          "normalize_url: preserve root slash")
    check(normalize_url("http://example.com/a?x=1") == "http://example.com/a?x=1",
          "normalize_url: preserve query string")

    check(tokenize("Hello, World! 123") == ["hello", "world", "123"],
          "tokenize splits on punctuation and lowercases")
    check(tokenize("") == [], "tokenize empty → empty")

    title, body = extract_title_and_content(
        "<html><head><title>Hello &amp; World</title></head>"
        "<body><p>some body</p></body></html>"
    )
    check("Hello" in title and "World" in title, f"Title: {title!r}")
    check("some body" in body, f"Body: {body!r}")

    _, body2 = extract_title_and_content(
        "<html><head><title>T</title></head><body>"
        "<script>var x = 1;</script><style>.a{color:red}</style>"
        "<p>visible</p></body></html>"
    )
    check("var x" not in body2, "Script content stripped")
    check("color" not in body2, "Style content stripped")
    check("visible" in body2, "Visible text preserved")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    print("=" * 60)
    print("  Google-in-a-Day  --  System Verification")
    print("=" * 60)

    server = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    base = f"http://127.0.0.1:{port}"
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()
    print(f"\nDummy HTTP server on {base}  ({len(PAGES)} pages, depths 0-3)\n")

    data_dir = tempfile.mkdtemp(prefix="giad_test_")
    os.makedirs(os.path.join(data_dir, "storage"), exist_ok=True)

    try:
        section_deliverables()
        section_storage(data_dir)
        section_indexer(base, data_dir)
        section_backpressure(base, data_dir)
        section_search(data_dir)
        section_web_api(base, data_dir)
        section_unit_checks()
    finally:
        server.shutdown()
        shutil.rmtree(data_dir, ignore_errors=True)

    total = _passed + _failed
    print("\n" + "=" * 60)
    print(f"  Results:  {_passed}/{total} passed,  {_failed} failed")
    if _failed == 0:
        print("  All checks passed.")
    else:
        print("  Failed checks:")
        for f in _failures:
            print(f"    - {f}")
    print("=" * 60)
    return 0 if _failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
