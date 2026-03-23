"""Core crawler engine with multi-job support.

Each CrawlerJob runs in its own thread, with worker sub-threads for
concurrent fetching.  CrawlerManager coordinates multiple jobs and the
shared storage layer (visited URLs, word index, crawler data files).
"""

from __future__ import annotations

import logging
import os
import ssl
import sys
import threading
import uuid
import time
from collections import Counter
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from queue import Empty, Full, Queue
from typing import Optional
from urllib import error, parse, request

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from storage.file_store import (
    CrawlerDataStore,
    VisitedUrlsStore,
    WordStore,
    append_crawler_log_line,
    clear_crawler_auxiliary_files,
    count_crawler_queue_lines,
    delete_crawler_queue_file,
    load_crawler_queue_snapshot,
    peek_crawler_queue_snapshot,
    save_crawler_queue_snapshot,
)
from utils import extract_title_and_content, normalize_url, tokenize

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CrawlTask:
    url: str
    origin_url: str
    depth: int


class UrlQueue:
    """Thread-safe bounded URL queue with explicit back-pressure.

    When *maxsize* > 0, ``put(block=True)`` blocks when the queue is full,
    applying back-pressure to producers.
    """

    def __init__(self, maxsize: int = 0) -> None:
        self._maxsize = max(0, maxsize)
        self._queue: Queue[CrawlTask] = Queue(maxsize=self._maxsize)

    @property
    def maxsize(self) -> int:
        return self._maxsize

    def put(self, task: CrawlTask, block: bool = True, timeout: Optional[float] = None) -> None:
        self._queue.put(task, block=block, timeout=timeout)

    def put_with_backpressure(
        self,
        task: CrawlTask,
        stop_event: threading.Event,
        poll_interval: float = 1.0,
        max_wait: float = 30.0,
    ) -> bool:
        """Enqueue *task*, blocking when full (back-pressure).

        Returns ``False`` if *stop_event* was set or *max_wait* exceeded.
        ``max_wait`` prevents livelock when workers are all blocked on
        enqueue while the queue is full.
        """
        if self._maxsize == 0:
            self._queue.put(task)
            return True
        waited = 0.0
        while not stop_event.is_set():
            try:
                self._queue.put(task, block=True, timeout=poll_interval)
                return True
            except Full:
                waited += poll_interval
                if 0 < max_wait <= waited:
                    return False
        return False

    def get(self, block: bool = True, timeout: Optional[float] = None) -> CrawlTask:
        return self._queue.get(block=block, timeout=timeout)

    def try_get_nowait(self) -> Optional[CrawlTask]:
        try:
            return self._queue.get_nowait()
        except Empty:
            return None

    def try_put_nowait(self, task: CrawlTask) -> bool:
        """Non-blocking put.  Returns ``False`` when the queue is full."""
        try:
            self._queue.put_nowait(task)
            return True
        except Full:
            return False

    def task_done(self) -> None:
        self._queue.task_done()

    def qsize(self) -> int:
        return self._queue.qsize()

    def is_at_capacity(self) -> bool:
        """True when the queue is bounded and currently full."""
        return self._maxsize > 0 and self._queue.qsize() >= self._maxsize


class LinkExtractor(HTMLParser):
    """Extract absolute ``<a href>`` links from HTML."""

    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.links.append(parse.urljoin(self.base_url, value))
                break


# ---------------------------------------------------------------------------
# CrawlerJob — one crawl operation identified by EpochTime_randomhex
# ---------------------------------------------------------------------------

class CrawlerJob:
    """A single crawl job with a unique ID in the format ``EpochTime_randomhex``.

    On ``start()`` a main thread is spawned which seeds the URL queue and
    launches *max_workers* fetch workers.  Workers share the global
    ``VisitedUrlsStore`` and ``WordStore`` via the ``CrawlerManager``.
    """

    def __init__(
        self,
        origin_url: str,
        max_depth: int,
        visited_store: VisitedUrlsStore,
        word_store: WordStore,
        crawler_store: CrawlerDataStore,
        max_workers: int = 4,
        max_queue_size: int = 1000,
        max_pages: Optional[int] = None,
        hit_rate: float = 0.0,
        http_timeout: float = 10.0,
    ) -> None:
        if max_depth < 0:
            raise ValueError("max_depth must be non-negative")
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")

        self.origin_url = normalize_url(origin_url)
        self.max_depth = max_depth
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size
        self.max_pages = max_pages
        self.hit_rate = hit_rate
        self.http_timeout = http_timeout

        self._visited_store = visited_store
        self._word_store = word_store
        self._crawler_store = crawler_store
        self._data_dir = crawler_store.data_dir
        self._resume_mode = False
        self._resume_tasks: list[CrawlTask] = []

        self.queue = UrlQueue(maxsize=max_queue_size)
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()
        self._workers: list[threading.Thread] = []

        self._pages_processed = 0
        self._pages_lock = threading.Lock()
        self._urls_discovered = 0
        self._discovered_lock = threading.Lock()
        self._active_workers = 0
        self._active_workers_lock = threading.Lock()

        self._request_interval = 1.0 / hit_rate if hit_rate > 0 else 0.0
        self._last_request_time = 0.0
        self._rate_lock = threading.Lock()

        self._ssl_secure = ssl.create_default_context()
        self._ssl_permissive = ssl.create_default_context()
        self._ssl_permissive.check_hostname = False
        self._ssl_permissive.verify_mode = ssl.CERT_NONE

        self._logs: list[dict] = []
        self._logs_lock = threading.Lock()
        self._state_lock = threading.Lock()

        self._created_at = int(time.time())
        self._completed_at: Optional[int] = None
        self.crawler_id: str = ""
        self.status: str = "pending"

    # -- public API ---------------------------------------------------------

    def start(
        self,
        manager: Optional["CrawlerManager"] = None,
        *,
        resume: bool = False,
    ) -> str:
        """Start the crawl.  Returns the crawler ID.

        Pass ``resume=True`` only for jobs rebuilt via ``resume_from_disk``,
        which already have ``crawler_id`` and on-disk configuration.

        For new jobs, pass ``manager`` so the job is registered before the main
        thread starts; otherwise another HTTP worker can handle pause/stop
        before :meth:`CrawlerManager.create_job` inserts the job into ``_jobs``.
        """
        if resume:
            if not self.crawler_id:
                raise ValueError("resume start requires crawler_id")
            self._main_thread = threading.Thread(target=self._run_main, daemon=True)
            self._main_thread.start()
            self.status = "running"
            pending_n = len(self._resume_tasks)
            ov = getattr(self, "_resume_omitted_visited", 0)
            od = getattr(self, "_resume_omitted_snapshot_dup", 0)
            msg = f"Resumed from disk — {pending_n} URL(s) queued to fetch"
            if ov or od:
                msg += f" ({ov} already visited"
                if od:
                    msg += f", {od} duplicate line(s) in snapshot file"
                msg += " omitted)"
            self._add_log(msg)
            self._save_state()
            return self.crawler_id

        self._id_ready = threading.Event()
        # ID before thread start so the manager can register the job immediately;
        # ``ThreadingHTTPServer`` can otherwise serve /pause before ``_jobs`` is updated.
        self.crawler_id = f"{self._created_at}_{uuid.uuid4().hex[:10]}"
        self.status = "running"
        if manager is not None:
            with manager._lock:
                manager._jobs[self.crawler_id] = self
        self._main_thread = threading.Thread(target=self._run_main, daemon=True)
        self._main_thread.start()
        self._id_ready.set()
        self._add_log(f"Crawler started for {self.origin_url} (depth={self.max_depth})")
        self._save_state()
        return self.crawler_id

    @classmethod
    def resume_from_disk(cls, crawler_id: str, manager: "CrawlerManager") -> "CrawlerJob":
        """Rebuild a job from ``{id}.data`` plus optional ``{id}.queue`` snapshot."""
        raw = manager.crawler_store.read(crawler_id)
        if not raw:
            raise ValueError("crawler state not found")
        if raw.get("status") == "finished":
            raise ValueError("cannot resume a finished crawler")

        rows = load_crawler_queue_snapshot(manager.crawler_store.data_dir, crawler_id)
        tasks: list[CrawlTask] = []
        seen_norm: set[str] = set()
        omitted_visited = 0
        omitted_snapshot_dup = 0
        for row in rows:
            try:
                u = normalize_url(str(row["url"]))
                ou = str(row.get("origin_url", u))
                d = int(row.get("depth", 0))
                if u in seen_norm:
                    omitted_snapshot_dup += 1
                    continue
                seen_norm.add(u)
                if u in manager.visited_store:
                    omitted_visited += 1
                    continue
                tasks.append(CrawlTask(u, ou, d))
            except (KeyError, TypeError, ValueError):
                continue

        job = cls(
            origin_url=raw["origin_url"],
            max_depth=int(raw["max_depth"]),
            visited_store=manager.visited_store,
            word_store=manager.word_store,
            crawler_store=manager.crawler_store,
            max_workers=int(raw.get("max_workers", 4)),
            max_queue_size=int(raw.get("queue_capacity", 1000)),
            max_pages=raw.get("max_pages"),
            hit_rate=float(raw.get("hit_rate", 0.0)),
            http_timeout=float(raw.get("http_timeout", 10.0)),
        )
        job.crawler_id = crawler_id
        job._created_at = int(raw.get("created_at", time.time()))
        job._resume_mode = True
        job._resume_tasks = tasks
        job._resume_omitted_visited = omitted_visited
        job._resume_omitted_snapshot_dup = omitted_snapshot_dup
        job._pages_processed = int(raw.get("pages_processed", 0))
        job._urls_discovered = int(raw.get("urls_discovered", 0))
        with job._logs_lock:
            job._logs = list(raw.get("logs", []))[-2000:]
        job._completed_at = None
        job.status = "pending"
        return job

    def join(self, timeout: float = 5.0) -> None:
        """Wait for the main crawl thread to finish."""
        if hasattr(self, "_main_thread"):
            self._main_thread.join(timeout=timeout)

    def stop(self) -> None:
        """Signal workers to stop after completing current tasks."""
        self._stop_event.set()
        self._pause_event.set()
        if self.status in ("running", "paused"):
            self.status = "interrupted"
            self._completed_at = int(time.time())
            self._add_log("Crawler interrupted by user")
            self._save_state()

    def pause(self) -> None:
        """Pause the crawler; workers block until resumed."""
        if self.status == "running":
            self._pause_event.clear()
            self.status = "paused"
            self._add_log("Crawler paused")
            self._save_state()

    def resume(self) -> None:
        """Resume a paused crawler."""
        if self.status == "paused":
            self._pause_event.set()
            self.status = "running"
            self._add_log("Crawler resumed")
            self._save_state()

    @property
    def is_running(self) -> bool:
        return self.status in ("running", "paused")

    @property
    def is_paused(self) -> bool:
        return self.status == "paused"

    def get_status(self) -> dict:
        """Return a JSON-serialisable status snapshot."""
        return self._build_state()

    # -- internal helpers ---------------------------------------------------

    def _add_log(self, message: str) -> None:
        ts = int(time.time())
        with self._logs_lock:
            self._logs.append({"timestamp": ts, "message": message})
        if self.crawler_id:
            try:
                append_crawler_log_line(self._data_dir, self.crawler_id, ts, message)
            except OSError:
                pass

    def _build_state(self) -> dict:
        with self._pages_lock:
            processed = self._pages_processed
        with self._discovered_lock:
            discovered = self._urls_discovered
        with self._active_workers_lock:
            active = self._active_workers
        with self._logs_lock:
            logs = list(self._logs)
        preview: list[dict] = []
        persisted_n = 0
        if self.status == "interrupted" and self.crawler_id:
            preview = peek_crawler_queue_snapshot(self._data_dir, self.crawler_id, 40)
            persisted_n = count_crawler_queue_lines(self._data_dir, self.crawler_id)
        now = int(time.time())
        return {
            "id": self.crawler_id,
            "origin_url": self.origin_url,
            "max_depth": self.max_depth,
            "max_workers": self.max_workers,
            "http_timeout": self.http_timeout,
            "hit_rate": self.hit_rate,
            "status": self.status,
            "created_at": self._created_at,
            "updated_at": now,
            "completed_at": self._completed_at,
            "queue_capacity": self.max_queue_size,
            "max_pages": self.max_pages,
            "pages_processed": processed,
            "urls_discovered": discovered,
            "active_workers": active,
            "queue_size": self.queue.qsize(),
            "backpressure_active": self.queue.is_at_capacity(),
            "queue_preview": preview,
            "persisted_queue_count": persisted_n,
            "logs": logs,
        }

    def _save_state(self) -> None:
        with self._state_lock:
            self._crawler_store.save(self.crawler_id, self._build_state())

    # -- main job thread ----------------------------------------------------

    def _run_main(self) -> None:
        if not self._resume_mode:
            self._id_ready.wait()

        if self._resume_mode:
            if self._resume_tasks:
                for t in self._resume_tasks:
                    if not self.queue.try_put_nowait(t):
                        break
                delete_crawler_queue_file(self._data_dir, self.crawler_id)
                self._resume_tasks = []
            else:
                self.queue.put(CrawlTask(self.origin_url, self.origin_url, 0))
        else:
            self.queue.put(CrawlTask(self.origin_url, self.origin_url, 0))
            with self._discovered_lock:
                self._urls_discovered = 1

        for i in range(self.max_workers):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"{self.crawler_id}-w{i}",
                daemon=True,
            )
            self._workers.append(t)
            t.start()

        self._wait_for_completion()

        if self.status in ("running", "pending"):
            self.status = "finished"
            self._completed_at = int(time.time())
            self._add_log("Crawler finished")
            delete_crawler_queue_file(self._data_dir, self.crawler_id)
        elif self.status == "interrupted":
            self._persist_unserved_queue_to_disk()

        self._save_state()
        self._visited_store.save()

    def _persist_unserved_queue_to_disk(self) -> None:
        """Drain the in-memory queue after workers stop; persist for resume."""
        pending: list[CrawlTask] = []
        while True:
            t = self.queue.try_get_nowait()
            if t is None:
                break
            pending.append(t)
        if pending:
            save_crawler_queue_snapshot(
                self._data_dir,
                self.crawler_id,
                [asdict(x) for x in pending],
            )
            self._add_log(f"Saved {len(pending)} pending URL(s) for resume")
        else:
            delete_crawler_queue_file(self._data_dir, self.crawler_id)

    def _wait_for_completion(self) -> None:
        """Block until workers drain the queue or stop is signalled.

        Uses active completion detection (queue empty + no active workers)
        rather than relying solely on worker thread joins, which can hang
        if a worker is stuck in a slow/hanging HTTP request.
        """
        idle_rounds = 0
        stall_rounds = 0
        while not self._stop_event.is_set():
            time.sleep(0.5)

            alive = sum(1 for t in self._workers if t.is_alive())
            if alive == 0:
                remaining = self.queue.qsize()
                if remaining:
                    self._add_log(
                        f"All workers exited with {remaining} task(s) still queued"
                    )
                break

            with self._active_workers_lock:
                active = self._active_workers
            q_empty = self.queue.qsize() == 0
            not_paused = self._pause_event.is_set()
            if active == 0 and q_empty and not_paused:
                idle_rounds += 1
                if idle_rounds >= 6:
                    break
            else:
                idle_rounds = 0

            if active == 0 and not q_empty and not_paused:
                stall_rounds += 1
                if stall_rounds >= 20:
                    self._add_log(
                        f"Workers stalled: {alive} alive, 0 active, "
                        f"{self.queue.qsize()} queued — stopping"
                    )
                    break
            else:
                stall_rounds = 0

        self._stop_event.set()
        deadline = time.time() + 3
        for t in self._workers:
            remaining = max(0.1, deadline - time.time())
            t.join(timeout=remaining)

    # -- worker loop --------------------------------------------------------

    def _worker_loop(self) -> None:
        # Do not exit on an empty queue; only _wait_for_completion sets _stop_event.
        while not self._stop_event.is_set():
            if not self._pause_event.is_set():
                self._pause_event.wait(timeout=0.5)
                continue
            task = self.queue.try_get_nowait()
            if task is None:
                self._stop_event.wait(0.05)
                continue
            with self._active_workers_lock:
                self._active_workers += 1
            try:
                self._rate_limit()
                if self._stop_event.is_set():
                    return
                self._process_task(task)
                if self.max_pages is not None:
                    with self._pages_lock:
                        if self._pages_processed >= self.max_pages:
                            self._stop_event.set()
                            return
            except Exception as exc:
                self._add_log(f"Worker error processing {task.url}: {exc}")
            finally:
                with self._active_workers_lock:
                    self._active_workers -= 1

    def _process_task(self, task: CrawlTask) -> None:
        if task.depth > self.max_depth:
            return

        normalized = normalize_url(task.url)
        if not self._visited_store.add_if_new(normalized):
            return

        with self._pages_lock:
            if self.max_pages is not None and self._pages_processed >= self.max_pages:
                return
            self._pages_processed += 1

        self._add_log(f"Fetching {task.url} (depth={task.depth})")

        html = self._fetch_html(task.url)
        if html is None:
            return

        if len(html) > self._MAX_PARSE_BYTES:
            self._add_log(
                f"Truncating HTML to {self._MAX_PARSE_BYTES // 1024} KiB "
                f"({len(html)} bytes) for {task.url}"
            )
            html = html[: self._MAX_PARSE_BYTES]

        title, content = extract_title_and_content(html)
        title = title[:2000]
        if len(content) > self._MAX_TEXT_CHARS:
            content = content[: self._MAX_TEXT_CHARS]
        word_counts = Counter(tokenize(content) + tokenize(title))
        if len(word_counts) > self._MAX_INDEX_TERMS:
            word_counts = Counter(dict(word_counts.most_common(self._MAX_INDEX_TERMS)))
        if word_counts:
            self._word_store.add_words(
                word_counts, task.url, task.origin_url, task.depth,
            )

        if task.depth < self.max_depth:
            raw_links = self._extract_links(task.url, html)
            candidates = self._visited_store.filter_unvisited(raw_links)
            if len(candidates) > self._MAX_OUTLINKS_PER_PAGE:
                candidates = candidates[: self._MAX_OUTLINKS_PER_PAGE]
            for link in candidates:
                if self._stop_event.is_set():
                    break
                new_task = CrawlTask(link, task.origin_url, task.depth + 1)
                if not self.queue.try_put_nowait(new_task):
                    break
                with self._discovered_lock:
                    self._urls_discovered += 1

        with self._pages_lock:
            should_save = self._pages_processed % 10 == 0
        if should_save:
            self._save_state()

    # -- rate limiting / HTTP / HTML ----------------------------------------

    def _rate_limit(self) -> None:
        """Throttle requests according to ``hit_rate``.

        Reserves the next available slot under the lock, then sleeps
        outside the lock so other workers can reserve subsequent slots
        concurrently.
        """
        if self._request_interval <= 0:
            return
        with self._rate_lock:
            now = time.time()
            earliest = self._last_request_time + self._request_interval
            if earliest <= now:
                self._last_request_time = now
                return
            self._last_request_time = earliest
            sleep_for = earliest - now
        time.sleep(sleep_for)

    _MAX_RESPONSE_BYTES = 5 * 1024 * 1024  # 5 MB
    # Parsing/indexing very large HTML (e.g. Wikipedia) can take minutes on one CPU-bound
    # thread with no further "Fetching" logs; cap bytes fed to HTMLParser and WordStore.
    _MAX_PARSE_BYTES = 512 * 1024  # 512 KiB
    # WordStore rewrites one JSON file per letter touched; huge pages + large indexes
    # can spend minutes per page with no further logs. Bound text and distinct terms.
    _MAX_TEXT_CHARS = 350_000
    _MAX_INDEX_TERMS = 5_000
    # Wikipedia Main Page can expose 10k+ hrefs; checking each with ``in visited`` took a
    # lock per link. Batch-filter instead and cap how many we enqueue per page.
    _MAX_OUTLINKS_PER_PAGE = 500

    def _fetch_html(self, url: str) -> Optional[str]:
        if self._stop_event.is_set():
            return None
        req = request.Request(url, headers={"User-Agent": "google-in-a-day-crawler/1.0"})
        contexts = [self._ssl_secure, self._ssl_permissive]
        for attempt, ctx in enumerate(contexts, 1):
            if self._stop_event.is_set():
                return None
            try:
                with request.urlopen(req, timeout=self.http_timeout, context=ctx) as resp:
                    ct = resp.headers.get("Content-Type", "")
                    if "text/html" not in ct:
                        self._add_log(f"Skipping non-HTML: {url} ({ct})")
                        return None

                    content_len = resp.headers.get("Content-Length")
                    if content_len and int(content_len) > self._MAX_RESPONSE_BYTES:
                        self._add_log(f"Skipping oversized page: {url} ({content_len} bytes)")
                        return None

                    charset = "utf-8"
                    try:
                        charset = resp.headers.get_content_charset() or "utf-8"
                    except AttributeError:
                        pass

                    data = resp.read(self._MAX_RESPONSE_BYTES + 1)
                    if len(data) > self._MAX_RESPONSE_BYTES:
                        self._add_log(f"Truncated oversized page: {url}")
                        data = data[:self._MAX_RESPONSE_BYTES]
                    return data.decode(charset, errors="replace")
            except ssl.SSLError:
                if attempt < len(contexts):
                    continue
                self._add_log(f"SSL error fetching {url} (all contexts failed)")
            except error.HTTPError as exc:
                self._add_log(f"HTTP error fetching {url}: {exc}")
                return None
            except error.URLError as exc:
                self._add_log(f"URL error fetching {url}: {exc}")
                return None
            except Exception as exc:
                self._add_log(f"Error fetching {url}: {exc}")
                return None
        return None

    def _extract_links(self, base_url: str, html: str) -> list[str]:
        parser = LinkExtractor(base_url)
        try:
            parser.feed(html)
            parser.close()
        except Exception:
            pass
        out: list[str] = []
        for raw in parser.links:
            norm = normalize_url(raw)
            if parse.urlparse(norm).scheme in ("http", "https"):
                out.append(norm)
        return out


# ---------------------------------------------------------------------------
# CrawlerManager — coordinates jobs and shared storage
# ---------------------------------------------------------------------------

class CrawlerManager:
    """Create, track, and query multiple crawler jobs."""

    def __init__(self, data_dir: str) -> None:
        self._data_dir = data_dir
        os.makedirs(os.path.join(data_dir, "storage"), exist_ok=True)

        self.visited_store = VisitedUrlsStore(data_dir)
        self.word_store = WordStore(data_dir)
        self.crawler_store = CrawlerDataStore(data_dir)

        self._jobs: dict[str, CrawlerJob] = {}
        self._lock = threading.Lock()

    def create_job(
        self,
        origin_url: str,
        max_depth: int = 2,
        max_workers: int = 4,
        max_queue_size: int = 1000,
        max_pages: Optional[int] = 500,
        hit_rate: float = 0.0,
        http_timeout: float = 10.0,
    ) -> str:
        """Create and start a new crawl job.  Returns the crawler ID."""
        job = CrawlerJob(
            origin_url=origin_url,
            max_depth=max_depth,
            visited_store=self.visited_store,
            word_store=self.word_store,
            crawler_store=self.crawler_store,
            max_workers=max_workers,
            max_queue_size=max_queue_size,
            max_pages=max_pages,
            hit_rate=hit_rate,
            http_timeout=http_timeout,
        )
        return job.start(manager=self)

    def get_job(self, crawler_id: str) -> Optional[CrawlerJob]:
        with self._lock:
            return self._jobs.get(crawler_id)

    def get_job_status(self, crawler_id: str) -> Optional[dict]:
        """Return live status for an active job, else read from disk."""
        with self._lock:
            job = self._jobs.get(crawler_id)
        if job:
            return job.get_status()
        data = self.crawler_store.read(crawler_id)
        if not data:
            return None
        out = dict(data)
        if out.get("status") == "interrupted":
            out["queue_preview"] = peek_crawler_queue_snapshot(
                self.crawler_store.data_dir, crawler_id, 40,
            )
            out["persisted_queue_count"] = count_crawler_queue_lines(
                self.crawler_store.data_dir, crawler_id,
            )
        else:
            out.setdefault("queue_preview", [])
            out.setdefault("persisted_queue_count", 0)
        out.setdefault("max_workers", 4)
        out.setdefault("http_timeout", 10.0)
        out.setdefault("updated_at", out.get("created_at"))
        out.setdefault("completed_at", None)
        return out

    def list_jobs(self) -> list[dict]:
        """All jobs: prefer live status for running ones, disk for historical."""
        with self._lock:
            active = {cid: j.get_status() for cid, j in self._jobs.items()}
        from_disk = self.crawler_store.list_all()
        merged: dict[str, dict] = {}
        for d in from_disk:
            cid = d.get("id", "")
            merged[cid] = active.get(cid, d)
        for cid, status in active.items():
            if cid not in merged:
                merged[cid] = status
        return sorted(merged.values(), key=lambda d: d.get("created_at", 0), reverse=True)

    # -- job control --------------------------------------------------------

    def stop_job(self, crawler_id: str) -> bool:
        """Stop a running/paused job.  Returns True if the signal was sent."""
        with self._lock:
            job = self._jobs.get(crawler_id)
        if job and job.is_running:
            job.stop()
            return True
        return False

    def pause_job(self, crawler_id: str) -> bool:
        """Pause a running job.  Returns True if paused."""
        with self._lock:
            job = self._jobs.get(crawler_id)
        if job and job.status == "running":
            job.pause()
            return True
        return False

    def resume_job(self, crawler_id: str) -> bool:
        """Resume a paused job.  Returns True if resumed."""
        with self._lock:
            job = self._jobs.get(crawler_id)
        if job and job.is_paused:
            job.resume()
            return True
        return False

    def resume_job_from_disk(self, crawler_id: str) -> bool:
        """Restart an interrupted crawler using ``{id}.data`` and ``{id}.queue``."""
        with self._lock:
            existing = self._jobs.get(crawler_id)
            if existing and existing.is_running:
                return False
            try:
                job = CrawlerJob.resume_from_disk(crawler_id, self)
            except ValueError:
                return False
            self._jobs[crawler_id] = job
        job.start(resume=True)
        return True

    # -- statistics ---------------------------------------------------------

    def get_statistics(self) -> dict:
        """Return aggregate statistics across all crawlers."""
        jobs = self.list_jobs()
        active = sum(1 for j in jobs if j.get("status") in ("running", "paused"))
        total_pages = sum(j.get("pages_processed", 0) for j in jobs)
        return {
            "total_crawlers": len(jobs),
            "active_crawlers": active,
            "total_visited_urls": len(self.visited_store),
            "total_pages_processed": total_pages,
            "total_words_indexed": self.word_store.total_words(),
        }

    # -- data management ----------------------------------------------------

    def clear_data(self) -> dict:
        """Delete all runtime data files.  Returns summary."""
        with self._lock:
            for job in self._jobs.values():
                if job.is_running:
                    job.stop()
            self._jobs.clear()
        removed = self.crawler_store.clear_all()
        aux = clear_crawler_auxiliary_files(self._data_dir)
        self.visited_store.clear()
        self.word_store.clear()
        return {"cleared": True, "files_removed": removed + aux}

    def shutdown(self) -> None:
        """Stop all running jobs, wait for them to finish, persist visited URLs."""
        with self._lock:
            jobs = list(self._jobs.values())
        for job in jobs:
            if job.is_running:
                job.stop()
        for job in jobs:
            job.join(timeout=3)
        try:
            self.visited_store.save()
        except Exception:
            pass


__all__ = [
    "CrawlerJob",
    "CrawlerManager",
    "CrawlTask",
    "LinkExtractor",
    "UrlQueue",
]
