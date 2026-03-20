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
import time
from collections import Counter
from dataclasses import dataclass
from html.parser import HTMLParser
from queue import Empty, Full, Queue
from typing import Optional
from urllib import error, parse, request

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from storage.file_store import CrawlerDataStore, VisitedUrlsStore, WordStore
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
# CrawlerJob — one crawl operation identified by [EpochTime_ThreadID]
# ---------------------------------------------------------------------------

class CrawlerJob:
    """A single crawl job with a unique ID in the format ``EpochTime_ThreadID``.

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
        self.crawler_id: str = ""
        self.status: str = "pending"

    # -- public API ---------------------------------------------------------

    def start(self) -> str:
        """Start the crawl.  Returns the crawler ID."""
        self._id_ready = threading.Event()
        self._main_thread = threading.Thread(target=self._run_main, daemon=True)
        self._main_thread.start()
        self.crawler_id = f"{self._created_at}_{self._main_thread.ident}"
        self.status = "running"
        self._id_ready.set()
        self._add_log(f"Crawler started for {self.origin_url} (depth={self.max_depth})")
        self._save_state()
        return self.crawler_id

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
        with self._logs_lock:
            self._logs.append({"timestamp": int(time.time()), "message": message})

    def _build_state(self) -> dict:
        with self._pages_lock:
            processed = self._pages_processed
        with self._discovered_lock:
            discovered = self._urls_discovered
        with self._active_workers_lock:
            active = self._active_workers
        with self._logs_lock:
            logs = list(self._logs)
        return {
            "id": self.crawler_id,
            "origin_url": self.origin_url,
            "max_depth": self.max_depth,
            "hit_rate": self.hit_rate,
            "status": self.status,
            "created_at": self._created_at,
            "queue_capacity": self.max_queue_size,
            "max_pages": self.max_pages,
            "pages_processed": processed,
            "urls_discovered": discovered,
            "active_workers": active,
            "queue_size": self.queue.qsize(),
            "backpressure_active": self.queue.is_at_capacity(),
            "logs": logs,
        }

    def _save_state(self) -> None:
        with self._state_lock:
            self._crawler_store.save(self.crawler_id, self._build_state())

    # -- main job thread ----------------------------------------------------

    def _run_main(self) -> None:
        self._id_ready.wait()

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
            self._add_log("Crawler finished")
        self._save_state()
        self._visited_store.save()

    def _wait_for_completion(self) -> None:
        """Block until workers drain the queue or stop is signalled.

        Uses active completion detection (queue empty + no active workers)
        rather than relying solely on worker thread joins, which can hang
        if a worker is stuck in a slow/hanging HTTP request.
        """
        idle_rounds = 0
        while not self._stop_event.is_set():
            time.sleep(0.5)

            if all(not t.is_alive() for t in self._workers):
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

        self._stop_event.set()
        deadline = time.time() + 3
        for t in self._workers:
            remaining = max(0.1, deadline - time.time())
            t.join(timeout=remaining)

    # -- worker loop --------------------------------------------------------

    def _worker_loop(self) -> None:
        idle_ticks = 0
        while not self._stop_event.is_set():
            if not self._pause_event.is_set():
                idle_ticks = 0
                self._pause_event.wait(timeout=0.5)
                continue
            task = self.queue.try_get_nowait()
            if task is None:
                with self._active_workers_lock:
                    peers_busy = self._active_workers > 0
                if peers_busy:
                    idle_ticks = 0
                else:
                    idle_ticks += 1
                    if idle_ticks > 60:
                        return
                self._stop_event.wait(0.05)
                continue
            idle_ticks = 0
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

        title, content = extract_title_and_content(html)
        word_counts = Counter(tokenize(content) + tokenize(title))
        if word_counts:
            self._word_store.add_words(
                word_counts, task.url, task.origin_url, task.depth,
            )

        if task.depth < self.max_depth:
            for link in self._extract_links(task.url, html):
                if link in self._visited_store:
                    continue
                new_task = CrawlTask(link, task.origin_url, task.depth + 1)
                if not self.queue.put_with_backpressure(new_task, self._stop_event):
                    if self._stop_event.is_set():
                        break
                else:
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
        crawler_id = job.start()
        with self._lock:
            self._jobs[crawler_id] = job
        return crawler_id

    def get_job(self, crawler_id: str) -> Optional[CrawlerJob]:
        with self._lock:
            return self._jobs.get(crawler_id)

    def get_job_status(self, crawler_id: str) -> Optional[dict]:
        """Return live status for an active job, else read from disk."""
        with self._lock:
            job = self._jobs.get(crawler_id)
        if job:
            return job.get_status()
        return self.crawler_store.read(crawler_id)

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
        self.visited_store.clear()
        self.word_store.clear()
        return {"cleared": True, "files_removed": removed}

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
