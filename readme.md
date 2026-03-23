## Google in a Day

This is a web crawler and search engine project that provides basic crawling and search capabilities. The system is implemented as a single-node Python application using only standard library modules.

The site has 3 main pages:

- **Crawler**
- **Crawler Status**
- **Search**

The **Crawler** page allows users to create crawler jobs with a given origin URL and depth, along with other optional parameters such as number of workers, queue capacity (which defines when to pause based on queued URL pages), and maximum pages to visit. When the user clicks the Crawl button, a new thread is created to start crawling the initial page at the specified depth, performing the crawler operation as defined below. After creating a crawler, users can see its status, whether it's still running or finished. During or after completion, a link directs users to the crawler's current status based on its ID. Additionally, the crawler page displays the status of previous crawler operations below, ordered by time, with links to view each.

The **Crawler Status** page allows users to view the current status of each crawler operation. It uses long polling to display state logs and provide key insights (pages processed, URLs discovered, queue depth, active workers). Users can **pause**, **resume**, or **stop** active crawlers directly from this page. If the operation is completed or interrupted, it will be noted at the top of the page.

The **Search** page allows users to search with a query. The query will be parsed into text, and results will be returned based on the maximum hits from the filesystem, showing the URL, origin URL, and depth relevant to the search. Results are paginated.

---

### Crawler Job (Major Component)

This is the core of the project. It receives a URL and depth, along with optional parameters such as number of workers, queue capacity (which defines when to pause based on queued URL pages), maximum pages to visit, and hit rate (requests per second for rate limiting).

**Workers (`max_workers`) vs. the status metric "active workers".** Many sample crawlers use a single thread to do all fetching; this project uses a **configurable pool of worker threads per crawl job** (`max_workers`, default 4). Each worker runs the same loop: take the next URL from that job's shared bounded queue, fetch HTML, extract links, and enqueue new tasks up to the depth limit. **`max_workers`** is therefore the maximum **parallel** fetch-and-process pipelines for one job (subject to `hit_rate`, queue back-pressure, and how many URLs are actually available). On the **Crawler Status** page, **active workers** is a live count of how many workers are **currently busy** with a URL—not the configured cap. If the frontier is small or workers are waiting on I/O, active workers can be lower than `max_workers`.

The crawler starts if all input parameters are valid. It creates a new thread to perform the job. The thread ID is used to create the crawler ID, combined with the creation time. The crawler ID is in the format `[EpochTimeCreated_ThreadID]`, where "EpochTimeCreated" is the Unix epoch time and "ThreadID" is the system-defined thread ID. As soon as the crawler ID is defined, the thread creates a file named `[crawlerId].data` that holds the crawler's status, including all logs and other details in JSON format.

A crawler thread begins by reading `visited_urls.data`. If the file does not exist, an empty `visited_urls.data` file is created. This file stores each URL on a new line, allowing the crawler to check each new URL and skip it if it has already been visited. If the origin page has not been visited, the thread fetches the HTML page using Python's standard library (`urllib`). If the status is 200, it means the crawler successfully retrieved the page and continues; otherwise, it logs the error and moves on. When the crawler obtains a valid HTML page source, it retrieves the word counter (text and frequency) and a list of other URL pages. At this point, the pages are stored as visited pages, and each word is stored by its initial letter (e.g., `[letter].data`) under the `storage` folder, along with its corresponding origin URL, current URL, current depth value, and frequency count.

Back-pressure is enforced via a bounded URL queue. When the queue reaches its configured capacity, producer threads block until space becomes available, preventing unbounded resource consumption.

Rate limiting is configurable via the `hit_rate` parameter (requests per second). When set to a positive value, worker threads throttle their HTTP requests to stay within the configured rate. A value of 0 means unlimited.

SSL certificate handling uses a dual-context strategy: the crawler first attempts HTTPS requests with full certificate verification, and automatically falls back to a permissive SSL context for sites with certificate issues.

Crawlers support **pause/resume/stop** controls. Pausing blocks all worker threads until the crawler is resumed, while stopping signals workers to finish their current task and exit gracefully. Crawler state is persisted in `[crawlerId].data`, and visited URLs plus the word index are written to disk as work proceeds.

**Resume from disk.** When a crawler is **stopped** (interrupted), all pending URLs still in the queue are saved to a `[crawlerId].queue` snapshot file (NDJSON). The Crawler Status page then shows a **Resume from disk** button for that job. Clicking it rebuilds the job from `[crawlerId].data` (configuration, metrics, logs) and `[crawlerId].queue` (pending frontier), filtering out URLs that already appear in the visited set. The resumed crawler continues where it left off with its original settings. On the Crawler listing page, interrupted jobs also show a Resume from disk button for convenience. If the **process exits abruptly** (crash, `kill -9`), the queue snapshot is not written because the in-memory queue is lost. In that case only work already recorded in `visited_urls.data` and the letter files is retained; the visited set still prevents fetching the same normalized URL twice on a subsequent crawl.

### Search (Minor Component)

As pages are crawled, each word is stored by its initial letter in `[letter].data` under `data/storage`. Queries use terms of length ≥ 2. For each term we try an **exact** dictionary key first, then the **longest indexed prefix** (length ≥ 3) so minor query/index mismatches can still match without a stemmer. Hits are merged **per URL**; each row gets `total_frequency` (sum of term counts) and a **`relevance_score`** (frequency weight, bonus for exact key match, partial-match weight, and a small depth penalty—similar in spirit to a simple BM25-style heuristic). The UI and `/api/search` support **`sort=relevance|frequency|depth`** (default `relevance`). Results are paginated.

Search uses the same on-disk word index the crawler updates while jobs run. There is no separate "indexing finished" gate: queries reflect pages already written, and new hits appear as workers persist content. `WordStore` uses per-letter locks so crawler threads and search requests can run concurrently without corrupting JSON files.

---

### Requirements

- **Python**: 3.10+ (tested on CPython 3.10/3.11).
- **OS**: macOS or Linux.
- **Dependencies**: Only Python standard library modules are used; no external packages are required.

---

### Quick Start

```bash
python3 run.py
```

Then open [http://localhost:3600](http://localhost:3600) in your browser.

---

### Project Layout

- `crawler/indexer.py` — `CrawlerJob` (pause/resume/stop, rate limiting, SSL fallback), `CrawlerManager` (statistics, clear data), `UrlQueue`, `LinkExtractor`.
- `storage/file_store.py` — `VisitedUrlsStore`, `WordStore`, `CrawlerDataStore` (with clear/stats support).
- `search/searcher.py` — File-based `Searcher` (prefix fallback, relevance score, `sort` modes, pagination).
- `web/server.py` — HTTP server with pages + JSON API + crawler controls (pause/resume/stop/clear/stats); `/api/crawler-dashboard` feeds the live Crawler UI.
- `utils.py` — Shared text utilities (`normalize_url`, `tokenize`, `extract_title_and_content`).
- `run.py` — Main entry point (starts the web server).
- `verify_system.py` — End-to-end verification script (73 automated checks).
- `product_prd.md` — Full Product Requirements Document for the MVP.
- `recommendation.md` — Production deployment recommendations.
- `data/` — Runtime data directory:
  - `visited_urls.data` — Shared visited URL set (one URL per line).
  - `storage/` — Word index files by initial letter (`a.data` … `z.data`).
  - `[crawlerId].data` — Per-crawler status, metrics, and logs in JSON format.
  - `[crawlerId].queue` — Pending URL snapshot (NDJSON), written on stop for resume-from-disk; deleted on finish.

---

### Running the Verification Suite

```bash
python3 verify_system.py
```

This runs 73 automated checks covering:

- Deliverable file existence.
- File-based storage (visited\_urls.data, \[letter\].data, \[crawlerId\].data).
- Depth-limited crawling (k=2) and visited-set deduplication.
- Back-pressure under a tiny bounded queue.
- Search with pagination.
- Web API endpoints (Crawler, Status, Search pages + JSON API).
- Unit checks for URL normalization, tokenization, and HTML parsing.

---

### Data File Formats

**`data/visited_urls.data`** — one normalized URL per line:

```
http://example.com
http://example.com/alpha
http://example.com/beta
```

**`data/storage/q.data`** — JSON, words starting with letter 'q':

```json
{
  "quantum": [
    {"url": "http://example.com/", "origin_url": "http://example.com/", "depth": 0, "frequency": 3}
  ]
}
```

**`data/1679000000_12345.data`** — JSON, per-crawler state:

```json
{
  "id": "1679000000_12345",
  "origin_url": "http://example.com",
  "max_depth": 2,
  "max_workers": 4,
  "http_timeout": 10.0,
  "status": "finished",
  "created_at": 1679000000,
  "updated_at": 1679000120,
  "completed_at": 1679000120,
  "queue_capacity": 1000,
  "pages_processed": 42,
  "urls_discovered": 100,
  "logs": [
    {"timestamp": 1679000001, "message": "Fetching http://example.com (depth=0)"}
  ]
}
```

`updated_at` is refreshed on every state save. `completed_at` is set when the job reaches a terminal state (`finished` or `interrupted`); it is `null` while the crawl is running and cleared when resuming from disk after an interrupt.

---

### Non-goals and Limitations

This MVP is intentionally not a full internet search engine. In particular:

- No distributed crawling, sharding, or cross-region deployment.
- No advanced ranking (PageRank, learning-to-rank, personalization) beyond simple keyword frequency.
- No JavaScript execution or headless browser support; only HTML fetched via standard library HTTP.

The codebase is structured so that these capabilities can be added incrementally.
