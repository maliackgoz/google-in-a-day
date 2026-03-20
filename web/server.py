"""Web server for Google in a Day.

Pages:
  /              – Crawler page (create jobs, view history, statistics)
  /status/<id>   – Crawler status (long-polling, pause/resume/stop controls)
  /search        – Search page (query + paginated results)

JSON API:
  /api/status/<id>       – status with optional long-polling
  /api/search            – paginated search results
  /api/stats             – aggregate crawler statistics
  POST /stop/<id>        – stop a running crawler
  POST /pause/<id>       – pause a running crawler
  POST /resume/<id>      – resume a paused crawler
  POST /clear            – clear all crawler data
"""

from __future__ import annotations

import html as html_mod
import http.server
import json
import logging
import os
import sys
import threading
import time
from typing import TYPE_CHECKING, Any
from urllib import parse

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

if TYPE_CHECKING:
    from crawler.indexer import CrawlerManager
    from search.searcher import Searcher

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared CSS
# ---------------------------------------------------------------------------

_CSS = """\
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
line-height:1.6;color:#333;max-width:960px;margin:0 auto;padding:20px;background:#fafafa}
h1{font-size:1.8rem;margin-bottom:4px}
h2{font-size:1.3rem;margin:24px 0 12px;color:#444}
a{color:#1a73e8;text-decoration:none}a:hover{text-decoration:underline}
nav{display:flex;gap:18px;padding:12px 0;margin-bottom:16px;border-bottom:1px solid #e0e0e0;font-weight:500}
.subtitle{color:#666;font-size:.95rem;margin-bottom:16px}
.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;max-width:600px}
.form-group{display:flex;flex-direction:column}
.form-group.full{grid-column:1/-1}
label{font-size:.85rem;font-weight:600;margin-bottom:4px;color:#555}
input{padding:8px 10px;border:1px solid #ccc;border-radius:6px;font-size:.95rem;outline:none}
input:focus{border-color:#1a73e8;box-shadow:0 0 0 2px rgba(26,115,232,.15)}
button,.btn{display:inline-block;padding:9px 24px;background:#1a73e8;color:#fff;
border:none;border-radius:6px;font-size:.95rem;cursor:pointer;font-weight:500;margin-top:8px}
button:hover,.btn:hover{background:#1557b0}
table{width:100%;border-collapse:collapse;margin-top:8px;background:#fff;
border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.08)}
th{background:#f5f5f5;text-align:left;padding:10px 14px;font-size:.82rem;
text-transform:uppercase;color:#666;letter-spacing:.4px}
td{padding:10px 14px;border-top:1px solid #f0f0f0;font-size:.93rem}
tr:hover td{background:#fafcff}
.badge{display:inline-block;padding:2px 10px;border-radius:12px;font-size:.78rem;font-weight:600}
.badge-running{background:#e6f4ea;color:#1e7e34}
.badge-finished{background:#e8eaf6;color:#3949ab}
.badge-interrupted{background:#fce4ec;color:#c62828}
.badge-pending{background:#fff3e0;color:#e65100}
.badge-paused{background:#fff3e0;color:#e65100}
.controls{display:flex;gap:8px;margin:16px 0}
.btn-pause{background:#f9a825;color:#fff;border:none;border-radius:6px;padding:8px 18px;
cursor:pointer;font-weight:500;font-size:.9rem}
.btn-pause:hover{background:#f57f17}
.btn-resume{background:#43a047;color:#fff;border:none;border-radius:6px;padding:8px 18px;
cursor:pointer;font-weight:500;font-size:.9rem}
.btn-resume:hover{background:#2e7d32}
.btn-stop{background:#e53935;color:#fff;border:none;border-radius:6px;padding:8px 18px;
cursor:pointer;font-weight:500;font-size:.9rem}
.btn-stop:hover{background:#c62828}
.btn-danger{background:#e53935}
.btn-danger:hover{background:#c62828}
.stats-bar{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:20px}
.stat-item{background:#fff;padding:10px 16px;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.06);
font-size:.9rem}
.stat-item strong{color:#1a73e8}
.metrics{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px;margin:16px 0}
.metric-card{padding:14px;background:#fff;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.metric-card .value{font-size:1.6rem;font-weight:700;color:#1a73e8}
.metric-card .label{font-size:.8rem;color:#888;margin-top:2px}
.log-box{background:#1e1e2e;color:#cdd6f4;border-radius:8px;padding:14px;
font-family:"SF Mono",Menlo,monospace;font-size:.82rem;line-height:1.7;
max-height:400px;overflow-y:auto}
.log-entry{white-space:pre-wrap;word-break:break-all}
.log-ts{color:#89b4fa}
.search-box{display:flex;gap:8px;max-width:600px;margin-bottom:20px}
.search-box input{flex:1}
.result-card{padding:14px;margin-bottom:10px;background:#fff;
border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.06)}
.result-card .url{font-size:1.05rem;font-weight:500}
.result-card .meta{font-size:.82rem;color:#888;margin-top:4px}
.pagination{display:flex;gap:4px;margin-top:20px;flex-wrap:wrap}
.pagination a,.pagination span{padding:6px 14px;border:1px solid #ddd;
border-radius:6px;font-size:.88rem}
.pagination .active{background:#1a73e8;color:#fff;border-color:#1a73e8}
.empty-state{color:#999;padding:30px;text-align:center}
"""

# ---------------------------------------------------------------------------
# Layout helper
# ---------------------------------------------------------------------------

def _layout(title: str, body: str, active: str = "") -> str:
    nav_items = [("/", "Crawler"), ("/search", "Search")]
    nav_links = ""
    for href, label in nav_items:
        bold = ' style="color:#1557b0;text-decoration:underline"' if active == href else ""
        nav_links += f'<a href="{href}"{bold}>{label}</a>\n'
    return (
        '<!DOCTYPE html><html lang="en"><head>'
        '<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{html_mod.escape(title)}</title><style>{_CSS}</style></head>"
        f'<body><h1>Google in a Day</h1><p class="subtitle">Web Crawler &amp; Search Engine</p>'
        f"<nav>{nav_links}</nav>{body}</body></html>"
    )


# ---------------------------------------------------------------------------
# Page builders
# ---------------------------------------------------------------------------

def _crawler_page(jobs: list[dict], stats: dict | None = None) -> str:
    stats_html = ""
    if stats:
        stats_html = (
            '<div class="stats-bar">'
            f'<div class="stat-item">Crawlers: <strong>{stats.get("total_crawlers", 0)}</strong>'
            f' ({stats.get("active_crawlers", 0)} active)</div>'
            f'<div class="stat-item">Visited URLs: <strong>{stats.get("total_visited_urls", 0)}</strong></div>'
            f'<div class="stat-item">Pages Processed: <strong>{stats.get("total_pages_processed", 0)}</strong></div>'
            f'<div class="stat-item">Words Indexed: <strong>{stats.get("total_words_indexed", 0)}</strong></div>'
            "</div>"
        )

    rows = ""
    for j in jobs:
        cid = html_mod.escape(str(j.get("id", "")))
        origin = html_mod.escape(str(j.get("origin_url", "")))
        status = j.get("status", "unknown")
        pages = j.get("pages_processed", 0)
        ts = j.get("created_at", 0)
        created = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else "-"
        rows += (
            f'<tr><td><a href="/status/{cid}">{cid}</a></td>'
            f'<td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{origin}</td>'
            f'<td><span class="badge badge-{status}">{status}</span></td>'
            f"<td>{pages}</td><td>{created}</td></tr>\n"
        )
    if not rows:
        rows = '<tr><td colspan="5" class="empty-state">No crawl jobs yet.</td></tr>'

    clear_btn = ""
    if jobs:
        clear_btn = (
            '<form method="POST" action="/clear" style="display:inline;margin-left:12px">'
            '<button type="submit" class="btn btn-danger" '
            'onclick="return confirm(\'Clear all data?\')">Clear All Data</button></form>'
        )

    body = (
        f"{stats_html}"
        "<h2>New Crawl Job</h2>"
        '<form method="POST" action="/">'
        '<div class="form-grid">'
        '<div class="form-group full"><label for="origin_url">Origin URL</label>'
        '<input id="origin_url" name="origin_url" type="url" value="https://example.com" required></div>'
        '<div class="form-group"><label for="max_depth">Max Depth</label>'
        '<input id="max_depth" name="max_depth" type="number" value="2" min="0" max="10"></div>'
        '<div class="form-group"><label for="max_workers">Workers</label>'
        '<input id="max_workers" name="max_workers" type="number" value="4" min="1" max="32"></div>'
        '<div class="form-group"><label for="queue_capacity">Queue Capacity</label>'
        '<input id="queue_capacity" name="queue_capacity" type="number" value="1000" min="1"></div>'
        '<div class="form-group"><label for="max_pages">Max Pages</label>'
        '<input id="max_pages" name="max_pages" type="number" value="500" min="1"></div>'
        '<div class="form-group"><label for="hit_rate">Hit Rate (req/s, 0=unlimited)</label>'
        '<input id="hit_rate" name="hit_rate" type="number" value="0" min="0" max="1000" step="0.1"></div>'
        "</div>"
        '<button type="submit">Crawl</button></form>'
        f"<h2>Previous Crawl Operations{clear_btn}</h2>"
        "<table><thead><tr>"
        "<th>Crawler ID</th><th>Origin</th><th>Status</th><th>Pages</th><th>Created</th>"
        f"</tr></thead><tbody>{rows}</tbody></table>"
    )
    return _layout("Crawler", body, active="/")


def _status_page(crawler_id: str, data: dict) -> str:
    cid = html_mod.escape(crawler_id)
    status = data.get("status", "unknown")
    origin = html_mod.escape(str(data.get("origin_url", "")))
    log_count = len(data.get("logs", []))

    logs_html = ""
    for log in data.get("logs", []):
        ts = time.strftime("%H:%M:%S", time.localtime(log.get("timestamp", 0)))
        msg = html_mod.escape(str(log.get("message", "")))
        logs_html += f'<div class="log-entry"><span class="log-ts">[{ts}]</span> {msg}</div>\n'

    body = (
        f'<h2>Crawler Status &mdash; <code>{cid}</code></h2>'
        f'<p>Origin: <a href="{origin}" target="_blank">{origin}</a>'
        f' &nbsp;|&nbsp; Depth: {data.get("max_depth", "?")}'
        f' &nbsp;|&nbsp; <span class="badge badge-{status}" id="status-badge">{status}</span></p>'
        '<div class="controls" id="controls"></div>'
        '<div class="metrics">'
        f'<div class="metric-card"><div class="value" id="m-processed">{data.get("pages_processed", 0)}</div>'
        '<div class="label">Pages Processed</div></div>'
        f'<div class="metric-card"><div class="value" id="m-discovered">{data.get("urls_discovered", 0)}</div>'
        '<div class="label">URLs Discovered</div></div>'
        f'<div class="metric-card"><div class="value" id="m-queue">{data.get("queue_size", 0)}</div>'
        '<div class="label">Queue Size</div></div>'
        f'<div class="metric-card"><div class="value" id="m-workers">{data.get("active_workers", 0)}</div>'
        '<div class="label">Active Workers</div></div>'
        "</div>"
        "<h2>Logs</h2>"
        f'<div class="log-box" id="log-box">{logs_html}</div>'
        "<script>"
        "(function(){"
        f'var cid="{cid}",lc={log_count};'
        'var badge=document.getElementById("status-badge"),'
        'lb=document.getElementById("log-box"),'
        'ctrl=document.getElementById("controls");'
        "function isActive(s){return s==='running'||s==='pending'||s==='paused'}"
        "function sendCmd(action){"
        'fetch("/"+action+"/"+cid,{method:"POST"}).then(function(){});'
        "}"
        "function renderControls(s){"
        "if(!isActive(s)){ctrl.innerHTML='';return;}"
        "var h='';"
        "if(s==='running'){"
        """h+='<button class="btn-pause" onclick="sendCmd(\\'pause\\')">Pause</button>';"""
        "}else if(s==='paused'){"
        """h+='<button class="btn-resume" onclick="sendCmd(\\'resume\\')">Resume</button>';"""
        "}"
        """h+='<button class="btn-stop" onclick="sendCmd(\\'stop\\')">Stop</button>';"""
        "ctrl.innerHTML=h;}"
        f"renderControls('{status}');"
        "async function poll(){"
        "while(isActive(badge.textContent)){"
        "try{"
        'var r=await fetch("/api/status/"+cid+"?poll=true&last_log_count="+lc);'
        "if(!r.ok)break;var d=await r.json();"
        'document.getElementById("m-processed").textContent=d.pages_processed||0;'
        'document.getElementById("m-discovered").textContent=d.urls_discovered||0;'
        'document.getElementById("m-queue").textContent=d.queue_size||0;'
        'document.getElementById("m-workers").textContent=d.active_workers||0;'
        'badge.textContent=d.status;badge.className="badge badge-"+d.status;'
        "renderControls(d.status);"
        "var logs=d.logs||[];if(logs.length>lc){"
        "for(var i=lc;i<logs.length;i++){"
        'var ts=new Date(logs[i].timestamp*1000).toLocaleTimeString();'
        'var div=document.createElement("div");div.className="log-entry";'
        'div.innerHTML=\'<span class="log-ts">[\'+'
        "ts+\"]</span> \"+logs[i].message.replace(/</g,\"&lt;\");"
        "lb.appendChild(div)}"
        "lb.scrollTop=lb.scrollHeight;lc=logs.length}"
        "if(!isActive(d.status))break;"
        "}catch(e){break}"
        "await new Promise(function(r){setTimeout(r,1000)})}"
        "ctrl.innerHTML='';}"
        "window.sendCmd=sendCmd;"
        "poll();"
        "})();"
        "</script>"
    )
    return _layout(f"Status — {cid}", body)


def _search_page(query: str, results: dict) -> str:
    q_esc = html_mod.escape(query)
    per_page = results.get("per_page", 10)

    results_html = ""
    if query:
        for r in results.get("results", []):
            url = html_mod.escape(str(r.get("url", "")))
            origin = html_mod.escape(str(r.get("origin_url", "")))
            depth = r.get("depth", "?")
            freq = r.get("total_frequency", 0)
            results_html += (
                f'<div class="result-card">'
                f'<div class="url"><a href="{url}" target="_blank">{url}</a></div>'
                f'<div class="meta">Origin: {origin} &nbsp;|&nbsp; '
                f"Depth: {depth} &nbsp;|&nbsp; Frequency: {freq}</div></div>\n"
            )
        if not results.get("results"):
            results_html = '<div class="empty-state">No results found.</div>'

    total = results.get("total", 0)
    count_line = f'<p style="color:#666;font-size:.9rem;margin-bottom:12px">{total} result(s)</p>' if query else ""

    total_pages = results.get("total_pages", 0)
    current = results.get("page", 1)
    pag = ""
    if total_pages > 1:
        pag = '<div class="pagination">'
        for p in range(1, total_pages + 1):
            cls = ' class="active"' if p == current else ""
            pag += f'<a href="/search?q={parse.quote(query)}&page={p}&per_page={per_page}"{cls}>{p}</a>'
        pag += "</div>"

    body = (
        "<h2>Search</h2>"
        '<form method="GET" action="/search"><div class="search-box">'
        f'<input name="q" type="text" value="{q_esc}" placeholder="Enter search query&hellip;" autofocus>'
        '<button type="submit">Search</button></div></form>'
        f"{count_line}{results_html}{pag}"
    )
    return _layout("Search", body, active="/search")


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

class CrawlerHTTPServer(http.server.ThreadingHTTPServer):
    """Threaded HTTP server carrying references to the manager and searcher.

    ``daemon_threads`` is True so handler threads (including long-poll
    requests) don't prevent process exit on Ctrl+C.  A ``shutdown_event``
    lets long-poll handlers break early when the server is stopping.
    """

    daemon_threads = True

    def __init__(
        self,
        address: tuple,
        handler: type,
        manager: "CrawlerManager",
        searcher: "Searcher",
    ) -> None:
        super().__init__(address, handler)
        self.manager = manager
        self.searcher = searcher
        self.shutdown_event = threading.Event()


class CrawlerHandler(http.server.BaseHTTPRequestHandler):
    """Request handler for the three pages and JSON API."""

    server: CrawlerHTTPServer  # type: ignore[assignment]

    # -- routing ------------------------------------------------------------

    def do_GET(self) -> None:
        parsed = parse.urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        qs = parse.parse_qs(parsed.query)

        if path == "/":
            jobs = self.server.manager.list_jobs()
            stats = self.server.manager.get_statistics()
            self._respond_html(_crawler_page(jobs, stats))
        elif path.startswith("/status/"):
            cid = path[len("/status/"):]
            data = self.server.manager.get_job_status(cid)
            if data is None:
                self.send_error(404, "Crawler not found")
            else:
                self._respond_html(_status_page(cid, data))
        elif path == "/search":
            q = qs.get("q", [""])[0]
            page = int(qs.get("page", ["1"])[0])
            pp = int(qs.get("per_page", ["10"])[0])
            res = (
                self.server.searcher.search(q, page=page, per_page=pp)
                if q
                else {"results": [], "total": 0, "page": 1, "per_page": pp, "total_pages": 0}
            )
            self._respond_html(_search_page(q, res))
        elif path.startswith("/api/status/"):
            cid = path[len("/api/status/"):]
            self._api_status(cid, qs)
        elif path == "/api/search":
            self._api_search(qs)
        elif path == "/api/stats":
            self._respond_json(self.server.manager.get_statistics())
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        path = parse.urlparse(self.path).path.rstrip("/") or "/"
        if path == "/":
            self._handle_create_job()
        elif path.startswith("/stop/"):
            cid = path[len("/stop/"):]
            ok = self.server.manager.stop_job(cid)
            self._respond_json({"ok": ok})
        elif path.startswith("/pause/"):
            cid = path[len("/pause/"):]
            ok = self.server.manager.pause_job(cid)
            self._respond_json({"ok": ok})
        elif path.startswith("/resume/"):
            cid = path[len("/resume/"):]
            ok = self.server.manager.resume_job(cid)
            self._respond_json({"ok": ok})
        elif path == "/clear":
            self.server.manager.clear_data()
            self.send_response(303)
            self.send_header("Location", "/")
            self.end_headers()
        else:
            self.send_error(404)

    # -- helpers ------------------------------------------------------------

    def _respond_html(self, body: str, code: int = 200) -> None:
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _respond_json(self, obj: Any, code: int = 200) -> None:
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # -- POST handler -------------------------------------------------------

    def _handle_create_job(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8")
        params = parse.parse_qs(body)

        origin = params.get("origin_url", [""])[0].strip()
        if not origin:
            self.send_error(400, "Missing origin_url")
            return

        depth = int(params.get("max_depth", ["2"])[0])
        workers = int(params.get("max_workers", ["4"])[0])
        q_cap = int(params.get("queue_capacity", ["1000"])[0])
        max_pg = int(params.get("max_pages", ["500"])[0])
        hit_rate = float(params.get("hit_rate", ["0"])[0])

        cid = self.server.manager.create_job(
            origin_url=origin,
            max_depth=depth,
            max_workers=workers,
            max_queue_size=q_cap,
            max_pages=max_pg,
            hit_rate=hit_rate,
        )
        self.send_response(303)
        self.send_header("Location", f"/status/{cid}")
        self.end_headers()

    # -- JSON API -----------------------------------------------------------

    def _api_status(self, crawler_id: str, qs: dict) -> None:
        poll = qs.get("poll", [None])[0]
        last_count = int(qs.get("last_log_count", ["0"])[0])
        shutdown_ev = self.server.shutdown_event

        if poll:
            deadline = time.time() + 10
            while time.time() < deadline and not shutdown_ev.is_set():
                data = self.server.manager.get_job_status(crawler_id)
                if data is None:
                    break
                if len(data.get("logs", [])) > last_count:
                    break
                if data.get("status") not in ("running", "pending", "paused"):
                    break
                shutdown_ev.wait(0.5)

        data = self.server.manager.get_job_status(crawler_id)
        if data is None:
            self._respond_json({"error": "not found"}, 404)
        else:
            self._respond_json(data)

    def _api_search(self, qs: dict) -> None:
        q = qs.get("q", [""])[0]
        page = int(qs.get("page", ["1"])[0])
        pp = int(qs.get("per_page", ["10"])[0])
        self._respond_json(self.server.searcher.search(q, page=page, per_page=pp))

    # -- suppress noisy per-request logs ------------------------------------

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.debug(fmt, *args)


def start_server(
    manager: "CrawlerManager",
    searcher: "Searcher",
    host: str = "0.0.0.0",
    port: int = 8080,
) -> None:
    """Start the HTTP server and block until interrupted.

    Uses ``serve_forever()``'s own shutdown mechanism (triggered via
    ``signal.SIGINT``) so we don't fight the kqueue/epoll selector on
    macOS.  A second Ctrl+C force-exits.
    """
    import signal

    server = CrawlerHTTPServer((host, port), CrawlerHandler, manager, searcher)
    logger.info("Server running on http://%s:%d", host, port)
    print(f"  Server running on http://localhost:{port}")
    print("  Press Ctrl+C to stop.\n")

    def _request_shutdown(signum: int, frame: object) -> None:
        signal.signal(signal.SIGINT, _force_exit)
        server.shutdown_event.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    def _force_exit(signum: int, frame: object) -> None:
        print("\nForce quit.")
        os._exit(1)

    signal.signal(signal.SIGINT, _request_shutdown)
    try:
        server.serve_forever()
    finally:
        server.shutdown_event.set()
        server.shutdown()
