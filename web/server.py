"""Web server for Google in a Day.

Pages:
  /              – Crawler page (create jobs, view history, statistics)
  /status/<id>   – Crawler status (long-polling, pause/resume/stop controls)
  /search        – Search (Google-style home + SERP; ``I'm Feeling Lucky`` → first hit)

JSON API:
  /api/status/<id>       – status with optional long-polling
  /api/search            – paginated search results
  /api/stats             – aggregate crawler statistics
  /api/crawler-dashboard – JSON ``{stats, jobs}`` for live Crawler page updates
  POST /stop/<id>        – stop a running crawler
  POST /pause/<id>       – pause a running crawler
  POST /resume/<id>      – resume a paused crawler
  POST /resume-disk/<id> – resume an interrupted crawler from ``{id}.queue``
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
/* --- Google-like search UI --- */
.search-app{background:#fff;min-height:100vh}
.search-app--home .search-app__main{max-width:100%;padding:0 24px}
.search-app__topnav{display:flex;justify-content:flex-end;align-items:center;padding:12px 24px;font-size:.9rem;gap:20px;border-bottom:1px solid transparent}
.search-app--serp .search-app__topnav,.search-app--crawler .search-app__topnav,.search-app--status .search-app__topnav{border-color:#ebebeb}
.search-app__topnav a{color:#202124}
.search-app__hero{display:flex;flex-direction:column;align-items:center;padding:12vh 16px 48px;text-align:center}
.search-app__logo{font-size:5.5rem;font-weight:400;letter-spacing:-2px;line-height:1.15;margin-bottom:8px;font-family:Product Sans,Segoe UI,Roboto,sans-serif}
.search-app__logo .c1{color:#4285F4}.search-app__logo .c2{color:#EA4335}.search-app__logo .c3{color:#FBBC05}
.search-app__logo .c4{color:#4285F4}.search-app__logo .c5{color:#34A853}.search-app__logo .c6{color:#EA4335}
.search-app__tagline{color:#70757a;font-size:1rem;font-weight:400;margin-bottom:36px}
.search-app__form-wrap{width:100%;max-width:584px}
.search-app__field{display:flex;align-items:center;border:1px solid #dfe1e5;border-radius:24px;padding:4px 16px;margin-bottom:24px;background:#fff;transition:box-shadow .2s}
.search-app__field:hover,.search-app__field:focus-within{box-shadow:0 1px 6px rgba(32,33,36,.28);border-color:rgba(223,225,229,0)}
.search-app__field input{flex:1;border:none;outline:none;font-size:1rem;padding:10px 8px;background:transparent}
.search-app__actions{display:flex;flex-wrap:wrap;gap:12px;justify-content:center}
.search-app__btn{background:#f8f9fa;border:1px solid #f8f9fa;border-radius:4px;color:#3c4043;font-size:.875rem;padding:11px 20px;cursor:pointer}
.search-app__btn:hover{box-shadow:0 1px 1px rgba(0,0,0,.1);border-color:#dadce0}
.search-app__btn--primary{background:#1a73e8;color:#fff;border-color:#1a73e8}
.search-app__btn--primary:hover{background:#1557b0;box-shadow:none}
.search-app__serp-head{display:flex;align-items:center;gap:20px;flex-wrap:wrap;padding:16px 24px;border-bottom:1px solid #ebebeb;max-width:1100px;margin:0 auto}
.search-app__serp-logo{font-size:1.75rem;font-weight:400;letter-spacing:-1px;white-space:nowrap}
.search-app__serp-logo .c1{color:#4285F4}.search-app__serp-logo .c2{color:#EA4335}.search-app__serp-logo .c3{color:#FBBC05}
.search-app__serp-logo .c4{color:#4285F4}.search-app__serp-logo .c5{color:#34A853}.search-app__serp-logo .c6{color:#EA4335}
.search-app__serp-form{flex:1;min-width:200px;max-width:692px}
.search-app__serp-form .search-app__field{margin-bottom:0}
.search-app__serp-tools{max-width:1100px;margin:0 auto;padding:8px 24px 16px;color:#70757a;font-size:.85rem}
.search-app__serp-tools a{color:#1a73e8;margin-right:12px}
.search-app__results{max-width:1100px;margin:0 auto;padding:0 24px 48px}
.g-result{padding:12px 0;max-width:652px}
.g-result__show{color:#202124;font-size:1.15rem;line-height:1.3;margin-bottom:4px}
.g-result__show a{color:#1a0dab;text-decoration:none}
.g-result__show a:hover{text-decoration:underline}
.g-result__url{font-size:.875rem;color:#006621;line-height:1.3;word-break:break-all}
.g-result__meta{font-size:.8rem;color:#70757a;margin-top:6px}
.search-app__hint{color:#d93025;font-size:.9rem;margin-top:8px}
/* --- Crawler dashboard (matches app shell) --- */
.search-app--crawler,.search-app--status{background:#fff}
.search-app--crawler .search-app__main,.search-app--status .search-app__main{max-width:960px;margin:0 auto;padding:16px 24px 40px}
.crawler-brand{display:flex;align-items:center;gap:14px;margin-bottom:8px;flex-wrap:wrap}
.crawler-brand__title{font-size:1.35rem;color:#202124;font-weight:400}
.crawler-hero{padding-bottom:20px;margin-bottom:20px;border-bottom:1px solid #ebebeb}
.crawler-hero h1{font-size:1.75rem;color:#202124;font-weight:400;margin:12px 0 4px}
.crawler-hero p{color:#70757a;font-size:.95rem;margin:0}
.crawler-stat-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(148px,1fr));gap:12px;margin-bottom:28px}
.crawler-stat-card{background:#fff;border:1px solid #ebebeb;border-radius:8px;padding:14px 16px;box-shadow:0 1px 2px rgba(60,64,67,.08)}
.crawler-stat-card .v{font-size:1.45rem;color:#1a73e8;font-weight:500;line-height:1.2}
.crawler-stat-card .l{font-size:.72rem;color:#70757a;text-transform:uppercase;letter-spacing:.5px;margin-top:4px}
.crawler-live{display:inline-flex;align-items:center;gap:8px;font-size:.8rem;color:#5f6368;margin-left:10px;vertical-align:middle}
.crawler-live i{display:inline-block;width:8px;height:8px;border-radius:50%;background:#34a853;
animation:livepulse 2s ease-in-out infinite}
@keyframes livepulse{0%,100%{opacity:1}50%{opacity:.35}}
.crawler-section{margin-top:28px}
.crawler-section h2{font-size:1.15rem;color:#202124;font-weight:500;margin-bottom:14px}
.crawler-form-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;max-width:720px}
@media(max-width:640px){.crawler-form-grid{grid-template-columns:1fr}}
.crawler-form .fg{margin-bottom:0}
.crawler-form label{display:block;font-size:.8rem;font-weight:500;color:#5f6368;margin-bottom:6px}
.crawler-form .search-app__field{margin-bottom:0}
.crawler-form input[type=number],.crawler-form input[type=url]{width:100%}
.crawler-actions{margin-top:18px;display:flex;flex-wrap:wrap;gap:10px;align-items:center}
.crawler-table-wrap{background:#fff;border:1px solid #ebebeb;border-radius:8px;overflow:hidden}
.crawler-table-wrap table{width:100%;border-collapse:collapse;margin:0}
.crawler-table-wrap th{background:#f8f9fa;text-align:left;padding:12px 14px;font-size:.78rem;text-transform:uppercase;color:#5f6368;font-weight:600;letter-spacing:.3px;border-bottom:1px solid #ebebeb}
.crawler-table-wrap td{padding:12px 14px;border-top:1px solid #f0f0f0;font-size:.9rem}
.crawler-table-wrap tr:hover td{background:#fafcff}
.crawler-table-wrap .badge{font-size:.75rem}
.status-page-toolbar{display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px;margin-bottom:16px}
"""

# ---------------------------------------------------------------------------
# Layout helper (shared shell: same top nav as Search)
# ---------------------------------------------------------------------------

def _layout_app(
    title: str,
    body: str,
    active: str = "/",
    *,
    skin: str = "home",
) -> str:
    """Shell with top-right Crawler | Search links.  *skin*: home | serp | crawler | status."""
    nav_items = [("/", "Crawler"), ("/search", "Search")]
    nav_links = ""
    for href, label in nav_items:
        bold = ' style="color:#1557b0;text-decoration:underline"' if active == href else ""
        nav_links += f'<a href="{href}"{bold}>{label}</a>\n'
    skin_css = {
        "home": "search-app--home",
        "serp": "search-app--serp",
        "crawler": "search-app--crawler",
        "status": "search-app--status",
    }.get(skin, "search-app--home")
    return (
        '<!DOCTYPE html><html lang="en"><head>'
        '<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{html_mod.escape(title)}</title><style>{_CSS}</style></head>"
        f'<body class="search-app {skin_css}">'
        f'<header class="search-app__topnav">{nav_links}</header>'
        f'<main class="search-app__main">{body}</main>'
        "</body></html>"
    )


def _google_logo_html(*, compact: bool = False) -> str:
    """Colorful 'Google' wordmark + optional 'in a Day' subtitle."""
    cls = "search-app__serp-logo" if compact else "search-app__logo"
    parts = [
        ("G", "c1"), ("o", "c2"), ("o", "c3"), ("g", "c4"), ("l", "c5"), ("e", "c6"),
    ]
    spans = "".join(
        f'<span class="{c}">{html_mod.escape(ch)}</span>' for ch, c in parts
    )
    if compact:
        return f'<a href="/search" class="{cls}" style="text-decoration:none">{spans}</a>'
    tail = ' <span style="color:#5f6368;font-weight:400;font-size:2.2rem">in a Day</span>'
    return f'<div class="{cls}">{spans}{tail}</div>'


# ---------------------------------------------------------------------------
# Page builders
# ---------------------------------------------------------------------------

def _crawler_job_rows_html(jobs: list[dict]) -> str:
    rows = ""
    for j in jobs:
        cid = html_mod.escape(str(j.get("id", "")))
        cid_raw = str(j.get("id", ""))
        qid = parse.quote(cid_raw, safe="")
        origin = html_mod.escape(str(j.get("origin_url", "")))
        status = j.get("status", "unknown")
        pages = j.get("pages_processed", 0)
        ts = j.get("created_at", 0)
        created = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else "-"
        if status == "interrupted":
            actions = (
                f'<form method="POST" action="/resume-disk/{qid}" style="display:inline;margin:0">'
                '<button type="submit" class="search-app__btn" style="font-size:12px;padding:4px 10px">'
                "Resume from disk</button></form>"
            )
        else:
            actions = "—"
        rows += (
            f'<tr data-cid="{html_mod.escape(cid_raw)}"><td><a href="/status/{cid}">{cid}</a></td>'
            f'<td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{origin}</td>'
            f'<td><span class="badge badge-{status}">{html_mod.escape(str(status))}</span></td>'
            f"<td>{pages}</td><td>{created}</td><td>{actions}</td></tr>\n"
        )
    if not rows:
        rows = '<tr><td colspan="6" class="empty-state">No crawl jobs yet.</td></tr>'
    return rows


def _crawler_page(jobs: list[dict], stats: dict | None = None) -> str:
    stats = stats or {}
    tc = stats.get("total_crawlers", 0)
    ac = stats.get("active_crawlers", 0)
    vu = stats.get("total_visited_urls", 0)
    pp = stats.get("total_pages_processed", 0)
    wi = stats.get("total_words_indexed", 0)

    clear_btn = ""
    if jobs:
        clear_btn = (
            '<form method="POST" action="/clear" style="display:inline">'
            '<button type="submit" class="search-app__btn" style="border-color:#dadce0;color:#c5221f" '
            'onclick="return confirm(\'Clear all crawler data and index?\')">Clear all data</button></form>'
        )

    rows = _crawler_job_rows_html(jobs)

    body = (
        '<div class="crawler-brand">'
        f"{_google_logo_html(compact=True)}"
        '<span class="crawler-brand__title">Crawler</span>'
        '<span class="crawler-live"><i></i> Live</span></div>'
        '<div class="crawler-hero">'
        "<h1>Start a crawl</h1>"
        "<p>Add a seed URL and limits. Open a job for <strong>Pause</strong> and <strong>Stop</strong>."
        " After <strong>Stop</strong> (interrupted), <strong>Resume from disk</strong> is available on that "
        "job&rsquo;s status page and in the Jobs table below.</p></div>"
        '<div class="crawler-stat-grid" id="dash-stats">'
        f'<div class="crawler-stat-card"><div class="v" id="dash-stat-tc">{tc}</div>'
        '<div class="l">Total jobs</div></div>'
        f'<div class="crawler-stat-card"><div class="v" id="dash-stat-ac">{ac}</div>'
        '<div class="l">Active</div></div>'
        f'<div class="crawler-stat-card"><div class="v" id="dash-stat-vu">{vu}</div>'
        '<div class="l">Visited URLs</div></div>'
        f'<div class="crawler-stat-card"><div class="v" id="dash-stat-pp">{pp}</div>'
        '<div class="l">Pages processed</div></div>'
        f'<div class="crawler-stat-card"><div class="v" id="dash-stat-wi">{wi}</div>'
        '<div class="l">Words indexed</div></div>'
        "</div>"
        '<section class="crawler-section"><h2>New crawl</h2>'
        '<form method="POST" action="/" class="crawler-form">'
        '<div class="crawler-form-grid">'
        '<div class="fg" style="grid-column:1/-1">'
        "<label for=\"origin_url\">Origin URL</label>"
        '<div class="search-app__field"><input id="origin_url" name="origin_url" type="url" '
        'value="https://example.com" required autocomplete="off"></div></div>'
        '<div class="fg"><label for="max_depth">Max depth</label>'
        '<div class="search-app__field"><input id="max_depth" name="max_depth" type="number" '
        'value="2" min="0" max="10"></div></div>'
        '<div class="fg"><label for="max_workers">Workers</label>'
        '<div class="search-app__field"><input id="max_workers" name="max_workers" type="number" '
        'value="4" min="1" max="32"></div></div>'
        '<div class="fg"><label for="queue_capacity">Queue capacity</label>'
        '<div class="search-app__field"><input id="queue_capacity" name="queue_capacity" '
        'type="number" value="1000" min="1"></div></div>'
        '<div class="fg"><label for="max_pages">Max pages</label>'
        '<div class="search-app__field"><input id="max_pages" name="max_pages" type="number" '
        'value="500" min="1"></div></div>'
        '<div class="fg"><label for="hit_rate">Hit rate (req/s, 0 = unlimited)</label>'
        '<div class="search-app__field"><input id="hit_rate" name="hit_rate" type="number" '
        'value="0" min="0" max="1000" step="0.1"></div></div>'
        "</div>"
        '<div class="crawler-actions">'
        '<button type="submit" class="search-app__btn search-app__btn--primary">Start crawl</button>'
        "</div></form></section>"
        '<section class="crawler-section">'
        f'<div class="status-page-toolbar"><h2 style="margin:0">Jobs</h2>{clear_btn}</div>'
        '<div class="crawler-table-wrap"><table><thead><tr>'
        "<th>Crawler ID</th><th>Origin</th><th>Status</th><th>Pages</th><th>Created</th><th>Actions</th>"
        f'</tr></thead><tbody id="dash-jobs-tbody">{rows}</tbody></table></div></section>'
        "<script>"
        "(function(){"
        "function fmt(ts){if(!ts)return'-';var d=new Date(ts*1000);"
        "return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+"
        "String(d.getDate()).padStart(2,'0')+' '+String(d.getHours()).padStart(2,'0')+':'+"
        "String(d.getMinutes()).padStart(2,'0')+':'+String(d.getSeconds()).padStart(2,'0');}"
        "function row(j){"
        "var tr=document.createElement('tr');"
        "var id=String(j.id||'');"
        "var td=document.createElement('td');"
        "var a=document.createElement('a');a.href='/status/'+encodeURIComponent(id);a.textContent=id;td.appendChild(a);tr.appendChild(td);"
        "td=document.createElement('td');td.style.maxWidth='220px';td.style.overflow='hidden';"
        "td.style.textOverflow='ellipsis';td.style.whiteSpace='nowrap';td.textContent=j.origin_url||'';tr.appendChild(td);"
        "td=document.createElement('td');var sp=document.createElement('span');"
        "var st=j.status||'unknown';sp.className='badge badge-'+st;sp.textContent=st;td.appendChild(sp);tr.appendChild(td);"
        "td=document.createElement('td');td.textContent=String(j.pages_processed!=null?j.pages_processed:0);tr.appendChild(td);"
        "td=document.createElement('td');td.textContent=fmt(j.created_at);tr.appendChild(td);"
        "td=document.createElement('td');"
        "if((j.status||'')==='interrupted'){"
        "var b=document.createElement('button');b.type='button';b.className='search-app__btn';"
        "b.style.fontSize='12px';b.style.padding='4px 10px';b.textContent='Resume from disk';"
        "b.onclick=function(){fetch('/resume-disk/'+encodeURIComponent(id),{method:'POST'})"
        ".then(function(){location.href='/status/'+encodeURIComponent(id);});};"
        "td.appendChild(b);}else{td.textContent='—';}"
        "tr.appendChild(td);"
        "return tr;}"
        "function render(jobs){"
        "var tb=document.getElementById('dash-jobs-tbody');tb.innerHTML='';"
        "if(!jobs||!jobs.length){"
        "var er=document.createElement('tr');var ec=document.createElement('td');"
        "ec.colSpan=6;ec.className='empty-state';ec.textContent='No crawl jobs yet.';"
        "er.appendChild(ec);tb.appendChild(er);return;}"
        "jobs.forEach(function(j){tb.appendChild(row(j));});}"
        "async function tick(){"
        "try{var r=await fetch('/api/crawler-dashboard');if(!r.ok)return;"
        "var d=await r.json();var s=d.stats||{};"
        "document.getElementById('dash-stat-tc').textContent=s.total_crawlers||0;"
        "document.getElementById('dash-stat-ac').textContent=s.active_crawlers||0;"
        "document.getElementById('dash-stat-vu').textContent=s.total_visited_urls||0;"
        "document.getElementById('dash-stat-pp').textContent=s.total_pages_processed||0;"
        "document.getElementById('dash-stat-wi').textContent=s.total_words_indexed||0;"
        "render(d.jobs||[]);}catch(e){}}"
        "setInterval(tick,2000);tick();"
        "})();"
        "</script>"
    )
    return _layout_app("Crawler — Google in a Day", body, active="/", skin="crawler")


def _status_page(crawler_id: str, data: dict) -> str:
    cid = html_mod.escape(crawler_id)
    status = data.get("status", "unknown")
    origin = html_mod.escape(str(data.get("origin_url", "")))
    log_count = len(data.get("logs", []))

    pq = int(data.get("persisted_queue_count") or 0)
    preview_rows = data.get("queue_preview") or []
    preview_html = ""
    if pq or preview_rows:
        lines = ""
        for row in preview_rows[:40]:
            u = html_mod.escape(str(row.get("url", "")))
            du = html_mod.escape(str(row.get("depth", "")))
            lines += f"<li><code>{u}</code> &mdash; depth {du}</li>"
        more = ""
        if pq > len(preview_rows):
            more = (
                f'<p class="subtitle">{pq} total in snapshot '
                f"(showing first {len(preview_rows)})</p>"
            )
        preview_html = (
            f"<h2>Pending URLs (disk snapshot)</h2>{more}<ul>{lines}</ul>"
        )

    logs_html = ""
    for log in data.get("logs", []):
        ts = time.strftime("%H:%M:%S", time.localtime(log.get("timestamp", 0)))
        msg = html_mod.escape(str(log.get("message", "")))
        logs_html += f'<div class="log-entry"><span class="log-ts">[{ts}]</span> {msg}</div>\n'

    body = (
        f'<h2>Crawler Status &mdash; <code>{cid}</code></h2>'
        f'<p>Origin: <a href="{origin}" target="_blank">{origin}</a>'
        f' &nbsp;|&nbsp; Depth: {data.get("max_depth", "?")}'
        f' &nbsp;|&nbsp; Workers: {data.get("max_workers", "?")}'
        f' &nbsp;|&nbsp; <span class="badge badge-{status}" id="status-badge">{status}</span></p>'
        f"{preview_html}"
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
        "function formatLogTime(sec){var d=new Date(sec*1000);"
        "return String(d.getHours()).padStart(2,'0')+':'+"
        "String(d.getMinutes()).padStart(2,'0')+':'+String(d.getSeconds()).padStart(2,'0');}"
        "function sendCmd(action){"
        'fetch("/"+action+"/"+cid,{method:"POST"}).then(function(){});'
        "}"
        "function resumeDisk(){"
        'fetch("/resume-disk/"+cid,{method:"POST"}).then(function(){location.reload()});'
        "}"
        "function renderControls(s){"
        "if(s==='interrupted'){"
        """ctrl.innerHTML='<button class="btn-resume" onclick="resumeDisk()">Resume from disk</button>';"""
        "return;}"
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
        'var ts=formatLogTime(logs[i].timestamp);'
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
    return _layout_app(f"Status — {cid}", body, active="/", skin="status")


def _search_page(
    query: str,
    results: dict,
    *,
    lucky_miss: bool = False,
) -> str:
    q_esc = html_mod.escape(query)
    per_page = results.get("per_page", 10)
    sort_cur = results.get("sort_by", "relevance")
    if sort_cur not in ("relevance", "frequency", "depth"):
        sort_cur = "relevance"
    q_enc = parse.quote(query)
    sort_esc = html_mod.escape(sort_cur)

    results_html = ""
    if query:
        for r in results.get("results", []):
            raw_url = str(r.get("url", ""))
            url = html_mod.escape(raw_url)
            origin = html_mod.escape(str(r.get("origin_url", "")))
            depth = r.get("depth", "?")
            freq = r.get("total_frequency", 0)
            rel = r.get("relevance_score", 0)
            results_html += (
                '<div class="g-result">'
                f'<div class="g-result__url">{url}</div>'
                f'<div class="g-result__show"><a href="{url}" target="_blank" rel="noopener">{url}</a></div>'
                f'<div class="g-result__meta">Origin: {origin} &middot; depth {depth} &middot; '
                f"freq {freq} &middot; score {rel}</div></div>\n"
            )
        if not results.get("results"):
            results_html = (
                '<p class="empty-state" style="text-align:left;max-width:652px;padding:24px 0">'
                "No results found.</p>"
            )

    total = results.get("total", 0)
    total_pages = results.get("total_pages", 0)
    current = results.get("page", 1)
    pag = ""
    if query and total_pages > 1:
        pag = '<div class="pagination" style="margin-top:24px">'
        for p in range(1, total_pages + 1):
            cls = ' class="active"' if p == current else ""
            pag += (
                f'<a href="/search?q={q_enc}&page={p}&per_page={per_page}'
                f'&sort={sort_cur}"{cls}>{p}</a>'
            )
        pag += "</div>"

    sort_links = ""
    if query:
        for label, key in (
            ("Relevance", "relevance"),
            ("Frequency", "frequency"),
            ("Depth", "depth"),
        ):
            bold = ' style="font-weight:600;color:#1557b0"' if sort_cur == key else ""
            sort_links += (
                f'<a href="/search?q={q_enc}&per_page={per_page}&sort={key}"{bold}>{label}</a> '
            )

    form_buttons = (
        '<div class="search-app__actions">'
        '<button type="submit" name="btn" value="search" '
        'class="search-app__btn search-app__btn--primary">Google Search</button>'
        '<button type="submit" name="btn" value="lucky" class="search-app__btn">'
        "I'm Feeling Lucky</button>"
        "</div>"
        f'<input type="hidden" name="sort" value="{sort_esc}">'
        f'<input type="hidden" name="per_page" value="{per_page}">'
    )

    if not query:
        body = (
            '<div class="search-app__hero">'
            f"{_google_logo_html(compact=False)}"
            '<p class="search-app__tagline">Search pages indexed by your crawler</p>'
            '<div class="search-app__form-wrap">'
            '<form method="GET" action="/search">'
            '<div class="search-app__field">'
            '<input name="q" type="search" value="" autocomplete="off" '
            'placeholder="Search the index" autofocus aria-label="Search"></div>'
            f"{form_buttons}"
            "</form></div></div>"
        )
        return _layout_app("Google in a Day — Search", body, active="/search", skin="home")

    lucky_note = ""
    if lucky_miss:
        lucky_note = (
            '<p class="search-app__hint">No results to open — showing the results page instead. '
            "Try another query.</p>"
        )

    body = (
        '<div class="search-app__serp-head">'
        f"{_google_logo_html(compact=True)}"
        '<div style="flex:1;min-width:240px">'
        '<form method="GET" action="/search">'
        '<div class="search-app__field">'
        f'<input name="q" type="search" value="{q_esc}" autocomplete="off" '
        'placeholder="Search" autofocus aria-label="Search"></div>'
        f"{form_buttons}"
        "</form></div></div>"
        f'<div class="search-app__serp-tools">About {total} results'
        f' &nbsp;&middot;&nbsp; Sort: {sort_links}</div>'
        f"{lucky_note}"
        f'<div class="search-app__results">{results_html}{pag}</div>'
    )
    return _layout_app("Search — Google in a Day", body, active="/search", skin="serp")


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
            q = qs.get("q", [""])[0].strip()
            page = int(qs.get("page", ["1"])[0])
            pp = int(qs.get("per_page", ["10"])[0])
            sort = qs.get("sort", ["relevance"])[0]
            if sort not in ("relevance", "frequency", "depth"):
                sort = "relevance"
            btn = qs.get("btn", [""])[0]
            wants_lucky = btn == "lucky" or qs.get("lucky", [""])[0] == "1"

            if q and wants_lucky:
                top = self.server.searcher.search(
                    q, page=1, per_page=1, sort_by="relevance",
                )
                row = (top.get("results") or [None])[0]
                if row:
                    dest = str(row.get("url", ""))
                    if parse.urlparse(dest).scheme in ("http", "https"):
                        self.send_response(302)
                        self.send_header("Location", dest)
                        self.end_headers()
                        return
                res = self.server.searcher.search(
                    q, page=page, per_page=pp, sort_by=sort,
                )
                self._respond_html(_search_page(q, res, lucky_miss=True))
                return

            res = (
                self.server.searcher.search(q, page=page, per_page=pp, sort_by=sort)
                if q
                else {
                    "results": [],
                    "total": 0,
                    "page": 1,
                    "per_page": pp,
                    "total_pages": 0,
                    "sort_by": sort,
                }
            )
            self._respond_html(_search_page(q, res))
        elif path.startswith("/api/status/"):
            cid = path[len("/api/status/"):]
            self._api_status(cid, qs)
        elif path == "/api/search":
            self._api_search(qs)
        elif path == "/api/stats":
            self._respond_json(self.server.manager.get_statistics())
        elif path == "/api/crawler-dashboard":
            m = self.server.manager
            self._respond_json({"stats": m.get_statistics(), "jobs": m.list_jobs()})
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
        elif path.startswith("/resume-disk/"):
            cid = parse.unquote(path[len("/resume-disk/") :])
            ok = self.server.manager.resume_job_from_disk(cid)
            if ok:
                loc = "/status/" + parse.quote(cid, safe="")
                self.send_response(303)
                self.send_header("Location", loc)
                self.end_headers()
            else:
                self._respond_json({"ok": False}, 400)
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
        sort = qs.get("sort", ["relevance"])[0]
        if sort not in ("relevance", "frequency", "depth"):
            sort = "relevance"
        self._respond_json(
            self.server.searcher.search(q, page=page, per_page=pp, sort_by=sort)
        )

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
