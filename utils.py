"""Shared text-processing utilities for crawl indexing and search."""

from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Optional, Tuple
from urllib import parse


def normalize_url(url: str) -> str:
    """Normalize a URL for consistent deduplication.

    Lowercases scheme and host, removes fragments, strips trailing slashes
    (except for root path), and preserves query strings.
    """
    parsed = parse.urlparse(url)
    scheme = (parsed.scheme or "http").lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return parse.urlunparse((scheme, netloc, path, "", parsed.query, ""))


def tokenize(text: str) -> list[str]:
    """Case-insensitive split on non-alphanumeric characters."""
    if not text:
        return []
    return [t for t in re.split(r"[^\w]+", text.lower()) if t]


def extract_title_and_content(html: str) -> Tuple[str, str]:
    """Return (title, body_text) extracted from raw HTML."""
    parser = _TitleAndBodyParser()
    try:
        parser.feed(html)
        parser.close()
    except Exception:
        pass
    return parser.title.strip(), parser.body_text


class _TitleAndBodyParser(HTMLParser):
    """Lightweight HTML parser that collects <title> and visible body text."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title = ""
        self._body_parts: list[str] = []
        self._in_script = False
        self._in_style = False
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        tag = tag.lower()
        if tag == "script":
            self._in_script = True
        elif tag == "style":
            self._in_style = True
        elif tag == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "script":
            self._in_script = False
        elif tag == "style":
            self._in_style = False
        elif tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_script or self._in_style:
            return
        if self._in_title:
            self.title += data
            return
        if data.strip():
            self._body_parts.append(data)

    @property
    def body_text(self) -> str:
        return " ".join(self._body_parts)
