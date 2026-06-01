"""
normalizer.py
─────────────
Pure-function HTML → plain-text converter.

Designed to be called inside a ProcessPoolExecutor worker:
  • No global state, no I/O, fully picklable.
  • Uses only stdlib (html.parser) — no extra deps.

What it does
────────────
1. Unescape HTML entities (handles double-escaped &amp;amp; etc.)
2. Strip all tags; keep readable text with structure:
     block tags  → newline boundaries
     <li>        → bullet "• "
     <script/style> subtrees → dropped entirely
3. Extract image URLs embedded inside <img src="…"> tags.
4. Collapse whitespace / blank lines.
"""

from __future__ import annotations

import html
import re
from html.parser import HTMLParser
from typing import NamedTuple

_BLOCK_TAGS = {
    "p", "div", "br", "h1", "h2", "h3", "h4", "h5", "h6",
    "ul", "ol", "li", "tr", "td", "th", "blockquote", "pre",
    "section", "article", "header", "footer",
}
_SKIP_TAGS = {"script", "style", "noscript", "iframe", "object", "embed"}


class _DescriptionParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._image_urls: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list) -> None:
        tag = tag.lower()
        if tag in _SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if tag == "li":
            self._parts.append("\n• ")
        elif tag in _BLOCK_TAGS:
            self._parts.append("\n")
        if tag == "img":
            attr_map = {k.lower(): v for k, v in attrs}
            src = (attr_map.get("src") or "").strip()
            if src:
                self._image_urls.append(src)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in _SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
            return
        if self._skip_depth:
            return
        if tag in _BLOCK_TAGS:
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip_depth:
            self._parts.append(data)

    @property
    def text(self) -> str:
        return "".join(self._parts)

    @property
    def image_urls(self) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for url in self._image_urls:
            if url not in seen:
                seen.add(url)
                out.append(url)
        return out


class NormalizedDescription(NamedTuple):
    text: str
    images_in_desc: list[str]


def normalize_description(raw: str | None) -> NormalizedDescription:
    if not raw:
        return NormalizedDescription(text="", images_in_desc=[])

    parser = _DescriptionParser()
    parser.feed(html.unescape(raw))

    text = parser.text
    text = text.replace("\xa0", " ").replace("\u200b", "")

    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]

    cleaned: list[str] = []
    blank_run = 0
    for line in lines:
        if line == "":
            blank_run += 1
            if blank_run <= 1:
                cleaned.append(line)
        else:
            blank_run = 0
            cleaned.append(line)

    return NormalizedDescription(
        text="\n".join(cleaned).strip(),
        images_in_desc=parser.image_urls,
    )
