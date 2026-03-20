"""Safe limited rich-text helpers for task descriptions."""

from __future__ import annotations

import re
from html.parser import HTMLParser

from markupsafe import Markup, escape


_BOLD_STYLE_RE = re.compile(r"font-weight\s*:\s*(bold|[6-9]00)", re.IGNORECASE)
_UNDERLINE_STYLE_RE = re.compile(
    r"text-decoration(?:-line)?\s*:\s*[^;]*underline",
    re.IGNORECASE,
)

_ALLOWED_TAGS = {"b", "strong", "u", "ul", "ol", "li", "br", "p"}
_BLOCK_TAG_ALIASES = {"div": "p"}
_SELF_CLOSING_TAGS = {"br"}


def _normalize_newlines(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n")


class _RichTextSanitizer(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._stack: list[tuple[str, str]] = []

    def sanitize(self, value: str) -> str:
        self._parts = []
        self._stack = []
        self.feed(_normalize_newlines(value))
        self.close()
        while self._stack:
            _, output_tag = self._stack.pop()
            self._parts.append(f"</{output_tag}>")
        return "".join(self._parts).strip()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for source_tag, output_tag in self._map_start_tag(tag, attrs):
            if output_tag in _SELF_CLOSING_TAGS:
                self._parts.append(f"<{output_tag}>")
                continue
            self._parts.append(f"<{output_tag}>")
            self._stack.append((source_tag, output_tag))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        source_tag = (tag or "").lower()
        for index in range(len(self._stack) - 1, -1, -1):
            stacked_source_tag, _ = self._stack[index]
            if stacked_source_tag != source_tag:
                continue
            while len(self._stack) > index:
                _, output_tag = self._stack.pop()
                self._parts.append(f"</{output_tag}>")
            break

    def handle_data(self, data: str) -> None:
        if not data:
            return
        escaped = str(escape(_normalize_newlines(data)))
        self._parts.append(escaped.replace("\n", "<br>"))

    def _map_start_tag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> list[tuple[str, str]]:
        source_tag = (tag or "").lower()
        if source_tag in _BLOCK_TAG_ALIASES:
            return [(source_tag, _BLOCK_TAG_ALIASES[source_tag])]
        if source_tag in _ALLOWED_TAGS:
            return [(source_tag, source_tag)]
        if source_tag != "span":
            return []

        style = dict(attrs).get("style") or ""
        mapped_tags: list[tuple[str, str]] = []
        if _BOLD_STYLE_RE.search(style):
            mapped_tags.append((source_tag, "strong"))
        if _UNDERLINE_STYLE_RE.search(style):
            mapped_tags.append((source_tag, "u"))
        return mapped_tags


class _RichTextTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []

    def extract(self, value: str) -> str:
        self._parts = []
        self.feed(value)
        self.close()
        text = "".join(self._parts)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_tag = (tag or "").lower()
        if normalized_tag == "br":
            self._append_break()
        elif normalized_tag == "li":
            self._append_break()
            self._parts.append("• ")
        elif normalized_tag == "p":
            self._append_break()

    def handle_endtag(self, tag: str) -> None:
        if (tag or "").lower() in {"li", "p"}:
            self._append_break()

    def handle_data(self, data: str) -> None:
        if data:
            self._parts.append(data)

    def _append_break(self) -> None:
        if not self._parts or self._parts[-1].endswith("\n"):
            return
        self._parts.append("\n")


def sanitize_rich_text(value: str | None) -> str:
    raw_value = _normalize_newlines(value or "")
    if not raw_value.strip():
        return ""
    sanitized = _RichTextSanitizer().sanitize(raw_value)
    if not _RichTextTextExtractor().extract(sanitized):
        return ""
    return sanitized


def rich_text_visible_text(value: str | None) -> str:
    raw_value = value or ""
    if not raw_value:
        return ""
    sanitized = _RichTextSanitizer().sanitize(raw_value)
    if not sanitized:
        return ""
    return _RichTextTextExtractor().extract(sanitized)


def render_rich_text(value: str | None) -> Markup:
    return Markup(sanitize_rich_text(value))
