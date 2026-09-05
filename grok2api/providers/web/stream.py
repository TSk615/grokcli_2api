"""Minimal incremental parser for evidenced Grok Web JSON/SSE text frames."""

from __future__ import annotations

import codecs
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


class WebDeltaKind(str, Enum):
    TEXT = "text"
    REASONING = "reasoning"
    CITATION = "citation"


@dataclass(frozen=True, slots=True)
class WebCitation:
    url: str
    title: str = ""


@dataclass(frozen=True, slots=True)
class WebDelta:
    kind: WebDeltaKind
    text: str = ""
    citation: WebCitation | None = None


class WebStreamError(RuntimeError):
    """Sanitized stream error that never includes an upstream body."""


_SENSITIVE_QUERY_KEYS = {
    "access_token",
    "authorization",
    "cf_clearance",
    "cookie",
    "sso",
    "sso-rw",
    "token",
}


def _safe_url(raw: Any) -> str | None:
    if not isinstance(raw, str) or len(raw) > 8192:
        return None
    try:
        parsed = urlsplit(raw.strip())
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            return None
        if parsed.username is not None or parsed.password is not None:
            return None
        query = urlencode(
            [
                (key, value)
                for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if key.lower() not in _SENSITIVE_QUERY_KEYS
            ],
            doseq=True,
        )
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))
    except (TypeError, ValueError):
        return None


class GrokWebStreamParser:
    """Extract complete JSON objects from SSE or concatenated JSON chunks.

    Unknown envelopes and event types are ignored. The scanner is string-aware,
    supports arbitrary network chunk boundaries, and bounds individual frames.
    """

    def __init__(self, *, max_frame_chars: int = 8 << 20) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")("strict")
        self._max_frame_chars = max(1024, int(max_frame_chars))
        self._frame: list[str] = []
        self._depth = 0
        self._in_string = False
        self._escaped = False
        self._text_so_far = ""
        self._seen_citations: set[str] = set()

    def feed(self, chunk: bytes | str) -> list[WebDelta]:
        if isinstance(chunk, bytes):
            try:
                text = self._decoder.decode(chunk)
            except UnicodeDecodeError as exc:
                raise WebStreamError("Grok Web stream is not valid UTF-8") from exc
        elif isinstance(chunk, str):
            text = chunk
        else:
            raise TypeError("stream chunk must be bytes or text")
        return self._scan(text)

    def finish(self) -> list[WebDelta]:
        try:
            tail = self._decoder.decode(b"", final=True)
        except UnicodeDecodeError as exc:
            raise WebStreamError("Grok Web stream ended with invalid UTF-8") from exc
        deltas = self._scan(tail)
        if self._depth:
            raise WebStreamError("Grok Web stream ended with an incomplete JSON frame")
        return deltas

    def _scan(self, text: str) -> list[WebDelta]:
        output: list[WebDelta] = []
        for char in text:
            if self._depth == 0:
                if char != "{":
                    continue
                self._frame = [char]
                self._depth = 1
                self._in_string = False
                self._escaped = False
                continue

            self._frame.append(char)
            if len(self._frame) > self._max_frame_chars:
                self._reset_frame()
                raise WebStreamError("Grok Web response frame exceeds the safety limit")
            if self._in_string:
                if self._escaped:
                    self._escaped = False
                elif char == "\\":
                    self._escaped = True
                elif char == '"':
                    self._in_string = False
                continue
            if char == '"':
                self._in_string = True
            elif char == "{":
                self._depth += 1
            elif char == "}":
                self._depth -= 1
                if self._depth == 0:
                    raw = "".join(self._frame)
                    self._reset_frame()
                    try:
                        root = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(root, Mapping):
                        output.extend(self._parse_root(root))
        return output

    def _reset_frame(self) -> None:
        self._frame = []
        self._depth = 0
        self._in_string = False
        self._escaped = False

    def _parse_root(self, root: Mapping[str, Any]) -> list[WebDelta]:
        event = root.get("event")
        if isinstance(event, Mapping):
            return self._parse_gateway_event(event)
        if isinstance(root.get("error"), Mapping):
            raise WebStreamError("Grok Web returned an upstream error event")
        result = root.get("result")
        if not isinstance(result, Mapping):
            return []
        response = result.get("response")
        if not isinstance(response, Mapping):
            return []
        if isinstance(response.get("error"), Mapping):
            raise WebStreamError("Grok Web returned an upstream error event")
        return self._parse_legacy_response(response)

    def _parse_gateway_event(self, event: Mapping[str, Any]) -> list[WebDelta]:
        event_type = event.get("type")
        if event_type == "error":
            raise WebStreamError("Grok Web returned an upstream error event")
        if event_type == "response.output_text.delta":
            return self._text_delta(event.get("delta"))
        if event_type == "response.output_text.done" and not self._text_so_far:
            return self._text_delta(event.get("text"))
        if event_type == "response.search.result":
            return self._citation_delta(event.get("result"))
        if event_type != "response.chunk":
            return []
        chunk = event.get("chunk")
        if not isinstance(chunk, Mapping):
            return []
        citation = chunk.get("render_citation")
        if isinstance(citation, Mapping):
            return self._citation_delta(citation)
        text = chunk.get("text")
        if not isinstance(text, Mapping):
            return []
        delta = text.get("text")
        channel = str(text.get("channel") or "").strip().upper()
        if "ANALYSIS" in channel or "REASONING" in channel:
            return self._reasoning_delta(delta)
        if channel in ("", "CHANNEL_ASSISTANT_RESPONSE"):
            return self._text_delta(delta)
        return []

    def _parse_legacy_response(self, response: Mapping[str, Any]) -> list[WebDelta]:
        output = self._legacy_citations(response)
        token = response.get("token")
        tag = str(response.get("messageTag") or "")
        if tag == "tool_usage_card":
            return output
        if response.get("isThinking") is True:
            output.extend(self._reasoning_delta(token))
            return output
        if tag in ("", "final"):
            output.extend(self._text_delta(token))
        model_response = response.get("modelResponse")
        if isinstance(model_response, Mapping):
            output.extend(self._legacy_citations(model_response))
            message = model_response.get("message")
            if isinstance(message, str) and message.startswith(self._text_so_far):
                output.extend(self._text_delta(message[len(self._text_so_far) :]))
        return output

    def _legacy_citations(self, response: Mapping[str, Any]) -> list[WebDelta]:
        output: list[WebDelta] = []
        for key in ("webSearchResults", "citedWebSearchResults"):
            value = response.get(key)
            if isinstance(value, Mapping):
                value = value.get("results")
            if not isinstance(value, list):
                continue
            for item in value:
                output.extend(self._citation_delta(item))
        for key in ("xSearchResults", "xposts", "citedXposts"):
            value = response.get(key)
            if isinstance(value, Mapping):
                value = value.get("results")
            if not isinstance(value, list):
                continue
            for item in value:
                if not isinstance(item, Mapping):
                    continue
                username = item.get("username")
                post_id = item.get("postId")
                if isinstance(username, str) and isinstance(post_id, str):
                    output.extend(
                        self._citation_delta(
                            {
                                "url": f"https://x.com/{username}/status/{post_id}",
                                "title": item.get("text"),
                            }
                        )
                    )
        return output

    def _text_delta(self, value: Any) -> list[WebDelta]:
        if not isinstance(value, str) or not value:
            return []
        self._text_so_far += value
        return [WebDelta(WebDeltaKind.TEXT, text=value)]

    @staticmethod
    def _reasoning_delta(value: Any) -> list[WebDelta]:
        if not isinstance(value, str) or not value:
            return []
        return [WebDelta(WebDeltaKind.REASONING, text=value)]

    def _citation_delta(self, value: Any) -> list[WebDelta]:
        if not isinstance(value, Mapping):
            return []
        url = _safe_url(value.get("url"))
        if not url or url in self._seen_citations:
            return []
        self._seen_citations.add(url)
        title = value.get("title")
        return [
            WebDelta(
                WebDeltaKind.CITATION,
                citation=WebCitation(url=url, title=title if isinstance(title, str) else ""),
            )
        ]
