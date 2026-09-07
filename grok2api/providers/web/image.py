"""Grok Web Imagine image protocol.

This module deliberately contains only the non-streaming generation path.  It
mirrors the upstream Imagine WebSocket messages while keeping account cookies
and the selected Resin-bound transport owned by :class:`GrokWebGateway`.
"""

from __future__ import annotations

import base64
import json
import time
import uuid
from dataclasses import dataclass
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit


MAX_IMAGES = 10
MAX_DOWNLOAD_BYTES = 32 << 20
IMAGE_TIMEOUT = 300.0
TRUSTED_IMAGE_HOSTS = frozenset({"assets.grok.com", "imagine-public.x.ai", "imgen.x.ai"})


class WebImageError(RuntimeError):
    """Sanitized image generation failure."""


class WebImageProtocolError(ValueError):
    """Invalid image request."""


@dataclass(frozen=True, slots=True)
class GeneratedImage:
    url: str = ""
    blob: str = ""
    mime_type: str = "image/jpeg"
    width: int | None = None
    height: int | None = None
    position: int | None = None

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {"url": self.url}
        if self.blob:
            value["b64_json"] = self.blob
        if self.mime_type:
            value["mime_type"] = self.mime_type
        if self.width:
            value["width"] = self.width
        if self.height:
            value["height"] = self.height
        return value


def resolve_aspect_ratio(aspect_ratio: Any = "", size: Any = "") -> str:
    aliases = {
        "auto": "auto", "1:1": "1:1", "16:9": "16:9", "9:16": "9:16",
        "4:3": "4:3", "3:4": "3:4", "3:2": "3:2", "2:3": "2:3",
        "2:1": "2:1", "1:2": "1:2", "19.5:9": "19.5:9", "9:19.5": "9:19.5",
        "20:9": "20:9", "9:20": "9:20", "1280x720": "16:9", "720x1280": "9:16",
        "1792x1024": "3:2", "1536x1024": "3:2", "1024x1792": "2:3",
        "1024x1536": "2:3", "1024x1024": "1:1",
    }
    value = str(aspect_ratio or "").strip().lower() or str(size or "").strip().lower()
    if not value:
        return "auto"
    try:
        return aliases[value]
    except KeyError:
        raise WebImageProtocolError("aspect_ratio is not supported") from None


def imagine_url(base_url: str) -> str:
    parsed = urlsplit(str(base_url or "").strip())
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise WebImageProtocolError("Grok Web base URL must be an http(s) origin")
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunsplit((scheme, parsed.netloc, "/ws/imagine/listen", "", ""))


def imagine_reset_message() -> dict[str, Any]:
    return {
        "type": "conversation.item.create",
        "timestamp": int(time.time() * 1000),
        "item": {"type": "message", "content": [{"type": "reset"}]},
    }


def imagine_request_message(
    prompt: str, ratio: str, *, pro: bool, generations: int, nsfw: bool = False
) -> dict[str, Any]:
    return {
        "type": "conversation.item.create",
        "timestamp": int(time.time() * 1000),
        "item": {
            "type": "message",
            "content": [{
                "requestId": "img_" + uuid.uuid4().hex,
                "text": prompt,
                "type": "input_text",
                "properties": {
                    "section_count": 0, "is_kids_mode": False,
                    "enable_nsfw": nsfw, "skip_upsampler": False,
                    "enable_side_by_side": True, "is_initial": False,
                    "aspect_ratio": ratio, "enable_pro": pro,
                    "num_generations": generations,
                },
            }],
        },
    }


def _number(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _absolute_url(value: Any) -> str:
    raw = str(value or "").strip()
    if raw.startswith(("http://", "https://")):
        return raw
    return "https://assets.grok.com/" + raw.lstrip("/") if raw else ""


class ImagineCollector:
    """Collect final image frames, discarding previews and moderated slots."""

    def __init__(self) -> None:
        self._slots: dict[str, dict[str, Any]] = {}

    def accept(self, message: Mapping[str, Any]) -> None:
        if str(message.get("type") or "") not in {"image", "json"}:
            return
        raw_url = message.get("url")
        image_id = next((str(message.get(k) or "").strip() for k in ("image_id", "job_id", "id") if message.get(k)), "")
        if not image_id and raw_url:
            image_id = str(raw_url).rstrip("/").rsplit("/", 1)[-1].split(".", 1)[0]
        if not image_id:
            return
        slot = self._slots.setdefault(image_id, {"id": image_id, "position": None, "final": False, "completed": False, "moderated": False})
        position = next((_number(message.get(k)) for k in ("side_by_side_index", "order", "grid_index") if _number(message.get(k)) is not None), None)
        if position is not None:
            slot["position"] = position
        if str(message.get("type")) == "image":
            progress = _number(message.get("percentage_complete"))
            if progress is not None and progress < 100:
                return
            slot.update(url=_absolute_url(raw_url), blob=str(message.get("blob") or ""), width=_number(message.get("width")), height=_number(message.get("height")), final=True)
            return
        if str(message.get("current_status") or "") != "completed":
            return
        if raw_url and not slot.get("final"):
            slot.update(url=_absolute_url(raw_url), blob=str(message.get("blob") or ""), final=True)
        slot["completed"] = True
        slot["moderated"] = bool(message.get("moderated"))

    def done(self, expected: int) -> bool:
        completed = [v for v in self._slots.values() if v["completed"]]
        if len(completed) < expected:
            return False
        return all(v["moderated"] or (v["final"] and (v.get("url") or v.get("blob"))) for v in completed)

    def images(self) -> list[GeneratedImage]:
        values = [v for v in self._slots.values() if v["completed"] and not v["moderated"] and v["final"] and (v.get("url") or v.get("blob"))]
        values.sort(key=lambda v: (v.get("position") is None, v.get("position") or 0, v["id"]))
        return [GeneratedImage(url=v.get("url", ""), blob=v.get("blob", ""), width=v.get("width"), height=v.get("height"), position=v.get("position")) for v in values]


def _walk_image_urls(value: Any, output: list[str]) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            low = str(key).lower()
            if low in {"generatedimageurls", "generated_image_urls", "imageurls", "image_urls"} and isinstance(item, list):
                for raw in item:
                    if isinstance(raw, str):
                        url = _absolute_url(raw)
                        if url and url not in output:
                            output.append(url)
                continue
            if low in {"imageurl", "image_url", "generatedimageurl", "generated_image_url", "url"} and isinstance(item, str) and ("image" in low or "grok" in item or "/generated/" in item):
                url = _absolute_url(item)
                if url and url not in output:
                    output.append(url)
            else:
                _walk_image_urls(item, output)
    elif isinstance(value, list):
        for item in value:
            _walk_image_urls(item, output)


def extract_lite_images(body: bytes, *, max_count: int = MAX_IMAGES) -> list[GeneratedImage]:
    """Extract final image URLs from legacy REST/SSE chat responses."""
    text = body.decode("utf-8", errors="replace")
    values: list[str] = []
    decoder = json.JSONDecoder()
    pos = 0
    while pos < len(text):
        start = text.find("{", pos)
        if start < 0:
            break
        try:
            obj, end = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            pos = start + 1
            continue
        _walk_image_urls(obj, values)
        pos = start + end
    return [GeneratedImage(url=url) for url in values[:max_count]]


def trusted_asset_url(url: str) -> bool:
    try:
        parsed = urlsplit(url)
    except ValueError:
        return False
    return parsed.scheme == "https" and (parsed.hostname or "").lower() in TRUSTED_IMAGE_HOSTS and not parsed.username and not parsed.password


def decode_blob(value: str) -> tuple[bytes, str]:
    raw = value.strip()
    mime = "image/jpeg"
    if raw.lower().startswith("data:"):
        head, sep, raw = raw.partition(",")
        if not sep or ";base64" not in head.lower():
            raise WebImageError("invalid image data URI")
        mime = head[5:].split(";", 1)[0] or mime
    data = base64.b64decode(raw, validate=False)
    if not data or len(data) > MAX_DOWNLOAD_BYTES:
        raise WebImageError("image exceeds safety limit")
    return data, mime
