"""Console Imagine image/video media transport.

The Console API is HTTP + DPoP for job creation/status polling.  Media assets
are fetched with the same account-bound browser client, but without attaching a
DPoP proof to the asset host.
"""

from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlparse

from .auth import ConsoleCredential
from .client import ConsoleDPoPClient
from .errors import ConsoleEgressChallengeError, ConsoleTokenError
from .headers import DEFAULT_USER_AGENT, browser_headers
from grok2api.upstream.browser_transport import BrowserTransportError


MAX_IMAGE_BYTES = 32 * 1024 * 1024
MAX_VIDEO_BYTES = 256 * 1024 * 1024
DEFAULT_VIDEO_TIMEOUT = 300.0
TRUSTED_ASSET_HOSTS = frozenset({"assets.grok.com", "imagine-public.x.ai", "imgen.x.ai"})


class ConsoleMediaError(RuntimeError):
    """Sanitized Console media failure with an upstream classification."""

    def __init__(self, message: str, *, status_code: int | None = None, phase: str = "") -> None:
        super().__init__(message)
        self.status_code = int(status_code) if status_code is not None else None
        self.phase = str(phase or "")

    @property
    def classification(self) -> str:
        status = self.status_code
        if status == 429:
            return "quota_or_rate_limit"
        if status in {401, 403}:
            return "auth_or_challenge"
        if status is not None and status >= 500:
            return "upstream_network"
        if status is not None:
            return "upstream_rejected"
        return "client_or_protocol"


def classify_console_media_failure(exc: BaseException) -> tuple[str, int | None, str]:
    """Return a credential-free category, status, and phase for media failures."""

    status = getattr(exc, "status_code", None)
    safe_status = int(status) if isinstance(status, int) else None
    phase = str(getattr(exc, "phase", "") or "")
    if isinstance(exc, ConsoleMediaError):
        return exc.classification, safe_status, phase
    if isinstance(exc, ConsoleEgressChallengeError):
        return "auth_or_challenge", safe_status, "dpop_session"
    if isinstance(exc, ConsoleTokenError):
        if safe_status == 429:
            return "quota_or_rate_limit", safe_status, "dpop_session"
        if safe_status in {401, 403}:
            return "auth_or_challenge", safe_status, "dpop_session"
        if safe_status is not None and safe_status >= 500:
            return "upstream_network", safe_status, "dpop_session"
        return "auth_or_session", safe_status, "dpop_session"
    if isinstance(exc, (BrowserTransportError, TimeoutError)):
        return "upstream_network", None, "transport"
    return "internal_error", safe_status, phase


@dataclass(frozen=True, slots=True)
class ConsoleImage:
    url: str = ""
    b64_json: str = ""
    mime_type: str = "image/jpeg"


@dataclass(frozen=True, slots=True)
class ConsoleVideo:
    request_id: str
    url: str
    mime_type: str = "video/mp4"


def _trusted_asset_url(raw: str, *, media: str) -> str:
    parsed = urlparse(str(raw or "").strip())
    if parsed.scheme != "https" or parsed.username or parsed.password:
        raise ConsoleMediaError(f"Console {media} URL is not trusted")
    if (parsed.hostname or "").lower() not in TRUSTED_ASSET_HOSTS:
        raise ConsoleMediaError(f"Console {media} URL is not trusted")
    return parsed.geturl()


class ConsoleMediaGateway:
    """Account-bound image generation and video job polling."""

    def __init__(
        self,
        dpop: ConsoleDPoPClient,
        asset_client: Any,
        *,
        video_timeout: float = DEFAULT_VIDEO_TIMEOUT,
    ) -> None:
        self._dpop = dpop
        self._assets = asset_client
        self.video_timeout = max(5.0, float(video_timeout))

    async def generate_image(
        self,
        request: Mapping[str, Any],
        credential: ConsoleCredential,
        *,
        account_id: str,
        egress_identity: str = "",
    ) -> list[ConsoleImage]:
        body = dict(request)
        model = str(body.get("model") or "").strip()
        if model.lower().startswith("console/"):
            model = model.split("/", 1)[1]
        body["model"] = model
        response = await self._dpop.request(
            account_id,
            credential.sso_token,
            "POST",
            "/images/generations",
            egress_identity=egress_identity,
            cloudflare_cookies=credential.cloudflare_cookies,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            json=body,
        )
        try:
            raw = await response.aread()
            status = response.status_code
        finally:
            await response.aclose()
        if status < 200 or status >= 300:
            raise ConsoleMediaError(f"Console image generation returned {status}", status_code=status, phase="image_create")
        try:
            payload = json.loads(raw)
            items = payload.get("data") if isinstance(payload, Mapping) else None
        except (ValueError, TypeError):
            raise ConsoleMediaError("Console image response is invalid") from None
        if not isinstance(items, list) or not items:
            raise ConsoleMediaError("Console image response has no data")
        result: list[ConsoleImage] = []
        for item in items[:10]:
            if not isinstance(item, Mapping):
                continue
            url = str(item.get("url") or "").strip()
            blob = str(item.get("b64_json") or "").strip()
            if url:
                url = _trusted_asset_url(url, media="image")
            elif blob:
                try:
                    base64.b64decode(blob, validate=False)
                except Exception:
                    raise ConsoleMediaError("Console image data is invalid") from None
            else:
                continue
            result.append(ConsoleImage(url=url, b64_json=blob, mime_type=str(item.get("mime_type") or "image/jpeg")))
        if not result:
            raise ConsoleMediaError("Console image response has no usable image")
        return result

    async def generate_video(
        self,
        request: Mapping[str, Any],
        credential: ConsoleCredential,
        *,
        account_id: str,
        egress_identity: str = "",
    ) -> ConsoleVideo:
        body = dict(request)
        model = str(body.get("model") or "grok-imagine-video").strip()
        if model.lower().startswith("console/"):
            model = model.split("/", 1)[1]
        body["model"] = model
        response = await self._dpop.request(
            account_id,
            credential.sso_token,
            "POST",
            "/videos/generations",
            egress_identity=egress_identity,
            cloudflare_cookies=credential.cloudflare_cookies,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            json=body,
        )
        try:
            raw = await response.aread()
            status = response.status_code
        finally:
            await response.aclose()
        if status < 200 or status >= 300:
            raise ConsoleMediaError(f"Console video creation returned {status}", status_code=status, phase="video_create")
        try:
            created = json.loads(raw)
            request_id = str(created.get("request_id") or created.get("id") or "").strip()
        except (ValueError, TypeError):
            request_id = ""
        if not request_id:
            raise ConsoleMediaError("Console video response has no request_id")

        deadline = asyncio.get_running_loop().time() + self.video_timeout
        while True:
            if asyncio.get_running_loop().time() >= deadline:
                raise ConsoleMediaError("Console video generation timed out")
            status_response = await self._dpop.request(
                account_id,
                credential.sso_token,
                "GET",
                f"/videos/{request_id}",
                egress_identity=egress_identity,
                cloudflare_cookies=credential.cloudflare_cookies,
                headers={"Accept": "application/json"},
            )
            try:
                status_raw = await status_response.aread()
                status_code = status_response.status_code
            finally:
                await status_response.aclose()
            if status_code < 200 or status_code >= 300:
                raise ConsoleMediaError(f"Console video status returned {status_code}", status_code=status_code, phase="video_poll")
            try:
                state = json.loads(status_raw)
            except (ValueError, TypeError):
                raise ConsoleMediaError("Console video status is invalid") from None
            state_name = str(state.get("status") or "").strip().lower()
            if state_name in {"failed", "error", "expired", "cancelled", "canceled"}:
                raise ConsoleMediaError("Console video generation failed")
            video = state.get("video") if isinstance(state.get("video"), Mapping) else {}
            url = str(video.get("url") or state.get("url") or "").strip()
            if state_name in {"done", "completed", "succeeded", "success", "ready"} and url:
                return ConsoleVideo(request_id, _trusted_asset_url(url, media="video"))
            await asyncio.sleep(2.0)

    async def download_asset(
        self,
        raw_url: str,
        credential: ConsoleCredential,
        *,
        media: str,
    ) -> tuple[bytes, str]:
        url = _trusted_asset_url(raw_url, media=media)
        response = await self._assets.get(
            url,
            headers=browser_headers(
                credential.sso_token,
                cloudflare_cookies=credential.cloudflare_cookies,
                user_agent=DEFAULT_USER_AGENT,
            ),
            timeout=45.0,
        )
        try:
            if response.status_code < 200 or response.status_code >= 300:
                raise ConsoleMediaError(f"Console {media} download returned {response.status_code}", status_code=response.status_code, phase=f"{media}_download")
            limit = MAX_IMAGE_BYTES if media == "image" else MAX_VIDEO_BYTES
            data = await response.aread()
            if not data or len(data) > limit:
                raise ConsoleMediaError(f"Console {media} exceeds safety limit")
            content_type = str(response.headers.get("content-type") or "")
            content_type = content_type.split(";", 1)[0].strip().lower()
            return data, content_type or ("image/jpeg" if media == "image" else "video/mp4")
        finally:
            await response.aclose()


__all__ = [
    "ConsoleImage",
    "ConsoleMediaError",
    "ConsoleMediaGateway",
    "ConsoleVideo",
    "classify_console_media_failure",
]
