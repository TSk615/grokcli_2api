"""Current Grok Web ``/ws/mgw/`` text Gateway client.

The client owns neither credentials nor an HTTP session. Both the
``httpx.AsyncClient`` and WebSocket connector are injected so identity lookup,
proxying, browser state, and tests can share one transport boundary.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import suppress
from typing import Any, Protocol
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx

from grok2api.upstream.browser_transport import is_cloudflare_challenge

from .auth import WebCredential
from .account_settings import WebAccountSettingsClient
from .headers import DEFAULT_USER_AGENT, build_cookie_header, build_web_headers
from .protocol import WebProtocolError, build_rest_chat_payload, convert_chat_completion
from .image import (
    GeneratedImage,
    IMAGE_TIMEOUT,
    ImagineCollector,
    MAX_DOWNLOAD_BYTES,
    MAX_IMAGES,
    WebImageError,
    WebImageProtocolError,
    decode_blob,
    extract_lite_images,
    imagine_request_message,
    imagine_reset_message,
    imagine_url,
    resolve_aspect_ratio,
    trusted_asset_url,
)
from .stream import GrokWebStreamParser, WebDelta, WebDeltaKind, WebStreamError


SESSION_PATH = "/api/auth/session"
GATEWAY_PATH = "/ws/mgw/"
DEFAULT_HANDSHAKE_TIMEOUT = 20.0
DEFAULT_TOTAL_TIMEOUT = 300.0
DEFAULT_HEARTBEAT_INTERVAL = 25.0
DEFAULT_MAX_FRAME_BYTES = 16 << 20
SESSION_BODY_LIMIT = 64 << 10


class WebGatewayError(RuntimeError):
    """A sanitized Gateway failure with no upstream body or auth context."""


class WebImageEditError(WebGatewayError):
    def __init__(self, stage: str, status: int | None = None):
        self.stage = stage
        self.status = status
        logging.getLogger(__name__).warning("web_image_edit_failure stage=%s status=%s", stage, status)
        super().__init__(f"Grok Web image edit failed: {stage} (HTTP {status})")


def _extract_final_edit_urls(raw: bytes) -> list[str]:
    """Ignore partial images and consume concatenated JSON or SSE frames."""
    decoder = json.JSONDecoder()
    text = raw.decode("utf-8", errors="replace")
    pos = 0
    urls: list[str] = []
    while pos < len(text):
        start = text.find("{", pos)
        if start < 0:
            break
        try:
            frame, end = decoder.raw_decode(text[start:])
        except ValueError:
            raise WebImageEditError("invalid_response") from None
        pos = start + end
        if not isinstance(frame, Mapping):
            continue
        result = frame.get("result", {})
        if not isinstance(result, Mapping):
            continue
        result = result.get("response", result)
        if not isinstance(result, Mapping):
            continue
        if frame.get("error") or result.get("error"):
            raise WebImageEditError("upstream_error_event")
        event = result.get("streamingImageGenerationResponse", {})
        model = result.get("modelResponse", {})
        if isinstance(event, Mapping) and event:
            if event.get("moderated") is True:
                raise WebImageEditError("moderated")
            if event.get("progress") == 100 and not event.get("partial"):
                candidates = [event.get("imageUrl")]
            else:
                candidates = []
        else:
            candidates = []
        if isinstance(model, Mapping):
            final_urls = model.get("generatedImageUrls")
            if isinstance(final_urls, list):
                candidates.extend(final_urls)
        for candidate in candidates:
            if not isinstance(candidate, str) or not candidate.strip():
                continue
            url = candidate.strip()
            if not url.startswith(("https://", "http://")):
                url = "https://assets.grok.com/" + url.lstrip("/")
            if trusted_asset_url(url) and url not in urls:
                urls.append(url)
    return urls


class WebGatewayAuthError(WebGatewayError):
    pass


class WebGatewayEgressError(WebGatewayError):
    pass


class WebSocketConnection(Protocol):
    async def send(self, message: str) -> Any: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> Any: ...


WebSocketConnector = Callable[
    [str, Mapping[str, str], float], Awaitable[WebSocketConnection]
]


def _extract_media_urls(raw: bytes, *, media: str) -> list[str]:
    """Extract final image/video URLs from JSON or SSE media responses."""

    values: list[str] = []
    decoder = json.JSONDecoder()
    text = raw.decode("utf-8", errors="replace")
    candidates: list[Any] = []
    for line in text.splitlines():
        value = line.strip()
        if value.startswith("data:"):
            value = value[5:].strip()
        if value and value != "[DONE]" and value.startswith("{"):
            try:
                candidates.append(json.loads(value))
            except ValueError:
                pass
    if not candidates:
        pos = 0
        while pos < len(text):
            start = text.find("{", pos)
            if start < 0:
                break
            try:
                value, end = decoder.raw_decode(text[start:])
            except ValueError:
                pos = start + 1
                continue
            candidates.append(value)
            pos = start + end

    wanted = ("videoUrl", "contentUrl", "contentURL", "assetUrl", "assetURL", "fileUrl", "fileUri", "fileURL", "url") if media == "video" else ("imageUrl", "generatedImageUrl", "url")

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            moderated = value.get("moderated") is True
            for key in wanted:
                item = value.get(key)
                if isinstance(item, str) and item.strip() and not moderated:
                    candidate = item.strip()
                    if not candidate.startswith(("http://", "https://")):
                        candidate = "https://assets.grok.com/" + candidate.lstrip("/")
                    if candidate not in values:
                        values.append(candidate)
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    for candidate in candidates:
        walk(candidate)
    return values


async def _default_websocket_connector(
    url: str,
    headers: Mapping[str, str],
    open_timeout: float,
    *,
    proxy: str | None = None,
) -> WebSocketConnection:
    """Import websockets only when a real Gateway connection is requested."""

    try:
        import websockets  # type: ignore[import-not-found]
    except ImportError:
        raise WebGatewayError(
            "Grok Web Gateway requires the optional 'websockets' dependency"
        ) from None

    connect = websockets.connect
    kwargs: dict[str, Any] = {
        "open_timeout": open_timeout,
        "max_size": DEFAULT_MAX_FRAME_BYTES,
    }
    if proxy:
        kwargs["proxy"] = proxy
    try:
        return await connect(url, additional_headers=dict(headers), **kwargs)
    except TypeError:
        if proxy:
            # Never silently fall back to a direct WebSocket connection when
            # the selected account is bound to a proxy. Older websockets
            # releases don't support proxy= and would split HTTP/WS egress.
            raise WebGatewayError(
                "installed websockets does not support configured proxy egress"
            ) from None
        # websockets < 14 names the same argument ``extra_headers``.
        try:
            return await connect(url, extra_headers=dict(headers), **kwargs)
        except Exception:
            raise WebGatewayError("Grok Web Gateway handshake failed") from None
    except Exception:
        raise WebGatewayError("Grok Web Gateway handshake failed") from None


def _safe_origin(base_url: str) -> tuple[str, str]:
    try:
        parsed = urlsplit(str(base_url or "").strip())
    except ValueError:
        parsed = None
    if parsed is None or parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("Grok Web base URL must be an http(s) origin")
    origin = urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
    ws_scheme = "wss" if parsed.scheme == "https" else "ws"
    return origin, ws_scheme


def gateway_endpoint(base_url: str, user_id: str) -> str:
    origin, ws_scheme = _safe_origin(base_url)
    parsed = urlsplit(origin)
    return urlunsplit(
        (ws_scheme, parsed.netloc, GATEWAY_PATH, urlencode({"uid": user_id}), "")
    )


def gateway_headers(
    credential: WebCredential,
    user_id: str,
    *,
    origin: str,
    user_agent: str = DEFAULT_USER_AGENT,
) -> dict[str, str]:
    """Build the evidenced MGW handshake headers, including account identity."""

    # user_id is canonical UUID text by the time this function is called.
    return {
        "Origin": origin,
        "User-Agent": user_agent,
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Cookie": f"{build_cookie_header(credential)}; x-userid={user_id}",
    }


def _first_text(*values: Any) -> str:
    return next((value.strip() for value in values if isinstance(value, str) and value.strip()), "")


def _parse_user_id(value: Any) -> str:
    if not isinstance(value, Mapping):
        raise WebGatewayAuthError("Grok Web session identity is unavailable")
    status = str(value.get("status") or "").strip().lower()
    if status in ("blocked", "unauthenticated"):
        raise WebGatewayAuthError("Grok Web session is not authenticated")
    session = value.get("session") if isinstance(value.get("session"), Mapping) else {}
    user = value.get("user") if isinstance(value.get("user"), Mapping) else {}
    user_id = _first_text(
        session.get("userId"),
        user.get("id"),
        user.get("userId"),
        user.get("sub"),
        value.get("id"),
        value.get("userId"),
        value.get("sub"),
    )
    try:
        return str(uuid.UUID(user_id))
    except (ValueError, AttributeError):
        raise WebGatewayAuthError("Grok Web session has no valid user identity") from None


class GrokWebGateway:
    """Resolve identity, drive one MGW turn, and yield normalized deltas."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        connector: WebSocketConnector | None = None,
        base_url: str = "https://grok.com",
        user_agent: str = DEFAULT_USER_AGENT,
        handshake_timeout: float = DEFAULT_HANDSHAKE_TIMEOUT,
        total_timeout: float = DEFAULT_TOTAL_TIMEOUT,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        proxy: str | None = None,
    ) -> None:
        if client is None:
            raise ValueError("an httpx.AsyncClient is required")
        origin, _ = _safe_origin(base_url)
        self._client = client
        self._connector = connector or _default_websocket_connector
        self.origin = origin
        self.user_agent = user_agent
        self.handshake_timeout = max(0.001, float(handshake_timeout))
        self.total_timeout = max(0.001, float(total_timeout))
        self.heartbeat_interval = max(0.001, float(heartbeat_interval))
        self.max_frame_bytes = max(1024, int(max_frame_bytes))
        self.proxy = str(proxy or "").strip() or None
        self._statsig = WebAccountSettingsClient(
            client,
            base_url=self.origin,
            user_agent=self.user_agent,
        )

    async def _media_headers(
        self,
        credential: WebCredential,
        *,
        path: str,
        referer: str,
        accept: str,
    ) -> dict[str, str]:
        """Build browser media headers, including the optional Statsig proof."""

        headers = build_web_headers(
            credential,
            user_agent=self.user_agent,
            extra={
                "Accept": accept,
                "Referer": referer,
                "x-xai-request-id": str(uuid.uuid4()),
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            },
        )
        from .statsig import generate

        signature = generate(path)
        if not signature:
            signature = await self._statsig._optional_signed_statsig(credential, path)
        if signature:
            headers["x-statsig-id"] = signature
        return headers

    async def _connect(self, endpoint: str, headers: Mapping[str, str]) -> WebSocketConnection:
        if self._connector is _default_websocket_connector:
            return await _default_websocket_connector(
                endpoint,
                headers,
                self.handshake_timeout,
                proxy=self.proxy,
            )
        return await self._connector(endpoint, headers, self.handshake_timeout)

    async def fetch_user_id(self, credential: WebCredential) -> str:
        """Resolve the stable MGW uid using this instance's HTTP client."""

        try:
            response = await self._client.get(
                f"{self.origin}{SESSION_PATH}",
                headers=build_web_headers(
                    credential,
                    user_agent=self.user_agent,
                    extra={"Accept": "*/*", "Cache-Control": "no-cache"},
                ),
                timeout=min(15.0, self.handshake_timeout),
            )
        except Exception:
            raise WebGatewayError("Grok Web session lookup failed") from None
        body = response.content
        if len(body) > SESSION_BODY_LIMIT:
            raise WebGatewayError("Grok Web session response exceeds the safety limit")
        if is_cloudflare_challenge(response.status_code, response.headers, body):
            raise WebGatewayEgressError("Grok Web browser session was challenged")
        if response.status_code == 401:
            raise WebGatewayAuthError("Grok Web session is not authenticated")
        if response.status_code < 200 or response.status_code >= 300:
            raise WebGatewayError("Grok Web session lookup was rejected")
        try:
            value = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise WebGatewayError("Grok Web session response is invalid") from None
        return _parse_user_id(value)

    async def iter_chat(
        self, request: Mapping[str, Any], credential: WebCredential
    ) -> AsyncIterator[WebDelta]:
        """Run one text turn and yield text/reasoning/citation deltas."""

        try:
            converted = convert_chat_completion(request)
        except WebProtocolError:
            raise
        user_id = await self.fetch_user_id(credential)
        endpoint = gateway_endpoint(self.origin, user_id)
        headers = gateway_headers(
            credential, user_id, origin=self.origin, user_agent=self.user_agent
        )
        try:
            connection = await asyncio.wait_for(
                self._connect(endpoint, headers),
                timeout=self.handshake_timeout,
            )
        except WebGatewayError:
            raise
        except Exception:
            raise WebGatewayError("Grok Web Gateway handshake failed") from None

        deadline = asyncio.get_running_loop().time() + self.total_timeout
        send_lock = asyncio.Lock()
        parser = GrokWebStreamParser(max_frame_chars=self.max_frame_bytes)
        heartbeat: asyncio.Task[None] | None = None
        try:
            initial_event_id = f"evt_init_{uuid.uuid4()}"
            await self._send_json(
                connection,
                send_lock,
                {
                    "event": {
                        "type": "session.create",
                        "event_id": initial_event_id,
                        "session": self._session(converted.upstream_mode),
                    }
                },
            )
            heartbeat = asyncio.create_task(self._heartbeat(connection, send_lock))
            created = False
            attached = False
            turn_sent = False
            session_id = ""
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise WebGatewayError("Grok Web Gateway total timeout exceeded")
                try:
                    raw = await asyncio.wait_for(connection.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    raise WebGatewayError("Grok Web Gateway total timeout exceeded") from None
                except Exception:
                    raise WebGatewayError("Grok Web Gateway connection closed unexpectedly") from None
                frame = self._decode_frame(raw)
                try:
                    envelope = json.loads(frame)
                except json.JSONDecodeError:
                    continue
                if not isinstance(envelope, Mapping):
                    continue
                event = envelope.get("event")
                if not isinstance(event, Mapping):
                    continue
                event_type = str(event.get("type") or "")

                try:
                    deltas = parser.feed(frame)
                except WebStreamError:
                    raise WebGatewayError("Grok Web Gateway returned an error event") from None
                for delta in deltas:
                    yield delta

                if event_type == "session.created":
                    client_event_id = str(event.get("client_event_id") or "")
                    if client_event_id and client_event_id != initial_event_id:
                        continue
                    created = True
                    session_id = session_id or str(envelope.get("session_id") or "")
                elif event_type == "conversation.attached":
                    conversation = event.get("conversation")
                    attached_id = (
                        str(conversation.get("id") or "")
                        if isinstance(conversation, Mapping)
                        else ""
                    )
                    if session_id and attached_id and session_id != attached_id:
                        raise WebGatewayError(
                            "Grok Web Gateway returned inconsistent conversation identity"
                        )
                    session_id = session_id or attached_id
                    attached = bool(session_id)
                elif event_type == "ping":
                    # Reference implementation emits heartbeat pings. Peer pings
                    # need no application payload and are safely ignored.
                    pass
                elif event_type == "error":
                    raise WebGatewayError("Grok Web Gateway returned an error event")
                elif event_type == "response.done":
                    response = event.get("response")
                    status = (
                        str(response.get("status") or "")
                        if isinstance(response, Mapping)
                        else ""
                    )
                    if status and status != "completed":
                        raise WebGatewayError("Grok Web Gateway response did not complete")
                    parser.finish()
                    return
                elif event_type == "session.ended":
                    raise WebGatewayError("Grok Web Gateway session ended before completion")

                if created and attached and not turn_sent:
                    turn_sent = True
                    item_event, response_event = self._turn_events(
                        session_id, str(converted.payload["message"])
                    )
                    await self._send_json(connection, send_lock, item_event)
                    await self._send_json(connection, send_lock, response_event)
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat
            with suppress(Exception):
                await connection.close()

    async def generate_image(
        self, request: Mapping[str, Any], credential: WebCredential
    ) -> list[GeneratedImage]:
        """Generate Web images without falling back to another provider.

        ``grok-imagine-image-lite`` uses the legacy chat stream and the two
        Imagine models use ``/ws/imagine/listen``.  The returned URLs are
        upstream asset URLs; callers can use :meth:`download_image` to archive
        them while preserving the same account cookies and Resin egress.
        """
        if not isinstance(request, Mapping):
            raise WebImageProtocolError("image request must be an object")
        model_id = str(request.get("model") or "").strip()
        if model_id.lower().startswith("web/"):
            model_id = model_id.split("/", 1)[1].strip()
        from .models import get_web_model

        model = get_web_model(model_id)
        if model is None or not model.supports("image"):
            raise WebImageProtocolError("unsupported Grok Web image model")
        prompt = request.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip():
            raise WebImageProtocolError("prompt must be a non-empty string")
        if bool(request.get("stream", False)):
            raise WebImageProtocolError("Web image streaming is not enabled yet")
        count = request.get("n", 1)
        try:
            count = int(count)
        except (TypeError, ValueError):
            raise WebImageProtocolError("n must be an integer") from None
        if count < 1 or count > MAX_IMAGES:
            raise WebImageProtocolError("n must be between 1 and 10")
        response_format = str(request.get("response_format") or "url").strip().lower()
        if response_format not in {"url", "b64_json"}:
            raise WebImageProtocolError("response_format must be url or b64_json")
        ratio = resolve_aspect_ratio(request.get("aspect_ratio"), request.get("size"))
        # Web image generation always enables the upstream NSFW capability.
        # This is an operator policy: callers cannot disable it by omitting the
        # field or explicitly sending ``nsfw=false`` / ``enable_nsfw=false``.
        nsfw = True
        if model.protocol_model == "imagine-lite":
            return await self._generate_lite_images(prompt.strip(), count, credential)
        return await self._generate_imagine_images(
            prompt.strip(), count, ratio, model.imagine_pro, credential, nsfw=nsfw
        )

    async def _generate_lite_images(
        self, prompt: str, count: int, credential: WebCredential
    ) -> list[GeneratedImage]:
        values: list[GeneratedImage] = []
        # Lite is an image-enabled MGW fast chat, as in the current upstream
        # implementation. Each turn contributes at most one requested image.
        for _ in range(count):
            try:
                async for delta in self.iter_chat(
                    {
                        "model": "grok-chat-fast",
                        "stream": False,
                        "messages": [{"role": "user", "content": "Drawing: " + prompt}],
                    },
                    credential,
                ):
                    if delta.kind is WebDeltaKind.IMAGE and delta.text:
                        image = GeneratedImage(url=delta.text)
                        if image.url not in {item.url for item in values}:
                            values.append(image)
                            break
            except Exception:
                raise WebGatewayError("Grok Web Lite image request failed") from None
            if len(values) >= count:
                break
        if len(values) < count:
            raise WebGatewayError("Grok Web Lite returned no complete images")
        return values[:count]

    async def _generate_imagine_images(
        self,
        prompt: str,
        count: int,
        ratio: str,
        pro: bool,
        credential: WebCredential,
        *,
        nsfw: bool = False,
    ) -> list[GeneratedImage]:
        endpoint = imagine_url(self.origin)
        headers = {
            "Origin": self.origin,
            "User-Agent": self.user_agent,
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Cookie": build_cookie_header(credential),
        }
        try:
            connection = await asyncio.wait_for(
                self._connect(endpoint, headers), timeout=self.handshake_timeout
            )
        except Exception:
            raise WebGatewayError("Grok Web Imagine handshake failed") from None
        collector = ImagineCollector()
        deadline = asyncio.get_running_loop().time() + min(self.total_timeout, IMAGE_TIMEOUT)
        try:
            await self._send_json(connection, asyncio.Lock(), imagine_reset_message())
            await self._send_json(
                connection,
                asyncio.Lock(),
                imagine_request_message(
                    prompt,
                    ratio,
                    pro=pro,
                    generations=count,
                    nsfw=nsfw,
                ),
            )
            while not collector.done(count):
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise WebGatewayError("Grok Web Imagine generation timed out")
                try:
                    raw = await asyncio.wait_for(connection.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    raise WebGatewayError("Grok Web Imagine generation timed out") from None
                frame = self._decode_frame(raw)
                try:
                    message = json.loads(frame)
                except json.JSONDecodeError:
                    continue
                if not isinstance(message, Mapping):
                    continue
                if str(message.get("type") or "") == "error":
                    raise WebGatewayError("Grok Web Imagine returned an error")
                collector.accept(message)
            images = collector.images()
            if len(images) < count:
                raise WebGatewayError("Grok Web Imagine returned incomplete images")
            return images[:count]
        finally:
            with suppress(Exception):
                await connection.close()

    async def edit_image(
        self,
        request: Mapping[str, Any],
        credential: WebCredential,
        *,
        image_bytes: bytes,
        filename: str = "input.png",
        content_type: str = "image/png",
    ) -> list[GeneratedImage]:
        """Edit an uploaded image through Grok Web's current REST media flow."""

        prompt = str(request.get("prompt") or "").strip()
        if not prompt:
            raise WebImageProtocolError("prompt must be a non-empty string")
        if not image_bytes:
            raise WebImageProtocolError("image must not be empty")
        headers = await self._media_headers(
            credential,
            path="/http/upload-file-v2/direct",
            referer=f"{self.origin}/imagine",
            accept="*/*",
        )
        # Encode with httpx, send raw bytes through the existing account-bound
        # client. curl_cffi rejects the httpx-style files= argument.
        upload_request = httpx.Request(
            "POST", f"{self.origin}/http/upload-file-v2/direct",
            files={"file": (filename or "input.png", image_bytes, content_type or "image/png")},
            data={"file_source": "IMAGINE_SELF_UPLOAD_FILE_SOURCE"},
        )
        upload_body = upload_request.read()
        headers["Content-Type"] = upload_request.headers["Content-Type"]
        response = await self._client.post(
            f"{self.origin}/http/upload-file-v2/direct",
            headers=headers,
            content=upload_body,
            timeout=60,
        )
        try:
            upload_raw = await response.aread()
            upload_status = response.status_code
        finally:
            await response.aclose()
        if upload_status < 200 or upload_status >= 300:
            raise WebImageEditError("upload", upload_status)
        try:
            uploaded = json.loads(upload_raw)
        except (ValueError, TypeError):
            raise WebGatewayError("Grok Web image upload response is invalid") from None
        if not isinstance(uploaded, Mapping):
            raise WebImageEditError("upload_metadata")
        if uploaded.get("terminalError"):
            raise WebImageEditError("upload_terminal_error", upload_status)
        metadata = uploaded.get("fileMetadata")
        asset_id = str(metadata.get("fileMetadataId") or "").strip() if isinstance(metadata, Mapping) else ""
        if not asset_id:
            raise WebGatewayError("Grok Web image upload returned no asset")
        payload = {
            "modelName": "imagine-image-edit",
            "message": prompt,
            "enableImageStreaming": True,
            "enableSideBySide": True,
            "sendFinalMetadata": True,
            "mediaGenInput": {"imageToImage": {"prompt": prompt, "inputAssets": [asset_id]}},
        }
        ratio = str(request.get("aspect_ratio") or "").strip()
        if ratio:
            payload["mediaGenInput"]["imageToImage"]["aspectRatio"] = ratio
        response = await self._client.post(
            f"{self.origin}/rest/app-chat/conversations/new",
            headers=await self._media_headers(
                credential,
                path="/rest/app-chat/conversations/new",
                referer=f"{self.origin}/imagine",
                accept="*/*",
            ),
            json=payload,
            timeout=self.total_timeout,
        )
        try:
            raw = await response.aread()
            status = response.status_code
        finally:
            await response.aclose()
        if status < 200 or status >= 300:
            raise WebImageEditError("generate", status)
        urls = _extract_final_edit_urls(raw)
        if not urls:
            raise WebGatewayError("Grok Web image edit returned no image")
        return [GeneratedImage(url=url) for url in urls[: int(request.get("n") or 1)]]

    async def generate_video(
        self,
        request: Mapping[str, Any],
        credential: WebCredential,
    ) -> str:
        """Create a Web text-to-video job and return its authenticated asset URL."""

        prompt = str(request.get("prompt") or "").strip()
        if not prompt:
            raise WebGatewayError("video prompt must be non-empty")
        duration = int(request.get("duration") or 6)
        resolution = str(request.get("resolution") or "480p").strip().lower()
        ratio = str(request.get("aspect_ratio") or "1:1").strip()
        payload = {
            "modelName": "imagine-video-gen",
            "message": prompt + " --mode=custom",
            "enableImageStreaming": True,
            "enableSideBySide": True,
            "sendFinalMetadata": True,
            "responseMetadata": {"experiments": [], "modelConfigOverride": {"modelMap": {}}},
            "mediaGenInput": {"textToVideo": {"prompt": prompt, "aspectRatio": ratio, "duration": duration, "resolutionName": resolution}},
            "kind": "CONVERSATION_KIND_IMAGINE",
        }
        response = await self._client.post(
            f"{self.origin}/rest/app-chat/conversations/new",
            headers=await self._media_headers(
                credential,
                path="/rest/app-chat/conversations/new",
                referer=f"{self.origin}/imagine",
                accept="*/*",
            ),
            json=payload,
            timeout=self.total_timeout,
        )
        try:
            raw = await response.aread()
            status = response.status_code
        finally:
            await response.aclose()
        if status < 200 or status >= 300:
            raise WebGatewayError(f"Grok Web video generation failed ({status})")
        urls = _extract_media_urls(raw, media="video")
        if not urls:
            raise WebGatewayError("Grok Web video generation returned no asset")
        return urls[0]

    async def download_image(
        self, image: GeneratedImage, credential: WebCredential
    ) -> tuple[bytes, str]:
        """Download a generated asset using the same credential-bound client."""
        if image.blob:
            return decode_blob(image.blob)
        if not image.url or not trusted_asset_url(image.url):
            raise WebImageError("image URL host is not trusted")
        try:
            response = await self._client.get(
                image.url,
                headers=build_web_headers(
                    credential, user_agent=self.user_agent, extra={"Accept": "image/*"}
                ),
                timeout=60.0,
            )
            length = int(response.headers.get("content-length", "0") or 0)
            if length > MAX_DOWNLOAD_BYTES:
                raise WebImageError("image exceeds safety limit")
            body = response.content
        except WebImageError:
            raise
        except Exception:
            raise WebImageError("image download failed") from None
        if response.status_code < 200 or response.status_code >= 300 or not body:
            raise WebImageError("image download was rejected")
        if len(body) > MAX_DOWNLOAD_BYTES:
            raise WebImageError("image exceeds safety limit")
        content_type = str(response.headers.get("content-type", "image/jpeg")).split(";", 1)[0].strip().lower()
        if not content_type.startswith("image/"):
            content_type = "image/jpeg"
        return body, content_type

    async def download_video(self, url: str, credential: WebCredential) -> tuple[bytes, str]:
        """Download a generated Web video through the same credential session."""
        if not url or not trusted_asset_url(url):
            raise WebGatewayError("video URL host is not trusted")
        try:
            response = await self._client.get(
                url,
                headers=build_web_headers(
                    credential, user_agent=self.user_agent, extra={"Accept": "video/*, application/octet-stream"}
                ),
                timeout=self.total_timeout,
            )
            body = response.content
            content_type = str(response.headers.get("content-type", "video/mp4")).split(";", 1)[0].strip().lower()
        except Exception:
            raise WebGatewayError("video download failed") from None
        if response.status_code < 200 or response.status_code >= 300 or not body:
            raise WebGatewayError("video download was rejected")
        if not content_type.startswith("video/"):
            content_type = "video/mp4"
        return body, content_type

    @staticmethod
    def _session(mode: str) -> dict[str, Any]:
        return {
            "model": mode,
            "x_grok": {
                "protocol_capabilities": [
                    "conversation_attached",
                    "custom_methods_v1",
                ],
                "use_chunk": True,
                "enable_side_by_side": True,
                "force_side_by_side": False,
                "enable_image_generation": True,
                "image_generation_count": 2,
                "disable_text_follow_ups": False,
                "disable_artifact": True,
                "force_concise": False,
                "keep_context": False,
                "is_temporary": True,
                "disable_memory": True,
            },
        }

    @staticmethod
    def _turn_events(session_id: str, prompt: str) -> tuple[dict[str, Any], dict[str, Any]]:
        now = int(time.time() * 1000)
        item = {
            "type": "message",
            "role": "user",
            "x_grok": {
                "client_message_id": str(uuid.uuid4()),
                "input_chunks": [{"text": {"text": prompt}}],
            },
        }
        return (
            {
                "session_id": session_id,
                "event": {
                    "type": "conversation.item.create",
                    "event_id": f"evt_msg_{now}",
                    "item": item,
                },
            },
            {
                "session_id": session_id,
                "event": {
                    "type": "response.create",
                    "event_id": f"evt_resp_{now}",
                },
            },
        )

    async def _heartbeat(
        self, connection: WebSocketConnection, send_lock: asyncio.Lock
    ) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            try:
                await self._send_json(
                    connection,
                    send_lock,
                    {
                        "event": {
                            "type": "ping",
                            "event_id": f"evt_hb_{int(time.time() * 1000)}",
                        }
                    },
                )
            except Exception:
                return

    @staticmethod
    async def _send_json(
        connection: WebSocketConnection,
        send_lock: asyncio.Lock,
        value: Mapping[str, Any],
    ) -> None:
        try:
            encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
            async with send_lock:
                await connection.send(encoded)
        except Exception:
            raise WebGatewayError("Grok Web Gateway send failed") from None

    def _decode_frame(self, raw: str | bytes) -> str:
        if isinstance(raw, bytes):
            size = len(raw)
            if size > self.max_frame_bytes:
                raise WebGatewayError("Grok Web Gateway frame exceeds the safety limit")
            try:
                return raw.decode("utf-8")
            except UnicodeDecodeError:
                raise WebGatewayError("Grok Web Gateway frame is not valid UTF-8") from None
        if not isinstance(raw, str):
            raise WebGatewayError("Grok Web Gateway returned an unsupported frame")
        if len(raw.encode("utf-8")) > self.max_frame_bytes:
            raise WebGatewayError("Grok Web Gateway frame exceeds the safety limit")
        return raw
