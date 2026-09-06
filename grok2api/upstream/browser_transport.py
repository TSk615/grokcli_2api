"""Browser-shaped HTTP and WebSocket transport for Web/Console providers.

The Build provider intentionally keeps its ordinary ``httpx`` transport.  Web
and Console share this adapter so the selected proxy, Chromium TLS profile,
headers, and WebSocket handshake cannot silently drift onto different egress
paths.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Mapping

try:
    from curl_cffi import CurlWsFlag
    from curl_cffi.requests import AsyncSession
except Exception:  # pragma: no cover - exercised only in broken deployments
    CurlWsFlag = None  # type: ignore[assignment]
    AsyncSession = None  # type: ignore[assignment,misc]


DEFAULT_BROWSER_IMPERSONATE = "chrome131"
DEFAULT_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
MAX_CHALLENGE_SCAN_BYTES = 64 << 10


class BrowserTransportError(RuntimeError):
    """Sanitized browser transport failure with no request or proxy context."""


def _header_value(headers: Mapping[str, Any] | None, name: str) -> str:
    wanted = name.lower()
    for key, value in (headers or {}).items():
        if str(key).lower() == wanted:
            return str(value or "")
    return ""


def is_cloudflare_challenge(
    status_code: int,
    headers: Mapping[str, Any] | None,
    body: bytes | str | None,
    *,
    max_scan: int = MAX_CHALLENGE_SCAN_BYTES,
) -> bool:
    """Detect Cloudflare challenge documents without retaining large bodies."""

    limit = max(0, min(int(max_scan), MAX_CHALLENGE_SCAN_BYTES))
    if isinstance(body, bytes):
        text = body[:limit].decode("utf-8", errors="ignore").lower()
    else:
        text = str(body or "")[:limit].lower()
    server = _header_value(headers, "server").lower()
    cf_ray = _header_value(headers, "cf-ray")
    content_type = _header_value(headers, "content-type").lower()
    strong_marker = any(
        marker in text
        for marker in (
            "_cf_chl_opt",
            "cf-chl-",
            "/cdn-cgi/challenge-platform",
            "just a moment",
            "attention required",
            "cloudflare ray id",
        )
    )
    if strong_marker:
        return True
    return int(status_code or 0) == 403 and (
        "cloudflare" in server or bool(cf_ray) or "text/html" in content_type
    )


def _curl_timeout(value: Any) -> Any:
    """Convert an httpx-style timeout to curl_cffi's scalar/tuple form."""

    if value is None:
        return None
    connect = getattr(value, "connect", None)
    read = getattr(value, "read", None)
    if connect is not None or read is not None:
        connect_f = float(connect if connect is not None else read)
        read_f = float(read if read is not None else connect)
        return (connect_f, read_f)
    return value


@dataclass(slots=True, repr=False)
class BrowserRequest:
    method: str
    url: str
    kwargs: dict[str, Any] = field(default_factory=dict, repr=False)

    def __repr__(self) -> str:
        return f"BrowserRequest(method={self.method!r}, url={self.url!r})"


class BrowserResponse:
    """The small httpx.Response surface used by the provider adapters."""

    __slots__ = ("_raw", "_stream", "_buffer", "_consumed")

    def __init__(self, raw: Any, *, stream: bool = False) -> None:
        self._raw = raw
        self._stream = bool(stream)
        self._buffer: bytes | None = None
        self._consumed = False

    @property
    def status_code(self) -> int:
        return int(getattr(self._raw, "status_code", 0) or 0)

    @property
    def headers(self) -> Any:
        return getattr(self._raw, "headers", {})

    @property
    def content(self) -> bytes:
        if self._buffer is not None:
            return self._buffer
        return bytes(getattr(self._raw, "content", b"") or b"")

    @property
    def text(self) -> str:
        if self._buffer is not None:
            return self._buffer.decode("utf-8", errors="replace")
        return str(getattr(self._raw, "text", "") or "")

    def json(self) -> Any:
        if self._buffer is not None:
            import json

            return json.loads(self._buffer)
        return self._raw.json()

    async def aread(self) -> bytes:
        if self._buffer is not None:
            return self._buffer
        if self._stream:
            chunks = [chunk async for chunk in self._raw.aiter_content()]
            self._buffer = b"".join(bytes(chunk) for chunk in chunks)
            self._consumed = True
            return self._buffer
        self._buffer = self.content
        self._consumed = True
        return self._buffer

    async def aiter_bytes(self):
        if self._buffer is not None:
            if self._buffer:
                yield self._buffer
            return
        if self._stream:
            async for chunk in self._raw.aiter_content():
                self._consumed = True
                yield bytes(chunk)
            return
        content = self.content
        self._consumed = True
        if content:
            yield content

    async def aclose(self) -> None:
        closer = getattr(self._raw, "aclose", None)
        if callable(closer):
            result = closer()
            if inspect.isawaitable(result):
                await result
            return
        closer = getattr(self._raw, "close", None)
        if callable(closer):
            closer()

    def __repr__(self) -> str:
        return f"BrowserResponse(status_code={self.status_code})"


class _BrowserWebSocketConnection:
    __slots__ = ("_raw",)

    def __init__(self, raw: Any) -> None:
        self._raw = raw

    async def send(self, message: str | bytes) -> Any:
        try:
            if isinstance(message, str) and hasattr(self._raw, "send_str"):
                return await self._raw.send_str(message)
            if isinstance(message, str) and CurlWsFlag is not None:
                return await self._raw.send(message, CurlWsFlag.TEXT)
            return await self._raw.send(message)
        except Exception:
            raise BrowserTransportError("browser WebSocket send failed") from None

    async def recv(self) -> str | bytes:
        try:
            value = await self._raw.recv()
        except Exception:
            raise BrowserTransportError("browser WebSocket receive failed") from None
        flags = None
        if isinstance(value, tuple) and len(value) == 2:
            value, flags = value
        if isinstance(value, bytes) and (
            flags is None
            or CurlWsFlag is None
            or bool(int(flags) & int(CurlWsFlag.TEXT))
        ):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                return value
        return value

    async def close(self) -> Any:
        try:
            result = self._raw.close()
            if inspect.isawaitable(result):
                return await result
            return result
        except Exception:
            return None

    def __repr__(self) -> str:
        return "BrowserWebSocketConnection()"


class BrowserAsyncClient:
    """Account-isolated curl_cffi session with a fixed browser fingerprint."""

    def __init__(
        self,
        *,
        proxy: str | None = None,
        proxy_auth: tuple[str, str] | None = None,
        impersonate: str = DEFAULT_BROWSER_IMPERSONATE,
        max_clients: int = 16,
    ) -> None:
        if AsyncSession is None:
            raise BrowserTransportError("browser transport dependency is unavailable")
        self._proxy = str(proxy or "").strip() or None
        if proxy_auth is not None:
            if len(proxy_auth) != 2 or not all(
                isinstance(part, str) and part and not any(c in part for c in ("\r", "\n", "\x00"))
                for part in proxy_auth
            ):
                raise ValueError("proxy_auth is invalid")
        self._proxy_auth = proxy_auth
        self._impersonate = str(impersonate or DEFAULT_BROWSER_IMPERSONATE).strip()
        self._closed = False
        try:
            session_kwargs: dict[str, Any] = {
                "proxy": self._proxy,
                "impersonate": self._impersonate,
                "default_headers": False,
                "allow_redirects": False,
                "trust_env": False,
                "max_clients": max(1, int(max_clients)),
            }
            if self._proxy_auth is not None:
                session_kwargs["proxy_auth"] = self._proxy_auth
            self._session = AsyncSession(**session_kwargs)
        except Exception:
            raise BrowserTransportError("browser transport initialization failed") from None

    @property
    def is_closed(self) -> bool:
        return self._closed

    def build_request(self, method: str, url: str, **kwargs: Any) -> BrowserRequest:
        if any(
            key in kwargs
            for key in ("proxy", "proxies", "proxy_auth", "impersonate")
        ):
            raise ValueError("request cannot override the bound browser egress")
        return BrowserRequest(str(method).upper(), str(url), dict(kwargs))

    async def send(self, request: BrowserRequest, *, stream: bool = False) -> BrowserResponse:
        if not isinstance(request, BrowserRequest):
            raise TypeError("BrowserAsyncClient.send requires BrowserRequest")
        kwargs = dict(request.kwargs)
        kwargs["stream"] = bool(stream)
        return await self.request(request.method, request.url, **kwargs)

    async def get(self, url: str, **kwargs: Any) -> BrowserResponse:
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> BrowserResponse:
        return await self.request("POST", url, **kwargs)

    async def request(self, method: str, url: str, **kwargs: Any) -> BrowserResponse:
        if self._closed:
            raise BrowserTransportError("browser transport is closed")
        if any(
            key in kwargs
            for key in ("proxy", "proxies", "proxy_auth", "impersonate")
        ):
            raise ValueError("request cannot override the bound browser egress")
        stream = bool(kwargs.pop("stream", False))
        if "timeout" in kwargs:
            converted = _curl_timeout(kwargs["timeout"])
            if converted is None:
                kwargs.pop("timeout", None)
            else:
                kwargs["timeout"] = converted
        kwargs.setdefault("discard_cookies", True)
        try:
            response = await self._session.request(
                str(method).upper(),
                str(url),
                stream=stream,
                **kwargs,
            )
        except Exception:
            raise BrowserTransportError("browser HTTP request failed") from None
        return BrowserResponse(response, stream=stream)

    async def websocket_connector(
        self,
        endpoint: str,
        headers: Mapping[str, str],
        open_timeout: float,
    ) -> _BrowserWebSocketConnection:
        if self._closed:
            raise BrowserTransportError("browser transport is closed")
        connect = getattr(self._session, "ws_connect", None)
        if not callable(connect):
            raise BrowserTransportError("browser WebSocket transport is unavailable")
        try:
            context = connect(
                endpoint,
                headers=dict(headers),
                timeout=float(open_timeout),
                autoclose=True,
                proxy=self._proxy,
                proxy_auth=self._proxy_auth,
            )
            raw = await context
        except Exception:
            raise BrowserTransportError("browser WebSocket handshake failed") from None
        return _BrowserWebSocketConnection(raw)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            result = self._session.close()
            if inspect.isawaitable(result):
                await result
        except Exception:
            pass

    def __repr__(self) -> str:
        return f"BrowserAsyncClient(impersonate={self._impersonate!r}, closed={self._closed})"
