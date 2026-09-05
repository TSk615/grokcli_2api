"""Low-level, injectable HTTP transport adapter for Grok Web.

Protocol conversion intentionally lives outside this adapter. A streaming
response is returned unread and must be closed by its caller.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import urljoin

import httpx

from ..types import Capability, ErrorKind, ModelRoute, ProviderName, ProviderStatus
from .auth import WebCredential
from .errors import WebErrorKind, classify_web_error
from .headers import DEFAULT_USER_AGENT, build_web_headers
from .models import WEB_MODELS


DEFAULT_CHAT_PATH = "/rest/app-chat/conversations/new"


class GrokWebAdapter:
    provider = ProviderName.WEB

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        base_url: str = "https://grok.com",
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        self._client = client
        self.base_url = base_url.rstrip("/") + "/"
        self.user_agent = user_agent

    def url_for(self, path: str) -> str:
        if not path:
            raise ValueError("path must not be empty")
        return urljoin(self.base_url, path.lstrip("/"))

    def model_routes(self) -> Iterable[ModelRoute]:
        """Expose the static Web catalog through the shared routing contract."""

        return [
            ModelRoute(
                public_model=model.id,
                provider=self.provider,
                upstream_model=model.upstream_mode,
                capability=Capability.CHAT,
                minimum_tier=model.minimum_tier.value,
                metadata={
                    "description": model.description,
                    "web_capabilities": sorted(
                        capability.value for capability in model.capabilities
                    ),
                },
            )
            for model in WEB_MODELS
        ]

    def classify_status(
        self,
        status_code: int,
        *,
        headers: dict[str, str] | None = None,
        body: str | bytes | None = None,
    ) -> ProviderStatus:
        """Map Web-specific semantics into the gateway's shared status type."""

        del body  # Never retain or inspect response bodies that may contain secrets.
        web_error = classify_web_error(status_code, headers=headers)
        if web_error.kind is WebErrorKind.AUTH:
            return ProviderStatus(
                ErrorKind.AUTH,
                retryable=web_error.retryable,
                account_scoped=True,
                invalidate_credential=web_error.invalidates_credential,
            )
        if web_error.kind is WebErrorKind.EGRESS_CLOUDFLARE:
            return ProviderStatus(ErrorKind.EGRESS, retryable=web_error.retryable)
        if web_error.kind is WebErrorKind.RATE_LIMIT:
            return ProviderStatus(
                ErrorKind.RATE_LIMIT,
                retryable=web_error.retryable,
                account_scoped=True,
                retry_after_seconds=web_error.retry_after_seconds,
            )
        if web_error.kind is WebErrorKind.BAD_REQUEST:
            return ProviderStatus(ErrorKind.REQUEST)
        if web_error.kind is WebErrorKind.TRANSIENT:
            return ProviderStatus(ErrorKind.TRANSIENT, retryable=web_error.retryable)
        return ProviderStatus(ErrorKind.UPSTREAM, retryable=web_error.retryable)

    async def request(
        self,
        method: str,
        path: str,
        credential: WebCredential,
        *,
        json: Any = None,
        content: bytes | str | None = None,
        headers: Mapping[str, str] | None = None,
        stream: bool = False,
        timeout: float | httpx.Timeout | None = None,
    ) -> httpx.Response:
        """Send a request and preserve the response stream when requested.

        The returned response is not automatically raised for status. This lets
        the gateway classify 401/403/429 with provider-specific semantics.
        When ``stream`` is true, callers own ``await response.aclose()``.
        """

        if self._client is None:
            raise RuntimeError("Grok Web transport requires an httpx.AsyncClient")

        request = self._client.build_request(
            method.upper(),
            self.url_for(path),
            headers=build_web_headers(
                credential, user_agent=self.user_agent, extra=headers
            ),
            json=json,
            content=content,
            timeout=timeout,
        )
        return await self._client.send(request, stream=stream)

    async def chat(
        self,
        payload: Mapping[str, Any],
        credential: WebCredential,
        *,
        stream: bool = False,
        path: str = DEFAULT_CHAT_PATH,
        headers: Mapping[str, str] | None = None,
        timeout: float | httpx.Timeout | None = None,
    ) -> httpx.Response:
        """Forward an already-normalized Grok Web chat payload."""

        return await self.request(
            "POST",
            path,
            credential,
            json=dict(payload),
            headers=headers,
            stream=stream,
            timeout=timeout,
        )
