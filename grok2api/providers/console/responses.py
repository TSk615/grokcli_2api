"""OpenAI Responses boundary for the Grok Console transport."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx

from grok2api.upstream.browser_transport import BrowserTransportError

from ..types import Capability, ModelRoute, ProviderName
from .auth import ConsoleCredential
from .client import ConsoleDPoPClient
from .errors import ConsoleTokenError
from .headers import DEFAULT_USER_AGENT
from .models import DEFAULT_WEB_SEARCH_MODELS


def _with_default_web_search(body: Mapping[str, Any], upstream_model: str) -> dict[str, Any]:
    """Copy a Responses payload and enable web search for opted-in models."""

    payload = dict(body)
    payload["model"] = upstream_model
    if upstream_model.strip().lower() not in DEFAULT_WEB_SEARCH_MODELS:
        return payload

    tools = payload.get("tools")
    if tools is None:
        payload["tools"] = [{"type": "web_search"}]
        return payload
    if not isinstance(tools, list):
        # Preserve malformed caller input so the upstream API can report the
        # normal validation error instead of silently changing its meaning.
        return payload
    if any(
        isinstance(tool, Mapping)
        and str(tool.get("type") or "").strip().lower() == "web_search"
        for tool in tools
    ):
        return payload
    payload["tools"] = [*tools, {"type": "web_search"}]
    return payload


class ConsoleResponsesError(RuntimeError):
    """Sanitized boundary failure which never retains request credentials."""


class ConsoleResponsesTransport:
    """Validate a Console route and forward a stateless Responses request."""

    __slots__ = ("_client",)

    def __init__(self, client: ConsoleDPoPClient) -> None:
        self._client = client

    def __repr__(self) -> str:
        return "ConsoleResponsesTransport()"

    async def forward(
        self,
        account_id: str | int,
        credential: ConsoleCredential,
        body: Mapping[str, Any],
        route: ModelRoute,
        *,
        egress_identity: str = "",
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> httpx.Response:
        """Return an unconsumed response; the caller must call ``aclose()``."""

        try:
            provider = ProviderName.normalize(route.provider)
        except (AttributeError, ValueError):
            raise ConsoleResponsesError("Responses route is invalid") from None
        if provider is not ProviderName.CONSOLE:
            raise ConsoleResponsesError("Responses route must use grok_console")
        if route.capability is not Capability.RESPONSES:
            raise ConsoleResponsesError("Console route must have responses capability")
        if not route.upstream_model.strip():
            raise ConsoleResponsesError("Console route is missing an upstream model")
        if not isinstance(credential, ConsoleCredential) or credential.provider != ProviderName.CONSOLE.value:
            raise ConsoleResponsesError("A Grok Console credential is required")
        if not isinstance(body, Mapping):
            raise ConsoleResponsesError("OpenAI Responses body must be an object")

        # The selected route is authoritative. In particular, never allow the
        # client's model field to cross provider/model boundaries.
        payload = _with_default_web_search(body, route.upstream_model)
        failed = False
        try:
            response = await self._client.request_stream(
                account_id,
                credential.sso_token,
                "POST",
                "/responses",
                egress_identity=egress_identity,
                cloudflare_cookies=credential.cloudflare_cookies,
                user_agent=user_agent,
                headers={"Content-Type": "application/json", "Accept": "*/*"},
                json=payload,
            )
        except (
            httpx.HTTPError,
            BrowserTransportError,
            ConsoleTokenError,
            TypeError,
            ValueError,
        ):
            # Leave the exception scope before raising. This avoids retaining
            # even a suppressed __context__: an HTTPX request object contains
            # Cookie, Authorization and DPoP headers.
            failed = True
        if failed:
            raise ConsoleResponsesError("Grok Console Responses request failed")
        content_type = str(response.headers.get("content-type") or "").lower()
        if "text/html" in content_type:
            await response.aclose()
            raise ConsoleResponsesError("Grok Console browser session was challenged")
        return response
