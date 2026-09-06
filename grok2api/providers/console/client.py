"""Minimal async Console client with DPoP minting, caching, and 401 refresh."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib.parse import urlsplit

import httpx
from cryptography.hazmat.primitives.asymmetric import ec

from grok2api.upstream.browser_transport import is_cloudflare_challenge

from .dpop import (
    MAX_TOKEN_LIFETIME,
    REFRESH_SKEW,
    DPoPSession,
    DPoPSessionCache,
    clock_skew_from_date_header,
    create_dpop_proof,
    jwk_thumbprint,
    parse_access_token_claims,
    public_jwk,
    session_cache_key,
)
from .errors import ConsoleEgressChallengeError, ConsoleTokenError
from .headers import DEFAULT_USER_AGENT, browser_headers


@dataclass(frozen=True, slots=True)
class ConsoleDPoPConfig:
    base_url: str = "https://console.x.ai"
    max_cache_entries: int = 4096
    refresh_skew_seconds: int = 20
    max_token_lifetime_seconds: int = 3600

    def __post_init__(self) -> None:
        parsed = urlsplit(self.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Console base_url must be an absolute HTTP(S) URL")
        if self.max_cache_entries <= 0:
            raise ValueError("max_cache_entries must be positive")
        if self.refresh_skew_seconds < 0 or self.max_token_lifetime_seconds <= 0:
            raise ValueError("invalid DPoP lifetime configuration")


class ConsoleDPoPClient:
    """Uses an injected ``httpx.AsyncClient``; it never owns or closes it."""

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        config: ConsoleDPoPConfig | None = None,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._http = http_client
        self.config = config or ConsoleDPoPConfig()
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._cache = DPoPSessionCache(
            self.config.max_cache_entries,
            now=self._now,
            refresh_skew=timedelta(seconds=self.config.refresh_skew_seconds),
        )
        # Fixed lock stripes coalesce concurrent token mints without retaining
        # one cache-key fingerprint for every account ever seen.
        stripe_count = min(self.config.max_cache_entries, 256)
        self._locks = tuple(asyncio.Lock() for _ in range(stripe_count))

    @property
    def cached_session_count(self) -> int:
        return len(self._cache)

    def _endpoint(self, path_or_url: str) -> str:
        parsed = urlsplit(path_or_url)
        if parsed.scheme and parsed.netloc:
            return path_or_url
        base = self.config.base_url.rstrip("/")
        path = "/" + path_or_url.strip().lstrip("/")
        if base.endswith("/v1"):
            return base + path
        return base + "/v1" + path

    def _key_lock(self, key: str) -> asyncio.Lock:
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        return self._locks[int.from_bytes(digest[:8], "big") % len(self._locks)]

    async def _get_session(
        self,
        key: str,
        sso_token: str,
        cloudflare_cookies: str,
        user_agent: str,
    ) -> DPoPSession:
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        lock = self._key_lock(key)
        async with lock:
            cached = self._cache.get(key)
            if cached is not None:
                return cached
            session = await self._mint_session(sso_token, cloudflare_cookies, user_agent)
            self._cache.put(key, session)
            return session

    async def _mint_session(
        self,
        sso_token: str,
        cloudflare_cookies: str,
        user_agent: str,
    ) -> DPoPSession:
        private_key = ec.generate_private_key(ec.SECP256R1())
        jwk = public_jwk(private_key)
        headers = browser_headers(
            sso_token,
            cloudflare_cookies=cloudflare_cookies,
            user_agent=user_agent,
        )
        headers["Content-Type"] = "application/json"
        local_before = self._now().astimezone(timezone.utc)
        response = await self._http.request(
            "POST",
            self._endpoint("/dpop/token"),
            json={"jwk": jwk},
            headers=headers,
        )
        local_after = self._now().astimezone(timezone.utc)
        if is_cloudflare_challenge(
            response.status_code,
            response.headers,
            response.content,
        ):
            await response.aclose()
            raise ConsoleEgressChallengeError(response.status_code)
        if not 200 <= response.status_code < 300:
            await response.aclose()
            raise ConsoleTokenError(response.status_code)
        try:
            payload = response.json()
            access_token = str(payload["access_token"])
            token_type = str(payload["token_type"])
            expires_in = int(payload["expires_in"])
        except (ValueError, TypeError, KeyError) as exc:
            raise ConsoleTokenError(response.status_code, "Invalid Console DPoP token response") from exc
        finally:
            await response.aclose()
        max_lifetime = min(
            timedelta(seconds=self.config.max_token_lifetime_seconds),
            MAX_TOKEN_LIFETIME,
        )
        if not access_token.strip() or token_type.lower().strip() != "dpop":
            raise ConsoleTokenError(response.status_code, "Invalid Console DPoP token response")
        if expires_in <= 0 or timedelta(seconds=expires_in) > max_lifetime:
            raise ConsoleTokenError(response.status_code, "Invalid Console DPoP token lifetime")
        try:
            token_expiry, token_thumbprint = parse_access_token_claims(access_token)
        except ValueError as exc:
            raise ConsoleTokenError(response.status_code, "Invalid Console DPoP access token") from exc
        if token_thumbprint != jwk_thumbprint(jwk):
            raise ConsoleTokenError(response.status_code, "Console DPoP token key binding mismatch")
        now = self._now().astimezone(timezone.utc)
        expires_at = min(now + timedelta(seconds=expires_in), token_expiry)
        refresh_skew = max(REFRESH_SKEW, timedelta(seconds=self.config.refresh_skew_seconds))
        if expires_at <= now + refresh_skew:
            raise ConsoleTokenError(response.status_code, "Console DPoP token is expired or near expiry")
        return DPoPSession(
            access_token=access_token,
            private_key=private_key,
            public_jwk=jwk,
            expires_at=expires_at,
            clock_skew=clock_skew_from_date_header(response.headers.get("Date"), local_before, local_after),
        )

    async def request(
        self,
        account_id: str | int,
        sso_token: str,
        method: str,
        path_or_url: str,
        *,
        egress_identity: str = "",
        cloudflare_cookies: str = "",
        user_agent: str = DEFAULT_USER_AGENT,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send an authorized request, refreshing once after a 401 response."""

        return await self._request(
            account_id,
            sso_token,
            method,
            path_or_url,
            egress_identity=egress_identity,
            cloudflare_cookies=cloudflare_cookies,
            user_agent=user_agent,
            headers=headers,
            stream_response=False,
            **kwargs,
        )

    async def request_stream(
        self,
        account_id: str | int,
        sso_token: str,
        method: str,
        path_or_url: str,
        *,
        egress_identity: str = "",
        cloudflare_cookies: str = "",
        user_agent: str = DEFAULT_USER_AGENT,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send an authorized request and leave the final body unconsumed."""

        return await self._request(
            account_id,
            sso_token,
            method,
            path_or_url,
            egress_identity=egress_identity,
            cloudflare_cookies=cloudflare_cookies,
            user_agent=user_agent,
            headers=headers,
            stream_response=True,
            **kwargs,
        )

    async def _request(
        self,
        account_id: str | int,
        sso_token: str,
        method: str,
        path_or_url: str,
        *,
        egress_identity: str,
        cloudflare_cookies: str,
        user_agent: str,
        headers: dict[str, str] | None,
        stream_response: bool,
        **kwargs: Any,
    ) -> httpx.Response:

        endpoint = self._endpoint(path_or_url)
        cache_key = session_cache_key(
            self.config.base_url,
            account_id,
            egress_identity,
            sso_token,
            user_agent=user_agent,
            cloudflare_cookies=cloudflare_cookies,
        )
        for attempt in range(2):
            session = await self._get_session(
                cache_key,
                sso_token,
                cloudflare_cookies,
                user_agent,
            )
            request_headers = browser_headers(
                sso_token,
                cloudflare_cookies=cloudflare_cookies,
                user_agent=user_agent,
            )
            if headers:
                request_headers.update(headers)
            if urlsplit(endpoint).path.endswith("/responses"):
                request_headers["x-cluster"] = "https://us-east-1.api.x.ai"
            request_headers["Authorization"] = f"DPoP {session.access_token}"
            request_headers["DPoP"] = create_dpop_proof(
                session.private_key,
                session.access_token,
                method,
                endpoint,
                now=self._now(),
                clock_skew=session.clock_skew,
            )
            if stream_response:
                outbound = self._http.build_request(method.upper(), endpoint, headers=request_headers, **kwargs)
                response = await self._http.send(outbound, stream=True)
            else:
                response = await self._http.request(method.upper(), endpoint, headers=request_headers, **kwargs)
            if response.status_code != 401 or attempt == 1:
                return response
            await response.aread()
            await response.aclose()
            self._cache.invalidate(cache_key, session.access_token)
        raise RuntimeError("unreachable Console DPoP retry state")
