"""Grok Web account preparation: terms, birth date, and NSFW preference.

This mirrors the current upstream Web protocol while keeping every request on
the caller-provided account-isolated browser transport.  Errors expose only a
coarse classification and HTTP status; response bodies and credentials are
never retained on exceptions or results.
"""

from __future__ import annotations

import base64
import json
import secrets
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlsplit

from .auth import WebCredential
from .headers import DEFAULT_USER_AGENT, build_cookie_header


CURRENT_TERMS_VERSION = 5
ACCOUNTS_BASE_URL = "https://accounts.x.ai"
DEFAULT_SIGNER_URL = "https://grok.wodf.de/sign"
ACCEPT_TERMS_FRAME = bytes((0, 0, 0, 0, 2, 0x10, 1))
ENABLE_NSFW_FRAME = bytes.fromhex(
    "00000000200a021001121a0a18616c776179735f73686f775f6e7366775f636f6e74656e74"
)
BODY_LIMIT = 64 * 1024
_META_DASH_TRANSLATION = str.maketrans({char: "-" for char in "‐‑‒–—―"})


@dataclass(frozen=True, slots=True)
class AccountSettingResult:
    phase: str
    success: bool
    status_code: int | None
    classification: str


class WebAccountSettingError(RuntimeError):
    def __init__(self, phase: str, classification: str, status_code: int | None = None) -> None:
        super().__init__(f"Web account setting failed: {phase}/{classification}")
        self.phase = phase
        self.classification = classification
        self.status_code = status_code


class _VerificationMetaParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.value = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "meta" or self.value:
            return
        values = {str(key).lower(): str(value or "").strip() for key, value in attrs}
        meta_name = values.get("name", "").lower().translate(_META_DASH_TRANSLATION)
        if meta_name == "grok-site-verification":
            self.value = values.get("content", "")


def _error_result(exc: WebAccountSettingError) -> AccountSettingResult:
    return AccountSettingResult(exc.phase, False, exc.status_code, exc.classification)


def random_adult_birth_date(*, today: date | None = None) -> date:
    """Return a uniformly selected calendar date for an age from 20 to 40."""

    now = today or datetime.now(timezone.utc).date()
    latest = date(now.year - 20, now.month, min(now.day, 28))
    earliest = date(now.year - 41, now.month, min(now.day, 28)).fromordinal(
        date(now.year - 41, now.month, min(now.day, 28)).toordinal() + 1
    )
    span = latest.toordinal() - earliest.toordinal() + 1
    return date.fromordinal(earliest.toordinal() + secrets.randbelow(span))


def _classification(status: int, body: bytes, *, phase: str) -> str:
    lowered = body[:BODY_LIMIT].lower()
    if b"birth-date-change-limit-reached" in lowered:
        return "birth_date_locked"
    if b"tos-accepted-version-required" in lowered or b"must accept tos" in lowered:
        return "terms_required"
    if status == 429:
        return "rate_limited"
    if status in {401, 403}:
        return "auth_or_challenge"
    if status >= 500:
        return "upstream_network"
    if status >= 400:
        return "upstream_rejected"
    if phase.endswith("grpc"):
        return "grpc_rejected"
    return "ok"


def _grpc_status(headers: Any, body: bytes) -> str:
    value = str(headers.get("grpc-status") or "").strip()
    if value:
        return value
    offset = 0
    while offset + 5 <= len(body):
        flag = body[offset]
        size = int.from_bytes(body[offset + 1 : offset + 5], "big")
        payload = body[offset + 5 : offset + 5 + size]
        if len(payload) != size:
            break
        if flag & 0x80:
            for line in payload.decode("utf-8", errors="ignore").splitlines():
                name, separator, item = line.partition(":")
                if separator and name.strip().lower() == "grpc-status":
                    return item.strip()
        offset += 5 + size
    return ""


class WebAccountSettingsClient:
    def __init__(
        self,
        client: Any,
        *,
        base_url: str = "https://grok.com",
        signer_url: str = DEFAULT_SIGNER_URL,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        self._client = client
        self.base_url = base_url.rstrip("/")
        self.signer_url = signer_url
        self.user_agent = user_agent
        parsed = urlsplit(signer_url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("Statsig signer URL must be an absolute HTTPS URL")
        self._statsig_cache: dict[str, str] = {}

    def _headers(
        self,
        credential: WebCredential,
        *,
        content_type: str,
        origin: str,
        referer: str,
        include_cloudflare: bool = True,
        grpc: bool = False,
        connect_es: bool = False,
        statsig: str = "",
    ) -> dict[str, str]:
        cookie = build_cookie_header(credential)
        if not include_cloudflare:
            cookie = f"sso={credential.sso}; sso-rw={credential.sso_rw}"
        headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Content-Type": content_type,
            "Cookie": cookie,
            "Origin": origin,
            "Pragma": "no-cache",
            "Priority": "u=1, i",
            "Referer": referer,
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": self.user_agent,
            "x-xai-request-id": str(uuid.uuid4()),
        }
        if grpc:
            headers["x-grpc-web"] = "1"
        if connect_es:
            headers["x-user-agent"] = "connect-es/2.1.1"
        if statsig:
            headers["x-statsig-id"] = statsig
        return headers

    async def _read_response(self, response: Any, phase: str, *, grpc: bool = False) -> AccountSettingResult:
        try:
            body = await response.aread()
            status = int(response.status_code)
            headers = response.headers
        finally:
            await response.aclose()
        if len(body) > BODY_LIMIT:
            return AccountSettingResult(phase, False, status, "response_too_large")
        category = _classification(status, body, phase=phase)
        if 200 <= status < 300 and grpc:
            grpc_value = _grpc_status(headers, body)
            if grpc_value and grpc_value != "0":
                category = _classification(status, body, phase=phase + "_grpc")
                if category == "ok":
                    category = f"grpc_status_{grpc_value}"
                return AccountSettingResult(phase, False, status, category)
        return AccountSettingResult(phase, 200 <= status < 300, status, category)

    async def _signed_statsig(self, credential: WebCredential, path: str) -> str:
        cached = self._statsig_cache.get(path)
        if cached:
            return cached
        headers = self._headers(
            credential,
            content_type="text/html",
            origin=self.base_url,
            referer=self.base_url + "/",
        )
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        response = await self._client.get(self.base_url + "/index", headers=headers, timeout=15.0)
        try:
            body = await response.aread()
            status = response.status_code
        finally:
            await response.aclose()
        if status == 404:
            response = await self._client.get(self.base_url + "/", headers=headers, timeout=15.0)
            try:
                body = await response.aread()
                status = response.status_code
            finally:
                await response.aclose()
        if not 200 <= status < 300 or len(body) > 4 * 1024 * 1024:
            raise WebAccountSettingError("statsig_meta", _classification(status, body, phase="statsig_meta"), status)
        parser = _VerificationMetaParser()
        parser.feed(body.decode("utf-8", errors="ignore"))
        if not parser.value:
            raise WebAccountSettingError("statsig_meta", "verification_meta_missing", status)
        signed = await self._client.post(
            self.signer_url,
            json={"method": "POST", "path": path, "environment": {"metaContent": parser.value}},
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=15.0,
        )
        try:
            signed_body = await signed.aread()
            signed_status = int(signed.status_code)
        finally:
            await signed.aclose()
        if not 200 <= signed_status < 300:
            raise WebAccountSettingError("statsig_signer", "signer_unavailable", signed_status)
        if len(signed_body) > BODY_LIMIT:
            raise WebAccountSettingError("statsig_signer", "response_too_large", signed_status)
        try:
            value = str(json.loads(signed_body)["x-statsig-id"]).strip()
            padding = "=" * (-len(value) % 4)
            if len(base64.b64decode(value + padding)) != 70:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            raise WebAccountSettingError("statsig_signer", "invalid_signature", signed_status) from None
        self._statsig_cache[path] = value
        return value

    async def accept_terms(self, credential: WebCredential) -> list[AccountSettingResult]:
        accounts_origin = ACCOUNTS_BASE_URL
        response = await self._client.post(
            accounts_origin + "/auth_mgmt.AuthManagement/SetTosAcceptedVersion",
            content=ACCEPT_TERMS_FRAME,
            headers=self._headers(
                credential,
                content_type="application/grpc-web+proto",
                origin=accounts_origin,
                referer=accounts_origin + "/accept-tos",
                include_cloudflare=False,
                grpc=True,
                connect_es=True,
            ),
            timeout=15.0,
        )
        first = await self._read_response(response, "account_terms", grpc=True)
        if not first.success:
            return [first]
        path = "/rest/auth/set-tos-accepted"
        try:
            signature = await self._signed_statsig(credential, path)
        except WebAccountSettingError as exc:
            return [first, _error_result(exc)]
        response = await self._client.post(
            self.base_url + path,
            json={"tosVersion": CURRENT_TERMS_VERSION},
            headers=self._headers(
                credential,
                content_type="application/json",
                origin=self.base_url,
                referer=self.base_url + "/",
                statsig=signature,
            ),
            timeout=15.0,
        )
        return [first, await self._read_response(response, "product_terms")]

    async def set_birth_date(self, credential: WebCredential, value: date) -> AccountSettingResult:
        path = "/rest/auth/set-birth-date"
        signature = await self._signed_statsig(credential, path)
        response = await self._client.post(
            self.base_url + path,
            json={"birthDate": value.isoformat() + "T16:00:00.000Z"},
            headers=self._headers(
                credential,
                content_type="application/json",
                origin=self.base_url,
                referer=self.base_url + "/",
                statsig=signature,
            ),
            timeout=15.0,
        )
        result = await self._read_response(response, "birth_date")
        if result.classification == "birth_date_locked":
            return AccountSettingResult(result.phase, True, result.status_code, result.classification)
        return result

    async def enable_nsfw(self, credential: WebCredential) -> AccountSettingResult:
        path = "/auth_mgmt.AuthManagement/UpdateUserFeatureControls"
        signature = await self._signed_statsig(credential, path)
        response = await self._client.post(
            self.base_url + path,
            content=ENABLE_NSFW_FRAME,
            headers=self._headers(
                credential,
                content_type="application/grpc-web+proto",
                origin=self.base_url,
                referer=self.base_url + "/",
                grpc=True,
                statsig=signature,
            ),
            timeout=15.0,
        )
        return await self._read_response(response, "nsfw", grpc=True)

    async def prepare(self, credential: WebCredential) -> list[AccountSettingResult]:
        results = await self.accept_terms(credential)
        if not results or not results[-1].success:
            return results
        try:
            birth = await self.set_birth_date(credential, random_adult_birth_date())
        except WebAccountSettingError as exc:
            results.append(_error_result(exc))
            return results
        results.append(birth)
        if not birth.success:
            return results
        try:
            results.append(await self.enable_nsfw(credential))
        except WebAccountSettingError as exc:
            results.append(_error_result(exc))
        return results


__all__ = [
    "AccountSettingResult",
    "CURRENT_TERMS_VERSION",
    "WebAccountSettingError",
    "WebAccountSettingsClient",
    "random_adult_birth_date",
]
