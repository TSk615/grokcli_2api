"""Credential parsing for grok.com SSO accounts.

This module parses credentials only. Persistence must encrypt the returned raw
values before storing them; callers should use ``fingerprint`` as a stable,
non-secret source key.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Mapping


_TOKEN_KEYS = ("sso", "sso_token", "token")
_SSO_RW_KEYS = ("sso-rw", "sso_rw", "ssoRw")
_COOKIE_CONTAINER_KEYS = ("cookie", "cookies", "cookie_header")
_CLOUDFLARE_COOKIE_RE = re.compile(
    r"^(?:cf_clearance|__cf_bm|_cfuvid|cf_chl_[A-Za-z0-9_-]+)$"
)
_COOKIE_NAME_RE = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


def _clean_cookie_value(value: Any, *, field_name: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise ValueError(f"{field_name} must not be empty")
    if any(char in cleaned for char in (";", "\r", "\n", "\x00")):
        raise ValueError(f"{field_name} contains unsafe cookie characters")
    return cleaned


def _clean_cookie_name(value: Any) -> str:
    cleaned = str(value or "").strip()
    if not _COOKIE_NAME_RE.fullmatch(cleaned):
        raise ValueError("invalid cookie name")
    return cleaned


def _parse_cookie_header(raw: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for segment in raw.split(";"):
        segment = segment.strip()
        if not segment:
            continue
        name, separator, value = segment.partition("=")
        if not separator:
            continue
        name = _clean_cookie_name(name)
        cookies[name] = _clean_cookie_value(value, field_name=name)
    return cookies


def _parse_cookie_container(raw: Any) -> dict[str, Any]:
    if isinstance(raw, str):
        return _parse_cookie_header(raw)
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, (list, tuple)):
        cookies: dict[str, Any] = {}
        for item in raw:
            if not isinstance(item, Mapping):
                continue
            name = item.get("name")
            if name is None or item.get("value") is None:
                continue
            cookies[_clean_cookie_name(name)] = item["value"]
        return cookies
    return {}


def sanitize_cloudflare_cookies(raw: Mapping[str, Any] | None) -> dict[str, str]:
    """Keep only recognized Cloudflare cookie names and injection-safe values."""

    sanitized: dict[str, str] = {}
    for raw_name, raw_value in (raw or {}).items():
        name = _clean_cookie_name(raw_name)
        if not _CLOUDFLARE_COOKIE_RE.fullmatch(name):
            continue
        sanitized[name] = _clean_cookie_value(raw_value, field_name=name)
    return sanitized


@dataclass(frozen=True, slots=True, repr=False)
class WebCredential:
    """In-memory Grok Web credential with deliberately redacted representation."""

    sso: str = field(repr=False)
    sso_rw: str = field(repr=False)
    cloudflare_cookies: Mapping[str, str] = field(default_factory=dict, repr=False)
    label: str | None = None

    def __post_init__(self) -> None:
        clean_sso = _clean_cookie_value(self.sso, field_name="sso")
        clean_rw = _clean_cookie_value(self.sso_rw, field_name="sso-rw")
        clean_cf = sanitize_cloudflare_cookies(self.cloudflare_cookies)
        object.__setattr__(self, "sso", clean_sso)
        object.__setattr__(self, "sso_rw", clean_rw)
        object.__setattr__(self, "cloudflare_cookies", clean_cf)

    @property
    def fingerprint(self) -> str:
        digest = hashlib.sha256(self.sso.encode("utf-8")).hexdigest()
        return f"web-sso:{digest[:16]}"

    def __repr__(self) -> str:
        return (
            "WebCredential("
            f"fingerprint={self.fingerprint!r}, "
            f"label={self.label!r}, "
            f"cloudflare_cookies={sorted(self.cloudflare_cookies)!r}"
            ")"
        )

    __str__ = __repr__


def _first(mapping: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None and str(value).strip():
            return value
    return None


def _credential_from_mapping(raw: Mapping[str, Any]) -> WebCredential:
    cookies: dict[str, Any] = {}
    for key in _COOKIE_CONTAINER_KEYS:
        container = raw.get(key)
        cookies.update(_parse_cookie_container(container))

    # Top-level cookie keys are common in exported browser JSON.
    for key, value in raw.items():
        if key in ("sso", "sso-rw", "cf_clearance", "__cf_bm") or key.startswith(
            "cf_chl_"
        ):
            cookies[key] = value

    sso = _first(raw, _TOKEN_KEYS) or cookies.get("sso")
    if sso is None:
        raise ValueError("credential does not contain an sso token")
    sso_rw = _first(raw, _SSO_RW_KEYS) or cookies.get("sso-rw") or sso
    label = raw.get("label") or raw.get("email") or raw.get("name")
    return WebCredential(
        sso=str(sso),
        sso_rw=str(sso_rw),
        cloudflare_cookies=sanitize_cloudflare_cookies(cookies),
        label=str(label).strip() if label is not None else None,
    )


def _credential_from_text(raw: str) -> WebCredential:
    text = raw.strip()
    if not text:
        raise ValueError("credential is empty")
    if "=" in text and ("sso=" in text.lower() or ";" in text):
        return _credential_from_mapping({"cookie": text})
    return WebCredential(sso=text, sso_rw=text)


def _parse_item(raw: Any) -> list[WebCredential]:
    if isinstance(raw, WebCredential):
        return [raw]
    if isinstance(raw, Mapping):
        for collection_key in ("accounts", "credentials", "items"):
            collection = raw.get(collection_key)
            if isinstance(collection, list):
                return [credential for item in collection for credential in _parse_item(item)]
        return [_credential_from_mapping(raw)]
    if isinstance(raw, (list, tuple)):
        # Cookie-editor/browser exports commonly use [{"name": ..., "value": ...}].
        exported_cookies = _parse_cookie_container(raw)
        if "sso" in exported_cookies:
            return [_credential_from_mapping({"cookies": exported_cookies})]
        return [credential for item in raw for credential in _parse_item(item)]
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8-sig")
    if isinstance(raw, str):
        return [_credential_from_text(raw)]
    raise TypeError("credential payload must be text, bytes, an object, or a list")


def parse_web_credentials(payload: Any) -> list[WebCredential]:
    """Parse pasted SSO text, TXT bytes, cookie strings, or JSON exports.

    Plain TXT accepts one token or cookie header per non-comment line. Duplicate
    SSO tokens are removed while preserving input order.
    """

    if isinstance(payload, bytes):
        payload = payload.decode("utf-8-sig")
    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            return []
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            lines = [
                line.strip()
                for line in text.splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            ]
            # A Cookie header may contain spaces but normally occupies one line.
            parsed = [credential for line in lines for credential in _parse_item(line)]
        else:
            parsed = _parse_item(decoded)
    else:
        parsed = _parse_item(payload)

    unique: list[WebCredential] = []
    seen: set[str] = set()
    for credential in parsed:
        if credential.fingerprint in seen:
            continue
        seen.add(credential.fingerprint)
        unique.append(credential)
    return unique
