"""Secret-safe Resin forward-proxy bindings for provider accounts."""

from __future__ import annotations

import hashlib
import hmac
import os
import re
from dataclasses import dataclass, field
from urllib.parse import urlunsplit


_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_SAFE_HOST = re.compile(r"^[A-Za-z0-9_.-]{1,253}$")


class ResinConfigError(RuntimeError):
    """Raised when Resin mode is enabled without a safe complete config."""


def resin_enabled() -> bool:
    return str(os.getenv("GROK2API_RESIN_PROXY_ENABLED") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _config() -> tuple[str, int, str, str, str]:
    host = str(os.getenv("GROK2API_RESIN_PROXY_HOST") or "").strip()
    port_raw = str(os.getenv("GROK2API_RESIN_PROXY_PORT") or "2260").strip()
    platform = str(os.getenv("GROK2API_RESIN_PLATFORM") or "").strip()
    token = str(os.getenv("GROK2API_RESIN_TOKEN") or "").strip()
    identity_secret = str(
        os.getenv("GROK2API_RESIN_IDENTITY_SECRET")
        or os.getenv("GROK2API_SECRET_KEY")
        or ""
    ).strip()
    try:
        port = int(port_raw)
    except ValueError:
        raise ResinConfigError("Resin proxy port is invalid") from None
    if not _SAFE_HOST.fullmatch(host) or "://" in host or "@" in host:
        raise ResinConfigError("Resin proxy host is invalid")
    if not 1 <= port <= 65535:
        raise ResinConfigError("Resin proxy port is invalid")
    if not _SAFE_SEGMENT.fullmatch(platform):
        raise ResinConfigError("Resin platform is invalid")
    if not token or any(char in token for char in ("\r", "\n", "\x00")):
        raise ResinConfigError("Resin token is unavailable")
    if len(identity_secret) < 32 or any(
        char in identity_secret for char in ("\r", "\n", "\x00")
    ):
        raise ResinConfigError("Resin identity secret is unavailable")
    return host, port, platform, token, identity_secret


def derive_resin_account(
    provider: str,
    account_id: str,
    *,
    egress_identity: str | None = None,
) -> str:
    """Derive a stable opaque Resin Account without retaining source identity."""

    _host, _port, _platform, _token, identity_secret = _config()
    provider_name = str(provider or "").strip().lower()
    source = str(egress_identity or "").strip() or str(account_id or "").strip()
    if not provider_name or not source:
        raise ResinConfigError("Resin account identity is unavailable")
    digest = hmac.new(
        identity_secret.encode("utf-8"),
        f"grok2api/resin-account/v1\x00{provider_name}\x00{source}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"g2a-{digest[:32]}"


@dataclass(frozen=True, slots=True, repr=False)
class ResinProxyBinding:
    gateway_url: str
    platform: str
    account: str
    token: str = field(repr=False)
    identity_hash: str

    @property
    def username(self) -> str:
        return f"{self.platform}.{self.account}"

    @property
    def proxy_auth(self) -> tuple[str, str]:
        return self.username, self.token

    @property
    def cache_key(self) -> str:
        return f"resin:{self.identity_hash}"

    def __repr__(self) -> str:
        return f"ResinProxyBinding(identity_hash={self.identity_hash!r})"


def resin_binding_for_account(
    provider: str,
    account_id: str,
    *,
    egress_identity: str | None = None,
) -> ResinProxyBinding | None:
    if not resin_enabled():
        return None
    host, port, platform, token, _identity_secret = _config()
    account = derive_resin_account(
        provider,
        account_id,
        egress_identity=egress_identity,
    )
    gateway_url = urlunsplit(("http", f"{host}:{port}", "", "", ""))
    # The token fingerprint makes a rotation select a fresh bound transport,
    # while the independently derived Resin Account remains stable.
    token_fingerprint = hashlib.sha256(token.encode("utf-8")).hexdigest()
    identity_hash = hashlib.sha256(
        (
            f"{provider}\x00{account_id}\x00{account}\x00{gateway_url}"
            f"\x00{token_fingerprint}"
        ).encode("utf-8")
    ).hexdigest()[:24]
    return ResinProxyBinding(
        gateway_url=gateway_url,
        platform=platform,
        account=account,
        token=token,
        identity_hash=identity_hash,
    )


def resin_public_status() -> dict[str, object]:
    """Return only non-secret booleans and sanitized mode information."""

    enabled = resin_enabled()
    valid = False
    if enabled:
        try:
            _config()
            valid = True
        except ResinConfigError:
            valid = False
    return {
        "mode": "resin" if enabled else "direct",
        "enabled": enabled,
        "valid": valid,
        "direct_fallback": False if enabled else True,
    }
