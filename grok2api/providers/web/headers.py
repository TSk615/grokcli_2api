"""Header and Cookie construction for Grok Web requests."""

from __future__ import annotations

from collections.abc import Mapping

from grok2api.upstream.browser_transport import DEFAULT_BROWSER_USER_AGENT

from .auth import WebCredential


DEFAULT_USER_AGENT = DEFAULT_BROWSER_USER_AGENT

_PROTECTED_HEADERS = {"cookie", "host", "content-length", "transfer-encoding"}


def build_cookie_header(credential: WebCredential) -> str:
    pairs = [("sso", credential.sso), ("sso-rw", credential.sso_rw)]
    pairs.extend(sorted(credential.cloudflare_cookies.items()))
    return "; ".join(f"{name}={value}" for name, value in pairs)


def build_web_headers(
    credential: WebCredential,
    *,
    user_agent: str = DEFAULT_USER_AGENT,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build browser-like headers while preventing credential overrides."""

    if any(char in user_agent for char in ("\r", "\n", "\x00")):
        raise ValueError("user_agent contains unsafe characters")
    headers = {
        "Accept": "application/json, text/event-stream",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "Origin": "https://grok.com",
        "Referer": "https://grok.com/",
        "User-Agent": user_agent,
        "Cookie": build_cookie_header(credential),
    }
    for name, value in (extra or {}).items():
        if name.lower() in _PROTECTED_HEADERS:
            raise ValueError(f"cannot override protected header: {name}")
        if any(char in str(value) for char in ("\r", "\n", "\x00")):
            raise ValueError(f"header {name} contains unsafe characters")
        headers[str(name)] = str(value)
    return headers
