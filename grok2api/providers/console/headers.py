"""Browser-shaped headers used by console.x.ai."""

from __future__ import annotations

import re

from grok2api.upstream.browser_transport import DEFAULT_BROWSER_USER_AGENT

DEFAULT_USER_AGENT = DEFAULT_BROWSER_USER_AGENT


def _clean_cookie_fragment(value: str) -> str:
    return value.replace("\r", "").replace("\n", "").replace("\x00", "").strip().strip(";")


def _cloudflare_cookie_fragment(value: str) -> str:
    allowed: list[str] = []
    for fragment in _clean_cookie_fragment(value).split(";"):
        name, separator, cookie_value = fragment.strip().partition("=")
        if not separator or not cookie_value.strip():
            continue
        if name.strip().lower() in {"cf_clearance", "__cf_bm", "_cfuvid"}:
            allowed.append(f"{name.strip()}={cookie_value.strip()}")
    return "; ".join(allowed)


def build_sso_cookie(sso_token: str, cloudflare_cookies: str = "") -> str:
    token = _clean_cookie_fragment(sso_token).split(";", 1)[0]
    if not token:
        raise ValueError("Grok Console SSO token is empty")
    cookies = [f"sso={token}", f"sso-rw={token}"]
    clearance = _cloudflare_cookie_fragment(cloudflare_cookies)
    if clearance:
        cookies.append(clearance)
    return "; ".join(cookies)


def browser_headers(
    sso_token: str,
    *,
    cloudflare_cookies: str = "",
    user_agent: str = DEFAULT_USER_AGENT,
) -> dict[str, str]:
    agent = user_agent.strip() or DEFAULT_USER_AGENT
    result = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Cookie": build_sso_cookie(sso_token, cloudflare_cookies),
        "Origin": "https://console.x.ai",
        "Referer": "https://console.x.ai/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Priority": "u=1, i",
        "Pragma": "no-cache",
        "User-Agent": agent,
    }
    match = re.search(r"(?:Chrome|Chromium)/(\d+)", agent)
    if match:
        major = match.group(1)
        platform = "Windows" if "Windows" in agent else "macOS" if "Macintosh" in agent else "Linux"
        result.update(
            {
                "Sec-CH-UA": f'"Chromium";v="{major}", "Not:A-Brand";v="24", "Google Chrome";v="{major}"',
                "Sec-CH-UA-Mobile": "?0",
                "Sec-CH-UA-Platform": f'"{platform}"',
            }
        )
    return result
