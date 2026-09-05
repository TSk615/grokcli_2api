"""Provider-specific HTTP failure classification for Grok Web."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping


class WebErrorKind(str, Enum):
    AUTH = "auth"
    EGRESS_CLOUDFLARE = "egress_cloudflare"
    RATE_LIMIT = "rate_limit"
    BAD_REQUEST = "bad_request"
    TRANSIENT = "transient"
    UPSTREAM = "upstream"


@dataclass(frozen=True, slots=True)
class WebError:
    kind: WebErrorKind
    status_code: int
    retryable: bool
    invalidates_credential: bool = False
    retry_after_seconds: float | None = None


def _retry_after(headers: Mapping[str, str] | None) -> float | None:
    if not headers:
        return None
    value = next(
        (raw for key, raw in headers.items() if str(key).lower() == "retry-after"), None
    )
    try:
        return max(0.0, float(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def classify_web_error(
    status_code: int, *, headers: Mapping[str, str] | None = None
) -> WebError:
    """Classify without consuming or retaining a potentially sensitive body.

    In particular, 403 is an egress/Cloudflare condition and must not disable
    an SSO account. Only 401 marks credentials as invalid.
    """

    status = int(status_code)
    if status == 401:
        return WebError(WebErrorKind.AUTH, status, False, invalidates_credential=True)
    if status == 403:
        return WebError(WebErrorKind.EGRESS_CLOUDFLARE, status, True)
    if status == 429:
        return WebError(
            WebErrorKind.RATE_LIMIT,
            status,
            True,
            retry_after_seconds=_retry_after(headers),
        )
    if status in (408, 425) or 500 <= status <= 599:
        return WebError(WebErrorKind.TRANSIENT, status, True)
    if 400 <= status <= 499:
        return WebError(WebErrorKind.BAD_REQUEST, status, False)
    return WebError(WebErrorKind.UPSTREAM, status, False)


def classify_web_response(response: object) -> WebError:
    """Classify an httpx-like response without importing httpx here."""

    return classify_web_error(
        int(getattr(response, "status_code")),
        headers=getattr(response, "headers", None),
    )
