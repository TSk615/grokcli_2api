"""Provider-neutral error classification for Grok Console responses."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from enum import Enum


class ConsoleErrorKind(str, Enum):
    INVALID_REQUEST = "invalid_request"
    AUTHENTICATION = "authentication"
    DPOP_SESSION = "dpop_session"
    ACCOUNT_BLOCKED = "account_blocked"
    EGRESS_CHALLENGE = "egress_challenge"
    QUOTA = "quota"
    RATE_LIMIT = "rate_limit"
    NOT_FOUND = "not_found"
    UPSTREAM_TRANSIENT = "upstream_transient"
    UPSTREAM = "upstream"


@dataclass(frozen=True, slots=True)
class ErrorClassification:
    kind: ConsoleErrorKind
    retryable: bool = False
    invalidate_dpop: bool = False
    credential_invalid: bool = False
    egress_failure: bool = False
    retry_after_seconds: int | None = None


def _body_text(body: bytes | str | None) -> str:
    if isinstance(body, bytes):
        return body[:16_384].decode("utf-8", errors="ignore").lower()
    return str(body or "")[:16_384].lower()


def _retry_after(value: str | None, now: datetime | None) -> int | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return max(0, int(float(raw)))
    except ValueError:
        try:
            target = parsedate_to_datetime(raw).astimezone(timezone.utc)
        except (TypeError, ValueError, OverflowError):
            return None
        reference = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        return max(0, int((target - reference).total_seconds()))


def classify_error(
    status_code: int,
    *,
    body: bytes | str | None = None,
    retry_after: str | None = None,
    now: datetime | None = None,
) -> ErrorClassification:
    text = _body_text(body)
    if status_code in (400, 422):
        return ErrorClassification(ConsoleErrorKind.INVALID_REQUEST)
    if status_code == 401:
        return ErrorClassification(
            ConsoleErrorKind.AUTHENTICATION,
            invalidate_dpop=True,
            credential_invalid=True,
        )
    if status_code == 403:
        if any(marker in text for marker in ("dpop proof", "dpop_proof", "invalid dpop", "dpop required")):
            return ErrorClassification(ConsoleErrorKind.DPOP_SESSION, retryable=True, invalidate_dpop=True)
        if any(marker in text for marker in ("account suspended", "account disabled", "account blocked", "sso disabled")):
            return ErrorClassification(ConsoleErrorKind.ACCOUNT_BLOCKED, credential_invalid=True)
        return ErrorClassification(
            ConsoleErrorKind.EGRESS_CHALLENGE,
            retryable=True,
            egress_failure=True,
        )
    if status_code == 404:
        return ErrorClassification(ConsoleErrorKind.NOT_FOUND)
    if status_code == 402:
        return ErrorClassification(ConsoleErrorKind.QUOTA, retryable=True)
    if status_code == 429:
        return ErrorClassification(
            ConsoleErrorKind.RATE_LIMIT,
            retryable=True,
            retry_after_seconds=_retry_after(retry_after, now),
        )
    if status_code in (408, 425) or status_code >= 500:
        return ErrorClassification(ConsoleErrorKind.UPSTREAM_TRANSIENT, retryable=True)
    return ErrorClassification(ConsoleErrorKind.UPSTREAM)


class ConsoleTokenError(RuntimeError):
    """Sanitized token-mint failure; response bodies are never retained."""

    def __init__(self, status_code: int, message: str = "Console DPoP token request failed") -> None:
        self.status_code = status_code
        super().__init__(f"{message} ({status_code})")


class ConsoleEgressChallengeError(ConsoleTokenError):
    """Cloudflare/browser challenge; never marks the SSO credential invalid."""

    def __init__(self, status_code: int) -> None:
        super().__init__(status_code, "Console browser session was challenged")
