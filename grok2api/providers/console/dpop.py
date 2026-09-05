"""RFC 9449 DPoP primitives and a bounded in-memory session cache."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import threading
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Callable
from urllib.parse import urlsplit, urlunsplit

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature


REFRESH_SKEW = timedelta(seconds=20)
MAX_TOKEN_LIFETIME = timedelta(hours=1)


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decode_b64url(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _json_segment(value: object) -> str:
    return _b64url(json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8"))


def public_jwk(key: ec.EllipticCurvePrivateKey | ec.EllipticCurvePublicKey) -> dict[str, str]:
    public = key.public_key() if isinstance(key, ec.EllipticCurvePrivateKey) else key
    if not isinstance(public.curve, ec.SECP256R1):
        raise ValueError("DPoP requires a P-256 key")
    numbers = public.public_numbers()
    return {
        "kty": "EC",
        "crv": "P-256",
        "x": _b64url(numbers.x.to_bytes(32, "big")),
        "y": _b64url(numbers.y.to_bytes(32, "big")),
    }


def jwk_thumbprint(jwk: dict[str, str]) -> str:
    required = {name: str(jwk.get(name, "")) for name in ("crv", "kty", "x", "y")}
    if required["kty"] != "EC" or required["crv"] != "P-256" or not required["x"] or not required["y"]:
        raise ValueError("invalid P-256 public JWK")
    return _b64url(hashlib.sha256(json.dumps(required, separators=(",", ":"), sort_keys=True).encode()).digest())


def normalized_htu(url: str) -> str:
    parsed = urlsplit(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("DPoP request URL must be absolute")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path or "/", "", ""))


def access_token_hash(access_token: str) -> str:
    return _b64url(hashlib.sha256(access_token.encode("utf-8")).digest())


def create_dpop_proof(
    private_key: ec.EllipticCurvePrivateKey,
    access_token: str,
    method: str,
    url: str,
    *,
    now: datetime | None = None,
    clock_skew: timedelta = timedelta(0),
    jti: str | None = None,
) -> str:
    if not access_token.strip() or not method.strip():
        raise ValueError("DPoP proof requires a token and method")
    issued_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc) + clock_skew
    header = {"alg": "ES256", "typ": "dpop+jwt", "jwk": public_jwk(private_key)}
    claims = {
        "jti": jti or str(uuid.uuid4()),
        "htm": method.upper(),
        "htu": normalized_htu(url),
        "iat": math.floor(issued_at.timestamp()),
        "ath": access_token_hash(access_token),
    }
    signing_input = f"{_json_segment(header)}.{_json_segment(claims)}".encode("ascii")
    der_signature = private_key.sign(signing_input, ec.ECDSA(hashlib_to_sha256()))
    r, s = decode_dss_signature(der_signature)
    signature = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    return signing_input.decode("ascii") + "." + _b64url(signature)


def hashlib_to_sha256():
    # Kept as a tiny function so importing this module has no mutable hash state.
    from cryptography.hazmat.primitives import hashes

    return hashes.SHA256()


def parse_access_token_claims(token: str) -> tuple[datetime, str]:
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("invalid Console DPoP access token format")
    try:
        payload = json.loads(_decode_b64url(parts[1]))
        expiry = int(payload["exp"])
        thumbprint = str(payload["cnf"]["jkt"]).strip()
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        raise ValueError("invalid Console DPoP access token claims") from exc
    if expiry <= 0 or not thumbprint:
        raise ValueError("invalid Console DPoP access token claims")
    return datetime.fromtimestamp(expiry, timezone.utc), thumbprint


def clock_skew_from_date_header(
    date_header: str | None,
    local_before: datetime,
    local_after: datetime,
) -> timedelta:
    if not (date_header or "").strip():
        return timedelta(0)
    try:
        server = parsedate_to_datetime(str(date_header)).astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return timedelta(0)
    before = local_before.astimezone(timezone.utc)
    after = local_after.astimezone(timezone.utc)
    if after < before:
        after = before
    midpoint = before + (after - before) / 2
    seconds = (server - midpoint).total_seconds()
    rounded = math.floor(seconds + 0.5) if seconds >= 0 else math.ceil(seconds - 0.5)
    return timedelta(seconds=rounded)


@dataclass(frozen=True, slots=True)
class DPoPSession:
    access_token: str = field(repr=False)
    private_key: ec.EllipticCurvePrivateKey = field(repr=False)
    public_jwk: dict[str, str]
    expires_at: datetime
    clock_skew: timedelta = timedelta(0)


class DPoPSessionCache:
    """Thread-safe LRU cache with expiry skew and token-safe invalidation."""

    def __init__(
        self,
        max_entries: int = 4096,
        *,
        now: Callable[[], datetime] | None = None,
        refresh_skew: timedelta = REFRESH_SKEW,
    ) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._refresh_skew = refresh_skew
        self._entries: OrderedDict[str, DPoPSession] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: str) -> DPoPSession | None:
        with self._lock:
            session = self._entries.get(key)
            if session is None:
                return None
            if session.expires_at <= self._now().astimezone(timezone.utc) + self._refresh_skew:
                del self._entries[key]
                return None
            self._entries.move_to_end(key, last=False)
            return session

    def put(self, key: str, session: DPoPSession) -> None:
        with self._lock:
            self._entries[key] = session
            self._entries.move_to_end(key, last=False)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=True)

    def invalidate(self, key: str, access_token: str | None = None) -> bool:
        with self._lock:
            session = self._entries.get(key)
            if session is None or (access_token is not None and session.access_token != access_token):
                return False
            del self._entries[key]
            return True

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


def session_cache_key(base_url: str, account_id: str | int, egress_identity: str, sso_token: str) -> str:
    fingerprint = hashlib.sha256(sso_token.encode("utf-8")).hexdigest()
    return f"{base_url.rstrip('/')}|{account_id}|{egress_identity}|{fingerprint}"
