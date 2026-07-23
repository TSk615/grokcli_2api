"""Load Grok session tokens from project data/auth.json (multi-account aware)."""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from grok2api.pool.auth_store import read_auth_entry, read_auth_map
from grok2api.config import (
    AUTH_FILE,
    CLI_VERSION,
    CLIENT_IDENTIFIER,
    CLIENT_SURFACE,
    TOKEN_REFRESH_SKEW,
)
from grok2api.upstream.oidc_auth import parse_expires_at


class AuthError(Exception):
    """Raised when credentials cannot be loaded or are expired."""


@dataclass
class GrokCredentials:
    token: str
    email: str | None = None
    user_id: str | None = None
    expires_at: float | None = None
    auth_key: str | None = None
    team_id: str | None = None
    refresh_token: str | None = None
    oidc_client_id: str | None = None

    @property
    def expired(self) -> bool:
        if self.expires_at is None:
            return False
        # refresh a bit early
        return time.time() >= (self.expires_at - 60)

    @property
    def needs_refresh(self) -> bool:
        if not self.refresh_token:
            return False
        if self.expires_at is None:
            return False
        return time.time() >= (self.expires_at - TOKEN_REFRESH_SKEW)


def _read_auth(path: Path) -> dict[str, Any]:
    # Prefer locked/PG store for default AUTH_FILE (multi-account safe on Linux).
    # Hybrid: PostgreSQL is primary; missing local auth.json must NOT block reads.
    if path == AUTH_FILE or path.resolve() == AUTH_FILE.resolve():
        data = read_auth_map(path)
        if data:
            return data
        if path.is_file():
            raise AuthError(f"Unexpected/empty auth store (file + DB) for {path}")
        raise AuthError(
            "No accounts in durable store. "
            "Use device-code login, register, or import a token first."
        )
    if not path.is_file():
        raise AuthError(
            f"Auth file not found: {path}. "
            "Use device-code login or import a token first."
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise AuthError(f"Failed to read {path}: {e}") from e
    if not isinstance(data, dict):
        raise AuthError(f"Unexpected auth.json format in {path}")
    return data


def _entry_to_creds(name: str, entry: dict[str, Any]) -> GrokCredentials:
    token = entry.get("key") or entry.get("access_token") or entry.get("token")
    if not token or not isinstance(token, str):
        raise AuthError(f"Entry {name} has no usable token")
    expires_at = parse_expires_at(entry.get("expires_at"), token)
    return GrokCredentials(
        token=token,
        email=entry.get("email"),
        user_id=entry.get("user_id") or entry.get("principal_id"),
        expires_at=expires_at,
        auth_key=name,
        team_id=entry.get("team_id"),
        refresh_token=entry.get("refresh_token")
        if isinstance(entry.get("refresh_token"), str)
        else None,
        oidc_client_id=entry.get("oidc_client_id"),
    )


def _iter_entries(data: dict[str, Any]) -> list[tuple[str, dict[str, Any], float]]:
    candidates: list[tuple[str, dict[str, Any], float]] = []
    for key, value in data.items():
        if not isinstance(value, dict):
            continue
        token = value.get("key") or value.get("access_token") or value.get("token")
        if not token or not isinstance(token, str):
            continue
        exp = parse_expires_at(value.get("expires_at"), token)
        exp_f = float(exp) if exp is not None else 0.0
        candidates.append((key, value, exp_f))
    return candidates


def _pick_entry(data: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """
    auth.json keys look like:
      - https://auth.x.ai::<user_id>     (multi-account)
      - https://auth.x.ai::<client_id>   (legacy Grok CLI single slot)
    Pick any non-expired entry (newest expires_at first). Pool rotation
    is handled by account_pool — this is only a fallback for status checks.
    """
    candidates = _iter_entries(data)
    if not candidates:
        raise AuthError(
            f"No usable token in {AUTH_FILE}. Login or import a token first."
        )

    now = time.time()
    live = [c for c in candidates if c[2] == 0.0 or c[2] > now]
    pool = live or candidates
    pool.sort(key=lambda c: c[2], reverse=True)
    name, entry, _ = pool[0]
    return name, entry


# Short process-local cache for request-path account picks. Large pools spend
# multi-seconds re-hydrating 1k+ credentials on every request otherwise.
_live_creds_cache_lock = threading.RLock()
_live_creds_cache: dict[str, Any] = {
    "at": 0.0,
    "path": None,
    "include_expired": None,
    "creds": None,
}
_LIVE_CREDS_CACHE_TTL = float(os.getenv("GROK2API_LIVE_CREDS_CACHE_TTL", "2.0") or 2.0)


def invalidate_live_credentials_cache() -> None:
    with _live_creds_cache_lock:
        _live_creds_cache["at"] = 0.0
        _live_creds_cache["creds"] = None


def get_cached_live_credentials(
    path: Path | None = None,
    *,
    include_expired: bool = False,
) -> list[GrokCredentials] | None:
    """Return warm process-local live-creds cache only (no rebuild / no IO).

    Used by sticky TTFT fast path so multi-turn picks never pay a cold full-pool
    scan just to assemble optional failover backups.
    """
    path = path or AUTH_FILE
    now = time.time()
    with _live_creds_cache_lock:
        if (
            _live_creds_cache.get("creds") is not None
            and _live_creds_cache.get("path") == str(path)
            and bool(_live_creds_cache.get("include_expired")) == bool(include_expired)
            and now - float(_live_creds_cache.get("at") or 0.0)
            < max(0.2, _LIVE_CREDS_CACHE_TTL)
        ):
            return list(_live_creds_cache["creds"])
    return None


def list_live_credentials(
    path: Path | None = None,
    *,
    include_expired: bool = False,
    auto_refresh: bool = True,
) -> list[GrokCredentials]:
    """Return all accounts with tokens (PG primary; auth.json only in file mode).

    Do not gate on AUTH_FILE existence: hybrid/PG stores accounts in the DB.
    Short-circuiting on missing auth.json made the pool report zero live
    credentials after register/import wrote only to PostgreSQL.
    """
    path = path or AUTH_FILE
    # Request path never network-refreshes here; cache by include_expired only.
    cached = get_cached_live_credentials(path, include_expired=include_expired)
    if cached is not None:
        return cached
    now = time.time()
    with _live_creds_cache_lock:
        if (
            _live_creds_cache.get("creds") is not None
            and _live_creds_cache.get("path") == str(path)
            and bool(_live_creds_cache.get("include_expired")) == bool(include_expired)
            and now - float(_live_creds_cache.get("at") or 0.0) < max(0.2, _LIVE_CREDS_CACHE_TTL)
        ):
            # Return a shallow copy so callers can reorder safely.
            return list(_live_creds_cache["creds"])

    try:
        data = _read_auth(path)
    except AuthError:
        return []

    # IMPORTANT: never fan-out refresh across the whole pool here.
    # With 700+ accounts this used to open hundreds of OIDC calls + rewrite
    # auth.json per account and freeze WSL. Background token_maintainer owns
    # bulk refresh. Request path must stay O(n) pure CPU over the map — any
    # network refresh here directly inflates TTFT / first-token latency.
    out: list[GrokCredentials] = []
    for name, entry, _exp in _iter_entries(data):
        try:
            creds = _entry_to_creds(name, entry)
        except AuthError:
            continue
        # still usable if has refresh_token even when access near-expired
        if include_expired or not creds.expired or creds.refresh_token:
            if include_expired or not creds.expired:
                out.append(creds)
    # newest expiry first for stable ordering
    out.sort(key=lambda c: c.expires_at or 0.0, reverse=True)
    with _live_creds_cache_lock:
        _live_creds_cache["at"] = time.time()
        _live_creds_cache["path"] = str(path)
        _live_creds_cache["include_expired"] = bool(include_expired)
        _live_creds_cache["creds"] = list(out)
    return out


def load_credentials(
    path: Path | None = None,
) -> GrokCredentials:
    path = path or AUTH_FILE
    data = _read_auth(path)

    name, entry = _pick_entry(data)

    # auto refresh if needed
    try:
        from grok2api.upstream.oidc_auth import ensure_fresh_entry, parse_expires_at as _parse_exp

        tok0 = entry.get("key") if isinstance(entry.get("key"), str) else None
        exp0 = _parse_exp(entry.get("expires_at"), tok0)
        must_refresh = exp0 is not None and exp0 <= time.time()
        entry = ensure_fresh_entry(
            name,
            entry,
            skew_seconds=TOKEN_REFRESH_SKEW,
            raise_on_error=must_refresh,
        )
        # re-read id if remounted
        data = _read_auth(path)
        name, entry = _pick_entry(data)
    except Exception as e:
        # Permanent RT failures already delete inside ensure_fresh_entry.
        # Still surface a clear AuthError for the request path.
        try:
            from grok2api.upstream.oidc_auth import RefreshRevokedError, parse_expires_at as _parse_exp2

            if isinstance(e, RefreshRevokedError):
                raise AuthError(
                    f"Token expired and refresh permanently failed: {e}. Re-login or import."
                ) from e
            tok1 = entry.get("key") if isinstance(entry.get("key"), str) else None
            exp1 = _parse_exp2(entry.get("expires_at"), tok1)
            if exp1 is not None and exp1 <= time.time() and entry.get("refresh_token"):
                raise AuthError(
                    f"Token expired and refresh failed: {e}. Re-login or import."
                ) from e
        except AuthError:
            raise
        except Exception:
            pass

    creds = _entry_to_creds(name, entry)
    if creds.expired and not creds.refresh_token:
        # No RT and access already dead — drop so it cannot be reselected.
        try:
            from grok2api.upstream.oidc_auth import delete_account_for_refresh_failure

            delete_account_for_refresh_failure(
                name, reason="no_refresh_token_and_access_expired"
            )
        except Exception:
            pass
        raise AuthError(
            "Session token expired. Use device-code login or import a fresh token."
        )
    if creds.expired and creds.refresh_token:
        try:
            from grok2api.upstream.oidc_auth import RefreshRevokedError, refresh_and_persist

            r = refresh_and_persist(name, entry)
            creds = _entry_to_creds(r["account_id"], r["entry"])
        except RefreshRevokedError as e:
            try:
                from grok2api.upstream.oidc_auth import delete_account_for_refresh_failure

                delete_account_for_refresh_failure(name, reason=str(e))
            except Exception:
                pass
            raise AuthError(
                f"Token expired and refresh permanently failed: {e}. Re-login or import."
            ) from e
        except Exception as e:
            raise AuthError(
                f"Token expired and refresh failed: {e}. Re-login or import."
            ) from e
    return creds


def peek_credentials_by_id(
    account_id: str, path: Path | None = None
) -> GrokCredentials | None:
    """Read one account without OIDC refresh (sticky TTFT fast path).

    Returns None when the account is missing or has no usable access token.
    Expired-but-refreshable accounts are still returned so callers can decide
    whether to pay for refresh or fall back to full pool pick.
    """
    if not account_id:
        return None
    path = path or AUTH_FILE
    try:
        hit = read_auth_entry(str(account_id), path)
    except Exception:
        hit = None
    if hit is None:
        # Last resort: full map (file backend / cold PG cache).
        try:
            data = _read_auth(path)
        except AuthError:
            return None
        entry = data.get(account_id)
        resolved = str(account_id)
        if not isinstance(entry, dict):
            for k, v in data.items():
                if isinstance(v, dict) and (
                    k == account_id
                    or v.get("user_id") == account_id
                    or v.get("principal_id") == account_id
                    or str(k).endswith(f"::{account_id}")
                ):
                    entry = v
                    resolved = str(k)
                    break
            else:
                return None
        else:
            resolved = str(account_id)
    else:
        resolved, entry = hit
    try:
        return _entry_to_creds(resolved, entry)
    except AuthError:
        return None


def load_credentials_by_id(account_id: str, path: Path | None = None) -> GrokCredentials:
    path = path or AUTH_FILE
    # Prefer single-account read so sticky multi-turn requests don't re-scan
    # the whole accounts table on every turn (dominant pick cost on large pools).
    hit = None
    try:
        hit = read_auth_entry(str(account_id), path)
    except Exception:
        hit = None
    if hit is not None:
        account_id, entry = hit
    else:
        data = _read_auth(path)
        entry = data.get(account_id)
        if not isinstance(entry, dict):
            # try match by user_id suffix
            for k, v in data.items():
                if isinstance(v, dict) and (
                    k == account_id
                    or v.get("user_id") == account_id
                    or k.endswith(f"::{account_id}")
                ):
                    entry = v
                    account_id = k
                    break
            else:
                raise AuthError(f"Account not found: {account_id}")

    try:
        from grok2api.upstream.oidc_auth import ensure_fresh_entry, parse_expires_at as _parse_exp

        tok0 = entry.get("key") if isinstance(entry.get("key"), str) else None
        exp0 = _parse_exp(entry.get("expires_at"), tok0)
        must_refresh = exp0 is not None and exp0 <= time.time()
        entry = ensure_fresh_entry(
            account_id,
            entry,
            skew_seconds=TOKEN_REFRESH_SKEW,
            raise_on_error=must_refresh,
        )
        # account_id may have changed after remount — only re-resolve if needed.
        if not isinstance(entry, dict) or not (
            entry.get("key") or entry.get("access_token") or entry.get("token")
        ):
            data = _read_auth(path)
            if account_id not in data:
                for k, v in data.items():
                    if isinstance(v, dict) and v.get("user_id") == (
                        entry.get("user_id") if isinstance(entry, dict) else None
                    ):
                        account_id = k
                        entry = v
                        break
            else:
                entry = data.get(account_id) or entry
    except Exception as e:
        try:
            from grok2api.upstream.oidc_auth import RefreshRevokedError, parse_expires_at as _parse_exp2

            if isinstance(e, RefreshRevokedError):
                raise AuthError(
                    f"Account token expired / refresh permanently failed: {e}"
                ) from e
            tok1 = entry.get("key") if isinstance(entry.get("key"), str) else None
            exp1 = _parse_exp2(entry.get("expires_at"), tok1)
            if exp1 is not None and exp1 <= time.time() and entry.get("refresh_token"):
                raise AuthError(f"Account token expired / refresh failed: {e}") from e
        except AuthError:
            raise
        except Exception:
            pass

    creds = _entry_to_creds(account_id, entry)
    if creds.expired:
        if creds.refresh_token:
            try:
                from grok2api.upstream.oidc_auth import RefreshRevokedError, refresh_and_persist

                r = refresh_and_persist(account_id, entry)
                return _entry_to_creds(r["account_id"], r["entry"])
            except RefreshRevokedError as e:
                try:
                    from grok2api.upstream.oidc_auth import delete_account_for_refresh_failure

                    delete_account_for_refresh_failure(account_id, reason=str(e))
                except Exception:
                    pass
                raise AuthError(
                    f"Account token expired / refresh permanently failed: {e}"
                ) from e
            except Exception as e:
                raise AuthError(f"Account token expired / refresh failed: {e}") from e
        # No RT left — permanently unusable once access expired.
        try:
            from grok2api.upstream.oidc_auth import delete_account_for_refresh_failure

            delete_account_for_refresh_failure(
                account_id, reason="no_refresh_token_and_access_expired"
            )
        except Exception:
            pass
        raise AuthError(f"Account token expired: {account_id}")
    return creds


def upstream_headers(
    token: str,
    model: str,
    conv_id: str | None = None,
) -> dict[str, str]:
    """Headers required by cli-chat-proxy (mirror Grok CLI)."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "X-XAI-Token-Auth": "xai-grok-cli",
        "x-grok-model-override": model,
        # Required — without this, proxy returns 426 with version=(none)
        "x-grok-client-version": CLI_VERSION,
        "x-grok-client-surface": CLIENT_SURFACE,
        "x-grok-client-identifier": CLIENT_IDENTIFIER,
        "User-Agent": f"grok-cli/{CLI_VERSION}",
        "Accept": "text/event-stream, application/json",
    }
    cid = (conv_id or "").strip()
    if cid:
        # CPA / Grok prompt-cache parity: same account + same prompt_cache_key
        # should also share the upstream conversation id header.
        headers["x-grok-conv-id"] = cid[:240]
    return headers
