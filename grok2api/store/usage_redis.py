"""Redis hot buckets for proxy-side token / request usage."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

from grok2api.store.redis_client import (
    expire,
    get_client,
    hgetall,
    hincrby,
    key,
    redis_enabled,
)

_FIELDS = (
    "requests",
    "success",
    "fail",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "cache_read_tokens",
    "cache_creation_tokens",
    "reasoning_tokens",
    "cache_hit_requests",
)

# Keep ~40 days of daily buckets.
_DAY_TTL_SEC = 40 * 24 * 3600


def enabled() -> bool:
    return redis_enabled()


def _day_str(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(float(ts or time.time()), tz=timezone.utc)
    return dt.strftime("%Y%m%d")


def _day_iso(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(float(ts or time.time()), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def _bucket_key(day: str, dim: str, dim_id: str = "") -> str:
    dim = (dim or "global").strip() or "global"
    dim_id = (dim_id or "").strip()
    if dim == "global":
        return key("usage", "day", day, "global")
    return key("usage", "day", day, dim, dim_id or "_")


def _life_key(dim: str = "global", dim_id: str = "") -> str:
    dim = (dim or "global").strip() or "global"
    dim_id = (dim_id or "").strip()
    if dim == "global":
        return key("usage", "life", "global")
    return key("usage", "life", dim, dim_id or "_")


def _empty() -> dict[str, int]:
    return {f: 0 for f in _FIELDS}


def _parse_hash(raw: dict[str, str] | None) -> dict[str, int]:
    out = _empty()
    if not raw:
        return out
    for f in _FIELDS:
        try:
            out[f] = max(0, int(float(raw.get(f) or 0)))
        except (TypeError, ValueError):
            out[f] = 0
    return out


def _incr_hash(rk: str, deltas: dict[str, int], *, ttl_sec: int | None) -> bool:
    c = get_client()
    if c is None:
        return False
    try:
        pipe = c.pipeline(transaction=False)
        touched = False
        for f, n in deltas.items():
            if not n:
                continue
            pipe.hincrby(rk, f, int(n))
            touched = True
        if not touched:
            return True
        if ttl_sec is not None:
            pipe.expire(rk, max(1, int(ttl_sec)))
        pipe.execute()
        return True
    except Exception:
        # Fallback field-by-field
        ok = False
        for f, n in deltas.items():
            if not n:
                continue
            if hincrby(rk, f, int(n)) is not None:
                ok = True
        if ok and ttl_sec is not None:
            try:
                expire(rk, ttl_sec)
            except Exception:
                pass
        return ok


def record(
    *,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    total_tokens: int = 0,
    cache_read_tokens: int = 0,
    cache_creation_tokens: int = 0,
    reasoning_tokens: int = 0,
    ok: bool = True,
    api_key_id: str | None = None,
    account_id: str | None = None,
    model: str | None = None,
    ts: float | None = None,
) -> bool:
    """Atomically bump daily + lifetime buckets. Best-effort."""
    if not enabled():
        return False
    day = _day_str(ts)
    pt = max(0, int(prompt_tokens or 0))
    ct = max(0, int(completion_tokens or 0))
    tt = max(0, int(total_tokens or 0))
    if tt <= 0:
        tt = pt + ct
    cr = max(0, int(cache_read_tokens or 0))
    cc = max(0, int(cache_creation_tokens or 0))
    rt = max(0, int(reasoning_tokens or 0))
    deltas = {
        "requests": 1,
        "success": 1 if ok else 0,
        "fail": 0 if ok else 1,
        "prompt_tokens": pt if ok else 0,
        "completion_tokens": ct if ok else 0,
        "total_tokens": tt if ok else 0,
        "cache_read_tokens": cr if ok else 0,
        "cache_creation_tokens": cc if ok else 0,
        "reasoning_tokens": rt if ok else 0,
        "cache_hit_requests": 1 if ok and cr > 0 else 0,
    }
    dims: list[tuple[str, str]] = [("global", "")]
    if api_key_id:
        dims.append(("key", str(api_key_id)))
    if account_id:
        dims.append(("account", str(account_id)))
    if model:
        dims.append(("model", str(model)[:120]))

    any_ok = False
    for dim, dim_id in dims:
        if _incr_hash(_bucket_key(day, dim, dim_id), deltas, ttl_sec=_DAY_TTL_SEC):
            any_ok = True
        # Lifetime only for global + key + account (model lifetime optional).
        if dim in ("global", "key", "account"):
            _incr_hash(_life_key(dim, dim_id), deltas, ttl_sec=None)
    return any_ok


def get_day(
    dim: str = "global",
    dim_id: str = "",
    *,
    day: str | None = None,
    ts: float | None = None,
) -> dict[str, int]:
    if not enabled():
        return _empty()
    d = (day or _day_str(ts)).replace("-", "")
    return _parse_hash(hgetall(_bucket_key(d, dim, dim_id)))


def get_lifetime(dim: str = "global", dim_id: str = "") -> dict[str, int]:
    if not enabled():
        return _empty()
    return _parse_hash(hgetall(_life_key(dim, dim_id)))


def list_days(
    dim: str = "global",
    dim_id: str = "",
    *,
    days: int = 7,
    end_ts: float | None = None,
) -> list[dict[str, Any]]:
    """Return newest-first daily snapshots for the last N days (UTC)."""
    n = max(1, min(90, int(days or 7)))
    end = datetime.fromtimestamp(float(end_ts or time.time()), tz=timezone.utc).date()
    out: list[dict[str, Any]] = []
    for i in range(n):
        d = end - timedelta(days=i)
        day_compact = d.strftime("%Y%m%d")
        day_iso = d.strftime("%Y-%m-%d")
        stats = get_day(dim, dim_id, day=day_compact)
        out.append({"day": day_iso, **stats})
    return out


def light_snapshot() -> dict[str, int]:
    """Cheap fields for status/dashboard cards."""
    today = get_day("global")
    life = get_lifetime("global")
    return {
        "today_requests": int(today.get("requests") or 0),
        "today_success": int(today.get("success") or 0),
        "today_fail": int(today.get("fail") or 0),
        "today_tokens": int(today.get("total_tokens") or 0),
        "today_prompt_tokens": int(today.get("prompt_tokens") or 0),
        "today_completion_tokens": int(today.get("completion_tokens") or 0),
        "today_cache_read_tokens": int(today.get("cache_read_tokens") or 0),
        "today_cache_creation_tokens": int(today.get("cache_creation_tokens") or 0),
        "today_reasoning_tokens": int(today.get("reasoning_tokens") or 0),
        "total_requests": int(life.get("requests") or 0),
        "total_tokens": int(life.get("total_tokens") or 0),
        "total_prompt_tokens": int(life.get("prompt_tokens") or 0),
        "total_completion_tokens": int(life.get("completion_tokens") or 0),
        "total_cache_read_tokens": int(life.get("cache_read_tokens") or 0),
        "total_cache_creation_tokens": int(life.get("cache_creation_tokens") or 0),
        "total_reasoning_tokens": int(life.get("reasoning_tokens") or 0),
    }
