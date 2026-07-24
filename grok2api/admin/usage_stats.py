"""Proxy-side token / request usage stats (Redis hot + PostgreSQL durable).

Best-effort only: never raise into the chat path.
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any

_FIELDS = (
    "requests",
    "success",
    "fail",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
)

_light_cache: dict[str, Any] | None = None
_light_cache_at = 0.0
_light_lock = threading.Lock()
_LIGHT_TTL = 3.0


def _capture_policy() -> dict[str, Any]:
    try:
        from grok2api.admin.settings_store import get_usage_capture

        return get_usage_capture()
    except Exception:
        try:
            from grok2api.config import USAGE_EVENT_SAMPLE_RATE, USAGE_SLOW_MS

            return {
                "sample_rate": USAGE_EVENT_SAMPLE_RATE,
                "all_failures": True,
                "slow_ms": USAGE_SLOW_MS,
                "audit_until": 0.0,
                "audit_key_ids": [],
            }
        except Exception:
            return {
                "sample_rate": 0.05,
                "all_failures": True,
                "slow_ms": 5000,
                "audit_until": 0.0,
                "audit_key_ids": [],
            }


def _capture_decision(
    *,
    request_id: str,
    ok: bool,
    status_code: int | None,
    latency_ms: int | None,
    api_key_id: str | None,
) -> tuple[bool, str]:
    policy = _capture_policy()
    if not ok and bool(policy.get("all_failures", True)):
        return True, "failure"
    if status_code is not None and int(status_code) >= 400:
        return True, "http_error"
    try:
        if latency_ms is not None and int(latency_ms) >= int(policy.get("slow_ms") or 0) > 0:
            return True, "slow"
    except (TypeError, ValueError):
        pass
    try:
        audit_until = float(policy.get("audit_until") or 0)
    except (TypeError, ValueError):
        audit_until = 0.0
    audit_ids = {str(x) for x in (policy.get("audit_key_ids") or [])}
    if audit_until > time.time() and (not audit_ids or str(api_key_id or "") in audit_ids):
        return True, "audit"
    try:
        from grok2api.store.events_redis import deterministic_sample

        if deterministic_sample(request_id, float(policy.get("sample_rate") or 0.0)):
            return True, "sample"
    except Exception:
        pass
    return False, "unsampled"


def _empty() -> dict[str, int]:
    return {f: 0 for f in _FIELDS}


def _i(v: Any) -> int:
    try:
        return max(0, int(float(v or 0)))
    except (TypeError, ValueError):
        return 0


def _nested_int(usage: dict[str, Any], *paths: Any) -> int:
    """Read first matching int path: 'key' or ('parent', 'child')."""
    for path in paths:
        try:
            if isinstance(path, (tuple, list)) and len(path) == 2:
                parent, child = path
                node = usage.get(parent)
                if not isinstance(node, dict):
                    continue
                val = _i(node.get(child))
            else:
                val = _i(usage.get(path))
        except Exception:
            continue
        if val > 0:
            return val
    return 0


def _normalize_usage_dict(usage: dict[str, Any] | None) -> dict[str, int]:
    if not isinstance(usage, dict):
        return {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "cache_read_tokens": 0,
            "cache_creation_tokens": 0,
            "reasoning_tokens": 0,
        }
    pt = _i(usage.get("prompt_tokens") or usage.get("input_tokens"))
    ct = _i(usage.get("completion_tokens") or usage.get("output_tokens"))
    tt = _i(usage.get("total_tokens"))
    if tt <= 0:
        tt = pt + ct
    cache_read = _nested_int(
        usage,
        ("prompt_tokens_details", "cached_tokens"),
        ("input_tokens_details", "cached_tokens"),
        "cached_tokens",
        "cache_read_input_tokens",
        "prompt_cache_hit_tokens",
    )
    cache_creation = _nested_int(
        usage,
        "cache_creation_input_tokens",
        ("prompt_tokens_details", "cache_creation_tokens"),
        ("input_tokens_details", "cache_creation_tokens"),
        "cache_creation_tokens",
    )
    reasoning = _nested_int(
        usage,
        ("completion_tokens_details", "reasoning_tokens"),
        ("output_tokens_details", "reasoning_tokens"),
        "reasoning_tokens",
    )
    return {
        "prompt_tokens": pt,
        "completion_tokens": ct,
        "total_tokens": tt,
        "cache_read_tokens": cache_read,
        "cache_creation_tokens": cache_creation,
        "reasoning_tokens": reasoning,
    }


def _merge_stats(*parts: dict[str, int] | None) -> dict[str, int]:
    """Prefer max per field (used when Redis today supersedes PG same-day)."""
    out = _empty()
    for p in parts:
        if not p:
            continue
        for f in _FIELDS:
            out[f] = max(out[f], _i(p.get(f)))
    return out


def _sum_stats(*parts: dict[str, int] | None) -> dict[str, int]:
    out = _empty()
    for p in parts:
        if not p:
            continue
        for f in _FIELDS:
            out[f] = out[f] + _i(p.get(f))
    return out


def _rate(success: int, requests: int) -> float | None:
    if requests <= 0:
        return None
    return round(100.0 * float(success) / float(requests), 2)


def record_usage(
    *,
    usage: dict[str, Any] | None = None,
    prompt_tokens: int | None = None,
    completion_tokens: int | None = None,
    total_tokens: int | None = None,
    cache_read_tokens: int | None = None,
    cache_creation_tokens: int | None = None,
    reasoning_tokens: int | None = None,
    ok: bool = True,
    api_key_id: str | None = None,
    account_id: str | None = None,
    model: str | None = None,
    protocol: str | None = None,
    path: str | None = None,
    stream: bool | None = None,
    client_ip: str | None = None,
    user_agent: str | None = None,
    status_code: int | None = None,
    latency_ms: int | None = None,
    ttft_ms: int | None = None,
    error: str | None = None,
    detail: dict[str, Any] | None = None,
    ts: float | None = None,
    request_id: str | None = None,
) -> None:
    """Record one finished request. Safe to call from any path; never raises."""
    try:
        norm = _normalize_usage_dict(usage)
        if prompt_tokens is not None:
            norm["prompt_tokens"] = _i(prompt_tokens)
        if completion_tokens is not None:
            norm["completion_tokens"] = _i(completion_tokens)
        if total_tokens is not None:
            norm["total_tokens"] = _i(total_tokens)
        if cache_read_tokens is not None:
            norm["cache_read_tokens"] = _i(cache_read_tokens)
        if cache_creation_tokens is not None:
            norm["cache_creation_tokens"] = _i(cache_creation_tokens)
        if reasoning_tokens is not None:
            norm["reasoning_tokens"] = _i(reasoning_tokens)
        if norm["total_tokens"] <= 0:
            norm["total_tokens"] = norm["prompt_tokens"] + norm["completion_tokens"]

        key_id = (str(api_key_id).strip() if api_key_id else "") or None
        if key_id in ("", "none", "null"):
            key_id = None
        acc_id = (str(account_id).strip() if account_id else "") or None
        model_s = (str(model).strip() if model else "") or None
        if model_s:
            model_s = model_s[:120]
        proto_s = (str(protocol).strip() if protocol else "") or None
        if proto_s:
            proto_s = proto_s[:40]
        path_s = (str(path).strip() if path else "") or None
        if path_s:
            path_s = path_s[:200]
        ip_s = (str(client_ip).strip() if client_ip else "") or None
        if ip_s:
            ip_s = ip_s[:80]
        ua_s = (str(user_agent).strip() if user_agent else "") or None
        if ua_s:
            ua_s = ua_s[:300]

        # Keep a compact raw usage snapshot so admin can verify whether upstream
        # actually reported cache fields (we never invent cache hits).
        detail_out: dict[str, Any] = {}
        if isinstance(detail, dict):
            detail_out.update(detail)
        # Normalize timing fields onto both top-level columns and detail JSON.
        try:
            if latency_ms is not None and "latency_ms" not in detail_out:
                detail_out["latency_ms"] = int(latency_ms)
        except Exception:
            pass
        try:
            if ttft_ms is not None:
                detail_out["ttft_ms"] = int(ttft_ms)
            elif "ttft_ms" in detail_out:
                try:
                    ttft_ms = int(detail_out.get("ttft_ms"))  # type: ignore[assignment]
                except Exception:
                    pass
        except Exception:
            pass
        if isinstance(usage, dict) and "raw_usage" not in detail_out:
            raw_snap: dict[str, Any] = {}
            for k in (
                "prompt_tokens",
                "completion_tokens",
                "total_tokens",
                "input_tokens",
                "output_tokens",
                "cached_tokens",
                "cache_read_input_tokens",
                "cache_creation_input_tokens",
                "cache_creation_tokens",
                "prompt_cache_hit_tokens",
                "reasoning_tokens",
            ):
                if k in usage:
                    raw_snap[k] = usage.get(k)
            for parent in (
                "prompt_tokens_details",
                "input_tokens_details",
                "completion_tokens_details",
                "output_tokens_details",
            ):
                node = usage.get(parent)
                if isinstance(node, dict) and node:
                    # shallow copy only; keep small
                    raw_snap[parent] = {
                        kk: node.get(kk)
                        for kk in (
                            "cached_tokens",
                            "cache_creation_tokens",
                            "reasoning_tokens",
                        )
                        if kk in node
                    }
            if raw_snap:
                detail_out["raw_usage"] = raw_snap
            detail_out["parsed_cache"] = {
                "cache_read_tokens": norm.get("cache_read_tokens") or 0,
                "cache_creation_tokens": norm.get("cache_creation_tokens") or 0,
                "reasoning_tokens": norm.get("reasoning_tokens") or 0,
            }

        # Process-local Prometheus counters (optional).
        try:
            from grok2api.store.metrics import inc

            inc("g2a_usage_requests_total", 1)
            if ok:
                inc("g2a_usage_success_total", 1)
                if norm["prompt_tokens"]:
                    inc("g2a_prompt_tokens_total", float(norm["prompt_tokens"]))
                if norm["completion_tokens"]:
                    inc("g2a_completion_tokens_total", float(norm["completion_tokens"]))
                if norm["total_tokens"]:
                    inc("g2a_total_tokens_total", float(norm["total_tokens"]))
                if norm.get("cache_read_tokens"):
                    inc(
                        "g2a_cache_read_tokens_total",
                        float(norm["cache_read_tokens"]),
                    )
            else:
                inc("g2a_usage_fail_total", 1)
        except Exception:
            pass

        try:
            from grok2api.store import usage_redis

            usage_redis.record(
                prompt_tokens=norm["prompt_tokens"],
                completion_tokens=norm["completion_tokens"],
                total_tokens=norm["total_tokens"],
                cache_read_tokens=norm.get("cache_read_tokens") or 0,
                cache_creation_tokens=norm.get("cache_creation_tokens") or 0,
                reasoning_tokens=norm.get("reasoning_tokens") or 0,
                ok=bool(ok),
                api_key_id=key_id,
                account_id=acc_id,
                model=model_s,
                ts=ts,
            )
        except Exception:
            pass

        queued = False
        try:
            from grok2api.config import ASYNC_USAGE
            from grok2api.store import events_redis

            if ASYNC_USAGE and events_redis.redis_enabled():
                req_id = str(
                    request_id
                    or detail_out.get("request_id")
                    or detail_out.get("req_id")
                    or uuid.uuid4().hex
                )
                capture, capture_reason = _capture_decision(
                    request_id=req_id,
                    ok=bool(ok),
                    status_code=status_code,
                    latency_ms=latency_ms,
                    api_key_id=key_id,
                )
                payload = {
                    "api_key_id": key_id,
                    "account_id": acc_id,
                    "model": model_s,
                    "protocol": proto_s,
                    "path": path_s,
                    "stream": stream,
                    "ok": bool(ok),
                    "prompt_tokens": norm["prompt_tokens"],
                    "completion_tokens": norm["completion_tokens"],
                    "total_tokens": norm["total_tokens"],
                    "cache_read_tokens": norm.get("cache_read_tokens") or 0,
                    "cache_creation_tokens": norm.get("cache_creation_tokens") or 0,
                    "reasoning_tokens": norm.get("reasoning_tokens") or 0,
                    "client_ip": ip_s,
                    "user_agent": ua_s,
                    "status_code": status_code,
                    "latency_ms": latency_ms,
                    "ttft_ms": ttft_ms,
                    "error": error,
                    "detail": detail_out or {},
                    "capture": capture,
                    "capture_reason": capture_reason,
                }
                queued = bool(
                    events_redis.publish(
                        "usage",
                        payload,
                        request_id=req_id,
                        account_id=acc_id,
                        occurred_at=ts,
                    )
                )
        except Exception:
            queued = False

        if not queued:
            # Compatibility/failure fallback: file-mode, async disabled, or a
            # Redis enqueue failure keeps the durable legacy behavior.
            try:
                from grok2api.store import usage_pg

                usage_pg.record(
                    prompt_tokens=norm["prompt_tokens"],
                    completion_tokens=norm["completion_tokens"],
                    total_tokens=norm["total_tokens"],
                    cache_read_tokens=norm.get("cache_read_tokens") or 0,
                    cache_creation_tokens=norm.get("cache_creation_tokens") or 0,
                    reasoning_tokens=norm.get("reasoning_tokens") or 0,
                    ok=bool(ok),
                    api_key_id=key_id,
                    account_id=acc_id,
                    model=model_s,
                    ts=ts,
                )
                usage_pg.record_event(
                    prompt_tokens=norm["prompt_tokens"],
                    completion_tokens=norm["completion_tokens"],
                    total_tokens=norm["total_tokens"],
                    cache_read_tokens=norm.get("cache_read_tokens") or 0,
                    cache_creation_tokens=norm.get("cache_creation_tokens") or 0,
                    reasoning_tokens=norm.get("reasoning_tokens") or 0,
                    ok=bool(ok),
                    api_key_id=key_id,
                    account_id=acc_id,
                    model=model_s,
                    protocol=proto_s,
                    path=path_s,
                    stream=stream,
                    client_ip=ip_s,
                    user_agent=ua_s,
                    status_code=status_code,
                    latency_ms=latency_ms,
                    ttft_ms=ttft_ms,
                    error=error,
                    detail=detail_out or None,
                    ts=ts,
                )
            except Exception:
                pass

        # Invalidate light cache so status picks up quickly.
        global _light_cache, _light_cache_at
        with _light_lock:
            _light_cache = None
            _light_cache_at = 0.0
    except Exception:
        return


def light_snapshot(*, force: bool = False) -> dict[str, Any]:
    """Cheap today + lifetime numbers for status/dashboard."""
    global _light_cache, _light_cache_at
    now = time.time()
    with _light_lock:
        if (
            not force
            and _light_cache is not None
            and now - _light_cache_at < _LIGHT_TTL
        ):
            return dict(_light_cache)

    today = _empty()
    life = _empty()
    source = "none"
    try:
        from grok2api.store import usage_redis

        if usage_redis.enabled():
            snap = usage_redis.light_snapshot()
            today = {
                "requests": _i(snap.get("today_requests")),
                "success": _i(snap.get("today_success")),
                "fail": _i(snap.get("today_fail")),
                "prompt_tokens": _i(snap.get("today_prompt_tokens")),
                "completion_tokens": _i(snap.get("today_completion_tokens")),
                "total_tokens": _i(snap.get("today_tokens")),
            }
            life = {
                "requests": _i(snap.get("total_requests")),
                "success": 0,
                "fail": 0,
                "prompt_tokens": _i(snap.get("total_prompt_tokens")),
                "completion_tokens": _i(snap.get("total_completion_tokens")),
                "total_tokens": _i(snap.get("total_tokens")),
            }
            source = "redis"
    except Exception:
        pass

    try:
        from grok2api.store import usage_pg

        if usage_pg.enabled():
            pg_today = usage_pg.get_day("global")
            # Prefer Redis for today when present; else PG.
            if source != "redis" or sum(today.values()) == 0:
                today = _merge_stats(today, pg_today)
                if source == "none":
                    source = "postgres"
            pg_life = usage_pg.lifetime_global()
            # Lifetime: take max of Redis life vs PG sum (covers Redis-only lag).
            life = _merge_stats(life, pg_life)
            if source == "redis":
                source = "hybrid"
            elif source == "none":
                source = "postgres"
    except Exception:
        pass

    out = {
        "today_requests": today["requests"],
        "today_success": today["success"],
        "today_fail": today["fail"],
        "today_tokens": today["total_tokens"],
        "today_prompt_tokens": today["prompt_tokens"],
        "today_completion_tokens": today["completion_tokens"],
        "total_requests": life["requests"],
        "total_tokens": life["total_tokens"],
        "total_prompt_tokens": life["prompt_tokens"],
        "total_completion_tokens": life["completion_tokens"],
        "today_success_rate": _rate(today["success"], today["requests"]),
        "source": source,
    }
    with _light_lock:
        _light_cache = dict(out)
        _light_cache_at = now
    return out


def summary(*, days: int = 7) -> dict[str, Any]:
    """Today / last N days / lifetime aggregates for the usage page."""
    n = max(1, min(90, int(days or 7)))
    today = _empty()
    window = _empty()
    life = _empty()
    series_rows: list[dict[str, Any]] = []
    source_bits: list[str] = []

    try:
        from grok2api.store import usage_redis

        if usage_redis.enabled():
            today = usage_redis.get_day("global")
            redis_days = usage_redis.list_days("global", days=n)
            # list_days is newest-first; reverse for chart oldest→newest
            series_rows = list(reversed(redis_days))
            life = usage_redis.get_lifetime("global")
            source_bits.append("redis")
    except Exception:
        pass

    try:
        from grok2api.store import usage_pg

        if usage_pg.enabled():
            pg_today = usage_pg.get_day("global")
            today = _merge_stats(today, pg_today)
            pg_series = usage_pg.series("global", days=n)
            if series_rows:
                # Overlay PG history; for each day take max (Redis today usually wins).
                by = {r["day"]: r for r in series_rows}
                for r in pg_series:
                    d = r["day"]
                    if d in by:
                        merged = _merge_stats(by[d], r)
                        by[d] = {"day": d, **merged}
                    else:
                        by[d] = r
                series_rows = [by[k] for k in sorted(by.keys())]
            else:
                series_rows = pg_series
            # Window sum from series (includes today merge).
            window = _empty()
            for r in series_rows:
                window = _sum_stats(window, r)
            life = _merge_stats(life, usage_pg.lifetime_global())
            source_bits.append("postgres")
    except Exception:
        pass

    if not window or sum(window.values()) == 0:
        # Fallback: sum series if window empty
        window = _empty()
        for r in series_rows:
            window = _sum_stats(window, r)
        if sum(window.values()) == 0:
            window = dict(today)

    # If only Redis had today, window may under-count without PG — ensure today in window.
    if series_rows:
        last = series_rows[-1]
        from datetime import datetime, timezone

        today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if last.get("day") == today_iso:
            series_rows[-1] = {"day": today_iso, **_merge_stats(last, today)}
            window = _empty()
            for r in series_rows:
                window = _sum_stats(window, r)

    cache = {
        "ok": False,
        "source": "none",
        "today": {},
        "window": {},
        "lifetime": {},
        "days": n,
    }
    try:
        from grok2api.store import usage_pg

        if usage_pg.enabled():
            cache = usage_pg.cache_aggregate(days=n)
            if cache.get("source") == "postgres" and "postgres" not in source_bits:
                source_bits.append("postgres")
    except Exception:
        pass

    return {
        "ok": True,
        "days": n,
        "today": {**today, "success_rate": _rate(today["success"], today["requests"])},
        "window": {
            **window,
            "success_rate": _rate(window["success"], window["requests"]),
        },
        "lifetime": {
            **life,
            "success_rate": _rate(life["success"], life["requests"]),
        },
        "cache": cache,
        "series": series_rows,
        "source": "+".join(source_bits) if source_bits else "none",
        "light": light_snapshot(force=True),
    }


def breakdown(dim: str, *, days: int = 7, limit: int = 50) -> dict[str, Any]:
    dim = (dim or "").strip().lower()
    if dim not in ("key", "account", "model"):
        return {"ok": False, "error": "dim must be key|account|model", "items": []}
    n = max(1, min(90, int(days or 7)))
    lim = max(1, min(200, int(limit or 50)))
    items: list[dict[str, Any]] = []
    source = "none"

    # PG is the right source for multi-day multi-dim aggregation.
    try:
        from grok2api.store import usage_pg

        if usage_pg.enabled():
            items = usage_pg.breakdown(dim, days=n, limit=lim)
            source = "postgres"
    except Exception:
        items = []

    # Overlay today's Redis numbers for dims that may not yet be flushed equally.
    try:
        from grok2api.store import usage_redis

        if usage_redis.enabled() and items:
            for it in items:
                rid = it.get("id") or ""
                hot = usage_redis.get_day(dim, rid)
                # Prefer max so Redis-ahead today doesn't double-count with PG.
                for f in _FIELDS:
                    it[f] = max(_i(it.get(f)), _i(hot.get(f)))
            source = "hybrid" if source == "postgres" else "redis"
        elif usage_redis.enabled() and not items and n <= 1:
            # Only today available via Redis; we don't SCAN all keys (expensive).
            # Return empty list — UI can still show global summary.
            source = "redis"
    except Exception:
        pass

    # Enrich key/account labels when possible.
    if dim == "key" and items:
        try:
            import grok2api.admin.apikeys as apikeys

            by_id = {k.get("id"): k for k in apikeys.list_keys()}
            for it in items:
                meta = by_id.get(it.get("id")) or {}
                it["name"] = meta.get("name") or it.get("id")
                it["prefix"] = meta.get("prefix") or ""
                it["enabled"] = meta.get("enabled")
                # lifetime tokens from key row if present
                if meta.get("total_tokens_total") is not None:
                    it["lifetime_tokens"] = _i(meta.get("total_tokens_total"))
        except Exception:
            pass
    if dim == "account" and items:
        try:
            import grok2api.pool.account_pool as account_pool

            # Light map from pool summary if cheap enough
            summary = account_pool.pool_summary(include_accounts=True)
            accs = summary.get("accounts") or []
            by_id = {}
            for a in accs:
                if not isinstance(a, dict):
                    continue
                aid = a.get("id") or a.get("account_id") or a.get("auth_key")
                if aid:
                    by_id[str(aid)] = a
            for it in items:
                meta = by_id.get(str(it.get("id") or "")) or {}
                it["email"] = meta.get("email") or meta.get("label") or ""
                it["enabled"] = meta.get("enabled")
                if meta.get("total_tokens_total") is not None:
                    it["lifetime_tokens"] = _i(meta.get("total_tokens_total"))
        except Exception:
            pass

    for it in items:
        it["success_rate"] = _rate(_i(it.get("success")), _i(it.get("requests")))

    return {
        "ok": True,
        "dim": dim,
        "days": n,
        "items": items[:lim],
        "source": source,
    }


def series(*, days: int = 7) -> dict[str, Any]:
    s = summary(days=days)
    return {
        "ok": True,
        "days": s.get("days"),
        "series": s.get("series") or [],
        "source": s.get("source"),
    }


def list_events(
    *,
    q: str = "",
    api_key_id: str = "",
    account_id: str = "",
    model: str = "",
    protocol: str = "",
    client_ip: str = "",
    ok: bool | None = None,
    page: int = 1,
    page_size: int = 50,
    since_ts: float | None = None,
    until_ts: float | None = None,
) -> dict[str, Any]:
    """Paginated request-level usage details with key labels enriched."""
    try:
        from grok2api.store import usage_pg

        data = usage_pg.list_events(
            q=q,
            api_key_id=api_key_id,
            account_id=account_id,
            model=model,
            protocol=protocol,
            client_ip=client_ip,
            ok=ok,
            page=page,
            page_size=page_size,
            since_ts=since_ts,
            until_ts=until_ts,
        )
    except Exception as e:
        return {
            "ok": False,
            "items": [],
            "total": 0,
            "page": 1,
            "page_size": page_size,
            "total_pages": 1,
            "error": str(e)[:300],
            "store_source": "none",
        }

    items = list(data.get("items") or [])
    if items:
        try:
            import grok2api.admin.apikeys as apikeys

            by_id = {k.get("id"): k for k in apikeys.list_keys()}
            for it in items:
                meta = by_id.get(it.get("api_key_id")) or {}
                it["api_key_name"] = meta.get("name") or ""
                it["api_key_prefix"] = meta.get("prefix") or ""
                # Surface thinking intensity even if older rows only stored it in detail.
                if not it.get("reasoning_effort"):
                    d = it.get("detail") if isinstance(it.get("detail"), dict) else {}
                    it["reasoning_effort"] = (
                        d.get("reasoning_effort")
                        or d.get("thinking_intensity")
                        or d.get("thinking_effort")
                        or ""
                    )
        except Exception:
            pass
        try:
            import grok2api.pool.account_pool as account_pool

            summary = account_pool.pool_summary(include_accounts=True)
            accs = summary.get("accounts") or []
            by_acc: dict[str, Any] = {}
            for a in accs:
                if not isinstance(a, dict):
                    continue
                aid = a.get("id") or a.get("account_id") or a.get("auth_key")
                if aid:
                    by_acc[str(aid)] = a
            for it in items:
                meta = by_acc.get(str(it.get("account_id") or "")) or {}
                it["account_email"] = meta.get("email") or meta.get("label") or ""
        except Exception:
            pass
    data["items"] = items
    return data
