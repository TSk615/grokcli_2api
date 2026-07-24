"""PostgreSQL durable daily usage rollups + optional lifetime token columns."""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from grok2api.store.pg import connection, json_dump, pg_enabled

_FIELDS = (
    "requests",
    "success",
    "fail",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
)


def enabled() -> bool:
    return pg_enabled()


def _empty() -> dict[str, int]:
    return {f: 0 for f in _FIELDS}


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    s = str(value).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def _day_from_ts(ts: float | None = None) -> date:
    return datetime.fromtimestamp(float(ts or time.time()), tz=timezone.utc).date()


def ensure_event_partition(cur: Any, ts: float | None = None) -> str:
    """Create the UTC month partition needed by one detail event."""
    dt = datetime.fromtimestamp(float(ts or time.time()), tz=timezone.utc)
    start = datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)
    if dt.month == 12:
        end = datetime(dt.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(dt.year, dt.month + 1, 1, tzinfo=timezone.utc)
    table = f"usage_events_y{dt.year:04d}m{dt.month:02d}"
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (table,))
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table}
        PARTITION OF usage_events_partitioned
        FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}')
        """
    )
    return table


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
    """Upsert daily deltas for global/key/account/model. Best-effort."""
    if not enabled():
        return False
    day = _day_from_ts(ts)
    pt = max(0, int(prompt_tokens or 0))
    ct = max(0, int(completion_tokens or 0))
    tt = max(0, int(total_tokens or 0))
    cr = max(0, int(cache_read_tokens or 0))
    cc = max(0, int(cache_creation_tokens or 0))
    rt = max(0, int(reasoning_tokens or 0))
    if tt <= 0:
        tt = pt + ct
    req = 1
    suc = 1 if ok else 0
    fail = 0 if ok else 1
    # Only count tokens on success (failed upstream often has no usage).
    if not ok:
        pt = ct = tt = cr = cc = rt = 0
    cache_hit = 1 if ok and cr > 0 else 0

    dims: list[tuple[str, str]] = [("global", "")]
    if api_key_id:
        dims.append(("key", str(api_key_id)[:256]))
    if account_id:
        dims.append(("account", str(account_id)[:256]))
    if model:
        dims.append(("model", str(model)[:120]))

    try:
        with connection() as conn:
            with conn.cursor() as cur:
                for dim, dim_id in dims:
                    cur.execute(
                        """
                        INSERT INTO usage_daily (
                          day, dim, dim_id,
                          requests, success, fail,
                          prompt_tokens, completion_tokens, total_tokens,
                          cache_read_tokens, cache_creation_tokens, reasoning_tokens,
                          cache_hit_requests,
                          updated_at
                        ) VALUES (
                          %s, %s, %s,
                          %s, %s, %s,
                          %s, %s, %s,
                          %s, %s, %s, %s,
                          now()
                        )
                        ON CONFLICT (day, dim, dim_id) DO UPDATE SET
                          requests = usage_daily.requests + EXCLUDED.requests,
                          success = usage_daily.success + EXCLUDED.success,
                          fail = usage_daily.fail + EXCLUDED.fail,
                          prompt_tokens = usage_daily.prompt_tokens + EXCLUDED.prompt_tokens,
                          completion_tokens = usage_daily.completion_tokens + EXCLUDED.completion_tokens,
                          total_tokens = usage_daily.total_tokens + EXCLUDED.total_tokens,
                          cache_read_tokens = usage_daily.cache_read_tokens + EXCLUDED.cache_read_tokens,
                          cache_creation_tokens = usage_daily.cache_creation_tokens + EXCLUDED.cache_creation_tokens,
                          reasoning_tokens = usage_daily.reasoning_tokens + EXCLUDED.reasoning_tokens,
                          cache_hit_requests = usage_daily.cache_hit_requests + EXCLUDED.cache_hit_requests,
                          updated_at = now()
                        """,
                        (day, dim, dim_id, req, suc, fail, pt, ct, tt, cr, cc, rt, cache_hit),
                    )
                # Lifetime columns on keys / accounts (success tokens only).
                if ok and api_key_id and (pt or ct or tt):
                    try:
                        cur.execute(
                            """
                            UPDATE api_keys
                            SET prompt_tokens_total = COALESCE(prompt_tokens_total, 0) + %s,
                                completion_tokens_total = COALESCE(completion_tokens_total, 0) + %s,
                                total_tokens_total = COALESCE(total_tokens_total, 0) + %s
                            WHERE id = %s
                            """,
                            (pt, ct, tt, str(api_key_id)),
                        )
                    except Exception:
                        pass
                if ok and account_id and (pt or ct or tt):
                    try:
                        cur.execute(
                            """
                            UPDATE account_pool
                            SET prompt_tokens_total = COALESCE(prompt_tokens_total, 0) + %s,
                                completion_tokens_total = COALESCE(completion_tokens_total, 0) + %s,
                                total_tokens_total = COALESCE(total_tokens_total, 0) + %s,
                                updated_at = now()
                            WHERE account_id = %s
                            """,
                            (pt, ct, tt, str(account_id)),
                        )
                    except Exception:
                        pass
            conn.commit()
        return True
    except Exception:
        return False


def get_day(dim: str = "global", dim_id: str = "", *, day: date | str | None = None) -> dict[str, int]:
    if not enabled():
        return _empty()
    d = _as_date(day) or _day_from_ts()
    try:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT requests, success, fail, prompt_tokens, completion_tokens, total_tokens
                    FROM usage_daily
                    WHERE day = %s AND dim = %s AND dim_id = %s
                    LIMIT 1
                    """,
                    (d, (dim or "global"), (dim_id or "")),
                )
                row = cur.fetchone()
        if not row:
            return _empty()
        return {
            "requests": int(row[0] or 0),
            "success": int(row[1] or 0),
            "fail": int(row[2] or 0),
            "prompt_tokens": int(row[3] or 0),
            "completion_tokens": int(row[4] or 0),
            "total_tokens": int(row[5] or 0),
        }
    except Exception:
        return _empty()


def sum_range(
    dim: str = "global",
    dim_id: str = "",
    *,
    days: int = 7,
    end: date | None = None,
) -> dict[str, int]:
    if not enabled():
        return _empty()
    n = max(1, min(366, int(days or 7)))
    end_d = end or _day_from_ts()
    start_d = end_d - timedelta(days=n - 1)
    try:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      COALESCE(SUM(requests), 0),
                      COALESCE(SUM(success), 0),
                      COALESCE(SUM(fail), 0),
                      COALESCE(SUM(prompt_tokens), 0),
                      COALESCE(SUM(completion_tokens), 0),
                      COALESCE(SUM(total_tokens), 0)
                    FROM usage_daily
                    WHERE dim = %s AND dim_id = %s AND day >= %s AND day <= %s
                    """,
                    ((dim or "global"), (dim_id or ""), start_d, end_d),
                )
                row = cur.fetchone()
        if not row:
            return _empty()
        return {
            "requests": int(row[0] or 0),
            "success": int(row[1] or 0),
            "fail": int(row[2] or 0),
            "prompt_tokens": int(row[3] or 0),
            "completion_tokens": int(row[4] or 0),
            "total_tokens": int(row[5] or 0),
        }
    except Exception:
        return _empty()


def series(
    dim: str = "global",
    dim_id: str = "",
    *,
    days: int = 7,
    end: date | None = None,
) -> list[dict[str, Any]]:
    """Return oldest→newest daily rows, filling missing days with zeros."""
    n = max(1, min(90, int(days or 7)))
    end_d = end or _day_from_ts()
    start_d = end_d - timedelta(days=n - 1)
    by_day: dict[str, dict[str, int]] = {}
    if enabled():
        try:
            with connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT day, requests, success, fail,
                               prompt_tokens, completion_tokens, total_tokens
                        FROM usage_daily
                        WHERE dim = %s AND dim_id = %s AND day >= %s AND day <= %s
                        ORDER BY day ASC
                        """,
                        ((dim or "global"), (dim_id or ""), start_d, end_d),
                    )
                    for row in cur.fetchall() or []:
                        d = _as_date(row[0])
                        if not d:
                            continue
                        by_day[d.isoformat()] = {
                            "requests": int(row[1] or 0),
                            "success": int(row[2] or 0),
                            "fail": int(row[3] or 0),
                            "prompt_tokens": int(row[4] or 0),
                            "completion_tokens": int(row[5] or 0),
                            "total_tokens": int(row[6] or 0),
                        }
        except Exception:
            by_day = {}
    out: list[dict[str, Any]] = []
    cur_d = start_d
    while cur_d <= end_d:
        iso = cur_d.isoformat()
        stats = by_day.get(iso) or _empty()
        out.append({"day": iso, **stats})
        cur_d += timedelta(days=1)
    return out


def lifetime_global() -> dict[str, int]:
    """Sum all global daily rows (durable lifetime)."""
    if not enabled():
        return _empty()
    try:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      COALESCE(SUM(requests), 0),
                      COALESCE(SUM(success), 0),
                      COALESCE(SUM(fail), 0),
                      COALESCE(SUM(prompt_tokens), 0),
                      COALESCE(SUM(completion_tokens), 0),
                      COALESCE(SUM(total_tokens), 0)
                    FROM usage_daily
                    WHERE dim = 'global'
                    """
                )
                row = cur.fetchone()
        if not row:
            return _empty()
        return {
            "requests": int(row[0] or 0),
            "success": int(row[1] or 0),
            "fail": int(row[2] or 0),
            "prompt_tokens": int(row[3] or 0),
            "completion_tokens": int(row[4] or 0),
            "total_tokens": int(row[5] or 0),
        }
    except Exception:
        return _empty()


def breakdown(
    dim: str,
    *,
    days: int = 7,
    limit: int = 50,
    end: date | None = None,
) -> list[dict[str, Any]]:
    """Top dim_ids by total_tokens (then requests) over the window."""
    if not enabled() or dim not in ("key", "account", "model"):
        return []
    n = max(1, min(366, int(days or 7)))
    lim = max(1, min(200, int(limit or 50)))
    end_d = end or _day_from_ts()
    start_d = end_d - timedelta(days=n - 1)
    try:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT dim_id,
                           COALESCE(SUM(requests), 0),
                           COALESCE(SUM(success), 0),
                           COALESCE(SUM(fail), 0),
                           COALESCE(SUM(prompt_tokens), 0),
                           COALESCE(SUM(completion_tokens), 0),
                           COALESCE(SUM(total_tokens), 0)
                    FROM usage_daily
                    WHERE dim = %s AND day >= %s AND day <= %s
                    GROUP BY dim_id
                    ORDER BY COALESCE(SUM(total_tokens), 0) DESC,
                             COALESCE(SUM(requests), 0) DESC
                    LIMIT %s
                    """,
                    (dim, start_d, end_d, lim),
                )
                rows = cur.fetchall() or []
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": r[0] or "",
                    "requests": int(r[1] or 0),
                    "success": int(r[2] or 0),
                    "fail": int(r[3] or 0),
                    "prompt_tokens": int(r[4] or 0),
                    "completion_tokens": int(r[5] or 0),
                    "total_tokens": int(r[6] or 0),
                }
            )
        return out
    except Exception:
        return []


def record_event(
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
    event_id: str | None = None,
    capture_reason: str | None = None,
) -> int | None:
    """Insert one per-request usage event. Best-effort; returns id or None."""
    if not enabled():
        return None
    pt = max(0, int(prompt_tokens or 0))
    ct = max(0, int(completion_tokens or 0))
    tt = max(0, int(total_tokens or 0))
    if tt <= 0:
        tt = pt + ct
    cr = max(0, int(cache_read_tokens or 0))
    cc = max(0, int(cache_creation_tokens or 0))
    rt = max(0, int(reasoning_tokens or 0))
    # Failed requests usually have no reliable token numbers.
    if not ok:
        pt = ct = tt = cr = cc = rt = 0
    payload = detail if isinstance(detail, dict) else {}
    # Prefer explicit kwargs; fall back to detail JSON for older call sites.
    lat_out = latency_ms
    ttft_out = ttft_ms
    if isinstance(payload, dict):
        if lat_out is None and payload.get("latency_ms") is not None:
            try:
                lat_out = int(payload.get("latency_ms"))
            except Exception:
                pass
        if ttft_out is None and payload.get("ttft_ms") is not None:
            try:
                ttft_out = int(payload.get("ttft_ms"))
            except Exception:
                pass
        # Mirror top-level columns into detail for the admin detail panel.
        if lat_out is not None and payload.get("latency_ms") is None:
            payload["latency_ms"] = int(lat_out)
        if ttft_out is not None and payload.get("ttft_ms") is None:
            payload["ttft_ms"] = int(ttft_out)
    created_expr = "now()"
    params_extra: list[Any] = []
    if ts is not None:
        try:
            from datetime import datetime, timezone

            created_expr = "%s"
            params_extra = [datetime.fromtimestamp(float(ts), tz=timezone.utc)]
        except Exception:
            created_expr = "now()"
            params_extra = []
    try:
        with connection() as conn:
            with conn.cursor() as cur:
                ensure_event_partition(cur, ts)
                cur.execute(
                    f"""
                    INSERT INTO usage_events_partitioned (
                      event_id, created_at,
                      api_key_id, account_id, model, protocol, path, stream, ok,
                      prompt_tokens, completion_tokens, total_tokens,
                      cache_read_tokens, cache_creation_tokens, reasoning_tokens,
                      client_ip, user_agent, status_code, latency_ms, ttft_ms,
                      error, capture_reason, detail
                    ) VALUES (
                      %s, {created_expr},
                      %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s,
                      %s, %s, %s,
                      %s, %s, %s, %s, %s,
                      %s, %s, %s::jsonb
                    )
                    RETURNING id
                    """,
                    (
                        (str(event_id)[:128] if event_id else None),
                        *params_extra,
                        (str(api_key_id)[:256] if api_key_id else None),
                        (str(account_id)[:256] if account_id else None),
                        (str(model)[:120] if model else None),
                        (str(protocol)[:40] if protocol else None),
                        (str(path)[:200] if path else None),
                        (bool(stream) if stream is not None else None),
                        bool(ok),
                        pt,
                        ct,
                        tt,
                        cr,
                        cc,
                        rt,
                        (str(client_ip)[:80] if client_ip else None),
                        (str(user_agent)[:300] if user_agent else None),
                        (int(status_code) if status_code is not None else None),
                        (int(lat_out) if lat_out is not None else None),
                        (int(ttft_out) if ttft_out is not None else None),
                        (str(error)[:500] if error else None),
                        (str(capture_reason)[:80] if capture_reason else None),
                        json_dump(payload),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return int(row[0]) if row else None
    except Exception:
        return None


def cache_aggregate(*, days: int | None = 7) -> dict[str, Any]:
    """Aggregate prompt-cache stats from usage_events.

    Returns token-level and request-level hit metrics for:
      - today (UTC)
      - last N days window (when days is set)
      - lifetime (all rows)

    Source of truth is the full daily rollup. ``usage_events`` may be sampled.
    """
    empty_bucket = {
        "prompt_tokens": 0,
        "cache_read_tokens": 0,
        "cache_creation_tokens": 0,
        "ok_requests": 0,
        "cache_hit_requests": 0,
        "token_hit_ratio": None,
        "request_hit_ratio": None,
    }
    out: dict[str, Any] = {
        "ok": True,
        "source": "none",
        "today": dict(empty_bucket),
        "window": dict(empty_bucket),
        "lifetime": dict(empty_bucket),
        "days": max(1, min(90, int(days or 7))),
    }
    if not enabled():
        return out

    def _bucket_from_row(row: Any) -> dict[str, Any]:
        if not row:
            return dict(empty_bucket)
        pt = max(0, int(row[0] or 0))
        cr = max(0, int(row[1] or 0))
        cc = max(0, int(row[2] or 0))
        ok_req = max(0, int(row[3] or 0))
        hit_req = max(0, int(row[4] or 0))
        token_ratio = round(100.0 * cr / pt, 2) if pt > 0 else None
        req_ratio = round(100.0 * hit_req / ok_req, 2) if ok_req > 0 else None
        return {
            "prompt_tokens": pt,
            "cache_read_tokens": cr,
            "cache_creation_tokens": cc,
            "ok_requests": ok_req,
            "cache_hit_requests": hit_req,
            "token_hit_ratio": token_ratio,
            "request_hit_ratio": req_ratio,
        }

    n = max(1, min(90, int(days or 7)))
    try:
        with connection() as conn:
            with conn.cursor() as cur:
                # lifetime
                cur.execute(
                    """
                    SELECT
                      COALESCE(SUM(prompt_tokens), 0),
                      COALESCE(SUM(cache_read_tokens), 0),
                      COALESCE(SUM(cache_creation_tokens), 0),
                      COALESCE(SUM(success), 0),
                      COALESCE(SUM(cache_hit_requests), 0)
                    FROM usage_daily
                    WHERE dim = 'global'
                    """
                )
                out["lifetime"] = _bucket_from_row(cur.fetchone())

                # today UTC
                cur.execute(
                    """
                    SELECT
                      COALESCE(SUM(prompt_tokens), 0),
                      COALESCE(SUM(cache_read_tokens), 0),
                      COALESCE(SUM(cache_creation_tokens), 0),
                      COALESCE(SUM(success), 0),
                      COALESCE(SUM(cache_hit_requests), 0)
                    FROM usage_daily
                    WHERE dim = 'global'
                      AND day = (now() AT TIME ZONE 'UTC')::date
                    """
                )
                out["today"] = _bucket_from_row(cur.fetchone())

                # window: last N UTC days inclusive of today
                cur.execute(
                    """
                    SELECT
                      COALESCE(SUM(prompt_tokens), 0),
                      COALESCE(SUM(cache_read_tokens), 0),
                      COALESCE(SUM(cache_creation_tokens), 0),
                      COALESCE(SUM(success), 0),
                      COALESCE(SUM(cache_hit_requests), 0)
                    FROM usage_daily
                    WHERE dim = 'global'
                      AND day >= (now() AT TIME ZONE 'UTC')::date - (%s::int - 1)
                    """,
                    (n,),
                )
                out["window"] = _bucket_from_row(cur.fetchone())
        out["source"] = "postgres"
        out["days"] = n
        return out
    except Exception:
        out["ok"] = False
        return out


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
    """Paginated per-request usage details for the admin console."""
    empty = {
        "ok": True,
        "items": [],
        "total": 0,
        "page": 1,
        "page_size": page_size,
        "total_pages": 1,
        "store_source": "none",
    }
    if not enabled():
        return empty

    try:
        page_i = max(1, int(page))
    except Exception:
        page_i = 1
    try:
        size_i = int(page_size)
    except Exception:
        size_i = 50
    size_i = max(1, min(200, size_i if size_i > 0 else 50))

    where: list[str] = []
    params: list[Any] = []
    qq = (q or "").strip()
    if qq:
        where.append(
            "("
            "COALESCE(api_key_id,'') ILIKE %s OR COALESCE(account_id,'') ILIKE %s "
            "OR COALESCE(model,'') ILIKE %s OR COALESCE(protocol,'') ILIKE %s "
            "OR COALESCE(path,'') ILIKE %s OR COALESCE(client_ip,'') ILIKE %s "
            "OR COALESCE(error,'') ILIKE %s"
            ")"
        )
        like = f"%{qq}%"
        params.extend([like, like, like, like, like, like, like])
    kid = (api_key_id or "").strip()
    if kid and kid != "all":
        where.append("api_key_id = %s")
        params.append(kid)
    aid = (account_id or "").strip()
    if aid and aid != "all":
        where.append("account_id = %s")
        params.append(aid)
    md = (model or "").strip()
    if md and md != "all":
        where.append("model = %s")
        params.append(md)
    proto = (protocol or "").strip()
    if proto and proto != "all":
        where.append("protocol = %s")
        params.append(proto)
    ip = (client_ip or "").strip()
    if ip and ip != "all":
        where.append("client_ip = %s")
        params.append(ip)
    if ok is not None:
        where.append("ok = %s")
        params.append(bool(ok))
    if since_ts is not None:
        where.append("created_at >= to_timestamp(%s)")
        params.append(float(since_ts))
    if until_ts is not None:
        where.append("created_at <= to_timestamp(%s)")
        params.append(float(until_ts))
    wh = (" WHERE " + " AND ".join(where)) if where else ""

    try:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM usage_events_all{wh}", params)
                total = int((cur.fetchone() or [0])[0] or 0)
                total_pages = max(1, (total + size_i - 1) // size_i) if total else 1
                page_i = min(page_i, total_pages)
                offset = (page_i - 1) * size_i
                cur.execute(
                    f"""
                    SELECT id, created_at, api_key_id, account_id, model, protocol, path,
                           stream, ok,
                           prompt_tokens, completion_tokens, total_tokens,
                           cache_read_tokens, cache_creation_tokens, reasoning_tokens,
                           client_ip, user_agent, status_code, latency_ms, ttft_ms,
                           error, detail, event_id, capture_reason
                    FROM usage_events_all
                    {wh}
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s OFFSET %s
                    """,
                    [*params, size_i, offset],
                )
                rows = cur.fetchall() or []
    except Exception:
        return empty

    items: list[dict[str, Any]] = []
    for r in rows:
        detail = r[21]
        if isinstance(detail, str):
            try:
                import json

                detail = json.loads(detail)
            except Exception:
                detail = {"raw": detail}
        created = r[1]
        try:
            created_ts = (
                created.timestamp() if hasattr(created, "timestamp") else float(created or 0)
            )
        except Exception:
            created_ts = time.time()
        latency_val = r[18]
        ttft_val = r[19]
        # Fall back to detail JSON for rows written before top-level columns.
        if isinstance(detail, dict):
            if latency_val is None and detail.get("latency_ms") is not None:
                try:
                    latency_val = int(detail.get("latency_ms"))
                except Exception:
                    pass
            if ttft_val is None and detail.get("ttft_ms") is not None:
                try:
                    ttft_val = int(detail.get("ttft_ms"))
                except Exception:
                    pass
        items.append(
            {
                "id": int(r[0]),
                "created_at": created_ts,
                "api_key_id": r[2],
                "account_id": r[3],
                "model": r[4],
                "protocol": r[5],
                "path": r[6],
                "stream": r[7],
                "ok": bool(r[8]),
                "prompt_tokens": int(r[9] or 0),
                "completion_tokens": int(r[10] or 0),
                "total_tokens": int(r[11] or 0),
                "cache_read_tokens": int(r[12] or 0),
                "cache_creation_tokens": int(r[13] or 0),
                "reasoning_tokens": int(r[14] or 0),
                "client_ip": r[15],
                "user_agent": r[16],
                "status_code": r[17],
                "latency_ms": latency_val,
                "ttft_ms": ttft_val,
                "error": r[20],
                "detail": detail if isinstance(detail, dict) else {},
                "event_id": r[22],
                "capture_reason": r[23],
                "reasoning_effort": (
                    (detail.get("reasoning_effort") if isinstance(detail, dict) else None)
                    or (detail.get("thinking_intensity") if isinstance(detail, dict) else None)
                    or (detail.get("thinking_effort") if isinstance(detail, dict) else None)
                    or ""
                ),
            }
        )
    return {
        "ok": True,
        "items": items,
        "total": total,
        "page": page_i,
        "page_size": size_i,
        "total_pages": total_pages,
        "q": qq,
        "api_key_id": kid or "all",
        "account_id": aid or "all",
        "model": md or "all",
        "protocol": proto or "all",
        "client_ip": ip or "all",
        "store_source": "postgres",
    }
