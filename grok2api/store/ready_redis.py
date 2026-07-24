"""Redis account-id ready index and bounded inflight leases."""

from __future__ import annotations

import json
import time
from typing import Any

from grok2api.config import ACCOUNT_LEASE_TTL_SEC, MAX_ACCOUNT_INFLIGHT
from grok2api.store.redis_client import delete, get_client, get_json, key, redis_enabled


def ready_key() -> str:
    return key("pool", "ready")


def cooldown_key() -> str:
    return key("pool", "cooldown")


def status_key(account_id: str) -> str:
    return key("pool", "status", account_id)


def inflight_key(account_id: str) -> str:
    return key("pool", "lease_count", account_id)


def index_meta_key() -> str:
    return key("pool", "ready_index_meta")


def _set_status(account_id: str, meta: dict[str, Any]) -> None:
    c = get_client()
    if c is not None:
        c.set(status_key(account_id), json.dumps(meta, ensure_ascii=True, separators=(",", ":")))


def _raise_state_sequence_floor(account_id: str, value: int) -> None:
    c = get_client()
    if c is None:
        return
    script = """
    local current = tonumber(redis.call('get', KEYS[1]) or '0')
    local incoming = tonumber(ARGV[1])
    if incoming > current then
      redis.call('set', KEYS[1], incoming)
    end
    return math.max(current, incoming)
    """
    c.eval(script, 1, key("pool", "state_seq", account_id), str(max(0, int(value))))


def _status_active(meta: dict[str, Any], now: float | None = None) -> bool:
    now = float(now or time.time())
    if not isinstance(meta, dict) or meta.get("enabled") is False:
        return False
    if meta.get("disabled_for_quota"):
        return False
    if str(meta.get("pool_status") or "normal").lower() in {
        "disabled",
        "expired",
        "quota_disabled",
        "cooldown",
    }:
        return False
    exp = meta.get("expires_at")
    try:
        if exp is not None and float(exp) <= now:
            return False
    except (TypeError, ValueError):
        pass
    until = meta.get("cooldown_until")
    try:
        if until is not None and float(until) > now:
            return False
    except (TypeError, ValueError):
        pass
    return True


def _score(meta: dict[str, Any], mode: str = "round_robin") -> float:
    if mode == "least_used":
        try:
            return float(meta.get("request_count") or 0)
        except (TypeError, ValueError):
            return 0.0
    try:
        return float(meta.get("last_used_at") or 0)
    except (TypeError, ValueError):
        return 0.0


def apply_status_patch(
    account_id: str,
    patch: dict[str, Any],
    *,
    state_seq: int | None = None,
) -> None:
    if not redis_enabled() or not account_id:
        return
    c = get_client()
    if c is None:
        return
    current = get_json(status_key(account_id))
    meta = dict(current) if isinstance(current, dict) else {}
    current_seq = int(meta.get("state_version") or 0)
    incoming_seq = int(state_seq or current_seq + 1)
    _raise_state_sequence_floor(account_id, max(current_seq, incoming_seq))
    if incoming_seq < current_seq:
        return
    for k, value in (patch or {}).items():
        if value is None:
            meta.pop(k, None)
        else:
            meta[k] = value
    meta["state_version"] = incoming_seq
    _set_status(account_id, meta)
    c.zrem(ready_key(), account_id)
    c.zrem(cooldown_key(), account_id)
    now = time.time()
    until = meta.get("cooldown_until")
    try:
        until_f = float(until) if until is not None else 0.0
    except (TypeError, ValueError):
        until_f = 0.0
    if until_f > now or str(meta.get("pool_status") or "").lower() == "cooldown":
        c.zadd(cooldown_key(), {account_id: max(until_f, now + 365 * 86400)})
    elif _status_active(meta, now):
        c.zadd(ready_key(), {account_id: _score(meta)})


def apply_success_sequence(account_id: str, state_seq: int) -> None:
    """Reset the hot failure streak without perturbing the selector score."""
    if not redis_enabled() or not account_id or int(state_seq or 0) <= 0:
        return
    current = get_json(status_key(account_id))
    meta = dict(current) if isinstance(current, dict) else {}
    current_seq = int(meta.get("state_version") or 0)
    incoming_seq = int(state_seq)
    _raise_state_sequence_floor(account_id, max(current_seq, incoming_seq))
    if incoming_seq <= current_seq:
        return
    meta["consecutive_fails"] = 0
    meta["state_version"] = incoming_seq
    _set_status(account_id, meta)


def sync_account(account_id: str, meta: dict[str, Any]) -> None:
    if not account_id:
        return
    apply_status_patch(account_id, dict(meta or {}), state_seq=int(meta.get("state_version") or 0))


def remove(account_id: str) -> None:
    if not redis_enabled() or not account_id:
        return
    c = get_client()
    if c is None:
        return
    c.zrem(ready_key(), account_id)
    c.zrem(cooldown_key(), account_id)
    delete(status_key(account_id), inflight_key(account_id))


def release(account_id: str) -> None:
    if not redis_enabled() or not account_id:
        return
    c = get_client()
    if c is None:
        return
    count = c.decr(inflight_key(account_id))
    if count is not None and int(count) <= 0:
        c.delete(inflight_key(account_id))
    else:
        c.expire(inflight_key(account_id), max(1, int(ACCOUNT_LEASE_TTL_SEC)))


_CLAIM_SCRIPT = """
local ids = redis.call('zrange', KEYS[1], 0, 63)
for _, aid in ipairs(ids) do
  local ik = ARGV[4] .. aid
  local n = tonumber(redis.call('get', ik) or '0')
  if n < tonumber(ARGV[1]) then
    redis.call('incr', ik)
    redis.call('expire', ik, tonumber(ARGV[2]))
    if ARGV[5] == 'least_used' then
      redis.call('zincrby', KEYS[1], 1, aid)
    else
      redis.call('zadd', KEYS[1], tonumber(ARGV[3]) + n, aid)
    end
    return aid
  end
end
return false
"""


def claim(*, mode: str = "round_robin") -> str | None:
    if not redis_enabled():
        return None
    c = get_client()
    if c is None:
        return None
    try:
        # Keep one ready set and use score updates for both round-robin and
        # least-used. The durable selector remains available during rollout.
        aid = c.eval(
            _CLAIM_SCRIPT,
            1,
            ready_key(),
            str(MAX_ACCOUNT_INFLIGHT),
            str(max(1, int(ACCOUNT_LEASE_TTL_SEC))),
            str(time.time()),
            key("pool", "lease_count") + ":",
            mode,
        )
        return str(aid) if aid else None
    except Exception:
        return None


def claim_preferred(account_id: str, *, mode: str = "round_robin") -> bool:
    if not redis_enabled() or not account_id:
        return False
    c = get_client()
    if c is None:
        return False
    script = """
    if redis.call('zscore', KEYS[1], ARGV[1]) == false then return 0 end
    local n = tonumber(redis.call('get', KEYS[2]) or '0')
    if n >= tonumber(ARGV[2]) then return 0 end
    redis.call('incr', KEYS[2])
    redis.call('expire', KEYS[2], tonumber(ARGV[3]))
    if ARGV[5] == 'least_used' then
      redis.call('zincrby', KEYS[1], 1, ARGV[1])
    else
      redis.call('zadd', KEYS[1], tonumber(ARGV[4]) + n, ARGV[1])
    end
    return 1
    """
    try:
        return bool(
            c.eval(
                script,
                2,
                ready_key(),
                inflight_key(account_id),
                account_id,
                str(MAX_ACCOUNT_INFLIGHT),
                str(max(1, int(ACCOUNT_LEASE_TTL_SEC))),
                str(time.time()),
                mode,
            )
        )
    except Exception:
        return False


def list_candidates(*, limit: int = 4, mode: str = "round_robin") -> list[str]:
    if not redis_enabled():
        return []
    c = get_client()
    if c is None:
        return []
    n = max(1, min(64, int(limit or 4)))
    try:
        if mode == "random":
            raw = c.zrandmember(ready_key(), count=n) or []
        else:
            raw = c.zrange(ready_key(), 0, n - 1) or []
        return [str(x) for x in raw]
    except Exception:
        return []


def get_status(account_id: str) -> dict[str, Any]:
    raw = get_json(status_key(account_id))
    return dict(raw) if isinstance(raw, dict) else {}


def is_ready(account_id: str) -> bool:
    if not redis_enabled() or not account_id:
        return False
    c = get_client()
    return bool(c is not None and c.zscore(ready_key(), account_id) is not None)


def reclaim_expired_cooldowns() -> int:
    if not redis_enabled():
        return 0
    c = get_client()
    if c is None:
        return 0
    now = time.time()
    ids = c.zrangebyscore(cooldown_key(), "-inf", now, start=0, num=500) or []
    if not ids:
        return 0
    c.zrem(cooldown_key(), *ids)
    # State reconciliation will re-add only accounts whose durable status is
    # normal; this avoids wall-clock recovery for probe-only cooldowns.
    return len(ids)


def rebuild_ready_index(*, page_size: int = 1000) -> int:
    """Rebuild ID/status index without selecting accounts.payload."""
    if not redis_enabled():
        return 0
    from grok2api.store.pg import connection

    c = get_client()
    if c is None:
        return 0
    c.delete(ready_key(), cooldown_key(), index_meta_key())
    last_id = ""
    total = 0
    while True:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT a.id, a.expires_at,
                           COALESCE(ap.enabled, true),
                           COALESCE(ap.disabled_for_quota, false),
                           COALESCE(ap.pool_status, 'normal'),
                           ap.cooldown_until,
                           COALESCE(ap.request_count, 0),
                           ap.last_used_at,
                           COALESCE(ap.state_version, 0),
                           COALESCE(ap.blocked_models, '{}'::jsonb),
                           COALESCE(ap.cooldown_count, 0),
                           ap.cooldown_reason,
                           ap.cooldown_code,
                           ap.cooldown_model,
                           ap.last_error,
                           ap.extra->'status_stack',
                           ap.extra->>'consecutive_fails',
                           ap.extra->>'last_status_code',
                           ap.extra->>'disabled_source',
                           ap.extra->>'last_probe_fail_at'
                    FROM accounts a
                    LEFT JOIN account_pool ap ON ap.account_id = a.id
                    WHERE a.id > %s
                    ORDER BY a.id
                    LIMIT %s
                    """,
                    (last_id, int(page_size)),
                )
                rows = cur.fetchall() or []
        if not rows:
            break
        for row in rows:
            aid = str(row[0])
            last_id = aid
            meta = {
                "expires_at": row[1].timestamp() if hasattr(row[1], "timestamp") else row[1],
                "enabled": bool(row[2]),
                "disabled_for_quota": bool(row[3]),
                "pool_status": row[4] or "normal",
                "cooldown_until": row[5].timestamp() if hasattr(row[5], "timestamp") else row[5],
                "request_count": int(row[6] or 0),
                "last_used_at": row[7].timestamp() if hasattr(row[7], "timestamp") else row[7],
                "state_version": int(row[8] or 0),
                "blocked_models": row[9] if isinstance(row[9], dict) else {},
                "cooldown_count": int(row[10] or 0),
                "cooldown_reason": row[11],
                "cooldown_code": row[12],
                "cooldown_model": row[13],
                "last_error": row[14],
                "status_stack": row[15] if isinstance(row[15], list) else [],
                "consecutive_fails": int(row[16] or 0),
                "last_status_code": row[17],
                "disabled_source": row[18],
                "last_probe_fail_at": row[19],
            }
            _set_status(aid, meta)
            until = meta.get("cooldown_until")
            try:
                until_f = float(until) if until is not None else 0.0
            except (TypeError, ValueError):
                until_f = 0.0
            if until_f > time.time() or meta["pool_status"] == "cooldown":
                c.zadd(cooldown_key(), {aid: max(until_f, time.time() + 365 * 86400)})
            elif _status_active(meta):
                c.zadd(ready_key(), {aid: _score(meta)})
            total += 1
    indexed = int(c.zcard(ready_key()) or 0) + int(c.zcard(cooldown_key()) or 0)
    c.set(
        index_meta_key(),
        json.dumps(
            {"built_at": time.time(), "total": total, "indexed": indexed},
            ensure_ascii=True,
            separators=(",", ":"),
        ),
    )
    return total


def ensure_ready_index(*, wait_sec: float = 30.0) -> int:
    """Build once across workers; followers wait for the first ready snapshot."""
    if not redis_enabled():
        return 0
    c = get_client()
    if c is None:
        return 0
    existing = int(c.zcard(ready_key()) or 0) + int(c.zcard(cooldown_key()) or 0)
    marker = get_json(index_meta_key())
    if isinstance(marker, dict):
        expected = int(marker.get("indexed") or 0)
        if existing > 0 or expected == 0:
            return existing
    lock = key("pool", "ready_rebuild_lock")
    owner = str(time.time_ns())
    if c.set(lock, owner, nx=True, ex=max(30, int(wait_sec) + 30)):
        try:
            return rebuild_ready_index()
        finally:
            try:
                if c.get(lock) == owner:
                    c.delete(lock)
            except Exception:
                pass
    deadline = time.time() + max(1.0, float(wait_sec))
    while time.time() < deadline:
        existing = int(c.zcard(ready_key()) or 0) + int(c.zcard(cooldown_key()) or 0)
        marker = get_json(index_meta_key())
        if isinstance(marker, dict):
            return existing
        time.sleep(0.1)
    return 0
