"""PostgreSQL backend for app_settings + durable account_pool rows."""

from __future__ import annotations

import json
import time
from typing import Any

from grok2api.store.pg import _ts, _unix, connection, json_dump, pg_enabled


def enabled() -> bool:
    return pg_enabled()


def get_setting(key: str, default: Any = None) -> Any:
    if not enabled() or not key:
        return default
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM app_settings WHERE key = %s", (key,))
            row = cur.fetchone()
    if not row:
        return default
    val = row[0]
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return val
    return val


def set_setting(key: str, value: Any) -> None:
    if not enabled() or not key:
        return
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_settings (key, value, updated_at)
                VALUES (%s, %s::jsonb, now())
                ON CONFLICT (key) DO UPDATE SET
                  value = EXCLUDED.value,
                  updated_at = now()
                """,
                (key, json_dump(value)),
            )
        conn.commit()


def delete_setting(key: str) -> None:
    if not enabled() or not key:
        return
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app_settings WHERE key = %s", (key,))
        conn.commit()


def list_settings(prefix: str | None = None) -> dict[str, Any]:
    """Return all (or prefix-filtered) app_settings as a dict."""
    if not enabled():
        return {}
    out: dict[str, Any] = {}
    with connection() as conn:
        with conn.cursor() as cur:
            if prefix:
                cur.execute(
                    "SELECT key, value FROM app_settings WHERE key LIKE %s",
                    (f"{prefix}%",),
                )
            else:
                cur.execute("SELECT key, value FROM app_settings")
            for key, val in cur.fetchall():
                if isinstance(val, str):
                    try:
                        val = json.loads(val)
                    except json.JSONDecodeError:
                        pass
                out[str(key)] = val
    return out


# Columns stored in dedicated account_pool fields (everything else → extra JSONB).
_POOL_CORE_KEYS = frozenset(
    {
        "enabled",
        "weight",
        "disabled_for_quota",
        "disabled_reason",
        "quota_disabled_at",
        "quota_source",
        "last_quota",
        "last_probe",
        "blocked_models",
        "request_count",
        "success_count",
        "fail_count",
        "last_used_at",
        "last_error",
        "cooldown_until",
        # Durable account status (bound to account_id; source of truth for pool).
        "pool_status",
        "cooldown_count",
        "cooldown_reason",
        "cooldown_code",
        "cooldown_model",
        "cooldown_tokens_actual",
        "cooldown_tokens_limit",
        "last_probe_status",
        "state_version",
    }
)

# Non-core status-ish fields that still live in extra JSONB.
_POOL_STATUS_EXTRA_KEYS = (
    "cooldown_sec",
    "consecutive_fails",
    "probe_fail_streak",
    "last_status_code",
    "disabled_source",
    "last_probe_ok_at",
    "last_probe_fail_at",
    "status_stack",
    "cooldown_detail",
    "token_expired_at",
    "token_expired_reason",
    "renew_fail_count",
    "last_renew_fail_at",
    "last_renew_error",
    "last_renew_status",
    "last_renew_source",
    "last_renew_ok_at",
    "sso_reauth_attempted_at",
    "sso_reauth_failed_at",
    "sso_reauth_error",
    "sso_reauth_next_at",
)


def _decode_jsonish(val: Any) -> Any:
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return val
    return val


_POOL_SELECT_COLS = """
    account_id, enabled, weight, disabled_for_quota, disabled_reason,
    quota_disabled_at, quota_source, last_quota, last_probe,
    blocked_models, request_count, success_count, fail_count,
    last_used_at, last_error, cooldown_until, extra,
    pool_status, cooldown_count, cooldown_reason, cooldown_code,
    cooldown_model, cooldown_tokens_actual, cooldown_tokens_limit,
    last_probe_status, state_version
"""


def _row_to_meta(r) -> dict[str, Any]:
    """Map a SELECT row (core + status columns + extra jsonb) to pool meta dict."""
    blocked = _decode_jsonish(r[9]) or {}
    if not isinstance(blocked, dict):
        blocked = {}
    meta: dict[str, Any] = {
        "enabled": bool(r[1]),
        "weight": int(r[2] or 1),
        "disabled_for_quota": bool(r[3]),
        "disabled_reason": r[4],
        "quota_disabled_at": _unix(r[5]),
        "quota_source": r[6],
        "last_quota": _decode_jsonish(r[7]),
        "last_probe": _decode_jsonish(r[8]),
        "blocked_models": blocked,
        "request_count": int(r[10] or 0),
        "success_count": int(r[11] or 0),
        "fail_count": int(r[12] or 0),
        "last_used_at": _unix(r[13]),
        "last_error": r[14],
        "cooldown_until": _unix(r[15]),
    }
    # extra jsonb (index 16)
    if len(r) > 16 and r[16] is not None:
        extra = _decode_jsonish(r[16])
        if isinstance(extra, dict):
            for k, v in extra.items():
                if k not in _POOL_CORE_KEYS:
                    meta[k] = v
    # dedicated status columns (17+) — override extra when present
    if len(r) > 17 and r[17] is not None:
        meta["pool_status"] = str(r[17] or "normal")
    if len(r) > 18 and r[18] is not None:
        try:
            meta["cooldown_count"] = int(r[18] or 0)
        except (TypeError, ValueError):
            meta["cooldown_count"] = 0
    if len(r) > 19 and r[19] is not None:
        meta["cooldown_reason"] = r[19]
    if len(r) > 20 and r[20] is not None:
        meta["cooldown_code"] = r[20]
    if len(r) > 21 and r[21] is not None:
        meta["cooldown_model"] = r[21]
    if len(r) > 22 and r[22] is not None:
        try:
            meta["cooldown_tokens_actual"] = int(r[22])
        except (TypeError, ValueError):
            meta["cooldown_tokens_actual"] = r[22]
    if len(r) > 23 and r[23] is not None:
        try:
            meta["cooldown_tokens_limit"] = int(r[23])
        except (TypeError, ValueError):
            meta["cooldown_tokens_limit"] = r[23]
    if len(r) > 24 and r[24] is not None:
        meta["last_probe_status"] = r[24]
    if len(r) > 25 and r[25] is not None:
        try:
            meta["state_version"] = int(r[25] or 0)
        except (TypeError, ValueError):
            meta["state_version"] = 0
    # Normalize computed view flags from durable DB fields only.
    try:
        cd = int(meta.get("cooldown_count") or 0)
    except (TypeError, ValueError):
        cd = 0
    meta["cooldown_count"] = cd
    if not meta.get("pool_status"):
        meta["pool_status"] = _derive_pool_status(meta)
    return meta


_pool_state_cache: dict[str, Any] | None = None
_pool_state_cache_at = 0.0
_POOL_STATE_CACHE_TTL = 1.5


def invalidate_pool_state_cache() -> None:
    global _pool_state_cache, _pool_state_cache_at
    _pool_state_cache = None
    _pool_state_cache_at = 0.0


def get_cached_account_pool_state() -> dict[str, Any] | None:
    """Return warm process-local pool-state cache only (no DB)."""
    now = time.time()
    if (
        _pool_state_cache is not None
        and now - _pool_state_cache_at < _POOL_STATE_CACHE_TTL
    ):
        return dict(_pool_state_cache)
    return None


def get_account_pool_state() -> dict[str, Any]:
    if not enabled():
        return {}
    global _pool_state_cache, _pool_state_cache_at
    cached = get_cached_account_pool_state()
    if cached is not None:
        return cached
    out: dict[str, Any] = {}
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {_POOL_SELECT_COLS}
                FROM account_pool
                """
            )
            for r in cur.fetchall():
                out[str(r[0])] = _row_to_meta(r)
    _pool_state_cache = out
    _pool_state_cache_at = time.time()
    return dict(out)


def get_pool_meta_many(account_ids: list[str]) -> dict[str, Any]:
    """Fetch pool meta for a small set of account ids (admin page rows)."""
    if not enabled() or not account_ids:
        return {}
    ids = [str(x) for x in account_ids if str(x).strip()]
    if not ids:
        return {}
    # Prefer warm full-state cache for single/few lookups (sticky TTFT path).
    now = time.time()
    if (
        _pool_state_cache is not None
        and now - _pool_state_cache_at < _POOL_STATE_CACHE_TTL
        and len(ids) <= 8
    ):
        out_cached: dict[str, Any] = {}
        for aid in ids:
            m = _pool_state_cache.get(aid)
            if isinstance(m, dict):
                out_cached[aid] = dict(m)
        if len(out_cached) == len(ids):
            return out_cached
    out: dict[str, Any] = {}
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {_POOL_SELECT_COLS}
                FROM account_pool
                WHERE account_id = ANY(%s)
                """,
                (ids,),
            )
            for r in cur.fetchall():
                out[str(r[0])] = _row_to_meta(r)
    return out


def get_pool_meta(account_id: str) -> dict[str, Any]:
    """Single-account durable pool meta (O(1) for sticky TTFT)."""
    if not enabled() or not account_id:
        return {}
    aid = str(account_id).strip()
    if not aid:
        return {}
    now = time.time()
    if (
        _pool_state_cache is not None
        and now - _pool_state_cache_at < _POOL_STATE_CACHE_TTL
    ):
        m = _pool_state_cache.get(aid)
        return dict(m) if isinstance(m, dict) else {}
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {_POOL_SELECT_COLS}
                FROM account_pool
                WHERE account_id = %s
                LIMIT 1
                """,
                (aid,),
            )
            row = cur.fetchone()
    if not row:
        return {}
    return _row_to_meta(row)


def pool_counts(*, maintain: bool = False) -> dict[str, int]:
    """Count accounts by durable account_pool status fields.

    Categories are **mutually exclusive** (priority order):
      expired > quota_disabled > disabled > cooldown > model_blocked > enabled

    ``live`` / ``rotatable`` = accounts currently eligible for request rotation
    (enabled + normal + not quota-disabled). Cool/expired/disabled are NOT live.

    ``total`` = every row in ``accounts`` (credentials still stored).
    """
    empty = {
        "total": 0,
        "enabled": 0,
        "quota_disabled": 0,
        "in_cooldown": 0,
        "model_blocked": 0,
        "live": 0,
        "rotatable": 0,
        "expired": 0,
        "disabled": 0,
    }
    if not enabled():
        return dict(empty)
    with connection() as conn:
        with conn.cursor() as cur:
            if maintain:
                try:
                    cur.execute(
                        """
                        DELETE FROM account_pool ap
                        WHERE NOT EXISTS (
                          SELECT 1 FROM accounts a WHERE a.id = ap.account_id
                        )
                        """
                    )
                except Exception:
                    pass
            # Exclusive status classification — every account counted once.
            # Missing pool row → treat as normal/enabled (eligible for rotation).
            cur.execute(
                """
                WITH base AS (
                  SELECT
                    a.id AS account_id,
                    COALESCE(ap.pool_status, 'normal') AS pool_status,
                    COALESCE(ap.enabled, true) AS enabled,
                    COALESCE(ap.disabled_for_quota, false) AS disabled_for_quota,
                    CASE
                      WHEN ap.blocked_models IS NOT NULL
                       AND ap.blocked_models <> '{}'::jsonb
                       AND ap.blocked_models <> 'null'::jsonb
                      THEN true ELSE false
                    END AS has_model_blocks
                  FROM accounts a
                  LEFT JOIN account_pool ap ON ap.account_id = a.id
                ),
                classified AS (
                  SELECT
                    account_id,
                    CASE
                      WHEN pool_status = 'expired' THEN 'expired'
                      WHEN disabled_for_quota OR pool_status = 'quota_disabled'
                        THEN 'quota_disabled'
                      WHEN pool_status = 'disabled'
                        OR (enabled = false AND pool_status NOT IN (
                              'expired', 'quota_disabled', 'cooldown', 'model_blocked'
                            ))
                        THEN 'disabled'
                      WHEN pool_status = 'cooldown' THEN 'cooldown'
                      WHEN pool_status = 'model_blocked' OR has_model_blocks
                        THEN 'model_blocked'
                      ELSE 'enabled'
                    END AS bucket
                  FROM base
                )
                SELECT
                  COUNT(*) AS total,
                  COUNT(*) FILTER (WHERE bucket = 'enabled') AS enabled,
                  COUNT(*) FILTER (WHERE bucket = 'quota_disabled') AS quota_disabled,
                  COUNT(*) FILTER (WHERE bucket = 'cooldown') AS in_cooldown,
                  COUNT(*) FILTER (WHERE bucket = 'model_blocked') AS model_blocked,
                  COUNT(*) FILTER (WHERE bucket = 'enabled') AS live,
                  COUNT(*) FILTER (WHERE bucket = 'expired') AS expired,
                  COUNT(*) FILTER (WHERE bucket = 'disabled') AS disabled
                FROM classified
                """
            )
            r = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0)
        if maintain:
            try:
                conn.commit()
            except Exception:
                pass
    total = int(r[0] or 0)
    enabled_n = int(r[1] or 0)
    live_n = int(r[5] or 0)  # same as enabled under exclusive rules
    return {
        "total": total,
        "enabled": enabled_n,
        "quota_disabled": int(r[2] or 0),
        "in_cooldown": int(r[3] or 0),
        "model_blocked": int(r[4] or 0),
        "live": live_n,
        "rotatable": live_n,
        "expired": int(r[6] or 0),
        "disabled": int(r[7] or 0) if len(r) > 7 else 0,
    }


def refresh_pool_summary_snapshot() -> dict[str, Any]:
    """Compute pool counts and persist snapshot into app_settings for durable reads."""
    from time import time as _time

    counts = pool_counts(maintain=True)
    try:
        from grok2api.admin.settings_store import get_account_mode
        mode = get_account_mode()
    except Exception:
        mode = "round_robin"
    snap = {
        "mode": mode,
        "total": int(counts.get("total") or 0),
        "live": int(counts.get("live") or 0),
        "rotatable": int(counts.get("rotatable") or counts.get("live") or 0),
        "enabled": int(counts.get("enabled") or 0),
        "in_cooldown": int(counts.get("in_cooldown") or 0),
        "quota_disabled": int(counts.get("quota_disabled") or 0),
        "model_blocked": int(counts.get("model_blocked") or 0),
        "expired": int(counts.get("expired") or 0),
        "disabled": int(counts.get("disabled") or 0),
        "updated_at": _time(),
        "source": "postgres",
    }
    try:
        set_setting("pool_summary_snapshot", snap)
    except Exception:
        pass
    return snap


def get_pool_summary_snapshot() -> dict[str, Any] | None:
    raw = get_setting("pool_summary_snapshot", None)
    return raw if isinstance(raw, dict) else None


def save_account_pool_state(state: dict[str, Any]) -> None:
    """Bulk upsert pool meta.

    Never blindly overwrite a still-active durable cooldown with a stale
    snapshot that has cooldown_until=NULL. Cooldown may only clear when the
    incoming meta explicitly sets a future until, or the existing until has
    already expired (or the caller passed an explicit clear via patch).
    """
    if not enabled():
        return
    state = state if isinstance(state, dict) else {}
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT account_id FROM account_pool")
            existing = {r[0] for r in cur.fetchall()}
            incoming = set(state.keys())
            for aid, meta in state.items():
                if not isinstance(meta, dict):
                    continue
                _upsert_pool(cur, str(aid), meta, preserve_active_cooldown=True)
            for aid in existing - incoming:
                cur.execute("DELETE FROM account_pool WHERE account_id = %s", (aid,))
        conn.commit()
    invalidate_pool_state_cache()


def upsert_pool_meta(account_id: str, meta: dict[str, Any]) -> None:
    if not enabled() or not account_id:
        return
    with connection() as conn:
        with conn.cursor() as cur:
            _upsert_pool(cur, account_id, meta, preserve_active_cooldown=True)
        conn.commit()
    invalidate_pool_state_cache()


def patch_pool_meta(account_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    """Merge patch into one account_pool row and commit immediately.

    Every account status change (enabled / cooldown / quota / model block /
    last_error / counters) goes through here so PostgreSQL is always current.
    """
    if not enabled() or not account_id:
        return {}
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {_POOL_SELECT_COLS}
                FROM account_pool WHERE account_id = %s
                FOR UPDATE
                """,
                (account_id,),
            )
            r = cur.fetchone()
            if r:
                meta = _row_to_meta(r)
            else:
                meta = {
                    "enabled": True,
                    "weight": 1,
                    "blocked_models": {},
                    "pool_status": "normal",
                    "cooldown_count": 0,
                }
            # Apply patch (None values mean pop). Explicit cooldown_until=None
            # / cooldown_count=0 are intentional clears (probe recover / manual).
            for k, v in (patch or {}).items():
                if v is None:
                    meta.pop(k, None)
                else:
                    meta[k] = v
            current_version = int(meta.get("state_version") or 0)
            try:
                from grok2api.store.events_redis import next_state_sequence

                redis_version = int(next_state_sequence(account_id) or 0)
            except Exception:
                redis_version = 0
            meta["state_version"] = max(current_version + 1, redis_version)
            # Status is computed from durable fields already on this account row.
            meta["pool_status"] = _derive_pool_status(meta)
            _upsert_pool(cur, account_id, meta, preserve_active_cooldown=False)
        conn.commit()
    invalidate_pool_state_cache()
    return meta


def apply_pool_state_event(
    cur,
    account_id: str,
    patch: dict[str, Any],
    state_version: int,
) -> bool:
    """Apply one queued state patch inside the writer transaction.

    The upsert is guarded by the durable version so a delayed request failure
    cannot overwrite a later probe or admin action.
    """
    if not account_id or not isinstance(patch, dict):
        return False
    cur.execute(
        f"""
        SELECT {_POOL_SELECT_COLS}
        FROM account_pool WHERE account_id = %s
        FOR UPDATE
        """,
        (account_id,),
    )
    row = cur.fetchone()
    if row:
        meta = _row_to_meta(row)
    else:
        meta = {
            "enabled": True,
            "weight": 1,
            "blocked_models": {},
            "pool_status": "normal",
            "cooldown_count": 0,
            "state_version": 0,
        }
    current_version = int(meta.get("state_version") or 0)
    version = max(0, int(state_version or 0))
    if version <= current_version:
        return False
    for key, value in patch.items():
        if value is None:
            meta.pop(key, None)
        else:
            meta[key] = value
    meta["state_version"] = version
    meta["pool_status"] = _derive_pool_status(meta)
    _upsert_pool(
        cur,
        account_id,
        meta,
        preserve_active_cooldown=False,
        only_if_newer=True,
        preserve_counters=True,
    )
    return True


def _derive_pool_status(meta: dict[str, Any]) -> str:
    """Canonical status from durable DB fields (no Redis refresh)."""
    if not isinstance(meta, dict):
        return "normal"
    # Token/renewal expiry outranks disabled: broken RT accounts stay labeled
    # expired even after kick_from_pool sets enabled=False.
    if str(meta.get("pool_status") or "").strip().lower() == "expired":
        return "expired"
    if meta.get("token_expired_at") is not None and not meta.get("disabled_for_quota"):
        # Soft-expire path may only stamp token_expired_at before pool_status.
        if meta.get("enabled") is not False or meta.get("last_renew_status") in {
            "failed",
            "expired",
            "sso_failed",
            "no_sso_removed",
            "sso_attempt",
        }:
            if str(meta.get("last_renew_status") or "") in {
                "failed",
                "expired",
                "sso_failed",
                "no_sso_removed",
                "sso_attempt",
            } or meta.get("renew_fail_count"):
                return "expired"
    if meta.get("disabled_for_quota"):
        return "quota_disabled"
    if meta.get("enabled") is False:
        return "disabled"
    try:
        if int(meta.get("cooldown_count") or 0) > 0:
            return "cooldown"
    except (TypeError, ValueError):
        pass
    # status_stack entries also mean cooling even if count was lost.
    stack = meta.get("status_stack")
    if isinstance(stack, list) and len(stack) > 0:
        return "cooldown"
    until = meta.get("cooldown_until")
    try:
        if until is not None and float(until) > time.time():
            return "cooldown"
    except (TypeError, ValueError):
        pass
    blocked = meta.get("blocked_models") or {}
    if isinstance(blocked, dict) and blocked:
        return "model_blocked"
    return "normal"


def _active_cooldown_until(meta: dict[str, Any] | None) -> float | None:
    if not isinstance(meta, dict):
        return None
    until = meta.get("cooldown_until")
    if until is None:
        return None
    try:
        u = float(until)
    except (TypeError, ValueError):
        return None
    return u if u > time.time() else None


def _upsert_pool(
    cur,
    account_id: str,
    meta: dict[str, Any],
    *,
    preserve_active_cooldown: bool = False,
    only_if_newer: bool = False,
    preserve_counters: bool = False,
) -> None:
    blocked = meta.get("blocked_models") or {}
    if not isinstance(blocked, dict):
        blocked = {}

    # Protect active durable cooldown against stale full-state rewrites
    # (e.g. last_probe snapshot save that never reloaded cooldown_until).
    cooldown_until = meta.get("cooldown_until")
    if preserve_active_cooldown and cooldown_until is None:
        try:
            cur.execute(
                "SELECT cooldown_until FROM account_pool WHERE account_id = %s",
                (account_id,),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                existing_until = _unix(row[0])
                if existing_until is not None and float(existing_until) > time.time():
                    cooldown_until = existing_until
                    meta = dict(meta)
                    meta["cooldown_until"] = existing_until
        except Exception:
            pass

    # Always stamp derived status so DB row is self-describing.
    meta = dict(meta)
    if cooldown_until is not None:
        meta["cooldown_until"] = cooldown_until
    # When preserving active cooldown, also keep count/status if incoming wiped them.
    if preserve_active_cooldown:
        try:
            cur.execute(
                """
                SELECT cooldown_count, pool_status, cooldown_reason, cooldown_code,
                       cooldown_model, cooldown_tokens_actual, cooldown_tokens_limit
                FROM account_pool WHERE account_id = %s
                """,
                (account_id,),
            )
            row = cur.fetchone()
            if row:
                if not meta.get("cooldown_count") and row[0]:
                    meta["cooldown_count"] = int(row[0] or 0)
                if not meta.get("pool_status") and row[1]:
                    meta["pool_status"] = row[1]
                for i, key in enumerate(
                    (
                        "cooldown_reason",
                        "cooldown_code",
                        "cooldown_model",
                        "cooldown_tokens_actual",
                        "cooldown_tokens_limit",
                    ),
                    start=2,
                ):
                    if meta.get(key) is None and row[i] is not None:
                        meta[key] = row[i]
        except Exception:
            pass
    meta["pool_status"] = _derive_pool_status(meta)
    try:
        cd_count = int(meta.get("cooldown_count") or 0)
    except (TypeError, ValueError):
        cd_count = 0
    if cd_count < 0:
        cd_count = 0
    meta["cooldown_count"] = cd_count

    extra = {
        k: v
        for k, v in meta.items()
        if k not in _POOL_CORE_KEYS and v is not None
    }
    # Keep a small status mirror in extra for older readers, but core columns
    # are the source of truth.
    extra["pool_status"] = meta.get("pool_status") or "normal"
    extra["cooldown_count"] = cd_count

    cur.execute(
        """
        INSERT INTO account_pool (
          account_id, enabled, weight, disabled_for_quota, disabled_reason,
          quota_disabled_at, quota_source, last_quota, last_probe, blocked_models,
          request_count, success_count, fail_count, last_used_at, last_error,
          cooldown_until, extra, updated_at,
          pool_status, cooldown_count, cooldown_reason, cooldown_code,
          cooldown_model, cooldown_tokens_actual, cooldown_tokens_limit,
          last_probe_status, state_version
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,
          %s,%s,%s,%s,%s,%s,%s::jsonb, now(),
          %s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (account_id) DO UPDATE SET
          enabled = EXCLUDED.enabled,
          weight = EXCLUDED.weight,
          disabled_for_quota = EXCLUDED.disabled_for_quota,
          disabled_reason = EXCLUDED.disabled_reason,
          quota_disabled_at = EXCLUDED.quota_disabled_at,
          quota_source = EXCLUDED.quota_source,
          last_quota = EXCLUDED.last_quota,
          last_probe = EXCLUDED.last_probe,
          blocked_models = EXCLUDED.blocked_models,
          request_count = CASE WHEN %s THEN account_pool.request_count ELSE EXCLUDED.request_count END,
          success_count = CASE WHEN %s THEN account_pool.success_count ELSE EXCLUDED.success_count END,
          fail_count = CASE WHEN %s THEN account_pool.fail_count ELSE EXCLUDED.fail_count END,
          last_used_at = CASE WHEN %s
            THEN GREATEST(account_pool.last_used_at, EXCLUDED.last_used_at)
            ELSE EXCLUDED.last_used_at END,
          last_error = EXCLUDED.last_error,
          cooldown_until = CASE
            WHEN EXCLUDED.cooldown_until IS NOT NULL THEN EXCLUDED.cooldown_until
            WHEN %s AND account_pool.cooldown_until IS NOT NULL
                 AND account_pool.cooldown_until > now()
              THEN account_pool.cooldown_until
            ELSE EXCLUDED.cooldown_until
          END,
          extra = EXCLUDED.extra,
          pool_status = EXCLUDED.pool_status,
          cooldown_count = EXCLUDED.cooldown_count,
          cooldown_reason = EXCLUDED.cooldown_reason,
          cooldown_code = EXCLUDED.cooldown_code,
          cooldown_model = EXCLUDED.cooldown_model,
          cooldown_tokens_actual = EXCLUDED.cooldown_tokens_actual,
          cooldown_tokens_limit = EXCLUDED.cooldown_tokens_limit,
          last_probe_status = EXCLUDED.last_probe_status,
          state_version = GREATEST(account_pool.state_version, EXCLUDED.state_version),
          updated_at = now()
        WHERE (NOT %s) OR account_pool.state_version < EXCLUDED.state_version
        """,
        (
            account_id,
            bool(meta.get("enabled", True)),
            max(1, int(meta.get("weight") or 1)),
            bool(meta.get("disabled_for_quota")),
            meta.get("disabled_reason"),
            _ts(meta.get("quota_disabled_at")),
            meta.get("quota_source"),
            json_dump(meta.get("last_quota")) if meta.get("last_quota") is not None else None,
            json_dump(meta.get("last_probe")) if meta.get("last_probe") is not None else None,
            json_dump(blocked),
            int(meta.get("request_count") or 0),
            int(meta.get("success_count") or 0),
            int(meta.get("fail_count") or 0),
            _ts(meta.get("last_used_at")),
            meta.get("last_error"),
            _ts(cooldown_until),
            json_dump(extra),
            str(meta.get("pool_status") or "normal"),
            cd_count,
            meta.get("cooldown_reason"),
            meta.get("cooldown_code"),
            meta.get("cooldown_model"),
            meta.get("cooldown_tokens_actual"),
            meta.get("cooldown_tokens_limit"),
            meta.get("last_probe_status"),
            int(meta.get("state_version") or 0),
            bool(preserve_counters),
            bool(preserve_counters),
            bool(preserve_counters),
            bool(preserve_counters),
            bool(preserve_active_cooldown),
            bool(only_if_newer),
        ),
    )
