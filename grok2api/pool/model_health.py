"""Per-account model probe + periodic error check.

- Manual probe for a single account (admin UI)
- Background worker: periodically probe each live account; on hard errors
  block model / disable account and record last_probe on pool meta

Scale notes (many models / large pools):
- Background cycles rotate ONE model per account per cycle (not accounts×models).
- Background batch selection is priority-aware: cooldown-due / never-probed /
  fail-streak first, then fair non-repeat sweep for the rest.
- Shared httpx client + connection limits avoid per-probe TCP churn.
- Probe path never reloads the full account_pool state for a single account.
"""

from __future__ import annotations

import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import httpx

from grok2api.pool.auth import GrokCredentials, list_live_credentials, load_credentials_by_id, upstream_headers
from grok2api.config import (
    DEFAULT_MODEL,
    MODEL_HEALTH_AUTO_DISABLE,
    MODEL_HEALTH_INTERVAL,
    MODEL_HEALTH_STARTUP_DELAY,
    MODEL_PROBE_BATCH,
    MODEL_PROBE_MAX_MODELS_PER_ACCOUNT,
    MODEL_PROBE_WORKERS,
    PROBE_MODELS,
    UPSTREAM_BASE,
)
from grok2api.admin.maintenance_gate import maintenance_slot

_PROBE_TIMEOUT = 30.0
# Hard ceiling for one maintenance-slot hold (manual_all / background).
# Prevents multi-model full-pool probes from starving token refresh forever.
_CYCLE_BUDGET_SEC = 150.0

# Background worker state
_stop = threading.Event()
_thread: threading.Thread | None = None
_wakeup = threading.Event()
_last_run: dict[str, Any] = {}
_lock = threading.RLock()
# Round-robin cursor across PROBE_MODELS for background cycles.
_model_rr_index = 0

# Strict non-repeat sweep (background only): cover each live account once per
# sweep generation, then start a new generation. Shared via Redis when multi-
# worker; falls back to in-process state for single-worker / no-redis.
_SWEEP_META_KEY = ("model_health", "sweep", "meta")
_SWEEP_COVERED_KEY = ("model_health", "sweep", "covered")
# Keep sweep state long enough for multi-hour full-pool coverage.
_SWEEP_TTL_SEC = 12 * 3600
_local_sweep: dict[str, Any] = {
    "generation": 0,
    "started_at": 0.0,
    "covered": set(),  # type: ignore[dict-item]
}

# Shared HTTP client for probe workers (connection reuse across accounts).
# Direct + per-proxy clients so outbound proxy pool can stick per account.
_http_client: httpx.Client | None = None
_http_clients_by_proxy: dict[str, httpx.Client] = {}
_http_client_lock = threading.Lock()


def _new_probe_client(
    *, proxy: str | None = None, proxy_auth: tuple[str, str] | None = None
) -> httpx.Client:
    limits = httpx.Limits(
        max_connections=max(8, int(MODEL_PROBE_WORKERS) * 4),
        max_keepalive_connections=max(4, int(MODEL_PROBE_WORKERS) * 2),
        keepalive_expiry=30.0,
    )
    kwargs: dict[str, Any] = {
        "timeout": httpx.Timeout(_PROBE_TIMEOUT, connect=10.0),
        "limits": limits,
        "http2": False,
    }
    proxy_url = (proxy or "").strip()
    if proxy_auth and proxy_url:
        # Keep Resin credentials out of proxy URLs and repr/log output.
        proxy_obj = httpx.Proxy(proxy_url, auth=proxy_auth)
        return httpx.Client(proxy=proxy_obj, **kwargs)
    if proxy_url:
        try:
            return httpx.Client(proxy=proxy_url, **kwargs)
        except TypeError:
            return httpx.Client(
                proxies={"http://": proxy_url, "https://": proxy_url},
                **kwargs,
            )
    return httpx.Client(**kwargs)


def _probe_http_client(account_id: str | None = None) -> httpx.Client:
    """Lazy shared client — reuses TLS/TCP for dense probe batches.

    When an outbound proxy pool is configured, pins a proxy by account id so
    probe egress matches live traffic stickiness.
    """
    global _http_client
    proxy_url: str | None = None
    proxy_auth: tuple[str, str] | None = None
    cache_key = ""
    if account_id:
        try:
            from grok2api.upstream.resin_proxy import resin_binding_for_account

            # Health checks must observe the same sticky egress as live Build
            # traffic and OIDC refresh for this account.
            binding = resin_binding_for_account("grok_build", account_id)
            if binding is not None:
                proxy_url = binding.gateway_url
                proxy_auth = binding.proxy_auth
                cache_key = binding.cache_key
            else:
                from grok2api.upstream.proxy_pool import pick_proxy_for_account

                proxy_url = pick_proxy_for_account(account_id)
                cache_key = proxy_url or ""
        except Exception:
            from grok2api.upstream.resin_proxy import resin_enabled
            if resin_enabled():
                raise
            proxy_url = None
            cache_key = ""

    if not proxy_url:
        with _http_client_lock:
            if _http_client is None or _http_client.is_closed:
                _http_client = _new_probe_client()
            return _http_client

    with _http_client_lock:
        client = _http_clients_by_proxy.get(cache_key)
        if client is None or client.is_closed:
            client = _new_probe_client(proxy=proxy_url, proxy_auth=proxy_auth)
            _http_clients_by_proxy[cache_key] = client
            if len(_http_clients_by_proxy) > 32:
                for old_key in list(_http_clients_by_proxy.keys()):
                    if old_key == cache_key:
                        continue
                    old = _http_clients_by_proxy.pop(old_key, None)
                    if old is not None and not old.is_closed:
                        try:
                            old.close()
                        except Exception:
                            pass
                    break
        return client


def invalidate_probe_http_client() -> None:
    """Drop cached probe clients after outbound proxy config changes."""
    global _http_client
    with _http_client_lock:
        clients = []
        if _http_client is not None:
            clients.append(_http_client)
            _http_client = None
        clients.extend(list(_http_clients_by_proxy.values()))
        _http_clients_by_proxy.clear()
    for c in clients:
        try:
            if c is not None and not c.is_closed:
                c.close()
        except Exception:
            pass


def _live_probe_models() -> list[str]:
    """Runtime probe model list (settings DB overrides env/config)."""
    try:
        from grok2api.admin.settings_store import get_probe_models

        models = get_probe_models()
        if models:
            return list(models)
    except Exception:
        pass
    return list(PROBE_MODELS) or [DEFAULT_MODEL]


def _live_auto_disable() -> bool:
    try:
        from grok2api.admin.settings_store import get_model_health_auto_disable

        return bool(get_model_health_auto_disable())
    except Exception:
        return bool(MODEL_HEALTH_AUTO_DISABLE)


def _normalize_probe_models(models: list[str] | None = None) -> list[str]:
    """De-dupe + stabilize probe model list (preserve order)."""
    raw = models if models is not None else _live_probe_models()
    seen: set[str] = set()
    out: list[str] = []
    for m in raw:
        mid = str(m or "").strip()
        if not mid:
            continue
        key = mid.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(mid)
    return out or [DEFAULT_MODEL]


def _pick_background_models(models: list[str]) -> list[str]:
    """Rotate probe models across background cycles.

    With many PROBE_MODELS, probing every model every cycle explodes load
    (accounts × models). One model per cycle still covers the full list over
    successive sweeps while keeping each maintenance slot short.
    """
    models = _normalize_probe_models(models)
    if len(models) <= 1:
        return models
    global _model_rr_index
    with _lock:
        idx = int(_model_rr_index) % len(models)
        _model_rr_index = (idx + 1) % len(models)
        return [models[idx]]


def _models_for_account(
    models: list[str],
    *,
    source: str,
    max_models: int | None = None,
) -> list[str]:
    """Cap models probed for one account in a single cycle.

    Background already rotates to 1 model. Manual paths may probe several, but
    still hard-capped so a long PROBE_MODELS list cannot freeze the host.
    """
    models = _normalize_probe_models(models)
    if source == "background":
        return models[:1] if models else [DEFAULT_MODEL]
    cap = (
        max_models
        if max_models is not None
        else int(MODEL_PROBE_MAX_MODELS_PER_ACCOUNT or 2)
    )
    cap = max(1, min(int(cap), len(models) or 1))
    return models[:cap]

# Hard signals that this account cannot use the requested model
# permanently (or for a long TTL). Temporary free-usage / rolling 429s are
# handled separately — permanently blocking them makes agent frontends
# (sub2api / Claude Code) look like "scheduling stopped".
_MODEL_UNAVAILABLE_RE = re.compile(
    r"("
    r"model[_ -]?not[_ -]?found|"
    r"model[_ -]?not[_ -]?available|"
    r"model[_ -]?unavailable|"
    r"unknown[_ -]?model|"
    r"does\s+not\s+(?:have\s+)?access|"
    r"not\s+(?:allowed|authorized|permitted)\s+to\s+use|"
    r"no\s+access\s+to\s+(?:this\s+)?model|"
    r"unsupported[_ -]?model|"
    r"invalid[_ -]?model|"
    r"model[_ -]?is[_ -]?not[_ -]?supported|"
    r"not\s+supported\s+for\s+(?:this\s+)?model|"
    r"subscription\s+required|"
    r"need\s+a\s+(?:grok\s+)?subscription|"
    r"plan\s+does\s+not\s+include|"
    r"not\s+available\s+(?:for|on)\s+your|"
    r"access[_ -]?denied|"
    r"forbidden.*model|"
    r"model[_ -]?access[_ -]?denied|"
    r"cannot\s+use\s+(?:this\s+)?model|"
    r"disabled\s+model|"
    r"model\s+disabled"
    r")",
    re.IGNORECASE,
)

# Temporary free-tier / rolling usage exhaustion — cool down, do NOT permanent-block.
_TEMP_USAGE_RE = re.compile(
    r"("
    r"free-usage-exhausted|"
    r"subscription:free-usage-exhausted|"
    r"used\s+all\s+the\s+included\s+free\s+usage|"
    r"free\s+usage\s+for\s+model|"
    r"usage\s+resets\s+over\s+a\s+rolling|"
    r"usage\s+limit\s+(exceeded|reached)|"
    r"quota[-_ ]exceeded|"
    r"out\s+of\s+(free\s+)?(quota|credits?|tokens?)|"
    r"no\s+(remaining\s+)?(quota|credits?|tokens?)|"
    r"rate[_ -]?limit|"
    r"too\s+many\s+requests|"
    r"try\s+again\s+later|"
    r"额度(不足|耗尽|用完)|"
    r"免费额度|"
    r"用量(超限|耗尽)|"
    r"配额(不足|耗尽|超限)"
    r")",
    re.IGNORECASE,
)

# Account-wide hard blocks (stop all scheduling). Keep this narrow —
# temporary free-usage / rate-limit must not disable the whole account.
_ACCOUNT_BLOCK_RE = re.compile(
    r"("
    r"user[_ -]?blocked|"
    r"account[_ -]?blocked|"
    r"account[_ -]?suspended|"
    r"account[_ -]?disabled|"
    r"personal-team-blocked|"
    r"need\s+a\s+grok\s+subscription|"
    r"run\s+out\s+of\s+credits|"
    r"out\s+of\s+credits"
    r")",
    re.IGNORECASE,
)


def is_temporary_usage_error(
    error: str | None, status_code: int | None = None
) -> bool:
    """True for rolling free-usage / rate-limit style failures (recoverable)."""
    text = (error or "").strip()
    if not text:
        return False
    if _TEMP_USAGE_RE.search(text):
        return True
    # Bare 429 without a hard "model does not exist" body is temporary.
    if status_code == 429 and not _MODEL_UNAVAILABLE_RE.search(text):
        return True
    return False


def is_model_unavailable_error(
    error: str | None, status_code: int | None = None
) -> bool:
    """True only for durable model unavailability (not free-usage 429)."""
    text = (error or "").strip()
    if not text:
        return False
    # Temporary free usage / rate limits are never permanent model blocks.
    if is_temporary_usage_error(text, status_code):
        return False
    # 429 is almost always temporary — never permanent-block from rate limits.
    if status_code == 429:
        return False
    if _MODEL_UNAVAILABLE_RE.search(text):
        return True
    if status_code in (403, 404) and re.search(r"\bmodel\b", text, re.I):
        return True
    return False


def is_account_block_error(
    error: str | None, status_code: int | None = None
) -> bool:
    text = (error or "").strip()
    if not text:
        return False
    if is_temporary_usage_error(text, status_code):
        return False
    if status_code == 429:
        return False
    if _ACCOUNT_BLOCK_RE.search(text):
        return True
    return False


def handle_upstream_error_for_model(
    account_id: str | None,
    *,
    model: str | None = None,
    error: str = "",
    status_code: int | None = None,
) -> dict[str, Any] | None:
    """
    On upstream failure: block model (or whole account) from scheduling
    when the error indicates the model / account is unusable.

    Temporary free-usage / 429s only get a short model soft-block TTL so
    rotation skips the hot account briefly without killing the pool.
    """
    if not account_id or not _live_auto_disable():
        return None

    import grok2api.pool.account_pool as account_pool

    # Recoverable free-usage: stack durable account status in PostgreSQL.
    # Reference payload: subscription:free-usage-exhausted (+ tokens actual/limit).
    if is_temporary_usage_error(error, status_code):
        stacked = None
        try:
            stacked = account_pool.apply_free_usage_cooldown(
                account_id,
                error=error,
                status_code=status_code,
                model=model,
                source="upstream",
            )
        except Exception:
            stacked = None
        if stacked:
            return stacked
        # Fallback soft path if parser missed.
        reason = (
            f"临时额度耗尽，已冷却，等待下次测活成功"
            + (f" · {model}" if model else "")
        )[:300]
        if model:
            try:
                account_pool.block_model(
                    account_id,
                    model,
                    reason=reason,
                    source="temp_usage",
                    ttl_sec=float(account_pool.PROBE_KICK_COOLDOWN_SEC),
                )
            except Exception:
                pass
        return {"id": account_id, "in_cooldown": True, "reason": reason, "pool_status": "cooldown"}

    if is_account_block_error(error, status_code):
        reason = f"账号不可用 (HTTP {status_code}): {(error or '')[:120]}"
        # Soft kick first (cooldown), then hard disable if still broken on next probes.
        try:
            pol = account_pool.cooldown_defaults()
            kick_cd = float(pol.get("probe_kick_cooldown_sec") or 600.0)
        except Exception:
            kick_cd = 600.0
        kicked = account_pool.kick_from_pool(
            account_id, reason=reason, cooldown_sec=kick_cd
        )
        # Also mark disabled_for_quota for billing-style blocks so UI surfaces it.
        try:
            account_pool.disable_for_quota(
                account_id, reason=reason, source="model_health"
            )
        except Exception:
            pass
        return kicked or account_pool.disable_for_quota(
            account_id, reason=reason, source="model_health"
        )

    if model and is_model_unavailable_error(error, status_code):
        reason = f"模型不可用 (HTTP {status_code}): {(error or '')[:160]}"
        try:
            pol = account_pool.cooldown_defaults()
            durable_ttl = float(pol.get("durable_block_ttl") or 3600.0)
        except Exception:
            durable_ttl = 3600.0
        # Durable-but-not-forever: auto recheck via model_health probe cycle.
        blocked = account_pool.block_model(
            account_id,
            model,
            reason=reason,
            source="upstream_error",
            ttl_sec=durable_ttl,
        )
        # Also put the whole account on cooldown so other models aren't hammered
        # by a half-dead token during the same failure wave.
        try:
            cd = account_pool.compute_cooldown_seconds(
                status_code=status_code,
                error=error or reason,
                consecutive_fails=2,
            )
            account_pool.kick_from_pool(
                account_id,
                reason=reason,
                cooldown_sec=max(30.0, min(cd, durable_ttl)),
            )
        except Exception:
            pass
        return blocked
    return None


def _save_last_probe(account_id: str | None, result: dict[str, Any], *, overwrite: bool = True) -> None:
    """Persist probe snapshot on pool meta for admin UI.

    Field-level patch only — never rewrite the whole account_pool table, so an
    active durable cooldown cannot be wiped by a concurrent last_probe write.
    """
    if not account_id:
        return
    try:
        from grok2api.admin.settings_store import get_account_pool_meta, patch_account_pool_meta

        # Single-account read only — full-pool load here multiplies with models.
        meta = get_account_pool_meta(account_id) or {}
        if not isinstance(meta, dict):
            meta = {}
        snap = {
            "ok": bool(result.get("ok")),
            "available": bool(result.get("available")),
            "model": result.get("model"),
            "status_code": result.get("status_code"),
            "error": (result.get("error") or "")[:400] or None,
            "probed_at": result.get("probed_at") or time.time(),
            "source": result.get("source") or "manual",
            "auto_disabled": bool(result.get("auto_disabled")),
            "stream_ok": result.get("stream_ok"),
            "latency_ms": result.get("latency_ms"),
        }
        # Only update last_probe if it's an explicit probe, or if there is no
        # existing probe snapshot. API call failures must not overwrite the
        # admin/model-health probe display.
        existing = meta.get("last_probe")
        patch: dict[str, Any] = {}
        if overwrite or not existing:
            patch["last_probe"] = snap
        if not snap["available"] and snap.get("error") and overwrite:
            err = str(snap.get("error") or "")
            low = err.lower()
            if "free-usage-exhausted" in low or "free usage" in low:
                patch["last_error"] = (
                    f"[probe {snap.get('model')}] 临时额度耗尽，已冷却，等待下次测活成功"
                )[:300]
            else:
                # avoid storing huge JSON blobs in admin UI
                if err.startswith("{") and len(err) > 160:
                    err = err[:160] + "…"
                patch["last_error"] = f"[probe {snap.get('model')}] {err}"[:300]
        elif snap["available"]:
            # clear probe-sourced last_error prefix only if success
            le = meta.get("last_error") or ""
            if isinstance(le, str) and le.startswith("[probe "):
                patch["last_error"] = None
        if patch:
            patch_account_pool_meta(account_id, patch)
    except Exception:
        pass


def probe_model_for_creds(
    creds: GrokCredentials,
    model: str,
    *,
    auto_disable: bool | None = None,
    source: str = "manual",
    report_stats: bool = True,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Probe one account/model.

    Status mutations (cooldown / model soft-block / recover) happen ONLY for this
    scanned account, and ONLY when conditions are met:
      - fail + free-usage/temp → cooldown + model soft-block
      - fail + durable model/account issue → block/cooldown
      - success + currently cooling/blocked → clear cooldown & unblock model
    Probe does NOT call report_failure/report_success (those are live-traffic paths).
    last_probe is always written for the scanned account.
    """
    if auto_disable is None:
        auto_disable = _live_auto_disable()

    t0 = time.time()
    base: dict[str, Any] = {
        "ok": False,
        "available": False,
        "account_id": creds.auth_key,
        "email": creds.email,
        "user_id": creds.user_id,
        "model": model,
        "probed_at": t0,
        "source": source,
    }
    url = f"{UPSTREAM_BASE}/chat/completions"
    headers = upstream_headers(creds.token, model)
    headers["Accept"] = "text/event-stream, application/json"
    body = {
        "model": model,
        "messages": [{"role": "user", "content": "ping"}],
        "stream": True,
        "max_tokens": 8,
        "max_completion_tokens": 8,
    }

    def _apply_fail_status(err_text: str, status_code: int | None) -> None:
        """Mutate pool only when scanned and error class matches a policy."""
        if not auto_disable or not creds.auth_key:
            return
        try:
            import grok2api.pool.account_pool as account_pool
        except Exception:
            return
        action = None
        kick = None
        try:
            # free-usage / model-unavail / account-block policies
            action = handle_upstream_error_for_model(
                creds.auth_key,
                model=model,
                error=err_text,
                status_code=status_code,
            )
        except Exception:
            action = None
        try:
            # streak + cooldown path; free-usage never disables
            kick = account_pool.record_model_probe_outcome(
                creds.auth_key,
                model=model,
                available=False,
                error=err_text,
                status_code=status_code,
                source=source,
                auto_kick=True,
            )
        except Exception:
            kick = None
        if action or kick:
            merged = dict(action or {})
            if kick:
                merged["probe_kick"] = kick
                if kick.get("cooldown_until"):
                    merged["cooldown_until"] = kick.get("cooldown_until")
            base["auto_action"] = {
                "enabled": merged.get("enabled", True),
                "disabled_for_quota": merged.get("disabled_for_quota"),
                "blocked_model_ids": merged.get("blocked_model_ids")
                or merged.get("blocked_models"),
                "disabled_reason": merged.get("disabled_reason")
                or (kick or {}).get("action"),
                "probe_kick": kick,
            }
            # "auto_disabled" here means "status changed due to probe policy"
            base["auto_disabled"] = bool(
                (kick and kick.get("action") in ("cooldown", "disabled"))
                or merged.get("blocked_models")
                or merged.get("in_cooldown")
            )

    def _apply_success_status() -> None:
        """测活成功：冷却中 → 正常，并立即写库。

        Successful probe is the recovery signal for free-usage / temp cooldown.
        Always clear durable cooldown + soft model blocks for this account.
        """
        if not creds.auth_key:
            return
        try:
            import grok2api.pool.account_pool as account_pool
            from grok2api.admin.settings_store import get_account_pool_meta
        except Exception:
            return
        try:
            meta = get_account_pool_meta(creds.auth_key) or {}
        except Exception:
            meta = {}
        if not isinstance(meta, dict):
            meta = {}
        in_cd = False
        try:
            until = meta.get("cooldown_until")
            if until is not None:
                in_cd = time.time() < float(until)
        except Exception:
            in_cd = False
        # Prefer pool helper (merges redis) for accurate cooling flag.
        try:
            in_cd = bool(account_pool.is_in_cooldown(account_pool._pool_meta(creds.auth_key, {creds.auth_key: meta}))) or in_cd
        except Exception:
            pass
        blocked = meta.get("blocked_models") if isinstance(meta.get("blocked_models"), dict) else {}
        model_blocked = bool(model and isinstance(blocked, dict) and model in blocked)
        recovered = None
        # Always record successful probe → clears cooldown, sets pool_status=normal.
        try:
            recovered = account_pool.record_model_probe_outcome(
                creds.auth_key,
                model=model,
                available=True,
                source=source,
                auto_kick=True,
            )
        except Exception:
            recovered = None
        # Drop soft/temp model block for the probed model.
        if model_blocked:
            try:
                account_pool.unblock_model(creds.auth_key, model)
            except Exception:
                pass
        # Belt-and-suspenders: explicit cooldown clear so DB is definitely normal.
        if in_cd or (recovered and recovered.get("cleared_cooldown")):
            try:
                account_pool.clear_account_cooldown(creds.auth_key)
            except Exception:
                pass
        if in_cd or model_blocked or (recovered and recovered.get("cleared_cooldown")):
            base["auto_action"] = {
                "recovered": True,
                "cleared_cooldown": bool(in_cd or (recovered and recovered.get("cleared_cooldown"))),
                "unblocked_model": model_blocked,
                "pool_status": "normal",
            }
            base["pool_status"] = "normal"
            base["in_cooldown"] = False

    owns_client = client is None
    http = client or _probe_http_client(creds.auth_key)
    try:
        with http.stream("POST", url, headers=headers, json=body) as resp:
            status = resp.status_code
            if status >= 400:
                err_text = (resp.read()).decode("utf-8", errors="replace")[:800]
                base["status_code"] = status
                base["error"] = err_text
                base["available"] = False
                base["latency_ms"] = int((time.time() - t0) * 1000)
                # Do NOT report_failure here — probe is not live traffic.
                _apply_fail_status(err_text, status)
                _save_last_probe(creds.auth_key, base, overwrite=report_stats)
                return base

            got_data = False
            for line in resp.iter_lines():
                if not line:
                    continue
                if isinstance(line, bytes):
                    line = line.decode("utf-8", errors="replace")
                if line.startswith("data:"):
                    got_data = True
                    break
            base["ok"] = True
            base["available"] = True
            base["status_code"] = status
            base["stream_ok"] = got_data
            base["latency_ms"] = int((time.time() - t0) * 1000)
            # Do NOT report_success here — probe is not live traffic.
            _apply_success_status()
            _save_last_probe(creds.auth_key, base, overwrite=report_stats)
            return base
    except httpx.HTTPError as e:
        base["error"] = f"network: {e}"
        base["latency_ms"] = int((time.time() - t0) * 1000)
        # Network errors: only record last_probe; do not cool/disable unless auto_disable
        # and we treat as temporary server issue via probe outcome streak.
        if auto_disable:
            _apply_fail_status(base["error"], 502)
        _save_last_probe(creds.auth_key, base, overwrite=report_stats)
        return base
    except Exception as e:  # noqa: BLE001
        base["error"] = str(e)[:300]
        base["latency_ms"] = int((time.time() - t0) * 1000)
        if auto_disable:
            _apply_fail_status(base["error"], 502)
        _save_last_probe(creds.auth_key, base, overwrite=report_stats)
        return base
    finally:
        # Shared client is never closed here; only caller-owned clients would be.
        _ = owns_client



async def probe_model_for_creds_async(
    creds: GrokCredentials,
    model: str,
    *,
    auto_disable: bool | None = None,
    source: str = "manual",
) -> dict[str, Any]:
    import asyncio

    return await asyncio.to_thread(
        probe_model_for_creds,
        creds,
        model,
        auto_disable=auto_disable,
        source=source,
    )


def probe_single_account(
    account_id: str,
    model: str | None = None,
    *,
    auto_disable: bool | None = None,
    source: str = "manual",
) -> dict[str, Any]:
    """Probe one account with one model (default DEFAULT / PROBE_MODELS[0])."""
    defaults = _live_probe_models()
    model = (model or (defaults[0] if defaults else DEFAULT_MODEL)).strip()
    creds = load_credentials_by_id(account_id)
    result = probe_model_for_creds(
        creds, model, auto_disable=auto_disable, source=source
    )
    return {
        "ok": bool(result.get("available")),
        "account_id": result.get("account_id") or account_id,
        "email": result.get("email") or creds.email,
        "result": result,
    }


def _unique_live_creds(*, auto_refresh: bool = False) -> list[GrokCredentials]:
    """De-dupe live credentials. Default auto_refresh=False avoids startup storms."""
    all_c = list_live_credentials(include_expired=False, auto_refresh=auto_refresh)
    seen: set[str] = set()
    out: list[GrokCredentials] = []
    for c in all_c:
        uid = c.user_id or c.auth_key or ""
        if uid in seen:
            continue
        seen.add(uid)
        out.append(c)
    # Deterministic order so full probe starts from the first account stably.
    out.sort(key=lambda c: (c.auth_key or c.user_id or c.email or ""))
    return out


def _account_key(c: GrokCredentials) -> str:
    return (c.auth_key or c.user_id or "").strip()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _last_probe_at(meta: dict[str, Any] | None) -> float:
    """Best-effort last probe timestamp from pool meta / last_probe snapshot."""
    if not isinstance(meta, dict):
        return 0.0
    for key in ("last_probe_ok_at", "last_probe_fail_at"):
        ts = _safe_float(meta.get(key), 0.0)
        if ts > 0:
            return ts
    snap = meta.get("last_probe")
    if isinstance(snap, dict):
        return _safe_float(snap.get("probed_at"), 0.0)
    return 0.0


def _load_probe_priority_meta(
    creds_list: list[GrokCredentials],
) -> dict[str, dict[str, Any]]:
    """Batch-load pool meta used only for background probe ordering.

    Large uncovered sets (thousands+) prefer the warm full pool-state cache so
    we do not issue a giant multi-id query just to order one batch. Falls back
    to empty meta when unavailable — selection then degrades to fair sequential
    sweep (still non-repeat).
    """
    ids = [_account_key(c) for c in creds_list if _account_key(c)]
    if not ids:
        return {}

    def _from_state(state: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
        if not isinstance(state, dict) or not state:
            return {}
        out: dict[str, dict[str, Any]] = {}
        for aid in ids:
            meta = state.get(aid)
            if isinstance(meta, dict):
                out[aid] = meta  # read-only for sort; no per-row copy
        return out

    # Prefer warm full-state for big windows; small windows can use many-ids.
    if len(ids) >= 200:
        try:
            from grok2api.admin.settings_store import get_cached_account_pool_state, get_account_pool_state

            cached = None
            try:
                cached = get_cached_account_pool_state()
            except Exception:
                cached = None
            hit = _from_state(cached if isinstance(cached, dict) else None)
            if hit:
                return hit
            hit = _from_state(get_account_pool_state() or {})
            if hit:
                return hit
        except Exception:
            pass

    try:
        from grok2api.admin.settings_store import get_account_pool_meta_many

        raw = get_account_pool_meta_many(ids)
        if isinstance(raw, dict) and raw:
            return {
                str(k): (v if isinstance(v, dict) else {})
                for k, v in raw.items()
                if k
            }
    except Exception:
        pass
    # Full-state fallback (file mode / older stores).
    try:
        from grok2api.admin.settings_store import get_account_pool_state

        return _from_state(get_account_pool_state() or {})
    except Exception:
        return {}


def _background_probe_priority(
    creds: GrokCredentials,
    meta: dict[str, Any] | None,
    *,
    now: float,
) -> tuple[int, float, float, str]:
    """Lower tuple sorts first for background selection.

    Priority bands (within uncovered set of a sweep generation):
      0  cooldown due / expired marker — need recovery verification
      1  never probed — unknown health
      2  recent probe failure / fail streak — recheck sooner
      3  normal healthy — fair sweep by oldest probe time
      9  hard-disabled / quota-disabled — last resort only
    """
    aid = _account_key(creds)
    m = meta if isinstance(meta, dict) else {}
    enabled = m.get("enabled", True)
    if enabled is False or bool(m.get("disabled_for_quota")):
        return (9, 0.0, 0.0, aid)

    last_at = _last_probe_at(m)
    streak = max(0, _safe_int(m.get("probe_fail_streak"), 0))
    until = _safe_float(m.get("cooldown_until"), 0.0)
    count = max(0, _safe_int(m.get("cooldown_count"), 0))
    stack = m.get("status_stack")
    stacked = isinstance(stack, list) and len(stack) > 0
    pool_status = str(m.get("pool_status") or "").strip().lower()
    last_status = str(m.get("last_probe_status") or "").strip().lower()
    cooling = (
        stacked
        or count > 0
        or pool_status == "cooldown"
        or last_status == "cooldown"
        or (until > 0 and now < until)
    )
    # Cooldown whose until marker already elapsed, or count-stack cooling with
    # no fresh success — verify recovery first so free-usage accounts return.
    cooldown_due = cooling and (until <= 0 or now >= until or count > 0 or stacked)
    if cooldown_due:
        # Oldest due first (until=0 → treat as very due).
        due_rank = until if until > 0 else 0.0
        return (0, due_rank, last_at or 0.0, aid)

    if last_at <= 0:
        return (1, 0.0, 0.0, aid)

    snap = m.get("last_probe")
    if isinstance(snap, dict) and "ok" in snap:
        last_ok = bool(snap.get("ok"))
    elif isinstance(snap, dict) and "available" in snap:
        last_ok = bool(snap.get("available"))
    else:
        last_ok = True
    if streak > 0 or last_status in ("error", "fail", "failed", "cooldown") or last_ok is False:
        # Higher streak first, then oldest fail.
        return (2, -float(streak), last_at, aid)

    # Healthy: oldest successful/checked first so fair full-pool coverage.
    return (3, last_at, 0.0, aid)


def _sort_background_probe_candidates(
    creds_list: list[GrokCredentials],
    *,
    meta_by_id: dict[str, dict[str, Any]] | None = None,
    now: float | None = None,
) -> list[GrokCredentials]:
    """Stable priority sort for background batch selection."""
    t0 = float(now if now is not None else time.time())
    metas = meta_by_id if isinstance(meta_by_id, dict) else {}
    decorated: list[tuple[tuple[Any, ...], GrokCredentials]] = []
    for c in creds_list:
        aid = _account_key(c)
        key = _background_probe_priority(c, metas.get(aid), now=t0)
        decorated.append((key, c))
    decorated.sort(key=lambda item: item[0])
    return [c for _, c in decorated]


def _priority_band_counts(
    creds_list: list[GrokCredentials],
    *,
    meta_by_id: dict[str, dict[str, Any]] | None = None,
    now: float | None = None,
) -> dict[str, int]:
    t0 = float(now if now is not None else time.time())
    metas = meta_by_id if isinstance(meta_by_id, dict) else {}
    counts = {
        "cooldown_due": 0,
        "never_probed": 0,
        "fail_streak": 0,
        "healthy": 0,
        "disabled": 0,
    }
    band_to_key = {
        0: "cooldown_due",
        1: "never_probed",
        2: "fail_streak",
        3: "healthy",
        9: "disabled",
    }
    for c in creds_list:
        aid = _account_key(c)
        band = _background_probe_priority(c, metas.get(aid), now=t0)[0]
        counts[band_to_key.get(band, "healthy")] += 1
    return counts


def _priority_band_of(
    creds: GrokCredentials,
    meta: dict[str, Any] | None,
    *,
    now: float,
) -> int:
    return int(_background_probe_priority(creds, meta, now=now)[0])


def _is_recoverable_probe_result(result: dict[str, Any] | None) -> bool:
    """True when this probe outcome should NOT permanently mark sweep covered.

    Free-usage / rate-limit / still-cooling failures need rechecks inside the
    same generation; treating them as covered freezes recovery for hours on
    large pools.
    """
    if not isinstance(result, dict):
        return False
    if result.get("available"):
        return False
    err = str(result.get("error") or "")
    code = result.get("status_code")
    try:
        code_i = int(code) if code is not None else None
    except (TypeError, ValueError):
        code_i = None
    if is_temporary_usage_error(err, code_i):
        return True
    act = result.get("auto_action") if isinstance(result.get("auto_action"), dict) else {}
    kick = act.get("probe_kick") if isinstance(act.get("probe_kick"), dict) else {}
    if kick.get("action") == "cooldown":
        return True
    if act.get("in_cooldown") or result.get("in_cooldown"):
        return True
    low = err.lower()
    if "cooldown" in low or "free-usage" in low or "rate limit" in low or "too many" in low:
        return True
    return False


def _account_probe_aggregate(results: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Collapse multi-model results into one row per account for sweep decisions."""
    by_id: dict[str, dict[str, Any]] = {}
    for r in results or []:
        if not isinstance(r, dict):
            continue
        aid = str(r.get("account_id") or "").strip()
        if not aid:
            continue
        cur = by_id.get(aid)
        if cur is None:
            by_id[aid] = dict(r)
            continue
        # Prefer available=True if any model succeeded; else keep latest fail.
        if r.get("available") and not cur.get("available"):
            by_id[aid] = dict(r)
        elif (not cur.get("available")) and (not r.get("available")):
            # Prefer recoverable signal so we do not over-cover.
            if _is_recoverable_probe_result(r) and not _is_recoverable_probe_result(cur):
                by_id[aid] = dict(r)
            else:
                by_id[aid] = dict(r)
    return by_id


def _pick_priority_reserved_batch(
    remaining_sorted: list[GrokCredentials],
    *,
    max_accounts: int,
    meta_by_id: dict[str, dict[str, Any]] | None,
    now: float,
) -> list[GrokCredentials]:
    """Reserve part of each cycle for recovery/unknown before healthy fair-scan.

    Without a reservation, a giant healthy uncovered set can starve cooldown
    recovery for many cycles even though priority sort puts them first — if
    those cool accounts were already marked covered earlier in the generation.
    Re-admission of recoverable ids (see probe_account_models) pairs with this.
    """
    n = max(0, int(max_accounts or 0))
    if n <= 0 or not remaining_sorted:
        return []
    # ~60% of the batch for recovery bands, rest for fair healthy/disabled.
    reserved = max(1, min(n, int(round(n * 0.6)))) if n >= 2 else n
    recovery: list[GrokCredentials] = []
    rest: list[GrokCredentials] = []
    metas = meta_by_id if isinstance(meta_by_id, dict) else {}
    for c in remaining_sorted:
        band = _priority_band_of(c, metas.get(_account_key(c)), now=now)
        if band in (0, 1, 2):
            recovery.append(c)
        else:
            rest.append(c)
    out: list[GrokCredentials] = []
    out.extend(recovery[:reserved])
    if len(out) < n:
        out.extend(rest[: n - len(out)])
    # If recovery was smaller than reserved, fill from remaining recovery tail
    # then rest already handled; if still short, append leftover recovery.
    if len(out) < n:
        seen = {_account_key(c) for c in out}
        for c in recovery[reserved:] + rest:
            aid = _account_key(c)
            if aid in seen:
                continue
            out.append(c)
            seen.add(aid)
            if len(out) >= n:
                break
    return out[:n]


def _sweep_ttl() -> int:
    try:
        interval = float(
            os.getenv("GROK2API_MODEL_HEALTH_INTERVAL", str(MODEL_HEALTH_INTERVAL))
            or MODEL_HEALTH_INTERVAL
        )
    except Exception:
        interval = float(MODEL_HEALTH_INTERVAL or 600)
    # Keep at least 6h, or ~3× full estimated coverage window.
    return max(int(_SWEEP_TTL_SEC), int(max(600.0, interval) * 36))


def _sweep_load() -> tuple[int, set[str], float]:
    """Return (generation, covered_ids, started_at)."""
    # Prefer Redis (shared across workers / restarts).
    try:
        from grok2api.store.redis_client import (
            get_str,
            key,
            redis_enabled,
            scard,
            smembers,
        )

        if redis_enabled():
            meta_raw = get_str(key(*_SWEEP_META_KEY)) or ""
            gen = 0
            started = 0.0
            if meta_raw:
                # format: gen|started_at
                parts = str(meta_raw).split("|", 1)
                try:
                    gen = int(parts[0] or 0)
                except (TypeError, ValueError):
                    gen = 0
                if len(parts) > 1:
                    try:
                        started = float(parts[1] or 0)
                    except (TypeError, ValueError):
                        started = 0.0
            covered = smembers(key(*_SWEEP_COVERED_KEY))
            # sanity: if set exists but gen is 0, still return covered
            _ = scard  # keep import used for optional debug callers
            return gen, covered, started
    except Exception:
        pass
    with _lock:
        covered = set(_local_sweep.get("covered") or set())
        return (
            int(_local_sweep.get("generation") or 0),
            covered,
            float(_local_sweep.get("started_at") or 0.0),
        )


def _sweep_start_new(live_ids: list[str] | None = None) -> tuple[int, set[str], float]:
    """Begin a new sweep generation (clear covered)."""
    now = time.time()
    gen = int(now)  # monotonic enough across restarts
    try:
        from grok2api.store.redis_client import (
            delete,
            key,
            redis_enabled,
            set_ex,
        )

        if redis_enabled():
            delete(key(*_SWEEP_COVERED_KEY))
            set_ex(key(*_SWEEP_META_KEY), f"{gen}|{now}", _sweep_ttl())
            with _lock:
                _local_sweep["generation"] = gen
                _local_sweep["started_at"] = now
                _local_sweep["covered"] = set()
            return gen, set(), now
    except Exception:
        pass
    with _lock:
        _local_sweep["generation"] = gen
        _local_sweep["started_at"] = now
        _local_sweep["covered"] = set()
        return gen, set(), now


def _sweep_mark_covered(account_ids: list[str]) -> int:
    """Mark accounts covered in the current sweep. Returns new covered total (best-effort)."""
    ids = [a for a in account_ids if a]
    if not ids:
        try:
            from grok2api.store.redis_client import key, redis_enabled, scard

            if redis_enabled():
                return scard(key(*_SWEEP_COVERED_KEY))
        except Exception:
            pass
        with _lock:
            return len(_local_sweep.get("covered") or set())
    try:
        from grok2api.store.redis_client import (
            expire,
            key,
            redis_enabled,
            sadd,
            scard,
            set_ex,
            get_str,
        )

        if redis_enabled():
            # Ensure meta exists / TTL refreshed
            meta = get_str(key(*_SWEEP_META_KEY))
            if not meta:
                gen, _, started = _sweep_start_new()
            else:
                set_ex(key(*_SWEEP_META_KEY), meta, _sweep_ttl())
            sadd(key(*_SWEEP_COVERED_KEY), *ids, ttl_sec=_sweep_ttl())
            expire(key(*_SWEEP_COVERED_KEY), _sweep_ttl())
            with _lock:
                cov = _local_sweep.setdefault("covered", set())
                if not isinstance(cov, set):
                    cov = set()
                    _local_sweep["covered"] = cov
                cov.update(ids)
            return scard(key(*_SWEEP_COVERED_KEY))
    except Exception:
        pass
    with _lock:
        cov = _local_sweep.setdefault("covered", set())
        if not isinstance(cov, set):
            cov = set()
            _local_sweep["covered"] = cov
        cov.update(ids)
        return len(cov)


def _select_probe_batch(
    creds_list: list[GrokCredentials],
    *,
    max_accounts: int,
    source: str,
) -> tuple[list[GrokCredentials], dict[str, Any]]:
    """Pick up to max_accounts for this cycle.

    Background source uses a strict non-repeat sweep: each live account is
    probed at most once per generation. Within the uncovered set, order is
    priority-aware so multi-k/10k pools recover cooldown / unknown accounts
    before re-checking healthy ones. Manual full probe stays sequential.
    """
    info: dict[str, Any] = {
        "mode": "priority",
        "sweep_generation": None,
        "sweep_covered": 0,
        "sweep_live": len(creds_list),
        "sweep_remaining": len(creds_list),
        "sweep_reset": False,
        "priority": None,
    }
    if max_accounts <= 0 or not creds_list:
        return [], info

    # Admin full probe / manual: sequential from the first account (stable order).
    # Do not prioritize fails — user expects a full pass from account #1.
    if source != "background":
        def _stable_key(c: GrokCredentials) -> str:
            return (c.auth_key or c.user_id or c.email or "")

        # Preserve input order if already stable; sort by id for deterministic "from first".
        ordered = sorted(list(creds_list), key=_stable_key)
        info["mode"] = "sequential"
        info["sweep_remaining"] = max(0, len(ordered) - max_accounts)
        return ordered[:max_accounts], info

    # ── strict sweep + priority within uncovered ──────────────────────────
    live_ids = [_account_key(c) for c in creds_list if _account_key(c)]
    live_set = set(live_ids)
    gen, covered, started = _sweep_load()
    if gen <= 0:
        gen, covered, started = _sweep_start_new(live_ids)
        info["sweep_reset"] = True

    # Drop covered ids that no longer exist (deleted accounts).
    covered = {x for x in covered if x in live_set}

    now = time.time()
    # Read meta for the full live set so cooldown accounts already marked
    # covered can be re-admitted for recovery verification.
    meta_all = _load_probe_priority_meta(creds_list)
    remaining = [c for c in creds_list if _account_key(c) not in covered]

    # Re-admit covered accounts that still look cooldown/fail-recoverable and
    # have not been probed too recently (avoid hot-looping the same 429s).
    recheck_min_age = max(60.0, float(MODEL_HEALTH_INTERVAL or 300.0) * 0.5)
    re_admit: list[GrokCredentials] = []
    for c in creds_list:
        aid = _account_key(c)
        if not aid or aid not in covered:
            continue
        meta = meta_all.get(aid) or {}
        band = _priority_band_of(c, meta, now=now)
        if band not in (0, 2):
            continue
        last_at = _last_probe_at(meta)
        if last_at > 0 and (now - last_at) < recheck_min_age:
            continue
        re_admit.append(c)

    # If nothing left (and no re-admit), start a new sweep generation.
    if not remaining and not re_admit:
        gen, covered, started = _sweep_start_new(live_ids)
        remaining = list(creds_list)
        re_admit = []
        info["sweep_reset"] = True
        print(
            f"  [model-health] sweep reset gen={gen} live={len(live_ids)} "
            f"(previous generation fully covered)"
        )
        # New generation: refresh meta window.
        meta_all = _load_probe_priority_meta(remaining)

    # Merge remaining + re-admit (dedupe), then priority sort.
    merged: list[GrokCredentials] = []
    seen_ids: set[str] = set()
    for c in re_admit + remaining:
        aid = _account_key(c)
        if not aid or aid in seen_ids:
            continue
        seen_ids.add(aid)
        merged.append(c)

    remaining_sorted = _sort_background_probe_candidates(
        merged, meta_by_id=meta_all, now=now
    )
    batch = _pick_priority_reserved_batch(
        remaining_sorted,
        max_accounts=max_accounts,
        meta_by_id=meta_all,
        now=now,
    )
    batch_priority = _priority_band_counts(batch, meta_by_id=meta_all, now=now)
    remaining_priority = _priority_band_counts(
        remaining_sorted, meta_by_id=meta_all, now=now
    )
    info.update(
        {
            "mode": "priority_sweep",
            "sweep_generation": gen,
            "sweep_covered": len(covered),
            "sweep_live": len(live_ids),
            "sweep_remaining": max(0, len(remaining_sorted) - len(batch)),
            "sweep_started_at": started or None,
            "re_admit": len(re_admit),
            "priority": {
                "batch": batch_priority,
                "remaining": remaining_priority,
            },
        }
    )
    return batch, info


def probe_account_models(
    account_id: str | None = None,
    models: list[str] | None = None,
    *,
    auto_disable: bool | None = None,
    source: str = "manual",
    max_workers: int | None = None,
    max_accounts: int | None = None,
    max_models_per_account: int | None = None,
    cycle_budget_sec: float | None = None,
) -> dict[str, Any]:
    """Probe one or all accounts for model availability (concurrency-capped).

    Background source rotates a single model per cycle to avoid accounts×models
    explosion when PROBE_MODELS is long. Manual paths may probe a small cap of
    models per account, still sequential per account so one bad model cannot
    multiply concurrent load for the same token.
    """
    all_models = _normalize_probe_models(models)
    if source == "background":
        # One model per background cycle — full list covered across cycles.
        cycle_models = _pick_background_models(all_models)
    else:
        cycle_models = _models_for_account(
            all_models, source=source, max_models=max_models_per_account
        )

    sweep_info: dict[str, Any] = {}
    if account_id:
        creds_list = [load_credentials_by_id(account_id)]
        deferred = 0
    else:
        # Do NOT auto-refresh all tokens here — token_maintainer owns that path.
        all_creds = _unique_live_creds(auto_refresh=False)
        deferred = 0
        # Background cycles batch; manual all can go larger but still hard-capped
        if max_accounts is None:
            if source == "background":
                max_accounts = MODEL_PROBE_BATCH
            elif source in ("manual_all", "manual", "admin"):
                # Admin "全部模型探测" should cover every live account once.
                max_accounts = len(all_creds)
            else:
                max_accounts = MODEL_PROBE_BATCH * 2
        if max_accounts and len(all_creds) > max_accounts:
            deferred = len(all_creds) - max_accounts
        creds_list, sweep_info = _select_probe_batch(
            all_creds, max_accounts=int(max_accounts or len(all_creds)), source=source
        )
        # For strict sweep, deferred = remaining uncovered after this batch
        if sweep_info.get("mode") == "strict_sweep":
            deferred = int(sweep_info.get("sweep_remaining") or 0)

    results: list[dict[str, Any]] = []
    budget = (
        float(cycle_budget_sec)
        if cycle_budget_sec is not None
        else float(_CYCLE_BUDGET_SEC)
    )
    budget = max(15.0, budget)
    deadline = time.time() + budget
    budget_hit = False
    def _probe_account(creds: GrokCredentials) -> list[dict[str, Any]]:
        """Probe models for one account sequentially (same token, less stampede)."""
        # Per-account client so outbound proxy stickiness matches live traffic.
        account_client = _probe_http_client(creds.auth_key)
        out: list[dict[str, Any]] = []
        for model in cycle_models:
            if time.time() >= deadline:
                break
            try:
                out.append(
                    probe_model_for_creds(
                        creds,
                        model,
                        auto_disable=auto_disable,
                        source=source,
                        client=account_client,
                    )
                )
            except Exception as e:  # noqa: BLE001
                out.append(
                    {
                        "ok": False,
                        "available": False,
                        "account_id": creds.auth_key,
                        "email": creds.email,
                        "model": model,
                        "error": str(e)[:300],
                        "source": source,
                        "probed_at": time.time(),
                    }
                )
        return out

    # Parallelize across accounts; models for one account stay sequential.
    workers = max_workers if max_workers is not None else MODEL_PROBE_WORKERS
    workers = min(int(workers), max(1, len(creds_list))) if creds_list else 1
    skipped_accounts = 0
    if creds_list:
        with ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="model-probe-"
        ) as ex:
            futures = []
            for c in creds_list:
                if time.time() >= deadline:
                    budget_hit = True
                    skipped_accounts += 1
                    continue
                futures.append(ex.submit(_probe_account, c))
            for fut in as_completed(futures):
                try:
                    results.extend(fut.result())
                except Exception as e:  # noqa: BLE001
                    results.append(
                        {
                            "ok": False,
                            "available": False,
                            "error": str(e)[:300],
                            "source": source,
                            "probed_at": time.time(),
                        }
                    )
                if time.time() >= deadline:
                    budget_hit = True

    # Mark covered after probes complete.
    # Recoverable free-usage / rate-limit failures stay UNcovered so the same
    # generation can re-check them after a cool-down gap (see re_admit path).
    # Only mark accounts we actually attempted so budget-cut leftovers stay pending.
    if source == "background" and not account_id:
        by_account = _account_probe_aggregate(results)
        cover_ids: list[str] = []
        hold_ids: list[str] = []
        for aid, row in by_account.items():
            if _is_recoverable_probe_result(row):
                hold_ids.append(aid)
            else:
                cover_ids.append(aid)
        # If no results (all skipped by budget), do not advance sweep.
        if cover_ids:
            covered_total = _sweep_mark_covered(cover_ids)
            if sweep_info is not None:
                sweep_info["sweep_covered"] = covered_total
                live_n = int(sweep_info.get("sweep_live") or 0)
                if live_n:
                    sweep_info["sweep_remaining"] = max(0, live_n - covered_total)
                    deferred = int(sweep_info["sweep_remaining"])
                sweep_info["held_recoverable"] = len(hold_ids)
        elif hold_ids and sweep_info is not None:
            sweep_info["held_recoverable"] = len(hold_ids)
        elif budget_hit and sweep_info is not None:
            # Keep previous remaining estimate when nothing ran.
            deferred = int(sweep_info.get("sweep_remaining") or deferred)

    if budget_hit:
        # Accounts we intended to probe this cycle but skipped due to budget.
        deferred = int(deferred or 0) + int(skipped_accounts)

    available = sum(1 for r in results if r.get("available"))
    blocked = sum(
        1 for r in results if not r.get("available") and r.get("auto_disabled")
    )
    out = {
        "ok": True,
        "probed_at": time.time(),
        "models": cycle_models,
        "models_configured": all_models,
        "models_per_account": len(cycle_models),
        "count": len(results),
        "available_count": available,
        "unavailable_count": len(results) - available,
        "auto_action_count": blocked,
        "deferred": deferred,
        "workers": workers,
        "budget_sec": budget,
        "budget_hit": budget_hit,
        "results": results,
        "source": source,
    }
    if sweep_info:
        out["sweep"] = sweep_info
    return out


def probe_all_accounts_concurrent(
    models: list[str] | None = None,
    *,
    auto_disable: bool | None = None,
    source: str = "manual",
    max_workers: int | None = None,
    max_accounts: int | None = None,
) -> dict[str, Any]:
    """Probe accounts concurrently (admin UI "全部模型探测") with hard caps."""
    if max_workers is None:
        max_workers = MODEL_PROBE_WORKERS
    # Reuse batched probe_account_models for consistent limits
    return probe_account_models(
        None,
        models,
        auto_disable=auto_disable,
        source=source,
        max_workers=max_workers,
        max_accounts=max_accounts,
    )


# ── Background periodic checker ─────────────────────────────────────────────


def _interval() -> float:
    try:
        from grok2api.admin.settings_store import get_model_health_interval_sec

        return max(0.0, float(get_model_health_interval_sec()))
    except Exception:
        pass
    try:
        # 0 = disabled (on-demand only)
        v = float(os.getenv("GROK2API_MODEL_HEALTH_INTERVAL", str(MODEL_HEALTH_INTERVAL)))
        return max(0.0, v)
    except ValueError:
        return float(MODEL_HEALTH_INTERVAL)


def run_once(*, source: str = "background") -> dict[str, Any]:
    """Probe a batch of live accounts with PROBE_MODELS (error check cycle)."""
    # Background cycles defer quickly if token refresh holds the slot so they
    # never stampede together. Manual admin "probe all" waits longer.
    wait_timeout = 5.0 if source == "background" else None
    with maintenance_slot(
        f"model_health:{source}",
        blocking=True,
        timeout=wait_timeout,
    ) as got:
        if not got:
            result = {
                "ok": True,
                "deferred_busy": True,
                "error": "maintenance slot busy — deferred",
                "source": source,
                "probed_at": time.time(),
                "count": 0,
                "available_count": 0,
                "unavailable_count": 0,
                "auto_action_count": 0,
                "kick_cooldown": 0,
                "kick_disabled": 0,
                "results": [],
            }
            with _lock:
                _last_run.clear()
                _last_run.update(result)
                _last_run["at"] = time.time()
            if source == "background":
                print("  [model-health] deferred: maintenance slot busy")
            return result
        # Reclaim free-usage / temporary cooldowns whose wall-clock TTL elapsed
        # so pool_status does not stay "cooldown" after the window ends.
        try:
            import grok2api.pool.account_pool as _ap

            expired = _ap.expire_due_cooldowns(limit=300)
            if expired.get("cleared"):
                print(
                    f"  [model-health] expired cooldowns cleared="
                    f"{expired.get('cleared')} scanned={expired.get('scanned')}"
                )
        except Exception:
            pass
        # Prefer accounts that look unhealthy / never probed so kicks land faster.
        workers = None
        # Keep cycles inside maintenance lock budget even with many models.
        cycle_budget = float(_CYCLE_BUDGET_SEC)
        if source == "manual_all":
            try:
                workers = max(int(MODEL_PROBE_WORKERS), min(8, int(MODEL_PROBE_WORKERS) * 2))
            except Exception:
                workers = MODEL_PROBE_WORKERS
            # Admin full probe may run longer, still below lock timeout.
            try:
                from grok2api.config import MAINTENANCE_LOCK_TIMEOUT as _mlt

                cycle_budget = min(max(60.0, float(_mlt) * 0.8), 240.0)
            except Exception:
                cycle_budget = 150.0
        result = probe_account_models(
            None,
            _live_probe_models(),
            auto_disable=_live_auto_disable(),
            source=source,
            max_workers=workers,
            cycle_budget_sec=cycle_budget,
        )
        try:
            import grok2api.pool.account_pool as account_pool

            # Opportunistic cleanup of expired soft model blocks each cycle.
            pruned = account_pool.prune_expired_model_blocks()
            if pruned:
                result["pruned_model_blocks"] = pruned
        except Exception:
            pass
    # Aggregate kick actions from probe results for operator visibility.
    kick_cd = 0
    kick_dis = 0
    for r in result.get("results") or []:
        act = (r.get("auto_action") or {}).get("probe_kick") or {}
        if act.get("action") == "cooldown":
            kick_cd += 1
        elif act.get("action") == "disabled":
            kick_dis += 1
    result["kick_cooldown"] = kick_cd
    result["kick_disabled"] = kick_dis
    # Durable task log for admin「任务日志」.
    # Always keep manual probes; for background only log when something happened.
    if not result.get("deferred_busy"):
        try:
            import grok2api.admin.task_log as task_log

            count = int(result.get("count") or 0)
            available = int(result.get("available_count") or 0)
            unavailable = int(result.get("unavailable_count") or 0)
            auto_n = int(result.get("auto_action_count") or 0)
            is_manual = str(source or "").startswith("manual")
            meaningful = bool(
                is_manual
                or count
                or auto_n
                or kick_cd
                or kick_dis
                or result.get("pruned_model_blocks")
                or result.get("ok") is False
            )
            if meaningful:
                summary = (
                    f"模型探测[{source}]：可用 {available}/{count}"
                    f" · 冷却踢出 {kick_cd} · 禁用 {kick_dis}"
                )
                st = "done"
                if unavailable and available:
                    st = "partial"
                elif unavailable and not available and count:
                    st = "error"
                task_log.record(
                    "probe",
                    summary=summary,
                    status=st,
                    ok=bool(result.get("ok", True)) and (available > 0 or count == 0),
                    progress_done=available,
                    progress_total=count,
                    detail={
                        "source": source,
                        "count": count,
                        "available_count": available,
                        "unavailable_count": unavailable,
                        "auto_action_count": auto_n,
                        "kick_cooldown": kick_cd,
                        "kick_disabled": kick_dis,
                        "pruned_model_blocks": result.get("pruned_model_blocks"),
                    },
                )
        except Exception:
            pass
    # Drop per-account payloads from last_run so /health and admin status stay small.
    slim = {
        k: v
        for k, v in result.items()
        if k != "results"
    }
    # Keep a tiny sample for debugging, not full rows.
    sample = []
    for r in (result.get("results") or [])[:5]:
        if not isinstance(r, dict):
            continue
        sample.append(
            {
                "account_id": r.get("account_id"),
                "email": r.get("email"),
                "available": r.get("available"),
                "status_code": r.get("status_code"),
                "error": (r.get("error") or "")[:120] or None,
            }
        )
    slim["results_sample"] = sample
    slim["at"] = time.time()
    with _lock:
        _last_run.clear()
        _last_run.update(slim)
    # Mirror for non-leader workers / admin UI.
    try:
        from grok2api.store.redis_client import key, redis_enabled, set_ex
        import json as _json

        if redis_enabled():
            set_ex(
                key("model_health", "last_run"),
                _json.dumps(slim, ensure_ascii=False, default=str),
                7200,
            )
    except Exception:
        pass
    bad = [r for r in result.get("results") or [] if not r.get("available")]
    sweep = result.get("sweep") or {}
    if bad or result.get("deferred") or kick_cd or kick_dis or sweep:
        sw = ""
        if sweep:
            pr = (sweep.get("priority") or {}).get("batch") or {}
            pr_txt = ""
            if pr:
                pr_txt = (
                    f" prio[cd={pr.get('cooldown_due', 0)}"
                    f"/new={pr.get('never_probed', 0)}"
                    f"/fail={pr.get('fail_streak', 0)}"
                    f"/ok={pr.get('healthy', 0)}]"
                )
            sw = (
                f" sweep={sweep.get('mode')} gen={sweep.get('sweep_generation')} "
                f"covered={sweep.get('sweep_covered')}/{sweep.get('sweep_live')} "
                f"left={sweep.get('sweep_remaining')}"
                + (" reset" if sweep.get("sweep_reset") else "")
                + pr_txt
            )
        print(
            f"  [model-health] cycle: {result.get('available_count')}/"
            f"{result.get('count')} ok; "
            f"{len(bad)} error(s); deferred={result.get('deferred')} "
            f"— auto_action={result.get('auto_action_count')} "
            f"kick_cd={kick_cd} kick_off={kick_dis}{sw}"
        )
    return result


def request_run_soon() -> None:
    _wakeup.set()


def _startup_delay() -> float:
    try:
        return max(15.0, float(MODEL_HEALTH_STARTUP_DELAY))
    except Exception:
        return 90.0


def _worker() -> None:
    # Stagger well after token maintainer so we never double-fan-out on boot
    # (700 accounts × probe was freezing WSL via thread/network peak).
    if _stop.wait(_startup_delay()):
        return
    while not _stop.is_set():
        if not is_enabled():
            _wakeup.clear()
            _wakeup.wait(timeout=5.0)
            continue
        interval = _interval()
        if interval <= 0:
            # disabled: sleep long, only run on wakeup
            _wakeup.clear()
            triggered = _wakeup.wait(timeout=3600.0)
            if _stop.is_set():
                break
            if triggered:
                run_once(source="manual_all")
            continue
        try:
            run_once(source="background")
        except Exception as e:  # noqa: BLE001
            with _lock:
                _last_run.clear()
                _last_run.update({"ok": False, "error": str(e)[:400], "at": time.time()})
            print(f"  [model-health] cycle error: {e}")
        _wakeup.clear()
        triggered = _wakeup.wait(timeout=interval)
        if _stop.is_set():
            break
        if triggered:
            try:
                run_once(source="manual_all")
            except Exception as e:  # noqa: BLE001
                print(f"  [model-health] forced cycle error: {e}")


def is_enabled() -> bool:
    try:
        from grok2api.admin.settings_store import get_model_health_enabled
        return bool(get_model_health_enabled())
    except Exception:
        return os.getenv("GROK2API_MODEL_HEALTH", "1").lower() not in ("0", "false", "no")


def start_background() -> None:
    global _thread
    if not is_enabled():
        return
    if _thread and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(
        target=_worker, name="g2a-model-health", daemon=True
    )
    _thread.start()


def stop_background() -> None:
    global _thread
    _stop.set()
    _wakeup.set()
    th = _thread
    if th and th.is_alive():
        th.join(timeout=2.0)
    _thread = None


def status(*, light: bool = False) -> dict[str, Any]:
    interval = _interval()
    local_running = bool(_thread and _thread.is_alive())
    cluster_running = local_running
    leader_id = None
    is_leader = False
    try:
        from grok2api.store.leader import is_leader as _is_leader, status as _leader_status
        is_leader = bool(_is_leader())
        ls = _leader_status()
        leader_id = ls.get("leader_id")
        if not local_running and is_enabled():
            try:
                from grok2api.store.redis_client import get_str, key, redis_enabled
                if redis_enabled():
                    lid = get_str(key("lock", "maintainer_leader"))
                    if lid:
                        leader_id = lid
                        cluster_running = True
            except Exception:
                pass
    except Exception:
        pass

    last = dict(_last_run) if _last_run else None
    if last is None:
        try:
            from grok2api.store.redis_client import get_str, key, redis_enabled
            import json as _json

            if redis_enabled():
                raw = get_str(key("model_health", "last_run"))
                if raw:
                    last = _json.loads(raw)
        except Exception:
            last = None
    if light and isinstance(last, dict):
        # Drop bulky samples from light payload.
        last = {
            k: v
            for k, v in last.items()
            if k not in ("results_sample", "results")
        }

    sweep = None
    try:
        gen, covered, started = _sweep_load()
        sweep = {
            "mode": "priority_sweep",
            "generation": gen or None,
            "covered": len(covered),
            "started_at": started or None,
        }
        # Enrich sweep with live totals from last cycle when present.
        if isinstance(last, dict):
            sw = last.get("sweep") if isinstance(last.get("sweep"), dict) else {}
            if sw:
                sweep["live"] = sw.get("sweep_live")
                sweep["remaining"] = sw.get("sweep_remaining")
                if sw.get("sweep_covered") is not None:
                    sweep["covered"] = sw.get("sweep_covered")
                if sw.get("mode"):
                    sweep["mode"] = sw.get("mode")
                if isinstance(sw.get("priority"), dict):
                    sweep["priority"] = sw.get("priority")
    except Exception:
        sweep = None
    # Rough ETA for full-pool coverage under current batch/interval caps.
    # Useful for large pools (thousands+) so operators can tune workers/batch.
    eta_sec = None
    try:
        live_n = None
        if isinstance(sweep, dict) and sweep.get("live") is not None:
            live_n = int(sweep.get("live") or 0)
        elif isinstance(last, dict):
            sw = last.get("sweep") if isinstance(last.get("sweep"), dict) else {}
            if sw.get("sweep_live") is not None:
                live_n = int(sw.get("sweep_live") or 0)
        batch_n = max(1, int(MODEL_PROBE_BATCH or 1))
        if live_n and live_n > 0 and interval > 0:
            cycles = (live_n + batch_n - 1) // batch_n
            eta_sec = float(cycles) * float(interval)
    except Exception:
        eta_sec = None
    return {
        "running": bool(cluster_running),
        "local_running": local_running,
        "cluster_running": bool(cluster_running),
        "leader_running": bool(cluster_running and is_enabled()),
        "is_leader": is_leader,
        "leader_id": leader_id,
        "enabled": is_enabled(),
        "interval_sec": interval,
        "last": last,
        "startup_delay_sec": _startup_delay(),
        "probe_workers": MODEL_PROBE_WORKERS,
        "probe_batch": MODEL_PROBE_BATCH,
        "probe_models": _live_probe_models(),
        # Background rotates 1 model/cycle; manual probes cap models/account.
        "probe_models_per_cycle": 1,
        "probe_max_models_per_account": int(MODEL_PROBE_MAX_MODELS_PER_ACCOUNT or 2),
        "auto_disable": _live_auto_disable(),
        "sweep": sweep,
        "full_pool_eta_sec": eta_sec,
        "selection": "priority_sweep",
    }
