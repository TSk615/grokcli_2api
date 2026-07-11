"""Background maintenance for multi-account auth on long-running servers.

- Normalize auth.json keys (CLI client_id → per-user multi-account)
- Proactively refresh access tokens via refresh_token before expiry
- Adaptive interval: refresh sooner when any token is near expiry
- Batched / concurrency-capped cycles so large pools (700+) don't freeze WSL
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any

from maintenance_gate import maintenance_slot

_stop = threading.Event()
_thread: threading.Thread | None = None
_last_run: dict[str, Any] = {}
_wakeup = threading.Event()  # force an early cycle from admin UI
_force_next = False
_force_lock = threading.Lock()
_min_remaining_cache: dict[str, Any] = {"at": 0.0, "value": None}
_MIN_REMAINING_CACHE_TTL = 15.0


def _interval() -> float:
    try:
        return max(60.0, float(os.getenv("GROK2API_TOKEN_MAINTAIN_INTERVAL", "180")))
    except ValueError:
        return 180.0


def _skew() -> float:
    try:
        return float(os.getenv("GROK2API_TOKEN_REFRESH_SKEW", "120"))
    except ValueError:
        return 120.0


def _startup_delay() -> float:
    try:
        from config import TOKEN_MAINTAIN_STARTUP_DELAY

        return max(5.0, float(TOKEN_MAINTAIN_STARTUP_DELAY))
    except Exception:
        return 45.0


def _min_remaining_seconds(*, force: bool = False) -> float | None:
    """Smallest access-token remaining lifetime across live accounts."""
    now = time.time()
    if (
        not force
        and _min_remaining_cache.get("at")
        and now - float(_min_remaining_cache["at"]) < _MIN_REMAINING_CACHE_TTL
    ):
        return _min_remaining_cache.get("value")  # type: ignore[return-value]
    try:
        from auth import list_live_credentials

        remains: list[float] = []
        for c in list_live_credentials(include_expired=True, auto_refresh=False):
            if c.expires_at is None:
                continue
            remains.append(float(c.expires_at) - now)
        value = min(remains) if remains else None
    except Exception:
        value = None
    _min_remaining_cache["at"] = now
    _min_remaining_cache["value"] = value
    return value


def _next_wait_seconds() -> float:
    """
    Adaptive sleep: if any token expires soon, poll more frequently so
    expires_at gets refreshed automatically without manual clicks.
    """
    base = _interval()
    rem = _min_remaining_seconds()
    if rem is None:
        return base
    # Within 15 minutes of expiry → check every 60s
    if rem <= 15 * 60:
        return min(base, 60.0)
    # Within 1 hour → check every 2 minutes
    if rem <= 3600:
        return min(base, 120.0)
    return base


def run_once(*, force: bool = False) -> dict[str, Any]:
    """
    Normalize keys + refresh tokens.
    force=True refreshes every account that has refresh_token (updates expires_at),
    still batch-capped so a single cycle never fans out to all 700 accounts.
    """
    result: dict[str, Any] = {
        "ok": True,
        "normalized": None,
        "refresh": None,
        "force": force,
        "accounts": [],
        "deferred_busy": False,
    }
    # Prefer waiting for model probes to finish (tokens are more important),
    # but never hang forever if a probe cycle is stuck on network.
    with maintenance_slot("token_maintainer", blocking=True, timeout=180.0) as got:
        if not got:
            result["ok"] = True
            result["deferred_busy"] = True
            result["error"] = "maintenance slot busy — deferred"
            _last_run.clear()
            _last_run.update(result)
            _last_run["at"] = time.time()
            print("  [token-maintainer] deferred: maintenance slot busy")
            return result
        try:
            from accounts import list_accounts
            from oidc_auth import normalize_auth_file_keys, refresh_all_accounts

            result["normalized"] = normalize_auth_file_keys()
            # force: still only-near-expiry=False, but max_accounts batch applies
            skew = max(300.0, _skew() * 2)
            # force: refresh even far-from-expiry, but still batch-capped so one
            # admin click never rewrites 700 accounts at once on WSL.
            try:
                from config import TOKEN_REFRESH_BATCH
            except Exception:
                TOKEN_REFRESH_BATCH = 20
            force_batch = min(TOKEN_REFRESH_BATCH * 2, 40) if force else TOKEN_REFRESH_BATCH
            refresh = refresh_all_accounts(
                only_near_expiry=not force,
                skew_seconds=skew if not force else 365 * 86400.0,
                max_accounts=force_batch,
            )
            # Keep full result for the direct admin/API caller, but never retain
            # hundreds of per-account rows in the background status cache —
            # that alone made /health ~100KB on a 400+ pool.
            rows = refresh.get("results") if isinstance(refresh, dict) else None
            slim_refresh = {
                k: v
                for k, v in (refresh or {}).items()
                if k != "results"
            }
            if isinstance(rows, list):
                failed = [r for r in rows if not r.get("ok")]
                slim_refresh["failed_sample"] = failed[:5]
                slim_refresh["failed"] = len(failed)
                slim_refresh["skipped"] = sum(1 for r in rows if r.get("skipped"))
            result["refresh"] = slim_refresh
            accounts = list_accounts()
            result["accounts"] = []  # never embed full account list in status cache
            result["accounts_total"] = len(accounts)
            result["min_remaining_sec"] = _min_remaining_seconds(force=True)
            # Attach full refresh only on the returned object for admin force-run.
            result_full = dict(result)
            result_full["refresh"] = refresh
            result = result_full
        except Exception as e:  # noqa: BLE001
            result["ok"] = False
            result["error"] = str(e)[:400]
    # Persist a slim snapshot for status()/health, not the full per-account dump.
    slim_last = {
        k: v
        for k, v in result.items()
        if k not in ("accounts",)
    }
    if isinstance(slim_last.get("refresh"), dict) and "results" in slim_last["refresh"]:
        rows = slim_last["refresh"].get("results") or []
        slim_last["refresh"] = {
            k: v for k, v in slim_last["refresh"].items() if k != "results"
        }
        slim_last["refresh"]["failed"] = sum(1 for r in rows if not r.get("ok"))
        slim_last["refresh"]["skipped"] = sum(1 for r in rows if r.get("skipped"))
        slim_last["refresh"]["failed_sample"] = [r for r in rows if not r.get("ok")][:5]
    _last_run.clear()
    _last_run.update(slim_last)
    _last_run["at"] = time.time()
    return result


def request_run_soon(*, force: bool = True) -> None:
    """Wake the background worker for an early cycle."""
    global _force_next
    with _force_lock:
        _force_next = bool(force)
    _wakeup.set()


def _worker() -> None:
    # Stagger startup so normalize + first HTTP requests aren't simultaneous
    # with model-health probe fan-out (large pools freeze WSL otherwise).
    if _stop.wait(_startup_delay()):
        return
    while not _stop.is_set():
        run_once(force=False)
        wait = _next_wait_seconds()
        # Wait either for interval or an admin-triggered wakeup
        _wakeup.clear()
        triggered = _wakeup.wait(timeout=wait)
        if _stop.is_set():
            break
        if triggered:
            with _force_lock:
                global _force_next
                do_force = _force_next
                _force_next = False
            # admin asked for refresh — do a force pass (still batch-capped)
            run_once(force=do_force)


def start_background() -> None:
    global _thread
    if os.getenv("GROK2API_TOKEN_MAINTAIN", "1").lower() in ("0", "false", "no"):
        return
    if _thread and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(target=_worker, name="g2a-token-maintainer", daemon=True)
    _thread.start()


def stop_background() -> None:
    _stop.set()
    _wakeup.set()


def status(*, light: bool = False) -> dict[str, Any]:
    rem = None if light else _min_remaining_seconds()
    try:
        from config import TOKEN_REFRESH_BATCH, TOKEN_REFRESH_WORKERS
    except Exception:
        TOKEN_REFRESH_BATCH = 20
        TOKEN_REFRESH_WORKERS = 2
    out = {
        "running": bool(_thread and _thread.is_alive()),
        "enabled": os.getenv("GROK2API_TOKEN_MAINTAIN", "1").lower()
        not in ("0", "false", "no"),
        "interval_sec": _interval(),
        "refresh_skew_sec": _skew(),
        "startup_delay_sec": _startup_delay(),
        "refresh_workers": TOKEN_REFRESH_WORKERS,
        "refresh_batch": TOKEN_REFRESH_BATCH,
    }
    if light:
        # Keep /health tiny: only last outcome summary, no per-account rows.
        if _last_run:
            out["last"] = {
                "ok": _last_run.get("ok"),
                "at": _last_run.get("at"),
                "force": _last_run.get("force"),
                "deferred_busy": _last_run.get("deferred_busy"),
                "accounts_total": _last_run.get("accounts_total"),
                "min_remaining_sec": _last_run.get("min_remaining_sec"),
                "refresh": {
                    k: v
                    for k, v in ((_last_run.get("refresh") or {}).items())
                    if k
                    in (
                        "ok",
                        "refreshed",
                        "deferred",
                        "attempted",
                        "workers",
                        "failed",
                        "skipped",
                    )
                }
                if isinstance(_last_run.get("refresh"), dict)
                else _last_run.get("refresh"),
            }
        else:
            out["last"] = None
    else:
        out["next_wait_sec"] = _next_wait_seconds()
        out["min_remaining_sec"] = rem
        out["last"] = dict(_last_run) if _last_run else None
    return out
