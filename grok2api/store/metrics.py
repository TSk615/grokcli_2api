"""In-process counters for /metrics (Prometheus text format)."""

from __future__ import annotations

import threading
import time
from typing import Any

_lock = threading.Lock()
_started = time.time()
_counters: dict[str, float] = {
    "g2a_requests_total": 0,
    "g2a_auth_failures_total": 0,
    "g2a_upstream_failures_total": 0,
    "g2a_account_failovers_total": 0,
    "g2a_affinity_hits_total": 0,
    "g2a_affinity_misses_total": 0,
    "g2a_usage_requests_total": 0,
    "g2a_usage_success_total": 0,
    "g2a_usage_fail_total": 0,
    "g2a_prompt_tokens_total": 0,
    "g2a_completion_tokens_total": 0,
    "g2a_total_tokens_total": 0,
}


def inc(name: str, amount: float = 1.0) -> None:
    with _lock:
        _counters[name] = float(_counters.get(name, 0)) + amount


def set_gauge(name: str, value: float) -> None:
    with _lock:
        _counters[name] = float(value)


def snapshot() -> dict[str, float]:
    with _lock:
        out = dict(_counters)
    out["g2a_uptime_seconds"] = time.time() - _started
    return out


def prometheus_text() -> str:
    lines: list[str] = []
    data = snapshot()
    # Enrich with live store/leader gauges
    try:
        from grok2api.store import store_status

        st = store_status()
        data["g2a_workers"] = float(st.get("workers") or 1)
        data["g2a_redis_up"] = 1.0 if (st.get("redis") or {}).get("ok") else 0.0
        data["g2a_postgres_up"] = 1.0 if (st.get("postgres") or {}).get("ok") else 0.0
    except Exception:
        pass
    try:
        from grok2api.store.leader import status as leader_status

        ls = leader_status()
        data["g2a_is_leader"] = 1.0 if ls.get("is_leader") else 0.0
    except Exception:
        pass
    try:
        from grok2api.store.events_redis import writer_metrics

        data.update(writer_metrics())
    except Exception:
        pass
    for k, v in sorted(data.items()):
        lines.append(f"# TYPE {k} gauge")
        lines.append(f"{k} {v}")
    return "\n".join(lines) + "\n"


def status() -> dict[str, Any]:
    return snapshot()
