"""Redis Streams event transport for gateway-to-writer persistence."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from typing import Any

from grok2api.store.redis_client import get_client, key, redis_enabled

EVENT_VERSION = 1
CONSUMER_GROUP = "g2a-writers"
_STREAM_PARTS = {
    "account_stats": ("stream", "account_stats"),
    "account_state": ("stream", "account_state"),
    "usage": ("stream", "usage"),
}


def stream_key(kind: str) -> str:
    parts = _STREAM_PARTS.get(str(kind))
    if parts is None:
        raise ValueError(f"unsupported event kind: {kind}")
    return key(*parts)


def dlq_key() -> str:
    return key("stream", "dlq")


def writer_metrics_key() -> str:
    return key("writer", "metrics")


def build_event(
    kind: str,
    payload: dict[str, Any],
    *,
    event_id: str | None = None,
    request_id: str | None = None,
    account_id: str | None = None,
    attempt: int | None = None,
    occurred_at: float | None = None,
) -> dict[str, Any]:
    if kind not in _STREAM_PARTS:
        raise ValueError(f"unsupported event kind: {kind}")
    return {
        "v": EVENT_VERSION,
        "event_id": event_id or uuid.uuid4().hex,
        "request_id": request_id or uuid.uuid4().hex,
        "occurred_at": float(occurred_at or time.time()),
        "kind": kind,
        "account_id": account_id or payload.get("account_id"),
        "attempt": 0 if attempt is None else int(attempt),
        "payload": dict(payload or {}),
    }


def publish_event(event: dict[str, Any]) -> str | None:
    """Append one event. Returns its event_id when Redis accepted it."""
    if not redis_enabled():
        return None
    kind = str(event.get("kind") or "")
    try:
        c = get_client()
        if c is None:
            return None
        c.xadd(
            stream_key(kind),
            {"event": json.dumps(event, ensure_ascii=False, separators=(",", ":"))},
        )
        try:
            from grok2api.store.metrics import inc

            inc("g2a_writer_events_enqueued_total")
        except Exception:
            pass
        return str(event.get("event_id") or "") or None
    except Exception:
        try:
            from grok2api.store.metrics import inc

            inc("g2a_writer_events_enqueue_fail_total")
        except Exception:
            pass
        return None


def publish(
    kind: str,
    payload: dict[str, Any],
    **meta: Any,
) -> str | None:
    return publish_event(build_event(kind, payload, **meta))


def publish_account_stats(
    account_id: str,
    *,
    success: bool,
    request_id: str | None = None,
    occurred_at: float | None = None,
) -> str | None:
    if not account_id:
        return None
    state_seq = int(next_state_sequence(account_id) or 0) if success else 0
    if state_seq:
        try:
            from grok2api.store.ready_redis import apply_success_sequence

            apply_success_sequence(account_id, state_seq)
        except Exception:
            pass
    payload = {
        "account_id": account_id,
        "requests": 1,
        "success": 1 if success else 0,
        "fail": 0 if success else 1,
        "reset_consecutive_fails": bool(success),
        "state_seq": state_seq,
        "last_used_at": float(occurred_at or time.time()),
    }
    return publish(
        "account_stats",
        payload,
        request_id=request_id,
        account_id=account_id,
        occurred_at=occurred_at,
    )


def next_state_sequence(account_id: str) -> int | None:
    if not redis_enabled() or not account_id:
        return None
    c = get_client()
    if c is None:
        return None
    return int(c.incr(key("pool", "state_seq", account_id)))


def publish_account_state(
    account_id: str,
    patch: dict[str, Any],
    *,
    request_id: str | None = None,
    state_seq: int | None = None,
) -> tuple[str | None, int]:
    if not account_id:
        return None, 0
    seq = int(state_seq or next_state_sequence(account_id) or 0)
    payload = {
        "account_id": account_id,
        "state_seq": seq,
        "patch": dict(patch or {}),
    }
    try:
        from grok2api.store.ready_redis import apply_status_patch

        apply_status_patch(account_id, patch, state_seq=seq)
    except Exception:
        pass
    event_id = publish(
        "account_state",
        payload,
        request_id=request_id,
        account_id=account_id,
    )
    return event_id, seq


def deterministic_sample(request_id: str, rate: float) -> bool:
    value = max(0.0, min(1.0, float(rate or 0.0)))
    if value <= 0:
        return False
    if value >= 1:
        return True
    digest = hashlib.sha256(str(request_id).encode("utf-8", errors="ignore")).digest()
    bucket = int.from_bytes(digest[:8], "big") / float(2**64)
    return bucket < value


def decode_message(fields: dict[str, Any]) -> dict[str, Any] | None:
    raw = (fields or {}).get("event")
    if raw is None:
        return None
    try:
        event = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(event, dict) or not event.get("event_id"):
        return None
    return event


def ensure_consumer_groups() -> None:
    c = get_client()
    if c is None:
        raise RuntimeError("Redis is not configured")
    for kind in _STREAM_PARTS:
        try:
            c.xgroup_create(stream_key(kind), CONSUMER_GROUP, id="0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise


def move_to_dlq(
    stream: str,
    message_id: str,
    event: dict[str, Any] | None,
    error: str,
) -> None:
    c = get_client()
    if c is None:
        return
    c.xadd(
        dlq_key(),
        {
            "source_stream": stream,
            "source_id": message_id,
            "error": str(error)[:1000],
            "event": json.dumps(event or {}, ensure_ascii=False, separators=(",", ":")),
            "failed_at": str(time.time()),
        },
        maxlen=100_000,
        approximate=True,
    )


def queue_metrics() -> dict[str, float]:
    c = get_client()
    if c is None:
        return {}
    out: dict[str, float] = {}
    for kind in _STREAM_PARTS:
        stream = stream_key(kind)
        try:
            length = float(c.xlen(stream) or 0)
            out[f"g2a_writer_stream_{kind}_length"] = length
            pending = c.xpending(stream, CONSUMER_GROUP)
            if isinstance(pending, dict):
                out[f"g2a_writer_stream_{kind}_pending"] = float(
                    pending.get("pending") or 0
                )
            first = c.xrange(stream, min="-", max="+", count=1) if length else []
            if first:
                message_id = str(first[0][0])
                created_ms = int(message_id.split("-", 1)[0])
                out[f"g2a_writer_stream_{kind}_lag_seconds"] = max(
                    0.0, time.time() - created_ms / 1000.0
                )
            else:
                out[f"g2a_writer_stream_{kind}_lag_seconds"] = 0.0
        except Exception:
            continue
    return out


def record_writer_batch(*, events: int, seconds: float, failed: bool = False) -> None:
    c = get_client()
    if c is None:
        return
    k = writer_metrics_key()
    pipe = c.pipeline(transaction=False)
    pipe.hincrby(k, "batches_failed_total" if failed else "batches_total", 1)
    if not failed:
        pipe.hincrby(k, "events_processed_total", max(0, int(events)))
        pipe.hincrbyfloat(k, "commit_seconds_total", max(0.0, float(seconds)))
        pipe.hset(k, mapping={"last_commit_at": time.time(), "last_batch_size": int(events)})
    pipe.execute()


def writer_metrics() -> dict[str, float]:
    c = get_client()
    if c is None:
        return {}
    out = queue_metrics()
    try:
        raw = c.hgetall(writer_metrics_key()) or {}
        for name, value in raw.items():
            out[f"g2a_writer_{name}"] = float(value or 0)
        last = float(raw.get("last_commit_at") or 0)
        if last > 0:
            out["g2a_writer_last_commit_age_seconds"] = max(0.0, time.time() - last)
    except Exception:
        pass
    out["g2a_writer_lag_seconds"] = max(
        (
            value
            for name, value in out.items()
            if name.startswith("g2a_writer_stream_") and name.endswith("_lag_seconds")
        ),
        default=0.0,
    )
    try:
        out["g2a_writer_dlq_length"] = float(c.xlen(dlq_key()) or 0)
    except Exception:
        pass
    return out
