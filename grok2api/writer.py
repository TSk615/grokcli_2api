"""Durable Redis Streams writer for account stats, state and usage."""

from __future__ import annotations

import json
import signal
import socket
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from grok2api.config import (
    USAGE_EVENT_RETENTION_DAYS,
    WRITER_BATCH_SIZE,
    WRITER_FLUSH_SEC,
    WRITER_MAX_RETRIES,
)
from grok2api.store import events_redis
from grok2api.store.pg import connection, json_dump, pg_enabled
from grok2api.store.redis_client import get_client


def _utc_day(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).date().isoformat()
    except (TypeError, ValueError, OverflowError):
        return datetime.now(timezone.utc).date().isoformat()


def _event_ts(event: dict[str, Any]) -> float:
    try:
        return float(event.get("occurred_at") or time.time())
    except (TypeError, ValueError):
        return time.time()


class Writer:
    def __init__(self, *, consumer: str | None = None) -> None:
        self.consumer = consumer or f"writer-{socket.gethostname()}-{id(self):x}"
        self.running = True
        self._last_retention = 0.0

    def stop(self, *_args: Any) -> None:
        self.running = False

    def _read(
        self,
        *,
        count: int | None = None,
        block_ms: int | None = None,
    ) -> list[tuple[str, str, dict[str, Any]]]:
        c = get_client()
        if c is None:
            return []
        streams = {events_redis.stream_key(k): ">" for k in events_redis._STREAM_PARTS}
        try:
            kwargs: dict[str, Any] = {
                "count": max(1, min(WRITER_BATCH_SIZE, int(count or WRITER_BATCH_SIZE)))
            }
            if block_ms is not None:
                kwargs["block"] = max(1, int(block_ms))
            rows = c.xreadgroup(
                events_redis.CONSUMER_GROUP,
                self.consumer,
                streams,
                **kwargs,
            )
        except Exception:
            return []
        out: list[tuple[str, str, dict[str, Any]]] = []
        for stream, messages in rows or []:
            for message_id, fields in messages or []:
                event = events_redis.decode_message(fields)
                if event is None:
                    events_redis.move_to_dlq(str(stream), str(message_id), None, "invalid_event")
                    try:
                        c.xack(str(stream), events_redis.CONSUMER_GROUP, message_id)
                        c.xdel(str(stream), message_id)
                    except Exception:
                        pass
                    continue
                out.append((str(stream), str(message_id), event))
        return out

    def _reclaim(self) -> list[tuple[str, str, dict[str, Any]]]:
        c = get_client()
        if c is None:
            return []
        out: list[tuple[str, str, dict[str, Any]]] = []
        for kind in events_redis._STREAM_PARTS:
            if len(out) >= WRITER_BATCH_SIZE:
                break
            stream = events_redis.stream_key(kind)
            try:
                claimed = c.xautoclaim(
                    stream,
                    events_redis.CONSUMER_GROUP,
                    self.consumer,
                    min_idle_time=max(5000, int(WRITER_FLUSH_SEC * 5000)),
                    start_id="0-0",
                    count=min(100, WRITER_BATCH_SIZE - len(out)),
                )
                messages = claimed[1] if isinstance(claimed, (list, tuple)) and len(claimed) > 1 else []
                for message_id, fields in messages or []:
                    event = events_redis.decode_message(fields)
                    if event is not None:
                        out.append((stream, str(message_id), event))
                    else:
                        events_redis.move_to_dlq(
                            stream, str(message_id), None, "invalid_reclaimed_event"
                        )
                        c.xack(stream, events_redis.CONSUMER_GROUP, message_id)
                        c.xdel(stream, message_id)
            except Exception:
                continue
        return out

    def _collect_batch(self) -> list[tuple[str, str, dict[str, Any]]]:
        """Collect until the configured time window or size threshold."""
        batch = self._reclaim()
        deadline = time.monotonic() + max(0.001, float(WRITER_FLUSH_SEC))
        while self.running and len(batch) < WRITER_BATCH_SIZE:
            remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
            if remaining_ms <= 0:
                break
            incoming = self._read(
                count=WRITER_BATCH_SIZE - len(batch),
                block_ms=remaining_ms,
            )
            if incoming:
                batch.extend(incoming)
                continue
            break
        return batch

    @staticmethod
    def _pending_deliveries(stream: str, message_id: str) -> int:
        c = get_client()
        if c is None:
            return 0
        try:
            rows = c.xpending_range(
                stream,
                events_redis.CONSUMER_GROUP,
                min=message_id,
                max=message_id,
                count=1,
            )
            if not rows:
                return 0
            row = rows[0]
            if isinstance(row, dict):
                return int(row.get("times_delivered") or 0)
        except Exception:
            pass
        return 0

    @classmethod
    def _dead_letter_exhausted(
        cls,
        messages: list[tuple[str, str, dict[str, Any]]],
        error: Exception,
    ) -> None:
        c = get_client()
        if c is None:
            return
        for stream, message_id, event in messages:
            if cls._pending_deliveries(stream, message_id) < WRITER_MAX_RETRIES:
                continue
            try:
                events_redis.move_to_dlq(stream, message_id, event, str(error))
                c.xack(stream, events_redis.CONSUMER_GROUP, message_id)
                c.xdel(stream, message_id)
                try:
                    from grok2api.store.metrics import inc

                    inc("g2a_writer_events_dlq_total")
                except Exception:
                    pass
            except Exception:
                continue

    @staticmethod
    def _insert_inbox(cur: Any, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        accepted: list[dict[str, Any]] = []
        for event in events:
            event_id = str(event.get("event_id") or "")
            if not event_id:
                continue
            cur.execute(
                """
                INSERT INTO writer_inbox (event_id, kind)
                VALUES (%s, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """,
                (event_id, str(event.get("kind") or "unknown")),
            )
            if cur.fetchone():
                accepted.append(event)
        return accepted

    @staticmethod
    def _apply_account_stats(cur: Any, events: list[dict[str, Any]]) -> int:
        grouped: dict[str, dict[str, Any]] = {}
        for event in events:
            p = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            aid = str(p.get("account_id") or event.get("account_id") or "").strip()
            if not aid:
                continue
            row = grouped.setdefault(
                aid,
                {
                    "account_id": aid,
                    "requests": 0,
                    "success": 0,
                    "fail": 0,
                    "reset_consecutive_fails": False,
                    "state_version": 0,
                    "last_used_at": 0.0,
                },
            )
            row["requests"] += int(p.get("requests") or 0)
            row["success"] += int(p.get("success") or 0)
            row["fail"] += int(p.get("fail") or 0)
            try:
                success_seq = int(p.get("state_seq") or 0) if int(p.get("success") or 0) else 0
            except (TypeError, ValueError):
                success_seq = 0
            if success_seq > int(row["state_version"] or 0):
                row["state_version"] = success_seq
                row["reset_consecutive_fails"] = bool(
                    p.get("reset_consecutive_fails")
                )
            row["last_used_at"] = max(
                float(row["last_used_at"] or 0), float(p.get("last_used_at") or _event_ts(event))
            )
        if not grouped:
            return 0
        cur.execute(
            """
            WITH delta AS (
              SELECT account_id, requests::bigint, success::bigint, fail::bigint,
                     reset_consecutive_fails::boolean,
                     state_version::bigint,
                     last_used_at::double precision
              FROM jsonb_to_recordset(%s::jsonb) AS x(
                account_id text, requests bigint, success bigint, fail bigint,
                reset_consecutive_fails boolean,
                state_version bigint,
                last_used_at double precision
              )
            )
            INSERT INTO account_pool (
              account_id, request_count, success_count, fail_count, last_used_at,
              extra, updated_at, state_version
            )
            SELECT account_id, requests, success, fail,
                   to_timestamp(last_used_at),
                   CASE WHEN reset_consecutive_fails
                        THEN '{"consecutive_fails":0}'::jsonb ELSE '{}'::jsonb END,
                   now(), state_version
            FROM delta
            ON CONFLICT (account_id) DO UPDATE SET
              request_count = account_pool.request_count + EXCLUDED.request_count,
              success_count = account_pool.success_count + EXCLUDED.success_count,
              fail_count = account_pool.fail_count + EXCLUDED.fail_count,
              last_used_at = GREATEST(account_pool.last_used_at, EXCLUDED.last_used_at),
              extra = CASE
                WHEN EXCLUDED.state_version > account_pool.state_version
                     AND EXCLUDED.extra ? 'consecutive_fails' THEN
                  jsonb_set(COALESCE(account_pool.extra, '{}'::jsonb),
                            '{consecutive_fails}', '0'::jsonb, true)
                ELSE account_pool.extra
              END,
              state_version = GREATEST(account_pool.state_version, EXCLUDED.state_version),
              updated_at = now()
            """,
            (json_dump(list(grouped.values())),),
        )
        return len(grouped)

    @staticmethod
    def _apply_usage(cur: Any, events: list[dict[str, Any]]) -> tuple[int, int]:
        daily: dict[tuple[str, str, str], dict[str, int]] = {}
        detail_rows: list[dict[str, Any]] = []
        token_by_key: defaultdict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        token_by_account: defaultdict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for event in events:
            p = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            d = p.get("daily") if isinstance(p.get("daily"), dict) else p
            ok = bool(d.get("ok", p.get("ok", True)))
            pt = max(0, int(d.get("prompt_tokens") or 0)) if ok else 0
            ct = max(0, int(d.get("completion_tokens") or 0)) if ok else 0
            tt = max(0, int(d.get("total_tokens") or pt + ct)) if ok else 0
            cr = max(0, int(d.get("cache_read_tokens") or 0)) if ok else 0
            cc = max(0, int(d.get("cache_creation_tokens") or 0)) if ok else 0
            rt = max(0, int(d.get("reasoning_tokens") or 0)) if ok else 0
            req = {"requests": 1, "success": int(ok), "fail": int(not ok), "prompt_tokens": pt,
                   "completion_tokens": ct, "total_tokens": tt, "cache_read_tokens": cr,
                   "cache_creation_tokens": cc, "reasoning_tokens": rt,
                   "cache_hit_requests": int(bool(ok and cr))}
            day = _utc_day(event.get("occurred_at"))
            dims = [("global", "")]
            if p.get("api_key_id"):
                dims.append(("key", str(p["api_key_id"])[:256]))
            if p.get("account_id"):
                dims.append(("account", str(p["account_id"])[:256]))
            if p.get("model"):
                dims.append(("model", str(p["model"])[:120]))
            for dim, dim_id in dims:
                bucket = daily.setdefault((day, dim, dim_id), {k: 0 for k in req})
                for k, value in req.items():
                    bucket[k] += int(value)
            if p.get("api_key_id"):
                row = token_by_key[str(p["api_key_id"])]
                row["requests"] += 1
                row["last_used_at"] = max(
                    float(row["last_used_at"] or 0), _event_ts(event)
                )
                if ok:
                    row["prompt_tokens"] += pt
                    row["completion_tokens"] += ct
                    row["total_tokens"] += tt
            if ok and p.get("account_id"):
                row = token_by_account[str(p["account_id"])]
                row["prompt_tokens"] += pt
                row["completion_tokens"] += ct
                row["total_tokens"] += tt
            if bool(p.get("capture")):
                detail = p.get("detail") if isinstance(p.get("detail"), dict) else {}
                detail_rows.append(
                    {
                        "event_id": str(event.get("event_id")),
                        "created_at": _event_ts(event),
                        "api_key_id": p.get("api_key_id"),
                        "account_id": p.get("account_id"),
                        "model": p.get("model"),
                        "protocol": p.get("protocol"),
                        "path": p.get("path"),
                        "stream": p.get("stream"),
                        "ok": ok,
                        "prompt_tokens": pt,
                        "completion_tokens": ct,
                        "total_tokens": tt,
                        "cache_read_tokens": cr,
                        "cache_creation_tokens": cc,
                        "reasoning_tokens": rt,
                        "client_ip": p.get("client_ip"),
                        "user_agent": p.get("user_agent"),
                        "status_code": p.get("status_code"),
                        "latency_ms": p.get("latency_ms"),
                        "ttft_ms": p.get("ttft_ms"),
                        "error": p.get("error"),
                        "detail": detail,
                        "capture_reason": p.get("capture_reason") or "sample",
                    }
                )
        if daily:
            cur.execute(
                """
                WITH delta AS (
                  SELECT day::date, dim, dim_id, requests::bigint, success::bigint,
                         fail::bigint, prompt_tokens::bigint, completion_tokens::bigint,
                         total_tokens::bigint, cache_read_tokens::bigint,
                         cache_creation_tokens::bigint, reasoning_tokens::bigint,
                         cache_hit_requests::bigint
                  FROM jsonb_to_recordset(%s::jsonb) AS x(
                    day text, dim text, dim_id text, requests bigint, success bigint,
                    fail bigint, prompt_tokens bigint, completion_tokens bigint,
                    total_tokens bigint, cache_read_tokens bigint,
                    cache_creation_tokens bigint, reasoning_tokens bigint,
                    cache_hit_requests bigint
                  )
                )
                INSERT INTO usage_daily (
                  day, dim, dim_id, requests, success, fail, prompt_tokens,
                  completion_tokens, total_tokens, cache_read_tokens,
                  cache_creation_tokens, reasoning_tokens, cache_hit_requests, updated_at
                )
                SELECT day, dim, dim_id, requests, success, fail, prompt_tokens,
                       completion_tokens, total_tokens, cache_read_tokens,
                       cache_creation_tokens, reasoning_tokens, cache_hit_requests, now()
                FROM delta
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
                (json_dump([{"day": day, "dim": dim, "dim_id": dim_id, **vals} for (day, dim, dim_id), vals in daily.items()]),),
            )
        for aid, vals in token_by_account.items():
            cur.execute(
                """
                UPDATE account_pool
                SET prompt_tokens_total = COALESCE(prompt_tokens_total, 0) + %s,
                    completion_tokens_total = COALESCE(completion_tokens_total, 0) + %s,
                    total_tokens_total = COALESCE(total_tokens_total, 0) + %s,
                    updated_at = now()
                WHERE account_id = %s
                """,
                (vals["prompt_tokens"], vals["completion_tokens"], vals["total_tokens"], aid),
            )
        for kid, vals in token_by_key.items():
            cur.execute(
                """
                UPDATE api_keys
                SET request_count = COALESCE(request_count, 0) + %s,
                    last_used_at = GREATEST(last_used_at, to_timestamp(%s)),
                    prompt_tokens_total = COALESCE(prompt_tokens_total, 0) + %s,
                    completion_tokens_total = COALESCE(completion_tokens_total, 0) + %s,
                    total_tokens_total = COALESCE(total_tokens_total, 0) + %s
                WHERE id = %s
                """,
                (
                    vals["requests"],
                    vals["last_used_at"],
                    vals["prompt_tokens"],
                    vals["completion_tokens"],
                    vals["total_tokens"],
                    kid,
                ),
            )
        from grok2api.store.usage_pg import ensure_event_partition

        ensured_partitions: set[str] = set()
        for row in detail_rows:
            partition_month = datetime.fromtimestamp(
                float(row["created_at"]), tz=timezone.utc
            ).strftime("%Y%m")
            if partition_month not in ensured_partitions:
                ensure_event_partition(cur, row["created_at"])
                ensured_partitions.add(partition_month)
            cur.execute(
                """
                INSERT INTO usage_events_partitioned (
                  event_id, created_at, api_key_id, account_id, model, protocol, path,
                  stream, ok, prompt_tokens, completion_tokens, total_tokens,
                  cache_read_tokens, cache_creation_tokens, reasoning_tokens,
                  client_ip, user_agent, status_code, latency_ms, ttft_ms, error,
                  capture_reason, detail
                ) VALUES (
                  %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                ) ON CONFLICT DO NOTHING
                """,
                (
                    row["event_id"], row["created_at"], row["api_key_id"], row["account_id"],
                    row["model"], row["protocol"], row["path"], row["stream"], row["ok"],
                    row["prompt_tokens"], row["completion_tokens"], row["total_tokens"],
                    row["cache_read_tokens"], row["cache_creation_tokens"], row["reasoning_tokens"],
                    row["client_ip"], row["user_agent"], row["status_code"], row["latency_ms"],
                    row["ttft_ms"], row["error"], row["capture_reason"], json_dump(row["detail"]),
                ),
            )
        return len(daily), len(detail_rows)

    def _process(self, messages: list[tuple[str, str, dict[str, Any]]]) -> None:
        if not messages or not pg_enabled():
            return
        started = time.perf_counter()
        try:
            with connection() as conn:
                with conn.cursor() as cur:
                    accepted = self._insert_inbox(cur, [m[2] for m in messages])
                    stats = [e for e in accepted if e.get("kind") == "account_stats"]
                    states = [e for e in accepted if e.get("kind") == "account_state"]
                    usage = [e for e in accepted if e.get("kind") == "usage"]
                    self._apply_account_stats(cur, stats)
                    for event in states:
                        p = event.get("payload") if isinstance(event.get("payload"), dict) else {}
                        from grok2api.store.settings_pg import apply_pool_state_event

                        apply_pool_state_event(
                            cur,
                            str(p.get("account_id") or event.get("account_id") or ""),
                            p.get("patch") if isinstance(p.get("patch"), dict) else {},
                            int(p.get("state_seq") or 0),
                        )
                    self._apply_usage(cur, usage)
                    if time.time() - self._last_retention > 3600:
                        cur.execute(
                            "DELETE FROM usage_events WHERE created_at < now() - (%s::int * INTERVAL '1 day')",
                            (USAGE_EVENT_RETENTION_DAYS,),
                        )
                        cur.execute(
                            "DELETE FROM usage_events_partitioned WHERE created_at < now() - (%s::int * INTERVAL '1 day')",
                            (USAGE_EVENT_RETENTION_DAYS,),
                        )
                        cur.execute(
                            "DELETE FROM writer_inbox WHERE processed_at < now() - (%s::int * INTERVAL '2 days')",
                            (USAGE_EVENT_RETENTION_DAYS,),
                        )
                        self._last_retention = time.time()
                conn.commit()
            c = get_client()
            if c is not None:
                by_stream: dict[str, list[str]] = defaultdict(list)
                for stream, message_id, _event in messages:
                    by_stream[stream].append(message_id)
                for stream, ids in by_stream.items():
                    c.xack(stream, events_redis.CONSUMER_GROUP, *ids)
                    c.xdel(stream, *ids)
            try:
                from grok2api.store.metrics import inc, set_gauge

                inc("g2a_writer_batches_total")
                inc("g2a_writer_events_processed_total", len(messages))
                inc("g2a_writer_commit_seconds_total", time.perf_counter() - started)
                for name, value in events_redis.queue_metrics().items():
                    set_gauge(name, value)
            except Exception:
                pass
            try:
                events_redis.record_writer_batch(
                    events=len(messages), seconds=time.perf_counter() - started
                )
            except Exception:
                pass
        except Exception as exc:
            try:
                from grok2api.store.metrics import inc

                inc("g2a_writer_batches_failed_total")
            except Exception:
                pass
            try:
                events_redis.record_writer_batch(
                    events=len(messages),
                    seconds=time.perf_counter() - started,
                    failed=True,
                )
            except Exception:
                pass
            self._dead_letter_exhausted(messages, exc)
            # Messages below the threshold stay pending for reclaim/retry.

    def run(self) -> None:
        events_redis.ensure_consumer_groups()
        while self.running:
            batch = self._collect_batch()
            if batch:
                self._process(batch)
        # One final non-blocking drain on graceful shutdown.
        batch = self._read(block_ms=1)
        if batch:
            self._process(batch)


def main() -> None:
    writer = Writer()
    signal.signal(signal.SIGTERM, writer.stop)
    signal.signal(signal.SIGINT, writer.stop)
    writer.run()


if __name__ == "__main__":
    main()
