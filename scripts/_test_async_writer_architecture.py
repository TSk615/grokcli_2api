"""Focused regression tests for the Redis-stream writer architecture."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from unittest import mock

os.environ.setdefault("GROK2API_STORE_BACKEND", "file")
os.environ.setdefault("GROK2API_REDIS_URL", "")
os.environ.setdefault("REDIS_URL", "")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from grok2api.admin import settings_store, usage_stats  # noqa: E402
from grok2api.pool import account_pool as ap  # noqa: E402
from grok2api.pool.auth import GrokCredentials  # noqa: E402
from grok2api.store import events_redis, ready_redis  # noqa: E402
from grok2api.store import settings_pg  # noqa: E402
from grok2api.writer import Writer  # noqa: E402


def ok(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  ok: {message}")


def test_event_schema_and_sampling() -> None:
    print("[event schema and deterministic sampling]")
    event = events_redis.build_event(
        "usage",
        {"total_tokens": 3},
        request_id="req-1",
        account_id="acc-1",
        attempt=2,
    )
    ok(event["v"] == 1, "schema version is present")
    ok(bool(event["event_id"]), "event id is present")
    ok(event["kind"] == "usage" and event["attempt"] == 2, "routing metadata is present")
    first = events_redis.deterministic_sample("stable-request", 0.05)
    second = events_redis.deterministic_sample("stable-request", 0.05)
    ok(first == second, "sampling is stable for one request id")
    ok(not events_redis.deterministic_sample("x", 0.0), "zero sample rate rejects")
    ok(events_redis.deterministic_sample("x", 1.0), "full sample rate accepts")


def test_capture_policy() -> None:
    print("[usage capture policy]")
    base = {
        "sample_rate": 0.0,
        "all_failures": True,
        "slow_ms": 5000,
        "audit_until": 0.0,
        "audit_key_ids": [],
    }
    with mock.patch.object(usage_stats, "_capture_policy", return_value=base):
        ok(
            usage_stats._capture_decision(
                request_id="failure", ok=False, status_code=502, latency_ms=10, api_key_id=None
            )
            == (True, "failure"),
            "failures are always captured",
        )
        ok(
            usage_stats._capture_decision(
                request_id="slow", ok=True, status_code=200, latency_ms=5000, api_key_id=None
            )
            == (True, "slow"),
            "slow requests are always captured",
        )
        ok(
            usage_stats._capture_decision(
                request_id="normal", ok=True, status_code=200, latency_ms=10, api_key_id=None
            )
            == (False, "unsampled"),
            "ordinary requests honor sampling",
        )
    audit = {**base, "audit_until": time.time() + 60, "audit_key_ids": ["key-1"]}
    with mock.patch.object(usage_stats, "_capture_policy", return_value=audit):
        ok(
            usage_stats._capture_decision(
                request_id="audit", ok=True, status_code=200, latency_ms=10, api_key_id="key-1"
            )
            == (True, "audit"),
            "audit key window forces capture",
        )


class _InboxCursor:
    def __init__(self) -> None:
        self.seen: set[str] = set()
        self.inserted = False

    def execute(self, _sql: str, params: tuple[str, str]) -> None:
        event_id = params[0]
        self.inserted = event_id not in self.seen
        self.seen.add(event_id)

    def fetchone(self):
        return ("inserted",) if self.inserted else None


class _CaptureCursor:
    def __init__(self) -> None:
        self.params = None
        self.sql = ""

    def execute(self, sql: str, params) -> None:
        self.sql = sql
        self.params = params


def test_writer_idempotency_and_aggregation() -> None:
    print("[writer inbox idempotency and aggregation]")
    cur = _InboxCursor()
    events = [
        {"event_id": "evt-1", "kind": "usage"},
        {"event_id": "evt-1", "kind": "usage"},
    ]
    first = Writer._insert_inbox(cur, events)
    second = Writer._insert_inbox(cur, events)
    ok(len(first) == 1 and not second, "duplicate event ids are accepted once")

    stats_cur = _CaptureCursor()
    count = Writer._apply_account_stats(
        stats_cur,
        [
            {
                "account_id": "acc-1",
                "occurred_at": 10,
                "payload": {
                    "requests": 1,
                    "success": 1,
                    "fail": 0,
                    "state_seq": 7,
                    "reset_consecutive_fails": True,
                },
            },
            {
                "account_id": "acc-1",
                "occurred_at": 20,
                "payload": {"requests": 1, "success": 0, "fail": 1},
            },
        ],
    )
    rows = json.loads(stats_cur.params[0])
    ok(count == 1 and len(rows) == 1, "account deltas are grouped per account")
    ok(rows[0]["requests"] == 2 and rows[0]["success"] == 1 and rows[0]["fail"] == 1,
       "grouped counters are additive")
    ok(rows[0]["last_used_at"] == 20, "latest usage timestamp wins")
    ok(rows[0]["state_version"] == 7, "success carries a monotonic state version")
    ok(
        "EXCLUDED.state_version > account_pool.state_version" in stats_cur.sql,
        "older success cannot clear a newer failure state",
    )


def test_sql_placeholder_contracts() -> None:
    print("[batched SQL placeholder contracts]")

    class ContractCursor:
        def __init__(self) -> None:
            self.calls = 0

        def execute(self, sql: str, params=()) -> None:
            expected = sql.count("%s")
            actual = len(params or ())
            if expected != actual:
                raise AssertionError(f"placeholder mismatch: expected={expected} actual={actual}")
            self.calls += 1

    usage_cur = ContractCursor()
    daily_count, detail_count = Writer._apply_usage(
        usage_cur,
        [
            {
                "event_id": "evt-usage-1",
                "occurred_at": time.time(),
                "payload": {
                    "ok": True,
                    "prompt_tokens": 3,
                    "completion_tokens": 4,
                    "total_tokens": 7,
                    "cache_read_tokens": 2,
                    "cache_creation_tokens": 1,
                    "reasoning_tokens": 1,
                    "api_key_id": "key-1",
                    "account_id": "acc-1",
                    "model": "grok-4",
                    "capture": True,
                    "capture_reason": "sample",
                    "detail": {"request": "test"},
                },
            }
        ],
    )
    ok(daily_count == 4 and detail_count == 1, "usage batch covers global/key/account/model")
    ok(usage_cur.calls >= 4, "usage batch SQL contracts are valid")

    pool_cur = ContractCursor()
    settings_pg._upsert_pool(
        pool_cur,
        "acc-1",
        {
            "enabled": True,
            "weight": 1,
            "blocked_models": {},
            "pool_status": "normal",
            "state_version": 7,
        },
        only_if_newer=True,
    )
    ok(pool_cur.calls == 1, "versioned account state upsert contract is valid")


def test_writer_dlq_threshold() -> None:
    print("[writer DLQ threshold]")

    class FakeRedis:
        def __init__(self) -> None:
            self.acked: list[tuple[str, str]] = []

        def xpending_range(self, *_args, **_kwargs):
            return [{"times_delivered": 999}]

        def xack(self, stream, _group, message_id):
            self.acked.append((stream, message_id))

        def xdel(self, _stream, _message_id):
            return 1

    fake = FakeRedis()
    moved: list[tuple[str, str]] = []
    with mock.patch("grok2api.writer.get_client", return_value=fake), mock.patch.object(
        events_redis,
        "move_to_dlq",
        side_effect=lambda stream, message_id, _event, _error: moved.append((stream, message_id)),
    ):
        Writer._dead_letter_exhausted(
            [("stream-a", "1-0", {"event_id": "evt-1"})], RuntimeError("bad batch")
        )
    ok(moved == [("stream-a", "1-0")], "exhausted event is copied to DLQ")
    ok(fake.acked == [("stream-a", "1-0")], "DLQ event is acknowledged from source stream")


def test_writer_collects_to_batch_threshold() -> None:
    print("[writer time-window batching]")
    writer = Writer(consumer="test-writer")
    reclaimed = [("stats", "1-0", {"event_id": "evt-1"})]
    incoming = [
        ("usage", "2-0", {"event_id": "evt-2"}),
        ("state", "3-0", {"event_id": "evt-3"}),
    ]
    with mock.patch("grok2api.writer.WRITER_BATCH_SIZE", 3), mock.patch.object(
        writer, "_reclaim", return_value=reclaimed
    ), mock.patch.object(writer, "_read", return_value=incoming) as read:
        batch = writer._collect_batch()
    ok(len(batch) == 3, "reclaimed and new events share one batch")
    read.assert_called_once()
    ok(read.call_args.kwargs["count"] == 2, "reader requests only remaining capacity")


def test_state_version_guard() -> None:
    print("[ready state version guard]")

    class FakeRedis:
        def __init__(self) -> None:
            self.writes = 0

        def set(self, *_args, **_kwargs):
            self.writes += 1

        def zrem(self, *_args, **_kwargs):
            self.writes += 1

        def zadd(self, *_args, **_kwargs):
            self.writes += 1

    fake = FakeRedis()
    with mock.patch.object(ready_redis, "redis_enabled", return_value=True), mock.patch.object(
        ready_redis, "get_client", return_value=fake
    ), mock.patch.object(
        ready_redis, "get_json", return_value={"enabled": True, "state_version": 10}
    ), mock.patch.object(ready_redis, "_raise_state_sequence_floor") as floor:
        ready_redis.apply_status_patch("acc-1", {"enabled": False}, state_seq=9)
    floor.assert_called_once_with("acc-1", 10)
    ok(fake.writes == 0, "older state event cannot overwrite the ready index")


def test_indexed_selection_is_payload_lazy() -> None:
    print("[indexed selection payload laziness]")
    creds = {
        aid: GrokCredentials(token=f"token-{aid}", auth_key=aid, user_id=aid)
        for aid in ("acc-a", "acc-b", "acc-c")
    }
    lease_results = iter((False, True, True))
    with mock.patch.object(ready_redis, "list_candidates", return_value=list(creds)), mock.patch.object(
        ready_redis, "get_status", return_value={"enabled": True, "pool_status": "normal"}
    ), mock.patch.object(ready_redis, "is_ready", return_value=True), mock.patch.object(
        ap, "peek_credentials_by_id", side_effect=lambda aid: creds[aid]
    ) as lazy_load, mock.patch.object(
        ap, "note_account_pick", side_effect=lambda _aid: next(lease_results)
    ), mock.patch.object(ap, "get_account_mode", return_value="round_robin"), mock.patch.object(
        ap, "is_model_blocked", return_value=False
    ), mock.patch.object(ap, "list_live_credentials") as full_scan:
        chain = ap._try_acquire_indexed_sequence(
            2, model="grok-4", prefer_account_id=None
        )
        ok(chain[0].auth_key == "acc-b", "selector advances to the first leased account")
        ok(lazy_load.call_count == 1, "only the leased first payload is loaded eagerly")
        iterator = iter(chain)
        ok(next(iterator).auth_key == "acc-b", "iteration reuses the leased first account")
        ok(lazy_load.call_count == 1, "first attempt does not prefetch a backup payload")
        ok(next(iterator).auth_key == "acc-c", "next failover is leased on demand")
        ok(lazy_load.call_count == 2, "backup payload loads only when failover advances")
    full_scan.assert_not_called()
    ok(True, "full credential map is not materialized")


def test_indexed_failure_meta_avoids_pg() -> None:
    print("[indexed failure metadata is Redis-only]")
    hot = {
        "enabled": True,
        "pool_status": "normal",
        "consecutive_fails": 3,
        "status_stack": [{"kind": "request_fail"}],
    }
    with mock.patch("grok2api.config.READY_INDEX_MODE", "on"), mock.patch(
        "grok2api.store.redis_client.redis_enabled", return_value=True
    ), mock.patch.object(ready_redis, "get_status", return_value=hot), mock.patch.object(
        ap, "get_account_pool_meta", side_effect=AssertionError("unexpected PG read")
    ) as pg_read:
        with settings_store.async_pool_state_writes():
            meta = ap._request_path_meta("acc-1")
    ok(meta == hot, "request failure metadata comes from the ready index")
    pg_read.assert_not_called()
    ok(True, "indexed request failure performs no PG metadata read")


def main() -> int:
    tests = [
        test_event_schema_and_sampling,
        test_capture_policy,
        test_writer_idempotency_and_aggregation,
        test_sql_placeholder_contracts,
        test_writer_dlq_threshold,
        test_writer_collects_to_batch_threshold,
        test_state_version_guard,
        test_indexed_selection_is_payload_lazy,
        test_indexed_failure_meta_avoids_pg,
    ]
    failed = 0
    for test in tests:
        try:
            test()
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {test.__name__}: {exc}")
    if failed:
        print(f"\n{failed}/{len(tests)} failed")
        return 1
    print(f"\nall {len(tests)} passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
