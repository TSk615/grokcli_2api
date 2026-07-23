#!/usr/bin/env python3
"""Regression: CPA-style session affinity improvements (v1.9.83).

Covers:
  - Claude Code session_<uuid> extraction from metadata.user_id
  - Expanded session headers (Session_id / x-amp-thread-id / …)
  - messages content hash short/full fallback
  - model-scoped fingerprint (same session, different model → different fp)
  - clear_affinity_for_account drops sticky bindings
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

# Isolate file-mode affinity from any live Redis / shared affinity.json.
# Must set env BEFORE importing config / conversation_affinity (REDIS_URL is
# read at import time from .env). Also monkeypatch _redis_mode after import
# so a host with local Redis cannot divert bind/get into Redis.
_tmp = tempfile.mkdtemp(prefix="g2a-aff-test-")
os.environ["REDIS_URL"] = ""
os.environ["GROK2API_REDIS_URL"] = ""
os.environ["GROK2API_STORE_BACKEND"] = "file"
os.environ["GROK2API_AFFINITY_FILE"] = str(Path(_tmp) / "affinity.json")
os.environ["GROK2API_CONVERSATION_AFFINITY"] = "1"

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from grok2api.pool import conversation_affinity as aff  # noqa: E402

# Force pure in-process map for unit tests (ignore host .env Redis).
aff._redis_mode = lambda: False  # type: ignore[assignment]


def _reset() -> None:
    with aff._lock:
        aff._map.clear()
        aff._loaded = True
        aff._dirty = False


def ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ok: {msg}")


def test_claude_session_extract() -> None:
    print("[claude session extract]")
    raw = "user_abc_session_01234567-89ab-cdef-0123-456789abcdef_extra"
    got = aff.extract_claude_session_id(raw)
    ok(got == "session_01234567-89ab-cdef-0123-456789abcdef", f"extract got {got!r}")
    ok(aff.extract_claude_session_id("plain-user") is None, "plain user_id ignored")
    ok(
        aff.extract_claude_session_id("session_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        == "session_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "bare session id accepted",
    )


def test_body_metadata_claude_session() -> None:
    print("[body metadata.user_id → conversation id]")
    req = SimpleNamespace(
        metadata={
            "user_id": "acct:foo_session_deadbeef-0000-1111-2222-333333333333"
        }
    )
    cid = aff.extract_conversation_id_from_body(req)
    ok(
        cid == "session_deadbeef-0000-1111-2222-333333333333",
        f"body extract got {cid!r}",
    )


def test_headers_cpa_aliases() -> None:
    print("[headers CPA aliases]")

    class H(dict):
        def get(self, k, default=None):  # noqa: ANN001
            # case-insensitive like Starlette headers roughly
            for kk, vv in self.items():
                if str(kk).lower() == str(k).lower():
                    return vv
            return default

    h = H({"Session_id": "codex-sess-1"})
    ok(
        aff.extract_conversation_id_from_headers(h) == "codex-sess-1",
        "Session_id header",
    )
    h2 = H({"x-amp-thread-id": "amp-thread-9"})
    ok(
        aff.extract_conversation_id_from_headers(h2) == "amp-thread-9",
        "x-amp-thread-id header",
    )
    h3 = H({"x-client-request-id": "cli-req-7"})
    ok(
        aff.extract_conversation_id_from_headers(h3) == "cli-req-7",
        "x-client-request-id header",
    )


def test_messages_hash_short_vs_full() -> None:
    print("[messages hash short/full]")
    m1 = [{"role": "user", "content": "hello world unique-A"}]
    fp_short = aff.messages_content_fingerprint(m1)
    ok(fp_short is not None and fp_short.startswith("fp:"), f"short fp={fp_short}")

    m2 = [
        {"role": "user", "content": "hello world unique-A"},
        {"role": "assistant", "content": "hi there"},
        {"role": "user", "content": "follow up"},
    ]
    fp_full = aff.messages_content_fingerprint(m2)
    ok(fp_full is not None and fp_full.startswith("fp:"), f"full fp={fp_full}")
    ok(fp_short != fp_full, "short first-turn != full multi-turn")

    # Full hash stays stable if only system changes (system is excluded).
    m3 = [
        {"role": "system", "content": "you are helpful v2"},
        {"role": "user", "content": "hello world unique-A"},
        {"role": "assistant", "content": "hi there"},
        {"role": "user", "content": "follow up"},
    ]
    fp_full2 = aff.messages_content_fingerprint(m3)
    ok(fp_full2 == fp_full, "system churn does not change full messages hash")

    # conversation_fingerprint falls through to the stable conversation seed when
    # no cid/pck/user is present.
    fp_via = aff.conversation_fingerprint(m2)
    fp_seed = aff.stable_conversation_seed(m2)
    ok(fp_via == aff.conversation_fingerprint(m1), "seed is stable across turns")
    ok(fp_seed is not None, f"stable seed={fp_seed}")


def test_model_scoped_fingerprint() -> None:
    print("[model-scoped fingerprint]")
    msgs = [{"role": "user", "content": "same chat"}]
    fp_a = aff.conversation_fingerprint(
        msgs, conversation_id="sess-1", model="grok-4"
    )
    fp_b = aff.conversation_fingerprint(
        msgs, conversation_id="sess-1", model="grok-3"
    )
    fp_c = aff.conversation_fingerprint(
        msgs, conversation_id="sess-1", model="grok-4"
    )
    ok(fp_a and fp_b and fp_a != fp_b, "different models → different fp")
    ok(fp_a == fp_c, "same model → same fp")

    # prompt_cache_key path is also model-scoped.
    fp_p1 = aff.conversation_fingerprint(
        msgs, prompt_cache_key="pck-stable", model="m1"
    )
    fp_p2 = aff.conversation_fingerprint(
        msgs, prompt_cache_key="pck-stable", model="m2"
    )
    ok(fp_p1 != fp_p2, "pck + different model → different fp")


def test_clear_affinity_for_account() -> None:
    print("[clear_affinity_for_account]")
    _reset()
    fp1 = aff.conversation_fingerprint(
        [{"role": "user", "content": "a"}], conversation_id="c1", model="m"
    )
    fp2 = aff.conversation_fingerprint(
        [{"role": "user", "content": "b"}], conversation_id="c2", model="m"
    )
    ok(fp1 and fp2 and fp1 != fp2, "two distinct fps")
    aff.bind_affinity(fp1, "acct-dead")
    aff.bind_affinity(fp2, "acct-live")
    ok(aff.get_affinity(fp1) == "acct-dead", "bound dead")
    ok(aff.get_affinity(fp2) == "acct-live", "bound live")
    n = aff.clear_affinity_for_account("acct-dead")
    ok(n >= 1, f"cleared {n} entries")
    ok(aff.get_affinity(fp1) is None, "dead account binding gone")
    ok(aff.get_affinity(fp2) == "acct-live", "other account untouched")


def test_resolve_responses_messages_hash_source() -> None:
    print("[resolve_responses source=root]")
    _reset()
    msgs = [
        {"role": "user", "content": "r1"},
        {"role": "assistant", "content": "a1"},
        {"role": "user", "content": "r2"},
    ]
    fp, prefer, source = aff.resolve_responses_affinity(msgs, model="grok-x")
    ok(fp is not None, f"fp={fp}")
    ok(prefer is None, "first turn no prefer")
    ok(source in ("root_new", "conversation_id_new"), f"source={source}")
    aff.bind_affinity(fp, "acct-1")
    fp2, prefer2, source2 = aff.resolve_responses_affinity(msgs, model="grok-x")
    ok(prefer2 == "acct-1", "sticky hit")
    ok(source2 in ("root", "conversation_id"), f"hit source={source2}")


def main() -> int:
    tests = [
        test_claude_session_extract,
        test_body_metadata_claude_session,
        test_headers_cpa_aliases,
        test_messages_hash_short_vs_full,
        test_model_scoped_fingerprint,
        test_clear_affinity_for_account,
        test_resolve_responses_messages_hash_source,
    ]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
    if failed:
        print(f"\n{failed}/{len(tests)} failed")
        return 1
    print(f"\nall {len(tests)} passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
