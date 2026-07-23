"""Conversation → account sticky affinity.

Keeps multi-turn chats on the **same Grok account** so rotating the pool
(round_robin / least_used / random) does not interrupt prior memory mid-chat.

Fingerprint priority (callers may re-order for Responses):
  1. Explicit conversation id (header or body `conversation_id` / metadata)
  2. OpenAI ``prompt_cache_key`` (alone — do not fold message root)
  3. Responses ``previous_response_id`` chain → linked session_fp + account
  4. OpenAI `user` + conversation root
  5. Stable conversation seed (first user + weak system salt)

Optional ``model`` scopes the fingerprint so the same client session on a
different model does not inherit a stale account binding (CPA
``provider::session::model`` pattern).

When REDIS_URL is set (production hybrid), bindings live in Redis (TTL keys)
so multi-worker processes share sticky sessions. No affinity.json is written.

affinity.json is only a single-process file-mode fallback when Redis is off.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Any

from grok2api.config import DATA_DIR

_lock = threading.RLock()
# fingerprint -> {account_id, bound_at, last_seen, hits}
_map: dict[str, dict[str, Any]] = {}
_loaded = False
_dirty = False
_last_flush = 0.0

AFFINITY_FILE = Path(os.getenv("GROK2API_AFFINITY_FILE", DATA_DIR / "affinity.json"))

# Claude Code embeds session ids as ``session_<uuid>`` inside metadata.user_id.
_CLAUDE_SESSION_RE = re.compile(r"session_[0-9a-fA-F-]{8,}")
# Cap message-hash material so huge tool dumps cannot explode CPU.
_MSG_HASH_MAX_CHARS = 48_000
_MSG_HASH_MAX_MSGS = 48


def _redis_mode() -> bool:
    try:
        from grok2api.store.redis_client import redis_enabled

        return redis_enabled()
    except Exception:
        return False


def _enabled() -> bool:
    try:
        from grok2api.admin.settings_store import get_conversation_affinity_enabled

        return bool(get_conversation_affinity_enabled())
    except Exception:
        pass
    return os.getenv("GROK2API_CONVERSATION_AFFINITY", "1").lower() not in (
        "0",
        "false",
        "no",
    )


def _ttl() -> float:
    try:
        from grok2api.admin.settings_store import get_conversation_affinity_ttl_sec

        return max(60.0, float(get_conversation_affinity_ttl_sec()))
    except Exception:
        pass
    try:
        from grok2api.config import AFFINITY_TTL

        return max(60.0, float(AFFINITY_TTL or 7200.0))
    except Exception:
        pass
    try:
        return max(60.0, float(os.getenv("GROK2API_AFFINITY_TTL", "7200")))
    except ValueError:
        return 7200.0


def _max_entries() -> int:
    try:
        return max(100, int(os.getenv("GROK2API_AFFINITY_MAX", "5000")))
    except ValueError:
        return 5000


def _flush_interval() -> float:
    try:
        return max(5.0, float(os.getenv("GROK2API_AFFINITY_FLUSH_SEC", "15")))
    except ValueError:
        return 15.0


def _ensure_loaded() -> None:
    """Load affinity.json only for file-mode fallback (Redis off)."""
    global _loaded
    if _loaded:
        return
    _loaded = True
    if _redis_mode():
        return
    try:
        if not AFFINITY_FILE.is_file():
            return
        data = json.loads(AFFINITY_FILE.read_text(encoding="utf-8"))
        entries = data.get("entries") if isinstance(data, dict) else None
        if not isinstance(entries, dict):
            return
        now = time.time()
        ttl = _ttl()
        for k, v in entries.items():
            if not isinstance(v, dict) or not v.get("account_id"):
                continue
            last = float(v.get("last_seen") or v.get("bound_at") or 0)
            if now - last > ttl:
                continue
            _map[str(k)] = {
                "account_id": str(v["account_id"]),
                "bound_at": float(v.get("bound_at") or last),
                "last_seen": last,
                "hits": int(v.get("hits") or 0),
                **(
                    {"session_fp": str(v["session_fp"])}
                    if v.get("session_fp")
                    else {}
                ),
            }
    except Exception:
        pass


def _schedule_flush_locked() -> None:
    global _dirty, _last_flush
    if _redis_mode():
        return
    _dirty = True
    now = time.time()
    if now - _last_flush >= _flush_interval():
        _flush_locked()


def _flush_locked() -> None:
    """Persist in-memory map to affinity.json (file-mode only)."""
    global _dirty, _last_flush
    if _redis_mode():
        _dirty = False
        return
    _dirty = False
    _last_flush = time.time()
    try:
        AFFINITY_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": time.time(),
            "ttl_sec": _ttl(),
            "entries": _map,
        }
        tmp = AFFINITY_FILE.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        tmp.replace(AFFINITY_FILE)
    except OSError:
        _dirty = True


def flush() -> None:
    if _redis_mode():
        return
    with _lock:
        _ensure_loaded()
        _flush_locked()


def _content_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for p in content:
            if isinstance(p, str):
                parts.append(p)
            elif isinstance(p, dict):
                if isinstance(p.get("text"), str):
                    parts.append(p["text"])
                elif p.get("type") == "text" and isinstance(p.get("content"), str):
                    parts.append(p["content"])
        return "\n".join(parts)
    if isinstance(content, dict):
        if isinstance(content.get("text"), str):
            return content["text"]
        return str(content)[:500]
    return str(content)[:500]


def _msg_role_content(m: Any) -> tuple[str, str]:
    if hasattr(m, "role"):
        role = str(getattr(m, "role", "") or "")
        content = _content_text(getattr(m, "content", None))
        return role, content
    if isinstance(m, dict):
        return str(m.get("role") or ""), _content_text(m.get("content"))
    return "", ""


def _normalize_model_scope(model: str | None) -> str | None:
    """Scope affinity by resolved model id (skip empty / placeholder)."""
    if not model:
        return None
    m = str(model).strip().lower()
    if not m or m in ("default", "auto", "string", "null", "none"):
        return None
    # Keep short; aliases already resolved by callers via resolve_model.
    return m[:80]


def _fp_parts_base(*, api_key_id: str | None = None, model: str | None = None) -> list[str]:
    parts: list[str] = []
    if api_key_id:
        parts.append(f"key:{api_key_id}")
    m = _normalize_model_scope(model)
    if m:
        parts.append(f"model:{m}")
    return parts


def _finalize_fp(parts: list[str]) -> str:
    return "fp:" + hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:32]


def _message_hash_material(messages: list[Any] | None, *, mode: str) -> str | None:
    """CPA-style messages content hash.

    ``short``: only the first non-system/developer message (stable first turn).
    ``full``: all non-system messages (stable multi-turn when no client session).
    """
    if not messages:
        return None
    chunks: list[str] = []
    total = 0
    count = 0
    for m in messages:
        role, content = _msg_role_content(m)
        r = (role or "").lower()
        if r in ("system", "developer"):
            continue
        text = (content or "").strip()
        if not text:
            # Still include role markers so empty tool rounds stay distinct.
            piece = f"{r}:"
        else:
            piece = f"{r}:{text}"
        if mode == "short":
            if not text and r not in ("user", "human"):
                continue
            chunks.append(piece[:4000])
            break
        remain = _MSG_HASH_MAX_CHARS - total
        if remain <= 0 or count >= _MSG_HASH_MAX_MSGS:
            break
        if len(piece) > remain:
            piece = piece[:remain]
        chunks.append(piece)
        total += len(piece)
        count += 1
    if not chunks:
        return None
    raw = "\n".join(chunks)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def messages_content_fingerprint(
    messages: list[Any] | None,
    *,
    api_key_id: str | None = None,
    model: str | None = None,
    mode: str | None = None,
) -> str | None:
    """Fingerprint from message contents when no explicit session id is present.

    Mirrors CLIProxyAPI SessionAffinitySelector fallback:
      - first turn (no assistant yet) → short hash of first user message
      - multi-turn → full hash of non-system messages
    """
    if not _enabled() or not messages:
        return None
    has_assistant = False
    for m in messages:
        role, _ = _msg_role_content(m)
        if (role or "").lower() in ("assistant", "model"):
            has_assistant = True
            break
    use_mode = (mode or ("full" if has_assistant else "short")).lower()
    if use_mode not in ("short", "full"):
        use_mode = "full" if has_assistant else "short"
    digest = _message_hash_material(messages, mode=use_mode)
    if not digest:
        return None
    parts = _fp_parts_base(api_key_id=api_key_id, model=model)
    parts.append(f"msg{use_mode}:{digest}")
    return _finalize_fp(parts)


def conversation_fingerprint(
    messages: list[Any] | None,
    *,
    user: str | None = None,
    conversation_id: str | None = None,
    api_key_id: str | None = None,
    prompt_cache_key: str | None = None,
    model: str | None = None,
) -> str | None:
    """
    Stable id for one multi-turn chat. Same sticky identity → same fingerprint
    across turns; different chats → new id.

    Priority for sticky identity:
      1. conversation_id (explicit client session / chat id)
      2. prompt_cache_key (OpenAI / sub2api / Claude Code cache sticky key)
      3. user + conversation root
      4. conversation root alone

    When ``prompt_cache_key`` is present it is used *alone* (plus optional
    api_key_id / model). We intentionally do **not** fold conversation root into
    the fingerprint: Responses / partial-history clients change root every turn,
    and mixing root would break account stickiness that prompt caching needs.

    ``model`` (when provided) scopes the fingerprint so the same session on a
    different model does not reuse a stale account binding.
    """
    if not _enabled():
        return None

    parts = _fp_parts_base(api_key_id=api_key_id, model=model)

    cid = (conversation_id or "").strip()
    if cid:
        parts.append(f"cid:{cid}")
        return _finalize_fp(parts)

    pck = (prompt_cache_key or "").strip()
    if pck:
        # Stable cache key is the multi-turn identity. Do not mix message root —
        # partial histories / Responses input would change it every turn.
        parts.append(f"pck:{pck}")
        return _finalize_fp(parts)

    u = (user or "").strip()
    if u and u.lower() not in ("user", "default", "anonymous", "string"):
        parts.append(f"user:{u}")
        root = _conversation_root(messages)
        if root:
            parts.append(f"root:{root}")
        return _finalize_fp(parts)

    root = _conversation_root(messages)
    if not root:
        return None
    parts.append(f"root:{root}")
    return _finalize_fp(parts)


def response_chain_fingerprint(
    response_id: str | None,
    *,
    api_key_id: str | None = None,
) -> str | None:
    """Sticky key for OpenAI Responses ``previous_response_id`` chains.

    Each Responses turn mints a new response_id. Binding the *emitted*
    response_id → account (+ optional session_fp) lets the next turn's
    previous_response_id pin the same multi-turn identity even when
    prompt_cache_key / full history root are absent or unstable.
    """
    if not _enabled():
        return None
    rid = (response_id or "").strip()
    if not rid:
        return None
    parts: list[str] = []
    if api_key_id:
        parts.append(f"key:{api_key_id}")
    parts.append(f"resp:{rid}")
    return "fp:" + hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:32]


# Lines Claude Code / agent harnesses often rewrite every turn — strip for root.
_VOLATILE_SYSTEM_LINE = re.compile(
    r"(?i)^("
    r"current\s+date|today'?s\s+date|date\s*:|time\s*:|"
    r"cwd\s*:|working\s+directory|present\s+working\s+directory|"
    r"git\s+status|git\s+branch|branch\s*:|"
    r"model\s*:|session\s*id\s*:|"
    r"\d{4}-\d{2}-\d{2}([t\s]\d{2}:\d{2})?"  # bare ISO dates
    r")"
)


def _stable_system_salt(text: str) -> str:
    """Weak, churn-resistant salt from system text (not full identity)."""
    if not text:
        return ""
    keep: list[str] = []
    for raw in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = raw.strip()
        if not line:
            continue
        if _VOLATILE_SYSTEM_LINE.search(line):
            continue
        # Skip pure absolute paths / file lists that agents re-dump.
        if line.startswith("/") and " " not in line[:4]:
            continue
        keep.append(line[:160])
        if len(keep) >= 24:
            break
    joined = "\n".join(keep)
    if not joined:
        # Fall back to a short head hash so empty-after-strip still salts a bit.
        head = re.sub(r"\s+", " ", text).strip()[:240]
        if not head:
            return ""
        return hashlib.sha256(head.encode("utf-8")).hexdigest()[:12]
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]


def _conversation_root(messages: list[Any] | None) -> str:
    """Root identity of a chat for affinity when no explicit session key exists.

    Claude Code / agent harnesses often rewrite large system blocks every turn
    (date, cwd, git). Using the full system text as identity shatters stickiness.

    Strategy:
      - Primary: first user message (stable for the whole chat)
      - Secondary: weak system salt (volatile lines stripped, then hashed)
      - Later assistant/tool turns never affect the root
    """
    if not messages:
        return ""
    system_parts: list[str] = []
    first_user: str | None = None
    for m in messages:
        role, content = _msg_role_content(m)
        role_l = role.lower()
        if role_l == "system" and content:
            system_parts.append(content[:2000])
        elif role_l == "user" and content and first_user is None:
            first_user = content[:2000]
            break
    sys_salt = _stable_system_salt("\n".join(system_parts))
    if first_user is not None:
        if sys_salt:
            return f"user:{first_user}|sys:{sys_salt}"
        return f"user:{first_user}"
    if system_parts:
        return f"sys:{sys_salt or _stable_system_salt(system_parts[0])}"
    # tool-only / truncated history: use first few messages as weak root
    chunks: list[str] = []
    for m in messages[:3]:
        role, content = _msg_role_content(m)
        if content or role:
            chunks.append(f"{role}:{content[:800]}")
    return "prefix:" + "\n".join(chunks)


def stable_conversation_seed(messages: list[Any] | None) -> str | None:
    """Stable first-turn seed for auto prompt_cache_key minting.

    This intentionally matches the sticky fallback: first user text plus a weak,
    churn-resistant system salt. It avoids full messages hashes, which change as
    history grows and break prompt-cache locality.
    """
    root = _conversation_root(messages)
    return root or None


def _purge_locked(now: float | None = None) -> None:
    now = now or time.time()
    ttl = _ttl()
    dead = [k for k, v in _map.items() if now - float(v.get("last_seen") or 0) > ttl]
    for k in dead:
        _map.pop(k, None)
    max_n = _max_entries()
    if len(_map) > max_n:
        ordered = sorted(
            _map.items(), key=lambda kv: float(kv[1].get("last_seen") or 0)
        )
        for k, _ in ordered[: len(_map) - max_n]:
            _map.pop(k, None)


def get_affinity_entry(fingerprint: str | None) -> dict[str, Any] | None:
    """Return full affinity entry ``{account_id, session_fp?, hits, ...}`` or None."""
    if not fingerprint or not _enabled():
        return None
    if _redis_mode():
        try:
            from grok2api.store import affinity_redis

            entry = affinity_redis.get_entry(fingerprint, ttl_sec=_ttl())
            if isinstance(entry, dict) and entry.get("account_id"):
                return entry
            return None
        except Exception:
            pass  # fall through to file map
    with _lock:
        _ensure_loaded()
        _purge_locked()
        entry = _map.get(fingerprint)
        if not entry:
            return None
        aid = entry.get("account_id")
        if not aid:
            return None
        entry["last_seen"] = time.time()
        entry["hits"] = int(entry.get("hits") or 0) + 1
        _schedule_flush_locked()
        out = {
            "account_id": str(aid),
            "hits": int(entry.get("hits") or 0),
            "bound_at": entry.get("bound_at"),
            "last_seen": entry.get("last_seen"),
        }
        sfp = entry.get("session_fp")
        if sfp:
            out["session_fp"] = str(sfp)
        # Must surface stored prompt_cache_key so previous_response_id-only
        # turns (Codex) recover the same synthetic key instead of minting a
        # new one every turn (cold account + cache miss + slow TTFT).
        pck = entry.get("prompt_cache_key")
        if pck:
            out["prompt_cache_key"] = str(pck)
        return out


def get_affinity(fingerprint: str | None) -> str | None:
    """Return bound account_id if still valid."""
    entry = get_affinity_entry(fingerprint)
    if not entry:
        return None
    aid = entry.get("account_id")
    return str(aid) if aid else None


def bind_affinity(
    fingerprint: str | None,
    account_id: str | None,
    *,
    session_fp: str | None = None,
    prompt_cache_key: str | None = None,
) -> None:
    """Pin conversation fingerprint to account after successful use.

    Optional ``session_fp`` is stored on the entry (used by response-chain
    links so later turns recover the multi-turn identity, not just the account).
    Optional ``prompt_cache_key`` is stored so a later previous_response_id
    lookup can recover the synthetic cache key for clients that never echo it.
    """
    if not fingerprint or not account_id or not _enabled():
        return
    sfp = (session_fp or "").strip() or None
    pck = normalize_prompt_cache_key(prompt_cache_key)
    if _redis_mode():
        try:
            from grok2api.store import affinity_redis

            affinity_redis.bind(
                fingerprint,
                account_id,
                ttl_sec=_ttl(),
                session_fp=sfp,
                prompt_cache_key=pck,
            )
            return
        except Exception:
            pass
    now = time.time()
    with _lock:
        _ensure_loaded()
        _purge_locked(now)
        prev = _map.get(fingerprint)
        if prev and prev.get("account_id") == account_id:
            prev["last_seen"] = now
            prev["hits"] = int(prev.get("hits") or 0) + 1
            if sfp:
                prev["session_fp"] = sfp
            elif not prev.get("session_fp") and fingerprint.startswith("fp:"):
                # Self-link when binding a session identity entry.
                prev.setdefault("session_fp", fingerprint)
            if pck:
                prev["prompt_cache_key"] = pck
            _schedule_flush_locked()
            return
        entry = {
            "account_id": account_id,
            "bound_at": now,
            "last_seen": now,
            "hits": 1 if not prev else int(prev.get("hits") or 0) + 1,
        }
        if sfp:
            entry["session_fp"] = sfp
        elif prev and prev.get("session_fp"):
            entry["session_fp"] = prev.get("session_fp")
        if pck:
            entry["prompt_cache_key"] = pck
        elif prev and prev.get("prompt_cache_key"):
            entry["prompt_cache_key"] = prev.get("prompt_cache_key")
        _map[fingerprint] = entry
        _schedule_flush_locked()


def clear_affinity(fingerprint: str | None) -> None:
    if not fingerprint:
        return
    if _redis_mode():
        try:
            from grok2api.store import affinity_redis

            affinity_redis.clear(fingerprint)
            return
        except Exception:
            pass
    with _lock:
        _ensure_loaded()
        if fingerprint in _map:
            _map.pop(fingerprint, None)
            _schedule_flush_locked()


def clear_affinity_for_account(account_id: str | None) -> int:
    """Drop all sticky bindings pointing at a dead / disabled account.

    Mirrors CLIProxyAPI SessionAffinitySelector.InvalidateAuth — once an auth
    is unusable, keep later turns from pinning to it via a stale cache entry.
    Returns the number of entries cleared (best-effort).

    Redis mode: SCAN + delete matching keys. Always also scrub the in-process
    map so a Redis outage that fell back to memory for bind/get does not leave
    stale pins behind.
    """
    if not account_id:
        return 0
    aid = str(account_id)
    removed = 0
    if _redis_mode():
        try:
            from grok2api.store import affinity_redis

            removed += int(affinity_redis.clear_for_account(aid) or 0)
        except Exception:
            pass
    # Always scrub local map (file mode primary path; redis outage fallback).
    with _lock:
        _ensure_loaded()
        dead = [k for k, v in _map.items() if str(v.get("account_id") or "") == aid]
        for k in dead:
            _map.pop(k, None)
            removed += 1
        if dead:
            _schedule_flush_locked()
    return removed


def rebind_on_failover(
    fingerprint: str | None,
    failed_account_id: str | None,
    new_account_id: str | None,
    *,
    session_fp: str | None = None,
) -> None:
    """
    Sticky account failed; rebind so later turns stay on the account that worked.
    """
    if not fingerprint or not new_account_id:
        return
    # Preserve session_fp across failover when present.
    sfp = (session_fp or "").strip() or None
    if not sfp:
        entry = get_affinity_entry(fingerprint)
        if entry and entry.get("session_fp"):
            sfp = str(entry["session_fp"])
    if _redis_mode():
        # In Redis mode, always rebind to the account that worked.
        cur = get_affinity(fingerprint)
        if cur and failed_account_id and cur != failed_account_id:
            return
        bind_affinity(fingerprint, new_account_id, session_fp=sfp)
        return
    with _lock:
        _ensure_loaded()
        entry = _map.get(fingerprint)
        if entry and failed_account_id and entry.get("account_id") != failed_account_id:
            return
    bind_affinity(fingerprint, new_account_id, session_fp=sfp)


def status() -> dict[str, Any]:
    if _redis_mode():
        try:
            from grok2api.store import affinity_redis

            sample = affinity_redis.status_sample()
            return {
                "enabled": _enabled(),
                "ttl_sec": _ttl(),
                "max_entries": _max_entries(),
                "backend": "redis",
                "active": sample.get("active", 0),
                "persist_file": None,
                "sample": sample.get("sample") or [],
            }
        except Exception as e:  # noqa: BLE001
            return {
                "enabled": _enabled(),
                "backend": "redis",
                "error": str(e),
                "active": 0,
                "sample": [],
            }
    with _lock:
        _ensure_loaded()
        _purge_locked()
        return {
            "enabled": _enabled(),
            "ttl_sec": _ttl(),
            "max_entries": _max_entries(),
            "backend": "file",
            "active": len(_map),
            "persist_file": str(AFFINITY_FILE),
            "sample": [
                {
                    "fp": k[:12] + "…",
                    "account_id": (v.get("account_id") or "")[:48],
                    "hits": v.get("hits"),
                    "session_fp": (
                        (str(v.get("session_fp") or "")[:16] + "…")
                        if v.get("session_fp")
                        else None
                    ),
                    "age_sec": int(
                        time.time() - float(v.get("bound_at") or time.time())
                    ),
                }
                for k, v in list(_map.items())[:8]
            ],
        }


def extract_claude_session_id(user_id: str | None) -> str | None:
    """Extract Claude Code ``session_<uuid>`` from metadata.user_id (CPA parity)."""
    if not user_id:
        return None
    s = str(user_id).strip()
    if not s:
        return None
    m = _CLAUDE_SESSION_RE.search(s)
    if m:
        return m.group(0)[:200]
    # Bare session_… string without surrounding junk.
    if s.startswith("session_") and len(s) >= 16:
        return s[:200]
    return None


def extract_conversation_id_from_headers(headers: Any) -> str | None:
    """Read optional client conversation / session id from request headers.

    Header priority follows CLIProxyAPI SessionAffinitySelector where useful:
      x-session-id / Session_id (Codex) / x-amp-thread-id / x-client-request-id
    plus our existing grok2api / chat aliases.
    """
    if headers is None:
        return None
    try:
        get = headers.get
    except Exception:
        return None
    for name in (
        "x-grok2api-conversation-id",
        "x-conversation-id",
        "x-chat-id",
        "x-session-id",
        "session_id",  # Codex-style
        "x-amp-thread-id",
        "x-client-request-id",
        "x-thread-id",
        "x-request-id",
    ):
        v = get(name)
        if v and str(v).strip():
            return str(v).strip()[:200]
    return None


def extract_conversation_id_from_body(req: Any) -> str | None:
    """Body conversation_id / metadata.session (OpenAI + Claude Code extras).

    Also peels Claude Code ``session_<uuid>`` out of ``metadata.user_id`` so
    Claude Code multi-turn sticks even when conversation_id is absent.
    """
    if req is None:
        return None
    for attr in (
        "conversation_id",
        "conversationId",
        "chat_id",
        "session_id",
        "thread_id",
    ):
        v = getattr(req, attr, None)
        if v is None and isinstance(req, dict):
            v = req.get(attr)
        if v and str(v).strip():
            return str(v).strip()[:200]
    meta = getattr(req, "metadata", None)
    if meta is None and isinstance(req, dict):
        meta = req.get("metadata")
    if isinstance(meta, dict):
        for key in (
            "conversation_id",
            "conversationId",
            "chat_id",
            "session_id",
            "thread_id",
            "user_id",  # may contain Claude session_…; also plain id
        ):
            v = meta.get(key)
            if not v:
                continue
            s = str(v).strip()
            if not s:
                continue
            if key == "user_id":
                claude = extract_claude_session_id(s)
                if claude:
                    return claude
                # Plain metadata.user_id as session only when it looks session-like.
                if s.startswith("session_") or s.startswith("conv_") or s.startswith("thread_"):
                    return s[:200]
                continue
            return s[:200]
    # pydantic extra fields
    extra = getattr(req, "model_extra", None)
    if isinstance(extra, dict):
        for key in ("conversation_id", "conversationId", "chat_id", "session_id"):
            v = extra.get(key)
            if v and str(v).strip():
                return str(v).strip()[:200]
    return None


def extract_prompt_cache_key(req: Any) -> str | None:
    """OpenAI prompt_cache_key (body / metadata / extras / headers-like attrs).

    Used for sticky affinity so multi-turn cache-oriented clients stay on one
    account even when conversation_id is absent.
    """
    if req is None:
        return None

    def _take(v: Any) -> str | None:
        if v is None:
            return None
        s = str(v).strip()
        if not s or s.lower() in ("null", "none", "undefined"):
            return None
        return s[:240]

    for attr in ("prompt_cache_key", "promptCacheKey", "cache_key", "cacheKey"):
        v = getattr(req, attr, None)
        if v is None and isinstance(req, dict):
            v = req.get(attr)
        got = _take(v)
        if got:
            return got

    meta = getattr(req, "metadata", None)
    if meta is None and isinstance(req, dict):
        meta = req.get("metadata")
    if isinstance(meta, dict):
        for key in (
            "prompt_cache_key",
            "promptCacheKey",
            "cache_key",
            "cacheKey",
            "session_id",
            "sessionId",
        ):
            got = _take(meta.get(key))
            if got:
                return got

    extra = getattr(req, "model_extra", None)
    if isinstance(extra, dict):
        for key in ("prompt_cache_key", "promptCacheKey", "cache_key", "cacheKey"):
            got = _take(extra.get(key))
            if got:
                return got
    return None


def extract_prompt_cache_key_from_headers(headers: Any) -> str | None:
    """Optional client prompt-cache sticky key from request headers."""
    if headers is None:
        return None
    try:
        get = headers.get
    except Exception:
        return None
    for name in (
        "x-prompt-cache-key",
        "x-openai-prompt-cache-key",
        "x-grok2api-prompt-cache-key",
        "x-cache-key",
    ):
        v = get(name)
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() not in ("null", "none", "undefined"):
            return s[:240]
    return None


def normalize_prompt_cache_key(value: Any) -> str | None:
    """Sanitize a client or synthetic prompt_cache_key for sticky affinity."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in ("null", "none", "undefined"):
        return None
    # Keep keys compact and log-safe; clients may pass long session URLs.
    return s[:240]


def mint_prompt_cache_key(
    *,
    api_key_id: str | None = None,
    conversation_id: str | None = None,
    previous_response_id: str | None = None,
    user: str | None = None,
    seed: str | None = None,
) -> str:
    """Mint a stable multi-turn prompt_cache_key when the client omitted one.

    Priority of identity material:
      1. conversation_id / session seed (already stable)
      2. previous_response_id (ties into the Responses chain)
      3. user id
      4. random uuid (first turn only — subsequent turns should reuse the echo)

    The returned key is namespaced so it will not collide with client keys.
    """
    import uuid

    parts: list[str] = ["g2a"]
    kid = (api_key_id or "").strip()
    if kid:
        parts.append(f"k{hashlib.sha256(kid.encode('utf-8')).hexdigest()[:10]}")

    cid = (conversation_id or "").strip()
    if cid:
        parts.append(f"c{hashlib.sha256(cid.encode('utf-8')).hexdigest()[:16]}")
        return "pck_" + hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:28]

    seed_s = (seed or "").strip()
    if seed_s:
        parts.append(f"s{hashlib.sha256(seed_s.encode('utf-8')).hexdigest()[:16]}")
        return "pck_" + hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:28]

    prev = (previous_response_id or "").strip()
    if prev:
        # Deterministic from previous response so a client that only sends
        # previous_response_id (no prompt_cache_key) still lands on one sticky key.
        parts.append(f"p{hashlib.sha256(prev.encode('utf-8')).hexdigest()[:16]}")
        return "pck_" + hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:28]

    user_s = (user or "").strip()
    if user_s and user_s.lower() not in ("user", "default", "anonymous", "string"):
        parts.append(f"u{hashlib.sha256(user_s.encode('utf-8')).hexdigest()[:12]}")
        return "pck_" + hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:28]

    # Brand-new session with no sticky material: mint once; caller must echo it.
    parts.append(f"n{uuid.uuid4().hex[:16]}")
    return "pck_" + hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:28]


def bind_response_chain(
    response_id: str | None,
    account_id: str | None,
    *,
    api_key_id: str | None = None,
    session_fp: str | None = None,
    prompt_cache_key: str | None = None,
) -> None:
    """Pin an emitted Responses id so the next previous_response_id sticks.

    Also stores ``session_fp`` (the multi-turn conversation fingerprint) so the
    next turn can recover a stable sticky identity even when message roots
    churn. The session_fp entry itself is also refreshed on the account.

    When ``prompt_cache_key`` is provided (often a server-minted key), it is
    stored on the chain entry so a client that only sends previous_response_id
    next turn still recovers the same synthetic cache key.
    """
    if not response_id or not account_id:
        return
    chain_fp = response_chain_fingerprint(response_id, api_key_id=api_key_id)
    if not chain_fp:
        return
    sfp = (session_fp or "").strip() or None
    pck = normalize_prompt_cache_key(prompt_cache_key)
    # Link response_id → account + session_fp + optional pck.
    bind_affinity(chain_fp, account_id, session_fp=sfp, prompt_cache_key=pck)
    # Keep the stable session identity warm so direct get_affinity(session_fp) works.
    if sfp:
        bind_affinity(sfp, account_id, session_fp=sfp, prompt_cache_key=pck)


def get_response_chain_affinity(
    previous_response_id: str | None,
    *,
    api_key_id: str | None = None,
) -> str | None:
    """Lookup account bound to a previous Responses id."""
    entry = get_response_chain_entry(
        previous_response_id, api_key_id=api_key_id
    )
    if not entry:
        return None
    aid = entry.get("account_id")
    return str(aid) if aid else None


def get_response_chain_entry(
    previous_response_id: str | None,
    *,
    api_key_id: str | None = None,
) -> dict[str, Any] | None:
    """Lookup full entry for a previous Responses id (account + session_fp)."""
    fp = response_chain_fingerprint(previous_response_id, api_key_id=api_key_id)
    return get_affinity_entry(fp) if fp else None

def resolve_responses_affinity(
    messages: list[Any] | None,
    *,
    user: str | None = None,
    conversation_id: str | None = None,
    api_key_id: str | None = None,
    prompt_cache_key: str | None = None,
    previous_response_id: str | None = None,
    model: str | None = None,
) -> tuple[str | None, str | None, str]:
    """Resolve sticky (session_fp, prefer_account, source) for Responses turns.

    Priority:
      1. explicit conversation_id
      2. prompt_cache_key
      3. previous_response_id chain (account + linked session_fp)
      4. user / stable conversation seed

    When a previous_response_id hits, the returned session_fp is the *linked*
    multi-turn identity (not the per-response chain key and not a fresh root
    hash). Callers must bind both session_fp and the newly emitted response_id
    with that same session_fp so the chain continues.

    ``model`` scopes fingerprints (CPA-style) so cross-model reuse of a session
    id does not force a stale account binding.
    """
    if not _enabled():
        return None, None, "disabled"

    # 1–2 / 4: ordinary conversation fingerprint (cid / pck / stable seed).
    base_fp = conversation_fingerprint(
        messages,
        user=user,
        conversation_id=conversation_id,
        api_key_id=api_key_id,
        prompt_cache_key=prompt_cache_key,
        model=model,
    )

    # Prefer explicit cid / pck — they are already stable multi-turn keys.
    if conversation_id and str(conversation_id).strip() and base_fp:
        prefer = get_affinity(base_fp)
        return base_fp, prefer, "conversation_id" if prefer else "conversation_id_new"
    if prompt_cache_key and str(prompt_cache_key).strip() and base_fp:
        prefer = get_affinity(base_fp)
        return base_fp, prefer, "prompt_cache_key" if prefer else "prompt_cache_key_new"

    # 3. previous_response_id chain — recover linked session_fp when present.
    prev = (previous_response_id or "").strip()
    if prev:
        entry = get_response_chain_entry(prev, api_key_id=api_key_id)
        if entry and entry.get("account_id"):
            account = str(entry["account_id"])
            linked = (entry.get("session_fp") or "").strip() or None
            if linked:
                # Touch the session identity entry as well.
                get_affinity(linked)  # refresh TTL/hits when present
                return linked, account, "previous_response_id"
            # Legacy chain entry without session_fp: still stick the account,
            # but adopt base_fp (or a synthetic chain-derived session) going forward.
            session = base_fp or response_chain_fingerprint(prev, api_key_id=api_key_id)
            return session, account, "previous_response_id_legacy"

    # 4. stable seed / user fingerprint.
    if base_fp:
        prefer = get_affinity(base_fp)
        return base_fp, prefer, "root" if prefer else "root_new"
    return None, None, "none"
