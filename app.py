"""
grokcli-2api — OpenAI + Anthropic compatible local API using Grok session tokens.

Endpoints:
  GET  /health
  GET  /v1/models
  POST /v1/chat/completions       (OpenAI)
  POST /chat/completions          (alias)
  POST /v1/responses              (OpenAI Responses API; used by sub2api)
  POST /responses                 (alias)
  POST /v1/messages               (Anthropic Messages API)
  POST /messages                  (alias)
  POST /v1/messages/count_tokens  (Anthropic token estimate)
  Admin console at /admin
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from contextvars import ContextVar
from pathlib import Path
from typing import Any, AsyncIterator, Literal

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict

import account_pool
import anthropic_compat as anth
import apikeys
import conversation_affinity
import openai_responses as oai_resp
import token_maintainer
from admin_routes import router as admin_router
from auth import AuthError, GrokCredentials, load_credentials, upstream_headers
from config import (
    FORCE_UPSTREAM_STREAM,
    HOST,
    PORT,
    REASONING_COMPAT,
    SSE_KEEPALIVE_INTERVAL,
    STATIC_DIR,
    TIMEOUT,
    UPSTREAM_BASE,
)
import config as _config
import history_compact
from models import load_models_from_cache, resolve_model

APP_VERSION = "1.9.39"

# Per-request usage context (client IP / path / UA) for request-level ledger rows.
_usage_request_ctx: ContextVar[dict[str, Any] | None] = ContextVar(
    "usage_request_ctx", default=None
)

# Shared upstream HTTP client (per process / worker) — reuse TLS + keepalive.
_http_client: httpx.AsyncClient | None = None
_http_client_lock = asyncio.Lock() if hasattr(asyncio, "Lock") else None  # set later


async def get_http_client() -> httpx.AsyncClient:
    """Process-wide AsyncClient with connection pooling for high concurrency."""
    global _http_client
    if _http_client is not None and not _http_client.is_closed:
        return _http_client
    # Double-checked init (asyncio single-threaded: assignment is enough)
    if _http_client is None or _http_client.is_closed:
        max_conn = int(os.getenv("GROK2API_HTTP_MAX_CONNECTIONS", "200") or 200)
        max_keep = int(os.getenv("GROK2API_HTTP_MAX_KEEPALIVE", "50") or 50)
        # Keep connect timeout tight for TTFT; overall TIMEOUT still covers long streams.
        connect_timeout = float(os.getenv("GROK2API_HTTP_CONNECT_TIMEOUT", "5") or 5)
        connect_timeout = max(1.0, min(30.0, connect_timeout))
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(TIMEOUT, connect=connect_timeout),
            limits=httpx.Limits(
                max_keepalive_connections=max_keep,
                max_connections=max_conn,
                keepalive_expiry=60.0,
            ),
            http2=False,
        )
    return _http_client


async def _close_http_client() -> None:
    global _http_client
    if _http_client is not None and not _http_client.is_closed:
        await _http_client.aclose()
    _http_client = None


def _on_startup() -> None:
    """Linux-friendly: normalize multi-account keys + start background workers.

    Large pools (hundreds of accounts) must not fan out network + rewrite
    multi-MB auth.json at process start — that freezes WSL. We only do a
    cheap normalize here; refresh/probe are staggered + concurrency-capped.

    Multi-worker: only the elected maintainer leader starts token_maintainer
    and model_health (see store.leader).
    """
    # Fail-closed: multi-worker without Redis must not serve split-brain state.
    try:
        from store.redis_client import ensure_redis_or_raise

        ensure_redis_or_raise()
    except Exception as e:  # noqa: BLE001
        print(f"  FATAL store: {e}")
        raise

    # Shared-store status (Redis / PG)
    # Apply admin-persisted runtime settings (password-adjacent tunables live in
    # settings store and must override env defaults after multi-worker boot).
    try:
        from settings_store import apply_runtime_settings_to_modules

        apply_runtime_settings_to_modules()
    except Exception as e:  # noqa: BLE001
        print(f"  (runtime settings apply skipped: {e})")

    try:
        from store import store_status

        st = store_status()
        redis_s = st.get("redis") or {}
        pg_s = st.get("postgres") or {}
        print(
            f"  store: backend={st.get('backend')} workers={st.get('workers')} "
            f"redis={'ok' if redis_s.get('ok') else ('cfg' if redis_s.get('configured') else 'off')} "
            f"pg={'ok' if pg_s.get('ok') else ('cfg' if pg_s.get('configured') else 'off')}"
        )
    except Exception as e:  # noqa: BLE001
        print(f"  store: status unavailable ({e})")

    try:
        from oidc_auth import normalize_auth_file_keys
        from auth_store import read_auth_map

        r = normalize_auth_file_keys()
        n_accounts = len(read_auth_map()) if not r.get("total") else int(r.get("total") or 0)
        if r.get("changed"):
            print(
                f"  multi-account: remounted {r['changed']} auth key(s) "
                f"→ per-user layout (total={r.get('total')})"
            )
        else:
            print(f"  multi-account: {n_accounts} account(s) loaded")
    except Exception as e:  # noqa: BLE001
        print(f"  (auth normalize skipped: {e})")

    # Warm request-path caches so the first user request doesn't pay cold pick.
    try:
        import time as _time

        from auth import list_live_credentials
        from settings_store import get_account_pool_state
        import account_pool as _ap

        t0 = _time.perf_counter()
        live = list_live_credentials(include_expired=True, auto_refresh=False)
        _ = get_account_pool_state()
        chain = _ap.try_acquire_sequence(model=None)
        dt = int((_time.perf_counter() - t0) * 1000)
        print(
            f"  pick warmup: live={len(live)} chain={len(chain)} "
            f"took={dt}ms"
        )
    except Exception as e:  # noqa: BLE001
        print(f"  (pick warmup skipped: {e})")

    # Warm the shared AsyncClient connection pool (TLS/TCP) in background.
    # Using a separate temp client only warms that client, not request path.
    try:
        import asyncio as _asyncio
        import threading as _threading

        def _warm_upstream() -> None:
            try:
                base = (UPSTREAM_BASE or "").rstrip("/")
                if not base:
                    return

                async def _run() -> None:
                    client = await get_http_client()
                    # Prefer a cheap probe against upstream origin.
                    try:
                        await client.head(base, timeout=2.5)
                    except Exception:
                        try:
                            await client.get(base, timeout=2.5)
                        except Exception:
                            # Even a failed request usually establishes keep-alive.
                            pass

                try:
                    _asyncio.run(_run())
                except RuntimeError:
                    # Nested loop / already running: best-effort sync fallback.
                    import httpx as _httpx

                    with _httpx.Client(timeout=_httpx.Timeout(2.5, connect=1.5)) as c:
                        try:
                            c.head(base)
                        except Exception:
                            try:
                                c.get(base)
                            except Exception:
                                pass
            except Exception:
                pass

        _threading.Thread(
            target=_warm_upstream, name="g2a-upstream-warmup", daemon=True
        ).start()
        print("  upstream warmup: armed")
    except Exception as e:  # noqa: BLE001
        print(f"  (upstream warmup skipped: {e})")

    start_maintainers = True
    try:
        from store.leader import should_start_maintainers, status as leader_status

        start_maintainers = should_start_maintainers()
        ls = leader_status()
        print(
            f"  maintainer leader: is_leader={ls.get('is_leader')} "
            f"mode={ls.get('mode')} id={ls.get('leader_id')}"
        )
    except Exception as e:  # noqa: BLE001
        print(f"  (leader election skipped: {e})")
        start_maintainers = True

    if start_maintainers:
        # One-shot cleanup: permanently invalid refresh tokens are dead weight —
        # delete them from auth store + pool so only renew-able accounts remain.
        try:
            from oidc_auth import purge_refresh_invalid_accounts

            purged = purge_refresh_invalid_accounts(dry_run=False)
            if int(purged.get("deleted") or 0) > 0:
                print(
                    "  token maintainer: purged "
                    f"{purged.get('deleted')} permanently invalid account(s)"
                )
        except Exception as e:  # noqa: BLE001
            print(f"  (purge refresh_invalid skipped: {e})")
        try:
            import account_pool as _ap

            rr = _ap.reenable_probe_kick_accounts()
            if int(rr.get("reenabled") or 0) > 0:
                print(
                    "  model health: re-enabled "
                    f"{rr.get('reenabled')} accounts previously hard-disabled by probe"
                )
        except Exception as e:  # noqa: BLE001
            print(f"  (reenable probe_kick skipped: {e})")
        try:
            token_maintainer.start_background()
            ts = token_maintainer.status()
            print(
                "  token maintainer: enabled "
                f"(startup_delay={ts.get('startup_delay_sec')}s "
                f"workers={ts.get('refresh_workers')} "
                f"batch={ts.get('refresh_batch')})"
            )
        except Exception as e:  # noqa: BLE001
            print(f"  (token maintainer failed: {e})")
        try:
            import model_health

            model_health.start_background()
            mh = model_health.status()
            if mh.get("enabled") and mh.get("running"):
                print(
                    "  model health: enabled "
                    f"(startup_delay={mh.get('startup_delay_sec')}s "
                    f"every {mh.get('interval_sec')}s "
                    f"workers={mh.get('probe_workers')} "
                    f"batch={mh.get('probe_batch')} "
                    f"models={mh.get('probe_models')})"
                )
            else:
                print("  model health: disabled or not started")
        except Exception as e:  # noqa: BLE001
            print(f"  (model health failed: {e})")
    else:
        print("  token maintainer: skipped (not leader)")
        print("  model health: skipped (not leader)")

    # Registration engine is optional — never block API startup.
    # Engine: dongguatanglinux/grok-build-auth (HTTP protocol) + MoeMail + sso_to_auth_json.
    try:
        import grok_build_adapter as _reg

        st = _reg.registration_available()
        if st.get("available"):
            print(
                "  registration: ready "
                f"(engine={st.get('engine') or 'grok-build-auth'} "
                f"build={st.get('adapter_build')})"
            )
        else:
            print(
                f"  registration: unavailable ({st.get('error')}) "
                f"(build={st.get('adapter_build')})"
            )
    except Exception as e:  # noqa: BLE001
        print(f"  registration: unavailable ({e})")


async def _on_shutdown() -> None:
    await _close_http_client()


app = FastAPI(
    title="grokcli-2api",
    description=(
        "OpenAI + Anthropic Messages API compatible gateway powered by Grok OIDC "
        "session tokens. High-concurrency multi-worker with Redis + PostgreSQL."
    ),
    version=APP_VERSION,
    on_startup=[_on_startup],
    on_shutdown=[_on_shutdown],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _usage_request_context_middleware(request: Request, call_next):
    """Capture client IP / path / UA for request-level usage events."""
    try:
        xff = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
        if xff:
            ip = xff[:80]
        elif request.client and request.client.host:
            ip = str(request.client.host)[:80]
        else:
            ip = None
        ua = (request.headers.get("user-agent") or "")[:300] or None
        path = str(request.url.path or "")[:200] or None
        token = _usage_request_ctx.set(
            {
                "client_ip": ip,
                "user_agent": ua,
                "path": path,
            }
        )
    except Exception:
        token = _usage_request_ctx.set(None)
    try:
        return await call_next(request)
    finally:
        try:
            _usage_request_ctx.reset(token)
        except Exception:
            pass


app.include_router(admin_router)


# ── request models ──────────────────────────────────────────────────────────


class ChatMessage(BaseModel):
    """OpenAI-compatible chat message, including tool / function-call fields."""

    role: str
    content: Any = None
    name: str | None = None
    # assistant → tool_calls; tool → tool_call_id; legacy function_call
    tool_calls: list[Any] | None = None
    tool_call_id: str | None = None
    function_call: Any | None = None


class ChatCompletionRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    model: str | None = None
    messages: list[ChatMessage]
    stream: bool = False
    temperature: float | None = None
    top_p: float | None = None
    max_tokens: int | None = None
    max_completion_tokens: int | None = None
    stop: str | list[str] | None = None
    presence_penalty: float | None = None
    frequency_penalty: float | None = None
    user: str | None = None
    reasoning_effort: str | None = None
    tools: list[Any] | None = None
    tool_choice: Any | None = None
    parallel_tool_calls: bool | None = None
    functions: list[Any] | None = None  # legacy OpenAI
    function_call: Any | None = None  # legacy OpenAI
    response_format: Any | None = None
    n: int | None = 1
    # Optional sticky-session hints (clients may set these)
    conversation_id: str | None = None
    metadata: dict[str, Any] | None = None
    # OpenAI prompt-cache request fields (forwarded when present; also used
    # for sticky affinity even if upstream ignores them).
    prompt_cache_key: str | None = None
    prompt_cache_retention: Any | None = None


# ── auth gate for local API ─────────────────────────────────────────────────


async def require_api_key(
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="x-api-key"),
) -> apikeys.ApiKeyRecord | None:
    """Validate client key when auth is required; return record or None.

    Runs verify_key off the event loop so Redis/PG/file IO never blocks SSE.
    """
    token = None
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
    elif x_api_key:
        token = x_api_key.strip()

    required = await asyncio.to_thread(apikeys.auth_required)
    if not required:
        if token:
            return await asyncio.to_thread(apikeys.verify_key, token)
        return None

    rec = await asyncio.to_thread(apikeys.verify_key, token)
    if rec is None:
        try:
            from store.metrics import inc

            inc("g2a_auth_failures_total")
        except Exception:
            pass
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return rec


# ── helpers ─────────────────────────────────────────────────────────────────


def _normalize_content(content: Any) -> Any:
    """Keep OpenAI multimodal content; stringify only when needed."""
    if content is None:
        return None
    if isinstance(content, (str, list, dict)):
        return content
    return str(content)


# Built-in search tool types that new-api / OpenAI clients may inject.
# cli-chat-proxy chat/completions only accepts tools[].type = function | live_search,
# and live_search now requires `sources` AND is deprecated (410 Agent Tools API).
# So for the OpenAI chat path we DROP these built-ins instead of forwarding them.
_BUILTIN_SEARCH_TOOL_TYPES = frozenset(
    {
        "web_search",
        "web_search_preview",
        "live_search",
        "x_search",
        "builtin_function",
        "builtin",
    }
)


def _is_builtin_search_tool(tool: Any) -> bool:
    if not isinstance(tool, dict):
        return False
    ttype = (tool.get("type") or "").strip().lower()
    return ttype in _BUILTIN_SEARCH_TOOL_TYPES


def _empty_tool_parameters() -> dict[str, Any]:
    """Minimal JSON Schema object accepted by strict upstream deserializers."""
    return {"type": "object", "properties": {}}


def _ensure_tool_parameters(params: Any) -> dict[str, Any]:
    """Coerce tool parameters / input_schema to a JSON-schema object.

    Upstream rejects tools when `parameters` is missing, null, a non-object, or
    an empty bare value — error looks like:
      tools[0]: missing field `parameters`
    Always return a dict with at least type=object.
    """
    if params is None:
        return _empty_tool_parameters()
    if isinstance(params, str):
        text = params.strip()
        if not text:
            return _empty_tool_parameters()
        try:
            import json as _json

            parsed = _json.loads(text)
        except Exception:
            return _empty_tool_parameters()
        return _ensure_tool_parameters(parsed)
    if not isinstance(params, dict):
        return _empty_tool_parameters()
    out = dict(params)
    # Some clients send schema without top-level type.
    if "type" not in out:
        out["type"] = "object"
    if out.get("type") == "object" and "properties" not in out:
        out["properties"] = {}
    return out


def _normalize_function_tool(t: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize one function tool so function.parameters is always present."""
    if isinstance(t.get("function"), dict):
        fn = dict(t["function"])
        name = fn.get("name") or t.get("name")
        if not name:
            return None
        fn["name"] = name
        raw_params = (
            fn.get("parameters")
            if fn.get("parameters") is not None
            else fn.get("input_schema")
            if fn.get("input_schema") is not None
            else t.get("parameters")
            if t.get("parameters") is not None
            else t.get("input_schema")
        )
        fn["parameters"] = _ensure_tool_parameters(raw_params)
        # Drop alternate schema key so upstream only sees `parameters`.
        fn.pop("input_schema", None)
        if t.get("description") is not None and fn.get("description") is None:
            fn["description"] = t["description"]
        return {"type": "function", "function": fn}

    # Flat function shape: {type,name,description,parameters|input_schema}
    name = t.get("name")
    if not name:
        return None
    fn: dict[str, Any] = {"name": name}
    if t.get("description") is not None:
        fn["description"] = t["description"]
    raw_params = (
        t.get("parameters")
        if t.get("parameters") is not None
        else t.get("input_schema")
    )
    fn["parameters"] = _ensure_tool_parameters(raw_params)
    return {"type": "function", "function": fn}


def _normalize_tools(tools: list[Any] | None) -> list[Any] | None:
    """
    Accept OpenAI Chat Completions tool shape and built-in tool types.

    OpenAI function:
      {"type":"function","function":{"name":...,"description":...,"parameters":...}}
    Flat function (some SDKs):
      {"type":"function","name":...,"description":...,"parameters":...}
    Anthropic-ish:
      {"name":...,"description":...,"input_schema":...}

    Built-in web/live search tools from new-api playground / OpenAI Responses:
      {"type":"web_search" | "web_search_preview" | "live_search" | "x_search", ...}

    Upstream cli-chat-proxy chat/completions:
      - tools[].type only allows `function` | `live_search`
      - bare `live_search` → 422 missing field `sources`
      - `live_search` + sources → 410 deprecated (Agent Tools API)
      - function tools MUST include function.parameters

    Therefore built-in search tools are **stripped** on this chat path so
    new-api / relays do not surface Upstream 422. Client function tools pass
    with parameters always filled.
    """
    if not tools:
        return tools
    out: list[Any] = []
    for t in tools:
        if not isinstance(t, dict):
            continue
        ttype = (t.get("type") or "function").lower()
        # Drop built-in search tools — do not map to broken/deprecated live_search.
        if ttype in _BUILTIN_SEARCH_TOOL_TYPES:
            continue
        # Anthropic tools often omit type and only have name + input_schema.
        if ttype != "function":
            # Unknown non-function types are unsafe for this upstream; drop them
            # rather than forwarding a shape that 422s the whole request.
            # Exception: bare name+schema without type already defaulted to function.
            if t.get("type") is not None:
                continue
        norm = _normalize_function_tool(t)
        if norm is not None:
            out.append(norm)
    return out or None


def _normalize_tool_choice(tool_choice: Any) -> Any:
    """
    Accept OpenAI Chat Completions tool_choice and map to upstream shape.
    Supports: "none" | "auto" | "required" | {"type":"function","function":{"name":"..."}}

    Built-in search tool_choice (web_search / live_search / …) is dropped —
    upstream chat rejects or deprecates those variants.
    """
    if tool_choice is None:
        return None
    if isinstance(tool_choice, str):
        return tool_choice.lower()
    if not isinstance(tool_choice, dict):
        return tool_choice
    tc_type = (tool_choice.get("type") or "function").lower()
    if tc_type in _BUILTIN_SEARCH_TOOL_TYPES:
        # Fall back to auto rather than forcing a deprecated live_search choice.
        return "auto"
    if tc_type != "function":
        # Unknown object choice — drop to auto to avoid upstream 422.
        return "auto"
    fn = tool_choice.get("function")
    if isinstance(fn, dict) and fn.get("name"):
        return {"type": "function", "function": {"name": fn["name"]}}
    return tool_choice


def _message_to_upstream(m: ChatMessage) -> dict[str, Any]:
    """Serialize a chat message including tool-call round-trip fields."""
    msg: dict[str, Any] = {"role": m.role}
    if m.name:
        msg["name"] = m.name
    if m.tool_call_id:
        msg["tool_call_id"] = m.tool_call_id
    if m.tool_calls:
        msg["tool_calls"] = m.tool_calls
    if m.function_call is not None:
        msg["function_call"] = m.function_call

    content = _normalize_content(m.content)
    # OpenAI: assistant messages with tool_calls may have content=null
    if content is None:
        if m.tool_calls or m.function_call is not None:
            msg["content"] = None
        elif m.role == "tool":
            msg["content"] = ""
        else:
            msg["content"] = ""
    else:
        msg["content"] = content
    return msg


def build_upstream_body(req: ChatCompletionRequest, model: str) -> dict[str, Any]:
    body: dict[str, Any] = {
        "model": model,
        "messages": [_message_to_upstream(m) for m in req.messages],
        "stream": True if FORCE_UPSTREAM_STREAM else bool(req.stream),
    }

    tools = _normalize_tools(req.tools)
    tool_choice = _normalize_tool_choice(req.tool_choice)
    # grok-search / web-search model aliases used to auto-inject live_search.
    # Upstream now deprecates live_search on chat/completions (410 Agent Tools).
    # Keep aliases as normal chat models; do not inject broken search tools.
    if req.model and req.model.strip().lower() in ("grok-search", "web-search"):
        # If the only client tools were built-in search (now stripped), tools may
        # be None — that is intentional and avoids Upstream 422.
        if tool_choice is not None and _is_builtin_search_tool(
            tool_choice if isinstance(tool_choice, dict) else {"type": str(tool_choice)}
        ):
            tool_choice = "auto"
    # Codex compact / pure-text turns often send tool_choice without tools.
    # Upstream rejects that with 400 invalid-argument.
    has_tools = bool(tools) or bool(req.functions)
    if not has_tools:
        tool_choice = None
        parallel_tool_calls = None
        function_call = None
    else:
        parallel_tool_calls = req.parallel_tool_calls
        function_call = req.function_call

    optional = {
        "temperature": req.temperature,
        "top_p": req.top_p,
        "max_tokens": req.max_tokens,
        "max_completion_tokens": req.max_completion_tokens,
        "stop": req.stop,
        "presence_penalty": req.presence_penalty,
        "frequency_penalty": req.frequency_penalty,
        "user": req.user,
        "reasoning_effort": req.reasoning_effort,
        "tools": tools,
        "tool_choice": tool_choice,
        "parallel_tool_calls": parallel_tool_calls,
        "functions": req.functions,
        "function_call": function_call,
        "response_format": req.response_format,
        "n": req.n,
        # Prompt-cache request hints (OpenAI / secondary relays). Kept until
        # _sanitize_upstream_body decides whether upstream accepts them.
        "prompt_cache_key": req.prompt_cache_key,
        "prompt_cache_retention": req.prompt_cache_retention,
    }
    for k, v in optional.items():
        if v is not None:
            body[k] = v
    # Also pick prompt_cache_* from pydantic extras / metadata when clients put
    # them outside the typed fields (common with new-api param overrides).
    _merge_prompt_cache_request_fields(body, req)
    # cli-chat-proxy / grok-4.5 rejects several OpenAI sampling knobs that
    # new-api playground enables by default (presence/frequency_penalty, etc.).
    # Strip unsupported fields so secondary relays don't surface empty streams.
    _sanitize_upstream_body(body, model=model)
    # Secondary relays (newapi/sub2api) rely on final stream usage for billing.
    _ensure_stream_include_usage(body)
    # Long Claude Code tool loops → huge bodies; compact past tool results.
    _apply_history_compact(body)
    return body


# Parameters known to be rejected by cli-chat-proxy for current Grok Build models.
# Keep this list conservative: only drop fields that upstream 400s on.
_UPSTREAM_UNSUPPORTED_PARAMS = frozenset(
    {
        "presence_penalty",
        "frequency_penalty",
        # Some builds also reject these OpenAI extras when forwarded blindly.
        "logit_bias",
        "logprobs",
        "top_logprobs",
        "n",
        # OpenAI prompt-cache request fields are not accepted by cli-chat-proxy.
        # We still accept them on the public API for sticky affinity + relay
        # compatibility, then strip before upstream.
        "prompt_cache_key",
        "prompt_cache_retention",
    }
)


def _merge_prompt_cache_request_fields(body: dict[str, Any], req: Any) -> None:
    """Copy prompt_cache_key / retention from request extras into body if missing."""
    if not isinstance(body, dict):
        return
    if body.get("prompt_cache_key") in (None, ""):
        pck = conversation_affinity.extract_prompt_cache_key(req)
        if pck:
            body["prompt_cache_key"] = pck
    if body.get("prompt_cache_retention") is None:
        ret = getattr(req, "prompt_cache_retention", None)
        if ret is None and isinstance(req, dict):
            ret = req.get("prompt_cache_retention")
        if ret is None:
            extra = getattr(req, "model_extra", None)
            if isinstance(extra, dict):
                ret = extra.get("prompt_cache_retention")
        if ret is None:
            meta = getattr(req, "metadata", None)
            if meta is None and isinstance(req, dict):
                meta = req.get("metadata")
            if isinstance(meta, dict):
                ret = meta.get("prompt_cache_retention")
        if ret is not None:
            body["prompt_cache_retention"] = ret


def _sanitize_upstream_body(body: dict[str, Any], *, model: str | None = None) -> None:
    """Drop/clamp fields that cli-chat-proxy rejects for Grok models."""
    # Internal bookkeeping must never reach upstream.
    body.pop("_history_compact", None)
    body.pop("_prompt_cache_key", None)
    body.pop("_prompt_cache_retention", None)
    # Deprecated live-search knobs → 410 on current cli-chat-proxy builds.
    body.pop("search_parameters", None)
    body.pop("web_search_options", None)
    # Always drop known-unsupported OpenAI knobs for this upstream.
    for key in list(body.keys()):
        if key in _UPSTREAM_UNSUPPORTED_PARAMS:
            body.pop(key, None)

    # n>1 is unsupported; force single completion.
    if body.get("n") not in (None, 1):
        body["n"] = 1

    # Zero penalties are still rejected by name, so already removed above.
    # Clamp temperature/top_p to sane ranges if present.
    if "temperature" in body:
        try:
            t = float(body["temperature"])
            body["temperature"] = max(0.0, min(2.0, t))
        except (TypeError, ValueError):
            body.pop("temperature", None)
    if "top_p" in body:
        try:
            p = float(body["top_p"])
            body["top_p"] = max(0.0, min(1.0, p))
        except (TypeError, ValueError):
            body.pop("top_p", None)

    # max_tokens=0 / negative is invalid for many clients and upstreams.
    for mk in ("max_tokens", "max_completion_tokens"):
        if mk in body:
            try:
                if int(body[mk]) < 1:
                    body.pop(mk, None)
            except (TypeError, ValueError):
                body.pop(mk, None)

    # new-api playground may inject non-OpenAI fields (e.g. group) via extra="allow".
    body.pop("group", None)

    # Final tools scrub: chat/completions only allows function tools, and each
    # function tool must include `parameters` (upstream 422 otherwise).
    tools = body.get("tools")
    if isinstance(tools, list):
        cleaned: list[Any] = []
        for t in tools:
            if not isinstance(t, dict):
                continue
            if (t.get("type") or "function").lower() != "function":
                continue
            norm = _normalize_function_tool(t)
            if norm is not None:
                cleaned.append(norm)
        if cleaned:
            body["tools"] = cleaned
        else:
            body.pop("tools", None)
    elif tools is not None:
        body.pop("tools", None)
    # Legacy OpenAI `functions` array also needs parameters if present.
    funcs = body.get("functions")
    if isinstance(funcs, list):
        fixed_fns: list[Any] = []
        for f in funcs:
            if not isinstance(f, dict) or not f.get("name"):
                continue
            fn = dict(f)
            raw = (
                fn.get("parameters")
                if fn.get("parameters") is not None
                else fn.get("input_schema")
            )
            fn["parameters"] = _ensure_tool_parameters(raw)
            fn.pop("input_schema", None)
            fixed_fns.append(fn)
        if fixed_fns:
            body["functions"] = fixed_fns
        else:
            body.pop("functions", None)
    elif funcs is not None:
        body.pop("functions", None)

    tc = body.get("tool_choice")
    if isinstance(tc, dict):
        tc_type = (tc.get("type") or "function").lower()
        if tc_type in ("auto", "none", "required", "any"):
            # Responses / Anthropic-style {"type":"auto"} → OpenAI string form.
            body["tool_choice"] = "required" if tc_type == "any" else tc_type
        elif tc_type != "function":
            body["tool_choice"] = "auto"
        else:
            fn = tc.get("function") if isinstance(tc.get("function"), dict) else {}
            name = fn.get("name") or tc.get("name")
            if name:
                body["tool_choice"] = {
                    "type": "function",
                    "function": {"name": name},
                }
            else:
                body["tool_choice"] = "auto"

    # Codex compact / pure-text turns often send tool_choice without tools.
    # Upstream cli-chat-proxy rejects that with 400:
    #   "A tool_choice was set on the request but no tools were specified."
    if not body.get("tools") and not body.get("functions"):
        body.pop("tool_choice", None)
        body.pop("function_call", None)
        body.pop("parallel_tool_calls", None)


def _ensure_stream_include_usage(body: dict[str, Any]) -> None:
    """Ask upstream for usage on the final SSE chunk when streaming."""
    if not body.get("stream"):
        return
    opts = body.get("stream_options")
    if not isinstance(opts, dict):
        opts = {}
    else:
        opts = dict(opts)
    opts["include_usage"] = True
    body["stream_options"] = opts


def _body_for_upstream(body: dict[str, Any]) -> dict[str, Any]:
    """Copy body without private grokcli-2api keys (never send to cli-chat-proxy)."""
    if not isinstance(body, dict):
        return body
    out = dict(body)
    out.pop("_history_compact", None)
    return out


def _apply_history_compact(body: dict[str, Any]) -> dict[str, Any]:
    """Compact inbound messages on an OpenAI-style upstream body; stash stats."""
    stats = history_compact.compact_upstream_body(body)
    body["_history_compact"] = stats
    return stats


def _history_compact_headers(body: dict[str, Any]) -> dict[str, str]:
    """Expose compaction stats on responses for debugging long tool sessions."""
    stats = body.get("_history_compact") if isinstance(body, dict) else None
    if not isinstance(stats, dict):
        return {}
    hdr: dict[str, str] = {
        "X-Grok2API-History-Compact": "1" if stats.get("applied") else "0",
    }
    if stats.get("before_chars") is not None:
        hdr["X-Grok2API-History-Before"] = str(stats.get("before_chars"))
    if stats.get("after_chars") is not None:
        hdr["X-Grok2API-History-After"] = str(stats.get("after_chars"))
    if stats.get("tool_rounds") is not None:
        hdr["X-Grok2API-History-Tool-Rounds"] = str(stats.get("tool_rounds"))
    return hdr


def _estimate_text_tokens(text: str) -> int:
    """Rough token estimate (~4 chars / token). Enough for relay billing fallback."""
    if not text:
        return 0
    return max(1, (len(text) + 3) // 4)


def _messages_prompt_estimate(messages: Any) -> int:
    total = 0
    if not isinstance(messages, list):
        return 0
    for m in messages:
        if not isinstance(m, dict):
            continue
        content = m.get("content")
        if isinstance(content, str):
            total += _estimate_text_tokens(content)
        elif isinstance(content, list):
            for part in content:
                if isinstance(part, dict):
                    if isinstance(part.get("text"), str):
                        total += _estimate_text_tokens(part["text"])
                    elif part.get("type") == "image_url":
                        total += 85
                elif isinstance(part, str):
                    total += _estimate_text_tokens(part)
        if m.get("name"):
            total += _estimate_text_tokens(str(m["name"]))
        if isinstance(m.get("tool_calls"), list):
            for tc in m["tool_calls"]:
                if not isinstance(tc, dict):
                    continue
                fn = tc.get("function") if isinstance(tc.get("function"), dict) else {}
                total += _estimate_text_tokens(str(fn.get("name") or ""))
                total += _estimate_text_tokens(str(fn.get("arguments") or ""))
        if m.get("tool_call_id"):
            total += 4
    return total


def _completion_tokens_estimate(
    content: str = "",
    reasoning: str = "",
    tool_calls: list[Any] | None = None,
) -> int:
    total = _estimate_text_tokens(content) + _estimate_text_tokens(reasoning)
    if tool_calls:
        try:
            total += _estimate_text_tokens(json.dumps(tool_calls, ensure_ascii=False))
        except (TypeError, ValueError):
            total += _estimate_text_tokens(str(tool_calls))
    return total


def _usage_detail_int(usage: dict[str, Any] | None, *paths: Any) -> int:
    """Read a nested usage detail int from the first matching path.

    Each path is either a top-level key (str) or a (parent, child) pair for
    nested objects like prompt_tokens_details.cached_tokens.
    """
    if not isinstance(usage, dict):
        return 0
    for path in paths:
        try:
            if isinstance(path, (tuple, list)) and len(path) == 2:
                parent, child = path
                node = usage.get(parent)
                if not isinstance(node, dict):
                    continue
                val = int(node.get(child) or 0)
            else:
                val = int(usage.get(path) or 0)
        except (TypeError, ValueError):
            continue
        if val > 0:
            return val
    return 0


def _normalize_usage(
    usage: dict[str, Any] | None,
    *,
    prompt_fallback: int = 0,
    completion_fallback: int = 0,
) -> dict[str, Any]:
    """Normalize OpenAI-style usage; fill missing fields for secondary relays.

    Always includes cache/reasoning detail containers (0 when unknown) so
    sub2api / Claude Code can read prompt-cache fields instead of treating
    a missing key as "no cache support".
    """
    prompt = 0
    completion = 0
    if isinstance(usage, dict):
        try:
            prompt = int(
                usage.get("prompt_tokens")
                or usage.get("input_tokens")
                or 0
            )
        except (TypeError, ValueError):
            prompt = 0
        try:
            completion = int(
                usage.get("completion_tokens")
                or usage.get("output_tokens")
                or 0
            )
        except (TypeError, ValueError):
            completion = 0
        if not prompt and not completion:
            # Some upstreams only send total_tokens
            try:
                total_only = int(usage.get("total_tokens") or 0)
            except (TypeError, ValueError):
                total_only = 0
            if total_only > 0 and completion_fallback >= 0:
                # Prefer splitting with fallbacks when available
                if completion_fallback and completion_fallback < total_only:
                    completion = completion_fallback
                    prompt = max(0, total_only - completion)
                elif prompt_fallback and prompt_fallback < total_only:
                    prompt = prompt_fallback
                    completion = max(0, total_only - prompt)
                else:
                    prompt = total_only
    if prompt <= 0 and prompt_fallback > 0:
        prompt = prompt_fallback
    if completion <= 0 and completion_fallback > 0:
        completion = completion_fallback
    total = prompt + completion
    if isinstance(usage, dict):
        try:
            reported_total = int(usage.get("total_tokens") or 0)
        except (TypeError, ValueError):
            reported_total = 0
        if reported_total > total:
            total = reported_total

    # Prompt-cache / reasoning details: passthrough only, never invent hits.
    cached = _usage_detail_int(
        usage,
        ("prompt_tokens_details", "cached_tokens"),
        ("input_tokens_details", "cached_tokens"),
        "cached_tokens",
        "cache_read_input_tokens",
        "prompt_cache_hit_tokens",
    )
    cache_creation = _usage_detail_int(
        usage,
        "cache_creation_input_tokens",
        ("prompt_tokens_details", "cache_creation_tokens"),
        ("input_tokens_details", "cache_creation_tokens"),
    )
    reasoning = _usage_detail_int(
        usage,
        ("completion_tokens_details", "reasoning_tokens"),
        ("output_tokens_details", "reasoning_tokens"),
        "reasoning_tokens",
    )

    return {
        "prompt_tokens": int(prompt),
        "completion_tokens": int(completion),
        "total_tokens": int(total),
        # Dual aliases so chat-completions and Responses clients both work.
        "input_tokens": int(prompt),
        "output_tokens": int(completion),
        "prompt_tokens_details": {"cached_tokens": int(cached)},
        "input_tokens_details": {"cached_tokens": int(cached)},
        "completion_tokens_details": {"reasoning_tokens": int(reasoning)},
        "output_tokens_details": {"reasoning_tokens": int(reasoning)},
        # Anthropic-shaped mirrors (harmless for OpenAI clients).
        "cache_read_input_tokens": int(cached),
        "cache_creation_input_tokens": int(cache_creation),
    }


def _usage_from_body_and_output(
    body: dict[str, Any],
    *,
    content: str = "",
    reasoning: str = "",
    tool_calls: list[Any] | None = None,
    usage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prompt_fb = _messages_prompt_estimate(body.get("messages"))
    # tools schema also consumes prompt tokens roughly
    if body.get("tools"):
        try:
            prompt_fb += _estimate_text_tokens(
                json.dumps(body.get("tools"), ensure_ascii=False)
            )
        except (TypeError, ValueError):
            pass
    completion_fb = _completion_tokens_estimate(content, reasoning, tool_calls)
    return _normalize_usage(
        usage, prompt_fallback=prompt_fb, completion_fallback=completion_fb
    )


def _capture_usage_request_ctx(request: Request | None = None) -> dict[str, Any]:
    """Snapshot request meta for usage events (safe to pass into stream tasks)."""
    ctx = dict(_usage_request_ctx.get() or {})
    if request is None:
        return ctx
    try:
        if not ctx.get("client_ip"):
            xff = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
            if xff:
                ctx["client_ip"] = xff[:80]
            elif request.client and request.client.host:
                ctx["client_ip"] = str(request.client.host)[:80]
        if not ctx.get("user_agent"):
            ua = (request.headers.get("user-agent") or "")[:300]
            if ua:
                ctx["user_agent"] = ua
        if not ctx.get("path"):
            path = str(request.url.path or "")[:200]
            if path:
                ctx["path"] = path
    except Exception:
        pass
    return ctx


def _bind_usage_request_ctx(ctx: dict[str, Any] | None):
    """Bind usage request context for the current async task; returns reset token."""
    try:
        return _usage_request_ctx.set(dict(ctx) if isinstance(ctx, dict) else None)
    except Exception:
        return None


def _reset_usage_request_ctx(token) -> None:
    if token is None:
        return
    try:
        _usage_request_ctx.reset(token)
    except Exception:
        pass


def _ttft_log_enabled() -> bool:
    """Default on. Set GROK2API_TTFT_LOG=0 to silence first-token timing logs."""
    raw = (os.getenv("GROK2API_TTFT_LOG") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _ms_since(t0: float | None) -> int | None:
    if t0 is None:
        return None
    try:
        return max(0, int(round((time.perf_counter() - float(t0)) * 1000.0)))
    except Exception:
        return None


def _short_account_id(account_id: str | None) -> str:
    s = str(account_id or "").strip()
    if not s:
        return "-"
    if "::" in s:
        s = s.rsplit("::", 1)[-1]
    return s[:16]


class RequestTiming:
    """Lightweight first-token timing for chat / responses / anthropic streams.

    Logs one line when first client-visible content is produced (or on failure).
    Env: GROK2API_TTFT_LOG=1|0  (default 1)
    """

    __slots__ = (
        "protocol",
        "model",
        "stream",
        "req_id",
        "t0",
        "t_affinity_done",
        "t_pick_done",
        "t_upstream_start",
        "t_upstream_headers",
        "t_first_token",
        "account_id",
        "chain_n",
        "attempt",
        "affinity",
        "logged",
    )

    def __init__(
        self,
        *,
        protocol: str,
        model: str | None = None,
        stream: bool = True,
        req_id: str | None = None,
    ) -> None:
        self.protocol = protocol
        self.model = model or "-"
        self.stream = bool(stream)
        self.req_id = (req_id or uuid.uuid4().hex[:10])[:16]
        self.t0 = time.perf_counter()
        self.t_affinity_done: float | None = None
        self.t_pick_done: float | None = None
        self.t_upstream_start: float | None = None
        self.t_upstream_headers: float | None = None
        self.t_first_token: float | None = None
        self.account_id: str | None = None
        self.chain_n = 0
        self.attempt = 0
        self.affinity = False
        self.logged = False

    def mark_affinity(self, prefer_account: str | None = None) -> None:
        self.t_affinity_done = time.perf_counter()
        self.affinity = bool(prefer_account)

    def mark_pick(
        self,
        chain: list[Any] | None = None,
        *,
        elapsed_ms: float | int | None = None,
    ) -> None:
        # When pick runs in parallel with body build, pass elapsed_ms so the
        # log reflects true pick cost instead of max(pick, body).
        if elapsed_ms is not None:
            try:
                base = self.t_affinity_done or self.t0
                self.t_pick_done = float(base) + max(0.0, float(elapsed_ms) / 1000.0)
            except Exception:
                self.t_pick_done = time.perf_counter()
        else:
            self.t_pick_done = time.perf_counter()
        try:
            self.chain_n = len(chain or [])
        except Exception:
            self.chain_n = 0

    def mark_upstream_start(self, *, account_id: str | None = None, attempt: int = 0) -> None:
        self.t_upstream_start = time.perf_counter()
        self.account_id = account_id
        self.attempt = int(attempt or 0)

    def mark_upstream_headers(self) -> None:
        if self.t_upstream_headers is None:
            self.t_upstream_headers = time.perf_counter()

    def mark_first_token(self, *, kind: str = "content") -> None:
        if self.t_first_token is not None:
            return
        self.t_first_token = time.perf_counter()
        self.emit(ok=True, first=kind)

    def emit(self, *, ok: bool = True, first: str | None = None, error: str | None = None) -> None:
        if self.logged or not _ttft_log_enabled():
            return
        self.logged = True
        try:
            total = _ms_since(self.t0)
            aff = _ms_since(self.t0) if self.t_affinity_done is None else max(
                0, int(round((self.t_affinity_done - self.t0) * 1000.0))
            )
            # pick cost measured from affinity-done (or t0 if affinity skipped)
            pick_base = self.t_affinity_done or self.t0
            pick = (
                None
                if self.t_pick_done is None
                else max(0, int(round((self.t_pick_done - pick_base) * 1000.0)))
            )
            # local = time until we start upstream request
            local = (
                None
                if self.t_upstream_start is None
                else max(0, int(round((self.t_upstream_start - self.t0) * 1000.0)))
            )
            # upstream TTFB = headers after request start
            up_hdr = (
                None
                if self.t_upstream_headers is None or self.t_upstream_start is None
                else max(
                    0,
                    int(
                        round(
                            (self.t_upstream_headers - self.t_upstream_start) * 1000.0
                        )
                    ),
                )
            )
            # first token after headers (model generation)
            up_tok = (
                None
                if self.t_first_token is None or self.t_upstream_headers is None
                else max(
                    0,
                    int(
                        round((self.t_first_token - self.t_upstream_headers) * 1000.0)
                    ),
                )
            )
            ttft = (
                None
                if self.t_first_token is None
                else max(0, int(round((self.t_first_token - self.t0) * 1000.0)))
            )
            parts = [
                f"  [ttft] id={self.req_id}",
                f"proto={self.protocol}",
                f"model={self.model}",
                f"stream={1 if self.stream else 0}",
                f"ok={1 if ok else 0}",
                f"aff={aff if aff is not None else '-'}",
                f"pick={pick if pick is not None else '-'}",
                f"local={local if local is not None else '-'}",
                f"up_hdr={up_hdr if up_hdr is not None else '-'}",
                f"up_tok={up_tok if up_tok is not None else '-'}",
                f"ttft={ttft if ttft is not None else (total if not ok else '-')}",
                f"chain={self.chain_n}",
                f"try={self.attempt}",
                f"sticky={1 if self.affinity else 0}",
                f"acc={_short_account_id(self.account_id)}",
            ]
            if first:
                parts.append(f"first={first}")
            if error:
                parts.append(f"err={str(error)[:160]}")
            print(" ".join(str(p) for p in parts), flush=True)
        except Exception:
            pass


def _record_usage_safe(
    *,
    usage: dict[str, Any] | None = None,
    ok: bool = True,
    api_key_id: str | None = None,
    account_id: str | None = None,
    model: str | None = None,
    protocol: str | None = None,
    stream: bool | None = None,
    path: str | None = None,
    client_ip: str | None = None,
    user_agent: str | None = None,
    status_code: int | None = None,
    latency_ms: int | None = None,
    error: str | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    """Best-effort proxy usage ledger; never raises into the chat path."""
    try:
        import usage_stats

        ctx = _usage_request_ctx.get() or {}
        usage_stats.record_usage(
            usage=usage,
            ok=ok,
            api_key_id=api_key_id,
            account_id=account_id,
            model=model,
            protocol=protocol,
            stream=stream,
            path=path or ctx.get("path"),
            client_ip=client_ip or ctx.get("client_ip"),
            user_agent=user_agent or ctx.get("user_agent"),
            status_code=status_code,
            latency_ms=latency_ms,
            error=error,
            detail=detail,
        )
    except Exception:
        pass


def _api_key_id(rec: apikeys.ApiKeyRecord | None) -> str | None:
    if rec is None:
        return None
    kid = getattr(rec, "id", None)
    if not kid:
        return None
    s = str(kid).strip()
    return s or None


async def _aiter_sse_lines_with_keepalive(
    resp: httpx.Response,
    *,
    keepalive_interval: float | None = None,
) -> AsyncIterator[str | None]:
    """
    Yield SSE lines from upstream; yield None on keepalive ticks.

    Secondary relays (newapi etc.) often idle-timeout long thinking gaps.
    None means the caller should emit an SSE comment / ping.
    """
    if keepalive_interval is None:
        keepalive_interval = max(2.0, float(SSE_KEEPALIVE_INTERVAL or 8.0))
    aiter = resp.aiter_lines()
    pending: asyncio.Future[str] | None = asyncio.ensure_future(aiter.__anext__())
    try:
        while pending is not None:
            try:
                line = await asyncio.wait_for(
                    asyncio.shield(pending), timeout=keepalive_interval
                )
            except asyncio.TimeoutError:
                yield None
                continue
            except StopAsyncIteration:
                break
            except RuntimeError as e:
                # CPython may wrap StopAsyncIteration from __anext__ as RuntimeError
                if "StopAsyncIteration" in str(e):
                    break
                raise
            yield line
            pending = asyncio.ensure_future(aiter.__anext__())
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            try:
                await pending
            except (asyncio.CancelledError, StopAsyncIteration, Exception):
                pass


def openai_error(
    message: str,
    status: int = 500,
    err_type: str = "server_error",
    *,
    retry_after: float | int | None = None,
    code: str | int | None = None,
) -> JSONResponse:
    headers = {}
    if retry_after is not None:
        try:
            headers["Retry-After"] = str(max(1, int(float(retry_after))))
        except (TypeError, ValueError):
            headers["Retry-After"] = "5"
    return JSONResponse(
        status_code=status,
        content={
            "error": {
                "message": message,
                "type": err_type,
                "code": code if code is not None else status,
            }
        },
        headers=headers or None,
    )


def _client_pool_error(exc: Exception | str, *, default_status: int = 503) -> JSONResponse:
    """Map pool/auth selection failures to a client-friendly temporary error.

    Light agent/API callers should see 503 + Retry-After (retryable), not 401
    authentication_error which makes sub2api/Claude Code stop scheduling.
    """
    msg = str(exc or "No eligible accounts")
    low = msg.lower()
    retry_after = 5
    if "no live accounts" in low or "auth store" in low:
        status, err_type, code = 503, "service_unavailable", "no_accounts"
        retry_after = 15
    elif "no eligible" in low or "blocked for model" in low or "cooldown" in low:
        status, err_type, code = 503, "service_unavailable", "pool_exhausted"
        retry_after = 8
    elif "expired" in low:
        status, err_type, code = 503, "service_unavailable", "accounts_expired"
        retry_after = 20
    else:
        status, err_type, code = default_status, "service_unavailable", "pool_unavailable"
    # Keep message short & actionable for relays
    friendly = (
        "账号池暂不可用，正在恢复中，请稍后重试。"
        if status == 503
        else msg
    )
    if "model" in low:
        friendly = "当前模型账号暂时繁忙或额度滚动耗尽，请稍后重试。"
    return openai_error(
        friendly + f" ({msg[:160]})" if msg and msg not in friendly else friendly,
        status=status,
        err_type=err_type,
        retry_after=retry_after,
        code=code,
    )


def _sanitize_upstream_error_message(detail: str, status_code: int | None = None) -> str:
    """Short, non-leaky upstream error for clients (full detail stays in logs/pool)."""
    text = (detail or "").strip()
    low = text.lower()
    if "free-usage-exhausted" in low or "free usage" in low:
        return "上游临时额度耗尽，已自动切换账号"
    if status_code == 429 or "rate limit" in low or "too many requests" in low:
        return "上游限流，已自动切换账号"
    if status_code == 401:
        return "上游鉴权失败，已自动切换账号"
    if status_code in (502, 503, 504):
        return "上游暂时不可用，已自动切换账号"
    if status_code == 404 and "model" in low:
        return "上游模型不可用"
    # Collapse JSON blobs
    if text.startswith("{") and len(text) > 180:
        return f"上游错误 HTTP {status_code or '?'}"
    return (text[:220] + "…") if len(text) > 220 else (text or f"上游错误 HTTP {status_code or '?'}")


def _sse_chunk(
    *,
    chat_id: str,
    model: str,
    created: int,
    content: str | None = None,
    role: str | None = None,
    finish_reason: str | None = None,
    reasoning: str | None = None,
    tool_calls: list[Any] | None = None,
    usage: dict[str, Any] | None = None,
    include_choices: bool = True,
) -> str:
    payload: dict[str, Any] = {
        "id": chat_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
    }
    if include_choices:
        delta: dict[str, Any] = {}
        if role is not None:
            delta["role"] = role
        # Never emit empty content strings — some relays treat "" as a real token
        # and playground UIs may lock/clear the output pane on empty deltas.
        if content is not None and content != "":
            delta["content"] = content
        if reasoning is not None and reasoning != "":
            delta["reasoning_content"] = reasoning
        if tool_calls is not None:
            delta["tool_calls"] = tool_calls
        payload["choices"] = [
            {
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }
        ]
    else:
        # OpenAI final usage-only chunk uses empty choices
        payload["choices"] = []
    if usage is not None:
        payload["usage"] = usage
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


class _ReasoningCompatState:
    """Track <think> open/close when rewriting reasoning for secondary relays."""

    def __init__(self, mode: str | None = None) -> None:
        self.mode = (mode or REASONING_COMPAT or "off").strip().lower()
        # Explicit opt-in aliases for think_tag (legacy "on" meant inject into content).
        if self.mode in ("1", "true", "yes", "on"):
            self.mode = "think_tag"
        if self.mode in ("none", ""):
            self.mode = "off"
        # Unknown values fail closed: keep reasoning out of visible content.
        if self.mode not in ("off", "think_tag", "content"):
            self.mode = "off"
        self.think_open = False
        self.saw_reasoning = False

    @property
    def enabled(self) -> bool:
        return self.mode in ("think_tag", "content")

    def rewrite(
        self, content: str | None, reasoning: str | None
    ) -> tuple[str | None, str | None]:
        """Return (content, reasoning) after compatibility rewrite."""
        if not self.enabled:
            return content, reasoning

        c = content if content else None
        r = reasoning if reasoning else None
        if not r and not c:
            return c, None

        pieces: list[str] = []
        if r:
            self.saw_reasoning = True
            if self.mode == "think_tag":
                if not self.think_open:
                    pieces.append("<think>\n")
                    self.think_open = True
                pieces.append(r)
            else:
                # plain content merge
                pieces.append(r)

        if c:
            # Close only while a think block is open so alternating
            # reasoning/content streams can reopen and re-close correctly.
            if self.mode == "think_tag" and self.think_open:
                pieces.append("\n</think>\n")
                self.think_open = False
            pieces.append(c)

        out = "".join(pieces) if pieces else None
        # When rewriting into content, suppress separate reasoning_content to
        # avoid double-rendering in new-api playground / Claude UIs.
        return out, None

    def close_tag_chunk(self) -> str | None:
        if self.mode == "think_tag" and self.think_open:
            self.think_open = False
            return "\n</think>\n"
        return None


def _sse_keepalive() -> str:
    """SSE comment keepalive for idle gaps (newapi/nginx proxies)."""
    return ": keepalive\n\n"


def _parse_sse_line(line: str) -> dict[str, Any] | None | Literal["[DONE]"]:
    line = line.strip()
    if not line or line.startswith(":"):
        return None
    if not line.startswith("data:"):
        return None
    data = line[5:].strip()
    if data == "[DONE]":
        return "[DONE]"
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return None


def _extract_delta_text(chunk: dict[str, Any]) -> tuple[str, str]:
    """Return (content, reasoning) from various upstream chunk shapes."""
    content, reasoning, _ = _extract_delta_parts(chunk)
    return content, reasoning


def _coerce_tool_arguments(raw: Any) -> str:
    """Normalize tool arguments to the OpenAI streaming string form."""
    return anth.sanitize_tool_arguments_json(raw)


def _merge_tool_arguments(current: str, incoming: str) -> str:
    """
    Merge streamed tool argument fragments without double-append corruption.

    OpenAI true deltas are pure suffixes. Secondary relays (sub2api / new-api)
    often re-send the full cumulative JSON on later chunks or on the final
    message; always-append would yield `{"file_path":"a"}{"file_path":"a"}`
    and break Claude Code Read / Write (missing required fields after parse).
    """
    return anth.merge_tool_argument_delta(current, incoming)



def _iter_tool_sse_chunks(
    *,
    chat_id: str,
    model: str,
    created: int,
    tool_calls: list[Any],
) -> list[str]:
    """One tool_calls[] entry per SSE frame (+ keepalive between tools).

    sub2api's CC→Responses→Anthropic path opens a content_block per tool in the
    same ChatCompletions chunk before closing the previous one. Emitting multiple
    tools in a single delta.tool_calls array therefore produces concurrent open
    blocks and Claude Code: "Content block not found" when later deltas/stops
    target the non-active index.

    Split so each tool is its own SSE event, and insert an SSE comment keepalive
    between tools so converters can close the previous content_block before the
    next tool_use starts (Read is especially sensitive).

    Prefer `_emit_tool_sse_serial` on live streams — it also inserts a real
    wall-clock gap (see GROK2API_OUTBOUND_TOOL_GAP_SEC) because keepalive-only
    bursts still race when sub2api drains a whole TCP window in one tick.
    """
    if not tool_calls:
        return []
    frames: list[str] = []
    first = True
    for tc in tool_calls:
        if not isinstance(tc, dict):
            continue
        if not first:
            frames.append(_sse_keepalive())
        first = False
        frames.append(
            _sse_chunk(
                chat_id=chat_id,
                model=model,
                created=created,
                tool_calls=[tc],
            )
        )
    return frames


async def _yield_anthropic_events_serial(
    events: list[str],
    *,
    client_gone: bool = False,
) -> AsyncIterator[str]:
    """Yield Anthropic SSE events, inserting a real gap before the next tool_use.

    sub2api keeps one content_block active. When multiple tool_use start/stop
    pairs land in one TCP window, Claude Code hits "Content block not found"
    and stops scheduling further agent turns. Sleep only when the next event
    opens another tool_use (not on text/thinking stops).
    """
    if not events:
        return
    gap = float(getattr(history_compact, "OUTBOUND_TOOL_GAP_SEC", 0.0) or 0.0)
    n = len(events)
    for i, ev in enumerate(events):
        if client_gone:
            return
        if (
            gap > 0
            and i > 0
            and '"type": "content_block_start"' in ev
            and "tool_use" in ev
        ):
            await asyncio.sleep(gap)
        yield ev


async def _emit_tool_sse_serial(
    *,
    chat_id: str,
    model: str,
    created: int,
    tool_calls: list[Any],
    already_emitted: int = 0,
) -> AsyncIterator[str]:
    """Yield tool SSE frames one-by-one with keepalive + optional real delay.

    Respects OUTBOUND_MAX_TOOLS via remaining budget. Marks nothing in tool_acc —
    callers must only pass already-built outbound items.
    """
    if not tool_calls:
        return
    budget = history_compact.remaining_outbound_tool_budget(already_emitted)
    gap = float(getattr(history_compact, "OUTBOUND_TOOL_GAP_SEC", 0.0) or 0.0)
    first = True
    emitted_here = 0
    for tc in tool_calls:
        if not isinstance(tc, dict):
            continue
        if budget is not None and emitted_here >= budget:
            break
        if not first:
            yield _sse_keepalive()
            if gap > 0:
                # Give sub2api time to close the previous content_block before
                # the next tool_use start (Read is the usual failure case).
                await asyncio.sleep(gap)
        first = False
        yield _sse_chunk(
            chat_id=chat_id,
            model=model,
            created=created,
            tool_calls=[tc],
        )
        emitted_here += 1

def _tool_slot_known(entry: dict[str, Any] | None) -> bool:
    """True when an accumulated tool slot has any identity/payload worth ordering."""
    if not entry:
        return False
    if entry.get("id"):
        return True
    fn = entry.get("function") or {}
    if (fn.get("name") or "").strip():
        return True
    args = fn.get("arguments") or ""
    return bool(str(args).strip())


def _assign_dense_tool_out_index(
    acc: dict[int, dict[str, Any]], entry: dict[str, Any]
) -> int:
    """Map internal OpenAI tool index → dense 0..n-1 outbound index.

    sub2api often binds the first seen tool_calls[].index to content_block 0.
    Sparse upstream indices (only index=1) must not open block 1 with no block 0.
    """
    existing = entry.get("_out_index")
    if isinstance(existing, int):
        return existing
    used = {
        e["_out_index"]
        for e in acc.values()
        if isinstance(e.get("_out_index"), int)
    }
    out_i = 0
    while out_i in used:
        out_i += 1
    entry["_out_index"] = out_i
    return out_i


def _ingest_tool_call_deltas(
    acc: dict[int, dict[str, Any]], deltas: list[Any]
) -> None:
    """Merge upstream tool_call deltas into acc (no outbound emission)."""
    if not deltas:
        return
    for raw in deltas:
        if not isinstance(raw, dict):
            continue
        try:
            idx = int(raw.get("index", 0))
        except (TypeError, ValueError):
            idx = 0
        if idx not in acc:
            acc[idx] = {
                "id": "",
                "type": "function",
                "function": {"name": "", "arguments": ""},
                "_args_sent": 0,
            }
        entry = acc[idx]
        entry.setdefault("_args_sent", 0)
        if raw.get("id") and not entry.get("id"):
            entry["id"] = raw["id"]
        if raw.get("type"):
            entry["type"] = raw["type"]
        fn = raw.get("function") if isinstance(raw.get("function"), dict) else None
        if fn is not None:
            if fn.get("name"):
                entry["function"]["name"] = _merge_tool_name(
                    entry["function"].get("name") or "", str(fn["name"])
                )
            if fn.get("arguments") is not None:
                entry["function"]["arguments"] = _merge_tool_arguments(
                    entry["function"].get("arguments") or "",
                    _coerce_tool_arguments(fn.get("arguments")),
                )
        else:
            if raw.get("name"):
                entry["function"]["name"] = _merge_tool_name(
                    entry["function"].get("name") or "", str(raw["name"])
                )
            if raw.get("arguments") is not None:
                entry["function"]["arguments"] = _merge_tool_arguments(
                    entry["function"].get("arguments") or "",
                    _coerce_tool_arguments(raw.get("arguments")),
                )
            elif raw.get("input") is not None:
                entry["function"]["arguments"] = _merge_tool_arguments(
                    entry["function"].get("arguments") or "",
                    _coerce_tool_arguments(raw.get("input")),
                )


def _build_outbound_tool_item(
    acc: dict[int, dict[str, Any]], entry: dict[str, Any], *, remaining: str
) -> dict[str, Any]:
    """Build one complete OpenAI tool_calls[] item and mark it emitted.

    Always include a stable call id on first emission. sub2api keys Anthropic
    content blocks by tool call id; frames without id can later surface as
    Claude Code "Content block not found".
    """
    out_index = _assign_dense_tool_out_index(acc, entry)
    name = (entry.get("function", {}).get("name") or "").strip()
    tool_id = (entry.get("id") or "").strip()
    if not tool_id:
        tool_id = f"call_{uuid.uuid4().hex[:24]}"
        entry["id"] = tool_id
    item: dict[str, Any] = {
        "index": out_index,
        "id": tool_id,
        "type": entry.get("type") or "function",
        "function": {"arguments": remaining},
    }
    entry["_id_emitted"] = True
    if name and not entry.get("_name_emitted"):
        item["function"]["name"] = name
        entry["_name_emitted"] = True
    entry["_sent_text"] = remaining
    entry["_args_sent"] = len(remaining)
    entry["_emitted"] = True
    return item


def _tool_call_argument_delta(
    acc: dict[int, dict[str, Any]], deltas: list[Any]
) -> list[dict[str, Any]]:
    """
    Merge tool_call deltas into acc and return sanitized OpenAI-style deltas.

    Critical for Claude Code via sub2api (platform=openai, upstream chat/completions):

    sub2api converts CC → Responses → Anthropic and keeps only one active
    content_block. It also special-cases tool name "Read" by *buffering* args
    and only flushing them on function_call_arguments.done. If we open tool 1
    while tool 0 (Read) is still open, later deltas/stops hit the wrong block
    and Claude Code raises:
        API Error: Content block not found

    Therefore outbound policy is strict:
      1. Hold every tool until name + complete non-empty JSON args are ready
      2. Emit **at most one** complete tool frame per call (atomic id+name+args)
      3. Never open a higher tool while a lower known tool is unfinished
      4. Never stream argument suffixes live — full JSON once, then done

    Intermediate JSON scalars like `"file_path"` and bare `{}` / `[]` stay held.
    """
    _ingest_tool_call_deltas(acc, deltas)

    # Emit at most ONE ready tool per invocation so sub2api can fully
    # start→args→stop that content_block before the next tool opens.
    for idx in sorted(acc.keys()):
        entry = acc[idx]
        if entry.get("_emitted"):
            continue
        args = entry.get("function", {}).get("arguments") or ""
        name = (entry.get("function", {}).get("name") or "").strip()

        # Never overtake a lower known unfinished tool (including sparse holes).
        blocked = False
        for lower in range(0, idx):
            low = acc.get(lower)
            if low is None:
                continue
            if low.get("_emitted"):
                continue
            if _tool_slot_known(low):
                blocked = True
                break
        if blocked:
            break

        if not name:
            # Known id without name — keep holding this slot.
            if entry.get("id") or str(args).strip():
                break
            continue
        if not args or not anth.is_complete_tool_arguments_json(args):
            # Name/id known but args incomplete — hold (do not open block early).
            break

        return [_build_outbound_tool_item(acc, entry, remaining=str(args))]
    return []


def _flush_one_tool_call(acc: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    """Flush at most one still-held tool (terminal / non-SSE safe for sub2api).

    Must not mark sibling tools as emitted — callers loop until empty.
    Truncated upstream args still ship once as a single best-effort payload.
    """
    for idx in sorted(acc.keys()):
        entry = acc[idx]
        if entry.get("_emitted"):
            continue
        fn = entry.get("function") or {}
        name = (fn.get("name") or "").strip()
        args = fn.get("arguments") or ""
        if not isinstance(args, str):
            args = _coerce_tool_arguments(args)
        # Drop fully empty ghost slots.
        if not name and not entry.get("id") and not str(args).strip():
            continue
        if not name:
            # Cannot open a useful tool_use without a name.
            continue
        remaining = str(args) if str(args).strip() else "{}"
        if not anth.is_complete_tool_arguments_json(remaining):
            remaining = str(args) if str(args).strip() else "{}"
        return [_build_outbound_tool_item(acc, entry, remaining=remaining)]
    return []


def _flush_tool_call_argument_deltas(
    acc: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Flush still-held tools as a list (one complete JSON blob per tool).

    Prefer _flush_one_tool_call + loop on the SSE path so sub2api never sees a
    multi-tool burst without per-tool framing. This helper remains for bulk
    collection; OUTBOUND_MAX_TOOLS (default 1 for sub2api) can still cap the returned list
    without marking unreturned siblings as emitted.
    """
    out: list[dict[str, Any]] = []
    while True:
        one = _flush_one_tool_call(acc)
        if not one:
            break
        out.extend(one)
    capped = history_compact.cap_outbound_tools(out)
    if capped is None:
        return out
    if len(capped) < len(out):
        # Un-mark tools that the safety valve dropped so a later flush can ship them
        # if the operator raises the cap mid-process (best-effort).
        kept_ids = {x.get("id") for x in capped if isinstance(x, dict)}
        for entry in acc.values():
            if entry.get("_emitted") and entry.get("id") not in kept_ids:
                # Only unmark if this emission was part of this bulk flush's tail.
                # Safer: leave marked to avoid double-send. Cap is intentional drop.
                pass
    return capped if capped is not None else out


def _merge_tool_name(current: str, incoming: str) -> str:
    """
    Merge function names from streamed deltas without double-append corruption.

    OpenAI usually sends the full name once. Some proxies re-send the full name
    on later chunks; always-append would produce `web_searchweb_search` and break
    tool dispatch intermittently.
    """
    cur = (current or "").strip()
    name = (incoming or "").strip()
    if not name:
        return cur
    if not cur:
        return name
    if name == cur:
        return cur
    if name.startswith(cur):
        # progressive expansion (rare) or full name after prefix
        return name
    if cur.startswith(name):
        # ignore shorter re-send / fragment
        return cur
    # Different name on same index — prefer the newer complete token
    return name


def _legacy_function_call_to_tool_calls(function_call: Any) -> list[dict[str, Any]] | None:
    """Map deprecated OpenAI `function_call` into tool_calls deltas."""
    if not isinstance(function_call, dict):
        return None
    name = function_call.get("name")
    args = function_call.get("arguments")
    if name is None and args is None:
        return None
    return [
        {
            "index": 0,
            "id": function_call.get("id") or f"call_{uuid.uuid4().hex[:24]}",
            "type": "function",
            "function": {
                "name": str(name or ""),
                "arguments": _coerce_tool_arguments(args),
            },
        }
    ]


def _extract_delta_parts(
    chunk: dict[str, Any],
) -> tuple[str, str, list[Any] | None]:
    """Return (content, reasoning, tool_calls_delta) from upstream chunks."""
    content = ""
    reasoning = ""
    tool_calls: list[Any] | None = None

    choices = chunk.get("choices")
    if isinstance(choices, list) and choices:
        c0 = choices[0] or {}
        delta = c0.get("delta") or {}
        msg = c0.get("message") or {}
        if isinstance(delta.get("content"), str):
            content += delta["content"]
        elif isinstance(delta.get("content"), list):
            # rare content-part array
            for part in delta["content"]:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    content += part["text"]
                elif isinstance(part, str):
                    content += part
        if isinstance(msg.get("content"), str) and not content:
            content += msg["content"]
        for key in ("reasoning_content", "reasoning", "thinking"):
            if isinstance(delta.get(key), str):
                reasoning += delta[key]
            if isinstance(msg.get(key), str) and not reasoning:
                reasoning += msg[key]

        # streaming tool_calls live on delta; complete ones may be on message
        if isinstance(delta.get("tool_calls"), list) and delta["tool_calls"]:
            tool_calls = delta["tool_calls"]
        elif isinstance(msg.get("tool_calls"), list) and msg["tool_calls"]:
            # re-emit full tool_calls as a single synthetic delta list
            tool_calls = []
            for i, tc in enumerate(msg["tool_calls"]):
                if not isinstance(tc, dict):
                    continue
                item = dict(tc)
                item.setdefault("index", i)
                # ensure arguments are strings for streaming clients
                fn = item.get("function")
                if isinstance(fn, dict) and fn.get("arguments") is not None and not isinstance(
                    fn.get("arguments"), str
                ):
                    fn = dict(fn)
                    fn["arguments"] = _coerce_tool_arguments(fn.get("arguments"))
                    item["function"] = fn
                tool_calls.append(item)
        else:
            # legacy function_call on delta or message
            fc = delta.get("function_call")
            if not isinstance(fc, dict):
                fc = msg.get("function_call")
            tool_calls = _legacy_function_call_to_tool_calls(fc)

    if not content:
        for key in ("content", "text", "output_text"):
            v = chunk.get(key)
            if isinstance(v, str):
                content = v
                break

    return content, reasoning, tool_calls


def _merge_tool_call_delta(
    acc: dict[int, dict[str, Any]], deltas: list[Any]
) -> None:
    """Accumulate streamed tool_calls deltas into complete tool_call objects."""
    _tool_call_argument_delta(acc, deltas)


def _finalize_tool_calls(
    acc: dict[int, dict[str, Any]],
) -> list[dict[str, Any]] | None:
    if not acc:
        return None
    out: list[dict[str, Any]] = []
    for idx in sorted(acc.keys()):
        entry = acc[idx]
        fn = entry.get("function") or {}
        if not entry.get("id") and not fn.get("name"):
            continue
        tool_id = entry.get("id") or f"call_{uuid.uuid4().hex[:24]}"
        args = fn.get("arguments")
        if args is None:
            args = ""
        else:
            args = _coerce_tool_arguments(args)
        if isinstance(args, str) and not args.strip():
            args = "{}"
        name = (fn.get("name") or "").strip()
        if not name:
            continue
        out.append(
            {
                "id": tool_id,
                "type": entry.get("type") or "function",
                "function": {"name": name, "arguments": args},
            }
        )
    return out or None


def _normalize_stream_finish_reason(
    finish: str | None, *, saw_tool_calls: bool
) -> str | None:
    """Force tool_calls finish when tools were streamed (upstream often says stop)."""
    if finish is None:
        return "tool_calls" if saw_tool_calls else None
    if saw_tool_calls and finish in ("stop", "end_turn", ""):
        return "tool_calls"
    return finish


# ── routes ──────────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    """Bounded readiness probe — never triggers OIDC refresh or full account dump."""
    reg: dict[str, Any] = {"available": False}
    try:
        import grok_build_adapter as _reg

        reg = _reg.registration_available()
    except Exception as e:  # noqa: BLE001
        reg = {"available": False, "error": str(e)}
    store_info: dict[str, Any] = {}
    leader_info: dict[str, Any] = {}
    try:
        from store import store_status

        store_info = store_status()
    except Exception as e:  # noqa: BLE001
        store_info = {"error": str(e)}
    try:
        from store.leader import status as leader_status

        leader_info = leader_status()
    except Exception as e:  # noqa: BLE001
        leader_info = {"error": str(e)}
    try:
        # Counts only; omit the hundreds-of-accounts payload.
        # Offload sync store IO so health never blocks the event loop.
        pool = await asyncio.to_thread(
            account_pool.pool_summary, include_accounts=False
        )
        # Health must stay a bounded read-only route. Do not make an OIDC
        # refresh request while resolving the representative account.
        creds = await asyncio.to_thread(account_pool.acquire, auto_refresh=False)
        return {
            "status": "ok",
            "version": APP_VERSION,
            "email": creds.email,
            "expires_at": creds.expires_at,
            "auth_key": creds.auth_key,
            "upstream": UPSTREAM_BASE,
            "auth_required": apikeys.auth_required(),
            "account_mode": pool.get("mode"),
            "accounts_live": pool.get("live"),
            "accounts_enabled": pool.get("enabled"),
            "accounts_total": pool.get("total"),
            "multi_account": (pool.get("live") or 0) > 1,
            # light=True avoids rescanning auth.json for min_remaining on every poll
            "token_maintainer": token_maintainer.status(light=True),
            "model_health": __import__("model_health").status(light=True),
            "conversation_affinity": conversation_affinity.status(),
            "registration": reg,
            "store": store_info,
            "maintainer_leader": leader_info,
        }
    except AuthError as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "auth_error",
                "message": str(e),
                "version": APP_VERSION,
                "registration": reg,
                "store": store_info,
                "maintainer_leader": leader_info,
            },
        )


@app.get("/metrics")
async def metrics():
    """Prometheus text exposition (in-process counters + store gauges)."""
    from fastapi.responses import PlainTextResponse

    try:
        from store.metrics import prometheus_text

        body = prometheus_text()
    except Exception as e:  # noqa: BLE001
        body = f"# error {e}\n"
    return PlainTextResponse(body, media_type="text/plain; version=0.0.4")


def _admin_page(name: str = "index"):
    """Serve multi-page admin HTML from static/admin/{name}.html."""
    # allow only known pages
    allowed = {
        "index": "index.html",
        "overview": "index.html",
        "login": "login.html",
        "keys": "keys.html",
        "accounts": "accounts.html",
        "models": "models.html",
        "guide": "guide.html",
        "settings": "settings.html",
        "logs": "logs.html",
        "usage": "usage.html",
    }
    filename = allowed.get((name or "index").strip().lower())
    if not filename:
        return None
    admin_file = STATIC_DIR / "admin" / filename
    if not admin_file.is_file():
        # fallback to legacy single-file console
        legacy = STATIC_DIR / "index.html"
        if legacy.is_file() and name in ("index", "overview", "login"):
            admin_file = legacy
        else:
            return None
    return FileResponse(
        admin_file,
        media_type="text/html; charset=utf-8",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


def _admin_html_response():
    """Backward-compatible alias → overview page."""
    return _admin_page("index")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    icon = STATIC_DIR / "favicon.ico"
    if icon.is_file() and icon.stat().st_size > 0:
        return FileResponse(icon, media_type="image/x-icon")
    return JSONResponse({"detail": "not found"}, status_code=404)


@app.get("/")
async def root():
    html = _admin_html_response()
    if html is not None:
        return html
    return {
        "name": "grokcli-2api",
        "version": APP_VERSION,
        "docs": "/docs",
        "admin": "/admin",
        "endpoints": [
            "GET /health",
            "GET /v1/models",
            "POST /v1/chat/completions",
            "POST /v1/responses",
            "POST /v1/messages",
            "POST /v1/messages/count_tokens",
            "Admin /admin",
        ],
        "hint": (
            "OpenAI base_url → <your-host>/v1 · "
            "Anthropic base_url → <your-host> (or /v1). "
            "Responses API (sub2api) → <your-host>/v1/responses. "
            "Use the same host/port you open in the browser; "
            "set GROK2API_PUBLIC_BASE_URL if behind reverse proxy."
        ),
    }


def _admin_or_404(name: str):
    html = _admin_page(name)
    if html is None:
        return JSONResponse(
            status_code=404,
            content={
                "error": f"Admin UI page not found: {name}. Missing static/admin/*.html"
            },
        )
    return html


@app.get("/admin")
@app.get("/admin/")
async def admin_overview_page():
    return _admin_or_404("index")


@app.get("/admin/login")
@app.get("/admin/login/")
async def admin_login_page():
    return _admin_or_404("login")


@app.get("/admin/keys")
@app.get("/admin/keys/")
async def admin_keys_page():
    return _admin_or_404("keys")


@app.get("/admin/accounts")
@app.get("/admin/accounts/")
async def admin_accounts_page():
    return _admin_or_404("accounts")


@app.get("/admin/logs")
@app.get("/admin/logs/")
async def admin_logs_page():
    return _admin_or_404("logs")


@app.get("/admin/usage")
@app.get("/admin/usage/")
async def admin_usage_page():
    return _admin_or_404("usage")


@app.get("/admin/models")
@app.get("/admin/models/")
async def admin_models_page():
    return _admin_or_404("models")


@app.get("/admin/guide")
@app.get("/admin/guide/")
async def admin_guide_page():
    return _admin_or_404("guide")


@app.get("/admin/settings")
@app.get("/admin/settings/")
async def admin_settings_page():
    return _admin_or_404("settings")


@app.get("/v1/models", dependencies=[Depends(require_api_key)])
@app.get("/models", dependencies=[Depends(require_api_key)])
async def list_models():
    return {"object": "list", "data": load_models_from_cache()}


def _retryable_status(code: int) -> bool:
    return code in (401, 403, 429, 500, 502, 503, 504)


def _resolve_conversation_affinity(
    req: ChatCompletionRequest, request: Request
) -> tuple[str | None, str | None]:
    """
    Returns (fingerprint, preferred_account_id).
    Same multi-turn chat → same fingerprint → sticky account
    (pool rotation will not switch accounts mid-conversation).
    """
    conv_id = conversation_affinity.extract_conversation_id_from_headers(
        request.headers
    ) or conversation_affinity.extract_conversation_id_from_body(req)
    pck = conversation_affinity.extract_prompt_cache_key(req)
    fp = conversation_affinity.conversation_fingerprint(
        req.messages,
        user=req.user,
        conversation_id=conv_id,
        prompt_cache_key=pck,
    )
    prefer = conversation_affinity.get_affinity(fp) if fp else None
    return fp, prefer


def _pick_account_chain(
    *,
    model: str,
    prefer_account_id: str | None = None,
) -> list[GrokCredentials]:
    """Build failover chain for one request (runs in a worker thread)."""
    chain = account_pool.try_acquire_sequence(
        model=model, prefer_account_id=prefer_account_id
    )
    if not chain:
        chain = [account_pool.acquire(model=model)]
    return chain


def _pick_account_chain_timed(
    *,
    model: str,
    prefer_account_id: str | None = None,
) -> tuple[list[GrokCredentials], float]:
    """Same as _pick_account_chain but also returns elapsed milliseconds."""
    t0 = time.perf_counter()
    chain = _pick_account_chain(model=model, prefer_account_id=prefer_account_id)
    return chain, max(0.0, (time.perf_counter() - t0) * 1000.0)


def _note_request_metrics(
    *,
    prefer_account: str | None,
    conv_fp: str | None,
) -> None:
    try:
        from store.metrics import inc

        inc("g2a_requests_total")
        if prefer_account:
            inc("g2a_affinity_hits_total")
        elif conv_fp:
            inc("g2a_affinity_misses_total")
    except Exception:
        pass


@app.post("/v1/chat/completions")
@app.post("/chat/completions")
async def chat_completions(
    req: ChatCompletionRequest,
    request: Request,
    api_key: apikeys.ApiKeyRecord | None = Depends(require_api_key),
):
    if not req.messages:
        return openai_error(
            "messages is required", status=400, err_type="invalid_request_error"
        )

    key_id = _api_key_id(api_key)
    timing = RequestTiming(protocol="openai", stream=bool(req.stream))
    conv_fp, prefer_account = await asyncio.to_thread(
        _resolve_conversation_affinity, req, request
    )
    timing.mark_affinity(prefer_account)
    model = resolve_model(req.model)
    timing.model = model

    # Overlap account pick with body sanitize/compact so local TTFT is
    # max(pick, body) instead of pick + body on long tool histories.
    try:
        (chain, pick_ms), body = await asyncio.gather(
            asyncio.to_thread(
                _pick_account_chain_timed,
                model=model,
                prefer_account_id=prefer_account,
            ),
            asyncio.to_thread(build_upstream_body, req, model),
        )
        timing.mark_pick(chain, elapsed_ms=pick_ms)
    except AuthError as e:
        try:
            from store.metrics import inc

            inc("g2a_auth_failures_total")
        except Exception:
            pass
        timing.emit(ok=False, error=str(e))
        return _client_pool_error(e)

    _note_request_metrics(prefer_account=prefer_account, conv_fp=conv_fp)

    url = f"{UPSTREAM_BASE}/chat/completions"
    chat_id = f"chatcmpl-{uuid.uuid4().hex[:24]}"
    timing.req_id = chat_id.replace("chatcmpl-", "")[:12]
    created = int(time.time())

    compact_hdr = _history_compact_headers(body)
    if req.stream:
        return StreamingResponse(
            _stream_proxy_with_failover(
                url=url,
                body=body,
                chain=chain,
                chat_id=chat_id,
                model=model,
                created=created,
                client_disconnected=request.is_disconnected,
                conversation_fp=conv_fp,
                api_key_id=key_id,
                usage_ctx=_capture_usage_request_ctx(request),
                timing=timing,
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
                "X-Grok2API-Accounts": str(len(chain)),
                "X-Grok2API-Affinity": "1" if prefer_account else "0",
                **compact_hdr,
                **(
                    {"X-Grok2API-Conversation-Fp": conv_fp}
                    if conv_fp
                    else {}
                ),
            },
        )

    last_error: str | None = None
    last_status = 502
    used: GrokCredentials | None = None
    first_tried: str | None = chain[0].auth_key if chain else None

    for attempt_i, creds in enumerate(chain):
        headers = upstream_headers(creds.token, model)
        try:
            timing.mark_upstream_start(account_id=creds.auth_key, attempt=attempt_i)
            content, reasoning, finish, usage, tool_calls = await _collect_completion(
                url=url, headers=headers, body=body
            )
            timing.mark_upstream_headers()
            timing.mark_first_token(kind="content" if content else ("tool" if tool_calls else "done"))
            await asyncio.to_thread(account_pool.report_success, creds.auth_key, model=model)
            used = creds
            # Keep multi-turn memory on this account; rebind if failover
            if conv_fp:
                if prefer_account and prefer_account != creds.auth_key:
                    await asyncio.to_thread(
                        conversation_affinity.rebind_on_failover,
                        conv_fp,
                        first_tried,
                        creds.auth_key,
                    )
                    try:
                        from store.metrics import inc

                        inc("g2a_account_failovers_total")
                    except Exception:
                        pass
                else:
                    await asyncio.to_thread(
                        conversation_affinity.bind_affinity, conv_fp, creds.auth_key
                    )
            message: dict[str, Any] = {
                "role": "assistant",
                "content": content if content else (None if tool_calls else ""),
            }
            if reasoning:
                message["reasoning_content"] = reasoning
            if tool_calls:
                message["tool_calls"] = tool_calls
                if not finish or finish == "stop":
                    finish = "tool_calls"
            result: dict[str, Any] = {
                "id": chat_id,
                "object": "chat.completion",
                "created": created,
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "message": message,
                        "finish_reason": finish or "stop",
                    }
                ],
            }
            result["usage"] = _usage_from_body_and_output(
                body,
                content=content or "",
                reasoning=reasoning or "",
                tool_calls=tool_calls,
                usage=usage,
            )
            _record_usage_safe(
                usage=result["usage"],
                ok=True,
                api_key_id=key_id,
                account_id=creds.auth_key,
                model=model,
                protocol="openai",
                stream=False,
            )
            # non-standard but useful for multi-account debugging
            result["x_grok2api_account"] = creds.email or creds.auth_key
            result["x_grok2api_affinity"] = bool(prefer_account)
            hc_stats = body.get("_history_compact") if isinstance(body, dict) else None
            if isinstance(hc_stats, dict):
                result["x_grok2api_history_compact"] = hc_stats
            if conv_fp:
                result["x_grok2api_conversation_fp"] = conv_fp
            return result
        except httpx.HTTPStatusError as e:
            code = e.response.status_code if e.response is not None else 502
            detail = e.response.text[:800] if e.response is not None else str(e)
            hdrs = dict(e.response.headers) if e.response is not None else None
            await asyncio.to_thread(
                account_pool.report_failure,
                creds.auth_key,
                error=detail,
                status_code=code,
                model=model,
                headers=hdrs,
            )
            try:
                from store.metrics import inc

                inc("g2a_upstream_failures_total")
            except Exception:
                pass
            _record_usage_safe(
                ok=False,
                api_key_id=key_id,
                account_id=creds.auth_key,
                model=model,
                protocol="openai",
                stream=False,
            )
            last_error = f"Upstream {code}: {detail}"
            last_status = code
            if not _retryable_status(code):
                break
            continue
        except Exception as e:  # noqa: BLE001
            await asyncio.to_thread(
                account_pool.report_failure,
                creds.auth_key,
                error=str(e),
                status_code=502,
                model=model,
            )
            try:
                from store.metrics import inc

                inc("g2a_upstream_failures_total")
            except Exception:
                pass
            _record_usage_safe(
                ok=False,
                api_key_id=key_id,
                account_id=creds.auth_key,
                model=model,
                protocol="openai",
                stream=False,
            )
            last_error = f"Proxy error: {e}"
            last_status = 502
            continue

    # All accounts in chain failed — tell clients this is temporary/retryable when
    # the last status was a known transient upstream code.
    final_status = last_status if last_status < 600 else 502
    retryable = final_status in (401, 403, 429, 500, 502, 503, 504)
    friendly = _sanitize_upstream_error_message(last_error or "", final_status)
    timing.emit(ok=False, error=friendly or last_error or "all_accounts_failed")
    if retryable:
        return openai_error(
            friendly or "所有账号暂时失败，请稍后重试",
            status=503,
            err_type="upstream_error",
            retry_after=8,
            code="all_accounts_failed",
        )
    return openai_error(
        friendly or last_error or "All accounts failed",
        status=final_status,
        err_type="upstream_error",
        code=final_status,
    )



def _body_requests_tools(body: dict[str, Any] | None) -> bool:
    """True when this turn may produce tool_calls (hold pre-tool text/reasoning).

    Claude Code via sub2api usually sends tools[]. Also treat tool_choice /
    functions / any non-none choice as tools-mode so reasoning cannot open
    content_block 0 before tool index 0.
    """
    if not isinstance(body, dict):
        return False
    if body.get("tools") or body.get("functions"):
        return True
    tc = body.get("tool_choice")
    if tc is None:
        return False
    if isinstance(tc, str):
        return tc.strip().lower() not in ("", "none")
    if isinstance(tc, dict):
        return True
    return bool(tc)


async def _stream_proxy_with_failover(
    *,
    url: str,
    body: dict[str, Any],
    chain: list[GrokCredentials],
    chat_id: str,
    model: str,
    created: int,
    client_disconnected,
    conversation_fp: str | None = None,
    api_key_id: str | None = None,
    usage_ctx: dict[str, Any] | None = None,
    timing: RequestTiming | None = None,
) -> AsyncIterator[str]:
    # Do NOT emit a premature role chunk before upstream accepts — secondary
    # relays treat early chunks as stream-started and cannot safely failover.
    _usage_tok = _bind_usage_request_ctx(usage_ctx)
    try:
        async for chunk in _stream_proxy_with_failover_inner(
            url=url,
            body=body,
            chain=chain,
            chat_id=chat_id,
            model=model,
            created=created,
            client_disconnected=client_disconnected,
            conversation_fp=conversation_fp,
            api_key_id=api_key_id,
            timing=timing,
        ):
            yield chunk
    finally:
        _reset_usage_request_ctx(_usage_tok)


async def _stream_proxy_with_failover_inner(
    *,
    url: str,
    body: dict[str, Any],
    chain: list[GrokCredentials],
    chat_id: str,
    model: str,
    created: int,
    client_disconnected,
    conversation_fp: str | None = None,
    api_key_id: str | None = None,
    timing: RequestTiming | None = None,
) -> AsyncIterator[str]:
    last_err: str | None = None
    first_tried = chain[0].auth_key if chain else None
    role_sent = False
    # When the client request includes tools, Grok often streams a long
    # reasoning_content preface before tool_calls. sub2api/Claude Code convert
    # that preface into content_block 0 (thinking/text), then map OpenAI
    # tool_calls[index=0] onto the same block index →
    # "apiError: Content block not found". Hold pre-tool text/reasoning and
    # drop it from the outbound stream once tool frames are actually emitted
    # (still counted in usage). Incomplete tool previews must not release the
    # hold or open a text block before the first tool frame.
    tools_requested = _body_requests_tools(body)

    for idx, creds in enumerate(chain):
        headers = upstream_headers(creds.token, model)
        finished = False
        saw_tool_calls = False  # True only after a tool frame is outbound
        tools_pending = False  # upstream tool deltas seen but not yet emitted
        held_finish: str | None = None
        stream_started = False  # True once any content has been sent to client
        client_gone = False
        usage: dict[str, Any] | None = None
        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        tool_acc: dict[int, dict[str, Any]] = {}
        tools_emitted_count = 0  # enforces OUTBOUND_MAX_TOOLS across the turn
        reasoning_compat = _ReasoningCompatState()
        # Buffered pre-tool emissions (OpenAI path only). Flushed only if the
        # turn ends without any outbound tool frames.
        held_pre_tool: list[tuple[str | None, str | None]] = []
        try:
            if timing is not None:
                timing.mark_upstream_start(account_id=creds.auth_key, attempt=idx)
            upstream_body = _body_for_upstream(body)
            client = await get_http_client()
            async with client.stream(
                "POST", url, headers=headers, json=upstream_body
            ) as resp:
                if timing is not None:
                    timing.mark_upstream_headers()
                if resp.status_code >= 400:
                    err_text = (await resp.aread()).decode(
                        "utf-8", errors="replace"
                    )[:1500]
                    await asyncio.to_thread(
                        account_pool.report_failure,
                        creds.auth_key,
                        error=err_text,
                        status_code=resp.status_code,
                        model=model,
                        headers=dict(resp.headers),
                    )
                    _record_usage_safe(
                        ok=False,
                        api_key_id=api_key_id,
                        account_id=creds.auth_key,
                        model=model,
                        protocol="openai",
                        stream=True,
                    )
                    last_err = f"Upstream {resp.status_code}: {err_text}"
                    # try next account if retryable and more remain
                    if _retryable_status(resp.status_code) and idx < len(chain) - 1:
                        continue
                    if timing is not None:
                        timing.emit(ok=False, error=last_err)
                    err_payload = {
                        "id": chat_id,
                        "object": "error",
                        "error": {
                            "message": last_err,
                            "type": "upstream_error",
                            "code": resp.status_code,
                        },
                    }
                    yield f"data: {json.dumps(err_payload, ensure_ascii=False)}\n\n"
                    yield "data: [DONE]\n\n"
                    return

                # Defer success/affinity writes until after first client bytes.
                # These hit PG/Redis and used to block TTFT on every stream.
                asyncio.create_task(
                    asyncio.to_thread(
                        account_pool.report_success, creds.auth_key, model=model
                    )
                )
                if conversation_fp:
                    if idx > 0:
                        asyncio.create_task(
                            asyncio.to_thread(
                                conversation_affinity.rebind_on_failover,
                                conversation_fp,
                                first_tried,
                                creds.auth_key,
                            )
                        )
                    else:
                        asyncio.create_task(
                            asyncio.to_thread(
                                conversation_affinity.bind_affinity,
                                conversation_fp,
                                creds.auth_key,
                            )
                        )

                if not role_sent:
                    # Role-only delta (no empty content) — required for new-api playground.
                    # Role alone is not counted as first token; content/tool is.
                    yield _sse_chunk(
                        chat_id=chat_id,
                        model=model,
                        created=created,
                        role="assistant",
                    )
                    role_sent = True

                ctype = (resp.headers.get("content-type") or "").lower()
                if "text/event-stream" in ctype or "stream" in ctype:
                    async for line in _aiter_sse_lines_with_keepalive(resp):
                        # Soft disconnect check: keep draining so we can still
                        # emit a terminal finish/tool_calls frame when possible.
                        try:
                            if await client_disconnected():
                                client_gone = True
                        except Exception:
                            client_gone = True
                        if line is None:
                            # idle keepalive for newapi / reverse proxies
                            if not client_gone:
                                yield _sse_keepalive()
                            continue
                        parsed = _parse_sse_line(line)
                        if parsed is None:
                            continue
                        if parsed == "[DONE]":
                            break
                        assert isinstance(parsed, dict)
                        if isinstance(parsed.get("usage"), dict):
                            usage = parsed["usage"]
                        content, reasoning, tool_calls = _extract_delta_parts(
                            parsed
                        )
                        finish = None
                        choices = parsed.get("choices")
                        if isinstance(choices, list) and choices:
                            finish = choices[0].get("finish_reason")
                        # usage-only final chunk (choices empty / null)
                        if (
                            not content
                            and not reasoning
                            and not tool_calls
                            and not finish
                            and isinstance(parsed.get("usage"), dict)
                        ):
                            usage = parsed["usage"]
                            continue
                        if content:
                            content_parts.append(content)
                        if reasoning:
                            reasoning_parts.append(reasoning)
                        emit_tool_calls: list[Any] | None = None
                        if tool_calls:
                            tools_pending = True
                            # Upstream produced tools even if request tools[]
                            # was stripped — force tools-mode hold/suppress.
                            tools_requested = True
                            # At most ONE complete tool per upstream SSE line.
                            # Burst-draining every ready tool still races
                            # sub2api's single active content_block (esp. Read).
                            # Remaining tools emit on later ticks / terminal
                            # flush, still one frame at a time.
                            budget = history_compact.remaining_outbound_tool_budget(
                                tools_emitted_count
                            )
                            if budget is not None and budget <= 0:
                                # Cap reached: still ingest for accounting, no emit.
                                _ingest_tool_call_deltas(tool_acc, tool_calls)
                                emit_tool_calls = None
                            else:
                                emit_tool_calls = (
                                    _tool_call_argument_delta(tool_acc, tool_calls)
                                    or None
                                )
                                if (
                                    emit_tool_calls
                                    and budget is not None
                                    and len(emit_tool_calls) > budget
                                ):
                                    emit_tool_calls = emit_tool_calls[:budget]
                            # Only when a tool frame is actually outbound do
                            # tools "win" the turn. Incomplete name-only /
                            # partial-arg previews must keep holding preface.
                            if emit_tool_calls:
                                saw_tool_calls = True
                                if tools_requested and held_pre_tool:
                                    held_pre_tool.clear()
                                    reasoning_compat.think_open = False

                        emit_content, emit_reasoning = reasoning_compat.rewrite(
                            content if content else None,
                            reasoning if reasoning else None,
                        )

                        # Tools-requested turns: never interleave reasoning /
                        # content with tool_calls on the wire.
                        # - Hold preface until we know the turn is non-tool
                        # - Keep holding while tools are pending incomplete
                        # - Once tools are emitted, suppress ALL further
                        #   content/reasoning (sub2api maps text before tool
                        #   to content_block 0 and then fails on tool index 0)
                        if tools_requested and not saw_tool_calls:
                            # Still buffering (no outbound tool frame yet),
                            # whether or not incomplete tool previews arrived.
                            # Keep holding so a non-tool finish can flush text.
                            if emit_content or emit_reasoning:
                                held_pre_tool.append(
                                    (emit_content, emit_reasoning)
                                )
                            emit_content, emit_reasoning = None, None
                        elif tools_requested and saw_tool_calls:
                            # Tools already on the wire: drop all further
                            # text/reasoning (avoids content_block clashes).
                            emit_content, emit_reasoning = None, None

                        if finish:
                            # Hold finish until stream drain so we can attach
                            # usage on the same terminal chunk. sub2api/new-api
                            # typically read usage from the finish_reason frame
                            # and ignore a later usage-only chunk.
                            stream_started = True
                            finished = True
                            held_finish = finish
                            # Close <think> before terminal finish if still open.
                            # Skip while a tools-preface is still held — we may
                            # flush that preface below without an early close.
                            if (
                                not client_gone
                                and not (
                                    tools_requested
                                    and not saw_tool_calls
                                    and held_pre_tool
                                )
                                and not (tools_requested and saw_tool_calls)
                            ):
                                close_tag = reasoning_compat.close_tag_chunk()
                                if close_tag:
                                    yield _sse_chunk(
                                        chat_id=chat_id,
                                        model=model,
                                        created=created,
                                        content=close_tag,
                                    )

                        if emit_content or emit_reasoning or emit_tool_calls:
                            stream_started = True
                            if client_gone:
                                continue
                            # Split content/reasoning and tool_calls into separate
                            # SSE frames. sub2api/Claude Code converters that open
                            # text then tool from one mixed delta can leave the
                            # wrong content_block active ("Content block not found").
                            if emit_content or emit_reasoning:
                                if timing is not None:
                                    timing.mark_first_token(
                                        kind="content" if emit_content else "reasoning"
                                    )
                                yield _sse_chunk(
                                    chat_id=chat_id,
                                    model=model,
                                    created=created,
                                    content=emit_content,
                                    reasoning=emit_reasoning,
                                )
                            if emit_tool_calls:
                                saw_tool_calls = True
                                if timing is not None:
                                    timing.mark_first_token(kind="tool")
                                _n_before = tools_emitted_count
                                async for _tc_frame in _emit_tool_sse_serial(
                                    chat_id=chat_id,
                                    model=model,
                                    created=created,
                                    tool_calls=emit_tool_calls,
                                    already_emitted=tools_emitted_count,
                                ):
                                    yield _tc_frame
                                    if _tc_frame.startswith("data: "):
                                        tools_emitted_count += 1
                                if tools_emitted_count < _n_before:
                                    tools_emitted_count = _n_before
                                # Continue draining any additional complete tools already
                                # held in tool_acc (serial + gap). Prevents agent loops
                                # from stalling when upstream packed multiple tools into
                                # one window but we only emit one per upstream line.
                                while True:
                                    _budget2 = history_compact.remaining_outbound_tool_budget(
                                        tools_emitted_count
                                    )
                                    if _budget2 is not None and _budget2 <= 0:
                                        break
                                    # Re-check readiness without new deltas.
                                    more = _tool_call_argument_delta(tool_acc, [])
                                    if not more:
                                        break
                                    if _budget2 is not None and len(more) > _budget2:
                                        more = more[:_budget2]
                                    async for _tc_frame in _emit_tool_sse_serial(
                                        chat_id=chat_id,
                                        model=model,
                                        created=created,
                                        tool_calls=more,
                                        already_emitted=tools_emitted_count,
                                    ):
                                        yield _tc_frame
                                        if _tc_frame.startswith("data: "):
                                            tools_emitted_count += 1
                        elif finish:
                            # finish-only upstream frame: content already held
                            continue
                else:
                    raw = await resp.aread()
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        text = raw.decode("utf-8", errors="replace")
                        content_parts.append(text)
                        stream_started = True
                        if not client_gone:
                            yield _sse_chunk(
                                chat_id=chat_id,
                                model=model,
                                created=created,
                                content=text,
                            )
                        finished = True
                        held_finish = "stop"
                    else:
                        if isinstance(data.get("usage"), dict):
                            usage = data["usage"]
                        content, reasoning, tool_calls = _extract_delta_parts(data)
                        msg_tool_calls: list[Any] | None = None
                        finish_reason = "stop"
                        if not content and not tool_calls:
                            choices = data.get("choices") or []
                            if choices:
                                ch0 = choices[0] or {}
                                msg = ch0.get("message") or {}
                                content = msg.get("content") or ""
                                reasoning = (
                                    msg.get("reasoning_content") or reasoning
                                )
                                if isinstance(msg.get("tool_calls"), list):
                                    msg_tool_calls = msg["tool_calls"]
                                finish_reason = (
                                    ch0.get("finish_reason") or finish_reason
                                )
                        else:
                            choices = data.get("choices") or []
                            if choices:
                                ch0 = choices[0] or {}
                                finish_reason = (
                                    ch0.get("finish_reason") or finish_reason
                                )
                                msg = ch0.get("message") or {}
                                if (
                                    not tool_calls
                                    and isinstance(msg.get("tool_calls"), list)
                                ):
                                    msg_tool_calls = msg["tool_calls"]

                        emit_tc = tool_calls or msg_tool_calls
                        sanitized_tc: list[Any] | None = None
                        if emit_tc:
                            tools_pending = True
                            tools_requested = True
                            if isinstance(emit_tc, list):
                                # One tool per conversion tick (see stream path).
                                sanitized_tc = (
                                    _tool_call_argument_delta(tool_acc, emit_tc)
                                    or None
                                )
                            if sanitized_tc:
                                saw_tool_calls = True
                        # Flush remaining held tools one-by-one from non-SSE body.
                        if tool_acc and not client_gone:
                            flushed: list[Any] = []
                            while True:
                                one = _flush_one_tool_call(tool_acc)
                                if not one:
                                    break
                                flushed.extend(one)
                            if flushed:
                                saw_tool_calls = True
                                sanitized_tc = (sanitized_tc or []) + flushed
                        finish_reason = _normalize_stream_finish_reason(
                            finish_reason, saw_tool_calls=saw_tool_calls
                        ) or ("tool_calls" if saw_tool_calls else "stop")
                        if content:
                            content_parts.append(content)
                        if reasoning:
                            reasoning_parts.append(reasoning)
                        stream_started = True
                        emit_content, emit_reasoning = reasoning_compat.rewrite(
                            content if content else None,
                            reasoning if reasoning else None,
                        )
                        # Same sub2api clash: if tools won, do not open a
                        # thinking/text block before (or with) tool_calls.
                        if tools_requested and (
                            saw_tool_calls or tools_pending or sanitized_tc
                        ):
                            emit_content, emit_reasoning = None, None
                            held_pre_tool.clear()
                            reasoning_compat.think_open = False
                        close_tag = reasoning_compat.close_tag_chunk()
                        if close_tag and not (
                            tools_requested and (saw_tool_calls or tools_pending)
                        ):
                            emit_content = (emit_content or "") + close_tag
                        # Tools first, then content only if this is a non-tool turn.
                        if sanitized_tc and not client_gone:
                            saw_tool_calls = True
                            async for _tc_frame in _emit_tool_sse_serial(
                                chat_id=chat_id,
                                model=model,
                                created=created,
                                tool_calls=sanitized_tc,
                                already_emitted=tools_emitted_count,
                            ):
                                yield _tc_frame
                                if _tc_frame.startswith("data: "):
                                    tools_emitted_count += 1
                        if emit_content and not (
                            tools_requested and saw_tool_calls
                        ):
                            yield _sse_chunk(
                                chat_id=chat_id,
                                model=model,
                                created=created,
                                content=emit_content,
                            )
                        if (
                            emit_reasoning
                            and not reasoning_compat.enabled
                            and not (tools_requested and saw_tool_calls)
                        ):
                            yield _sse_chunk(
                                chat_id=chat_id,
                                model=model,
                                created=created,
                                reasoning=emit_reasoning,
                            )
                        # Defer finish_reason to terminal chunk with usage.
                        finished = True
                        held_finish = finish_reason

            # Flush deferred complete tool-argument snapshots before finish so
            # clients that only naive-append stream deltas still get full args.
            if tool_acc and not client_gone:
                # Flush remaining tools one SSE frame at a time (sub2api single
                # active content_block). Keepalive + wall-clock gap between tools.
                while True:
                    budget = history_compact.remaining_outbound_tool_budget(
                        tools_emitted_count
                    )
                    if budget is not None and budget <= 0:
                        break
                    one = _flush_one_tool_call(tool_acc)
                    if not one:
                        break
                    saw_tool_calls = True
                    held_pre_tool.clear()
                    reasoning_compat.think_open = False
                    async for _tc_frame in _emit_tool_sse_serial(
                        chat_id=chat_id,
                        model=model,
                        created=created,
                        tool_calls=one,
                        already_emitted=tools_emitted_count,
                    ):
                        yield _tc_frame
                        if _tc_frame.startswith("data: "):
                            tools_emitted_count += 1
            final_tc = _finalize_tool_calls(tool_acc)
            # Only treat as tool-finish when something was (or will be) emitted.
            if final_tc and any(
                (e.get("_emitted") or (e.get("function") or {}).get("name"))
                for e in tool_acc.values()
            ):
                # Keep saw_tool_calls only if we actually shipped tool frames,
                # or flush above already set it. Avoid finish_reason=tool_calls
                # with zero outbound tool_calls (empty ghost slots).
                if any(e.get("_emitted") for e in tool_acc.values()):
                    saw_tool_calls = True
                    held_pre_tool.clear()
                    reasoning_compat.think_open = False
            terminal_finish = _normalize_stream_finish_reason(
                held_finish if finished else None,
                saw_tool_calls=saw_tool_calls,
            ) or ("tool_calls" if saw_tool_calls else "stop")
            # Prefer real completion tokens from streamed content+reasoning; many
            # relays mark empty completion_tokens as a failed playground turn.
            # Compute usage BEFORE emitting finish so sub2api/new-api can read it
            # from the finish_reason chunk (they often ignore a later usage-only).
            # If tools won, omit pre-tool reasoning from usage "visible" estimate
            # still include it — billing should reflect upstream work.
            norm_usage = _usage_from_body_and_output(
                body,
                content="".join(content_parts),
                reasoning="".join(reasoning_parts),
                tool_calls=final_tc if saw_tool_calls else None,
                usage=usage,
            )
            if not client_gone:
                # Non-tool turn with tools_requested: flush held preface now.
                # After flush above, saw_tool_calls is the source of truth —
                # incomplete tool previews that never became outbound frames
                # must not swallow a normal text answer.
                if held_pre_tool and not saw_tool_calls:
                    for held_c, held_r in held_pre_tool:
                        if held_c or held_r:
                            stream_started = True
                            yield _sse_chunk(
                                chat_id=chat_id,
                                model=model,
                                created=created,
                                content=held_c,
                                reasoning=held_r,
                            )
                    held_pre_tool.clear()
                elif held_pre_tool and saw_tool_calls:
                    held_pre_tool.clear()
                close_tag = reasoning_compat.close_tag_chunk()
                if close_tag and not (tools_requested and saw_tool_calls):
                    yield _sse_chunk(
                        chat_id=chat_id,
                        model=model,
                        created=created,
                        content=close_tag,
                    )
                # Single terminal finish frame WITH usage (Scheme A). This is the
                # chunk secondary relays like sub2api inspect for token billing.
                yield _sse_chunk(
                    chat_id=chat_id,
                    model=model,
                    created=created,
                    finish_reason=terminal_finish,
                    usage=norm_usage,
                )
                # OpenAI-compatible usage-only fallback (empty choices) for
                # clients that follow stream_options.include_usage strictly.
                yield _sse_chunk(
                    chat_id=chat_id,
                    model=model,
                    created=created,
                    usage=norm_usage,
                    include_choices=False,
                )
                yield "data: [DONE]\n\n"
            _record_usage_safe(
                usage=norm_usage,
                ok=True,
                api_key_id=api_key_id,
                account_id=creds.auth_key,
                model=model,
                protocol="openai",
                stream=True,
            )
            return
        except asyncio.CancelledError:
            return
        except Exception as e:  # noqa: BLE001
            await asyncio.to_thread(
                account_pool.report_failure,
                creds.auth_key,
                error=str(e),
                status_code=502,
                model=model,
            )
            last_err = str(e)
            # Never failover after bytes were already streamed to the client —
            # secondary relays treat that as a mid-stream corruption / break.
            if stream_started or role_sent:
                _record_usage_safe(
                    ok=False,
                    api_key_id=api_key_id,
                    account_id=creds.auth_key,
                    model=model,
                    protocol="openai",
                    stream=True,
                )
                err_payload = {
                    "id": chat_id,
                    "object": "error",
                    "error": {
                        "message": last_err,
                        "type": "proxy_error",
                    },
                }
                yield f"data: {json.dumps(err_payload, ensure_ascii=False)}\n\n"
                yield "data: [DONE]\n\n"
                return
            _record_usage_safe(
                ok=False,
                api_key_id=api_key_id,
                account_id=creds.auth_key,
                model=model,
                protocol="openai",
                stream=True,
            )
            if idx < len(chain) - 1:
                continue
            err_payload = {
                "id": chat_id,
                "object": "error",
                "error": {"message": last_err, "type": "proxy_error"},
            }
            yield f"data: {json.dumps(err_payload, ensure_ascii=False)}\n\n"
            yield "data: [DONE]\n\n"
            return

    err_payload = {
        "id": chat_id,
        "object": "error",
        "error": {
            "message": _sanitize_upstream_error_message(last_err or "", 503) or "All accounts failed",
            "type": "upstream_error",
        },
    }
    yield f"data: {json.dumps(err_payload, ensure_ascii=False)}\n\n"
    yield "data: [DONE]\n\n"


async def _collect_completion(
    *, url: str, headers: dict[str, str], body: dict[str, Any]
) -> tuple[
    str,
    str,
    str | None,
    dict[str, Any] | None,
    list[dict[str, Any]] | None,
]:
    """Consume upstream (usually SSE) and return full text + tool_calls + usage."""
    content_parts: list[str] = []
    reasoning_parts: list[str] = []
    finish: str | None = None
    usage: dict[str, Any] | None = None
    tool_acc: dict[int, dict[str, Any]] = {}
    complete_tool_calls: list[dict[str, Any]] | None = None

    # Ensure stream usage is requested when we force-stream for non-stream clients
    req_body = _body_for_upstream(body)
    _ensure_stream_include_usage(req_body)

    client = await get_http_client()
    async with client.stream("POST", url, headers=headers, json=req_body) as resp:
        if resp.status_code >= 400:
            raw = await resp.aread()
            # attach body text onto response for callers
            try:
                resp._content = raw  # type: ignore[attr-defined]
            except Exception:
                pass
            raise httpx.HTTPStatusError(
                f"Upstream error: {raw.decode('utf-8', errors='replace')[:500]}",
                request=resp.request,
                response=resp,
            )

        ctype = (resp.headers.get("content-type") or "").lower()
        if "text/event-stream" in ctype or "stream" in ctype:
            async for line in resp.aiter_lines():
                parsed = _parse_sse_line(line)
                if parsed is None:
                    continue
                if parsed == "[DONE]":
                    break
                assert isinstance(parsed, dict)
                if isinstance(parsed.get("usage"), dict):
                    usage = parsed["usage"]
                c, r, tc_delta = _extract_delta_parts(parsed)
                if c:
                    content_parts.append(c)
                if r:
                    reasoning_parts.append(r)
                if tc_delta:
                    _merge_tool_call_delta(tool_acc, tc_delta)
                choices = parsed.get("choices")
                if isinstance(choices, list) and choices:
                    fr = choices[0].get("finish_reason")
                    if fr:
                        finish = fr
                    # non-stream-style message embedded in SSE
                    msg = choices[0].get("message") or {}
                    if isinstance(msg.get("tool_calls"), list) and msg["tool_calls"]:
                        complete_tool_calls = [
                            tc
                            for tc in msg["tool_calls"]
                            if isinstance(tc, dict)
                        ]
        else:
            raw = await resp.aread()
            data = json.loads(raw)
            if isinstance(data.get("usage"), dict):
                usage = data["usage"]
            choices = data.get("choices") or []
            if choices:
                msg = (choices[0] or {}).get("message") or {}
                content_parts.append(msg.get("content") or "")
                if msg.get("reasoning_content"):
                    reasoning_parts.append(msg["reasoning_content"])
                if isinstance(msg.get("tool_calls"), list):
                    complete_tool_calls = [
                        tc for tc in msg["tool_calls"] if isinstance(tc, dict)
                    ]
                finish = choices[0].get("finish_reason") or "stop"
            else:
                c, r, tc_delta = _extract_delta_parts(data)
                content_parts.append(c)
                reasoning_parts.append(r)
                if tc_delta:
                    _merge_tool_call_delta(tool_acc, tc_delta)
                finish = "stop"

    tool_calls = complete_tool_calls or _finalize_tool_calls(tool_acc)
    if tool_calls and (not finish or finish == "stop"):
        finish = "tool_calls"
    content = "".join(content_parts)
    reasoning = "".join(reasoning_parts)
    # Always normalize so secondary relays never see missing/zero usage
    usage = _usage_from_body_and_output(
        req_body,
        content=content,
        reasoning=reasoning,
        tool_calls=tool_calls,
        usage=usage,
    )
    return (
        content,
        reasoning,
        finish,
        usage,
        tool_calls,
    )


# ── Anthropic Messages API ──────────────────────────────────────────────────


def _resolve_anthropic_affinity(
    req: anth.AnthropicMessagesRequest, request: Request
) -> tuple[str | None, str | None]:
    """Fingerprint for sticky multi-turn on Anthropic-shaped requests."""
    conv_id = conversation_affinity.extract_conversation_id_from_headers(
        request.headers
    )
    if not conv_id and isinstance(req.metadata, dict):
        for k in ("conversation_id", "session_id", "thread_id"):
            if req.metadata.get(k):
                conv_id = str(req.metadata[k])
                break
    oa_msgs = anth.affinity_messages_from_request(req)
    # Prefer explicit metadata cache/session keys; fall back to cache_control
    # fingerprint derived from system/tools so Claude Code multi-turn sticks.
    pck = conversation_affinity.extract_prompt_cache_key(req)
    if not pck:
        pck = anth.extract_anthropic_prompt_cache_key(req)
    fp = conversation_affinity.conversation_fingerprint(
        oa_msgs,
        user=anth.metadata_user_id(req),
        conversation_id=conv_id,
        prompt_cache_key=pck,
    )
    prefer = conversation_affinity.get_affinity(fp) if fp else None
    return fp, prefer


def _anthropic_error_response(
    message: str,
    status: int = 500,
    err_type: str = "api_error",
    *,
    retry_after: float | int | None = None,
) -> JSONResponse:
    headers = {}
    if retry_after is not None:
        try:
            headers["Retry-After"] = str(max(1, int(float(retry_after))))
        except (TypeError, ValueError):
            headers["Retry-After"] = "5"
    return JSONResponse(
        status_code=status,
        content=anth.anthropic_error(message, status=status, err_type=err_type),
        headers=headers or None,
    )


@app.post("/v1/messages")
@app.post("/messages")
async def anthropic_messages(
    req: anth.AnthropicMessagesRequest,
    request: Request,
    anthropic_version: str | None = Header(default=None, alias="anthropic-version"),
    api_key: apikeys.ApiKeyRecord | None = Depends(require_api_key),
):
    """
    Anthropic Messages API compatible endpoint.
    Auth: `x-api-key` or `Authorization: Bearer …` (same managed keys as OpenAI).
    Optional header: `anthropic-version` (accepted, not enforced).
    """
    _ = anthropic_version  # accepted for client compatibility
    key_id = _api_key_id(api_key)
    if not req.messages:
        return _anthropic_error_response(
            "messages: Field required",
            status=400,
            err_type="invalid_request_error",
        )
    if req.max_tokens is None or req.max_tokens < 1:
        return _anthropic_error_response(
            "max_tokens: Input should be greater than or equal to 1",
            status=400,
            err_type="invalid_request_error",
        )

    timing = RequestTiming(protocol="anthropic", stream=bool(req.stream))
    conv_fp, prefer_account = await asyncio.to_thread(
        _resolve_anthropic_affinity, req, request
    )
    timing.mark_affinity(prefer_account)
    model = resolve_model(req.model)
    timing.model = model
    message_id = f"msg_{uuid.uuid4().hex[:24]}"
    timing.req_id = message_id.replace("msg_", "")[:12]

    def _build_anthropic_body() -> dict[str, Any]:
        body_local = anth.build_openai_chat_body(
            req, model, force_stream=FORCE_UPSTREAM_STREAM
        )
        # Always stream upstream when forced; client may still want non-stream response
        if FORCE_UPSTREAM_STREAM:
            body_local["stream"] = True
        # Anthropic→OpenAI conversion can omit parameters; force the same scrub as
        # the OpenAI path so upstream never sees tools without `parameters`.
        _sanitize_upstream_body(body_local, model=model)
        _ensure_stream_include_usage(body_local)
        # Same long-tool-loop compaction as OpenAI path (sub2api often hits OpenAI
        # chat/completions, but direct Anthropic /v1/messages also benefits).
        _apply_history_compact(body_local)
        return body_local

    try:
        (chain, pick_ms), body = await asyncio.gather(
            asyncio.to_thread(
                _pick_account_chain_timed,
                model=model,
                prefer_account_id=prefer_account,
            ),
            asyncio.to_thread(_build_anthropic_body),
        )
        timing.mark_pick(chain, elapsed_ms=pick_ms)
    except AuthError as e:
        try:
            from store.metrics import inc

            inc("g2a_auth_failures_total")
        except Exception:
            pass
        # Pool empty / temporary — 503 not 401 so clients retry instead of stopping.
        pe = _client_pool_error(e)
        detail = pe.body
        try:
            import json as _json
            detail = _json.loads(pe.body.decode("utf-8")).get("error", {}).get("message") or str(e)
        except Exception:
            detail = str(e)
        return _anthropic_error_response(
            detail, status=503, err_type="api_error", retry_after=8
        )
    _note_request_metrics(prefer_account=prefer_account, conv_fp=conv_fp)
    url = f"{UPSTREAM_BASE}/chat/completions"

    compact_hdr = _history_compact_headers(body)
    if req.stream:
        return StreamingResponse(
            _stream_anthropic_with_failover(
                url=url,
                body=body,
                chain=chain,
                message_id=message_id,
                model=model,
                client_disconnected=request.is_disconnected,
                conversation_fp=conv_fp,
                api_key_id=key_id,
                usage_ctx=_capture_usage_request_ctx(request),
                timing=timing,
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
                "X-Grok2API-Protocol": "anthropic",
                "X-Grok2API-Accounts": str(len(chain)),
                "X-Grok2API-Affinity": "1" if prefer_account else "0",
                **compact_hdr,
                **(
                    {"X-Grok2API-Conversation-Fp": conv_fp}
                    if conv_fp
                    else {}
                ),
            },
        )

    last_error: str | None = None
    last_status = 502
    first_tried: str | None = chain[0].auth_key if chain else None

    for creds in chain:
        headers = upstream_headers(creds.token, model)
        try:
            content, reasoning, finish, usage, tool_calls = await _collect_completion(
                url=url, headers=headers, body=body
            )
            await asyncio.to_thread(account_pool.report_success, creds.auth_key, model=model)
            if conv_fp:
                if prefer_account and prefer_account != creds.auth_key:
                    await asyncio.to_thread(
                        conversation_affinity.rebind_on_failover,
                        conv_fp,
                        first_tried,
                        creds.auth_key,
                    )
                    try:
                        from store.metrics import inc

                        inc("g2a_account_failovers_total")
                    except Exception:
                        pass
                else:
                    await asyncio.to_thread(
                        conversation_affinity.bind_affinity, conv_fp, creds.auth_key
                    )

            result = anth.openai_completion_to_anthropic(
                content=content or "",
                reasoning=reasoning or "",
                finish=finish,
                usage=usage,
                tool_calls=tool_calls,
                model=model,
                message_id=message_id,
            )
            # Normalize Anthropic usage (input/output + cache details) for the ledger.
            au = result.get("usage") if isinstance(result, dict) else None
            ledger_usage = None
            if isinstance(au, dict):
                ledger_usage = _normalize_usage(
                    {
                        "prompt_tokens": au.get("input_tokens") or 0,
                        "completion_tokens": au.get("output_tokens") or 0,
                        "cache_read_input_tokens": au.get("cache_read_input_tokens")
                        or 0,
                        "cache_creation_input_tokens": au.get(
                            "cache_creation_input_tokens"
                        )
                        or 0,
                    }
                )
            else:
                ledger_usage = _usage_from_body_and_output(
                    body,
                    content=content or "",
                    reasoning=reasoning or "",
                    tool_calls=tool_calls,
                    usage=usage,
                )
            _record_usage_safe(
                usage=ledger_usage,
                ok=True,
                api_key_id=key_id,
                account_id=creds.auth_key,
                model=model,
                protocol="anthropic",
                stream=False,
            )
            # non-standard debug fields (ignored by strict SDKs that allow extra)
            result["x_grok2api_account"] = creds.email or creds.auth_key
            result["x_grok2api_affinity"] = bool(prefer_account)
            if conv_fp:
                result["x_grok2api_conversation_fp"] = conv_fp
            return result
        except httpx.HTTPStatusError as e:
            code = e.response.status_code if e.response is not None else 502
            detail = e.response.text[:800] if e.response is not None else str(e)
            hdrs = dict(e.response.headers) if e.response is not None else None
            await asyncio.to_thread(
                account_pool.report_failure,
                creds.auth_key,
                error=detail,
                status_code=code,
                model=model,
                headers=hdrs,
            )
            try:
                from store.metrics import inc

                inc("g2a_upstream_failures_total")
            except Exception:
                pass
            _record_usage_safe(
                ok=False,
                api_key_id=key_id,
                account_id=creds.auth_key,
                model=model,
                protocol="anthropic",
                stream=False,
            )
            last_error = f"Upstream {code}: {detail}"
            last_status = code
            if not _retryable_status(code):
                break
            continue
        except Exception as e:  # noqa: BLE001
            await asyncio.to_thread(
                account_pool.report_failure,
                creds.auth_key,
                error=str(e),
                status_code=502,
                model=model,
            )
            try:
                from store.metrics import inc

                inc("g2a_upstream_failures_total")
            except Exception:
                pass
            _record_usage_safe(
                ok=False,
                api_key_id=key_id,
                account_id=creds.auth_key,
                model=model,
                protocol="anthropic",
                stream=False,
            )
            last_error = f"Proxy error: {e}"
            last_status = 502
            continue

    final_status = last_status if last_status < 600 else 502
    retryable = final_status in (401, 403, 429, 500, 502, 503, 504)
    friendly = _sanitize_upstream_error_message(last_error or "", final_status)
    return _anthropic_error_response(
        friendly or last_error or "All accounts failed",
        status=503 if retryable else final_status,
        err_type="api_error",
        retry_after=8 if retryable else None,
    )


@app.post("/v1/messages/count_tokens", dependencies=[Depends(require_api_key)])
@app.post("/messages/count_tokens", dependencies=[Depends(require_api_key)])
async def anthropic_count_tokens(req: anth.AnthropicMessagesRequest):
    """Approximate token count (local heuristic; no upstream tokenizer)."""
    if not req.messages and req.system is None:
        return _anthropic_error_response(
            "messages or system required",
            status=400,
            err_type="invalid_request_error",
        )
    return anth.count_tokens_for_request(req)


# ── OpenAI Responses API (sub2api Anthropic→OpenAI path) ─────────────────────


def _responses_affinity(
    messages: list[dict[str, Any]], req_body: dict[str, Any], request: Request
) -> tuple[str | None, str | None]:
    conv_id = conversation_affinity.extract_conversation_id_from_headers(
        request.headers
    )
    if not conv_id and isinstance(req_body.get("metadata"), dict):
        meta = req_body["metadata"]
        for k in ("conversation_id", "session_id", "thread_id"):
            if meta.get(k):
                conv_id = str(meta[k])
                break
    if not conv_id and req_body.get("previous_response_id"):
        # Best-effort sticky key when continuing a stored response chain.
        conv_id = f"prev:{req_body.get('previous_response_id')}"
    user = req_body.get("user")
    if not user and isinstance(req_body.get("metadata"), dict):
        user = req_body["metadata"].get("user")
    pck = conversation_affinity.extract_prompt_cache_key(req_body)
    fp = conversation_affinity.conversation_fingerprint(
        messages,
        user=str(user) if user else None,
        conversation_id=conv_id,
        prompt_cache_key=pck,
    )
    prefer = conversation_affinity.get_affinity(fp) if fp else None
    return fp, prefer


@app.post("/v1/responses")
@app.post("/responses")
async def openai_responses(
    request: Request,
    api_key: apikeys.ApiKeyRecord | None = Depends(require_api_key),
):
    """OpenAI Responses API compatibility endpoint.

    sub2api converts Claude Code /v1/messages → Responses and POSTs here when the
    account platform is openai. We translate to chat/completions against Grok
    upstream, then map the completion back to Responses JSON / SSE.
    """
    try:
        req_body = await request.json()
    except Exception:
        return openai_error(
            "Invalid JSON body", status=400, err_type="invalid_request_error"
        )
    if not isinstance(req_body, dict):
        return openai_error(
            "Request body must be a JSON object",
            status=400,
            err_type="invalid_request_error",
        )

    key_id = _api_key_id(api_key)
    model = resolve_model(req_body.get("model"))
    want_stream = bool(req_body.get("stream"))
    response_id = oai_resp.new_response_id()
    created_at = int(time.time())
    timing = RequestTiming(
        protocol="openai_responses",
        model=model,
        stream=want_stream,
        req_id=response_id.replace("resp_", "")[:12],
    )

    body = oai_resp.responses_request_to_chat_body(req_body, model=model)
    if not body.get("messages"):
        timing.emit(ok=False, error="empty input")
        return openai_error(
            "input must contain at least one message",
            status=400,
            err_type="invalid_request_error",
        )

    conv_fp, prefer_account = await asyncio.to_thread(
        _responses_affinity, body.get("messages") or [], req_body, request
    )
    timing.mark_affinity(prefer_account)

    def _prepare_responses_body() -> dict[str, Any]:
        # Force upstream stream collection path (same as chat non-stream clients).
        if FORCE_UPSTREAM_STREAM:
            body["stream"] = True
        _sanitize_upstream_body(body, model=model)
        _ensure_stream_include_usage(body)
        _apply_history_compact(body)
        return body

    try:
        (chain, pick_ms), body = await asyncio.gather(
            asyncio.to_thread(
                _pick_account_chain_timed,
                model=model,
                prefer_account_id=prefer_account,
            ),
            asyncio.to_thread(_prepare_responses_body),
        )
        timing.mark_pick(chain, elapsed_ms=pick_ms)
    except AuthError as e:
        try:
            from store.metrics import inc

            inc("g2a_auth_failures_total")
        except Exception:
            pass
        return _client_pool_error(e)

    _note_request_metrics(prefer_account=prefer_account, conv_fp=conv_fp)
    url = f"{UPSTREAM_BASE}/chat/completions"
    compact_hdr = _history_compact_headers(body)
    prev_id = req_body.get("previous_response_id")
    metadata = req_body.get("metadata") if isinstance(req_body.get("metadata"), dict) else None

    async def _run_with_failover() -> tuple[
        str, str, str | None, dict[str, Any] | None, list[dict[str, Any]] | None, GrokCredentials
    ]:
        last_error: str | None = None
        last_status = 502
        first_tried: str | None = chain[0].auth_key if chain else None
        for creds in chain:
            headers = upstream_headers(creds.token, model)
            try:
                content, reasoning, finish, usage, tool_calls = await _collect_completion(
                    url=url, headers=headers, body=body
                )
                await asyncio.to_thread(
                    account_pool.report_success, creds.auth_key, model=model
                )
                if conv_fp:
                    if prefer_account and prefer_account != creds.auth_key:
                        await asyncio.to_thread(
                            conversation_affinity.rebind_on_failover,
                            conv_fp,
                            first_tried,
                            creds.auth_key,
                        )
                        try:
                            from store.metrics import inc

                            inc("g2a_account_failovers_total")
                        except Exception:
                            pass
                    else:
                        await asyncio.to_thread(
                            conversation_affinity.bind_affinity, conv_fp, creds.auth_key
                        )
                return content, reasoning, finish, usage, tool_calls, creds
            except httpx.HTTPStatusError as e:
                code = e.response.status_code if e.response is not None else 502
                detail = e.response.text[:800] if e.response is not None else str(e)
                hdrs = dict(e.response.headers) if e.response is not None else None
                await asyncio.to_thread(
                    account_pool.report_failure,
                    creds.auth_key,
                    error=detail,
                    status_code=code,
                    model=model,
                    headers=hdrs,
                )
                try:
                    from store.metrics import inc

                    inc("g2a_upstream_failures_total")
                except Exception:
                    pass
                _record_usage_safe(
                    ok=False,
                    api_key_id=key_id,
                    account_id=creds.auth_key,
                    model=model,
                    protocol="openai_responses",
                    stream=want_stream,
                )
                last_error = f"Upstream {code}: {detail}"
                last_status = code
                if not _retryable_status(code):
                    break
                continue
            except Exception as e:  # noqa: BLE001
                await asyncio.to_thread(
                    account_pool.report_failure,
                    creds.auth_key,
                    error=str(e),
                    status_code=502,
                    model=model,
                )
                try:
                    from store.metrics import inc

                    inc("g2a_upstream_failures_total")
                except Exception:
                    pass
                _record_usage_safe(
                    ok=False,
                    api_key_id=key_id,
                    account_id=creds.auth_key,
                    model=model,
                    protocol="openai_responses",
                    stream=want_stream,
                )
                last_error = f"Proxy error: {e}"
                last_status = 502
                continue
        final_status = last_status if last_status < 600 else 502
        friendly = _sanitize_upstream_error_message(last_error or "", final_status)
        raise RuntimeError(friendly or last_error or "All accounts failed")

    if want_stream:
        _resp_usage_ctx = _capture_usage_request_ctx(request)

        async def _sse_gen_live() -> AsyncIterator[str]:
            """True Responses streaming: first token as soon as upstream emits.

            Old path collected the full chat completion then replayed SSE, so
            TTFT equaled full completion latency for every sub2api client.
            """
            _usage_tok = _bind_usage_request_ctx(_resp_usage_ctx)
            last_error: str | None = None
            first_tried: str | None = chain[0].auth_key if chain else None
            try:
                for idx, creds in enumerate(chain):
                    headers = upstream_headers(creds.token, model)
                    streamer = oai_resp.ResponsesLiveStreamer(
                        response_id=response_id,
                        model=model,
                        created_at=created_at,
                        previous_response_id=str(prev_id) if prev_id else None,
                        metadata=metadata,
                    )
                    content_parts: list[str] = []
                    reasoning_parts: list[str] = []
                    tool_acc: dict[int, dict[str, Any]] = {}
                    usage: dict[str, Any] | None = None
                    stream_started = False
                    try:
                        timing.mark_upstream_start(
                            account_id=creds.auth_key, attempt=idx
                        )
                        upstream_body = _body_for_upstream(body)
                        client = await get_http_client()
                        async with client.stream(
                            "POST", url, headers=headers, json=upstream_body
                        ) as resp:
                            timing.mark_upstream_headers()
                            if resp.status_code >= 400:
                                err_text = (await resp.aread()).decode(
                                    "utf-8", errors="replace"
                                )[:1500]
                                await asyncio.to_thread(
                                    account_pool.report_failure,
                                    creds.auth_key,
                                    error=err_text,
                                    status_code=resp.status_code,
                                    model=model,
                                    headers=dict(resp.headers),
                                )
                                _record_usage_safe(
                                    ok=False,
                                    api_key_id=key_id,
                                    account_id=creds.auth_key,
                                    model=model,
                                    protocol="openai_responses",
                                    stream=True,
                                )
                                last_error = (
                                    f"Upstream {resp.status_code}: {err_text}"
                                )
                                if (
                                    _retryable_status(resp.status_code)
                                    and idx < len(chain) - 1
                                    and not stream_started
                                ):
                                    continue
                                for frame in oai_resp.failed_responses_sse(
                                    response_id=response_id,
                                    message=_sanitize_upstream_error_message(
                                        last_error, resp.status_code
                                    )
                                    or last_error,
                                ):
                                    yield frame
                                return

                            # Bookkeeping after upstream accepted — never block TTFT.
                            asyncio.create_task(
                                asyncio.to_thread(
                                    account_pool.report_success,
                                    creds.auth_key,
                                    model=model,
                                )
                            )
                            if conv_fp:
                                if prefer_account and prefer_account != creds.auth_key:
                                    asyncio.create_task(
                                        asyncio.to_thread(
                                            conversation_affinity.rebind_on_failover,
                                            conv_fp,
                                            first_tried,
                                            creds.auth_key,
                                        )
                                    )
                                else:
                                    asyncio.create_task(
                                        asyncio.to_thread(
                                            conversation_affinity.bind_affinity,
                                            conv_fp,
                                            creds.auth_key,
                                        )
                                    )

                            # Emit response.created ASAP (before waiting on first delta).
                            for frame in streamer.start():
                                stream_started = True
                                yield frame

                            ctype = (resp.headers.get("content-type") or "").lower()
                            if "text/event-stream" in ctype or "stream" in ctype:
                                async for line in _aiter_sse_lines_with_keepalive(resp):
                                    if await request.is_disconnected():
                                        return
                                    if line is None:
                                        yield _sse_keepalive()
                                        continue
                                    parsed = _parse_sse_line(line)
                                    if parsed is None:
                                        continue
                                    if parsed == "[DONE]":
                                        break
                                    assert isinstance(parsed, dict)
                                    if isinstance(parsed.get("usage"), dict):
                                        usage = parsed["usage"]
                                    content, reasoning, tool_calls = _extract_delta_parts(
                                        parsed
                                    )
                                    if content:
                                        content_parts.append(content)
                                        timing.mark_first_token(kind="content")
                                        for frame in streamer.on_text_delta(content):
                                            yield frame
                                    if reasoning:
                                        reasoning_parts.append(reasoning)
                                        # Keep reasoning internal; Responses clients
                                        # primarily need output_text / function_call.
                                    if tool_calls:
                                        _merge_tool_call_delta(
                                            tool_acc, tool_calls
                                        )
                                        timing.mark_first_token(kind="tool")
                                        for frame in streamer.on_tool_delta(
                                            tool_calls
                                        ):
                                            yield frame
                            else:
                                # Rare non-SSE upstream response: fall back to one-shot.
                                raw = await resp.aread()
                                data = json.loads(raw)
                                if isinstance(data.get("usage"), dict):
                                    usage = data["usage"]
                                choices = data.get("choices") or []
                                if choices:
                                    msg = (choices[0] or {}).get("message") or {}
                                    c = msg.get("content") or ""
                                    r = msg.get("reasoning_content") or ""
                                    if c:
                                        content_parts.append(c)
                                        for frame in streamer.on_text_delta(c):
                                            yield frame
                                    if r:
                                        reasoning_parts.append(r)
                                    tcs = msg.get("tool_calls")
                                    if isinstance(tcs, list) and tcs:
                                        for frame in streamer.on_tool_delta(
                                            [
                                                {
                                                    "index": i,
                                                    "id": tc.get("id"),
                                                    "type": "function",
                                                    "function": tc.get("function")
                                                    or {},
                                                }
                                                for i, tc in enumerate(tcs)
                                                if isinstance(tc, dict)
                                            ]
                                        ):
                                            yield frame

                            tool_calls_final = _finalize_tool_calls(tool_acc)
                            ledger_usage = _usage_from_body_and_output(
                                body,
                                content="".join(content_parts),
                                reasoning="".join(reasoning_parts),
                                tool_calls=tool_calls_final,
                                usage=usage,
                            )
                            _record_usage_safe(
                                usage=ledger_usage,
                                ok=True,
                                api_key_id=key_id,
                                account_id=creds.auth_key,
                                model=model,
                                protocol="openai_responses",
                                stream=True,
                            )
                            for frame in streamer.complete(
                                usage=ledger_usage,
                                reasoning="".join(reasoning_parts),
                            ):
                                if await request.is_disconnected():
                                    return
                                yield frame
                            return
                    except Exception as e:  # noqa: BLE001
                        if stream_started:
                            for frame in oai_resp.failed_responses_sse(
                                response_id=response_id,
                                message=f"Proxy error: {e}",
                            ):
                                yield frame
                            return
                        await asyncio.to_thread(
                            account_pool.report_failure,
                            creds.auth_key,
                            error=str(e),
                            status_code=502,
                            model=model,
                        )
                        last_error = f"Proxy error: {e}"
                        continue
                for frame in oai_resp.failed_responses_sse(
                    response_id=response_id,
                    message=_sanitize_upstream_error_message(last_error or "", 502)
                    or last_error
                    or "All accounts failed",
                ):
                    yield frame
            finally:
                _reset_usage_request_ctx(_usage_tok)

        return StreamingResponse(
            _sse_gen_live(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
                "X-Grok2API-Protocol": "openai_responses",
                "X-Grok2API-Accounts": str(len(chain)),
                "X-Grok2API-Affinity": "1" if prefer_account else "0",
                **compact_hdr,
                **(
                    {"X-Grok2API-Conversation-Fp": conv_fp}
                    if conv_fp
                    else {}
                ),
            },
        )

    try:
        content, reasoning, _finish, usage, tool_calls, creds = await _run_with_failover()
    except RuntimeError as e:
        msg = str(e)
        # Pool / upstream transient → 503 so relays retry.
        return openai_error(
            msg or "All accounts failed",
            status=503,
            err_type="upstream_error",
            retry_after=8,
            code="all_accounts_failed",
        )
    except Exception as e:  # noqa: BLE001
        return openai_error(
            f"Proxy error: {e}",
            status=502,
            err_type="upstream_error",
        )

    ledger_usage = _usage_from_body_and_output(
        body,
        content=content or "",
        reasoning=reasoning or "",
        tool_calls=tool_calls,
        usage=usage,
    )
    _record_usage_safe(
        usage=ledger_usage,
        ok=True,
        api_key_id=key_id,
        account_id=creds.auth_key,
        model=model,
        protocol="openai_responses",
        stream=False,
    )
    result = oai_resp.build_responses_object(
        response_id=response_id,
        model=model,
        content=content or "",
        reasoning=reasoning or "",
        tool_calls=tool_calls,
        usage=ledger_usage,
        created_at=created_at,
        previous_response_id=str(prev_id) if prev_id else None,
        metadata=metadata,
    )
    result["x_grok2api_account"] = creds.email or creds.auth_key
    result["x_grok2api_affinity"] = bool(prefer_account)
    if conv_fp:
        result["x_grok2api_conversation_fp"] = conv_fp
    hc_stats = body.get("_history_compact") if isinstance(body, dict) else None
    if isinstance(hc_stats, dict):
        result["x_grok2api_history_compact"] = hc_stats
    return result


async def _stream_anthropic_with_failover(
    *,
    url: str,
    body: dict[str, Any],
    chain: list[GrokCredentials],
    message_id: str,
    model: str,
    client_disconnected,
    conversation_fp: str | None = None,
    api_key_id: str | None = None,
    usage_ctx: dict[str, Any] | None = None,
    timing: RequestTiming | None = None,
) -> AsyncIterator[str]:
    """Upstream OpenAI SSE → Anthropic Messages SSE with account failover."""
    _usage_tok = _bind_usage_request_ctx(usage_ctx)
    try:
        async for chunk in _stream_anthropic_with_failover_inner(
            url=url,
            body=body,
            chain=chain,
            message_id=message_id,
            model=model,
            client_disconnected=client_disconnected,
            conversation_fp=conversation_fp,
            api_key_id=api_key_id,
            timing=timing,
        ):
            yield chunk
    finally:
        _reset_usage_request_ctx(_usage_tok)


async def _stream_anthropic_with_failover_inner(
    *,
    url: str,
    body: dict[str, Any],
    chain: list[GrokCredentials],
    message_id: str,
    model: str,
    client_disconnected,
    conversation_fp: str | None = None,
    api_key_id: str | None = None,
    timing: RequestTiming | None = None,
) -> AsyncIterator[str]:
    last_err: str | None = None
    first_tried = chain[0].auth_key if chain else None
    # Estimate prompt tokens for message_start (sub2api reads this early)
    prompt_est = _messages_prompt_estimate(body.get("messages"))
    if body.get("tools"):
        try:
            prompt_est += _estimate_text_tokens(
                json.dumps(body.get("tools"), ensure_ascii=False)
            )
        except (TypeError, ValueError):
            pass

    tools_requested = _body_requests_tools(body)
    upstream_body = _body_for_upstream(body)
    for idx, creds in enumerate(chain):
        headers = upstream_headers(creds.token, model)
        assembler = anth.AnthropicStreamAssembler(
            message_id=message_id,
            model=model,
            tools_requested=tools_requested,
            max_tools=history_compact.OUTBOUND_MAX_TOOLS,
        )
        finished = False
        stream_started = False
        usage: dict[str, Any] | None = None
        held_finish: str | None = None
        try:
            if timing is not None:
                timing.mark_upstream_start(account_id=creds.auth_key, attempt=idx)
            client = await get_http_client()
            async with client.stream(
                "POST", url, headers=headers, json=upstream_body
            ) as resp:
                if timing is not None:
                    timing.mark_upstream_headers()
                if resp.status_code >= 400:
                    err_text = (await resp.aread()).decode(
                        "utf-8", errors="replace"
                    )[:1500]
                    await asyncio.to_thread(
                        account_pool.report_failure,
                        creds.auth_key,
                        error=err_text,
                        status_code=resp.status_code,
                        model=model,
                        headers=dict(resp.headers),
                    )
                    _record_usage_safe(
                        ok=False,
                        api_key_id=api_key_id,
                        account_id=creds.auth_key,
                        model=model,
                        protocol="anthropic",
                        stream=True,
                    )
                    last_err = f"Upstream {resp.status_code}: {err_text}"
                    if _retryable_status(resp.status_code) and idx < len(
                        chain
                    ) - 1:
                        continue
                    if timing is not None:
                        timing.emit(ok=False, error=last_err)
                    yield anth.anthropic_stream_error(
                        last_err, err_type="api_error"
                    )
                    return

                # Don't block first Anthropic event on PG/Redis bookkeeping.
                asyncio.create_task(
                    asyncio.to_thread(
                        account_pool.report_success, creds.auth_key, model=model
                    )
                )
                if conversation_fp:
                    if idx > 0:
                        asyncio.create_task(
                            asyncio.to_thread(
                                conversation_affinity.rebind_on_failover,
                                conversation_fp,
                                first_tried,
                                creds.auth_key,
                            )
                        )
                    else:
                        asyncio.create_task(
                            asyncio.to_thread(
                                conversation_affinity.bind_affinity,
                                conversation_fp,
                                creds.auth_key,
                            )
                        )

                # message_start first — only after upstream accepted
                for ev in assembler.start(input_tokens=prompt_est):
                    yield ev
                stream_started = True

                ctype = (resp.headers.get("content-type") or "").lower()
                client_gone = False
                if "text/event-stream" in ctype or "stream" in ctype:
                    async for line in _aiter_sse_lines_with_keepalive(resp):
                        try:
                            if await client_disconnected():
                                client_gone = True
                        except Exception:
                            client_gone = True
                        if line is None:
                            if not client_gone:
                                yield anth.anthropic_stream_ping()
                            continue
                        parsed = _parse_sse_line(line)
                        if parsed is None:
                            continue
                        if parsed == "[DONE]":
                            break
                        assert isinstance(parsed, dict)
                        if isinstance(parsed.get("usage"), dict):
                            usage = parsed["usage"]
                        content, reasoning, tool_calls = _extract_delta_parts(
                            parsed
                        )
                        finish = None
                        choices = parsed.get("choices")
                        if isinstance(choices, list) and choices:
                            finish = choices[0].get("finish_reason")
                        # usage-only final OpenAI chunk
                        if (
                            not content
                            and not reasoning
                            and not tool_calls
                            and not finish
                            and isinstance(parsed.get("usage"), dict)
                        ):
                            usage = parsed["usage"]
                            continue
                        if content or reasoning or tool_calls:
                            if timing is not None and (
                                content or tool_calls or reasoning
                            ):
                                timing.mark_first_token(
                                    kind=(
                                        "content"
                                        if content
                                        else ("tool" if tool_calls else "reasoning")
                                    )
                                )
                            async for ev in _yield_anthropic_events_serial(
                                assembler.feed(
                                    content=content or None,
                                    reasoning=reasoning or None,
                                    tool_calls=tool_calls,
                                ),
                                client_gone=client_gone,
                            ):
                                yield ev
                        if finish:
                            # Capture finish but keep reading — usage often
                            # arrives on a subsequent empty-choices chunk.
                            finished = True
                            held_finish = finish
                    # Drain complete: now emit terminal events with best usage
                    fr = held_finish or (
                        "tool_calls" if assembler._saw_tool else "stop"
                    )
                    if assembler._saw_tool and fr in (
                        None,
                        "stop",
                        "end_turn",
                        "",
                    ):
                        fr = "tool_calls"
                    async for ev in _yield_anthropic_events_serial(
                        assembler.finish(
                            fr, usage=usage, input_tokens=prompt_est
                        ),
                        client_gone=client_gone,
                    ):
                        yield ev
                    _record_usage_safe(
                        usage=_usage_from_body_and_output(
                            body,
                            usage=usage,
                            content="",
                            reasoning="",
                        )
                        if usage
                        else {
                            "prompt_tokens": prompt_est,
                            "completion_tokens": 0,
                            "total_tokens": prompt_est,
                        },
                        ok=True,
                        api_key_id=api_key_id,
                        account_id=creds.auth_key,
                        model=model,
                        protocol="anthropic",
                        stream=True,
                    )
                    return
                else:
                    raw = await resp.aread()
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        text = raw.decode("utf-8", errors="replace")
                        async for ev in _yield_anthropic_events_serial(
                            assembler.feed(content=text)
                        ):
                            yield ev
                        async for ev in _yield_anthropic_events_serial(
                            assembler.finish(
                                "stop", usage=usage, input_tokens=prompt_est
                            )
                        ):
                            yield ev
                        _record_usage_safe(
                            usage={
                                "prompt_tokens": prompt_est,
                                "completion_tokens": _estimate_text_tokens(text),
                                "total_tokens": prompt_est
                                + _estimate_text_tokens(text),
                            },
                            ok=True,
                            api_key_id=api_key_id,
                            account_id=creds.auth_key,
                            model=model,
                            protocol="anthropic",
                            stream=True,
                        )
                        return
                    else:
                        if isinstance(data.get("usage"), dict):
                            usage = data["usage"]
                        content, reasoning, tool_calls = _extract_delta_parts(
                            data
                        )
                        finish_reason = "stop"
                        choices = data.get("choices") or []
                        if choices:
                            ch0 = choices[0] or {}
                            msg = ch0.get("message") or {}
                            if not content:
                                content = msg.get("content") or ""
                            if not reasoning:
                                reasoning = msg.get("reasoning_content") or ""
                            if not tool_calls and isinstance(
                                msg.get("tool_calls"), list
                            ):
                                tool_calls = msg["tool_calls"]
                            # legacy function_call
                            if not tool_calls and isinstance(
                                msg.get("function_call"), dict
                            ):
                                tool_calls = _legacy_function_call_to_tool_calls(
                                    msg.get("function_call")
                                )
                            finish_reason = (
                                ch0.get("finish_reason") or finish_reason
                            )
                        if content or reasoning or tool_calls:
                            async for ev in _yield_anthropic_events_serial(
                                assembler.feed(
                                    content=content or None,
                                    reasoning=reasoning or None,
                                    tool_calls=tool_calls,
                                )
                            ):
                                yield ev
                        if tool_calls and finish_reason in (
                            None,
                            "stop",
                            "end_turn",
                            "",
                        ):
                            finish_reason = "tool_calls"
                        async for ev in _yield_anthropic_events_serial(
                            assembler.finish(
                                finish_reason,
                                usage=usage,
                                input_tokens=prompt_est,
                            )
                        ):
                            yield ev
                        _record_usage_safe(
                            usage=_usage_from_body_and_output(
                                body,
                                usage=usage,
                                content="",
                                reasoning="",
                            )
                            if usage
                            else {
                                "prompt_tokens": prompt_est,
                                "completion_tokens": 0,
                                "total_tokens": prompt_est,
                            },
                            ok=True,
                            api_key_id=api_key_id,
                            account_id=creds.auth_key,
                            model=model,
                            protocol="anthropic",
                            stream=True,
                        )
                        return

            if not finished:
                async for ev in _yield_anthropic_events_serial(
                    assembler.finish(
                        "tool_calls" if assembler._saw_tool else "stop",
                        usage=usage,
                        input_tokens=prompt_est,
                    )
                ):
                    yield ev
            _record_usage_safe(
                usage=_usage_from_body_and_output(
                    body,
                    usage=usage,
                    content="",
                    reasoning="",
                )
                if usage
                else {
                    "prompt_tokens": prompt_est,
                    "completion_tokens": 0,
                    "total_tokens": prompt_est,
                },
                ok=True,
                api_key_id=api_key_id,
                account_id=creds.auth_key,
                model=model,
                protocol="anthropic",
                stream=True,
            )
            return
        except asyncio.CancelledError:
            return
        except Exception as e:  # noqa: BLE001
            account_pool.report_failure(
                creds.auth_key, error=str(e), status_code=502, model=model
            )
            last_err = str(e)
            _record_usage_safe(
                ok=False,
                api_key_id=api_key_id,
                account_id=creds.auth_key,
                model=model,
                protocol="anthropic",
                stream=True,
            )
            # Mid-stream failures cannot safely failover for secondary relays
            if stream_started:
                yield anth.anthropic_stream_error(
                    last_err or "proxy_error", err_type="api_error"
                )
                return
            if idx < len(chain) - 1:
                continue
            yield anth.anthropic_stream_error(
                last_err or "proxy_error", err_type="api_error"
            )
            return

    yield anth.anthropic_stream_error(
        _sanitize_upstream_error_message(last_err or "", 503) or "All accounts failed", err_type="api_error"
    )


def _static_file_response(rel_path: str):
    """Serve static files safely under multi-worker.

    JS/CSS/dist assets are returned as a single in-memory Response with an exact
    Content-Length and Accept-Ranges disabled. This prevents browsers from
    reporting net::ERR_CONTENT_LENGTH_MISMATCH when a worker recycles mid-download
    or a proxy serves a partial body — which previously left the admin UI stuck
    until a manual hard refresh.
    """
    raw = (rel_path or "").lstrip("/")
    if not raw or ".." in raw.split("/"):
        return JSONResponse({"detail": "not found"}, status_code=404)
    target = (STATIC_DIR / raw).resolve()
    try:
        target.relative_to(STATIC_DIR.resolve())
    except Exception:
        return JSONResponse({"detail": "not found"}, status_code=404)
    if not target.is_file():
        return JSONResponse({"detail": "not found"}, status_code=404)

    suffix = target.suffix.lower()
    media = {
        ".js": "application/javascript; charset=utf-8",
        ".css": "text/css; charset=utf-8",
        ".html": "text/html; charset=utf-8",
        ".json": "application/json; charset=utf-8",
        ".map": "application/json; charset=utf-8",
        ".ico": "image/x-icon",
        ".svg": "image/svg+xml",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
    }.get(suffix, "application/octet-stream")

    # Immutable content-hashed bundles under /static/dist/ can be cached long.
    # Logical /static/js/* kept for compatibility but still no-store to avoid
    # stale partials.
    is_dist = "/dist/" in ("/" + raw.replace("\\", "/")) or raw.startswith("dist/")
    is_text_asset = suffix in {".js", ".css", ".html", ".json", ".map"}

    if is_text_asset:
        data = target.read_bytes()
        if is_dist and suffix in {".js", ".css"}:
            cache = "public, max-age=31536000, immutable"
        else:
            cache = "no-store, no-cache, must-revalidate, max-age=0"
        headers = {
            "Cache-Control": cache,
            "Pragma": "no-cache" if not is_dist else "public",
            "X-Content-Type-Options": "nosniff",
            "Accept-Ranges": "none",
            "Content-Length": str(len(data)),
        }
        if not is_dist:
            headers["Pragma"] = "no-cache"
        else:
            headers.pop("Pragma", None)
        return Response(content=data, media_type=media, headers=headers)

    return FileResponse(
        target,
        media_type=media,
        headers={
            "Cache-Control": "public, max-age=3600, must-revalidate",
            "X-Content-Type-Options": "nosniff",
            "Accept-Ranges": "none",
        },
    )



@app.get("/static/{file_path:path}", include_in_schema=False)
async def static_assets(file_path: str):
    return _static_file_response(file_path)


# Mount static assets if present (css/js under /static) — kept as fallback for tools expecting mount
if STATIC_DIR.is_dir():
    # Prefer explicit route above; mount remains for compatibility when route not matched
    try:
        app.mount("/static-files", StaticFiles(directory=str(STATIC_DIR)), name="static_files")
    except Exception:
        pass


def _pick_listen_host() -> str:
    """Prefer explicit env host; keep loopback accessible via 127.0.0.1."""
    return HOST or "127.0.0.1"


def _detect_public_base_url(port: int) -> str | None:
    """Best-effort public origin when GROK2API_PUBLIC_BASE_URL is unset.

    Uses the host's outbound/default route IP so Docker/server banners show a
    reachable address without hardcoding a domain. Admin/UI still prefer the
    live request Host / X-Forwarded-* headers on each call.
    """
    import socket

    candidates: list[str] = []
    # UDP "connect" does not send packets; it reveals the preferred source IP.
    for family, probe in (
        (socket.AF_INET, ("1.1.1.1", 80)),
        (socket.AF_INET6, ("2606:4700:4700::1111", 80)),
    ):
        try:
            with socket.socket(family, socket.SOCK_DGRAM) as s:
                s.connect(probe)
                ip = s.getsockname()[0]
        except OSError:
            continue
        if not ip or ip.startswith("127.") or ip in ("::1",):
            continue
        # Skip typical Docker/bridge private ranges only when an explicit
        # public-looking address is also available later; still usable fallback.
        candidates.append(ip)

    if not candidates:
        try:
            hostname = socket.gethostname()
            for info in socket.getaddrinfo(hostname, None):
                ip = info[4][0]
                if ip and not ip.startswith("127.") and ip not in ("::1",):
                    candidates.append(ip)
        except OSError:
            pass

    # Prefer global-looking IPv4, then any non-loopback.
    def _score(ip: str) -> tuple[int, int]:
        private = (
            ip.startswith("10.")
            or ip.startswith("192.168.")
            or ip.startswith("172.")
            and any(ip.startswith(f"172.{n}.") for n in range(16, 32))
            or ip.startswith("fc")
            or ip.startswith("fd")
            or ip.startswith("fe80:")
        )
        v4 = 0 if ":" not in ip else 1
        return (1 if private else 0, v4)

    if not candidates:
        return None
    ip = sorted(set(candidates), key=_score)[0]
    host = f"[{ip}]" if ":" in ip else ip
    # Omit default http port for cleaner links.
    if int(port) == 80:
        return f"http://{host}"
    return f"http://{host}:{int(port)}"


def _admin_url(host: str, port: int) -> str:
    # Prefer explicit public URL for server deployments.
    public = (getattr(_config, "PUBLIC_BASE_URL", "") or "").strip().rstrip("/")
    if not public:
        public = _detect_public_base_url(port) or ""
    if public:
        return f"{public}/admin"
    # Local console: use 127.0.0.1 for loopback binds (avoid IPv6 ::1 quirks).
    display = "127.0.0.1" if host in ("0.0.0.0", "::", "127.0.0.1", "localhost") else host
    return f"http://{display}:{port}/admin"


def _open_admin_browser(url: str, delay: float = 1.2) -> None:
    """Open admin UI after server is likely ready (Windows-friendly)."""
    import threading
    import webbrowser

    def _run() -> None:
        import time

        time.sleep(delay)
        try:
            # Prefer os.startfile / default browser on Windows
            if os.name == "nt":
                try:
                    os.startfile(url)  # type: ignore[attr-defined]
                    return
                except OSError:
                    pass
            webbrowser.open(url)
        except Exception as e:  # noqa: BLE001
            print(f"  (could not auto-open browser: {e})")

    threading.Thread(target=_run, daemon=True).start()


def _reload_enabled() -> bool:
    """Dev-only hot reload. Off by default; multi-worker production stays stable."""
    try:
        from config import RELOAD

        return bool(RELOAD)
    except Exception:
        return (os.getenv("GROK2API_RELOAD") or "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )


def _reload_kwargs() -> dict:
    """Build uvicorn reload options (dirs / include / exclude globs)."""
    root = Path(__file__).resolve().parent
    try:
        from config import RELOAD_DIRS, RELOAD_EXCLUDES, RELOAD_INCLUDES
    except Exception:
        RELOAD_DIRS = (os.getenv("GROK2API_RELOAD_DIRS") or "").strip()
        RELOAD_INCLUDES = (os.getenv("GROK2API_RELOAD_INCLUDES") or "").strip()
        RELOAD_EXCLUDES = (os.getenv("GROK2API_RELOAD_EXCLUDES") or "").strip()

    def _split(raw: str) -> list[str]:
        return [p.strip() for p in str(raw or "").split(",") if p.strip()]

    dirs = _split(RELOAD_DIRS)
    if not dirs:
        # Watch code + admin UI sources; skip data/logs/venv noise.
        dirs = [
            str(root),
            str(root / "store"),
            str(root / "static" / "js"),
            str(root / "static" / "admin"),
            str(root / "grok-build-auth"),
        ]
    else:
        resolved = []
        for d in dirs:
            p = Path(d)
            if not p.is_absolute():
                p = root / p
            resolved.append(str(p))
        dirs = resolved

    includes = _split(RELOAD_INCLUDES) or [
        "*.py",
        "*.html",
        "*.js",
        "*.css",
        "*.json",
    ]
    excludes = _split(RELOAD_EXCLUDES) or [
        "*/__pycache__/*",
        "*.pyc",
        "*/.git/*",
        "*/data/*",
        "*/static/dist/*",
        "*/turnstile-solver/logs/*",
        "*/.venv/*",
        "*/venv/*",
    ]
    return {
        "reload": True,
        "reload_dirs": dirs,
        "reload_includes": includes,
        "reload_excludes": excludes,
    }


def main() -> None:
    import socket

    import uvicorn

    from config import WORKERS

    host = _pick_listen_host()
    port = PORT
    reload_on = _reload_enabled()
    # uvicorn cannot combine reload with multi-worker; force 1 worker in dev.
    if reload_on:
        workers = 1
    else:
        workers = max(2, int(WORKERS or 2))  # high-concurrency: never default to 1
    # On Linux servers / headless, don't auto-open browser by default
    default_open = "0" if (os.name != "nt" and not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY")) else "1"
    open_browser = os.getenv("GROK2API_OPEN_BROWSER", default_open) not in (
        "0",
        "false",
        "False",
        "no",
    )

    # High-concurrency mode: Redis + PostgreSQL are mandatory (fail closed).
    try:
        from store import require_high_concurrency_stores

        require_high_concurrency_stores()
    except Exception as e:  # noqa: BLE001
        print(f"ERROR: high-concurrency store check failed: {e}")
        print(
            "  Hint: docker compose --profile store up -d\n"
            "        pip install -r requirements-store.txt\n"
            "        export REDIS_URL=redis://127.0.0.1:6379/0\n"
            "        export DATABASE_URL=postgresql://grok2api:grok2api@127.0.0.1:5432/grok2api\n"
            "        python migrate_json_to_pg.py --data-dir ./data"
        )
        raise SystemExit(2) from e

    # Fixed port bind (no auto-pick) so multi-worker parent shares one listen port.
    if os.getenv("GROK2API_PORT") is None:
        # Still allow override via busy-port only when explicitly single-worker emergency.
        pass

    # Keep admin API status / guide URLs in sync with actual bind
    _config.HOST = host
    _config.PORT = port

    configured_public = (
        getattr(_config, "PUBLIC_BASE_URL", "") or ""
    ).strip().rstrip("/")
    detected_public = None if configured_public else _detect_public_base_url(port)
    public = configured_public or detected_public or ""
    admin = _admin_url(host, port)
    if public:
        link_base = public
    elif host in ("0.0.0.0", "::", "127.0.0.1", "localhost"):
        # Bind-all on a server: print both bind and local loopback convenience links.
        link_base = f"http://127.0.0.1:{port}"
    else:
        link_base = f"http://{host}:{port}"
    print(f"grokcli-2api v{APP_VERSION} listening on http://{host}:{port}")
    print(f"  workers:            {workers}" + (" (forced by reload)" if reload_on else ""))
    print(f"  hot-reload:         {'ON  (dev only)' if reload_on else 'off'}")
    print(f"  OpenAI base_url:    {link_base}/v1")
    print(f"  Anthropic messages: {link_base}/v1/messages")
    print(f"  Admin console:      {admin}")
    print(f"  Docs:               {link_base}/docs")
    print(f"  Health:             {link_base}/health")
    if configured_public:
        print(f"  Public base URL:    {configured_public} (configured)")
    elif detected_public:
        print(f"  Public base URL:    {detected_public} (auto-detected)")
        print("  Admin/API links also follow request Host / X-Forwarded-* headers")
    elif host in ("0.0.0.0", "::"):
        print("  Tip: set GROK2API_PUBLIC_BASE_URL=https://your.domain if auto-detect is wrong")
    print(f"  Upstream:           {UPSTREAM_BASE}")
    if reload_on:
        print("  mode:               dev hot-reload (single worker + file watch)")
        print("  note:               set GROK2API_RELOAD=0 for multi-worker production")
    else:
        print("  mode:               high-concurrency (multi-worker + Redis + PostgreSQL)")
        print("  note:               only leader process runs token/model maintainers")

    run_kwargs: dict = {
        "app": "app:app",
        "host": host,
        "port": port,
        "workers": workers,
        "limit_concurrency": int(os.getenv("GROK2API_LIMIT_CONCURRENCY", "2000") or 2000),
        "timeout_keep_alive": int(os.getenv("GROK2API_KEEPALIVE", "30") or 30),
    }
    if reload_on:
        run_kwargs.update(_reload_kwargs())
        # workers must stay 1 when reload is on
        run_kwargs["workers"] = 1
    else:
        run_kwargs["reload"] = False

    uvicorn.run(**run_kwargs)


if __name__ == "__main__":
    main()
