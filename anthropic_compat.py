"""
Anthropic Messages API compatibility layer for grokcli-2api.

Converts Anthropic `/v1/messages` requests ↔ OpenAI-style upstream bodies
used by cli-chat-proxy, and maps responses / SSE streams back to Anthropic
event shapes so Claude Code, Anthropic SDK, Cursor (Anthropic mode), etc.
can talk to this gateway.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict

# ── request models ──────────────────────────────────────────────────────────


class AnthropicMessagesRequest(BaseModel):
    """Subset of Anthropic Messages API create params (extra fields allowed)."""

    model_config = ConfigDict(extra="allow")

    model: str | None = None
    messages: list[Any]
    max_tokens: int = 4096
    system: Any | None = None
    stream: bool = False
    temperature: float | None = None
    top_p: float | None = None
    top_k: int | None = None
    stop_sequences: list[str] | None = None
    metadata: dict[str, Any] | None = None
    tools: list[Any] | None = None
    tool_choice: Any | None = None
    # Extended / optional fields clients may send
    thinking: Any | None = None
    container: Any | None = None


# Anthropic thinking budget → OpenAI reasoning_effort mapping.
# Include xhigh so Claude Code / Anthropic clients can request the top Grok tier
# (cli-chat-proxy / grok-4.5 accept reasoning_effort=xhigh).
_THINKING_EFFORT_MAP: dict[str, str] = {
    "low": "low",
    "medium": "medium",
    "high": "high",
    "xhigh": "xhigh",
    "extra_high": "xhigh",
    "extrahigh": "xhigh",
}


def _anthropic_thinking_to_reasoning_effort(thinking: Any) -> str | None:
    """
    Convert Anthropic `thinking` field to OpenAI `reasoning_effort`.

    Accepts:
      - {"type": "enabled", "budget_tokens": 1024}
      - {"type": "enabled", "budget_tokens": 32000}
      - {"type": "enabled", "budget_tokens": 64000}  → xhigh
      - true / "enabled"
      - "low" / "medium" / "high" / "xhigh"
    """
    if thinking is None:
        return None
    if isinstance(thinking, str):
        return _THINKING_EFFORT_MAP.get(thinking.lower().strip())
    if isinstance(thinking, bool):
        return "medium" if thinking else None
    if isinstance(thinking, dict):
        ttype = (thinking.get("type") or "").lower()
        if ttype not in ("enabled", ""):
            return None
        # Prefer explicit effort string when clients send it alongside budget.
        for key in ("effort", "reasoning_effort"):
            raw = thinking.get(key)
            if isinstance(raw, str) and raw.strip():
                mapped = _THINKING_EFFORT_MAP.get(raw.lower().strip())
                if mapped:
                    return mapped
        budget = thinking.get("budget_tokens")
        try:
            budget = int(budget) if budget is not None else None
        except (TypeError, ValueError):
            budget = None
        if budget is None:
            return "medium"
        if budget <= 4096:
            return "low"
        if budget <= 16000:
            return "medium"
        # High-but-not-extreme budgets stay on high; very large budgets map to
        # xhigh so "max thinking" clients actually get the top tier.
        if budget <= 48000:
            return "high"
        return "xhigh"
    return None


# ── content helpers ─────────────────────────────────────────────────────────


def _as_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                btype = (block.get("type") or "").lower()
                if btype in ("text", "input_text", "output_text") and isinstance(
                    block.get("text"), str
                ):
                    parts.append(block["text"])
                elif isinstance(block.get("text"), str):
                    parts.append(block["text"])
                elif btype == "thinking" and isinstance(block.get("thinking"), str):
                    parts.append(block["thinking"])
                elif btype == "tool_result":
                    parts.append(_tool_result_to_text(block))
        return "\n".join(parts)
    if isinstance(content, dict):
        if isinstance(content.get("text"), str):
            return content["text"]
        return json.dumps(content, ensure_ascii=False)
    return str(content)


def _tool_result_to_text(block: dict[str, Any]) -> str:
    c = block.get("content")
    if isinstance(c, str):
        return c
    if isinstance(c, list):
        return _as_text(c)
    if c is None:
        return ""
    try:
        return json.dumps(c, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(c)


def _image_to_openai_part(block: dict[str, Any]) -> dict[str, Any] | None:
    """Anthropic image block → OpenAI image_url content part."""
    source = block.get("source") or {}
    if not isinstance(source, dict):
        return None
    stype = (source.get("type") or "").lower()
    if stype == "base64":
        media = source.get("media_type") or "image/png"
        data = source.get("data") or ""
        return {
            "type": "image_url",
            "image_url": {"url": f"data:{media};base64,{data}"},
        }
    if stype == "url":
        url = source.get("url") or ""
        if url:
            return {"type": "image_url", "image_url": {"url": url}}
    return None


def _user_content_to_openai(content: Any) -> Any:
    """Anthropic user content → OpenAI message content (str | list parts)."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return _as_text(content)

    parts: list[Any] = []
    has_non_text = False
    for block in content:
        if isinstance(block, str):
            parts.append({"type": "text", "text": block})
            continue
        if not isinstance(block, dict):
            continue
        btype = (block.get("type") or "text").lower()
        if btype in ("text", "input_text"):
            parts.append({"type": "text", "text": block.get("text") or ""})
        elif btype == "image":
            img = _image_to_openai_part(block)
            if img:
                has_non_text = True
                parts.append(img)
        elif btype == "tool_result":
            # handled at message-split level; skip here
            continue
        else:
            # document / other: best-effort text
            t = block.get("text") or block.get("title")
            if t:
                parts.append({"type": "text", "text": str(t)})

    if not parts:
        return ""
    if not has_non_text and all(
        isinstance(p, dict) and p.get("type") == "text" for p in parts
    ):
        return "\n".join(str(p.get("text") or "") for p in parts)
    return parts


def anthropic_messages_to_openai(
    messages: list[Any],
    system: Any = None,
) -> list[dict[str, Any]]:
    """
    Convert Anthropic messages (+ optional system) to OpenAI chat messages,
    including tool_use / tool_result round-trips.
    """
    out: list[dict[str, Any]] = []

    # system prompt(s)
    if system is not None:
        if isinstance(system, str) and system.strip():
            out.append({"role": "system", "content": system})
        elif isinstance(system, list):
            text = _as_text(system)
            if text.strip():
                out.append({"role": "system", "content": text})
        elif isinstance(system, dict):
            text = _as_text([system]) if system else ""
            if text.strip():
                out.append({"role": "system", "content": text})

    for raw in messages or []:
        if not isinstance(raw, dict):
            continue
        role = (raw.get("role") or "user").lower()
        content = raw.get("content")

        if role == "user":
            # Split tool_result blocks into OpenAI tool messages
            if isinstance(content, list):
                pending_text_blocks: list[Any] = []
                for block in content:
                    if isinstance(block, dict) and (
                        block.get("type") or ""
                    ).lower() == "tool_result":
                        # flush pending text first as user msg
                        if pending_text_blocks:
                            out.append(
                                {
                                    "role": "user",
                                    "content": _user_content_to_openai(
                                        pending_text_blocks
                                    ),
                                }
                            )
                            pending_text_blocks = []
                        tool_id = (
                            block.get("tool_use_id")
                            or block.get("tool_call_id")
                            or block.get("id")
                            or ""
                        )
                        out.append(
                            {
                                "role": "tool",
                                "tool_call_id": str(tool_id),
                                "content": _tool_result_to_text(block),
                            }
                        )
                    else:
                        pending_text_blocks.append(block)
                if pending_text_blocks:
                    out.append(
                        {
                            "role": "user",
                            "content": _user_content_to_openai(pending_text_blocks),
                        }
                    )
            else:
                out.append(
                    {"role": "user", "content": _user_content_to_openai(content)}
                )

        elif role == "assistant":
            text_parts: list[str] = []
            tool_calls: list[dict[str, Any]] = []
            thinking_parts: list[str] = []

            if isinstance(content, str):
                text_parts.append(content)
            elif isinstance(content, list):
                for block in content:
                    if not isinstance(block, dict):
                        if isinstance(block, str):
                            text_parts.append(block)
                        continue
                    btype = (block.get("type") or "text").lower()
                    if btype in ("text", "output_text"):
                        text_parts.append(block.get("text") or "")
                    elif btype == "thinking":
                        thinking_parts.append(block.get("thinking") or "")
                    elif btype == "tool_use":
                        name = block.get("name") or ""
                        inp = block.get("input")
                        if isinstance(inp, str):
                            args = inp
                        else:
                            try:
                                args = json.dumps(
                                    inp if inp is not None else {},
                                    ensure_ascii=False,
                                )
                            except (TypeError, ValueError):
                                args = "{}"
                        tool_calls.append(
                            {
                                "id": block.get("id")
                                or f"toolu_{uuid.uuid4().hex[:24]}",
                                "type": "function",
                                "function": {
                                    "name": name,
                                    "arguments": args,
                                },
                            }
                        )
            else:
                text_parts.append(_as_text(content))

            msg: dict[str, Any] = {"role": "assistant"}
            joined = "\n".join(p for p in text_parts if p)
            if thinking_parts:
                # upstream OpenAI path uses reasoning_content when present
                msg["reasoning_content"] = "\n".join(thinking_parts)
            if tool_calls:
                msg["tool_calls"] = tool_calls
                msg["content"] = joined if joined else None
            else:
                msg["content"] = joined
            out.append(msg)

        elif role in ("system", "developer"):
            text = _as_text(content)
            if text.strip():
                out.append({"role": "system", "content": text})
        else:
            # unknown role — pass as user text
            out.append({"role": "user", "content": _as_text(content)})

    return out


def _empty_tool_parameters() -> dict[str, Any]:
    return {"type": "object", "properties": {}}


def _ensure_tool_parameters(params: Any) -> dict[str, Any]:
    """Always produce a JSON-schema object for function.parameters."""
    if params is None:
        return _empty_tool_parameters()
    if isinstance(params, str):
        text = params.strip()
        if not text:
            return _empty_tool_parameters()
        try:
            parsed = json.loads(text)
        except Exception:
            return _empty_tool_parameters()
        return _ensure_tool_parameters(parsed)
    if not isinstance(params, dict):
        return _empty_tool_parameters()
    out = dict(params)
    if "type" not in out:
        out["type"] = "object"
    if out.get("type") == "object" and "properties" not in out:
        out["properties"] = {}
    return out


def anthropic_tools_to_openai(tools: list[Any] | None) -> list[dict[str, Any]] | None:
    """Convert Anthropic tools → OpenAI function tools (stable name order).

    Sorting by tool name keeps multi-turn prompt prefixes byte-stable when
    clients reshuffle tools arrays — important for automatic upstream cache.
    """
    if not tools:
        return None
    out: list[dict[str, Any]] = []
    for t in tools:
        if not isinstance(t, dict):
            continue
        # Already OpenAI shape
        if isinstance(t.get("function"), dict):
            fn = dict(t["function"])
            name = fn.get("name") or t.get("name")
            if not name:
                continue
            fn["name"] = name
            raw = (
                fn.get("parameters")
                if fn.get("parameters") is not None
                else fn.get("input_schema")
                if fn.get("input_schema") is not None
                else t.get("parameters")
                if t.get("parameters") is not None
                else t.get("input_schema")
            )
            fn["parameters"] = _ensure_tool_parameters(raw)
            fn.pop("input_schema", None)
            out.append({"type": "function", "function": fn})
            continue
        name = t.get("name")
        if not name:
            continue
        fn: dict[str, Any] = {"name": name}
        if t.get("description") is not None:
            fn["description"] = t["description"]
        schema = (
            t.get("input_schema")
            if t.get("input_schema") is not None
            else t.get("parameters")
        )
        fn["parameters"] = _ensure_tool_parameters(schema)
        out.append({"type": "function", "function": fn})
    if not out:
        return None
    out.sort(
        key=lambda t: str(
            ((t.get("function") or {}) if isinstance(t.get("function"), dict) else {}).get(
                "name"
            )
            or t.get("name")
            or ""
        ).lower()
    )
    return out


def anthropic_tool_choice_to_openai(tool_choice: Any) -> Any:
    if tool_choice is None:
        return None
    if isinstance(tool_choice, str):
        low = tool_choice.lower()
        if low == "any":
            return "required"
        if low in ("auto", "none", "required"):
            return low
        return tool_choice
    if isinstance(tool_choice, dict):
        t = (tool_choice.get("type") or "").lower()
        if t == "auto":
            return "auto"
        if t == "any":
            return "required"
        if t == "none":
            return "none"
        if t == "tool":
            name = tool_choice.get("name") or ""
            return {"type": "function", "function": {"name": name}}
        if t == "function":
            return tool_choice
    return tool_choice


def build_openai_chat_body(
    req: AnthropicMessagesRequest,
    model: str,
    *,
    force_stream: bool = False,
) -> dict[str, Any]:
    """Build OpenAI-compatible chat.completions body for upstream."""
    messages = anthropic_messages_to_openai(req.messages, system=req.system)
    body: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "stream": True if force_stream else bool(req.stream),
        "max_tokens": req.max_tokens,
    }
    tools = anthropic_tools_to_openai(req.tools)
    if tools:
        body["tools"] = tools
        # Never send tool_choice without tools — upstream 400:
        # "A tool_choice was set on the request but no tools were specified."
        tc = anthropic_tool_choice_to_openai(req.tool_choice)
        if tc is not None:
            body["tool_choice"] = tc
    if req.temperature is not None:
        body["temperature"] = req.temperature
    if req.top_p is not None:
        body["top_p"] = req.top_p
    if req.stop_sequences:
        body["stop"] = req.stop_sequences
    # metadata.user_id → OpenAI user (affinity)
    if isinstance(req.metadata, dict) and req.metadata.get("user_id"):
        body["user"] = str(req.metadata["user_id"])
    # Surface Anthropic cache markers as OpenAI-style prompt_cache_key so the
    # shared affinity layer can pin multi-turn chats. Stripped before Grok
    # upstream (unsupported), but kept through body build intentionally.
    pck = extract_anthropic_prompt_cache_key(req)
    if pck:
        body["prompt_cache_key"] = pck
    # Anthropic thinking → OpenAI reasoning_effort
    effort = _anthropic_thinking_to_reasoning_effort(req.thinking)
    if effort:
        body["reasoning_effort"] = effort
    # Request final-chunk usage so secondary relays can bill correctly
    if body.get("stream"):
        opts = body.get("stream_options")
        if not isinstance(opts, dict):
            opts = {}
        else:
            opts = dict(opts)
        opts["include_usage"] = True
        body["stream_options"] = opts
    return body


# ── response mapping ────────────────────────────────────────────────────────


def map_finish_to_stop_reason(
    finish: str | None, has_tool_calls: bool = False
) -> str:
    if has_tool_calls or finish == "tool_calls":
        return "tool_use"
    if not finish or finish == "stop":
        return "end_turn"
    if finish in ("length", "max_tokens"):
        return "max_tokens"
    if finish == "content_filter":
        return "refusal"
    if finish == "stop_sequence":
        return "stop_sequence"
    return "end_turn"


def _tool_arg_value_score(value: Any) -> tuple[int, int, int, int]:
    """Higher score = richer / more usable tool-argument payload.

    Empty strings still count as *present* keys (Edit/Update new_string="" is a
    valid delete). Rank by key count first so path-only partials lose to full
    schemas even when new_string is blank.
    """
    if isinstance(value, dict):
        present = 0
        non_empty = 0
        for v in value.values():
            if v is None:
                continue
            present += 1
            if isinstance(v, str) and not v.strip():
                continue
            if isinstance(v, (list, dict)) and not v:
                continue
            non_empty += 1
        try:
            nbytes = len(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
        except (TypeError, ValueError):
            nbytes = len(str(value))
        # kind, key_count, present_keys, non_empty*1e6+bytes
        return (3, len(value), present, non_empty * 1_000_000 + nbytes)
    if isinstance(value, list):
        try:
            nbytes = len(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
        except (TypeError, ValueError):
            nbytes = len(str(value))
        return (2, len(value), 0, nbytes)
    if value is None:
        return (0, 0, 0, 0)
    text = str(value)
    return (1, 1 if text.strip() else 0, 0, len(text))


def _merge_tool_arg_dicts(values: list[Any]) -> dict[str, Any] | None:
    """Deep-ish merge of successive dict rewrites (later values win).

    Explicit later empty string (Edit/Update new_string="") must overwrite an
    earlier non-empty value when the model rewrites the full object; missing
    keys still do not wipe earlier content.
    """
    dicts = [v for v in values if isinstance(v, dict)]
    if not dicts:
        return None
    merged: dict[str, Any] = {}
    for d in dicts:
        for k, v in d.items():
            if k not in merged:
                merged[k] = v
                continue
            old = merged.get(k)
            if old in (None, "", [], {}):
                merged[k] = v
            elif isinstance(v, str) and not v.strip():
                # Later explicit empty string (delete match).
                merged[k] = v
            elif v in (None, [], {}):
                # Keep earlier non-empty over later null/empty-container.
                continue
            elif isinstance(old, dict) and isinstance(v, dict):
                tmp = dict(old)
                tmp.update(v)
                merged[k] = tmp
            elif isinstance(old, list) and isinstance(v, list) and len(v) >= len(old):
                merged[k] = v
            else:
                # Prefer later value when both set (correction / expansion).
                if v not in (None, "", [], {}):
                    merged[k] = v
    return merged


def sanitize_tool_arguments_json(raw: Any) -> str:
    """
    Normalize tool argument text and recover doubled JSON blobs.

    Secondary relays may emit one chunk containing two complete objects:
    `{"file_path":"a"}{"file_path":"a"}`. Clients that concatenate stream
    pieces then fail required-field validation (Claude Code Read/Write).

    Grok also commonly rewrites tool args mid-stream in a *single* SSE line:
      {"file_path":"/x"}{"file_path":"/x","old_string":"a","new_string":"b"}
    Always keeping the first object drops Update/Edit payloads and makes the
    client call Update with only a path (looks like a random/wrong edit).

    When the input is already a single valid JSON value, return the original
    string unchanged so true OpenAI delta suffixes keep prefix continuity.
    """
    if raw is None:
        return ""
    if isinstance(raw, (dict, list)):
        try:
            return json.dumps(raw, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError):
            return str(raw)
    if not isinstance(raw, str):
        try:
            return json.dumps(raw, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError):
            return str(raw)

    s = raw
    if not s:
        return ""
    try:
        json.loads(s)
        return s
    except json.JSONDecodeError:
        pass

    stripped = s.strip()
    if stripped and stripped != s:
        try:
            json.loads(stripped)
            return stripped
        except json.JSONDecodeError:
            pass

    decoder = json.JSONDecoder()
    src = stripped or s
    idx = 0
    n = len(src)
    values: list[Any] = []
    ends: list[int] = []
    while idx < n:
        while idx < n and src[idx].isspace():
            idx += 1
        if idx >= n:
            break
        try:
            obj, end = decoder.raw_decode(src, idx)
        except json.JSONDecodeError:
            break
        values.append(obj)
        ends.append(end)
        idx = end
    if len(values) < 2:
        return s

    first = values[0]
    first_text = src[: ends[0]].strip()
    if all(v == first for v in values[1:]):
        return first_text

    # Prefer a deep-merge of successive dict rewrites when that yields a richer
    # payload (Update: partial file_path object + full edit object in one chunk).
    merged = _merge_tool_arg_dicts(values)
    candidates: list[tuple[tuple[int, int, int, int], str]] = []
    if merged is not None:
        try:
            merged_text = json.dumps(merged, ensure_ascii=False, separators=(",", ":"))
            candidates.append((_tool_arg_value_score(merged), merged_text))
        except (TypeError, ValueError):
            pass
    for i, v in enumerate(values):
        try:
            if i == 0:
                text = first_text
            else:
                # Reconstruct each complete value slice when possible.
                start = ends[i - 1]
                while start < ends[i] and src[start].isspace():
                    start += 1
                text = src[start : ends[i]].strip()
                # Fallback serialize if slice is awkward.
                if not text:
                    text = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
            candidates.append((_tool_arg_value_score(v), text))
        except (TypeError, ValueError):
            continue
    if not candidates:
        return first_text
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def is_complete_json_text(s: str) -> bool:
    """True when s is one full JSON value (object/array/scalar)."""
    if not s or not str(s).strip():
        return False
    try:
        json.loads(s)
        return True
    except (TypeError, ValueError, json.JSONDecodeError):
        return False


# Claude Code / common agent tools: require these keys before first emission.
# Grok/relays often stream a *syntactically complete* partial object first
# (e.g. Update with only {"file_path":"..."}), then rewrite with the rest.
# Emitting early freezes naive-append clients and makes Update/Edit fail.
_TOOL_REQUIRED_KEYS: dict[str, tuple[str, ...]] = {
    # Claude Code filesystem tools
    "read": ("file_path",),
    "write": ("file_path", "content"),
    "edit": ("file_path", "old_string", "new_string"),
    "update": ("file_path", "old_string", "new_string"),
    # Common aliases / historical names for the same Edit tool.
    "strreplace": ("file_path", "old_string", "new_string"),
    "str_replace": ("file_path", "old_string", "new_string"),
    "stringreplace": ("file_path", "old_string", "new_string"),
    "replace": ("file_path", "old_string", "new_string"),
    "multiedit": ("file_path", "edits"),
    "notebookedit": ("notebook_path", "new_source"),
    # Shell / search
    "bash": ("command",),
    "shell": ("command",),
    "grep": ("pattern",),
    "glob": ("pattern",),
    # Web
    "webfetch": ("url",),
    "websearch": ("query",),
    "web_search": ("query",),
}

# Keys that MAY be empty string when present (still required to exist).
# Claude Code Edit/Update uses new_string="" to delete matched text — treating
# empty as "not ready" held the tool forever and looked like Update broken.
_TOOL_EMPTY_STRING_OK: frozenset[str] = frozenset(
    {
        "new_string",
        "new_source",  # NotebookEdit may clear a cell
        "content",  # Write empty file is valid
    }
)

# Alternate key spellings some models / relays emit. Normalized to the Claude
# Code schema before readiness checks and outbound emission.
_TOOL_ARG_KEY_ALIASES: dict[str, str] = {
    "path": "file_path",
    "filepath": "file_path",
    "file": "file_path",
    "filename": "file_path",
    "oldstring": "old_string",
    "oldstr": "old_string",
    "oldtext": "old_string",
    "old": "old_string",
    "newstring": "new_string",
    "newstr": "new_string",
    "newtext": "new_string",
    "new": "new_string",
    # Write: models often invent "contents" while Claude Code requires "content".
    "contents": "content",
    "filecontent": "content",
    "file_content": "content",
    "filecontents": "content",
    "notebookpath": "notebook_path",
    "notebook": "notebook_path",
    "cmd": "command",
    "shell_command": "command",
    "q": "query",
    "search": "query",
    "search_query": "query",
    "uri": "url",
    "href": "url",
    "regex": "pattern",
    "glob_pattern": "pattern",
}

# Grok / OpenAI-style agents often emit Update / StrReplace while Claude Code
# only registers Edit. Emitting the invented name makes sub2api → Claude Code
# reject the tool ("unknown tool") and the edit never applies — looks like
# "Update still broken". Remap known edit aliases to Edit on the way out.
# NEVER include TaskUpdate / TodoWrite here (different tools).
_EDIT_TOOL_NAME_ALIASES: frozenset[str] = frozenset(
    {
        "update",
        "strreplace",
        "str_replace",
        "stringreplace",
        "string_replace",
        "fileedit",
        "file_edit",
        "replace",
        "strreplaceeditor",
        "str_replace_editor",
        "strreplacebasededittool",
        "str_replace_based_edit_tool",
    }
)

# Agent/task tools that share a suffix with filesystem tools — never remap.
_PROTECTED_TOOL_NAME_KEYS: frozenset[str] = frozenset(
    {
        "taskupdate",
        "taskcreate",
        "taskget",
        "tasklist",
        "taskoutput",
        "taskstop",
        "todowrite",
        "todoread",
    }
)


def _tool_name_key(name: str | None) -> str:
    return re.sub(r"[^a-z0-9_]+", "", (name or "").strip().lower())


def canonical_outbound_tool_name(
    name: str | None,
    *,
    allowed_names: set[str] | list[str] | tuple[str, ...] | None = None,
) -> str:
    """Map model-invented edit tool names to Claude Code's registered name.

    Claude Code → sub2api → grokcli-2api registers ``Edit``. Upstream Grok often
    returns ``Update`` / ``StrReplace`` with the same args. Shipping that name
    makes Claude Code reject the tool_use (unknown tool) even when JSON is valid.

    Rules:
    - TaskUpdate / TodoWrite / etc. never remapped
    - If ``allowed_names`` contains an exact/case match for the raw name, keep it
    - Else if name is an edit alias and ``Edit`` (any case) is allowed → that Edit
    - Else if name is an edit alias and no allow-list → ``Edit``
    - Otherwise keep the original name
    """
    raw = (name or "").strip()
    if not raw:
        return raw
    key = _tool_name_key(raw)
    if not key or key in _PROTECTED_TOOL_NAME_KEYS:
        return raw

    allowed_list = [str(x).strip() for x in (allowed_names or []) if str(x).strip()]
    allowed_by_key: dict[str, str] = {}
    for a in allowed_list:
        ak = _tool_name_key(a)
        if ak and ak not in allowed_by_key:
            allowed_by_key[ak] = a

    # Prefer exact registered spelling when the model already used a known tool.
    if key in allowed_by_key:
        return allowed_by_key[key]

    if key in _EDIT_TOOL_NAME_ALIASES or key == "update":
        if "edit" in allowed_by_key:
            return allowed_by_key["edit"]
        # No allow-list (or Edit not advertised): still normalize for Claude Code.
        if not allowed_by_key or any(
            k in allowed_by_key for k in ("edit", "update", "strreplace", "str_replace")
        ):
            # If only Update was advertised, keep the advertised spelling.
            for alt in ("update", "strreplace", "str_replace", "stringreplace"):
                if alt in allowed_by_key:
                    return allowed_by_key[alt]
            return "Edit"
        return "Edit"

    return raw


def extract_tool_names(tools: Any) -> set[str]:
    """Collect tool names from OpenAI / Anthropic / Responses tool lists."""
    names: set[str] = set()
    if not isinstance(tools, list):
        return names
    for t in tools:
        if not isinstance(t, dict):
            continue
        n = t.get("name")
        if isinstance(n, str) and n.strip():
            names.add(n.strip())
            continue
        fn = t.get("function") if isinstance(t.get("function"), dict) else None
        if fn and isinstance(fn.get("name"), str) and fn["name"].strip():
            names.add(fn["name"].strip())
    return names


def _required_keys_for_tool(name: str | None) -> tuple[str, ...]:
    key = _tool_name_key(name)
    if not key:
        return ()
    # Readiness for Update/StrReplace uses the same keys as Edit.
    if key in _EDIT_TOOL_NAME_ALIASES:
        key = "edit"
    if key in _TOOL_REQUIRED_KEYS:
        return _TOOL_REQUIRED_KEYS[key]
    # Suffix match ONLY on token boundaries (underscore / mcp double-underscore).
    # Plain endswith("update") wrongly maps TaskUpdate → Update and TodoWrite →
    # Write. Those agent tools then wait forever for file_path/old_string and
    # never ship — Claude Code via sub2api shows "task status not updating".
    # mcp__x__Read / company_Update still match via _read / _update.
    for short, req in _TOOL_REQUIRED_KEYS.items():
        if key.endswith(f"_{short}") or key.endswith(f"__{short}"):
            return req
    return ()


def _canonical_tool_arg_key(key: str) -> str:
    raw = str(key or "").strip()
    if not raw:
        return raw
    # Fold separators / camelCase to a stable alnum form for alias lookup.
    # filePath / file_path / File-Path → filepath / file_path / filepath
    folded_keep_us = re.sub(r"[^a-z0-9_]+", "", raw.lower())
    folded_alnum = folded_keep_us.replace("_", "")
    if folded_keep_us in _TOOL_ARG_KEY_ALIASES:
        return _TOOL_ARG_KEY_ALIASES[folded_keep_us]
    if folded_alnum in _TOOL_ARG_KEY_ALIASES:
        return _TOOL_ARG_KEY_ALIASES[folded_alnum]
    return raw


def normalize_tool_argument_keys(obj: Any) -> Any:
    """Rename common alternate tool-arg keys to Claude Code schema names.

    Does not invent values — only remaps keys like path→file_path,
    oldString→old_string. Existing canonical keys win over aliases.
    """
    if not isinstance(obj, dict):
        return obj
    out: dict[str, Any] = {}
    for k, v in obj.items():
        canon = _canonical_tool_arg_key(str(k))
        # Prefer already-canonical / first non-empty value.
        if canon in out:
            old = out.get(canon)
            if old in (None, "", [], {}) and v not in (None, "", [], {}):
                out[canon] = v
            continue
        out[canon] = v
    return out


def normalize_tool_arguments_json(
    raw: Any, *, tool_name: str | None = None
) -> str:
    """Sanitize + alias-normalize tool arguments JSON for readiness/emission."""
    cleaned = sanitize_tool_arguments_json(raw)
    if not cleaned or not str(cleaned).strip():
        return cleaned
    text = str(cleaned).strip()
    if text[0] not in "{[":
        return cleaned
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return cleaned
    if not isinstance(parsed, dict):
        return cleaned
    normalized = normalize_tool_argument_keys(parsed)
    if normalized == parsed:
        return cleaned
    try:
        return json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return cleaned


def is_complete_tool_arguments_json(
    s: str, *, tool_name: str | None = None
) -> bool:
    """True when s is complete tool `function.arguments` for streaming gates.

    OpenAI true-delta streams often emit intermediate JSON *scalars* such as
    `"file_path"` while building `{"file_path":"..."}`. `json.loads` accepts
    those scalars, but emitting them early opens the wrong content_block and
    later yields Claude Code / sub2api: "Content block not found".

    Require a complete JSON *object* or *array* for first emission / readiness.
    Bare `{}` / `[]` are intentionally NOT ready during live streaming: Grok /
    relays sometimes preview an empty object before the real arguments rewrite.
    Emitting `{}` first freezes naive-append clients and can leave Claude Code
    with empty tool input. Empty placeholders only flush on finish/close.

    When ``tool_name`` is known (Update/Edit/Read/…), also require the tool's
    mandatory keys so a partial object like ``{"file_path":"..."}`` is NOT
    treated as ready before old_string/new_string arrive.
    """
    if not s or not str(s).strip():
        return False
    text = normalize_tool_arguments_json(s, tool_name=tool_name)
    if not text or not str(text).strip():
        return False
    text = str(text).strip()
    if text[0] not in "{[":
        return False
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    if not isinstance(parsed, (dict, list)):
        return False
    # Hold empty containers until terminal flush — they are not real payloads.
    if parsed == {} or parsed == []:
        return False
    if isinstance(parsed, dict):
        required = _required_keys_for_tool(tool_name)
        if required:
            for k in required:
                if k not in parsed:
                    return False
                val = parsed.get(k)
                if val is None:
                    return False
                # Empty string is OK for some fields (Edit/Update new_string=""
                # deletes matched text). Path-like keys still require content.
                if isinstance(val, str) and not val.strip():
                    if k not in _TOOL_EMPTY_STRING_OK:
                        return False
                    # still require the key to be a string (already is)
                    continue
                if isinstance(val, (list, dict)) and not val:
                    return False
    return True


def _parse_tool_arguments(raw: Any) -> dict[str, Any]:
    """Parse tool arguments; recover doubled JSON from secondary relays."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return normalize_tool_argument_keys(raw)
    if isinstance(raw, list):
        return {"value": raw}
    if isinstance(raw, str):
        cleaned = normalize_tool_arguments_json(raw)
        if not cleaned:
            return {}
        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, dict):
                return normalize_tool_argument_keys(parsed)
            return {"value": parsed}
        except json.JSONDecodeError:
            return {"_raw": raw}
    return {"value": raw}


def openai_tool_calls_to_content_blocks(
    tool_calls: list[Any] | None,
    *,
    allowed_tool_names: set[str] | list[str] | tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    if not tool_calls:
        return blocks
    for tc in tool_calls:
        if not isinstance(tc, dict):
            continue
        fn = tc.get("function") if isinstance(tc.get("function"), dict) else {}
        name = (fn or {}).get("name") or tc.get("name") or ""
        name = canonical_outbound_tool_name(
            str(name or ""), allowed_names=allowed_tool_names
        )
        args_raw = (fn or {}).get("arguments")
        if args_raw is None:
            args_raw = tc.get("arguments") or tc.get("input")
        # Normalize arg aliases with tool context (contents→content for Write).
        if isinstance(args_raw, str):
            args_raw = normalize_tool_arguments_json(args_raw, tool_name=name)
        blocks.append(
            {
                "type": "tool_use",
                "id": tc.get("id") or f"toolu_{uuid.uuid4().hex[:24]}",
                "name": name,
                "input": _parse_tool_arguments(args_raw),
            }
        )
    return blocks


def _anthropic_cache_tokens(usage: dict[str, Any] | None) -> tuple[int, int]:
    """Return (cache_read, cache_creation) from OpenAI/Anthropic-shaped usage."""
    if not isinstance(usage, dict):
        return 0, 0
    read = 0
    create = 0
    for parent in ("prompt_tokens_details", "input_tokens_details"):
        node = usage.get(parent)
        if isinstance(node, dict):
            try:
                read = max(read, int(node.get("cached_tokens") or 0))
            except (TypeError, ValueError):
                pass
            try:
                create = max(create, int(node.get("cache_creation_tokens") or 0))
            except (TypeError, ValueError):
                pass
    for key in (
        "cache_read_input_tokens",
        "cached_tokens",
        "prompt_cache_hit_tokens",
    ):
        try:
            read = max(read, int(usage.get(key) or 0))
        except (TypeError, ValueError):
            pass
    for key in ("cache_creation_input_tokens", "cache_creation_tokens"):
        try:
            create = max(create, int(usage.get(key) or 0))
        except (TypeError, ValueError):
            pass
    return max(0, read), max(0, create)


def openai_completion_to_anthropic(
    *,
    content: str,
    reasoning: str = "",
    finish: str | None = None,
    usage: dict[str, Any] | None = None,
    tool_calls: list[Any] | None = None,
    model: str,
    message_id: str | None = None,
    allowed_tool_names: set[str] | list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Map collected OpenAI-style completion fields to Anthropic message."""
    blocks: list[dict[str, Any]] = []
    if reasoning:
        blocks.append({"type": "thinking", "thinking": reasoning})
    if content:
        blocks.append({"type": "text", "text": content})
    tool_blocks = openai_tool_calls_to_content_blocks(
        tool_calls, allowed_tool_names=allowed_tool_names
    )
    blocks.extend(tool_blocks)

    if not blocks:
        blocks.append({"type": "text", "text": ""})

    stop_reason = map_finish_to_stop_reason(finish, has_tool_calls=bool(tool_blocks))

    input_tokens = 0
    output_tokens = 0
    if isinstance(usage, dict):
        try:
            input_tokens = int(
                usage.get("prompt_tokens")
                or usage.get("input_tokens")
                or 0
            )
        except (TypeError, ValueError):
            input_tokens = 0
        try:
            output_tokens = int(
                usage.get("completion_tokens")
                or usage.get("output_tokens")
                or 0
            )
        except (TypeError, ValueError):
            output_tokens = 0
        # Some relays only provide total_tokens
        if input_tokens <= 0 and output_tokens <= 0:
            try:
                total_only = int(usage.get("total_tokens") or 0)
            except (TypeError, ValueError):
                total_only = 0
            if total_only > 0:
                # Best-effort split: treat all as input if no completion signal
                input_tokens = total_only

    # Local fallback so secondary relays never show 0/0 usage
    if output_tokens <= 0:
        approx = 0
        if content:
            approx += max(1, (len(content) + 3) // 4)
        if reasoning:
            approx += max(1, (len(reasoning) + 3) // 4)
        if tool_blocks:
            try:
                approx += max(
                    1, (len(json.dumps(tool_blocks, ensure_ascii=False)) + 3) // 4
                )
            except (TypeError, ValueError):
                pass
        output_tokens = approx

    cache_read, cache_creation = _anthropic_cache_tokens(usage)

    return {
        "id": message_id or f"msg_{uuid.uuid4().hex[:24]}",
        "type": "message",
        "role": "assistant",
        "content": blocks,
        "model": model,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": {
            "input_tokens": int(input_tokens),
            "output_tokens": int(output_tokens),
            # Always present so clients can distinguish "0 hit" from "unsupported".
            "cache_creation_input_tokens": int(cache_creation),
            "cache_read_input_tokens": int(cache_read),
        },
    }


def anthropic_error(
    message: str,
    *,
    status: int = 500,
    err_type: str = "api_error",
) -> dict[str, Any]:
    """Anthropic-style error body (use with JSONResponse)."""
    # Map HTTP status → Anthropic error type when not specified carefully
    if status == 401:
        err_type = "authentication_error"
    elif status == 403:
        err_type = "permission_error"
    elif status == 404:
        err_type = "not_found_error"
    elif status == 429:
        err_type = "rate_limit_error"
    elif status == 400:
        err_type = "invalid_request_error"
    elif status >= 500 and err_type == "api_error":
        err_type = "api_error"
    return {
        "type": "error",
        "error": {
            "type": err_type,
            "message": message,
        },
    }


def estimate_tokens(text: str) -> int:
    """Rough token estimate (~4 chars/token) for count_tokens stub."""
    if not text:
        return 0
    return max(1, (len(text) + 3) // 4)


def count_tokens_for_request(req: AnthropicMessagesRequest) -> dict[str, Any]:
    """Approximate input token count (no upstream tokenizer available)."""
    total = 0
    if req.system is not None:
        total += estimate_tokens(_as_text(req.system))
    for m in req.messages or []:
        if isinstance(m, dict):
            total += estimate_tokens(_as_text(m.get("content")))
            # tool_use names etc.
            content = m.get("content")
            if isinstance(content, list):
                for b in content:
                    if isinstance(b, dict) and b.get("type") == "tool_use":
                        total += estimate_tokens(str(b.get("name") or ""))
                        total += estimate_tokens(
                            json.dumps(b.get("input") or {}, ensure_ascii=False)
                        )
    if req.tools:
        for t in req.tools:
            if isinstance(t, dict):
                total += estimate_tokens(str(t.get("name") or ""))
                total += estimate_tokens(str(t.get("description") or ""))
                schema = t.get("input_schema") or t.get("parameters") or {}
                try:
                    total += estimate_tokens(json.dumps(schema, ensure_ascii=False))
                except (TypeError, ValueError):
                    pass
    return {"input_tokens": total}


# ── SSE stream helpers ──────────────────────────────────────────────────────


def _sse_event(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def anthropic_stream_message_start(
    *, message_id: str, model: str, input_tokens: int = 0
) -> str:
    return _sse_event(
        "message_start",
        {
            "type": "message_start",
            "message": {
                "id": message_id,
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": model,
                "stop_reason": None,
                "stop_sequence": None,
                "usage": {
                    "input_tokens": input_tokens,
                    "output_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0,
                },
            },
        },
    )


def anthropic_stream_block_start_text(index: int) -> str:
    return _sse_event(
        "content_block_start",
        {
            "type": "content_block_start",
            "index": index,
            "content_block": {"type": "text", "text": ""},
        },
    )


def anthropic_stream_block_start_thinking(index: int) -> str:
    return _sse_event(
        "content_block_start",
        {
            "type": "content_block_start",
            "index": index,
            "content_block": {"type": "thinking", "thinking": ""},
        },
    )


def anthropic_stream_block_start_tool(
    index: int, *, tool_id: str, name: str
) -> str:
    return _sse_event(
        "content_block_start",
        {
            "type": "content_block_start",
            "index": index,
            "content_block": {
                "type": "tool_use",
                "id": tool_id,
                "name": name,
                "input": {},
            },
        },
    )


def anthropic_stream_text_delta(index: int, text: str) -> str:
    return _sse_event(
        "content_block_delta",
        {
            "type": "content_block_delta",
            "index": index,
            "delta": {"type": "text_delta", "text": text},
        },
    )


def anthropic_stream_thinking_delta(index: int, text: str) -> str:
    return _sse_event(
        "content_block_delta",
        {
            "type": "content_block_delta",
            "index": index,
            "delta": {"type": "thinking_delta", "thinking": text},
        },
    )


def anthropic_stream_input_json_delta(index: int, partial_json: str) -> str:
    return _sse_event(
        "content_block_delta",
        {
            "type": "content_block_delta",
            "index": index,
            "delta": {"type": "input_json_delta", "partial_json": partial_json},
        },
    )


def anthropic_stream_block_stop(index: int) -> str:
    return _sse_event(
        "content_block_stop",
        {"type": "content_block_stop", "index": index},
    )


def anthropic_stream_message_delta(
    *,
    stop_reason: str,
    output_tokens: int = 0,
    input_tokens: int | None = None,
    cache_read_input_tokens: int | None = None,
    cache_creation_input_tokens: int | None = None,
) -> str:
    usage: dict[str, Any] = {"output_tokens": int(output_tokens or 0)}
    # Some secondary relays (sub2api) also read input_tokens from message_delta
    if input_tokens is not None:
        usage["input_tokens"] = int(input_tokens or 0)
    # Always emit cache fields when known (including explicit 0).
    if cache_read_input_tokens is not None:
        usage["cache_read_input_tokens"] = int(cache_read_input_tokens or 0)
    if cache_creation_input_tokens is not None:
        usage["cache_creation_input_tokens"] = int(cache_creation_input_tokens or 0)
    return _sse_event(
        "message_delta",
        {
            "type": "message_delta",
            "delta": {
                "stop_reason": stop_reason,
                "stop_sequence": None,
            },
            "usage": usage,
        },
    )


def anthropic_stream_message_stop() -> str:
    return _sse_event("message_stop", {"type": "message_stop"})


def anthropic_stream_error(message: str, err_type: str = "api_error") -> str:
    """Emit Anthropic error event.

    Prefer :func:`anthropic_stream_terminal_error` at the end of a stream so
    clients also receive ``message_stop`` (sub2api treats a bare error without
    a terminal stop as ``missing terminal event`` / hard disconnect).
    """
    return _sse_event(
        "error",
        {
            "type": "error",
            "error": {"type": err_type, "message": message},
        },
    )


def anthropic_stream_terminal_error(
    message: str, err_type: str = "api_error"
) -> list[str]:
    """Error + terminal envelope so secondary relays close cleanly.

    sub2api / Claude Code need a full stop sequence to update task state:
    optional message_delta(stop_reason) + message_stop. A bare error without
    those leaves the agent hanging ("task status not updating").
    """
    return [
        anthropic_stream_error(message, err_type=err_type),
        # stop_reason=null is invalid for some converters; use end_turn so the
        # client can mark the turn finished (failed) rather than stuck running.
        anthropic_stream_message_delta(
            stop_reason="end_turn",
            output_tokens=0,
        ),
        anthropic_stream_message_stop(),
    ]


def anthropic_stream_ping() -> str:
    return _sse_event("ping", {"type": "ping"})



def merge_tool_argument_delta(
    current: str,
    incoming: str,
    *,
    tool_name: str | None = None,
) -> str:
    """
    Merge tool argument stream pieces (delta or cumulative re-send).

    Secondary relays may re-broadcast the full arguments JSON; naive append
    corrupts Claude Code tools (Read requires file_path, etc.).

    Incomplete buffer + later complete non-prefix rewrite is common
    (`{"file_path":` then `{"file_path" : "/x"}`). Prefer the complete value
    instead of concatenating into invalid JSON.

    For object rewrites with different key sets (Update: first only file_path,
    later file_path+old_string+new_string), prefer the richer later object and
    deep-merge dict keys so partial previews don't win forever.
    """
    # Sanitize doubled blobs first, then alias-normalize keys so readiness checks
    # see file_path/old_string even when the model sent path/oldString.
    cur = (
        normalize_tool_arguments_json(current, tool_name=tool_name)
        if current
        else ""
    )
    piece = (
        normalize_tool_arguments_json(incoming, tool_name=tool_name)
        if incoming
        else ""
    )
    if not piece:
        return cur
    if not cur:
        return piece
    if piece == cur:
        return cur
    if piece.startswith(cur):
        return piece
    if cur.startswith(piece):
        return cur

    # Prefer object/array completeness for tool args. Intermediate scalars such
    # as `"file_path"` are complete JSON but not complete tool arguments.
    cur_complete = is_complete_tool_arguments_json(cur, tool_name=tool_name)
    piece_complete = is_complete_tool_arguments_json(piece, tool_name=tool_name)
    cur_any = is_complete_json_text(cur)
    piece_any = is_complete_json_text(piece)

    # Incomplete → complete rewrite (spacing / key order / full resend).
    if piece_complete and not cur_complete:
        return piece
    # Complete object/array → refuse trailing junk or second incomplete fragment.
    if cur_complete and not piece_complete:
        return cur
    # If cur is only a scalar fragment and piece continues the real object,
    # fall through to append / structural merge below.
    if cur_any and not cur_complete and piece_any and not piece_complete:
        # both scalar-ish complete JSON fragments — usually not a rewrite
        pass

    try:
        a = json.loads(cur)
        b = json.loads(piece)
        if a == b:
            return cur
        if isinstance(a, dict) and isinstance(b, dict):
            # Field growth / partial rewrite: merge keys.
            # Example: {"file_path":"x"} + {"file_path":"x","old_string":"a",...}
            # Explicit later empty string is kept (Edit/Update new_string="" delete).
            merged = dict(a)
            for k, v in b.items():
                if k not in merged:
                    merged[k] = v
                    continue
                old = merged.get(k)
                if old in (None, "", [], {}):
                    merged[k] = v
                elif isinstance(v, str) and not v.strip():
                    merged[k] = v
                elif isinstance(old, dict) and isinstance(v, dict):
                    tmp = dict(old)
                    tmp.update(v)
                    merged[k] = tmp
                elif isinstance(old, list) and isinstance(v, list) and len(v) >= len(old):
                    merged[k] = v
                else:
                    # Prefer later value when both set (correction / expansion).
                    merged[k] = v
            try:
                return json.dumps(merged, ensure_ascii=False, separators=(",", ":"))
            except (TypeError, ValueError):
                return piece if len(b) >= len(a) else cur
        if isinstance(a, list) and isinstance(b, list):
            # Prefer later / longer list.
            return piece if len(b) >= len(a) else cur
        if isinstance(a, (dict, list)) and not isinstance(b, (dict, list)):
            return cur
        if isinstance(b, (dict, list)) and not isinstance(a, (dict, list)):
            return piece
    except (TypeError, ValueError, json.JSONDecodeError):
        pass
    # Both incomplete / non-JSON: only append when it looks like a true delta.
    return cur + piece


class AnthropicStreamAssembler:
    """
    Stateful converter: OpenAI chat.completion.chunk deltas → Anthropic SSE events.

    Call `feed_delta` for each content/reasoning/tool_calls piece, then `finish`.
    """

    def __init__(
        self,
        *,
        message_id: str,
        model: str,
        tools_requested: bool = False,
        max_tools: int | None = None,
        allowed_tool_names: set[str] | list[str] | tuple[str, ...] | None = None,
    ) -> None:
        self.message_id = message_id
        self.model = model
        self._next_index = 0
        self._text_index: int | None = None
        self._thinking_index: int | None = None
        # OpenAI tool call index → (content_block_index, name_emitted, args_buf)
        self._tools: dict[int, dict[str, Any]] = {}
        self._started = False
        self._saw_tool = False  # True once a tool_use block is started outbound
        self._tools_pending = False  # upstream tool deltas seen, may be incomplete
        self._tools_requested = bool(tools_requested)
        # Client-registered tool names (Claude Code Edit, …). Used to remap
        # model-invented Update/StrReplace → Edit on outbound emission.
        self._allowed_tool_names: set[str] = set(allowed_tool_names or ())
        # Held (content, reasoning) pairs while waiting to learn if tools win.
        self._held_pre_tool: list[tuple[str | None, str | None]] = []
        self._output_chars = 0
        # Cap outbound tool_use blocks (sub2api single-active-block safety).
        # None → read history_compact.OUTBOUND_MAX_TOOLS at first use.
        self._max_tools = max_tools
        self._tools_started_count = 0

    def _tool_budget_left(self) -> int | None:
        """How many more tool_use blocks may start. None = unlimited."""
        max_tools = self._max_tools
        if max_tools is None:
            try:
                import history_compact

                max_tools = int(getattr(history_compact, "OUTBOUND_MAX_TOOLS", 1) or 0)
            except Exception:
                max_tools = 1
            self._max_tools = max_tools
        if max_tools is None or int(max_tools) <= 0:
            return None
        return max(0, int(max_tools) - self._tools_started_count)

    def start(self, input_tokens: int = 0) -> list[str]:
        # Idempotent: early-open paths (v1.9.72+) may call start at upstream 200
        # and again from feed()/finish() guards.
        if self._started:
            return []
        self._started = True
        return [
            anthropic_stream_message_start(
                message_id=self.message_id,
                model=self.model,
                input_tokens=input_tokens,
            )
        ]

    def _close_text(self) -> list[str]:
        events: list[str] = []
        if self._text_index is not None:
            events.append(anthropic_stream_block_stop(self._text_index))
            self._text_index = None
        return events

    def _close_thinking(self) -> list[str]:
        events: list[str] = []
        if self._thinking_index is not None:
            events.append(anthropic_stream_block_stop(self._thinking_index))
            self._thinking_index = None
        return events

    @staticmethod
    def _merge_name(current: str, incoming: str) -> str:
        """Avoid `web_searchweb_search` when proxies re-send full names."""
        cur = (current or "").strip()
        name = (incoming or "").strip()
        if not name:
            return cur
        if not cur:
            return name
        if name == cur or cur.startswith(name):
            return cur
        if name.startswith(cur):
            return name
        return name

    @staticmethod
    def _coerce_args_piece(raw: Any, *, tool_name: str | None = None) -> str:
        if raw is None:
            return ""
        return normalize_tool_arguments_json(raw, tool_name=tool_name)

    def _flush_tool_args(self, state: dict[str, Any]) -> list[str]:
        """Emit any not-yet-sent tool args (complete preferred; raw fallback)."""
        events: list[str] = []
        if not state.get("started"):
            return events
        args = state.get("args") or ""
        sent_text = state.get("args_sent_text") or ""
        if not args:
            return events
        if sent_text and not args.startswith(sent_text):
            return events
        remaining = args[len(sent_text) :]
        if not remaining:
            return events
        # Prefer holding incomplete live fragments; only force-send when closing.
        if (
            not is_complete_tool_arguments_json(
                args, tool_name=state.get("name") or ""
            )
            and not state.get("_closing")
        ):
            return events
        events.append(
            anthropic_stream_input_json_delta(state["block_index"], remaining)
        )
        state["args_sent_text"] = sent_text + remaining
        state["args_sent"] = len(state["args_sent_text"])
        self._output_chars += len(remaining)
        return events

    def _close_tools(self) -> list[str]:
        """Stop all open tool_use blocks (flush args first).

        Never invent empty ``{}`` for known schema tools that still lack
        required keys — that freezes Claude Code (Update/Edit with no body).
        Incomplete open tools still get content_block_stop so the client can
        leave the block, but without a fake empty input payload.
        """
        events: list[str] = []
        # Close in ascending content_block index order (not OpenAI tool index).
        open_states = [
            state
            for state in self._tools.values()
            if state.get("started") and not state.get("stopped")
        ]
        open_states.sort(
            key=lambda s: (
                s["block_index"]
                if isinstance(s.get("block_index"), int)
                else 10**9
            )
        )
        for state in open_states:
            if state.get("block_index") is None:
                state["block_index"] = self._next_index
                self._next_index += 1
            state["_closing"] = True
            events.extend(self._flush_tool_args(state))
            sent = (state.get("args_sent_text") or "").strip()
            name = (state.get("name") or "").strip()
            if not sent:
                # Only invent "{}" for unknown / free-form tools. Known schema
                # tools (Update/Edit/…) must not ship empty input — Claude Code
                # then marks the tool failed and task status stops advancing.
                allow_empty = True
                try:
                    required = _required_keys_for_tool(name)
                    if required:
                        allow_empty = False
                except Exception:
                    allow_empty = True
                if allow_empty:
                    events.append(
                        anthropic_stream_input_json_delta(
                            state["block_index"], "{}"
                        )
                    )
                    state["args"] = state.get("args") or "{}"
                    state["args_sent_text"] = "{}"
                    state["args_sent"] = 2
                    self._output_chars += 2
            events.append(anthropic_stream_block_stop(state["block_index"]))
            state["stopped"] = True
            state.pop("_closing", None)
        return events

    def _emit_text_and_thinking(
        self, content: str | None, reasoning: str | None
    ) -> list[str]:
        """Open/continue thinking then text blocks (never across open tools)."""
        events: list[str] = []
        if reasoning:
            # Never leave tool_use open across thinking/text — converters and
            # Claude Code expect stop before a new block type.
            events.extend(self._close_tools())
            if self._thinking_index is None:
                events.extend(self._close_text())
                self._thinking_index = self._next_index
                self._next_index += 1
                events.append(
                    anthropic_stream_block_start_thinking(self._thinking_index)
                )
            events.append(
                anthropic_stream_thinking_delta(self._thinking_index, reasoning)
            )
            self._output_chars += len(reasoning)

        if content:
            events.extend(self._close_tools())
            events.extend(self._close_thinking())
            if self._text_index is None:
                self._text_index = self._next_index
                self._next_index += 1
                events.append(anthropic_stream_block_start_text(self._text_index))
            events.append(anthropic_stream_text_delta(self._text_index, content))
            self._output_chars += len(content)
        return events

    def feed(
        self,
        *,
        content: str | None = None,
        reasoning: str | None = None,
        tool_calls: list[Any] | None = None,
    ) -> list[str]:
        events: list[str] = []
        if not self._started:
            events.extend(self.start())

        # When the client requested tools, hold thinking/text until we know
        # whether tools win. Opening thinking as content_block 0 then later
        # tool_use at index 1 is valid Anthropic, but some Claude Code /
        # secondary paths still expect tools-first on tool turns and surface
        # "Content block not found" when mixed.
        if self._tools_requested and not self._saw_tool:
            if content or reasoning:
                self._held_pre_tool.append((content, reasoning))
                # Still count toward output estimate for usage fallbacks.
                if content:
                    self._output_chars += len(content)
                if reasoning:
                    self._output_chars += len(reasoning)
                content, reasoning = None, None
        elif self._tools_requested and self._saw_tool:
            # Tools already outbound: never reopen thinking/text mid-tool turn.
            content, reasoning = None, None
        else:
            events.extend(self._emit_text_and_thinking(content, reasoning))
            content, reasoning = None, None

        if tool_calls:
            self._tools_pending = True
            # Do NOT clear held preface yet — incomplete tool previews must not
            # permanently discard a potential non-tool text answer. Preface is
            # dropped only when a tool_use block actually starts outbound below,
            # or when finish() confirms tools won.
            events.extend(self._close_thinking())
            events.extend(self._close_text())
            for raw in tool_calls:
                if not isinstance(raw, dict):
                    continue
                try:
                    oi = int(raw.get("index", 0))
                except (TypeError, ValueError):
                    oi = 0
                if oi not in self._tools:
                    # IMPORTANT: do NOT assign content_block index here.
                    # Name-only / incomplete args must not reserve an index —
                    # otherwise a later text/thinking block takes a higher index
                    # and finish() starts the tool at a lower index (out of order
                    # → secondary relays / Claude Code: "Content block not found").
                    tid = raw.get("id") or f"toolu_{uuid.uuid4().hex[:24]}"
                    self._tools[oi] = {
                        "block_index": None,
                        "id": tid,
                        "name": "",
                        "args": "",
                        "args_sent": 0,
                        "started": False,
                        "stopped": False,
                    }
                state = self._tools[oi]
                # A closed tool index must not be revived mid-stream (would reuse
                # a stopped content_block index → "Content block not found").
                if state.get("stopped"):
                    continue
                fn = raw.get("function") if isinstance(raw.get("function"), dict) else {}
                # Keep tool id stable once set (tool_result matching depends on it)
                if raw.get("id") and not state.get("id"):
                    state["id"] = raw["id"]
                elif raw.get("id") and not str(state.get("id") or "").startswith("toolu_"):
                    # already have a real id — ignore later rewrites
                    pass
                elif raw.get("id") and str(state.get("id") or "").startswith("toolu_"):
                    # upgrade synthetic id to real upstream id
                    state["id"] = raw["id"]

                if (fn or {}).get("name"):
                    state["name"] = self._merge_name(
                        state.get("name") or "", str(fn["name"])
                    )
                if raw.get("name"):
                    state["name"] = self._merge_name(
                        state.get("name") or "", str(raw["name"])
                    )
                # Remap Update/StrReplace → Edit (Claude Code) as soon as we know
                # the name, so readiness + outbound use the registered tool.
                if state.get("name"):
                    state["name"] = canonical_outbound_tool_name(
                        state.get("name") or "",
                        allowed_names=self._allowed_tool_names,
                    )

                args_piece = None
                _tn = state.get("name") or ""
                if isinstance(fn, dict) and fn.get("arguments") is not None:
                    args_piece = self._coerce_args_piece(
                        fn.get("arguments"), tool_name=_tn
                    )
                elif raw.get("arguments") is not None:
                    args_piece = self._coerce_args_piece(
                        raw.get("arguments"), tool_name=_tn
                    )
                elif raw.get("input") is not None:
                    args_piece = self._coerce_args_piece(
                        raw.get("input"), tool_name=_tn
                    )
                if args_piece:
                    # Merge delta OR full re-send (double-proxy safe)
                    state["args"] = merge_tool_argument_delta(
                        state.get("args") or "",
                        args_piece,
                        tool_name=state.get("name") or "",
                    )

            # Start / flush in ascending OpenAI tool index order. If a lower *known*
            # tool is not ready, hold higher tools. Sparse missing lower indices
            # are holes — we still assign dense content_block indices via
            # _next_index, so converters never see block 1 without block 0.
            #
            # Claude Code / sub2api often keep only one content_block "active".
            # Opening tool_use 1 while tool_use 0 is still open yields intermittent
            # "Content block not found" / "content block not found". Emit tools
            # strictly one-at-a-time: start → args → stop, then the next tool.
            for oi in sorted(self._tools.keys()):
                state = self._tools[oi]
                if state.get("stopped"):
                    continue
                args_now = state.get("args") or ""
                ready = bool(
                    state.get("name")
                    and args_now
                    and is_complete_tool_arguments_json(
                        args_now, tool_name=state.get("name") or ""
                    )
                )
                # Open tool_use only when name is known AND args are complete JSON
                # (or finish() will open). Avoids empty tool blocks that secondary
                # relays close early, then fail on later input_json_delta.
                if not state["started"]:
                    if not ready:
                        # Known lower tool still buffering — do not start higher ones.
                        if state.get("name") or state.get("id"):
                            break
                        continue
                    blocked = False
                    for lower_oi in range(0, oi):
                        lower = self._tools.get(lower_oi)
                        if lower is None:
                            continue  # sparse hole
                        if lower.get("stopped"):
                            continue
                        # Any still-open lower tool must finish first (sequential).
                        if lower.get("started") and not lower.get("stopped"):
                            blocked = True
                            break
                        if not lower.get("started"):
                            # Only block on known lower tools (name/id/args).
                            if (
                                lower.get("name")
                                or lower.get("id")
                                or (lower.get("args") or "").strip()
                            ):
                                blocked = True
                                break
                    if blocked:
                        break
                    # Also block if ANY earlier-started tool is still open, even if
                    # its OpenAI index is higher (shouldn't happen with dense order,
                    # but keep the single-active-block invariant hard).
                    if any(
                        s.get("started") and not s.get("stopped")
                        for s in self._tools.values()
                    ):
                        break
                    budget = self._tool_budget_left()
                    if budget is not None and budget <= 0:
                        # Cap reached — leave remaining tools unstarted this turn.
                        break
                    state["block_index"] = self._next_index
                    self._next_index += 1
                    state["started"] = True
                    self._tools_started_count += 1
                    self._saw_tool = True
                    # Tools confirmed on the wire — drop held thinking/text preface.
                    if self._held_pre_tool:
                        self._held_pre_tool.clear()
                    events.append(
                        anthropic_stream_block_start_tool(
                            state["block_index"],
                            tool_id=state["id"],
                            name=state["name"],
                        )
                    )
                # Hold incomplete fragments. Emit only complete JSON (or a pure
                # suffix after a prior complete send). Incomplete live pieces +
                # later full rewrites corrupt naive-append clients (Read.file_path).
                if state["started"] and not state.get("stopped"):
                    events.extend(self._flush_tool_args(state))
                    # Sequential close: once this tool has a complete live payload
                    # on the wire, stop it before opening the next tool block.
                    sent = state.get("args_sent_text") or ""
                    args_now = state.get("args") or ""
                    if (
                        sent
                        and args_now
                        and sent == args_now
                        and is_complete_tool_arguments_json(
                            args_now, tool_name=state.get("name") or ""
                        )
                    ):
                        events.append(
                            anthropic_stream_block_stop(state["block_index"])
                        )
                        state["stopped"] = True

        return events

    def finish(
        self,
        finish_reason: str | None = None,
        *,
        usage: dict[str, Any] | None = None,
        input_tokens: int | None = None,
    ) -> list[str]:
        events: list[str] = []
        if not self._started:
            events.extend(self.start(input_tokens=int(input_tokens or 0)))

        # Decide whether tools won before opening text/thinking preface.
        # Only drop preface when a tool is truly ready or already on the wire.
        # Mere _tools_pending (name-only / incomplete previews) must not discard
        # a real text answer — that also avoids opening a ghost tool_use block
        # that Claude Code later fails to find.
        has_ready_tool = any(
            (s.get("name") or "").strip()
            and (s.get("args") or "").strip()
            and is_complete_tool_arguments_json(
                s.get("args") or "", tool_name=s.get("name") or ""
            )
            and not s.get("stopped")
            for s in self._tools.values()
        )
        has_any_tool_identity = any(
            (s.get("name") or s.get("id") or (s.get("args") or "").strip())
            and not s.get("stopped")
            for s in self._tools.values()
        )
        if self._saw_tool or has_ready_tool:
            # Real tools path: drop preface so first block can be tool_use.
            self._held_pre_tool.clear()
        elif self._held_pre_tool:
            # Incomplete tool previews only — keep the text answer.
            for held_c, held_r in self._held_pre_tool:
                events.extend(self._emit_text_and_thinking(held_c, held_r))
            self._held_pre_tool.clear()
        # else: no preface; finish may still open known incomplete tools below

        events.extend(self._close_thinking())
        events.extend(self._close_text())
        # Open any buffered tools that never became "started", then close each.
        # CRITICAL: only open tools with *complete* args (or already started).
        # Opening incomplete Update/Edit with "{}" freezes Claude Code task state
        # (tool_use received, execution fails / hangs, status never advances).
        for oi in sorted(self._tools.keys()):
            state = self._tools[oi]
            if state.get("stopped"):
                continue
            name = (state.get("name") or "").strip()
            if name:
                name = canonical_outbound_tool_name(
                    name, allowed_names=self._allowed_tool_names
                )
                state["name"] = name
            args = (state.get("args") or "").strip()
            if args and name:
                try:
                    args = normalize_tool_arguments_json(args, tool_name=name)
                    state["args"] = args
                except Exception:
                    pass
            # Skip pure ghost previews (no name, no args).
            if not state.get("started") and not name and not args:
                continue
            if not state.get("started"):
                # Incomplete tools never go on the wire.
                if not (
                    name
                    and args
                    and is_complete_tool_arguments_json(args, tool_name=name)
                ):
                    continue
                # If we still hold a non-tool preface and somehow no ready tool,
                # prefer the text answer (already handled above) — skip.
                if self._held_pre_tool and not self._saw_tool and not has_ready_tool:
                    continue
                budget = self._tool_budget_left()
                if budget is not None and budget <= 0:
                    continue
                # Close any still-open prior tool before opening this one.
                events.extend(self._close_tools())
                if state.get("block_index") is None:
                    state["block_index"] = self._next_index
                    self._next_index += 1
                state["started"] = True
                self._tools_started_count += 1
                self._saw_tool = True
                if self._held_pre_tool:
                    self._held_pre_tool.clear()
                events.append(
                    anthropic_stream_block_start_tool(
                        state["block_index"],
                        tool_id=state["id"],
                        name=name or "tool",
                    )
                )
            # Close this tool (flush remaining args) before the next.
            events.extend(self._close_tools())
        events.extend(self._close_tools())
        # Upstream often finishes with stop even when tools were emitted.
        effective_finish = finish_reason
        if self._saw_tool and effective_finish in (None, "stop", "end_turn", ""):
            effective_finish = "tool_calls"
        # If we never shipped a tool or text, still close with end_turn so
        # Claude Code can leave "running" (empty answer is better than hang).
        stop = map_finish_to_stop_reason(
            effective_finish, has_tool_calls=self._saw_tool
        )

        # Prefer real upstream usage (OpenAI prompt/completion tokens)
        out_tok = 0
        in_tok = int(input_tokens or 0)
        cache_read, cache_creation = _anthropic_cache_tokens(usage)
        if isinstance(usage, dict):
            try:
                out_tok = int(
                    usage.get("completion_tokens")
                    or usage.get("output_tokens")
                    or 0
                )
            except (TypeError, ValueError):
                out_tok = 0
            try:
                prompt = int(
                    usage.get("prompt_tokens")
                    or usage.get("input_tokens")
                    or 0
                )
            except (TypeError, ValueError):
                prompt = 0
            if prompt > 0:
                in_tok = prompt
        # Fallback: rough estimate from streamed chars when upstream omitted usage
        if out_tok <= 0:
            out_tok = (
                max(1, self._output_chars // 4) if self._output_chars else 0
            )

        events.append(
            anthropic_stream_message_delta(
                stop_reason=stop,
                output_tokens=out_tok,
                input_tokens=in_tok if in_tok > 0 else None,
                # Always include (0 ok) so sub2api can see cache support shape.
                cache_read_input_tokens=cache_read,
                cache_creation_input_tokens=cache_creation,
            )
        )
        events.append(anthropic_stream_message_stop())
        return events


# ── affinity helpers ────────────────────────────────────────────────────────


def affinity_messages_from_request(
    req: AnthropicMessagesRequest,
) -> list[dict[str, Any]]:
    """OpenAI-shaped messages suitable for conversation_affinity fingerprint."""
    return anthropic_messages_to_openai(req.messages, system=req.system)


def metadata_user_id(req: AnthropicMessagesRequest) -> str | None:
    if isinstance(req.metadata, dict):
        uid = req.metadata.get("user_id")
        if uid:
            return str(uid)
    return None


def _cache_control_fingerprint_piece(block: Any) -> str | None:
    """Stable, short marker from an Anthropic content block's cache_control."""
    if not isinstance(block, dict):
        return None
    cc = block.get("cache_control")
    if cc is None:
        return None
    if isinstance(cc, bool):
        return "cc:1" if cc else None
    if isinstance(cc, str) and cc.strip():
        return f"cc:{cc.strip()[:40]}"
    if isinstance(cc, dict):
        ctype = str(cc.get("type") or "ephemeral").strip() or "ephemeral"
        ttl = cc.get("ttl")
        if ttl is not None and str(ttl).strip():
            return f"cc:{ctype}:{str(ttl).strip()[:24]}"
        return f"cc:{ctype}"
    return "cc:1"


def extract_anthropic_prompt_cache_key(
    req: AnthropicMessagesRequest | dict[str, Any] | None,
) -> str | None:
    """Derive a sticky cache key from Anthropic cache_control / metadata.

    Grok upstream does not honor Anthropic prompt caching, but Claude Code
    multi-turn still benefits from sticky account routing keyed on the same
    system/tool cache breakpoints clients mark.
    """
    if req is None:
        return None

    def _meta(obj: Any) -> dict[str, Any] | None:
        if isinstance(obj, dict):
            m = obj.get("metadata")
            return m if isinstance(m, dict) else None
        m = getattr(obj, "metadata", None)
        return m if isinstance(m, dict) else None

    meta = _meta(req)
    if meta:
        for key in (
            "prompt_cache_key",
            "promptCacheKey",
            "cache_key",
            "cacheKey",
            "session_id",
            "sessionId",
            "user_id",
        ):
            v = meta.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()[:240]

    pieces: list[str] = []

    system = req.get("system") if isinstance(req, dict) else getattr(req, "system", None)
    if isinstance(system, list):
        for block in system:
            mark = _cache_control_fingerprint_piece(block)
            if mark:
                text = ""
                if isinstance(block, dict):
                    text = str(block.get("text") or "")[:120]
                pieces.append(f"sys:{mark}:{text}")
    elif isinstance(system, dict):
        mark = _cache_control_fingerprint_piece(system)
        if mark:
            pieces.append(f"sys:{mark}:{str(system.get('text') or '')[:120]}")

    tools = req.get("tools") if isinstance(req, dict) else getattr(req, "tools", None)
    if isinstance(tools, list):
        for t in tools:
            if not isinstance(t, dict):
                continue
            mark = _cache_control_fingerprint_piece(t)
            if not mark:
                continue
            name = str(t.get("name") or "")[:80]
            pieces.append(f"tool:{mark}:{name}")

    messages = (
        req.get("messages") if isinstance(req, dict) else getattr(req, "messages", None)
    )
    if isinstance(messages, list):
        for m in messages[:4]:
            if not isinstance(m, dict):
                continue
            content = m.get("content")
            if not isinstance(content, list):
                continue
            for block in content:
                mark = _cache_control_fingerprint_piece(block)
                if not mark:
                    continue
                btype = ""
                if isinstance(block, dict):
                    btype = str(block.get("type") or "")[:32]
                pieces.append(f"msg:{mark}:{btype}")
                if len(pieces) >= 12:
                    break
            if len(pieces) >= 12:
                break

    if not pieces:
        return None
    # Compact deterministic key (not a cryptographic cache, just sticky routing).
    digest = hashlib.sha256("|".join(pieces).encode("utf-8")).hexdigest()[:32]
    return f"acc:{digest}"
