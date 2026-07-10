"""
Anthropic Messages API compatibility layer for grokcli-2api.

Converts Anthropic `/v1/messages` requests ↔ OpenAI-style upstream bodies
used by cli-chat-proxy, and maps responses / SSE streams back to Anthropic
event shapes so Claude Code, Anthropic SDK, Cursor (Anthropic mode), etc.
can talk to this gateway.
"""

from __future__ import annotations

import json
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


# Anthropic thinking budget → OpenAI reasoning_effort mapping
_THINKING_EFFORT_MAP: dict[str, str] = {
    "low": "low",
    "medium": "medium",
    "high": "high",
}


def _anthropic_thinking_to_reasoning_effort(thinking: Any) -> str | None:
    """
    Convert Anthropic `thinking` field to OpenAI `reasoning_effort`.

    Accepts:
      - {"type": "enabled", "budget_tokens": 1024}
      - {"type": "enabled", "budget_tokens": 32000}
      - true / "enabled"
      - "low" / "medium" / "high"
    """
    if thinking is None:
        return None
    if isinstance(thinking, str):
        return _THINKING_EFFORT_MAP.get(thinking.lower())
    if isinstance(thinking, bool):
        return "medium" if thinking else None
    if isinstance(thinking, dict):
        ttype = (thinking.get("type") or "").lower()
        if ttype not in ("enabled", ""):
            return None
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
        return "high"
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


def anthropic_tools_to_openai(tools: list[Any] | None) -> list[dict[str, Any]] | None:
    if not tools:
        return None
    out: list[dict[str, Any]] = []
    for t in tools:
        if not isinstance(t, dict):
            continue
        # Already OpenAI shape
        if isinstance(t.get("function"), dict):
            out.append(t)
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
        if schema is not None:
            fn["parameters"] = schema
        out.append({"type": "function", "function": fn})
    return out or None


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
) -> tuple[dict[str, Any], bool]:
    """Build OpenAI-compatible chat.completions body for upstream.

    Returns (body, wants_server_search). Server search is triggered when
    grok-search model or web_search tools are requested.
    """
    messages = anthropic_messages_to_openai(req.messages, system=req.system)
    is_search_model = (req.model or "").strip().lower() in ("grok-search", "web-search")

    tools = anthropic_tools_to_openai(req.tools)
    wants_search = is_search_model or _has_web_search_tool(req.tools)

    if is_search_model:
        if not tools:
            tools = [_build_search_function_tool_for_anthropic()]
        elif not any(
            (t.get("function") or {}).get("name") == "web_search"
            for t in tools
            if isinstance(t, dict)
        ):
            tools = tools + [_build_search_function_tool_for_anthropic()]

    body: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "stream": True if force_stream else bool(req.stream),
        "max_tokens": req.max_tokens,
    }
    if tools:
        body["tools"] = tools
    tc = anthropic_tool_choice_to_openai(req.tool_choice)
    if tc is not None:
        body["tool_choice"] = tc
    elif is_search_model:
        body["tool_choice"] = "auto"
    if req.temperature is not None:
        body["temperature"] = req.temperature
    if req.top_p is not None:
        body["top_p"] = req.top_p
    if req.stop_sequences:
        body["stop"] = req.stop_sequences
    if isinstance(req.metadata, dict) and req.metadata.get("user_id"):
        body["user"] = str(req.metadata["user_id"])
    effort = _anthropic_thinking_to_reasoning_effort(req.thinking)
    if effort:
        body["reasoning_effort"] = effort
    return body, wants_search


def _has_web_search_tool(tools: list[Any] | None) -> bool:
    if not tools:
        return False
    for t in tools:
        if not isinstance(t, dict):
            continue
        ttype = (t.get("type") or "").lower()
        if ttype in ("web_search_preview", "web_search", "live_search"):
            return True
        if ttype == "function" and (t.get("function") or {}).get("name") in (
            "web_search", "search", "live_search", "google_search",
        ):
            return True
    return False


def _build_search_function_tool_for_anthropic() -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the web for current information.",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string", "description": "Search query"}},
                "required": ["query"],
            },
        },
    }


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


def _parse_tool_arguments(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return {}
        try:
            parsed = json.loads(s)
            return parsed if isinstance(parsed, dict) else {"value": parsed}
        except json.JSONDecodeError:
            return {"_raw": raw}
    return {"value": raw}


def openai_tool_calls_to_content_blocks(
    tool_calls: list[Any] | None,
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    if not tool_calls:
        return blocks
    for tc in tool_calls:
        if not isinstance(tc, dict):
            continue
        fn = tc.get("function") if isinstance(tc.get("function"), dict) else {}
        name = (fn or {}).get("name") or tc.get("name") or ""
        args_raw = (fn or {}).get("arguments")
        if args_raw is None:
            args_raw = tc.get("arguments") or tc.get("input")
        blocks.append(
            {
                "type": "tool_use",
                "id": tc.get("id") or f"toolu_{uuid.uuid4().hex[:24]}",
                "name": name,
                "input": _parse_tool_arguments(args_raw),
            }
        )
    return blocks


def openai_completion_to_anthropic(
    *,
    content: str,
    reasoning: str = "",
    finish: str | None = None,
    usage: dict[str, Any] | None = None,
    tool_calls: list[Any] | None = None,
    model: str,
    message_id: str | None = None,
) -> dict[str, Any]:
    """Map collected OpenAI-style completion fields to Anthropic message."""
    blocks: list[dict[str, Any]] = []
    if reasoning:
        blocks.append({"type": "thinking", "thinking": reasoning})
    if content:
        blocks.append({"type": "text", "text": content})
    tool_blocks = openai_tool_calls_to_content_blocks(tool_calls)
    blocks.extend(tool_blocks)

    if not blocks:
        blocks.append({"type": "text", "text": ""})

    stop_reason = map_finish_to_stop_reason(finish, has_tool_calls=bool(tool_blocks))

    input_tokens = 0
    output_tokens = 0
    if isinstance(usage, dict):
        input_tokens = int(
            usage.get("prompt_tokens")
            or usage.get("input_tokens")
            or 0
        )
        output_tokens = int(
            usage.get("completion_tokens")
            or usage.get("output_tokens")
            or 0
        )

    return {
        "id": message_id or f"msg_{uuid.uuid4().hex[:24]}",
        "type": "message",
        "role": "assistant",
        "content": blocks,
        "model": model,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
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
) -> str:
    return _sse_event(
        "message_delta",
        {
            "type": "message_delta",
            "delta": {
                "stop_reason": stop_reason,
                "stop_sequence": None,
            },
            "usage": {"output_tokens": output_tokens},
        },
    )


def anthropic_stream_message_stop() -> str:
    return _sse_event("message_stop", {"type": "message_stop"})


def anthropic_stream_error(message: str, err_type: str = "api_error") -> str:
    return _sse_event(
        "error",
        {
            "type": "error",
            "error": {"type": err_type, "message": message},
        },
    )


def anthropic_stream_ping() -> str:
    return _sse_event("ping", {"type": "ping"})


class AnthropicStreamAssembler:
    """
    Stateful converter: OpenAI chat.completion.chunk deltas → Anthropic SSE events.

    Call `feed_delta` for each content/reasoning/tool_calls piece, then `finish`.
    """

    def __init__(self, *, message_id: str, model: str) -> None:
        self.message_id = message_id
        self.model = model
        self._next_index = 0
        self._text_index: int | None = None
        self._thinking_index: int | None = None
        # OpenAI tool call index → (content_block_index, name_emitted, args_buf)
        self._tools: dict[int, dict[str, Any]] = {}
        self._started = False
        self._saw_tool = False
        self._output_chars = 0

    def start(self, input_tokens: int = 0) -> list[str]:
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

        if reasoning:
            if self._thinking_index is None:
                # close text before thinking if any (order: thinking then text usually)
                # Keep open text; Anthropic allows interleaved in theory but
                # typically thinking comes first — if text already open, close it.
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
            events.extend(self._close_thinking())
            if self._text_index is None:
                self._text_index = self._next_index
                self._next_index += 1
                events.append(anthropic_stream_block_start_text(self._text_index))
            events.append(anthropic_stream_text_delta(self._text_index, content))
            self._output_chars += len(content)

        if tool_calls:
            events.extend(self._close_thinking())
            events.extend(self._close_text())
            self._saw_tool = True
            for raw in tool_calls:
                if not isinstance(raw, dict):
                    continue
                try:
                    oi = int(raw.get("index", 0))
                except (TypeError, ValueError):
                    oi = 0
                if oi not in self._tools:
                    tid = raw.get("id") or f"toolu_{uuid.uuid4().hex[:24]}"
                    bi = self._next_index
                    self._next_index += 1
                    self._tools[oi] = {
                        "block_index": bi,
                        "id": tid,
                        "name": "",
                        "args": "",
                        "args_sent": 0,
                        "started": False,
                    }
                state = self._tools[oi]
                fn = raw.get("function") if isinstance(raw.get("function"), dict) else {}
                if raw.get("id"):
                    state["id"] = raw["id"]
                if (fn or {}).get("name"):
                    state["name"] = (state.get("name") or "") + str(fn["name"])
                if raw.get("name"):
                    state["name"] = (state.get("name") or "") + str(raw["name"])

                args_piece = None
                if isinstance(fn, dict) and fn.get("arguments") is not None:
                    args_piece = str(fn["arguments"])
                elif raw.get("arguments") is not None:
                    args_piece = str(raw["arguments"])
                if args_piece:
                    state["args"] += args_piece

                if (
                    not state["started"]
                    and state.get("name")
                    and args_piece is not None
                ):
                    state["started"] = True
                    events.append(
                        anthropic_stream_block_start_tool(
                            state["block_index"],
                            tool_id=state["id"],
                            name=state["name"],
                        )
                    )
                    if state["args"]:
                        events.append(
                            anthropic_stream_input_json_delta(
                                state["block_index"], state["args"]
                            )
                        )
                        state["args_sent"] = len(state["args"])
                        self._output_chars += len(state["args"])
                elif state["started"] and args_piece:
                    events.append(
                        anthropic_stream_input_json_delta(
                            state["block_index"], args_piece
                        )
                    )
                    state["args_sent"] = int(state.get("args_sent") or 0) + len(
                        args_piece
                    )
                    self._output_chars += len(args_piece)

        return events

    def finish(self, finish_reason: str | None = None) -> list[str]:
        events: list[str] = []
        if not self._started:
            events.extend(self.start())
        events.extend(self._close_thinking())
        events.extend(self._close_text())
        for state in self._tools.values():
            if not state.get("started"):
                state["started"] = True
                events.append(
                    anthropic_stream_block_start_tool(
                        state["block_index"],
                        tool_id=state["id"],
                        name=state.get("name") or "tool",
                    )
                )
                if state.get("args"):
                    sent = int(state.get("args_sent") or 0)
                    remaining = state["args"][sent:]
                    if remaining:
                        events.append(
                            anthropic_stream_input_json_delta(
                                state["block_index"], remaining
                            )
                        )
                        self._output_chars += len(remaining)
            if state.get("started"):
                events.append(anthropic_stream_block_stop(state["block_index"]))
        stop = map_finish_to_stop_reason(
            finish_reason, has_tool_calls=self._saw_tool
        )
        # rough output token estimate from chars
        out_tok = max(1, self._output_chars // 4) if self._output_chars else 0
        events.append(
            anthropic_stream_message_delta(
                stop_reason=stop, output_tokens=out_tok
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
