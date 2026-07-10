"""Server-side web search tool for grokcli-2api.

When a client requests `grok-search` model or includes a `web_search` tool,
the gateway intercepts assistant tool_calls, runs DuckDuckGo search, feeds
results back to the model, and returns the final answer.

OpenAI flow:
  user message -> assistant (with tool_calls) -> gateway executes search
  -> tool result -> assistant final answer

Anthropic flow:
  user message -> assistant (with tool_use) -> gateway executes search
  -> tool_result -> assistant final answer
"""
from __future__ import annotations

import asyncio
import json
import re
import time
import urllib.parse
from typing import Any

import httpx

_SEARCH_TIMEOUT = 15.0
_MAX_SNIPPET_LEN = 800
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)


async def _duckduckgo_html(query: str, *, max_results: int = 5) -> list[dict[str, str]]:
    """Scrape DuckDuckGo HTML results. Returns list of {title, snippet, url}."""
    encoded = urllib.parse.quote(query)
    url = f"https://html.duckduckgo.com/html/?q={encoded}"
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(_SEARCH_TIMEOUT, connect=10.0),
            headers={"User-Agent": _USER_AGENT, "Accept-Language": "en-US,en;q=0.9"},
            follow_redirects=True,
        ) as client:
            resp = await client.get(url)
    except Exception as exc:  # noqa: BLE001
        return [{"title": "search error", "snippet": str(exc), "url": ""}]

    text = resp.text
    results: list[dict[str, str]] = []
    # DuckDuckGo HTML result blocks
    for m in re.finditer(
        r'<div class="result results_links[^"]*">(.*?)<div class="result__snippet">(.*?)</div>(.*?)</div>\s*</div>',
        text,
        re.DOTALL | re.IGNORECASE,
    ):
        block = m.group(0)
        title_match = re.search(r'<a[^>]*class="result__a"[^>]*>(.*?)</a>', block, re.DOTALL | re.IGNORECASE)
        title = re.sub(r'<[^>]+>', '', title_match.group(1)) if title_match else ""
        snippet = re.sub(r'<[^>]+>', '', m.group(2))
        url_match = re.search(r'<a[^>]*class="result__url"[^>]*href="([^"]+)"', block, re.IGNORECASE)
        result_url = urllib.parse.unquote(url_match.group(1)) if url_match else ""
        if not title and not snippet:
            continue
        results.append({
            "title": title.strip(),
            "snippet": snippet.strip()[:_MAX_SNIPPET_LEN],
            "url": result_url.strip(),
        })
        if len(results) >= max_results:
            break

    if not results:
        # fallback: extract any result snippets we can find
        for snippet in re.findall(r'<div class="result__snippet">(.*?)</div>', text, re.DOTALL | re.IGNORECASE):
            clean = re.sub(r'<[^>]+>', '', snippet).strip()
            if clean:
                results.append({"title": "", "snippet": clean[:_MAX_SNIPPET_LEN], "url": ""})
            if len(results) >= max_results:
                break
    return results or [{"title": "", "snippet": "未找到搜索结果", "url": ""}]


def _format_search_results(results: list[dict[str, str]]) -> str:
    parts: list[str] = []
    for i, r in enumerate(results, 1):
        line = f"[{i}]"
        if r.get("title"):
            line += f" {r['title']}"
        if r.get("snippet"):
            line += f"\n{r['snippet']}"
        if r.get("url"):
            line += f"\nURL: {r['url']}"
        parts.append(line)
    return "\n\n".join(parts)


def _looks_like_web_search_tool(tool: Any) -> bool:
    if not isinstance(tool, dict):
        return False
    ttype = (tool.get("type") or "").lower()
    if ttype in ("web_search_preview", "web_search", "live_search"):
        return True
    if ttype == "function":
        fn = tool.get("function") or {}
        name = (fn.get("name") or "").lower()
        return name in ("web_search", "search", "live_search", "google_search")
    return False


def _extract_search_query(tool_call: dict[str, Any]) -> str:
    args = tool_call.get("function", {}).get("arguments")
    if isinstance(args, str):
        try:
            args = json.loads(args)
        except Exception:
            args = {}
    if isinstance(args, dict):
        return args.get("query") or args.get("q") or ""
    return ""


def _has_web_search_tool(tools: list[Any] | None) -> bool:
    if not tools:
        return False
    return any(_looks_like_web_search_tool(t) for t in tools)


def _build_search_function_tool() -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the web for current information.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query",
                    }
                },
                "required": ["query"],
            },
        },
    }


def normalize_tools_for_search(tools: list[Any] | None, model: str) -> tuple[list[Any] | None, bool]:
    """
    Returns (normalized_tools, wants_server_search).
    If grok-search/web-search model is used or any web_search tool is present,
    inject a function-style web_search tool.
    """
    is_search_model = model.strip().lower() in ("grok-search", "web-search")
    if not tools and not is_search_model:
        return None, False

    out: list[Any] = []
    has_search = is_search_model
    for t in tools or []:
        if _looks_like_web_search_tool(t):
            has_search = True
        else:
            out.append(t)
    if has_search:
        out.append(_build_search_function_tool())
    return (out or None), has_search


def openai_messages_with_tool_result(
    messages: list[dict[str, Any]],
    assistant_message: dict[str, Any],
    tool_call_id: str,
    result_text: str,
) -> list[dict[str, Any]]:
    """Append assistant tool_call + tool result to OpenAI message list."""
    out = list(messages)
    out.append(assistant_message)
    out.append({
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": result_text,
    })
    return out


def anthropic_messages_with_tool_result(
    messages: list[Any],
    assistant_message: dict[str, Any],
    tool_use_id: str,
    result_text: str,
) -> list[Any]:
    """Append assistant tool_use + tool_result to Anthropic message list."""
    out = list(messages)
    content = assistant_message.get("content") or []
    if isinstance(content, list):
        out.append({"role": "assistant", "content": content})
    else:
        out.append({"role": "assistant", "content": content})
    out.append({
        "role": "user",
        "content": [{
            "type": "tool_result",
            "tool_use_id": tool_use_id,
            "content": result_text,
        }],
    })
    return out


async def execute_search_tool_calls(tool_calls: list[dict[str, Any]]) -> dict[str, str]:
    """Run search for each web_search tool call concurrently."""
    results: dict[str, str] = {}
    queries: list[tuple[str, dict[str, Any]]] = []
    for tc in tool_calls:
        if not isinstance(tc, dict):
            continue
        fn = tc.get("function") or {}
        name = (fn.get("name") or "").lower()
        if name != "web_search":
            continue
        query = _extract_search_query(tc)
        if query:
            queries.append((tc.get("id") or tc.get("tool_use_id") or str(time.time()), query))

    async def _run(qid: str, query: str) -> tuple[str, str]:
        res = await _duckduckgo_html(query)
        return qid, _format_search_results(res)

    for qid, text in await asyncio.gather(*[_run(qid, q) for qid, q in queries], return_exceptions=True):
        if isinstance(text, Exception):
            results[qid] = f"search error: {text}"
        else:
            results[qid] = text
    return results


def extract_openai_tool_calls(message: dict[str, Any]) -> list[dict[str, Any]]:
    tcs = message.get("tool_calls")
    return tcs if isinstance(tcs, list) else []


def extract_anthropic_tool_use(message: dict[str, Any]) -> list[dict[str, Any]]:
    content = message.get("content")
    if not isinstance(content, list):
        return []
    return [c for c in content if isinstance(c, dict) and (c.get("type") or "").lower() == "tool_use"]


def anthropic_tool_use_to_openai(tool_uses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for tu in tool_uses:
        if not isinstance(tu, dict):
            continue
        input_data = tu.get("input") or {}
        out.append({
            "id": tu.get("id") or "",
            "type": "function",
            "function": {
                "name": tu.get("name") or "web_search",
                "arguments": json.dumps(input_data, ensure_ascii=False) if isinstance(input_data, dict) else str(input_data),
            },
        })
    return out
