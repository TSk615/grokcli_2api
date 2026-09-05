"""Offline contract tests for the provider dispatch boundary in app.py.

These tests stop immediately after the Build resolver (or before an optional
provider gateway) so they never contact x.ai and never need real credentials.
"""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from fastapi import Request

import grok2api.app as app
from grok2api.app import ChatCompletionRequest, ChatMessage


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [],
            "query_string": b"",
        }
    )


class _BuildResolverReached(RuntimeError):
    pass


class AppProviderRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_chat_without_model_stays_on_legacy_build_path(self) -> None:
        req = ChatCompletionRequest(messages=[ChatMessage(role="user", content="hi")])
        seen: list[str | None] = []

        def resolve(model: str | None) -> str:
            seen.append(model)
            raise _BuildResolverReached

        with patch.object(app, "resolve_model", side_effect=resolve):
            with self.assertRaises(_BuildResolverReached):
                await app.chat_completions(req, _request(), None)
        self.assertEqual(seen, [None])

    async def test_chat_explicit_build_namespace_is_unqualified_before_legacy_path(self) -> None:
        req = ChatCompletionRequest(
            model="Build/grok-4.5",
            messages=[ChatMessage(role="user", content="hi")],
        )
        seen: list[str | None] = []

        def resolve(model: str | None) -> str:
            seen.append(model)
            raise _BuildResolverReached

        with patch.object(app, "resolve_model", side_effect=resolve):
            with self.assertRaises(_BuildResolverReached):
                await app.chat_completions(req, _request(), None)
        self.assertEqual(seen, ["grok-4.5"])

    async def test_explicit_web_chat_never_enters_build_resolver(self) -> None:
        req = ChatCompletionRequest(
            model="Web/grok-chat-fast",
            messages=[ChatMessage(role="user", content="hi")],
        )
        sentinel = object()
        fake_web = AsyncMock(return_value=sentinel)
        with patch.object(app._config, "WEB_PROVIDER_ENABLED", True), patch.object(
            app, "_web_chat_completions", fake_web
        ), patch.object(app, "resolve_model", side_effect=_BuildResolverReached):
            result = await app.chat_completions(req, _request(), None)
        self.assertIs(result, sentinel)
        fake_web.assert_awaited_once()

    async def test_explicit_console_chat_is_rejected_before_build(self) -> None:
        req = ChatCompletionRequest(
            model="Console/grok-4.5",
            messages=[ChatMessage(role="user", content="hi")],
        )
        with patch.object(app, "resolve_model", side_effect=_BuildResolverReached):
            result = await app.chat_completions(req, _request(), None)
        self.assertEqual(result.status_code, 400)
        self.assertIn("/v1/responses", bytes(result.body).decode())

    async def test_responses_without_model_stays_on_legacy_build_path(self) -> None:
        class _BodyRequest:
            async def json(self):
                return {"input": [{"role": "user", "content": "hi"}]}

        seen: list[str | None] = []

        def resolve(model: str | None) -> str:
            seen.append(model)
            raise _BuildResolverReached

        with patch.object(app, "resolve_model", side_effect=resolve):
            with self.assertRaises(_BuildResolverReached):
                await app.openai_responses(_BodyRequest(), None)
        self.assertEqual(seen, [None])

    async def test_responses_explicit_build_namespace_is_unqualified(self) -> None:
        class _BodyRequest:
            async def json(self):
                return {
                    "model": "Build/grok-4.5",
                    "input": [{"role": "user", "content": "hi"}],
                }

        seen: list[str | None] = []

        def resolve(model: str | None) -> str:
            seen.append(model)
            raise _BuildResolverReached

        with patch.object(app, "resolve_model", side_effect=resolve):
            with self.assertRaises(_BuildResolverReached):
                await app.openai_responses(_BodyRequest(), None)
        self.assertEqual(seen, ["grok-4.5"])

    async def test_responses_web_namespace_is_rejected_before_build(self) -> None:
        class _BodyRequest:
            async def json(self):
                return {"model": "Web/grok-chat-fast", "input": "hi"}

        with patch.object(app, "resolve_model", side_effect=_BuildResolverReached):
            result = await app.openai_responses(_BodyRequest(), None)
        self.assertEqual(result.status_code, 400)
        self.assertIn("chat/completions", bytes(result.body).decode())

    async def test_responses_console_disabled_is_rejected_before_build(self) -> None:
        class _BodyRequest:
            async def json(self):
                return {"model": "Console/grok-4.5", "input": "hi"}

        with patch.object(app._config, "CONSOLE_PROVIDER_ENABLED", False), patch.object(
            app, "resolve_model", side_effect=_BuildResolverReached
        ):
            result = await app.openai_responses(_BodyRequest(), None)
        self.assertEqual(result.status_code, 400)
        self.assertIn("disabled", bytes(result.body).decode())


if __name__ == "__main__":
    unittest.main()
