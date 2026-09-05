from __future__ import annotations

import json
import unittest

import httpx

from grok2api.providers.web.adapter import GrokWebAdapter
from grok2api.providers.web.auth import WebCredential


class _CountingStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.iterations = 0
        self.closed = False

    async def __aiter__(self):
        self.iterations += 1
        yield b"data: one\n\n"
        yield b"data: two\n\n"

    async def aclose(self) -> None:
        self.closed = True


class WebAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_injected_client_receives_headers_and_payload(self) -> None:
        seen: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"ok": True})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            adapter = GrokWebAdapter(client)
            credential = WebCredential(sso="read", sso_rw="write")
            response = await adapter.chat({"message": "hello"}, credential)

        self.assertEqual(response.json(), {"ok": True})
        self.assertEqual(seen[0].url.host, "grok.com")
        self.assertEqual(seen[0].headers["cookie"], "sso=read; sso-rw=write")
        self.assertEqual(json.loads(seen[0].content), {"message": "hello"})

    async def test_stream_is_not_consumed_by_adapter(self) -> None:
        body = _CountingStream()

        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                stream=body,
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            adapter = GrokWebAdapter(client)
            response = await adapter.chat(
                {"message": "hello"},
                WebCredential(sso="read", sso_rw="write"),
                stream=True,
            )
            self.assertEqual(body.iterations, 0)
            chunks = [chunk async for chunk in response.aiter_bytes()]
            self.assertEqual(chunks, [b"data: one\n\n", b"data: two\n\n"])
            await response.aclose()

        self.assertTrue(body.closed)


if __name__ == "__main__":
    unittest.main()
