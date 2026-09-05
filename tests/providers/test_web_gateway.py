from __future__ import annotations

import asyncio
import json
import unittest
from collections.abc import Mapping

import httpx

from grok2api.providers.web.auth import WebCredential
from grok2api.providers.web.gateway import (
    GrokWebGateway,
    WebGatewayAuthError,
    WebGatewayError,
)
from grok2api.providers.web.stream import WebDeltaKind


_USER_ID = "9d7dcf8c-4d51-4c2f-af43-d5f6f16be18a"


class _FakeSocket:
    def __init__(self, frames: list[str | bytes | Exception]) -> None:
        self.frames = list(frames)
        self.sent: list[dict] = []
        self.closed = False

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def recv(self) -> str | bytes:
        if not self.frames:
            await asyncio.Future()
        value = self.frames.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    async def close(self) -> None:
        self.closed = True


def _event(event_type: str, **values) -> str:
    event = {"type": event_type, **values}
    return json.dumps({"session_id": "conversation-1", "event": event})


class WebGatewayTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _credential() -> WebCredential:
        return WebCredential(
            sso="secret-sso",
            sso_rw="secret-rw",
            cloudflare_cookies={"cf_clearance": "secret-cf"},
        )

    @staticmethod
    def _request() -> dict:
        return {
            "model": "grok-chat-expert",
            "stream": True,
            "messages": [{"role": "user", "content": "hello"}],
        }

    async def test_identity_handshake_turn_and_normalized_deltas(self) -> None:
        http_requests: list[httpx.Request] = []

        async def http_handler(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(
                200,
                json={"status": "authenticated", "session": {"userId": _USER_ID}},
            )

        socket = _FakeSocket(
            [
                _event("session.created"),
                _event("conversation.attached", conversation={"id": "conversation-1"}),
                _event("ping"),
                _event(
                    "response.chunk",
                    chunk={"text": {"text": "think ", "channel": "CHANNEL_ANALYSIS"}},
                ),
                _event(
                    "response.chunk",
                    chunk={
                        "text": {
                            "text": "answer ",
                            "channel": "CHANNEL_ASSISTANT_RESPONSE",
                        }
                    },
                ),
                _event(
                    "response.chunk",
                    chunk={
                        "render_citation": {
                            "url": "https://example.com/source",
                            "title": "Source",
                        }
                    },
                ),
                _event("response.output_text.delta", delta="done"),
                _event("response.done", response={"id": "response-1", "status": "completed"}),
            ]
        )
        connect_calls: list[tuple[str, Mapping[str, str], float]] = []

        async def connector(url, headers, timeout):
            connect_calls.append((url, dict(headers), timeout))
            return socket

        async with httpx.AsyncClient(transport=httpx.MockTransport(http_handler)) as client:
            gateway = GrokWebGateway(client, connector=connector, heartbeat_interval=60)
            deltas = [item async for item in gateway.iter_chat(self._request(), self._credential())]

        self.assertEqual(http_requests[0].url.path, "/api/auth/session")
        self.assertIn("sso=secret-sso", http_requests[0].headers["cookie"])
        url, headers, timeout = connect_calls[0]
        self.assertEqual(url, f"wss://grok.com/ws/mgw/?uid={_USER_ID}")
        self.assertGreater(timeout, 0)
        for value in (
            "sso=secret-sso",
            "sso-rw=secret-rw",
            "cf_clearance=secret-cf",
            f"x-userid={_USER_ID}",
        ):
            self.assertIn(value, headers["Cookie"])

        event_types = [item["event"]["type"] for item in socket.sent]
        self.assertEqual(
            event_types,
            ["session.create", "conversation.item.create", "response.create"],
        )
        session = socket.sent[0]["event"]["session"]
        self.assertEqual(session["model"], "expert")
        prompt = socket.sent[1]["event"]["item"]["x_grok"]["input_chunks"][0]
        self.assertEqual(prompt["text"]["text"], "[user]\nhello")
        self.assertTrue(socket.closed)

        self.assertEqual(
            "".join(item.text for item in deltas if item.kind is WebDeltaKind.REASONING),
            "think ",
        )
        self.assertEqual(
            "".join(item.text for item in deltas if item.kind is WebDeltaKind.TEXT),
            "answer done",
        )
        citation = next(item.citation for item in deltas if item.kind is WebDeltaKind.CITATION)
        self.assertEqual(citation.url, "https://example.com/source")

    async def test_auth_and_gateway_errors_do_not_echo_sensitive_context(self) -> None:
        secret = "secret-sso"

        async def blocked_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"status": "blocked", "message": f"sso={secret}"},
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(blocked_handler)) as client:
            gateway = GrokWebGateway(client, connector=lambda *_: None)  # type: ignore[arg-type]
            with self.assertRaises(WebGatewayAuthError) as caught:
                await gateway.fetch_user_id(self._credential())
        self.assertNotIn(secret, str(caught.exception))

        async def ok_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"session": {"userId": _USER_ID}})

        socket = _FakeSocket(
            [
                _event("session.created"),
                _event("conversation.attached", conversation={"id": "conversation-1"}),
                _event("error", error={"message": f"sso={secret}"}),
            ]
        )

        async def connector(*_args):
            return socket

        async with httpx.AsyncClient(transport=httpx.MockTransport(ok_handler)) as client:
            gateway = GrokWebGateway(client, connector=connector)
            with self.assertRaises(WebGatewayError) as caught:
                _ = [item async for item in gateway.iter_chat(self._request(), self._credential())]
        self.assertNotIn(secret, str(caught.exception))
        self.assertTrue(socket.closed)

    async def test_handshake_total_timeout_and_frame_limit(self) -> None:
        async def ok_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"session": {"userId": _USER_ID}})

        async def slow_connector(*_args):
            await asyncio.Future()

        async with httpx.AsyncClient(transport=httpx.MockTransport(ok_handler)) as client:
            gateway = GrokWebGateway(
                client, connector=slow_connector, handshake_timeout=0.01
            )
            with self.assertRaises(WebGatewayError) as caught:
                _ = [item async for item in gateway.iter_chat(self._request(), self._credential())]
            self.assertIn("handshake", str(caught.exception).lower())

        waiting_socket = _FakeSocket([_event("session.created")])

        async def waiting_connector(*_args):
            return waiting_socket

        async with httpx.AsyncClient(transport=httpx.MockTransport(ok_handler)) as client:
            gateway = GrokWebGateway(
                client,
                connector=waiting_connector,
                total_timeout=0.01,
                heartbeat_interval=60,
            )
            with self.assertRaises(WebGatewayError) as caught:
                _ = [item async for item in gateway.iter_chat(self._request(), self._credential())]
            self.assertIn("timeout", str(caught.exception).lower())
        self.assertTrue(waiting_socket.closed)

        large_socket = _FakeSocket(["{" + ("x" * 2048) + "}"])

        async def large_connector(*_args):
            return large_socket

        async with httpx.AsyncClient(transport=httpx.MockTransport(ok_handler)) as client:
            gateway = GrokWebGateway(client, connector=large_connector, max_frame_bytes=1024)
            with self.assertRaises(WebGatewayError) as caught:
                _ = [item async for item in gateway.iter_chat(self._request(), self._credential())]
            self.assertIn("safety limit", str(caught.exception).lower())
        self.assertTrue(large_socket.closed)


if __name__ == "__main__":
    unittest.main()
