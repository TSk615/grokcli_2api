from __future__ import annotations

import base64
import json
import unittest
from datetime import datetime, timedelta, timezone

import httpx

from grok2api.providers.console.auth import ConsoleCredential
from grok2api.providers.console.client import ConsoleDPoPClient
from grok2api.providers.console.dpop import jwk_thumbprint
from grok2api.providers.console.responses import ConsoleResponsesError, ConsoleResponsesTransport
from grok2api.providers.types import Capability, ModelRoute, ProviderName


def _access_token(expiry: datetime, thumbprint: str, serial: int = 1) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(
        json.dumps(
            {"exp": int(expiry.timestamp()), "cnf": {"jkt": thumbprint}, "serial": serial},
            separators=(",", ":"),
        ).encode()
    ).rstrip(b"=").decode()
    return f"{header}.{payload}.unsigned"


class TrackingStream(httpx.AsyncByteStream):
    def __init__(self, *chunks: bytes) -> None:
        self.chunks = chunks
        self.iterated = False
        self.closed = False

    async def __aiter__(self):
        self.iterated = True
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class ConsoleResponsesTransportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        self.credential = ConsoleCredential(
            name="Console account",
            source_key="console-sso:fingerprint",
            sso_token="sso-must-stay-secret",
            cloudflare_cookies="cf_clearance=clearance-must-stay-secret",
        )
        self.route = ModelRoute(
            public_model="grok-4.5",
            provider=ProviderName.CONSOLE,
            upstream_model="grok-4.5-upstream",
            capability=Capability.RESPONSES,
        )

    def _mint_response(self, request: httpx.Request, serial: int = 1) -> httpx.Response:
        jwk = json.loads(request.content)["jwk"]
        return httpx.Response(
            200,
            json={
                "access_token": _access_token(
                    self.now + timedelta(minutes=10), jwk_thumbprint(jwk), serial
                ),
                "token_type": "DPoP",
                "expires_in": 600,
            },
        )

    async def test_overrides_model_preserves_stream_and_returns_unconsumed_response(self) -> None:
        final_stream = TrackingStream(b'data: {"type":"response.completed"}\n\n')
        captured: dict[str, object] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/v1/dpop/token":
                return self._mint_response(request)
            captured["json"] = json.loads(request.content)
            captured["cookie"] = request.headers["Cookie"]
            captured["authorization"] = request.headers["Authorization"]
            captured["dpop"] = request.headers["DPoP"]
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=final_stream)

        original = {"model": "client-selected-model", "stream": True, "input": "hello"}
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            transport = ConsoleResponsesTransport(ConsoleDPoPClient(http, now=lambda: self.now))
            response = await transport.forward("account-1", self.credential, original, self.route)
            self.assertEqual(original["model"], "client-selected-model")
            self.assertEqual(captured["json"], {"model": "grok-4.5-upstream", "stream": True, "input": "hello"})
            self.assertFalse(response.is_stream_consumed)
            self.assertFalse(response.is_closed)
            self.assertFalse(final_stream.iterated)
            self.assertFalse(final_stream.closed)
            self.assertNotIn(self.credential.sso_token, repr(transport))
            self.assertTrue(str(captured["authorization"]).startswith("DPoP "))
            self.assertTrue(captured["dpop"])
            self.assertIn("sso=sso-must-stay-secret", str(captured["cookie"]))
            self.assertIn("response.completed", (await response.aread()).decode())
            await response.aclose()
        self.assertTrue(final_stream.iterated)
        self.assertTrue(final_stream.closed)

    async def test_first_401_is_consumed_but_final_response_is_not(self) -> None:
        mint_count = 0
        protected_count = 0
        first = TrackingStream(b'{"error":"expired"}')
        final = TrackingStream(b'{"ok":true}')

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal mint_count, protected_count
            if request.url.path == "/v1/dpop/token":
                mint_count += 1
                return self._mint_response(request, mint_count)
            protected_count += 1
            if protected_count == 1:
                return httpx.Response(401, stream=first)
            return httpx.Response(200, stream=final)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            transport = ConsoleResponsesTransport(ConsoleDPoPClient(http, now=lambda: self.now))
            response = await transport.forward(1, self.credential, {"input": "hi"}, self.route)
            self.assertEqual((mint_count, protected_count), (2, 2))
            self.assertTrue(first.iterated)
            self.assertTrue(first.closed)
            self.assertFalse(final.iterated)
            self.assertFalse(final.closed)
            await response.aclose()
        self.assertTrue(final.closed)

    async def test_route_boundary_rejects_non_console_or_non_responses(self) -> None:
        calls = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(500)

        wrong_provider = ModelRoute(
            "grok-4.5", ProviderName.BUILD, "grok-4.5", Capability.RESPONSES
        )
        wrong_capability = ModelRoute(
            "grok-4.5", ProviderName.CONSOLE, "grok-4.5", Capability.CHAT
        )
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            transport = ConsoleResponsesTransport(ConsoleDPoPClient(http, now=lambda: self.now))
            with self.assertRaises(ConsoleResponsesError):
                await transport.forward(1, self.credential, {}, wrong_provider)
            with self.assertRaises(ConsoleResponsesError):
                await transport.forward(1, self.credential, {}, wrong_capability)
        self.assertEqual(calls, 0)

    async def test_transport_exception_does_not_expose_sso_or_dpop(self) -> None:
        captured_proof = ""

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal captured_proof
            if request.url.path == "/v1/dpop/token":
                return self._mint_response(request)
            captured_proof = request.headers["DPoP"]
            raise httpx.ReadError(
                f"unsafe {self.credential.sso_token} {captured_proof}",
                request=request,
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            transport = ConsoleResponsesTransport(ConsoleDPoPClient(http, now=lambda: self.now))
            with self.assertRaises(ConsoleResponsesError) as raised:
                await transport.forward(1, self.credential, {"input": "hello"}, self.route)
        self.assertTrue(captured_proof)
        for rendered in (str(raised.exception), repr(raised.exception)):
            self.assertNotIn(self.credential.sso_token, rendered)
            self.assertNotIn(captured_proof, rendered)
            self.assertNotIn(self.credential.cloudflare_cookies, rendered)
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)


if __name__ == "__main__":
    unittest.main()
