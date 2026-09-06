"""Contract tests for the browser-shaped outbound transport.

The tests are deliberately offline: the curl_cffi session is replaced with a
small fake so no request can reach x.ai.  They document the security boundary
expected by Web and Console providers: a selected proxy and TLS impersonation
profile must be passed at session construction, responses must remain
httpx-like, and Cloudflare challenge pages must be distinguishable from
ordinary HTML.
"""

from __future__ import annotations

import json
import unittest
from unittest import mock


class _FakeCurlResponse:
    def __init__(self, status_code: int = 200, body: bytes = b'{"ok":true}') -> None:
        self.status_code = status_code
        self.headers = {"content-type": "application/json"}
        self.content = body
        self.text = body.decode("utf-8", errors="replace")
        self.closed = False

    def json(self):
        return json.loads(self.content)

    async def aclose(self):
        self.closed = True

    async def aread(self):
        return self.content

    async def aiter_bytes(self):
        yield self.content


class _FakeCurlSession:
    instances: list["_FakeCurlSession"] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False
        self.requests: list[tuple[str, str, dict]] = []
        self.response = _FakeCurlResponse()
        self.error: Exception | None = None
        type(self).instances.append(self)

    async def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        if self.error is not None:
            raise self.error
        return self.response

    async def close(self):
        self.closed = True


class BrowserTransportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        _FakeCurlSession.instances.clear()

    def test_cloudflare_challenge_detection_distinguishes_html(self) -> None:
        from grok2api.upstream.browser_transport import is_cloudflare_challenge

        challenge = (
            b"<!doctype html><title>Just a moment...</title>"
            b"<script>window._cf_chl_opt = {}</script>"
        )
        self.assertTrue(is_cloudflare_challenge(200, {"content-type": "text/html"}, challenge))
        self.assertTrue(is_cloudflare_challenge(403, {"server": "cloudflare"}, b"forbidden"))
        self.assertFalse(
            is_cloudflare_challenge(
                200,
                {"content-type": "text/html"},
                b"<html><title>Documentation</title><body>Hello</body></html>",
            )
        )
        self.assertFalse(is_cloudflare_challenge(200, {"content-type": "application/json"}, b'{"ok":true}'))

    def test_challenge_scan_is_bounded(self) -> None:
        from grok2api.upstream.browser_transport import is_cloudflare_challenge

        body = b"a" * 32 + b"Just a moment" + b"b" * 128
        self.assertFalse(is_cloudflare_challenge(200, {}, body, max_scan=32))
        self.assertTrue(is_cloudflare_challenge(200, {}, body, max_scan=len(body)))

    async def test_session_receives_proxy_and_impersonation_and_adapts_response(self) -> None:
        from grok2api.upstream import browser_transport

        with mock.patch.object(browser_transport, "AsyncSession", _FakeCurlSession):
            client = browser_transport.BrowserAsyncClient(
                proxy="http://user:pass@proxy.example:8080",
                impersonate="chrome131",
            )
            response = await client.request(
                "GET",
                "https://grok.com/api/auth/session",
                headers={"Accept": "application/json"},
            )

            self.assertEqual(len(_FakeCurlSession.instances), 1)
            session = _FakeCurlSession.instances[0]
            self.assertEqual(session.kwargs.get("proxy"), "http://user:pass@proxy.example:8080")
            self.assertEqual(session.kwargs.get("impersonate"), "chrome131")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), {"ok": True})
            self.assertEqual(await response.aread(), b'{"ok":true}')
            chunks = [chunk async for chunk in response.aiter_bytes()]
            self.assertEqual(chunks, [b'{"ok":true}'])
            await response.aclose()
            self.assertTrue(session.response.closed)

            await client.aclose()
            self.assertTrue(client.is_closed)
            self.assertTrue(session.closed)

    async def test_get_and_build_request_preserve_selected_egress(self) -> None:
        from grok2api.upstream import browser_transport

        with mock.patch.object(browser_transport, "AsyncSession", _FakeCurlSession):
            client = browser_transport.BrowserAsyncClient(proxy="http://proxy.example:8080")
            response = await client.get("https://console.x.ai/v1/health", timeout=3)
            self.assertEqual(response.status_code, 200)
            session = _FakeCurlSession.instances[0]
            method, url, kwargs = session.requests[0]
            self.assertEqual((method, url), ("GET", "https://console.x.ai/v1/health"))
            self.assertEqual(kwargs.get("timeout"), 3)

            request = client.build_request("POST", "https://console.x.ai/v1/responses", json={"model": "grok"})
            await client.send(request)
            self.assertEqual(session.requests[1][0], "POST")
            self.assertEqual(session.requests[1][2].get("json"), {"model": "grok"})

    async def test_repr_and_transport_errors_do_not_retain_secrets(self) -> None:
        from grok2api.upstream import browser_transport

        proxy_password = "proxy-password-must-not-leak"
        sso = "sso-value-must-not-leak"
        proxy = f"http://user:{proxy_password}@proxy.example:8080"
        with mock.patch.object(browser_transport, "AsyncSession", _FakeCurlSession):
            client = browser_transport.BrowserAsyncClient(proxy=proxy, impersonate="chrome")
            self.assertNotIn(proxy_password, repr(client))

            request = client.build_request(
                "GET",
                "https://grok.com/api/auth/session",
                headers={"Cookie": f"sso={sso}", "Authorization": f"Bearer {sso}"},
            )
            self.assertNotIn(sso, repr(request))
            self.assertNotIn(proxy_password, repr(request))

            session = _FakeCurlSession.instances[0]
            session.error = RuntimeError(f"failed {proxy} Cookie: sso={sso}")
            with self.assertRaises(Exception) as caught:
                await client.send(request)
            rendered = f"{caught.exception!s} {caught.exception!r}"
            self.assertNotIn(proxy_password, rendered)
            self.assertNotIn(sso, rendered)


if __name__ == "__main__":
    unittest.main()
