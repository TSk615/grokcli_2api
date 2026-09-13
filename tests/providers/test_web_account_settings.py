from __future__ import annotations

import base64
import json
import unittest
from datetime import date
from unittest.mock import patch

from grok2api.providers.web.auth import WebCredential
from grok2api.providers.web.account_settings import (
    ACCEPT_TERMS_FRAME,
    CURRENT_TERMS_VERSION,
    ENABLE_NSFW_FRAME,
    _VerificationMetaParser,
    _classification,
    _grpc_status,
    WebAccountSettingsClient,
    random_adult_birth_date,
)


class _Response:
    def __init__(self, status_code: int, body: bytes) -> None:
        self.status_code = status_code
        self._body = body
        self.headers: dict[str, str] = {}

    async def aread(self) -> bytes:
        return self._body

    async def aclose(self) -> None:
        return None


class WebAccountSettingsTests(unittest.TestCase):
    def test_frames_match_reference_protocol(self) -> None:
        self.assertEqual(ACCEPT_TERMS_FRAME.hex(), "00000000021001")
        self.assertEqual(
            ENABLE_NSFW_FRAME.hex(),
            "00000000200a021001121a0a18616c776179735f73686f775f6e7366775f636f6e74656e74",
        )
        self.assertEqual(CURRENT_TERMS_VERSION, 5)

    def test_classifies_required_markers(self) -> None:
        self.assertEqual(
            _classification(429, b"[WKE=account:birth-date-change-limit-reached]", phase="birth"),
            "birth_date_locked",
        )
        self.assertEqual(
            _classification(403, b"User must accept ToS [WKE=unauthorized:tos-accepted-version-required]", phase="nsfw"),
            "terms_required",
        )
        self.assertEqual(_classification(429, b"", phase="image"), "rate_limited")

    def test_parses_grpc_web_trailer(self) -> None:
        payload = b"grpc-status: 7\r\n"
        frame = bytes([0x80]) + len(payload).to_bytes(4, "big") + payload
        self.assertEqual(_grpc_status({}, frame), "7")

    def test_statsig_meta_parser_normalizes_current_unicode_dash(self) -> None:
        for name in ("grok-site-verification", "grok-site―verification"):
            parser = _VerificationMetaParser()
            parser.feed(f'<html><head><meta name="{name}" content="meta-value"></head></html>')
            self.assertEqual(parser.value, "meta-value")

    def test_birth_date_is_adult_range(self) -> None:
        today = date(2026, 9, 7)
        for _ in range(20):
            value = random_adult_birth_date(today=today)
            age = today.year - value.year - ((today.month, today.day) < (value.month, value.day))
            self.assertGreaterEqual(age, 20)
            self.assertLessEqual(age, 40)


class WebAccountSettingsAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_statsig_signer_uses_direct_client_without_account_secrets(self) -> None:
        signature = base64.b64encode(b"x" * 70).decode()

        class Client:
            async def get(self, url: str, **kwargs):
                return _Response(200, b'<meta name="grok-site-verification" content="meta-value">')

            async def post(self, url: str, **kwargs):
                raise AssertionError("signer must not use the account-bound client")

        class DirectResponse:
            status_code = 200
            content = json.dumps({"x-statsig-id": signature}).encode()

        class DirectClient:
            def __init__(self, **kwargs):
                self.init_kwargs = kwargs

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return None

            async def post(self, url: str, **kwargs):
                self.url = url
                self.kwargs = kwargs
                return DirectResponse()

        client = Client()
        direct = DirectClient()
        with patch(
            "grok2api.providers.web.account_settings.httpx.AsyncClient",
            return_value=direct,
        ) as factory:
            worker = WebAccountSettingsClient(client)
            value = await worker._signed_statsig(WebCredential("secret-sso", "secret-rw"), "/rest/test")
        self.assertEqual(value, signature)
        factory.assert_called_once_with(timeout=15.0, trust_env=False, follow_redirects=False)
        self.assertEqual(direct.url, "https://grok.wodf.de/sign")
        self.assertNotIn("Cookie", direct.kwargs["headers"])
        self.assertNotIn("Authorization", direct.kwargs["headers"])
        self.assertNotIn("secret-sso", repr(direct.kwargs))

    async def test_nsfw_request_continues_without_statsig_when_signer_returns_403(self) -> None:
        class Client:
            async def get(self, url: str, **kwargs):
                return _Response(200, b'<meta name="grok-site-verification" content="meta-value">')

            async def post(self, url: str, **kwargs):
                self.url = url
                self.kwargs = kwargs
                return _Response(200, b"")

        class RejectedSignerResponse:
            status_code = 403
            content = b"forbidden"

        class RejectedSigner:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return None

            async def post(self, url: str, **kwargs):
                return RejectedSignerResponse()

        client = Client()
        with patch(
            "grok2api.providers.web.account_settings.httpx.AsyncClient",
            return_value=RejectedSigner(),
        ):
            result = await WebAccountSettingsClient(client).enable_nsfw(
                WebCredential("secret-sso", "secret-rw")
            )

        self.assertTrue(result.success)
        self.assertEqual(
            client.url,
            "https://grok.com/auth_mgmt.AuthManagement/UpdateUserFeatureControls",
        )
        self.assertEqual(client.kwargs["content"], ENABLE_NSFW_FRAME)
        self.assertNotIn("x-statsig-id", client.kwargs["headers"])


if __name__ == "__main__":
    unittest.main()
