from __future__ import annotations

import base64
import json
import unittest
from datetime import date

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
    async def test_statsig_signer_uses_bound_client_without_account_secrets(self) -> None:
        signature = base64.b64encode(b"x" * 70).decode()

        class Client:
            async def get(self, url: str, **kwargs):
                return _Response(200, b'<meta name="grok-site-verification" content="meta-value">')

            async def post(self, url: str, **kwargs):
                self.url = url
                self.kwargs = kwargs
                return _Response(200, json.dumps({"x-statsig-id": signature}).encode())

        client = Client()
        worker = WebAccountSettingsClient(client)
        value = await worker._signed_statsig(WebCredential("secret-sso", "secret-rw"), "/rest/test")
        self.assertEqual(value, signature)
        self.assertEqual(client.url, "https://grok.wodf.de/sign")
        self.assertNotIn("Cookie", client.kwargs["headers"])
        self.assertNotIn("Authorization", client.kwargs["headers"])
        self.assertNotIn("secret-sso", repr(client.kwargs))


if __name__ == "__main__":
    unittest.main()
