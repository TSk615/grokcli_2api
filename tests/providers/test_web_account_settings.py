from __future__ import annotations

import unittest
from datetime import date

from grok2api.providers.web.account_settings import (
    ACCEPT_TERMS_FRAME,
    CURRENT_TERMS_VERSION,
    ENABLE_NSFW_FRAME,
    _classification,
    _grpc_status,
    random_adult_birth_date,
)


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

    def test_birth_date_is_adult_range(self) -> None:
        today = date(2026, 9, 7)
        for _ in range(20):
            value = random_adult_birth_date(today=today)
            age = today.year - value.year - ((today.month, today.day) < (value.month, value.day))
            self.assertGreaterEqual(age, 20)
            self.assertLessEqual(age, 40)


if __name__ == "__main__":
    unittest.main()
