from __future__ import annotations

import unittest

from grok2api.providers.web.errors import WebErrorKind, classify_web_error
from grok2api.providers.web.models import WebCapability, WebTier, list_web_models


class WebModelsAndErrorsTests(unittest.TestCase):
    def test_catalog_filters_by_tier_and_exposes_capabilities(self) -> None:
        basic = list_web_models(WebTier.BASIC)
        heavy = list_web_models("heavy")
        self.assertEqual(
            [item["id"] for item in basic],
            [
                "grok-chat-fast", "grok-chat-auto", "grok-imagine-image-lite",
                "grok-imagine-image", "grok-imagine-image-2.0",
            ],
        )
        self.assertEqual(len(heavy), 7)
        self.assertIn(WebCapability.STREAMING.value, heavy[0]["capabilities"])
        self.assertTrue(all(item["provider"] == "grok_web" for item in heavy))

    def test_auth_egress_and_rate_limit_are_distinct(self) -> None:
        unauthorized = classify_web_error(401)
        forbidden = classify_web_error(403)
        limited = classify_web_error(429, headers={"Retry-After": "17"})

        self.assertEqual(unauthorized.kind, WebErrorKind.AUTH)
        self.assertTrue(unauthorized.invalidates_credential)
        self.assertFalse(unauthorized.retryable)

        self.assertEqual(forbidden.kind, WebErrorKind.EGRESS_CLOUDFLARE)
        self.assertFalse(forbidden.invalidates_credential)
        self.assertTrue(forbidden.retryable)

        self.assertEqual(limited.kind, WebErrorKind.RATE_LIMIT)
        self.assertEqual(limited.retry_after_seconds, 17.0)


if __name__ == "__main__":
    unittest.main()
