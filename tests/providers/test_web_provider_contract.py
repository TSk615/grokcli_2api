from __future__ import annotations

import unittest

from grok2api.providers.base import ProviderAdapter
from grok2api.providers.registry import ProviderRegistry
from grok2api.providers.types import Capability, ErrorKind, ModelRoute, ProviderName
from grok2api.providers.web.adapter import GrokWebAdapter
from grok2api.providers.web.errors import WebErrorKind, classify_web_error


class WebProviderContractTests(unittest.TestCase):
    def setUp(self) -> None:
        # Discovery and status classification do not touch the injected client.
        self.adapter = GrokWebAdapter(object())  # type: ignore[arg-type]

    def test_adapter_implements_shared_protocol_and_uses_enum(self) -> None:
        self.assertIsInstance(self.adapter, ProviderAdapter)
        self.assertIs(self.adapter.provider, ProviderName.WEB)

    def test_routes_are_shared_types_and_registry_resolves_web_prefix(self) -> None:
        routes = list(self.adapter.model_routes())
        self.assertEqual(len(routes), 4)
        self.assertTrue(all(isinstance(route, ModelRoute) for route in routes))
        self.assertTrue(all(route.provider is ProviderName.WEB for route in routes))
        self.assertTrue(all(route.capability is Capability.CHAT for route in routes))

        registry = ProviderRegistry([self.adapter])
        route = registry.resolve("Web/grok-chat-expert")
        self.assertEqual(route.upstream_model, "expert")
        self.assertEqual(route.minimum_tier, "super")
        self.assertIn("reasoning", route.metadata["web_capabilities"])

    def test_shared_status_preserves_web_specific_classification(self) -> None:
        auth = self.adapter.classify_status(401)
        blocked = self.adapter.classify_status(403, body="ignored")
        limited = self.adapter.classify_status(429, headers={"Retry-After": "9"})

        self.assertEqual(auth.kind, ErrorKind.AUTH)
        self.assertTrue(auth.account_scoped)
        self.assertTrue(auth.invalidate_credential)

        self.assertEqual(blocked.kind, ErrorKind.EGRESS)
        self.assertTrue(blocked.retryable)
        self.assertFalse(blocked.account_scoped)
        self.assertFalse(blocked.invalidate_credential)

        self.assertEqual(limited.kind, ErrorKind.RATE_LIMIT)
        self.assertTrue(limited.account_scoped)
        self.assertEqual(limited.retry_after_seconds, 9.0)

        # The original public API remains available and retains its richer kind.
        self.assertEqual(classify_web_error(403).kind, WebErrorKind.EGRESS_CLOUDFLARE)


if __name__ == "__main__":
    unittest.main()
