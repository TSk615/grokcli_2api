from __future__ import annotations

import unittest

from grok2api.providers.catalog import append_optional_provider_models
from grok2api.providers.registry import ProviderRegistry
from grok2api.providers.types import Capability, ModelRoute, ProviderName, ProviderStatus, ErrorKind


class _Adapter:
    def __init__(self, provider, routes):
        self.provider = provider
        self._routes = routes

    def model_routes(self):
        return self._routes

    def classify_status(self, _status_code, **_kwargs):
        return ProviderStatus(ErrorKind.UPSTREAM)


class ProviderCatalogTests(unittest.TestCase):
    def test_build_rows_are_unchanged_and_optional_ids_are_qualified(self):
        build = {"id": "grok-4.5", "object": "model", "custom": "kept"}
        routes = [
            ModelRoute(
                "grok-4.5",
                ProviderName.CONSOLE,
                "grok-4.5",
                Capability.RESPONSES,
            ),
            ModelRoute(
                "grok-4.5",
                ProviderName.CONSOLE,
                "grok-4.5",
                Capability.CHAT,
            ),
            ModelRoute(
                "grok-chat-fast",
                ProviderName.WEB,
                "fast",
                Capability.CHAT,
                minimum_tier="basic",
            ),
        ]
        registry = ProviderRegistry(
            [
                _Adapter(ProviderName.CONSOLE, routes[:2]),
                _Adapter(ProviderName.WEB, routes[2:]),
            ]
        )
        output = append_optional_provider_models([build], registry)
        self.assertEqual(output[0], build)
        by_id = {item["id"]: item for item in output}
        self.assertNotIn("grok-chat-fast", by_id)
        self.assertEqual(
            by_id["Console/grok-4.5"]["capabilities"],
            ["chat", "responses"],
        )
        self.assertEqual(by_id["Web/grok-chat-fast"]["minimum_tier"], "basic")


if __name__ == "__main__":
    unittest.main()
