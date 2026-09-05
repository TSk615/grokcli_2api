from __future__ import annotations

import unittest

from grok2api.providers.registry import (
    AmbiguousModelRouteError,
    ProviderRegistry,
    parse_model_reference,
)
from grok2api.providers.types import Capability, ModelRoute, ProviderName, ProviderStatus, ErrorKind


class _Adapter:
    def __init__(self, provider: ProviderName, routes: list[ModelRoute]) -> None:
        self.provider = provider
        self._routes = routes

    def model_routes(self):
        return list(self._routes)

    def classify_status(self, status_code, **_kwargs):
        return ProviderStatus(ErrorKind.UPSTREAM, retryable=status_code >= 500)


class ProviderCoreTests(unittest.TestCase):
    def test_provider_prefixes_are_parsed(self):
        self.assertEqual(parse_model_reference("Web/grok-chat-fast"), (ProviderName.WEB, "grok-chat-fast"))
        self.assertEqual(parse_model_reference("Console/grok-4.5"), (ProviderName.CONSOLE, "grok-4.5"))
        self.assertEqual(parse_model_reference("vendor/model"), (None, "vendor/model"))

    def test_explicit_prefix_prevents_cross_provider_selection(self):
        build = ModelRoute("grok-4.5", ProviderName.BUILD, "grok-4.5", Capability.CHAT)
        console = ModelRoute("grok-4.5", ProviderName.CONSOLE, "grok-4.5", Capability.CHAT)
        registry = ProviderRegistry([
            _Adapter(ProviderName.BUILD, [build]),
            _Adapter(ProviderName.CONSOLE, [console]),
        ])
        self.assertEqual(registry.resolve("Build/grok-4.5").provider, ProviderName.BUILD)
        self.assertEqual(registry.resolve("Console/grok-4.5").provider, ProviderName.CONSOLE)
        with self.assertRaises(AmbiguousModelRouteError):
            registry.resolve("grok-4.5")

    def test_capability_is_part_of_route_identity(self):
        image = ModelRoute("grok-imagine", ProviderName.CONSOLE, "grok-imagine", Capability.IMAGE)
        edit = ModelRoute("grok-imagine", ProviderName.CONSOLE, "grok-imagine", Capability.IMAGE_EDIT)
        registry = ProviderRegistry([_Adapter(ProviderName.CONSOLE, [image, edit])])
        self.assertEqual(registry.resolve("Console/grok-imagine", capability="image_edit"), edit)


if __name__ == "__main__":
    unittest.main()
