from __future__ import annotations

import unittest

from grok2api.providers import ProviderName, create_provider_registry


class ProviderFactoryTests(unittest.TestCase):
    def test_build_only_is_default_compatible(self):
        registry = create_provider_registry(
            web_enabled=False,
            console_enabled=False,
            secret_key="",
        )
        self.assertEqual(registry.providers(), (ProviderName.BUILD,))

    def test_flags_register_three_strict_provider_namespaces(self):
        registry = create_provider_registry(
            web_enabled=True,
            console_enabled=True,
            secret_key="test-only-secret",
        )
        self.assertEqual(
            registry.providers(),
            (ProviderName.BUILD, ProviderName.WEB, ProviderName.CONSOLE),
        )
        self.assertEqual(
            registry.resolve("Web/grok-chat-fast").provider,
            ProviderName.WEB,
        )
        self.assertEqual(
            registry.resolve("Console/grok-4.5", capability="responses").provider,
            ProviderName.CONSOLE,
        )

    def test_factory_checks_secret_before_optional_imports(self):
        with self.assertRaisesRegex(RuntimeError, "GROK2API_SECRET_KEY"):
            create_provider_registry(
                web_enabled=True,
                console_enabled=False,
                secret_key="",
            )


if __name__ == "__main__":
    unittest.main()
