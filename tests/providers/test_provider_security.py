from __future__ import annotations

import unittest

from grok2api.config import validate_provider_security


class ProviderSecurityTests(unittest.TestCase):
    def test_build_only_does_not_require_provider_secret(self):
        validate_provider_security(
            web_enabled=False,
            console_enabled=False,
            secret_key="",
        )

    def test_web_and_console_fail_closed_without_secret(self):
        for web, console in ((True, False), (False, True), (True, True)):
            with self.subTest(web=web, console=console):
                with self.assertRaisesRegex(RuntimeError, "GROK2API_SECRET_KEY"):
                    validate_provider_security(
                        web_enabled=web,
                        console_enabled=console,
                        secret_key="",
                    )

    def test_enabled_provider_accepts_explicit_secret(self):
        validate_provider_security(
            web_enabled=True,
            console_enabled=True,
            secret_key="test-only-secret",
        )


if __name__ == "__main__":
    unittest.main()
