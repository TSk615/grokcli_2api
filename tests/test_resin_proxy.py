from __future__ import annotations

import os
import unittest
from unittest import mock


class ResinProxyTests(unittest.TestCase):
    def _env(self, *, token: str = "token-one"):
        return mock.patch.dict(
            os.environ,
            {
                "GROK2API_RESIN_PROXY_ENABLED": "1",
                "GROK2API_RESIN_PROXY_HOST": "resin",
                "GROK2API_RESIN_PROXY_PORT": "2260",
                "GROK2API_RESIN_PLATFORM": "grok",
                "GROK2API_RESIN_TOKEN": token,
                "GROK2API_RESIN_IDENTITY_SECRET": "i" * 48,
            },
            clear=False,
        )

    def test_identity_is_stable_opaque_and_token_rotation_safe(self) -> None:
        from grok2api.upstream.resin_proxy import (
            derive_resin_account,
            resin_binding_for_account,
        )

        sensitive_id = "account-sensitive-value"
        with self._env(token="token-one"):
            first = derive_resin_account("grok_web", sensitive_id)
            first_binding = resin_binding_for_account("grok_web", sensitive_id)
        with self._env(token="token-two"):
            second = derive_resin_account("grok_web", sensitive_id)
            second_binding = resin_binding_for_account("grok_web", sensitive_id)
        self.assertEqual(first, second)
        assert first_binding is not None and second_binding is not None
        self.assertEqual(first_binding.account, second_binding.account)
        self.assertNotEqual(first_binding.cache_key, second_binding.cache_key)
        self.assertNotIn(sensitive_id, first)
        self.assertRegex(first, r"^g2a-[0-9a-f]{32}$")

    def test_binding_uses_platform_account_and_keeps_token_out_of_url_repr(self) -> None:
        from grok2api.upstream.resin_proxy import resin_binding_for_account

        secret = "proxy-token-must-not-leak"
        with self._env(token=secret):
            binding = resin_binding_for_account("grok_console", "account-1")
        assert binding is not None
        self.assertRegex(binding.username, r"^grok\.g2a-[0-9a-f]{32}$")
        self.assertNotIn(secret, binding.gateway_url)
        self.assertNotIn(secret, repr(binding))
        self.assertEqual(binding.proxy_auth, (binding.username, secret))

    def test_different_accounts_and_providers_are_isolated(self) -> None:
        from grok2api.upstream.resin_proxy import resin_binding_for_account

        with self._env():
            web_a = resin_binding_for_account("grok_web", "account-a")
            web_b = resin_binding_for_account("grok_web", "account-b")
            console_a = resin_binding_for_account("grok_console", "account-a")
        assert web_a is not None and web_b is not None and console_a is not None
        self.assertEqual(len({web_a.account, web_b.account, console_a.account}), 3)
        self.assertEqual(len({web_a.cache_key, web_b.cache_key, console_a.cache_key}), 3)

    def test_enabled_incomplete_config_fails_closed(self) -> None:
        from grok2api.upstream.resin_proxy import ResinConfigError, resin_binding_for_account

        with mock.patch.dict(
            os.environ,
            {"GROK2API_RESIN_PROXY_ENABLED": "1", "GROK2API_RESIN_TOKEN": ""},
            clear=True,
        ):
            with self.assertRaises(ResinConfigError):
                resin_binding_for_account("grok_web", "account-a")

    def test_public_status_never_exposes_credentials_or_gateway(self) -> None:
        from grok2api.upstream.resin_proxy import resin_public_status

        secret = "proxy-token-must-not-leak"
        with self._env(token=secret):
            status = resin_public_status()
        self.assertEqual(
            status,
            {
                "mode": "resin",
                "enabled": True,
                "valid": True,
                "direct_fallback": False,
            },
        )
        rendered = repr(status)
        self.assertNotIn(secret, rendered)
        self.assertNotIn("resin:2260", rendered)


if __name__ == "__main__":
    unittest.main()
