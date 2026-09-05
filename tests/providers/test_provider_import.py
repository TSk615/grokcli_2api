from __future__ import annotations

import os
import unittest
from unittest import mock

from grok2api.admin.provider_accounts import ProviderImportError, import_provider_accounts
from grok2api.providers.web.auth import WebCredential


class ProviderImportTests(unittest.TestCase):
    def test_web_import_returns_only_public_metadata(self):
        credential = WebCredential("sso-secret", "rw-secret")
        with mock.patch("grok2api.config.WEB_PROVIDER_ENABLED", True), mock.patch.dict(
            os.environ, {"GROK2API_SECRET_KEY": "key"}, clear=True
        ), mock.patch(
            "grok2api.providers.web.auth.parse_web_credentials",
            return_value=[credential],
        ), mock.patch(
            "grok2api.admin.provider_accounts.accounts_pg.find_account_by_source",
            return_value=None,
        ), mock.patch(
            "grok2api.admin.provider_accounts.store_provider_credential"
        ) as stored:
            result = import_provider_accounts("web", "must-not-echo", web_tier="super")
        self.assertTrue(result["ok"])
        self.assertEqual(result["provider"], "grok_web")
        self.assertEqual(result["accounts"][0]["web_tier"], "super")
        self.assertNotIn("sso-secret", repr(result))
        self.assertNotIn("rw-secret", repr(result))
        stored.assert_called_once()

    def test_disabled_provider_rejects_before_parsing(self):
        with mock.patch("grok2api.config.CONSOLE_PROVIDER_ENABLED", False):
            with self.assertRaisesRegex(ProviderImportError, "disabled"):
                import_provider_accounts("console", "secret")


if __name__ == "__main__":
    unittest.main()
