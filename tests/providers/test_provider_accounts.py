from __future__ import annotations

import unittest
from unittest import mock

from grok2api.providers.accounts import (
    ProviderAccountsUnavailable,
    acquire_provider_sequence,
)
from grok2api.providers.types import ProviderName
from grok2api.providers.web.auth import WebCredential


class ProviderAccountPoolTests(unittest.TestCase):
    def test_provider_query_and_credentials_are_strictly_scoped(self):
        rows = [
            {"id": "a", "web_tier": "super", "source_key": "one"},
            {"id": "b", "web_tier": "heavy", "source_key": "two"},
        ]
        credential = WebCredential("secret", "secret")
        with mock.patch(
            "grok2api.providers.accounts.accounts_pg.list_provider_account_refs",
            return_value=rows,
        ) as listed, mock.patch(
            "grok2api.providers.accounts.load_provider_credential",
            return_value=credential,
        ) as loaded, mock.patch(
            "grok2api.providers.accounts._next_index", return_value=1
        ):
            sequence = acquire_provider_sequence(
                ProviderName.WEB, minimum_tier="super"
            )
        listed.assert_called_once_with("grok_web")
        self.assertEqual([item.account_id for item in sequence], ["b", "a"])
        self.assertTrue(
            all(call.args[1] == "grok_web" for call in loaded.call_args_list)
        )
        self.assertNotIn("secret", repr(sequence))

    def test_web_tier_filter_fails_without_cross_provider_fallback(self):
        with mock.patch(
            "grok2api.providers.accounts.accounts_pg.list_provider_account_refs",
            return_value=[{"id": "basic", "web_tier": "basic"}],
        ):
            with self.assertRaises(ProviderAccountsUnavailable):
                acquire_provider_sequence("web", minimum_tier="heavy")

    def test_build_pool_is_not_reused(self):
        with self.assertRaises(ValueError):
            acquire_provider_sequence("build")


if __name__ == "__main__":
    unittest.main()
