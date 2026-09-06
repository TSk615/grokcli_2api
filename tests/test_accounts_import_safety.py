from __future__ import annotations

import time
import unittest
from unittest import mock

from grok2api.pool import accounts


class AccountImportSafetyTests(unittest.TestCase):
    def test_merge_uses_latest_locked_generation(self) -> None:
        current = {
            "https://auth.x.ai::user-1": {
                "key": "access-refreshed",
                "refresh_token": "refresh-refreshed",
                "user_id": "user-1",
                "expires_at": 500.0,
            }
        }
        incoming = {
            "https://auth.x.ai::user-1": {
                "key": "access-stale-import",
                "refresh_token": "refresh-stale-import",
                "user_id": "user-1",
                "expires_at": 100.0,
            }
        }

        def mutate(mutator):
            mutator(current)
            return current

        with mock.patch.object(accounts, "mutate_auth_map", side_effect=mutate) as atomic, \
             mock.patch.object(accounts, "read_auth_map") as read, \
             mock.patch.object(accounts, "write_auth_map") as write, \
             mock.patch.object(accounts, "_backup_auth_file"):
            result = accounts.merge_normalized_accounts(incoming, merge=True)

        self.assertTrue(result["ok"])
        atomic.assert_called_once()
        read.assert_not_called()
        write.assert_not_called()
        saved = current["https://auth.x.ai::user-1"]
        self.assertEqual(saved["key"], "access-refreshed")
        self.assertEqual(saved["refresh_token"], "refresh-refreshed")

    def test_cookie_shaped_sso_is_rejected_as_access_token(self) -> None:
        result = accounts.collect_normalized_entries(
            {"key": "Cookie: cf_clearance=x; sso=secret-value; other=y"}
        )
        self.assertFalse(result["ok"])
        self.assertIn("SSO Cookie", result["error"])
        self.assertNotIn("secret-value", result["error"])

    def test_anonymous_opaque_token_has_stable_non_secret_id(self) -> None:
        token = "opaque-access-token-value"
        first_id, _ = accounts._normalize_entry({"key": token})
        second_id, _ = accounts._normalize_entry({"key": token})
        self.assertEqual(first_id, second_id)
        self.assertNotIn(token, first_id)

    def test_shared_client_id_does_not_merge_different_emails(self) -> None:
        common = {
            "oidc_client_id": "shared-public-client",
            "expires_at": time.time() + 3600,
        }
        first_id, _ = accounts._normalize_entry(
            {**common, "key": "opaque-one", "email": "one@example.test"}
        )
        second_id, _ = accounts._normalize_entry(
            {**common, "key": "opaque-two", "email": "two@example.test"}
        )
        self.assertNotEqual(first_id, second_id)
        self.assertNotIn("one@example.test", first_id)
        self.assertNotIn("two@example.test", second_id)


if __name__ == "__main__":
    unittest.main()
