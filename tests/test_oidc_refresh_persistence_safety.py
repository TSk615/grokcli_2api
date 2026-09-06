from __future__ import annotations

import os
import unittest
from contextlib import contextmanager
from unittest.mock import call, patch

from grok2api.upstream import oidc_auth


@contextmanager
def _acquired_refresh_slot(*_args, **_kwargs):
    yield True


class InvalidRefreshPolicyTests(unittest.TestCase):
    def test_default_policy_retains_credentials_and_soft_disables(self) -> None:
        stored = {
            "account-1": {
                "key": "access-token-must-be-retained",
                "refresh_token": "refresh-token-must-be-retained",
            }
        }

        def mutate(callback):
            callback(stored)

        with patch.dict(os.environ, {}, clear=True), patch.object(
            oidc_auth, "mutate_auth_map", side_effect=mutate
        ), patch("grok2api.pool.account_pool.kick_from_pool") as kick:
            result = oidc_auth.mark_refresh_invalid(
                "account-1", reason="confirmed invalid_grant"
            )

        self.assertTrue(result["ok"])
        self.assertFalse(result["deleted"])
        self.assertTrue(result["disabled"])
        self.assertEqual(result["action"], "disabled")
        self.assertIn("account-1", stored)
        self.assertEqual(
            stored["account-1"]["refresh_token"],
            "refresh-token-must-be-retained",
        )
        self.assertTrue(stored["account-1"]["refresh_invalid"])
        kick.assert_called_once()

    def test_hard_delete_requires_an_explicit_truthy_switch(self) -> None:
        for value in ("1", "true", "yes", "on", "TRUE"):
            with self.subTest(value=value), patch.dict(
                os.environ,
                {"GROK2API_DELETE_INVALID_REFRESH": value},
                clear=True,
            ):
                self.assertTrue(oidc_auth._hard_delete_invalid_refresh_enabled())

        for value in ("", "0", "false", "no", "off", "unexpected"):
            with self.subTest(value=value), patch.dict(
                os.environ,
                {"GROK2API_DELETE_INVALID_REFRESH": value},
                clear=True,
            ):
                self.assertFalse(oidc_auth._hard_delete_invalid_refresh_enabled())

    def test_destructive_compatibility_wrapper_is_explicit(self) -> None:
        with patch.object(oidc_auth, "mark_refresh_invalid") as mark:
            oidc_auth.delete_account_for_refresh_failure("account-1", reason="bad")
        mark.assert_called_once_with("account-1", reason="bad", hard_delete=True)


class RefreshPersistenceRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.old_entry = {"key": "old-access", "refresh_token": "old-refresh"}
        self.token_response = {
            "access_token": "new-access",
            "refresh_token": "rotated-refresh",
        }
        self.new_entry = {
            "key": "new-access",
            "refresh_token": "rotated-refresh",
        }

    def _common_patches(self):
        return (
            patch.object(
                oidc_auth,
                "_distributed_refresh_slot",
                side_effect=_acquired_refresh_slot,
            ),
            patch.object(
                oidc_auth,
                "refresh_access_token",
                return_value=self.token_response,
            ),
            patch.object(
                oidc_auth,
                "entry_from_token_response",
                return_value=("account-1", self.new_entry),
            ),
            patch("grok2api.pool.account_pool.record_renew_success"),
        )

    def test_retries_only_persistence_after_one_successful_exchange(self) -> None:
        slot, exchange, convert, renew_success = self._common_patches()
        with patch.dict(
            os.environ,
            {
                "GROK2API_REFRESH_PERSIST_ATTEMPTS": "3",
                "GROK2API_REFRESH_PERSIST_RETRY_DELAY": "0.01",
            },
            clear=False,
        ), slot, exchange as refresh, convert, patch.object(
            oidc_auth,
            "upsert_entry",
            side_effect=[RuntimeError("pg-1"), RuntimeError("pg-2"), "account-1"],
        ) as upsert, patch.object(oidc_auth.time, "sleep") as sleep, renew_success:
            result = oidc_auth.refresh_and_persist(
                "account-1",
                self.old_entry,
                persist=True,
                recheck_latest=False,
            )

        self.assertTrue(result["performed"])
        self.assertEqual(result["entry"]["refresh_token"], "rotated-refresh")
        self.assertEqual(refresh.call_count, 1)
        self.assertEqual(upsert.call_count, 3)
        self.assertEqual(sleep.call_args_list, [call(0.01), call(0.02)])

    def test_persistence_failure_is_bounded_and_never_reexchanges(self) -> None:
        slot, exchange, convert, renew_success = self._common_patches()
        with patch.dict(
            os.environ,
            {
                "GROK2API_REFRESH_PERSIST_ATTEMPTS": "3",
                "GROK2API_REFRESH_PERSIST_RETRY_DELAY": "0",
            },
            clear=False,
        ), slot, exchange as refresh, convert, patch.object(
            oidc_auth,
            "upsert_entry",
            side_effect=RuntimeError("database unavailable"),
        ) as upsert, patch.object(oidc_auth.time, "sleep") as sleep, renew_success:
            with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                oidc_auth.refresh_and_persist(
                    "account-1",
                    self.old_entry,
                    persist=True,
                    recheck_latest=False,
                )

        self.assertEqual(refresh.call_count, 1)
        self.assertEqual(upsert.call_count, 3)
        self.assertEqual(sleep.call_count, 2)


if __name__ == "__main__":
    unittest.main()
