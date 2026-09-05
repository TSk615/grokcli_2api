#!/usr/bin/env python3
"""Focused regressions for repeated imports and refresh-token safety."""

from __future__ import annotations

import os
import sys
import time
from contextlib import nullcontext
from pathlib import Path
from unittest import mock

os.environ["GROK2API_STORE_BACKEND"] = "file"
os.environ["GROK2API_REDIS_URL"] = ""
os.environ["REDIS_URL"] = ""

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# The application config intentionally supplies localhost Redis/PostgreSQL
# defaults when environment variables are blank.  Override the imported
# runtime values as well so this focused regression stays fully offline on a
# workstation that has the optional store drivers installed but no daemons.
from grok2api import config as app_config  # noqa: E402

app_config.DATABASE_URL = ""
app_config.REDIS_URL = ""

from grok2api.pool import account_pool, accounts  # noqa: E402
from grok2api.upstream import oidc_auth  # noqa: E402


def ok(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  ok: {message}")


class _FakeClient:
    def __init__(self, *_args, **_kwargs) -> None:
        self.is_closed = False

    def close(self) -> None:
        self.is_closed = True


def _entry(token: str, refresh: str | None, expires_at: float) -> dict:
    out = {
        "key": token,
        "user_id": "user-1",
        "expires_at": expires_at,
    }
    if refresh is not None:
        out["refresh_token"] = refresh
    return out


def test_repeated_import_selects_fresh_credential_generation() -> None:
    print("[repeated import credential generation]")
    old = _entry("access-old", "refresh-old", 200.0)

    fresh = accounts.merge_imported_account_fields(
        _entry("access-new", "refresh-new", 300.0), old
    )
    ok(fresh["key"] == "access-new", "newer access token wins")
    ok(fresh["refresh_token"] == "refresh-new", "paired newer refresh token wins")

    stale = accounts.merge_imported_account_fields(
        _entry("access-stale", "refresh-stale", 100.0), old
    )
    ok(stale["key"] == "access-old", "stale access token cannot roll storage back")
    ok(stale["refresh_token"] == "refresh-old", "stale refresh token cannot roll storage back")

    access_only = accounts.merge_imported_account_fields(
        _entry("access-newer", None, 400.0), old
    )
    ok(access_only["key"] == "access-newer", "fresh access-only import is accepted")
    ok(access_only["refresh_token"] == "refresh-old", "missing refresh token is preserved")


def test_duplicate_batch_order_does_not_select_stale_tokens() -> None:
    print("[duplicate batch selection]")
    fresh = _entry("access-fresh", "refresh-fresh", 500.0)
    stale = _entry("access-stale", "refresh-stale", 100.0)
    merged = accounts._merge_normalized_duplicate(fresh, stale)
    ok(merged["key"] == "access-fresh", "later stale duplicate does not win")
    ok(merged["refresh_token"] == "refresh-fresh", "credential pair stays consistent")


def test_legacy_key_normalization_selects_fresh_tokens() -> None:
    print("[legacy duplicate normalization]")
    fresh = _entry("access-fresh", "refresh-fresh", 500.0)
    stale = _entry("access-stale", "refresh-stale", 100.0)
    captured: dict = {}

    def _capture(data: dict) -> None:
        captured.update(data)

    with mock.patch.object(
        oidc_auth,
        "read_auth_map",
        return_value={"legacy-a": fresh, "legacy-b": stale},
    ), mock.patch.object(oidc_auth, "write_auth_map", side_effect=_capture):
        result = oidc_auth.normalize_auth_file_keys()

    normalized = captured["https://auth.x.ai::user-1"]
    ok(result["total"] == 1, "same user is normalized to one account")
    ok(normalized["key"] == "access-fresh", "normalization retains fresher access token")
    ok(
        normalized["refresh_token"] == "refresh-fresh",
        "normalization retains paired refresh token",
    )


def test_refresh_persists_rotated_token_immediately() -> None:
    print("[immediate rotated-token persistence]")
    now = time.time()
    original = _entry("access-old", "refresh-old", now - 1)
    with mock.patch.object(
        oidc_auth, "_distributed_refresh_slot", return_value=nullcontext(True)
    ), mock.patch.object(
        oidc_auth, "read_auth_entry", return_value=("account-1", dict(original))
    ), mock.patch.object(
        oidc_auth,
        "refresh_access_token",
        return_value={
            "access_token": "access-new",
            "refresh_token": "refresh-new",
            "expires_in": 3600,
        },
    ), mock.patch.object(oidc_auth, "upsert_entry") as save, mock.patch.object(
        account_pool, "record_renew_success"
    ):
        result = oidc_auth.refresh_and_persist("account-1", original)

    ok(result["performed"] is True, "OIDC exchange is reported as performed")
    ok(save.call_count == 1, "new credential generation is persisted once")
    saved = save.call_args.args[1]
    ok(saved["key"] == "access-new", "new access token is persisted")
    ok(saved["refresh_token"] == "refresh-new", "rotated refresh token is persisted")


def test_concurrent_refresh_reuses_persisted_generation() -> None:
    print("[concurrent refresh generation]")
    now = time.time()
    original = _entry("access-old", "refresh-old", now - 1)
    latest = _entry("access-new", "refresh-new", now + 3600)
    with mock.patch.object(
        oidc_auth, "_distributed_refresh_slot", return_value=nullcontext(True)
    ), mock.patch.object(
        oidc_auth, "read_auth_entry", return_value=("account-1", latest)
    ), mock.patch.object(oidc_auth, "refresh_access_token") as exchange, mock.patch.object(
        account_pool, "record_renew_success"
    ):
        result = oidc_auth.refresh_and_persist("account-1", original)

    ok(result["performed"] is False, "waiting worker reuses the persisted generation")
    exchange.assert_not_called()
    ok(True, "rotating refresh token is not redeemed twice")


def test_two_transient_failures_never_delete_credentials() -> None:
    print("[transient refresh failure retention]")
    now = time.time()
    stored = _entry("access-old", "refresh-old", now - 1)
    with mock.patch.object(oidc_auth, "read_auth_map", return_value={"account-1": stored}), \
         mock.patch.object(oidc_auth, "refresh_and_persist", side_effect=ValueError("timeout")), \
         mock.patch.object(oidc_auth.httpx, "Client", _FakeClient), \
         mock.patch.object(account_pool, "mark_account_expired"), \
         mock.patch.object(account_pool, "record_renew_failure", return_value=2), \
         mock.patch.object(account_pool, "remove_from_pool_after_renew_failure") as remove, \
         mock.patch.object(oidc_auth, "mark_refresh_invalid") as invalidate:
        result = oidc_auth.refresh_all_accounts(
            only_near_expiry=False,
            max_workers=1,
            max_accounts=None,
            strict_sweep=False,
        )

    row = next(r for r in result["results"] if r.get("id") == "account-1")
    ok(row["reason"] == "renew_failed_transient", "transient failure is classified")
    ok(not row.get("deleted"), "credentials are retained after repeated transient failures")
    remove.assert_not_called()
    invalidate.assert_not_called()


def test_stale_invalid_grant_cannot_invalidate_new_generation() -> None:
    print("[stale invalid_grant protection]")
    now = time.time()
    original = _entry("access-old", "refresh-old", now - 1)
    latest = _entry("access-new", "refresh-new", now + 3600)
    with mock.patch.object(oidc_auth, "read_auth_map", return_value={"account-1": original}), \
         mock.patch.object(
             oidc_auth,
             "refresh_and_persist",
             side_effect=oidc_auth.RefreshRevokedError("invalid_grant"),
         ), \
         mock.patch.object(oidc_auth, "read_auth_entry", return_value=("account-1", latest)), \
         mock.patch.object(oidc_auth.httpx, "Client", _FakeClient), \
         mock.patch.object(account_pool, "mark_account_expired"), \
         mock.patch.object(account_pool, "record_renew_success"), \
         mock.patch.object(oidc_auth, "mark_refresh_invalid") as invalidate:
        result = oidc_auth.refresh_all_accounts(
            only_near_expiry=False,
            max_workers=1,
            max_accounts=None,
            strict_sweep=False,
        )

    row = next(r for r in result["results"] if r.get("id") == "account-1")
    ok(row.get("skipped") is True, "stale rejection is ignored")
    ok(row["reason"] == "stale_refresh_rejection", "new generation is recognized")
    invalidate.assert_not_called()


def test_purge_retains_sso_recoverable_account() -> None:
    print("[purge retains SSO recovery credential]")
    stored = {
        **_entry("access-old", None, time.time() - 60),
        "sso": "saved-sso-cookie",
    }
    with mock.patch.object(
        oidc_auth, "read_auth_map", return_value={"account-1": stored}
    ):
        result = oidc_auth.purge_refresh_invalid_accounts(
            dry_run=True, hard_delete=True
        )
    ok(result["would_delete"] == 0, "expired account with SSO is not purged")


def test_missing_refresh_token_recovers_through_sso() -> None:
    print("[missing refresh token SSO recovery]")
    now = time.time()
    stored = {
        **_entry("access-old", None, now - 60),
        "sso": "saved-sso-cookie",
    }
    recovered = {
        **_entry("access-new", "refresh-new", now + 3600),
        "sso": "saved-sso-cookie",
    }
    with mock.patch.object(
        oidc_auth, "read_auth_map", return_value={"account-1": stored}
    ), mock.patch.object(
        oidc_auth, "read_auth_entry", return_value=("account-1", stored)
    ), mock.patch.object(
        oidc_auth,
        "_try_sso_reauth",
        return_value={"ok": True, "account_id": "account-1", "entry": recovered},
    ), mock.patch.object(oidc_auth.httpx, "Client", _FakeClient), mock.patch.object(
        account_pool, "mark_account_expired"
    ), mock.patch.object(
        account_pool, "record_renew_failure", return_value=1
    ), mock.patch.object(
        account_pool, "get_account_pool_meta", return_value={}
    ), mock.patch.object(
        account_pool, "mark_sso_reauth_attempt"
    ), mock.patch.object(
        account_pool, "record_renew_success"
    ), mock.patch.object(oidc_auth, "upsert_entry") as save:
        result = oidc_auth.refresh_all_accounts(
            only_near_expiry=True,
            max_workers=1,
            max_accounts=None,
            strict_sweep=False,
        )

    row = next(r for r in result["results"] if r.get("id") == "account-1")
    ok(row["renew_source"] == "sso", "SSO fallback is used")
    ok(save.call_count == 1, "SSO-recovered refresh token is persisted immediately")


def main() -> int:
    tests = [
        test_repeated_import_selects_fresh_credential_generation,
        test_duplicate_batch_order_does_not_select_stale_tokens,
        test_legacy_key_normalization_selects_fresh_tokens,
        test_refresh_persists_rotated_token_immediately,
        test_concurrent_refresh_reuses_persisted_generation,
        test_two_transient_failures_never_delete_credentials,
        test_stale_invalid_grant_cannot_invalidate_new_generation,
        test_purge_retains_sso_recoverable_account,
        test_missing_refresh_token_recovers_through_sso,
    ]
    failed = 0
    for test in tests:
        try:
            test()
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {test.__name__}: {exc}")
    if failed:
        print(f"\n{failed}/{len(tests)} failed")
        return 1
    print(f"\nall {len(tests)} passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
