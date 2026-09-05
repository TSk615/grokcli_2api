from __future__ import annotations

import os
import subprocess
import sys
import unittest
from unittest import mock

from grok2api.providers.console.auth import ConsoleCredential
from grok2api.providers.web.auth import WebCredential
from grok2api.store import crypto, provider_credentials


class ProviderCredentialStoreTests(unittest.TestCase):
    def tearDown(self) -> None:
        crypto._fernet.cache_clear()

    def test_auth_and_store_import_without_httpx(self) -> None:
        script = """
import builtins
original_import = builtins.__import__
def guarded_import(name, *args, **kwargs):
    if name == 'httpx' or name.startswith('httpx.'):
        raise ModuleNotFoundError('httpx intentionally unavailable')
    return original_import(name, *args, **kwargs)
builtins.__import__ = guarded_import
from grok2api.providers.web.auth import WebCredential
from grok2api.providers.console.auth import ConsoleCredential
from grok2api.store.provider_credentials import store_provider_credential
assert WebCredential and ConsoleCredential and store_provider_credential
"""
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=os.getcwd(),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_web_store_encrypts_sso_and_cloudflare_values(self) -> None:
        credential = WebCredential(
            sso="sso-secret-value",
            sso_rw="sso-rw-secret-value",
            cloudflare_cookies={"cf_clearance": "cf-secret-value"},
            label="primary",
        )
        with mock.patch.dict(
            os.environ, {"GROK2API_SECRET_KEY": "test-key-a"}, clear=True
        ), mock.patch.object(
            provider_credentials.accounts_pg, "enabled", return_value=True
        ), mock.patch.object(
            provider_credentials.accounts_pg, "upsert_provider_account"
        ) as upsert:
            crypto._fernet.cache_clear()
            provider_credentials.store_provider_credential(
                "web-1", credential, web_tier="super", egress_identity="edge-a"
            )

        upsert.assert_called_once()
        account_id, public_payload = upsert.call_args.args
        kwargs = upsert.call_args.kwargs
        ciphertext = kwargs["credential_enc"]
        self.assertEqual(account_id, "web-1")
        self.assertEqual(kwargs["provider"], "grok_web")
        self.assertTrue(ciphertext.startswith("enc:v1:"))
        for secret in ("sso-secret-value", "sso-rw-secret-value", "cf-secret-value"):
            self.assertNotIn(secret, ciphertext)
            self.assertNotIn(secret, repr(public_payload))

    def test_console_round_trip_returns_redacted_repr_object(self) -> None:
        credential = ConsoleCredential(
            name="Console primary",
            source_key="console-sso:abc123",
            sso_token="console-sso-secret",
            email="owner@example.test",
            user_id="user-1",
            cloudflare_cookies="cf_clearance=console-cf-secret",
        )
        captured: dict[str, object] = {}

        def capture(*args, **kwargs) -> None:
            captured["account_id"] = args[0]
            captured.update(kwargs)

        with mock.patch.dict(
            os.environ, {"GROK2API_SECRET_KEY": "test-key-b"}, clear=True
        ), mock.patch.object(
            provider_credentials.accounts_pg, "enabled", return_value=True
        ), mock.patch.object(
            provider_credentials.accounts_pg,
            "upsert_provider_account",
            side_effect=capture,
        ):
            crypto._fernet.cache_clear()
            provider_credentials.store_provider_credential("console-1", credential)
            with mock.patch.object(
                provider_credentials.accounts_pg,
                "read_provider_credential_envelope",
                return_value=(
                    str(captured["credential_enc"]),
                    "console-sso:abc123",
                ),
            ) as read_envelope:
                loaded = provider_credentials.load_console_credential("console-1")

        read_envelope.assert_called_once_with("console-1", "grok_console")
        self.assertIsInstance(loaded, ConsoleCredential)
        self.assertEqual(loaded.sso_token, "console-sso-secret")
        self.assertEqual(loaded.cloudflare_cookies, "cf_clearance=console-cf-secret")
        self.assertNotIn("console-sso-secret", repr(loaded))
        self.assertNotIn("console-cf-secret", repr(loaded))

    def test_provider_mismatch_is_rejected(self) -> None:
        credential = WebCredential(sso="web-secret", sso_rw="web-secret")
        captured: dict[str, object] = {}

        def capture(*args, **kwargs) -> None:
            captured.update(kwargs)

        with mock.patch.dict(
            os.environ, {"GROK2API_SECRET_KEY": "test-key-c"}, clear=True
        ), mock.patch.object(
            provider_credentials.accounts_pg, "enabled", return_value=True
        ), mock.patch.object(
            provider_credentials.accounts_pg,
            "upsert_provider_account",
            side_effect=capture,
        ):
            crypto._fernet.cache_clear()
            provider_credentials.store_provider_credential("web-2", credential)
            with mock.patch.object(
                provider_credentials.accounts_pg,
                "read_provider_credential_envelope",
                return_value=(str(captured["credential_enc"]), credential.fingerprint),
            ) as read_envelope:
                with self.assertRaises(provider_credentials.CredentialDecryptionError):
                    provider_credentials.load_console_credential("web-2")

        read_envelope.assert_called_once_with("web-2", "grok_console")

    def test_wrong_key_never_returns_or_reports_secrets(self) -> None:
        credential = WebCredential(
            sso="wrong-key-sso-secret",
            sso_rw="wrong-key-rw-secret",
            cloudflare_cookies={"cf_clearance": "wrong-key-cf-secret"},
        )
        captured: dict[str, object] = {}

        def capture(*args, **kwargs) -> None:
            captured.update(kwargs)

        with mock.patch.dict(
            os.environ, {"GROK2API_SECRET_KEY": "correct-key"}, clear=True
        ), mock.patch.object(
            provider_credentials.accounts_pg, "enabled", return_value=True
        ), mock.patch.object(
            provider_credentials.accounts_pg,
            "upsert_provider_account",
            side_effect=capture,
        ):
            crypto._fernet.cache_clear()
            provider_credentials.store_provider_credential("web-3", credential)

        with mock.patch.dict(
            os.environ, {"GROK2API_SECRET_KEY": "incorrect-key"}, clear=True
        ), mock.patch.object(
            provider_credentials.accounts_pg,
            "read_provider_credential_envelope",
            return_value=(str(captured["credential_enc"]), credential.fingerprint),
        ):
            crypto._fernet.cache_clear()
            with self.assertRaises(
                provider_credentials.CredentialDecryptionError
            ) as raised:
                provider_credentials.load_web_credential("web-3")

        error = str(raised.exception)
        self.assertNotIn("wrong-key-sso-secret", error)
        self.assertNotIn("wrong-key-rw-secret", error)
        self.assertNotIn("wrong-key-cf-secret", error)

    def test_missing_key_fails_before_reading_storage(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(
            provider_credentials.accounts_pg, "read_provider_credential_envelope"
        ) as read_envelope:
            crypto._fernet.cache_clear()
            with self.assertRaises(provider_credentials.CredentialStorageUnavailable):
                provider_credentials.load_web_credential("web-4")

        read_envelope.assert_not_called()

    def test_missing_key_never_writes_plaintext(self) -> None:
        credential = WebCredential(sso="must-not-write", sso_rw="must-not-write")
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(
            provider_credentials.accounts_pg, "upsert_provider_account"
        ) as upsert:
            crypto._fernet.cache_clear()
            with self.assertRaises(provider_credentials.CredentialStorageUnavailable):
                provider_credentials.store_provider_credential("web-5", credential)

        upsert.assert_not_called()


if __name__ == "__main__":
    unittest.main()
