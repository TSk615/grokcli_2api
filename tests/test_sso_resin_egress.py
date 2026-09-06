from __future__ import annotations

import io
import os
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch


def _resin_env() -> dict[str, str]:
    return {
        "GROK2API_RESIN_PROXY_ENABLED": "1",
        "GROK2API_RESIN_PROXY_HOST": "resin",
        "GROK2API_RESIN_PROXY_PORT": "2260",
        "GROK2API_RESIN_PLATFORM": "grok",
        "GROK2API_RESIN_TOKEN": "resin-secret-not-for-output",
        "GROK2API_RESIN_IDENTITY_SECRET": "identity-secret-at-least-thirty-two-bytes-long",
        "GROK2API_SSO_DEVICE_GAP_SEC": "0",
        "GROK2API_SSO_DEVICE_RETRIES": "1",
    }


class _Response:
    def __init__(
        self,
        *,
        url: str,
        status_code: int = 200,
        payload: dict | None = None,
        text: str = "",
    ) -> None:
        self.url = url
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text
        self.content = b"1" if payload is not None else b""

    def json(self) -> dict:
        return dict(self._payload)


class _Cookies:
    def set(self, *_args, **_kwargs) -> None:
        return None


class _Session:
    def __init__(self) -> None:
        self.cookies = _Cookies()
        self.calls: list[tuple[str, str, dict]] = []

    def get(self, url: str, **kwargs):
        self.calls.append(("GET", url, kwargs))
        if url == "https://accounts.x.ai/":
            return _Response(url=url)
        return _Response(url=url)

    def post(self, url: str, **kwargs):
        self.calls.append(("POST", url, kwargs))
        if url.endswith("/oauth2/device/code"):
            return _Response(
                url=url,
                payload={
                    "device_code": "opaque-device-code",
                    "user_code": "opaque-user-code",
                    "verification_uri_complete": "https://auth.x.ai/device",
                    "interval": 1,
                    "expires_in": 60,
                },
            )
        if url.endswith("/oauth2/device/verify"):
            return _Response(url="https://auth.x.ai/consent")
        if url.endswith("/oauth2/device/approve"):
            return _Response(url="https://auth.x.ai/done")
        if url.endswith("/oauth2/token"):
            return _Response(
                url=url,
                payload={
                    "access_token": "access-secret-not-for-output",
                    "refresh_token": "refresh-secret-not-for-output",
                    "expires_in": 3600,
                },
            )
        raise AssertionError(f"unexpected URL: {url}")


class SsoResinEgressTests(unittest.TestCase):
    def test_entire_conversion_reuses_one_resin_identity(self) -> None:
        from scripts import sso_to_auth_json as sut

        session = _Session()
        stdout = io.StringIO()
        with patch.dict(os.environ, _resin_env(), clear=False), patch.object(
            sut.requests, "Session", return_value=session
        ), patch.object(sut, "_wait_device_flow_slot"), redirect_stdout(stdout):
            result = sut.sso_to_token(
                "sso-secret-not-for-output",
                egress_identity="persisted-build-account",
            )

        self.assertEqual(result["expires_in"], 3600)
        self.assertEqual(len(session.calls), 6)
        proxy_pairs = {
            (kwargs.get("proxy"), kwargs.get("proxy_auth"))
            for _method, _url, kwargs in session.calls
        }
        self.assertEqual(len(proxy_pairs), 1)
        proxy_url, proxy_auth = proxy_pairs.pop()
        self.assertEqual(proxy_url, "http://resin:2260")
        self.assertEqual(proxy_auth[0].split(".", 1)[0], "grok")
        self.assertEqual(proxy_auth[1], _resin_env()["GROK2API_RESIN_TOKEN"])

        rendered = stdout.getvalue()
        for secret in (
            "sso-secret-not-for-output",
            "access-secret-not-for-output",
            "refresh-secret-not-for-output",
            "resin-secret-not-for-output",
            "person@example.invalid",
        ):
            self.assertNotIn(secret, rendered)

    def test_fresh_import_identity_is_stable_without_exposing_sso(self) -> None:
        from scripts import sso_to_auth_json as sut

        identities: list[str] = []

        def _capture(identity: str) -> dict:
            identities.append(identity)
            raise RuntimeError("stop before network")

        with patch.object(sut, "_proxy_kwargs", side_effect=_capture):
            sut.sso_to_token("first-sso-secret", quiet=True)
            sut.sso_to_token("first-sso-secret", quiet=True)
            sut.sso_to_token("second-sso-secret", quiet=True)

        self.assertEqual(identities[0], identities[1])
        self.assertNotEqual(identities[0], identities[2])
        self.assertTrue(identities[0].startswith("sso-"))
        self.assertNotIn("first-sso-secret", identities[0])

    def test_invalid_resin_config_fails_closed_before_network(self) -> None:
        from scripts import sso_to_auth_json as sut

        env = _resin_env()
        env["GROK2API_RESIN_TOKEN"] = ""
        session_factory = MagicMock()
        direct_pool = MagicMock()
        stdout = io.StringIO()
        with patch.dict(os.environ, env, clear=False), patch.object(
            sut.requests, "Session", session_factory
        ), patch("proxy_pool.resolve_proxy_for_request", direct_pool), redirect_stdout(stdout):
            result = sut.sso_to_token("sso-secret-not-for-output")

        self.assertIsNone(result)
        session_factory.assert_not_called()
        direct_pool.assert_not_called()
        self.assertNotIn("sso-secret-not-for-output", stdout.getvalue())

    def test_oidc_sso_fallback_passes_persisted_account_identity(self) -> None:
        import sso_to_auth_json as sso_module
        from grok2api.upstream import oidc_auth

        token = {"access_token": "access-secret-not-for-output"}
        converted = {
            "key": "access-secret-not-for-output",
            "user_id": "new-user-id",
        }
        with patch(
            "grok2api.pool.accounts.get_sso_value",
            return_value="sso-secret-not-for-output",
        ), patch.object(
            sso_module, "sso_to_token", return_value=token
        ) as convert, patch.object(
            sso_module, "token_to_auth_entry", return_value=("ignored", converted)
        ), patch(
            "grok2api.pool.accounts.merge_durable_account_fields",
            side_effect=lambda new, _old: new,
        ):
            result = oidc_auth._try_sso_reauth(
                "persisted-build-account",
                {"sso": "present"},
            )

        self.assertTrue(result["ok"])
        convert.assert_called_once_with(
            "sso-secret-not-for-output",
            quiet=True,
            egress_identity="persisted-build-account",
        )


if __name__ == "__main__":
    unittest.main()
