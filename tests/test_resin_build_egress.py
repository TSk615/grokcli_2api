from __future__ import annotations

import os
import unittest
from collections import OrderedDict
from unittest.mock import MagicMock, patch


def _resin_env() -> dict[str, str]:
    return {
        "GROK2API_RESIN_PROXY_ENABLED": "1",
        "GROK2API_RESIN_PROXY_HOST": "resin",
        "GROK2API_RESIN_PROXY_PORT": "2260",
        "GROK2API_RESIN_PLATFORM": "grok",
        "GROK2API_RESIN_TOKEN": "test-token-do-not-log",
        "GROK2API_RESIN_IDENTITY_SECRET": "identity-secret-at-least-thirty-two-bytes-long",
    }


class _AsyncClientStub:
    is_closed = False


class BuildResinEgressTests(unittest.IsolatedAsyncioTestCase):
    async def test_build_client_uses_account_resin_auth_and_skips_direct_pool(self) -> None:
        import grok2api.app as app

        created = _AsyncClientStub()
        with patch.dict(os.environ, _resin_env(), clear=False), patch.object(
            app, "_http_client", None
        ), patch.object(app, "_http_clients_by_proxy", OrderedDict()), patch.object(
            app, "_new_async_client", return_value=created
        ) as make_client, patch(
            "grok2api.upstream.proxy_pool.pick_proxy_for_account"
        ) as direct_pool:
            result = await app.get_http_client("build-account-a")

        self.assertIs(result, created)
        direct_pool.assert_not_called()
        kwargs = make_client.call_args.kwargs
        self.assertEqual(kwargs["proxy"], "http://resin:2260")
        self.assertEqual(kwargs["proxy_auth"][0].split(".", 1)[0], "grok")
        self.assertEqual(kwargs["proxy_auth"][1], _resin_env()["GROK2API_RESIN_TOKEN"])

    async def test_invalid_resin_config_fails_closed_before_direct_pool(self) -> None:
        import grok2api.app as app
        from grok2api.upstream.resin_proxy import ResinConfigError

        env = _resin_env()
        env["GROK2API_RESIN_TOKEN"] = ""
        with patch.dict(os.environ, env, clear=False), patch.object(
            app, "_http_client", None
        ), patch.object(app, "_http_clients_by_proxy", OrderedDict()), patch(
            "grok2api.upstream.proxy_pool.pick_proxy_for_account"
        ) as direct_pool:
            with self.assertRaises(ResinConfigError):
                await app.get_http_client("build-account-a")
        direct_pool.assert_not_called()

    def test_httpx_proxy_receives_auth_separately_from_url(self) -> None:
        import grok2api.app as app

        proxy_obj = object()
        with patch.object(app.httpx, "Proxy", return_value=proxy_obj) as proxy, patch.object(
            app.httpx, "AsyncClient", return_value=_AsyncClientStub()
        ) as async_client:
            app._new_async_client(
                proxy="http://resin:2260",
                proxy_auth=("grok.opaque-account", "test-token-do-not-log"),
            )
        proxy.assert_called_once_with(
            "http://resin:2260",
            auth=("grok.opaque-account", "test-token-do-not-log"),
        )
        self.assertIs(async_client.call_args.kwargs["proxy"], proxy_obj)


class OidcAndProbeResinEgressTests(unittest.TestCase):
    def test_oidc_refresh_uses_resin_even_if_unbound_client_was_supplied(self) -> None:
        from grok2api.upstream import oidc_auth

        response = MagicMock(status_code=200)
        response.json.return_value = {"access_token": "new-access"}
        bound = MagicMock(is_closed=False)
        bound.post.return_value = response
        unbound = MagicMock()
        proxy_obj = object()
        with patch.dict(os.environ, _resin_env(), clear=False), patch.object(
            oidc_auth.httpx, "Proxy", return_value=proxy_obj
        ) as proxy, patch.object(
            oidc_auth.httpx, "Client", return_value=bound
        ) as client_factory, patch(
            "grok2api.upstream.proxy_pool.pick_proxy_for_account"
        ) as direct_pool:
            result = oidc_auth.refresh_access_token(
                {
                    "id": "build-account-a",
                    "refresh_token": "refresh-secret",
                },
                client=unbound,
            )

        self.assertEqual(result["access_token"], "new-access")
        unbound.post.assert_not_called()
        direct_pool.assert_not_called()
        self.assertEqual(proxy.call_args.args, ("http://resin:2260",))
        self.assertIn("auth", proxy.call_args.kwargs)
        self.assertIs(client_factory.call_args.kwargs["proxy"], proxy_obj)

    def test_model_probe_uses_account_resin_binding_not_direct_pool(self) -> None:
        from grok2api.pool import model_health

        client = MagicMock(is_closed=False)
        with patch.dict(os.environ, _resin_env(), clear=False), patch.object(
            model_health, "_http_clients_by_proxy", {}
        ), patch.object(model_health, "_new_probe_client", return_value=client) as make, patch(
            "grok2api.upstream.proxy_pool.pick_proxy_for_account"
        ) as direct_pool:
            result = model_health._probe_http_client("build-account-a")

        self.assertIs(result, client)
        direct_pool.assert_not_called()
        kwargs = make.call_args.kwargs
        self.assertEqual(kwargs["proxy"], "http://resin:2260")
        self.assertEqual(kwargs["proxy_auth"][0].split(".", 1)[0], "grok")

    def test_build_oidc_probe_share_identity_and_accounts_stay_isolated(self) -> None:
        from grok2api.upstream.resin_proxy import resin_binding_for_account

        with patch.dict(os.environ, _resin_env(), clear=False):
            live_a = resin_binding_for_account("grok_build", "build-account-a")
            refresh_a = resin_binding_for_account("grok_build", "build-account-a")
            probe_a = resin_binding_for_account("grok_build", "build-account-a")
            live_b = resin_binding_for_account("grok_build", "build-account-b")
        self.assertIsNotNone(live_a)
        self.assertIsNotNone(refresh_a)
        self.assertIsNotNone(probe_a)
        self.assertIsNotNone(live_b)
        assert live_a and refresh_a and probe_a and live_b
        self.assertEqual(live_a.account, refresh_a.account)
        self.assertEqual(live_a.account, probe_a.account)
        self.assertEqual(live_a.cache_key, refresh_a.cache_key)
        self.assertEqual(live_a.cache_key, probe_a.cache_key)
        self.assertNotEqual(live_a.account, live_b.account)
        self.assertNotEqual(live_a.cache_key, live_b.cache_key)


if __name__ == "__main__":
    unittest.main()
