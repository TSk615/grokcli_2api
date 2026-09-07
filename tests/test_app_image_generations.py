from __future__ import annotations

import base64
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

import grok2api.app as app


PNG = b"\x89PNG\r\n\x1a\n" + b"generated-test-image"


class AppImageGenerationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._old_web_gateways = app._web_gateways.copy()
        app._web_gateways.clear()
        self.tempdir = tempfile.TemporaryDirectory()
        self.media_root = Path(self.tempdir.name) / "images"

    def tearDown(self) -> None:
        app._web_gateways.clear()
        app._web_gateways.update(self._old_web_gateways)
        self.tempdir.cleanup()

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app.app),
            base_url="https://images.example.test",
        )

    async def test_disabled_switch_returns_openai_error(self) -> None:
        with patch.object(app._config, "WEB_PROVIDER_ENABLED", False), patch.object(
            app._config, "WEB_IMAGES_ENABLED", True
        ), patch.object(app.apikeys, "auth_required", return_value=False):
            async with self._client() as client:
                response = await client.post(
                    "/v1/images/generations",
                    json={"model": "Web/grok-imagine-image", "prompt": "x"},
                )
        self.assertEqual(response.status_code, 400)
        body = response.json()
        self.assertEqual(body["error"]["code"], "web_images_disabled")

    async def test_invalid_json_and_parameters_return_400(self) -> None:
        with patch.object(app._config, "WEB_PROVIDER_ENABLED", True), patch.object(
            app._config, "WEB_IMAGES_ENABLED", True
        ), patch.object(app.apikeys, "auth_required", return_value=False):
            async with self._client() as client:
                invalid_json = await client.post(
                    "/v1/images/generations",
                    content=b"{not-json",
                    headers={"content-type": "application/json"},
                )
                invalid_params = await client.post(
                    "/v1/images/generations",
                    json={"model": "Web/grok-imagine-image", "prompt": "x", "n": 11},
                )
        self.assertEqual(invalid_json.status_code, 400)
        self.assertEqual(invalid_json.json()["error"]["type"], "invalid_request_error")
        self.assertEqual(invalid_params.status_code, 400)
        self.assertIn("n", invalid_params.json()["error"]["message"])

    async def _run_success(self, response_format: str):
        account = SimpleNamespace(
            account_id="web-account-1",
            credential=object(),
            egress_identity="resin-edge-1",
        )
        route = SimpleNamespace(
            public_model="grok-imagine-image",
            minimum_tier=None,
        )
        binding = SimpleNamespace(
            gateway_url="http://resin-gateway:2260",
            proxy_auth=("resin-user", "resin-password"),
            cache_key="resin-cache-key",
            account="resin-account",
        )
        # Resin mode requires the browser transport's credential-aware WebSocket
        # connector; image generation must never silently fall back to a direct
        # socket even when its HTTP client was proxy-bound.
        client = SimpleNamespace(websocket_connector=lambda *_args, **_kwargs: None)

        class _Gateway:
            async def generate_image(self, body, credential):
                self.body = body
                self.credential = credential
                return ["https://upstream.example/image.png"]

            async def download_image(self, image, credential):
                return PNG, "image/png"

        gateway = _Gateway()
        with patch.object(app._config, "WEB_PROVIDER_ENABLED", True), patch.object(
            app._config, "WEB_IMAGES_ENABLED", True
        ), patch.object(app._config, "PUBLIC_BASE_URL", ""), patch.object(
            app.apikeys, "auth_required", return_value=False
        ), patch(
            "grok2api.providers.create_provider_registry",
            return_value=SimpleNamespace(
                resolve=lambda *_args, **_kwargs: route,
            ),
        ), patch(
            "grok2api.providers.accounts.acquire_provider_sequence",
            return_value=[account],
        ), patch(
            "grok2api.upstream.resin_proxy.resin_binding_for_account",
            return_value=binding,
        ), patch(
            "grok2api.upstream.proxy_pool.pick_proxy_for_account",
        ) as pick_proxy, patch.object(
            app, "_get_provider_http_client", AsyncMock(return_value=client)
        ) as get_client, patch(
            "grok2api.providers.web.GrokWebGateway", return_value=gateway
        ) as gateway_factory, patch(
            "grok2api.media.image_store.DATA_DIR", Path(self.tempdir.name)
        ):
            async with self._client() as http_client:
                response = await http_client.post(
                    "/v1/images/generations",
                    json={
                        "model": "Web/grok-imagine-image",
                        "prompt": "a test image",
                        "response_format": response_format,
                    },
                )

        self.assertEqual(
            response.status_code,
            200,
            (response.content, get_client.await_args_list, gateway_factory.call_args_list),
        )
        body = response.json()
        self.assertEqual(len(body["data"]), 1)
        self.assertEqual(response.headers["x-grok2api-provider"], "grok_web")
        self.assertEqual(gateway.body["model"], "grok-imagine-image")
        self.assertEqual(gateway.body["prompt"], "a test image")
        gateway_factory.assert_called_once()
        pick_proxy.assert_not_called()
        get_client.assert_awaited_once_with(
            "grok_web",
            "web-account-1",
            proxy="http://resin-gateway:2260",
            proxy_auth=("resin-user", "resin-password"),
            proxy_identity="resin-cache-key",
            resin_account="resin-account",
        )
        return body

    async def test_web_gateway_success_returns_local_url(self) -> None:
        body = await self._run_success("url")
        self.assertTrue(body["data"][0]["url"].startswith("https://images.example.test/v1/media/images/"))

    async def test_web_gateway_success_returns_b64_json(self) -> None:
        body = await self._run_success("b64_json")
        self.assertEqual(body["data"][0]["b64_json"], base64.b64encode(PNG).decode("ascii"))

    async def test_media_get_and_path_traversal(self) -> None:
        from grok2api.media import ImageMediaStore

        with patch("grok2api.media.image_store.DATA_DIR", Path(self.tempdir.name)):
            stored = ImageMediaStore().store(PNG)
            async with self._client() as client:
                response = await client.get(stored.public_path)
                traversal = await client.get(
                    "/v1/media/images/%2e%2e%2f" + stored.filename
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "image/png")
        self.assertEqual(response.content, PNG)
        self.assertEqual(traversal.status_code, 404)


if __name__ == "__main__":
    unittest.main()
