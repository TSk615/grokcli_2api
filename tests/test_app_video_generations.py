import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

import grok2api.app as app


class AppVideoGenerationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        root = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.stack.enter_context(patch("grok2api.media.video_store.DATA_DIR", Path(root)))
        for name, value in {"WEB_PROVIDER_ENABLED": True, "CONSOLE_PROVIDER_ENABLED": False,
                            "CONSOLE_MEDIA_ENABLED": False, "PUBLIC_BASE_URL": ""}.items():
            self.stack.enter_context(patch.object(app._config, name, value))
        self.stack.enter_context(patch.object(app.apikeys, "auth_required", return_value=False))
        account = SimpleNamespace(account_id="fixture-account", web_tier="basic", credential=object())
        route = SimpleNamespace(public_model="grok-imagine-video", qualified_model="Web/grok-imagine-video", minimum_tier="basic")
        self.stack.enter_context(patch("grok2api.providers.create_provider_registry", return_value=SimpleNamespace(resolve=lambda *a, **kw: route)))
        self.stack.enter_context(patch("grok2api.providers.accounts.acquire_provider_sequence", return_value=[account]))
        self.gateway = SimpleNamespace(
            generate_video=AsyncMock(return_value="https://assets.grok.com/video.mp4"),
            download_video=AsyncMock(return_value=(b"fixture-mp4", "video/mp4")),
        )
        self.stack.enter_context(patch.object(app, "_web_gateway_for_account", AsyncMock(return_value=self.gateway)))

    async def request(self, **fields):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url="https://video.example.test") as client:
            return await client.post("/v1/videos/generations", json={"model": "Web/grok-imagine-video", "prompt": "blue ball", **fields})

    async def test_default_basic_480p_six_seconds_without_console(self):
        response = await self.request()
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers["x-grok2api-resolution"], "480p")
        self.assertEqual(response.headers["x-grok2api-duration"], "6")
        self.assertEqual(response.json()["status"], "completed")
        self.assertTrue(response.json()["url"].startswith("https://video.example.test/v1/media/videos/"))
        body, credential = self.gateway.generate_video.await_args.args
        self.assertEqual(body["resolution"], "480p")
        self.assertEqual(body["duration"], 6)
        self.assertIs(self.gateway.download_video.await_args.args[1], credential)

    async def test_explicit_480p_and_basic_duration_cap(self):
        response = await self.request(resolution="480p", duration=15)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.gateway.generate_video.await_args.args[0]["duration"], 6)

    async def test_720p_remains_selectable(self):
        response = await self.request(resolution="720p", duration=6)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.gateway.generate_video.await_args.args[0]["resolution"], "720p")

    async def test_invalid_resolution_and_duration_do_not_generate(self):
        for fields in ({"resolution": "1080p"}, {"duration": "oops"}, {"duration": -1}, {"duration": 16}):
            response = await self.request(**fields)
            self.assertEqual(response.status_code, 400, response.text)
        self.gateway.generate_video.assert_not_awaited()

    async def test_disabled_web_does_not_generate(self):
        with patch.object(app._config, "WEB_PROVIDER_ENABLED", False):
            response = await self.request()
        self.assertEqual(response.status_code, 400)
        self.gateway.generate_video.assert_not_awaited()

    async def test_console_still_requires_its_own_switches(self):
        response = await self.request(model="Console/grok-imagine-video")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "console_media_disabled")
        self.gateway.generate_video.assert_not_awaited()
