import json
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from grok2api.providers.web.auth import WebCredential
from grok2api.providers.web.gateway import GrokWebGateway, WebGatewayError


class WebVideoTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_480p_six_seconds_uses_local_signature(self):
        calls = []

        def handle(request):
            calls.append(request)
            return httpx.Response(200, json={"result": {
                "streamingVideoGenerationResponse": {
                    "progress": 100, "videoUrl": "https://assets.grok.com/video.mp4",
                },
            }})

        with patch("grok2api.providers.web.statsig.generate", return_value="fixture-proof") as signer:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
                gateway = GrokWebGateway(client)
                gateway._statsig._optional_signed_statsig = AsyncMock()
                url = await gateway.generate_video(
                    {"prompt": "blue ball"}, WebCredential(sso="fixture", sso_rw="fixture"),
                )
                gateway._statsig._optional_signed_statsig.assert_not_awaited()
        signer.assert_called_once_with("/rest/app-chat/conversations/new")
        self.assertEqual(url, "https://assets.grok.com/video.mp4")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].headers["x-statsig-id"], "fixture-proof")
        self.assertEqual(calls[0].headers["referer"], "https://grok.com/imagine")
        self.assertEqual(calls[0].headers["sec-fetch-site"], "same-origin")
        payload = json.loads(calls[0].content)
        self.assertEqual(payload["modelName"], "imagine-video-gen")
        self.assertEqual(payload["mediaGenInput"]["textToVideo"], {
            "prompt": "blue ball", "duration": 6, "resolutionName": "480p", "aspectRatio": "1:1",
        })

    async def test_explicit_720p_and_fallback_signature(self):
        def handle(request):
            body = json.loads(request.content)["mediaGenInput"]["textToVideo"]
            self.assertEqual(body["resolutionName"], "720p")
            self.assertEqual(body["duration"], 6)
            self.assertEqual(request.headers["x-statsig-id"], "fallback-proof")
            return httpx.Response(200, json={"result": {"videoUrl": "clip.mp4"}})

        with patch("grok2api.providers.web.statsig.generate", return_value=None):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
                gateway = GrokWebGateway(client)
                gateway._statsig._optional_signed_statsig = AsyncMock(return_value="fallback-proof")
                await gateway.generate_video({"prompt": "x", "resolution": "720p"}, WebCredential(sso="fixture", sso_rw="fixture"))
                gateway._statsig._optional_signed_statsig.assert_awaited_once()

    async def test_upstream_errors_do_not_expose_body(self):
        with patch("grok2api.providers.web.statsig.generate", return_value="fixture"):
            async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(429, json={"error": "private-account-detail"}),
            )) as client:
                with self.assertRaises(WebGatewayError) as raised:
                    await GrokWebGateway(client).generate_video({"prompt": "x"}, WebCredential(sso="fixture", sso_rw="fixture"))
        self.assertIn("429", str(raised.exception))
        self.assertNotIn("private-account-detail", str(raised.exception))
