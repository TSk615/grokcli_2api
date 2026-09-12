from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.responses import JSONResponse

import grok2api.app as app


class AdminImageWorkbenchTests(unittest.IsolatedAsyncioTestCase):
    async def test_admin_session_can_use_regular_image_pipeline(self) -> None:
        generated = JSONResponse(
            {"created": 123, "data": [{"url": "/v1/media/images/example.jpg"}]}
        )
        with patch(
            "grok2api.admin.admin_routes.verify_session_token", return_value=True
        ), patch.object(
            app, "image_generations", AsyncMock(return_value=generated)
        ) as handler:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app.app),
                base_url="https://images.example.test",
            ) as client:
                response = await client.post(
                    "/admin/api/images/generations",
                    headers={"X-Admin-Token": "admin-session"},
                    json={
                        "model": "Web/grok-imagine-image-2.0",
                        "prompt": "a studio portrait",
                        "enable_nsfw": True,
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"][0]["url"], "/v1/media/images/example.jpg")
        handler.assert_awaited_once()
        self.assertIsNone(handler.await_args.kwargs["api_key"])

    async def test_workbench_requires_admin_session(self) -> None:
        with patch(
            "grok2api.admin.admin_routes.verify_session_token", return_value=False
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app.app),
                base_url="https://images.example.test",
            ) as client:
                response = await client.post(
                    "/admin/api/images/generations",
                    json={"model": "Web/grok-imagine-image", "prompt": "x"},
                )
        self.assertEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()
