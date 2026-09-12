from __future__ import annotations

import asyncio
import json
import unittest

import httpx

from grok2api.providers.web.auth import WebCredential
from grok2api.providers.web.gateway import GrokWebGateway
from grok2api.providers.web.image import (
    ImagineCollector,
    WebImageProtocolError,
    imagine_request_message,
    imagine_reset_message,
    resolve_aspect_ratio,
)


class _Socket:
    def __init__(self, frames: list[str]) -> None:
        self.frames = list(frames)
        self.sent: list[dict] = []
        self.closed = False

    async def send(self, value: str) -> None:
        self.sent.append(json.loads(value))

    async def recv(self) -> str:
        if not self.frames:
            await asyncio.Future()
        return self.frames.pop(0)

    async def close(self) -> None:
        self.closed = True


class WebImageProtocolTests(unittest.TestCase):
    def test_aspect_ratio_and_messages_match_imagine_protocol(self) -> None:
        self.assertEqual(resolve_aspect_ratio("1280x720"), "16:9")
        with self.assertRaises(WebImageProtocolError):
            resolve_aspect_ratio("5:7")
        reset = imagine_reset_message()
        self.assertEqual(reset["item"]["content"][0]["type"], "reset")
        request = imagine_request_message("a cat", "1:1", pro=True, generations=2)
        props = request["item"]["content"][0]["properties"]
        self.assertEqual(props["aspect_ratio"], "1:1")
        self.assertTrue(props["enable_pro"])
        self.assertEqual(props["num_generations"], 2)
        self.assertFalse(props["enable_nsfw"])

        nsfw_request = imagine_request_message(
            "a tasteful adult art portrait",
            "3:2",
            pro=True,
            generations=1,
            nsfw=True,
        )
        self.assertTrue(nsfw_request["item"]["content"][0]["properties"]["enable_nsfw"])

    def test_collector_discards_preview_and_moderated_and_sorts(self) -> None:
        collector = ImagineCollector()
        collector.accept({"type": "image", "image_id": "b", "url": "b.jpg", "percentage_complete": 30})
        collector.accept({"type": "image", "image_id": "a", "url": "a.jpg", "percentage_complete": 100, "order": 0})
        collector.accept({"type": "json", "image_id": "a", "url": "a.jpg", "current_status": "completed", "moderated": False})
        collector.accept({"type": "image", "image_id": "b", "url": "b.jpg", "percentage_complete": 100, "order": 1})
        collector.accept({"type": "json", "image_id": "b", "url": "b.jpg", "current_status": "completed", "moderated": True})
        self.assertTrue(collector.done(1))
        values = collector.images()
        self.assertEqual([value.url for value in values], ["https://assets.grok.com/a.jpg"])

    def test_gateway_imagine_generation_uses_reset_and_request(self) -> None:
        socket = _Socket([
            json.dumps({"type": "image", "image_id": "img", "url": "generated/final.jpg", "percentage_complete": 100, "order": 0}),
            json.dumps({"type": "json", "image_id": "img", "url": "generated/final.jpg", "current_status": "completed", "moderated": False}),
        ])

        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"ok": True})

        async def connector(url, headers, timeout):
            self.assertEqual(url, "wss://grok.com/ws/imagine/listen")
            self.assertIn("sso=secret", headers["Cookie"])
            return socket

        async def run() -> list:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                gateway = GrokWebGateway(client, connector=connector, total_timeout=2)
                return await gateway.generate_image(
                    {
                        "model": "Web/grok-imagine-image",
                        "prompt": "a cat",
                        "enable_nsfw": True,
                    },
                    WebCredential(sso="secret", sso_rw="secret"),
                )

        values = asyncio.run(run())
        self.assertEqual([value.url for value in values], ["https://assets.grok.com/generated/final.jpg"])
        self.assertEqual([item["item"]["content"][0].get("type") for item in socket.sent], ["reset", "input_text"])
        self.assertTrue(socket.sent[1]["item"]["content"][0]["properties"]["enable_nsfw"])
        self.assertTrue(socket.closed)

    def test_lite_generation_extracts_legacy_image_url(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"session": {"userId": "9d7dcf8c-4d51-4c2f-af43-d5f6f16be18a"}})

        socket = _Socket([
            json.dumps({"session_id": "conversation-1", "event": {"type": "session.created"}}),
            json.dumps({"session_id": "conversation-1", "event": {"type": "conversation.attached", "conversation": {"id": "conversation-1"}}}),
            json.dumps(
                {
                    "session_id": "conversation-1",
                    "event": {
                        "type": "response.grok.output",
                        "output": {
                            "card_attachment": {
                                "image_chunk": {
                                    "progress": 100,
                                    "moderated": False,
                                    "imageUrl": "users/u/generated/cat.jpg",
                                }
                            }
                        },
                    },
                }
            ),
            json.dumps({"session_id": "conversation-1", "event": {"type": "response.done", "response": {"status": "completed"}}}),
        ])

        async def connector(url, headers, timeout):
            return socket

        async def run() -> list:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                gateway = GrokWebGateway(client, connector=connector)
                return await gateway.generate_image(
                    {"model": "grok-imagine-image-lite", "prompt": "a cat"},
                    WebCredential(sso="secret", sso_rw="secret"),
                )

        values = asyncio.run(run())
        self.assertEqual(values[0].url, "https://assets.grok.com/users/u/generated/cat.jpg")


if __name__ == "__main__":
    unittest.main()
