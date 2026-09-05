from __future__ import annotations

import json
import unittest
from pathlib import Path

from grok2api.providers.web.protocol import WebProtocolError, convert_chat_completion
from grok2api.providers.web.stream import (
    GrokWebStreamParser,
    WebDeltaKind,
    WebStreamError,
)


_FIXTURES = Path(__file__).with_name("fixtures")


class WebRequestProtocolTests(unittest.TestCase):
    def test_converts_openai_messages_model_and_stream(self) -> None:
        converted = convert_chat_completion(
            {
                "model": "Web/grok-chat-expert",
                "stream": True,
                "messages": [
                    {"role": "system", "content": "Be precise."},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "First"},
                            {"type": "input_text", "text": "Second"},
                        ],
                    },
                ],
                "cookie": "must-not-forward",
                "sso": "must-not-forward",
            }
        )
        self.assertEqual(converted.public_model, "grok-chat-expert")
        self.assertEqual(converted.upstream_mode, "expert")
        self.assertTrue(converted.stream)
        self.assertEqual(
            converted.payload["message"],
            "[system]\nBe precise.\n\n[user]\nFirst\nSecond",
        )
        self.assertEqual(converted.payload["modeId"], "expert")
        self.assertNotIn("stream", converted.payload)
        encoded = json.dumps(converted.payload)
        self.assertNotIn("must-not-forward", encoded)
        self.assertNotIn("cookie", encoded.lower())
        self.assertNotIn("sso", encoded.lower())

    def test_rejects_unverified_content_without_echoing_it(self) -> None:
        secret = "sso-secret-never-echo"
        with self.assertRaises(WebProtocolError) as caught:
            convert_chat_completion(
                {
                    "model": "grok-chat-fast",
                    "messages": [
                        {
                            "role": "user",
                            "content": [{"type": "image_url", "image_url": secret}],
                        }
                    ],
                }
            )
        self.assertNotIn(secret, str(caught.exception))


class WebStreamProtocolTests(unittest.TestCase):
    @staticmethod
    def _feed_fragmented(parser: GrokWebStreamParser, data: bytes):
        output = []
        for offset in range(0, len(data), 7):
            output.extend(parser.feed(data[offset : offset + 7]))
        output.extend(parser.finish())
        return output

    def test_parses_legacy_sse_text_reasoning_and_citation(self) -> None:
        data = (_FIXTURES / "web_chat_sse.txt").read_bytes()
        deltas = self._feed_fragmented(GrokWebStreamParser(), data)
        reasoning = "".join(item.text for item in deltas if item.kind is WebDeltaKind.REASONING)
        text = "".join(item.text for item in deltas if item.kind is WebDeltaKind.TEXT)
        citations = [item.citation for item in deltas if item.kind is WebDeltaKind.CITATION]
        self.assertEqual(reasoning, "I should check. ")
        self.assertEqual(text, "Hello world")
        self.assertEqual(len(citations), 1)
        self.assertEqual(citations[0].url, "https://example.com/source")
        self.assertEqual(citations[0].title, "Example source")

    def test_parses_concatenated_gateway_json_and_sanitizes_citation(self) -> None:
        data = (_FIXTURES / "web_chat_concat.jsons").read_bytes()
        deltas = self._feed_fragmented(GrokWebStreamParser(), data)
        reasoning = "".join(item.text for item in deltas if item.kind is WebDeltaKind.REASONING)
        text = "".join(item.text for item in deltas if item.kind is WebDeltaKind.TEXT)
        citation = next(item.citation for item in deltas if item.kind is WebDeltaKind.CITATION)
        self.assertEqual(reasoning, "Reason ")
        self.assertEqual(text, "Answer done")
        self.assertEqual(citation.url, "https://example.org/page?ok=1")

    def test_unknown_events_are_ignored_and_errors_are_redacted(self) -> None:
        parser = GrokWebStreamParser()
        self.assertEqual(parser.feed('{"event":{"type":"future.event"}}'), [])
        secret = "sso=do-not-leak"
        with self.assertRaises(WebStreamError) as caught:
            parser.feed(json.dumps({"error": {"message": secret}}))
        self.assertNotIn(secret, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
