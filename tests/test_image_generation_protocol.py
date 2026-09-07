from __future__ import annotations

import base64
import unittest

from grok2api.protocol.image_generation import (
    ImageGenerationRequest,
    ImageRequestValidationError,
    build_image_generation_response,
    image_generation_error,
)


class ImageGenerationProtocolTests(unittest.TestCase):
    def test_defaults_and_normalization(self) -> None:
        request = ImageGenerationRequest.from_payload(
            {
                "model": " Web/grok-imagine-image ",
                "prompt": "  a rainy street  ",
                "aspect_ratio": "16:9",
                "response_format": " B64_JSON ",
            }
        )
        self.assertEqual(request.model, "Web/grok-imagine-image")
        self.assertEqual(request.prompt, "a rainy street")
        self.assertEqual(request.n, 1)
        self.assertEqual(request.aspect_ratio, "16:9")
        self.assertEqual(request.response_format, "b64_json")
        self.assertFalse(request.stream)

    def test_n_is_limited_to_one_through_ten(self) -> None:
        for count in (0, 11):
            with self.subTest(count=count):
                with self.assertRaises(ImageRequestValidationError) as raised:
                    ImageGenerationRequest.from_payload(
                        {"model": "Web/grok-imagine-image", "prompt": "x", "n": count}
                    )
                self.assertIn("n", raised.exception.message)

    def test_invalid_size_and_aspect_ratio_are_rejected(self) -> None:
        for field in ("size", "aspect_ratio"):
            with self.subTest(field=field):
                with self.assertRaises(ImageRequestValidationError) as raised:
                    ImageGenerationRequest.from_payload(
                        {
                            "model": "Web/grok-imagine-image",
                            "prompt": "x",
                            field: "7:5",
                        }
                    )
                self.assertIn(field, raised.exception.message)

    def test_stream_is_explicitly_rejected_for_first_release(self) -> None:
        with self.assertRaises(ImageRequestValidationError) as raised:
            ImageGenerationRequest.from_payload(
                {"model": "Web/grok-imagine-image", "prompt": "x", "stream": True}
            )
        self.assertIn("stream", raised.exception.message)

    def test_missing_or_non_object_payload_has_openai_error(self) -> None:
        with self.assertRaises(ImageRequestValidationError) as raised:
            ImageGenerationRequest.from_payload([])
        body = image_generation_error(raised.exception)
        self.assertEqual(body["error"]["type"], "invalid_request_error")
        self.assertIn("JSON", body["error"]["message"])

    def test_response_urls_and_base64_bytes(self) -> None:
        urls = build_image_generation_response(
            [" https://cdn.example/image.png "], response_format="url", created=123
        )
        self.assertEqual(urls, {"created": 123, "data": [{"url": "https://cdn.example/image.png"}]})

        raw = b"fake-png"
        encoded = build_image_generation_response([raw], response_format="b64_json", created=123)
        self.assertEqual(encoded["created"], 123)
        self.assertEqual(encoded["data"][0]["b64_json"], base64.b64encode(raw).decode("ascii"))

    def test_url_format_rejects_raw_bytes(self) -> None:
        with self.assertRaises(ImageRequestValidationError):
            build_image_generation_response([b"bytes"], response_format="url")


if __name__ == "__main__":
    unittest.main()
