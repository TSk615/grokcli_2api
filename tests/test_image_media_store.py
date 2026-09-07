from __future__ import annotations

import base64
import tempfile
import unittest
from pathlib import Path

from grok2api.media import (
    ImageMediaStore,
    ImageNotFoundError,
    ImageTooLargeError,
    InvalidImageError,
)


PNG = b"\x89PNG\r\n\x1a\n" + b"generated-image-payload"
JPEG = b"\xff\xd8\xff\xe0" + b"generated-jpeg-payload"


class ImageMediaStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "images"
        self.store = ImageMediaStore(self.root, max_bytes=1024)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_store_is_content_addressed_atomic_and_deduplicated(self) -> None:
        first = self.store.store(PNG, declared_content_type="image/png; charset=binary")
        second = self.store.store(PNG, declared_content_type="application/octet-stream")

        self.assertEqual(first, second)
        self.assertEqual(first.content_type, "image/png")
        self.assertEqual(first.size, len(PNG))
        self.assertEqual(len(first.image_id), 64)
        self.assertTrue(first.filename.endswith(".png"))
        self.assertEqual(first.path.read_bytes(), PNG)
        self.assertEqual(list(self.root.glob(".image-*.tmp")), [])
        self.assertEqual(len(list(self.root.glob("*.png"))), 1)

    def test_url_and_base64_openai_response_metadata(self) -> None:
        stored = self.store.store(JPEG)

        self.assertEqual(
            stored.as_openai(base_url="https://images.example.test/"),
            {"url": f"https://images.example.test{stored.public_path}"},
        )
        self.assertEqual(
            stored.as_openai("b64_json"),
            {"b64_json": base64.b64encode(JPEG).decode("ascii")},
        )
        with self.assertRaises(ValueError):
            stored.as_openai("unsupported")

    def test_resolve_public_path_verifies_content_and_digest(self) -> None:
        stored = self.store.store(PNG)
        resolved = self.store.resolve_public_path(stored.public_path)
        self.assertEqual(resolved, stored)

        stored.path.write_bytes(JPEG)
        with self.assertRaises(InvalidImageError):
            self.store.resolve_public_path(stored.public_path)

    def test_rejects_unknown_mismatched_and_oversized_images(self) -> None:
        with self.assertRaises(InvalidImageError):
            self.store.store(b"<svg onload='alert(1)'></svg>")
        with self.assertRaises(InvalidImageError):
            self.store.store(PNG, declared_content_type="image/jpeg")
        with self.assertRaises(ImageTooLargeError):
            ImageMediaStore(self.root, max_bytes=8).store(PNG)
        with self.assertRaises(TypeError):
            self.store.store(bytearray(PNG))  # type: ignore[arg-type]

    def test_public_path_parser_rejects_traversal_and_noncanonical_names(self) -> None:
        stored = self.store.store(PNG)
        invalid_paths = (
            f"/v1/media/images/../{stored.filename}",
            f"/v1/media/images/%2e%2e/{stored.filename}",
            f"/v1/media/images/{stored.filename}/extra",
            f"/v1/media/images\\{stored.filename}",
            "/v1/media/images/not-a-digest.png",
            f"/different-prefix/{stored.filename}",
            stored.public_path + "?download=1",
        )
        for path in invalid_paths:
            with self.subTest(path=path), self.assertRaises(ImageNotFoundError):
                self.store.resolve_public_path(path)

    def test_missing_digest_is_not_found(self) -> None:
        with self.assertRaises(ImageNotFoundError):
            self.store.resolve_public_path("/v1/media/images/" + "0" * 64 + ".png")


if __name__ == "__main__":
    unittest.main()
