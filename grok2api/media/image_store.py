"""Content-addressed storage for generated raster images.

The store deliberately accepts bytes, not arbitrary source paths.  File names
are derived from the image digest and a MIME type detected from its magic
bytes, so neither upstream URLs nor client supplied names can influence the
filesystem path.
"""

from __future__ import annotations

import base64
import hashlib
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from grok2api.config import DATA_DIR


DEFAULT_IMAGE_MAX_BYTES = 20 * 1024 * 1024
DEFAULT_PUBLIC_PREFIX = "/v1/media/images"

_MIME_TO_EXTENSION = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/avif": ".avif",
}
_EXTENSION_TO_MIME = {extension: mime for mime, extension in _MIME_TO_EXTENSION.items()}
_STORED_NAME_RE = re.compile(r"^(?P<digest>[0-9a-f]{64})(?P<extension>\.[a-z0-9]+)$")


class ImageMediaError(ValueError):
    """Base class for safe, client-presentable image storage failures."""


class InvalidImageError(ImageMediaError):
    """The supplied bytes are not an allowed raster image."""


class ImageTooLargeError(ImageMediaError):
    """The supplied image exceeds the configured storage limit."""


class ImageNotFoundError(ImageMediaError):
    """A public image path does not identify an existing stored image."""


def image_bytes_to_b64(data: bytes) -> str:
    """Return an OpenAI ``b64_json`` compatible base64 value."""

    return base64.b64encode(data).decode("ascii")


def _sniff_image_mime(data: bytes) -> str | None:
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    # ISO BMFF: bytes 4..8 contain ``ftyp`` followed by a major AVIF brand.
    if len(data) >= 16 and data[4:8] == b"ftyp" and data[8:12] in (b"avif", b"avis"):
        return "image/avif"
    return None


def _normalise_content_type(value: str | None) -> str | None:
    if not value:
        return None
    return value.split(";", 1)[0].strip().lower() or None


@dataclass(frozen=True)
class StoredImage:
    """Metadata for an image whose bytes are owned by :class:`ImageMediaStore`."""

    image_id: str
    filename: str
    content_type: str
    size: int
    path: Path
    public_path: str

    def read_bytes(self) -> bytes:
        return self.path.read_bytes()

    def as_openai(self, response_format: str = "url", *, base_url: str = "") -> dict[str, str]:
        """Build one item for the OpenAI Images response ``data`` array."""

        if response_format == "b64_json":
            return {"b64_json": image_bytes_to_b64(self.read_bytes())}
        if response_format != "url":
            raise ValueError("response_format must be 'url' or 'b64_json'")
        if base_url:
            return {"url": f"{base_url.rstrip('/')}{self.public_path}"}
        return {"url": self.public_path}


class ImageMediaStore:
    """Store and retrieve generated images beneath one configured directory."""

    def __init__(
        self,
        root: str | Path | None = None,
        *,
        public_prefix: str = DEFAULT_PUBLIC_PREFIX,
        max_bytes: int = DEFAULT_IMAGE_MAX_BYTES,
    ) -> None:
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        prefix = "/" + public_prefix.strip("/")
        if prefix == "/" or ".." in PurePosixPath(prefix).parts:
            raise ValueError("public_prefix must be a non-root URL path")
        self.root = Path(root) if root is not None else DATA_DIR / "media" / "images"
        self.public_prefix = prefix
        self.max_bytes = int(max_bytes)

    def store(self, data: bytes, *, declared_content_type: str | None = None) -> StoredImage:
        """Validate and atomically persist image bytes, deduplicated by digest."""

        if not isinstance(data, bytes):
            raise TypeError("image data must be bytes")
        if len(data) > self.max_bytes:
            raise ImageTooLargeError(
                f"image exceeds configured limit of {self.max_bytes} bytes"
            )
        sniffed_type = _sniff_image_mime(data)
        if sniffed_type not in _MIME_TO_EXTENSION:
            raise InvalidImageError("unsupported or unrecognised raster image")
        declared_type = _normalise_content_type(declared_content_type)
        if declared_type not in (None, "application/octet-stream", sniffed_type):
            raise InvalidImageError("declared content type does not match image bytes")

        digest = hashlib.sha256(data).hexdigest()
        filename = digest + _MIME_TO_EXTENSION[sniffed_type]
        self.root.mkdir(parents=True, exist_ok=True)
        destination = self._safe_destination(filename)

        if destination.exists():
            return self._metadata_for_existing(destination, digest, sniffed_type)

        temporary_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=".image-", suffix=".tmp", dir=self.root, delete=False
            ) as temporary:
                temporary_name = temporary.name
                temporary.write(data)
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_name, destination)
            temporary_name = None
        finally:
            if temporary_name:
                try:
                    os.unlink(temporary_name)
                except FileNotFoundError:
                    pass

        return self._build_metadata(destination, digest, sniffed_type, len(data))

    def resolve_public_path(self, public_path: str) -> StoredImage:
        """Strictly resolve a store-owned public relative path to image metadata."""

        if not isinstance(public_path, str):
            raise ImageNotFoundError("invalid image path")
        expected_prefix = self.public_prefix + "/"
        if not public_path.startswith(expected_prefix):
            raise ImageNotFoundError("image path is outside the media prefix")
        filename = public_path[len(expected_prefix) :]
        if not filename or "/" in filename or "\\" in filename or "%" in filename:
            raise ImageNotFoundError("invalid image path")
        match = _STORED_NAME_RE.fullmatch(filename)
        if match is None:
            raise ImageNotFoundError("invalid image identifier")
        extension = match.group("extension")
        content_type = _EXTENSION_TO_MIME.get(extension)
        if content_type is None:
            raise ImageNotFoundError("unsupported image extension")
        destination = self._safe_destination(filename)
        if not destination.is_file():
            raise ImageNotFoundError("stored image does not exist")
        return self._metadata_for_existing(
            destination, match.group("digest"), content_type
        )

    def _safe_destination(self, filename: str) -> Path:
        root = self.root.resolve()
        destination = (root / filename).resolve()
        if destination.parent != root:
            raise ImageMediaError("image path escapes the media directory")
        return destination

    def _metadata_for_existing(
        self, destination: Path, expected_digest: str, expected_type: str
    ) -> StoredImage:
        try:
            data = destination.read_bytes()
        except OSError as exc:
            raise ImageNotFoundError("stored image cannot be read") from exc
        if len(data) > self.max_bytes:
            raise ImageTooLargeError("stored image exceeds configured limit")
        actual_type = _sniff_image_mime(data)
        actual_digest = hashlib.sha256(data).hexdigest()
        if actual_type != expected_type or actual_digest != expected_digest:
            raise InvalidImageError("stored image failed integrity validation")
        return self._build_metadata(destination, expected_digest, expected_type, len(data))

    def _build_metadata(
        self, destination: Path, digest: str, content_type: str, size: int
    ) -> StoredImage:
        return StoredImage(
            image_id=digest,
            filename=destination.name,
            content_type=content_type,
            size=size,
            path=destination,
            public_path=f"{self.public_prefix}/{destination.name}",
        )
