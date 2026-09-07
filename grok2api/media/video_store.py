"""Content-addressed storage for generated Console videos."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from grok2api.config import DATA_DIR


MAX_VIDEO_BYTES = 256 * 1024 * 1024


class VideoStoreError(ValueError):
    pass


class VideoMediaStore:
    def __init__(self, root: str | Path | None = None, *, max_bytes: int = MAX_VIDEO_BYTES) -> None:
        self.root = Path(root) if root is not None else DATA_DIR / "media" / "videos"
        self.max_bytes = int(max_bytes)

    def store(self, data: bytes, *, content_type: str = "video/mp4") -> tuple[str, int]:
        if not isinstance(data, bytes) or not data:
            raise VideoStoreError("video data is empty")
        if len(data) > self.max_bytes:
            raise VideoStoreError("video exceeds configured limit")
        if not str(content_type).lower().startswith("video/"):
            raise VideoStoreError("invalid video content type")
        digest = hashlib.sha256(data).hexdigest()
        extension = ".webm" if "webm" in content_type.lower() else ".mp4"
        self.root.mkdir(parents=True, exist_ok=True)
        destination = (self.root / f"{digest}{extension}").resolve()
        if destination.parent != self.root.resolve():
            raise VideoStoreError("video path escapes media directory")
        if not destination.exists():
            temporary_name: str | None = None
            try:
                with tempfile.NamedTemporaryFile(mode="wb", prefix=".video-", suffix=".tmp", dir=self.root, delete=False) as handle:
                    temporary_name = handle.name
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary_name, destination)
                temporary_name = None
            finally:
                if temporary_name:
                    try:
                        os.unlink(temporary_name)
                    except FileNotFoundError:
                        pass
        return f"{digest}{extension}", len(data)


__all__ = ["MAX_VIDEO_BYTES", "VideoMediaStore", "VideoStoreError"]
