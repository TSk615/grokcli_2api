"""OpenAI-compatible image generation request/response helpers.

The Web image adapter is intentionally kept separate from the HTTP handler.  This
module contains only protocol validation and response shaping, so the same rules
can be used by FastAPI routes and by provider adapters.  The accepted values are
the values used by Grok's Imagine endpoint (see the upstream grok2api
``resolveImageAspectRatio`` implementation).
"""

from __future__ import annotations

import base64
import time
from typing import Any, Literal, Sequence

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


# Imagine accepts these ratios directly.  The dimension aliases are retained for
# OpenAI clients which send ``size`` instead of ``aspect_ratio``.
SUPPORTED_ASPECT_RATIOS: frozenset[str] = frozenset(
    {
        "auto",
        "1:1",
        "16:9",
        "9:16",
        "4:3",
        "3:4",
        "3:2",
        "2:3",
        "2:1",
        "1:2",
        "19.5:9",
        "9:19.5",
        "20:9",
        "9:20",
    }
)

SUPPORTED_SIZES: frozenset[str] = frozenset(
    {
        *SUPPORTED_ASPECT_RATIOS,
        "1280x720",
        "720x1280",
        "1792x1024",
        "1536x1024",
        "1024x1792",
        "1024x1536",
        "1024x1024",
    }
)


class ImageRequestValidationError(ValueError):
    """A client error that can be rendered as an OpenAI error envelope."""

    def __init__(self, message: str, *, code: str = "invalid_request") -> None:
        super().__init__(message)
        self.message = message
        self.code = code


class ImageGenerationRequest(BaseModel):
    """Validated subset of ``POST /v1/images/generations``.

    Unknown fields are preserved for forward compatibility with OpenAI clients,
    while fields implemented by this first Web-only release are validated here.
    ``stream`` is accepted so the handler can return a useful 400 rather than a
    generic validation error; streaming/partial images are not implemented yet.
    """

    model_config = ConfigDict(extra="allow", str_strip_whitespace=True)

    model: str = Field(min_length=1)
    prompt: str = Field(min_length=1)
    n: int = Field(default=1, ge=1, le=10)
    size: str | None = None
    aspect_ratio: str | None = None
    response_format: Literal["url", "b64_json"] = "url"
    stream: bool = False

    @field_validator("model", "prompt")
    @classmethod
    def _non_empty_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("model 和 prompt 不能为空")
        return value

    @field_validator("size")
    @classmethod
    def _validate_size(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().lower()
        if not value:
            return None
        if value not in SUPPORTED_SIZES:
            raise ValueError(
                "size 不受支持；请使用 auto、1:1、16:9、9:16、4:3、3:4、3:2、2:3 "
                "或受支持的像素尺寸"
            )
        return value

    @field_validator("aspect_ratio")
    @classmethod
    def _validate_aspect_ratio(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().lower()
        if not value:
            return None
        if value not in SUPPORTED_ASPECT_RATIOS:
            raise ValueError(
                "aspect_ratio 必须是 auto、1:1、16:9、9:16、4:3、3:4、3:2、2:3、2:1、1:2、19.5:9、9:19.5、20:9 或 9:20"
            )
        return value

    @field_validator("response_format", mode="before")
    @classmethod
    def _normalize_response_format(cls, value: Any) -> str:
        if value is None or value == "":
            return "url"
        if not isinstance(value, str):
            raise ValueError("response_format 必须是 url 或 b64_json")
        return value.strip().lower()

    @field_validator("stream")
    @classmethod
    def _reject_stream(cls, value: bool) -> bool:
        if value:
            raise ValueError("Web 图片首版暂不支持 stream=true")
        return value

    @classmethod
    def from_payload(cls, payload: Any) -> "ImageGenerationRequest":
        """Parse a JSON object and expose protocol errors consistently.

        FastAPI's automatic model parsing emits a 422 response.  Existing
        handlers in this project use OpenAI's 400 error envelope instead, so
        callers can use this method and map ``ImageRequestValidationError`` to
        ``openai_error(..., status=400, err_type='invalid_request_error')``.
        """

        if not isinstance(payload, dict):
            raise ImageRequestValidationError("请求体必须是 JSON 对象")
        try:
            return cls.model_validate(payload)
        except ValidationError as exc:
            # Pydantic's detailed path is useful in logs, but keep the client
            # response short and stable (and avoid exposing implementation
            # internals).
            errors = getattr(exc, "errors", lambda: [])()
            first = errors[0] if errors else {}
            location = ".".join(str(part) for part in first.get("loc", ()))
            message = str(first.get("msg") or "图片请求参数无效")
            if location:
                message = f"{location}: {message}"
            raise ImageRequestValidationError(message) from exc


def image_generation_error(exc: ImageRequestValidationError) -> dict[str, Any]:
    """Return an OpenAI-compatible error body for a validation exception."""

    return {
        "error": {
            "message": exc.message,
            "type": "invalid_request_error",
            "code": exc.code,
        }
    }


def build_image_generation_response(
    images: Sequence[str | bytes],
    *,
    response_format: Literal["url", "b64_json"] = "url",
    created: int | None = None,
) -> dict[str, Any]:
    """Build the OpenAI image response from URLs or raw image bytes.

    Web adapters normally return HTTPS URLs.  For ``b64_json`` callers may pass
    an already encoded string or raw bytes; raw bytes are encoded here to keep
    the provider code focused on transport/protocol parsing.
    """

    fmt = str(response_format or "url").strip().lower()
    if fmt not in {"url", "b64_json"}:
        raise ImageRequestValidationError("response_format 必须是 url 或 b64_json")

    key = "url" if fmt == "url" else "b64_json"
    data: list[dict[str, str]] = []
    for image in images:
        if isinstance(image, bytes):
            if fmt == "url":
                raise ImageRequestValidationError(
                    "response_format=url 时图片结果必须是 URL"
                )
            value = base64.b64encode(image).decode("ascii")
        elif isinstance(image, str) and image.strip():
            value = image.strip()
        else:
            raise ImageRequestValidationError("图片结果不能为空")
        data.append({key: value})
    if not data:
        raise ImageRequestValidationError("上游未返回图片结果", code="image_generation_empty")
    return {"created": int(created if created is not None else time.time()), "data": data}


__all__ = [
    "ImageGenerationRequest",
    "ImageRequestValidationError",
    "SUPPORTED_ASPECT_RATIOS",
    "SUPPORTED_SIZES",
    "build_image_generation_response",
    "image_generation_error",
]
