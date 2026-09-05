"""Static Grok Console model and capability catalog."""

from __future__ import annotations

from dataclasses import dataclass


RESPONSES = "responses"
CHAT = "chat"
MESSAGES = "messages"
IMAGE = "image"
IMAGE_EDIT = "image_edit"
VIDEO = "video"
TTS = "tts"
STT = "stt"
REALTIME = "realtime"


@dataclass(frozen=True, slots=True)
class ModelSpec:
    public_id: str
    upstream_model: str
    capabilities: tuple[str, ...]
    supports_reasoning: bool = False
    supports_reasoning_effort: bool = False
    default_reasoning_effort: str | None = None
    max_output_tokens: int | None = None


CATALOG: tuple[ModelSpec, ...] = (
    ModelSpec("grok-4.3", "grok-4.3", (RESPONSES, CHAT, MESSAGES), True, True, "medium", 1_000_000),
    ModelSpec(
        "grok-4.20-0309-reasoning",
        "grok-4.20-0309-reasoning",
        (RESPONSES, CHAT, MESSAGES),
        True,
        False,
        None,
        1_000_000,
    ),
    ModelSpec(
        "grok-4.20-0309-non-reasoning",
        "grok-4.20-0309-non-reasoning",
        (RESPONSES, CHAT, MESSAGES),
        False,
        False,
        None,
        1_000_000,
    ),
    ModelSpec(
        "grok-4.20-multi-agent-0309",
        "grok-4.20-multi-agent-0309",
        (RESPONSES, CHAT, MESSAGES),
        True,
        True,
        None,
        1_000_000,
    ),
    ModelSpec("grok-4.5", "grok-4.5", (RESPONSES, CHAT, MESSAGES), True, True, "medium", 1_000_000),
    ModelSpec(
        "grok-build-0.1",
        "grok-build-0.1",
        (RESPONSES, CHAT, MESSAGES),
        max_output_tokens=256_000,
    ),
    ModelSpec("grok-imagine-image", "grok-imagine-image", (IMAGE, IMAGE_EDIT)),
    ModelSpec("grok-imagine-image-quality", "grok-imagine-image-quality", (IMAGE, IMAGE_EDIT)),
    ModelSpec("grok-imagine-image-2.0", "grok-imagine-image-2.0", (IMAGE, IMAGE_EDIT)),
    ModelSpec("grok-imagine-video", "grok-imagine-video", (VIDEO,)),
    ModelSpec("grok-imagine-video-1.5", "grok-imagine-video-1.5", (VIDEO,)),
    ModelSpec("grok-voice-latest", "grok-voice-latest", (REALTIME, TTS)),
    ModelSpec("grok-voice-think-fast-2.0", "grok-voice-think-fast-2.0", (REALTIME, TTS)),
    ModelSpec("grok-voice-think-fast-1.0", "grok-voice-think-fast-1.0", (REALTIME, TTS)),
    ModelSpec("grok-stt", "grok-stt", (STT,)),
)

_BY_UPSTREAM = {item.upstream_model: item for item in CATALOG}


def list_models(*, capability: str | None = None) -> tuple[ModelSpec, ...]:
    """Return immutable catalog entries, optionally filtered by capability."""

    if capability is None:
        return CATALOG
    return tuple(item for item in CATALOG if capability in item.capabilities)


def resolve_model(upstream_model: str) -> ModelSpec | None:
    return _BY_UPSTREAM.get(upstream_model.strip())
