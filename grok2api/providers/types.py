"""Small, dependency-free types shared by provider implementations."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping


class ProviderName(str, Enum):
    BUILD = "grok_build"
    WEB = "grok_web"
    CONSOLE = "grok_console"

    @property
    def namespace(self) -> str:
        return {
            ProviderName.BUILD: "Build",
            ProviderName.WEB: "Web",
            ProviderName.CONSOLE: "Console",
        }[self]

    @classmethod
    def normalize(cls, value: "ProviderName | str") -> "ProviderName":
        if isinstance(value, cls):
            return value
        text = str(value or "").strip().lower().replace("-", "_")
        aliases = {
            "build": cls.BUILD,
            "grok_build": cls.BUILD,
            "web": cls.WEB,
            "grok_web": cls.WEB,
            "console": cls.CONSOLE,
            "grok_console": cls.CONSOLE,
        }
        try:
            return aliases[text]
        except KeyError as exc:
            raise ValueError(f"unsupported provider: {value!r}") from exc


class Capability(str, Enum):
    CHAT = "chat"
    RESPONSES = "responses"
    MESSAGES = "messages"
    IMAGE = "image"
    IMAGE_EDIT = "image_edit"
    VIDEO = "video"
    TTS = "tts"
    STT = "stt"
    REALTIME = "realtime"

    @classmethod
    def normalize(cls, value: "Capability | str") -> "Capability":
        if isinstance(value, cls):
            return value
        text = str(value or "").strip().lower().replace("-", "_")
        aliases = {
            "chat_completions": cls.CHAT,
            "chat/completions": cls.CHAT,
            "anthropic": cls.MESSAGES,
            "image_generation": cls.IMAGE,
            "images": cls.IMAGE,
            "images_edits": cls.IMAGE_EDIT,
            "audio_speech": cls.TTS,
            "audio_transcription": cls.STT,
        }
        if text in aliases:
            return aliases[text]
        try:
            return cls(text)
        except ValueError as exc:
            raise ValueError(f"unsupported capability: {value!r}") from exc


class ErrorKind(str, Enum):
    REQUEST = "request"
    AUTH = "auth"
    QUOTA = "quota"
    RATE_LIMIT = "rate_limit"
    EGRESS = "egress"
    UPSTREAM = "upstream"
    TRANSIENT = "transient"


@dataclass(frozen=True, slots=True)
class ProviderStatus:
    """Provider-specific interpretation of an upstream response status."""

    kind: ErrorKind
    retryable: bool = False
    account_scoped: bool = False
    invalidate_credential: bool = False
    retry_after_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class ModelRoute:
    """One public-model/capability route to a concrete provider model."""

    public_model: str
    provider: ProviderName
    upstream_model: str
    capability: Capability = Capability.CHAT
    priority: int = 1
    enabled: bool = True
    minimum_tier: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider", ProviderName.normalize(self.provider))
        object.__setattr__(self, "capability", Capability.normalize(self.capability))
        public_model = str(self.public_model or "").strip()
        upstream_model = str(self.upstream_model or "").strip()
        if not public_model:
            raise ValueError("public_model is required")
        if not upstream_model:
            raise ValueError("upstream_model is required")
        object.__setattr__(self, "public_model", public_model)
        object.__setattr__(self, "upstream_model", upstream_model)
        object.__setattr__(self, "priority", max(1, int(self.priority or 1)))

    @property
    def qualified_model(self) -> str:
        return f"{self.provider.namespace}/{self.public_model}"

    @property
    def identity(self) -> tuple[str, str, str]:
        return (self.provider.value, self.upstream_model, self.capability.value)
