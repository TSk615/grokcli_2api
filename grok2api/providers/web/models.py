"""Static model metadata for the Grok Web provider."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class WebTier(str, Enum):
    """Known grok.com subscription tiers in ascending capability order."""

    BASIC = "basic"
    SUPER = "super"
    HEAVY = "heavy"


class WebCapability(str, Enum):
    CHAT = "chat"
    STREAMING = "streaming"
    CITATIONS = "citations"
    REASONING = "reasoning"
    IMAGE = "image"


_TIER_RANK = {
    WebTier.BASIC: 0,
    WebTier.SUPER: 1,
    WebTier.HEAVY: 2,
}


@dataclass(frozen=True, slots=True)
class WebModel:
    """A public model route and the minimum account tier it requires."""

    id: str
    upstream_mode: str
    minimum_tier: WebTier
    capabilities: frozenset[WebCapability]
    description: str
    protocol_model: str = ""
    imagine_pro: bool = False

    def supports(self, capability: WebCapability | str) -> bool:
        try:
            normalized = WebCapability(capability)
        except ValueError:
            return False
        return normalized in self.capabilities

    def available_to(self, tier: WebTier | str) -> bool:
        try:
            normalized = tier if isinstance(tier, WebTier) else WebTier(str(tier).lower())
        except ValueError:
            return False
        return _TIER_RANK[normalized] >= _TIER_RANK[self.minimum_tier]

    def as_public_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "object": "model",
            "owned_by": "xai",
            "provider": "grok_web",
            "upstream_model": self.upstream_mode,
            "minimum_tier": self.minimum_tier.value,
            "capabilities": sorted(item.value for item in self.capabilities),
            "description": self.description,
        }


_BASE_CAPABILITIES = frozenset(
    {WebCapability.CHAT, WebCapability.STREAMING, WebCapability.CITATIONS}
)

_IMAGE_CAPABILITIES = frozenset({WebCapability.IMAGE})

WEB_MODELS: tuple[WebModel, ...] = (
    WebModel(
        id="grok-chat-fast",
        upstream_mode="fast",
        minimum_tier=WebTier.BASIC,
        capabilities=_BASE_CAPABILITIES,
        description="Low-latency Grok Web chat mode.",
    ),
    WebModel(
        id="grok-chat-auto",
        upstream_mode="auto",
        # Keep the package's original catalog contract. The reference repo's
        # current account routing raises this to Super, but tier enforcement
        # belongs to account capability sync rather than protocol conversion.
        minimum_tier=WebTier.BASIC,
        capabilities=_BASE_CAPABILITIES | {WebCapability.REASONING},
        description="Grok Web automatic model selection mode.",
    ),
    WebModel(
        id="grok-chat-expert",
        upstream_mode="expert",
        minimum_tier=WebTier.SUPER,
        capabilities=_BASE_CAPABILITIES | {WebCapability.REASONING},
        description="Higher-reasoning Grok Web expert mode.",
    ),
    WebModel(
        id="grok-chat-heavy",
        upstream_mode="heavy",
        minimum_tier=WebTier.HEAVY,
        capabilities=_BASE_CAPABILITIES | {WebCapability.REASONING},
        description="Grok Web Heavy-tier reasoning mode.",
    ),
    WebModel(
        id="grok-imagine-image-lite",
        upstream_mode="grok-imagine-image",
        minimum_tier=WebTier.BASIC,
        capabilities=_IMAGE_CAPABILITIES,
        description="Fast Grok Web image generation mode.",
        protocol_model="imagine-lite",
    ),
    WebModel(
        id="grok-imagine-image",
        upstream_mode="grok-imagine-image-quality",
        minimum_tier=WebTier.BASIC,
        capabilities=_IMAGE_CAPABILITIES,
        description="Grok Web Imagine image generation mode.",
        protocol_model="imagine",
    ),
    WebModel(
        id="grok-imagine-image-2.0",
        upstream_mode="grok-imagine-image-2.0",
        minimum_tier=WebTier.BASIC,
        capabilities=_IMAGE_CAPABILITIES,
        description="Grok Web Imagine 2.0 Pro image generation mode.",
        protocol_model="imagine",
        imagine_pro=True,
    ),
)


def list_web_models(tier: WebTier | str | None = None) -> list[dict[str, object]]:
    """Return catalog entries, optionally filtered for an account tier."""

    models = WEB_MODELS
    if tier is not None:
        models = tuple(model for model in models if model.available_to(tier))
    return [model.as_public_dict() for model in models]


def get_web_model(model_id: str) -> WebModel | None:
    normalized = str(model_id or "").strip().lower()
    return next((model for model in WEB_MODELS if model.id == normalized), None)
