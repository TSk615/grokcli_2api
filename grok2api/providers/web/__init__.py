"""Isolated Grok Web provider building blocks.

The package intentionally has no dependency on the gateway/provider core so it
can be integrated incrementally without changing the existing Build provider.
"""

from .auth import WebCredential, parse_web_credentials
from .errors import WebError, WebErrorKind, classify_web_error
from .models import WEB_MODELS, WebCapability, WebModel, WebTier, list_web_models
from .protocol import (
    ConvertedWebChat,
    WebProtocolError,
    build_rest_chat_payload,
    convert_chat_completion,
    messages_to_prompt,
)
from .stream import (
    GrokWebStreamParser,
    WebCitation,
    WebDelta,
    WebDeltaKind,
    WebStreamError,
)

PROVIDER = "grok_web"

__all__ = [
    "DEFAULT_CHAT_PATH",
    "GrokWebAdapter",
    "GrokWebGateway",
    "GrokWebStreamParser",
    "PROVIDER",
    "WEB_MODELS",
    "WebCapability",
    "WebCitation",
    "WebCredential",
    "WebDelta",
    "WebDeltaKind",
    "WebError",
    "WebErrorKind",
    "WebGatewayAuthError",
    "WebGatewayEgressError",
    "WebGatewayError",
    "WebModel",
    "WebProtocolError",
    "WebStreamError",
    "WebTier",
    "classify_web_error",
    "gateway_endpoint",
    "gateway_headers",
    "build_rest_chat_payload",
    "convert_chat_completion",
    "list_web_models",
    "messages_to_prompt",
    "parse_web_credentials",
    "ConvertedWebChat",
    "GATEWAY_PATH",
    "SESSION_PATH",
]


def __getattr__(name: str):
    """Keep credential/auth imports independent of the HTTP transport stack."""
    if name in {"DEFAULT_CHAT_PATH", "GrokWebAdapter"}:
        from .adapter import DEFAULT_CHAT_PATH, GrokWebAdapter

        return {
            "DEFAULT_CHAT_PATH": DEFAULT_CHAT_PATH,
            "GrokWebAdapter": GrokWebAdapter,
        }[name]
    gateway_names = {
        "GATEWAY_PATH",
        "SESSION_PATH",
        "GrokWebGateway",
        "WebGatewayAuthError",
        "WebGatewayEgressError",
        "WebGatewayError",
        "gateway_endpoint",
        "gateway_headers",
    }
    if name in gateway_names:
        from . import gateway

        return getattr(gateway, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
