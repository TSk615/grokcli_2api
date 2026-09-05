"""Provider registry and shared contracts for Grok upstreams.

The existing application historically spoke only to the Grok Build
``cli-chat-proxy`` endpoint.  This package introduces a provider boundary so
Build, Web and Console can coexist without sharing credentials or pool state.
"""

from .registry import (
    AmbiguousModelRouteError,
    ProviderNotFoundError,
    ProviderRegistry,
    RouteNotFoundError,
    parse_model_reference,
)
from .types import Capability, ErrorKind, ModelRoute, ProviderName, ProviderStatus

__all__ = [
    "AmbiguousModelRouteError",
    "Capability",
    "ErrorKind",
    "ModelRoute",
    "ProviderName",
    "ProviderNotFoundError",
    "ProviderRegistry",
    "ProviderStatus",
    "RouteNotFoundError",
    "parse_model_reference",
    "create_provider_registry",
    "append_optional_provider_models",
]


def __getattr__(name: str):
    """Lazily expose helpers which otherwise import concrete transports."""
    if name == "create_provider_registry":
        from .factory import create_provider_registry

        return create_provider_registry
    if name == "append_optional_provider_models":
        from .catalog import append_optional_provider_models

        return append_optional_provider_models
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
