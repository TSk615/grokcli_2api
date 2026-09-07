"""Isolated Grok Console provider primitives.

The package deliberately has no dependency on the gateway/provider registry so
it can be integrated without creating an import cycle during the provider-core
migration.
"""

from .auth import ConsoleCredential, CredentialImportError, parse_credentials
from .dpop import (
    DPoPSession,
    DPoPSessionCache,
    create_dpop_proof,
    jwk_thumbprint,
    public_jwk,
)
from .errors import ConsoleErrorKind, ErrorClassification, classify_error
from .models import CATALOG, ModelSpec, list_models, resolve_model

__all__ = [
    "CATALOG",
    "ConsoleCredential",
    "ConsoleDPoPClient",
    "ConsoleDPoPConfig",
    "ConsoleErrorKind",
    "ConsoleProviderAdapter",
    "ConsoleResponsesError",
    "ConsoleResponsesTransport",
    "ConsoleGateway",
    "ConsoleGatewayResult",
    "ConsoleImage",
    "ConsoleVideo",
    "ConsoleMediaGateway",
    "ConsoleMediaError",
    "CredentialImportError",
    "DPoPSession",
    "DPoPSessionCache",
    "ErrorClassification",
    "ModelSpec",
    "classify_error",
    "create_dpop_proof",
    "jwk_thumbprint",
    "list_models",
    "parse_credentials",
    "public_jwk",
    "resolve_model",
]


def __getattr__(name: str):
    """Keep credential/auth imports independent of the HTTP transport stack."""
    if name == "ConsoleProviderAdapter":
        from .adapter import ConsoleProviderAdapter

        return ConsoleProviderAdapter
    if name in {"ConsoleDPoPClient", "ConsoleDPoPConfig"}:
        from .client import ConsoleDPoPClient, ConsoleDPoPConfig

        return {
            "ConsoleDPoPClient": ConsoleDPoPClient,
            "ConsoleDPoPConfig": ConsoleDPoPConfig,
        }[name]
    if name in {"ConsoleResponsesError", "ConsoleResponsesTransport"}:
        from .responses import ConsoleResponsesError, ConsoleResponsesTransport

        return {
            "ConsoleResponsesError": ConsoleResponsesError,
            "ConsoleResponsesTransport": ConsoleResponsesTransport,
        }[name]
    if name in {"ConsoleGateway", "ConsoleGatewayResult"}:
        from .gateway import ConsoleGateway, ConsoleGatewayResult

        return {
            "ConsoleGateway": ConsoleGateway,
            "ConsoleGatewayResult": ConsoleGatewayResult,
        }[name]
    if name in {"ConsoleImage", "ConsoleVideo", "ConsoleMediaGateway", "ConsoleMediaError"}:
        from .media import ConsoleImage, ConsoleMediaError, ConsoleMediaGateway, ConsoleVideo

        return {
            "ConsoleImage": ConsoleImage,
            "ConsoleVideo": ConsoleVideo,
            "ConsoleMediaGateway": ConsoleMediaGateway,
            "ConsoleMediaError": ConsoleMediaError,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
