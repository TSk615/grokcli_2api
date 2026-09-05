"""Feature-flagged construction of the provider discovery registry."""

from __future__ import annotations

from typing import TYPE_CHECKING

from grok2api import config

from .build import BuildProvider
from .registry import ProviderRegistry

if TYPE_CHECKING:
    import httpx


def create_provider_registry(
    *,
    web_enabled: bool | None = None,
    console_enabled: bool | None = None,
    secret_key: str | None = None,
    web_client: "httpx.AsyncClient | None" = None,
) -> ProviderRegistry:
    """Create the default registry without enabling implicit cross-pool routing.

    Build is always registered for backward compatibility. Web and Console are
    opt-in and the security gate is evaluated before either adapter is loaded.
    The optional Web client lets runtime code attach its process-local shared
    transport; catalog-only callers may leave it unset.
    """

    use_web = config.WEB_PROVIDER_ENABLED if web_enabled is None else bool(web_enabled)
    use_console = (
        config.CONSOLE_PROVIDER_ENABLED
        if console_enabled is None
        else bool(console_enabled)
    )
    config.validate_provider_security(
        web_enabled=use_web,
        console_enabled=use_console,
        secret_key=secret_key,
    )

    registry = ProviderRegistry([BuildProvider()])
    if use_web:
        from .web import GrokWebAdapter

        registry.register(
            GrokWebAdapter(web_client, base_url=config.WEB_PROVIDER_BASE_URL)
        )
    if use_console:
        from .console.adapter import ConsoleProviderAdapter

        registry.register(ConsoleProviderAdapter())
    return registry
