"""Provider registration and strict model-route resolution."""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from .base import ProviderAdapter
from .types import Capability, ModelRoute, ProviderName


class ProviderRegistryError(LookupError):
    pass


class ProviderNotFoundError(ProviderRegistryError):
    pass


class RouteNotFoundError(ProviderRegistryError):
    pass


class AmbiguousModelRouteError(ProviderRegistryError):
    pass


def parse_model_reference(model: str) -> tuple[ProviderName | None, str]:
    """Parse ``Build/foo``, ``Web/foo`` or ``Console/foo`` model names."""

    value = str(model or "").strip()
    if not value:
        raise ValueError("model is required")
    if "/" not in value:
        return None, value
    prefix, slug = value.split("/", 1)
    slug = slug.strip()
    if not slug:
        raise ValueError("model name after provider prefix is required")
    try:
        return ProviderName.normalize(prefix), slug
    except ValueError:
        # A slash can be a legitimate part of an OpenRouter-style model.  Only
        # reserve prefixes that actually name one of our providers.
        return None, value


class ProviderRegistry:
    """In-memory adapter registry with no implicit cross-provider failover."""

    def __init__(self, adapters: Iterable[ProviderAdapter] = ()) -> None:
        self._adapters: dict[ProviderName, ProviderAdapter] = {}
        for adapter in adapters:
            self.register(adapter)

    def register(self, adapter: ProviderAdapter, *, replace: bool = False) -> None:
        provider = ProviderName.normalize(adapter.provider)
        if provider in self._adapters and not replace:
            raise ValueError(f"provider already registered: {provider.value}")
        self._adapters[provider] = adapter

    def get(self, provider: ProviderName | str) -> ProviderAdapter:
        normalized = ProviderName.normalize(provider)
        try:
            return self._adapters[normalized]
        except KeyError as exc:
            raise ProviderNotFoundError(normalized.value) from exc

    def providers(self) -> tuple[ProviderName, ...]:
        return tuple(self._adapters)

    def routes(
        self,
        *,
        provider: ProviderName | str | None = None,
        capability: Capability | str | None = None,
        enabled_only: bool = True,
    ) -> list[ModelRoute]:
        provider_filter = ProviderName.normalize(provider) if provider is not None else None
        capability_filter = Capability.normalize(capability) if capability is not None else None
        values: list[ModelRoute] = []
        for current_provider, adapter in self._adapters.items():
            if provider_filter is not None and current_provider != provider_filter:
                continue
            for route in adapter.model_routes():
                if enabled_only and not route.enabled:
                    continue
                if capability_filter is not None and route.capability != capability_filter:
                    continue
                values.append(route)
        values.sort(key=lambda route: (route.priority, route.provider.value, route.public_model))
        return values

    def resolve(
        self,
        model: str,
        *,
        capability: Capability | str = Capability.CHAT,
        provider: ProviderName | str | None = None,
    ) -> ModelRoute:
        prefixed_provider, public_model = parse_model_reference(model)
        requested_provider = ProviderName.normalize(provider) if provider is not None else None
        if prefixed_provider is not None:
            if requested_provider is not None and prefixed_provider != requested_provider:
                raise RouteNotFoundError(
                    f"model prefix {prefixed_provider.namespace} conflicts with provider "
                    f"{requested_provider.value}"
                )
            requested_provider = prefixed_provider

        wanted_capability = Capability.normalize(capability)
        candidates = [
            route
            for route in self.routes(
                provider=requested_provider,
                capability=wanted_capability,
                enabled_only=True,
            )
            if route.public_model == public_model or route.upstream_model == public_model
        ]
        if not candidates:
            raise RouteNotFoundError(
                f"no route for model={model!r} capability={wanted_capability.value}"
            )

        by_provider: dict[ProviderName, list[ModelRoute]] = defaultdict(list)
        for candidate in candidates:
            by_provider[candidate.provider].append(candidate)
        if requested_provider is None and len(by_provider) > 1:
            choices = ", ".join(sorted(route.qualified_model for route in candidates))
            raise AmbiguousModelRouteError(
                f"model {public_model!r} exists in multiple providers; use one of: {choices}"
            )
        return min(candidates, key=lambda route: route.priority)
