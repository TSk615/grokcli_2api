"""Public model-catalog projection for the multi-provider API surface."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping

from .registry import ProviderRegistry
from .types import ModelRoute, ProviderName


def append_optional_provider_models(
    build_models: Iterable[Mapping[str, Any]],
    registry: ProviderRegistry,
) -> list[dict[str, Any]]:
    """Keep legacy Build rows intact and append qualified optional models.

    Optional Provider IDs are always namespaced (``Web/...`` and
    ``Console/...``), so enabling another pool cannot silently change which
    account or upstream receives an existing unqualified Build model.
    """

    result = [dict(item) for item in build_models if isinstance(item, Mapping)]
    grouped: dict[tuple[ProviderName, str], list[ModelRoute]] = defaultdict(list)
    for route in registry.routes():
        if route.provider is ProviderName.BUILD:
            continue
        grouped[(route.provider, route.public_model)].append(route)

    for (provider, public_model), routes in sorted(
        grouped.items(), key=lambda item: (item[0][0].value, item[0][1])
    ):
        first = min(routes, key=lambda route: route.priority)
        metadata: dict[str, Any] = {}
        for route in routes:
            metadata.update(dict(route.metadata))
        result.append(
            {
                "id": f"{provider.namespace}/{public_model}",
                "object": "model",
                "owned_by": "xai",
                "provider": provider.value,
                "upstream_model": first.upstream_model,
                "capabilities": sorted({route.capability.value for route in routes}),
                "minimum_tier": first.minimum_tier,
                **metadata,
            }
        )
    return result
