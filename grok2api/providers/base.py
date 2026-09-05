"""Provider adapter protocol.

Network transports remain provider-owned because Build, Web and Console have
materially different authentication and streaming semantics.  The gateway
depends only on this deliberately small discovery/error contract.
"""

from __future__ import annotations

from typing import Iterable, Protocol, runtime_checkable

from .types import ModelRoute, ProviderName, ProviderStatus


@runtime_checkable
class ProviderAdapter(Protocol):
    provider: ProviderName

    def model_routes(self) -> Iterable[ModelRoute]: ...

    def classify_status(
        self,
        status_code: int,
        *,
        headers: dict[str, str] | None = None,
        body: str | bytes | None = None,
    ) -> ProviderStatus: ...
