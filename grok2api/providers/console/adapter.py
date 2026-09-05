"""Compatibility adapter for the shared provider registry contract."""

from __future__ import annotations

from collections.abc import Iterable

from ..base import ProviderAdapter
from ..types import Capability, ErrorKind, ModelRoute, ProviderName, ProviderStatus
from .errors import ConsoleErrorKind, classify_error
from .models import CATALOG


_ERROR_KIND_MAP = {
    ConsoleErrorKind.INVALID_REQUEST: ErrorKind.REQUEST,
    ConsoleErrorKind.AUTHENTICATION: ErrorKind.AUTH,
    ConsoleErrorKind.DPOP_SESSION: ErrorKind.AUTH,
    ConsoleErrorKind.ACCOUNT_BLOCKED: ErrorKind.AUTH,
    ConsoleErrorKind.EGRESS_CHALLENGE: ErrorKind.EGRESS,
    ConsoleErrorKind.QUOTA: ErrorKind.QUOTA,
    ConsoleErrorKind.RATE_LIMIT: ErrorKind.RATE_LIMIT,
    ConsoleErrorKind.NOT_FOUND: ErrorKind.REQUEST,
    ConsoleErrorKind.UPSTREAM_TRANSIENT: ErrorKind.TRANSIENT,
    ConsoleErrorKind.UPSTREAM: ErrorKind.UPSTREAM,
}


class ConsoleProviderAdapter:
    """Discovery and error semantics; transport remains in ConsoleDPoPClient."""

    provider = ProviderName.CONSOLE

    def model_routes(self) -> Iterable[ModelRoute]:
        routes: list[ModelRoute] = []
        for model in CATALOG:
            metadata = {
                "supports_reasoning": model.supports_reasoning,
                "supports_reasoning_effort": model.supports_reasoning_effort,
                "default_reasoning_effort": model.default_reasoning_effort,
                "max_output_tokens": model.max_output_tokens,
            }
            for capability in model.capabilities:
                routes.append(
                    ModelRoute(
                        public_model=model.public_id,
                        provider=self.provider,
                        upstream_model=model.upstream_model,
                        capability=Capability.normalize(capability),
                        metadata=metadata,
                    )
                )
        return tuple(routes)

    def classify_status(
        self,
        status_code: int,
        *,
        headers: dict[str, str] | None = None,
        body: str | bytes | None = None,
    ) -> ProviderStatus:
        retry_after = None
        for name, value in (headers or {}).items():
            if str(name).lower() == "retry-after":
                retry_after = str(value)
                break
        classified = classify_error(status_code, body=body, retry_after=retry_after)
        account_scoped = classified.kind in {
            ConsoleErrorKind.AUTHENTICATION,
            ConsoleErrorKind.ACCOUNT_BLOCKED,
            ConsoleErrorKind.QUOTA,
            ConsoleErrorKind.RATE_LIMIT,
        }
        return ProviderStatus(
            kind=_ERROR_KIND_MAP[classified.kind],
            retryable=classified.retryable,
            account_scoped=account_scoped,
            invalidate_credential=classified.credential_invalid,
            retry_after_seconds=classified.retry_after_seconds,
        )


# Structural check for maintainers and type checkers; no runtime registration.
_provider_adapter_contract: ProviderAdapter = ConsoleProviderAdapter()
