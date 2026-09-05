"""Adapter around the existing Grok Build/CLI implementation."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from grok2api.config import UPSTREAM_BASE
from grok2api.pool.auth import upstream_headers
from grok2api.upstream.models import load_models_from_cache, resolve_model

from ..types import Capability, ErrorKind, ModelRoute, ProviderName, ProviderStatus


class BuildProvider:
    provider = ProviderName.BUILD

    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = str(base_url or UPSTREAM_BASE).strip().rstrip("/")
        if not self.base_url:
            raise ValueError("Build provider base URL is required")

    def endpoint(self, path: str) -> str:
        return f"{self.base_url}/{str(path or '').lstrip('/')}"

    def headers(
        self,
        token: str,
        model: str,
        conversation_id: str | None = None,
    ) -> dict[str, str]:
        return upstream_headers(token, resolve_model(model), conversation_id)

    def model_routes(self) -> Iterable[ModelRoute]:
        try:
            catalog = load_models_from_cache()
        except Exception:
            catalog = []
        routes: list[ModelRoute] = []
        for item in catalog or []:
            if not isinstance(item, dict):
                continue
            model_id = str(item.get("id") or "").strip()
            if not model_id:
                continue
            routes.append(
                ModelRoute(
                    public_model=model_id,
                    provider=self.provider,
                    upstream_model=resolve_model(model_id),
                    capability=Capability.CHAT,
                    metadata={"synthetic": bool(item.get("synthetic"))},
                )
            )
        return routes

    def classify_status(
        self,
        status_code: int,
        *,
        headers: dict[str, str] | None = None,
        body: str | bytes | None = None,
    ) -> ProviderStatus:
        del headers
        text = body.decode("utf-8", "replace") if isinstance(body, bytes) else str(body or "")
        lowered = text.lower()
        if status_code == 401:
            return ProviderStatus(ErrorKind.AUTH, account_scoped=True, invalidate_credential=True)
        if status_code in (402, 429) or any(
            marker in lowered for marker in ("quota exceeded", "insufficient quota", "额度", "配额")
        ):
            return ProviderStatus(
                ErrorKind.QUOTA if status_code == 402 else ErrorKind.RATE_LIMIT,
                retryable=True,
                account_scoped=True,
            )
        if status_code in (408, 409, 425) or status_code >= 500:
            return ProviderStatus(ErrorKind.TRANSIENT, retryable=True)
        if status_code == 403:
            # Existing Build behavior retries another account.  Do not treat a
            # generic 403 as permanent credential revocation.
            return ProviderStatus(ErrorKind.UPSTREAM, retryable=True, account_scoped=True)
        if 400 <= status_code < 500:
            return ProviderStatus(ErrorKind.REQUEST)
        return ProviderStatus(ErrorKind.UPSTREAM)
