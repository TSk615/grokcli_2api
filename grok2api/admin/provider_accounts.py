"""Admin import service for encrypted Web and Console credentials."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from grok2api import config
from grok2api.providers.types import ProviderName
from grok2api.store import accounts_pg
from grok2api.store.provider_credentials import store_provider_credential


class ProviderImportError(ValueError):
    pass


def _account_id(provider: ProviderName, source_key: str) -> str:
    digest = hashlib.sha256(source_key.encode("utf-8")).hexdigest()[:24]
    return f"{provider.value}::{digest}"


def import_provider_accounts(
    provider: ProviderName | str,
    payload: Any,
    *,
    web_tier: str | None = None,
    egress_identity: str | None = None,
) -> dict[str, Any]:
    """Parse and persist browser credentials without returning secret fields."""
    normalized = ProviderName.normalize(provider)
    if normalized is ProviderName.BUILD:
        raise ProviderImportError("Build credentials use the legacy importer")
    if normalized is ProviderName.WEB and not config.WEB_PROVIDER_ENABLED:
        raise ProviderImportError("grok_web provider is disabled")
    if normalized is ProviderName.CONSOLE and not config.CONSOLE_PROVIDER_ENABLED:
        raise ProviderImportError("grok_console provider is disabled")
    config.validate_provider_security(
        web_enabled=normalized is ProviderName.WEB,
        console_enabled=normalized is ProviderName.CONSOLE,
    )

    try:
        if normalized is ProviderName.WEB:
            from grok2api.providers.web.auth import parse_web_credentials
            from grok2api.providers.web.models import WebTier

            credentials = parse_web_credentials(payload)
            tier = WebTier(str(web_tier or "basic").strip().lower()).value
        else:
            from grok2api.providers.console.auth import parse_credentials

            serialized = (
                json.dumps(payload, ensure_ascii=False)
                if not isinstance(payload, (str, bytes))
                else payload
            )
            credentials = parse_credentials(serialized)
            tier = None
    except (TypeError, ValueError) as exc:
        # Parser exceptions are designed to be secret-safe; do not attach the
        # original payload or preserve a traceback containing it.
        raise ProviderImportError(str(exc)) from None

    imported: list[dict[str, Any]] = []
    for credential in credentials:
        source_key = str(getattr(credential, "source_key", "") or "")
        if normalized is ProviderName.WEB:
            source_key = str(getattr(credential, "fingerprint", "") or "")
        existing = accounts_pg.find_account_by_source(normalized.value, source_key)
        account_id = existing[0] if existing else _account_id(normalized, source_key)
        store_provider_credential(
            account_id,
            credential,
            web_tier=tier,
            egress_identity=(str(egress_identity).strip() if egress_identity else None),
        )
        imported.append(
            {
                "id": account_id,
                "provider": normalized.value,
                "auth_type": "sso",
                "source_key": source_key,
                "web_tier": tier,
                "egress_identity": (
                    str(egress_identity).strip() if egress_identity else None
                ),
            }
        )
    return {"ok": True, "provider": normalized.value, "count": len(imported), "accounts": imported}
