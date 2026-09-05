"""Provider-isolated account selection for Web and Console credentials."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from grok2api.store import accounts_pg
from grok2api.store.provider_credentials import (
    Credential,
    load_provider_credential,
)

from .types import ProviderName


class ProviderAccountsUnavailable(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ProviderAccount:
    account_id: str
    provider: ProviderName
    credential: Credential = field(repr=False)
    source_key: str | None = None
    web_tier: str | None = None
    egress_identity: str | None = None


_local_lock = threading.Lock()
_local_indexes: dict[ProviderName, int] = {}
_TIER_RANK = {"basic": 0, "super": 1, "heavy": 2}


def _next_index(provider: ProviderName) -> int:
    try:
        from grok2api.store.redis_client import incr, key, redis_enabled

        if redis_enabled():
            value = incr(key("provider", provider.value, "rr"))
            if value is not None:
                return max(0, value - 1)
    except Exception:
        pass
    with _local_lock:
        current = _local_indexes.get(provider, 0)
        _local_indexes[provider] = current + 1
        return current


def _tier_eligible(actual: Any, minimum: str | None) -> bool:
    if not minimum:
        return True
    return _TIER_RANK.get(str(actual or "basic").lower(), -1) >= _TIER_RANK.get(
        str(minimum).lower(), 99
    )


def acquire_provider_sequence(
    provider: ProviderName | str,
    *,
    minimum_tier: str | None = None,
) -> list[ProviderAccount]:
    """Return one Provider's accounts in round-robin failover order."""
    normalized = ProviderName.normalize(provider)
    if normalized is ProviderName.BUILD:
        raise ValueError("Build accounts are managed by the legacy Build pool")
    rows = accounts_pg.list_provider_account_refs(normalized.value)
    rows = [row for row in rows if _tier_eligible(row.get("web_tier"), minimum_tier)]
    if not rows:
        raise ProviderAccountsUnavailable(
            f"no eligible {normalized.value} accounts"
        )
    start = _next_index(normalized) % len(rows)
    ordered = rows[start:] + rows[:start]
    result: list[ProviderAccount] = []
    for row in ordered:
        account_id = str(row.get("id") or "").strip()
        if not account_id:
            continue
        credential = load_provider_credential(account_id, normalized.value)
        if credential is None:
            continue
        result.append(
            ProviderAccount(
                account_id=account_id,
                provider=normalized,
                credential=credential,
                source_key=row.get("source_key"),
                web_tier=row.get("web_tier"),
                egress_identity=row.get("egress_identity"),
            )
        )
    if not result:
        raise ProviderAccountsUnavailable(
            f"no decryptable {normalized.value} accounts"
        )
    return result
