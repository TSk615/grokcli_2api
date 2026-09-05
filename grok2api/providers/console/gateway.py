"""Account-failover gateway for Console Responses requests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import httpx

from ..accounts import ProviderAccount
from ..types import ModelRoute
from .adapter import ConsoleProviderAdapter
from .auth import ConsoleCredential
from .responses import ConsoleResponsesError, ConsoleResponsesTransport


@dataclass(frozen=True, slots=True)
class ConsoleGatewayResult:
    response: httpx.Response
    account_id: str
    attempts: int


class ConsoleGateway:
    def __init__(self, transport: ConsoleResponsesTransport) -> None:
        self._transport = transport
        self._adapter = ConsoleProviderAdapter()

    async def forward(
        self,
        body: Mapping[str, Any],
        route: ModelRoute,
        accounts: Sequence[ProviderAccount],
    ) -> ConsoleGatewayResult:
        if not accounts:
            raise ConsoleResponsesError("No Grok Console accounts are available")
        last_response: httpx.Response | None = None
        for index, account in enumerate(accounts):
            credential = account.credential
            if not isinstance(credential, ConsoleCredential):
                continue
            try:
                response = await self._transport.forward(
                    account.account_id,
                    credential,
                    body,
                    route,
                    egress_identity=account.egress_identity or "",
                )
            except ConsoleResponsesError:
                if index + 1 < len(accounts):
                    continue
                raise
            last_response = response
            if response.status_code < 400:
                return ConsoleGatewayResult(response, account.account_id, index + 1)
            status = self._adapter.classify_status(
                response.status_code,
                headers=dict(response.headers),
            )
            if not status.retryable or index + 1 >= len(accounts):
                return ConsoleGatewayResult(response, account.account_id, index + 1)
            await response.aclose()
        if last_response is not None:
            return ConsoleGatewayResult(last_response, accounts[-1].account_id, len(accounts))
        raise ConsoleResponsesError("No usable Grok Console credentials are available")
