from __future__ import annotations

import unittest
from unittest import mock

import httpx

from grok2api.providers.accounts import ProviderAccount
from grok2api.providers.console.auth import ConsoleCredential
from grok2api.providers.console.gateway import ConsoleGateway
from grok2api.providers.types import Capability, ModelRoute, ProviderName


class ConsoleGatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_retryable_response_fails_over_without_crossing_pool(self):
        first = httpx.Response(429, request=httpx.Request("POST", "https://console.test/v1/responses"))
        second = httpx.Response(200, request=httpx.Request("POST", "https://console.test/v1/responses"))
        transport = mock.AsyncMock()
        transport.forward.side_effect = [first, second]
        credential = ConsoleCredential("n", "source", "secret")
        accounts = [
            ProviderAccount("a", ProviderName.CONSOLE, credential),
            ProviderAccount("b", ProviderName.CONSOLE, credential),
        ]
        route = ModelRoute("grok-4.5", ProviderName.CONSOLE, "grok-4.5", Capability.RESPONSES)
        result = await ConsoleGateway(transport).forward({}, route, accounts)
        self.assertEqual(result.account_id, "b")
        self.assertEqual(result.attempts, 2)
        self.assertTrue(first.is_closed)
        self.assertNotIn("secret", repr(result))


if __name__ == "__main__":
    unittest.main()
