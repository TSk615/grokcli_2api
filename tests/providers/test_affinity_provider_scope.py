from __future__ import annotations

import unittest
from unittest import mock

from grok2api.pool import conversation_affinity


class ProviderAffinityScopeTests(unittest.TestCase):
    def setUp(self):
        self.enabled = mock.patch.object(conversation_affinity, "_enabled", return_value=True)
        self.enabled.start()

    def tearDown(self):
        self.enabled.stop()

    def test_existing_build_fingerprint_is_unchanged_when_provider_omitted(self):
        baseline = conversation_affinity.conversation_fingerprint(
            [{"role": "user", "content": "hello"}],
            api_key_id="key-1",
            model="grok-4.5",
        )
        explicit_build = conversation_affinity.conversation_fingerprint(
            [{"role": "user", "content": "hello"}],
            api_key_id="key-1",
            model="grok-4.5",
            provider="grok_build",
        )
        self.assertNotEqual(baseline, explicit_build)

    def test_same_session_isolated_between_web_and_console(self):
        kwargs = {
            "messages": [{"role": "user", "content": "hello"}],
            "api_key_id": "key-1",
            "conversation_id": "session-1",
            "model": "grok-4.5",
        }
        web = conversation_affinity.conversation_fingerprint(**kwargs, provider="grok_web")
        console = conversation_affinity.conversation_fingerprint(**kwargs, provider="grok_console")
        self.assertNotEqual(web, console)

    def test_response_chain_is_provider_scoped(self):
        web = conversation_affinity.response_chain_fingerprint(
            "resp_1", api_key_id="key-1", provider="web"
        )
        console = conversation_affinity.response_chain_fingerprint(
            "resp_1", api_key_id="key-1", provider="console"
        )
        self.assertNotEqual(web, console)


if __name__ == "__main__":
    unittest.main()
