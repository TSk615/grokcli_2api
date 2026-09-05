from __future__ import annotations

import unittest

from grok2api.providers.build import BuildProvider
from grok2api.providers.types import ErrorKind


class BuildProviderTests(unittest.TestCase):
    def test_endpoint_and_headers_preserve_current_build_surface(self):
        provider = BuildProvider("https://cli-chat-proxy.grok.com/v1/")
        self.assertEqual(provider.endpoint("/chat/completions"), "https://cli-chat-proxy.grok.com/v1/chat/completions")
        headers = provider.headers("secret-token", "grok-build", "conversation")
        self.assertEqual(headers["Authorization"], "Bearer secret-token")
        self.assertEqual(headers["X-XAI-Token-Auth"], "xai-grok-cli")
        self.assertEqual(headers["x-grok-conv-id"], "conversation")

    def test_build_error_classification_does_not_revoke_on_403(self):
        provider = BuildProvider("https://example.test/v1")
        status = provider.classify_status(403, body="forbidden")
        self.assertEqual(status.kind, ErrorKind.UPSTREAM)
        self.assertTrue(status.retryable)
        self.assertFalse(status.invalidate_credential)


if __name__ == "__main__":
    unittest.main()
