import base64
import hashlib
import json
import os
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from grok2api.providers.web.statsig import generate


class WebStatsigTests(unittest.TestCase):
    def test_runtime_pair_generates_70_byte_header(self):
        seed = base64.b64encode(bytes(range(48))).decode()
        with patch.dict(os.environ, {
            "GROK2API_WEB_STATSIG_SEED": seed,
            "GROK2API_WEB_STATSIG_HEX": "a" * 80,
        }, clear=False):
            value = generate("/rest/app-chat/conversations/new", now=1_789_000_000)
        raw = base64.b64decode(value + "==")
        self.assertEqual(len(raw), 70)
        decoded = bytes(b ^ raw[0] for b in raw[1:])
        self.assertEqual(decoded[:48], bytes(range(48)))
        number = 1_789_000_000 - 1_682_924_400
        self.assertEqual(struct.unpack('<I', decoded[48:52])[0], number)
        expected = hashlib.sha256(('POST!/rest/app-chat/conversations/new!' + str(number) + 'obfiowerehiring' + 'a' * 80).encode()).digest()[:16]
        self.assertEqual(decoded[52:68], expected)
        self.assertEqual(decoded[-1], 3)

    def test_missing_runtime_pair_is_disabled(self):
        with tempfile.TemporaryDirectory() as directory, patch('grok2api.config.DATA_DIR', Path(directory)), patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(generate("/rest/app-chat/conversations/new"))

    def test_private_file_is_reloaded_without_restart(self):
        with tempfile.TemporaryDirectory() as directory, patch('grok2api.config.DATA_DIR', Path(directory)), patch.dict(os.environ, {}, clear=True):
            path = Path(directory) / 'web-statsig.json'
            path.write_text(json.dumps({'seed': base64.b64encode(bytes(range(48))).decode(), 'hex': 'abc'}))
            first = generate('/test', now=1_789_000_000)
            path.write_text(json.dumps({'seed': base64.b64encode(bytes(range(48))).decode(), 'hex': 'def'}))
            second = generate('/test', now=1_789_000_000)
            def digest(value):
                raw = base64.b64decode(value + '==')
                return bytes(b ^ raw[0] for b in raw[53:69])
            self.assertNotEqual(digest(first), digest(second))

    def test_bad_pair_does_not_leak_values(self):
        with patch.dict(os.environ, {'GROK2API_WEB_STATSIG_SEED':'private-invalid-value', 'GROK2API_WEB_STATSIG_HEX':'abc'}):
            with self.assertRaises(ValueError) as raised:
                generate('/test')
            self.assertNotIn('private-invalid-value', str(raised.exception))
