from __future__ import annotations

import unittest

from grok2api.providers.web.auth import WebCredential, parse_web_credentials
from grok2api.providers.web.headers import build_web_headers


class WebAuthTests(unittest.TestCase):
    def test_plain_txt_json_and_cookie_import(self) -> None:
        plain = parse_web_credentials("token-a\n# ignored\ntoken-b\ntoken-a")
        self.assertEqual([item.sso for item in plain], ["token-a", "token-b"])

        exported = parse_web_credentials(
            '{"accounts": ['
            '{"sso": "one", "sso_rw": "write-one", "email": "a@example.com"},'
            '{"cookie": "sso=two; sso-rw=write-two; cf_clearance=clear; other=x"}'
            "]}"
        )
        self.assertEqual(len(exported), 2)
        self.assertEqual(exported[0].sso_rw, "write-one")
        self.assertEqual(exported[1].cloudflare_cookies, {"cf_clearance": "clear"})

        browser_export = parse_web_credentials(
            '[{"name":"sso","value":"browser-read"},'
            '{"name":"sso-rw","value":"browser-write"},'
            '{"name":"cf_clearance","value":"browser-clear"}]'
        )
        self.assertEqual(len(browser_export), 1)
        self.assertEqual(browser_export[0].sso_rw, "browser-write")
        self.assertEqual(
            browser_export[0].cloudflare_cookies, {"cf_clearance": "browser-clear"}
        )

    def test_repr_and_str_never_contain_secrets(self) -> None:
        credential = WebCredential(
            sso="super-secret-sso",
            sso_rw="super-secret-rw",
            cloudflare_cookies={"cf_clearance": "super-secret-cf"},
            label="test",
        )
        rendered = repr(credential) + str(credential)
        self.assertNotIn("super-secret-sso", rendered)
        self.assertNotIn("super-secret-rw", rendered)
        self.assertNotIn("super-secret-cf", rendered)
        self.assertIn("web-sso:", rendered)

    def test_cookie_headers_and_sanitization(self) -> None:
        credential = parse_web_credentials(
            "sso=read; sso-rw=write; cf_clearance=clear; __cf_bm=bm; "
            "_cfuvid=uv; arbitrary=no"
        )[0]
        headers = build_web_headers(credential)
        self.assertEqual(
            headers["Cookie"],
            "sso=read; sso-rw=write; __cf_bm=bm; _cfuvid=uv; cf_clearance=clear",
        )
        self.assertNotIn("arbitrary", headers["Cookie"])

    def test_rejects_cookie_and_header_injection(self) -> None:
        with self.assertRaises(ValueError):
            WebCredential(sso="safe\r\nX-Evil: yes", sso_rw="safe")
        credential = WebCredential(sso="safe", sso_rw="safe")
        with self.assertRaises(ValueError):
            build_web_headers(credential, extra={"Cookie": "replacement"})


if __name__ == "__main__":
    unittest.main()
