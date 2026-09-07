from __future__ import annotations

import base64
import asyncio
import hashlib
import json
import unittest
from datetime import datetime, timedelta, timezone

import httpx
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature

from grok2api.providers.base import ProviderAdapter
from grok2api.providers.types import Capability, ErrorKind, ProviderName
from grok2api.providers.console.adapter import ConsoleProviderAdapter
from grok2api.providers.console.auth import CredentialImportError, parse_credentials
from grok2api.providers.console.client import ConsoleDPoPClient
from grok2api.providers.console.dpop import (
    DPoPSession,
    DPoPSessionCache,
    access_token_hash,
    clock_skew_from_date_header,
    create_dpop_proof,
    jwk_thumbprint,
    normalized_htu,
    public_jwk,
)
from grok2api.providers.console.errors import (
    ConsoleEgressChallengeError,
    ConsoleErrorKind,
    ConsoleTokenError,
    classify_error,
)
from grok2api.providers.console.headers import browser_headers
from grok2api.providers.console.media import ConsoleMediaError, classify_console_media_failure
from grok2api.providers.console.models import IMAGE_EDIT, RESPONSES, list_models, resolve_model
from grok2api.upstream.browser_transport import BrowserTransportError


def _decode_segment(value: str) -> dict:
    raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    return json.loads(raw)


def _access_token(expiry: datetime, thumbprint: str, serial: int = 1) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(
        json.dumps(
            {"exp": int(expiry.timestamp()), "cnf": {"jkt": thumbprint}, "serial": serial},
            separators=(",", ":"),
        ).encode()
    ).rstrip(b"=").decode()
    return f"{header}.{payload}.unsigned"


class DPoPProofTests(unittest.TestCase):
    def test_proof_has_bound_claims_and_valid_signature(self) -> None:
        key = ec.generate_private_key(ec.SECP256R1())
        now = datetime(2026, 9, 5, 12, 0, tzinfo=timezone.utc)
        token = "access-token-is-secret"
        proof = create_dpop_proof(
            key,
            token,
            "post",
            "https://console.x.ai/v1/responses?ignored=yes#fragment",
            now=now,
            clock_skew=timedelta(seconds=17),
            jti="fixed-jti",
        )
        encoded_header, encoded_claims, encoded_signature = proof.split(".")
        header = _decode_segment(encoded_header)
        claims = _decode_segment(encoded_claims)

        self.assertEqual(header["alg"], "ES256")
        self.assertEqual(header["typ"], "dpop+jwt")
        self.assertEqual(header["jwk"], public_jwk(key))
        self.assertEqual(claims["jti"], "fixed-jti")
        self.assertEqual(claims["htm"], "POST")
        self.assertEqual(claims["htu"], "https://console.x.ai/v1/responses")
        self.assertEqual(claims["iat"], int(now.timestamp()) + 17)
        self.assertEqual(claims["ath"], access_token_hash(token))

        raw_signature = base64.urlsafe_b64decode(encoded_signature + "=" * (-len(encoded_signature) % 4))
        self.assertEqual(len(raw_signature), 64)
        r = int.from_bytes(raw_signature[:32], "big")
        s = int.from_bytes(raw_signature[32:], "big")
        key.public_key().verify(
            encode_dss_signature(r, s),
            f"{encoded_header}.{encoded_claims}".encode("ascii"),
            ec.ECDSA(hashes.SHA256()),
        )

    def test_thumbprint_is_rfc7638_canonical(self) -> None:
        key = ec.generate_private_key(ec.SECP256R1())
        jwk = public_jwk(key)
        canonical = json.dumps(
            {"crv": jwk["crv"], "kty": jwk["kty"], "x": jwk["x"], "y": jwk["y"]},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        expected = base64.urlsafe_b64encode(hashlib.sha256(canonical).digest()).rstrip(b"=").decode()
        self.assertEqual(jwk_thumbprint(jwk), expected)
        self.assertEqual(normalized_htu("https://console.x.ai?x=1"), "https://console.x.ai/")

    def test_clock_skew_uses_observation_midpoint(self) -> None:
        before = datetime(2026, 9, 5, 11, 59, 59, tzinfo=timezone.utc)
        after = before + timedelta(seconds=2)
        self.assertEqual(
            clock_skew_from_date_header("Sat, 05 Sep 2026 12:00:10 GMT", before, after),
            timedelta(seconds=10),
        )
        self.assertEqual(clock_skew_from_date_header("bad", before, after), timedelta(0))


class SessionCacheTests(unittest.TestCase):
    def _session(self, token: str, expiry: datetime) -> DPoPSession:
        key = ec.generate_private_key(ec.SECP256R1())
        return DPoPSession(token, key, public_jwk(key), expiry)

    def test_cache_is_bounded_expires_and_invalidates_by_token(self) -> None:
        now = datetime(2026, 9, 5, tzinfo=timezone.utc)
        cache = DPoPSessionCache(max_entries=2, now=lambda: now)
        first = self._session("first", now + timedelta(minutes=5))
        self.assertNotIn("first", repr(first))
        second = self._session("second", now + timedelta(minutes=5))
        third = self._session("third", now + timedelta(minutes=5))
        cache.put("a", first)
        cache.put("b", second)
        self.assertIs(cache.get("a"), first)
        cache.put("c", third)
        self.assertIsNone(cache.get("b"))
        self.assertFalse(cache.invalidate("a", "stale-token"))
        self.assertTrue(cache.invalidate("a", "first"))
        cache.put("near-expiry", self._session("near", now + timedelta(seconds=20)))
        self.assertIsNone(cache.get("near-expiry"))


class ConsoleClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_cache_and_one_401_refresh_are_offline(self) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        counts = {"mint": 0, "protected": 0}
        auth_headers: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/v1/dpop/token":
                counts["mint"] += 1
                payload = json.loads(request.content)
                thumbprint = jwk_thumbprint(payload["jwk"])
                token = _access_token(now + timedelta(minutes=10), thumbprint, counts["mint"])
                self.assertNotIn("access-token-is-secret", repr(request))
                return httpx.Response(
                    200,
                    json={"access_token": token, "token_type": "DPoP", "expires_in": 600},
                    headers={"Date": now.strftime("%a, %d %b %Y %H:%M:%S GMT")},
                )
            counts["protected"] += 1
            auth_headers.append(request.headers["Authorization"])
            proof_claims = _decode_segment(request.headers["DPoP"].split(".")[1])
            self.assertEqual(proof_claims["htu"], "https://console.x.ai/v1/responses")
            self.assertEqual(proof_claims["htm"], "POST")
            self.assertEqual(request.headers["x-cluster"], "https://us-east-1.api.x.ai")
            if counts["protected"] == 1:
                return httpx.Response(401, json={"error": "expired"})
            return httpx.Response(200, json={"ok": True})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = ConsoleDPoPClient(http, now=lambda: now)
            response = await client.request(
                42,
                "secret-sso",
                "POST",
                "/responses?stream=true",
                egress_identity="proxy-a",
                json={"model": "grok-4.5"},
            )
            self.assertEqual(response.status_code, 200)
            second = await client.request(42, "secret-sso", "POST", "/responses", egress_identity="proxy-a")
            self.assertEqual(second.status_code, 200)
            self.assertEqual(counts, {"mint": 2, "protected": 3})
            self.assertNotEqual(auth_headers[0], auth_headers[1])
            self.assertEqual(auth_headers[1], auth_headers[2])
            self.assertEqual(client.cached_session_count, 1)

            other_egress = await client.request(
                42, "secret-sso", "POST", "/responses", egress_identity="proxy-b"
            )
            self.assertEqual(other_egress.status_code, 200)
            self.assertEqual(counts["mint"], 3)
            self.assertEqual(client.cached_session_count, 2)

    async def test_concurrent_requests_coalesce_token_mint(self) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        counts = {"mint": 0, "protected": 0}

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/v1/dpop/token":
                counts["mint"] += 1
                await asyncio.sleep(0.01)
                jwk = json.loads(request.content)["jwk"]
                return httpx.Response(
                    200,
                    json={
                        "access_token": _access_token(now + timedelta(minutes=10), jwk_thumbprint(jwk)),
                        "token_type": "DPoP",
                        "expires_in": 600,
                    },
                )
            counts["protected"] += 1
            return httpx.Response(200, json={"ok": True})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = ConsoleDPoPClient(http, now=lambda: now)
            responses = await asyncio.gather(
                *(client.request("same", "same-sso", "POST", "/responses") for _ in range(8))
            )
        self.assertTrue(all(response.status_code == 200 for response in responses))
        self.assertEqual(counts, {"mint": 1, "protected": 8})

    async def test_second_401_is_returned_without_a_third_attempt(self) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        counts = {"mint": 0, "protected": 0}

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/v1/dpop/token":
                counts["mint"] += 1
                jwk = json.loads(request.content)["jwk"]
                return httpx.Response(
                    200,
                    json={
                        "access_token": _access_token(
                            now + timedelta(minutes=10), jwk_thumbprint(jwk), counts["mint"]
                        ),
                        "token_type": "DPoP",
                        "expires_in": 600,
                    },
                )
            counts["protected"] += 1
            return httpx.Response(401, json={"error": "still unauthorized"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = ConsoleDPoPClient(http, now=lambda: now)
            response = await client.request("account", "sso", "GET", "/models")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(counts, {"mint": 2, "protected": 2})


class ImportAndErrorsTests(unittest.TestCase):
    def test_plain_text_json_dedup_and_secret_safe_repr(self) -> None:
        plain = parse_credentials("sso=alpha; sso-rw=alpha\nalpha\n beta \n")
        self.assertEqual([item.sso_token for item in plain], ["alpha", "beta"])
        self.assertNotIn("alpha", repr(plain[0]))
        self.assertTrue(plain[0].source_key.startswith("console-sso:"))

        document = {
            "provider": "grok_console",
            "accounts": [
                {
                    "name": "Main",
                    "email": " user@example.com ",
                    "user_id": "u1",
                    "sso_token": "json-secret",
                    "cloudflare_cookies": "cf_clearance=also-secret",
                }
            ],
        }
        credential = parse_credentials("\ufeff" + json.dumps(document))[0]
        self.assertEqual(credential.name, "Main")
        self.assertEqual(credential.email, "user@example.com")
        self.assertNotIn("json-secret", repr(credential))
        self.assertNotIn("also-secret", repr(credential))

    def test_build_export_json_can_reuse_its_sso_for_console(self) -> None:
        parsed = parse_credentials(
            json.dumps({"type": "xai", "auth_kind": "oauth", "sso": "shared-sso"})
        )
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].sso_token, "shared-sso")

    def test_malformed_or_wrong_provider_json_is_rejected(self) -> None:
        with self.assertRaises(CredentialImportError):
            parse_credentials('["bare-token"]')
        with self.assertRaises(CredentialImportError):
            parse_credentials('{"provider":"grok_web","accounts":[]}')
        with self.assertRaises(CredentialImportError):
            parse_credentials("{")

    def test_headers_and_error_classification(self) -> None:
        headers = browser_headers("token", cloudflare_cookies="cf_clearance=value")
        self.assertEqual(headers["Origin"], "https://console.x.ai")
        self.assertIn("sso=token", headers["Cookie"])
        self.assertIn("sso-rw=token", headers["Cookie"])
        self.assertIn("cf_clearance=value", headers["Cookie"])
        self.assertIn("Sec-CH-UA", headers)
        injected = browser_headers("token", cloudflare_cookies="sso=wrong; cf_clearance=ok")
        self.assertNotIn("sso=wrong", injected["Cookie"])

        self.assertEqual(classify_error(400).kind, ConsoleErrorKind.INVALID_REQUEST)
        self.assertTrue(classify_error(401).invalidate_dpop)
        generic_403 = classify_error(403, body="cloudflare challenge")
        self.assertEqual(generic_403.kind, ConsoleErrorKind.EGRESS_CHALLENGE)
        self.assertTrue(generic_403.egress_failure)
        self.assertEqual(
            classify_error(403, body="DPoP proof required").kind,
            ConsoleErrorKind.DPOP_SESSION,
        )
        self.assertEqual(
            classify_error(403, body="account suspended").kind,
            ConsoleErrorKind.ACCOUNT_BLOCKED,
        )
        limited = classify_error(429, retry_after="17")
        self.assertEqual(limited.kind, ConsoleErrorKind.RATE_LIMIT)
        self.assertEqual(limited.retry_after_seconds, 17)
        self.assertTrue(classify_error(503).retryable)

    def test_media_failure_classification_keeps_quota_network_and_session_distinct(self) -> None:
        self.assertEqual(
            classify_console_media_failure(ConsoleMediaError("safe", status_code=429, phase="image_create")),
            ("quota_or_rate_limit", 429, "image_create"),
        )
        self.assertEqual(
            classify_console_media_failure(ConsoleMediaError("safe", status_code=502, phase="image_create")),
            ("upstream_network", 502, "image_create"),
        )
        self.assertEqual(
            classify_console_media_failure(ConsoleTokenError(429)),
            ("quota_or_rate_limit", 429, "dpop_session"),
        )
        self.assertEqual(
            classify_console_media_failure(ConsoleTokenError(401)),
            ("auth_or_challenge", 401, "dpop_session"),
        )
        self.assertEqual(
            classify_console_media_failure(ConsoleEgressChallengeError(403)),
            ("auth_or_challenge", 403, "dpop_session"),
        )
        self.assertEqual(
            classify_console_media_failure(BrowserTransportError("safe")),
            ("upstream_network", None, "transport"),
        )
        self.assertEqual(
            classify_console_media_failure(RuntimeError("safe")),
            ("internal_error", None, ""),
        )

    def test_catalog_exposes_conversation_and_future_media_capabilities(self) -> None:
        self.assertIn(RESPONSES, resolve_model("grok-4.5").capabilities)
        self.assertIn(IMAGE_EDIT, resolve_model("grok-imagine-image-2.0").capabilities)
        self.assertIn("grok-build-0.1", {model.upstream_model for model in list_models(capability=RESPONSES)})

    def test_shared_provider_adapter_contract(self) -> None:
        adapter = ConsoleProviderAdapter()
        self.assertIsInstance(adapter, ProviderAdapter)
        self.assertEqual(adapter.provider, ProviderName.CONSOLE)
        routes = tuple(adapter.model_routes())
        self.assertTrue(
            any(
                route.public_model == "grok-4.5"
                and route.capability == Capability.CHAT
                and route.qualified_model == "Console/grok-4.5"
                for route in routes
            )
        )
        self.assertTrue(
            any(
                route.public_model == "grok-imagine-image"
                and route.capability == Capability.IMAGE_EDIT
                for route in routes
            )
        )
        challenged = adapter.classify_status(403, body="cloudflare challenge")
        self.assertEqual(challenged.kind, ErrorKind.EGRESS)
        self.assertTrue(challenged.retryable)
        self.assertFalse(challenged.account_scoped)
        limited = adapter.classify_status(429, headers={"Retry-After": "9"})
        self.assertEqual(limited.kind, ErrorKind.RATE_LIMIT)
        self.assertEqual(limited.retry_after_seconds, 9)


if __name__ == "__main__":
    unittest.main()
