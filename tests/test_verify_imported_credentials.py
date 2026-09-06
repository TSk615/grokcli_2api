from __future__ import annotations

import asyncio
import io
import json
import unittest
from contextlib import redirect_stdout
from unittest import mock

from scripts import verify_imported_credentials as verifier


def _triplet(index: int) -> verifier.CredentialTriplet:
    identity = f"credential-{index:032x}"
    return verifier.CredentialTriplet(
        build=verifier.ProviderTarget(f"secret-build-id-{index}", identity),
        web=verifier.ProviderTarget(f"secret-web-id-{index}", identity),
        console=verifier.ProviderTarget(f"secret-console-id-{index}", identity),
    )


class _Backend:
    def __init__(self, outcomes=None, *, status=None, binding_failure=False):
        self.outcomes = outcomes or {}
        self.status = status or {
            "mode": "resin",
            "enabled": True,
            "valid": True,
            "direct_fallback": False,
        }
        self.binding_failure = binding_failure
        self.calls: list[str] = []
        self.bindings: list[str] = []

    def resin_status(self):
        return self.status

    def binding(self, provider, target):
        del target
        self.bindings.append(provider)
        if self.binding_failure:
            raise RuntimeError("secret Resin token and account")
        return object()

    async def _verify(self, provider):
        self.calls.append(provider)
        outcome = self.outcomes.get(provider, verifier.ProbeOutcome(True))
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def verify_build(self, target):
        del target
        return await self._verify("grok_build")

    async def verify_web(self, target):
        del target
        return await self._verify("grok_web")

    async def verify_console(self, target):
        del target
        return await self._verify("grok_console")


async def _no_sleep(_delay: float) -> None:
    return None


class VerifyImportedCredentialsTests(unittest.IsolatedAsyncioTestCase):
    async def test_strict_build_web_console_phases_and_aggregate_success(self):
        triplets = [_triplet(1), _triplet(2)]
        backend = _Backend()
        summary = verifier.VerificationSummary(expected=2, topology_complete=2)

        await verifier.run_verification(
            triplets,
            summary=summary,
            backend=backend,
            workers=2,
            starts_per_second=10,
            retry_delays=(),
            sleep=_no_sleep,
        )

        self.assertTrue(summary.ok)
        self.assertEqual(summary.bindings_validated, 6)
        self.assertEqual(backend.calls[:2], ["grok_build", "grok_build"])
        self.assertEqual(backend.calls[2:4], ["grok_web", "grok_web"])
        self.assertEqual(backend.calls[4:], ["grok_console", "grok_console"])
        self.assertEqual(summary.providers["grok_build"]["reply_ok"], 2)
        self.assertEqual(summary.providers["grok_web"]["reply_ok"], 2)
        self.assertEqual(summary.providers["grok_console"]["reply_ok"], 2)

    async def test_invalid_resin_fails_closed_before_any_binding_or_request(self):
        backend = _Backend(
            status={
                "mode": "direct",
                "enabled": False,
                "valid": False,
                "direct_fallback": True,
            }
        )
        summary = verifier.VerificationSummary(expected=1, topology_complete=1)

        await verifier.run_verification(
            [_triplet(1)], summary=summary, backend=backend, sleep=_no_sleep
        )

        self.assertFalse(summary.ok)
        self.assertEqual(summary.failures["resin_invalid"], 1)
        self.assertEqual(backend.bindings, [])
        self.assertEqual(backend.calls, [])

    async def test_binding_failure_fails_closed_before_network(self):
        backend = _Backend(binding_failure=True)
        summary = verifier.VerificationSummary(expected=1, topology_complete=1)

        await verifier.run_verification(
            [_triplet(1)], summary=summary, backend=backend, sleep=_no_sleep
        )

        self.assertFalse(summary.ok)
        self.assertEqual(summary.failures["resin_invalid"], 1)
        self.assertEqual(backend.calls, [])

    async def test_retry_is_limited_to_transient_failures(self):
        class RetryBackend(_Backend):
            def __init__(self):
                super().__init__()
                self.web_count = 0

            async def verify_web(self, target):
                del target
                self.calls.append("grok_web")
                self.web_count += 1
                if self.web_count == 1:
                    return verifier.ProbeOutcome(False, "rate_limited", True)
                return verifier.ProbeOutcome(True)

        backend = RetryBackend()
        summary = verifier.VerificationSummary(expected=1, topology_complete=1)
        await verifier.run_verification(
            [_triplet(1)],
            summary=summary,
            backend=backend,
            workers=1,
            starts_per_second=10,
            retry_delays=(0,),
            sleep=_no_sleep,
        )

        self.assertTrue(summary.ok)
        self.assertEqual(summary.providers["grok_web"]["attempts"], 2)
        self.assertEqual(summary.failures["rate_limited"], 0)

    async def test_exception_body_ids_and_secrets_never_enter_public_output(self):
        secret = "sso-super-secret"
        backend = _Backend(outcomes={"grok_build": RuntimeError(secret)})
        summary = verifier.VerificationSummary(expected=1, topology_complete=1)
        await verifier.run_verification(
            [_triplet(1)],
            summary=summary,
            backend=backend,
            workers=1,
            starts_per_second=10,
            retry_delays=(),
            sleep=_no_sleep,
        )
        rendered = json.dumps(summary.public_dict(), sort_keys=True)

        self.assertNotIn(secret, rendered)
        self.assertNotIn("secret-build-id", rendered)
        self.assertNotIn("secret-web-id", rendered)
        self.assertNotIn("secret-console-id", rendered)
        self.assertEqual(summary.failures["internal_error"], 1)

    async def test_nonempty_text_is_required_by_build_and_console_parsers(self):
        self.assertFalse(verifier._build_chunk_has_text({"choices": [{"delta": {"content": "  "}}]}))
        self.assertTrue(verifier._build_chunk_has_text({"choices": [{"delta": {"content": "OK"}}]}))
        self.assertFalse(verifier._console_payload_has_text({"output": [{"content": [{"text": ""}]}]}))
        self.assertTrue(verifier._console_payload_has_text({"output": [{"content": [{"text": "OK"}]}]}))


class VerifyImportedCredentialsMainTests(unittest.TestCase):
    def test_main_prints_only_fixed_aggregate_when_source_raises_secret(self):
        output = io.StringIO()
        secret = "secret-path-file-token-response"
        with mock.patch.object(
            verifier, "load_topology", side_effect=RuntimeError(secret)
        ), redirect_stdout(output):
            code = verifier.main(["ignored-secret-file-name", "--expected", "1"])
        rendered = output.getvalue()
        payload = json.loads(rendered)

        self.assertEqual(code, 1)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["failure_categories"], {"source_invalid": 1})
        self.assertNotIn(secret, rendered)
        self.assertNotIn("ignored-secret-file-name", rendered)


if __name__ == "__main__":
    unittest.main()
