"""Secret-safe, per-account acceptance checks for one imported credential batch.

Run this command inside the application container *before* deleting the staged
credential directory.  The directory is used only to derive the exact opaque
identity set imported by ``import_grok_credential_directory.py``.  No secret,
account identifier, file name, response body, or exception message is emitted.

The verifier is deliberately fail-closed: Resin must be enabled and valid, and
every outbound request must receive an explicit Resin binding.  There is no
direct-proxy fallback in this module.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import logging
import random
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))


PROVIDERS = ("grok_build", "grok_web", "grok_console")
DEFAULT_EXPECTED = 1189
DEFAULT_WORKERS = 3
DEFAULT_STARTS_PER_SECOND = 2.0
DEFAULT_RETRY_DELAYS = (30.0, 120.0)
MAX_RESPONSE_BYTES = 1 << 20

FAILURE_CATEGORIES = (
    "auth_rejected",
    "credential_expired",
    "credential_missing",
    "egress_rejected",
    "expected_count_mismatch",
    "internal_error",
    "invalid_response",
    "rate_limited",
    "resin_invalid",
    "source_invalid",
    "timeout",
    "topology_duplicate",
    "topology_missing",
    "transport_error",
    "upstream_rejected",
)


@dataclass(frozen=True, slots=True, repr=False)
class ExpectedCredential:
    build_account_id: str = field(repr=False)
    egress_identity: str = field(repr=False)


@dataclass(frozen=True, slots=True, repr=False)
class ProviderTarget:
    account_id: str = field(repr=False)
    egress_identity: str = field(repr=False)


@dataclass(frozen=True, slots=True, repr=False)
class CredentialTriplet:
    build: ProviderTarget = field(repr=False)
    web: ProviderTarget = field(repr=False)
    console: ProviderTarget = field(repr=False)


@dataclass(frozen=True, slots=True)
class ProbeOutcome:
    ok: bool
    category: str = ""
    transient: bool = False

    def __post_init__(self) -> None:
        if self.ok:
            object.__setattr__(self, "category", "")
            object.__setattr__(self, "transient", False)
        elif self.category not in FAILURE_CATEGORIES:
            object.__setattr__(self, "category", "internal_error")
            object.__setattr__(self, "transient", False)


class SafeFailure(RuntimeError):
    """A fixed-category failure which deliberately carries no sensitive text."""

    def __init__(self, category: str, *, transient: bool = False) -> None:
        safe_category = category if category in FAILURE_CATEGORIES else "internal_error"
        self.category = safe_category
        self.transient = bool(transient)
        super().__init__(safe_category)


@dataclass(slots=True)
class VerificationSummary:
    expected: int
    topology_complete: int = 0
    topology_missing: int = 0
    topology_duplicate: int = 0
    resin_enabled: bool = False
    resin_valid: bool = False
    direct_fallback: bool = True
    bindings_required: int = 0
    bindings_validated: int = 0
    providers: dict[str, Counter[str]] = field(
        default_factory=lambda: {
            provider: Counter(attempted=0, attempts=0, reply_ok=0, failed=0)
            for provider in PROVIDERS
        }
    )
    failures: Counter[str] = field(default_factory=Counter)

    @property
    def ok(self) -> bool:
        return bool(
            self.expected > 0
            and self.topology_complete == self.expected
            and not self.topology_missing
            and not self.topology_duplicate
            and self.resin_enabled
            and self.resin_valid
            and not self.direct_fallback
            and self.bindings_validated == self.bindings_required
            and all(
                self.providers[provider]["reply_ok"] == self.expected
                and self.providers[provider]["failed"] == 0
                for provider in PROVIDERS
            )
            and not sum(self.failures.values())
        )

    def public_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "expected": int(self.expected),
            "topology": {
                "complete": int(self.topology_complete),
                "missing": int(self.topology_missing),
                "duplicate": int(self.topology_duplicate),
            },
            "resin": {
                "enabled": bool(self.resin_enabled),
                "valid": bool(self.resin_valid),
                "direct_fallback": bool(self.direct_fallback),
                "bindings_required": int(self.bindings_required),
                "bindings_validated": int(self.bindings_validated),
            },
            "providers": {
                provider: {
                    key: int(self.providers[provider][key])
                    for key in ("attempted", "attempts", "reply_ok", "failed")
                }
                for provider in PROVIDERS
            },
            "failure_categories": {
                category: int(self.failures[category])
                for category in FAILURE_CATEGORIES
                if self.failures[category]
            },
        }


def _fixed_failure_from_exception(exc: BaseException) -> ProbeOutcome:
    """Classify by type/status only.  Never inspect or retain exception text."""

    if isinstance(exc, SafeFailure):
        return ProbeOutcome(False, exc.category, exc.transient)
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return ProbeOutcome(False, "timeout", True)

    try:
        import httpx

        if isinstance(exc, httpx.TimeoutException):
            return ProbeOutcome(False, "timeout", True)
        if isinstance(exc, httpx.TransportError):
            return ProbeOutcome(False, "transport_error", True)
    except Exception:
        pass

    name = type(exc).__name__
    if name == "WebGatewayAuthError":
        return ProbeOutcome(False, "auth_rejected", False)
    if name in {"WebGatewayEgressError", "ConsoleEgressChallengeError"}:
        return ProbeOutcome(False, "egress_rejected", True)
    if name in {
        "BrowserTransportError",
        "ConsoleResponsesError",
        "WebGatewayError",
    }:
        return ProbeOutcome(False, "transport_error", True)
    if name == "ConsoleTokenError":
        status = int(getattr(exc, "status_code", 0) or 0)
        return _failure_for_status(status)
    if isinstance(exc, (json.JSONDecodeError, UnicodeDecodeError)):
        return ProbeOutcome(False, "invalid_response", False)
    return ProbeOutcome(False, "internal_error", False)


def _failure_for_status(status: int) -> ProbeOutcome:
    status = int(status or 0)
    if status in (401, 403):
        return ProbeOutcome(False, "auth_rejected", False)
    if status == 407:
        return ProbeOutcome(False, "egress_rejected", True)
    if status == 429:
        return ProbeOutcome(False, "rate_limited", True)
    if status in (408, 425) or status >= 500 or status <= 0:
        return ProbeOutcome(False, "upstream_rejected", True)
    return ProbeOutcome(False, "upstream_rejected", False)


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _build_chunk_has_text(chunk: Any) -> bool:
    if not isinstance(chunk, Mapping):
        return False
    if _has_text(chunk.get("output_text")):
        return True
    choices = chunk.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, Mapping):
                continue
            for container_name in ("delta", "message"):
                container = choice.get(container_name)
                if not isinstance(container, Mapping):
                    continue
                if any(
                    _has_text(container.get(key))
                    for key in ("content", "reasoning_content", "reasoning")
                ):
                    return True
    return False


def _console_payload_has_text(payload: Any) -> bool:
    if not isinstance(payload, Mapping):
        return False
    if _has_text(payload.get("output_text")):
        return True
    output = payload.get("output")
    if not isinstance(output, list):
        return False
    for item in output:
        if not isinstance(item, Mapping):
            continue
        if _has_text(item.get("text")):
            return True
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if isinstance(part, Mapping) and _has_text(part.get("text")):
                return True
    return False


async def _read_limited(response: Any) -> bytes:
    chunks: list[bytes] = []
    total = 0
    async for raw in response.aiter_bytes():
        chunk = bytes(raw)
        total += len(chunk)
        if total > MAX_RESPONSE_BYTES:
            raise SafeFailure("invalid_response")
        chunks.append(chunk)
    return b"".join(chunks)


def _expected_credentials(source_dir: Path) -> tuple[list[ExpectedCredential], int]:
    """Derive only non-secret routing identities from the staged import batch."""

    from scripts.import_grok_credential_directory import build_import_plan

    plan = build_import_plan(source_dir)
    expected: list[ExpectedCredential] = []
    for payload in plan.build_payloads:
        if not isinstance(payload, dict) or len(payload) != 1:
            continue
        account_id, entry = next(iter(payload.items()))
        identity = str(entry.get("egress_identity") or "").strip()
        if account_id and identity:
            expected.append(ExpectedCredential(str(account_id), identity))
    del plan
    gc.collect()
    unique = len({item.egress_identity for item in expected})
    return expected, unique


def _group_rows_by_identity(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        identity = str(row.get("egress_identity") or "").strip()
        account_id = str(row.get("id") or "").strip()
        if identity and account_id:
            grouped.setdefault(identity, []).append(row)
    return grouped


def load_topology(
    source_dir: Path,
    *,
    expected_count: int,
    summary: VerificationSummary,
) -> list[CredentialTriplet]:
    """Resolve the exact imported triplets without returning credential values."""

    from grok2api.store import accounts_pg

    expected, unique_count = _expected_credentials(source_dir)
    if len(expected) != expected_count or unique_count != expected_count:
        summary.failures["expected_count_mismatch"] += 1
        return []

    wanted = {item.egress_identity for item in expected}
    build_rows = accounts_pg.list_account_summaries(
        page_size=0, provider="grok_build"
    ).get("accounts", [])
    web_rows = accounts_pg.list_provider_account_refs("grok_web")
    console_rows = accounts_pg.list_provider_account_refs("grok_console")
    grouped = {
        "grok_build": _group_rows_by_identity(
            row for row in build_rows if row.get("egress_identity") in wanted
        ),
        "grok_web": _group_rows_by_identity(
            row for row in web_rows if row.get("egress_identity") in wanted
        ),
        "grok_console": _group_rows_by_identity(
            row for row in console_rows if row.get("egress_identity") in wanted
        ),
    }

    triplets: list[CredentialTriplet] = []
    for item in expected:
        matches = {
            provider: grouped[provider].get(item.egress_identity, [])
            for provider in PROVIDERS
        }
        build_matches = [
            row
            for row in matches["grok_build"]
            if str(row.get("id") or "") == item.build_account_id
        ]
        matches["grok_build"] = build_matches
        if any(len(matches[provider]) == 0 for provider in PROVIDERS):
            summary.topology_missing += 1
            continue
        if any(len(matches[provider]) != 1 for provider in PROVIDERS):
            summary.topology_duplicate += 1
            continue
        triplets.append(
            CredentialTriplet(
                build=ProviderTarget(item.build_account_id, item.egress_identity),
                web=ProviderTarget(
                    str(matches["grok_web"][0]["id"]), item.egress_identity
                ),
                console=ProviderTarget(
                    str(matches["grok_console"][0]["id"]), item.egress_identity
                ),
            )
        )

    summary.topology_complete = len(triplets)
    if summary.topology_missing:
        summary.failures["topology_missing"] += summary.topology_missing
    if summary.topology_duplicate:
        summary.failures["topology_duplicate"] += summary.topology_duplicate
    return triplets


class LiveBackend:
    """Production provider calls.  All methods require an explicit Resin binding."""

    def resin_status(self) -> Mapping[str, Any]:
        from grok2api.upstream.resin_proxy import resin_public_status

        return resin_public_status()

    def binding(self, provider: str, target: ProviderTarget) -> Any:
        from grok2api.upstream.resin_proxy import resin_binding_for_account

        binding = resin_binding_for_account(
            provider,
            target.account_id,
            egress_identity=(
                target.egress_identity if provider != "grok_build" else None
            ),
        )
        if binding is None:
            raise SafeFailure("resin_invalid")
        return binding

    async def verify_build(self, target: ProviderTarget) -> ProbeOutcome:
        import httpx

        from grok2api.config import DEFAULT_MODEL, UPSTREAM_BASE
        from grok2api.pool.auth import peek_credentials_by_id, upstream_headers

        creds = await asyncio.to_thread(peek_credentials_by_id, target.account_id)
        if creds is None:
            return ProbeOutcome(False, "credential_missing")
        if creds.expired:
            return ProbeOutcome(False, "credential_expired")
        binding = self.binding("grok_build", target)
        proxy = httpx.Proxy(binding.gateway_url, auth=binding.proxy_auth)
        timeout = httpx.Timeout(45.0, connect=10.0)
        body = {
            "model": DEFAULT_MODEL,
            "messages": [{"role": "user", "content": "Reply only OK"}],
            "stream": True,
            "max_tokens": 8,
            "max_completion_tokens": 8,
        }
        saw_text = False
        async with httpx.AsyncClient(
            proxy=proxy,
            timeout=timeout,
            http2=False,
            trust_env=False,
            follow_redirects=False,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        ) as client:
            async with client.stream(
                "POST",
                f"{UPSTREAM_BASE}/chat/completions",
                headers=upstream_headers(creds.token, DEFAULT_MODEL),
                json=body,
            ) as response:
                if response.status_code < 200 or response.status_code >= 300:
                    return _failure_for_status(response.status_code)
                async for line in response.aiter_lines():
                    if not isinstance(line, str) or not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if not data or data == "[DONE]":
                        continue
                    if len(data.encode("utf-8")) > MAX_RESPONSE_BYTES:
                        return ProbeOutcome(False, "invalid_response")
                    try:
                        payload = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    if _build_chunk_has_text(payload):
                        saw_text = True
        return ProbeOutcome(True) if saw_text else ProbeOutcome(False, "invalid_response")

    async def verify_web(self, target: ProviderTarget) -> ProbeOutcome:
        from grok2api import config
        from grok2api.providers.web import GrokWebGateway, WebDeltaKind
        from grok2api.store.provider_credentials import load_web_credential
        from grok2api.upstream.browser_transport import BrowserAsyncClient

        credential = await asyncio.to_thread(load_web_credential, target.account_id)
        if credential is None:
            return ProbeOutcome(False, "credential_missing")
        binding = self.binding("grok_web", target)
        client = BrowserAsyncClient(
            proxy=binding.gateway_url,
            proxy_auth=binding.proxy_auth,
            max_clients=1,
        )
        saw_text = False
        try:
            gateway = GrokWebGateway(
                client,
                connector=client.websocket_connector,
                base_url=config.WEB_PROVIDER_BASE_URL,
                proxy=binding.gateway_url,
                handshake_timeout=20.0,
                total_timeout=60.0,
            )
            request = {
                "model": "grok-chat-fast",
                "messages": [{"role": "user", "content": "Reply only OK"}],
                "stream": True,
            }
            async with asyncio.timeout(65.0):
                async for delta in gateway.iter_chat(request, credential):
                    if delta.kind is WebDeltaKind.TEXT and _has_text(delta.text):
                        saw_text = True
            return ProbeOutcome(True) if saw_text else ProbeOutcome(False, "invalid_response")
        finally:
            await client.aclose()

    async def verify_console(self, target: ProviderTarget) -> ProbeOutcome:
        from grok2api import config
        from grok2api.providers.console import (
            ConsoleDPoPClient,
            ConsoleDPoPConfig,
            ConsoleResponsesTransport,
        )
        from grok2api.providers.types import Capability, ModelRoute, ProviderName
        from grok2api.store.provider_credentials import load_console_credential
        from grok2api.upstream.browser_transport import BrowserAsyncClient

        credential = await asyncio.to_thread(load_console_credential, target.account_id)
        if credential is None:
            return ProbeOutcome(False, "credential_missing")
        binding = self.binding("grok_console", target)
        client = BrowserAsyncClient(
            proxy=binding.gateway_url,
            proxy_auth=binding.proxy_auth,
            max_clients=1,
        )
        response = None
        try:
            dpop = ConsoleDPoPClient(
                client,
                ConsoleDPoPConfig(base_url=config.CONSOLE_PROVIDER_BASE_URL),
            )
            transport = ConsoleResponsesTransport(dpop)
            route = ModelRoute(
                public_model="grok-4.3",
                provider=ProviderName.CONSOLE,
                upstream_model="grok-4.3",
                capability=Capability.RESPONSES,
            )
            async with asyncio.timeout(65.0):
                response = await transport.forward(
                    target.account_id,
                    credential,
                    {
                        "input": "Reply only OK",
                        "stream": False,
                        "max_output_tokens": 8,
                    },
                    route,
                    egress_identity=target.egress_identity,
                )
                if response.status_code < 200 or response.status_code >= 300:
                    return _failure_for_status(response.status_code)
                body = await _read_limited(response)
            try:
                payload = json.loads(body)
            except (json.JSONDecodeError, UnicodeDecodeError):
                return ProbeOutcome(False, "invalid_response")
            return (
                ProbeOutcome(True)
                if _console_payload_has_text(payload)
                else ProbeOutcome(False, "invalid_response")
            )
        finally:
            if response is not None:
                await response.aclose()
            await client.aclose()


class StartRateLimiter:
    def __init__(
        self,
        starts_per_second: float,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[float, float], float] = random.uniform,
    ) -> None:
        self._interval = 1.0 / max(0.1, float(starts_per_second))
        self._sleep = sleep
        self._jitter = jitter
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            delay = max(0.0, self._next - now)
            if delay:
                await self._sleep(delay)
            self._next = max(now, self._next) + self._interval + self._jitter(0.0, 0.25)


def _target_for_provider(triplet: CredentialTriplet, provider: str) -> ProviderTarget:
    return {
        "grok_build": triplet.build,
        "grok_web": triplet.web,
        "grok_console": triplet.console,
    }[provider]


async def _run_phase(
    provider: str,
    triplets: list[CredentialTriplet],
    *,
    backend: Any,
    summary: VerificationSummary,
    workers: int,
    starts_per_second: float,
    retry_delays: tuple[float, ...],
    sleep: Callable[[float], Awaitable[None]],
) -> None:
    verify = getattr(backend, f"verify_{provider.removeprefix('grok_')}")
    queue: asyncio.Queue[ProviderTarget] = asyncio.Queue()
    for triplet in triplets:
        queue.put_nowait(_target_for_provider(triplet, provider))
    limiter = StartRateLimiter(starts_per_second, sleep=sleep)

    async def worker() -> None:
        while True:
            try:
                target = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            summary.providers[provider]["attempted"] += 1
            final = ProbeOutcome(False, "internal_error")
            for attempt in range(len(retry_delays) + 1):
                await limiter.wait()
                summary.providers[provider]["attempts"] += 1
                try:
                    outcome = await verify(target)
                    final = outcome if isinstance(outcome, ProbeOutcome) else ProbeOutcome(False, "internal_error")
                except BaseException as exc:  # noqa: BLE001 - fixed type-only classifier
                    final = _fixed_failure_from_exception(exc)
                if final.ok or not final.transient or attempt >= len(retry_delays):
                    break
                await sleep(max(0.0, float(retry_delays[attempt])))
            if final.ok:
                summary.providers[provider]["reply_ok"] += 1
            else:
                summary.providers[provider]["failed"] += 1
                summary.failures[final.category or "internal_error"] += 1
            queue.task_done()

    await asyncio.gather(*(worker() for _ in range(max(1, min(workers, len(triplets))))))


async def run_verification(
    triplets: list[CredentialTriplet],
    *,
    summary: VerificationSummary,
    backend: Any | None = None,
    workers: int = DEFAULT_WORKERS,
    starts_per_second: float = DEFAULT_STARTS_PER_SECOND,
    retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> VerificationSummary:
    backend = backend or LiveBackend()
    try:
        status = dict(backend.resin_status())
    except BaseException:  # noqa: BLE001 - never render exception details
        status = {}
    summary.resin_enabled = status.get("enabled") is True
    summary.resin_valid = status.get("valid") is True
    summary.direct_fallback = status.get("direct_fallback") is not False
    if not (
        summary.resin_enabled
        and summary.resin_valid
        and not summary.direct_fallback
        and status.get("mode") == "resin"
    ):
        summary.failures["resin_invalid"] += 1
        return summary
    if len(triplets) != summary.expected or summary.topology_complete != summary.expected:
        return summary

    summary.bindings_required = len(triplets) * len(PROVIDERS)
    try:
        for triplet in triplets:
            for provider in PROVIDERS:
                binding = backend.binding(provider, _target_for_provider(triplet, provider))
                if binding is None:
                    raise SafeFailure("resin_invalid")
                summary.bindings_validated += 1
    except BaseException:  # noqa: BLE001 - never render exception details
        summary.failures["resin_invalid"] += 1
        return summary

    # Strict phase boundary: every Build credential is checked before Web, and
    # every Web credential before Console.
    for provider in PROVIDERS:
        await _run_phase(
            provider,
            triplets,
            backend=backend,
            summary=summary,
            workers=max(1, min(8, int(workers))),
            starts_per_second=max(0.1, min(10.0, float(starts_per_second))),
            retry_delays=retry_delays,
            sleep=sleep,
        )
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify one imported credential batch through Resin"
    )
    parser.add_argument("directory", type=Path)
    parser.add_argument("--expected", type=int, default=DEFAULT_EXPECTED)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument(
        "--starts-per-second", type=float, default=DEFAULT_STARTS_PER_SECOND
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.disable(logging.CRITICAL)
    args = parse_args(argv)
    expected = max(1, int(args.expected))
    summary = VerificationSummary(expected=expected)
    try:
        triplets = load_topology(
            args.directory,
            expected_count=expected,
            summary=summary,
        )
        if not triplets and not summary.failures:
            summary.failures["source_invalid"] += 1
        asyncio.run(
            run_verification(
                triplets,
                summary=summary,
                workers=args.workers,
                starts_per_second=args.starts_per_second,
            )
        )
    except BaseException:  # noqa: BLE001 - the public result is category-only
        summary.failures["source_invalid"] += 1
    print(json.dumps(summary.public_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if summary.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
