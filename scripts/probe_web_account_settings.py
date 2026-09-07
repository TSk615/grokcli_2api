"""Safely prepare a tiny Web-account sample and classify Console image quota.

The command is dry-run by default.  ``--apply`` is required to make upstream
account-setting requests.  It never prints account IDs, tokens, cookies, proxy
credentials, upstream response bodies, or generated media URLs.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from typing import Any

from grok2api.providers.accounts import ProviderAccount, acquire_provider_sequence
from grok2api.providers.console import ConsoleDPoPClient, ConsoleDPoPConfig, ConsoleMediaError, ConsoleMediaGateway
from grok2api.providers.types import ProviderName
from grok2api.providers.web.account_settings import WebAccountSettingError, WebAccountSettingsClient
from grok2api.store import accounts_pg
from grok2api.store.provider_credentials import load_console_credential
from grok2api.upstream.browser_transport import BrowserAsyncClient
from grok2api.upstream.proxy_pool import pick_proxy_for_account
from grok2api.upstream.resin_proxy import resin_binding_for_account


def _console_ref_for(web_account: ProviderAccount) -> dict[str, Any] | None:
    digest = hashlib.sha256(web_account.credential.sso.encode("utf-8")).hexdigest()
    source_key = "console-sso:" + digest
    return next(
        (row for row in accounts_pg.list_provider_account_refs(ProviderName.CONSOLE.value) if row.get("source_key") == source_key),
        None,
    )


def _bound_client(provider: ProviderName, account_id: str, egress_identity: str | None) -> tuple[BrowserAsyncClient, str]:
    key = str(egress_identity or "").strip() or account_id
    binding = resin_binding_for_account(provider.value, account_id, egress_identity=key)
    proxy = binding.gateway_url if binding is not None else pick_proxy_for_account(key)
    client = BrowserAsyncClient(
        proxy=proxy,
        proxy_auth=binding.proxy_auth if binding is not None else None,
    )
    return client, key


async def _probe_console(ref: dict[str, Any] | None) -> dict[str, Any]:
    if ref is None:
        return {"classification": "linked_console_missing", "status": None}
    account_id = str(ref.get("id") or "")
    credential = load_console_credential(account_id)
    if credential is None:
        return {"classification": "linked_console_unavailable", "status": None}
    client, egress_key = _bound_client(ProviderName.CONSOLE, account_id, ref.get("egress_identity"))
    try:
        gateway = ConsoleMediaGateway(
            ConsoleDPoPClient(client, ConsoleDPoPConfig(base_url="https://console.x.ai")),
            client,
        )
        try:
            images = await gateway.generate_image(
                {
                    "model": "grok-imagine-image",
                    "prompt": "a simple blue circle on a white background",
                    "n": 1,
                    "response_format": "url",
                },
                credential,
                account_id=account_id,
                egress_identity=egress_key,
            )
            return {"classification": "success", "status": 200, "items": len(images)}
        except ConsoleMediaError as exc:
            return {"classification": exc.classification, "status": exc.status_code}
        except Exception:
            return {"classification": "transport_or_session", "status": None}
    finally:
        await client.aclose()


async def _one(index: int, account: ProviderAccount, *, apply: bool) -> dict[str, Any]:
    linked = _console_ref_for(account)
    before = await _probe_console(linked) if apply else {"classification": "not_run", "status": None}
    if not apply:
        return {"sample": index, "before": before, "settings": [], "after": {"classification": "not_run", "status": None}}

    client, _ = _bound_client(ProviderName.WEB, account.account_id, account.egress_identity)
    settings: list[dict[str, Any]] = []
    try:
        worker = WebAccountSettingsClient(client)
        try:
            results = await worker.prepare(account.credential)
            settings = [
                {
                    "phase": item.phase,
                    "success": item.success,
                    "status": item.status_code,
                    "classification": item.classification,
                }
                for item in results
            ]
        except WebAccountSettingError as exc:
            settings = [{"phase": exc.phase, "success": False, "status": exc.status_code, "classification": exc.classification}]
        except Exception:
            settings = [{"phase": "unknown", "success": False, "status": None, "classification": "transport_or_protocol"}]
    finally:
        await client.aclose()

    # A successful preflight already proves availability; avoid consuming a
    # second image.  Otherwise retry once after account preparation.
    after = before if before.get("classification") == "success" else await _probe_console(linked)
    return {"sample": index, "before": before, "settings": settings, "after": after}


async def _main(args: argparse.Namespace) -> int:
    accounts = acquire_provider_sequence(ProviderName.WEB)[: args.limit]
    semaphore = asyncio.Semaphore(args.concurrency)

    async def run(index: int, account: ProviderAccount) -> dict[str, Any]:
        async with semaphore:
            return await _one(index, account, apply=args.apply)

    results = await asyncio.gather(*(run(i, account) for i, account in enumerate(accounts, 1)))
    print(json.dumps({"apply": args.apply, "limit": len(results), "concurrency": args.concurrency, "results": results}, ensure_ascii=False))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=2)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.limit <= 5:
        parser.error("--limit must be between 1 and 5")
    if not 1 <= args.concurrency <= 2:
        parser.error("--concurrency must be between 1 and 2")
    return asyncio.run(_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
