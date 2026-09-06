"""Safely import a directory of CLIProxyAPI xAI credential JSON files.

Build credentials are normalized and imported first with the existing account
importer.  Any SSO value carried by the same normalized account is then imported
for both Web and Console with the same stable egress identity.

This command deliberately has no online credential-conversion path.  A record
without an existing access token is classified as ``build_offline_token_missing``
instead of attempting SSO/OIDC conversion over a direct connection.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))


MAX_CREDENTIAL_FILE_BYTES = 2 << 20
IDENTITY_DOMAIN = "grokcli-2api/credential-egress/v1"


def stable_egress_identity(account_id: str) -> str:
    """Return an opaque stable identity derived only from a non-secret ID."""

    normalized = str(account_id or "").strip()
    if not normalized:
        raise ValueError("stable account id is required")
    digest = hashlib.sha256(
        f"{IDENTITY_DOMAIN}\0{normalized}".encode("utf-8")
    ).hexdigest()
    return f"credential-{digest[:32]}"


@dataclass(slots=True, repr=False)
class ProviderJob:
    provider: str
    payload: dict[str, str] = field(repr=False)
    egress_identity: str


@dataclass(slots=True, repr=False)
class ImportPlan:
    build_payloads: list[dict[str, dict[str, Any]]] = field(
        default_factory=list, repr=False
    )
    provider_jobs: list[ProviderJob] = field(default_factory=list, repr=False)
    files: list[Path] = field(default_factory=list, repr=False)
    counts: Counter[str] = field(default_factory=Counter)


def _safe_error_category(exc: BaseException) -> str:
    """Classify failures without preserving messages or credential values."""

    name = type(exc).__name__.lower()
    if isinstance(exc, (json.JSONDecodeError, UnicodeDecodeError)):
        return "invalid_json"
    if isinstance(exc, OSError):
        return "file_io_error"
    if "config" in name or "security" in name:
        return "provider_configuration_error"
    if isinstance(exc, (TypeError, ValueError)):
        return "unsupported_credential_format"
    return "internal_error"


def _load_json_file(path: Path) -> Any:
    if path.is_symlink():
        raise OSError("symlink credential files are not accepted")
    size = path.stat().st_size
    if size > MAX_CREDENTIAL_FILE_BYTES:
        raise OverflowError("credential file exceeds size limit")
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def _provider_payloads(entry: dict[str, Any]) -> list[tuple[str, dict[str, str]]]:
    from grok2api.pool.accounts import get_sso_value

    sso = get_sso_value(entry)
    if not sso:
        return []
    # Keep the payload minimal.  In particular, do not copy email, password,
    # OAuth tokens, or the original account/storage ID into provider stores.
    return [
        ("web", {"sso": sso, "sso-rw": sso}),
        ("console", {"sso": sso}),
    ]


def build_import_plan(directory: Path) -> ImportPlan:
    """Read and offline-normalize every JSON credential under *directory*."""

    from grok2api.pool import accounts

    root = directory.resolve(strict=True)
    if not root.is_dir():
        raise NotADirectoryError(str(root))

    plan = ImportPlan()
    candidates = sorted(root.rglob("*.json"))
    plan.counts["files_discovered"] = len(candidates)
    if not candidates:
        plan.counts["json_files_missing"] = 1
    for path in candidates:
        if path.is_symlink():
            plan.counts["symlink_skipped"] += 1
            continue
        try:
            raw = _load_json_file(path)
        except OverflowError:
            plan.counts["oversized_file"] += 1
            continue
        except BaseException as exc:  # noqa: BLE001 - category-only reporting
            plan.counts[_safe_error_category(exc)] += 1
            continue

        plan.files.append(path)
        normalized = accounts.collect_normalized_entries(raw)
        if not normalized.get("ok"):
            plan.counts["build_offline_token_missing"] += 1
            continue
        entries = normalized.get("normalized") or {}
        if not entries:
            plan.counts["build_offline_token_missing"] += 1
            continue

        for account_id, raw_entry in entries.items():
            if not isinstance(raw_entry, dict):
                plan.counts["build_unsupported_entry"] += 1
                continue
            identity = stable_egress_identity(str(account_id))
            entry = dict(raw_entry)
            entry["egress_identity"] = identity
            plan.build_payloads.append({str(account_id): entry})
            plan.counts["build_planned"] += 1

            provider_payloads = _provider_payloads(entry)
            if not provider_payloads:
                plan.counts["sso_absent"] += 1
            for provider, payload in provider_payloads:
                plan.provider_jobs.append(
                    ProviderJob(
                        provider=provider,
                        payload=payload,
                        egress_identity=identity,
                    )
                )
                plan.counts[f"{provider}_planned"] += 1
    return plan


def _validate_provider_job(job: ProviderJob) -> None:
    if job.provider == "web":
        from grok2api.providers.web.auth import parse_web_credentials

        parsed = parse_web_credentials(job.payload)
    elif job.provider == "console":
        from grok2api.providers.console.auth import parse_credentials

        parsed = parse_credentials(json.dumps(job.payload))
    else:
        raise ValueError("unsupported provider")
    if len(parsed) != 1:
        raise ValueError("provider payload must contain one account")


def execute_import(plan: ImportPlan, *, dry_run: bool) -> Counter[str]:
    """Validate or import a plan, always running the Build stage first."""

    from grok2api.pool import accounts

    result = Counter(plan.counts)
    build_ready = bool(plan.build_payloads)
    if dry_run:
        result["dry_run"] = 1
        result["build_validated"] = len(plan.build_payloads)
    elif plan.build_payloads:
        try:
            imported = accounts.import_auth_payloads_bulk(
                plan.build_payloads, merge=True
            )
            if not imported.get("ok"):
                result["build_import_failed"] += len(plan.build_payloads)
                build_ready = False
            else:
                result["build_imported"] += int(imported.get("count") or 0)
                result["build_parse_errors"] += int(
                    imported.get("parse_errors") or 0
                )
                if int(imported.get("parse_errors") or 0):
                    build_ready = False
        except BaseException as exc:  # noqa: BLE001 - never echo exception text
            result[f"build_{_safe_error_category(exc)}"] += len(
                plan.build_payloads
            )
            build_ready = False

    # Provider imports are intentionally attempted only after the complete
    # Build batch.  Parsing and persistence are offline except for the configured
    # database connection; this command never performs upstream token exchange.
    for job in plan.provider_jobs:
        if not build_ready:
            result[f"{job.provider}_skipped_build_failure"] += 1
            continue
        try:
            if dry_run:
                _validate_provider_job(job)
                result[f"{job.provider}_validated"] += 1
            else:
                from grok2api.admin.provider_accounts import import_provider_accounts

                imported = import_provider_accounts(
                    job.provider,
                    job.payload,
                    egress_identity=job.egress_identity,
                )
                result[f"{job.provider}_imported"] += int(
                    imported.get("count") or 0
                )
        except BaseException as exc:  # noqa: BLE001 - category-only reporting
            result[
                f"{job.provider}_{_safe_error_category(exc)}"
            ] += 1

    result["upstream_network_conversions"] = 0
    result["direct_upstream_requests"] = 0
    return result


def _has_failures(counts: Counter[str]) -> bool:
    failure_markers = (
        "error",
        "failed",
        "missing",
        "unsupported",
        "oversized",
        "skipped",
    )
    return any(
        value > 0 and any(marker in key for marker in failure_markers)
        for key, value in counts.items()
    )


def cleanup_imported_files(plan: ImportPlan, counts: Counter[str]) -> None:
    """Delete only the exact regular files that this successful plan consumed."""

    if _has_failures(counts):
        counts["cleanup_skipped_due_to_failures"] += 1
        return
    for path in plan.files:
        try:
            if path.is_file() and not path.is_symlink():
                path.unlink()
                counts["files_cleaned"] += 1
        except OSError:
            counts["cleanup_file_io_error"] += 1


def public_summary(counts: Counter[str], *, cleanup_requested: bool) -> dict[str, Any]:
    """Return only booleans, counts, and fixed category names."""

    ordered = {key: int(counts[key]) for key in sorted(counts)}
    return {
        "ok": not _has_failures(counts),
        "cleanup_requested": bool(cleanup_requested),
        "counts": ordered,
        "failure_categories": sorted(
            key
            for key, value in counts.items()
            if value > 0
            and any(
                marker in key
                for marker in (
                    "error",
                    "failed",
                    "missing",
                    "unsupported",
                    "oversized",
                    "skipped",
                )
            )
        ),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline-import CLIProxyAPI xAI credentials for Build/Web/Console"
    )
    parser.add_argument("directory", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--cleanup-source",
        action="store_true",
        help="delete consumed JSON files only after a fully successful live import",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        plan = build_import_plan(args.directory)
        counts = execute_import(plan, dry_run=bool(args.dry_run))
        if args.cleanup_source and not args.dry_run:
            cleanup_imported_files(plan, counts)
        summary = public_summary(
            counts, cleanup_requested=bool(args.cleanup_source)
        )
    except BaseException as exc:  # noqa: BLE001 - safe top-level result only
        counts = Counter({_safe_error_category(exc): 1})
        summary = public_summary(
            counts, cleanup_requested=bool(args.cleanup_source)
        )
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
