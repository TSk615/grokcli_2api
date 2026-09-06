from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import import_grok_credential_directory as importer


def _record(*, suffix: str, sso: str | None = "sso-secret") -> dict[str, object]:
    record: dict[str, object] = {
        "type": "xai",
        "access_token": f"opaque-access-{suffix}",
        "refresh_token": f"opaque-refresh-{suffix}",
        "account_id": f"account-{suffix}",
    }
    if sso is not None:
        record["sso"] = sso
    return record


class CredentialDirectoryImportTests(unittest.TestCase):
    def test_plan_assigns_same_stable_opaque_identity_to_all_providers(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            (root / "one.json").write_text(
                json.dumps(_record(suffix="one")), encoding="utf-8"
            )
            plan = importer.build_import_plan(root)

        self.assertEqual(len(plan.build_payloads), 1)
        self.assertEqual(len(plan.provider_jobs), 2)
        build_entry = next(iter(plan.build_payloads[0].values()))
        identities = {job.egress_identity for job in plan.provider_jobs}
        self.assertEqual(identities, {build_entry["egress_identity"]})
        identity = str(build_entry["egress_identity"])
        self.assertRegex(identity, r"^credential-[0-9a-f]{32}$")
        self.assertNotIn("account-one", identity)
        self.assertNotIn("sso-secret", repr(plan))
        self.assertNotIn("opaque-access", repr(plan))

    def test_identity_is_stable_across_token_rotation(self):
        first = importer.stable_egress_identity("stable-account")
        second = importer.stable_egress_identity("stable-account")
        other = importer.stable_egress_identity("other-account")
        self.assertEqual(first, second)
        self.assertNotEqual(first, other)

    def test_dry_run_validates_without_writes_or_network_conversion(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            (root / "one.json").write_text(
                json.dumps(_record(suffix="one")), encoding="utf-8"
            )
            plan = importer.build_import_plan(root)
            with mock.patch(
                "grok2api.pool.accounts.import_auth_payloads_bulk"
            ) as build_import, mock.patch(
                "grok2api.admin.provider_accounts.import_provider_accounts"
            ) as provider_import:
                counts = importer.execute_import(plan, dry_run=True)

        build_import.assert_not_called()
        provider_import.assert_not_called()
        self.assertEqual(counts["build_validated"], 1)
        self.assertEqual(counts["web_validated"], 1)
        self.assertEqual(counts["console_validated"], 1)
        self.assertEqual(counts["upstream_network_conversions"], 0)
        self.assertEqual(counts["direct_upstream_requests"], 0)

    def test_live_import_orders_build_before_web_and_console(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            (root / "one.json").write_text(
                json.dumps(_record(suffix="one")), encoding="utf-8"
            )
            plan = importer.build_import_plan(root)
            calls: list[str] = []

            def build(payloads, *, merge):
                calls.append("build")
                self.assertTrue(merge)
                return {"ok": True, "count": len(payloads), "parse_errors": 0}

            def provider(name, payload, *, egress_identity):
                calls.append(name)
                self.assertRegex(egress_identity, r"^credential-[0-9a-f]{32}$")
                self.assertEqual(set(payload), {"sso", "sso-rw"} if name == "web" else {"sso"})
                return {"ok": True, "count": 1}

            with mock.patch(
                "grok2api.pool.accounts.import_auth_payloads_bulk",
                side_effect=build,
            ), mock.patch(
                "grok2api.admin.provider_accounts.import_provider_accounts",
                side_effect=provider,
            ):
                counts = importer.execute_import(plan, dry_run=False)

        self.assertEqual(calls, ["build", "web", "console"])
        self.assertEqual(counts["build_imported"], 1)
        self.assertEqual(counts["web_imported"], 1)
        self.assertEqual(counts["console_imported"], 1)

    def test_provider_import_is_skipped_when_build_stage_fails(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            (root / "one.json").write_text(
                json.dumps(_record(suffix="one")), encoding="utf-8"
            )
            plan = importer.build_import_plan(root)
            with mock.patch(
                "grok2api.pool.accounts.import_auth_payloads_bulk",
                return_value={"ok": False},
            ), mock.patch(
                "grok2api.admin.provider_accounts.import_provider_accounts"
            ) as provider_import:
                counts = importer.execute_import(plan, dry_run=False)

        provider_import.assert_not_called()
        self.assertEqual(counts["build_import_failed"], 1)
        self.assertEqual(counts["web_skipped_build_failure"], 1)
        self.assertEqual(counts["console_skipped_build_failure"], 1)

    def test_missing_build_token_never_attempts_online_conversion(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            (root / "sso-only.json").write_text(
                json.dumps({"type": "xai", "sso": "sso-secret"}),
                encoding="utf-8",
            )
            plan = importer.build_import_plan(root)
            counts = importer.execute_import(plan, dry_run=True)

        self.assertEqual(counts["build_offline_token_missing"], 1)
        self.assertEqual(counts["upstream_network_conversions"], 0)
        self.assertEqual(counts["direct_upstream_requests"], 0)
        self.assertFalse(plan.provider_jobs)

    def test_public_output_never_contains_credentials_or_paths(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            secret = "highly-sensitive-sso-value"
            (root / "private-email.json").write_text(
                json.dumps(_record(suffix="one", sso=secret)), encoding="utf-8"
            )
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exit_code = importer.main([str(root), "--dry-run"])

        rendered = output.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertNotIn(secret, rendered)
        self.assertNotIn("private-email", rendered)
        self.assertNotIn(str(root), rendered)
        parsed = json.loads(rendered)
        self.assertEqual(set(parsed), {"ok", "cleanup_requested", "counts", "failure_categories"})

    def test_cleanup_removes_only_consumed_regular_files_after_success(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            consumed = root / "one.json"
            retained = root / "notes.txt"
            consumed.write_text(json.dumps(_record(suffix="one")), encoding="utf-8")
            retained.write_text("keep", encoding="utf-8")
            plan = importer.build_import_plan(root)
            counts = importer.execute_import(plan, dry_run=True)
            counts.pop("dry_run", None)
            importer.cleanup_imported_files(plan, counts)

            self.assertFalse(consumed.exists())
            self.assertTrue(retained.exists())
            self.assertEqual(counts["files_cleaned"], 1)

    def test_empty_directory_is_not_reported_as_success(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exit_code = importer.main([raw_dir, "--dry-run"])

        self.assertEqual(exit_code, 1)
        parsed = json.loads(output.getvalue())
        self.assertFalse(parsed["ok"])
        self.assertEqual(parsed["counts"]["json_files_missing"], 1)


if __name__ == "__main__":
    unittest.main()
