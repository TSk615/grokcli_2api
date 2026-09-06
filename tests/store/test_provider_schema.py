from __future__ import annotations

import unittest
from unittest import mock

from grok2api.store import accounts_pg, pg, ready_redis


def _compact(sql: str) -> str:
    return " ".join(sql.lower().split())


class ProviderSchemaTests(unittest.TestCase):
    def test_fresh_accounts_schema_has_safe_build_defaults(self) -> None:
        schema = _compact(pg.SCHEMA_SQL)
        self.assertIn("provider text not null default 'grok_build'", schema)
        self.assertIn("auth_type text not null default 'oauth'", schema)
        for column in (
            "source_key text",
            "credential_enc text",
            "web_tier text",
            "egress_identity text",
        ):
            self.assertIn(column, schema)

    def test_existing_accounts_receive_additive_provider_migrations(self) -> None:
        migrations = [_compact(stmt) for stmt in pg._SCHEMA_MIGRATIONS]
        joined = "\n".join(migrations)
        self.assertIn(
            "alter table accounts add column if not exists provider text not null default 'grok_build'",
            joined,
        )
        self.assertIn(
            "alter table accounts add column if not exists auth_type text not null default 'oauth'",
            joined,
        )
        self.assertIn(
            "create unique index if not exists idx_accounts_provider_source_key on accounts (provider, source_key) where source_key is not null",
            joined,
        )
        # Index creation must follow the ALTERs so an existing pre-provider DB
        # can be upgraded by the idempotent migration runner.
        provider_alter = next(
            i
            for i, stmt in enumerate(migrations)
            if stmt.startswith("alter table accounts add column if not exists provider ")
        )
        provider_index = next(
            i
            for i, stmt in enumerate(migrations)
            if "idx_accounts_provider_source_key" in stmt
        )
        self.assertLess(provider_alter, provider_index)

    def test_model_routes_enforces_provider_scoped_uniqueness(self) -> None:
        joined = "\n".join(_compact(stmt) for stmt in pg._SCHEMA_MIGRATIONS)
        self.assertIn("create table if not exists model_routes", joined)
        for column in (
            "public_model text not null",
            "provider text not null",
            "upstream_model text not null",
            "capability text not null",
            "priority int not null default 1",
            "enabled boolean not null default true",
            "minimum_tier text",
            "extra jsonb not null default '{}'::jsonb",
        ):
            self.assertIn(column, joined)
        self.assertIn(
            "create unique index if not exists idx_model_routes_unique on model_routes (public_model, provider, capability)",
            joined,
        )

    def test_both_usage_event_tables_and_union_view_expose_provider(self) -> None:
        joined = "\n".join(_compact(stmt) for stmt in pg._SCHEMA_MIGRATIONS)
        for table in ("usage_events", "usage_events_partitioned"):
            self.assertIn(
                f"alter table {table} add column if not exists provider text not null default 'grok_build'",
                joined,
            )
            self.assertIn(
                f"alter table {table} add column if not exists upstream_model text",
                joined,
            )
        view = next(
            _compact(stmt)
            for stmt in pg._SCHEMA_MIGRATIONS
            if "create or replace view usage_events_all" in stmt.lower()
        )
        self.assertEqual(view.count("provider, upstream_model"), 2)


class _RecordingCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.rowcount = -1

    def execute(self, sql: str, params=None) -> None:
        self.calls.append((sql, params))
        self.rowcount = 1 if "INSERT INTO accounts" in sql else -1

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _RecordingConnection:
    def __init__(self, cursor: _RecordingCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _RecordingCursor:
        return self._cursor

    def commit(self) -> None:
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class ProviderAccountUpsertTests(unittest.TestCase):
    def test_credential_envelope_lookup_is_provider_scoped(self) -> None:
        cursor = _RecordingCursor()
        cursor.fetchone = lambda: ("enc:v1:ciphertext", "web-sso:fingerprint")
        connection = _RecordingConnection(cursor)
        with mock.patch.object(accounts_pg, "enabled", return_value=True), mock.patch.object(
            accounts_pg, "connection", return_value=connection
        ):
            result = accounts_pg.read_provider_credential_envelope(
                "shared-id", "GROK_WEB"
            )

        self.assertEqual(result, ("enc:v1:ciphertext", "web-sso:fingerprint"))
        sql, params = cursor.calls[0]
        self.assertIn("id = %s AND provider = %s", " ".join(sql.split()))
        self.assertEqual(params, ("shared-id", "grok_web"))

    def test_admin_list_can_be_provider_filtered(self) -> None:
        cursor = _RecordingCursor()
        cursor.fetchone = lambda: (0,)
        connection = _RecordingConnection(cursor)
        with mock.patch.object(accounts_pg, "enabled", return_value=True), mock.patch.object(
            accounts_pg, "connection", return_value=connection
        ):
            result = accounts_pg.list_account_summaries(provider="GROK_WEB")

        self.assertEqual(result["provider"], "grok_web")
        count_sql, count_params = cursor.calls[0]
        self.assertIn("provider = %s", count_sql)
        self.assertEqual(count_params, ("grok_web",))
        list_sql, list_params = cursor.calls[1]
        self.assertIn("a.provider = %s", list_sql)
        self.assertEqual(list_params[0], "grok_web")

    def test_legacy_upsert_uses_defaults_without_payload_rewrite(self) -> None:
        cursor = _RecordingCursor()
        payload = {"email": "build@example.test", "key": "token"}

        accounts_pg._upsert_one(cursor, "build-1", payload)

        account_sql, params = next(
            (sql, values)
            for sql, values in cursor.calls
            if "INSERT INTO accounts" in sql
        )
        self.assertIn("COALESCE(%s, 'grok_build')", account_sql)
        self.assertIn("COALESCE(%s, 'oauth')", account_sql)
        self.assertIsNone(params[6])
        self.assertIsNone(params[7])
        self.assertEqual(payload, {"email": "build@example.test", "key": "token"})

    def test_provider_upsert_writes_metadata_separately(self) -> None:
        cursor = _RecordingCursor()
        payload = {"email": "web@example.test"}

        accounts_pg._upsert_one(
            cursor,
            "web-1",
            payload,
            provider="grok_web",
            auth_type="sso",
            source_key="subject-1",
            credential_enc="enc:v1:ciphertext",
            web_tier="super",
            egress_identity="edge-a",
        )

        _, params = next(
            (sql, values)
            for sql, values in cursor.calls
            if "INSERT INTO accounts" in sql
        )
        self.assertEqual(
            params[6:12],
            (
                "grok_web",
                "sso",
                "subject-1",
                "enc:v1:ciphertext",
                "super",
                "edge-a",
            ),
        )
        self.assertEqual(payload, {"email": "web@example.test"})

    def test_account_upsert_rejects_cross_provider_id_conflict(self) -> None:
        class _ConflictingCursor(_RecordingCursor):
            def execute(self, sql: str, params=None) -> None:
                super().execute(sql, params)
                if "INSERT INTO accounts" in sql:
                    self.rowcount = 0

        cursor = _ConflictingCursor()
        with self.assertRaisesRegex(ValueError, "different provider"):
            accounts_pg._upsert_one(
                cursor,
                "grok_web::shared-id",
                {"key": "build-token"},
                provider="grok_build",
                auth_type="oauth",
            )

        account_sql, params = next(
            (sql, values)
            for sql, values in cursor.calls
            if "INSERT INTO accounts" in sql
        )
        self.assertIn(
            "where accounts.provider = coalesce(%s, 'grok_build')",
            _compact(account_sql),
        )
        self.assertEqual(params[-1], "grok_build")
        self.assertFalse(
            any("INSERT INTO account_pool" in sql for sql, _ in cursor.calls)
        )


class _ReadyIndexRedis:
    def delete(self, *keys) -> None:
        return None

    def zcard(self, key) -> int:
        return 0

    def set(self, key, value) -> None:
        return None


class ReadyIndexIsolationTests(unittest.TestCase):
    def test_rebuild_indexes_only_build_accounts(self) -> None:
        cursor = _RecordingCursor()
        connection = _RecordingConnection(cursor)
        with mock.patch.object(ready_redis, "redis_enabled", return_value=True), \
             mock.patch.object(ready_redis, "get_client", return_value=_ReadyIndexRedis()), \
             mock.patch("grok2api.store.pg.connection", return_value=connection):
            self.assertEqual(ready_redis.rebuild_ready_index(), 0)

        sql, params = cursor.calls[0]
        self.assertIn("where a.provider = %s", _compact(sql))
        self.assertEqual(params, ("grok_build", "", 1000))


class LegacyBuildIsolationTests(unittest.TestCase):
    def _patch_store(self, cursor: _RecordingCursor):
        return (
            mock.patch.object(accounts_pg, "enabled", return_value=True),
            mock.patch.object(
                accounts_pg,
                "connection",
                return_value=_RecordingConnection(cursor),
            ),
            mock.patch.object(accounts_pg, "_auth_map_cache", None),
            mock.patch.object(accounts_pg, "_auth_map_cache_at", 0.0),
        )

    def test_legacy_map_reads_only_build_accounts(self) -> None:
        cursor = _RecordingCursor()
        patches = self._patch_store(cursor)
        with patches[0], patches[1], patches[2], patches[3]:
            self.assertEqual(accounts_pg.read_auth_map(), {})

        sql, params = cursor.calls[0]
        self.assertIn("where provider = %s", _compact(sql))
        self.assertEqual(params, ("grok_build",))

    def test_legacy_entry_lookup_is_build_scoped(self) -> None:
        cursor = _RecordingCursor()
        patches = self._patch_store(cursor)
        with patches[0], patches[1], patches[2], patches[3]:
            self.assertIsNone(accounts_pg.read_auth_entry("shared-id"))

        self.assertEqual(len(cursor.calls), 2)
        for sql, params in cursor.calls:
            self.assertIn("provider = %s", _compact(sql))
            self.assertEqual(params[0], "grok_build")

    def test_full_map_replace_only_compares_build_rows(self) -> None:
        cursor = _RecordingCursor()
        patches = self._patch_store(cursor)
        with patches[0], patches[1], patches[2], patches[3], mock.patch.object(
            accounts_pg, "invalidate_auth_map_cache"
        ):
            accounts_pg.write_auth_map({})

        sql, params = cursor.calls[0]
        self.assertIn("where provider = %s", _compact(sql))
        self.assertEqual(params, ("grok_build",))

    def test_transactional_mutation_locks_only_build_rows(self) -> None:
        cursor = _RecordingCursor()
        patches = self._patch_store(cursor)
        with patches[0], patches[1], patches[2], patches[3], mock.patch.object(
            accounts_pg, "invalidate_auth_map_cache"
        ):
            self.assertEqual(accounts_pg.mutate_auth_map(lambda data: data.clear()), {})

        account_queries = [
            (sql, params)
            for sql, params in cursor.calls
            if "from accounts" in _compact(sql)
        ]
        self.assertEqual(len(account_queries), 2)
        for sql, params in account_queries:
            self.assertIn("where provider = %s", _compact(sql))
            self.assertEqual(params, ("grok_build",))

    def test_legacy_merge_dedupe_never_crosses_provider_boundary(self) -> None:
        cursor = _RecordingCursor()
        patches = self._patch_store(cursor)
        with patches[0], patches[1], patches[2], patches[3], mock.patch.object(
            accounts_pg, "invalidate_auth_map_cache"
        ), mock.patch.object(accounts_pg, "_sync_ready_account"):
            accounts_pg.upsert_account_merged(
                "build-1",
                {"user_id": "shared-user", "key": "shared-token"},
            )

        # The merge lookup and collision delete are the first two statements;
        # later calls belong to the final row upsert and orphan-pool cleanup.
        account_queries = cursor.calls[:2]
        self.assertEqual(len(account_queries), 2)
        for sql, params in account_queries:
            self.assertIn("provider = %s", _compact(sql))
            self.assertEqual(params[0], "grok_build")


if __name__ == "__main__":
    unittest.main()
