"""PostgreSQL backend for auth account map (auth.json equivalent)."""

from __future__ import annotations

import json
import threading
import time
from collections import OrderedDict
from typing import Any, Callable

from grok2api.store.pg import _ts, _unix, connection, json_dump, pg_enabled

try:
    from grok2api.pool.accounts import get_sso_value, has_sso_value, merge_durable_account_fields
except Exception:  # pragma: no cover - import cycle safety
    def get_sso_value(entry: dict[str, Any] | None) -> str:
        if not isinstance(entry, dict):
            return ""
        return str(entry.get("sso") or entry.get("sso_cookie") or entry.get("sso_token") or "").strip()

    def has_sso_value(entry: dict[str, Any] | None) -> bool:
        return bool(get_sso_value(entry))

    def merge_durable_account_fields(
        entry: dict[str, Any], old_entry: dict[str, Any] | None
    ) -> dict[str, Any]:
        if not isinstance(entry, dict) or not isinstance(old_entry, dict):
            return entry
        old_sso = get_sso_value(old_entry)
        if old_sso and not get_sso_value(entry):
            entry["sso"] = old_sso
            entry.setdefault("sso_cookie", old_sso)
        return entry

# Short process-local cache so every API request doesn't re-scan the whole
# accounts table before the first upstream byte (large pools otherwise add
# hundreds of ms of TTFT just reading auth).
_auth_map_cache: dict[str, Any] | None = None
_auth_map_cache_at = 0.0
_auth_map_cache_lock = threading.RLock()
_AUTH_MAP_CACHE_TTL = 2.0
_entry_cache: OrderedDict[str, tuple[float, str, dict[str, Any]]] = OrderedDict()


def _entry_cache_limits() -> tuple[int, float]:
    try:
        from grok2api.config import (
            ACCOUNT_PAYLOAD_CACHE_SIZE,
            ACCOUNT_PAYLOAD_CACHE_TTL_SEC,
        )

        return int(ACCOUNT_PAYLOAD_CACHE_SIZE), float(ACCOUNT_PAYLOAD_CACHE_TTL_SEC)
    except Exception:
        return 2048, 600.0


def enabled() -> bool:
    return pg_enabled()


def _sync_ready_account(account_id: str, entry: dict[str, Any]) -> None:
    try:
        from grok2api.config import READY_INDEX_MODE

        if READY_INDEX_MODE == "off":
            return
        from grok2api.upstream.oidc_auth import parse_expires_at
        from grok2api.store.ready_redis import get_status, sync_account

        token = entry.get("key") or entry.get("access_token") or entry.get("token")
        meta = get_status(account_id)
        if not meta:
            try:
                from grok2api.store.settings_pg import get_pool_meta

                meta = get_pool_meta(account_id)
            except Exception:
                meta = {}
        if not meta:
            meta = {"enabled": True, "pool_status": "normal", "state_version": 0}
        meta["expires_at"] = parse_expires_at(
            entry.get("expires_at"), token if isinstance(token, str) else None
        )
        sync_account(account_id, meta)
    except Exception:
        pass


def invalidate_auth_map_cache() -> None:
    global _auth_map_cache, _auth_map_cache_at
    with _auth_map_cache_lock:
        _auth_map_cache = None
        _auth_map_cache_at = 0.0
        _entry_cache.clear()


def invalidate_auth_entry_cache(account_id: str) -> None:
    """Invalidate one lazy payload while dropping any obsolete full-map snapshot."""
    global _auth_map_cache, _auth_map_cache_at
    aid = str(account_id or "").strip()
    with _auth_map_cache_lock:
        _auth_map_cache = None
        _auth_map_cache_at = 0.0
        if not aid:
            return
        for cache_key, (_at, resolved_id, payload) in list(_entry_cache.items()):
            if (
                cache_key == aid
                or resolved_id == aid
                or payload.get("user_id") == aid
                or payload.get("principal_id") == aid
            ):
                _entry_cache.pop(cache_key, None)


def read_auth_map() -> dict[str, Any]:
    global _auth_map_cache, _auth_map_cache_at
    if not enabled():
        return {}
    now = time.time()
    with _auth_map_cache_lock:
        if (
            _auth_map_cache is not None
            and now - _auth_map_cache_at < _AUTH_MAP_CACHE_TTL
        ):
            # Shallow copy is enough: callers treat entries as read-only snapshots.
            return dict(_auth_map_cache)
    out: dict[str, Any] = {}
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, payload FROM accounts")
            for row in cur.fetchall():
                aid, payload = row[0], row[1]
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except json.JSONDecodeError:
                        continue
                if isinstance(payload, dict):
                    out[str(aid)] = payload
    with _auth_map_cache_lock:
        _auth_map_cache = out
        _auth_map_cache_at = time.time()
        return dict(out)


def read_auth_entry(account_id: str) -> tuple[str, dict[str, Any]] | None:
    """O(1)-ish single-account read for sticky TTFT path.

    Prefer process cache when hot; otherwise SELECT one row by id / user_id
    instead of re-scanning the whole accounts table.
    """
    if not enabled() or not account_id:
        return None
    aid = str(account_id).strip()
    if not aid:
        return None
    now = time.time()
    max_entries, ttl = _entry_cache_limits()
    with _auth_map_cache_lock:
        cached_entry = _entry_cache.get(aid)
        if cached_entry is not None:
            cached_at, resolved_id, payload = cached_entry
            if now - cached_at < ttl:
                _entry_cache.move_to_end(aid)
                return resolved_id, dict(payload)
            _entry_cache.pop(aid, None)
    with _auth_map_cache_lock:
        if (
            _auth_map_cache is not None
            and now - _auth_map_cache_at < _AUTH_MAP_CACHE_TTL
        ):
            hit = _auth_map_cache.get(aid)
            if isinstance(hit, dict):
                return aid, dict(hit)
            for k, v in _auth_map_cache.items():
                if not isinstance(v, dict):
                    continue
                if (
                    k == aid
                    or v.get("user_id") == aid
                    or v.get("principal_id") == aid
                    or str(k).endswith(f"::{aid}")
                ):
                    return str(k), dict(v)
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, payload FROM accounts WHERE id = %s LIMIT 1",
                (aid,),
            )
            row = cur.fetchone()
            if not row:
                cur.execute(
                    """
                    SELECT id, payload FROM accounts
                    WHERE user_id = %s
                       OR payload->>'user_id' = %s
                       OR payload->>'principal_id' = %s
                       OR id LIKE %s
                    LIMIT 1
                    """,
                    (aid, aid, aid, f"%::{aid}"),
                )
                row = cur.fetchone()
    if not row:
        return None
    rid, payload = str(row[0]), row[1]
    decoded = _decode_payload(payload)
    if not isinstance(decoded, dict):
        return None
    if max_entries > 0:
        with _auth_map_cache_lock:
            _entry_cache[aid] = (time.time(), rid, dict(decoded))
            _entry_cache[rid] = (time.time(), rid, dict(decoded))
            _entry_cache.move_to_end(rid)
            while len(_entry_cache) > max_entries:
                _entry_cache.popitem(last=False)
    return rid, decoded


def _decode_payload(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return None
    return payload if isinstance(payload, dict) else None


def count_accounts() -> int:
    if not enabled():
        return 0
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM accounts")
            row = cur.fetchone()
    return int(row[0] or 0) if row else 0


# Admin list sort keys → SQL ORDER BY (accounts a [LEFT JOIN account_pool ap]).
# Default "newest" keeps freshly registered/imported accounts on page 1.
_ACCOUNT_SORT_SQL: dict[str, str] = {
    "newest": "a.updated_at DESC NULLS LAST, a.id DESC",
    "oldest": "a.updated_at ASC NULLS LAST, a.id ASC",
    "expires_desc": "a.expires_at DESC NULLS LAST, a.updated_at DESC",
    "expires_asc": "a.expires_at ASC NULLS LAST, a.updated_at DESC",
    "email_asc": "lower(COALESCE(a.email, '')) ASC, a.id ASC",
    "email_desc": "lower(COALESCE(a.email, '')) DESC, a.id DESC",
    "last_used_desc": "ap.last_used_at DESC NULLS LAST, a.updated_at DESC",
    "last_used_asc": "ap.last_used_at ASC NULLS LAST, a.updated_at DESC",
    "requests_desc": "COALESCE(ap.request_count, 0) DESC, a.updated_at DESC",
    "cooldown_first": (
        "(CASE WHEN ap.cooldown_until IS NOT NULL AND ap.cooldown_until > now() "
        "THEN 0 ELSE 1 END) ASC, a.updated_at DESC"
    ),
    "disabled_first": (
        "(CASE WHEN COALESCE(ap.enabled, true) = false "
        "OR COALESCE(ap.disabled_for_quota, false) = true THEN 0 ELSE 1 END) ASC, "
        "a.updated_at DESC"
    ),
}
_ACCOUNT_SORT_ALIASES: dict[str, str] = {
    "updated_desc": "newest",
    "updated_asc": "oldest",
    "new": "newest",
    "old": "oldest",
    "expiry_desc": "expires_desc",
    "expiry_asc": "expires_asc",
    "expire_desc": "expires_desc",
    "expire_asc": "expires_asc",
    "used_desc": "last_used_desc",
    "used_asc": "last_used_asc",
    "usage_desc": "requests_desc",
    "cooldown": "cooldown_first",
    "disabled": "disabled_first",
}


def normalize_account_sort(sort: str | None) -> str:
    key = str(sort or "newest").strip().lower().replace("-", "_")
    key = _ACCOUNT_SORT_ALIASES.get(key, key)
    if key not in _ACCOUNT_SORT_SQL:
        return "newest"
    return key


def list_account_summaries(
    *,
    q: str = "",
    page: int = 1,
    page_size: int = 25,
    sort: str | None = None,
    has_sso: bool | None = None,
) -> dict[str, Any]:
    """Paged account list for admin UI without loading the full auth map.

    Returns admin-safe fields only (no full access/refresh tokens).
    `sort` defaults to newest (updated_at DESC) so fresh registrations appear first.
    `has_sso=True` returns only accounts whose payload keeps a non-empty SSO cookie.
    `has_sso=False` returns only accounts without SSO.
    """
    sort_key = normalize_account_sort(sort)
    order_sql = _ACCOUNT_SORT_SQL[sort_key]
    needs_pool = sort_key in (
        "last_used_desc",
        "last_used_asc",
        "requests_desc",
        "cooldown_first",
        "disabled_first",
    )

    if not enabled():
        return {
            "accounts": [],
            "total": 0,
            "page": 1,
            "page_size": page_size,
            "total_pages": 1,
            "q": q,
            "sort": sort_key,
            "has_sso": has_sso,
        }

    query = (q or "").strip().lower()
    try:
        page_i = max(1, int(page))
    except Exception:
        page_i = 1
    try:
        size_i = int(page_size)
    except Exception:
        size_i = 25
    if size_i <= 0 or size_i >= 10000:
        # "all" mode still streams only summary fields
        size_i = 0
    else:
        size_i = max(1, min(200, size_i))

    like = f"%{query}%" if query else None
    sso_clause = ""
    if has_sso is True:
        sso_clause = (
            " AND ("
            " nullif(btrim(COALESCE(payload->>'sso','')), '') IS NOT NULL"
            " OR nullif(btrim(COALESCE(payload->>'sso_cookie','')), '') IS NOT NULL"
            " OR nullif(btrim(COALESCE(payload->>'sso_token','')), '') IS NOT NULL"
            " OR nullif(btrim(COALESCE(payload#>>'{session_cookies,sso}','')), '') IS NOT NULL"
            " OR nullif(btrim(COALESCE(payload#>>'{session_cookies,sso-rw}','')), '') IS NOT NULL"
            " OR nullif(btrim(COALESCE(payload#>>'{cookies,sso}','')), '') IS NOT NULL"
            " OR nullif(btrim(COALESCE(payload#>>'{cookies,sso-rw}','')), '') IS NOT NULL"
            " OR COALESCE(payload->>'cookie','') ILIKE '%%sso=%%'"
            " OR COALESCE(payload->>'cookies','') ILIKE '%%sso=%%'"
            " OR COALESCE(payload->>'set_cookie','') ILIKE '%%sso=%%'"
            " OR COALESCE(payload->>'set-cookie','') ILIKE '%%sso=%%'"
            " OR COALESCE(payload->>'set_cookies','') ILIKE '%%sso=%%'"
            " )"
        )
    elif has_sso is False:
        sso_clause = (
            " AND NOT ("
            " nullif(btrim(COALESCE(payload->>'sso','')), '') IS NOT NULL"
            " OR nullif(btrim(COALESCE(payload->>'sso_cookie','')), '') IS NOT NULL"
            " OR nullif(btrim(COALESCE(payload->>'sso_token','')), '') IS NOT NULL"
            " OR nullif(btrim(COALESCE(payload#>>'{session_cookies,sso}','')), '') IS NOT NULL"
            " OR nullif(btrim(COALESCE(payload#>>'{session_cookies,sso-rw}','')), '') IS NOT NULL"
            " OR nullif(btrim(COALESCE(payload#>>'{cookies,sso}','')), '') IS NOT NULL"
            " OR nullif(btrim(COALESCE(payload#>>'{cookies,sso-rw}','')), '') IS NOT NULL"
            " OR COALESCE(payload->>'cookie','') ILIKE '%%sso=%%'"
            " OR COALESCE(payload->>'cookies','') ILIKE '%%sso=%%'"
            " OR COALESCE(payload->>'set_cookie','') ILIKE '%%sso=%%'"
            " OR COALESCE(payload->>'set-cookie','') ILIKE '%%sso=%%'"
            " OR COALESCE(payload->>'set_cookies','') ILIKE '%%sso=%%'"
            " )"
        )
    with connection() as conn:
        with conn.cursor() as cur:
            if like:
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM accounts
                    WHERE (
                       lower(COALESCE(email,'')) LIKE %s
                       OR lower(id) LIKE %s
                       OR lower(COALESCE(user_id,'')) LIKE %s
                    ){sso_clause}
                    """,
                    (like, like, like),
                )
            else:
                if sso_clause:
                    cur.execute(f"SELECT COUNT(*) FROM accounts WHERE TRUE{sso_clause}")
                else:
                    cur.execute("SELECT COUNT(*) FROM accounts")
            total = int((cur.fetchone() or [0])[0] or 0)

            if size_i == 0:
                size_i = total or 0
                page_i = 1
                total_pages = 1
                offset = 0
                limit = None
            else:
                total_pages = max(1, (total + size_i - 1) // size_i) if total else 1
                page_i = min(page_i, total_pages)
                offset = (page_i - 1) * size_i
                limit = size_i

            if needs_pool:
                sql = """
                    SELECT a.id, a.email, a.user_id, a.team_id, a.payload, a.expires_at,
                           a.updated_at
                    FROM accounts a
                    LEFT JOIN account_pool ap ON ap.account_id = a.id
                """
            else:
                sql = """
                    SELECT a.id, a.email, a.user_id, a.team_id, a.payload, a.expires_at,
                           a.updated_at
                    FROM accounts a
                """
            params: list[Any] = []
            where_parts: list[str] = []
            if like:
                where_parts.append(
                    "("
                    " lower(COALESCE(a.email,'')) LIKE %s"
                    " OR lower(a.id) LIKE %s"
                    " OR lower(COALESCE(a.user_id,'')) LIKE %s"
                    ")"
                )
                params.extend([like, like, like])
            if has_sso is True:
                where_parts.append(
                    "("
                    " nullif(btrim(COALESCE(a.payload->>'sso','')), '') IS NOT NULL"
                    " OR nullif(btrim(COALESCE(a.payload->>'sso_cookie','')), '') IS NOT NULL"
                    " OR nullif(btrim(COALESCE(a.payload->>'sso_token','')), '') IS NOT NULL"
                    " OR nullif(btrim(COALESCE(a.payload#>>'{session_cookies,sso}','')), '') IS NOT NULL"
                    " OR nullif(btrim(COALESCE(a.payload#>>'{session_cookies,sso-rw}','')), '') IS NOT NULL"
                    " OR nullif(btrim(COALESCE(a.payload#>>'{cookies,sso}','')), '') IS NOT NULL"
                    " OR nullif(btrim(COALESCE(a.payload#>>'{cookies,sso-rw}','')), '') IS NOT NULL"
                    " OR COALESCE(a.payload->>'cookie','') ILIKE '%%sso=%%'"
                    " OR COALESCE(a.payload->>'cookies','') ILIKE '%%sso=%%'"
                    " OR COALESCE(a.payload->>'set_cookie','') ILIKE '%%sso=%%'"
                    " OR COALESCE(a.payload->>'set-cookie','') ILIKE '%%sso=%%'"
                    " OR COALESCE(a.payload->>'set_cookies','') ILIKE '%%sso=%%'"
                    ")"
                )
            elif has_sso is False:
                where_parts.append(
                    "NOT ("
                    " nullif(btrim(COALESCE(a.payload->>'sso','')), '') IS NOT NULL"
                    " OR nullif(btrim(COALESCE(a.payload->>'sso_cookie','')), '') IS NOT NULL"
                    " OR nullif(btrim(COALESCE(a.payload->>'sso_token','')), '') IS NOT NULL"
                    " OR nullif(btrim(COALESCE(a.payload#>>'{session_cookies,sso}','')), '') IS NOT NULL"
                    " OR nullif(btrim(COALESCE(a.payload#>>'{session_cookies,sso-rw}','')), '') IS NOT NULL"
                    " OR nullif(btrim(COALESCE(a.payload#>>'{cookies,sso}','')), '') IS NOT NULL"
                    " OR nullif(btrim(COALESCE(a.payload#>>'{cookies,sso-rw}','')), '') IS NOT NULL"
                    " OR COALESCE(a.payload->>'cookie','') ILIKE '%%sso=%%'"
                    " OR COALESCE(a.payload->>'cookies','') ILIKE '%%sso=%%'"
                    " OR COALESCE(a.payload->>'set_cookie','') ILIKE '%%sso=%%'"
                    " OR COALESCE(a.payload->>'set-cookie','') ILIKE '%%sso=%%'"
                    " OR COALESCE(a.payload->>'set_cookies','') ILIKE '%%sso=%%'"
                    ")"
                )
            if where_parts:
                sql += " WHERE " + " AND ".join(where_parts)
            sql += f" ORDER BY {order_sql}"
            if limit is not None:
                sql += " LIMIT %s OFFSET %s"
                params.extend([limit, offset])
            cur.execute(sql, params)
            rows = cur.fetchall()

    now = time.time()
    accounts: list[dict[str, Any]] = []
    for r in rows:
        aid = str(r[0])
        payload = _decode_payload(r[4]) or {}
        token = payload.get("key") or payload.get("access_token") or payload.get("token")
        # Skip empty credential rows (shouldn't happen)
        if not token and not payload.get("refresh_token"):
            # still show if email exists
            if not (r[1] or payload.get("email")):
                continue
        exp = _unix(r[5])
        if exp is None:
            # fall back to payload expires_at if column empty
            try:
                from grok2api.upstream.oidc_auth import parse_expires_at

                exp = parse_expires_at(
                    payload.get("expires_at"),
                    token if isinstance(token, str) else None,
                )
            except Exception:
                exp = None
        expired = bool(exp is not None and now >= float(exp))
        tok = token if isinstance(token, str) else None
        if tok and len(tok) > 12:
            hint = tok[:6] + "..." + tok[-4:]
        elif tok:
            hint = "****"
        else:
            hint = ""
        updated_at = _unix(r[6]) if len(r) > 6 else None
        accounts.append(
            {
                "id": aid,
                "email": r[1] or payload.get("email"),
                "user_id": r[2] or payload.get("user_id") or payload.get("principal_id"),
                "team_id": r[3] or payload.get("team_id"),
                "auth_mode": payload.get("auth_mode"),
                "create_time": payload.get("create_time"),
                "updated_at": updated_at,
                "expires_at": exp,
                "expired": expired,
                "has_refresh_token": bool(payload.get("refresh_token")),
                "has_sso": has_sso_value(payload),
                "token_hint": hint,
                "first_name": payload.get("first_name"),
                "last_name": payload.get("last_name"),
                "principal_type": payload.get("principal_type"),
                "source": payload.get("source"),
            }
        )

    return {
        "accounts": accounts,
        "total": total,
        "page": page_i,
        "page_size": size_i,
        "total_pages": max(1, (total + size_i - 1) // size_i) if size_i else 1,
        "q": (q or "").strip(),
        "sort": sort_key,
        "has_sso": has_sso,
    }


def write_auth_map(data: dict[str, Any]) -> None:
    """Replace full account set (import/export style)."""
    if not enabled():
        return
    data = data if isinstance(data, dict) else {}
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM accounts")
            existing = {r[0] for r in cur.fetchall()}
            incoming = set(data.keys())
            # upsert all
            for aid, entry in data.items():
                if not isinstance(entry, dict):
                    continue
                _upsert_one(cur, str(aid), entry)
            # delete removed
            for aid in existing - incoming:
                cur.execute("DELETE FROM accounts WHERE id = %s", (aid,))
                cur.execute("DELETE FROM account_pool WHERE account_id = %s", (aid,))
        conn.commit()
    invalidate_auth_map_cache()
    try:
        from grok2api.config import READY_INDEX_MODE
        from grok2api.store.ready_redis import rebuild_ready_index

        if READY_INDEX_MODE != "off":
            rebuild_ready_index()
    except Exception:
        pass


def mutate_auth_map(mutator: Callable[[dict[str, Any]], Any]) -> dict[str, Any]:
    """Transactional read-modify-write of the full map (compatible with file API)."""
    if not enabled():
        return {}
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, payload FROM accounts FOR UPDATE")
            data: dict[str, Any] = {}
            for aid, payload in cur.fetchall():
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except json.JSONDecodeError:
                        payload = {}
                if isinstance(payload, dict):
                    data[str(aid)] = payload
            mutator(data)
            # rewrite set
            cur.execute("SELECT id FROM accounts")
            existing = {r[0] for r in cur.fetchall()}
            incoming = set(data.keys())
            for aid, entry in data.items():
                if isinstance(entry, dict):
                    _upsert_one(cur, str(aid), entry)
            for aid in existing - incoming:
                cur.execute("DELETE FROM accounts WHERE id = %s", (aid,))
                cur.execute("DELETE FROM account_pool WHERE account_id = %s", (aid,))
        conn.commit()
    invalidate_auth_map_cache()
    return data


def upsert_account(account_id: str, entry: dict[str, Any]) -> None:
    if not enabled() or not account_id or not isinstance(entry, dict):
        return
    with connection() as conn:
        with conn.cursor() as cur:
            _upsert_one(cur, account_id, entry)
        conn.commit()
    invalidate_auth_entry_cache(account_id)
    _sync_ready_account(account_id, entry)


def upsert_account_merged(
    account_id: str,
    entry: dict[str, Any],
    *,
    merge_same_user: bool = True,
) -> str:
    """Row-level upsert + optional same-user/token dedupe without rewriting whole table."""
    if not enabled() or not account_id or not isinstance(entry, dict):
        return account_id
    uid = entry.get("user_id") or entry.get("principal_id")
    token = entry.get("key")
    with connection() as conn:
        with conn.cursor() as cur:
            if merge_same_user and (uid or token):
                # Preserve durable metadata from the row(s) this import may replace.
                if uid and token:
                    cur.execute(
                        """
                        SELECT payload FROM accounts
                        WHERE id = %s
                           OR user_id = %s
                           OR payload->>'user_id' = %s
                           OR payload->>'principal_id' = %s
                           OR payload->>'key' = %s
                        """,
                        (account_id, str(uid), str(uid), str(uid), str(token)),
                    )
                elif uid:
                    cur.execute(
                        """
                        SELECT payload FROM accounts
                        WHERE id = %s
                           OR user_id = %s
                           OR payload->>'user_id' = %s
                           OR payload->>'principal_id' = %s
                        """,
                        (account_id, str(uid), str(uid), str(uid)),
                    )
                else:
                    cur.execute(
                        """
                        SELECT payload FROM accounts
                        WHERE id = %s OR payload->>'key' = %s
                        """,
                        (account_id, str(token)),
                    )
                for old_row in cur.fetchall() or []:
                    old_payload = _decode_payload(old_row[0])
                    if isinstance(old_payload, dict):
                        merge_durable_account_fields(entry, old_payload)

                # Drop other rows that collide on user_id / access token.
                if uid and token:
                    cur.execute(
                        """
                        DELETE FROM accounts
                        WHERE id <> %s
                          AND (
                            user_id = %s
                            OR payload->>'user_id' = %s
                            OR payload->>'principal_id' = %s
                            OR payload->>'key' = %s
                          )
                        """,
                        (account_id, str(uid), str(uid), str(uid), str(token)),
                    )
                elif uid:
                    cur.execute(
                        """
                        DELETE FROM accounts
                        WHERE id <> %s
                          AND (
                            user_id = %s
                            OR payload->>'user_id' = %s
                            OR payload->>'principal_id' = %s
                          )
                        """,
                        (account_id, str(uid), str(uid), str(uid)),
                    )
                elif token:
                    cur.execute(
                        """
                        DELETE FROM accounts
                        WHERE id <> %s AND payload->>'key' = %s
                        """,
                        (account_id, str(token)),
                    )
                # Clean orphan pool rows for deleted accounts
                cur.execute(
                    """
                    DELETE FROM account_pool ap
                    WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.id = ap.account_id)
                    """
                )
            _upsert_one(cur, account_id, entry)
        conn.commit()
    invalidate_auth_map_cache()
    _sync_ready_account(account_id, entry)
    return account_id


def delete_account(account_id: str) -> bool:
    if not enabled() or not account_id:
        return False
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM accounts WHERE id = %s", (account_id,))
            deleted = cur.rowcount > 0
            cur.execute("DELETE FROM account_pool WHERE account_id = %s", (account_id,))
        conn.commit()
    if deleted:
        invalidate_auth_entry_cache(account_id)
        try:
            from grok2api.store.ready_redis import remove

            remove(account_id)
        except Exception:
            pass
    return deleted


def _upsert_one(cur, account_id: str, entry: dict[str, Any]) -> None:
    email = entry.get("email")
    user_id = entry.get("user_id") or entry.get("principal_id")
    team_id = entry.get("team_id")
    expires_at = _ts(entry.get("expires_at"))
    # Preserve durable SSO / register password if a later write omits them
    # (e.g. token refresh / re-import without cookie).
    try:
        cur.execute("SELECT payload FROM accounts WHERE id = %s", (account_id,))
        row = cur.fetchone()
        if row and isinstance(row[0], dict):
            old_payload = row[0]
        elif row and isinstance(row[0], str):
            import json as _json

            old_payload = _json.loads(row[0]) if row[0] else {}
        else:
            old_payload = {}
        if isinstance(old_payload, dict):
            merge_durable_account_fields(entry, old_payload)
        else:
            merge_durable_account_fields(entry, None)
    except Exception:
        pass
    cur.execute(
        """
        INSERT INTO accounts (id, email, user_id, team_id, payload, expires_at, updated_at)
        VALUES (%s, %s, %s, %s, %s::jsonb, %s, now())
        ON CONFLICT (id) DO UPDATE SET
          email = EXCLUDED.email,
          user_id = EXCLUDED.user_id,
          team_id = EXCLUDED.team_id,
          payload = EXCLUDED.payload,
          expires_at = EXCLUDED.expires_at,
          updated_at = now()
        """,
        (
            account_id,
            email,
            user_id,
            team_id,
            json_dump(entry),
            expires_at,
        ),
    )
    # Every account must have a durable pool status row in PostgreSQL.
    # Do not overwrite existing cooldown/status — only create defaults for new ids.
    cur.execute(
        """
        INSERT INTO account_pool (
          account_id, enabled, weight, disabled_for_quota, blocked_models,
          request_count, success_count, fail_count, extra, updated_at,
          pool_status, cooldown_count
        ) VALUES (
          %s, true, 1, false, '{}'::jsonb,
          0, 0, 0, '{}'::jsonb, now(),
          'normal', 0
        )
        ON CONFLICT (account_id) DO NOTHING
        """,
        (account_id,),
    )
