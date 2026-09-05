"""Account (auth.json) management — standalone, no local Grok CLI.

Supports:
  - Native OIDC device-code (no grok binary; works on headless Linux)
  - Multi-account import / merge (per-user storage keys)
  - Token refresh via refresh_token
"""

from __future__ import annotations

import json
import re
import shutil
import sys
import time
import uuid
from typing import Any

from grok2api.pool.auth_store import read_auth_map, write_auth_map
from grok2api.config import AUTH_FILE
from grok2api.upstream.oidc_auth import (
    account_storage_id,
    decode_jwt_claims,
    get_device_session as oidc_get_device_session,
    list_device_sessions as oidc_list_device_sessions,
    normalize_auth_file_keys,
    parse_expires_at,
    refresh_all_accounts,
    start_device_authorization,
    upsert_entry,
)


def _mask_token(token: str | None) -> str:
    if not token:
        return ""
    if len(token) <= 12:
        return "****"
    return token[:6] + "..." + token[-4:]


_SSO_COOKIE_RE = re.compile(r"(?:^|[;,\s])sso(?:-rw)?=([^;,\s]+)", re.IGNORECASE)


def get_sso_value(entry: dict[str, Any] | None) -> str:
    """Return a saved xAI SSO cookie from known account payload shapes."""
    if not isinstance(entry, dict):
        return ""
    for key in ("sso", "sso_cookie", "sso_token"):
        val = entry.get(key)
        if isinstance(val, str) and val.strip():
            text = val.strip()
            if text.lower().startswith("sso="):
                return text.split("=", 1)[1].strip()
            return text
    for key in ("session_cookies", "cookies"):
        val = entry.get(key)
        if isinstance(val, dict):
            for cookie_key in ("sso", "sso-rw"):
                cookie_val = val.get(cookie_key)
                if isinstance(cookie_val, str) and cookie_val.strip():
                    return cookie_val.strip()
    for key in ("cookie", "cookies", "set_cookie", "set-cookie", "set_cookies"):
        val = entry.get(key)
        if isinstance(val, str):
            match = _SSO_COOKIE_RE.search(val)
            if match:
                return match.group(1).strip()
    return ""


def has_sso_value(entry: dict[str, Any] | None) -> bool:
    return bool(get_sso_value(entry))


_DURABLE_ACCOUNT_FIELDS = (
    "sso",
    "sso_cookie",
    "sso_token",
    "session_cookies",
    "cookies",
    "cookie",
    "set_cookie",
    "set-cookie",
    "set_cookies",
    "password",
    "register_password",
    "registration_session_id",
    "registration_batch_id",
    "sso_backup_path",
    "source",
)


def merge_durable_account_fields(
    entry: dict[str, Any], old_entry: dict[str, Any] | None
) -> dict[str, Any]:
    """Carry durable SSO/register metadata across token refresh/import overwrites."""
    if not isinstance(entry, dict):
        return entry
    if isinstance(old_entry, dict):
        old_sso = get_sso_value(old_entry)
        if not get_sso_value(entry) and old_sso:
            entry["sso"] = old_sso
            entry.setdefault("sso_cookie", old_sso)
        for key in _DURABLE_ACCOUNT_FIELDS:
            old_v = old_entry.get(key)
            new_v = entry.get(key)
            if (new_v is None or new_v == "") and old_v not in (None, ""):
                entry[key] = old_v
    sso_val = get_sso_value(entry)
    if sso_val:
        entry["sso"] = sso_val
        entry.setdefault("sso_cookie", sso_val)
    if not entry.get("password") and entry.get("register_password"):
        entry["password"] = entry.get("register_password")
    if not entry.get("register_password") and entry.get("password"):
        entry["register_password"] = entry.get("password")
    return entry


def merge_imported_account_fields(
    entry: dict[str, Any], old_entry: dict[str, Any] | None
) -> dict[str, Any]:
    """Merge a repeated import without rolling credentials backwards.

    Access/refresh tokens form one credential generation.  Prefer the entry
    whose access token expires later; this keeps stale backups from replacing a
    token that was refreshed in the running service, while still allowing a
    fresh device-flow/SSO import to replace an invalid stored refresh token.
    When the incoming entry has no refresh token, carry the stored one forward.
    """
    merge_durable_account_fields(entry, old_entry)
    if not isinstance(old_entry, dict):
        return entry

    incoming_token = entry.get("key") or entry.get("access_token") or entry.get("token")
    current_token = (
        old_entry.get("key") or old_entry.get("access_token") or old_entry.get("token")
    )
    incoming_exp = parse_expires_at(
        entry.get("expires_at"), incoming_token if isinstance(incoming_token, str) else None
    )
    current_exp = parse_expires_at(
        old_entry.get("expires_at"), current_token if isinstance(current_token, str) else None
    )

    keep_current_generation = False
    if current_token and incoming_token and current_token != incoming_token:
        if current_exp is not None and incoming_exp is not None:
            keep_current_generation = float(current_exp) > float(incoming_exp)
        elif current_exp is not None and incoming_exp is None:
            keep_current_generation = True
    elif current_token and current_token == incoming_token:
        # Same access token means the import cannot prove its refresh token is
        # newer.  Keep the durable value already used by the running service.
        keep_current_generation = True

    if keep_current_generation:
        for key in ("key", "access_token", "token", "expires_at", "refresh_token", "id_token"):
            current_value = old_entry.get(key)
            if current_value not in (None, ""):
                entry[key] = current_value
    elif not entry.get("refresh_token") and old_entry.get("refresh_token"):
        entry["refresh_token"] = old_entry["refresh_token"]
    return entry


def _merge_normalized_duplicate(
    current: dict[str, Any] | None, incoming: dict[str, Any]
) -> dict[str, Any]:
    """Deterministically merge duplicate identities inside one import batch."""
    if not isinstance(current, dict):
        return incoming
    merged = dict(incoming)
    return merge_imported_account_fields(merged, current)


def _accounts_store_source() -> str:
    """Where list/status currently reads from: postgres | file."""
    try:
        from grok2api.pool.auth_store import _pg_accounts

        if _pg_accounts() is not None:
            return "postgres"
    except Exception:
        pass
    return "file"


def list_accounts() -> list[dict[str, Any]]:
    """List all session entries from durable store (PostgreSQL when enabled).

    No full tokens are returned — only admin-safe fields.
    """
    data = read_auth_map()  # PG-first via auth_store
    if not data:
        return []

    now = time.time()
    out: list[dict[str, Any]] = []
    for key, entry in data.items():
        if not isinstance(entry, dict):
            continue
        token = entry.get("key") or entry.get("access_token") or entry.get("token")
        if not token:
            continue
        exp_f = parse_expires_at(
            entry.get("expires_at"), token if isinstance(token, str) else None
        )
        expired = bool(exp_f is not None and now >= exp_f)
        out.append(
            {
                "id": key,
                "email": entry.get("email"),
                "user_id": entry.get("user_id") or entry.get("principal_id"),
                "team_id": entry.get("team_id"),
                "auth_mode": entry.get("auth_mode"),
                "create_time": entry.get("create_time"),
                "expires_at": exp_f,
                "expired": expired,
                "has_refresh_token": bool(entry.get("refresh_token")),
                "has_sso": has_sso_value(entry),
                "token_hint": _mask_token(token if isinstance(token, str) else None),
                "first_name": entry.get("first_name"),
                "last_name": entry.get("last_name"),
                "principal_type": entry.get("principal_type"),
                "source": entry.get("source"),
            }
        )
    out.sort(key=lambda a: a.get("expires_at") or 0, reverse=True)
    return out


def account_status(*, include_accounts: bool = True) -> dict[str, Any]:
    """Account summary for admin UI.

    `include_accounts=False` returns counts only — used by frequent /status polls
    so a 400+ account list is not re-serialized on every heartbeat.
    Data source is PostgreSQL when hybrid/store backend is enabled.
    """
    source = _accounts_store_source()
    if include_accounts:
        all_accounts = list_accounts()
        active = [a for a in all_accounts if not a.get("expired")]
        account_count = len(all_accounts)
        active_count = len(active)
    else:
        # Cheap path: prefer SQL counts on PostgreSQL; avoid loading full payloads.
        account_count = 0
        active_count = 0
        all_accounts = []
        used_sql = False
        try:
            from grok2api.store.accounts_pg import enabled as pg_on, count_accounts
            from grok2api.store.pg import connection

            if pg_on():
                account_count = int(count_accounts())
                with connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT COUNT(*) FROM accounts
                            WHERE expires_at IS NULL OR expires_at > now()
                            """
                        )
                        active_count = int((cur.fetchone() or [0])[0] or 0)
                used_sql = True
        except Exception:
            used_sql = False
        if not used_sql:
            data = read_auth_map()
            now = time.time()
            for entry in data.values():
                if not isinstance(entry, dict):
                    continue
                token = entry.get("key") or entry.get("access_token") or entry.get("token")
                if not token:
                    continue
                account_count += 1
                exp_f = parse_expires_at(
                    entry.get("expires_at"), token if isinstance(token, str) else None
                )
                if exp_f is None or now < exp_f:
                    active_count += 1
    try:
        from grok2api.admin.settings_store import get_account_mode

        mode = get_account_mode()
    except Exception:
        mode = "round_robin"
    out = {
        "store_source": source,
        "store_backend": "postgres" if source == "postgres" else "file",
        "auth_file": str(AUTH_FILE),
        "auth_file_exists": AUTH_FILE.is_file(),
        "auth_file_role": "mirror" if source == "postgres" else "primary",
        "logged_in": bool(active_count),
        "account_count": account_count,
        "active_count": active_count,
        "account_mode": mode,
        "platform": sys.platform,
        "is_linux": sys.platform.startswith("linux"),
        "is_headless": _is_headless(),
        "native_oidc_available": True,
        "multi_account": account_count > 1,
    }
    if include_accounts:
        out["accounts"] = all_accounts
    return out


def _is_headless() -> bool:
    if sys.platform == "win32":
        return False
    import os

    display = os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")
    return not bool(display)


def _resolve_account_key(data: dict, account_id: str) -> str | None:
    if account_id in data:
        return account_id
    for k, v in data.items():
        if k == account_id:
            return k
        if isinstance(v, dict) and (
            v.get("user_id") == account_id or k.endswith(f"::{account_id}")
        ):
            return k
    return None


def _cleanup_account_side_state(account_ids: list[str]) -> None:
    """Clear pool meta + redis cooldown for deleted account ids (best-effort)."""
    ids = [str(x).strip() for x in (account_ids or []) if str(x).strip()]
    if not ids:
        return
    try:
        from grok2api.admin.settings_store import get_account_pool_state, save_account_pool_state

        state = get_account_pool_state()
        changed = False
        for aid in ids:
            if aid in state:
                state.pop(aid, None)
                changed = True
        if changed:
            save_account_pool_state(state)
    except Exception:
        pass
    for aid in ids:
        try:
            from grok2api.store.pool_redis import clear_cooldown

            clear_cooldown(aid)
        except Exception:
            pass
        # stats hash if present
        try:
            from grok2api.store.redis_client import delete, key, redis_enabled

            if redis_enabled():
                delete(key("stats", aid), key("cooldown", aid))
        except Exception:
            pass
    try:
        from grok2api.pool.account_pool import invalidate_pool_summary_cache

        invalidate_pool_summary_cache()
    except Exception:
        pass


# File-mode safety net only. When PostgreSQL is primary, auth.json is not used
# at runtime — recovery is PG / admin export, not local .bak spam.
_AUTH_BAK_KEEP = 5


def _auth_file_is_primary() -> bool:
    return _accounts_store_source() != "postgres"


def _auth_bak_paths() -> list:
    parent = AUTH_FILE.parent
    if not parent.is_dir():
        return []
    # Historical names: auth.bak.<ts> (from Path.with_suffix) and auth.json.bak.<ts>
    stem = AUTH_FILE.name  # auth.json
    legacy = AUTH_FILE.stem  # auth
    out = []
    try:
        for p in parent.iterdir():
            name = p.name
            if not p.is_file():
                continue
            if name.startswith(f"{stem}.bak.") or name.startswith(f"{legacy}.bak."):
                out.append(p)
    except OSError:
        return []
    out.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    return out


def _prune_auth_backups(keep: int = _AUTH_BAK_KEEP) -> None:
    keep = max(0, int(keep))
    for old in _auth_bak_paths()[keep:]:
        try:
            old.unlink()
        except OSError:
            pass


def _backup_auth_file() -> None:
    """Snapshot auth.json only when it is the durable primary store."""
    if not _auth_file_is_primary():
        return
    if not AUTH_FILE.is_file():
        return
    backup = AUTH_FILE.with_name(f"{AUTH_FILE.name}.bak.{int(time.time())}")
    try:
        shutil.copy2(AUTH_FILE, backup)
    except OSError:
        return
    _prune_auth_backups(_AUTH_BAK_KEEP)


def remove_account(account_id: str) -> bool:
    """Delete one account from durable store (PostgreSQL when enabled) + side state."""
    data = read_auth_map()
    matched = _resolve_account_key(data, account_id)
    if matched is None:
        return False
    _backup_auth_file()
    del data[matched]
    write_auth_map(data)  # PG primary (no auth.json mirror)
    _cleanup_account_side_state([matched, account_id])
    return True


def remove_accounts(account_ids: list[str]) -> dict:
    """Remove many accounts from durable store (PG/file) in one rewrite."""
    data = read_auth_map()
    removed: list[str] = []
    missing: list[str] = []
    seen: set[str] = set()
    for raw in account_ids:
        account_id = str(raw or "").strip()
        if not account_id or account_id in seen:
            continue
        seen.add(account_id)
        matched = _resolve_account_key(data, account_id)
        if matched is None or matched not in data:
            missing.append(account_id)
            continue
        del data[matched]
        removed.append(matched)
    if removed:
        _backup_auth_file()
        write_auth_map(data)  # PG primary (no auth.json mirror)
        _cleanup_account_side_state(removed)
    return {
        "removed": removed,
        "missing": missing,
        "removed_count": len(removed),
        "missing_count": len(missing),
        "requested": len(seen),
    }


def clear_all_accounts() -> bool:
    """Clear every account from durable store (PostgreSQL, or auth.json in file mode)."""
    _backup_auth_file()
    try:
        # Empty map → PG write_auth_map deletes all account + pool rows.
        write_auth_map({})
    except Exception:
        # Fallback: try file-only wipe (file mode / PG unavailable)
        try:
            if AUTH_FILE.is_file():
                AUTH_FILE.unlink()
        except OSError:
            return False
    # Extra safety: direct PG wipe if write path partially failed
    try:
        from grok2api.store.accounts_pg import enabled as pg_on, write_auth_map as pg_write

        if pg_on():
            pg_write({})
    except Exception:
        pass
    try:
        from grok2api.admin.settings_store import save_account_pool_state

        save_account_pool_state({})
    except Exception:
        pass
    # Best-effort: wipe redis cooldown/stats keys is expensive; clear known pool ids only.
    try:
        from grok2api.store.redis_client import delete, key, redis_enabled, get_client

        if redis_enabled():
            c = get_client()
            if c is not None:
                for pattern in (key("cooldown", "*"), key("stats", "*")):
                    try:
                        for k in c.scan_iter(match=pattern, count=200):
                            delete(k)
                    except Exception:
                        pass
    except Exception:
        pass
    try:
        from grok2api.pool.account_pool import invalidate_pool_summary_cache

        invalidate_pool_summary_cache()
    except Exception:
        pass
    # File mode only: leave an empty auth.json so tools that open AUTH_FILE don't 404.
    # Hybrid/PG mode never depends on this file at runtime.
    if _auth_file_is_primary():
        try:
            AUTH_FILE.parent.mkdir(parents=True, exist_ok=True)
            AUTH_FILE.write_text("{}", encoding="utf-8")
        except OSError:
            pass
    return True


def get_login_session(session_id: str) -> dict[str, Any] | None:
    return oidc_get_device_session(session_id)


def list_login_sessions() -> list[dict[str, Any]]:
    return oidc_list_device_sessions()


def start_login(mode: str = "device", *, capture: bool | None = None) -> dict[str, Any]:
    """
    Start native OIDC device-code login only.

    No local Grok CLI, no browser OAuth. Works on headless Linux.
    `mode` / `capture` kept for API compatibility; only device flow is used.
    """
    _ = capture  # unused; always native OIDC poll
    mode = (mode or "device").lower()
    if mode not in ("device", "oauth"):
        return {"ok": False, "error": "mode must be device (oauth removed)"}
    if mode == "oauth":
        # OAuth / local CLI login removed — fall through to device flow
        mode = "device"

    try:
        try:
            normalize_auth_file_keys()
        except Exception:
            pass
        result = start_device_authorization()
        if result.get("ok"):
            result["platform"] = sys.platform
            result["headless"] = _is_headless()
            result["auto_device_from_oauth"] = False
            result["message"] = result.get("message") or (
                "已启动设备码登录（原生 OIDC，无需本地 Grok CLI）。"
                "请用任意浏览器打开验证链接并输入设备码；完成后会自动写入账号池。"
            )
        return result
    except Exception as e:  # noqa: BLE001
        return {
            "ok": False,
            "error": (
                f"设备码登录失败: {e}。"
                "请重试，或使用「导入 Token / auth.json」。"
            ),
        }


def run_logout() -> dict[str, Any]:
    """Clear all accounts from durable store (PostgreSQL, or auth.json in file mode)."""
    ok = clear_all_accounts()
    return {
        "ok": ok,
        "message": "已清空账号池" if ok else "清空账号池失败",
    }


# ── import tokens ───────────────────────────────────────────────────────────


def _normalize_entry(
    entry: dict[str, Any], preferred_id: str | None = None
) -> tuple[str, dict[str, Any]]:
    """Normalize one account entry and return (storage_id, entry)."""
    tok = entry.get("key") or entry.get("access_token") or entry.get("token")
    if not tok or not isinstance(tok, str):
        raise ValueError("missing token")
    entry = dict(entry)
    entry["key"] = tok
    claims = decode_jwt_claims(tok)

    uid = (
        entry.get("user_id")
        or entry.get("principal_id")
        or claims.get("principal_id")
        or claims.get("sub")
    )
    if uid:
        entry["user_id"] = str(uid)
        entry.setdefault("principal_id", str(uid))
    if not entry.get("email") and claims.get("email"):
        entry["email"] = claims["email"]
    if not entry.get("team_id") and claims.get("team_id"):
        entry["team_id"] = claims["team_id"]
    if not entry.get("principal_type") and claims.get("principal_type"):
        entry["principal_type"] = claims["principal_type"]
    if not entry.get("oidc_client_id"):
        cid = claims.get("client_id") or claims.get("aud") or entry.get("oidc_client_id")
        if isinstance(cid, list):
            cid = cid[0] if cid else None
        if cid:
            entry["oidc_client_id"] = str(cid)

    exp = parse_expires_at(entry.get("expires_at"), tok)
    if exp is not None:
        entry["expires_at"] = float(exp)

    entry.setdefault("auth_mode", entry.get("auth_mode") or "imported")
    entry.setdefault(
        "create_time",
        time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )
    # Normalize SSO aliases into a single durable field on the account payload.
    sso_val = get_sso_value(entry)
    if sso_val:
        entry["sso"] = sso_val
        entry.setdefault("sso_cookie", sso_val)
    pwd_val = entry.get("password") or entry.get("register_password")
    if isinstance(pwd_val, str) and pwd_val.strip():
        entry["password"] = pwd_val.strip()

    aid = account_storage_id(
        user_id=str(uid) if uid else None,
        client_id=str(entry.get("oidc_client_id"))
        if entry.get("oidc_client_id")
        else None,
        fallback=preferred_id
        or f"https://auth.x.ai::imported-{uuid.uuid4().hex[:10]}",
    )
    return aid, entry


def export_auth_payload(
    *,
    include_secrets: bool = True,
    account_ids: list[str] | None = None,
) -> dict[str, Any]:
    """
    Export auth.json map for download/backup.
    include_secrets=True returns full tokens (needed for re-import).
    account_ids=None exports all accounts; otherwise only selected ids.
    """
    data = read_auth_map()
    wanted: set[str] | None = None
    if account_ids is not None:
        wanted = {str(x).strip() for x in account_ids if str(x).strip()}
        if not wanted:
            return {
                "ok": True,
                "auth": {},
                "count": 0,
                "auth_file": str(AUTH_FILE),
                "exported_at": time.time(),
                "selected": 0,
                "missing": [],
            }
        data = {k: v for k, v in data.items() if k in wanted}

    if not data:
        out_empty = {
            "ok": True,
            "auth": {},
            "count": 0,
            "auth_file": str(AUTH_FILE),
            "exported_at": time.time(),
        }
        if wanted is not None:
            out_empty["selected"] = len(wanted)
            out_empty["missing"] = sorted(wanted)
        return out_empty
    if include_secrets:
        out = {k: dict(v) if isinstance(v, dict) else v for k, v in data.items()}
    else:
        out = {}
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            safe = {
                kk: vv
                for kk, vv in v.items()
                if kk
                not in (
                    "key",
                    "access_token",
                    "token",
                    "refresh_token",
                    "sso",
                    "sso_cookie",
                    "sso_token",
                    "password",
                    "register_password",
                )
            }
            tok = v.get("key") or v.get("access_token") or v.get("token")
            if isinstance(tok, str):
                safe["token_hint"] = _mask_token(tok)
            safe["has_refresh_token"] = bool(v.get("refresh_token"))
            safe["has_sso"] = has_sso_value(v)
            out[k] = safe
    result = {
        "ok": True,
        "auth": out,
        "count": len(out),
        "auth_file": str(AUTH_FILE),
        "exported_at": time.time(),
    }
    if wanted is not None:
        result["selected"] = len(wanted)
        result["missing"] = sorted(wanted - set(out.keys()))
    return result



# ── CLIProxyAPI (CPA) auth files ────────────────────────────────────────────
# Official CPA per-account file (written by xai_oauth / save_cliproxyapi_auth_record):
#   {
#     "type": "xai",                 # also: grok / x-ai / x.ai
#     "auth_kind": "oauth",
#     "email": "...",
#     "sub": "...",
#     "access_token": "eyJ...",
#     "refresh_token": "...",
#     "id_token": "...",
#     "expired": "2026-06-11T06:45:02.000+08:00",   # ISO, not unix
#     "last_refresh": "...",
#     "base_url": "https://cli-chat-proxy.grok.com/v1",
#     "disabled": false,
#     "headers": { "X-XAI-Token-Auth": "xai-grok-cli", ... }
#   }
# Also accept:
#   - type=codex with access_token (same token fields; source marked)
#   - bundle { "type": "cliproxyapi-auth-bundle", "accounts": [ ... ] }
#   - bare list [ {...}, {...} ]
#   - map { "email.json": {...}, ... } when values look like CPA records

_CLIPROXY_XAI_TYPES = frozenset(
    {"xai", "grok", "x-ai", "x.ai", "x_ai", "xai-oauth", "grok-oauth"}
)
_CLIPROXY_TOKEN_TYPES = _CLIPROXY_XAI_TYPES | frozenset(
    {"codex", "openai", "chatgpt"}
)


def is_cliproxyapi_auth_record(obj: Any) -> bool:
    """True when *obj* looks like a CLIProxyAPI single-account auth JSON."""
    if not isinstance(obj, dict):
        return False
    access = (
        obj.get("access_token")
        or obj.get("accessToken")
        or obj.get("key")
        or obj.get("token")
    )
    if not isinstance(access, str) or not access.strip():
        return False
    t = str(obj.get("type") or obj.get("provider") or obj.get("platform") or "").strip().lower()
    if t in _CLIPROXY_TOKEN_TYPES:
        return True
    # Untyped but CPA-shaped (has expired ISO + refresh_token + email)
    if obj.get("refresh_token") and (
        obj.get("expired") is not None
        or obj.get("last_refresh") is not None
        or obj.get("base_url")
        or obj.get("headers")
        or obj.get("auth_kind")
    ):
        return True
    # base_url points at grok cli proxy
    base = str(obj.get("base_url") or obj.get("baseUrl") or "").lower()
    if "cli-chat-proxy.grok.com" in base or "api.x.ai" in base:
        return True
    return False


def cliproxyapi_record_to_entry(obj: dict[str, Any]) -> dict[str, Any]:
    """Convert one CLIProxyAPI auth record into our durable account entry shape."""
    access = (
        obj.get("access_token")
        or obj.get("accessToken")
        or obj.get("key")
        or obj.get("token")
        or ""
    )
    access = str(access).strip()
    if not access:
        raise ValueError("CLIProxyAPI record missing access_token")

    entry: dict[str, Any] = {
        "key": access,
        "auth_mode": "cliproxyapi_import",
        "source": "cliproxyapi",
        "create_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    refresh = obj.get("refresh_token") or obj.get("refreshToken")
    if isinstance(refresh, str) and refresh.strip():
        entry["refresh_token"] = refresh.strip()
    id_token = obj.get("id_token") or obj.get("idToken")
    if isinstance(id_token, str) and id_token.strip():
        entry["id_token"] = id_token.strip()

    email = obj.get("email") or obj.get("Email")
    if isinstance(email, str) and email.strip():
        entry["email"] = email.strip()

    # CPA uses account_id / sub as principal id
    uid = (
        obj.get("user_id")
        or obj.get("sub")
        or obj.get("account_id")
        or obj.get("principal_id")
    )
    if uid is not None and str(uid).strip():
        entry["user_id"] = str(uid).strip()
        entry["principal_id"] = str(uid).strip()

    # CPA field is ``expired`` (ISO). Also accept expires_at / expires_in.
    if obj.get("expires_at") is not None:
        entry["expires_at"] = obj.get("expires_at")
    elif obj.get("expired") is not None:
        entry["expires_at"] = obj.get("expired")
    elif obj.get("expires_in") is not None:
        try:
            entry["expires_at"] = float(time.time()) + float(obj["expires_in"])
        except (TypeError, ValueError):
            pass

    t = str(obj.get("type") or obj.get("provider") or "").strip().lower()
    if t:
        entry["cliproxyapi_type"] = t
    if obj.get("base_url"):
        entry["cliproxyapi_base_url"] = str(obj.get("base_url"))
    if isinstance(obj.get("headers"), dict):
        entry["cliproxyapi_headers"] = dict(obj["headers"])
    if obj.get("auth_kind"):
        entry["cliproxyapi_auth_kind"] = str(obj.get("auth_kind"))
    if obj.get("disabled") is True:
        entry["cliproxyapi_disabled"] = True
        entry["disabled_reason"] = "imported from CLIProxyAPI with disabled=true"

    # SSO cookies if CPA stored them under cookies / session_cookies
    for field in (
        "sso",
        "sso_cookie",
        "sso_token",
        "session_cookies",
        "cookies",
        "cookie",
    ):
        if obj.get(field) is not None and obj.get(field) != "":
            entry[field] = obj[field]
    sso_val = get_sso_value(entry)
    if sso_val:
        entry["sso"] = sso_val
        entry.setdefault("sso_cookie", sso_val)
    return entry


def coerce_cliproxyapi_payload(parsed: Any) -> list[dict[str, Any]] | None:
    """If *parsed* is CPA-shaped, return a list of raw CPA records; else None.

    Returning None means "not CPA — fall through to generic import".
    """
    if isinstance(parsed, list):
        recs = [x for x in parsed if is_cliproxyapi_auth_record(x)]
        return recs if recs else None

    if not isinstance(parsed, dict):
        return None

    # Bundle wrappers
    t = str(parsed.get("type") or "").strip().lower()
    if t in (
        "cliproxyapi-auth-bundle",
        "cliproxyapi-data",
        "cliproxyapi-auth",
        "cpa-auth-bundle",
        "cpa-data",
    ):
        accs = parsed.get("accounts") or parsed.get("auths") or parsed.get("items")
        if isinstance(accs, list):
            recs = [x for x in accs if is_cliproxyapi_auth_record(x)]
            return recs if recs else []
        if isinstance(accs, dict):
            recs = [v for v in accs.values() if is_cliproxyapi_auth_record(v)]
            return recs if recs else []

    # Single CPA record
    if is_cliproxyapi_auth_record(parsed):
        return [parsed]

    # Map of filename/id → record (CPA auth dir dump without auth.x.ai keys)
    if parsed and all(isinstance(v, dict) for v in parsed.values()):
        # Don't steal our own auth.json map (keys contain auth.x.ai::)
        keys = [str(k) for k in parsed.keys()]
        if any(("auth.x.ai" in k) or ("accounts.x.ai" in k) or ("::" in k) for k in keys):
            return None
        recs = [v for v in parsed.values() if is_cliproxyapi_auth_record(v)]
        # Only treat as CPA map when majority of values are CPA records
        if recs and len(recs) >= max(1, int(0.5 * len(parsed))):
            return recs

    return None


def collect_normalized_entries(raw: str | dict[str, Any] | list[Any]) -> dict[str, Any]:
    """Parse import payload into normalized account map without writing storage."""
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {"ok": False, "error": "empty payload"}
        if text.startswith("{") or text.startswith("["):
            try:
                parsed: Any = json.loads(text)
            except json.JSONDecodeError as e:
                return {"ok": False, "error": f"invalid JSON: {e}"}
        else:
            parsed = {"key": text}
    else:
        parsed = raw

    # CLIProxyAPI formats (single / list / bundle / auth-dir map)
    cpa_recs = coerce_cliproxyapi_payload(parsed)
    if cpa_recs is not None:
        if not cpa_recs:
            return {"ok": False, "error": "CLIProxyAPI 文件中没有可用的 access_token"}
        raw_entries: list[tuple[str | None, dict[str, Any]]] = []
        for rec in cpa_recs:
            try:
                ent = cliproxyapi_record_to_entry(rec)
            except ValueError:
                continue
            pref = (
                str(rec.get("email") or "").strip()
                or str(rec.get("account_id") or rec.get("sub") or "").strip()
                or None
            )
            raw_entries.append((pref, ent))
        if not raw_entries:
            return {"ok": False, "error": "CLIProxyAPI 记录无法解析为账号"}
        normalized: dict[str, dict[str, Any]] = {}
        for pref_id, ent in raw_entries:
            try:
                aid, nent = _normalize_entry(ent, preferred_id=pref_id)
            except ValueError:
                continue
            normalized[aid] = _merge_normalized_duplicate(normalized.get(aid), nent)
        if not normalized:
            return {"ok": False, "error": "CLIProxyAPI 记录缺少 token"}
        return {
            "ok": True,
            "normalized": normalized,
            "format": "cliproxyapi",
            "count": len(normalized),
        }

    if not isinstance(parsed, dict):
        return {"ok": False, "error": "payload must be object, list, or JWT string"}

    # Unwrap export format
    if (
        "auth" in parsed
        and isinstance(parsed.get("auth"), dict)
        and "key" not in parsed
        and "access_token" not in parsed
        and "token" not in parsed
    ):
        auth_map = parsed["auth"]
        if not auth_map:
            return {"ok": True, "normalized": {}}
        if all(isinstance(v, dict) for v in auth_map.values()):
            parsed = auth_map

    raw_entries = []
    looks_like_map = False
    if parsed and all(isinstance(v, dict) for v in parsed.values()):
        sample_vals = list(parsed.values())
        if sample_vals and any(
            "key" in v or "access_token" in v or "token" in v
            for v in sample_vals
            if isinstance(v, dict)
        ):
            if any(
                ("auth.x.ai" in str(k))
                or ("accounts.x.ai" in str(k))
                or ("::" in str(k))
                for k in parsed.keys()
            ):
                looks_like_map = True
            elif (
                "key" not in parsed
                and "access_token" not in parsed
                and "token" not in parsed
            ):
                looks_like_map = True

    if looks_like_map:
        for k, v in parsed.items():
            if isinstance(v, dict) and (
                v.get("key") or v.get("access_token") or v.get("token")
            ):
                raw_entries.append((str(k), dict(v)))
    else:
        token = (
            parsed.get("key")
            or parsed.get("token")
            or parsed.get("access_token")
            or parsed.get("accessToken")
        )
        if not token or not isinstance(token, str):
            return {
                "ok": False,
                "error": "missing token/key. Provide JWT、auth.json 或 CLIProxyAPI auth JSON。",
            }
        account_id = (
            parsed.get("account_id") or parsed.get("id") or parsed.get("auth_key")
        )
        entry: dict[str, Any] = {
            "key": token,
            "auth_mode": parsed.get("auth_mode") or "imported",
            "create_time": parsed.get("create_time")
            or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        # Accept CPA-style ``expired`` on generic single objects too
        if parsed.get("expires_at") is not None:
            entry["expires_at"] = parsed["expires_at"]
        elif parsed.get("expired") is not None:
            entry["expires_at"] = parsed["expired"]
        if parsed.get("refresh_token"):
            entry["refresh_token"] = parsed["refresh_token"]
        for field in (
            "email",
            "user_id",
            "team_id",
            "first_name",
            "last_name",
            "principal_type",
            "oidc_client_id",
            "oidc_issuer",
            # Persist registration SSO so it survives process restarts.
            "sso",
            "sso_cookie",
            "sso_token",
            "session_cookies",
            "cookies",
            "cookie",
            "set_cookie",
            "set-cookie",
            "set_cookies",
            "password",
            "register_password",
            "source",
            "registration_session_id",
            "registration_batch_id",
            "sso_backup_path",
            "id_token",
            "sub",
        ):
            if parsed.get(field) is not None and parsed.get(field) != "":
                entry[field] = parsed[field]
        if not entry.get("user_id") and entry.get("sub"):
            entry["user_id"] = str(entry.get("sub"))
        if not entry.get("sso"):
            _sso_val = get_sso_value(entry)
            if _sso_val:
                entry["sso"] = _sso_val
                entry.setdefault("sso_cookie", _sso_val)
        if not entry.get("password") and entry.get("register_password"):
            entry["password"] = entry.get("register_password")
        raw_entries.append((str(account_id) if account_id else None, entry))

    if not raw_entries:
        return {"ok": False, "error": "no valid account entries found"}

    normalized = {}
    for pref_id, ent in raw_entries:
        try:
            aid, nent = _normalize_entry(ent, preferred_id=pref_id)
        except ValueError:
            continue
        normalized[aid] = _merge_normalized_duplicate(normalized.get(aid), nent)
    if not normalized:
        return {"ok": False, "error": "entries missing token"}
    return {"ok": True, "normalized": normalized}


def build_cliproxyapi_export_record(entry: dict[str, Any], *, aid: str = "") -> dict[str, Any] | None:
    """Build one CLIProxyAPI-compatible auth JSON from a local account entry."""
    if not isinstance(entry, dict):
        return None
    access = (
        entry.get("key")
        or entry.get("access_token")
        or entry.get("token")
        or ""
    )
    if not isinstance(access, str) or not access.strip():
        return None
    access = access.strip()
    refresh = entry.get("refresh_token") or ""
    if isinstance(refresh, str):
        refresh = refresh.strip()
    else:
        refresh = ""

    email = str(entry.get("email") or "").strip()
    claims = decode_jwt_claims(access) if access else {}
    if not email and claims.get("email"):
        email = str(claims.get("email"))
    sub = (
        str(entry.get("user_id") or entry.get("principal_id") or entry.get("sub") or "")
        .strip()
        or str(claims.get("principal_id") or claims.get("sub") or "")
    )

    exp = parse_expires_at(entry.get("expires_at"), access)
    expired_iso = ""
    if exp is not None:
        try:
            from datetime import datetime, timezone

            expired_iso = datetime.fromtimestamp(float(exp), tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        except Exception:
            expired_iso = ""

    headers = entry.get("cliproxyapi_headers")
    if not isinstance(headers, dict) or not headers:
        headers = {
            "X-XAI-Token-Auth": "xai-grok-cli",
            "x-grok-client-version": "0.2.93",
            "x-grok-client-identifier": "grok-shell",
        }
    base_url = (
        str(entry.get("cliproxyapi_base_url") or "").strip()
        or "https://cli-chat-proxy.grok.com/v1"
    )
    rec: dict[str, Any] = {
        "type": str(entry.get("cliproxyapi_type") or "xai"),
        "auth_kind": str(entry.get("cliproxyapi_auth_kind") or "oauth"),
        "email": email,
        "sub": sub,
        "access_token": access,
        "refresh_token": refresh,
        "id_token": str(entry.get("id_token") or ""),
        "token_type": "Bearer",
        "expired": expired_iso,
        "last_refresh": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base_url": base_url,
        "disabled": bool(entry.get("cliproxyapi_disabled") or False),
        "headers": dict(headers),
    }
    if aid:
        rec["local_account_id"] = aid
    if entry.get("account_id"):
        rec["account_id"] = entry.get("account_id")
    elif sub:
        rec["account_id"] = sub
    return rec


def export_cliproxyapi_payload(
    *,
    account_ids: list[str] | None = None,
    push_all: bool = False,
) -> dict[str, Any]:
    """Export local accounts as CLIProxyAPI auth-bundle JSON.

    Shape (mirrors sub2api-data style so UI can download one file):

      {
        "type": "cliproxyapi-auth-bundle",
        "version": 1,
        "exported_at": "...",
        "accounts": [ { type:xai, access_token, ... }, ... ]
      }

    Each accounts[] item is a single CPA auth file body (drop into CPA auth dir
    as ``xai-<email>.json`` or import via this project's importer).
    """
    data = read_auth_map() or {}
    if push_all or account_ids is None:
        items = [(k, v) for k, v in data.items() if isinstance(v, dict)]
    else:
        wanted = {str(x).strip() for x in (account_ids or []) if str(x).strip()}
        items = []
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            if k in wanted or str(v.get("email") or "") in wanted:
                items.append((k, v))

    accounts_out: list[dict[str, Any]] = []
    skipped = 0
    for aid, entry in items:
        rec = build_cliproxyapi_export_record(entry, aid=str(aid))
        if not rec:
            skipped += 1
            continue
        accounts_out.append(rec)

    payload: dict[str, Any] = {
        "type": "cliproxyapi-auth-bundle",
        "version": 1,
        "exported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "grokcli-2api",
        "accounts": accounts_out,
    }
    if skipped:
        payload["skipped_no_token"] = skipped
    return payload


def merge_normalized_accounts(
    normalized: dict[str, dict[str, Any]], *, merge: bool = True
) -> dict[str, Any]:
    """Write normalized accounts once (single read/modify/write)."""
    if not normalized:
        return {
            "ok": False,
            "error": "no valid account entries found",
            "imported": [],
            "total_accounts": len(read_auth_map()),
        }

    existing: dict[str, Any] = {}
    if merge:
        existing = read_auth_map()
        _backup_auth_file()
        try:
            normalize_auth_file_keys()
            existing = read_auth_map()
        except Exception:
            pass
        for aid, nent in normalized.items():
            old_entry = existing.get(aid)
            old_entry = old_entry if isinstance(old_entry, dict) else None
            uid = nent.get("user_id") or nent.get("principal_id")
            if uid:
                for k in list(existing.keys()):
                    v = existing.get(k)
                    if not isinstance(v, dict):
                        continue
                    old_uid = v.get("user_id") or v.get("principal_id")
                    if str(old_uid or "") != str(uid):
                        continue
                    # Normalization normally makes this the same key. Keep this
                    # fallback for legacy keys and prefer a row that has the RT.
                    if old_entry is None or (
                        not old_entry.get("refresh_token") and v.get("refresh_token")
                    ):
                        old_entry = v
                    if k != aid:
                        existing.pop(k, None)
            merge_imported_account_fields(nent, old_entry)
        existing.update(normalized)
        write_auth_map(existing)
        total = len(existing)
    else:
        write_auth_map(normalized)
        total = len(normalized)

    try:
        from grok2api.pool.account_pool import invalidate_pool_summary_cache

        invalidate_pool_summary_cache()
    except Exception:
        pass

    imported = [
        {
            "id": aid,
            "email": nent.get("email"),
            "user_id": nent.get("user_id"),
            "expires_at": nent.get("expires_at"),
            "has_refresh_token": bool(nent.get("refresh_token")),
        }
        for aid, nent in normalized.items()
    ]
    return {
        "ok": True,
        "message": f"已导入 {len(imported)} 个账号",
        "imported": imported,
        "count": len(imported),
        "auth_file": str(AUTH_FILE),
        "total_accounts": total,
        "merged": bool(merge),
    }


def import_auth_payloads_bulk(
    payloads: list[Any], *, merge: bool = True
) -> dict[str, Any]:
    """Import many JSON payloads with one storage write."""
    if not payloads:
        return {"ok": False, "error": "empty payloads", "imported": [], "files": 0}

    normalized: dict[str, dict[str, Any]] = {}
    file_results: list[dict[str, Any]] = []
    parse_errors = 0
    for idx, raw in enumerate(payloads, 1):
        dry = collect_normalized_entries(raw)
        if not dry.get("ok"):
            parse_errors += 1
            file_results.append({
                "index": idx,
                "ok": False,
                "error": dry.get("error") or "parse failed",
                "format": dry.get("format"),
            })
            continue
        entries = dry.get("normalized") or {}
        for aid, entry in entries.items():
            if isinstance(entry, dict):
                normalized[aid] = _merge_normalized_duplicate(
                    normalized.get(aid), entry
                )
        file_results.append({
            "index": idx,
            "ok": True,
            "count": len(entries),
            "format": dry.get("format"),
        })

    if not normalized:
        return {
            "ok": False,
            "error": "no valid account entries found",
            "imported": [],
            "files": len(payloads),
            "file_results": file_results,
            "parse_errors": parse_errors,
        }

    result = merge_normalized_accounts(normalized, merge=merge)
    result["files"] = len(payloads)
    result["parse_errors"] = parse_errors
    result["file_results"] = file_results
    cpa_files = sum(1 for fr in file_results if fr.get("ok") and fr.get("format") == "cliproxyapi")
    # annotate per-file format from collect_normalized_entries
    # (re-run not needed — stash during loop below if present)
    if parse_errors:
        result["message"] = (
            f"批量导入完成：{result.get('count', 0)} 个账号，{parse_errors} 个文件失败"
        )
    else:
        result["message"] = f"批量导入完成：{result.get('count', 0)} 个账号"
    if cpa_files:
        result["cliproxyapi_files"] = cpa_files
        result["message"] = (
            f"CLIProxyAPI 导入完成：{result.get('count', 0)} 个账号"
            + (f"（{parse_errors} 个文件失败）" if parse_errors else "")
        )
    return result


def import_auth_payload(
    raw: str | dict[str, Any] | list[Any], *, merge: bool = True
) -> dict[str, Any]:
    """Import credentials (multi-account safe).

    Accepts:
      - full auth.json object { "https://auth.x.ai::uuid": { key, email, ... }, ... }
      - single entry object { key, email, ... }
      - { "token"|"key"|"access_token": "eyJ...", "email"?, "account_id"? }
      - export wrapper { "auth": { ... }, "count": N } from export_auth_payload
      - CLIProxyAPI single auth file { type:xai|codex, access_token, refresh_token, expired, ... }
      - CLIProxyAPI bundle { type:cliproxyapi-auth-bundle, accounts:[...] }
      - list of CPA / token objects
      - raw JWT string
    """
    dry = collect_normalized_entries(raw)
    if not dry.get("ok"):
        return {
            "ok": False,
            "error": dry.get("error") or "parse failed",
            "imported": [],
            "format": dry.get("format"),
        }
    normalized = dry.get("normalized") or {}
    if not normalized:
        return {
            "ok": True,
            "message": "无账号可导入",
            "imported": [],
            "count": 0,
            "auth_file": str(AUTH_FILE),
            "total_accounts": len(read_auth_map()),
            "format": dry.get("format"),
        }
    result = merge_normalized_accounts(normalized, merge=merge)
    if dry.get("format"):
        result["format"] = dry.get("format")
        if dry.get("format") == "cliproxyapi" and result.get("ok"):
            n = int(result.get("count") or len(result.get("imported") or []))
            result["message"] = f"已从 CLIProxyAPI 导入 {n} 个账号（合并={merge}）"
    return result



def do_refresh_all(
    *,
    force: bool = True,
    account_ids: list[str] | None = None,
    include_accounts: bool = True,
) -> dict[str, Any]:
    """
    Refresh accounts that have refresh_token.
    force=True: refresh all; force=False: only near-expiry.
    account_ids: optional subset to renew (single / multi-select).
    include_accounts=False keeps large-pool admin refresh responses slim.
    """
    from grok2api.config import TOKEN_REFRESH_SKEW

    result = refresh_all_accounts(
        only_near_expiry=not force,
        skew_seconds=max(300.0, float(TOKEN_REFRESH_SKEW) * 2),
        account_ids=account_ids,
    )
    now = time.time()
    for r in result.get("results") or []:
        exp = r.get("expires_at")
        if isinstance(exp, (int, float)):
            r["remaining_sec"] = max(0, int(float(exp) - now))
    if include_accounts:
        result["accounts"] = list_accounts()
    result["force"] = force
    if account_ids is not None:
        result["requested_ids"] = [str(x).strip() for x in account_ids if str(x).strip()]
    try:
        import grok2api.pool.token_maintainer as token_maintainer

        token_maintainer.request_run_soon()
    except Exception:
        pass
    return result


def do_normalize_keys() -> dict[str, Any]:
    return normalize_auth_file_keys()
