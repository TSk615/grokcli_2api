"""Push local Grok accounts into a sub2api (Wei-Shaw/sub2api) instance.

sub2api admin flow used here:

1. ``POST /api/v1/auth/login`` → JWT
2. ``GET  /api/v1/admin/groups`` → pick group_id (or create)
3. Prefer direct OAuth create with local access/refresh tokens:
   ``POST /api/v1/admin/accounts``  platform=grok type=oauth
4. Fallback when only SSO cookies are available:
   ``POST /api/v1/admin/grok/sso-to-oauth`` then create account

Config is stored under settings key ``sub2api_config``.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import accounts

_DEFAULT_TIMEOUT = 45.0
_USER_AGENT = "grokcli-2api-sub2api-push/1.0"


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------

def _default_config() -> dict[str, Any]:
    return {
        "enabled": False,
        "base_url": "",
        "email": "",
        "password": "",
        "group_id": None,
        "group_name": "",
        "auto_create_group": True,
        # After protocol registration succeeds and the account is imported
        # locally, automatically push it into sub2api (requires enabled + URL).
        "auto_push_on_register": False,
        # How many accounts to push in parallel (local → sub2api)
        "concurrency": 4,
        # Per-account capacity written into sub2api account.concurrency
        "account_concurrency": 3,
        "account_priority": 50,
        "account_rate_multiplier": 1.0,
        "notes_prefix": "grokcli-2api",
        # Cached JWT (optional; refreshed on 401)
        "token": "",
        "token_expires_at": 0,
    }


def _normalize_config(raw: Any, *, include_secrets: bool = True) -> dict[str, Any]:
    base = _default_config()
    if not isinstance(raw, dict):
        return base
    out = dict(base)
    out["enabled"] = bool(raw.get("enabled", False))
    out["base_url"] = str(raw.get("base_url") or raw.get("url") or "").strip().rstrip("/")
    out["email"] = str(raw.get("email") or raw.get("username") or "").strip()
    pw = raw.get("password")
    if include_secrets:
        out["password"] = "" if pw is None else str(pw)
    else:
        out["password"] = ""
        out["has_password"] = bool(str(pw or "").strip())
    gid = raw.get("group_id")
    if gid in (None, "", 0, "0"):
        out["group_id"] = None
    else:
        try:
            out["group_id"] = int(gid)
        except (TypeError, ValueError):
            out["group_id"] = None
    out["group_name"] = str(raw.get("group_name") or "").strip()
    out["auto_create_group"] = bool(raw.get("auto_create_group", True))
    # Accept several aliases from UI / older drafts
    auto_push = raw.get("auto_push_on_register")
    if auto_push is None:
        auto_push = raw.get("auto_import_on_register")
    if auto_push is None:
        auto_push = raw.get("auto_push_registration")
    out["auto_push_on_register"] = bool(auto_push)
    try:
        conc = int(raw.get("concurrency") or 4)
    except (TypeError, ValueError):
        conc = 4
    out["concurrency"] = max(1, min(16, conc))
    # Account capacity on sub2api side (also accept legacy aliases)
    try:
        acc_conc = int(
            raw.get("account_concurrency")
            if raw.get("account_concurrency") is not None
            else raw.get("account_capacity")
            if raw.get("account_capacity") is not None
            else 3
        )
    except (TypeError, ValueError):
        acc_conc = 3
    out["account_concurrency"] = max(1, min(100, acc_conc))
    try:
        prio = int(
            raw.get("account_priority")
            if raw.get("account_priority") is not None
            else 50
        )
    except (TypeError, ValueError):
        prio = 50
    out["account_priority"] = max(0, min(100, prio))
    try:
        rate = float(
            raw.get("account_rate_multiplier")
            if raw.get("account_rate_multiplier") is not None
            else raw.get("rate_multiplier")
            if raw.get("rate_multiplier") is not None
            else 1.0
        )
    except (TypeError, ValueError):
        rate = 1.0
    out["account_rate_multiplier"] = max(0.1, min(10.0, rate))
    out["notes_prefix"] = str(raw.get("notes_prefix") or "grokcli-2api").strip() or "grokcli-2api"
    if include_secrets:
        out["token"] = str(raw.get("token") or "").strip()
        try:
            out["token_expires_at"] = float(raw.get("token_expires_at") or 0)
        except (TypeError, ValueError):
            out["token_expires_at"] = 0.0
    else:
        out["has_token"] = bool(str(raw.get("token") or "").strip())
        out["token_expires_at"] = float(raw.get("token_expires_at") or 0) or 0
    return out


def get_sub2api_config(*, include_secrets: bool = True) -> dict[str, Any]:
    try:
        from settings_store import _get_setting_value  # type: ignore

        raw = _get_setting_value("sub2api_config", None)
    except Exception:
        raw = None
    return _normalize_config(raw, include_secrets=include_secrets)


def set_sub2api_config(patch: dict[str, Any] | None, *, replace: bool = False) -> dict[str, Any]:
    """Merge or replace sub2api_config. Empty password keeps previous."""
    if patch is None:
        patch = {}
    if not isinstance(patch, dict):
        raise ValueError("sub2api_config must be an object")
    current = get_sub2api_config(include_secrets=True)
    if replace:
        merged = _normalize_config(patch, include_secrets=True)
        # Preserve password/token when UI sends blank password intentionally keep
        if not str(merged.get("password") or "").strip() and current.get("password"):
            merged["password"] = current["password"]
        if not str(merged.get("token") or "").strip() and current.get("token"):
            merged["token"] = current.get("token") or ""
            merged["token_expires_at"] = current.get("token_expires_at") or 0
    else:
        merged = dict(current)
        for k, v in patch.items():
            if k in ("password", "token") and (v is None or str(v).strip() == ""):
                continue  # keep existing secret
            merged[k] = v
        merged = _normalize_config(merged, include_secrets=True)
    try:
        from settings_store import _set_setting_value  # type: ignore

        _set_setting_value("sub2api_config", merged)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to persist sub2api_config: {e}") from e
    return get_sub2api_config(include_secrets=True)


def public_sub2api_config() -> dict[str, Any]:
    """Admin UI payload — secrets redacted."""
    cfg = get_sub2api_config(include_secrets=True)
    return _normalize_config(cfg, include_secrets=False)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _urljoin(base: str, path: str) -> str:
    base = (base or "").rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    return base + path


def _http_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: Any = None,
    timeout: float = _DEFAULT_TIMEOUT,
) -> tuple[int, Any, str]:
    data = None
    hdrs = {
        "Accept": "application/json",
        "User-Agent": _USER_AGENT,
    }
    if headers:
        hdrs.update(headers)
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        hdrs["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            status = int(getattr(resp, "status", 200) or 200)
            try:
                parsed = json.loads(raw) if raw.strip() else None
            except json.JSONDecodeError:
                parsed = raw
            return status, parsed, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if e.fp else str(e)
        try:
            parsed = json.loads(raw) if raw.strip() else None
        except json.JSONDecodeError:
            parsed = raw
        return int(e.code), parsed, raw
    except Exception as e:  # noqa: BLE001
        return 0, None, str(e)


def _api_error_message(status: int, parsed: Any, raw: str) -> str:
    if isinstance(parsed, dict):
        # sub2api style: {code:N, message:"...", data:...} (HTTP may still be 200)
        code = parsed.get("code")
        if code not in (None, 0, "0", 200, "200"):
            msg = parsed.get("message") or parsed.get("msg") or parsed.get("error")
            if isinstance(msg, dict):
                msg = msg.get("message") or msg.get("msg") or str(msg)
            if msg:
                return f"HTTP {status}: {msg}"
            return f"HTTP {status}: code={code}"
        for k in ("message", "error", "detail", "msg"):
            v = parsed.get(k)
            if v and not isinstance(v, (dict, list)):
                return f"HTTP {status}: {v}"
        err = parsed.get("error")
        if isinstance(err, dict) and err.get("message"):
            return f"HTTP {status}: {err.get('message')}"
    text = (raw or "").strip()
    if text:
        return f"HTTP {status}: {text[:300]}"
    return f"HTTP {status}"


def _unwrap_data(parsed: Any) -> Any:
    """Unwrap sub2api `{code, data}` / `{data: ...}` envelopes."""
    if not isinstance(parsed, dict):
        return parsed
    code = parsed.get("code")
    if code not in (None, 0, "0", 200, "200"):
        return parsed
    if "data" in parsed:
        return parsed.get("data")
    return parsed


# ---------------------------------------------------------------------------
# Auth / groups
# ---------------------------------------------------------------------------

def login(cfg: dict[str, Any] | None = None, *, force: bool = False) -> dict[str, Any]:
    """Login to sub2api admin; cache JWT in settings when successful."""
    cfg = cfg or get_sub2api_config(include_secrets=True)
    base = cfg.get("base_url") or ""
    if not base:
        raise ValueError("sub2api base_url is required")
    email = cfg.get("email") or ""
    password = cfg.get("password") or ""
    if not email or not password:
        raise ValueError("sub2api email/password is required")

    token = str(cfg.get("token") or "").strip()
    exp = float(cfg.get("token_expires_at") or 0)
    if not force and token and exp > time.time() + 60:
        return {"ok": True, "token": token, "cached": True, "expires_at": exp}

    status, parsed, raw = _http_json(
        "POST",
        _urljoin(base, "/api/v1/auth/login"),
        body={"email": email, "password": password},
        timeout=30,
    )
    if status < 200 or status >= 300:
        raise RuntimeError(_api_error_message(status, parsed, raw) or "login failed")
    if isinstance(parsed, dict):
        code = parsed.get("code")
        if code not in (None, 0, "0", 200, "200"):
            raise RuntimeError(_api_error_message(status, parsed, raw) or "login failed")
    # Response shapes:
    #   {access_token, expires_in}
    #   {code:0, data:{access_token, expires_in, ...}}  (sub2api)
    #   {data:{token:...}}
    data = _unwrap_data(parsed)
    if not isinstance(data, dict):
        data = parsed if isinstance(parsed, dict) else {}
    new_token = (
        data.get("access_token")
        or data.get("token")
        or (parsed.get("access_token") if isinstance(parsed, dict) else None)
        or (parsed.get("token") if isinstance(parsed, dict) else None)
        or ""
    )
    new_token = str(new_token).strip()
    if not new_token:
        raise RuntimeError(f"login response missing token: {raw[:200]}")
    # Default 12h cache if server omits exp
    expires_in = data.get("expires_in") or parsed.get("expires_in") or 12 * 3600
    try:
        expires_in = float(expires_in)
    except (TypeError, ValueError):
        expires_in = 12 * 3600
    expires_at = time.time() + max(300.0, expires_in)
    try:
        set_sub2api_config({"token": new_token, "token_expires_at": expires_at})
    except Exception:
        pass
    return {"ok": True, "token": new_token, "cached": False, "expires_at": expires_at}


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _request_authed(
    method: str,
    path: str,
    *,
    cfg: dict[str, Any] | None = None,
    body: Any = None,
    timeout: float = _DEFAULT_TIMEOUT,
    retry_login: bool = True,
) -> tuple[int, Any, str]:
    cfg = cfg or get_sub2api_config(include_secrets=True)
    base = cfg.get("base_url") or ""
    if not base:
        raise ValueError("sub2api base_url is required")
    try:
        auth = login(cfg, force=False)
    except Exception:
        if not retry_login:
            raise
        auth = login(cfg, force=True)
    token = auth["token"]
    status, parsed, raw = _http_json(
        method,
        _urljoin(base, path),
        headers=_auth_headers(token),
        body=body,
        timeout=timeout,
    )
    if status in (401, 403) and retry_login:
        auth = login(cfg, force=True)
        status, parsed, raw = _http_json(
            method,
            _urljoin(base, path),
            headers=_auth_headers(auth["token"]),
            body=body,
            timeout=timeout,
        )
    return status, parsed, raw


def list_groups(cfg: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    status, parsed, raw = _request_authed("GET", "/api/v1/admin/groups", cfg=cfg)
    if status < 200 or status >= 300:
        raise RuntimeError(_api_error_message(status, parsed, raw))
    if isinstance(parsed, dict):
        code = parsed.get("code")
        if code not in (None, 0, "0", 200, "200"):
            raise RuntimeError(_api_error_message(status, parsed, raw))
    data = _unwrap_data(parsed)
    items: Any = data
    if isinstance(data, dict):
        items = (
            data.get("items")
            or data.get("groups")
            or data.get("list")
            or data.get("data")
            or []
        )
        if isinstance(items, dict):
            items = items.get("items") or items.get("list") or []
    elif isinstance(parsed, dict) and not isinstance(data, list):
        # fallback legacy
        items = (
            parsed.get("items")
            or parsed.get("groups")
            or parsed.get("list")
            or []
        )
    if not isinstance(items, list):
        return []
    out: list[dict[str, Any]] = []
    for g in items:
        if not isinstance(g, dict):
            continue
        out.append(
            {
                "id": g.get("id"),
                "name": g.get("name") or g.get("title") or "",
                "platform": g.get("platform") or g.get("platform_id") or "",
                "description": g.get("description") or "",
                "status": g.get("status"),
                "account_count": g.get("account_count") or g.get("accounts_count"),
            }
        )
    return out


def create_group(
    name: str,
    *,
    platform: str = "grok",
    description: str = "",
    cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    name = str(name or "").strip()
    if not name:
        raise ValueError("group name is required")
    body = {
        "name": name,
        "platform": platform or "grok",
        "description": description or "created by grokcli-2api",
        "rate_multiplier": 1.0,
        "is_exclusive": False,
    }
    status, parsed, raw = _request_authed(
        "POST", "/api/v1/admin/groups", cfg=cfg, body=body
    )
    if status < 200 or status >= 300:
        raise RuntimeError(_api_error_message(status, parsed, raw))
    if isinstance(parsed, dict):
        code = parsed.get("code")
        if code not in (None, 0, "0", 200, "200"):
            raise RuntimeError(_api_error_message(status, parsed, raw))
    data = _unwrap_data(parsed)
    if not isinstance(data, dict):
        data = parsed if isinstance(parsed, dict) else {"raw": parsed}
    return data


def resolve_group_id(cfg: dict[str, Any] | None = None) -> int:
    """Return configured group_id, matching by name, or auto-create."""
    cfg = cfg or get_sub2api_config(include_secrets=True)
    if cfg.get("group_id"):
        return int(cfg["group_id"])
    name = str(cfg.get("group_name") or "").strip() or "grokcli-2api"
    groups = list_groups(cfg)
    for g in groups:
        if str(g.get("name") or "").strip() == name:
            gid = int(g["id"])
            try:
                set_sub2api_config({"group_id": gid, "group_name": name})
            except Exception:
                pass
            return gid
        # also match platform-filtered same name ignore case
        if str(g.get("name") or "").strip().lower() == name.lower():
            gid = int(g["id"])
            try:
                set_sub2api_config({"group_id": gid, "group_name": g.get("name") or name})
            except Exception:
                pass
            return gid
    if not cfg.get("auto_create_group", True):
        raise RuntimeError(f"group not found: {name}")
    created = create_group(name, platform="grok", cfg=cfg)
    gid = created.get("id")
    if gid is None:
        # re-list
        for g in list_groups(cfg):
            if str(g.get("name") or "").strip() == name:
                gid = g.get("id")
                break
    if gid is None:
        raise RuntimeError(f"failed to create group {name}: {created}")
    gid_i = int(gid)
    try:
        set_sub2api_config({"group_id": gid_i, "group_name": name})
    except Exception:
        pass
    return gid_i


# ---------------------------------------------------------------------------
# Account push
# ---------------------------------------------------------------------------

def _local_account_entry(account_id: str) -> tuple[str, dict[str, Any]] | None:
    data = accounts.read_auth_map()
    if not data:
        return None
    aid = str(account_id or "").strip()
    if aid in data and isinstance(data[aid], dict):
        return aid, data[aid]
    # fuzzy match by email / suffix
    for k, v in data.items():
        if not isinstance(v, dict):
            continue
        if str(v.get("email") or "").strip() == aid:
            return k, v
        if k.endswith(aid) or aid.endswith(k):
            return k, v
    return None


def _entry_tokens(entry: dict[str, Any]) -> tuple[str, str]:
    access = (
        entry.get("key")
        or entry.get("access_token")
        or entry.get("token")
        or ""
    )
    refresh = entry.get("refresh_token") or ""
    return str(access).strip(), str(refresh).strip()


def _entry_sso_candidates(entry: dict[str, Any], account_id: str) -> list[str]:
    """Best-effort SSO cookie extraction (often empty after token import)."""
    out: list[str] = []
    for k in ("sso", "sso_cookie", "sso_token", "session_cookie"):
        v = entry.get(k)
        if isinstance(v, str) and v.strip():
            out.append(v.strip())
    sc = entry.get("session_cookies")
    if isinstance(sc, dict):
        for k in ("sso", "SSO", "session", "token"):
            v = sc.get(k)
            if isinstance(v, str) and v.strip():
                out.append(v.strip())
    elif isinstance(sc, list):
        for item in sc:
            if isinstance(item, str) and item.strip():
                out.append(item.strip())
            elif isinstance(item, dict):
                for k in ("sso", "value", "token"):
                    v = item.get(k)
                    if isinstance(v, str) and v.strip():
                        out.append(v.strip())
    # Dedup preserve order
    seen: set[str] = set()
    uniq: list[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _expires_at_iso(entry: dict[str, Any], access_token: str) -> str | None:
    exp = entry.get("expires_at")
    if isinstance(exp, (int, float)) and exp > 0:
        try:
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(exp)))
        except Exception:
            pass
    if isinstance(exp, str) and exp.strip():
        return exp.strip()
    # JWT payload exp
    try:
        import base64

        parts = access_token.split(".")
        if len(parts) >= 2:
            pad = "=" * (-len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
            if payload.get("exp"):
                return time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(payload["exp"]))
                )
    except Exception:
        pass
    return None


def sso_to_oauth(
    sso_tokens: list[str],
    *,
    cfg: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    tokens = [str(x).strip() for x in sso_tokens if str(x).strip()]
    if not tokens:
        return []
    status, parsed, raw = _request_authed(
        "POST",
        "/api/v1/admin/grok/sso-to-oauth",
        cfg=cfg,
        body={"sso_tokens": tokens, "proxy_id": None},
        timeout=120,
    )
    if status < 200 or status >= 300:
        raise RuntimeError(_api_error_message(status, parsed, raw))
    if isinstance(parsed, dict):
        code = parsed.get("code")
        if code not in (None, 0, "0", 200, "200"):
            raise RuntimeError(_api_error_message(status, parsed, raw))
    data = _unwrap_data(parsed)
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        return data["results"]
    if isinstance(data, list):
        return data
    if isinstance(parsed, dict) and isinstance(parsed.get("results"), list):
        return parsed["results"]
    return []


def create_grok_oauth_account(
    *,
    name: str,
    group_id: int,
    access_token: str,
    refresh_token: str = "",
    email: str = "",
    expires_at: str | None = None,
    notes: str = "",
    cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    credentials: dict[str, Any] = {
        "access_token": access_token,
        "email": email or "",
    }
    if refresh_token:
        credentials["refresh_token"] = refresh_token
    if expires_at:
        credentials["expires_at"] = expires_at
    live_cfg = cfg or get_sub2api_config(include_secrets=True)
    try:
        acc_conc = int(live_cfg.get("account_concurrency") or 3)
    except (TypeError, ValueError):
        acc_conc = 3
    acc_conc = max(1, min(100, acc_conc))
    try:
        acc_prio = int(live_cfg.get("account_priority") if live_cfg.get("account_priority") is not None else 50)
    except (TypeError, ValueError):
        acc_prio = 50
    acc_prio = max(0, min(100, acc_prio))
    try:
        acc_rate = float(live_cfg.get("account_rate_multiplier") or 1.0)
    except (TypeError, ValueError):
        acc_rate = 1.0
    acc_rate = max(0.1, min(10.0, acc_rate))
    body: dict[str, Any] = {
        "name": name[:200] if name else (email or "grok-account")[:200],
        "platform": "grok",
        "type": "oauth",
        "credentials": credentials,
        "extra": {},
        "proxy_id": None,
        "group_ids": [int(group_id)],
        "concurrency": acc_conc,
        "priority": acc_prio,
        "rate_multiplier": acc_rate,
        "notes": notes or "",
    }
    status, parsed, raw = _request_authed(
        "POST",
        "/api/v1/admin/accounts",
        cfg=cfg,
        body=body,
        timeout=60,
    )
    if status < 200 or status >= 300:
        raise RuntimeError(_api_error_message(status, parsed, raw))
    if isinstance(parsed, dict):
        code = parsed.get("code")
        if code not in (None, 0, "0", 200, "200"):
            raise RuntimeError(_api_error_message(status, parsed, raw))
    data = _unwrap_data(parsed)
    return data if isinstance(data, dict) else {"raw": parsed}


def push_account(
    account_id: str,
    *,
    group_id: int | None = None,
    cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Push one local account to sub2api. Returns result dict."""
    cfg = cfg or get_sub2api_config(include_secrets=True)
    matched = _local_account_entry(account_id)
    if not matched:
        return {"ok": False, "account_id": account_id, "error": "account not found"}
    aid, entry = matched
    email = str(entry.get("email") or "").strip()
    access, refresh = _entry_tokens(entry)
    notes_prefix = str(cfg.get("notes_prefix") or "grokcli-2api")
    notes = f"{notes_prefix}:{aid}"
    name = email or aid
    gid = int(group_id or resolve_group_id(cfg))

    # Path A: direct OAuth with local tokens
    if access:
        try:
            created = create_grok_oauth_account(
                name=name,
                group_id=gid,
                access_token=access,
                refresh_token=refresh,
                email=email,
                expires_at=_expires_at_iso(entry, access),
                notes=notes,
                cfg=cfg,
            )
            return {
                "ok": True,
                "account_id": aid,
                "email": email,
                "method": "oauth_token",
                "group_id": gid,
                "remote": {
                    "id": created.get("id"),
                    "name": created.get("name"),
                },
            }
        except Exception as e:  # noqa: BLE001
            token_err = str(e)
            # fall through to SSO if available
            sso_list = _entry_sso_candidates(entry, aid)
            if not sso_list:
                return {
                    "ok": False,
                    "account_id": aid,
                    "email": email,
                    "error": token_err,
                    "method": "oauth_token",
                }
    else:
        token_err = "missing access_token"
        sso_list = _entry_sso_candidates(entry, aid)
        if not sso_list:
            return {
                "ok": False,
                "account_id": aid,
                "email": email,
                "error": token_err,
                "method": "none",
            }

    # Path B: SSO → OAuth then create
    try:
        results = sso_to_oauth(sso_list, cfg=cfg)
    except Exception as e:  # noqa: BLE001
        return {
            "ok": False,
            "account_id": aid,
            "email": email,
            "error": f"sso-to-oauth failed: {e}",
            "method": "sso",
        }
    # Pick first success
    cred = None
    for r in results:
        if not isinstance(r, dict):
            continue
        if r.get("success") is False:
            continue
        # nested credentials or flat
        c = r.get("credentials") if isinstance(r.get("credentials"), dict) else r
        at = c.get("access_token") or c.get("AccessToken") or ""
        if at:
            cred = c
            if not email:
                email = str(c.get("email") or r.get("email") or "").strip()
            break
    if not cred:
        errs = []
        for r in results:
            if isinstance(r, dict) and (r.get("error") or r.get("message")):
                errs.append(str(r.get("error") or r.get("message")))
        return {
            "ok": False,
            "account_id": aid,
            "email": email,
            "error": "sso-to-oauth produced no credentials"
            + (f": {'; '.join(errs)}" if errs else ""),
            "method": "sso",
        }
    access2 = str(cred.get("access_token") or "").strip()
    refresh2 = str(cred.get("refresh_token") or "").strip()
    exp2 = cred.get("expires_at") or cred.get("expires_in")
    exp_iso = None
    if isinstance(exp2, str):
        exp_iso = exp2
    elif isinstance(exp2, (int, float)) and exp2 > 1_000_000_000:
        exp_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(exp2)))
    try:
        created = create_grok_oauth_account(
            name=email or name,
            group_id=gid,
            access_token=access2,
            refresh_token=refresh2,
            email=email,
            expires_at=exp_iso,
            notes=notes,
            cfg=cfg,
        )
        return {
            "ok": True,
            "account_id": aid,
            "email": email,
            "method": "sso",
            "group_id": gid,
            "remote": {
                "id": created.get("id"),
                "name": created.get("name"),
            },
        }
    except Exception as e:  # noqa: BLE001
        return {
            "ok": False,
            "account_id": aid,
            "email": email,
            "error": str(e),
            "method": "sso",
        }


def maybe_auto_push_registered_accounts(
    account_ids: list[str] | None,
    *,
    cfg: dict[str, Any] | None = None,
    source: str = "register-email",
) -> dict[str, Any]:
    """Push freshly registered local accounts into sub2api when configured.

    Safe no-op when:
    - ``auto_push_on_register`` is off
    - sub2api is not enabled / missing URL or credentials
    - no account ids were provided

    Never raises — registration success must not fail because of sub2api push.
    """
    ids = [str(x).strip() for x in (account_ids or []) if str(x).strip()]
    if not ids:
        return {"ok": True, "skipped": True, "reason": "no_accounts", "results": []}
    try:
        live = cfg or get_sub2api_config(include_secrets=True)
    except Exception as e:  # noqa: BLE001
        return {
            "ok": False,
            "skipped": True,
            "reason": f"config_error: {e}",
            "results": [],
        }
    if not bool(live.get("auto_push_on_register")):
        return {
            "ok": True,
            "skipped": True,
            "reason": "auto_push_on_register_disabled",
            "results": [],
        }
    if not bool(live.get("enabled")):
        return {
            "ok": False,
            "skipped": True,
            "reason": "sub2api_disabled",
            "results": [],
        }
    if not str(live.get("base_url") or "").strip():
        return {
            "ok": False,
            "skipped": True,
            "reason": "missing_base_url",
            "results": [],
        }
    if not str(live.get("email") or "").strip() or not (
        str(live.get("password") or "").strip() or str(live.get("token") or "").strip()
    ):
        return {
            "ok": False,
            "skipped": True,
            "reason": "missing_credentials",
            "results": [],
        }

    results: list[dict[str, Any]] = []
    ok_n = 0
    fail_n = 0
    for aid in ids:
        try:
            r = push_account(aid, cfg=live)
        except Exception as e:  # noqa: BLE001
            r = {"ok": False, "account_id": aid, "error": str(e), "method": "auto"}
        if not isinstance(r, dict):
            r = {"ok": False, "account_id": aid, "error": "invalid push result"}
        r.setdefault("source", source)
        results.append(r)
        if r.get("ok"):
            ok_n += 1
        else:
            fail_n += 1
        try:
            # Light pacing so bulk registration does not stampede sub2api.
            time.sleep(0.05)
        except Exception:
            pass
    summary = {
        "ok": fail_n == 0,
        "skipped": False,
        "source": source,
        "total": len(ids),
        "success": ok_n,
        "failed": fail_n,
        "results": results,
    }
    try:
        print(
            f"[sub2api] auto_push_on_register source={source} "
            f"total={len(ids)} ok={ok_n} fail={fail_n}"
        )
    except Exception:
        pass
    return summary


def push_accounts(
    account_ids: list[str] | None = None,
    *,
    group_id: int | None = None,
    cfg: dict[str, Any] | None = None,
    concurrency: int | None = None,
) -> dict[str, Any]:
    """Push selected or all local accounts to sub2api."""
    cfg = cfg or get_sub2api_config(include_secrets=True)
    if not cfg.get("base_url"):
        return {"ok": False, "error": "sub2api base_url not configured"}
    if not cfg.get("email") or not cfg.get("password"):
        return {"ok": False, "error": "sub2api login email/password not configured"}

    # Resolve ids
    data = accounts.read_auth_map() or {}
    if account_ids is None:
        ids = [k for k, v in data.items() if isinstance(v, dict)]
    else:
        ids = [str(x).strip() for x in account_ids if str(x).strip()]
    if not ids:
        return {"ok": True, "total": 0, "success": 0, "failed": 0, "results": []}

    try:
        gid = int(group_id) if group_id else resolve_group_id(cfg)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"resolve group failed: {e}"}

    # Ensure login works once
    try:
        login(cfg, force=False)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"sub2api login failed: {e}"}

    conc = concurrency if concurrency is not None else int(cfg.get("concurrency") or 4)
    conc = max(1, min(16, int(conc)))

    results: list[dict[str, Any]] = []
    if conc <= 1 or len(ids) == 1:
        for aid in ids:
            results.append(push_account(aid, group_id=gid, cfg=cfg))
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=conc) as pool:
            futs = {
                pool.submit(push_account, aid, group_id=gid, cfg=cfg): aid
                for aid in ids
            }
            for fut in as_completed(futs):
                try:
                    results.append(fut.result())
                except Exception as e:  # noqa: BLE001
                    results.append(
                        {
                            "ok": False,
                            "account_id": futs[fut],
                            "error": str(e),
                        }
                    )

    success = sum(1 for r in results if r.get("ok"))
    failed = len(results) - success
    return {
        "ok": failed == 0,
        "total": len(results),
        "success": success,
        "failed": failed,
        "group_id": gid,
        "results": results,
    }


def test_connection(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Login + list groups smoke test for settings UI."""
    cfg = cfg or get_sub2api_config(include_secrets=True)
    try:
        auth = login(cfg, force=True)
        groups = list_groups(cfg)
        return {
            "ok": True,
            "message": "连接成功",
            "token_cached": bool(auth.get("token")),
            "groups": groups,
            "group_count": len(groups),
        }
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)}
