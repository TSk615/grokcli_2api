"""Credential import for Grok Console SSO accounts.

Credential objects intentionally omit secret values from their repr.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any


PROVIDER = "grok_console"
MAX_IMPORT_ACCOUNTS = 10_000
MAX_SSO_TOKEN_BYTES = 16 << 10


class CredentialImportError(ValueError):
    """A safe import error which never includes credential contents."""


@dataclass(frozen=True, slots=True)
class ConsoleCredential:
    name: str
    source_key: str
    sso_token: str = field(repr=False)
    email: str = ""
    user_id: str = ""
    cloudflare_cookies: str = field(default="", repr=False)
    provider: str = PROVIDER
    auth_type: str = "sso"


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def sanitize_sso_token(value: object) -> str:
    token = str(value or "").strip()
    if token.lower().startswith("sso="):
        token = token[4:].strip()
    token = token.split(";", 1)[0]
    return token.replace("\r", "").replace("\n", "").replace("\x00", "").strip()


def _seed(entry: dict[str, Any], index: int) -> ConsoleCredential:
    token = sanitize_sso_token(
        entry.get("sso_token") or entry.get("sso") or entry.get("token")
    )
    if not token:
        raise CredentialImportError(f"account {index} is missing sso_token")
    if len(token.encode("utf-8")) > MAX_SSO_TOKEN_BYTES:
        raise CredentialImportError(f"account {index} sso_token exceeds 16 KiB")
    digest = _token_hash(token)
    name = str(entry.get("name") or "").strip() or f"Grok Console {digest[:8]}"
    return ConsoleCredential(
        name=name,
        source_key=f"console-sso:{digest}",
        sso_token=token,
        email=str(entry.get("email") or "").strip(),
        user_id=str(entry.get("user_id") or "").strip(),
        cloudflare_cookies=str(entry.get("cloudflare_cookies") or "").strip(),
    )


def _json_entries(value: object) -> list[dict[str, Any]]:
    provider = ""
    if isinstance(value, dict):
        provider = str(value.get("provider") or "").strip()
        entries: object = value.get("accounts")
        if entries is None and (
            "sso_token" in value or "sso" in value or "token" in value
        ):
            entries = [value]
    else:
        entries = value
    if provider and provider not in {PROVIDER, "console"}:
        raise CredentialImportError("credential document is not for grok_console")
    if not isinstance(entries, list):
        raise CredentialImportError("credential JSON must contain an accounts array")
    if len(entries) > MAX_IMPORT_ACCOUNTS:
        raise CredentialImportError("credential import exceeds 10000 accounts")
    if not all(isinstance(entry, dict) for entry in entries):
        raise CredentialImportError("each credential entry must be an object")
    return entries


def parse_credentials(data: bytes | str) -> list[ConsoleCredential]:
    """Parse the supported UTF-8 TXT or JSON credential formats."""

    if isinstance(data, bytes):
        try:
            text = data.removeprefix(b"\xef\xbb\xbf").decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CredentialImportError("credential file must be UTF-8") from exc
    else:
        text = data.removeprefix("\ufeff")
    text = text.strip()
    if not text:
        raise CredentialImportError("credential input is empty")

    if text.startswith(("{", "[")):
        try:
            raw_entries = _json_entries(json.loads(text))
        except json.JSONDecodeError as exc:
            raise CredentialImportError("invalid credential JSON") from exc
    else:
        raw_entries = [{"sso_token": line} for line in text.splitlines() if sanitize_sso_token(line)]
        if len(raw_entries) > MAX_IMPORT_ACCOUNTS:
            raise CredentialImportError("credential import exceeds 10000 accounts")

    if not raw_entries:
        raise CredentialImportError("no Grok Console credentials found")
    result: list[ConsoleCredential] = []
    seen: set[str] = set()
    for index, entry in enumerate(raw_entries, 1):
        credential = _seed(entry, index)
        if credential.source_key in seen:
            continue
        seen.add(credential.source_key)
        result.append(credential)
    if not result:
        raise CredentialImportError("no Grok Console credentials found")
    return result
