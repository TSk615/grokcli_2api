"""Encrypted persistence for Grok Web and Grok Console credentials.

Browser credentials are never stored in the legacy account payload. The full
credential document is JSON-serialized, encrypted with the configured store
key, and persisted only in ``accounts.credential_enc``. Loading is deliberately
fail-closed: missing keys, plaintext rows, corrupt ciphertext, and provider
mismatches never fall back to payload fields.
"""

from __future__ import annotations

import json
from typing import Any, Mapping, TypeAlias

from grok2api.providers.console.auth import ConsoleCredential
from grok2api.providers.web.auth import WebCredential
from grok2api.store import accounts_pg, crypto


WEB_PROVIDER = "grok_web"
CONSOLE_PROVIDER = "grok_console"
SUPPORTED_PROVIDERS = frozenset((WEB_PROVIDER, CONSOLE_PROVIDER))
Credential: TypeAlias = WebCredential | ConsoleCredential


class ProviderCredentialError(RuntimeError):
    """Base error whose message is safe to expose in an operational log."""


class CredentialStorageUnavailable(ProviderCredentialError):
    """Encrypted credential storage is unavailable or not configured."""


class CredentialDecryptionError(ProviderCredentialError):
    """A stored credential could not be authenticated and decoded."""


def _require_encryption() -> None:
    if not crypto.encryption_enabled():
        raise CredentialStorageUnavailable(
            "GROK2API_SECRET_KEY is required for provider credential storage"
        )


def _encrypt_document(document: Mapping[str, Any]) -> str:
    _require_encryption()
    serialized = json.dumps(
        dict(document),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    try:
        encrypted = crypto.encrypt_secret(serialized)
    except Exception as exc:
        raise CredentialStorageUnavailable(
            "provider credential encryption is unavailable"
        ) from exc
    # ``encrypt_secret`` historically stores plaintext when no key is present.
    # Requiring a versioned envelope here prevents that compatibility behavior
    # from ever applying to Web/Console browser credentials.
    if not isinstance(encrypted, str) or not encrypted.startswith("enc:v1:"):
        raise CredentialStorageUnavailable(
            "provider credential encryption did not produce a secure envelope"
        )
    return encrypted


def _document_for(credential: Credential) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
    if isinstance(credential, WebCredential):
        source_key = credential.fingerprint
        document = {
            "version": 1,
            "provider": WEB_PROVIDER,
            "source_key": source_key,
            "sso": credential.sso,
            "sso_rw": credential.sso_rw,
            "cloudflare_cookies": dict(credential.cloudflare_cookies),
            "label": credential.label,
        }
        public_payload = {
            "provider": WEB_PROVIDER,
            "auth_type": "sso",
            "source_key": source_key,
            "label": credential.label,
        }
        return WEB_PROVIDER, source_key, document, public_payload
    if isinstance(credential, ConsoleCredential):
        document = {
            "version": 1,
            "provider": CONSOLE_PROVIDER,
            "source_key": credential.source_key,
            "name": credential.name,
            "sso_token": credential.sso_token,
            "email": credential.email,
            "user_id": credential.user_id,
            "cloudflare_cookies": credential.cloudflare_cookies,
        }
        public_payload = {
            "provider": CONSOLE_PROVIDER,
            "auth_type": "sso",
            "source_key": credential.source_key,
            "name": credential.name,
            "email": credential.email,
            "user_id": credential.user_id,
        }
        return CONSOLE_PROVIDER, credential.source_key, document, public_payload
    raise TypeError("credential must be a WebCredential or ConsoleCredential")


def store_provider_credential(
    account_id: str,
    credential: Credential,
    *,
    web_tier: str | None = None,
    egress_identity: str | None = None,
) -> None:
    """Encrypt and persist one Web/Console credential.

    Only non-secret identity metadata is written to ``accounts.payload``. The
    SSO and Cloudflare material exists at rest exclusively in
    ``accounts.credential_enc``.
    """
    aid = str(account_id or "").strip()
    if not aid:
        raise ValueError("account_id must not be empty")
    if not accounts_pg.enabled():
        raise CredentialStorageUnavailable(
            "PostgreSQL is required for provider credential storage"
        )
    provider, source_key, document, public_payload = _document_for(credential)
    encrypted = _encrypt_document(document)
    accounts_pg.upsert_provider_account(
        aid,
        public_payload,
        provider=provider,
        auth_type="sso",
        source_key=source_key,
        credential_enc=encrypted,
        web_tier=(web_tier if provider == WEB_PROVIDER else None),
        egress_identity=egress_identity,
    )


def _load_document(account_id: str, provider: str) -> tuple[dict[str, Any], str | None] | None:
    _require_encryption()
    provider_name = str(provider or "").strip().lower()
    if provider_name not in SUPPORTED_PROVIDERS:
        raise ValueError("unsupported credential provider")
    stored = accounts_pg.read_provider_credential_envelope(account_id, provider_name)
    if stored is None:
        return None
    envelope, stored_source_key = stored
    if not envelope.startswith("enc:v1:"):
        raise CredentialDecryptionError("stored provider credential is not encrypted")
    try:
        decrypted = crypto.decrypt_secret(envelope)
    except Exception as exc:
        raise CredentialDecryptionError("stored provider credential is unavailable") from exc
    if decrypted is None:
        raise CredentialDecryptionError("stored provider credential is unavailable")
    try:
        document = json.loads(decrypted)
    except (TypeError, json.JSONDecodeError):
        raise CredentialDecryptionError("stored provider credential is invalid") from None
    if not isinstance(document, dict) or document.get("provider") != provider_name:
        raise CredentialDecryptionError("stored provider credential provider mismatch")
    source_key = document.get("source_key")
    if stored_source_key and source_key != stored_source_key:
        raise CredentialDecryptionError("stored provider credential identity mismatch")
    return document, stored_source_key


def load_provider_credential(account_id: str, provider: str) -> Credential | None:
    """Load a redacted-repr credential object for exactly one provider."""
    loaded = _load_document(account_id, provider)
    if loaded is None:
        return None
    document, stored_source_key = loaded
    provider_name = str(provider or "").strip().lower()
    try:
        if provider_name == WEB_PROVIDER:
            cookies = document.get("cloudflare_cookies")
            if not isinstance(cookies, Mapping):
                raise TypeError("invalid cookie mapping")
            credential = WebCredential(
                sso=str(document["sso"]),
                sso_rw=str(document["sso_rw"]),
                cloudflare_cookies=dict(cookies),
                label=(
                    str(document["label"])
                    if document.get("label") is not None
                    else None
                ),
            )
            if stored_source_key and credential.fingerprint != stored_source_key:
                raise CredentialDecryptionError(
                    "stored provider credential identity mismatch"
                )
            return credential
        credential = ConsoleCredential(
            name=str(document["name"]),
            source_key=str(document["source_key"]),
            sso_token=str(document["sso_token"]),
            email=str(document.get("email") or ""),
            user_id=str(document.get("user_id") or ""),
            cloudflare_cookies=str(document.get("cloudflare_cookies") or ""),
        )
        return credential
    except ProviderCredentialError:
        raise
    except Exception:
        # Constructor/key failures must not include decrypted values in errors.
        raise CredentialDecryptionError("stored provider credential is invalid") from None


def load_web_credential(account_id: str) -> WebCredential | None:
    credential = load_provider_credential(account_id, WEB_PROVIDER)
    return credential if isinstance(credential, WebCredential) else None


def load_console_credential(account_id: str) -> ConsoleCredential | None:
    credential = load_provider_credential(account_id, CONSOLE_PROVIDER)
    return credential if isinstance(credential, ConsoleCredential) else None
