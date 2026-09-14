"""Local x-statsig-id generation for Grok Web media requests."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import struct
import time


_EPOCH = 1_682_924_400
_SALT = "obfiowerehiring"


def generate(path: str, method: str = "POST", now: int | None = None) -> str | None:
    """Generate a fresh header from the private seed/HEX runtime pair."""

    seed_value = os.getenv("GROK2API_WEB_STATSIG_SEED", "").strip()
    fingerprint = os.getenv("GROK2API_WEB_STATSIG_HEX", "").strip()
    if not seed_value and not fingerprint:
        from grok2api.config import DATA_DIR

        pair_path = DATA_DIR / "web-statsig.json"
        try:
            with pair_path.open("r", encoding="utf-8") as source:
                pair = json.loads(source.read(4097))
            seed_value = str(pair.get("seed", "")).strip()
            fingerprint = str(pair.get("hex", "")).strip()
        except FileNotFoundError:
            return None
        except (OSError, ValueError, AttributeError):
            raise ValueError("Invalid private Web Statsig configuration") from None
    if not seed_value or not fingerprint:
        raise ValueError("Incomplete private Web Statsig configuration")
    try:
        seed = base64.b64decode(seed_value, validate=True)
    except (ValueError, base64.binascii.Error):
        raise ValueError("Invalid Web Statsig seed") from None
    if len(seed) != 48 or len(fingerprint) > 2048 or any(c not in "0123456789abcdefABCDEF" for c in fingerprint):
        raise ValueError("Invalid Web Statsig pair")
    number = int(time.time() if now is None else now) - _EPOCH
    if number < 0 or number > 0xFFFFFFFF:
        return None
    payload = f"{method.upper()}!{path}!{number}{_SALT}{fingerprint}".encode()
    digest = hashlib.sha256(payload).digest()
    key = secrets.randbelow(256)
    output = bytearray((key,))
    output.extend(value ^ key for value in seed)
    output.extend(value ^ key for value in struct.pack("<I", number))
    output.extend(value ^ key for value in digest[:16])
    output.append(0x03 ^ key)
    return base64.b64encode(bytes(output)).decode().rstrip("=")
