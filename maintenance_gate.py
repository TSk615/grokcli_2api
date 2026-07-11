"""Serialize heavy background maintenance across workers.

Token refresh and model probes both open outbound HTTP and rewrite shared
state. On a large multi-account pool they must not run at the same time or the
single Uvicorn worker + disk become unresponsive.
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Iterator

from config import MAINTENANCE_LOCK_TIMEOUT

_lock = threading.Lock()
_holder: str | None = None
_held_since = 0.0


@contextmanager
def maintenance_slot(
    owner: str,
    *,
    timeout: float | None = None,
    blocking: bool = True,
) -> Iterator[bool]:
    """
    Acquire the global maintenance slot.

    Yields True when the slot was acquired. When blocking=False and the slot is
    busy, yields False immediately so the caller can defer work.
    """
    global _holder, _held_since
    wait = MAINTENANCE_LOCK_TIMEOUT if timeout is None else max(0.0, float(timeout))
    acquired = _lock.acquire(blocking=blocking, timeout=wait if blocking else -1)
    if not acquired:
        yield False
        return
    _holder = owner
    _held_since = time.time()
    try:
        yield True
    finally:
        _holder = None
        _held_since = 0.0
        _lock.release()


def status() -> dict[str, float | str | bool | None]:
    held = _lock.locked()
    return {
        "busy": held,
        "holder": _holder if held else None,
        "held_for_sec": (time.time() - _held_since) if held and _held_since else 0.0,
        "timeout_sec": MAINTENANCE_LOCK_TIMEOUT,
    }
