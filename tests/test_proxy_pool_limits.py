from __future__ import annotations


def test_outbound_proxy_config_accepts_thousand_entry_pool() -> None:
    from grok2api.admin.admin_routes import MAX_OUTBOUND_PROXY_TEXT, RuntimeSettingsBody
    from grok2api.admin.settings_store import _normalize_outbound_proxy_config

    pool = "\n".join(f"http://198.51.100.{(i % 254) + 1}:3129" for i in range(3000))
    assert len(pool) < MAX_OUTBOUND_PROXY_TEXT
    normalized = _normalize_outbound_proxy_config(
        {"enabled": True, "proxy": pool, "proxy_strategy": "round_robin"},
        merge_env=False,
    )
    assert normalized["proxy"] == pool
    request = RuntimeSettingsBody(outbound_proxy=pool)
    assert request.outbound_proxy == pool
