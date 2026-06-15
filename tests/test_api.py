"""Tests for the public library API (immortal_mcp package surface)."""

import pytest

import immortal_mcp
from immortal_mcp import BackoffConfig, Config, IdleConfig, ProxyServer, build_stdio_config


def test_public_api_is_exported():
    """The embedding entry points are importable from the package root."""
    for name in ("Config", "BackoffConfig", "IdleConfig", "ProxyServer", "build_stdio_config", "serve_stdio"):
        assert hasattr(immortal_mcp, name)


def test_build_stdio_config_captures_command_and_defaults():
    config = build_stdio_config(["python", "-m", "my_server"])
    assert isinstance(config, Config)
    assert config.command == ["python", "-m", "my_server"]
    assert config.url is None
    assert config.reconnect_immediately is False
    assert config.backoff == BackoffConfig(max=60.0)
    assert config.idle == IdleConfig(timeout=0.0, client_only=False)


def test_build_stdio_config_forwards_options():
    config = build_stdio_config(
        ["srv"], reconnect_immediately=True, backoff_max=10.0
    )
    assert config.reconnect_immediately is True
    assert config.backoff.max == 10.0


def test_build_stdio_config_rejects_empty_command():
    with pytest.raises(ValueError):
        build_stdio_config([])


def test_build_stdio_config_rejects_mutually_exclusive_options():
    with pytest.raises(ValueError):
        build_stdio_config(["srv"], reconnect_immediately=True, idle_timeout=5.0)


def test_proxy_server_accepts_built_config():
    """A ProxyServer can be constructed from a built config (no run)."""
    ProxyServer(build_stdio_config(["srv"]))
