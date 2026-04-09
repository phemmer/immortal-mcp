"""Tests for CLI argument parsing (immortal_mcp.cli)."""

import pytest

from immortal_mcp.cli import Config, parse_args


# ---------------------------------------------------------------------------
# Downstream specification
# ---------------------------------------------------------------------------


def test_parse_stdio_command():
    """Multiple positional args are captured as the downstream command."""
    config = parse_args(["python", "-m", "my_server"])
    assert config.command == ["python", "-m", "my_server"]
    assert config.url is None


def test_parse_http_url():
    """A single positional arg starting with http(s): is auto-detected as a URL."""
    config = parse_args(["http://localhost:8000/sse"])
    assert config.url == "http://localhost:8000/sse"
    assert config.command is None


def test_parse_https_url():
    """https URLs are auto-detected too."""
    config = parse_args(["https://example.com/mcp"])
    assert config.url == "https://example.com/mcp"
    assert config.command is None


def test_single_non_url_arg_is_command():
    """A single positional arg that isn't a URL is treated as a command."""
    config = parse_args(["my-server"])
    assert config.command == ["my-server"]
    assert config.url is None


def test_error_no_downstream(capsys):
    """Providing no positional args is an error."""
    with pytest.raises(SystemExit):
        parse_args([])


# ---------------------------------------------------------------------------
# Reconnect flags
# ---------------------------------------------------------------------------


def test_default_reconnect_is_on_demand():
    """Reconnect mode defaults to on-demand (reconnect_immediately=False)."""
    config = parse_args(["cmd"])
    assert config.reconnect_immediately is False


def test_reconnect_immediately_flag():
    """--reconnect-immediately sets reconnect_immediately=True."""
    config = parse_args(["--reconnect-immediately", "cmd"])
    assert config.reconnect_immediately is True


def test_default_backoff_max():
    """backoff.max defaults to 60.0."""
    config = parse_args(["cmd"])
    assert config.backoff.max == 60.0


def test_custom_backoff_max():
    """--backoff-max overrides the default."""
    config = parse_args(["--backoff-max", "120", "--reconnect-immediately", "cmd"])
    assert config.backoff.max == 120.0


def test_backoff_max_without_reconnect_immediately_is_warning(capsys):
    """--backoff-max without --reconnect-immediately should warn but not error."""
    config = parse_args(["--backoff-max", "30", "cmd"])
    # Should succeed (no SystemExit), and emit a warning on stderr.
    assert config.backoff.max == 30.0
    captured = capsys.readouterr()
    assert "warning" in captured.err.lower() or "warn" in captured.err.lower()


# ---------------------------------------------------------------------------
# Idle flags
# ---------------------------------------------------------------------------


def test_default_idle_disabled():
    """Idle timeout defaults to 0 (disabled)."""
    config = parse_args(["cmd"])
    assert config.idle.timeout == 0.0
    assert config.idle.client_only is False


def test_idle_timeout():
    """--idle-timeout sets the idle threshold."""
    config = parse_args(["--idle-timeout", "300", "cmd"])
    assert config.idle.timeout == 300.0


def test_idle_client_only():
    """--idle-client-only sets client_only=True."""
    config = parse_args(["--idle-timeout", "60", "--idle-client-only", "cmd"])
    assert config.idle.client_only is True


def test_idle_client_only_without_timeout_is_warning(capsys):
    """--idle-client-only without --idle-timeout should warn but not error."""
    config = parse_args(["--idle-client-only", "cmd"])
    assert config.idle.client_only is True
    captured = capsys.readouterr()
    assert "warning" in captured.err.lower() or "warn" in captured.err.lower()


# ---------------------------------------------------------------------------
# Mutual exclusion
# ---------------------------------------------------------------------------


def test_error_idle_timeout_and_reconnect_immediately():
    """--idle-timeout and --reconnect-immediately together is an error."""
    with pytest.raises(SystemExit):
        parse_args([
            "--idle-timeout", "60",
            "--reconnect-immediately",
            "cmd",
        ])


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


def test_config_is_frozen():
    """Config must be a frozen dataclass (immutable)."""
    config = parse_args(["cmd"])
    with pytest.raises((AttributeError, TypeError)):
        config.reconnect_immediately = True  # type: ignore[misc]
