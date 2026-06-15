"""immortal-mcp — a resilient MCP proxy that transparently wraps another MCP server.

The proxy can be run from the command line (``immortal-mcp …`` / ``python -m immortal_mcp``) or
embedded as a library. For embedding, build a :class:`Config` and run a :class:`ProxyServer`, or use
the :func:`serve_stdio` convenience wrapper for the common case of supervising a local stdio
downstream command.
"""

from __future__ import annotations

import asyncio

from .cli import BackoffConfig, Config, IdleConfig
from .proxy import ProxyServer

__all__ = [
    "BackoffConfig",
    "Config",
    "IdleConfig",
    "ProxyServer",
    "build_stdio_config",
    "serve_stdio",
]


def build_stdio_config(
    command: list[str],
    *,
    reconnect_immediately: bool = False,
    backoff_max: float = 60.0,
    idle_timeout: float = 0.0,
    idle_client_only: bool = False,
) -> Config:
    """Build a :class:`Config` that supervises a local stdio downstream ``command``.

    ``command`` is the downstream server's argv (e.g. ``["python", "-m", "my_server"]``). The
    keyword arguments mirror the CLI options. ``idle_timeout`` and ``reconnect_immediately`` are
    mutually exclusive, matching the CLI.
    """
    if not command:
        raise ValueError("command must be a non-empty argv list")
    if idle_timeout > 0 and reconnect_immediately:
        raise ValueError("idle_timeout and reconnect_immediately are mutually exclusive")
    return Config(
        command=list(command),
        url=None,
        reconnect_immediately=reconnect_immediately,
        backoff=BackoffConfig(max=backoff_max),
        idle=IdleConfig(timeout=idle_timeout, client_only=idle_client_only),
    )


def serve_stdio(
    command: list[str],
    *,
    reconnect_immediately: bool = False,
    backoff_max: float = 60.0,
    idle_timeout: float = 0.0,
    idle_client_only: bool = False,
) -> None:
    """Supervise a local stdio downstream ``command``, blocking until the client disconnects.

    Convenience wrapper over :func:`build_stdio_config` and :meth:`ProxyServer.run` for embedding
    the proxy in a synchronous entry point. The keyword arguments are forwarded to
    :func:`build_stdio_config`.
    """
    config = build_stdio_config(
        command,
        reconnect_immediately=reconnect_immediately,
        backoff_max=backoff_max,
        idle_timeout=idle_timeout,
        idle_client_only=idle_client_only,
    )
    asyncio.run(ProxyServer(config).run())
