"""Command-line interface for immortal-mcp.

Parses argv into a Config dataclass consumed by the proxy.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class BackoffConfig:
    """Exponential backoff parameters for immediate-reconnect mode.

    Delays follow the sequence 3^0, 3^1, 3^2, … seconds (1, 3, 9, 27, …),
    capped at max.  The first reconnect attempt is always immediate (no delay).
    """

    max: float
    """Upper bound on the delay between reconnect attempts, in seconds."""


@dataclass(frozen=True)
class IdleConfig:
    """Parameters controlling idle-based downstream disconnection."""

    timeout: float
    """Seconds of inactivity before the downstream is disconnected. 0 disables."""

    client_only: bool
    """When True, only client→downstream messages reset the idle timer."""


@dataclass(frozen=True)
class Config:
    """Full resolved configuration for a proxy run."""

    # --- Downstream identification ---

    command: list[str] | None
    """Command and arguments for a local stdio downstream server, or None if using HTTP."""

    url: str | None
    """URL of an HTTP downstream server, or None if using a local command."""

    # --- Reconnect behaviour ---

    reconnect_immediately: bool
    """Whether to reconnect to the downstream immediately on disconnect."""

    backoff: BackoffConfig
    """Backoff parameters (only meaningful when reconnect_immediately is True)."""

    # --- Idle disconnection ---

    idle: IdleConfig


_DEFAULTS = dict(
    backoff_max=60.0,
    idle_timeout=0.0,
)


def parse_args(argv: list[str] | None = None) -> Config:
    """Parse command-line arguments and return a validated Config.

    Exits with an error message on invalid input.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    downstream: list[str] = args.downstream
    # Strip a leading "--" if the user passes one out of habit.
    if downstream and downstream[0] == "--":
        downstream = downstream[1:]

    if not downstream:
        parser.error("no downstream server specified")

    # Auto-detect URL vs command.
    url: str | None = None
    command: list[str] | None = None
    if len(downstream) == 1 and (
        downstream[0].startswith("http://") or downstream[0].startswith("https://")
    ):
        url = downstream[0]
    else:
        command = downstream

    # Mutual exclusion.
    if args.idle_timeout > 0 and args.reconnect_immediately:
        parser.error("--idle-timeout and --reconnect-immediately are mutually exclusive")

    # Warnings for likely-unintentional flag combos.
    if args.backoff_max != _DEFAULTS["backoff_max"] and not args.reconnect_immediately:
        print(
            "warning: --backoff-max has no effect without --reconnect-immediately",
            file=sys.stderr,
        )
    if args.idle_client_only and args.idle_timeout <= 0:
        print(
            "warning: --idle-client-only has no effect without --idle-timeout",
            file=sys.stderr,
        )

    return Config(
        command=command,
        url=url,
        reconnect_immediately=args.reconnect_immediately,
        backoff=BackoffConfig(max=args.backoff_max),
        idle=IdleConfig(timeout=args.idle_timeout, client_only=args.idle_client_only),
    )


def _build_parser() -> argparse.ArgumentParser:
    """Construct the ArgumentParser without executing a parse."""
    parser = argparse.ArgumentParser(
        prog="immortal-mcp",
        description="Resilient MCP proxy with automatic reconnection",
    )
    parser.add_argument(
        "--reconnect-immediately",
        action="store_true",
        default=False,
        help="Reconnect to downstream immediately on disconnect (default: on-demand)",
    )
    parser.add_argument(
        "--backoff-max",
        type=float,
        default=_DEFAULTS["backoff_max"],
        metavar="SECS",
        help=f"Maximum backoff delay in seconds (default: {_DEFAULTS['backoff_max']})",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=_DEFAULTS["idle_timeout"],
        metavar="SECS",
        help="Disconnect downstream after N seconds of inactivity; 0 disables (default: 0)",
    )
    parser.add_argument(
        "--idle-client-only",
        action="store_true",
        default=False,
        help="Only client→downstream messages count as activity",
    )
    parser.add_argument(
        "downstream",
        nargs=argparse.REMAINDER,
        help="Downstream server: command [args...] or URL",
    )
    return parser
