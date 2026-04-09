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
    # build the parser
    # parse argv (uses sys.argv[1:] if None)
    # the positional REMAINDER captures the downstream spec: the first arg
    #   without a leading "-" and everything after it
    # if no positional args: error ("no downstream specified")
    # auto-detect: if exactly one positional arg starting with "http:" or "https:",
    #   treat it as a URL; otherwise treat all positional args as a command
    # validate: --idle-timeout > 0 and --reconnect-immediately together is an error
    # warn to stderr if --backoff-max given without --reconnect-immediately
    # warn to stderr if --idle-client-only given without --idle-timeout > 0
    # construct and return a frozen Config from the parsed namespace


def _build_parser() -> argparse.ArgumentParser:
    """Construct the ArgumentParser without executing a parse."""
    # create ArgumentParser with prog="immortal-mcp" and appropriate description
    # add --reconnect-immediately flag (store_true)
    # add --backoff-max argument (float, default from _DEFAULTS)
    # add --idle-timeout argument (float, default from _DEFAULTS)
    # add --idle-client-only flag (store_true)
    # add positional "downstream" (nargs=REMAINDER): the first non-flag arg
    #   and everything after it becomes the downstream spec.
    #   Auto-detected as URL or command by parse_args.
    # return parser
