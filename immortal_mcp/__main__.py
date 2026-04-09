"""Entry point for ``python -m immortal_mcp`` and the ``immortal-mcp`` CLI script."""

from __future__ import annotations

import asyncio

from .cli import parse_args
from .proxy import ProxyServer


def main() -> None:
    """Parse arguments and run the proxy server."""
    config = parse_args()
    asyncio.run(ProxyServer(config).run())


if __name__ == "__main__":
    main()
