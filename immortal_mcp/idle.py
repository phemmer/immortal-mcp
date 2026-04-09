"""Idle-timeout tracker for the downstream connection.

Monitors MCP message activity and fires a callback when the downstream
has been idle for longer than the configured threshold.

Ping messages (method == "ping") are excluded from activity tracking
because they are keepalives, not application-level activity.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from enum import Enum, auto


class ActivitySource(Enum):
    """Identifies which side of the proxy generated an activity event."""

    CLIENT = auto()
    """A message was received from the client (client → downstream direction)."""

    DOWNSTREAM = auto()
    """A message was received from the downstream server (downstream → client direction)."""


class IdleTracker:
    """Fires a callback when no MCP activity has occurred for a given duration.

    Usage::

        tracker = IdleTracker(timeout=300.0, client_only=False, on_idle=my_callback)
        await tracker.start()
        tracker.record_activity(ActivitySource.CLIENT)
        tracker.stop()

    The tracker does nothing when timeout is 0.
    """

    def __init__(
        self,
        timeout: float,
        client_only: bool,
        on_idle: Callable[[], None],
    ) -> None:
        self._timeout = timeout
        self._client_only = client_only
        self._on_idle = on_idle
        self._last_activity = time.monotonic()
        self._task: asyncio.Task[None] | None = None

    def record_activity(self, source: ActivitySource) -> None:
        """Record that a non-ping MCP message was observed.

        Resets the idle timer if source is CLIENT, or if client_only is False and
        source is DOWNSTREAM.  If the timer had previously fired and stopped,
        restarts it so the tracker continues guarding subsequent idle periods.
        """
        if source is ActivitySource.DOWNSTREAM and self._client_only:
            return
        self._last_activity = time.monotonic()
        if self._task is not None and self._task.done():
            self._task = asyncio.create_task(self._timer_loop())

    async def start(self) -> None:
        """Start the background idle-timer task.

        Idempotent: calling start() when already running has no effect.
        """
        if not self.enabled:
            return
        if self._task is not None and not self._task.done():
            return
        self._last_activity = time.monotonic()
        self._task = asyncio.create_task(self._timer_loop())

    def stop(self) -> None:
        """Cancel the background idle-timer task.

        Idempotent: calling stop() when not running has no effect.
        """
        if self._task is not None:
            self._task.cancel()
            self._task = None

    @property
    def enabled(self) -> bool:
        """True when timeout > 0 (i.e. idle tracking is active)."""
        return self._timeout > 0

    async def _timer_loop(self) -> None:
        """Background task: poll for inactivity and fire on_idle when exceeded."""
        poll_interval = min(self._timeout / 10, 0.1)
        while True:
            await asyncio.sleep(poll_interval)
            if time.monotonic() - self._last_activity >= self._timeout:
                self._on_idle()
                return
