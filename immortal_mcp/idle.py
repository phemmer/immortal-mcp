"""Idle-timeout tracker for the downstream connection.

Monitors MCP message activity and fires a callback when the downstream
has been idle for longer than the configured threshold.

Ping messages (method == "ping") are excluded from activity tracking
because they are keepalives, not application-level activity.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from enum import Enum, auto


class ActivitySource(Enum):
    """Identifies which side of the proxy generated an activity event."""

    CLIENT = auto()
    """A message was received from the client (client → downstream direction)."""

    DOWNSTREAM = auto()
    """A message was received from the downstream server (downstream → client direction)."""


class IdleTracker:
    """Fires an async callback when no MCP activity has occurred for a given duration.

    Usage::

        async def on_idle():
            await downstream.disconnect()

        tracker = IdleTracker(timeout=300.0, client_only=False, on_idle=on_idle)
        await tracker.start()       # begins the background timer task
        tracker.record_activity(ActivitySource.CLIENT)   # resets the timer
        tracker.stop()              # cancels the timer task

    The tracker does nothing when timeout is 0.
    """

    def __init__(
        self,
        timeout: float,
        client_only: bool,
        on_idle: Callable[[], None],
    ) -> None:
        """
        Args:
            timeout: Seconds of inactivity before the idle callback fires. 0 disables.
            client_only: When True, only CLIENT activity resets the timer.
                DOWNSTREAM activity is ignored.
            on_idle: Synchronous callable invoked when the idle threshold is exceeded.
                Must not block; schedule coroutines via asyncio.create_task if needed.
        """
        # store timeout, client_only, on_idle on self
        # store _last_activity as current monotonic time
        # set _task to None (no background task yet)

    def record_activity(self, source: ActivitySource) -> None:
        """Record that a non-ping MCP message was observed.

        Resets the idle timer if source is CLIENT, or if client_only is False and
        source is DOWNSTREAM.  If the timer had previously fired and stopped,
        restarts it so the tracker continues guarding subsequent idle periods.

        Safe to call from any asyncio context; does not block.
        """
        # if source is DOWNSTREAM and client_only is True: return immediately
        # update _last_activity to current monotonic time
        # if _task is not None and done: create a new _task running _timer_loop()
        #   (timer fired since last activity; restart it now that activity resumed)

    async def start(self) -> None:
        """Start the background idle-timer task.

        Idempotent: calling start() when already running has no effect.
        After the timer has fired and stopped, calling start() restarts it.
        """
        # if not enabled: return
        # if _task is not None and not done: return  (already running)
        # reset _last_activity to now
        # create background asyncio task running _timer_loop()

    def stop(self) -> None:
        """Cancel the background idle-timer task.

        Idempotent: calling stop() when not running has no effect.
        """
        # if _task is not None: cancel it and set _task to None

    @property
    def enabled(self) -> bool:
        """True when timeout > 0 (i.e. idle tracking is active)."""
        # return timeout > 0

    async def _timer_loop(self) -> None:
        """Background task: poll for inactivity and fire on_idle when threshold exceeded.

        Wakes periodically (poll interval = min(timeout / 10, 0.1) seconds) and checks
        whether (now - _last_activity) >= timeout.  When the threshold is crossed,
        calls on_idle() once and exits.  record_activity() will restart this task
        if activity resumes after the timer has fired.
        """
        # compute poll_interval = min(timeout / 10, 0.1)
        # loop forever:
        #   sleep for poll_interval seconds
        #   if (monotonic_now - _last_activity) >= timeout:
        #     call on_idle()
        #     return
