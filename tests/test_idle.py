"""Tests for the idle-timeout tracker (immortal_mcp.idle)."""

import asyncio

import anyio
import pytest

from immortal_mcp.idle import ActivitySource, IdleTracker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tracker(
    timeout: float,
    client_only: bool = False,
) -> tuple[IdleTracker, asyncio.Event, list[int]]:
    """Return a tracker wired to an Event and a counter.

    The Event is set on the first on_idle invocation; the counter records
    total invocations so double-fire can be detected.
    """
    count: list[int] = []
    event = asyncio.Event()

    def on_idle() -> None:
        count.append(1)
        event.set()

    return IdleTracker(timeout=timeout, client_only=client_only, on_idle=on_idle), event, count


# ---------------------------------------------------------------------------
# enabled property
# ---------------------------------------------------------------------------


def test_enabled_when_timeout_positive():
    tracker, _, _ = _make_tracker(timeout=10.0)
    assert tracker.enabled is True


def test_disabled_when_timeout_zero():
    tracker, _, _ = _make_tracker(timeout=0.0)
    assert tracker.enabled is False


# ---------------------------------------------------------------------------
# Idle fires after timeout
# ---------------------------------------------------------------------------


async def test_fires_after_timeout():
    """Callback is invoked after the configured timeout with no activity."""
    tracker, event, _ = _make_tracker(timeout=0.05)
    await tracker.start()
    await asyncio.wait_for(event.wait(), timeout=2.0)
    tracker.stop()


async def test_does_not_fire_when_disabled():
    """Callback is never invoked when timeout=0."""
    tracker, event, _ = _make_tracker(timeout=0.0)
    await tracker.start()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(event.wait(), timeout=0.1)
    tracker.stop()


# ---------------------------------------------------------------------------
# Activity resets the timer
# ---------------------------------------------------------------------------


async def test_client_activity_resets_timer():
    """CLIENT activity prevents the idle callback from firing during activity, then fires after."""
    tracker, event, count = _make_tracker(timeout=0.05)
    await tracker.start()

    # Record activity every 20 ms for 5 iterations (total ~100 ms).
    # Each interval is shorter than the 50 ms timeout so the timer resets
    # before it can fire.  Sleeping here drives the test scenario; it is not
    # waiting for an asynchronous event.
    for _ in range(5):
        tracker.record_activity(ActivitySource.CLIENT)
        await asyncio.sleep(0.02)

    # The callback must not have fired while activity was ongoing.
    assert count == [], "Idle callback fired during active period"

    # After activity stops the timer fires; wait for it.
    await asyncio.wait_for(event.wait(), timeout=2.0)
    tracker.stop()


async def test_downstream_activity_resets_timer_when_not_client_only():
    """DOWNSTREAM activity resets the timer when client_only=False."""
    tracker, event, count = _make_tracker(timeout=0.05, client_only=False)
    await tracker.start()

    for _ in range(5):
        tracker.record_activity(ActivitySource.DOWNSTREAM)
        await asyncio.sleep(0.02)

    assert count == [], "Idle callback fired during active downstream period"
    await asyncio.wait_for(event.wait(), timeout=2.0)
    tracker.stop()


async def test_downstream_activity_ignored_when_client_only():
    """DOWNSTREAM activity does NOT reset the timer when client_only=True."""
    tracker, event, _ = _make_tracker(timeout=0.05, client_only=True)
    await tracker.start()

    # Downstream activity should be a no-op for the timer.
    tracker.record_activity(ActivitySource.DOWNSTREAM)

    # The timer fires despite the downstream activity.
    await asyncio.wait_for(event.wait(), timeout=2.0)
    tracker.stop()


# ---------------------------------------------------------------------------
# start / stop idempotency
# ---------------------------------------------------------------------------


async def test_start_idempotent():
    """Calling start() twice does not cause the callback to fire twice."""
    tracker, event, count = _make_tracker(timeout=0.05)
    await tracker.start()
    await tracker.start()  # second call should be a no-op

    await asyncio.wait_for(event.wait(), timeout=2.0)

    # Wait briefly to detect any spurious second firing.
    # Sleeping here checks the *absence* of a second event; no alternative exists.
    await asyncio.sleep(0.15)
    tracker.stop()

    assert len(count) == 1


async def test_stop_idempotent():
    """Calling stop() twice does not raise."""
    tracker, _, _ = _make_tracker(timeout=10.0)
    await tracker.start()
    tracker.stop()
    tracker.stop()


async def test_stop_before_start_does_not_raise():
    """Calling stop() without start() does not raise."""
    tracker, _, _ = _make_tracker(timeout=10.0)
    tracker.stop()
