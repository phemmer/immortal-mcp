"""Tests for DownstreamManager (immortal_mcp.downstream)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import mcp.types as types
import pytest
from mcp.shared.message import SessionMessage

from immortal_mcp.cli import BackoffConfig, Config, IdleConfig
from immortal_mcp.downstream import INFLIGHT_DISCONNECT_ERROR_MESSAGE, NOT_CONNECTED_ERROR_MESSAGE, DownstreamManager
from immortal_mcp.idle import IdleTracker

from .conftest import (
    make_initialize_request,
    make_initialize_response,
    make_initialized_notification,
    notif,
    req,
    resp,
    session,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_config(
    reconnect_immediately: bool = False,
    backoff_max: float = 60.0,
    idle_timeout: float = 0.0,
    idle_client_only: bool = False,
    command: list[str] | None = None,
    url: str | None = None,
) -> Config:
    if command is None and url is None:
        command = ["echo", "server"]
    return Config(
        command=command,
        url=url,
        reconnect_immediately=reconnect_immediately,
        backoff=BackoffConfig(max=backoff_max),
        idle=IdleConfig(timeout=idle_timeout, client_only=idle_client_only),
    )


def _make_idle_tracker() -> IdleTracker:
    return IdleTracker(timeout=0.0, client_only=False, on_idle=lambda: None)


def _make_manager(
    config: Config | None = None,
    on_notification=None,
    idle_tracker: IdleTracker | None = None,
) -> DownstreamManager:
    if config is None:
        config = _make_config()
    if on_notification is None:
        on_notification = MagicMock()
    if idle_tracker is None:
        idle_tracker = _make_idle_tracker()
    return DownstreamManager(
        config=config,
        on_notification=on_notification,
        idle_tracker=idle_tracker,
    )


def _make_in_memory_transport():
    """
    Return (read_stream_for_manager, write_stream_for_manager,
            inject_to_manager, read_from_manager).

    The manager receives from read_stream_for_manager.
    Tests send to inject_to_manager and observe via read_from_manager.
    """
    inject, manager_reads = anyio.create_memory_object_stream(32)
    manager_writes, observe = anyio.create_memory_object_stream(32)
    return manager_reads, manager_writes, inject, observe


# ---------------------------------------------------------------------------
# Backoff delay calculation
# ---------------------------------------------------------------------------


def test_backoff_attempt_0_is_immediate():
    """First reconnect attempt has zero delay."""
    mgr = _make_manager(_make_config(backoff_max=60.0))
    assert mgr._compute_backoff_delay(0) == 0.0


def test_backoff_sequence():
    """Delays follow 3^(N-1) for N >= 1."""
    mgr = _make_manager(_make_config(backoff_max=1000.0))
    assert mgr._compute_backoff_delay(1) == 1.0   # 3^0
    assert mgr._compute_backoff_delay(2) == 3.0   # 3^1
    assert mgr._compute_backoff_delay(3) == 9.0   # 3^2
    assert mgr._compute_backoff_delay(4) == 27.0  # 3^3
    assert mgr._compute_backoff_delay(5) == 81.0  # 3^4


def test_backoff_capped_at_max():
    """Delay is capped at config.backoff.max."""
    mgr = _make_manager(_make_config(backoff_max=10.0))
    assert mgr._compute_backoff_delay(4) == 10.0  # 27 would exceed 10


# ---------------------------------------------------------------------------
# is_connected initial state
# ---------------------------------------------------------------------------


def test_initially_disconnected():
    """A new DownstreamManager starts disconnected."""
    mgr = _make_manager()
    assert mgr.is_connected is False


# ---------------------------------------------------------------------------
# set_handshake
# ---------------------------------------------------------------------------


async def test_set_handshake_stores_values():
    """set_handshake caches the three handshake messages without error."""
    mgr = _make_manager()
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )
    # No assertion needed beyond "did not raise"; connect() will verify later.


# ---------------------------------------------------------------------------
# connect / disconnect lifecycle (mocked transport)
# ---------------------------------------------------------------------------


async def test_connect_marks_connected():
    """connect() sets is_connected to True after a successful handshake."""
    mgr = _make_manager()
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )
    manager_reads, manager_writes, inject, observe = _make_in_memory_transport()

    async def fake_open_transport():
        return manager_reads, manager_writes

    with patch.object(mgr, "_open_transport", side_effect=fake_open_transport):
        # The manager will send initialize during handshake; we must reply.
        async def reply_to_handshake():
            # Read the initialize request the manager sends.
            sm: SessionMessage = await observe.receive()
            init_req: types.JSONRPCRequest = sm.message
            # Send back a fake response with the same id.
            reply = types.JSONRPCResponse(id=init_req.id, result={}, jsonrpc="2.0")
            await inject.send(SessionMessage(reply))

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tg:
                tg.start_soon(mgr.connect)
                tg.start_soon(reply_to_handshake)

    assert mgr.is_connected is True


async def test_disconnect_marks_disconnected():
    """disconnect() sets is_connected to False."""
    mgr = _make_manager()
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )
    manager_reads, manager_writes, inject, observe = _make_in_memory_transport()

    async def fake_open_transport():
        return manager_reads, manager_writes

    with patch.object(mgr, "_open_transport", side_effect=fake_open_transport):
        async def reply_to_handshake():
            sm: SessionMessage = await observe.receive()
            init_req: types.JSONRPCRequest = sm.message
            reply = types.JSONRPCResponse(id=init_req.id, result={}, jsonrpc="2.0")
            await inject.send(SessionMessage(reply))

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tg:
                tg.start_soon(mgr.connect)
                tg.start_soon(reply_to_handshake)

    await mgr.disconnect()
    assert mgr.is_connected is False


async def test_disconnect_idempotent():
    """Calling disconnect() twice does not raise."""
    mgr = _make_manager()
    await mgr.disconnect()
    await mgr.disconnect()


# ---------------------------------------------------------------------------
# send_request (on-demand mode)
# ---------------------------------------------------------------------------


async def test_send_request_when_disconnected_on_demand_connects_first():
    """In on-demand mode, send_request() connects before forwarding."""
    mgr = _make_manager(_make_config(reconnect_immediately=False))
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )
    manager_reads, manager_writes, inject, observe = _make_in_memory_transport()

    async def fake_open_transport():
        return manager_reads, manager_writes

    with patch.object(mgr, "_open_transport", side_effect=fake_open_transport):
        async def server_side():
            # 1. Reply to the handshake initialize.
            sm: SessionMessage = await observe.receive()
            init_req: types.JSONRPCRequest = sm.message
            await inject.send(SessionMessage(
                types.JSONRPCResponse(id=init_req.id, result={}, jsonrpc="2.0")
            ))
            # 2. Read the initialized notification (no reply needed).
            await observe.receive()
            # 3. Read the forwarded request and reply.
            sm2: SessionMessage = await observe.receive()
            fwd_req: types.JSONRPCRequest = sm2.message
            await inject.send(SessionMessage(
                types.JSONRPCResponse(id=fwd_req.id, result={"ok": True}, jsonrpc="2.0")
            ))

        client_req = req("tools/list", id=42)
        result: types.JSONRPCResponse | types.JSONRPCError | None = None

        async def do_send():
            nonlocal result
            result = await mgr.send_request(client_req)

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tg:
                tg.start_soon(server_side)
                tg.start_soon(do_send)

    assert isinstance(result, types.JSONRPCResponse)
    assert result.id == 42


async def test_send_request_returns_disconnect_error_when_disconnected_immediate_mode():
    """In immediate mode, send_request() fails fast when not connected."""
    mgr = _make_manager(_make_config(reconnect_immediately=True))
    # No handshake set, no connection — manager is in reconnecting state.
    result = await mgr.send_request(req("tools/list", id=5))
    assert isinstance(result, types.JSONRPCError)
    assert NOT_CONNECTED_ERROR_MESSAGE in result.error.message


async def test_ping_does_not_trigger_on_demand_connect():
    """A ping request must not cause an on-demand reconnect."""
    mgr = _make_manager(_make_config(reconnect_immediately=False))
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )
    # Manager is disconnected. A ping should get a response without connecting.
    result = await mgr.send_request(req("ping", id=99))
    # The proxy should respond to ping itself (pong), not attempt to connect.
    assert isinstance(result, types.JSONRPCResponse)
    assert mgr.is_connected is False


async def test_ping_intercepted_when_disconnected_immediate_mode():
    """A ping returns a local pong (not an error) even in immediate-reconnect mode."""
    mgr = _make_manager(_make_config(reconnect_immediately=True))
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )
    result = await mgr.send_request(req("ping", id=50))
    assert isinstance(result, types.JSONRPCResponse)
    assert mgr.is_connected is False


async def test_ping_forwarded_when_connected():
    """When connected, ping is forwarded to the downstream — not intercepted."""
    mgr = _make_manager(_make_config(reconnect_immediately=False))
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )
    manager_reads, manager_writes, inject, observe = _make_in_memory_transport()

    async def fake_open_transport():
        return manager_reads, manager_writes

    with patch.object(mgr, "_open_transport", side_effect=fake_open_transport):
        async def handshake():
            sm: SessionMessage = await observe.receive()
            await inject.send(SessionMessage(
                types.JSONRPCResponse(id=sm.message.id, result={}, jsonrpc="2.0")
            ))
            await observe.receive()  # initialized

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tg:
                tg.start_soon(mgr.connect)
                tg.start_soon(handshake)

    assert mgr.is_connected is True

    # Send a ping while connected — it should be forwarded to downstream.
    async def downstream_replies_to_ping():
        sm: SessionMessage = await observe.receive()
        assert sm.message.method == "ping"
        await inject.send(SessionMessage(
            types.JSONRPCResponse(id=sm.message.id, result={}, jsonrpc="2.0")
        ))

    with anyio.fail_after(5):
        async with anyio.create_task_group() as tg:
            tg.start_soon(downstream_replies_to_ping)

            async def do_ping():
                result = await mgr.send_request(req("ping", id=77))
                assert isinstance(result, types.JSONRPCResponse)
                assert result.id == 77

            tg.start_soon(do_ping)


# ---------------------------------------------------------------------------
# In-flight request failure on disconnect
# ---------------------------------------------------------------------------


async def test_inflight_requests_failed_on_disconnect():
    """All pending requests receive a disconnect error when the downstream drops."""
    mgr = _make_manager()
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )
    manager_reads, manager_writes, inject, observe = _make_in_memory_transport()

    async def fake_open_transport():
        return manager_reads, manager_writes

    results: list[types.JSONRPCResponse | types.JSONRPCError] = []

    with patch.object(mgr, "_open_transport", side_effect=fake_open_transport):
        # Connect and complete handshake.
        async def reply_handshake():
            sm: SessionMessage = await observe.receive()
            init_req: types.JSONRPCRequest = sm.message
            await inject.send(SessionMessage(
                types.JSONRPCResponse(id=init_req.id, result={}, jsonrpc="2.0")
            ))
            await observe.receive()  # initialized notification

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tg:
                tg.start_soon(mgr.connect)
                tg.start_soon(reply_handshake)

        # Send two requests without responding — then close the stream.
        # The brief sleep allows the request futures to be registered before
        # the stream closes; it is not waiting for an asynchronous event.
        r1_task = asyncio.create_task(mgr.send_request(req("tools/list", id=10)))
        r2_task = asyncio.create_task(mgr.send_request(req("tools/call", id=11)))
        await asyncio.sleep(0.01)
        await inject.aclose()

        with anyio.fail_after(5):
            results.append(await r1_task)
            results.append(await r2_task)

    assert len(results) == 2
    for r in results:
        assert isinstance(r, types.JSONRPCError)
        assert INFLIGHT_DISCONNECT_ERROR_MESSAGE in r.error.message


# ---------------------------------------------------------------------------
# Notification forwarding
# ---------------------------------------------------------------------------


async def test_downstream_notifications_forwarded():
    """Notifications from the downstream are passed to the on_notification callback."""
    notifications: list[types.JSONRPCNotification] = []
    notification_received = asyncio.Event()

    def on_notification(n: types.JSONRPCNotification) -> None:
        notifications.append(n)
        notification_received.set()

    mgr = _make_manager(on_notification=on_notification)
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )
    manager_reads, manager_writes, inject, observe = _make_in_memory_transport()

    async def fake_open_transport():
        return manager_reads, manager_writes

    with patch.object(mgr, "_open_transport", side_effect=fake_open_transport):
        async def server_side():
            sm: SessionMessage = await observe.receive()
            init_req: types.JSONRPCRequest = sm.message
            await inject.send(SessionMessage(
                types.JSONRPCResponse(id=init_req.id, result={}, jsonrpc="2.0")
            ))
            await observe.receive()  # initialized
            await inject.send(SessionMessage(notif("notifications/tools/list_changed")))
            # Keep stream open until the notification is observed, then close.
            await notification_received.wait()
            await inject.aclose()

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tg:
                tg.start_soon(mgr.connect)
                tg.start_soon(server_side)

    assert len(notifications) == 1
    assert notifications[0].method == "notifications/tools/list_changed"


async def test_send_notification_dropped_when_disconnected():
    """send_notification() silently succeeds when downstream is disconnected."""
    mgr = _make_manager()
    # Should not raise even though not connected.
    await mgr.send_notification(notif("notifications/cancelled", {"id": 1}))


# ---------------------------------------------------------------------------
# Status notifications (disconnect / reconnect)
# ---------------------------------------------------------------------------


async def test_idle_disconnect_sends_info_notification():
    """An explicit disconnect() (e.g. idle timeout) sends an info notification."""
    notifications: list[types.JSONRPCNotification] = []

    def on_notification(n: types.JSONRPCNotification) -> None:
        notifications.append(n)

    mgr = _make_manager(on_notification=on_notification)
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )
    manager_reads, manager_writes, inject, observe = _make_in_memory_transport()

    async def fake_open_transport():
        return manager_reads, manager_writes

    with patch.object(mgr, "_open_transport", side_effect=fake_open_transport):
        async def server_side():
            sm: SessionMessage = await observe.receive()
            await inject.send(SessionMessage(
                types.JSONRPCResponse(id=sm.message.id, result={}, jsonrpc="2.0")
            ))
            await observe.receive()  # initialized

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tg:
                tg.start_soon(mgr.connect)
                tg.start_soon(server_side)

    # Now explicitly disconnect (simulates idle timeout).
    await mgr.disconnect()

    msg_notifications = [n for n in notifications if n.method == "notifications/message"]
    assert len(msg_notifications) >= 1
    assert msg_notifications[0].params["level"] == "info"


async def test_disconnect_notification_sent_on_unexpected_disconnect():
    """When the downstream stream ends, a warning notification is sent to the client."""
    notifications: list[types.JSONRPCNotification] = []
    disconnect_received = asyncio.Event()

    def on_notification(n: types.JSONRPCNotification) -> None:
        notifications.append(n)
        if n.method == "notifications/message":
            disconnect_received.set()

    mgr = _make_manager(on_notification=on_notification)
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )
    manager_reads, manager_writes, inject, observe = _make_in_memory_transport()

    async def fake_open_transport():
        return manager_reads, manager_writes

    with patch.object(mgr, "_open_transport", side_effect=fake_open_transport):
        async def server_side():
            sm: SessionMessage = await observe.receive()
            await inject.send(SessionMessage(
                types.JSONRPCResponse(id=sm.message.id, result={}, jsonrpc="2.0")
            ))
            await observe.receive()  # initialized
            # Simulate crash by closing the stream.
            await inject.aclose()

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tg:
                tg.start_soon(mgr.connect)
                tg.start_soon(server_side)

        # Wait for the disconnect notification.
        with anyio.fail_after(5):
            await disconnect_received.wait()

    msg_notifications = [n for n in notifications if n.method == "notifications/message"]
    assert len(msg_notifications) >= 1
    assert msg_notifications[0].params["level"] == "warning"


async def test_reconnect_sends_list_changed_notifications():
    """On reconnect, notifications/tools/list_changed (and prompts, resources) are sent."""
    notifications: list[types.JSONRPCNotification] = []
    list_changed_received = asyncio.Event()

    expected_methods = {
        "notifications/tools/list_changed",
        "notifications/prompts/list_changed",
        "notifications/resources/list_changed",
    }

    def on_notification(n: types.JSONRPCNotification) -> None:
        notifications.append(n)
        received = {nn.method for nn in notifications}
        if expected_methods <= received:
            list_changed_received.set()

    mgr = _make_manager(
        config=_make_config(reconnect_immediately=True),
        on_notification=on_notification,
    )
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )

    connect_count = 0
    manager_reads = manager_writes = inject = observe = None

    def make_streams():
        nonlocal manager_reads, manager_writes, inject, observe
        manager_reads, manager_writes, inject, observe = _make_in_memory_transport()

    async def fake_open_transport():
        nonlocal connect_count
        connect_count += 1
        make_streams()
        return manager_reads, manager_writes

    with patch.object(mgr, "_open_transport", side_effect=fake_open_transport):
        # First connection.
        make_streams()

        async def first_server():
            sm: SessionMessage = await observe.receive()
            await inject.send(SessionMessage(
                types.JSONRPCResponse(id=sm.message.id, result={}, jsonrpc="2.0")
            ))
            await observe.receive()  # initialized
            # Crash — triggers reconnect.
            await inject.aclose()

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tg:
                tg.start_soon(mgr.connect)
                tg.start_soon(first_server)

        # After reconnect succeeds, wait for list_changed notifications.
        # The _reconnect_loop will call connect() which calls fake_open_transport again.
        # We need a second server to reply to the second handshake.
        async def second_server():
            sm: SessionMessage = await observe.receive()
            await inject.send(SessionMessage(
                types.JSONRPCResponse(id=sm.message.id, result={}, jsonrpc="2.0")
            ))
            await observe.receive()  # initialized

        with anyio.fail_after(10):
            # Wait for the reconnect loop to fire.
            await list_changed_received.wait()

    received_methods = {n.method for n in notifications}
    assert expected_methods <= received_methods


# ---------------------------------------------------------------------------
# Downstream capabilities caching
# ---------------------------------------------------------------------------


async def test_downstream_capabilities_cached_from_handshake():
    """downstream_capabilities is populated from the downstream's initialize response."""
    mgr = _make_manager()
    mgr.set_handshake(
        initialize_request=make_initialize_request(),
        initialize_response=make_initialize_response(),
        initialized_notification=make_initialized_notification(),
    )
    manager_reads, manager_writes, inject, observe = _make_in_memory_transport()

    async def fake_open_transport():
        return manager_reads, manager_writes

    with patch.object(mgr, "_open_transport", side_effect=fake_open_transport):
        async def server_side():
            sm: SessionMessage = await observe.receive()
            init_resp = types.JSONRPCResponse(
                id=sm.message.id,
                result={
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}, "resources": {"subscribe": True}},
                    "serverInfo": {"name": "s", "version": "0.1"},
                },
                jsonrpc="2.0",
            )
            await inject.send(SessionMessage(init_resp))
            await observe.receive()  # initialized

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tg:
                tg.start_soon(mgr.connect)
                tg.start_soon(server_side)

    assert "tools" in mgr.downstream_capabilities
    assert "resources" in mgr.downstream_capabilities
    assert "prompts" not in mgr.downstream_capabilities
