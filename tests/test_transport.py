"""Unit tests isolating transport and connection components.

These tests identify that messages received from real MCP transports are
wrapped in a JSONRPCMessage container, but the code checks isinstance()
against the inner types (JSONRPCResponse, JSONRPCRequest, etc.) directly.
The isinstance checks never match, so the proxy never recognizes responses.

The tests progress from lowest-level to highest-level:
1. _open_transport() — streams are functional, but message type is wrapped
2. initial_connect() — hangs because the isinstance check never matches
3. _handle_initialize() — cascading failure
4. run() — cascading failure
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import asynccontextmanager

import anyio
import mcp.types as types
import pytest
from mcp.shared.message import SessionMessage

from immortal_mcp.cli import BackoffConfig, Config, IdleConfig
from immortal_mcp.downstream import DownstreamManager, unwrap_message
from immortal_mcp.idle import IdleTracker
from immortal_mcp.proxy import ProxyServer


def _make_config(command: list[str] | None = None, **kwargs) -> Config:
    defaults = dict(
        command=command,
        url=None,
        reconnect_immediately=False,
        backoff=BackoffConfig(max=60.0),
        idle=IdleConfig(timeout=0.0, client_only=False),
    )
    defaults.update(kwargs)
    return Config(**defaults)


def _make_idle_tracker() -> IdleTracker:
    return IdleTracker(timeout=0.0, client_only=False, on_idle=lambda: None)


def _make_init_request() -> types.JSONRPCRequest:
    return types.JSONRPCRequest(
        id=1,
        method="initialize",
        params={
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0.1"},
        },
        jsonrpc="2.0",
    )


# A minimal MCP server that responds to initialize and ping, then idles.
_MINIMAL_SERVER = [
    sys.executable,
    "-c",
    r"""
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    if msg.get("method") == "initialize":
        resp = {
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "test", "version": "0.1"},
            },
        }
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()
    elif msg.get("method") == "notifications/initialized":
        pass
    elif msg.get("method") == "ping":
        resp = {"jsonrpc": "2.0", "id": msg["id"], "result": {}}
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()
""",
]


# ---------------------------------------------------------------------------
# Level 1: _open_transport() returns functional streams, but the message
#          type is not what the code expects
# ---------------------------------------------------------------------------


async def test_open_transport_message_type():
    """Messages from a real transport are wrapped in JSONRPCMessage.

    unwrap_message() must correctly extract the inner type so that
    isinstance checks work.
    """
    config = _make_config(command=_MINIMAL_SERVER)
    mgr = DownstreamManager(
        config=config, on_notification=lambda n: None, idle_tracker=_make_idle_tracker()
    )

    read_stream, write_stream = await mgr._open_transport()

    try:
        init_req = _make_init_request()
        await asyncio.wait_for(write_stream.send(SessionMessage(types.JSONRPCMessage(init_req))), timeout=5)

        item = await asyncio.wait_for(read_stream.receive(), timeout=5)
        assert not isinstance(item, Exception), f"Got exception: {item}"

        msg = unwrap_message(item)
        assert isinstance(msg, (types.JSONRPCResponse, types.JSONRPCError)), (
            f"unwrap_message did not produce the right type: {type(msg).__name__}"
        )
        assert msg.id == 1
    finally:
        if mgr._transport_stack is not None:
            await mgr._transport_stack.aclose()


# ---------------------------------------------------------------------------
# Level 2: initial_connect() hangs because the isinstance check never matches
# ---------------------------------------------------------------------------


async def test_initial_connect_completes_with_real_subprocess():
    """initial_connect() must complete the MCP handshake with a real downstream.

    This currently times out because initial_connect iterates over the read
    stream looking for isinstance(msg, JSONRPCResponse), which never matches
    (msg is a JSONRPCMessage wrapper).  The loop reads the response, doesn't
    recognize it, and then blocks waiting for more messages that never come.
    """
    config = _make_config(command=_MINIMAL_SERVER)
    mgr = DownstreamManager(
        config=config, on_notification=lambda n: None, idle_tracker=_make_idle_tracker()
    )

    init_req = _make_init_request()
    response = await asyncio.wait_for(mgr.initial_connect(init_req), timeout=10)

    assert isinstance(response, types.JSONRPCResponse)
    assert response.id == 1
    assert mgr.is_connected is True

    await mgr.disconnect()


async def test_disconnect_completes_with_real_subprocess():
    """disconnect() must actually terminate the connection task and exit cleanly.

    The _reader_task runs in a different asyncio.Task than the one that entered
    the stdio_client context via AsyncExitStack.  When _reader_task's finally
    block tries to close the transport stack, anyio raises RuntimeError because
    the cancel scope was entered in another task.  This test verifies that
    disconnect() completes without hanging.
    """
    config = _make_config(command=_MINIMAL_SERVER)
    mgr = DownstreamManager(
        config=config, on_notification=lambda n: None, idle_tracker=_make_idle_tracker()
    )

    init_req = _make_init_request()
    await asyncio.wait_for(mgr.initial_connect(init_req), timeout=10)
    assert mgr.is_connected is True

    # This is the operation that hangs due to the cross-task cancel scope issue.
    await asyncio.wait_for(mgr.disconnect(), timeout=5)
    assert mgr.is_connected is False


# ---------------------------------------------------------------------------
# Level 3: _handle_initialize() cascading failure
# ---------------------------------------------------------------------------


async def test_handle_initialize_with_real_downstream():
    """_handle_initialize() must produce a response on the write stream.

    Cascading failure from the isinstance bug in initial_connect.
    """
    config = _make_config(command=_MINIMAL_SERVER)
    proxy = ProxyServer(config)

    client_write, client_observe = anyio.create_memory_object_stream[SessionMessage](32)
    idle_tracker = _make_idle_tracker()
    downstream = DownstreamManager(
        config=config, on_notification=lambda n: None, idle_tracker=idle_tracker
    )

    init_req = _make_init_request()
    try:
        await asyncio.wait_for(
            proxy._handle_initialize(init_req, client_write, downstream),
            timeout=10,
        )
    except TimeoutError:
        await downstream.disconnect()
        raise

    item = await asyncio.wait_for(client_observe.receive(), timeout=5)
    assert isinstance(unwrap_message(item), types.JSONRPCResponse)
    assert unwrap_message(item).id == 1

    await downstream.disconnect()


# ---------------------------------------------------------------------------
# Level 4: run() with real downstream, patched stdio — cascading failure
# ---------------------------------------------------------------------------


async def test_stdio_server_exits_after_stdin_eof():
    """stdio_server()'s context manager must exit cleanly after stdin reaches EOF.

    The MCP SDK's stdio_server has an internal stdout_writer task that blocks
    on write_stream.receive() forever.  After stdin hits EOF and all messages
    are drained, the write stream must be closed explicitly so the stdout_writer
    unblocks and the task group can exit.  This is the workaround applied in
    ProxyServer.run().
    """
    import io
    import os
    from io import TextIOWrapper

    from mcp.server.stdio import stdio_server as real_stdio_server

    read_fd, write_fd = os.pipe()
    stdin_reader = anyio.wrap_file(
        TextIOWrapper(os.fdopen(read_fd, "rb"), encoding="utf-8")
    )
    # stdout can be a simple StringIO — we just need something writable.
    stdout_writer = anyio.wrap_file(io.TextIOWrapper(io.BytesIO(), encoding="utf-8"))

    # Write a valid JSON-RPC message then close (EOF).
    os.write(write_fd, b'{"jsonrpc":"2.0","id":1,"method":"ping"}\n')
    os.close(write_fd)

    with anyio.fail_after(10):
        async with real_stdio_server(stdin=stdin_reader, stdout=stdout_writer) as (
            read_stream,
            write_stream,
        ):
            # Drain the one message.
            item = await asyncio.wait_for(read_stream.receive(), timeout=5)
            assert not isinstance(item, Exception)
            # Close the write stream so stdout_writer unblocks.
            await write_stream.aclose()

    # If we got here, the context manager exited without hanging.


async def test_run_with_real_downstream_patched_stdio():
    """run() must produce a response and exit cleanly when the input stream closes.

    Uses a real downstream subprocess with patched stdio (in-memory streams).
    After the initialize exchange, the input stream is closed and run() must
    return — not hang.
    """
    config = _make_config(command=_MINIMAL_SERVER)
    proxy = ProxyServer(config)

    client_inject, client_read = anyio.create_memory_object_stream[SessionMessage | Exception](32)
    client_write, client_observe = anyio.create_memory_object_stream[SessionMessage](32)

    @asynccontextmanager
    async def fake_stdio_server(stdin=None, stdout=None):
        yield client_read, client_write

    init_req = _make_init_request()
    await client_inject.send(SessionMessage(types.JSONRPCMessage(init_req)))

    async def run_proxy():
        from unittest.mock import patch
        with patch("immortal_mcp.proxy.stdio_server", fake_stdio_server):
            await proxy.run()

    async def observe_and_stop():
        item = await asyncio.wait_for(client_observe.receive(), timeout=10)
        assert isinstance(unwrap_message(item), types.JSONRPCResponse)
        assert unwrap_message(item).id == 1
        # Close the input — run() must exit cleanly.
        await client_inject.aclose()

    with anyio.fail_after(15):
        async with anyio.create_task_group() as tg:
            tg.start_soon(run_proxy)
            tg.start_soon(observe_and_stop)
