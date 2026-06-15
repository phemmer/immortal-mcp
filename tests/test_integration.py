"""Integration tests that exercise the proxy end-to-end.

These tests reproduce the issue where sending data to the proxy on stdin
produces no output on stdout.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from unittest.mock import patch

import anyio
import mcp.types as types
import pytest
from mcp.shared.message import SessionMessage

from immortal_mcp.cli import BackoffConfig, Config, IdleConfig
from immortal_mcp.downstream import unwrap_message
from immortal_mcp.proxy import ProxyServer

from .conftest import session


def _make_config(**kwargs) -> Config:
    defaults = dict(
        command=["echo", "fake"],
        url=None,
        reconnect_immediately=False,
        backoff=BackoffConfig(max=60.0),
        idle=IdleConfig(timeout=0.0, client_only=False),
    )
    defaults.update(kwargs)
    return Config(**defaults)


_INIT_REQUEST = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2024-11-05",
        "capabilities": {},
        "clientInfo": {"name": "test-client", "version": "0.0.1"},
    },
}

_INIT_RESPONSE_RESULT = {
    "protocolVersion": "2024-11-05",
    "capabilities": {},
    "serverInfo": {"name": "test-server", "version": "0.0.1"},
}


# ---------------------------------------------------------------------------
# Test run() with in-memory streams (patched stdio_server)
# ---------------------------------------------------------------------------


async def test_run_responds_to_initialize():
    """ProxyServer.run() must produce an initialize response on the write stream.

    This patches stdio_server with in-memory streams and _open_transport with
    a fake downstream, then sends an initialize request and verifies a
    response appears on the output.
    """
    # Client-facing streams (replacing stdio).
    client_inject, client_read = anyio.create_memory_object_stream[SessionMessage | Exception](32)
    client_write, client_observe = anyio.create_memory_object_stream[SessionMessage](32)

    @asynccontextmanager
    async def fake_stdio_server(stdin=None, stdout=None):
        async with anyio.create_task_group() as tg:
            yield client_read, client_write

    # Downstream-facing streams.
    ds_inject, ds_read = anyio.create_memory_object_stream[SessionMessage | Exception](32)
    ds_write, ds_observe = anyio.create_memory_object_stream[SessionMessage](32)

    async def fake_open_transport(self_):
        return ds_read, ds_write

    async def downstream_server():
        """Minimal fake downstream: responds to initialize, then idles."""
        sm: SessionMessage = await ds_observe.receive()
        msg = unwrap_message(sm)
        assert isinstance(msg, types.JSONRPCRequest)
        assert msg.method == "initialize"
        response = types.JSONRPCResponse(
            jsonrpc="2.0", id=msg.id, result=_INIT_RESPONSE_RESULT
        )
        await ds_inject.send(SessionMessage(types.JSONRPCMessage(response)))
        # Read initialized notification.
        await ds_observe.receive()

    config = _make_config()
    proxy = ProxyServer(config)

    # Send the initialize request.
    init_req = types.JSONRPCRequest(
        id=1, method="initialize", params=_INIT_REQUEST["params"], jsonrpc="2.0"
    )
    await client_inject.send(SessionMessage(types.JSONRPCMessage(init_req)))

    async def run_proxy():
        with patch("immortal_mcp.proxy.stdio_server", fake_stdio_server):
            with patch(
                "immortal_mcp.downstream.DownstreamManager._open_transport",
                lambda self: fake_open_transport(self),
            ):
                await proxy.run()

    async def close_after_response():
        """Wait for a response to appear, then close the client stream to stop run()."""
        with anyio.fail_after(5):
            sm: SessionMessage = await client_observe.receive()
        assert isinstance(unwrap_message(sm), types.JSONRPCResponse)
        assert unwrap_message(sm).id == 1
        # Close stdin to let run() finish.
        await client_inject.aclose()

    with anyio.fail_after(10):
        async with anyio.create_task_group() as tg:
            tg.start_soon(downstream_server)
            tg.start_soon(run_proxy)
            tg.start_soon(close_after_response)


# ---------------------------------------------------------------------------
# Subprocess integration test
# ---------------------------------------------------------------------------


_MINIMAL_MCP_SERVER = r'''
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
                "capabilities": {},
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
'''


async def test_subprocess_responds_to_initialize():
    """Launch the proxy as a real subprocess, send initialize on stdin, read response from stdout."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "immortal_mcp",
        sys.executable, "-c", _MINIMAL_MCP_SERVER,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        init_msg = json.dumps(_INIT_REQUEST) + "\n"
        proc.stdin.write(init_msg.encode())
        await proc.stdin.drain()

        line = await asyncio.wait_for(proc.stdout.readline(), timeout=10.0)
        assert line, "No response received from proxy"

        response = json.loads(line)
        assert response["id"] == 1
        assert "result" in response
    finally:
        proc.kill()
        await proc.wait()


async def test_subprocess_exits_cleanly_after_stdin_closes():
    """The proxy must exit on its own when stdin is closed, not hang forever."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "immortal_mcp",
        sys.executable, "-c", _MINIMAL_MCP_SERVER,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        # Complete a normal initialization exchange.
        proc.stdin.write((json.dumps(_INIT_REQUEST) + "\n").encode())
        await proc.stdin.drain()
        line = await asyncio.wait_for(proc.stdout.readline(), timeout=10.0)
        assert line, "No initialize response"

        proc.stdin.write((json.dumps({
            "jsonrpc": "2.0", "method": "notifications/initialized",
        }) + "\n").encode())
        await proc.stdin.drain()

        # Close stdin — the proxy should exit cleanly.
        proc.stdin.close()
        await proc.stdin.wait_closed()

        returncode = await asyncio.wait_for(proc.wait(), timeout=5.0)
        assert returncode == 0, f"Proxy exited with code {returncode}"
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise AssertionError("Proxy did not exit within 5 seconds after stdin closed")


async def test_subprocess_exits_cleanly_on_invalid_input():
    """Sending invalid JSON-RPC and closing stdin must not hang the proxy."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "immortal_mcp",
        sys.executable, "-c", _MINIMAL_MCP_SERVER,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        proc.stdin.write(b"{}\n")
        await proc.stdin.drain()
        proc.stdin.close()
        await proc.stdin.wait_closed()

        returncode = await asyncio.wait_for(proc.wait(), timeout=5.0)
        assert returncode == 0, f"Proxy exited with code {returncode}"
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise AssertionError("Proxy did not exit within 5 seconds after stdin closed")


async def test_subprocess_returns_error_for_invalid_input():
    """Sending invalid JSON-RPC must produce a JSON-RPC error response, not silence."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "immortal_mcp",
        sys.executable, "-c", _MINIMAL_MCP_SERVER,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        proc.stdin.write(b"{}\n")
        await proc.stdin.drain()

        line = await asyncio.wait_for(proc.stdout.readline(), timeout=5.0)
        assert line, "No error response received for invalid input"

        response = json.loads(line)
        assert "error" in response, f"Expected a JSON-RPC error, got: {response}"
    finally:
        proc.kill()
        await proc.wait()


async def test_subprocess_responds_to_request_after_initialize():
    """After initialization, a ping request should get a response."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "immortal_mcp",
        sys.executable, "-c", _MINIMAL_MCP_SERVER,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        # Send initialize.
        init_msg = json.dumps(_INIT_REQUEST) + "\n"
        proc.stdin.write(init_msg.encode())
        await proc.stdin.drain()

        init_line = await asyncio.wait_for(proc.stdout.readline(), timeout=10.0)
        assert init_line, "No initialize response received"

        # Send initialized notification.
        initialized_msg = json.dumps({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }) + "\n"
        proc.stdin.write(initialized_msg.encode())
        await proc.stdin.drain()

        # Send a ping.
        ping_msg = json.dumps({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "ping",
        }) + "\n"
        proc.stdin.write(ping_msg.encode())
        await proc.stdin.drain()

        ping_line = await asyncio.wait_for(proc.stdout.readline(), timeout=10.0)
        assert ping_line, "No ping response received"

        ping_response = json.loads(ping_line)
        assert ping_response["id"] == 2
    finally:
        proc.kill()
        await proc.wait()


# ---------------------------------------------------------------------------
# Environment variable passthrough
# ---------------------------------------------------------------------------


_ENV_ECHO_SERVER = r'''
import sys, json, os
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
                "capabilities": {},
                "serverInfo": {
                    "name": os.environ.get("IMMORTAL_MCP_TEST_VAR", ""),
                    "version": "0.1",
                },
            },
        }
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()
    elif msg.get("method") == "notifications/initialized":
        pass
'''


_INITIALIZED_COUNTER_SERVER = r'''
import sys, json
initialized_count = 0
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
        initialized_count += 1
    elif msg.get("method") == "tools/list":
        resp = {
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {
                "tools": [{
                    "name": "initialized_count",
                    "description": str(initialized_count),
                    "inputSchema": {"type": "object"},
                }],
            },
        }
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()
'''


async def test_downstream_receives_initialized_notification_exactly_once():
    """The downstream must receive exactly one notifications/initialized.

    Currently the proxy sends it twice: once automatically in
    initial_connect() and again when forwarding the client's copy via
    _handle_initialized().  The downstream should only receive it once.
    """
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "immortal_mcp",
        sys.executable, "-c", _INITIALIZED_COUNTER_SERVER,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        # Complete initialization.
        proc.stdin.write((json.dumps(_INIT_REQUEST) + "\n").encode())
        await proc.stdin.drain()
        line = await asyncio.wait_for(proc.stdout.readline(), timeout=10.0)
        assert line, "No initialize response"

        # Send the client's notifications/initialized.
        proc.stdin.write((json.dumps({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }) + "\n").encode())
        await proc.stdin.drain()

        # Query tools/list — the backend encodes the initialized count
        # in the first tool's description field.
        proc.stdin.write((json.dumps({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
        }) + "\n").encode())
        await proc.stdin.drain()

        tools_line = await asyncio.wait_for(proc.stdout.readline(), timeout=10.0)
        assert tools_line, "No tools/list response"
        tools_response = json.loads(tools_line)
        tools = tools_response["result"]["tools"]
        initialized_count = int(tools[0]["description"])

        assert initialized_count == 1, (
            f"Downstream received {initialized_count} notifications/initialized, expected 1"
        )
    finally:
        proc.kill()
        await proc.wait()


# ---------------------------------------------------------------------------
# Channel passthrough: experimental capability + custom notification method
# ---------------------------------------------------------------------------


# A minimal "channel" server: declares an experimental capability and, once the
# handshake completes, pushes an unsolicited notification with a custom method.
# This mirrors how a Claude Code channel server (e.g. claude-channels) works, and
# guards that the proxy neither strips experimental capabilities nor drops
# notifications whose method it does not recognize.
_CHANNEL_SERVER = r'''
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
                "capabilities": {"experimental": {"claude/channel": {}}, "tools": {}},
                "serverInfo": {"name": "chan", "version": "0.1"},
            },
        }
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()
    elif msg.get("method") == "notifications/initialized":
        note = {
            "jsonrpc": "2.0",
            "method": "notifications/claude/channel",
            "params": {"content": "hello", "meta": {"id": "slack:C1:1.2"}},
        }
        sys.stdout.write(json.dumps(note) + "\n")
        sys.stdout.flush()
'''


async def test_subprocess_preserves_experimental_capability_and_forwards_custom_notification():
    """The proxy must pass an experimental capability through the initialize response unchanged and
    forward a downstream notification whose method it does not recognize."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "immortal_mcp",
        sys.executable, "-c", _CHANNEL_SERVER,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        proc.stdin.write((json.dumps(_INIT_REQUEST) + "\n").encode())
        await proc.stdin.drain()
        proc.stdin.write((json.dumps({
            "jsonrpc": "2.0", "method": "notifications/initialized",
        }) + "\n").encode())
        await proc.stdin.drain()

        # The init response and the pushed notification may arrive in either order
        # (the proxy forwards the push as the reader task observes it), so scan.
        capabilities = None
        channel_note = None
        for _ in range(6):
            line = await asyncio.wait_for(proc.stdout.readline(), timeout=10.0)
            if not line:
                break
            msg = json.loads(line)
            if msg.get("id") == 1 and "result" in msg:
                capabilities = msg["result"]["capabilities"]
            if msg.get("method") == "notifications/claude/channel":
                channel_note = msg
            if capabilities is not None and channel_note is not None:
                break

        assert capabilities is not None, "no initialize response received"
        assert capabilities.get("experimental", {}).get("claude/channel") == {}, (
            f"experimental capability not preserved: {capabilities!r}"
        )
        assert channel_note is not None, "custom channel notification was not forwarded"
        assert channel_note["params"]["meta"]["id"] == "slack:C1:1.2"
    finally:
        proc.kill()
        await proc.wait()


async def test_subprocess_inherits_environment_variables():
    """Environment variables set in the parent must be visible to the downstream server."""
    env = {**os.environ, "IMMORTAL_MCP_TEST_VAR": "passthrough-ok"}
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "immortal_mcp",
        sys.executable, "-c", _ENV_ECHO_SERVER,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )

    try:
        init_msg = json.dumps(_INIT_REQUEST) + "\n"
        proc.stdin.write(init_msg.encode())
        await proc.stdin.drain()

        line = await asyncio.wait_for(proc.stdout.readline(), timeout=10.0)
        assert line, "No response received from proxy"

        response = json.loads(line)
        server_name = response["result"]["serverInfo"]["name"]
        assert server_name == "passthrough-ok", (
            f"Downstream server did not see IMMORTAL_MCP_TEST_VAR: serverInfo.name={server_name!r}"
        )
    finally:
        proc.kill()
        await proc.wait()
