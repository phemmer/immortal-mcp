"""Shared test fixtures and helpers."""

from __future__ import annotations

import anyio
import mcp.types as types
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp.shared.message import SessionMessage


def make_streams() -> tuple[
    MemoryObjectReceiveStream[SessionMessage | Exception],
    MemoryObjectSendStream[SessionMessage],
    MemoryObjectSendStream[SessionMessage | Exception],
    MemoryObjectReceiveStream[SessionMessage],
]:
    """Create a linked in-memory stream pair for testing.

    Returns (read_end, write_end, write_into_read_end, read_from_write_end).

    The proxy or downstream manager receives from read_end and sends to write_end.
    Tests inject messages via write_into_read_end and observe via read_from_write_end.
    """
    write_into_read, read_end = anyio.create_memory_object_stream(32)
    write_end, read_from_write = anyio.create_memory_object_stream(32)
    return read_end, write_end, write_into_read, read_from_write


def req(method: str, id: int = 1, params: dict | None = None) -> types.JSONRPCRequest:
    """Convenience constructor for JSONRPCRequest."""
    return types.JSONRPCRequest(id=id, method=method, params=params, jsonrpc="2.0")


def resp(id: int = 1, result: dict | None = None) -> types.JSONRPCResponse:
    """Convenience constructor for JSONRPCResponse."""
    return types.JSONRPCResponse(id=id, result=result or {}, jsonrpc="2.0")


def notif(method: str, params: dict | None = None) -> types.JSONRPCNotification:
    """Convenience constructor for JSONRPCNotification."""
    return types.JSONRPCNotification(method=method, params=params, jsonrpc="2.0")


def session(msg: types.JSONRPCMessage) -> SessionMessage:
    """Wrap a JSONRPCMessage in a SessionMessage."""
    return SessionMessage(msg)


def make_initialize_request() -> types.JSONRPCRequest:
    return req("initialize", id=1, params={
        "protocolVersion": "2024-11-05",
        "capabilities": {},
        "clientInfo": {"name": "test-client", "version": "0.0.1"},
    })


def make_initialize_response() -> types.JSONRPCResponse:
    return resp(id=1, result={
        "protocolVersion": "2024-11-05",
        "capabilities": {"tools": {}},
        "serverInfo": {"name": "test-server", "version": "0.0.1"},
    })


def make_initialized_notification() -> types.JSONRPCNotification:
    return notif("notifications/initialized")
