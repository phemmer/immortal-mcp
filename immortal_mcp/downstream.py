"""Downstream MCP server connection manager.

Manages the lifecycle of the connection to the wrapped downstream MCP server,
including initial connection, handshake replay on reconnect, backoff scheduling
for immediate-reconnect mode, and failure notification for in-flight requests.

The downstream manager operates at the raw JSON-RPC stream level.  It does not
interpret MCP semantics beyond the initialization handshake.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass

import anyio
import mcp.types as types
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp.client.sse import sse_client
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamablehttp_client
from mcp.shared.message import SessionMessage

from .cli import Config
from .idle import IdleTracker

logger = logging.getLogger(__name__)

# JSON-RPC error code for internal/unexpected errors.
_JSONRPC_INTERNAL_ERROR = -32603

# Error for requests that were never sent (downstream was already disconnected).
NOT_CONNECTED_ERROR_MESSAGE = (
    "The downstream MCP server is not connected. "
    "The request was not delivered."
)

# Error for in-flight requests when the connection drops after dispatch.
INFLIGHT_DISCONNECT_ERROR_MESSAGE = (
    "The connection to the downstream MCP server was lost. "
    "The request may have been received and processed, "
    "but the response was lost."
)

_METHOD_PING = "ping"

_LOGGER_NAME = "immortal-mcp"


def unwrap_message(session_msg: SessionMessage) -> types.JSONRPCRequest | types.JSONRPCResponse | types.JSONRPCNotification | types.JSONRPCError:
    """Extract the concrete JSON-RPC message from a SessionMessage.

    Real transports wrap messages in JSONRPCMessage (a pydantic RootModel).
    This function handles both wrapped and unwrapped forms.
    """
    msg = session_msg.message
    if isinstance(msg, types.JSONRPCMessage):
        return msg.root
    return msg


def wrap_message(msg: types.JSONRPCRequest | types.JSONRPCResponse | types.JSONRPCNotification | types.JSONRPCError) -> SessionMessage:
    """Wrap a concrete JSON-RPC message in SessionMessage(JSONRPCMessage(...))."""
    return SessionMessage(types.JSONRPCMessage(msg))


@dataclass
class _PendingRequest:
    """Tracks a single in-flight request awaiting a downstream response."""

    future: asyncio.Future[types.JSONRPCResponse | types.JSONRPCError]


def _make_error(
    request_id: types.RequestId, message: str
) -> types.JSONRPCError:
    return types.JSONRPCError(
        jsonrpc="2.0",
        id=request_id,
        error=types.ErrorData(code=_JSONRPC_INTERNAL_ERROR, message=message),
    )


def _make_log_notification(
    level: str, data: str
) -> types.JSONRPCNotification:
    return types.JSONRPCNotification(
        method="notifications/message",
        jsonrpc="2.0",
        params={"level": level, "logger": _LOGGER_NAME, "data": data},
    )


def _make_list_changed_notification(method: str) -> types.JSONRPCNotification:
    return types.JSONRPCNotification(method=method, jsonrpc="2.0")


class DownstreamManager:
    """Manages the downstream MCP server connection and reconnection logic.

    Thread-safety: all methods must be called from the same asyncio event loop.
    """

    def __init__(
        self,
        config: Config,
        on_notification: Callable[[types.JSONRPCNotification], None],
        idle_tracker: IdleTracker,
    ) -> None:
        self._config = config
        self._on_notification = on_notification
        self._idle_tracker = idle_tracker

        self._write_stream: MemoryObjectSendStream[SessionMessage] | None = None
        self._read_stream: MemoryObjectReceiveStream[SessionMessage | Exception] | None = None
        self._reader_task_handle: asyncio.Task[None] | None = None
        self._pending: dict[types.RequestId, _PendingRequest] = {}
        self._handshake: tuple[
            types.JSONRPCRequest,
            types.JSONRPCResponse,
            types.JSONRPCNotification,
        ] | None = None
        self._transport_stack: AsyncExitStack | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._was_connected = False
        self._explicit_disconnect = False
        self.downstream_capabilities: dict = {}

    # ------------------------------------------------------------------
    # Handshake cache
    # ------------------------------------------------------------------

    def set_handshake(
        self,
        initialize_request: types.JSONRPCRequest,
        initialize_response: types.JSONRPCResponse,
        initialized_notification: types.JSONRPCNotification,
    ) -> None:
        self._handshake = (initialize_request, initialize_response, initialized_notification)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def initial_connect(
        self, request: types.JSONRPCRequest
    ) -> types.JSONRPCResponse:
        """Connect to the downstream for the first time, retrying with backoff.

        Sends the client's `initialize` request, reads the response, sends
        `initialized`, starts the reader task, and returns the raw response.
        Retries indefinitely until the downstream responds.

        After this call, set_handshake() has been called internally and the
        manager is connected with the reader task running.
        """
        attempt = 0
        while True:
            delay = self._compute_backoff_delay(attempt)
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                read_stream, write_stream = await self._open_transport()
                await write_stream.send(SessionMessage(types.JSONRPCMessage(request)))
                response: types.JSONRPCResponse | None = None
                async for item in read_stream:
                    if isinstance(item, Exception):
                        raise item
                    msg = unwrap_message(item)
                    if isinstance(msg, (types.JSONRPCResponse, types.JSONRPCError)):
                        if msg.id == request.id:
                            if isinstance(msg, types.JSONRPCError):
                                raise RuntimeError(
                                    f"Downstream initialize failed: {msg.error.message}"
                                )
                            response = msg
                            break
                if response is None:
                    raise RuntimeError("Downstream closed before initialize response")

                result = response.result if isinstance(response.result, dict) else {}
                self.downstream_capabilities = result.get("capabilities", {})

                initialized_notif = types.JSONRPCNotification(
                    method="notifications/initialized", jsonrpc="2.0"
                )
                await write_stream.send(SessionMessage(types.JSONRPCMessage(initialized_notif)))

                self.set_handshake(request, response, initialized_notif)
                self._write_stream = write_stream
                self._read_stream = read_stream
                self._explicit_disconnect = False
                self._reader_task_handle = asyncio.create_task(
                    self._reader_task(read_stream)
                )
                self._was_connected = True
                return response

            except asyncio.CancelledError:
                raise
            except Exception:
                if self._transport_stack is not None:
                    try:
                        await self._transport_stack.aclose()
                    except Exception:
                        pass
                    self._transport_stack = None
                attempt += 1

    async def connect(self) -> None:
        """Open the downstream transport and replay the cached handshake."""
        if self._handshake is None:
            raise RuntimeError("handshake not set")

        read_stream, write_stream = await self._open_transport()
        self._write_stream = write_stream
        self._read_stream = read_stream

        await self._replay_handshake(write_stream, read_stream)

        self._explicit_disconnect = False
        self._reader_task_handle = asyncio.create_task(self._reader_task(read_stream))

        if self._was_connected:
            self._on_notification(
                _make_log_notification("info", "Downstream server connection restored")
            )
            for method in (
                "notifications/tools/list_changed",
                "notifications/prompts/list_changed",
                "notifications/resources/list_changed",
            ):
                self._on_notification(_make_list_changed_notification(method))

        self._was_connected = True

    async def disconnect(self) -> None:
        """Close the downstream transport and cancel the reader task."""
        if self._write_stream is None and self._reader_task_handle is None:
            return

        self._explicit_disconnect = True

        if self._reconnect_task is not None and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None

        # Grab and clear the transport stack before cancelling the reader task.
        # The reader task's finally block also tries to close _transport_stack,
        # but it runs in a different asyncio.Task than the one that entered the
        # transport's cancel scopes, so anyio rejects the close.  By clearing
        # the reference first, the reader task skips its close attempt, and we
        # close the stack here in the correct task.
        transport_stack = self._transport_stack
        self._transport_stack = None

        if self._reader_task_handle is not None and not self._reader_task_handle.done():
            self._reader_task_handle.cancel()
            try:
                await self._reader_task_handle
            except asyncio.CancelledError:
                pass
        self._reader_task_handle = None

        self._write_stream = None
        self._read_stream = None

        if transport_stack is not None:
            await transport_stack.aclose()

        self._fail_pending_requests()

    @property
    def is_connected(self) -> bool:
        return (
            self._write_stream is not None
            and self._reader_task_handle is not None
            and not self._reader_task_handle.done()
        )

    # ------------------------------------------------------------------
    # Message sending
    # ------------------------------------------------------------------

    async def send_request(
        self, request: types.JSONRPCRequest
    ) -> types.JSONRPCResponse | types.JSONRPCError:
        if not self.is_connected:
            if request.method == _METHOD_PING:
                return types.JSONRPCResponse(
                    jsonrpc="2.0", id=request.id, result={}
                )
            if self._config.reconnect_immediately:
                return _make_error(request.id, NOT_CONNECTED_ERROR_MESSAGE)
            # On-demand: attempt to connect first.
            try:
                await self.connect()
            except Exception:
                return _make_error(request.id, NOT_CONNECTED_ERROR_MESSAGE)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[types.JSONRPCResponse | types.JSONRPCError] = loop.create_future()
        self._pending[request.id] = _PendingRequest(future=future)
        try:
            await self._write_stream.send(SessionMessage(types.JSONRPCMessage(request)))
        except (anyio.ClosedResourceError, anyio.BrokenResourceError):
            self._pending.pop(request.id, None)
            if not future.done():
                future.set_result(_make_error(request.id, INFLIGHT_DISCONNECT_ERROR_MESSAGE))
        return await future

    async def send_notification(self, notification: types.JSONRPCNotification) -> None:
        if not self.is_connected:
            return
        try:
            await self._write_stream.send(SessionMessage(types.JSONRPCMessage(notification)))
        except (anyio.ClosedResourceError, anyio.BrokenResourceError):
            pass

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _open_transport(
        self,
    ) -> tuple[
        MemoryObjectReceiveStream[SessionMessage | Exception],
        MemoryObjectSendStream[SessionMessage],
    ]:
        stack = AsyncExitStack()
        await stack.__aenter__()
        try:
            if self._config.command is not None:
                params = StdioServerParameters(
                    command=self._config.command[0],
                    args=self._config.command[1:],
                    env=dict(os.environ),
                )
                read_stream, write_stream = await stack.enter_async_context(
                    stdio_client(params)
                )
            else:
                url = self._config.url
                try:
                    read_stream, write_stream, _ = await stack.enter_async_context(
                        streamablehttp_client(url)
                    )
                except Exception:
                    # Fall back to SSE transport.
                    await stack.aclose()
                    stack = AsyncExitStack()
                    await stack.__aenter__()
                    read_stream, write_stream = await stack.enter_async_context(
                        sse_client(url)
                    )
        except Exception:
            await stack.aclose()
            raise

        self._transport_stack = stack
        return read_stream, write_stream

    async def _replay_handshake(
        self,
        write_stream: MemoryObjectSendStream[SessionMessage],
        read_stream: MemoryObjectReceiveStream[SessionMessage | Exception],
    ) -> None:
        init_req, _, initialized_notif = self._handshake

        proxy_init = types.JSONRPCRequest(
            method="initialize",
            params=init_req.params,
            id="proxy-init",
            jsonrpc="2.0",
        )
        await write_stream.send(SessionMessage(types.JSONRPCMessage(proxy_init)))

        async for item in read_stream:
            if isinstance(item, Exception):
                raise item
            msg = unwrap_message(item)
            if isinstance(msg, (types.JSONRPCResponse, types.JSONRPCError)) and msg.id == "proxy-init":
                if isinstance(msg, types.JSONRPCError):
                    raise RuntimeError(f"Downstream initialize failed: {msg.error.message}")
                result = msg.result if isinstance(msg.result, dict) else {}
                self.downstream_capabilities = result.get("capabilities", {})
                break

        await write_stream.send(SessionMessage(types.JSONRPCMessage(initialized_notif)))

    async def _reader_task(
        self,
        read_stream: MemoryObjectReceiveStream[SessionMessage | Exception],
    ) -> None:
        try:
            async for item in read_stream:
                if isinstance(item, Exception):
                    raise item

                msg = unwrap_message(item)

                if hasattr(msg, "method") and getattr(msg, "method", None) != _METHOD_PING:
                    from .idle import ActivitySource
                    self._idle_tracker.record_activity(ActivitySource.DOWNSTREAM)

                if isinstance(msg, (types.JSONRPCResponse, types.JSONRPCError)):
                    pending = self._pending.pop(msg.id, None)
                    if pending is not None and not pending.future.done():
                        pending.future.set_result(msg)
                elif isinstance(msg, types.JSONRPCNotification):
                    self._on_notification(msg)
        except (anyio.ClosedResourceError, anyio.EndOfStream):
            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("Reader task error", exc_info=True)
        finally:
            self._write_stream = None
            self._reader_task_handle = None

            if self._transport_stack is not None:
                try:
                    await self._transport_stack.aclose()
                except Exception:
                    pass
                self._transport_stack = None

            self._fail_pending_requests()

            if not self._explicit_disconnect:
                self._on_notification(
                    _make_log_notification("warning", "Downstream server connection lost")
                )

            if self._config.reconnect_immediately and not self._explicit_disconnect:
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    def _fail_pending_requests(self) -> None:
        for req_id, pending in self._pending.items():
            if not pending.future.done():
                pending.future.set_result(
                    _make_error(req_id, INFLIGHT_DISCONNECT_ERROR_MESSAGE)
                )
        self._pending.clear()

    async def _reconnect_loop(self) -> None:
        attempt = 0
        while True:
            delay = self._compute_backoff_delay(attempt)
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                await self.connect()
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                attempt += 1

    def _compute_backoff_delay(self, attempt: int) -> float:
        if attempt == 0:
            return 0.0
        return min(3 ** (attempt - 1), self._config.backoff.max)
