"""Downstream MCP server connection manager.

Manages the lifecycle of the connection to the wrapped downstream MCP server,
including initial connection, handshake replay on reconnect, backoff scheduling
for immediate-reconnect mode, and failure notification for in-flight requests.

The downstream manager operates at the raw JSON-RPC stream level.  It does not
interpret MCP semantics beyond the initialization handshake.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass

import mcp.types as types
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp.shared.message import SessionMessage

from .cli import Config
from .idle import IdleTracker


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


@dataclass
class _PendingRequest:
    """Tracks a single in-flight request awaiting a downstream response."""

    future: asyncio.Future[types.JSONRPCResponse | types.JSONRPCError]
    """Resolved with the downstream response or rejected on disconnect."""


class DownstreamManager:
    """Manages the downstream MCP server connection and reconnection logic.

    Responsibilities:
    - Open and close the downstream transport (subprocess or HTTP).
    - Replay the MCP initialization handshake after each (re)connect.
    - Route outbound requests to the downstream and match inbound responses
      back to their waiting callers via request ID.
    - Fail all in-flight requests when the downstream disconnects.
    - Implement the configured reconnect policy (immediate with backoff,
      or on-demand).
    - Notify the proxy of downstream-originated notifications so they can
      be forwarded to the client.

    Thread-safety: all methods must be called from the same asyncio event loop.
    """

    def __init__(
        self,
        config: Config,
        on_notification: Callable[[types.JSONRPCNotification], None],
        idle_tracker: IdleTracker,
    ) -> None:
        """
        Args:
            config: Resolved proxy configuration.
            on_notification: Called with each notification received from the
                downstream server.  Must not block.
            idle_tracker: Shared idle tracker; downstream manager records
                DOWNSTREAM activity on each received message.
        """
        # store config, on_notification, idle_tracker on self
        # set _write_stream to None  (open downstream write stream, or None if disconnected)
        # set _reader_task_handle to None  (asyncio.Task running _reader_task, or None)
        # set _pending: dict[RequestId, _PendingRequest] = {}  (in-flight requests)
        # set _handshake to None  (cached tuple of (init_req, init_resp, initialized_notif))
        # set _transport_exit_stack to None  (AsyncExitStack owning the transport context manager)
        # set _reconnect_task to None  (asyncio.Task running _reconnect_loop, or None)
        # set _was_connected to False  (True after the first successful connect;
        #                               used to distinguish reconnect from initial connect
        #                               for notification purposes)
        # set downstream_capabilities to {}  (dict: cached capabilities from the
        #                                     downstream's initialize result; e.g.
        #                                     {"tools": {}, "resources": {"subscribe": true}}.
        #                                     Absence of a key means the downstream
        #                                     does not support that capability.)

    # ------------------------------------------------------------------
    # Handshake cache
    # ------------------------------------------------------------------

    def set_handshake(
        self,
        initialize_request: types.JSONRPCRequest,
        initialize_response: types.JSONRPCResponse,
        initialized_notification: types.JSONRPCNotification,
    ) -> None:
        """Cache the client's MCP initialization handshake for replay on reconnect.

        Must be called before the first connect() or before any reconnect.
        Subsequent calls overwrite the previously cached handshake.
        """
        # store (initialize_request, initialize_response, initialized_notification) in _handshake

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open the downstream transport and replay the cached handshake.

        Raises RuntimeError if the handshake has not been set yet.
        Raises an appropriate exception if the transport cannot be opened.

        After a successful connect, spawns a background reader task that
        dispatches inbound messages to pending-request futures or the
        notification callback.
        """
        # if _handshake is None: raise RuntimeError("handshake not set")
        # open transport: (read_stream, write_stream) = await _open_transport()
        # store write_stream on self as _write_stream
        # replay handshake: await _replay_handshake(write_stream, read_stream)
        #   (also updates downstream_capabilities from the handshake response)
        # spawn _reader_task as an asyncio.Task, store in _reader_task_handle
        # if _was_connected:
        #   send reconnect notification via on_notification (notifications/message, info)
        #   send notifications/tools/list_changed via on_notification
        #   send notifications/prompts/list_changed via on_notification
        #   send notifications/resources/list_changed via on_notification
        # set _was_connected to True

    async def disconnect(self) -> None:
        """Close the downstream transport and cancel the reader task.

        All in-flight requests are failed with DISCONNECT_ERROR_MESSAGE.
        Idempotent: calling disconnect() when already disconnected is a no-op.
        """
        # if _write_stream is None and _reader_task_handle is None: return  (already disconnected)
        # cancel and await _reconnect_task if running, set to None
        # cancel and await _reader_task_handle if not None, set to None
        # set _write_stream to None
        # close the transport via _transport_exit_stack if not None, set to None
        # call _fail_pending_requests()
        # if _was_connected:
        #   send idle-disconnect notification via on_notification
        #   (notifications/message, level=info, "Downstream server disconnected due to inactivity")

    @property
    def is_connected(self) -> bool:
        """True when the downstream transport is open and the reader is running."""
        # return _write_stream is not None and _reader_task_handle is not None and not done

    # ------------------------------------------------------------------
    # Message sending
    # ------------------------------------------------------------------

    async def send_request(
        self, request: types.JSONRPCRequest
    ) -> types.JSONRPCResponse | types.JSONRPCError:
        """Send a request to the downstream and return its response.

        If not connected and reconnect mode is on-demand, attempts to connect
        first.  If the connection attempt fails, returns a JSONRPCError.

        If not connected and reconnect mode is immediate (background reconnect
        is in progress), returns a JSONRPCError immediately without waiting.

        If the downstream disconnects while the request is in flight, the
        returned future is resolved with a JSONRPCError.
        """
        # if not connected:
        #   if request.method == "ping": return a pong response immediately
        #     (ping must not trigger a connect; respond locally instead)
        #   if reconnect_immediately: return _make_not_connected_error(request.id)
        #   else (on-demand):
        #     try: await connect()
        #     except Exception: return _make_not_connected_error(request.id)
        # create asyncio.Future, store in _pending[request.id] as _PendingRequest
        # send SessionMessage(request) to _write_stream
        # await the future and return its result

    async def send_notification(self, notification: types.JSONRPCNotification) -> None:
        """Send a notification to the downstream.

        Silently drops the notification if the downstream is not connected,
        as notifications have no response to fail.
        """
        # if not connected: return
        # send SessionMessage(notification) to _write_stream

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _open_transport(
        self,
    ) -> tuple[
        MemoryObjectReceiveStream[SessionMessage | Exception],
        MemoryObjectSendStream[SessionMessage],
    ]:
        """Open the appropriate downstream transport based on config.

        Returns a (read_stream, write_stream) pair.
        The async context manager that owns the transport lifetime is stored on
        self so that disconnect() can close it.
        """
        # create a new AsyncExitStack and enter it, storing it as _transport_exit_stack
        # if config.command is not None:
        #   build StdioServerParameters(command=config.command[0], args=config.command[1:])
        #   enter stdio_client(params) context via the stack
        # else (HTTP):
        #   enter streamablehttp_client(config.url) context via the stack;
        #   if that fails with a transport error, fall back to sse_client(config.url)
        # return (read_stream, write_stream) from the entered context

    async def _replay_handshake(
        self,
        write_stream: MemoryObjectSendStream[SessionMessage],
        read_stream: MemoryObjectReceiveStream[SessionMessage | Exception],
    ) -> None:
        """Replay the cached initialization handshake to the downstream.

        Sends `initialize` (with a proxy-generated request ID), awaits the
        response, then sends `initialized`.  The downstream's response is
        discarded; the client already has its own cached response.
        """
        # unpack _handshake into (init_req, _, initialized_notif)
        # build a new JSONRPCRequest with method="initialize", same params as init_req,
        #   but with a proxy-generated id (e.g. "proxy-init") to avoid collision
        # send SessionMessage(new_init_req) to write_stream
        # read messages from read_stream until a JSONRPCResponse or JSONRPCError
        #   matching our proxy id is received (discard other messages)
        # cache downstream_capabilities from the response result["capabilities"]
        # send SessionMessage(initialized_notif) to write_stream

    async def _reader_task(
        self,
        read_stream: MemoryObjectReceiveStream[SessionMessage | Exception],
    ) -> None:
        """Background task: read messages from downstream and dispatch them.

        Responses (JSONRPCResponse / JSONRPCError) are matched to pending
        request futures by ID.  Notifications (JSONRPCNotification) are
        forwarded to the on_notification callback.

        Terminates when the stream ends or raises, triggering disconnect
        cleanup and (if configured) the immediate-reconnect loop.
        """
        # try:
        #   async for session_message in read_stream:
        #     if session_message is an Exception: raise it (propagate transport errors)
        #     record DOWNSTREAM activity on idle_tracker for non-ping messages
        #     unwrap: message = session_message.message
        #     if message is JSONRPCResponse or JSONRPCError:
        #       if message.id in _pending: resolve the future with message; remove from _pending
        #       else: log/discard (unexpected response id)
        #     elif message is JSONRPCNotification:
        #       call on_notification(message)
        #     else: discard (JSONRPCRequest from downstream is unexpected)
        # except (anyio.ClosedResourceError, Exception):
        #   pass  (stream ended or transport error)
        # finally:
        #   set _write_stream to None
        #   set _reader_task_handle to None
        #   call _fail_pending_requests()
        #   send disconnect notification via on_notification (notifications/message, warning)
        #   if config.reconnect_immediately:
        #     spawn _reconnect_loop() as asyncio.Task, store in _reconnect_task

    def _fail_pending_requests(self) -> None:
        """Resolve all in-flight request futures with a disconnect error.

        Called when the downstream disconnects unexpectedly.
        """
        # for each (id, pending) in _pending.items():
        #   if not pending.future.done():
        #     pending.future.set_result(_make_inflight_error(id))
        # clear _pending

    async def _reconnect_loop(self) -> None:
        """Background task: repeatedly attempt to reconnect with exponential backoff.

        Only runs in immediate-reconnect mode.  Stops when a connection
        succeeds or when disconnect() is called explicitly (e.g. idle timeout).
        """
        # attempt = 0
        # loop:
        #   delay = _compute_backoff_delay(attempt)
        #   if delay > 0: await asyncio.sleep(delay)
        #   try:
        #     await connect()
        #     return  (success — normal forwarding resumes via _reader_task)
        #   except asyncio.CancelledError: raise  (disconnect() was called)
        #   except Exception:
        #     attempt += 1
        #     (loop again with increased delay)

    def _compute_backoff_delay(self, attempt: int) -> float:
        """Return the backoff delay in seconds for the given attempt number.

        Attempt 0 returns 0 (first reconnect is always immediate).
        Attempt N > 0 returns min(3^(N-1), config.backoff.max).
        Sequence after the first attempt: 1, 3, 9, 27, 81, … seconds.
        """
        # if attempt == 0: return 0.0
        # return min(3 ** (attempt - 1), config.backoff.max)
