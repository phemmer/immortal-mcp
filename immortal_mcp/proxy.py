"""Main proxy orchestrator.

Bridges a stdio MCP client to a downstream MCP server managed by
DownstreamManager.  Operates at the raw JSON-RPC stream level so that
all current and future MCP protocol messages are forwarded without
requiring explicit per-method handler registration.

Responsibilities:
- Run the outer stdio server (mcp.server.stdio.stdio_server).
- Intercept and cache the MCP initialization handshake.
- Forward all other client messages to the downstream.
- Forward all downstream notifications to the client.
- Track activity for the idle timeout.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import mcp.types as types
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage

from .cli import Config
from .downstream import DownstreamManager
from .idle import ActivitySource, IdleTracker


# MCP method name for the initialization request.
_METHOD_INITIALIZE = "initialize"

# MCP notification method sent after initialization completes.
_METHOD_INITIALIZED = "notifications/initialized"

# MCP ping method — excluded from idle-activity tracking.
_METHOD_PING = "ping"

# List methods whose responses we fake when the downstream doesn't support the capability.
_LIST_METHODS_BY_CAPABILITY: dict[str, list[str]] = {
    "tools": ["tools/list"],
    "prompts": ["prompts/list"],
    "resources": ["resources/list", "resources/templates/list"],
}

# Notifications sent to the client after a reconnect.
_LIST_CHANGED_NOTIFICATIONS = [
    "notifications/tools/list_changed",
    "notifications/prompts/list_changed",
    "notifications/resources/list_changed",
]


class ProxyServer:
    """Top-level proxy that forwards JSON-RPC between a stdio client and a downstream server.

    Lifecycle::

        server = ProxyServer(config)
        await server.run()   # blocks until stdin is closed or an error occurs

    All internal state is created fresh on each call to run(), so the object
    may not be reused across multiple run() invocations.
    """

    def __init__(self, config: Config) -> None:
        """
        Args:
            config: Resolved proxy configuration from parse_args().
        """
        # store config on self
        # set _downstream_capabilities to {}  (populated during _handle_initialize)

    async def run(self) -> None:
        """Run the proxy until the client disconnects or a fatal error occurs.

        Opens the outer stdio transport, sets up the downstream manager and
        idle tracker, then concurrently:
        - Reads client messages and processes them.
        - Reads downstream notifications and forwards them to the client.
        - Runs the idle timer (if configured).
        """
        # open stdio_server() as (read_stream, write_stream)
        # create IdleTracker with config.idle settings;
        #   on_idle callback calls asyncio.create_task(downstream.disconnect())
        # create notification handler via _make_notification_handler(write_stream)
        # create DownstreamManager with config, notification handler, idle_tracker
        # start idle_tracker
        # try:
        #   await _handle_client_messages(read_stream, write_stream, downstream, idle_tracker)
        # finally:
        #   idle_tracker.stop()
        #   await downstream.disconnect()

    # ------------------------------------------------------------------
    # Client message handling
    # ------------------------------------------------------------------

    async def _handle_client_messages(
        self,
        read_stream: MemoryObjectReceiveStream[SessionMessage | Exception],
        write_stream: MemoryObjectSendStream[SessionMessage],
        downstream: DownstreamManager,
        idle_tracker: IdleTracker,
    ) -> None:
        """Read messages from the client and dispatch them.

        Runs until the client read stream is exhausted.
        """
        # async for session_message in read_stream:
        #   if session_message is an Exception: continue (skip malformed input)
        #   message = session_message.message
        #   record client activity (unless ping)
        #   if message is JSONRPCRequest:
        #     if message.method == "initialize":
        #       await _handle_initialize(message, write_stream, downstream)
        #     else:
        #       await _forward_request(message, write_stream, downstream)
        #   elif message is JSONRPCNotification:
        #     if message.method == "notifications/initialized":
        #       await _handle_initialized(message, downstream)
        #     else:
        #       await _forward_notification(message, downstream)

    async def _handle_initialize(
        self,
        request: types.JSONRPCRequest,
        write_stream: MemoryObjectSendStream[SessionMessage],
        downstream: DownstreamManager,
    ) -> None:
        """Handle the client's `initialize` request.

        Connects to the downstream using the same backoff policy as reconnect,
        retrying indefinitely until the downstream responds.  Once connected,
        injects proxy capabilities into the response, caches the full handshake,
        and sends the modified response to the client.

        After this call, downstream.set_handshake() has been populated and
        the downstream is connected.
        """
        # Build a temporary handshake to bootstrap the first connect:
        #   we need set_handshake before connect(), but we don't have the response yet.
        #   Solution: send initialize to downstream manually during the first connect,
        #   outside of the normal replay path.
        #
        # attempt = 0
        # loop:
        #   delay = downstream._compute_backoff_delay(attempt)
        #   if delay > 0: await asyncio.sleep(delay)
        #   try:
        #     open transport via downstream._open_transport()
        #     send SessionMessage(request) to the write stream  (the client's original initialize)
        #     read from the read stream until a JSONRPCResponse matching request.id arrives
        #     break out of the retry loop
        #   except asyncio.CancelledError: raise
        #   except Exception:
        #     attempt += 1
        #     close transport if partially opened
        #     continue
        #
        # got the downstream's initialize response
        # inject proxy capabilities into the response result via _inject_capabilities
        # cache downstream_capabilities from the original (pre-injection) response
        # send the modified response to the client via write_stream
        #
        # now we need to complete the handshake: send initialized notification
        # build a placeholder initialized notification
        # call downstream.set_handshake(request, modified_response, initialized_notif)
        #   so future reconnects can replay it
        # send the initialized notification to the downstream
        # spawn the reader task on the downstream so normal message forwarding begins

    async def _handle_initialized(
        self,
        notification: types.JSONRPCNotification,
        downstream: DownstreamManager,
    ) -> None:
        """Handle the `notifications/initialized` notification from the client.

        Updates the cached handshake and forwards the notification to the
        downstream (it has already received it as part of the handshake
        replay, but this keeps the manager's cached copy current).
        """
        # forward the notification to downstream via send_notification
        # (The handshake was already set during _handle_initialize;
        #  the initialized notification was already sent to the downstream
        #  as part of the initial connect.  This is just the client's copy
        #  arriving — update the cached version if needed.)

    async def _forward_request(
        self,
        request: types.JSONRPCRequest,
        write_stream: MemoryObjectSendStream[SessionMessage],
        downstream: DownstreamManager,
    ) -> None:
        """Forward a non-initialize request to the downstream and write the response.

        If the request is a list method (tools/list, prompts/list, etc.) for a
        capability the downstream does not support, returns an empty list
        immediately without forwarding.

        On disconnect, writes a JSONRPCError with the disconnect message.
        """
        # if _is_unsupported_list_method(request.method):
        #   send _empty_list_response(request.id, request.method) to write_stream
        #   return
        # response = await downstream.send_request(request)
        # send SessionMessage(response) to write_stream

    async def _forward_notification(
        self,
        notification: types.JSONRPCNotification,
        downstream: DownstreamManager,
    ) -> None:
        """Forward a client notification to the downstream (fire-and-forget)."""
        # await downstream.send_notification(notification)

    # ------------------------------------------------------------------
    # Capability handling
    # ------------------------------------------------------------------

    @staticmethod
    def _inject_capabilities(result: dict) -> dict:
        """Modify the downstream's initialize result to advertise proxy capabilities.

        Ensures tools, prompts, and resources all have listChanged=true.
        Preserves any additional sub-capabilities from the downstream.
        Returns the modified result dict (mutates in place).
        """
        # caps = result.setdefault("capabilities", {})
        # for each key in ("tools", "prompts", "resources"):
        #   sub = caps.setdefault(key, {})
        #   sub["listChanged"] = True
        # return result

    def _is_unsupported_list_method(self, method: str) -> bool:
        """Return True if method is a list request for a capability the downstream lacks.

        Uses the cached downstream capabilities set during initialization.
        """
        # for each (capability, methods) in _LIST_METHODS_BY_CAPABILITY:
        #   if method in methods and capability not in _downstream_capabilities:
        #     return True
        # return False

    @staticmethod
    def _empty_list_response(request_id: types.RequestId, method: str) -> types.JSONRPCResponse:
        """Build an empty-list response for an unsupported list method."""
        # determine the result key from the method:
        #   "tools/list" -> {"tools": []}
        #   "prompts/list" -> {"prompts": []}
        #   "resources/list" -> {"resources": []}
        #   "resources/templates/list" -> {"resourceTemplates": []}
        # return JSONRPCResponse(id=request_id, result=<the empty dict>, jsonrpc="2.0")

    # ------------------------------------------------------------------
    # Downstream notification forwarding
    # ------------------------------------------------------------------

    def _make_notification_handler(
        self,
        write_stream: MemoryObjectSendStream[SessionMessage],
    ) -> Callable[[types.JSONRPCNotification], None]:
        """Return the on_notification callback passed to DownstreamManager.

        The callback enqueues each downstream notification onto the client
        write stream so the concurrent writer task can send it.
        """
        # def handler(notification: JSONRPCNotification) -> None:
        #   write_stream.send_nowait(SessionMessage(notification))
        # return handler

    # ------------------------------------------------------------------
    # Idle tracking helpers
    # ------------------------------------------------------------------

    def _record_client_activity(
        self,
        message: types.JSONRPCMessage,
        idle_tracker: IdleTracker,
    ) -> None:
        """Record CLIENT activity for a non-ping message."""
        # if message has a method attribute and method == "ping": return
        # idle_tracker.record_activity(ActivitySource.CLIENT)

    def _record_downstream_activity(
        self,
        message: types.JSONRPCMessage,
        idle_tracker: IdleTracker,
    ) -> None:
        """Record DOWNSTREAM activity for a non-ping message."""
        # if message has a method attribute and method == "ping": return
        # for responses (no method), we can't distinguish ping responses easily;
        #   record the activity (conservative — a ping response is rare and harmless)
        # idle_tracker.record_activity(ActivitySource.DOWNSTREAM)
