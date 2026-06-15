"""Main proxy orchestrator.

Bridges a stdio MCP client to a downstream MCP server managed by
DownstreamManager.  Operates at the raw JSON-RPC stream level so that
all current and future MCP protocol messages are forwarded without
requiring explicit per-method handler registration.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

import anyio
import mcp.types as types
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage

from .cli import Config
from .downstream import DownstreamManager, unwrap_message, wrap_message
from .idle import ActivitySource, IdleTracker

logger = logging.getLogger(__name__)

_METHOD_INITIALIZE = "initialize"
_METHOD_INITIALIZED = "notifications/initialized"
_METHOD_PING = "ping"

_LIST_METHODS_BY_CAPABILITY: dict[str, list[str]] = {
    "tools": ["tools/list"],
    "prompts": ["prompts/list"],
    "resources": ["resources/list", "resources/templates/list"],
}

_LIST_CHANGED_NOTIFICATIONS = [
    "notifications/tools/list_changed",
    "notifications/prompts/list_changed",
    "notifications/resources/list_changed",
]

# Map list methods to the result key that should contain the empty list.
_EMPTY_LIST_RESULT: dict[str, dict] = {
    "tools/list": {"tools": []},
    "prompts/list": {"prompts": []},
    "resources/list": {"resources": []},
    "resources/templates/list": {"resourceTemplates": []},
}


class ProxyServer:
    """Top-level proxy that forwards JSON-RPC between a stdio client and a downstream server."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._downstream_capabilities: dict = {}

    async def run(self) -> None:
        """Run the proxy until the client disconnects or a fatal error occurs."""
        async with stdio_server() as (read_stream, write_stream):
            self._outbound: asyncio.Queue[SessionMessage] = asyncio.Queue()
            notification_handler = self._make_notification_handler()
            idle_tracker = IdleTracker(
                timeout=self._config.idle.timeout,
                client_only=self._config.idle.client_only,
                on_idle=lambda: self._on_idle(notification_handler),
            )
            self._downstream = DownstreamManager(
                config=self._config,
                on_notification=notification_handler,
                idle_tracker=idle_tracker,
            )
            outbound_task = asyncio.create_task(self._drain_outbound(write_stream))
            await idle_tracker.start()
            try:
                await self._handle_client_messages(
                    read_stream, write_stream, self._downstream, idle_tracker
                )
            finally:
                idle_tracker.stop()
                await self._downstream.disconnect()
                outbound_task.cancel()
                try:
                    await outbound_task
                except asyncio.CancelledError:
                    pass
                # Close the write stream so stdio_server's stdout_writer task
                # unblocks and its task group can exit.
                await write_stream.aclose()

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
        async for item in read_stream:
            if isinstance(item, Exception):
                err = types.JSONRPCError.model_construct(
                    jsonrpc="2.0",
                    id=None,
                    error=types.ErrorData(
                        code=-32700, message=f"Parse error: {item}"
                    ),
                )
                await write_stream.send(wrap_message(err))
                continue
            message = unwrap_message(item)
            self._record_client_activity(message, idle_tracker)

            if isinstance(message, types.JSONRPCRequest):
                if message.method == _METHOD_INITIALIZE:
                    await self._handle_initialize(message, write_stream, downstream)
                else:
                    await self._forward_request(message, write_stream, downstream)
            elif isinstance(message, types.JSONRPCNotification):
                if message.method == _METHOD_INITIALIZED:
                    await self._handle_initialized(message, downstream)
                else:
                    await self._forward_notification(message, downstream)

    async def _handle_initialize(
        self,
        request: types.JSONRPCRequest,
        write_stream: MemoryObjectSendStream[SessionMessage],
        downstream: DownstreamManager,
    ) -> None:
        """Handle the client's `initialize` request.

        Delegates to downstream.initial_connect() which retries with backoff.
        Injects proxy capabilities into the response before forwarding.
        """
        response = await downstream.initial_connect(request)

        result = response.result if isinstance(response.result, dict) else {}
        self._downstream_capabilities = downstream.downstream_capabilities

        modified_result = dict(result)
        self._inject_capabilities(modified_result)
        modified_response = types.JSONRPCResponse(
            jsonrpc="2.0",
            id=request.id,
            result=modified_result,
        )
        await write_stream.send(SessionMessage(types.JSONRPCMessage(modified_response)))

    async def _handle_initialized(
        self,
        notification: types.JSONRPCNotification,
        downstream: DownstreamManager,
    ) -> None:
        # notifications/initialized was already sent to the downstream during
        # initial_connect().  The client's copy is dropped to prevent a
        # duplicate.
        pass

    async def _forward_request(
        self,
        request: types.JSONRPCRequest,
        write_stream: MemoryObjectSendStream[SessionMessage],
        downstream: DownstreamManager,
    ) -> None:
        if self._is_unsupported_list_method(request.method):
            response = self._empty_list_response(request.id, request.method)
            await write_stream.send(SessionMessage(types.JSONRPCMessage(response)))
            return
        response = await downstream.send_request(request)
        await write_stream.send(SessionMessage(types.JSONRPCMessage(response)))

    async def _forward_notification(
        self,
        notification: types.JSONRPCNotification,
        downstream: DownstreamManager,
    ) -> None:
        await downstream.send_notification(notification)

    # ------------------------------------------------------------------
    # Capability handling
    # ------------------------------------------------------------------

    @staticmethod
    def _inject_capabilities(result: dict) -> dict:
        caps = result.get("capabilities", {})
        for key in ("tools", "prompts", "resources"):
            if key in caps:
                caps[key]["listChanged"] = True
        return result

    def _is_unsupported_list_method(self, method: str) -> bool:
        for capability, methods in _LIST_METHODS_BY_CAPABILITY.items():
            if method in methods and capability not in self._downstream_capabilities:
                return True
        return False

    @staticmethod
    def _empty_list_response(
        request_id: types.RequestId, method: str
    ) -> types.JSONRPCResponse:
        result = _EMPTY_LIST_RESULT.get(method, {})
        return types.JSONRPCResponse(jsonrpc="2.0", id=request_id, result=result)

    # ------------------------------------------------------------------
    # Downstream notification forwarding
    # ------------------------------------------------------------------

    def _make_notification_handler(
        self,
    ) -> Callable[[types.JSONRPCNotification], None]:
        """Build the callback that queues a client-bound notification.

        The callback is synchronous so it can be invoked from both async contexts (the downstream
        reader and reconnect paths) and sync ones (the idle timer). It only enqueues; the actual
        send is performed by `_drain_outbound`. The queue is unbounded, so enqueuing never blocks
        or drops — this is what makes server-initiated notifications reliable over the unbuffered
        client stream, where a fire-and-forget `send_nowait` would silently drop them."""

        def handler(notification: types.JSONRPCNotification) -> None:
            self._outbound.put_nowait(SessionMessage(types.JSONRPCMessage(notification)))

        return handler

    async def _drain_outbound(
        self,
        write_stream: MemoryObjectSendStream[SessionMessage],
    ) -> None:
        """Forward queued client-bound notifications, each with an awaited send.

        Serializing notifications through this single task preserves their order and guarantees
        delivery on the unbuffered client stream (an awaited `send` waits for the writer to be
        ready, unlike `send_nowait`). Responses are still sent inline by the request handlers; both
        paths are independent senders on the same stream."""
        try:
            while True:
                message = await self._outbound.get()
                await write_stream.send(message)
        except (anyio.ClosedResourceError, anyio.BrokenResourceError):
            pass

    # ------------------------------------------------------------------
    # Idle tracking helpers
    # ------------------------------------------------------------------

    def _on_idle(
        self,
        notification_handler: Callable[[types.JSONRPCNotification], None],
    ) -> None:
        notification_handler(
            types.JSONRPCNotification(
                method="notifications/message",
                jsonrpc="2.0",
                params={
                    "level": "info",
                    "logger": "immortal-mcp",
                    "data": "Downstream server disconnected due to inactivity",
                },
            )
        )
        asyncio.ensure_future(self._downstream.disconnect())

    def _record_client_activity(
        self,
        message: types.JSONRPCMessage,
        idle_tracker: IdleTracker,
    ) -> None:
        if hasattr(message, "method") and getattr(message, "method", None) == _METHOD_PING:
            return
        idle_tracker.record_activity(ActivitySource.CLIENT)

    def _record_downstream_activity(
        self,
        message: types.JSONRPCMessage,
        idle_tracker: IdleTracker,
    ) -> None:
        if hasattr(message, "method") and getattr(message, "method", None) == _METHOD_PING:
            return
        idle_tracker.record_activity(ActivitySource.DOWNSTREAM)
