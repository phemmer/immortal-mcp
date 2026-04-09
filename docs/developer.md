# immortal-mcp — Developer Guide

## Architecture overview

```
stdin  ──► ProxyServer ──► DownstreamManager ──► subprocess / HTTP
stdout ◄──            ◄──                    ◄──
```

The proxy operates at the raw JSON-RPC stream level. It does not use the high-level `mcp.server.lowlevel.Server` handler-registration API. Instead it reads `JSONRPCMessage` objects from the anyio streams directly. This ensures all MCP protocol messages — including any future additions — are forwarded without requiring per-method handler registration.

## Module map

| Module | Purpose |
|--------|---------|
| `__main__.py` | Entry point; calls `parse_args()` then `ProxyServer.run()` |
| `cli.py` | `argparse`-based CLI; produces a frozen `Config` dataclass |
| `idle.py` | Background timer that fires when no activity is observed |
| `downstream.py` | Downstream transport lifecycle, request routing, reconnect |
| `proxy.py` | Outer stdio server; dispatches client messages; orchestrates the other modules |

### Dependency order (lowest to highest)

```
cli.py  idle.py
        ↓
    downstream.py
        ↓
      proxy.py
        ↓
    __main__.py
```

## Transport layer

Both outer (client-facing) and inner (downstream) transports expose the same anyio stream interface from the mcp SDK:

- **Read**: `MemoryObjectReceiveStream[JSONRPCMessage | Exception]`
- **Write**: `MemoryObjectSendStream[JSONRPCMessage]`

Outer transport: `mcp.server.stdio.stdio_server()` — reads client messages from stdin, writes responses to stdout.

Inner transports:
- Local stdio: `mcp.client.stdio.stdio_client(StdioServerParameters(...))` — launches a subprocess.
- HTTP: `mcp.client.sse.sse_client(url)` or `mcp.client.streamable_http.streamablehttp_client(url)`.

## Initialization handshake

MCP requires an `initialize` / `initialized` exchange before any other messages can be sent. From the client's perspective this happens once, at the start of the session. The proxy caches the full handshake so it can replay it to a fresh downstream connection without client involvement:

1. Client sends `initialize` → proxy forwards to downstream, caches `(request, response, ...)`.
2. Downstream responds → proxy caches the response and forwards it to the client.
3. Client sends `notifications/initialized` → proxy caches it, forwards to downstream.

On reconnect, the proxy replays steps 1–3 using the cached request params and a proxy-generated request ID (to avoid collisions with client-originated IDs). The downstream's new response is discarded; the client retains its original session state.

### Initial connection retry

When the proxy first starts, the downstream may not be available yet. The `_handle_initialize` method retries with the same 3^N backoff policy as reconnect, holding the client's `initialize` response until the downstream responds. The client blocks until the backend is ready — MCP clients do not have a hard timeout on `initialize`.

### Capability injection

The proxy always advertises `tools`, `prompts`, and `resources` with `listChanged: true` in the `initialize` response to the client, regardless of the downstream's actual capabilities. This is because the proxy itself generates `list_changed` notifications on reconnect (the downstream may have changed), and the downstream may gain new capabilities after a restart.

The downstream's real capabilities are cached in `DownstreamManager.downstream_capabilities`. When the client sends a list request (`tools/list`, `prompts/list`, etc.) for a capability the downstream doesn't support, the proxy returns an empty list instead of forwarding.

## Request routing

`DownstreamManager` maintains a `dict[RequestId, asyncio.Future]` of in-flight requests. When a response arrives from the downstream its ID is used to resolve the matching future. When the downstream disconnects all pending futures are rejected with a `JSONRPCError` carrying `DISCONNECT_ERROR_MESSAGE`.

## Reconnect modes

**On-demand** (default): `DownstreamManager.send_request()` calls `connect()` if not connected before sending. If `connect()` fails, the request future is rejected.

**Immediate**: On reader-task termination (downstream EOF / exception), a background `_reconnect_loop` task is spawned. It attempts `connect()` with delays following the sequence 0, 1, 3, 9, 27, … seconds (attempt 0 is immediate; subsequent delays are `min(3^(N-1), max)`). Any client request that arrives during reconnect is rejected immediately. When a connection succeeds the loop exits and normal forwarding resumes.

`--idle-timeout` and `--reconnect-immediately` are mutually exclusive (CLI error). This eliminates the need for any idle-disconnect suppression logic.

## Status notifications

The proxy sends `notifications/message` to the client for connection lifecycle events:
- **Unexpected disconnect** (server crash): level=warning, "Downstream server connection lost"
- **Idle disconnect**: level=info, "Downstream server disconnected due to inactivity"
- **Reconnect** (after any prior disconnect): level=info, "Downstream server connection restored"

On reconnect, the proxy also sends list invalidation notifications:
- `notifications/tools/list_changed`
- `notifications/prompts/list_changed`
- `notifications/resources/list_changed`

Notifications are not sent during the initial `initialize` phase (before the client's `initialize` response is delivered).

## Idle tracking

`IdleTracker` maintains `_last_activity_time` and runs a periodic check task. Activity is recorded by `ProxyServer` on each non-ping message. When `time.monotonic() - _last_activity_time >= timeout`, the on_idle callback is called synchronously (the callback schedules `downstream.disconnect()` via `asyncio.create_task`).

The `client_only` flag causes `record_activity(ActivitySource.DOWNSTREAM)` to be a no-op.

## Adding a new downstream transport

1. Add a new branch in `DownstreamManager._open_transport()` that returns the same `(read_stream, write_queue)` pair as the existing branches.
2. Add the corresponding CLI flag(s) in `cli.py` and update the `Config` dataclass.
3. Update `docs/user.md` with usage instructions.
