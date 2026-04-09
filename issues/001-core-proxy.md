---
id: "001"
title: "MCP messages not forwarded when downstream server is unavailable"
status: in_progress
---

## Description

There is no mechanism to forward MCP protocol messages between a client and a downstream MCP server through a resilient intermediary. When the downstream server is unavailable or dies, the client loses its connection and must reconnect from scratch, losing all session state.

The proxy must present as a standard stdio MCP server to the client, forwarding all messages to the downstream server transparently. The client's perspective must be that the connection is always alive.

## Requirements

- Accept a client connection via stdio (stdin/stdout)
- Forward all MCP JSON-RPC messages to the configured downstream server
- Cache the MCP initialization handshake (`initialize` request/response and `initialized` notification) from the client so it can be replayed to the downstream on reconnect without client involvement
- When the downstream is disconnected and a client request arrives, fail the request immediately with an error indicating the downstream connection was lost and the request may or may not have been received
- Forward notifications from the downstream server to the client (e.g., `notifications/tools/list_changed`)
- Activity in either direction (excluding pings) resets the idle timer (see issue #003)

## Error messages for failed requests

Two distinct error messages depending on whether the request was dispatched:

**Not connected** (request was never sent):

```json
{
  "jsonrpc": "2.0",
  "id": <original_id>,
  "error": {
    "code": -32603,
    "message": "The downstream MCP server is not connected. The request was not delivered."
  }
}
```

**In-flight disconnect** (request was sent but connection lost before response):

```json
{
  "jsonrpc": "2.0",
  "id": <original_id>,
  "error": {
    "code": -32603,
    "message": "The connection to the downstream MCP server was lost. The request may have been received and processed, but the response was lost."
  }
}
```

## Design notes

The proxy operates at the raw JSON-RPC stream level rather than the typed MCP handler level. This ensures all current and future MCP protocol messages are forwarded correctly without requiring explicit handler registration for each method.

- Outer transport: `mcp.server.stdio.stdio_server()` (anyio stream)
- Downstream transports: `mcp.client.stdio.stdio_client()` for local servers, `mcp.client.sse.sse_client()` or `mcp.client.streamable_http.streamablehttp_client()` for HTTP servers
- Both sides expose the same `(read_stream, write_stream)` interface, enabling uniform handling
