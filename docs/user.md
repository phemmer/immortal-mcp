# immortal-mcp — User Guide

immortal-mcp is a resilient MCP proxy server. It wraps another MCP server and keeps the client connection alive even when the downstream server dies or becomes unavailable. When the downstream is gone, requests fail fast with an informative error rather than hanging. The downstream is automatically reconnected according to the configured policy.

## Installation

```
pip install immortal-mcp
```

## Basic usage

### Wrapping a local (stdio) MCP server

```
immortal-mcp python -m my_mcp_server
immortal-mcp npx @modelcontextprotocol/server-filesystem /path/to/data
```

The first argument without a leading `-` ends option processing; it and everything after it is the downstream command.

### Wrapping an HTTP MCP server

```
immortal-mcp http://localhost:8000/sse
immortal-mcp https://example.com/mcp
```

A single argument starting with `http:` or `https:` is automatically detected as a URL. Both SSE and streamable-HTTP transports are supported.

## Options

### Reconnect behaviour

By default, immortal-mcp reconnects to the downstream on demand — only when the next client request arrives after a disconnect.

```
--reconnect-immediately
```

Reconnect to the downstream immediately in the background when the connection is lost, using exponential backoff.

```
--backoff-max SECONDS         (default: 60.0)
```

Cap on the delay between reconnect attempts. Delays follow the sequence 1, 3, 9, 27, 81, … seconds (powers of 3), capped at this value. Only meaningful with `--reconnect-immediately`.

### Idle disconnection

Disconnect the downstream after a period of inactivity to free resources. The downstream is reconnected automatically on the next client request.

```
--idle-timeout SECONDS
```

Seconds of inactivity before disconnecting. 0 (the default) disables idle disconnect.

```
--idle-client-only
```

When set, only messages from the client count as activity. Unsolicited notifications from the downstream server (such as `notifications/tools/list_changed`) do not reset the idle timer.

## Behaviour when the downstream is unavailable

When the downstream is disconnected and a client request arrives, immortal-mcp fails the request immediately with a JSON-RPC error rather than hanging. The error message distinguishes two cases:

**Request was never sent** (downstream was already disconnected when the request arrived):
> The downstream MCP server is not connected. The request was not delivered.

**Request was sent but connection lost before response** (in-flight at the time of disconnect):
> The connection to the downstream MCP server was lost. The request may have been received and processed, but the response was lost.

## Using with Claude Desktop / MCP clients

Configure immortal-mcp in place of the original server command. For example, in `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "my-server": {
      "command": "immortal-mcp",
      "args": ["--reconnect-immediately", "python", "-m", "my_mcp_server"]
    }
  }
}
```
