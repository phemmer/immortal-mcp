---
id: "002"
title: "Downstream connection not restored after failure"
status: in_progress
---

## Description

When the downstream MCP server dies or becomes unavailable, there is no mechanism to restore the connection. The proxy should support automatic reconnection with configurable behavior.

## Requirements

### Reconnect modes

**Immediate mode** (`--reconnect-immediately`): When the downstream connection is lost, immediately begin attempting to reconnect in the background. If a client request arrives while reconnecting, fail it immediately with the disconnect error.

**On-demand mode** (default): When the downstream connection is lost, do not attempt to reconnect until a client request arrives. On the next client request, attempt to connect first, then forward the request.

### Backoff

The same exponential backoff policy applies in two situations:

1. **Immediate reconnect mode**: after any unexpected disconnect, the background reconnect loop uses backoff.
2. **Initial connection** (all modes): when the proxy first starts and the backend is unavailable, the `initialize` phase retries with the same backoff until the backend responds. The client's `initialize` response is withheld until the connection succeeds. This ensures the proxy can come online even when the backend starts later.

Backoff sequence:
- First attempt: immediate (no delay)
- Subsequent attempts: `min(3^(N-1), max)` seconds — giving 1, 3, 9, 27, 81, … capped at max
- Configurable: maximum delay only

Backoff resets on successful connection.

### Handshake replay

On reconnect, the proxy replays the cached MCP handshake to the downstream server:
1. Send `initialize` with the same params received from the client (using a proxy-generated request ID)
2. Receive and discard the downstream's response (capabilities may have changed; clients will get updated info on next `tools/list` etc.)
3. Send `initialized` notification

## Configuration flags

```
--reconnect-immediately         Reconnect to downstream immediately on disconnect (default: on-demand)
--backoff-max SECONDS           Maximum backoff delay in seconds (default: 60.0)
```
