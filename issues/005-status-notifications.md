---
id: "005"
title: "Client not informed when downstream connection is lost or restored"
status: in_progress
---

## Description

When the downstream server dies or reconnects, the MCP client has no way to know this has happened. The client receives errors on in-flight requests but has no visibility into the overall health of the downstream connection. Additionally, after a reconnect the downstream's tool list may have silently changed, leaving the client with a stale view.

## Requirements

### Connection status notifications

The proxy must send a `notifications/message` notification to the client when:

1. **The downstream connection is lost unexpectedly** (server crash, process exit, network error) — level `warning`
2. **The downstream connection is restored** (reconnect succeeds after a prior unexpected disconnect) — level `info`

Standard MCP log notification format:

```json
{
  "jsonrpc": "2.0",
  "method": "notifications/message",
  "params": {
    "level": "warning",
    "logger": "immortal-mcp",
    "data": "Downstream server connection lost"
  }
}
```

### List invalidation on reconnect

After any reconnect (following a prior unexpected disconnect), the proxy must send the following notifications to the client:

- `notifications/tools/list_changed`
- `notifications/prompts/list_changed`
- `notifications/resources/list_changed`

The downstream may have changed its advertised tools, prompts, or resources during the outage (e.g., a new server version was deployed). The proxy cannot determine what changed, so it notifies the client to re-fetch all lists.

### Capability advertisement and interception

The proxy must always advertise full `tools`, `prompts`, and `resources` support with `listChanged: true` in the `initialize` response sent to the client, regardless of the downstream's actual capabilities. The downstream may add support for these in the future (after a reconnect to a newer version), and the proxy itself is the one generating the `list_changed` notifications on reconnect.

The proxy modifies the downstream's `initialize` result before forwarding it to the client, ensuring:
- `capabilities.tools.listChanged = true`
- `capabilities.prompts.listChanged = true`
- `capabilities.resources.listChanged = true`

Any additional sub-capabilities from the downstream (e.g. `resources.subscribe`) are preserved.

### Empty-list responses for unsupported capabilities

The proxy must cache the downstream's real capabilities from its `initialize` response. When the client sends a list request (`tools/list`, `prompts/list`, `resources/list`, `resources/templates/list`) for a capability the downstream does not actually support (key absent from the real capabilities), the proxy must respond with an empty list rather than forwarding the request (which would error on the downstream).

## Scope

Notifications are sent for all disconnect/reconnect events:
- **Unplanned disconnect** (server crash): `notifications/message` level=warning
- **Idle disconnect** (intentional): `notifications/message` level=info (the client should know the connection was dropped)
- **Reconnect** (after any prior disconnect, whether crash or idle): `notifications/message` level=info, plus all three `list_changed` notifications
- **Initial connection**: no notifications (the client cannot process them before the `initialize` response is delivered)

Notifications are sent regardless of the configured reconnect mode.

Notifications are NOT sent during the initial `initialize` phase (before the client's `initialize` response is delivered), as the client cannot process them before initialization completes.

## Design notes

`DownstreamManager` already holds an `on_notification` callback for forwarding downstream-originated notifications to the client. Proxy-generated status notifications use the same callback — the proxy does not need to distinguish the source.

A boolean flag `_was_connected_before` on `DownstreamManager` tracks whether the connection was ever successfully established, allowing `connect()` to determine whether to send reconnect notifications.

Capability injection (modifying the `initialize` result) is performed in `ProxyServer._handle_initialize` before caching or forwarding the response.

The downstream's real capabilities are cached on `DownstreamManager` (set during `_replay_handshake` and the initial `_handle_initialize`). `ProxyServer._forward_request` checks the cached capabilities before forwarding list requests and returns an empty result for unsupported ones.
