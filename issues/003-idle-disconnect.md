---
id: "003"
title: "Downstream remains connected indefinitely when idle"
status: in_progress
---

## Description

When no MCP activity is occurring, the downstream server remains connected, consuming resources. The proxy should support automatically disconnecting the downstream server after a configurable period of inactivity.

## Requirements

- Track the time since the last MCP message in either direction (excluding MCP ping/pong messages, which are keepalives and not application activity)
- When no activity has occurred for the configured duration, disconnect the downstream server
- On the next client request after an idle disconnect, reconnect to the downstream (behavior follows the configured reconnect mode)
- Optional: only count client-to-server messages as activity (server-to-client messages such as unsolicited notifications do not reset the timer). Controlled via `--idle-client-only`.

## Configuration flags

```
--idle-timeout SECONDS          Disconnect downstream after N seconds of inactivity (0 = disabled, default: 0)
--idle-client-only              Only client→downstream messages count as activity for the idle timer
```

## Notes

`--idle-timeout` and `--reconnect-immediately` are **mutually exclusive**. Combining them is architecturally contradictory: idle timeout disconnects the downstream to save resources, while immediate reconnect would restore it instantly, defeating the purpose. The CLI returns an error if both are specified.
