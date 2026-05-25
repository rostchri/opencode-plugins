# opencode-memcore

OpenCode plugin that gives an OpenCode session **long-term agent memory**
through a self-hosted [memcore][memcore] MCP server (the AMP tool family:
`amp.encode`, `amp.recall`, `amp.forget`, `amp.consolidate`, `amp.pin`,
`amp.move_memory`, `amp.stats`).

Conceptually parallel to [opencode-supermemory][supermem] and
[opencode-graphiti][graphiti]: same idea, different backend.

## Status

**Planned / skeleton.** Will likely start as a fork of `opencode-graphiti`
with the MCP tool calls swapped from Graphiti's `add_memory` /
`search_nodes` to memcore's `amp.encode` / `amp.recall` and the
multi-tenant `agent_id` derived from the OpenCode session identity.

## Configuration

All connection parameters come from environment variables.

| Variable | Required | Purpose |
|---|---|---|
| `OPENCODE_MEMCORE_MCP_URL` | yes | URL of the memcore MCP server (e.g. exposed through your MCP gateway) |
| `OPENCODE_MEMCORE_BEARER` | yes | bearer token if the gateway requires one |
| `OPENCODE_MEMCORE_AGENT_ID` | no | static agent_id override; otherwise derived from session |

## Wiring

```jsonc
// opencode.json
{
  "plugin": ["opencode-memcore"]
}
```

## License

MIT

[memcore]: https://github.com/srbhrai/smriti-memcore
[supermem]: https://github.com/supermemoryai/opencode-supermemory
[graphiti]: https://github.com/happycastle114/opencode-graphiti
