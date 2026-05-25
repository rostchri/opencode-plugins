# opencode-chatroom-voice

OpenCode plugin that adds **event-driven voice conversation** to any OpenCode
session through a backend that speaks the [Phoenix Channels][phx] protocol
(such as the chatroom reference implementation included in this repo's
ecosystem, or any other compatible backend).

Unlike a hold-to-talk CLI mic, this plugin treats voice as a first-class
event stream:

- a browser user speaks → the backend transcribes → the plugin injects
  the text as a regular user message into the OpenCode session via
  `client.session.sendMessage()`
- the agent's tool calls and replies are broadcast back to the browser
  through dedicated `voice_*` tools so the browser can render speech,
  markdown, mermaid diagrams, maps, etc. in real time

Multiple browser clients can join the same logical room and observe the
same agent session.

## Status

**Planned / skeleton.** The package layout, plugin entry-point and hook
wiring exist; the WebSocket/STT/TTS integration is the work item.

## Configuration

All connection parameters come from environment variables — **the plugin
itself ships no hostnames, tokens or other infrastructure assumptions**.

| Variable | Required | Purpose |
|---|---|---|
| `OPENCODE_VOICE_WS_URL` | yes | `wss://…/socket/websocket` of the voice backend |
| `OPENCODE_VOICE_BEARER` | yes | bearer token for the backend (e.g. a JWT) |
| `OPENCODE_VOICE_ROOM` | no | default room identifier (default: `Lobby`) |
| `OPENCODE_VOICE_SESSION_ID` | no | stable session identifier announced to the backend |

## Wiring

```jsonc
// opencode.json
{
  "plugin": ["opencode-chatroom-voice"]
}
```

## Hooks used

| Hook | Used for |
|---|---|
| `event` | session lifecycle: announce agent online/offline to the room |
| `chat.message` | mirror outgoing user-prompts to the browser transcript |
| `tool.execute.before` / `.after` | "working…" badge in the browser |
| custom `tool` registrations | `voice_reply`, `voice_markdown`, `voice_thought`, `voice_mermaid`, `voice_map` |

## License

MIT

[phx]: https://hexdocs.pm/phoenix/channels.html
