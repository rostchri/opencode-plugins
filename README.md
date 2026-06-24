# opencode-plugins

[OpenCode](https://opencode.ai) plugins as a single mono-repo.

Each subdirectory is an independent plugin that can be installed standalone via
`bunx` or referenced as a local path in your `opencode.json`.

## Plugins

| Plugin | Purpose | Status |
|--------|---------|--------|
| [`chatroom-voice`](./chatroom-voice/) | Event-driven voice integration (browser TTS+STT, multi-room) via a Phoenix-channel backend | planned |

## Design constraints

- **No secrets, no private infrastructure URLs in source.** Every backend URL,
  bearer token, room identifier or hostname comes from environment variables
  (documented per plugin). The repo is safe to publish.
- **Generic.** Each plugin targets a class of backend (any Phoenix-channel
- **TypeScript, [Bun](https://bun.sh)-runtime.** Same as upstream OpenCode.
- **Hooks-only.** No fork of OpenCode itself — everything happens through the
  documented [plugin API](https://opencode.ai/docs/plugins/).

## Repo layout

```
.
├── chatroom-voice/      one plugin per directory
│   ├── package.json
│   ├── src/index.ts
│   └── README.md
│   └── ...
├── package.json         workspace root
└── tsconfig.json        shared tsconfig
```

## Local development

```sh
# Install all workspace deps
bun install

# Build a specific plugin
cd chatroom-voice && bun run build

# Wire a plugin into your own opencode.json (local path)
{
  "plugin": ["/path/to/opencode-plugins/chatroom-voice/dist/index.js"]
}
```

## Publishing

Each plugin is independently `bun publish`-able when stable. Until then, use
local paths or `bunx --bun /path/to/dist/index.js`.

## License

MIT
