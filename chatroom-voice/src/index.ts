/**
 * opencode-chatroom-voice — skeleton
 *
 * Provides event-driven voice integration for OpenCode through any backend
 * speaking the Phoenix Channels protocol. **This is a placeholder** with the
 * plugin shape wired up; the actual WebSocket / STT / TTS work is not yet
 * implemented and lives in the linked GitHub issues.
 *
 * No infrastructure URLs, hostnames or tokens are baked in — every external
 * reference resolves from environment variables (see README.md).
 */
import type { Plugin } from "@opencode-ai/plugin"
import { tool } from "@opencode-ai/plugin"

/** Resolve required env vars at plugin-init time. */
function loadConfig() {
  const wsUrl = process.env.OPENCODE_VOICE_WS_URL
  const bearer = process.env.OPENCODE_VOICE_BEARER
  if (!wsUrl || !bearer) {
    throw new Error(
      "opencode-chatroom-voice: OPENCODE_VOICE_WS_URL and OPENCODE_VOICE_BEARER " +
        "must be set. See README for details.",
    )
  }
  return {
    wsUrl,
    bearer,
    defaultRoom: process.env.OPENCODE_VOICE_ROOM ?? "Lobby",
    sessionId: process.env.OPENCODE_VOICE_SESSION_ID,
  }
}

export const ChatroomVoice: Plugin = async (_ctx) => {
  const cfg = loadConfig()

  // TODO(#1): open a Phoenix-Channel WebSocket to cfg.wsUrl using cfg.bearer,
  //           subscribe to the room, register an onMessage handler that calls
  //           ctx.client.session.sendMessage({...}) for inbound speech.

  void cfg // silence "unused" until the body is filled in

  return {
    // TODO(#2): wire the session.created / .ended events to room presence
    //           (announce "agent online" / "agent offline" to the browser).
    event: async () => {
      /* not yet implemented */
    },

    // TODO(#3): broadcast tool activity so the browser can render a
    //           "working…" badge without polling.
    "tool.execute.before": async () => {
      /* not yet implemented */
    },
    "tool.execute.after": async () => {
      /* not yet implemented */
    },

    tool: {
      voice_reply: tool({
        description:
          "Speak a short text reply in the connected browser room via TTS.",
        args: {
          text: tool.schema.string().describe("Spoken text (1–3 sentences)."),
          room_id: tool.schema
            .string()
            .optional()
            .describe("Override the default room."),
          thought: tool.schema
            .string()
            .optional()
            .describe("Internal reasoning, shown as a collapsible bubble."),
        },
        async execute(_args) {
          // TODO(#4): send "voice:reply" event over the WebSocket.
          return "voice_reply: not yet implemented"
        },
      }),
      voice_markdown: tool({
        description: "Send markdown content to the voice chat UI (not spoken).",
        args: {
          text: tool.schema.string().describe("Markdown body."),
          room_id: tool.schema.string().optional(),
        },
        async execute(_args) {
          return "voice_markdown: not yet implemented"
        },
      }),
      voice_thought: tool({
        description:
          "Send a collapsible 'thinking' bubble to the voice chat UI (not spoken).",
        args: {
          text: tool.schema.string().describe("Reasoning text."),
          room_id: tool.schema.string().optional(),
        },
        async execute(_args) {
          return "voice_thought: not yet implemented"
        },
      }),
      voice_mermaid: tool({
        description: "Render a Mermaid diagram in the voice chat UI.",
        args: {
          code: tool.schema.string().describe("Mermaid source (no code fence)."),
          title: tool.schema.string().optional(),
          room_id: tool.schema.string().optional(),
        },
        async execute(_args) {
          return "voice_mermaid: not yet implemented"
        },
      }),
      voice_map: tool({
        description:
          "Render an interactive Leaflet map in the voice chat UI from a JSON payload.",
        args: {
          data: tool.schema
            .string()
            .describe(
              "JSON: { center: [lat, lon], zoom: number, markers: [...] }",
            ),
          room_id: tool.schema.string().optional(),
        },
        async execute(_args) {
          return "voice_map: not yet implemented"
        },
      }),
    },
  }
}

export default ChatroomVoice
