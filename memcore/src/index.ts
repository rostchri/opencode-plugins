/**
 * opencode-memcore — skeleton
 *
 * OpenCode plugin that exposes a self-hosted memcore MCP server as long-term
 * agent memory. **Placeholder only.** Real implementation will start as a fork
 * of opencode-graphiti with the MCP calls swapped to the AMP tool family
 * (amp.encode / amp.recall / amp.forget / amp.consolidate / amp.pin /
 * amp.move_memory / amp.stats) and the agent_id derived from the session.
 *
 * No infrastructure URLs or tokens are baked in — every external reference
 * comes from environment variables (see README.md).
 */
import type { Plugin } from "@opencode-ai/plugin"

function loadConfig() {
  const mcpUrl = process.env.OPENCODE_MEMCORE_MCP_URL
  if (!mcpUrl) {
    throw new Error(
      "opencode-memcore: OPENCODE_MEMCORE_MCP_URL must be set. See README.",
    )
  }
  return {
    mcpUrl,
    bearer: process.env.OPENCODE_MEMCORE_BEARER,
    agentId: process.env.OPENCODE_MEMCORE_AGENT_ID,
  }
}

export const Memcore: Plugin = async (_ctx) => {
  const cfg = loadConfig()

  // TODO(#1): on session start, inject relevant memories via amp.recall.
  // TODO(#2): hook into chat.message to detect "remember this" triggers and
  //           offer the agent an amp.encode tool call.
  // TODO(#3): hook into session compaction to save summaries via amp.encode.

  void cfg

  return {
    event: async () => {
      /* not yet implemented */
    },
    "chat.message": async () => {
      /* not yet implemented */
    },
  }
}

export default Memcore
