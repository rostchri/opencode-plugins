/**
 * opencode-chatroom-voice — event-driven voice integration plugin
 *
 * Architecture (CQRS-Hybrid):
 *
 * INBOUND (User → Agent):
 *   Phoenix-Channel WebSocket client subscribes to `room:<room_id>`.
 *   On "message" events, the user text is injected into the active opencode
 *   session via `client.session.promptAsync`.
 *
 * OUTBOUND via Event-Hooks (organic LLM token stream → Backend):
 *   - EventMessagePartUpdated with delta → POST /api/agent-stream-delta
 *   - EventMessageUpdated (assistant completed) → POST /api/agent-stream-complete
 *   - tool.execute.before / .after → POST /api/tool-status
 *
 * OUTBOUND via explicit Tools (manual voice actions):
 *   The 5 voice_* tools call the existing chatroom-controller REST routes
 *   (/api/agent-stream-complete, /api/agent-markdown, /api/agent-thought,
 *   /api/agent-mermaid, /api/agent-map). Streaming deltas are NOT sent for
 *   explicit tool invocations — the tool replaces the stream entirely with a
 *   final message. This avoids double-sending and keeps the two paths clean.
 *
 * Env vars (all resolved at init-time, never at import-time):
 *   OPENCODE_VOICE_WS_URL      wss://…/socket (Phoenix websocket endpoint)
 *   OPENCODE_VOICE_BEARER      Bearer token for chatroom-controller REST API
 *   OPENCODE_VOICE_HTTP_URL    https://… base URL for REST API
 *   OPENCODE_VOICE_ROOM        Default room_id (default: "Lobby")
 *   OPENCODE_VOICE_SESSION_ID  Optional stable session identifier override
 *   OPENCODE_VOICE_AGENT_NAME  sender field value (default: "opencode")
 *
 * Security note: Bearer token is passed as a Phoenix Socket param (query
 * string in the WebSocket upgrade request). This is the standard Phoenix
 * authentication pattern and is visible in reverse-proxy logs. A short-lived
 * token mechanism would mitigate this but requires backend changes (tracked
 * separately in issue #2329).
 */

import type { Plugin, Hooks } from '@opencode-ai/plugin';
import { tool } from '@opencode-ai/plugin';
import type { Socket, Channel } from 'phoenix';
import type {
  EventMessagePartUpdated,
  EventMessageUpdated,
  AssistantMessage,
} from '@opencode-ai/sdk';

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/** Allowed room_id format: alphanumeric, hyphen, underscore, 1-64 chars. */
const ROOM_ID_PATTERN = /^[a-zA-Z0-9_-]{1,64}$/;

/**
 * Validate a room_id. Returns an error message string if invalid, or undefined
 * if valid.
 */
function validateRoomId(room: string): string | undefined {
  if (!ROOM_ID_PATTERN.test(room)) {
    return `Invalid room_id "${room}". Must match /^[a-zA-Z0-9_-]{1,64}$/.`;
  }
  return undefined;
}

/** Max inbound text size in bytes (mirrors server.mjs 50 KB limit). */
const MAX_INBOUND_BYTES = 50_000;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/** Emitted once per process to avoid log-spam in tests. */
let bearerWarnEmitted = false;

interface Config {
  wsUrl: string;
  bearer: string;
  httpUrl: string;
  defaultRoom: string;
  sessionId: string | undefined;
  agentName: string;
}

/**
 * Load and validate env-vars. Throws early with a clear message if required
 * vars are missing so the user sees the problem at startup, not on first call.
 */
function loadConfig(): Config {
  const wsUrl = process.env['OPENCODE_VOICE_WS_URL'];
  const bearer = process.env['OPENCODE_VOICE_BEARER'];
  const httpUrl = process.env['OPENCODE_VOICE_HTTP_URL'];

  if (!wsUrl) throw new Error('opencode-chatroom-voice: OPENCODE_VOICE_WS_URL is required');
  if (!bearer) throw new Error('opencode-chatroom-voice: OPENCODE_VOICE_BEARER is required');
  if (!httpUrl) throw new Error('opencode-chatroom-voice: OPENCODE_VOICE_HTTP_URL is required');

  // Security notice: the bearer token is sent as a Phoenix Socket param which
  // becomes a query parameter in the WebSocket upgrade URL. This is the
  // standard Phoenix auth pattern but makes the token visible in proxy logs.
  // See issue #2329 for short-lived token mitigation (requires backend change).
  if (!bearerWarnEmitted) {
    bearerWarnEmitted = true;
    process.stderr.write(
      '[chatroom-voice] WARN: Bearer token is passed as a Phoenix Socket param ' +
      '(query string in WS upgrade). Token may appear in reverse-proxy access logs. ' +
      'See issue #2329 for short-lived token mitigation.\n',
    );
  }

  return {
    wsUrl,
    bearer,
    httpUrl: httpUrl.replace(/\/$/, ''),
    defaultRoom: process.env['OPENCODE_VOICE_ROOM'] ?? 'Lobby',
    sessionId: process.env['OPENCODE_VOICE_SESSION_ID'],
    agentName: process.env['OPENCODE_VOICE_AGENT_NAME'] ?? 'opencode',
  };
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

/** Timeout for all outbound HTTP calls in milliseconds. */
const POST_TIMEOUT_MS = 5_000;

/**
 * POST JSON payload to a chatroom-controller API route with a 5-second timeout.
 * Errors (network, 4xx, 5xx, timeout) are logged to stderr and swallowed — a
 * single failed HTTP call must not crash the plugin or the opencode session.
 */
async function postJson(
  bearer: string,
  url: string,
  body: Record<string, unknown>,
): Promise<void> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), POST_TIMEOUT_MS);

  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${bearer}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      process.stderr.write(
        `[chatroom-voice] POST ${url} → ${res.status}: ${text}\n`,
      );
    }
  } catch (err) {
    if (err instanceof Error && err.name === 'AbortError') {
      process.stderr.write(`[chatroom-voice] POST ${url} timed out after ${POST_TIMEOUT_MS}ms\n`);
    } else {
      process.stderr.write(`[chatroom-voice] POST ${url} failed: ${String(err)}\n`);
    }
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Fire-and-forget variant of postJson for hot-path delta events.
 * The Promise is intentionally not awaited to avoid blocking the event loop
 * when the backend is slow or unavailable. Errors are still logged via postJson.
 */
function postJsonFireAndForget(
  bearer: string,
  url: string,
  body: Record<string, unknown>,
): void {
  postJson(bearer, url, body).catch((err: unknown) => {
    // postJson already swallows errors internally; this is a safety net.
    process.stderr.write(`[chatroom-voice] Unexpected postJson throw: ${String(err)}\n`);
  });
}

// ---------------------------------------------------------------------------
// Part buffer (streaming delta accumulation)
// ---------------------------------------------------------------------------

/** Max entries in partBuffer before we warn and clear stale ones. */
const PART_BUFFER_MAX = 1_000;

/** TTL for part buffer entries in milliseconds (5 minutes). */
const PART_BUFFER_TTL_MS = 5 * 60 * 1_000;

interface PartEntry {
  kind: 'text' | 'thought';
  /** Accumulated chunks as array — joined on flush to avoid O(n^2) string concat. */
  chunks: string[];
  /** messageId this part belongs to — prevents cross-message bleed on flush. */
  messageId: string;
  /** Unix timestamp (ms) of last update — used for TTL eviction. */
  lastUpdatedAt: number;
}

/**
 * Evict stale entries from the part buffer.
 * Called lazily on each access to avoid setInterval overhead.
 * Entries older than PART_BUFFER_TTL_MS are removed.
 */
function evictStaleParts(partBuffer: Map<string, PartEntry>): void {
  const cutoff = Date.now() - PART_BUFFER_TTL_MS;
  for (const [id, entry] of partBuffer) {
    if (entry.lastUpdatedAt < cutoff) {
      partBuffer.delete(id);
    }
  }
}

// ---------------------------------------------------------------------------
// Context helper
// ---------------------------------------------------------------------------

/**
 * Resolve room_id and sessionId from tool args + plugin context.
 * Centralises the repeated resolution pattern across all 5 voice_* tools.
 */
function resolveCtx(
  args: { room_id?: string | undefined },
  context: { sessionID: string },
  cfg: Config,
  activeSessionId: string | undefined,
): { room: string; sessionId: string } {
  return {
    room: args.room_id ?? cfg.defaultRoom,
    sessionId: activeSessionId ?? context.sessionID,
  };
}

// ---------------------------------------------------------------------------
// Tool status helper
// ---------------------------------------------------------------------------

/**
 * Post a tool-status event (before/after phase) to the chatroom controller.
 * Keeps tool.execute.before and .after hooks DRY.
 */
function postToolStatus(
  cfg: Config,
  input: { callID: string; sessionID: string; tool: string },
  phase: 'before' | 'after',
  title?: string,
): void {
  const body: Record<string, unknown> = {
    room_id: cfg.defaultRoom,
    call_id: input.callID,
    sender: cfg.agentName,
    session_id: input.sessionID,
    phase,
    tool: input.tool,
  };
  if (title !== undefined) body['title'] = title;
  postJsonFireAndForget(cfg.bearer, `${cfg.httpUrl}/api/tool-status`, body);
}

// ---------------------------------------------------------------------------
// Phoenix Channel client (inbound)
// ---------------------------------------------------------------------------

/**
 * Open a Phoenix Channel WebSocket connection and return a cleanup function.
 *
 * The `phoenix` npm package (v1.x) ships as CJS/ESM. We import dynamically to
 * avoid a hard top-level dependency that would break environments where the
 * package is not installed (e.g. tests with a mock resolver).
 *
 * Message events arriving from the room are injected into the opencode session
 * via `client.session.promptAsync`. If no active session ID is known at event
 * time, the message is silently dropped (the agent is not yet ready).
 *
 * Inbound text larger than MAX_INBOUND_BYTES is rejected with a WARN log to
 * prevent oversized payloads from flooding the session queue.
 */
async function openPhoenixChannel(
  cfg: Config,
  getSessionId: () => string | undefined,
  promptAsync: (sessionId: string, text: string) => Promise<void>,
): Promise<() => void> {
  // Dynamic import so the module can be tree-shaken / mocked in tests.
  // Use the declared types from phoenix.d.ts — no inline cast needed.
  const { Socket } = await import('phoenix');

  const wsUrl = cfg.wsUrl.endsWith('/websocket')
    ? cfg.wsUrl
    : `${cfg.wsUrl}/websocket`;

  const socket: Socket = new Socket(wsUrl, {
    params: {
      token: cfg.bearer,
      identity: cfg.sessionId ?? 'opencode',
    },
  });

  socket.connect();

  const channel: Channel = socket.channel(`room:${cfg.defaultRoom}`, {});

  // Store listener reference so we can cleanly call channel.off() on teardown.
  const messageListenerRef: number = channel.on('message', (payload: unknown) => {
    const p = payload as Record<string, unknown>;
    const text = typeof p['text'] === 'string' ? p['text'] : String(p['body'] ?? '');
    if (!text.trim()) return;

    // Size guard: reject oversized inbound payloads (mirrors server.mjs 50 KB limit).
    if (Buffer.byteLength(text, 'utf8') > MAX_INBOUND_BYTES) {
      process.stderr.write(
        `[chatroom-voice] Inbound message rejected — exceeds ${MAX_INBOUND_BYTES} bytes\n`,
      );
      return;
    }

    const sid = getSessionId();
    if (!sid) {
      process.stderr.write('[chatroom-voice] Inbound message dropped — no active session\n');
      return;
    }

    promptAsync(sid, text).catch((err: unknown) => {
      process.stderr.write(`[chatroom-voice] promptAsync failed: ${String(err)}\n`);
    });
  });

  channel
    .join()
    .receive('ok', () => {
      process.stderr.write(
        `[chatroom-voice] Joined room:${cfg.defaultRoom} on ${cfg.wsUrl}\n`,
      );
    })
    .receive('error', (resp: unknown) => {
      process.stderr.write(
        `[chatroom-voice] Channel join error: ${JSON.stringify(resp)}\n`,
      );
    });

  return () => {
    // Clean up listener before leaving to prevent listener leaks on reconnect.
    channel.off('message', messageListenerRef);
    channel.leave();
    socket.disconnect();
  };
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

export const ChatroomVoice: Plugin = async (ctx): Promise<Hooks> => {
  const cfg = loadConfig();

  // Effective session ID: env override → ctx session ID from first chat message
  // The ctx does not expose a session ID directly — we capture it from
  // tool.execute.before / event hooks on first use.
  let activeSessionId: string | undefined = cfg.sessionId;

  /** Inject a user text message into the active opencode session. */
  const promptAsync = async (sessionId: string, text: string): Promise<void> => {
    await ctx.client.session.promptAsync({
      path: { id: sessionId },
      body: {
        parts: [{ type: 'text', text } as { type: 'text'; text: string }],
      },
    });
  };

  // Open the Phoenix Channel for inbound user messages.
  // Failure here is non-fatal — the outbound tools still work.
  let cleanupChannel: (() => void) | undefined;
  try {
    cleanupChannel = await openPhoenixChannel(
      cfg,
      () => activeSessionId,
      promptAsync,
    );
  } catch (err) {
    process.stderr.write(
      `[chatroom-voice] Phoenix Channel unavailable: ${String(err)}\n` +
      `[chatroom-voice] Inbound voice disabled; outbound tools still active.\n`,
    );
  }

  // Graceful shutdown: close the WebSocket when the process exits.
  // Safety-net: force-exit after 2s if cleanup hangs.
  const shutdown = () => {
    if (cleanupChannel) {
      cleanupChannel();
      cleanupChannel = undefined;
    }
    const timer = setTimeout(() => {
      process.stderr.write('[chatroom-voice] Shutdown timeout — force exit\n');
      process.exit(0);
    }, 2_000);
    // unref() so the timer does not prevent a clean exit if nothing else keeps
    // the event loop alive.
    if (typeof timer.unref === 'function') timer.unref();
  };
  process.once('SIGTERM', shutdown);
  process.once('SIGINT', shutdown);

  // ---------------------------------------------------------------------------
  // Streaming event hook (OUTBOUND — organic LLM token stream)
  // ---------------------------------------------------------------------------

  /**
   * Track the last seen part IDs and their accumulated text so we can send the
   * full_text on the "message.updated" (complete) event without keeping every
   * delta in memory indefinitely.
   *
   * Each entry is keyed by partId and includes the owning messageId to prevent
   * cross-message bleed when multiple messages stream concurrently.
   */
  const partBuffer = new Map<string, PartEntry>();

  const hooks: Hooks = {
    // -------------------------------------------------------------------------
    // Event hook
    // -------------------------------------------------------------------------
    event: async ({ event }) => {
      // -- Inbound session tracking --
      // Capture the session ID from session-status events so inbound messages
      // can be routed even if no env-override was provided.
      if (event.type === 'session.status') {
        const status = event.properties;
        if (typeof status === 'object' && status !== null) {
          const sid = (status as Record<string, unknown>)['sessionID'];
          if (typeof sid === 'string' && sid && !activeSessionId) {
            activeSessionId = sid;
          }
        }
        return;
      }

      // -- Streaming delta (token by token) --
      if (event.type === 'message.part.updated') {
        const ev = event as EventMessagePartUpdated;
        const delta = ev.properties.delta;
        if (!delta) return;

        const part = ev.properties.part;
        if (part.type !== 'text' && part.type !== 'reasoning') return;

        const kind: 'text' | 'thought' = part.type === 'reasoning' ? 'thought' : 'text';
        const sessionId = part.sessionID;
        const messageId = part.messageID;

        // Track the active session from stream events as well.
        if (sessionId && !activeSessionId) activeSessionId = sessionId;

        // Lazy TTL eviction before any buffer mutation.
        evictStaleParts(partBuffer);

        // Size-cap guard: warn and clear when buffer grows too large.
        if (partBuffer.size >= PART_BUFFER_MAX) {
          process.stderr.write(
            `[chatroom-voice] WARN: partBuffer size cap (${PART_BUFFER_MAX}) reached — clearing stale entries\n`,
          );
          partBuffer.clear();
        }

        // Accumulate in string[] chunks (avoids O(n^2) string concat).
        const existing = partBuffer.get(part.id);
        if (existing) {
          existing.chunks.push(delta);
          existing.lastUpdatedAt = Date.now();
        } else {
          partBuffer.set(part.id, {
            kind,
            chunks: [delta],
            messageId,
            lastUpdatedAt: Date.now(),
          });
        }

        // Fire-and-forget: do NOT await — keeps delta path non-blocking.
        postJsonFireAndForget(cfg.bearer, `${cfg.httpUrl}/api/agent-stream-delta`, {
          room_id: cfg.defaultRoom,
          message_id: messageId,
          sender: cfg.agentName,
          session_id: sessionId,
          kind,
          delta,
        });
        return;
      }

      // -- Message completed --
      if (event.type === 'message.updated') {
        const ev = event as EventMessageUpdated;
        const msg = ev.properties.info;

        // Only react to completed assistant messages.
        if (msg.role !== 'assistant') return;
        const assistant = msg as AssistantMessage;
        if (!assistant.time.completed) return;

        const sessionId = assistant.sessionID;
        if (sessionId && !activeSessionId) activeSessionId = sessionId;

        // Flush all buffered parts for THIS message only (messageId-gated).
        // Parts belonging to other messages (e.g. concurrent streams) are kept.
        const flushPromises: Promise<void>[] = [];
        for (const [partId, buf] of partBuffer) {
          if (buf.messageId !== assistant.id) continue;

          flushPromises.push(
            postJson(cfg.bearer, `${cfg.httpUrl}/api/agent-stream-complete`, {
              room_id: cfg.defaultRoom,
              message_id: assistant.id,
              sender: cfg.agentName,
              session_id: sessionId,
              kind: buf.kind,
              full_text: buf.chunks.join(''),
              trigger_tts: buf.kind === 'text',
            }),
          );
          partBuffer.delete(partId);
        }

        // Flush all matching parts in parallel (independent HTTP calls).
        await Promise.all(flushPromises);
        return;
      }
    },

    // -------------------------------------------------------------------------
    // Tool status hooks (OUTBOUND — "working…" badge)
    // Fire-and-forget: tool hooks must not block the tool execution pipeline.
    // -------------------------------------------------------------------------
    'tool.execute.before': async (input) => {
      if (!activeSessionId) activeSessionId = input.sessionID;
      postToolStatus(cfg, input, 'before');
    },

    'tool.execute.after': async (input, output) => {
      if (!activeSessionId) activeSessionId = input.sessionID;
      postToolStatus(cfg, input, 'after', output.title);
    },

    // -------------------------------------------------------------------------
    // Explicit voice tools (OUTBOUND — manual triggers)
    //
    // These tools use the existing chatroom-controller REST API directly and do
    // NOT emit stream-delta events. The two outbound paths (streaming via
    // event-hook vs. explicit tool call) are intentionally kept separate to
    // avoid double-sending content to the browser.
    // -------------------------------------------------------------------------
    tool: {
      /**
       * Speak a text reply in the connected browser room via TTS.
       * Optionally prefix with an internal thought bubble (not spoken).
       */
      voice_reply: tool({
        description:
          'Speak a short text reply in the connected browser room via TTS. ' +
          'Triggers text-to-speech on the receiving client. ' +
          'Use for concise spoken answers (1–3 sentences). ' +
          'Optionally include an internal thought shown as a collapsible bubble.',
        args: {
          text: tool.schema.string().describe('Spoken text (1–3 sentences).'),
          room_id: tool.schema
            .string()
            .optional()
            .describe('Override the default room (uses OPENCODE_VOICE_ROOM if omitted).'),
          thought: tool.schema
            .string()
            .optional()
            .describe('Internal reasoning shown as a collapsible bubble (not spoken).'),
        },
        async execute(args, context) {
          const { room, sessionId } = resolveCtx(args, context, cfg, activeSessionId);

          const roomErr = validateRoomId(room);
          if (roomErr) return { title: 'voice_reply error', output: roomErr };

          if (args.thought) {
            await postJson(cfg.bearer, `${cfg.httpUrl}/api/agent-stream-complete`, {
              room_id: room,
              message_id: context.messageID,
              sender: cfg.agentName,
              session_id: sessionId,
              kind: 'thought',
              full_text: args.thought,
              trigger_tts: false,
            });
          }

          await postJson(cfg.bearer, `${cfg.httpUrl}/api/agent-stream-complete`, {
            room_id: room,
            message_id: context.messageID,
            sender: cfg.agentName,
            session_id: sessionId,
            kind: 'text',
            full_text: args.text,
            trigger_tts: true,
          });

          return {
            title: 'voice_reply sent',
            output: `Sent to room "${room}": ${args.text.slice(0, 60)}${args.text.length > 60 ? '…' : ''}`,
          };
        },
      }),

      /**
       * Send markdown-formatted content to the voice chat UI.
       * Not spoken — displayed as a formatted bubble in the browser.
       *
       * NOTE: This calls /api/agent-markdown (dedicated route), NOT the stream
       * endpoints. Markdown has its own channel event in the backend and does not
       * go through the token-stream path.
       */
      voice_markdown: tool({
        description:
          'Send markdown-formatted content to the voice chat UI (not spoken). ' +
          'Use for code blocks, tables, lists, and structured reports.',
        args: {
          text: tool.schema.string().describe('Markdown body.'),
          room_id: tool.schema
            .string()
            .optional()
            .describe('Override the default room.'),
        },
        async execute(args, context) {
          const { room, sessionId } = resolveCtx(args, context, cfg, activeSessionId);

          const roomErr = validateRoomId(room);
          if (roomErr) return { title: 'voice_markdown error', output: roomErr };

          await postJson(cfg.bearer, `${cfg.httpUrl}/api/agent-markdown`, {
            room_id: room,
            message_id: context.messageID,
            sender: cfg.agentName,
            session_id: sessionId,
            text: args.text,
          });

          return {
            title: 'voice_markdown sent',
            output: `Markdown sent to room "${room}" (${args.text.length} chars)`,
          };
        },
      }),

      /**
       * Send a collapsible "thinking" bubble to the voice chat UI.
       * Not spoken — shown as an expandable reasoning section.
       */
      voice_thought: tool({
        description:
          'Send a collapsible thinking bubble to the voice chat UI (not spoken). ' +
          'Use to expose internal reasoning without cluttering the main response.',
        args: {
          text: tool.schema.string().describe('Reasoning text.'),
          room_id: tool.schema
            .string()
            .optional()
            .describe('Override the default room.'),
        },
        async execute(args, context) {
          const { room, sessionId } = resolveCtx(args, context, cfg, activeSessionId);

          const roomErr = validateRoomId(room);
          if (roomErr) return { title: 'voice_thought error', output: roomErr };

          await postJson(cfg.bearer, `${cfg.httpUrl}/api/agent-thought`, {
            room_id: room,
            message_id: context.messageID,
            sender: cfg.agentName,
            session_id: sessionId,
            text: args.text,
          });

          return {
            title: 'voice_thought sent',
            output: `Thought sent to room "${room}"`,
          };
        },
      }),

      /**
       * Render a Mermaid diagram in the voice chat UI.
       */
      voice_mermaid: tool({
        description:
          'Render a Mermaid diagram in the voice chat UI. ' +
          'Pass raw Mermaid source without code fences.',
        args: {
          code: tool.schema.string().describe('Mermaid source (no code fence).'),
          title: tool.schema.string().optional().describe('Optional diagram title.'),
          room_id: tool.schema
            .string()
            .optional()
            .describe('Override the default room.'),
        },
        async execute(args, context) {
          const { room, sessionId } = resolveCtx(args, context, cfg, activeSessionId);

          const roomErr = validateRoomId(room);
          if (roomErr) return { title: 'voice_mermaid error', output: roomErr };

          await postJson(cfg.bearer, `${cfg.httpUrl}/api/agent-mermaid`, {
            room_id: room,
            message_id: context.messageID,
            sender: cfg.agentName,
            session_id: sessionId,
            code: args.code,
            title: args.title,
          });

          return {
            title: 'voice_mermaid sent',
            output: `Mermaid diagram sent to room "${room}"`,
          };
        },
      }),

      /**
       * Render an interactive Leaflet map in the voice chat UI.
       */
      voice_map: tool({
        description:
          'Render an interactive Leaflet map in the voice chat UI from a JSON payload. ' +
          'Pass JSON string: { center: [lat, lon], zoom: number, markers: [{lat, lon, label}] }',
        args: {
          data: tool.schema
            .string()
            .describe('JSON: { center: [lat, lon], zoom: number, markers: [...] }'),
          room_id: tool.schema
            .string()
            .optional()
            .describe('Override the default room.'),
        },
        async execute(args, context) {
          const { room, sessionId } = resolveCtx(args, context, cfg, activeSessionId);

          const roomErr = validateRoomId(room);
          if (roomErr) return { title: 'voice_map error', output: roomErr };

          // Validate JSON structure before sending to avoid silently corrupt
          // payloads reaching the frontend.
          let parsed: unknown;
          try {
            parsed = JSON.parse(args.data);
          } catch {
            return {
              title: 'voice_map error',
              output: 'Invalid JSON in data parameter. Expected { center, zoom, markers }',
            };
          }

          await postJson(cfg.bearer, `${cfg.httpUrl}/api/agent-map`, {
            room_id: room,
            message_id: context.messageID,
            sender: cfg.agentName,
            session_id: sessionId,
            data: parsed,
          });

          return {
            title: 'voice_map sent',
            output: `Map sent to room "${room}"`,
          };
        },
      }),
    },
  };

  return hooks;
};

export default ChatroomVoice;

// Compatibility export for opencode plugin loader which expects a named `server` export.
export const server = ChatroomVoice;
