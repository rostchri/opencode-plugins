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

/** Safe accessor for event.properties as an indexed record. */
function ev_props(event: { properties?: unknown }): Record<string, unknown> {
  const p = event.properties;
  return typeof p === 'object' && p !== null ? (p as Record<string, unknown>) : {};
}

/**
 * System-prompt block appended to opencode's built-in mode prompt via the
 * `experimental.chat.system.transform` hook. Kept short on purpose — the
 * normal terminal output stream (text + reasoning) is already forwarded
 * automatically by our event-hook, so we don't need to advertise tools like
 * `voice_reply` / `voice_thought` / `voice_markdown`.
 */
const VOICE_MODE_SYSTEM_PROMPT = `You are connected to a browser voice chat via the chatroom-voice plugin.
User messages arrive as plain text (after STT from microphone).

Your normal terminal output streams live to the browser — text parts go
through TTS automatically, reasoning shows as collapsible bubble. Just
reply naturally, no special tool needed for normal answers.

For rich content use:
- \`voice_mermaid\` — diagrams
- \`voice_map\` — interactive maps

Reply in the user's language (German default). Use real umlauts (ä/ö/ü/ß)
— TTS mispronounces ae/oe/ue.`;

/**
 * Periodic heartbeat interval (ms) to keep the backend's session-presence
 * record fresh. Room changes are picked up event-driven via the
 * `agent_moved` channel event (no polling needed), so this can be a slow
 * liveness ping rather than a tight room-sync loop.
 */
const HEARTBEAT_INTERVAL_MS = 30_000;

/**
 * Stable, deterministic session id for this plugin instance. Used as the
 * `session_id` on /api/heartbeat so the backend can map our agent to a
 * persistent room across restarts.
 *
 * Priority: env override → hash(agentName + wsUrl + hostname).
 */
function deriveAgentSessionId(cfg: Config): string {
  if (cfg.sessionId && cfg.sessionId.trim()) return cfg.sessionId.trim();
  const hostname = process.env['HOSTNAME'] ?? 'unknown';
  const seed = `${cfg.agentName}|${cfg.wsUrl}|${hostname}`;
  // Lightweight stable hash (no crypto dep). 32-bit FNV-1a → hex.
  let h = 0x811c9dc5;
  for (let i = 0; i < seed.length; i++) {
    h ^= seed.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return `${cfg.agentName}-${h.toString(16).padStart(8, '0')}`;
}

/**
 * Ask the backend which room this agent belongs in. Backend response is the
 * authoritative source — never trust env-vars or persisted state on disk for
 * room assignment. Backend resolves: SessionServer > RoomPersistence > Default.
 */
async function resolveRoomViaHeartbeat(
  cfg: Config,
  sessionId: string,
  model: string | undefined,
): Promise<string | undefined> {
  try {
    // `Connection: close` defeats undici's keep-alive pool — long-idle
    // connections often get closed server-side, but the pool keeps handing
    // them out until the next request fails with `socket connection closed
    // unexpectedly`. Forcing a fresh TCP connection per heartbeat costs us
    // a few ms but eliminates the false-offline cycle.
    const res = await fetch(`${cfg.httpUrl}/api/heartbeat`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${cfg.bearer}`,
        'Connection': 'close',
      },
      body: JSON.stringify({
        session_id: sessionId,
        name: cfg.agentName,
        type: 'agent',
        agent_identity: sessionId,
        agent_type: 'opencode',
        ...(model ? { model } : {}),
      }),
    });
    if (!res.ok) {
      debugLog(
        `[chatroom-voice] heartbeat HTTP ${res.status} — falling back to env room\n`,
      );
      return undefined;
    }
    const body = (await res.json()) as {
      room_id?: string;
      observer?: {
        mode?: string;
        room_id?: string;
        display_char_name?: string;
        system_prompt?: string;
      };
    };
    // 2026-05-29: Cache observer-state aus heartbeat-response — wird im
    // chat.system.transform-Hook abgefragt um den User-spezifischen
    // Beobachter-System-Prompt ans LLM anzuhaengen.
    if (body.observer && body.observer.mode === 'observer_in_room') {
      observerStateCache = {
        roomId: typeof body.observer.room_id === 'string' ? body.observer.room_id : '',
        displayCharName:
          typeof body.observer.display_char_name === 'string' ? body.observer.display_char_name : '',
        systemPrompt:
          typeof body.observer.system_prompt === 'string' ? body.observer.system_prompt : '',
      };
    } else {
      observerStateCache = null;
    }
    return typeof body.room_id === 'string' && body.room_id ? body.room_id : undefined;
  } catch (err) {
    debugLog(
      `[chatroom-voice] heartbeat failed: ${String(err)} — falling back to env room\n`,
    );
    return undefined;
  }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/** Emitted once per process to avoid log-spam in tests. */
let bearerWarnEmitted = false;

/**
 * Observer-Mode-Cache: vom Heartbeat-Response gefuellt (Backend leitet den
 * vom Browser-User per setup_observer Modal eingegebenen system_prompt an
 * den Agenten weiter). Wird im chat.system.transform-Hook abgefragt damit
 * der LLM diese Anweisung als zusaetzlichen System-Prompt-Block bekommt.
 */
let observerStateCache: {
  roomId: string;
  displayCharName: string;
  systemPrompt: string;
} | null = null;

/**
 * Append a line to ~/.opencode-chatroom-voice.log. opencode's TUI swallows
 * `process.stderr.write` from plugins, so we route diagnostic logs to a
 * dedicated file the user can `tail -f` from another terminal.
 */
function debugLog(line: string): void {
  try {
    const homedir = process.env['HOME'] ?? '/tmp';
    const path = `${homedir}/.opencode-chatroom-voice.log`;
    const ts = new Date().toISOString();
    void import('node:fs').then((fs) => {
      try {
        fs.appendFileSync(path, `[${ts}] ${line}\n`);
      } catch {
        /* swallowed */
      }
    });
  } catch {
    /* swallowed */
  }
}

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
    debugLog(
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
    // `Connection: close` (see heartbeat) — avoids undici keep-alive stale
    // sockets on hot-path POSTs after the controller has restarted.
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${bearer}`,
        'Content-Type': 'application/json',
        'Connection': 'close',
      },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      debugLog(
        `[chatroom-voice] POST ${url} → ${res.status}: ${text}\n`,
      );
    }
  } catch (err) {
    if (err instanceof Error && err.name === 'AbortError') {
      debugLog(`[chatroom-voice] POST ${url} timed out after ${POST_TIMEOUT_MS}ms\n`);
    } else {
      debugLog(`[chatroom-voice] POST ${url} failed: ${String(err)}\n`);
    }
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Per-URL serialized POST queue (#2026-05-29 ordering fix).
 *
 * Background: opencode emits text-deltas at sub-ms intervals. `fetch` with
 * `Connection: close` spawns a fresh TCP socket per POST — so multiple
 * concurrent POSTs race against each other and can arrive at the backend
 * out-of-order (observed: token "OpenCode" reached backend 3ms before
 * " bin **" although the LLM emitted them in the reverse order).
 *
 * Phoenix.Channel broadcasts in receive-order, so out-of-order HTTP arrival
 * produces visually-glitching deltas in the browser AND — more critically —
 * a corrupted text that the TTS-paragraph-segmenter then synthesizes
 * before the final snapshot corrects the bubble.
 *
 * Fix: chain POSTs per target URL with `.then` so each request only fires
 * AFTER the previous one resolved. Errors do not break the chain (the
 * `.catch` continues the next request unconditionally).
 */
const postChains = new Map<string, Promise<unknown>>();
function postJsonFireAndForget(
  bearer: string,
  url: string,
  body: Record<string, unknown>,
): void {
  const prev = postChains.get(url) ?? Promise.resolve();
  const next = prev
    .catch(() => {}) // never break the chain on previous failure
    .then(() => postJson(bearer, url, body))
    .catch((err: unknown) => {
      debugLog(`[chatroom-voice] Unexpected postJson throw: ${String(err)}\n`);
    });
  postChains.set(url, next);
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
 * time, the message is dropped with a clear log line.
 *
 * `resolveSessionId` defaults to `async () => getSessionId()` — i.e. it just
 * reads the module-level `activeSessionId` variable. Callers SHOULD NOT pass a
 * fallback that calls `session.list()` because list() returns ALL historical
 * sessions sorted by creation time, not the session the TUI is currently
 * displaying. The correct seeding strategy is `seedSessionFromStatusApi()` at
 * init time + lifecycle-event hooks (session.created / session.status).
 *
 * Inbound text larger than MAX_INBOUND_BYTES is rejected with a WARN log to
 * prevent oversized payloads from flooding the session queue.
 */
async function openPhoenixChannel(
  cfg: Config,
  agentSessionId: string,
  initialRoom: string,
  getSessionId: () => string | undefined,
  getActiveModel: () => string | undefined,
  promptAsync: (sessionId: string, text: string) => Promise<void>,
  resolveSessionId: () => Promise<string | undefined> = async () => getSessionId(),
): Promise<() => void> {
  // Dynamic import so the module can be tree-shaken / mocked in tests.
  // Use the declared types from phoenix.d.ts — no inline cast needed.
  const { Socket } = await import('phoenix');

  // Phoenix Socket appends "/websocket" (or "/longpoll") to the base URL
  // itself. If the env-var already contains it, strip it so we don't end up
  // with "/socket/websocket/websocket" → 404.
  const wsUrl = cfg.wsUrl.endsWith('/websocket')
    ? cfg.wsUrl.slice(0, -'/websocket'.length)
    : cfg.wsUrl;

  // Node < 22 has no global WebSocket; the phoenix client otherwise silently
  // falls back to LongPoll which our backend does not serve (404 "not found"
  // parse errors). Load the `ws` polyfill and pass it as the `transport`
  // option so Socket() always uses a real WebSocket.
  let transport: unknown = (globalThis as { WebSocket?: unknown }).WebSocket;
  if (!transport) {
    try {
      const wsMod = (await import('ws')) as { default?: unknown };
      transport = wsMod.default ?? wsMod;
    } catch {
      // Fall back to whatever phoenix picks (LongPoll). The user will see the
      // "failed to parse JSON response" loop in stderr and can install `ws`.
    }
  }

  const socket: Socket = new Socket(wsUrl, {
    params: {
      token: cfg.bearer,
      identity: agentSessionId,
    },
    ...(transport ? { transport } : {}),
  } as ConstructorParameters<typeof Socket>[1]);

  socket.connect();

  // State for the single active room subscription. Single-room is the agent
  // contract (vs browser/user which can multi-room). Room changes are picked
  // up event-driven via `agent_moved` from the backend AgentMover.
  let currentRoom = initialRoom;
  let leaveCurrent: () => void;

  /** Subscribe to a single room. Returns a leave-fn that detaches listeners. */
  const subscribe = (roomId: string): (() => void) => {
    const channel: Channel = socket.channel(`room:${roomId}`, {});

    const messageListenerRef: number = channel.on('message', (payload: unknown) => {
      const p = payload as Record<string, unknown>;
      const text = typeof p['text'] === 'string' ? p['text'] : String(p['body'] ?? '');
      if (!text.trim()) return;

      if (Buffer.byteLength(text, 'utf8') > MAX_INBOUND_BYTES) {
        debugLog(
          `[chatroom-voice] Inbound message rejected — exceeds ${MAX_INBOUND_BYTES} bytes\n`,
        );
        return;
      }

      // Route inbound message to the active session. activeSessionId is seeded
      // at init via seedSessionFromStatusApi() + session.created/status events.
      // If it is still undefined here, opencode has no active session — drop
      // the message and log clearly so the user can diagnose the issue.
      void resolveSessionId().then((sid) => {
        if (!sid) {
          debugLog('[chatroom-voice] Inbound message dropped — no active opencode session\n');
          return;
        }
        promptAsync(sid, text).catch((err: unknown) => {
          debugLog(`[chatroom-voice] promptAsync failed: ${String(err)}\n`);
        });
      });
    });

    // 2026-05-29: opencode-Agent soll in Roleplay-Raeumen auch die Beitraege
    // anderer Agents (Roleplay-Charakter) lesen koennen — analog dem
    // 'message'-Listener oben aber fuer agent_message-broadcasts. WICHTIG:
    // eigene Beitraege filtern (sonst Feedback-Schleife).
    const agentMsgListenerRef: number = channel.on('agent_message', (payload: unknown) => {
      const p = payload as Record<string, unknown>;
      const senderId = typeof p['session_id'] === 'string' ? p['session_id'] : '';
      if (senderId === agentSessionId) return; // unser eigener Beitrag — ignorieren
      const senderName =
        typeof p['sender'] === 'string' && p['sender']
          ? p['sender']
          : senderId || 'Agent';
      const text = typeof p['text'] === 'string' ? p['text'] : String(p['content'] ?? '');
      if (!text.trim()) return;
      if (Buffer.byteLength(text, 'utf8') > MAX_INBOUND_BYTES) return;
      // Mit Sender-Praefix injizieren damit das LLM weiss von wem die Nachricht kommt.
      const prefixed = `[${senderName}]: ${text}`;
      void resolveSessionId().then((sid) => {
        if (!sid) return;
        promptAsync(sid, prefixed).catch((err: unknown) => {
          debugLog(`[chatroom-voice] promptAsync (agent_message) failed: ${String(err)}\n`);
        });
      });
    });

    // 2026-05-29 (Bug-Fix Roleplay-Observer): In Roleplay-Raeumen werden
    // KI-Char-Beitraege NICHT als 'agent_message' sondern als 'bubble_added'
    // (Observer-Pfad) oder 'message:new' / 'message:stream_end' (Bridge-Pfad)
    // an den Phoenix-Channel gepusht. Wir hoeren auf alle drei und reichen
    // jeden fremden Beitrag ans opencode-LLM weiter — eigene werden ueber
    // session_id-Vergleich gefiltert.
    function dispatchInbound(senderId: string, senderName: string, text: string): void {
      if (!text.trim()) return;
      if (senderId && senderId === agentSessionId) return;
      if (Buffer.byteLength(text, 'utf8') > MAX_INBOUND_BYTES) return;
      const prefixed = senderName ? `[${senderName}]: ${text}` : text;
      void resolveSessionId().then((sid) => {
        if (!sid) return;
        promptAsync(sid, prefixed).catch((err: unknown) => {
          debugLog(`[chatroom-voice] promptAsync (roleplay) failed: ${String(err)}\n`);
        });
      });
    }

    const bubbleAddedRef: number = channel.on('bubble_added', (payload: unknown) => {
      const p = payload as Record<string, unknown>;
      const sid =
        typeof p['session_id'] === 'string'
          ? p['session_id']
          : typeof p['agent_id'] === 'string'
            ? p['agent_id']
            : '';
      const name =
        (typeof p['char_name'] === 'string' && p['char_name']) ||
        (typeof p['sender'] === 'string' && p['sender']) ||
        sid ||
        'Char';
      const text =
        typeof p['content'] === 'string'
          ? p['content']
          : typeof p['text'] === 'string'
            ? p['text']
            : '';
      dispatchInbound(sid, name, text);
    });

    const messageNewRef: number = channel.on('message:new', (payload: unknown) => {
      const p = payload as Record<string, unknown>;
      const sid = typeof p['session_id'] === 'string' ? p['session_id'] : '';
      const name =
        (typeof p['char_name'] === 'string' && p['char_name']) ||
        (typeof p['sender'] === 'string' && p['sender']) ||
        'Char';
      const text = typeof p['text'] === 'string' ? p['text'] : '';
      dispatchInbound(sid, name, text);
    });

    const messageStreamEndRef: number = channel.on('message:stream_end', (payload: unknown) => {
      const p = payload as Record<string, unknown>;
      const sid = typeof p['session_id'] === 'string' ? p['session_id'] : '';
      const name =
        (typeof p['char_name'] === 'string' && p['char_name']) ||
        (typeof p['sender'] === 'string' && p['sender']) ||
        'Char';
      const text = typeof p['full_text'] === 'string' ? p['full_text'] : '';
      dispatchInbound(sid, name, text);
    });

    // Event-driven room reassignment: backend AgentMover.move/3 broadcasts
    // `agent_moved` on both old and new room. We filter by our own
    // agent_session_id and switch channels in-process.
    const movedListenerRef: number = channel.on('agent_moved', (payload: unknown) => {
      const p = payload as Record<string, unknown>;
      if (p['session_id'] !== agentSessionId) return; // not us
      const target = typeof p['to_room'] === 'string' ? p['to_room'] : undefined;
      if (!target || target === currentRoom) return;
      debugLog(
        `[chatroom-voice] agent_moved: ${currentRoom} → ${target}\n`,
      );
      try { leaveCurrent(); } catch { /* noop */ }
      currentRoom = target;
      leaveCurrent = subscribe(currentRoom);
    });

    channel
      .join()
      .receive('ok', () => {
        debugLog(
          `[chatroom-voice] Joined room:${roomId} as agent_session=${agentSessionId}\n`,
        );
      })
      .receive('error', (resp: unknown) => {
        debugLog(
          `[chatroom-voice] Channel join error (room:${roomId}): ${JSON.stringify(resp)}\n`,
        );
      });

    return () => {
      channel.off('message', messageListenerRef);
      channel.off('agent_message', agentMsgListenerRef);
      channel.off('bubble_added', bubbleAddedRef);
      channel.off('message:new', messageNewRef);
      channel.off('message:stream_end', messageStreamEndRef);
      channel.off('agent_moved', movedListenerRef);
      channel.leave();
    };
  };

  leaveCurrent = subscribe(currentRoom);

  // Liveness heartbeat: keeps backend's session_presence fresh and updates
  // `model` metadata. Backend has a ~60s presence timeout; we ping every 30s
  // to give one safety margin if a single heartbeat is in flight. We do NOT
  // `.unref()` the timer because opencode's plugin sandbox observably stops
  // firing unref'd timers, which caused false-offline cycles (see Task 5j).
  const heartbeatTimer = setInterval(() => {
    void resolveRoomViaHeartbeat(cfg, agentSessionId, getActiveModel());
  }, HEARTBEAT_INTERVAL_MS);

  return () => {
    clearInterval(heartbeatTimer);
    try { leaveCurrent(); } catch { /* noop */ }
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
  /**
   * Monotonic accumulator of full text per bubble (text + thought separately).
   * Keyed by opencode assistant-message id (= bubble id) so parallel
   * subagent-streams stay separated. Cleared on each `chat.message` (new turn).
   */
  const bubbleAcc = new Map<string, { text: string; thought: string }>();
  /**
   * partID → part.type cache learned from `message.part.updated` snapshots
   * (which carry the full part-shape) so we can correctly classify later
   * `message.part.delta` events (which carry only field+delta) as text vs
   * reasoning even when the model uses a non-standard field name.
   */
  const partTypeCache = new Map<string, string>();
  /**
   * Per-partID deferred queue for deltas that arrived BEFORE the first
   * .updated snapshot revealed part.type. Prevents misclassifying early
   * reasoning-tokens as text when delta-events race ahead of snapshots.
   * Auto-flushes as 'text' after PENDING_FLUSH_MS if no snapshot arrives.
   */
  type PendingDelta = { sessionId: string; messageId: string; partId: string; delta: string };
  const pendingPartDeltas = new Map<
    string,
    { queue: PendingDelta[]; flushTimer: ReturnType<typeof setTimeout> }
  >();
  const PENDING_FLUSH_MS = 250;
  function sendStreamDelta(p: PendingDelta, kind: 'text' | 'thought'): void {
    const bubbleId = p.messageId;
    const acc = bubbleAccGet(bubbleId);
    if (kind === 'thought') acc.thought += p.delta;
    else acc.text += p.delta;
    if (process.env['OPENCODE_VOICE_DEBUG']) {
      debugLog(`  POST agent-stream-delta bubble=${bubbleId} (msg=${p.messageId}) kind=${kind} delta="${p.delta.slice(0, 30)}"`);
    }
    postJsonFireAndForget(cfg.bearer, `${cfg.httpUrl}/api/agent-stream-delta`, {
      message_id: bubbleId,
      sender: cfg.agentName,
      session_id: agentSessionId,
      kind,
      delta: p.delta,
      // Bugfix 2026-05-28: include current model inline so the backend does
      // not have to rely on the heartbeat-set SessionServer.get_model
      // lookup (which has race-conditions: first deltas can arrive BEFORE
      // the eager-heartbeat round-trip after a chat.message hook fires).
      ...(activeModel ? { model: activeModel } : {}),
    });
  }
  function flushPendingPart(partId: string, kind: 'text' | 'thought'): void {
    const pending = pendingPartDeltas.get(partId);
    if (!pending) return;
    clearTimeout(pending.flushTimer);
    for (const item of pending.queue) sendStreamDelta(item, kind);
    pendingPartDeltas.delete(partId);
  }
  function bubbleAccGet(id: string): { text: string; thought: string } {
    let acc = bubbleAcc.get(id);
    if (!acc) {
      acc = { text: '', thought: '' };
      bubbleAcc.set(id, acc);
    }
    return acc;
  }

  // Track the currently active LLM model so we can report it to the backend
  // via /api/heartbeat. opencode emits the model on every chat.message call;
  // we update on change and send an eager heartbeat immediately so the backend
  // picks up the new model without waiting for the 60-second periodic tick.
  let activeModel: string | undefined;
  const getActiveModel = (): string | undefined => activeModel;

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
  // Agent room is backend-authoritative: ask /api/heartbeat first, fall back
  // to cfg.defaultRoom only if the backend is unreachable. Single-room is the
  // agent contract (vs. browser/user which can be in many rooms).
  // Failure here is non-fatal — the outbound tools still work.
  const agentSessionId = deriveAgentSessionId(cfg);
  const resolvedRoom =
    (await resolveRoomViaHeartbeat(cfg, agentSessionId, activeModel)) ?? cfg.defaultRoom;
  let cleanupChannel: (() => void) | undefined;

  /**
   * Seed activeSessionId from the opencode session-status API at plugin init.
   *
   * Strategy: call GET /session/status which returns { [sessionID]: SessionStatus }.
   * The TUI always has exactly the session it displays registered there. We pick
   * the first (and usually only) session ID returned — it is the TUI's session.
   * If multiple sessions are present we prefer a "busy" or "idle" one over any
   * others (both indicate the TUI is actively using that session).
   *
   * This is the authoritative seed mechanism. The old session.list()-newest
   * fallback was REMOVED because list() returns ALL sessions (including archived
   * ones from previous opencode runs) sorted by creation time, NOT by which
   * session the TUI is currently displaying. Picking "newest" from list() would
   * target the wrong session when the user had multiple sessions.
   *
   * The session.status API, by contrast, only returns sessions the TUI has
   * currently open/active — exactly the right set.
   */
  const seedSessionFromStatusApi = async (): Promise<void> => {
    if (activeSessionId) return; // already seeded (e.g. by env override)
    try {
      const res = await ctx.client.session.status();
      // response shape: { data?: { [sessionID]: SessionStatus } }
      const map = (res as { data?: Record<string, { type?: string }> }).data;
      if (map && typeof map === 'object') {
        const entries = Object.entries(map);
        if (entries.length === 0) return;
        // Prefer a "busy" session (TUI is actively processing a turn) or
        // "idle" (TUI is waiting for user input). Either means the TUI is
        // showing that session. If only one session, just use it regardless.
        const preferred =
          entries.find(([, s]) => s?.type === 'busy') ??
          entries.find(([, s]) => s?.type === 'idle') ??
          entries[0];
        if (preferred) {
          activeSessionId = preferred[0];
          debugLog(`[chatroom-voice] session seeded from status API → ${activeSessionId}\n`);
        }
      }
    } catch (err) {
      // Non-fatal: if the API call fails we will still seed from the first
      // event hook that carries a sessionID (session.status, chat.message,
      // tool.execute.before, message.updated).
      debugLog(`[chatroom-voice] session.status seed failed: ${String(err)}\n`);
    }
  };

  // Fire immediately — we want activeSessionId populated before the first
  // inbound chatroom message can arrive via the Phoenix channel.
  await seedSessionFromStatusApi();

  try {
    cleanupChannel = await openPhoenixChannel(
      cfg,
      agentSessionId,
      resolvedRoom,
      () => activeSessionId,
      getActiveModel,
      promptAsync,
      // No resolveSessionId fallback argument: pass the simple getter so
      // openPhoenixChannel's internal default `async () => getSessionId()` is
      // used. This means: use activeSessionId as-is, do NOT fall back to
      // session.list(). If activeSessionId is still undefined here, the message
      // will be dropped with a clear log line — which is the correct behavior
      // (it means opencode has no active session at all, not that we targeted
      // the wrong one).
    );
  } catch (err) {
    debugLog(
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
      debugLog('[chatroom-voice] Shutdown timeout — force exit\n');
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
    // chat.message hook — model tracking + eager heartbeat on model change
    //
    // opencode calls this hook for every chat turn. The `input.model` field
    // carries { providerID, modelID } for the model that will handle the turn.
    // We derive a canonical "<providerID>/<modelID>" string, update `activeModel`
    // when it changes, and fire an eager heartbeat immediately so the backend
    // receives the new model without waiting for the 60 s periodic tick.
    //
    // `input.sessionID` is captured as a fallback for `activeSessionId` so the
    // inbound message path works even before the first event hook fires.
    // -------------------------------------------------------------------------
    'chat.message': async (input) => {
      // Capture sessionID if not yet known.
      const sid = (input as unknown as Record<string, unknown>)['sessionID'];
      if (typeof sid === 'string' && sid && !activeSessionId) {
        activeSessionId = sid;
      }

      // Bugfix 2026-05-28b: clear per-bubble cumulative text on each new turn.
      // Per-bubble bubbles (one per assistant-message id) avoid the parallel
      // subagent-stream interleaving issue from 2026-05-28a.
      bubbleAcc.clear();

      // Derive canonical model string from input.model (providerID + modelID).
      const rawModel = (input as unknown as Record<string, unknown>)['model'] as
        | { providerID?: string; modelID?: string }
        | undefined;
      const newModel =
        rawModel && rawModel.providerID && rawModel.modelID
          ? `${rawModel.providerID}/${rawModel.modelID}`
          : undefined;

      if (newModel && newModel !== activeModel) {
        activeModel = newModel;
        debugLog(`[chatroom-voice] model changed → ${activeModel} — eager heartbeat\n`);
        // Eager heartbeat: fire-and-forget so we don't block the turn.
        // The return value (room_id) is intentionally discarded here — room
        // assignments are handled by agent_moved events.
        void resolveRoomViaHeartbeat(cfg, agentSessionId, activeModel);
      }
    },

    // -------------------------------------------------------------------------
    // Event hook
    // -------------------------------------------------------------------------
    event: async ({ event }) => {
      // DEBUG 2026-05-28: log every event type AND property keys/sample-values
      // so we can map what opencode actually emits (reasoning vs text vs task
      // vs tool vs subagent) to our content-type taxonomy.
      if (process.env['OPENCODE_VOICE_DEBUG']) {
        try {
          const props = ev_props(event);
          const keys = Object.keys(props);
          const sample: Record<string, string> = {};
          for (const k of keys) {
            const v = (props as Record<string, unknown>)[k];
            if (typeof v === 'string') sample[k] = v.length > 80 ? v.slice(0, 80) + '…' : v;
            else if (typeof v === 'number' || typeof v === 'boolean') sample[k] = String(v);
            else if (v && typeof v === 'object') sample[k] = `{${Object.keys(v as object).slice(0, 6).join(',')}}`;
            else sample[k] = typeof v;
          }
          debugLog(`event type=${event.type} keys=${keys.join(',')} sample=${JSON.stringify(sample)}`);
        } catch {
          debugLog(`event type=${event.type}`);
        }
      }
      // -- Inbound session tracking --
      // Capture the session ID from session-lifecycle events so inbound messages
      // can be routed even if the status-API seed (above) raced with the first
      // event. Priority: env override → status-API seed → first lifecycle event.
      //
      // session.created fires when opencode creates a new session (including at
      // TUI startup). This is the earliest possible signal for a fresh session.
      if (event.type === 'session.created') {
        const props = ev_props(event);
        const info = props['info'];
        if (info && typeof info === 'object') {
          const sid = (info as Record<string, unknown>)['id'];
          if (typeof sid === 'string' && sid && !activeSessionId) {
            activeSessionId = sid;
            debugLog(`[chatroom-voice] session seeded from session.created → ${sid}\n`);
          }
        }
        return;
      }

      // session.status fires when a session transitions to idle/busy/retry.
      // Use it to seed activeSessionId in case we missed session.created.
      if (event.type === 'session.status') {
        const status = event.properties;
        if (typeof status === 'object' && status !== null) {
          const sid = (status as Record<string, unknown>)['sessionID'];
          if (typeof sid === 'string' && sid && !activeSessionId) {
            activeSessionId = sid;
            debugLog(`[chatroom-voice] session seeded from session.status → ${sid}\n`);
          }
        }
        return;
      }

      // -- Streaming delta (token by token) --
      // opencode publishes two related events on its bus:
      //   - `message.part.delta`    — frequent token-by-token stream
      //   - `message.part.updated`  — less frequent state snapshots, may also
      //                               carry a `delta` field
      // The SDK type only describes `.updated`, but `.delta` is the actual
      // hot-path stream observed at runtime — handle both.
      // -- message.part.delta — the actual token-by-token stream --
      // Shape: properties = { sessionID, messageID, partID, field, delta }.
      // `field` is the part attribute being appended to ("text" or "reasoning"
      // for AssistantMessage parts). NO `part` object — only the delta string.
      if ((event.type as string) === 'message.part.delta') {
        const props = ev_props(event);
        const delta = typeof props['delta'] === 'string' ? props['delta'] : '';
        if (!delta) return;
        const field = typeof props['field'] === 'string' ? props['field'] : 'text';
        // DEBUG 2026-05-28: log raw field+keys so we can confirm whether
        // reasoning arrives as a distinct field or only inline in text deltas.
        if (process.env['OPENCODE_VOICE_DEBUG']) {
          debugLog(`  raw delta keys=${Object.keys(props).join(',')} field=${field}`);
        }
        if (field !== 'text' && field !== 'reasoning') return;
        const sessionId = typeof props['sessionID'] === 'string' ? props['sessionID'] : '';
        const messageId = typeof props['messageID'] === 'string' ? props['messageID'] : '';
        const partId = typeof props['partID'] === 'string' ? props['partID'] : '';
        if (!sessionId || !messageId || !partId) return;
        // Classify as thought if EITHER the field name says reasoning OR the
        // cached part.type from an earlier .updated snapshot says reasoning.
        // This catches models whose delta-stream emits field="text" but the
        // snapshot carries part.type="reasoning".
        if (sessionId && !activeSessionId) activeSessionId = sessionId;
        evictStaleParts(partBuffer);
        if (partBuffer.size >= PART_BUFFER_MAX) {
          debugLog(`partBuffer cap (${PART_BUFFER_MAX}) — clearing`);
          partBuffer.clear();
        }

        // Race-condition fix 2026-05-28: classification depends on part.type
        // from the .updated snapshot, but the first few .delta events may
        // arrive BEFORE the first snapshot. If we have no cache entry yet,
        // queue the delta and wait briefly for the snapshot — otherwise
        // early reasoning-tokens get misclassified as text and pollute the
        // text-body / TTS pipeline.
        const item: PendingDelta = { sessionId, messageId, partId, delta };
        const cachedType = partTypeCache.get(partId);
        if (cachedType !== undefined) {
          const isReasoning = field === 'reasoning' || cachedType === 'reasoning';
          sendStreamDelta(item, isReasoning ? 'thought' : 'text');
          return;
        }
        if (field === 'reasoning') {
          sendStreamDelta(item, 'thought');
          return;
        }
        let pending = pendingPartDeltas.get(partId);
        if (!pending) {
          const flushTimer = setTimeout(() => flushPendingPart(partId, 'text'), PENDING_FLUSH_MS);
          pending = { queue: [], flushTimer };
          pendingPartDeltas.set(partId, pending);
        }
        pending.queue.push(item);
        return;
      }

      // -- message.part.updated — snapshot, IGNORED for delta-broadcast --
      // Bugfix 2026-05-28: previously this handler also POSTed agent-stream-delta
      // which caused token DUPLICATION (UI rendered every delta twice, visible as
      // garbled "** Docsund**", "1.Ke ** ofin" etc.). The `.delta` handler above
      // is authoritative for token streaming; `.updated` snapshots are only
      // informational and require no separate broadcast.
      if (event.type === 'message.part.updated') {
        // Learn part.type → cache for later .delta classification.
        try {
          const props = ev_props(event);
          const part = (props as Record<string, unknown>)['part'];
          if (part && typeof part === 'object') {
            const p = part as Record<string, unknown>;
            const id = p['id'];
            const type = p['type'];
            if (typeof id === 'string' && typeof type === 'string') {
              partTypeCache.set(id, type);
              if (process.env['OPENCODE_VOICE_DEBUG']) {
                const preview =
                  typeof p['text'] === 'string'
                    ? (p['text'] as string).slice(0, 60)
                    : '';
                debugLog(`    part.type=${type} partID=${id} preview="${preview}"`);
              }
              // Flush any deltas that arrived before this snapshot.
              if (pendingPartDeltas.has(id)) {
                flushPendingPart(id, type === 'reasoning' ? 'thought' : 'text');
              }
            }
          }
        } catch {}
        // No re-broadcast — `.delta` is the authoritative streaming path.
        return;
      }

      // -- Message completed --
      // Per-bubble (=per-assistant-message) complete: flush the cumulative
      // text from bubbleAcc[assistant.id]. Each subagent and each sequential
      // assistant-message gets its own complete-event with its own bubble id.
      if (event.type === 'message.updated') {
        const ev = event as EventMessageUpdated;
        const msg = ev.properties.info;

        if (msg.role !== 'assistant') return;
        const assistant = msg as AssistantMessage;
        if (!assistant.time.completed) return;

        const sessionId = assistant.sessionID;
        if (sessionId && !activeSessionId) activeSessionId = sessionId;

        // Flush any pending deltas (race-condition fix) for this message
        // BEFORE we read bubbleAcc — otherwise the final completes will miss
        // the last 250ms worth of tokens that were still queued under
        // pendingPartDeltas waiting for a part-type snapshot. Default-flush
        // them with the cached type or fall back to text.
        for (const [partId, _pending] of pendingPartDeltas) {
          const t = partTypeCache.get(partId);
          flushPendingPart(partId, t === 'reasoning' ? 'thought' : 'text');
        }

        // Clean up partBuffer entries for this assistant-message (no broadcast
        // needed — bubbleAcc holds the authoritative cumulative text).
        for (const [partId, buf] of partBuffer) {
          if (buf.messageId === assistant.id) partBuffer.delete(partId);
        }

        const acc = bubbleAcc.get(assistant.id);
        if (!acc) return;

        const completes: Promise<void>[] = [];
        if (acc.text.length > 0) {
          completes.push(
            postJson(cfg.bearer, `${cfg.httpUrl}/api/agent-stream-complete`, {
              room_id: cfg.defaultRoom,
              message_id: assistant.id,
              sender: cfg.agentName,
              // Bugfix 2026-05-28: chatroom identity (not opencode-internal)
              session_id: agentSessionId,
              ...(activeModel ? { model: activeModel } : {}),
              kind: 'text',
              full_text: acc.text,
              trigger_tts: true,
            }),
          );
        }
        if (acc.thought.length > 0) {
          completes.push(
            postJson(cfg.bearer, `${cfg.httpUrl}/api/agent-stream-complete`, {
              room_id: cfg.defaultRoom,
              message_id: assistant.id,
              sender: cfg.agentName,
              // Bugfix 2026-05-28: chatroom identity (not opencode-internal)
              session_id: agentSessionId,
              ...(activeModel ? { model: activeModel } : {}),
              kind: 'thought',
              full_text: acc.thought,
              trigger_tts: false,
            }),
          );
        }
        await Promise.all(completes);
        return;
      }
    },

    // -------------------------------------------------------------------------
    // System-prompt injection — tell the LLM it's connected to a voice chat
    // and what to do with normal output vs. the explicit voice_mermaid/map
    // tools. Append so opencode's built-in mode prompt stays intact.
    // -------------------------------------------------------------------------
    'experimental.chat.system.transform': async (_input, output) => {
      // 2026-05-30: Wenn aktiv Observer-Modus in einem Roleplay-Raum, ERSETZEN
      // wir den eingebauten opencode-Software-Engineer-Prompt komplett durch
      // den User-definierten Char-Prompt (User-Vorgabe — opencode soll im
      // Roleplay-Raum kein Software-Engineer mehr sein). Voice-Mode-Block
      // bleibt erhalten weil TTS-Hinweise immer noch gelten.
      if (observerStateCache && observerStateCache.systemPrompt) {
        const charName = observerStateCache.displayCharName || 'Beobachter';
        output.system.length = 0;
        output.system.push(
          `# Beobachter-Modus: ${charName}\n` +
            `Du beobachtest einen Roleplay-Raum (room=${observerStateCache.roomId}). ` +
            `Beitraege der Roleplay-Charaktere werden dir mit Praefix [Name]: ... zugestellt. ` +
            `Deine Antworten gehen NIEMALS zurueck an die Roleplay-Charaktere — nur der Browser-User hoert sie via TTS. ` +
            `Du bist KEIN Software-Engineering-Assistent waehrend dieser Sitzung — du bist die Rolle die der Browser-User unten beschreibt. ` +
            `Folgende Charakter-/Verhaltensanweisung wurde dir vom Browser-User mitgegeben:\n\n${observerStateCache.systemPrompt}`,
        );
        output.system.push(VOICE_MODE_SYSTEM_PROMPT);
        return;
      }
      // Normaler Modus (kein Observer): nur unseren Voice-Block ANHAENGEN,
      // opencodes eingebauten Software-Engineer-Default-Prompt unangetastet.
      output.system.push(VOICE_MODE_SYSTEM_PROMPT);
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

// Display name for `/status`: opencode reads dirname-basename when loading a
// plugin via file:// URL, which yields "dist" for everyone using a tsc/bun
// build output. Attach `id` directly on the function so the runtime can pick
// it up if it supports that convention.
(ChatroomVoice as unknown as { id?: string }).id = 'chatroom-voice';
export default ChatroomVoice;

// Compatibility export for opencode plugin loader which expects a named `server` export.
export const server = ChatroomVoice;
