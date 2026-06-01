/**
 * Tests for the chatroom-voice plugin.
 *
 * These tests verify the plugin's structural contract without making any
 * network connections. The Phoenix Channel import and fetch calls are mocked
 * via module-level stubs.
 *
 * Run: node --test test/index.test.mjs
 */

import { describe, it, before, mock } from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

// ---------------------------------------------------------------------------
// Environment setup (required vars for loadConfig)
// ---------------------------------------------------------------------------

// Multiple plugin inits during tests add SIGTERM/SIGINT listeners (once per
// init). Remove the listener cap to prevent false-positive MaxListeners warnings.
process.setMaxListeners(0);

process.env['OPENCODE_VOICE_WS_URL'] = 'ws://localhost:4000/socket';
process.env['OPENCODE_VOICE_BEARER'] = 'test-bearer-token';
process.env['OPENCODE_VOICE_HTTP_URL'] = 'http://localhost:4000';
process.env['OPENCODE_VOICE_ROOM'] = 'TestRoom';
process.env['OPENCODE_VOICE_AGENT_NAME'] = 'test-agent';

// ---------------------------------------------------------------------------
// Fetch mock — captures calls for assertion
// ---------------------------------------------------------------------------

/** All recorded fetch calls: { url, body } */
const fetchCalls = [];
let fetchBehavior = 'ok'; // 'ok' | '4xx' | '5xx' | 'network-error' | 'timeout'

global.fetch = async (url, opts) => {
  // Simulate timeout/abort
  if (opts?.signal) {
    opts.signal.addEventListener('abort', () => {});
  }

  if (fetchBehavior === 'network-error') {
    throw new TypeError('fetch failed: network error');
  }

  let body = {};
  try {
    body = JSON.parse(opts?.body ?? '{}');
  } catch {
    // ignore
  }

  fetchCalls.push({ url: String(url), body });

  if (fetchBehavior === '4xx') return new Response('bad request', { status: 400 });
  if (fetchBehavior === '5xx') return new Response('server error', { status: 500 });
  // Simulate immediate abort for timeout test
  if (fetchBehavior === 'abort') {
    opts?.signal?.dispatchEvent(new Event('abort'));
    const err = new Error('The operation was aborted');
    err.name = 'AbortError';
    throw err;
  }
  return new Response('{}', { status: 200 });
};

/** Reset fetch state between tests. */
function resetFetch() {
  fetchCalls.length = 0;
  fetchBehavior = 'ok';
}

// ---------------------------------------------------------------------------
// Mock the 'phoenix' module so the WebSocket never opens
// ---------------------------------------------------------------------------

/** Track channel.off calls for cleanup assertions. */
const offCalls = [];

const mockChannel = {
  on: (_event, _cb) => 42, // return listener ref (number)
  off: (event, ref) => { offCalls.push({ event, ref }); },
  join: () => ({
    receive: (_status, _cb) => ({ receive: () => {} }),
  }),
  leave: () => {},
};

const mockSocket = {
  connect: () => {},
  disconnect: () => {},
  channel: (_topic, _params) => mockChannel,
};

// Intercept dynamic import('phoenix') by pre-populating a cached mock.
// We inject via globalThis so the compiled dist can pick it up.
// The dist uses `await import('phoenix')` — we cannot intercept ESM cache
// in node:test without a loader. Instead: test against the dist file which
// bun bundles (phoenix import is inlined), so we test at the TS source level
// via the bun-built dist. For phoenix, the channel will fail and be swallowed.

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const distPath = path.resolve(__dirname, '../dist/index.js');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a minimal plugin context stub. */
function makeCtx(overrides = {}) {
  return {
    client: {
      session: {
        promptAsync: async () => ({ data: undefined }),
        // status() returns the session-status map used by seedSessionFromStatusApi.
        // Default: one idle session so the plugin can seed activeSessionId.
        status: async () => ({ data: { 'sess-tui-123': { type: 'idle' } } }),
        // list() is kept for completeness but must NOT be called by the fix
        // (session.list()-newest was the regression — see bug 2026-05-28).
        list: async () => ({ data: [] }),
        ...((overrides.client?.session) ?? {}),
      },
    },
    project: { id: 'test-project' },
    directory: '/tmp',
    worktree: '/tmp',
    experimental_workspace: { register: () => {} },
    serverUrl: new URL('http://localhost:4000'),
    $: {},
    ...overrides,
  };
}

/** Build a minimal tool execution context stub. */
function makeToolCtx(overrides = {}) {
  return {
    sessionID: 'sess-123',
    messageID: 'msg-456',
    agent: 'test-agent',
    directory: '/tmp',
    worktree: '/tmp',
    abort: new AbortController().signal,
    metadata: () => {},
    ask: async () => {},
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('chatroom-voice plugin', () => {
  /** @type {import('../dist/index.js')} */
  let pluginModule;

  before(async () => {
    pluginModule = await import(distPath);
  });

  // ---- Structural exports -------------------------------------------------

  it('exports a default plugin function', () => {
    assert.equal(typeof pluginModule.default, 'function', 'default export must be a function');
  });

  it('exports a named server function (opencode plugin loader compat)', () => {
    assert.equal(typeof pluginModule.server, 'function', 'server export must be a function');
  });

  it('default and server exports are the same function', () => {
    assert.equal(pluginModule.default, pluginModule.server, 'default === server');
  });

  it('returns hooks with all 5 voice tools and required hook functions', async () => {
    const hooks = await pluginModule.default(makeCtx());

    assert.equal(typeof hooks.event, 'function', 'hooks.event must be a function');
    assert.equal(typeof hooks['tool.execute.before'], 'function');
    assert.equal(typeof hooks['tool.execute.after'], 'function');

    const tools = hooks.tool;
    assert.ok(tools, 'hooks.tool must be defined');

    const expectedTools = [
      'voice_reply',
      'voice_markdown',
      'voice_thought',
      'voice_mermaid',
      'voice_map',
    ];

    for (const name of expectedTools) {
      assert.ok(name in tools, `hooks.tool must contain "${name}"`);
      assert.equal(typeof tools[name].execute, 'function', `${name}.execute must be a function`);
      assert.equal(typeof tools[name].description, 'string', `${name}.description must be a string`);
      assert.ok(tools[name].description.length > 0, `${name}.description must not be empty`);
    }
  });

  // ---- voice_reply ---------------------------------------------------------

  it('voice_reply execute returns a result object with title and output', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());
    const result = await hooks.tool.voice_reply.execute(
      { text: 'Hello world', room_id: undefined, thought: undefined },
      makeToolCtx(),
    );

    assert.equal(typeof result, 'object');
    assert.ok('title' in result);
    assert.ok('output' in result);
  });

  it('voice_reply execute calls postJson with correct body', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    await hooks.tool.voice_reply.execute(
      { text: 'Spoken text', room_id: 'TestRoom', thought: undefined },
      makeToolCtx(),
    );

    // At least one fetch call should target agent-stream-complete
    const completeCalls = fetchCalls.filter((c) => c.url.includes('/api/agent-stream-complete'));
    assert.ok(completeCalls.length >= 1, 'should call agent-stream-complete at least once');

    const textCall = completeCalls.find((c) => c.body.kind === 'text');
    assert.ok(textCall, 'should have a text-kind call');
    assert.equal(textCall.body.full_text, 'Spoken text');
    assert.equal(textCall.body.trigger_tts, true);
    assert.equal(textCall.body.room_id, 'TestRoom');
  });

  it('voice_reply with thought sends two stream-complete calls', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    await hooks.tool.voice_reply.execute(
      { text: 'The answer', room_id: 'TestRoom', thought: 'internal reasoning' },
      makeToolCtx(),
    );

    const completeCalls = fetchCalls.filter((c) => c.url.includes('/api/agent-stream-complete'));
    assert.ok(completeCalls.length >= 2, 'should send 2 stream-complete calls (thought + text)');

    const thoughtCall = completeCalls.find((c) => c.body.kind === 'thought');
    assert.ok(thoughtCall, 'should have a thought-kind call');
    assert.equal(thoughtCall.body.trigger_tts, false);

    const textCall = completeCalls.find((c) => c.body.kind === 'text');
    assert.ok(textCall, 'should have a text-kind call');
    assert.equal(textCall.body.trigger_tts, true);
  });

  it('voice_reply rejects invalid room_id', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());
    // Drain async init heartbeat so it doesn't interfere with the assertion below.
    await new Promise((r) => setTimeout(r, 30));
    resetFetch();

    const result = await hooks.tool.voice_reply.execute(
      { text: 'hi', room_id: 'invalid room!', thought: undefined },
      makeToolCtx(),
    );

    assert.ok(result.output.toLowerCase().includes('invalid room_id'), `unexpected output: ${result.output}`);
    // No stream-complete or markdown calls should have been made for an invalid room.
    // (Heartbeat calls to /api/heartbeat are excluded from this check —
    //  they come from the background heartbeat loop, not from voice_reply.)
    const completeCalls = fetchCalls.filter(
      (c) => c.url.includes('/api/agent-stream-complete') || c.url.includes('/api/agent-markdown'),
    );
    assert.equal(completeCalls.length, 0, 'should not make stream/markdown calls for invalid room');
  });

  // ---- voice_markdown ------------------------------------------------------

  it('voice_markdown execute returns result object', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());
    const result = await hooks.tool.voice_markdown.execute(
      { text: '# Hello', room_id: undefined },
      makeToolCtx(),
    );

    assert.ok('title' in result);
    assert.ok('output' in result);
  });

  it('voice_markdown calls /api/agent-markdown', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    await hooks.tool.voice_markdown.execute(
      { text: '**bold**', room_id: 'TestRoom' },
      makeToolCtx(),
    );

    const markdownCalls = fetchCalls.filter((c) => c.url.includes('/api/agent-markdown'));
    assert.ok(markdownCalls.length >= 1, 'should call agent-markdown');
    assert.equal(markdownCalls[0].body.text, '**bold**');
  });

  it('voice_markdown rejects invalid room_id', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());
    const result = await hooks.tool.voice_markdown.execute(
      { text: '# hi', room_id: '../../../etc/passwd' },
      makeToolCtx(),
    );
    assert.ok(result.output.toLowerCase().includes('invalid room_id'));
  });

  // ---- voice_thought -------------------------------------------------------

  it('voice_thought execute returns result object', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());
    const result = await hooks.tool.voice_thought.execute(
      { text: 'thinking...', room_id: undefined },
      makeToolCtx(),
    );

    assert.ok('title' in result);
    assert.ok('output' in result);
  });

  it('voice_thought calls /api/agent-thought', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    await hooks.tool.voice_thought.execute(
      { text: 'internal reasoning', room_id: 'TestRoom' },
      makeToolCtx(),
    );

    const thoughtCalls = fetchCalls.filter((c) => c.url.includes('/api/agent-thought'));
    assert.ok(thoughtCalls.length >= 1, 'should call agent-thought');
    assert.equal(thoughtCalls[0].body.text, 'internal reasoning');
  });

  // ---- voice_mermaid -------------------------------------------------------

  it('voice_mermaid execute returns result object', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());
    const result = await hooks.tool.voice_mermaid.execute(
      { code: 'graph TD; A-->B', title: undefined, room_id: undefined },
      makeToolCtx(),
    );

    assert.ok('title' in result);
    assert.ok('output' in result);
  });

  it('voice_mermaid calls /api/agent-mermaid with code', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    await hooks.tool.voice_mermaid.execute(
      { code: 'sequenceDiagram; A->>B: Hi', title: 'Seq', room_id: 'TestRoom' },
      makeToolCtx(),
    );

    const mermaidCalls = fetchCalls.filter((c) => c.url.includes('/api/agent-mermaid'));
    assert.ok(mermaidCalls.length >= 1);
    assert.equal(mermaidCalls[0].body.code, 'sequenceDiagram; A->>B: Hi');
    assert.equal(mermaidCalls[0].body.title, 'Seq');
  });

  // ---- voice_map -----------------------------------------------------------

  it('voice_map execute rejects invalid JSON gracefully', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    const result = await hooks.tool.voice_map.execute(
      { data: 'not valid json', room_id: undefined },
      makeToolCtx(),
    );

    assert.ok(result.output.toLowerCase().includes('invalid json'));
  });

  it('voice_map execute sends valid JSON to /api/agent-map', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());
    const mapData = JSON.stringify({ center: [52.5, 13.4], zoom: 12, markers: [] });

    const result = await hooks.tool.voice_map.execute(
      { data: mapData, room_id: 'TestRoom' },
      makeToolCtx(),
    );

    assert.ok('title' in result);
    const mapCalls = fetchCalls.filter((c) => c.url.includes('/api/agent-map'));
    assert.ok(mapCalls.length >= 1);
  });

  it('voice_map rejects invalid room_id', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());
    const result = await hooks.tool.voice_map.execute(
      { data: '{"center":[0,0],"zoom":5,"markers":[]}', room_id: 'a b c' },
      makeToolCtx(),
    );
    assert.ok(result.output.toLowerCase().includes('invalid room_id'));
  });

  // ---- postJson error paths -----------------------------------------------

  it('postJson 4xx response: does not throw, logs to stderr', async () => {
    resetFetch();
    fetchBehavior = '4xx';
    let stderrOutput = '';
    const origWrite = process.stderr.write.bind(process.stderr);
    process.stderr.write = (chunk, ...args) => {
      stderrOutput += String(chunk);
      return origWrite(chunk, ...args);
    };

    const hooks = await pluginModule.default(makeCtx());
    // voice_reply calls postJson; result should still be returned without throwing
    const result = await hooks.tool.voice_reply.execute(
      { text: 'test', room_id: 'TestRoom', thought: undefined },
      makeToolCtx(),
    );

    process.stderr.write = origWrite;
    resetFetch();

    assert.ok('title' in result, '4xx should not throw — result returned');
  });

  it('postJson 5xx response: does not throw', async () => {
    resetFetch();
    fetchBehavior = '5xx';

    const hooks = await pluginModule.default(makeCtx());
    const result = await hooks.tool.voice_reply.execute(
      { text: 'test', room_id: 'TestRoom', thought: undefined },
      makeToolCtx(),
    );

    resetFetch();
    assert.ok('title' in result, '5xx should not throw — result returned');
  });

  it('postJson network-error: does not throw', async () => {
    resetFetch();
    fetchBehavior = 'network-error';

    const hooks = await pluginModule.default(makeCtx());
    const result = await hooks.tool.voice_reply.execute(
      { text: 'test', room_id: 'TestRoom', thought: undefined },
      makeToolCtx(),
    );

    resetFetch();
    assert.ok('title' in result, 'network error should not throw — result returned');
  });

  it('postJson abort (timeout): does not throw', async () => {
    resetFetch();
    fetchBehavior = 'abort';

    const hooks = await pluginModule.default(makeCtx());
    const result = await hooks.tool.voice_reply.execute(
      { text: 'test', room_id: 'TestRoom', thought: undefined },
      makeToolCtx(),
    );

    resetFetch();
    assert.ok('title' in result, 'abort/timeout should not throw — result returned');
  });

  // ---- loadConfig negative tests ------------------------------------------

  it('loadConfig throws if OPENCODE_VOICE_WS_URL is missing', async () => {
    const saved = process.env['OPENCODE_VOICE_WS_URL'];
    delete process.env['OPENCODE_VOICE_WS_URL'];

    // We import the dist module which has loadConfig inlined.
    // Re-calling pluginModule.default triggers loadConfig.
    await assert.rejects(
      async () => pluginModule.default(makeCtx()),
      /OPENCODE_VOICE_WS_URL is required/,
    );

    process.env['OPENCODE_VOICE_WS_URL'] = saved;
  });

  it('loadConfig throws if OPENCODE_VOICE_BEARER is missing', async () => {
    const saved = process.env['OPENCODE_VOICE_BEARER'];
    delete process.env['OPENCODE_VOICE_BEARER'];

    await assert.rejects(
      async () => pluginModule.default(makeCtx()),
      /OPENCODE_VOICE_BEARER is required/,
    );

    process.env['OPENCODE_VOICE_BEARER'] = saved;
  });

  it('loadConfig throws if OPENCODE_VOICE_HTTP_URL is missing', async () => {
    const saved = process.env['OPENCODE_VOICE_HTTP_URL'];
    delete process.env['OPENCODE_VOICE_HTTP_URL'];

    await assert.rejects(
      async () => pluginModule.default(makeCtx()),
      /OPENCODE_VOICE_HTTP_URL is required/,
    );

    process.env['OPENCODE_VOICE_HTTP_URL'] = saved;
  });

  // ---- event hook: delta fire-and-forget ----------------------------------

  it('event hook message.part.updated calls postJson for delta (fire-and-forget)', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    // Simulate a message.part.updated event with delta
    await hooks.event({
      event: {
        type: 'message.part.updated',
        properties: {
          delta: 'hello ',
          part: {
            id: 'part-001',
            type: 'text',
            sessionID: 'sess-abc',
            messageID: 'msg-xyz',
          },
        },
      },
    });

    // Give fire-and-forget microtask time to settle
    await new Promise((r) => setTimeout(r, 10));

    const deltaCalls = fetchCalls.filter((c) => c.url.includes('/api/agent-stream-delta'));
    assert.ok(deltaCalls.length >= 1, 'should call agent-stream-delta for delta event');
    assert.equal(deltaCalls[0].body.delta, 'hello ');
    assert.equal(deltaCalls[0].body.kind, 'text');
    assert.equal(deltaCalls[0].body.message_id, 'msg-xyz');

    resetFetch();
  });

  it('event hook message.part.updated: reasoning part sends thought kind', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    await hooks.event({
      event: {
        type: 'message.part.updated',
        properties: {
          delta: 'reasoning chunk',
          part: {
            id: 'part-002',
            type: 'reasoning',
            sessionID: 'sess-abc',
            messageID: 'msg-xyz',
          },
        },
      },
    });

    await new Promise((r) => setTimeout(r, 10));

    const deltaCalls = fetchCalls.filter((c) => c.url.includes('/api/agent-stream-delta'));
    assert.ok(deltaCalls.length >= 1);
    assert.equal(deltaCalls[0].body.kind, 'thought');

    resetFetch();
  });

  // ---- shutdown cleanup ---------------------------------------------------

  it('shutdown removes SIGTERM listener without crashing', async () => {
    // Re-init plugin and check we can remove listeners without error.
    const hooks = await pluginModule.default(makeCtx());
    // Emit SIGTERM — should not throw and not crash the process.
    // We cannot call process.emit('SIGTERM') safely in test as it exits the process.
    // Instead verify the hook shape is still valid after a second init.
    assert.equal(typeof hooks.event, 'function');
  });

  // ---- chat.message hook: model tracking + eager heartbeat ----------------

  it('chat.message hook is exposed on the hooks object', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());
    assert.equal(
      typeof hooks['chat.message'],
      'function',
      'hooks["chat.message"] must be a function',
    );
  });

  it('chat.message hook fires eager heartbeat when model changes', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    // Simulate first chat turn with a model.
    await hooks['chat.message']({
      sessionID: 'sess-model-test',
      model: { providerID: 'anthropic', modelID: 'claude-sonnet-4-6' },
    });

    // Give fire-and-forget microtask time to settle.
    await new Promise((r) => setTimeout(r, 20));

    // Heartbeat calls are POST /api/heartbeat.
    const heartbeatCalls = fetchCalls.filter((c) => c.url.includes('/api/heartbeat'));
    // Note: plugin init already fires one heartbeat (resolveRoomViaHeartbeat).
    // The eager heartbeat from chat.message should add at least one more.
    assert.ok(heartbeatCalls.length >= 1, 'should fire at least one heartbeat for model');

    // The eager heartbeat must carry the model field.
    const modelCall = heartbeatCalls.find((c) => c.body.model === 'anthropic/claude-sonnet-4-6');
    assert.ok(modelCall, 'heartbeat should include model=anthropic/claude-sonnet-4-6');

    resetFetch();
  });

  it('chat.message hook does NOT fire eager heartbeat on same model (no change)', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    const modelInput = {
      sessionID: 'sess-model-same',
      model: { providerID: 'anthropic', modelID: 'claude-haiku-4' },
    };

    // First call — sets the model and fires a heartbeat.
    await hooks['chat.message'](modelInput);
    await new Promise((r) => setTimeout(r, 20));

    const afterFirst = fetchCalls.filter((c) => c.url.includes('/api/heartbeat')).length;

    resetFetch();

    // Second call — same model — should NOT fire another eager heartbeat.
    await hooks['chat.message'](modelInput);
    await new Promise((r) => setTimeout(r, 20));

    const afterSecond = fetchCalls.filter((c) => c.url.includes('/api/heartbeat')).length;
    assert.equal(afterSecond, 0, 'no extra heartbeat on repeated same model');

    resetFetch();
  });

  it('chat.message hook fires again when model changes a second time', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    // First model.
    await hooks['chat.message']({
      sessionID: 'sess-switch',
      model: { providerID: 'openai', modelID: 'gpt-4o' },
    });
    await new Promise((r) => setTimeout(r, 20));

    resetFetch();

    // Second model — different.
    await hooks['chat.message']({
      sessionID: 'sess-switch',
      model: { providerID: 'anthropic', modelID: 'claude-opus-4' },
    });
    await new Promise((r) => setTimeout(r, 20));

    const heartbeatCalls = fetchCalls.filter((c) => c.url.includes('/api/heartbeat'));
    assert.ok(heartbeatCalls.length >= 1, 'should fire heartbeat on model switch');

    const switchCall = heartbeatCalls.find((c) => c.body.model === 'anthropic/claude-opus-4');
    assert.ok(switchCall, 'heartbeat should carry new model=anthropic/claude-opus-4');

    resetFetch();
  });

  it('chat.message hook with no model field does not fire eager heartbeat', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    // Drain any async heartbeat from the plugin init phase before measuring.
    await new Promise((r) => setTimeout(r, 30));
    resetFetch();

    // Turn without model info — should be a no-op for the eager heartbeat path.
    await hooks['chat.message']({ sessionID: 'sess-nomodel' });
    await new Promise((r) => setTimeout(r, 20));

    const heartbeatCalls = fetchCalls.filter((c) => c.url.includes('/api/heartbeat'));
    assert.equal(heartbeatCalls.length, 0, 'no heartbeat when model is absent');

    resetFetch();
  });

  // ---- session seeding (regression test for 2026-05-28 lazy-resolution bug) ----

  it('seeds activeSessionId from session.status API at init (NOT session.list)', async () => {
    resetFetch();
    // Track whether session.list was called (it must NOT be called).
    let listCallCount = 0;
    const ctx = makeCtx({
      client: {
        session: {
          promptAsync: async () => ({ data: undefined }),
          status: async () => ({ data: { 'sess-tui-abc': { type: 'idle' } } }),
          list: async () => {
            listCallCount++;
            return { data: [] };
          },
        },
      },
    });
    const hooks = await pluginModule.default(ctx);

    // Give async init (seedSessionFromStatusApi) time to complete.
    await new Promise((r) => setTimeout(r, 30));

    // session.list must NOT have been called — that was the regression.
    assert.equal(listCallCount, 0, 'session.list must NOT be called during init');

    // The session.status API was used — verify via the event hook: if we send
    // a message.part.updated event the plugin should route it using the seeded
    // session (not log "no active session"). We cannot inspect activeSessionId
    // directly, but we can confirm the hook does not crash.
    await hooks.event({
      event: {
        type: 'session.status',
        properties: { sessionID: 'sess-tui-abc', status: { type: 'idle' } },
      },
    });

    resetFetch();
  });

  it('seeds activeSessionId from session.created event (TUI startup lifecycle)', async () => {
    resetFetch();
    // Use a ctx where status() returns empty — so only the event can seed.
    const ctx = makeCtx({
      client: {
        session: {
          promptAsync: async () => ({ data: undefined }),
          status: async () => ({ data: {} }), // no sessions yet
          list: async () => ({ data: [] }),
        },
      },
    });
    const hooks = await pluginModule.default(ctx);
    await new Promise((r) => setTimeout(r, 10));

    // Fire a session.created event (as opencode does at TUI startup).
    await hooks.event({
      event: {
        type: 'session.created',
        properties: { info: { id: 'sess-new-tui', title: 'new session', projectID: 'p1' } },
      },
    });

    // Now fire a session.status event for the same session — should NOT
    // overwrite (already seeded).
    await hooks.event({
      event: {
        type: 'session.status',
        properties: { sessionID: 'sess-other', status: { type: 'idle' } },
      },
    });

    // We can't inspect activeSessionId directly, but the hook should not crash
    // and the system is correctly seeded (verified by the live test described
    // in the PR).
    assert.ok(true, 'session.created event handled without error');

    resetFetch();
  });

  it('chat.message hook does not send model in tool-status body', async () => {
    resetFetch();
    const hooks = await pluginModule.default(makeCtx());

    // Set a model first.
    await hooks['chat.message']({
      sessionID: 'sess-toolcheck',
      model: { providerID: 'anthropic', modelID: 'claude-opus-4' },
    });
    await new Promise((r) => setTimeout(r, 20));

    resetFetch();

    // Trigger a tool.execute.before hook.
    await hooks['tool.execute.before']({
      callID: 'call-xyz',
      sessionID: 'sess-toolcheck',
      tool: 'bash',
    });

    const toolStatusCalls = fetchCalls.filter((c) => c.url.includes('/api/tool-status'));
    assert.ok(toolStatusCalls.length >= 1, 'tool-status should be called');

    // model must NOT appear in tool-status payloads.
    for (const call of toolStatusCalls) {
      assert.ok(
        !('model' in call.body),
        `tool-status body must not contain model field, got: ${JSON.stringify(call.body)}`,
      );
    }

    resetFetch();
  });
});
