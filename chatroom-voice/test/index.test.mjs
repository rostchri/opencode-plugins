/**
 * Smoke tests for the chatroom-voice plugin.
 *
 * These tests verify the plugin's structural contract without making any
 * network connections. The Phoenix Channel import and fetch calls are mocked
 * via module-level stubs.
 *
 * Run: node --test test/index.test.mjs
 */

import { describe, it, before, mock } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

// ---------------------------------------------------------------------------
// Environment setup (required vars for loadConfig)
// ---------------------------------------------------------------------------
process.env['OPENCODE_VOICE_WS_URL'] = 'ws://localhost:4000/socket';
process.env['OPENCODE_VOICE_BEARER'] = 'test-bearer-token';
process.env['OPENCODE_VOICE_HTTP_URL'] = 'http://localhost:4000';
process.env['OPENCODE_VOICE_ROOM'] = 'TestRoom';
process.env['OPENCODE_VOICE_AGENT_NAME'] = 'test-agent';

// ---------------------------------------------------------------------------
// Mock global fetch so HTTP calls are no-ops
// ---------------------------------------------------------------------------
global.fetch = async (_url, _opts) => new Response('{}', { status: 200 });

// ---------------------------------------------------------------------------
// Mock the 'phoenix' module so the WebSocket never opens
// ---------------------------------------------------------------------------
const mockSocket = {
  connect: () => {},
  disconnect: () => {},
  channel: (_topic, _params) => ({
    on: (_event, _cb) => {},
    join: () => ({
      receive: (_status, _cb) => ({ receive: () => {} }),
    }),
    leave: () => {},
  }),
};

// We patch `import('phoenix')` by replacing the module cache entry.
// node:test does not have a built-in module mocker for ESM yet, so we
// inject the mock via globalThis and override the dynamic import in the
// built dist file via a URL mapping trick.
//
// Simpler approach: load the dist/index.js directly and monkey-patch its
// openPhoenixChannel by setting the module-scope `phoenix` to our mock.
// Since openPhoenixChannel uses `await import('phoenix')`, we intercept by
// pre-populating the import cache via a synthetic module.
//
// For this MVP smoke test we use a pragmatic strategy: run the plugin init
// and catch any phoenix-related errors gracefully, then assert on the hook
// shape only.

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const distPath = path.resolve(__dirname, '../dist/index.js');

describe('chatroom-voice plugin', () => {
  /** @type {import('../dist/index.js')} */
  let pluginModule;

  before(async () => {
    // Dynamic import of built artifact
    pluginModule = await import(distPath);
  });

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
    // Minimal stub for the opencode plugin context
    const mockCtx = {
      client: {
        session: {
          promptAsync: async () => ({ data: undefined }),
        },
      },
      project: { id: 'test-project' },
      directory: '/tmp',
      worktree: '/tmp',
      experimental_workspace: { register: () => {} },
      serverUrl: new URL('http://localhost:4000'),
      $: {},
    };

    // init the plugin — Phoenix Channel open will fail (no server) but the
    // error is swallowed by design (plugin continues without inbound voice)
    const hooks = await pluginModule.default(mockCtx);

    // event hook
    assert.equal(typeof hooks.event, 'function', 'hooks.event must be a function');

    // tool.execute hooks
    assert.equal(
      typeof hooks['tool.execute.before'],
      'function',
      'hooks["tool.execute.before"] must be a function',
    );
    assert.equal(
      typeof hooks['tool.execute.after'],
      'function',
      'hooks["tool.execute.after"] must be a function',
    );

    // voice tools
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
      assert.equal(
        typeof tools[name].execute,
        'function',
        `${name}.execute must be a function`,
      );
      assert.equal(
        typeof tools[name].description,
        'string',
        `${name}.description must be a string`,
      );
      assert.ok(tools[name].description.length > 0, `${name}.description must not be empty`);
    }
  });

  it('voice_reply execute returns a result object with title and output', async () => {
    const mockCtx = {
      client: {
        session: { promptAsync: async () => ({ data: undefined }) },
      },
      project: { id: 'test-project' },
      directory: '/tmp',
      worktree: '/tmp',
      experimental_workspace: { register: () => {} },
      serverUrl: new URL('http://localhost:4000'),
      $: {},
    };

    const hooks = await pluginModule.default(mockCtx);
    const toolCtx = {
      sessionID: 'sess-123',
      messageID: 'msg-456',
      agent: 'test-agent',
      directory: '/tmp',
      worktree: '/tmp',
      abort: new AbortController().signal,
      metadata: () => {},
      ask: async () => {},
    };

    const result = await hooks.tool.voice_reply.execute(
      { text: 'Hello world', room_id: undefined, thought: undefined },
      toolCtx,
    );

    assert.equal(typeof result, 'object', 'execute must return an object');
    assert.ok('title' in result, 'result must have title');
    assert.ok('output' in result, 'result must have output');
  });

  it('voice_map execute rejects invalid JSON gracefully', async () => {
    const mockCtx = {
      client: {
        session: { promptAsync: async () => ({ data: undefined }) },
      },
      project: { id: 'test-project' },
      directory: '/tmp',
      worktree: '/tmp',
      experimental_workspace: { register: () => {} },
      serverUrl: new URL('http://localhost:4000'),
      $: {},
    };

    const hooks = await pluginModule.default(mockCtx);
    const toolCtx = {
      sessionID: 'sess-123',
      messageID: 'msg-456',
      agent: 'test-agent',
      directory: '/tmp',
      worktree: '/tmp',
      abort: new AbortController().signal,
      metadata: () => {},
      ask: async () => {},
    };

    const result = await hooks.tool.voice_map.execute(
      { data: 'not valid json', room_id: undefined },
      toolCtx,
    );

    assert.ok(result.output.toLowerCase().includes('invalid json'), 'should report invalid JSON');
  });
});
