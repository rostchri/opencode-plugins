import type { Plugin, Hooks } from '@opencode-ai/plugin';
import { Socket } from 'phoenix';
import { HeadlessPromptManager } from './headless-prompt-manager.js';
import { CharCard } from './macros.js';
import * as fs from 'fs';
import * as path from 'path';

// Fix for Node.js __dirname in ESM
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function loadPreset() {
    // After bundling, __dirname points to dist/ (where dist/index.js lives).
    // The symlink (chatroom-ensemble.js -> dist/index.js) does NOT change __dirname —
    // import.meta.url still resolves to the real file inside dist/.
    // So relative to dist/:
    //   '../docs'  => Plugin-Root/docs  (CORRECT: docs/ is a sibling of dist/)
    //   '../../docs' => Plugin-Root/../docs (WRONG: one level too high)
    // We probe both candidates so the code survives future layout changes.
    const candidates = [
        path.join(__dirname, '../docs/st-chat-settings.json'),
        path.join(__dirname, '../../docs/st-chat-settings.json'),
        path.join(__dirname, 'docs/st-chat-settings.json'),
    ];
    for (const candidate of candidates) {
        if (fs.existsSync(candidate)) {
            console.log(`[chatroom-ensemble] Loading preset from ${candidate}`);
            return new HeadlessPromptManager(candidate);
        }
    }
    throw new Error(`Preset not found. Tried:\n${candidates.join('\n')}`);
}

async function streamOpenRouterCompatibleAPI(messages: any[], model: string, apiKey: string, baseUrl: string, onChunk: (text: string) => void) {
    const res = await fetch(`${baseUrl}/v1/chat/completions`, {
        method: "POST",
        headers: {
            "Authorization": `Bearer ${apiKey}`,
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            model,
            messages,
            stream: true,
            include_reasoning: true // For DeepSeek / Claude Think
        })
    });

    if (!res.ok || !res.body) {
        throw new Error(`API Error: ${res.status} ${res.statusText}`);
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || "";

        for (let line of lines) {
            line = line.trim();
            if (line.startsWith('data: ')) {
                const data = line.slice(6);
                if (data === '[DONE]') continue;
                try {
                    const parsed = JSON.parse(data);
                    const content = parsed.choices?.[0]?.delta?.content || "";
                    if (content) onChunk(content);
                } catch (e) {
                    // ignore parse errors for partial chunks
                }
            }
        }
    }
}

export const ChatroomEnsemble: Plugin = async (ctx): Promise<Hooks> => {
    const wsUrl = process.env['CHATROOM_WS_URL'];
    const bearer = process.env['CHATROOM_BEARER'];
    const roomId = process.env['CHATROOM_ROOM_ID'] || 'Lobby';
    const httpUrl = process.env['CHATROOM_HTTP_URL'];
    // Stable session_id: explicit env var takes precedence, otherwise derive from roomId.
    // Must be non-empty and identical across delta + complete requests for the same stream
    // (StreamDeltaStore keys on session_id + message_id).
    const sessionId = process.env['CHATROOM_SESSION_ID'] || `ensemble-plugin-${roomId}`;

    const apiKey = process.env['OPENROUTER_API_KEY'] || process.env['LLM_API_KEY'];
    const baseUrl = process.env['OPENROUTER_BASE_URL'] || process.env['LLM_BASE_URL'] || "https://openrouter.ai/api";

    if (!wsUrl || !bearer || !httpUrl || !apiKey) {
        console.warn('[chatroom-ensemble] Missing required env vars (CHATROOM_WS_URL, CHATROOM_BEARER, CHATROOM_HTTP_URL, OPENROUTER_API_KEY)');
        return {};
    }

    const pm = loadPreset();
    console.log('[chatroom-ensemble] Loaded Headless Prompt Manager with ST Preset');

    let transport: unknown = (globalThis as any).WebSocket;
    if (!transport) {
        // dynamic import for ESM
        const wsModule = await import('ws');
        transport = wsModule.default;
    }

    const socket = new Socket(wsUrl, {
        params: { token: bearer },
        transport
    } as any);

    socket.connect();
    const channel = socket.channel(`room:${roomId}`, {});

    // Listen to the Ensemble turn request
    channel.on('ensemble:run_turn', async (payload: any) => {
        const { char_card, history, message_id, room_id, char_id, stream_id, is_continue } = payload;

        // sender: use the character's display name, with a safe fallback
        const sender = (char_card?.name as string | undefined) || 'Ensemble';

        console.log(`[chatroom-ensemble] Running turn for ${sender} (message_id: ${message_id}, room_id: ${room_id}, char_id: ${char_id}, stream_id: ${stream_id}, is_continue: ${is_continue})`);

        // 1. Build the prompt using ST Logic
        const messages = pm.buildPrompt(
            char_card as CharCard,
            history || []
        );

        try {
            // 2. Stream from OpenRouter / NanoGPT
            // assigned_model_id is the primary field; metadata.model is a mirror;
            // fall back to a safe default if neither is set.
            const model = char_card?.assigned_model_id || char_card?.metadata?.model || "anthropic/claude-3-opus";
            let fullText = "";

            await streamOpenRouterCompatibleAPI(messages, model, apiKey, baseUrl, (chunk) => {
                fullText += chunk;

                // 3. Post Delta to Chatroom Controller
                // All required fields: session_id, message_id, sender, kind, delta, room_id, char_id
                fetch(`${httpUrl}/api/agent-stream-delta`, {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${bearer}`,
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        session_id: sessionId,
                        message_id,
                        sender,
                        kind: 'text',
                        delta: chunk,
                        room_id,
                        char_id
                    })
                }).catch(err => console.error('Delta error:', err));
            });

            // 4. Finalize
            // All required fields: session_id, message_id, sender, kind, full_text, room_id, char_id
            // trigger_tts is intentionally omitted — ensemble TTS routing happens via
            // ensemble:stream_done / voice_mapping on the controller side.
            await fetch(`${httpUrl}/api/agent-stream-complete`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${bearer}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    session_id: sessionId,
                    message_id,
                    sender,
                    kind: 'text',
                    full_text: fullText,
                    room_id,
                    char_id
                })
            });

        } catch (error) {
            console.error('[chatroom-ensemble] Turn failed:', error);
        }
    });

    channel.join()
        .receive('ok', () => console.log(`[chatroom-ensemble] Joined room:${roomId}`))
        .receive('error', (err: any) => console.error(`[chatroom-ensemble] Join failed:`, err));

    return {};
};
