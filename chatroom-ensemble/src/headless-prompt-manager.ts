import * as fs from 'fs';
import { substituteMacros, parseMesExamples, CharCard } from './macros.js';
import { cachingAtDepthForOpenRouterClaude, cachingSystemPromptForOpenRouter } from './prompt-converters.js';

export interface PromptPreset {
    prompts: Array<{
        identifier: string;
        name: string;
        role?: string;
        content?: string;
        system_prompt?: boolean;
        enabled?: boolean;
    }>;
}

export interface ChatMessage {
    role: 'user' | 'assistant' | 'system';
    content: string;
    name?: string;
}

/**
 * HEADLESS PROMPT MANAGER
 * 
 * This class replaces the DOM-heavy `PromptManager` from SillyTavern 
 * (public/scripts/PromptManager.js) and the prompt assembling logic 
 * from `preparePromptsForChatCompletion` and `populateChatCompletion` 
 * (public/scripts/openai.js).
 * 
 * It reads a standard SillyTavern Chat Settings preset (JSON) and builds 
 * an OpenRouter/OpenAI compatible message array, preserving the exact 
 * prompt insertion order and caching strategy used by SillyTavern.
 */
export class HeadlessPromptManager {
    private preset: PromptPreset;

    constructor(presetPathOrObj: string | PromptPreset) {
        if (typeof presetPathOrObj === 'string') {
            const raw = fs.readFileSync(presetPathOrObj, 'utf8');
            this.preset = JSON.parse(raw);
        } else {
            this.preset = presetPathOrObj;
        }
    }

    private getPromptContent(identifier: string): string {
        const prompt = this.preset.prompts.find(p => p.identifier === identifier);
        // Default to enabled if not explicitly set to false
        if (prompt && prompt.enabled !== false && prompt.content) {
            return prompt.content;
        }
        return '';
    }

    /**
     * Replicates the logic from `populateChatCompletion` in SillyTavern's openai.js.
     * It strictly adheres to the SillyTavern insertion order:
     * 1. Pre-History System Prompts (Main, Description, Scenario, World Info, etc.)
     * 2. Custom Preset Prompts (Tier 4, Visual Toolkit, etc.)
     * 3. Dialogue Examples
     * 4. NSFW Prompt
     * 5. Chat History
     * 6. Jailbreak (Post-History Instructions)
     */
    public buildPrompt(
        charCard: CharCard,
        history: ChatMessage[],
        worldInfoBefore: string = '',
        worldInfoAfter: string = '',
        userName: string = 'User'
    ): any[] {
        const messages: any[] = [];

        const addSystem = (content: string) => {
            if (content && content.trim()) {
                messages.push({ role: 'system', content: substituteMacros(content, charCard, userName) });
            }
        };

        // 1. World Info Before
        addSystem(worldInfoBefore);

        // 2. Main Prompt
        addSystem(this.getPromptContent('main'));

        // 3. World Info After
        addSystem(worldInfoAfter);

        // 4. Character Data
        addSystem(charCard.description);
        addSystem(charCard.personality);
        addSystem(charCard.scenario);

        // 5. Enhance Definitions
        addSystem(this.getPromptContent('enhanceDefinitions'));

        // 6. Custom Prompts (Tier 4, etc. from Preset)
        for (const prompt of this.preset.prompts) {
            const isDefault = ['main', 'nsfw', 'jailbreak', 'enhanceDefinitions', 'dialogueExamples', 'chatHistory', 'worldInfoBefore', 'worldInfoAfter', 'charDescription', 'charPersonality', 'scenario', 'personaDescription'].includes(prompt.identifier);
            if (!isDefault && prompt.enabled !== false && prompt.content) {
                addSystem(prompt.content);
            }
        }

        // 7. Dialogue Examples
        const examples = parseMesExamples(charCard.mes_example, charCard.name, userName);
        messages.push(...examples);

        // 8. NSFW Prompt
        addSystem(this.getPromptContent('nsfw'));

        // 9. Chat History
        // TODO: Token Budgeting (Sliding Window)
        // For now, we take the last 50 messages to prevent blowup
        const historySlice = history.slice(-50);
        messages.push(...historySlice);

        // 10. Jailbreak (Post-History Instructions)
        addSystem(this.getPromptContent('jailbreak'));

        // 11. Apply Claude / OpenRouter Caching
        // SillyTavern caches the system prompt and at depth 4
        cachingSystemPromptForOpenRouter(messages);
        cachingAtDepthForOpenRouterClaude(messages, 4);

        return messages;
    }
}
