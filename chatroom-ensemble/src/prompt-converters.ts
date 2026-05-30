/**
 * 1:1 PORT FROM SILLYTAVERN
 * Source: SillyTavern/src/prompt-converters.js
 * 
 * Append cache_control headers to an OpenRouter request at depth. Directly modifies the
 * messages array. Used to get 90% discount on Anthropic Claude API via OpenRouter.
 */
export function cachingAtDepthForOpenRouterClaude(messages: any[], cachingAtDepth: number, ttl?: string) {
    let passedThePrefill = false;
    let depth = 0;
    let previousRoleName = '';
    
    for (let i = messages.length - 1; i >= 0; i--) {
        if (!passedThePrefill && messages[i].role === 'assistant') {
            continue;
        }

        passedThePrefill = true;

        if (messages[i].role === 'system') {
            continue;
        }

        if (messages[i].role !== previousRoleName) {
            if (depth === cachingAtDepth || depth === cachingAtDepth + 2) {
                const content = messages[i].content;
                if (typeof content === 'string') {
                    messages[i].content = [{
                        type: 'text',
                        text: content,
                        cache_control: ttl ? { type: 'ephemeral', ttl: ttl } : { type: 'ephemeral' },
                    }];
                } else if (Array.isArray(content) && content.length > 0) {
                    content[content.length - 1].cache_control = ttl 
                        ? { type: 'ephemeral', ttl: ttl }
                        : { type: 'ephemeral' };
                }
            }

            if (depth === cachingAtDepth + 2) {
                break;
            }

            depth += 1;
            previousRoleName = messages[i].role;
        }
    }
}

/**
 * 1:1 PORT FROM SILLYTAVERN
 * Source: SillyTavern/src/prompt-converters.js
 * 
 * Adds cache_control to the system prompt for OpenRouter requests.
 */
export function cachingSystemPromptForOpenRouter(messages: any[], ttl?: string) {
    if (!Array.isArray(messages) || messages.length === 0) {
        return;
    }

    const systemMessage = messages.find(msg => msg.role === 'system');
    if (!systemMessage) {
        return;
    }

    if (systemMessage.cache_control) {
        return;
    }

    const cacheControl = ttl
        ? { type: 'ephemeral', ttl }
        : { type: 'ephemeral' };

    if (Array.isArray(systemMessage.content)) {
        const hasExistingCacheControl = systemMessage.content.some((part: any) => part?.cache_control);
        if (hasExistingCacheControl) {
            return;
        }

        for (let i = systemMessage.content.length - 1; i >= 0; i--) {
            if (systemMessage.content[i]?.type === 'text') {
                systemMessage.content[i].cache_control = cacheControl;
                return;
            }
        }
    } else if (typeof systemMessage.content === 'string') {
        systemMessage.content = [
            {
                type: 'text',
                text: systemMessage.content,
                cache_control: cacheControl,
            },
        ];
    }
}
