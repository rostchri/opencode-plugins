export interface CharCard {
    name: string;
    description: string;
    personality: string;
    scenario: string;
    first_mes: string;
    mes_example: string;
}

/**
 * PORTED FROM SILLYTAVERN (public/script.js and public/scripts/utils.js)
 * 
 * Replaces SillyTavern macro variables like {{char}}, {{user}}, etc.
 * with their actual values from the Character Card and current context.
 * In SillyTavern, this is handled by a combination of `substituteParams` 
 * and regular expressions. This is a headless, DOM-free version.
 */
export function substituteMacros(text: string, char: CharCard, userName: string = 'User'): string {
    if (!text) return '';
    
    let result = text;
    
    // Core macros
    result = result.replace(/{{char}}/gi, char.name);
    result = result.replace(/<BOT>/gi, char.name);
    
    result = result.replace(/{{user}}/gi, userName);
    result = result.replace(/<USER>/gi, userName);
    
    result = result.replace(/{{charIfNotGroup}}/gi, char.name);
    
    result = result.replace(/{{description}}/gi, char.description);
    result = result.replace(/{{personality}}/gi, char.personality);
    result = result.replace(/{{scenario}}/gi, char.scenario);
    
    // Some presets have things like {{trim}} or {{#if ...}} - for a headless port without full handlebars:
    result = result.replace(/{{trim}}/gi, '');
    
    return result;
}

/**
 * PORTED FROM SILLYTAVERN (public/scripts/openai.js)
 * 
 * Port of `parseMesExamples`. Parses the <START> delimited dialogue examples 
 * block from a SillyTavern character card and converts it into an array
 * of OpenRouter/OpenAI compatible message objects { role: 'user'|'assistant', content: '...' }.
 */
export function parseMesExamples(examplesStr: string, charName: string, userName: string = 'User'): any[] {
    if (!examplesStr) return [];
    
    // Simplistic parser for SillyTavern dialogue examples.
    // ST uses <START> as a delimiter.
    // Format: 
    // <START>
    // {{user}}: hello
    // {{char}}: hi
    
    const blocks = examplesStr.split(/<START>/i).map(s => s.trim()).filter(s => s.length > 0);
    const messages: any[] = [];
    
    for (const block of blocks) {
        const lines = block.split('\n');
        for (let line of lines) {
            line = substituteMacros(line, { name: charName, description: '', personality: '', scenario: '', first_mes: '', mes_example: '' } as CharCard, userName);
            
            if (line.toLowerCase().startsWith(`${userName.toLowerCase()}:`)) {
                messages.push({
                    role: 'user',
                    content: line.substring(userName.length + 1).trim()
                });
            } else if (line.toLowerCase().startsWith(`${charName.toLowerCase()}:`)) {
                messages.push({
                    role: 'assistant',
                    content: line.substring(charName.length + 1).trim()
                });
            } else if (line.includes(':')) {
                // Unknown speaker, append to previous or drop
            } else {
                // Continuation of previous
                if (messages.length > 0) {
                    messages[messages.length - 1].content += '\n' + line;
                }
            }
        }
    }
    
    return messages;
}
