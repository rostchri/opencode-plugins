/**
 * Minimal type declarations for the `phoenix` npm package (v1.x).
 * Only the subset used by the chatroom-voice plugin is declared here.
 * Full API docs: https://hexdocs.pm/phoenix/js/
 */
declare module 'phoenix' {
  export type SocketConnectOption = {
    params?: Record<string, unknown>;
    timeout?: number;
    heartbeatIntervalMs?: number;
    reconnectAfterMs?: (tries: number) => number;
    logger?: (kind: string, msg: string, data: unknown) => void;
    longpollerTimeout?: number;
    encode?: (payload: unknown, callback: (encoded: unknown) => void) => void;
    decode?: (payload: string, callback: (decoded: unknown) => void) => void;
    binaryType?: 'arraybuffer' | 'blob';
    vsn?: string;
  };

  export type ChannelReceive = {
    receive(status: string, callback: (response?: unknown) => void): ChannelReceive;
  };

  export class Channel {
    on(event: string, callback: (payload: unknown) => void): number;
    off(event: string, ref?: number): void;
    push(event: string, payload?: unknown, timeout?: number): Push;
    join(timeout?: number): Push;
    leave(timeout?: number): Push;
  }

  export class Push {
    receive(status: string, callback: (response?: unknown) => void): Push;
  }

  export class Socket {
    constructor(endPoint: string, opts?: SocketConnectOption);
    connect(params?: Record<string, unknown>): void;
    disconnect(callback?: () => void, code?: number, reason?: string): void;
    channel(topic: string, chanParams?: Record<string, unknown>): Channel;
    isConnected(): boolean;
    log(kind: string, msg: string, data?: unknown): void;
  }
}
