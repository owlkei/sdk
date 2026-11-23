import { ConnectionError, OwlkeiError, TimeoutError, errorBody } from "./errors.js";
import {
  type Envelope,
  type Incoming,
  decodeBinary,
  decodeText,
  encodeBinary,
  encodeText,
  isBinaryData,
  isTopic,
} from "./frame.js";
import { LIFETIME, delayFor, planFor } from "./retry.js";

/** The subprotocol of the wire protocol v1. */
export const PROTOCOL = "owlkei.v1";

const ABNORMAL = 1006;
const MAX_QUEUED = 256;

/** The minimal WebSocket surface the client needs; the platform's `WebSocket` provides it. */
export interface WebSocketLike {
  binaryType: string;
  readonly readyState: number;
  onopen: ((event: unknown) => void) | null;
  onclose: ((event: { readonly code: number }) => void) | null;
  onmessage: ((event: { readonly data: unknown }) => void) | null;
  onerror: ((event: unknown) => void) | null;
  send(data: string | Uint8Array): void;
  close(code?: number): void;
}

export type WebSocketConstructor = new (url: string, protocols: string) => WebSocketLike;

export interface ClientOptions {
  /** Reconnect after the connection is lost, as the close code allows. Default: true. */
  readonly reconnect?: boolean;
  /** Bounds of the exponential backoff with full jitter. Default: 1,000 to 30,000 ms. */
  readonly backoff?: { readonly minMs?: number; readonly maxMs?: number };
  /** Interval of the JSON ping, `false` for none. Default: 25,000 ms. */
  readonly heartbeatMs?: number | false;
  /** Time without any frame after which the connection counts as dead. Default: 60,000 ms. */
  readonly idleTimeoutMs?: number;
  /** Time a subscribe, unsubscribe, publish or ping waits for its answer. Default: 10,000 ms. */
  readonly requestTimeoutMs?: number;
  /** How binary data reaches handlers and replies. Default: `"arraybuffer"`. */
  readonly binaryType?: "arraybuffer" | "blob";
  /** The WebSocket implementation. Default: `globalThis.WebSocket`. */
  readonly WebSocket?: WebSocketConstructor;
}

/** `connecting` until the first open, `reconnecting` between a loss and the next open. */
export type State = "connecting" | "open" | "reconnecting" | "closed";

/** Receives the `data` of each event on a topic: a JSON value, or binary data. */
export type EventHandler = (data: unknown, topic: string) => void;

export interface Subscription {
  readonly topic: string;
  /** Stops this handler; the last handler of a topic also unsubscribes the connection. */
  unsubscribe(): Promise<void>;
}

interface ClientEvents {
  open: [];
  reconnecting: [delayMs: number, closeCode: number];
  close: [closeCode: number];
  error: [error: unknown];
}

type Listener<K extends keyof ClientEvents> = (...args: ClientEvents[K]) => void;

interface Pending {
  readonly resolve: (value: unknown) => void;
  readonly reject: (reason: unknown) => void;
  readonly timer: ReturnType<typeof setTimeout>;
  sent: boolean;
}

interface Topic {
  readonly handlers: Set<EventHandler>;
  ready: Promise<void>;
  joined: boolean;
}

type Outgoing = () => Promise<string | Uint8Array>;

/**
 * A connection to an Owlkei Go WebSocket endpoint. It reconnects with backoff, subscribes again to
 * every topic after a reconnect, and matches each answer to its request.
 */
export class Client {
  readonly #url: string;
  readonly #reconnect: boolean;
  readonly #backoff: { readonly minMs: number; readonly maxMs: number };
  readonly #heartbeatMs: number | false;
  readonly #idleTimeoutMs: number;
  readonly #requestTimeoutMs: number;
  readonly #binaryType: "arraybuffer" | "blob";
  readonly #WebSocket: WebSocketConstructor;

  #state: State = "connecting";
  #socket: WebSocketLike | undefined;
  #seq = 0;
  #attempt = 0;
  #lost = false;
  #heartbeat: ReturnType<typeof setInterval> | undefined;
  #idle: ReturnType<typeof setTimeout> | undefined;
  #retry: ReturnType<typeof setTimeout> | undefined;
  readonly #pending = new Map<string, Pending>();
  readonly #outbox: { readonly id: string | undefined; readonly build: Outgoing }[] = [];
  readonly #topics = new Map<string, Topic>();
  readonly #listeners: { [K in keyof ClientEvents]: Set<Listener<K>> } = {
    open: new Set(),
    reconnecting: new Set(),
    close: new Set(),
    error: new Set(),
  };

  constructor(url: string, options: ClientOptions = {}) {
    const ctor =
      options.WebSocket ??
      (globalThis as unknown as { WebSocket?: WebSocketConstructor }).WebSocket;
    if (ctor === undefined) {
      throw new TypeError("owlkei: no WebSocket implementation; pass options.WebSocket");
    }
    this.#url = url;
    this.#reconnect = options.reconnect ?? true;
    this.#backoff = {
      minMs: options.backoff?.minMs ?? 1000,
      maxMs: options.backoff?.maxMs ?? 30_000,
    };
    this.#heartbeatMs = options.heartbeatMs ?? 25_000;
    this.#idleTimeoutMs = options.idleTimeoutMs ?? 60_000;
    this.#requestTimeoutMs = options.requestTimeoutMs ?? 10_000;
    this.#binaryType = options.binaryType ?? "arraybuffer";
    this.#WebSocket = ctor;
    this.#open();
  }

  get state(): State {
    return this.#state;
  }

  /** Calls `listener` on each `open`, `reconnecting`, `close` or `error`; returns its remover. */
  on<K extends keyof ClientEvents>(event: K, listener: Listener<K>): () => void {
    this.#listeners[event].add(listener);
    return () => this.#listeners[event].delete(listener);
  }

  /**
   * Subscribes to `topic` and calls `handler` with each event. Resolves on the server's `ack`:
   * every event published after that reaches the handler. Rejects with an `OwlkeiError` when the
   * server refuses the topic.
   */
  async subscribe(topic: string, handler: EventHandler): Promise<Subscription> {
    if (!isTopic(topic)) throw new TypeError("owlkei: invalid topic");
    let entry = this.#topics.get(topic);
    if (entry === undefined) {
      entry = { handlers: new Set(), ready: Promise.resolve(), joined: false };
      const created = entry;
      created.ready = this.#join(topic, created);
      this.#topics.set(topic, created);
    }
    entry.handlers.add(handler);
    try {
      await entry.ready;
    } catch (err) {
      entry.handlers.delete(handler);
      throw err;
    }
    const joined = entry;
    return {
      topic,
      unsubscribe: async () => {
        if (!joined.handlers.delete(handler) || joined.handlers.size > 0) return;
        if (this.#topics.get(topic) === joined) this.#topics.delete(topic);
        if (this.#state === "closed") return;
        await this.#request({ type: "unsubscribe", topic });
      },
    };
  }

  /**
   * Publishes `data` to `topic`, which the connection must have subscribed to. Binary data
   * (`ArrayBuffer`, a typed array or `Blob`) goes in a binary frame, anything else as JSON. Resolves
   * with the server's reply value, `undefined` when it has none.
   */
  async publish(topic: string, data?: unknown): Promise<unknown> {
    if (!isTopic(topic)) throw new TypeError("owlkei: invalid topic");
    return this.#request({ type: "publish", topic }, data);
  }

  /** Sends a JSON ping and resolves on its pong. */
  async ping(): Promise<void> {
    await this.#request({ type: "ping" });
  }

  /** Closes the connection for good; pending requests reject with a `ConnectionError`. */
  close(): void {
    if (this.#state === "closed") return;
    this.#state = "closed";
    clearTimeout(this.#retry);
    this.#stopTimers();
    this.#failAll(new ConnectionError("owlkei: the client was closed"));
    const socket = this.#socket;
    this.#socket = undefined;
    socket?.close(1000);
    this.#emit("close", 1000);
  }

  async #join(topic: string, entry: Topic): Promise<void> {
    try {
      await this.#request({ type: "subscribe", topic });
      entry.joined = true;
    } catch (err) {
      if (this.#topics.get(topic) === entry) this.#topics.delete(topic);
      throw err;
    }
  }

  #request(envelope: Omit<Envelope, "id">, data?: unknown): Promise<unknown> {
    if (this.#state === "closed") {
      return Promise.reject(new ConnectionError("owlkei: the client was closed"));
    }
    if (this.#outbox.length >= MAX_QUEUED) {
      return Promise.reject(
        new ConnectionError("owlkei: too many messages waiting for a connection"),
      );
    }
    const id = String(++this.#seq);
    const withID: Envelope = { ...envelope, id };
    const build: Outgoing =
      data !== undefined && isBinaryData(data)
        ? () => encodeBinary(withID, data)
        : () => Promise.resolve(encodeText(data === undefined ? withID : { ...withID, data }));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(id);
        this.#dropQueued(id);
        reject(
          new TimeoutError(`owlkei: no answer to ${envelope.type} within the request timeout`),
        );
      }, this.#requestTimeoutMs);
      this.#pending.set(id, { resolve, reject, timer, sent: false });
      this.#outbox.push({ id, build });
      this.#flush();
    });
  }

  #open(): void {
    const socket = new this.#WebSocket(this.#url, PROTOCOL);
    socket.binaryType = "arraybuffer";
    this.#socket = socket;
    this.#lost = false;
    socket.onopen = () => {
      if (this.#socket !== socket) return;
      this.#state = "open";
      this.#attempt = 0;
      this.#startTimers();
      for (const [topic, entry] of this.#topics) {
        if (!entry.joined) continue;
        entry.joined = false;
        entry.ready = this.#join(topic, entry);
        entry.ready.catch((err: unknown) => {
          this.#emit("error", err);
        });
      }
      this.#flush();
      this.#emit("open");
    };
    socket.onmessage = (event) => {
      if (this.#socket !== socket) return;
      this.#touch();
      if (typeof event.data === "string") {
        const envelope = decodeText(event.data);
        if (envelope !== undefined) this.#receive({ envelope });
      } else if (event.data instanceof ArrayBuffer) {
        const incoming = decodeBinary(event.data);
        if (incoming !== undefined) this.#receive(incoming);
      }
    };
    socket.onerror = () => undefined;
    socket.onclose = (event) => {
      if (this.#socket !== socket) return;
      this.#closed(this.#lost ? ABNORMAL : event.code);
    };
  }

  #closed(code: number): void {
    this.#socket = undefined;
    this.#stopTimers();
    const error = new ConnectionError("owlkei: the connection closed", code);
    for (const [id, pending] of this.#pending) {
      if (!pending.sent) continue;
      clearTimeout(pending.timer);
      this.#pending.delete(id);
      pending.reject(error);
    }
    const plan = planFor(code);
    if (!this.#reconnect || plan.kind === "stop") {
      this.#state = "closed";
      this.#failAll(error);
      this.#emit("close", code);
      return;
    }
    const delay = delayFor(plan, code === LIFETIME ? 0 : this.#attempt, this.#backoff, Math.random);
    this.#attempt++;
    this.#state = "reconnecting";
    this.#emit("reconnecting", delay, code);
    this.#retry = setTimeout(() => {
      if (this.#state === "reconnecting") this.#open();
    }, delay);
  }

  #receive({ envelope, binary }: Incoming): void {
    switch (envelope.type) {
      case "ack":
      case "pong":
        this.#settle(envelope.id, (p) => {
          p.resolve(undefined);
        });
        return;
      case "reply":
        this.#settle(envelope.id, (p) => {
          p.resolve(binary === undefined ? envelope.data : this.#binary(binary));
        });
        return;
      case "error": {
        const error = new OwlkeiError(
          errorBody(envelope.error),
          envelope.requestId,
          envelope.retryAfter,
        );
        if (
          !this.#settle(envelope.id, (p) => {
            p.reject(error);
          })
        ) {
          this.#emit("error", error);
        }
        return;
      }
      case "event": {
        if (envelope.topic === undefined) return;
        const entry = this.#topics.get(envelope.topic);
        if (entry === undefined) return;
        const data = binary === undefined ? envelope.data : this.#binary(binary);
        for (const handler of [...entry.handlers]) {
          try {
            handler(data, envelope.topic);
          } catch (err) {
            this.#emit("error", err);
          }
        }
        return;
      }
      default:
        return;
    }
  }

  #settle(id: string | undefined, apply: (pending: Pending) => void): boolean {
    if (id === undefined) return false;
    const pending = this.#pending.get(id);
    if (pending === undefined) return false;
    clearTimeout(pending.timer);
    this.#pending.delete(id);
    apply(pending);
    return true;
  }

  #binary(data: ArrayBuffer): ArrayBuffer | Blob {
    return this.#binaryType === "blob" ? new Blob([data]) : data;
  }

  #flush(): void {
    const socket = this.#socket;
    if (socket === undefined || this.#state !== "open") return;
    const batch = this.#outbox.splice(0);
    void (async () => {
      for (const item of batch) {
        let frame: string | Uint8Array;
        try {
          frame = await item.build();
        } catch (err) {
          this.#settle(item.id, (p) => {
            p.reject(err);
          });
          continue;
        }
        if (this.#socket !== socket) {
          this.#outbox.push(item);
          continue;
        }
        const pending = item.id === undefined ? undefined : this.#pending.get(item.id);
        if (item.id !== undefined && pending === undefined) continue;
        if (pending !== undefined) pending.sent = true;
        socket.send(frame);
      }
    })();
  }

  #dropQueued(id: string): void {
    const index = this.#outbox.findIndex((item) => item.id === id);
    if (index >= 0) this.#outbox.splice(index, 1);
  }

  #failAll(error: Error): void {
    this.#outbox.length = 0;
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.#pending.clear();
  }

  #startTimers(): void {
    this.#touch();
    if (this.#heartbeatMs !== false) {
      this.#heartbeat = setInterval(() => {
        if (this.#state === "open") this.#socket?.send('{"type":"ping"}');
      }, this.#heartbeatMs);
    }
  }

  #stopTimers(): void {
    clearInterval(this.#heartbeat);
    clearTimeout(this.#idle);
    this.#heartbeat = undefined;
    this.#idle = undefined;
  }

  #touch(): void {
    clearTimeout(this.#idle);
    this.#idle = setTimeout(() => {
      this.#lost = true;
      this.#socket?.close();
    }, this.#idleTimeoutMs);
  }

  #emit<K extends keyof ClientEvents>(event: K, ...args: ClientEvents[K]): void {
    for (const listener of [...this.#listeners[event]]) {
      try {
        listener(...args);
      } catch (err) {
        queueMicrotask(() => {
          throw err;
        });
      }
    }
  }
}

/** Opens a client; the same as `new Client(url, options)`. */
export function connect(url: string, options?: ClientOptions): Client {
  return new Client(url, options);
}
