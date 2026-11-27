import {
  Client,
  type ClientOptions,
  type Subscription,
  type WebSocketConstructor,
  type WebSocketLike,
} from "../ws/client.js";
import { OwlkeiError, errorBody } from "../ws/errors.js";
import { DEFAULT_APP, appOrigin, checkedToken } from "./pairing.js";

const PATH = "/_owlkei/devtools";
const PAIRING_PROTOCOL = "owlkei.bearer.";
const GONE = 409;
const RING_LIMIT = 100;

const utf8 = new TextEncoder();

/** A section of the app's snapshot (spec S1). */
export type SectionName =
  | "pre_route"
  | "controller"
  | "render"
  | "modules"
  | "wiring"
  | "ws"
  | "auth"
  | "storage"
  | "jobs"
  | "httpclient";

/** A live stream of the app (spec L1-L5). */
export type StreamName = "requests" | "errors" | "jobs" | "storage" | "connections";

/** The app's answer to `GET /_owlkei/devtools` (spec V5). */
export interface Hello {
  readonly protocol: number;
  readonly owlkei: string;
  readonly go: string;
  readonly module: string;
  readonly pid: number;
  readonly started_at: string;
  readonly topics: readonly string[];
  readonly sections: readonly SectionName[];
}

/** One event of a stream: the common members, and the stream's own (spec L). */
export interface StreamEvent {
  readonly schema: number;
  readonly seq: number;
  readonly at: string;
  readonly skipped?: number;
  readonly [member: string]: unknown;
}

/** A read of a stream's ring (spec V7): `first > after + 1` means events were lost. */
export interface Recent {
  readonly schema: number;
  readonly first: number;
  readonly last: number;
  readonly events: readonly StreamEvent[];
  readonly more: boolean;
}

/** The answer to `runJob` (spec S8); `run_id` is present only with `started`. */
export interface RunJob {
  readonly schema: number;
  readonly status: string;
  readonly run_id?: string;
}

/** Opens the devtools WebSocket with a list of subprotocols; the platform's `WebSocket` does. */
export type DevtoolsWebSocketConstructor = new (url: string, protocols: string[]) => WebSocketLike;

export interface DevtoolsOptions extends Omit<ClientOptions, "WebSocket"> {
  /** The app's origin, on a loopback host. Default: `http://localhost:3000`. */
  readonly app?: string;
  /** The pairing token, `okd1_` and 43 base64url characters. */
  readonly token: string;
  /** The WebSocket implementation. Default: `globalThis.WebSocket`. */
  readonly WebSocket?: DevtoolsWebSocketConstructor;
}

/** A paired connection to an app's devtools. */
export interface Devtools {
  /** The `owlkei.v1` connection underneath: `state`, `on("open" | "close" …)`. */
  readonly client: Client;
  /** Reads the hello over HTTP; a refusal (401 for a wrong token) rejects with an `OwlkeiError`. */
  hello(): Promise<Hello>;
  /** Reads a section, joining its chunks; `null` for a section the app does not have. */
  section(name: SectionName): Promise<unknown>;
  /** Calls `handler` with each section's new value (`undefined` when too large to carry). */
  onChange(handler: (section: SectionName, value: unknown) => void): Promise<Subscription>;
  /**
   * Calls `handler` with each event of a stream, in `seq` order and once each. With `recent`, the
   * newest `recent` events of the ring come first. After a reconnect the ring is read from the
   * last `seq` received, so nothing the ring still holds is missed.
   */
  stream(
    name: StreamName,
    handler: (event: StreamEvent) => void,
    options?: { readonly recent?: number },
  ): Promise<Subscription>;
  /** Reads a stream's ring: the events after `after` (default 0), at most `limit` (1 to 100). */
  recent(
    name: StreamName,
    options?: { readonly after?: number; readonly limit?: number },
  ): Promise<Recent>;
  /** Runs a job now, through its guards (spec S8, S9). */
  runJob(name: string): Promise<RunJob>;
  /** Closes the connection for good. */
  close(): void;
}

interface Stream {
  readonly name: StreamName;
  readonly handler: (event: StreamEvent) => void;
  readonly onEvent: (data: unknown) => void;
  last: number;
  buffer: StreamEvent[] | undefined;
  isStarted: boolean;
}

const ignore = (): void => undefined;

/**
 * Pairs with an app's devtools: the WebSocket offers `owlkei.v1` and `owlkei.bearer.<token>`, the
 * hello sends `Authorization: Bearer <token>`. Throws a `TypeError` for an app that is not a
 * loopback origin or a malformed token, before anything is sent.
 */
export function connect(options: DevtoolsOptions): Devtools {
  return new DevtoolsClient(options);
}

class DevtoolsClient implements Devtools {
  readonly client: Client;
  readonly #app: string;
  readonly #token: string;
  readonly #streams = new Set<Stream>();
  #isOpened = false;

  constructor(options: DevtoolsOptions) {
    const { app, token, WebSocket: base, ...rest } = options;
    this.#app = appOrigin(app ?? DEFAULT_APP);
    this.#token = checkedToken(token);
    const ctor =
      base ?? (globalThis as unknown as { WebSocket?: DevtoolsWebSocketConstructor }).WebSocket;
    if (ctor === undefined) {
      throw new TypeError("owlkei: no WebSocket implementation; pass options.WebSocket");
    }
    const bearer = PAIRING_PROTOCOL + this.#token;
    const withToken = function (url: string, protocol: string): WebSocketLike {
      return new ctor(url, [protocol, bearer]);
    } as unknown as WebSocketConstructor;
    this.client = new Client(this.#app.replace(/^http/, "ws") + PATH, {
      ...rest,
      WebSocket: withToken,
    });
    this.client.on("open", () => {
      if (this.#isOpened) {
        for (const s of this.#streams) if (s.isStarted) void this.#resume(s);
      }
      this.#isOpened = true;
    });
  }

  async hello(): Promise<Hello> {
    const response = await fetch(this.#app + PATH, {
      headers: { Authorization: `Bearer ${this.#token}` },
    });
    const body: unknown = await response.json().catch(() => undefined);
    if (!response.ok) {
      throw new OwlkeiError(
        isRecord(body)
          ? errorBody(body)
          : { code: response.status, error: response.statusText, message: response.statusText },
      );
    }
    if (!isRecord(body) || typeof body["protocol"] !== "number") {
      throw new Error("owlkei: the app sent a malformed hello");
    }
    return body as unknown as Hello;
  }

  async section(name: SectionName): Promise<unknown> {
    await this.#ensure("snapshot");
    try {
      return await this.#readSection(name);
    } catch (err) {
      if (!(err instanceof OwlkeiError) || err.code !== GONE) throw err;
      return this.#readSection(name);
    }
  }

  onChange(handler: (section: SectionName, value: unknown) => void): Promise<Subscription> {
    return this.client.subscribe("snapshot", (data) => {
      if (isRecord(data) && typeof data["section"] === "string") {
        handler(data["section"] as SectionName, data["value"]);
      }
    });
  }

  async stream(
    name: StreamName,
    handler: (event: StreamEvent) => void,
    options: { readonly recent?: number } = {},
  ): Promise<Subscription> {
    const recent = options.recent ?? 0;
    const s: Stream = {
      name,
      handler,
      onEvent: (data) => {
        const event = asEvent(data);
        if (event === undefined) return;
        if (s.buffer === undefined) deliver(s, event);
        else s.buffer.push(event);
      },
      last: 0,
      buffer: recent > 0 ? [] : undefined,
      isStarted: false,
    };
    const subscription = await this.client.subscribe(name, s.onEvent);
    this.#streams.add(s);
    try {
      if (recent > 0) {
        const head = await this.recent(name, { limit: 1 });
        await this.#catchUp(s, Math.max(0, head.last - recent));
      }
    } catch (err) {
      this.#streams.delete(s);
      await subscription.unsubscribe().catch(ignore);
      throw err;
    } finally {
      flush(s);
      s.isStarted = true;
    }
    return {
      topic: subscription.topic,
      unsubscribe: () => {
        this.#streams.delete(s);
        return subscription.unsubscribe();
      },
    };
  }

  async recent(
    name: StreamName,
    options: { readonly after?: number; readonly limit?: number } = {},
  ): Promise<Recent> {
    await this.#ensure(name);
    const data: Record<string, number> = {};
    if (options.after !== undefined) data["after"] = options.after;
    if (options.limit !== undefined) data["limit"] = options.limit;
    const reply = await this.client.publish(name, data);
    if (!isRecord(reply) || !Array.isArray(reply["events"]) || typeof reply["last"] !== "number") {
      throw new Error("owlkei: the app sent a malformed ring");
    }
    return reply as unknown as Recent;
  }

  async runJob(name: string): Promise<RunJob> {
    await this.#ensure("run-job");
    const reply = await this.client.publish("run-job", { name });
    if (!isRecord(reply) || typeof reply["status"] !== "string") {
      throw new Error("owlkei: the app sent a malformed run-job reply");
    }
    return reply as unknown as RunJob;
  }

  close(): void {
    this.#streams.clear();
    this.client.close();
  }

  async #ensure(topic: string): Promise<void> {
    await this.client.subscribe(topic, ignore);
  }

  async #readSection(name: SectionName): Promise<unknown> {
    const first = sectionReply(await this.client.publish("snapshot", { section: name }), name, 0);
    const parts = [first.text];
    let offset = utf8.encode(first.text).byteLength;
    while (offset < first.size) {
      const next = sectionReply(
        await this.client.publish("snapshot", { section: name, id: first.id, offset }),
        name,
        offset,
      );
      const length = utf8.encode(next.text).byteLength;
      if (next.id !== first.id || length === 0) {
        throw new Error("owlkei: the app sent a malformed snapshot chunk");
      }
      parts.push(next.text);
      offset += length;
    }
    if (offset !== first.size) throw new Error("owlkei: the app sent a malformed snapshot chunk");
    return JSON.parse(parts.join(""));
  }

  async #resume(s: Stream): Promise<void> {
    s.buffer = [];
    try {
      await this.client.subscribe(s.name, s.onEvent);
      await this.#catchUp(s, s.last);
    } catch {
      return;
    } finally {
      flush(s);
    }
  }

  async #catchUp(s: Stream, from: number): Promise<void> {
    let after = from;
    for (;;) {
      const ring = await this.recent(s.name, { after, limit: RING_LIMIT });
      if (ring.last < s.last) {
        s.last = 0;
        if (after > 0) {
          after = 0;
          continue;
        }
      }
      for (const event of ring.events) deliver(s, event);
      const tail = ring.events.at(-1);
      if (!ring.more || tail === undefined) return;
      after = tail.seq;
    }
  }
}

interface SectionReply {
  readonly id: number;
  readonly size: number;
  readonly text: string;
}

function sectionReply(reply: unknown, name: SectionName, offset: number): SectionReply {
  if (
    !isRecord(reply) ||
    reply["section"] !== name ||
    reply["offset"] !== offset ||
    typeof reply["id"] !== "number" ||
    typeof reply["size"] !== "number" ||
    typeof reply["text"] !== "string"
  ) {
    throw new Error("owlkei: the app sent a malformed snapshot chunk");
  }
  return { id: reply["id"], size: reply["size"], text: reply["text"] };
}

function deliver(s: Stream, event: StreamEvent): void {
  if (event.seq <= s.last) return;
  s.last = event.seq;
  try {
    s.handler(event);
  } catch (err) {
    queueMicrotask(() => {
      throw err;
    });
  }
}

function flush(s: Stream): void {
  const buffered = s.buffer ?? [];
  s.buffer = undefined;
  buffered.sort((a, b) => a.seq - b.seq);
  for (const event of buffered) deliver(s, event);
}

function asEvent(data: unknown): StreamEvent | undefined {
  return isRecord(data) && typeof data["seq"] === "number"
    ? (data as unknown as StreamEvent)
    : undefined;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
