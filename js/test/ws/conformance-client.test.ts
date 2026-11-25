import { afterEach, describe, expect, it } from "vitest";

import {
  Client,
  OwlkeiError,
  type ClientOptions,
  type EventHandler,
  type WebSocketConstructor,
  type WebSocketLike,
} from "../../src/ws/index.js";

interface Received {
  readonly data: unknown;
  readonly topic: string;
}

const url = process.env["OWLKEI_CONFORMANCE_URL"];
const budgetURL = process.env["OWLKEI_CONFORMANCE_BUDGET_URL"];
const BUDGET_CONNECTIONS = 51;
const requestID = /^[0-9a-f]{16}$/;
const clients: Client[] = [];

function open(options: ClientOptions = {}, at = url): Client {
  const client = new Client(at ?? "", { heartbeatMs: false, ...options });
  clients.push(client);
  return client;
}

function inbox(): { readonly handler: EventHandler; next(): Promise<Received> } {
  const queued: Received[] = [];
  const waiting: ((r: Received) => void)[] = [];
  return {
    handler: (data, topic) => {
      const waiter = waiting.shift();
      if (waiter === undefined) queued.push({ data, topic });
      else waiter({ data, topic });
    },
    next: () => {
      const first = queued.shift();
      if (first !== undefined) return Promise.resolve(first);
      return new Promise((resolve) => waiting.push(resolve));
    },
  };
}

async function refusal(promise: Promise<unknown>): Promise<OwlkeiError> {
  try {
    await promise;
  } catch (err) {
    if (err instanceof OwlkeiError) return err;
    throw err;
  }
  throw new Error("the server accepted a message the conformance app refuses");
}

function once(client: Client, event: "open" | "reconnecting"): Promise<number | undefined> {
  return new Promise((resolve) => {
    const off = client.on(event, (...args: unknown[]) => {
      off();
      resolve(typeof args[1] === "number" ? args[1] : undefined);
    });
  });
}

function reconnecting(client: Client): Promise<{ delayMs: number; closeCode: number }> {
  return new Promise((resolve) => {
    const off = client.on("reconnecting", (delayMs, closeCode) => {
      off();
      resolve({ delayMs, closeCode });
    });
  });
}

function filler(at: string): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(at, "owlkei.v1");
    socket.onopen = () => {
      resolve(socket);
    };
    socket.onerror = () => {
      reject(new Error("the budget app refused a filler connection"));
    };
  });
}

afterEach(() => {
  for (const client of clients.splice(0)) client.close();
});

describe.skipIf(url === undefined)("Client against OWLKEI_CONFORMANCE_URL", () => {
  it("resolves a subscribe on the ack and a publish with the reply", async () => {
    const client = open();
    const sub = await client.subscribe("echo", () => undefined);
    expect(sub.topic).toBe("echo");
    expect(client.state).toBe("open");
    await expect(client.publish("echo", { text: "hi", n: [1, 2] })).resolves.toEqual({
      text: "hi",
      n: [1, 2],
    });
    await expect(client.ping()).resolves.toBeUndefined();
  });

  it("resolves a publish without a reply value with undefined", async () => {
    const client = open();
    await client.subscribe("slow", () => undefined);
    await expect(client.publish("slow", {})).resolves.toBeUndefined();
  });

  it("rejects a failed validation with the fields of the 422", async () => {
    const client = open();
    await client.subscribe("validated", () => undefined);
    await expect(client.publish("validated", { name: "An" })).resolves.toEqual({ name: "An" });
    const err = await refusal(client.publish("validated", {}));
    expect(err.code).toBe(422);
    expect(err.status).toBe("Unprocessable Entity");
    expect(err.fields).toEqual({ name: { rule: "required", message: "This field is required" } });
    expect(err.requestId).toMatch(requestID);
    expect(err.retryAfter).toBeUndefined();
  });

  it.each([
    ["nowhere", 404, "Not Found", false],
    ["guarded", 403, "Forbidden", false],
    ["pending", 503, "Service Unavailable", true],
    ["failed", 503, "Service Unavailable", false],
  ] as const)("rejects a subscribe to %s with %i", async (topic, code, message, hasRetry) => {
    const client = open({ requestTimeoutMs: 1000 });
    const err = await refusal(client.subscribe(topic, () => undefined));
    expect(err.code).toBe(code);
    expect(err.message).toBe(message);
    expect(err.fields).toBeUndefined();
    expect(err.requestId).toMatch(requestID);
    expect(err.retryAfter !== undefined).toBe(hasRetry);
    await expect(client.ping()).resolves.toBeUndefined();
  });

  it("rejects a publish to a topic that only listens with 405", async () => {
    const client = open();
    await client.subscribe("listen", () => undefined);
    const err = await refusal(client.publish("listen", {}));
    expect(err.code).toBe(405);
  });

  it("delivers a fan-out to every subscribed connection", async () => {
    const reader = open();
    const writer = open();
    const read = inbox();
    const own = inbox();
    await reader.subscribe("rooms.7.messages", read.handler);
    await writer.subscribe("rooms.7.messages", own.handler);
    await expect(writer.publish("rooms.7.messages", { text: "hi" })).resolves.toBeUndefined();
    const want = { data: { text: "hi" }, topic: "rooms.7.messages" };
    await expect(read.next()).resolves.toEqual(want);
    await expect(own.next()).resolves.toEqual(want);
  });

  it("unsubscribes the connection only with the topic's last handler", async () => {
    const client = open();
    const first = await client.subscribe("echo", () => undefined);
    const second = await client.subscribe("echo", () => undefined);
    await first.unsubscribe();
    await expect(client.publish("echo", { a: 1 })).resolves.toEqual({ a: 1 });
    await second.unsubscribe();
    const err = await refusal(client.publish("echo", { a: 1 }));
    expect(err.code).toBe(409);
    expect(err.message).toBe("Not subscribed");
  });

  it("round-trips binary data and refuses it on a JSON topic", async () => {
    const client = open();
    await client.subscribe("binary", () => undefined);
    const reply = await client.publish("binary", new Uint8Array([1, 2, 3]));
    expect(reply).toBeInstanceOf(ArrayBuffer);
    expect([...new Uint8Array(reply as ArrayBuffer)]).toEqual([1, 2, 3]);
    expect((await refusal(client.publish("binary", {}))).code).toBe(415);
    await client.subscribe("echo", () => undefined);
    expect((await refusal(client.publish("echo", new Uint8Array([120])))).code).toBe(415);
  });

  it("subscribes again after a lost connection", async () => {
    const client = open({ idleTimeoutMs: 300, backoff: { minMs: 10, maxMs: 20 } });
    const events = inbox();
    await client.subscribe("rooms.8.messages", events.handler);
    const lost = once(client, "reconnecting");
    await expect(lost).resolves.toBe(1006);
    await once(client, "open");
    await expect(client.publish("rooms.8.messages", { text: "again" })).resolves.toBeUndefined();
    await expect(events.next()).resolves.toEqual({
      data: { text: "again" },
      topic: "rooms.8.messages",
    });
  });
});

describe.skipIf(budgetURL === undefined)("Client against OWLKEI_CONFORMANCE_BUDGET_URL", () => {
  it("reconnects after a 1013 close and subscribes again", async () => {
    const client = open({ backoff: { minMs: 10, maxMs: 20 } }, budgetURL);
    await client.subscribe("echo", () => undefined);
    const fillers = await Promise.all(
      Array.from({ length: BUDGET_CONNECTIONS - 1 }, () => filler(budgetURL ?? "")),
    );
    try {
      const lost = reconnecting(client);
      client.ping().catch(() => undefined);
      await expect(lost).resolves.toMatchObject({ closeCode: 1013 });
      expect((await lost).delayMs).toBeGreaterThanOrEqual(1000);
    } finally {
      for (const socket of fillers) socket.close(1000);
    }
    await once(client, "open");
    await client.subscribe("echo", () => undefined);
    await expect(client.publish("echo", { text: "again" })).resolves.toEqual({ text: "again" });
  }, 15_000);
});

function sessionBase(at: string): string {
  return at.replace(/^ws/, "http").replace(/\/ws$/, "");
}

async function post(at: string, path: string, token?: string): Promise<Response> {
  const headers: Record<string, string> =
    token === undefined ? {} : { Authorization: `Bearer ${token}` };
  return fetch(sessionBase(at) + path, { method: "POST", headers });
}

async function login(at: string): Promise<string> {
  const response = await post(at, "/session/login");
  expect(response.status).toBe(200);
  const body = (await response.json()) as { token: string };
  expect(body.token).toMatch(/^oks1_[A-Za-z0-9_-]{43}$/);
  return body.token;
}

function signedIn(token: string): WebSocketConstructor {
  const Base = WebSocket as unknown as new (url: string, init: unknown) => WebSocketLike;
  class Authenticated extends Base {
    constructor(address: string, protocols: string) {
      super(address, { protocols, headers: { Authorization: `Bearer ${token}` } });
    }
  }
  return Authenticated;
}

describe.skipIf(url === undefined)("Client sessions against OWLKEI_CONFORMANCE_URL", () => {
  it("is refused ~me with a 401 while anonymous and stays connected", async () => {
    const client = open();
    const err = await refusal(client.subscribe("~me", () => undefined));
    expect(err.code).toBe(401);
    expect(err.message).toBe("Unauthorized");
    expect(client.state).toBe("open");
    await expect(client.ping()).resolves.toBeUndefined();
  });

  it("receives the events the server sends to its user on ~me", async () => {
    const token = await login(url ?? "");
    const client = open({ WebSocket: signedIn(token) });
    const events = inbox();
    await client.subscribe("~me", events.handler);
    expect((await post(url ?? "", "/session/notify", token)).status).toBe(204);
    await expect(events.next()).resolves.toEqual({ data: { hello: "you" }, topic: "~me" });
  });

  it("is closed with 4401 when its session ends and does not reconnect", async () => {
    const token = await login(url ?? "");
    const client = open({ WebSocket: signedIn(token), backoff: { minMs: 10, maxMs: 20 } });
    await client.subscribe("~me", () => undefined);
    const reconnects: number[] = [];
    client.on("reconnecting", (_delayMs, closeCode) => reconnects.push(closeCode));
    const closed = new Promise<number>((resolve) => {
      client.on("close", resolve);
    });

    expect((await post(url ?? "", "/session/logout", token)).status).toBe(204);

    await expect(closed).resolves.toBe(4401);
    expect(client.state).toBe("closed");
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(reconnects).toEqual([]);
    expect((await post(url ?? "", "/session/notify", token)).status).toBe(401);
  });
});
