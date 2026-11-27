import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  OwlkeiError,
  connect,
  parsePairing,
  type Devtools,
  type DevtoolsOptions,
  type StreamEvent,
} from "../../src/devtools/index.js";
import { FakeSocket, settle } from "../ws/fake.js";
import { DevtoolsSocket, ring, ringEvent, type Server } from "./fake.js";

const TOKEN = "okd1_" + "Zk3".repeat(14) + "x";
const opened: Devtools[] = [];

async function drain(): Promise<void> {
  for (let i = 0; i < 10; i++) await settle();
}

function open(
  server: Server = () => undefined,
  options: Partial<DevtoolsOptions> = {},
): { dt: Devtools; socket: DevtoolsSocket } {
  DevtoolsSocket.server = server;
  const dt = connect({ token: TOKEN, WebSocket: DevtoolsSocket, heartbeatMs: false, ...options });
  opened.push(dt);
  const socket = DevtoolsSocket.current();
  socket.open();
  return { dt, socket };
}

beforeEach(() => {
  FakeSocket.reset();
  vi.useFakeTimers();
});

afterEach(() => {
  for (const dt of opened.splice(0)) dt.close();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("parsePairing", () => {
  it("reads the app and the token of the link the app prints", () => {
    const link = `https://devtools.owlkei.com/#app=http://localhost:3000&token=${TOKEN}`;
    expect(parsePairing(link)).toEqual({ app: "http://localhost:3000", token: TOKEN });
  });

  it("reads a fragment with or without its #", () => {
    const fragment = `app=http://127.0.0.1:8080&token=${TOKEN}`;
    expect(parsePairing(fragment)).toEqual({ app: "http://127.0.0.1:8080", token: TOKEN });
    expect(parsePairing("#" + fragment).app).toBe("http://127.0.0.1:8080");
    expect(parsePairing(`#app=https://[::1]:3443&token=${TOKEN}`).app).toBe("https://[::1]:3443");
  });

  it("refuses a link without a token", () => {
    expect(() => parsePairing("#app=http://localhost:3000")).toThrow(TypeError);
  });

  it("refuses a malformed token without echoing it", () => {
    const bad = "okd1_zz-LEAK";
    expect(() => parsePairing(`#app=http://localhost:3000&token=${bad}`)).toThrow(
      expect.not.objectContaining({ message: expect.stringContaining("zz-LEAK") as unknown }),
    );
    expect(() => parsePairing(`#app=http://localhost:3000&token=${bad}`)).toThrow(TypeError);
  });

  it.each([
    "http://evil.example:3000",
    "http://localhost.evil.example",
    "http://app.localhost:3000",
    "ftp://localhost",
    "http://localhost:3000/path",
    "http://user@localhost:3000",
    "not a url",
  ])("refuses the app %s (T7)", (app) => {
    expect(() => parsePairing(`#app=${app}&token=${TOKEN}`)).toThrow(TypeError);
  });
});

describe("connect", () => {
  it("offers owlkei.v1 and the token entry on the devtools path", () => {
    const { socket } = open();
    expect(socket.url).toBe("ws://localhost:3000/_owlkei/devtools");
    expect(socket.offered).toEqual(["owlkei.v1", `owlkei.bearer.${TOKEN}`]);
  });

  it("connects to wss for an https app", () => {
    const { socket } = open(undefined, { app: "https://127.0.0.1:3443" });
    expect(socket.url).toBe("wss://127.0.0.1:3443/_owlkei/devtools");
  });

  it("refuses a non-loopback app and a malformed token before opening", () => {
    expect(() =>
      connect({ app: "http://evil.example", token: TOKEN, WebSocket: DevtoolsSocket }),
    ).toThrow(TypeError);
    expect(() => connect({ token: "okd1_short", WebSocket: DevtoolsSocket })).toThrow(TypeError);
    expect(FakeSocket.instances).toHaveLength(0);
  });
});

describe("hello", () => {
  it("sends the token in Authorization and returns the hello", async () => {
    const hello = { protocol: 1, topics: ["snapshot"], sections: ["controller"] };
    const fetch = vi.fn(() => Promise.resolve(new Response(JSON.stringify(hello))));
    vi.stubGlobal("fetch", fetch);
    const { dt } = open();
    expect(await dt.hello()).toEqual(hello);
    expect(fetch).toHaveBeenCalledWith("http://localhost:3000/_owlkei/devtools", {
      headers: { Authorization: `Bearer ${TOKEN}` },
    });
  });

  it("rejects a refusal with the server's error body", async () => {
    const body = {
      code: 401,
      error: "Unauthorized",
      message: "Unauthorized",
      requestId: "0123456789abcdef",
    };
    vi.stubGlobal("fetch", () =>
      Promise.resolve(new Response(JSON.stringify(body), { status: 401 })),
    );
    const { dt } = open();
    const err = await dt.hello().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(OwlkeiError);
    expect(err).toMatchObject({ code: 401, requestId: "0123456789abcdef" });
  });
});

describe("section", () => {
  it("subscribes to snapshot, then joins the chunks and parses once", async () => {
    const text = JSON.stringify({ http: [{ route: "/é" }, { route: "/notes" }] });
    const cut = text.indexOf("é") + 1;
    const head = text.slice(0, cut);
    const size = new TextEncoder().encode(text).byteLength;
    const headBytes = new TextEncoder().encode(head).byteLength;
    const { dt, socket } = open((topic, data) => {
      const d = data as { offset?: number; id?: number };
      if (d.offset === undefined) {
        return { reply: { schema: 1, section: "controller", id: 7, size, offset: 0, text: head } };
      }
      return {
        reply: {
          schema: 1,
          section: "controller",
          id: 7,
          size,
          offset: d.offset,
          text: text.slice(cut),
        },
      };
    });
    expect(await dt.section("controller")).toEqual(JSON.parse(text));
    expect(socket.sentJSON()[0]).toMatchObject({ type: "subscribe", topic: "snapshot" });
    expect(socket.published("snapshot").map((m) => m["data"])).toEqual([
      { section: "controller" },
      { section: "controller", id: 7, offset: headBytes },
    ]);
  });

  it("starts again once at offset 0 when the kept text is gone (409)", async () => {
    let id = 0;
    const { dt, socket } = open((_, data) => {
      const d = data as { offset?: number; id?: number };
      if (d.offset === undefined) {
        id++;
        return { reply: { schema: 1, section: "jobs", id, size: 7, offset: 0, text: '{"a":' } };
      }
      if (d.id === 1) return { error: 409 };
      return { reply: { schema: 1, section: "jobs", id: d.id, size: 7, offset: 5, text: "1}" } };
    });
    expect(await dt.section("jobs")).toEqual({ a: 1 });
    expect(socket.published("snapshot").map((m) => m["data"])).toEqual([
      { section: "jobs" },
      { section: "jobs", id: 1, offset: 5 },
      { section: "jobs" },
      { section: "jobs", id: 2, offset: 5 },
    ]);
  });

  it("rejects the second 409 with the OwlkeiError", async () => {
    const { dt } = open((_, data) => {
      const d = data as { offset?: number };
      if (d.offset === undefined) {
        return { reply: { schema: 1, section: "jobs", id: 1, size: 7, offset: 0, text: '{"a":' } };
      }
      return { error: 409 };
    });
    const err = await dt.section("jobs").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(OwlkeiError);
    expect(err).toMatchObject({ code: 409 });
  });

  it("returns null for a section the app does not have", async () => {
    const { dt } = open(() => ({
      reply: { schema: 1, section: "storage", id: 1, size: 4, offset: 0, text: "null" },
    }));
    expect(await dt.section("storage")).toBeNull();
  });
});

describe("stream", () => {
  it("delivers live events in seq order and once each", async () => {
    const { dt, socket } = open();
    const got: StreamEvent[] = [];
    await dt.stream("requests", (e) => got.push(e));
    socket.event("requests", ringEvent(1));
    socket.event("requests", ringEvent(2));
    socket.event("requests", ringEvent(2));
    socket.event("requests", ringEvent(1));
    socket.event("requests", { schema: 1 });
    socket.event("requests", ringEvent(3));
    expect(got.map((e) => e.seq)).toEqual([1, 2, 3]);
  });

  it("with recent, reads the newest events of the ring first and drops duplicates by seq", async () => {
    const reads: unknown[] = [];
    const { dt } = open((_, data) => {
      reads.push(data);
      const d = data as { after?: number; limit?: number };
      if (d.limit === 1) return ring([ringEvent(5)], 5, 9);
      DevtoolsSocket.current().event("errors", ringEvent(9));
      DevtoolsSocket.current().event("errors", ringEvent(10));
      return ring([ringEvent(7), ringEvent(8), ringEvent(9)], 5, 9);
    });
    const got: number[] = [];
    await dt.stream("errors", (e) => got.push(e.seq), { recent: 3 });
    expect(reads).toEqual([{ limit: 1 }, { after: 6, limit: 100 }]);
    expect(got).toEqual([7, 8, 9, 10]);
  });

  it("pages the ring while more is true", async () => {
    const { dt } = open((_, data) => {
      const d = data as { after?: number; limit?: number };
      if (d.limit === 1) return ring([ringEvent(1)], 1, 3);
      if (d.after === 0) return ring([ringEvent(1), ringEvent(2)], 1, 3, true);
      return ring([ringEvent(3)], 1, 3);
    });
    const got: number[] = [];
    await dt.stream("jobs", (e) => got.push(e.seq), { recent: 10 });
    expect(got).toEqual([1, 2, 3]);
  });

  it("after a reconnect, reads the ring from the last seq it received", async () => {
    const reads: unknown[] = [];
    const { dt, socket } = open((_, data) => {
      reads.push(data);
      return ring([ringEvent(3), ringEvent(4)], 1, 4);
    });
    const got: number[] = [];
    await dt.stream("requests", (e) => got.push(e.seq));
    socket.event("requests", ringEvent(1));
    socket.event("requests", ringEvent(2));
    socket.drop(1001);
    await vi.advanceTimersByTimeAsync(31_000);
    const next = DevtoolsSocket.current();
    expect(next).not.toBe(socket);
    next.open();
    await drain();
    expect(next.sentJSON()[0]).toMatchObject({ type: "subscribe", topic: "requests" });
    expect(reads).toEqual([{ after: 2, limit: 100 }]);
    expect(got).toEqual([1, 2, 3, 4]);
  });

  it("after a reconnect to a restarted app (a lower last), reads its ring from the start", async () => {
    const reads: unknown[] = [];
    const { dt, socket } = open((_, data) => {
      reads.push(data);
      return ring([ringEvent(1)], 1, 1);
    });
    const got: number[] = [];
    await dt.stream("connections", (e) => got.push(e.seq));
    socket.event("connections", ringEvent(40));
    socket.drop(1006);
    await vi.advanceTimersByTimeAsync(31_000);
    DevtoolsSocket.current().open();
    await drain();
    expect(reads).toEqual([
      { after: 40, limit: 100 },
      { after: 0, limit: 100 },
    ]);
    expect(got).toEqual([40, 1]);
  });
});

describe("recent", () => {
  it("subscribes, then reads the ring with the given bounds", async () => {
    const { dt, socket } = open(() => ring([ringEvent(3)], 1, 3));
    const r = await dt.recent("storage", { after: 2, limit: 5 });
    expect(r).toEqual({ schema: 1, first: 1, last: 3, events: [ringEvent(3)], more: false });
    expect(socket.sentJSON().map((m) => [m["type"], m["topic"], m["data"]])).toEqual([
      ["subscribe", "storage", undefined],
      ["publish", "storage", { after: 2, limit: 5 }],
    ]);
  });
});

describe("runJob", () => {
  it("subscribes to run-job, then publishes the name", async () => {
    const { dt, socket } = open(() => ({
      reply: { schema: 1, status: "started", run_id: "9f86d081884c7d65" },
    }));
    expect(await dt.runJob("tick")).toEqual({
      schema: 1,
      status: "started",
      run_id: "9f86d081884c7d65",
    });
    expect(socket.published("run-job").map((m) => m["data"])).toEqual([{ name: "tick" }]);
  });
});

describe("onChange", () => {
  it("calls the handler with the section and its value, undefined when left out", async () => {
    const { dt, socket } = open();
    const got: unknown[] = [];
    await dt.onChange((section, value) => got.push([section, value]));
    socket.event("snapshot", { schema: 1, section: "jobs", value: { items: [] } });
    socket.event("snapshot", { schema: 1, section: "modules" });
    expect(got).toEqual([
      ["jobs", { items: [] }],
      ["modules", undefined],
    ]);
  });
});
