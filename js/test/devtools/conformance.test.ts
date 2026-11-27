import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  OwlkeiError,
  connect,
  type Devtools,
  type SectionName,
  type StreamEvent,
  type StreamName,
} from "../../src/devtools/index.js";
import { Client } from "../../src/ws/index.js";
import { runSession, sessionVectors } from "../session.js";

const app = process.env["OWLKEI_CONFORMANCE_DEVTOOLS_URL"];
const token = process.env["OWLKEI_CONFORMANCE_DEVTOOLS_TOKEN"] ?? "";
const vectors = sessionVectors("devtools");
const waitMs = 5000;
const SECTIONS: readonly SectionName[] = [
  "pre_route",
  "controller",
  "render",
  "modules",
  "wiring",
  "ws",
  "auth",
  "storage",
  "jobs",
  "httpclient",
];

function inbox<T>(fits: (value: T) => boolean): {
  readonly handler: (value: T) => void;
  next(label: string): Promise<T>;
} {
  const queued: T[] = [];
  let wake: (() => void) | undefined;
  return {
    handler: (value) => {
      if (!fits(value)) return;
      queued.push(value);
      wake?.();
    },
    next: async (label) => {
      const deadline = Date.now() + waitMs;
      for (;;) {
        const first = queued.shift();
        if (first !== undefined) return first;
        const remaining = deadline - Date.now();
        if (remaining <= 0)
          throw new Error(`${label}: nothing arrived within ${String(waitMs)} ms`);
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, remaining);
          wake = () => {
            clearTimeout(timer);
            wake = undefined;
            resolve();
          };
        });
      }
    },
  };
}

async function watch(
  dt: Devtools,
  name: StreamName,
  fits: (e: StreamEvent) => boolean,
): Promise<{ next(label: string): Promise<StreamEvent>; stop(): Promise<void> }> {
  const box = inbox(fits);
  const sub = await dt.stream(name, box.handler);
  return { next: (label) => box.next(label), stop: () => sub.unsubscribe() };
}

describe.skipIf(app === undefined)(
  "devtools conformance against OWLKEI_CONFORMANCE_DEVTOOLS_URL",
  () => {
    const origin = app ?? "";
    const wsURL = origin.replace(/^http/, "ws") + "/_owlkei/devtools";
    let dt: Devtools;

    beforeAll(() => {
      dt = connect({ app: origin, token, heartbeatMs: false });
    });

    afterAll(() => {
      dt.close();
    });

    it("has every session vector", () => {
      expect(vectors.length).toBeGreaterThanOrEqual(9);
    });

    it.each(vectors.map((v) => [v.name, v] as const))(
      "vector %s",
      async (_, vector) => {
        await runSession(wsURL, vector, ["owlkei.v1", `owlkei.bearer.${token}`]);
      },
      15_000,
    );

    it("reads the hello with the token", async () => {
      const hello = await dt.hello();
      expect(hello.protocol).toBe(1);
      expect(hello.topics).toEqual([
        "snapshot",
        "requests",
        "errors",
        "jobs",
        "storage",
        "connections",
        "run-job",
      ]);
      expect(hello.sections).toContain("jobs");
    });

    it("refuses the hello with a wrong token: 401", async () => {
      const wrong = connect({ app: origin, token: "okd1_" + "A".repeat(43), reconnect: false });
      try {
        const err = await wrong.hello().catch((e: unknown) => e);
        expect(err).toBeInstanceOf(OwlkeiError);
        expect((err as OwlkeiError).code).toBe(401);
      } finally {
        wrong.close();
      }
    });

    it("reads every section as JSON, the controller one in chunks", async () => {
      for (const name of SECTIONS) {
        const value = await dt.section(name);
        expect(value === null || typeof value === "object", name).toBe(true);
      }
      const controller = (await dt.section("controller")) as { http: unknown[] };
      expect(controller.http.length).toBeGreaterThan(200);
      expect(await dt.section("storage")).toBeNull();
    });

    it("streams a request with its route pattern", async () => {
      const requests = await watch(dt, "requests", (e) => e["route"] === "/session/login");
      try {
        const response = await fetch(`${origin}/session/login`, { method: "POST" });
        expect(response.status).toBe(200);
        const e = await requests.next("requests");
        expect(e).toMatchObject({
          schema: 1,
          transport: "HTTP",
          operation: "POST",
          route: "/session/login",
          code: 200,
        });
        expect(e.seq).toBeGreaterThan(0);
        expect(typeof e["handler"]).toBe("string");
      } finally {
        await requests.stop();
      }
    });

    it("streams GET /boom on errors as an error, then a refusal with the same id", async () => {
      const errors = await watch(dt, "errors", (e) => e["route"] === "/boom");
      try {
        const response = await fetch(`${origin}/boom`);
        expect(response.status).toBe(500);
        const failure = await errors.next("errors: error");
        const refusal = await errors.next("errors: refusal");
        expect(failure).toMatchObject({ kind: "error", transport: "HTTP", code: 500 });
        expect(typeof failure["error"]).toBe("string");
        expect(refusal).toMatchObject({ kind: "refusal", id: failure["id"], code: 500 });
        expect(refusal.seq).toBeGreaterThan(failure.seq);
      } finally {
        await errors.stop();
      }
    });

    it("runs the job tick: a jobs run event and a snapshot change", async () => {
      const jobs = await watch(dt, "jobs", (e) => e["kind"] === "run" && e["name"] === "tick");
      const changes = inbox<[SectionName, unknown]>(([section]) => section === "jobs");
      const sub = await dt.onChange((section, value) => {
        changes.handler([section, value]);
      });
      try {
        const run = await dt.runJob("tick");
        expect(run.status).toBe("started");
        const e = await jobs.next("jobs");
        expect(e).toMatchObject({ trigger: "devtool", result: "ok", id: run.run_id });
        const [, value] = await changes.next("snapshot change");
        expect(value).toMatchObject({ items: [{ name: "tick" }] });
      } finally {
        await sub.unsubscribe();
        await jobs.stop();
      }
    });

    it("streams a connection to /ws as open, then close", async () => {
      let opened: string | undefined;
      const connections = await watch(dt, "connections", (e) => {
        if (e["kind"] === "open" && opened === undefined) {
          opened = String(e["connection"]);
          return true;
        }
        return e["kind"] === "close" && e["connection"] === opened;
      });
      const ws = new Client(origin.replace(/^http/, "ws") + "/ws", {
        reconnect: false,
        heartbeatMs: false,
      });
      try {
        await ws.ping();
        const opened = await connections.next("connections: open");
        expect(opened).toMatchObject({ kind: "open" });
        expect(typeof opened["connection"]).toBe("string");
        ws.close();
        const closed = await connections.next("connections: close");
        expect(closed).toMatchObject({ kind: "close", connection: opened["connection"] });
      } finally {
        ws.close();
        await connections.stop();
      }
    });

    it("reads the ring of a stream with recent", async () => {
      const got = inbox<StreamEvent>(() => true);
      const sub = await dt.stream("requests", got.handler, { recent: 5 });
      try {
        const first = await got.next("recent requests");
        expect(first.seq).toBeGreaterThan(0);
        const r = await dt.recent("requests", { limit: 1 });
        expect(r.first).toBeGreaterThan(0);
        expect(r.events).toHaveLength(1);
      } finally {
        await sub.unsubscribe();
      }
    });
  },
);
