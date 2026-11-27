import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { expect } from "vitest";

import { decodeBinary, decodeText, encodeBinary, type Envelope } from "../src/ws/frame.js";

interface Expected {
  readonly json?: Readonly<Record<string, unknown>>;
  readonly binary?: {
    readonly json: Readonly<Record<string, unknown>>;
    readonly dataBase64: string;
  };
}

interface Step {
  readonly send?: {
    readonly text?: string;
    readonly binary?: { readonly envelope: string; readonly dataBase64: string };
    readonly frameBase64?: string;
  };
  readonly expect?: Expected;
  readonly expectClose?: { readonly code: number };
  readonly wait?: { readonly ms: number };
}

export interface SessionVector {
  readonly name: string;
  readonly kind: "parse" | "session";
  readonly steps: readonly Step[];
}

interface Received {
  readonly envelope: Readonly<Record<string, unknown>>;
  readonly binary?: Uint8Array;
}

const waitMs = 5000;
const noCloseFrame = 1006;
const requestID = /^[0-9a-f]{16}$/;

export function sessionVectors(protocol: string): SessionVector[] {
  const dir = new URL(`../../spec/${protocol}/v1/vectors/`, import.meta.url).pathname;
  return readdirSync(dir)
    .filter((name) => name.startsWith("session-"))
    .map((name) => JSON.parse(readFileSync(join(dir, name), "utf8")) as SessionVector);
}

const bytes = (base64: string): Uint8Array<ArrayBuffer> =>
  Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));

class Session {
  private readonly queue: Received[] = [];
  private notify: (() => void) | undefined;
  private closeCode: number | undefined;

  private constructor(private readonly socket: WebSocket) {
    socket.binaryType = "arraybuffer";
    socket.addEventListener("message", (event: MessageEvent<unknown>) => {
      this.queue.push(receive(event.data));
      this.wake();
    });
    socket.addEventListener("close", (event) => {
      this.closeCode = event.code;
      this.wake();
    });
  }

  static async open(target: string, protocols: string[]): Promise<Session> {
    const socket = new WebSocket(target, protocols);
    await new Promise<void>((resolve, reject) => {
      socket.addEventListener("open", () => {
        resolve();
      });
      socket.addEventListener("error", () => {
        reject(new Error(`no WebSocket connection to ${target}`));
      });
    });
    return new Session(socket);
  }

  async send(send: NonNullable<Step["send"]>): Promise<void> {
    if (send.text !== undefined) {
      this.socket.send(send.text);
    } else if (send.binary !== undefined) {
      const envelope = JSON.parse(send.binary.envelope) as Envelope;
      this.socket.send(new Uint8Array(await encodeBinary(envelope, bytes(send.binary.dataBase64))));
    } else if (send.frameBase64 !== undefined) {
      this.socket.send(bytes(send.frameBase64));
    }
  }

  async take(label: string, fits: (r: Received) => boolean): Promise<Received> {
    const deadline = Date.now() + waitMs;
    for (;;) {
      const index = this.queue.findIndex(fits);
      const [found] = index >= 0 ? this.queue.splice(index, 1) : [];
      if (found !== undefined) return found;
      if (this.closeCode !== undefined)
        throw new Error(`${label}: closed (${String(this.closeCode)})`);
      await this.next(label, deadline);
    }
  }

  async closed(label: string): Promise<number> {
    const deadline = Date.now() + waitMs;
    while (this.closeCode === undefined) await this.next(label, deadline);
    return this.closeCode;
  }

  leftovers(): readonly Received[] {
    return this.queue.filter((r) => r.envelope["type"] !== "event");
  }

  async close(): Promise<void> {
    if (this.closeCode !== undefined) return;
    this.socket.close(1000);
    await this.closed("close");
  }

  private async next(label: string, deadline: number): Promise<void> {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error(`${label}: nothing arrived within ${String(waitMs)} ms`);
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, remaining);
      this.notify = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }

  private wake(): void {
    const notify = this.notify;
    this.notify = undefined;
    notify?.();
  }
}

function receive(data: unknown): Received {
  if (typeof data === "string") {
    const envelope = decodeText(data);
    return {
      envelope: envelope === undefined ? { type: "<undecodable text>" } : members(envelope),
    };
  }
  if (data instanceof ArrayBuffer) {
    const incoming = decodeBinary(data);
    if (incoming === undefined) return { envelope: { type: "<undecodable binary>" } };
    return {
      envelope: members(incoming.envelope),
      binary: new Uint8Array(incoming.binary ?? new ArrayBuffer(0)),
    };
  }
  return { envelope: { type: "<unknown frame>" } };
}

function members(envelope: Envelope): Readonly<Record<string, unknown>> {
  return Object.fromEntries(Object.entries(envelope));
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function match(bound: Map<string, unknown>, path: string, want: unknown, got: unknown): string {
  if (want === "@any") return "";
  if (want === "@id") {
    return typeof got === "string" && requestID.test(got)
      ? ""
      : `${path}: ${JSON.stringify(got)} is not 16 lower-case hex digits`;
  }
  if (typeof want === "string" && want.startsWith("@same:")) {
    const name = want.slice("@same:".length);
    if (bound.has(name) && !isDeepStrictEqual(bound.get(name), got)) {
      return `${path}: ${JSON.stringify(got)} does not repeat ${JSON.stringify(bound.get(name))}`;
    }
    bound.set(name, got);
    return "";
  }
  if (isRecord(want)) {
    if (!isRecord(got)) return `${path}: ${JSON.stringify(got)} is not an object`;
    for (const [key, value] of Object.entries(want)) {
      if (value === "@absent") {
        if (key in got) return `${path}.${key}: present, want absent`;
        continue;
      }
      if (!(key in got)) return `${path}.${key}: missing`;
      const message = match(bound, `${path}.${key}`, value, got[key]);
      if (message !== "") return message;
    }
    for (const key of Object.keys(got)) {
      if (!(key in want)) return `${path}.${key}: a member the vector does not list`;
    }
    return "";
  }
  return isDeepStrictEqual(want, got)
    ? ""
    : `${path}: ${JSON.stringify(got)}, want ${JSON.stringify(want)}`;
}

function substitute(bound: Map<string, unknown>, text: string): string {
  return text.replace(/\{\{([a-z0-9-]+)\}\}/g, (whole, name: string) =>
    bound.has(name) ? JSON.stringify(bound.get(name)) : whole,
  );
}

export async function runSession(
  target: string,
  vector: SessionVector,
  protocols: string[] = ["owlkei.v1"],
): Promise<void> {
  const session = await Session.open(target, protocols);
  const bound = new Map<string, unknown>();
  try {
    for (const [i, step] of vector.steps.entries()) {
      const label = `vector ${vector.name}, step ${String(i + 1)}`;
      if (step.send !== undefined) {
        const text = step.send.text;
        await session.send(text === undefined ? step.send : { text: substitute(bound, text) });
      } else if (step.expect !== undefined) {
        const json = step.expect.json ?? step.expect.binary?.json ?? {};
        const type = json["type"];
        const got = await session.take(label, (r) => r.envelope["type"] === type);
        expect(match(bound, "", json, got.envelope), label).toBe("");
        const data =
          step.expect.binary === undefined ? undefined : bytes(step.expect.binary.dataBase64);
        expect(
          got.binary === undefined ? undefined : [...got.binary],
          `${label}: binary data`,
        ).toEqual(data === undefined ? undefined : [...data]);
      } else if (step.expectClose !== undefined) {
        const want = step.expectClose.code === 0 ? noCloseFrame : step.expectClose.code;
        expect(await session.closed(label), `${label}: close code`).toBe(want);
        return;
      } else if (step.wait !== undefined) {
        await new Promise((resolve) => setTimeout(resolve, step.wait?.ms));
      }
    }
    await session.send({ text: '{"type":"ping","id":"zz-end"}' });
    await session.take(`vector ${vector.name}, end`, (r) => {
      return r.envelope["type"] === "pong" && r.envelope["id"] === "zz-end";
    });
    expect(
      session.leftovers(),
      `vector ${vector.name} ended with a frame no step expected`,
    ).toEqual([]);
  } finally {
    await session.close();
  }
}
