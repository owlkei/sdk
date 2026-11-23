import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { decodeBinary, decodeText, encodeBinary, isID, isTopic } from "../../src/ws/frame.js";

interface Vector {
  readonly name: string;
  readonly kind: "parse" | "session";
  readonly input?: {
    readonly text?: string;
    readonly binary?: { readonly envelope: string; readonly dataBase64?: string };
    readonly frameBase64?: string;
  };
  readonly want?: {
    readonly type?: string;
    readonly id?: string;
    readonly topic?: string;
    readonly data?: unknown;
    readonly dataBase64?: string;
    readonly error?: number;
  };
}

const dir = new URL("../../../spec/ws/v1/vectors/", import.meta.url).pathname;
const vectors = readdirSync(dir)
  .filter((name) => name.startsWith("parse-"))
  .map((name) => JSON.parse(readFileSync(join(dir, name), "utf8")) as Vector);

const bytes = (base64: string): Uint8Array => Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));

describe("spec vectors", () => {
  it("are present", () => {
    expect(vectors.length).toBeGreaterThan(30);
  });

  const valid = vectors.filter((v) => v.want?.error === undefined);
  it.each(valid.map((v) => [v.name, v] as const))(
    "%s parses to its wanted envelope",
    async (_, v) => {
      const want = v.want ?? {};
      if (v.input?.text !== undefined) {
        const envelope = decodeText(v.input.text);
        expect(envelope).toMatchObject({ type: want.type });
        if (want.id !== undefined) expect(envelope?.id).toBe(want.id);
        if (want.topic !== undefined) expect(envelope?.topic).toBe(want.topic);
        if ("data" in want) expect(envelope?.data).toEqual(want.data);
        if (want.id !== undefined) expect(isID(want.id)).toBe(true);
        if (want.topic !== undefined) expect(isTopic(want.topic)).toBe(true);
      }
      if (v.input?.binary !== undefined) {
        const envelope = JSON.parse(v.input.binary.envelope) as { type: string };
        const data = bytes(v.input.binary.dataBase64 ?? "");
        const frame = await encodeBinary(envelope, data);
        const decoded = decodeBinary(frame.buffer as ArrayBuffer);
        expect(decoded?.envelope).toEqual(envelope);
        expect([...new Uint8Array(decoded?.binary ?? new ArrayBuffer(0))]).toEqual([
          ...bytes(want.dataBase64 ?? ""),
        ]);
      }
    },
  );

  const malformed = vectors.filter((v) => v.input?.frameBase64 !== undefined);
  it.each(malformed.map((v) => [v.name, v] as const))(
    "%s is refused by the binary decoder",
    (_, v) => {
      const frame = bytes(v.input?.frameBase64 ?? "");
      expect(decodeBinary(frame.slice().buffer)).toBeUndefined();
    },
  );

  it("refuses a binary envelope that holds data", () => {
    const v = vectors.find((x) => x.name === "binary-envelope-with-data");
    expect(v).toBeDefined();
    const head = new TextEncoder().encode(v?.input?.binary?.envelope ?? "");
    const frame = new Uint8Array(4 + head.byteLength + 1);
    new DataView(frame.buffer).setUint32(0, head.byteLength);
    frame.set(head, 4);
    expect(decodeBinary(frame.buffer)).toBeUndefined();
  });

  it.each([
    ["topic-empty-segment", "rooms..42"],
    ["topic-trailing-dot", "rooms."],
    ["topic-wildcard", "rooms.*"],
    ["topic-256-bytes", "a".repeat(256)],
    ["topic-17-segments", Array(17).fill("s").join(".")],
  ])("%s is not a topic", (_, topic) => {
    expect(isTopic(topic)).toBe(false);
  });

  it.each([
    ["id-too-long", "x".repeat(65)],
    ["id-empty", ""],
    ["id-bad-char", "a b"],
  ])("%s is not an id", (_, id) => {
    expect(isID(id)).toBe(false);
  });
});
