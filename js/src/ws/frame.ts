const MAX_ENVELOPE_BYTES = 1024;
const HEADER_BYTES = 4;

const ID = /^[A-Za-z0-9._-]{1,64}$/;
const SEGMENT = /^[A-Za-z0-9_~-]+$/;
const MAX_TOPIC_BYTES = 255;
const MAX_SEGMENTS = 16;

export type BinaryData = ArrayBuffer | ArrayBufferView | Blob;

export interface Envelope {
  readonly type: string;
  readonly id?: string;
  readonly topic?: string;
  readonly requestId?: string;
  readonly retryAfter?: number;
  readonly error?: unknown;
  readonly data?: unknown;
}

export interface Incoming {
  readonly envelope: Envelope;
  readonly binary?: ArrayBuffer;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

export function isID(id: string): boolean {
  return ID.test(id);
}

export function isTopic(topic: string): boolean {
  if (topic.length === 0 || topic.length > MAX_TOPIC_BYTES) return false;
  const segments = topic.split(".");
  return segments.length <= MAX_SEGMENTS && segments.every((s) => SEGMENT.test(s));
}

export function isBinaryData(value: unknown): value is BinaryData {
  return (
    value instanceof ArrayBuffer ||
    ArrayBuffer.isView(value) ||
    (typeof Blob !== "undefined" && value instanceof Blob)
  );
}

export function encodeText(envelope: Envelope): string {
  return JSON.stringify(envelope);
}

export async function encodeBinary(envelope: Envelope, data: BinaryData): Promise<Uint8Array> {
  const head = encoder.encode(JSON.stringify(envelope));
  if (head.byteLength < 2 || head.byteLength > MAX_ENVELOPE_BYTES) {
    throw new RangeError("owlkei: the envelope of a binary frame must be 2 to 1024 bytes");
  }
  const body = await toBytes(data);
  const frame = new Uint8Array(HEADER_BYTES + head.byteLength + body.byteLength);
  new DataView(frame.buffer).setUint32(0, head.byteLength);
  frame.set(head, HEADER_BYTES);
  frame.set(body, HEADER_BYTES + head.byteLength);
  return frame;
}

export function decodeText(text: string): Envelope | undefined {
  return asEnvelope(parse(text));
}

export function decodeBinary(frame: ArrayBuffer): Incoming | undefined {
  if (frame.byteLength < HEADER_BYTES) return undefined;
  const length = new DataView(frame).getUint32(0);
  if (length < 2 || length > MAX_ENVELOPE_BYTES || length > frame.byteLength - HEADER_BYTES) {
    return undefined;
  }
  let text: string;
  try {
    text = decoder.decode(new Uint8Array(frame, HEADER_BYTES, length));
  } catch {
    return undefined;
  }
  const envelope = asEnvelope(parse(text));
  if (envelope === undefined || "data" in envelope) return undefined;
  return { envelope, binary: frame.slice(HEADER_BYTES + length) };
}

function parse(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}

function asEnvelope(value: unknown): Envelope | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const record = value as Record<string, unknown>;
  if (typeof record["type"] !== "string") return undefined;
  for (const key of ["id", "topic", "requestId"] as const) {
    if (key in record && typeof record[key] !== "string") return undefined;
  }
  if ("retryAfter" in record && typeof record["retryAfter"] !== "number") return undefined;
  return record as unknown as Envelope;
}

async function toBytes(data: BinaryData): Promise<Uint8Array> {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data))
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  return new Uint8Array(await data.arrayBuffer());
}
