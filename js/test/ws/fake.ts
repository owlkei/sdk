import type { WebSocketLike } from "../../src/ws/index.js";

export class FakeSocket implements WebSocketLike {
  static instances: FakeSocket[] = [];

  binaryType = "blob";
  readyState = 0;
  onopen: ((event: unknown) => void) | null = null;
  onclose: ((event: { readonly code: number }) => void) | null = null;
  onmessage: ((event: { readonly data: unknown }) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  readonly sent: (string | Uint8Array)[] = [];
  closedWith: number | undefined;

  constructor(
    readonly url: string,
    readonly protocols: string,
  ) {
    FakeSocket.instances.push(this);
  }

  static reset(): void {
    FakeSocket.instances = [];
  }

  static last(): FakeSocket {
    const socket = FakeSocket.instances.at(-1);
    if (socket === undefined) throw new Error("no socket was opened");
    return socket;
  }

  send(data: string | Uint8Array): void {
    this.sent.push(data);
  }

  close(code?: number): void {
    this.closedWith = code ?? 1005;
  }

  open(): void {
    this.readyState = 1;
    this.onopen?.({});
  }

  receive(data: string | ArrayBuffer): void {
    this.onmessage?.({ data });
  }

  receiveJSON(value: unknown): void {
    this.receive(JSON.stringify(value));
  }

  drop(code: number): void {
    this.readyState = 3;
    this.onclose?.({ code });
  }

  sentJSON(): Record<string, unknown>[] {
    return this.sent
      .filter((frame): frame is string => typeof frame === "string")
      .map((frame) => JSON.parse(frame) as Record<string, unknown>);
  }

  lastJSON(): Record<string, unknown> {
    const frame = this.sentJSON().at(-1);
    if (frame === undefined) throw new Error("no JSON frame was sent");
    return frame;
  }
}

export async function settle(): Promise<void> {
  for (let i = 0; i < 5; i++) await Promise.resolve();
}

export function binaryFrame(envelope: unknown, data: Uint8Array): ArrayBuffer {
  const head = new TextEncoder().encode(JSON.stringify(envelope));
  const frame = new Uint8Array(4 + head.byteLength + data.byteLength);
  new DataView(frame.buffer).setUint32(0, head.byteLength);
  frame.set(head, 4);
  frame.set(data, 4 + head.byteLength);
  return frame.buffer;
}
