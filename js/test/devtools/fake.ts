import { FakeSocket } from "../ws/fake.js";

export type Answer = { readonly reply: unknown } | { readonly error: number } | undefined;

export type Server = (topic: string, data: unknown) => Answer;

export class DevtoolsSocket extends FakeSocket {
  static server: Server = () => undefined;

  readonly offered: string[];

  constructor(url: string, protocols: string[]) {
    super(url, protocols.join(", "));
    this.offered = protocols;
  }

  static current(): DevtoolsSocket {
    const socket = FakeSocket.last();
    if (!(socket instanceof DevtoolsSocket)) throw new Error("not a devtools socket");
    return socket;
  }

  override send(data: string | Uint8Array): void {
    super.send(data);
    if (typeof data !== "string") return;
    const message = JSON.parse(data) as Record<string, unknown>;
    const id = message["id"];
    void Promise.resolve().then(() => {
      if (message["type"] === "subscribe") {
        this.receiveJSON({ type: "ack", id, requestId: "0123456789abcdef" });
        return;
      }
      if (message["type"] !== "publish") return;
      const answer = DevtoolsSocket.server(String(message["topic"]), message["data"]);
      if (answer === undefined) return;
      if ("error" in answer) {
        this.receiveJSON({
          type: "error",
          id,
          requestId: "0123456789abcdef",
          error: { code: answer.error, error: "Conflict", message: "Snapshot text is gone" },
        });
        return;
      }
      this.receiveJSON({ type: "reply", id, requestId: "0123456789abcdef", data: answer.reply });
    });
  }

  event(topic: string, data: unknown): void {
    this.receiveJSON({ type: "event", topic, data });
  }

  published(topic?: string): Record<string, unknown>[] {
    return this.sentJSON().filter(
      (m) => m["type"] === "publish" && (topic === undefined || m["topic"] === topic),
    );
  }
}

export function ringEvent(
  seq: number,
  extra: Record<string, unknown> = {},
): Record<string, unknown> {
  return { schema: 1, seq, at: "2026-11-27T10:15:02.123Z", ...extra };
}

export function ring(
  events: readonly Record<string, unknown>[],
  first: number,
  last: number,
  more = false,
): Answer {
  return { reply: { schema: 1, first, last, events, more } };
}
