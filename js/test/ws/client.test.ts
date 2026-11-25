import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  Client,
  ConnectionError,
  OwlkeiError,
  PROTOCOL,
  TimeoutError,
  type ClientOptions,
} from "../../src/ws/index.js";
import { FakeSocket, binaryFrame, settle } from "./fake.js";

function connect(options: ClientOptions = {}): { client: Client; socket: FakeSocket } {
  const client = new Client("ws://app.test/ws", { WebSocket: FakeSocket, ...options });
  return { client, socket: FakeSocket.last() };
}

function open(options: ClientOptions = {}): { client: Client; socket: FakeSocket } {
  const opened = connect(options);
  opened.socket.open();
  return opened;
}

async function subscribed(
  topic: string,
  handler = vi.fn(),
  options: ClientOptions = {},
): Promise<{ client: Client; socket: FakeSocket; handler: typeof handler }> {
  const { client, socket } = open(options);
  const done = client.subscribe(topic, handler);
  await settle();
  socket.receiveJSON({ type: "ack", id: socket.lastJSON()["id"], requestId: "0123456789abcdef" });
  await done;
  return { client, socket, handler };
}

function busy(id: string, retryAfter?: number): Record<string, unknown> {
  return {
    type: "error",
    id,
    requestId: "0123456789abcdef",
    ...(retryAfter === undefined ? {} : { retryAfter }),
    error: {
      code: 503,
      error: "Service Unavailable",
      message: "Service Unavailable",
      requestId: "0123456789abcdef",
    },
  };
}

beforeEach(() => {
  FakeSocket.reset();
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("connect", () => {
  it("offers the subprotocol of the wire protocol v1", () => {
    const { socket } = connect();
    expect(socket.protocols).toBe(PROTOCOL);
    expect(PROTOCOL).toBe("owlkei.v1");
    expect(socket.url).toBe("ws://app.test/ws");
  });

  it("reads binary frames as ArrayBuffer whatever binaryType asks for", () => {
    const { socket } = connect({ binaryType: "blob" });
    expect(socket.binaryType).toBe("arraybuffer");
  });

  it("starts in connecting and is open after the handshake", () => {
    const { client, socket } = connect();
    expect(client.state).toBe("connecting");
    socket.open();
    expect(client.state).toBe("open");
  });

  it("needs a WebSocket implementation", () => {
    const saved = globalThis.WebSocket;
    vi.stubGlobal("WebSocket", undefined);
    try {
      expect(() => new Client("ws://app.test/ws")).toThrow(TypeError);
    } finally {
      vi.stubGlobal("WebSocket", saved);
    }
  });
});

describe("subscribe", () => {
  it("sends a subscribe and resolves on its ack", async () => {
    const { socket } = await subscribed("rooms.42.messages");
    expect(socket.sentJSON()).toEqual([{ type: "subscribe", id: "1", topic: "rooms.42.messages" }]);
  });

  it("delivers the events of the topic to the handler", async () => {
    const { socket, handler } = await subscribed("rooms.42.messages");
    socket.receiveJSON({ type: "event", topic: "rooms.42.messages", data: { text: "hi" } });
    socket.receiveJSON({ type: "event", topic: "rooms.43.messages", data: { text: "other" } });
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith({ text: "hi" }, "rooms.42.messages");
  });

  it("subscribes the connection once for several handlers of one topic", async () => {
    const { client, socket, handler } = await subscribed("echo");
    const second = vi.fn();
    await client.subscribe("echo", second);
    expect(socket.sentJSON()).toHaveLength(1);
    socket.receiveJSON({ type: "event", topic: "echo", data: 1 });
    expect(handler).toHaveBeenCalledWith(1, "echo");
    expect(second).toHaveBeenCalledWith(1, "echo");
  });

  it("unsubscribes the connection when the last handler leaves", async () => {
    const { client, socket, handler } = await subscribed("echo");
    const other = vi.fn();
    const second = await client.subscribe("echo", other);
    await second.unsubscribe();
    expect(socket.sentJSON()).toHaveLength(1);
    const first = await client.subscribe("echo", handler);
    const leaving = first.unsubscribe();
    await settle();
    expect(socket.lastJSON()).toEqual({ type: "unsubscribe", id: "2", topic: "echo" });
    socket.receiveJSON({ type: "ack", id: "2", requestId: "0123456789abcdef" });
    await leaving;
    socket.receiveJSON({ type: "event", topic: "echo", data: 1 });
    expect(handler).not.toHaveBeenCalled();
  });

  it("rejects with the server's error and forgets the topic", async () => {
    const { client, socket } = open();
    const done = client.subscribe("guarded", vi.fn());
    await settle();
    socket.receiveJSON({
      type: "error",
      id: "1",
      requestId: "0123456789abcdef",
      error: { code: 403, error: "Forbidden", message: "Forbidden", requestId: "0123456789abcdef" },
    });
    const err = await done.catch((e: unknown) => e);
    expect(err).toBeInstanceOf(OwlkeiError);
    expect(err).toMatchObject({
      code: 403,
      status: "Forbidden",
      message: "Forbidden",
      requestId: "0123456789abcdef",
    });
    const again = client.subscribe("guarded", vi.fn());
    await settle();
    expect(socket.lastJSON()).toEqual({ type: "subscribe", id: "2", topic: "guarded" });
    socket.receiveJSON({ type: "ack", id: "2", requestId: "0123456789abcdef" });
    await again;
  });

  it("sends a subscribe refused with a 503 again after its retryAfter", async () => {
    const { client, socket } = open();
    const done = client.subscribe("echo", vi.fn());
    await settle();
    socket.receiveJSON(busy("1", 2));
    await settle();
    vi.advanceTimersByTime(1999);
    await settle();
    expect(socket.sentJSON()).toHaveLength(1);
    vi.advanceTimersByTime(1);
    await settle();
    expect(socket.lastJSON()).toEqual({ type: "subscribe", id: "2", topic: "echo" });
    socket.receiveJSON({ type: "ack", id: "2", requestId: "0123456789abcdef" });
    await expect(done).resolves.toMatchObject({ topic: "echo" });
  });

  it("sends a subscribe refused with a 503 without retryAfter again after one second", async () => {
    const { client, socket } = open();
    const done = client.subscribe("echo", vi.fn());
    await settle();
    socket.receiveJSON(busy("1"));
    await settle();
    vi.advanceTimersByTime(999);
    await settle();
    expect(socket.sentJSON()).toHaveLength(1);
    vi.advanceTimersByTime(1);
    await settle();
    expect(socket.lastJSON()).toEqual({ type: "subscribe", id: "2", topic: "echo" });
    socket.receiveJSON({ type: "ack", id: "2", requestId: "0123456789abcdef" });
    await done;
  });

  it("rejects with the 503 when the next try would end after the request timeout", async () => {
    const { client, socket } = open({ requestTimeoutMs: 3000 });
    const done = client.subscribe("pending", vi.fn());
    await settle();
    socket.receiveJSON(busy("1", 1));
    await settle();
    vi.advanceTimersByTime(1000);
    await settle();
    socket.receiveJSON(busy("2", 5));
    await expect(done).rejects.toMatchObject({ code: 503, retryAfter: 5 });
    expect(socket.sentJSON()).toHaveLength(2);
  });

  it("does not send a publish refused with a 503 again", async () => {
    const { client, socket } = await subscribed("echo");
    const reply = client.publish("echo", {});
    await settle();
    socket.receiveJSON(busy("2", 1));
    await expect(reply).rejects.toMatchObject({ code: 503 });
    vi.advanceTimersByTime(5000);
    await settle();
    expect(socket.sentJSON()).toHaveLength(2);
  });

  it("stops waiting to send a subscribe again when the client closes", async () => {
    const { client, socket } = open();
    const done = client.subscribe("echo", vi.fn());
    await settle();
    socket.receiveJSON(busy("1", 5));
    await settle();
    client.close();
    await expect(done).rejects.toBeInstanceOf(ConnectionError);
    expect(socket.sentJSON()).toHaveLength(1);
  });

  it("refuses an invalid topic before sending anything", async () => {
    const { client, socket } = open();
    await expect(client.subscribe("rooms.*", vi.fn())).rejects.toBeInstanceOf(TypeError);
    await expect(client.subscribe("", vi.fn())).rejects.toBeInstanceOf(TypeError);
    await expect(client.publish("a..b", {})).rejects.toBeInstanceOf(TypeError);
    expect(socket.sent).toHaveLength(0);
  });

  it("keeps calling the other handlers when one throws, and reports the throw", async () => {
    const errors: unknown[] = [];
    const { client, socket } = await subscribed(
      "echo",
      vi.fn(() => {
        throw new Error("boom");
      }),
    );
    client.on("error", (err) => errors.push(err));
    const second = vi.fn();
    await client.subscribe("echo", second);
    socket.receiveJSON({ type: "event", topic: "echo", data: 1 });
    expect(second).toHaveBeenCalled();
    expect(errors).toHaveLength(1);
  });
});

describe("publish", () => {
  it("sends JSON data and resolves with the reply's value", async () => {
    const { client, socket } = await subscribed("echo");
    const reply = client.publish("echo", { a: 1 });
    await settle();
    expect(socket.lastJSON()).toEqual({ type: "publish", id: "2", topic: "echo", data: { a: 1 } });
    socket.receiveJSON({ type: "reply", id: "2", requestId: "0123456789abcdef", data: { a: 1 } });
    await expect(reply).resolves.toEqual({ a: 1 });
  });

  it("omits data when there is none and resolves undefined for a reply without data", async () => {
    const { client, socket } = await subscribed("slow");
    const reply = client.publish("slow");
    await settle();
    expect(socket.lastJSON()).toEqual({ type: "publish", id: "2", topic: "slow" });
    socket.receiveJSON({ type: "reply", id: "2", requestId: "0123456789abcdef" });
    await expect(reply).resolves.toBeUndefined();
  });

  it("sends binary data in a binary frame: length, envelope, raw bytes", async () => {
    const { client, socket } = await subscribed("binary");
    void client.publish("binary", new Uint8Array([1, 2, 3]));
    await settle();
    const frame = socket.sent.at(-1);
    expect(frame).toBeInstanceOf(Uint8Array);
    const bytes = frame as Uint8Array;
    const length = new DataView(bytes.buffer, bytes.byteOffset).getUint32(0);
    const envelope = JSON.parse(new TextDecoder().decode(bytes.subarray(4, 4 + length))) as unknown;
    expect(envelope).toEqual({ type: "publish", topic: "binary", id: "2" });
    expect([...bytes.subarray(4 + length)]).toEqual([1, 2, 3]);
  });

  it("accepts an ArrayBuffer, a DataView and a Blob as binary data", async () => {
    const { client, socket } = await subscribed("binary");
    void client.publish("binary", new Uint8Array([7]).buffer);
    void client.publish("binary", new DataView(new Uint8Array([8]).buffer));
    void client.publish("binary", new Blob([new Uint8Array([9])]));
    await vi.waitFor(() => {
      expect(socket.sent.filter((f) => f instanceof Uint8Array)).toHaveLength(3);
    });
    const tails = socket.sent
      .filter((f): f is Uint8Array => f instanceof Uint8Array)
      .map((f) => f.at(-1));
    expect(tails).toEqual([7, 8, 9]);
  });

  it("resolves a binary reply as an ArrayBuffer, or a Blob when asked", async () => {
    const { client, socket } = await subscribed("binary");
    const reply = client.publish("binary", new Uint8Array([1]));
    await settle();
    socket.receive(
      binaryFrame(
        { type: "reply", id: "2", requestId: "0123456789abcdef" },
        new Uint8Array([4, 5]),
      ),
    );
    const value = await reply;
    expect(value).toBeInstanceOf(ArrayBuffer);
    expect([...new Uint8Array(value as ArrayBuffer)]).toEqual([4, 5]);

    const blob = await subscribed("binary", vi.fn(), { binaryType: "blob" });
    const second = blob.client.publish("binary", new Uint8Array([1]));
    await settle();
    blob.socket.receive(
      binaryFrame({ type: "reply", id: "2", requestId: "0123456789abcdef" }, new Uint8Array([6])),
    );
    const asBlob = await second;
    expect(asBlob).toBeInstanceOf(Blob);
    expect([...new Uint8Array(await (asBlob as Blob).arrayBuffer())]).toEqual([6]);
  });

  it("delivers binary events as binary data", async () => {
    const { socket, handler } = await subscribed("binary");
    socket.receive(binaryFrame({ type: "event", topic: "binary" }, new Uint8Array([9])));
    const data = handler.mock.calls[0]?.[0] as ArrayBuffer;
    expect([...new Uint8Array(data)]).toEqual([9]);
  });

  it("rejects with the validation fields of a 422", async () => {
    const { client, socket } = await subscribed("validated");
    const reply = client.publish("validated", {});
    await settle();
    socket.receiveJSON({
      type: "error",
      id: "2",
      requestId: "0123456789abcdef",
      error: {
        code: 422,
        error: "Unprocessable Entity",
        message: "Unprocessable Entity",
        fields: { name: { rule: "required", message: "This field is required" } },
        requestId: "0123456789abcdef",
      },
    });
    const err = (await reply.catch((e: unknown) => e)) as OwlkeiError;
    expect(err.code).toBe(422);
    expect(err.fields).toEqual({ name: { rule: "required", message: "This field is required" } });
  });

  it("carries retryAfter on a 503", async () => {
    const { client, socket } = await subscribed("echo");
    const reply = client.publish("echo", {});
    await settle();
    socket.receiveJSON({
      type: "error",
      id: "2",
      requestId: "0123456789abcdef",
      retryAfter: 1,
      error: {
        code: 503,
        error: "Service Unavailable",
        message: "Server is busy",
        requestId: "0123456789abcdef",
      },
    });
    await expect(reply).rejects.toMatchObject({ code: 503, retryAfter: 1 });
  });
});

describe("requests", () => {
  it("waits for the connection to open before sending", async () => {
    const { client, socket } = connect();
    const done = client.subscribe("echo", vi.fn());
    await settle();
    expect(socket.sent).toHaveLength(0);
    socket.open();
    await settle();
    expect(socket.lastJSON()).toEqual({ type: "subscribe", id: "1", topic: "echo" });
    socket.receiveJSON({ type: "ack", id: "1", requestId: "0123456789abcdef" });
    await done;
  });

  it("fails with a TimeoutError when no answer comes", async () => {
    const { client } = open({ requestTimeoutMs: 500 });
    const done = client.subscribe("echo", vi.fn());
    vi.advanceTimersByTime(500);
    await expect(done).rejects.toBeInstanceOf(TimeoutError);
  });

  it("gives each pending request its own id and matches answers in any order", async () => {
    const { client, socket } = await subscribed("echo");
    const a = client.publish("echo", "a");
    const b = client.publish("echo", "b");
    await settle();
    socket.receiveJSON({ type: "reply", id: "3", requestId: "0123456789abcdef", data: "B" });
    socket.receiveJSON({ type: "reply", id: "2", requestId: "0123456789abcdef", data: "A" });
    await expect(a).resolves.toBe("A");
    await expect(b).resolves.toBe("B");
  });

  it("resolves a ping on its pong", async () => {
    const { client, socket } = open();
    const done = client.ping();
    await settle();
    expect(socket.lastJSON()).toEqual({ type: "ping", id: "1" });
    socket.receiveJSON({ type: "pong", id: "1" });
    await done;
  });

  it("reports an error that answers no request", () => {
    const { client, socket } = open();
    const errors: unknown[] = [];
    client.on("error", (err) => errors.push(err));
    socket.receiveJSON({
      type: "error",
      requestId: "0123456789abcdef",
      error: {
        code: 400,
        error: "Bad Request",
        message: "Invalid message",
        requestId: "0123456789abcdef",
      },
    });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ code: 400, message: "Invalid message" });
  });

  it("ignores unknown types and malformed frames", async () => {
    const { client, socket } = await subscribed("echo");
    const reply = client.publish("echo", 1);
    await settle();
    socket.receiveJSON({ type: "future-type", id: "2" });
    socket.receive("not json");
    socket.receive(new Uint8Array([0, 0]).buffer);
    socket.receiveJSON({ type: "reply", id: "2", requestId: "0123456789abcdef", data: 1 });
    await expect(reply).resolves.toBe(1);
  });
});

describe("reconnect", () => {
  it("reconnects after a server shutdown and subscribes again", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    const { client, socket, handler } = await subscribed("echo");
    const delays: number[] = [];
    client.on("reconnecting", (delay) => delays.push(delay));
    socket.drop(1001);
    expect(client.state).toBe("reconnecting");
    expect(delays).toEqual([500]);
    vi.advanceTimersByTime(500);
    const next = FakeSocket.last();
    expect(next).not.toBe(socket);
    next.open();
    await settle();
    expect(next.lastJSON()).toEqual({ type: "subscribe", id: "2", topic: "echo" });
    next.receiveJSON({ type: "ack", id: "2", requestId: "0123456789abcdef" });
    next.receiveJSON({ type: "event", topic: "echo", data: 2 });
    expect(handler).toHaveBeenCalledWith(2, "echo");
  });

  it("grows the backoff and caps it", () => {
    vi.spyOn(Math, "random").mockReturnValue(0.999);
    const { client, socket } = open({ backoff: { minMs: 1000, maxMs: 5000 } });
    const delays: number[] = [];
    client.on("reconnecting", (delay) => delays.push(delay));
    socket.drop(1006);
    for (let i = 0; i < 4; i++) {
      vi.advanceTimersByTime(delays.at(-1) ?? 0);
      FakeSocket.last().drop(1006);
    }
    expect(delays).toEqual([999, 1998, 3996, 4995, 4995]);
  });

  it("reconnects at once, with jitter, when the connection reached its lifetime", () => {
    vi.spyOn(Math, "random").mockReturnValue(0.25);
    const { client, socket } = open();
    const delays: number[] = [];
    client.on("reconnecting", (delay) => delays.push(delay));
    socket.drop(4408);
    expect(delays).toEqual([250]);
  });

  it("waits longer when the server is out of memory", () => {
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    const { client, socket } = open();
    const delays: number[] = [];
    client.on("reconnecting", (delay) => delays.push(delay));
    socket.drop(1013);
    expect(delays).toEqual([2500]);
  });

  it("waits at least one second after a 1013 close, whatever the backoff", () => {
    vi.spyOn(Math, "random").mockReturnValue(0);
    const { client, socket } = open({ backoff: { minMs: 10, maxMs: 20 } });
    const delays: number[] = [];
    client.on("reconnecting", (delay) => delays.push(delay));
    socket.drop(1013);
    vi.advanceTimersByTime(999);
    expect(FakeSocket.instances).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(FakeSocket.instances).toHaveLength(2);
    expect(delays).toEqual([1000]);
  });

  it("subscribes again after a 1013 close", async () => {
    const { socket, handler } = await subscribed("echo");
    socket.drop(1013);
    vi.advanceTimersByTime(4000);
    const next = FakeSocket.last();
    expect(next).not.toBe(socket);
    next.open();
    await settle();
    expect(next.lastJSON()).toEqual({ type: "subscribe", id: "2", topic: "echo" });
    next.receiveJSON({ type: "ack", id: "2", requestId: "0123456789abcdef" });
    next.receiveJSON({ type: "event", topic: "echo", data: 3 });
    expect(handler).toHaveBeenCalledWith(3, "echo");
  });

  it.each([1000, 1002, 1003, 1007, 1008, 4401, 4403, 4999])("stops after close code %i", (code) => {
    const { client, socket } = open();
    const closes: number[] = [];
    client.on("close", (c) => closes.push(c));
    socket.drop(code);
    expect(client.state).toBe("closed");
    expect(closes).toEqual([code]);
    expect(FakeSocket.instances).toHaveLength(1);
  });

  it("rejects the requests already sent with 4401 and does not reconnect", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0);
    const { client, socket } = await subscribed("echo");
    const reply = client.publish("echo", 1);
    await settle();
    socket.drop(4401);
    const err = (await reply.catch((e: unknown) => e)) as ConnectionError;
    expect(err).toBeInstanceOf(ConnectionError);
    expect(err.closeCode).toBe(4401);
    vi.advanceTimersByTime(60_000);
    expect(client.state).toBe("closed");
    expect(FakeSocket.instances).toHaveLength(1);
  });

  it("rejects the requests already sent with the close code", async () => {
    const { client, socket } = await subscribed("echo");
    const reply = client.publish("echo", 1);
    await settle();
    socket.drop(1001);
    const err = (await reply.catch((e: unknown) => e)) as ConnectionError;
    expect(err).toBeInstanceOf(ConnectionError);
    expect(err.closeCode).toBe(1001);
  });

  it("keeps requests not yet sent for the next connection", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0);
    const { client, socket } = open();
    socket.drop(1006);
    const done = client.subscribe("echo", vi.fn());
    vi.advanceTimersByTime(0);
    const next = FakeSocket.last();
    next.open();
    await settle();
    expect(next.lastJSON()).toEqual({ type: "subscribe", id: "1", topic: "echo" });
    next.receiveJSON({ type: "ack", id: "1", requestId: "0123456789abcdef" });
    await done;
  });

  it("fails a subscribe that was sent when the connection drops, and does not repeat it", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0);
    const { client, socket } = open();
    const done = client.subscribe("echo", vi.fn());
    await settle();
    socket.drop(1006);
    await expect(done).rejects.toBeInstanceOf(ConnectionError);
    vi.advanceTimersByTime(0);
    const next = FakeSocket.last();
    next.open();
    await settle();
    expect(next.sent).toHaveLength(0);
  });

  it("does not reconnect when reconnect is off", () => {
    const { client, socket } = open({ reconnect: false });
    socket.drop(1001);
    expect(client.state).toBe("closed");
  });
});

describe("heartbeat", () => {
  it("sends a JSON ping on its interval", () => {
    const { socket } = open({ heartbeatMs: 1000 });
    vi.advanceTimersByTime(2500);
    expect(socket.sent.filter((f) => f === '{"type":"ping"}')).toHaveLength(2);
  });

  it("sends no ping when the heartbeat is off", () => {
    const { socket } = open({ heartbeatMs: false });
    vi.advanceTimersByTime(60_000);
    expect(socket.sent).toHaveLength(0);
  });

  it("treats a connection without frames for the idle timeout as lost", () => {
    vi.spyOn(Math, "random").mockReturnValue(0);
    const { client, socket } = open({ heartbeatMs: false, idleTimeoutMs: 1000 });
    vi.advanceTimersByTime(999);
    socket.receiveJSON({ type: "pong" });
    vi.advanceTimersByTime(999);
    expect(socket.closedWith).toBeUndefined();
    vi.advanceTimersByTime(1);
    expect(socket.closedWith).toBeDefined();
    socket.drop(1005);
    expect(client.state).toBe("reconnecting");
  });
});

describe("close", () => {
  it("closes for good and rejects every pending request", async () => {
    const { client, socket } = open();
    const done = client.subscribe("echo", vi.fn());
    await settle();
    client.close();
    expect(client.state).toBe("closed");
    expect(socket.closedWith).toBe(1000);
    await expect(done).rejects.toBeInstanceOf(ConnectionError);
    await expect(client.publish("echo", 1)).rejects.toBeInstanceOf(ConnectionError);
    socket.drop(1000);
    expect(FakeSocket.instances).toHaveLength(1);
  });

  it("cancels a pending reconnect", () => {
    const { client, socket } = open();
    socket.drop(1001);
    client.close();
    vi.advanceTimersByTime(60_000);
    expect(FakeSocket.instances).toHaveLength(1);
  });
});
