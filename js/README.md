# @owlkei/sdk

The client SDK for Owlkei Go apps. `@owlkei/sdk/ws` speaks the WebSocket wire protocol v1
(`owlkei.v1`, [spec](../spec/ws/v1/README.md)) on the platform's `WebSocket`: browsers, Node 22+,
Deno, Bun and React Native. No runtime dependency.

```ts
import { connect, OwlkeiError } from "@owlkei/sdk/ws";

const ws = connect("wss://app.example.com/ws");

// Subscribe first, then read the history over REST: nothing published after the ack is missed.
const sub = await ws.subscribe("rooms.42.messages", (message) => render(message));
const history = await fetch("/rooms/42/messages").then((r) => r.json());

try {
  const saved = await ws.publish("rooms.42.messages", { text: "hi" });
} catch (err) {
  if (err instanceof OwlkeiError && err.code === 422) showFieldErrors(err.fields);
}

await sub.unsubscribe();
ws.close();
```

In a page without a bundler: `<script src="owlkei.ws.min.js"></script>` defines `Owlkei.ws`.

## API

| Name                                                   | Does                                                                                                                                      |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `connect(url, options?)` / `new Client(url, options?)` | opens the connection, offering `owlkei.v1`                                                                                                |
| `client.subscribe(topic, handler)`                     | resolves on the server's ack with a `Subscription`; `handler(data, topic)` receives each event                                            |
| `subscription.unsubscribe()`                           | stops the handler; the last handler of a topic unsubscribes the connection                                                                |
| `client.publish(topic, data?)`                         | resolves with the server's reply value (`undefined` when none); binary data (`ArrayBuffer`, a typed array, `Blob`) goes in a binary frame |
| `client.ping()`                                        | resolves on the JSON pong                                                                                                                 |
| `client.close()`                                       | closes for good                                                                                                                           |
| `client.state`                                         | `connecting`, `open`, `reconnecting` or `closed`                                                                                          |
| `client.on(event, listener)`                           | `open`, `reconnecting(delayMs, closeCode)`, `close(closeCode)`, `error(error)`; returns the remover                                       |

Errors: `OwlkeiError` (the server refused the message: `code`, `status`, `message`, `fields`,
`requestId`, `retryAfter`), `ConnectionError` (`closeCode`), `TimeoutError`, and `TypeError` for an
invalid topic, before anything is sent.

## Options

| Option             | Default                         | Meaning                                                                                                                                                                              |
| ------------------ | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `reconnect`        | `true`                          | reconnect as the close code allows (shutdown, lost connection, lifetime, too slow, over budget); never after 1000, 1002, 1003, 1007, 1008, 4401, 4403 or an unknown 3xxx / 4xxx code |
| `backoff`          | `{ minMs: 1000, maxMs: 30000 }` | exponential backoff with full jitter; 1013 waits four times longer and at least 1 s, 4408 reconnects within `minMs`                                                                  |
| `heartbeatMs`      | `25000`                         | interval of the JSON ping, `false` for none                                                                                                                                          |
| `idleTimeoutMs`    | `60000`                         | a connection without any frame for this long counts as lost                                                                                                                          |
| `requestTimeoutMs` | `10000`                         | how long a request waits for its answer                                                                                                                                              |
| `binaryType`       | `"arraybuffer"`                 | binary events and replies as `ArrayBuffer` or `Blob`                                                                                                                                 |
| `WebSocket`        | `globalThis.WebSocket`          | another implementation                                                                                                                                                               |

After a reconnect the client subscribes again to every topic it held. A subscribe the server
refuses with `503` (busy, a part still starting, or its WebSocket memory full) is sent again on the
same connection after its `retryAfter`, or 1 s without one, as long as the wait ends within
`requestTimeoutMs`; then the last `503` rejects. A publish is never sent again. Requests made while the
connection is down wait for the next open (up to 256, each within `requestTimeoutMs`); requests
already sent when it dropped reject with a `ConnectionError`.
