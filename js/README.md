# @owlkei/sdk

The client SDK for Owlkei Go apps. `@owlkei/sdk/ws` speaks the WebSocket wire protocol v1
(`owlkei.v1`, [spec](../spec/ws/v1/README.md)) on the platform's `WebSocket`: browsers, Node 22+,
Deno, Bun and React Native. `@owlkei/sdk/devtools` reads an app's devtools on this machine
([below](#owlkeisdkdevtools)). No runtime dependency.

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

## @owlkei/sdk/devtools

Reads the devtools of an app started with `IsDevtool: true` on the same machine
([spec](../spec/devtools/v1/README.md)): the hello, the snapshot by section, the live streams, and
running a job. The app prints a pairing link on its terminal,
`https://devtools.owlkei.com/#app=http://localhost:3000&token=okd1_…`.

```ts
import { connect, parsePairing } from "@owlkei/sdk/devtools";

const pairing = parsePairing(location.hash); // throws for an app that is not on this machine
history.replaceState(null, "", location.pathname); // the token leaves the address bar

const dt = connect(pairing);
const hello = await dt.hello(); // a wrong token rejects with an OwlkeiError, code 401
const routes = await dt.section("controller"); // the chunks joined, then parsed once

await dt.stream("errors", (e) => show(e), { recent: 20 }); // the ring's newest 20, then live
await dt.onChange((section, value) => update(section, value));
const run = await dt.runJob("tick"); // { status: "started", run_id: "…" }

dt.close();
```

In a page without a bundler: `<script src="owlkei.devtools.min.js"></script>` defines
`Owlkei.devtools`.

| Name                                   | Does                                                                                                                                                                                                            |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `parsePairing(link)`                   | reads `{ app, token }` from the link, its fragment or the fragment without `#`; a `TypeError` (without the token) for a missing member, a malformed token, or an `app` not on `localhost`, `127.0.0.1`, `[::1]` |
| `connect({ app?, token, ...options })` | opens `/_owlkei/devtools`, offering `owlkei.v1` and `owlkei.bearer.<token>`; `app` defaults to `http://localhost:3000`; the options are the `Client`'s                                                          |
| `dt.hello()`                           | `GET /_owlkei/devtools` with `Authorization: Bearer <token>`: `protocol`, `owlkei`, `go`, `module`, `pid`, `started_at`, `topics`, `sections`                                                                   |
| `dt.section(name)`                     | the section's value (`null` when the app has none); starts again once when the server dropped the kept text (409)                                                                                               |
| `dt.onChange(handler)`                 | `handler(section, value)` for each change of `modules`, `wiring`, `jobs`, `ws`; `value` is `undefined` when too large: read the section                                                                         |
| `dt.stream(name, handler, options?)`   | the events of `requests`, `errors`, `jobs`, `storage` or `connections`, once each in `seq` order; `recent: n` delivers the ring's newest `n` first; after a reconnect it reads the ring from its last `seq`     |
| `dt.recent(name, options?)`            | one read of a ring: `{ first, last, events, more }`, the events after `after`, at most `limit` (1 to 100)                                                                                                       |
| `dt.runJob(name)`                      | runs a job through its guards: `{ status, run_id? }`, `status` one of `started`, `running`, `unknown`, `stopping`, `pending`, `failed`                                                                          |
| `dt.client`                            | the `Client` underneath: `state`, `on("open" \| "reconnecting" \| "close" \| "error")`                                                                                                                          |
| `dt.close()`                           | closes for good                                                                                                                                                                                                 |

The client stays subscribed to a topic it has read (a section, a ring, `run-job`), since the
server answers a publish only on a subscribed topic. Events are collected only while a devtools
client is attached; their `error`, `panic`, `stack` and `statement` members are the application's
own text: show them as text, never as markup.
