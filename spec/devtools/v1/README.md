# Owlkei devtools protocol v1

The wire contract between an Owlkei Go app started with `IsDevtool: true` and its devtools clients
(the hosted page at `https://devtools.owlkei.com`, `@owlkei/sdk/devtools`). It runs on the
WebSocket protocol [`owlkei.v1`](../../ws/v1/README.md) unchanged: this file only adds the endpoint,
the pairing, the topics and what their messages hold. The server's source of truth is design 0044
of Owlkei Go (sections T, G, V, S and L); this file is its transcription, and a change to it starts
there.

Rule numbers are the design's (T token, G gate, V protocol, S snapshot and actions, L streams), so
a test or a bug report can cite them. Gaps in the numbering are rules that concern only the server.

- [Pairing](#pairing)
- [Endpoint and gate](#endpoint-and-gate)
- [Protocol](#protocol)
- [Snapshot and actions](#snapshot-and-actions)
- [Streams](#streams)
- [Clients](#clients)
- [Test vectors](#test-vectors)
- [Conformance app](#conformance-app)

## Pairing

- **T1. Token.** `okd1_` followed by 43 characters of unpadded base64url (`A-Z a-z 0-9 - _`), the
  encoding of 32 random bytes: 48 bytes in all. One token per user and machine, kept by the app in
  `<user config dir>/owlkei/devtools-token` (mode 0600), so it survives restarts; it changes when
  that file is deleted.
- **T4. The link.** The token is shown in one place: the app's terminal banner prints
  `Devtools: https://devtools.owlkei.com/#app=<scheme>://localhost:<port>&token=<token>`
  (`https` when the app serves TLS). No log record, event, snapshot section or reply ever holds it.
  The fragment never reaches a server.
- **T7. Receiving the link.** The page reads the fragment, removes it from the address bar
  (`history.replaceState`), and refuses an `app` that is not an `http` or `https` origin whose host
  is `localhost`, `127.0.0.1` or `[::1]`. It may keep the token in memory or `sessionStorage`, never
  in a URL.

## Endpoint and gate

The app serves `/_owlkei/devtools` (and `/_owlkei/mcp`, not part of this spec) only to connections
on loopback addresses without a forwarding header; any other connection gets `404 Not Found`
whatever it sends. Refusals carry the REST error body (`code`, `error`, `message`, `requestId`) and
never hold a byte of the token, the `Origin` or the `Host`. In the order a client meets them:

| # | Check | Refusal |
|---|---|---|
| G3 | the host of `Host`, port removed, case-insensitive, is `localhost`, `127.0.0.1` or `[::1]` | 403 `Forbidden` |
| G4 | `Origin` absent (not a browser), or exactly `https://devtools.owlkei.com`; `null`, the app's own origin and any other value are refused | 403 `Origin not allowed` |
| G5 | `OPTIONS` (a CORS preflight) ends here: 204 with `Access-Control-Allow-Origin: <the origin>`, `Access-Control-Allow-Methods: GET`, `Access-Control-Allow-Headers: Authorization`, `Access-Control-Max-Age: 600`, and `Access-Control-Allow-Private-Network: true` when asked; without `Origin` or `Access-Control-Request-Method`, 405 | - |
| G6 | the token: `Authorization: Bearer <token>`; on a WebSocket handshake without that header, the subprotocol entry `owlkei.bearer.<token>`. Never the URL: a token in the query is ignored | 401 `Unauthorized`, `WWW-Authenticate: Bearer` |

- **G7.** From G4 on, every answer to a request with an allowed `Origin` carries
  `Access-Control-Allow-Origin` and `Vary: Origin`, so a page can read the 401.
- **G8. The handshake.** `GET /_owlkei/devtools` with `Upgrade: websocket`, after G6. The app holds
  at most 4 devtools connections: the 5th handshake answers 503 with `Retry-After: 5`.
  `Sec-WebSocket-Protocol`, when present, must list `owlkei.v1` (else 400
  `Unsupported subprotocol`). The answer selects `owlkei.v1`; the token entry is never echoed. A
  browser offers `["owlkei.v1", "owlkei.bearer.<token>"]`; a client that can set headers may send
  `Authorization` instead.

A method other than `GET` and `OPTIONS` answers 405 with `Allow: GET, OPTIONS`. While the app shuts
down the endpoint answers 503 with `Connection: close`.

## Protocol

- **V1. Transport.** `owlkei.v1` unchanged on `/_owlkei/devtools`: the envelope, `ack` / `reply` /
  `error` / `event` / `pong`, the error body, the 65,536-byte frame, the heartbeat, at-most-once
  delivery and the close codes of [`spec/ws/v1`](../../ws/v1/README.md). The codes a devtools
  client meets: 1001 (the app stops), 1008, 1009, 1013 (the devtools budget is full), 4429 (the
  client reads too slowly). `~me` answers 401.
- **V2. Versions.** The protocol number is `1`, given by the hello (V5). Every `data` object the
  server sends has `"schema": 1`. Inside version 1 the server may add members, topics, sections,
  event kinds, span stages and statuses; a client ignores what it does not know. A removed or
  renamed member, a changed meaning or a smaller limit needs protocol `2`, which an app announces
  in its hello; an app serves one protocol version.
- **V3. Topics.**

  | Topic | `subscribe` | `publish` |
  |---|---|---|
  | `snapshot` | `ack`; then change events (S7) | reads a section (S1-S5) |
  | `requests`, `errors`, `jobs`, `storage`, `connections` | `ack`; then the stream's events ([Streams](#streams)) | reads the ring (V7) |
  | `run-job` | `ack`; no event | runs a job (S8) |

  Any other topic answers 404. A connection subscribes before it publishes (409 `Not subscribed`
  otherwise), as on any `owlkei.v1` topic.
- **V4. Limits.** 4 devtools connections per app. An event's `data` is at most 8,192 bytes. A
  `reply` is at most 49,152 bytes of `data`. A snapshot chunk is at most 24,576 bytes of text. A
  section's text is at most 2 MiB (S4).
- **V5. Hello.** `GET /_owlkei/devtools` without `Upgrade`, after the gate, answers 200
  `application/json`, `Cache-Control: no-store`:
  ```json
  {"protocol":1,"owlkei":"v0.9.0","go":"go1.27","module":"github.com/acme/shop","pid":4242,
   "started_at":"2026-11-27T10:15:02Z",
   "topics":["snapshot","requests","errors","jobs","storage","connections","run-job"],
   "sections":["pre_route","controller","modules","wiring","jobs"]}
  ```
  `owlkei` and `module` are empty when the build does not record them; `sections` lists the
  snapshot sections this app has, in the order of S1.
- **V6. Input.** The `data` of a `publish` is a JSON object; a wrong member answers `error` 422
  with `fields` (the REST validation body), never an echo of the value. Unknown members are ignored.
- **V7. Reading a ring.** `publish` to a stream topic with `{"after": <seq>, "limit": <n>}`
  (`after` optional, default 0; `limit` optional, 1 to 100, default 100) is answered
  ```json
  {"schema":1,"first":812,"last":1042,"events":[…],"more":false}
  ```
  `events` are the ring's events with `seq` greater than `after`, oldest first, at most `limit`,
  and as many as fit V4; `more` tells that some were left out; `first` and `last` are the lowest
  and highest `seq` the ring holds (both 0 for an empty ring). `first > after + 1` means events
  were lost.
- **V8. Order.** The events of one stream reach a connection in `seq` order. `seq` starts at 1 per
  stream and grows by 1 per stored event for the life of the app process. A client that subscribes
  and then reads the ring drops the events it already has, by `seq`.
- **V9. A slow client.** A devtools connection whose queue would pass 128 KiB or 512 frames is
  closed with 4429; the others and the app are not delayed. The client reconnects with backoff,
  subscribes again and reads the rings.

Events are collected only while a client is attached (a devtools connection is open, or an MCP
session is live) and for 30 s after the last one left; a unit that began before is not collected.
The rings hold, at most: `requests` 256 events or 512 KiB, `errors` 128 or 512 KiB, `jobs` 64 or
128 KiB, `storage` 128 or 128 KiB, `connections` 128 or 64 KiB; the oldest is dropped first. When
collection stops the rings are emptied; `seq` keeps counting. Each stream stores at most 50 events
per second (burst 100): an event over the rate is dropped and the next stored one carries
`"skipped": <count>`.

## Snapshot and actions

- **S1. Reading a section.** `publish` to `snapshot` takes `{"section": <name>, "id": <n>,
  "offset": <n>}`. `section` is required and is one of `pre_route`, `controller`, `render`,
  `modules`, `wiring`, `ws`, `auth`, `storage`, `jobs`, `httpclient` (another value: 422,
  `fields.section` rule `oneof`). `offset` defaults to 0; `id` is required when `offset` is not 0.
- **S2. The reply.**
  ```json
  {"schema":1,"section":"controller","id":7,"size":183211,"offset":0,"text":"{\"http\":[…"}
  ```
  `text` is a slice of the section's JSON text, at most 24,576 bytes, cut at a UTF-8 boundary;
  `size` and `offset` count bytes of UTF-8. The text is complete when
  `offset + byteLength(text) == size`. The client joins the slices and parses once.
- **S3.** A section the app does not have (no jobs, no storage module) has the text `null`.
- **S4. The kept text.** A request with `offset` 0 encodes the section then and gives it a new `id`
  (a counter per app). When the text is longer than one chunk the app keeps it for this connection:
  one kept text per connection, released when its last chunk was sent, when a new `offset` 0
  request replaces it, or when the connection closes. A text over 2 MiB answers `error` 413
  `Payload Too Large`; a text the devtools budget can't hold now answers 503 with `retryAfter` 1.
- **S5.** A request with an `offset` above 0 is answered from the kept text when `section` and `id`
  match and `offset` is at most `size`; otherwise `error` 409 `Snapshot text is gone`, and the
  client starts again at `offset` 0. Asking the same offset twice, before the last chunk, gives the
  same slice.
- **S6.** A section's content is the app's devtool snapshot of that name (Owlkei Go's
  `internal/devtool/README.md`); this protocol adds no field to it.
- **S7. Changes.** A connection subscribed to `snapshot` receives
  ```json
  {"schema":1,"section":"modules","value":[{"name":"mail","state":"ready","is_async":true,"ready_after_ms":412}]}
  ```
  for the sections `modules`, `wiring`, `jobs` and `ws`. `value` is the section's whole new value,
  left out when its encoding is over 32,768 bytes (the client reads the section again). Triggers: a
  module's state changes (`modules`, `wiring`); a job run starts, ends or is skipped (`jobs`); a
  connection of the app's WebSocket endpoint opens or closes, or one of its messages completes
  (`ws`). A section is published at most once per 250 ms and only when it differs from the value
  last published. The other sections are read on demand; their live facts are the streams.
- **S8. `run-job`.** `publish` with `{"name": <job name>}` (required, at most 64 bytes) replies
  `{"schema":1,"status":"started","run_id":"9f86d081884c7d65"}`. `status` is `started`, `running`
  (the job is already running), `unknown` (no job of that name, or an app without jobs),
  `stopping`, `pending` or `failed` (its module's state); `run_id` is present only with `started`.
- **S9.** A run started this way goes through the job's guards, interceptors, pipes and filters,
  obeys the no-overlap rule, has the trigger `devtool`, and does not move the job's next run.

## Streams

Common members of every event: `schema` (1), `seq`, `at` (the unit's end, UTC, RFC 3339 with
milliseconds), and `skipped` when events were dropped before it. Durations are whole microseconds
(`duration_us`) unless named otherwise. A member without a value is left out.

- **L1. `requests`.** One event per HTTP request and per WebSocket `subscribe`, `publish` or
  `unsubscribe` of the app. A handshake that upgraded is not a request (it is a `connections`
  `open`).

  | Member | Value |
  |---|---|
  | `id`, `trace_id` | the request ID (16 hex) and the trace ID (32 hex) |
  | `transport` | `HTTP` or `WS` |
  | `operation` | the HTTP method, or `SUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE` |
  | `route` | the matched pattern (`/products/{id}`, `rooms.{room}.messages`, the WebSocket path for a refused handshake); absent when nothing matched |
  | `handler` | `ProductController.READ_products_BY_id`; absent without a route |
  | `code` | the status written, or the message's answer code |
  | `duration_us` | the unit's wall time |
  | `spans` | in emit order: `stage` (`pre-route`, `middleware`, `guard`, `pre_interceptor`, `pipe`, `handler`, `post_interceptor`, `exception_filter`, `render`, `storage`), `name` (the layer's type, the handler's method, the template, or the statement with its `?`), `duration_us`, `code` (when not 0); a `storage` span adds `operation` (`exec`, `select`, `transaction`, `savepoint`), `connection` (`sqlite/default`), `rows`, `cause`, `span` and `parent` |
  | `spans_dropped` | spans left out (at most 48 per unit, and an event fits 8,192 bytes) |

  ```json
  {"schema":1,"seq":1042,"at":"2026-11-27T10:15:02.123Z","id":"9f86d081884c7d65",
   "trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","transport":"HTTP","operation":"GET",
   "route":"/notes/{id}","handler":"NoteController.READ_notes_BY_id","code":200,"duration_us":1830,
   "spans":[{"stage":"guard","name":"auth.Guard","duration_us":12},
            {"stage":"storage","name":"select * from notes where id = ?","duration_us":420,
             "operation":"select","connection":"sqlite/default","rows":1,"span":"00f067aa0ba902b7"},
            {"stage":"handler","name":"READ_notes_BY_id","duration_us":1310}]}
  ```
- **L2. `errors`.** Three kinds; members besides `id`, `trace_id`, `transport` (`HTTP`, `WS` or
  `JOB`), `route`, `handler`:

  | `kind` | When | Members |
  |---|---|---|
  | `error` | the app logs `UnhandledError` (HTTP, a WebSocket message, a job run) | `code`, `error` (the logged text, at most 1,024 bytes), `filter` |
  | `panic` | the app logs `UnhandledPanic` | `panic` (the recovered value as text, at most 1,024 bytes), `stack` (at most 4,096 bytes, when logged), `filter`, `hint` |
  | `refusal` | a `requests` unit ended with a code of 400 or more | `operation`, `code`, `category`, `name` |

  For a job, `route` is the job name and the event follows the record's throttle (it has its
  `count`). A unit that failed with an error or a panic produces its `error` or `panic` event, then
  its `refusal` event with the same `id`. The `category` of a `refusal`: `stage` (a pre-route stage
  refused; `name` is the stage, `loadshed`, `hostcheck`, `csrf`…), `no-route`, the stage of the
  unit's last span that is not `exception_filter` or `storage` (`middleware`, `guard`,
  `pre_interceptor`, `pipe`, `handler`, `post_interceptor`, `render`; `name` is that span's name),
  else `other`.
- **L3. `jobs`.**

  | `kind` | When | Members |
  |---|---|---|
  | `run` | a job run ends | `name`, `handler`, `id` (the run ID), `trace_id`, `trigger` (`schedule`, `devtool`), `result` (`ok`, `refused`, `error`, `panic`), `code`, `duration_us`, `spans`, `spans_dropped` |
  | `skip` | the app logs `JobSkipped` (at most once a minute per job) | `name`, `handler`, `reason` (`running`, `late`, `pending`, `failed`), `count` |
- **L4. `storage`.**

  | Member | Value |
  |---|---|
  | `kind` | `slow` (a statement at or over the slow threshold) or `repeated` (one statement text run 5 times in one unit, once per unit and text) |
  | `connection` | the connection's name |
  | `statement` | the text as the app wrote it, with its `?`, at most 1,024 bytes |
  | `duration_us`, `rows` | `slow` only |
  | `count` | `repeated` only: 5 |
  | `route`, `handler`, `id`, `trace_id` | of the unit that ran it; absent outside a unit |
- **L5. `connections`** (the connections of the app's own WebSocket endpoint).

  | `kind` | When | Members |
  |---|---|---|
  | `open` | a handshake upgraded | `connection` (its ID) |
  | `close` | a connection ended | `connection`, `code` (the close code, 1006 without a close frame), `cause` (`peer`, `lost`, `timeout`, `protocol`, `policy`, `budget`, `slow`, `lifetime`, `shutdown`, `shutdown-abort`, `internal`, `auth`), `duration_ms` |

  No address, no user, no topic.
- **L6. No value.** No event holds a path, a concrete topic, a query string, a header, a cookie, a
  body, a form or message value, a parameter value, a statement argument, a row value, a client
  address, a user ID, a session or an API key. The members holding text the framework did not
  write are `error`, `panic` and `stack` (the application's own words, as its log prints them) and
  `statement` (source text). A client shows them as text, never as markup.

## Clients

What every client of this repository does with the rules above:

- It refuses, before connecting, an `app` that is not a loopback origin (T7) and a token that is not
  T1's form, and never puts the token in a URL, a log line or an error message.
- It reads a section by joining its chunks (S2) and starts again once at `offset` 0 on a 409 (S5).
- It subscribes to a topic before it publishes to it (V3), and stays subscribed to the topics it
  has read.
- It delivers a stream's events once each, in `seq` order (V8). Reading the ring after the `ack`
  gives the events that came before; live events that arrive during the read wait and are
  delivered after it, minus the ones it already holds.
- After a reconnect it subscribes again and reads each stream's ring from the last `seq` it
  delivered (V9). A ring whose `last` is lower than that `seq` belongs to a restarted app: the
  client reads it from the start.

## Test vectors

One JSON file per case in [`vectors/`](vectors), checked against
[`vector.schema.json`](vector.schema.json) by `make check-spec`. Every vector is a `session`, in
the format of [`spec/ws/v1` › Test vectors](../../ws/v1/README.md#test-vectors) (V20 there), run
against the [conformance app](#conformance-app) on `/_owlkei/devtools` with the suite's token
offered as `owlkei.bearer.<token>` next to `owlkei.v1`. One addition: in a `send.text`,
`{{<name>}}` is replaced by the JSON of the value a `"@same:<name>"` matcher bound earlier in the
session (the `id` of a snapshot text, which a vector can't know in advance). `rules` cite this
file's rule numbers.

## Conformance app

The devtools of the Owlkei Go conformance app (`internal/conformance`, the same app as
[`spec/ws/v1` › Conformance app](../../ws/v1/README.md#conformance-app)):

| What | Used by |
|---|---|
| one job, `tick` (`SCHEDULE_EVERY_1_HOUR_tick`), that returns at once | `run-job` `started`, a `jobs` `run` event, a `snapshot` change of `jobs` |
| `GET /boom`, a route that returns a plain error (500) | an `errors` `error` event, then its `refusal` |
| `POST /session/login` | a `requests` event with `route` `/session/login` |
| 200 redirect routes `/chunk/<n>` → `/boom` | a `controller` section of 24,577 to 49,152 bytes of ASCII: two chunks |
| no storage module, no render, no HTTP client | the sections `storage`, `render`, `httpclient` are `null`; the `storage` ring is empty |
| its WebSocket endpoint `/ws` | `connections` `open` and `close` |

The suite reads the app's origin from `OWLKEI_CONFORMANCE_DEVTOOLS_URL` (such as
`http://127.0.0.1:3000`) and the token from `OWLKEI_CONFORMANCE_DEVTOOLS_TOKEN`: the app's `cmd`
makes a fresh token for each run and writes it to the file named by `DEVTOOLS_TOKEN_FILE`. The
budget app has no devtools.
