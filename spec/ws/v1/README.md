# Owlkei WebSocket protocol v1

The wire contract between an Owlkei Go server and its WebSocket clients. Every client in this
repository implements it, passes the vectors in [`vectors/`](vectors) and the conformance suite
against a real server. The server's source of truth is design 0027 of Owlkei Go (section V); this
file is its transcription, and a change to it starts there.

Rule numbers (V1-V20) are the design's, so a test or a bug report can cite them.

- [Transport](#transport)
- [Envelope](#envelope)
- [Messages](#messages)
- [Errors](#errors)
- [Heartbeat and close](#heartbeat-and-close)
- [Delivery and reconnect](#delivery-and-reconnect)
- [Compatibility](#compatibility)
- [Test vectors](#test-vectors)
- [Conformance app](#conformance-app)

## Transport

- **V1. Version.** The subprotocol is `owlkei.v1`. A client offers it in `Sec-WebSocket-Protocol`
  and the server answers with it. A client that offers no subprotocol speaks v1. A client that
  offers subprotocols without `owlkei.v1` is refused before the upgrade with 400. A later version is
  a new subprotocol; a server may support several and picks its newest that the client offers.
- **V2. Text frame.** One JSON object (RFC 8259, UTF-8): the envelope. Whitespace is allowed where
  JSON allows it.
- **V4. Binary frame.** `[4 bytes: N, big-endian][N bytes: the envelope JSON][the raw data]`, with
  2 ≤ N ≤ 1,024 and N ≤ frame length − 4. The envelope has no `data` member (one present makes the
  envelope invalid); the data is the bytes after the envelope, possibly none. Only `publish` (from a
  client) and `reply` and `event` (from the server) travel in binary frames.
- **V6. Limits.** A frame is at most 65,536 bytes in each direction; a larger one closes the
  connection with 1009. JSON nested deeper than 32 levels anywhere in a frame makes the envelope
  invalid. A connection holds at most 256 subscriptions.

## Envelope

- **V3. Members.**

  | Member | From the client | From the server |
  |---|---|---|
  | `type` | required: `subscribe`, `publish`, `unsubscribe`, `ping` | `ack`, `reply`, `error`, `event`, `pong` |
  | `id` | required except for `ping` | the `id` of the message being answered |
  | `topic` | required except for `ping` | in `event` |
  | `data` | any JSON value, read only for `publish` | in `reply` and `event` |
  | `requestId` | - | in `ack`, `reply`, `error`: 16 lower-case hex digits, the server's id of this message in its logs |
  | `error` | - | in `error`: the REST error body |
  | `retryAfter` | - | in some `error`s: whole seconds |

  Unknown members are ignored by both sides. A known member given twice, or with another JSON type
  than listed, makes the envelope invalid. Member order is free; a receiver never depends on it.
- **V5. `id` and `topic`.** `id` is 1 to 64 bytes of `[A-Za-z0-9._-]`, chosen by the client and
  unique among its pending messages. `topic` is 1 to 16 segments joined by `.`, each made of 1 or
  more bytes of `[A-Za-z0-9_~-]`, 255 bytes at most. A JSON escape inside either string is decoded
  before the check. Widening either set later is a compatible change.

## Messages

- **V7. `subscribe`.**
  ```json
  {"type":"subscribe","id":"1","topic":"rooms.42.messages"}
  {"type":"ack","id":"1","requestId":"9f86d081884c7d65"}
  ```
  Answered by `ack` or by an `error`. After the `ack`, the connection receives the topic's `event`s;
  no `event` of a topic arrives before the `ack` of its subscribe. Subscribing to a topic the
  connection already holds is answered `ack` and changes nothing. There are no wildcard
  subscriptions.
- **V8. `unsubscribe`.** Answered `ack`, also when the connection did not hold the topic. No `event`
  of the topic arrives after the `ack`.
- **V9. `publish`.**
  ```json
  {"type":"publish","id":"2","topic":"rooms.42.messages","data":{"text":"hi"}}
  {"type":"reply","id":"2","requestId":"2c26b46b68ffc68f","data":{"id":"m9","text":"hi"}}
  ```
  Answered by `reply` (`data` is absent when the server has no value; a binary reply is a binary
  frame whose envelope is `{"type":"reply","id":…,"requestId":…}`) or by an `error`. The connection
  must hold the topic (V7) first. A publish reaches other clients only if the server's handler fans
  it out.
- **V10. `event`.**
  ```json
  {"type":"event","topic":"rooms.42.messages","data":{"id":"m9","text":"hi"}}
  ```
  or a binary frame with the envelope `{"type":"event","topic":…}`. No `id`, no `requestId`. A
  subscribed client whose publish causes an event receives it too.
- **V13. `ping`.** `{"type":"ping"}`, optionally with an `id`, is answered `{"type":"pong"}` with the
  same `id` when it was valid. It exists for clients that cannot send control frames (browsers). The
  server answers at most 4 per second per connection and drops the rest without an answer.

## Errors

- **V11. `error`.**
  ```json
  {"type":"error","id":"2","requestId":"2c26b46b68ffc68f","retryAfter":1,
   "error":{"code":503,"error":"Service Unavailable","message":"Server is busy","requestId":"2c26b46b68ffc68f"}}
  ```
  `error` is the REST error body, byte for byte what the server's HTTP layer writes for the same
  exception (`code`, `error`, `message`, `fields` for validation, `requestId`). `id` is absent when
  the message had none or an invalid one. `retryAfter` is present only with 429 and 503 answers that
  have one. The connection stays open after an `error`.
- **V12. Codes.** HTTP status codes; a client accepts codes not listed here.

  | Code | When | `message` |
  |---|---|---|
  | 400 | invalid envelope, unknown `type`, missing or invalid `id` or `topic`, `data` that is not an object where the handler reads an object | `Invalid message` |
  | 403 | refused by the application (a guard) | the application's |
  | 404 | no topic of that name | `Not Found` |
  | 405 | `publish` to a topic that only listens | `Method Not Allowed` |
  | 409 | `publish` to a topic the connection has not subscribed to | `Not subscribed` |
  | 415 | text frame where the topic takes binary, or the reverse | `Unsupported Media Type` |
  | 422 | validation; `fields` holds one entry per field | `Unprocessable Entity` or the application's |
  | 429 | a rate limit (with `retryAfter`), or the 257th subscription (without) | `Too Many Requests`, `Too many subscriptions` |
  | 500 | anything else | `Internal Server Error` |
  | 503 | overload (`retryAfter` 1), or a part of the server still starting (`retryAfter`) or failed (none) | `Server is busy`, `Service Unavailable` |

- **V19. No echo.** No server frame holds bytes of a client's `topic` or `data`, except an `event`
  whose topic and data come from the server's own publish. The `id` is returned only in the answer
  to its own message, and only when it passed V5.

## Heartbeat and close

- **V14. Heartbeat.** The server sends a control ping about every 25 s; a WebSocket stack answers it
  by itself. The server closes a connection from which no frame arrived for 60 s. A client treats
  60 s without any frame as a dead connection; a browser client sends the JSON `ping` every 25 s.
- **V15. Close codes.** A failure of one message never closes the connection; these do:

  | Code | Meaning | Client |
  |---|---|---|
  | 1000 | normal, answered to the client's close | - |
  | 1001 | the server is shutting down | reconnect with backoff |
  | 1002, 1003, 1007 | protocol error, unsupported data, invalid UTF-8 (RFC 6455) | fix the client |
  | 1008 | too many invalid messages | do not reconnect automatically |
  | 1009 | a frame over 65,536 bytes | do not resend |
  | 1011 | server error | reconnect with backoff |
  | 1013 | the server is out of memory for WebSockets (its budget is full) | reconnect with a jittered backoff of at least 1 s, then subscribe again to every topic |
  | 4408 | the connection reached its maximum lifetime | reconnect at once, with jitter |
  | 4429 | the client reads too slowly | reconnect with backoff |
  | 4401, 4403 | reserved for authentication | do not reconnect |
  | no close frame | the connection was lost, or idle for 60 s | reconnect with backoff |

  The server never sends a close reason.

## Delivery and reconnect

- **V16. Delivery.** Best effort, at most once, ordered per connection: the events of one topic
  arrive in the order the server published them. Nothing is stored, replayed or acknowledged by the
  client. After a reconnect the client subscribes again, then reads what it missed over REST and
  drops duplicates by the application's own identifiers.
- **V17. Reconnect.** Exponential backoff with full jitter, from 1 s to 30 s; a `retryAfter` or the
  table of V15 overrides it. Pending promises fail with a typed connection error. A `503` `error` to
  a `subscribe` (the server is busy, a part of it is still starting, or its memory budget is full)
  means: send the same subscribe again, on the same connection, after `retryAfter` seconds, or 1 s
  without it (design 0027 Amendment 3 › K3). A client stops when the wait would end after its request
  timeout, and the caller gets the last 503. A client never sends a `publish` again by itself.

## Compatibility

- **V18. Inside v1.** Allowed: new optional members, new server `type`s (a client ignores an unknown
  `type`), new error codes, new 4xxx close codes (an unknown one: do not reconnect automatically),
  wider `id` and `topic` sets, larger limits. Not allowed: a new required member, a changed meaning,
  a removed member, a smaller limit. Those need `owlkei.v2`.

## Test vectors

- **V20.** One JSON file per case in [`vectors/`](vectors), checked against
  [`vector.schema.json`](vector.schema.json) by `make check-spec`.
  - `kind: "parse"` needs no server. `input` is `{"text": …}`, `{"binary": {"envelope": …,
    "dataBase64": …}}` or `{"frameBase64": …}` for malformed bytes. `want` is
    `{"type", "id", "topic", "data" | "dataBase64"}` (only the members present) or `{"error": 400}`.
  - `kind: "session"` runs against the [conformance app](#conformance-app). `steps` is a list of:
    `{"send": {"text" | "binary" | "frameBase64"}}`; `{"expect": {"json": …}}` where every listed
    member must match and no unlisted member may be present, or `{"expect": {"binary": {"json": …,
    "dataBase64": …}}}`; `{"expectClose": {"code": …}}` (0 for no close frame); `{"wait": {"ms": …}}`.
    An `expect` consumes the first received frame of the expected `type` (the `type` member of
    `json`, or of the binary envelope); frames of other types stay queued, in order, for the
    following steps. When a session ends, every queued frame other than an `event` is a failure.
  - Matchers inside an expected value: `"@id"` (16 lower-case hex digits), `"@any"` (any value),
    `"@absent"` (the member must be missing), `"@same:<name>"` (the first match binds the name, later
    ones must equal it).

## Conformance app

The topics an Owlkei Go conformance server exposes, part of this spec because the session vectors
use them:

| Topic | Subscribe | Publish |
|---|---|---|
| `echo` | anyone | replies with its `data` |
| `listen` | anyone | not declared (405) |
| `rooms.{room}.messages` | anyone | fans the `data` out as an `event` on the same topic, replies nothing |
| `guarded` | refused by a guard (403, `message` `Forbidden`) | - |
| `validated` | anyone | reads `{"name": string, required}`; replies `{"name": …}`; a missing name is 422 with `fields.name` `{"rule":"required","message":"This field is required"}` |
| `binary` | anyone | takes binary data, replies the same bytes in a binary frame; a text frame is 415 |
| `slow` | anyone | waits 200 ms, then replies nothing |
| `pending` | a module that never becomes ready (503 with `retryAfter`) | - |
| `failed` | a module whose start failed (503 without `retryAfter`) | - |

A second app, the **budget app**, declares the same topics without `pending` and `failed`. Its
`WS.MaxMemory` is 1 MiB and its `MaxConnections` 51, with no limit per address: 51 open connections
hold 51 × 20 KiB of the 1,048,576 bytes, so less than one message cost (8 KiB) is left, and the next
message of any of them, a `ping` included, is closed with 1013. A suite fills it with 50 idle
connections next to its client's to check that the client reconnects after 1013 and subscribes
again (V15, V17).
