# Owlkei SDK

The client SDK for Owlkei Go apps, in TypeScript and Go.

| Protocol | TypeScript | Go | Spec |
|---|---|---|---|
| WebSocket | `@owlkei/sdk/ws` | `go.owlkei.com/sdk/ws` | [`spec/ws/v1`](spec/ws/v1) |
| HTTP | later | later | later |

Each client speaks the wire protocol of an Owlkei Go server, is tested against the spec's vectors
and against a real server, and has no runtime dependency.

## Development

```bash
make deps    # install the JavaScript tooling and the git hooks
make check   # what CI runs: the spec vectors, the TypeScript package and the Go module
```

The session vectors run against a real Owlkei Go conformance server (`internal/conformance` in
`owlkei/go`, whose `make conformance` starts it and runs both clients):

```bash
cd js && OWLKEI_CONFORMANCE_URL=ws://127.0.0.1:3000/ws OWLKEI_CONFORMANCE_BUDGET_URL=ws://127.0.0.1:3001/ws yarn run conformance
```

It runs every session vector on the platform `WebSocket` with the SDK's frame codec, and the
`Client` itself against the conformance app's topics (replies, typed errors, fan-out, binary,
unsubscribe, reconnect), and against the budget app (a reconnect after 1013, `spec/ws/v1` ›
Conformance app). Without `OWLKEI_CONFORMANCE_URL` (or `OWLKEI_CONFORMANCE_BUDGET_URL`) those tests
are skipped. The Go client runs them from
`owlkei/go` (`internal/conformance/sdkclient`), which starts the server in-process.

See [CLAUDE.md](CLAUDE.md) for the layout and the rules every change follows.
