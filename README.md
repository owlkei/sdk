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

See [CLAUDE.md](CLAUDE.md) for the layout and the rules every change follows.
