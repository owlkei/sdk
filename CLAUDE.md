# CLAUDE.md

Guidance for Claude Code in the Owlkei SDK repository.

## What this is

The client SDK for Owlkei Go apps: one repository for every language and every protocol.

| Path | Holds |
|---|---|
| `spec/<protocol>/v<N>/` | the wire contract between an Owlkei Go server and every client: the spec (`README.md`) and its test vectors (`vectors/*.json`) |
| `js/` | the npm package `@owlkei/sdk` in TypeScript; one subpath entry per protocol (`@owlkei/sdk/ws`, `@owlkei/sdk/devtools`), plus a `<script>` bundle per entry (`window.Owlkei`) |
| `go/` | the Go module `go.owlkei.com/sdk`; one package per protocol (`go.owlkei.com/sdk/ws`) |
| `scripts/` | repository tooling (`check-spec.mjs`) |

Protocols today: WebSocket (`ws`), and the devtools (`devtools`, TypeScript only), which runs on
`owlkei.v1` and is built on `@owlkei/sdk/ws` by composition. HTTP and GraphQL come later, each as a
new folder in `spec/`, `js/src/` and `go/`; adding one never changes another. Shared code lives in `core/` only when at
least two protocols need it.

## Rules

- **The server leads.** The spec follows what Owlkei Go implements; a change to the server side of
  a protocol updates that protocol's spec, both clients and the conformance tests in the same step.
  The SDK never asks the framework to change for its sake.
- **Every client passes the spec's vectors and the conformance suite** against a real Owlkei Go
  server. A behavior without a vector or a conformance test is not done.
- **Each language is a normal project** with its full tooling; `make check` runs what CI runs.
- **No runtime dependency** in either language (the Go client uses `go/internal/websocket`, a copy of
  the RFC 6455 package of Owlkei Go, `internal/websocket`, kept identical by Owlkei Go's
  `make ws-sync-check`; change it there first). Size budgets in `js/.size-limit.json` are checked in CI.
- **Errors never leak** a server's internal text: the client surfaces the server's error body
  (`code`, `error`, `message`, `fields`, `requestId`) as typed errors.
- Code is written as production code: small public API, every exported name documented, tests for
  every behavior, no speculative abstraction.

## Commands

```bash
make deps        # yarn install (root hooks + js/)
make check       # spec + js + go, what CI runs
make check-js    # cd js && yarn run check: prettier, eslint (strictTypeChecked), tsc, vitest, tsup, publint, attw, size-limit
make check-go    # cd go: gofmt, go vet, golangci-lint, go test -race
cd js && OWLKEI_CONFORMANCE_URL=ws://127.0.0.1:3000/ws OWLKEI_CONFORMANCE_BUDGET_URL=ws://127.0.0.1:3001/ws yarn run conformance  # session vectors and the Client against a server and its budget app
# add OWLKEI_CONFORMANCE_DEVTOOLS_URL=http://127.0.0.1:3000 OWLKEI_CONFORMANCE_DEVTOOLS_TOKEN=okd1_… for the devtools vectors and client
```

The conformance server is `internal/conformance` in `owlkei/go` (`PORT`, `BUDGET_PORT` for the
budget app, `DEVTOOLS_TOKEN_FILE` for the devtools token); its `make conformance` runs the
TypeScript suite above and the Go client suite (`internal/conformance/sdkclient`) against it.

## TypeScript

TypeScript 6.0 (typescript-eslint supports up to 6.0) in `strict` mode with
`noUncheckedIndexedAccess` and `exactOptionalPropertyTypes`; ESLint `strictTypeChecked` and
`stylisticTypeChecked`; Prettier (width 100); Vitest; tsup builds ESM with `.d.ts` and the IIFE
bundle. Targets: browsers, Node 22+, Deno, Bun and React Native (the platform `WebSocket`).

## Go

Go 1.27, standard library only. gofmt, vet, golangci-lint (`go/.golangci.yml`),
`go test -race`. Exported identifiers have godoc comments (a public module); unexported code has
none. Tests: `Test<Subject>_<Behavior>`, failures through `internal/test.DiffMessage`.

`go/internal/websocket` is not edited here: Owlkei Go's `internal/websocket` (proven by Autobahn) is
the source, and its `make ws-sync-check` fails when the two differ.

## Commits

Conventional Commits in English, one concern per commit, no AI attribution or trailers.
