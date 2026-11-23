# go.owlkei.com/sdk

The Go client SDK for Owlkei Go apps. Package `ws` speaks the WebSocket wire protocol v1
(`owlkei.v1`, [spec](../spec/ws/v1/README.md)) on `go.owlkei.com/websocket`.

```go
c, err := ws.Dial(ctx, "wss://app.example.com/ws", &ws.Options{
	Header: http.Header{"Authorization": {"Bearer " + token}},
})
if err != nil {
	return err
}
defer c.Close()

sub, err := c.Subscribe(ctx, "rooms.42.messages", func(e ws.Event) {
	messages <- e.Data // handlers run on the client's goroutine: hand work off, return fast
})

reply, err := c.Publish(ctx, "rooms.42.messages", NewMessage{Text: "hi"})
var e *ws.Error
if errors.As(err, &e) && e.Code == 422 {
	fmt.Println(e.Fields)
}
```

| Name | Does |
|---|---|
| `Dial(ctx, url, *Options)` | connects, offering `owlkei.v1`; the first connection must succeed, later losses reconnect in the background |
| `Client.Subscribe(ctx, topic, Handler)` | returns on the server's ack; the handler receives each `Event` |
| `Subscription.Unsubscribe(ctx)` | removes the handler; the last handler of a topic unsubscribes the connection |
| `Client.Publish(ctx, topic, data)` | returns the server's `Reply`; a `ws.Binary` goes in a binary frame, `nil` sends no data, other values are JSON; a bare `[]byte` is `ErrAmbiguousBytes` |
| `Client.Ping(ctx)` | a JSON ping, returns on its pong |
| `Client.Close()` | closes for good; pending calls return `ErrClosed` |

Errors: `*ws.Error` (the server refused: `Code`, `Status`, `Message`, `Fields`, `RequestID`,
`RetryAfter`), `*ws.ConnectionError` (`CloseCode`), `ErrTimeout`, `ErrClosed`, `ErrInvalidTopic`.

`Options`: `Header`, `IsNoReconnect`, `MinBackoff` / `MaxBackoff` (1 s / 30 s, full jitter; 1013 waits
four times longer, 4408 reconnects at once), `RequestTimeout` (10 s), `HeartbeatInterval` (25 s,
control pings), `OnError` (errors that answer no request), `NetDial`. The client never reconnects
after 1000, 1002, 1003, 1007, 1008, 4401, 4403 or an unknown 3xxx / 4xxx code. After a reconnect it
subscribes again to every topic with a handler.

Until `go.owlkei.com` serves its vanity imports, `go.mod` replaces `go.owlkei.com/websocket` with the
checkout at `../../websocket`.
