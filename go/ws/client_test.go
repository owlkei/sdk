package ws

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"go.owlkei.com/sdk/internal/test"
	"go.owlkei.com/websocket"
)

func TestDial_OffersTheProtocol(t *testing.T) {
	s := newServer(t)
	c, _ := s.dial(nil)
	if got := c.conn.Subprotocol(); got != Protocol {
		t.Error(test.DiffMessage(got, Protocol, "V1: the client offers owlkei.v1"))
	}
}

func TestDial_SendsTheHeaders(t *testing.T) {
	s := newServer(t)
	s.dial(&Options{Header: map[string][]string{"Authorization": {"Bearer t"}}})
	if got := s.headers[0].Get("Authorization"); got != "Bearer t" {
		t.Error(test.DiffMessage(got, "Bearer t", "Options.Header reaches the handshake"))
	}
}

func TestDial_FailsWhenTheServerIsDown(t *testing.T) {
	s := newServer(t)
	url := s.url()
	s.srv.Close()
	if _, err := Dial(context.Background(), url, nil); err == nil {
		t.Error(test.DiffMessage(nil, "an error", "the first connection must succeed"))
	}
}

func TestSubscribe_SendsSubscribeAndReturnsOnAck(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	done := make(chan error, 1)
	go func() {
		_, err := c.Subscribe(context.Background(), "rooms.42.messages", func(Event) {})
		done <- err
	}()
	m := sc.readJSON()
	want := map[string]any{"type": "subscribe", "id": "1", "topic": "rooms.42.messages"}
	if !reflect.DeepEqual(m, want) {
		t.Error(test.DiffMessage(m, want, "V7: the subscribe envelope"))
	}
	sc.ack("1")
	if err := <-done; err != nil {
		t.Error(test.DiffMessage(err, nil, "V7: the ack ends the subscribe"))
	}
}

func TestSubscribe_DeliversEventsOfItsTopic(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	events := make(chan Event, 2)
	subscribeAcked(t, c, sc, "rooms.42.messages", func(e Event) { events <- e })
	sc.send(map[string]any{"type": "event", "topic": "rooms.43.messages", "data": 0})
	sc.send(map[string]any{"type": "event", "topic": "rooms.42.messages", "data": map[string]any{"text": "hi"}})
	e := <-events
	if e.Topic != "rooms.42.messages" || string(e.Data) != `{"text":"hi"}` || e.IsBinary {
		t.Error(test.DiffMessage(e, `rooms.42.messages {"text":"hi"}`, "V10: only the topic's events reach the handler"))
	}
}

func TestSubscribe_OneServerSubscriptionForSeveralHandlers(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	first, second := make(chan Event, 1), make(chan Event, 1)
	subscribeAcked(t, c, sc, "echo", func(e Event) { first <- e })
	if _, err := c.Subscribe(context.Background(), "echo", func(e Event) { second <- e }); err != nil {
		t.Fatal(err)
	}
	sc.send(map[string]any{"type": "event", "topic": "echo", "data": 1})
	<-first
	<-second
	go func() { _ = c.Ping(context.Background()) }()
	if m := sc.readJSON(); m["type"] != "ping" {
		t.Error(test.DiffMessage(m, "the ping, no second subscribe", "one subscribe per topic"))
	}
}

func TestSubscribe_RefusalIsATypedError(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	done := make(chan error, 1)
	go func() {
		_, err := c.Subscribe(context.Background(), "guarded", func(Event) {})
		done <- err
	}()
	m := sc.readJSON()
	sc.send(map[string]any{
		"type": "error", "id": m["id"], "requestId": "0123456789abcdef",
		"error": map[string]any{"code": 403, "error": "Forbidden", "message": "Forbidden", "requestId": "0123456789abcdef"},
	})
	var e *Error
	if err := <-done; !errors.As(err, &e) || e.Code != 403 || e.Status != "Forbidden" || e.Message != "Forbidden" || e.RequestID != "0123456789abcdef" {
		t.Error(test.DiffMessage(err, "*Error 403 Forbidden", "V11: the server's error body"))
	}
}

func TestSubscribe_InvalidTopicSendsNothing(t *testing.T) {
	s := newServer(t)
	c, _ := s.dial(nil)
	for _, topic := range []string{"", "rooms.*", "a..b", "rooms."} {
		if _, err := c.Subscribe(context.Background(), topic, func(Event) {}); !errors.Is(err, ErrInvalidTopic) {
			t.Error(test.DiffMessage(err, ErrInvalidTopic, "V5: "+topic))
		}
	}
	if _, err := c.Publish(context.Background(), "a b", nil); !errors.Is(err, ErrInvalidTopic) {
		t.Error(test.DiffMessage(err, ErrInvalidTopic, "V5: a publish topic"))
	}
}

func TestUnsubscribe_LastHandlerUnsubscribes(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	a := subscribeAcked(t, c, sc, "echo", func(Event) {})
	b, err := c.Subscribe(context.Background(), "echo", func(Event) {})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Unsubscribe(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Unsubscribe(context.Background()) }()
	m := sc.readJSON()
	if m["type"] != "unsubscribe" || m["topic"] != "echo" {
		t.Error(test.DiffMessage(m, "an unsubscribe of echo", "V8: the last handler unsubscribes"))
	}
	sc.ack(m["id"])
	if err := <-done; err != nil {
		t.Error(test.DiffMessage(err, nil, "V8: the ack ends the unsubscribe"))
	}
}

func TestPublish_JSONReply(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	subscribeAcked(t, c, sc, "echo", func(Event) {})
	done := make(chan Reply, 1)
	go func() {
		r, err := c.Publish(context.Background(), "echo", map[string]int{"a": 1})
		if err != nil {
			t.Error(err)
		}
		done <- r
	}()
	m := sc.readJSON()
	want := map[string]any{"type": "publish", "id": "2", "topic": "echo", "data": map[string]any{"a": float64(1)}}
	if !reflect.DeepEqual(m, want) {
		t.Error(test.DiffMessage(m, want, "V9: the publish envelope"))
	}
	sc.send(map[string]any{"type": "reply", "id": "2", "requestId": "0123456789abcdef", "data": map[string]int{"a": 1}})
	r := <-done
	var got map[string]int
	if err := r.Decode(&got); err != nil || got["a"] != 1 {
		t.Error(test.DiffMessage(got, map[string]int{"a": 1}, "V9: the reply's value"))
	}
}

func TestPublish_ReplyWithoutData(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	subscribeAcked(t, c, sc, "slow", func(Event) {})
	done := make(chan Reply, 1)
	go func() {
		r, _ := c.Publish(context.Background(), "slow", nil)
		done <- r
	}()
	m := sc.readJSON()
	if _, ok := m["data"]; ok {
		t.Error(test.DiffMessage(m, "no data member", "a nil value sends no data"))
	}
	sc.send(map[string]any{"type": "reply", "id": m["id"], "requestId": "0123456789abcdef"})
	if r := <-done; len(r.Data) != 0 || r.IsBinary {
		t.Error(test.DiffMessage(r, Reply{}, "V9: a reply without data"))
	}
}

func TestPublish_BinaryFrameAndBinaryReply(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	subscribeAcked(t, c, sc, "binary", func(Event) {})
	done := make(chan Reply, 1)
	go func() {
		r, err := c.Publish(context.Background(), "binary", Binary{1, 2, 3})
		if err != nil {
			t.Error(err)
		}
		done <- r
	}()
	m, data, isBinary := sc.read()
	if !isBinary || !reflect.DeepEqual(data, []byte{1, 2, 3}) || m["type"] != "publish" || m["topic"] != "binary" {
		t.Error(test.DiffMessage(m, "a binary publish of 1 2 3", "V4: a Binary goes in a binary frame"))
	}
	sc.sendBinary(map[string]any{"type": "reply", "id": m["id"], "requestId": "0123456789abcdef"}, []byte{9})
	if r := <-done; !r.IsBinary || !reflect.DeepEqual([]byte(r.Binary), []byte{9}) {
		t.Error(test.DiffMessage(r, "binary 9", "V9: a binary reply"))
	}
}

func TestPublish_RefusesBareBytes(t *testing.T) {
	s := newServer(t)
	c, _ := s.dial(nil)
	if _, err := c.Publish(context.Background(), "echo", []byte("x")); !errors.Is(err, ErrAmbiguousBytes) {
		t.Error(test.DiffMessage(err, ErrAmbiguousBytes, "a bare []byte is ambiguous"))
	}
}

func TestPublish_ValidationFields(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	subscribeAcked(t, c, sc, "validated", func(Event) {})
	done := make(chan error, 1)
	go func() {
		_, err := c.Publish(context.Background(), "validated", map[string]any{})
		done <- err
	}()
	m := sc.readJSON()
	sc.send(map[string]any{
		"type": "error", "id": m["id"], "requestId": "0123456789abcdef", "retryAfter": 2,
		"error": map[string]any{
			"code": 422, "error": "Unprocessable Entity", "message": "Unprocessable Entity",
			"fields": map[string]any{"name": map[string]any{"rule": "required", "message": "This field is required"}},
		},
	})
	var e *Error
	if err := <-done; !errors.As(err, &e) || e.Code != 422 || e.Fields["name"].Rule != "required" || e.RetryAfter != 2 {
		t.Error(test.DiffMessage(err, "*Error 422 with fields.name", "V11: validation fields and retryAfter"))
	}
}

func TestRequest_TimesOut(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(&Options{RequestTimeout: 50 * time.Millisecond})
	done := make(chan error, 1)
	go func() {
		_, err := c.Subscribe(context.Background(), "echo", func(Event) {})
		done <- err
	}()
	sc.readJSON()
	if err := <-done; !errors.Is(err, ErrTimeout) {
		t.Error(test.DiffMessage(err, ErrTimeout, "no answer within the request timeout"))
	}
}

func TestRequest_AnswersInAnyOrder(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	subscribeAcked(t, c, sc, "echo", func(Event) {})
	results := make(chan string, 2)
	publish := func(v string) {
		r, err := c.Publish(context.Background(), "echo", v)
		if err != nil {
			t.Error(err)
		}
		var got string
		_ = r.Decode(&got)
		results <- v + "=" + got
	}
	go publish("a")
	first := sc.readJSON()
	go publish("b")
	second := sc.readJSON()
	sc.send(map[string]any{"type": "reply", "id": second["id"], "requestId": "0123456789abcdef", "data": second["data"]})
	sc.send(map[string]any{"type": "reply", "id": first["id"], "requestId": "0123456789abcdef", "data": first["data"]})
	got := map[string]bool{<-results: true, <-results: true}
	if !got["a=a"] || !got["b=b"] {
		t.Error(test.DiffMessage(got, "a=a and b=b", "each answer reaches its request"))
	}
}

func TestPing_JSON(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	done := make(chan error, 1)
	go func() { done <- c.Ping(context.Background()) }()
	m := sc.readJSON()
	if m["type"] != "ping" {
		t.Error(test.DiffMessage(m, "a ping", "V13"))
	}
	sc.send(map[string]any{"type": "pong", "id": m["id"]})
	if err := <-done; err != nil {
		t.Error(test.DiffMessage(err, nil, "V13: the pong ends the ping"))
	}
}

func TestReceive_ErrorWithoutIDGoesToOnError(t *testing.T) {
	s := newServer(t)
	errs := make(chan error, 1)
	_, sc := s.dial(&Options{OnError: func(err error) { errs <- err }})
	sc.send(map[string]any{
		"type": "error", "requestId": "0123456789abcdef",
		"error": map[string]any{"code": 400, "error": "Bad Request", "message": "Invalid message"},
	})
	var e *Error
	if err := <-errs; !errors.As(err, &e) || e.Code != 400 {
		t.Error(test.DiffMessage(err, "*Error 400", "an error without an id"))
	}
}

func TestReconnect_AfterShutdownSubscribesAgain(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	events := make(chan Event, 1)
	subscribeAcked(t, c, sc, "echo", func(e Event) { events <- e })
	if err := sc.conn.Close(websocket.CloseGoingAway, ""); err != nil {
		var ce *websocket.CloseError
		if !errors.As(err, &ce) {
			t.Fatal(err)
		}
	}
	next := s.accept()
	m := next.readJSON()
	if m["type"] != "subscribe" || m["topic"] != "echo" {
		t.Error(test.DiffMessage(m, "a subscribe of echo", "V16: subscribe again after a reconnect"))
	}
	next.ack(m["id"])
	next.send(map[string]any{"type": "event", "topic": "echo", "data": 2})
	if e := <-events; string(e.Data) != "2" {
		t.Error(test.DiffMessage(string(e.Data), "2", "events after the reconnect reach the handler"))
	}
}

func TestReconnect_StopsOnAuthentication(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	_ = sc.conn.Close(4401, "")
	select {
	case <-c.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("V15: 4401 must end the client")
	}
	if _, err := c.Publish(context.Background(), "echo", 1); !errors.Is(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "calls after the end"))
	}
}

func TestReconnect_PendingRequestGetsTheCloseCode(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(&Options{IsNoReconnect: true})
	subscribeAcked(t, c, sc, "echo", func(Event) {})
	done := make(chan error, 1)
	go func() {
		_, err := c.Publish(context.Background(), "echo", 1)
		done <- err
	}()
	sc.readJSON()
	_ = sc.conn.Close(websocket.CloseGoingAway, "")
	var ce *ConnectionError
	if err := <-done; !errors.As(err, &ce) || ce.CloseCode != 1001 {
		t.Error(test.DiffMessage(err, "ConnectionError 1001", "a pending request gets the close code"))
	}
}

func TestClose_EndsPendingCalls(t *testing.T) {
	s := newServer(t)
	c, sc := s.dial(nil)
	done := make(chan error, 1)
	go func() {
		_, err := c.Subscribe(context.Background(), "echo", func(Event) {})
		done <- err
	}()
	sc.readJSON()
	go func() { _, _, _ = sc.conn.ReadMessage(nil) }()
	if err := c.Close(); err != nil {
		t.Error(test.DiffMessage(err, nil, "Close"))
	}
	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "a pending call after Close"))
	}
	if err := c.Close(); err != nil {
		t.Error(test.DiffMessage(err, nil, "a second Close"))
	}
}

func TestPlanFor_CloseCodes(t *testing.T) {
	cases := map[int]plan{
		1000: {}, 1002: {}, 1003: {}, 1007: {}, 1008: {}, 4401: {}, 4403: {}, 4999: {}, 3000: {},
		1001: {true, 1}, 1006: {true, 1}, 1009: {true, 1}, 1011: {true, 1}, 1012: {true, 1}, 4429: {true, 1},
		1013: {true, 4}, 4408: {true, 0},
	}
	for code, want := range cases {
		if got := planFor(code); got != want {
			t.Error(test.DiffMessage(got, want, "V15: close code "+strconv.Itoa(code)))
		}
	}
}

func TestDelayFor_GrowsAndCaps(t *testing.T) {
	top := func() float64 { return 0.999 }
	got := []time.Duration{}
	for attempt := 0; attempt < 5; attempt++ {
		got = append(got, delayFor(plan{true, 1}, attempt, time.Second, 5*time.Second, top).Round(time.Millisecond))
	}
	want := []time.Duration{999 * time.Millisecond, 1998 * time.Millisecond, 3996 * time.Millisecond, 4995 * time.Millisecond, 4995 * time.Millisecond}
	if !reflect.DeepEqual(got, want) {
		t.Error(test.DiffMessage(got, want, "V17: exponential backoff with a cap"))
	}
	if d := delayFor(plan{true, 0}, 7, time.Second, 5*time.Second, top); d >= time.Second {
		t.Error(test.DiffMessage(d, "< 1s", "V15: 4408 reconnects at once"))
	}
}
