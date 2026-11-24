package ws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.owlkei.com/websocket"
)

type fakeServer struct {
	t       *testing.T
	srv     *httptest.Server
	conns   chan *serverConn
	mu      sync.Mutex
	headers []http.Header
}

type serverConn struct {
	t    *testing.T
	conn *websocket.Conn
}

func newServer(t *testing.T) *fakeServer {
	t.Helper()
	up, err := websocket.NewUpgrader(&websocket.UpgradeOptions{
		CheckOrigin:  func(*http.Request) bool { return true },
		Subprotocols: []string{Protocol},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeServer{t: t, conns: make(chan *serverConn, 8)}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.headers = append(s.headers, r.Header.Clone())
		s.mu.Unlock()
		conn, err := up.Upgrade(w, r)
		if err != nil {
			return
		}
		s.conns <- &serverConn{t: t, conn: conn}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *fakeServer) url() string {
	return "ws" + strings.TrimPrefix(s.srv.URL, "http") + "/ws"
}

func (s *fakeServer) accept() *serverConn {
	s.t.Helper()
	select {
	case c := <-s.conns:
		s.t.Cleanup(c.conn.Abort)
		return c
	case <-time.After(5 * time.Second):
		s.t.Fatal("no connection arrived")
		return nil
	}
}

func (s *fakeServer) dial(opts *Options) (*Client, *serverConn) {
	s.t.Helper()
	if opts == nil {
		opts = &Options{}
	}
	if opts.MinBackoff == 0 {
		opts.MinBackoff = time.Millisecond
		opts.MaxBackoff = 5 * time.Millisecond
	}
	c, err := Dial(context.Background(), s.url(), opts)
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { _ = c.Close() })
	return c, s.accept()
}

func (c *serverConn) read() (map[string]any, []byte, bool) {
	c.t.Helper()
	kind, frame, err := c.conn.ReadMessage(nil)
	if err != nil {
		c.t.Fatal(err)
	}
	if kind == websocket.KindBinary {
		in, err := decodeBinary(frame)
		if err != nil {
			c.t.Fatal(err)
		}
		var m map[string]any
		head, _ := json.Marshal(in.envelope)
		_ = json.Unmarshal(head, &m)
		return m, in.binary, true
	}
	var m map[string]any
	if err := json.Unmarshal(frame, &m); err != nil {
		c.t.Fatal(err)
	}
	return m, nil, false
}

func (c *serverConn) readJSON() map[string]any {
	c.t.Helper()
	m, _, _ := c.read()
	return m
}

func (c *serverConn) send(v any) {
	c.t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		c.t.Fatal(err)
	}
	if err := c.conn.WriteMessage(websocket.KindText, b); err != nil {
		c.t.Fatal(err)
	}
}

func (c *serverConn) ack(id any) {
	c.send(map[string]any{"type": "ack", "id": id, "requestId": "0123456789abcdef"})
}

func (c *serverConn) sendBinary(envelope map[string]any, data []byte) {
	c.t.Helper()
	head, _ := json.Marshal(envelope)
	frame := make([]byte, 4+len(head)+len(data))
	frame[0], frame[1], frame[2], frame[3] = 0, 0, byte(len(head)>>8), byte(len(head))
	copy(frame[4:], head)
	copy(frame[4+len(head):], data)
	if err := c.conn.WriteMessage(websocket.KindBinary, frame); err != nil {
		c.t.Fatal(err)
	}
}

func subscribeAcked(t *testing.T, c *Client, sc *serverConn, topic string, h Handler) *Subscription {
	t.Helper()
	type result struct {
		sub *Subscription
		err error
	}
	done := make(chan result, 1)
	go func() {
		sub, err := c.Subscribe(context.Background(), topic, h)
		done <- result{sub, err}
	}()
	m := sc.readJSON()
	sc.ack(m["id"])
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r.sub
}

func busy(id any, retryAfter int) map[string]any {
	m := map[string]any{
		"type": "error", "id": id, "requestId": "0123456789abcdef",
		"error": map[string]any{"code": 503, "error": "Service Unavailable", "message": "Service Unavailable", "requestId": "0123456789abcdef"},
	}
	if retryAfter > 0 {
		m["retryAfter"] = retryAfter
	}
	return m
}
