package ws

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.owlkei.com/websocket"
)

// Protocol is the subprotocol of the wire protocol v1, offered in every handshake.
const Protocol = "owlkei.v1"

// Binary is binary data: Publish sends it in a binary frame, and binary events and replies carry it.
type Binary []byte

// Options configures a Client. A nil *Options takes every default.
type Options struct {
	// Header holds extra handshake headers, such as Origin, Cookie or Authorization.
	Header http.Header
	// IsNoReconnect turns off reconnecting after the connection is lost. Default: false.
	IsNoReconnect bool
	// MinBackoff and MaxBackoff bound the exponential backoff with full jitter. Default: 1s and 30s.
	MinBackoff, MaxBackoff time.Duration
	// RequestTimeout bounds the wait for each answer. Default: 10s.
	RequestTimeout time.Duration
	// HeartbeatInterval is the interval of the control ping. Default: 25s.
	HeartbeatInterval time.Duration
	// OnError receives errors that answer no request: an error message without an id, a failed
	// resubscribe after a reconnect. Called from the client's goroutine; it must not block.
	OnError func(error)
	// NetDial opens the transport; tests use it. Default: the websocket package's dialer.
	NetDial func(ctx context.Context, network, address string) (net.Conn, error)
}

// Event is one message published on a subscribed topic. Data holds the JSON value; for a binary
// event IsBinary is true and Binary holds the bytes.
type Event struct {
	Topic    string
	Data     json.RawMessage
	Binary   Binary
	IsBinary bool
}

// Reply is the server's answer to a Publish. Data is empty when the server has no value.
type Reply struct {
	Data     json.RawMessage
	Binary   Binary
	IsBinary bool
}

// Decode unmarshals the reply's JSON value into v.
func (r Reply) Decode(v any) error {
	if r.IsBinary {
		return errors.New("ws: the reply is binary")
	}
	return json.Unmarshal(r.Data, v)
}

// Handler receives the events of a topic, one at a time, on the client's read goroutine: it must
// return quickly and hand slow work to another goroutine.
type Handler func(Event)

type answer struct {
	in  incoming
	err error
}

type topic struct {
	handlers map[*Subscription]Handler
	joined   bool
	ready    chan struct{}
	err      error
}

// Client is a connection to an Owlkei Go WebSocket endpoint. It reconnects after a loss as the
// close code allows and subscribes again to every topic it held. Its methods are safe for
// concurrent use.
type Client struct {
	url  string
	opts Options

	mu      sync.Mutex
	conn    *websocket.Conn
	open    chan struct{}
	seq     uint64
	pending map[string]chan answer
	topics  map[string]*topic
	isClose bool
	closed  chan struct{}
	done    sync.WaitGroup
}

// Subscription is one handler on one topic.
type Subscription struct {
	client *Client
	topic  string
}

// Dial connects to url (ws:// or wss://), offering owlkei.v1. The first connection must succeed;
// later losses reconnect in the background.
func Dial(ctx context.Context, url string, opts *Options) (*Client, error) {
	c := &Client{
		url:     url,
		pending: make(map[string]chan answer),
		topics:  make(map[string]*topic),
		closed:  make(chan struct{}),
		open:    make(chan struct{}),
	}
	if opts != nil {
		c.opts = *opts
	}
	c.opts.MinBackoff = orDefault(c.opts.MinBackoff, time.Second)
	c.opts.MaxBackoff = orDefault(c.opts.MaxBackoff, 30*time.Second)
	c.opts.RequestTimeout = orDefault(c.opts.RequestTimeout, 10*time.Second)
	c.opts.HeartbeatInterval = orDefault(c.opts.HeartbeatInterval, 25*time.Second)
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	c.setConn(conn)
	c.done.Add(2)
	go c.run(conn)
	go c.heartbeat()
	return c, nil
}

// Subscribe subscribes the connection to topic and adds handler. It returns on the server's ack:
// every event published after that reaches the handler. A 503 refusal is sent again after its
// RetryAfter (1s without one) on the same connection, as long as the wait ends within
// Options.RequestTimeout; any other refusal, or the last 503, is an *Error.
func (c *Client) Subscribe(ctx context.Context, name string, handler Handler) (*Subscription, error) {
	if !IsTopic(name) {
		return nil, ErrInvalidTopic
	}
	if handler == nil {
		return nil, errors.New("ws: Subscribe needs a handler")
	}
	sub := &Subscription{client: c, topic: name}
	c.mu.Lock()
	if c.isClose {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	t := c.topics[name]
	if t != nil {
		t.handlers[sub] = handler
		ready := t.ready
		c.mu.Unlock()
		select {
		case <-ready:
		case <-ctx.Done():
			c.remove(sub)
			return nil, ctx.Err()
		}
		c.mu.Lock()
		err := t.err
		c.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return sub, nil
	}
	t = &topic{handlers: map[*Subscription]Handler{sub: handler}, ready: make(chan struct{})}
	c.topics[name] = t
	c.mu.Unlock()
	if err := c.join(ctx, name, t); err != nil {
		return nil, err
	}
	return sub, nil
}

// Unsubscribe removes the subscription's handler; the last handler of a topic also unsubscribes the
// connection.
func (s *Subscription) Unsubscribe(ctx context.Context) error {
	if !s.client.remove(s) {
		return nil
	}
	_, err := s.client.request(ctx, envelope{Type: "unsubscribe", Topic: s.topic}, nil, false)
	if errors.Is(err, ErrClosed) {
		return nil
	}
	return err
}

// Publish publishes data to topic, which the connection must have subscribed to. A Binary goes in a
// binary frame, nil sends no data, any other value is marshaled to JSON. A bare []byte is refused
// with ErrAmbiguousBytes.
func (c *Client) Publish(ctx context.Context, name string, data any) (Reply, error) {
	if !IsTopic(name) {
		return Reply{}, ErrInvalidTopic
	}
	e := envelope{Type: "publish", Topic: name}
	var payload []byte
	isBinary := false
	switch v := data.(type) {
	case nil:
	case Binary:
		payload, isBinary = v, true
	case []byte:
		return Reply{}, ErrAmbiguousBytes
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return Reply{}, err
		}
		e.Data = raw
	}
	in, err := c.request(ctx, e, payload, isBinary)
	if err != nil {
		return Reply{}, err
	}
	if in.isBinary {
		return Reply{Binary: Binary(in.binary), IsBinary: true}, nil
	}
	if isJSONNull(in.envelope.Data) {
		return Reply{}, nil
	}
	return Reply{Data: in.envelope.Data}, nil
}

// Ping sends a JSON ping and returns on its pong.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.request(ctx, envelope{Type: "ping"}, nil, false)
	return err
}

// Close closes the connection for good. Pending calls return ErrClosed.
func (c *Client) Close() error {
	conn, ok := c.shutdown()
	if !ok {
		c.done.Wait()
		return nil
	}
	var err error
	if conn != nil {
		err = conn.Close(websocket.CloseNormal, "")
		var ce *websocket.CloseError
		if errors.As(err, &ce) || errors.Is(err, websocket.ErrClosed) {
			err = nil
		}
	}
	c.done.Wait()
	return err
}

func (c *Client) shutdown() (*websocket.Conn, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClose {
		return nil, false
	}
	c.isClose = true
	close(c.closed)
	c.failPendingLocked(ErrClosed)
	return c.conn, true
}

func (c *Client) join(ctx context.Context, name string, t *topic) error {
	err := c.subscribe(ctx, name)
	c.mu.Lock()
	t.err = err
	if err == nil {
		t.joined = true
	} else if c.topics[name] == t {
		delete(c.topics, name)
	}
	close(t.ready)
	c.mu.Unlock()
	return err
}

func (c *Client) subscribe(ctx context.Context, name string) error {
	deadline := time.Now().Add(c.opts.RequestTimeout)
	for {
		_, err := c.request(ctx, envelope{Type: "subscribe", Topic: name}, nil, false)
		var e *Error
		if !errors.As(err, &e) || e.Code != codeBusy {
			return err
		}
		wait := subscribeRetryWait(e.RetryAfter)
		if !time.Now().Add(wait).Before(deadline) {
			return err
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-c.closed:
			timer.Stop()
			return ErrClosed
		}
	}
}

func (c *Client) remove(s *Subscription) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.topics[s.topic]
	if t == nil {
		return false
	}
	if _, ok := t.handlers[s]; !ok {
		return false
	}
	delete(t.handlers, s)
	if len(t.handlers) > 0 {
		return false
	}
	delete(c.topics, s.topic)
	return t.joined
}

func (c *Client) request(ctx context.Context, e envelope, payload []byte, isBinary bool) (incoming, error) {
	ctx, cancel := context.WithTimeout(ctx, c.opts.RequestTimeout)
	defer cancel()
	conn, err := c.waitOpen(ctx)
	if err != nil {
		return incoming{}, err
	}
	ch := make(chan answer, 1)
	c.mu.Lock()
	c.seq++
	e.ID = strconv.FormatUint(c.seq, 10)
	c.pending[e.ID] = ch
	c.mu.Unlock()
	defer c.forget(e.ID)

	var frame []byte
	kind := websocket.KindText
	if isBinary {
		frame, err = encodeBinary(e, payload)
		kind = websocket.KindBinary
	} else {
		frame, err = encodeText(e)
	}
	if err != nil {
		return incoming{}, err
	}
	if err := conn.WriteMessage(kind, frame); err != nil {
		return incoming{}, c.lostError(err)
	}
	select {
	case a := <-ch:
		return a.in, a.err
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return incoming{}, ErrTimeout
		}
		return incoming{}, ctx.Err()
	}
}

func (c *Client) waitOpen(ctx context.Context) (*websocket.Conn, error) {
	for {
		c.mu.Lock()
		if c.isClose {
			c.mu.Unlock()
			return nil, ErrClosed
		}
		conn, open := c.conn, c.open
		c.mu.Unlock()
		if conn != nil {
			return conn, nil
		}
		select {
		case <-open:
		case <-c.closed:
			return nil, ErrClosed
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, ErrTimeout
			}
			return nil, ctx.Err()
		}
	}
}

func (c *Client) forget(id string) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *Client) dial(ctx context.Context) (*websocket.Conn, error) {
	return websocket.Dial(ctx, c.url, &websocket.DialOptions{
		Header:       c.opts.Header,
		Subprotocols: []string{Protocol},
		NetDial:      c.opts.NetDial,
	})
}

func (c *Client) setConn(conn *websocket.Conn) {
	c.mu.Lock()
	c.conn = conn
	close(c.open)
	c.mu.Unlock()
}

func (c *Client) run(conn *websocket.Conn) {
	defer c.done.Done()
	attempt := 0
	for {
		code := c.read(conn)
		c.mu.Lock()
		c.conn = nil
		c.open = make(chan struct{})
		c.failPendingLocked(&ConnectionError{CloseCode: code})
		for _, t := range c.topics {
			t.joined = false
		}
		isClose := c.isClose
		c.mu.Unlock()
		p := planFor(code)
		if isClose || c.opts.IsNoReconnect || !p.retry {
			c.shutdown()
			return
		}
		next, ok := c.reconnect(p, code, &attempt)
		if !ok {
			return
		}
		conn = next
	}
}

func (c *Client) reconnect(p plan, code int, attempt *int) (*websocket.Conn, bool) {
	for {
		n := *attempt
		if code == codeLifetime {
			n = 0
		}
		wait := delayFor(p, n, c.opts.MinBackoff, c.opts.MaxBackoff, jitter)
		*attempt++
		timer := time.NewTimer(wait)
		select {
		case <-c.closed:
			timer.Stop()
			return nil, false
		case <-timer.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.opts.RequestTimeout)
		conn, err := c.dial(ctx)
		cancel()
		if err != nil {
			p, code = plan{retry: true, scale: 1}, codeAbnormal
			continue
		}
		*attempt = 0
		c.setConn(conn)
		c.done.Add(1)
		go c.rejoin()
		return conn, true
	}
}

func (c *Client) rejoin() {
	defer c.done.Done()
	c.mu.Lock()
	names := make([]string, 0, len(c.topics))
	topics := make([]*topic, 0, len(c.topics))
	for name, t := range c.topics {
		if len(t.handlers) > 0 {
			t.ready = make(chan struct{})
			names = append(names, name)
			topics = append(topics, t)
		}
	}
	c.mu.Unlock()
	for i, name := range names {
		if err := c.join(context.Background(), name, topics[i]); err != nil && !errors.Is(err, ErrClosed) {
			c.report(err)
		}
	}
}

func (c *Client) read(conn *websocket.Conn) int {
	for {
		kind, frame, err := conn.ReadMessage(nil)
		if err != nil {
			return closeCodeOf(err)
		}
		var in incoming
		if kind == websocket.KindBinary {
			in, err = decodeBinary(frame)
		} else {
			in, err = decodeText(frame)
		}
		if err == nil {
			c.receive(in)
		}
	}
}

func (c *Client) receive(in incoming) {
	e := in.envelope
	switch e.Type {
	case "ack", "pong", "reply":
		c.answer(e.ID, answer{in: in})
	case "error":
		err := errorOf(e)
		if !c.answer(e.ID, answer{err: err}) {
			c.report(err)
		}
	case "event":
		c.mu.Lock()
		t := c.topics[e.Topic]
		var handlers []Handler
		if t != nil {
			handlers = make([]Handler, 0, len(t.handlers))
			for _, h := range t.handlers {
				handlers = append(handlers, h)
			}
		}
		c.mu.Unlock()
		ev := Event{Topic: e.Topic, Data: e.Data, Binary: Binary(in.binary), IsBinary: in.isBinary}
		for _, h := range handlers {
			h(ev)
		}
	}
}

func (c *Client) answer(id string, a answer) bool {
	if id == "" {
		return false
	}
	c.mu.Lock()
	ch, ok := c.pending[id]
	if ok {
		delete(c.pending, id)
	}
	c.mu.Unlock()
	if ok {
		ch <- a
	}
	return ok
}

func (c *Client) failPendingLocked(err error) {
	for id, ch := range c.pending {
		delete(c.pending, id)
		ch <- answer{err: err}
	}
}

func (c *Client) heartbeat() {
	defer c.done.Done()
	ticker := time.NewTicker(c.opts.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			c.mu.Lock()
			conn := c.conn
			c.mu.Unlock()
			if conn != nil {
				_ = conn.Ping(nil)
			}
		}
	}
}

func (c *Client) report(err error) {
	if c.opts.OnError != nil {
		c.opts.OnError(err)
	}
}

func (c *Client) lostError(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClose {
		return ErrClosed
	}
	return &ConnectionError{CloseCode: closeCodeOf(err)}
}

func closeCodeOf(err error) int {
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		return int(ce.Code())
	}
	return codeAbnormal
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}
