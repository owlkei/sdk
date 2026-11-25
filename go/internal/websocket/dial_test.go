package websocket

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.owlkei.com/sdk/internal/test"
)

type scriptedServer struct {
	requests chan *http.Request
	received chan []byte
}

func scriptedDial(respond func(r *http.Request) []byte) (func(context.Context, string, string) (net.Conn, error), *scriptedServer) {
	s := &scriptedServer{requests: make(chan *http.Request, 4), received: make(chan []byte, 4)}
	dial := func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			br := bufio.NewReader(server)
			r, err := http.ReadRequest(br)
			if err != nil {
				_ = server.Close()
				s.received <- nil
				return
			}
			s.requests <- r
			if reply := respond(r); reply != nil {
				_, _ = server.Write(reply)
			}
			rest, _ := io.ReadAll(br)
			_ = server.Close()
			s.received <- rest
		}()
		return client, nil
	}
	return dial, s
}

func acceptFor(r *http.Request) string {
	var out [28]byte
	return string(appendAccept(out[:0], r.Header.Get("Sec-WebSocket-Key")))
}

func response101(r *http.Request, extra string) []byte {
	return []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
		acceptFor(r) + "\r\n" + extra + "\r\n")
}

func validReply(r *http.Request) []byte { return response101(r, "") }

func dialScripted(t *testing.T, opts *DialOptions, respond func(r *http.Request) []byte) (*Conn, *scriptedServer, error) {
	t.Helper()
	dial, s := scriptedDial(respond)
	var o DialOptions
	if opts != nil {
		o = *opts
	}
	o.NetDial = dial
	c, err := Dial(context.Background(), "ws://example.test/chat", &o)
	return c, s, err
}

func expectRefusedDial(t *testing.T, rule string, c *Conn, err error, s *scriptedServer, class error, text string) {
	t.Helper()
	if c != nil {
		c.Abort()
		t.Error(test.DiffMessage(c, nil, rule+": no connection"))
	}
	checkErr(t, rule, err, class, text)
	if s == nil {
		return
	}
	select {
	case <-s.received:
	case <-time.After(5 * time.Second):
		t.Error(test.DiffMessage("open", "closed", rule+": the transport is closed after a refusal"))
	}
}

func badHandshakeDetail(detail string) string { return "websocket: bad handshake: " + detail }

func startEchoUpgrader(t *testing.T, opts *UpgradeOptions, server func(*httptest.Server)) (*httptest.Server, chan error) {
	t.Helper()
	u := mustUpgrader(t, opts)
	ends := make(chan error, 4)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := u.Upgrade(w, r)
		if err != nil {
			ends <- err
			return
		}
		buf := make([]byte, 0, 4096)
		for {
			kind, msg, err := c.ReadMessage(buf)
			if err != nil {
				ends <- err
				return
			}
			if string(msg) == "fragments please" {
				wr, _ := c.NextWriter(KindText)
				for _, part := range []string{"one ", "two ", "three"} {
					_, _ = wr.Write([]byte(part))
				}
				err = wr.Close()
			} else {
				err = c.WriteMessage(kind, msg)
			}
			if err != nil {
				ends <- err
				return
			}
		}
	}))
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	if server != nil {
		server(srv)
	} else {
		srv.Start()
	}
	t.Cleanup(srv.Close)
	return srv, ends
}

var bigConn = &ConnOptions{MaxMessageBytes: 1 << 20}

func addrOf(srv *httptest.Server) string { return srv.Listener.Addr().String() }

func expectEcho(t *testing.T, rule string, c *Conn, kind Kind, data []byte) {
	t.Helper()
	if err := c.WriteMessage(kind, data); err != nil {
		t.Fatal(err)
	}
	gotKind, got, err := c.ReadMessage(nil)
	if err != nil {
		t.Fatal(err)
	}
	if gotKind != kind || !bytes.Equal(got, data) {
		t.Error(test.DiffMessage(fmt.Sprint(gotKind, " ", len(got)), fmt.Sprint(kind, " ", len(data)), rule))
	}
}

func TestDial_TalksToUpgrader(t *testing.T) {
	srv, ends := startEchoUpgrader(t, &UpgradeOptions{CheckOrigin: allowAll, Subprotocols: []string{"chat"}, Conn: bigConn}, nil)
	c, err := Dial(context.Background(), "ws://"+addrOf(srv)+"/echo", &DialOptions{Subprotocols: []string{"other", "chat"}, Conn: bigConn})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Subprotocol(); got != "chat" {
		t.Error(test.DiffMessage(got, "chat", "D2: the subprotocol the server chose"))
	}
	expectEcho(t, "D1: a text message round trip", c, KindText, []byte("hello"))
	expectEcho(t, "D1: a binary message round trip", c, KindBinary, []byte{0, 1, 2, 0xff})
	expectEcho(t, "D1: a 70,000-byte message round trip", c, KindBinary, bytes.Repeat([]byte{7}, 70000))
	if err := c.Ping([]byte("p")); err != nil {
		t.Fatal(err)
	}
	w, err := c.NextWriter(KindText)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"frag", "mented"} {
		if _, err := w.Write([]byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, got, err := c.ReadMessage(nil); err != nil || string(got) != "fragmented" {
		t.Error(test.DiffMessage(string(got), "fragmented", "D1: a fragmented client message echoed whole"))
	}
	if err := c.WriteMessage(KindText, []byte("fragments please")); err != nil {
		t.Fatal(err)
	}
	if _, got, err := c.ReadMessage(nil); err != nil || string(got) != "one two three" {
		t.Error(test.DiffMessage(string(got), "one two three", "D1: a fragmented server message read whole"))
	}
	if err := c.Close(CloseNormal, "bye"); err != nil {
		t.Error(test.DiffMessage(err, nil, "C6: the client's close is answered"))
	}
	var ce *CloseError
	if end := <-ends; !errors.As(end, &ce) || ce.Code() != CloseNormal || ce.Reason() != "bye" {
		t.Error(test.DiffMessage(end, "closed by peer: 1000 bye", "C5: the server reads the client's close"))
	}
}

func TestDial_SendsHandshakeRequest(t *testing.T) {
	dial, s := scriptedDial(validReply)
	header := http.Header{"Origin": {"https://app.example"}, "Authorization": {"Bearer t"}}
	c, err := Dial(context.Background(), "ws://example.test:8080/chat?room=1", &DialOptions{
		NetDial: dial, Header: header, Subprotocols: []string{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Abort()
	r := <-s.requests
	got := []string{r.Method, r.RequestURI, r.Proto, r.Host, r.Header.Get("Upgrade"), r.Header.Get("Connection"),
		r.Header.Get("Sec-WebSocket-Version"), r.Header.Get("Sec-WebSocket-Protocol"), r.Header.Get("Origin"),
		r.Header.Get("Authorization")}
	want := []string{"GET", "/chat?room=1", "HTTP/1.1", "example.test:8080", "websocket", "Upgrade", "13", "a, b",
		"https://app.example", "Bearer t"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Error(test.DiffMessage(got, want, "D1: the request line and headers"))
	}
	if r.ContentLength != 0 || len(r.Header["Sec-Websocket-Extensions"]) != 0 {
		t.Error(test.DiffMessage(r.Header, "no body and no extension offered", "D1, T10: the request"))
	}
	key, err := base64.StdEncoding.Strict().DecodeString(r.Header.Get("Sec-WebSocket-Key"))
	if err != nil || len(key) != 16 {
		t.Error(test.DiffMessage(len(key), 16, "D1: the key is 16 bytes in base64"))
	}
}

func TestDial_KeyIsFreshPerDial(t *testing.T) {
	dial, s := scriptedDial(validReply)
	keys := map[string]bool{}
	for range 16 {
		c, err := Dial(context.Background(), "ws://example.test/", &DialOptions{NetDial: dial})
		if err != nil {
			t.Fatal(err)
		}
		c.Abort()
		keys[(<-s.requests).Header.Get("Sec-WebSocket-Key")] = true
	}
	if len(keys) != 16 {
		t.Error(test.DiffMessage(len(keys), 16, "D1: a random key per handshake"))
	}
}

func TestDial_RefusesWrongAccept(t *testing.T) {
	c, s, err := dialScripted(t, nil, func(*http.Request) []byte {
		return []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n")
	})
	expectRefusedDial(t, "D2: an accept value of another key", c, err, s, ErrBadHandshake, badHandshakeDetail("invalid Sec-WebSocket-Accept"))
}

func TestDial_RefusesTwoAcceptValues(t *testing.T) {
	c, s, err := dialScripted(t, nil, func(r *http.Request) []byte {
		return response101(r, "Sec-WebSocket-Accept: "+acceptFor(r)+"\r\n")
	})
	expectRefusedDial(t, "D2: two accept lines", c, err, s, ErrBadHandshake, badHandshakeDetail("invalid Sec-WebSocket-Accept"))
}

func TestDial_RefusesExtensions(t *testing.T) {
	for _, ext := range []string{"permessage-deflate", "x-custom; a=1", ""} {
		c, s, err := dialScripted(t, nil, func(r *http.Request) []byte {
			return response101(r, "Sec-WebSocket-Extensions: "+ext+"\r\n")
		})
		expectRefusedDial(t, "D2, T10: an extension named in the response: "+ext, c, err, s, ErrBadHandshake, badHandshakeDetail("response names an extension"))
	}
}

func TestDial_RefusesUnofferedSubprotocol(t *testing.T) {
	c, s, err := dialScripted(t, &DialOptions{Subprotocols: []string{"chat"}}, func(r *http.Request) []byte {
		return response101(r, "Sec-WebSocket-Protocol: other\r\n")
	})
	expectRefusedDial(t, "D2: a subprotocol that was not offered", c, err, s, ErrBadHandshake, badHandshakeDetail("Sec-WebSocket-Protocol was not offered"))
	c, s, err = dialScripted(t, nil, func(r *http.Request) []byte {
		return response101(r, "Sec-WebSocket-Protocol: chat\r\n")
	})
	expectRefusedDial(t, "D2: a subprotocol when none was offered", c, err, s, ErrBadHandshake, badHandshakeDetail("Sec-WebSocket-Protocol was not offered"))
	c, s, err = dialScripted(t, &DialOptions{Subprotocols: []string{"a", "b"}}, func(r *http.Request) []byte {
		return response101(r, "Sec-WebSocket-Protocol: a, b\r\n")
	})
	expectRefusedDial(t, "D2: a list of subprotocols in the response", c, err, s, ErrBadHandshake, badHandshakeDetail("Sec-WebSocket-Protocol was not offered"))
}

func TestDial_PicksOfferedSubprotocol(t *testing.T) {
	c, _, err := dialScripted(t, &DialOptions{Subprotocols: []string{"a", "b"}}, func(r *http.Request) []byte {
		return response101(r, "Sec-WebSocket-Protocol: b\r\n")
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Abort()
	if got := c.Subprotocol(); got != "b" {
		t.Error(test.DiffMessage(got, "b", "D2: the subprotocol the server picked"))
	}
}

func TestDial_NoSubprotocolWhenServerPicksNone(t *testing.T) {
	c, _, err := dialScripted(t, &DialOptions{Subprotocols: []string{"a"}}, validReply)
	if err != nil {
		t.Fatal(err)
	}
	c.Abort()
	if got := c.Subprotocol(); got != "" {
		t.Error(test.DiffMessage(got, "", "D2: no subprotocol in the response"))
	}
}

func TestDial_RefusesNon101Response(t *testing.T) {
	cases := map[string]func(r *http.Request) string{
		"200":          func(*http.Request) string { return "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n" },
		"HTTP/1.0 101": func(r *http.Request) string { return strings.Replace(string(validReply(r)), "HTTP/1.1", "HTTP/1.0", 1) },
		"no Upgrade": func(r *http.Request) string {
			return strings.Replace(string(validReply(r)), "Upgrade: websocket\r\n", "", 1)
		},
		"Upgrade h2c": func(r *http.Request) string {
			return strings.Replace(string(validReply(r)), "Upgrade: websocket", "Upgrade: h2c", 1)
		},
		"no Connection": func(r *http.Request) string {
			return strings.Replace(string(validReply(r)), "Connection: Upgrade\r\n", "", 1)
		},
		"no accept": func(r *http.Request) string {
			return strings.Replace(string(validReply(r)), "Sec-WebSocket-Accept", "X-Accept", 1)
		},
		"not HTTP":       func(*http.Request) string { return "hello\r\n\r\n" },
		"closed in head": func(*http.Request) string { return "HTTP/1.1 101 Switching Protocols\r\n" },
	}
	details := map[string]string{
		"200": "response status is not 101", "HTTP/1.0 101": "response status is not 101",
		"no Upgrade": "response Upgrade is not websocket", "Upgrade h2c": "response Upgrade is not websocket",
		"no Connection": "response Connection is not Upgrade", "no accept": "invalid Sec-WebSocket-Accept",
		"not HTTP": "malformed response", "closed in head": "malformed response",
	}
	for name, reply := range cases {
		dial, s := scriptedDial(func(r *http.Request) []byte { return []byte(reply(r)) })
		if name == "closed in head" {
			dial = closingAfterReply(dial)
		}
		c, err := Dial(context.Background(), "ws://example.test/", &DialOptions{NetDial: dial})
		expectRefusedDial(t, "D2: "+name, c, err, s, ErrBadHandshake, badHandshakeDetail(details[name]))
	}
}

type halfClosingConn struct {
	net.Conn
	reads int
}

func (h *halfClosingConn) Read(p []byte) (int, error) {
	if h.reads > 0 {
		return 0, io.EOF
	}
	h.reads++
	return h.Conn.Read(p)
}

func closingAfterReply(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := dial(ctx, network, address)
		return &halfClosingConn{Conn: c}, err
	}
}

func headOfLength(r *http.Request, total int) []byte {
	base := response101(r, "")
	pad := total - len(base) - len("X-Pad: \r\n")
	return response101(r, "X-Pad: "+strings.Repeat("p", pad)+"\r\n")
}

func TestDial_RefusesLongResponseHead(t *testing.T) {
	c, s, err := dialScripted(t, nil, func(r *http.Request) []byte { return headOfLength(r, maxResponseHead+1) })
	expectRefusedDial(t, "D2: a response head of 16 KiB + 1", c, err, s, ErrBadHandshake, badHandshakeDetail("response head over 16 KiB"))
}

func TestDial_AcceptsResponseHeadAtLimit(t *testing.T) {
	var head []byte
	c, _, err := dialScripted(t, nil, func(r *http.Request) []byte {
		head = headOfLength(r, maxResponseHead)
		return head
	})
	if err != nil {
		t.Fatal(test.DiffMessage(err, nil, "D2: a response head of exactly 16 KiB"))
	}
	c.Abort()
	if len(head) != maxResponseHead {
		t.Error(test.DiffMessage(len(head), maxResponseHead, "the test's head length"))
	}
}

func TestDial_TimeoutOnSilentServer(t *testing.T) {
	var took time.Duration
	var c *Conn
	var err error
	var s *scriptedServer
	took = elapsed(func() {
		c, s, err = dialScripted(t, &DialOptions{HandshakeTimeout: 60 * time.Millisecond}, func(*http.Request) []byte { return nil })
	})
	expectRefusedDial(t, "D3: a server that never answers", c, err, s, ErrTimeout, "websocket: timeout")
	if took < 60*time.Millisecond || took > 3*time.Second {
		t.Error(test.DiffMessage(took, "60ms to 3s", "D3: cut at HandshakeTimeout"))
	}
}

func TestDial_ContextCancelEndsHandshake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dial, s := scriptedDial(func(*http.Request) []byte {
		cancel()
		return nil
	})
	c, err := Dial(ctx, "ws://example.test/", &DialOptions{NetDial: dial, HandshakeTimeout: time.Minute})
	expectRefusedDial(t, "D3: ctx canceled during the handshake", c, err, s, ErrTimeout, "websocket: timeout")
}

func TestDial_ContextCanceledBeforeDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	c, err := Dial(ctx, "ws://example.test/", &DialOptions{NetDial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		called = true
		return nil, ctx.Err()
	}})
	expectRefusedDial(t, "D3: a done ctx", c, err, nil, ErrTimeout, "websocket: timeout")
	if !called {
		t.Error(test.DiffMessage(called, true, "the test's NetDial ran"))
	}
}

func TestDial_DialFailure(t *testing.T) {
	c, err := Dial(context.Background(), "ws://example.test/", &DialOptions{NetDial: func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("connection refused")
	}})
	expectRefusedDial(t, "D3: a failed connect", c, err, nil, ErrDial, "websocket: dial failed")
	ln, lerr := net.Listen("tcp", "127.0.0.1:0")
	if lerr != nil {
		t.Fatal(lerr)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	c, err = Dial(context.Background(), "ws://"+addr+"/", nil)
	expectRefusedDial(t, "D3: a closed port", c, err, nil, ErrDial, "websocket: dial failed")
}

func TestDial_ReadsFrameSentWithResponse(t *testing.T) {
	c, _, err := dialScripted(t, nil, func(r *http.Request) []byte {
		return append(validReply(r), serverFrame(testOpText, true, []byte("first"))...)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Abort()
	if _, got, err := c.ReadMessage(nil); err != nil || string(got) != "first" {
		t.Error(test.DiffMessage(string(got), "first", "D4: a frame in the same write as the 101"))
	}
}

func TestDial_RefusesMaskedServerFrame(t *testing.T) {
	c, s, err := dialScripted(t, nil, func(r *http.Request) []byte {
		return append(validReply(r), clientFrame(testOpText, true, []byte("x"))...)
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = c.ReadMessage(nil)
	checkErr(t, "D6, F2: a masked frame from the server", err, ErrProtocol, "websocket: protocol error: masked server frame")
	rest := <-s.received
	frames, perr := parseTestFrames(rest, true)
	if perr != nil || len(frames) != 1 || frames[0].Opcode != testOpClose || !bytes.Equal(frames[0].Payload, []byte{0x03, 0xea}) {
		t.Error(test.DiffMessage(frames, "one masked close frame 1002", "D6: the client fails with 1002"))
	}
}

type maskedFrame struct {
	key     [4]byte
	payload []byte
}

func readMaskedFrames(t *testing.T, b []byte) []maskedFrame {
	t.Helper()
	var out []maskedFrame
	for len(b) > 0 {
		if len(b) < 2 || b[1]&0x80 == 0 {
			t.Fatal(test.DiffMessage(b[:min(len(b), 2)], "a masked header", "D5: every client frame is masked"))
		}
		n, at := uint64(b[1]&0x7f), 2
		switch n {
		case 126:
			n, at = uint64(b[2])<<8|uint64(b[3]), 4
		case 127:
			n, at = 0, 10
			for _, c := range b[2:10] {
				n = n<<8 | uint64(c)
			}
		}
		var f maskedFrame
		copy(f.key[:], b[at:at+4])
		at += 4
		f.payload = append([]byte(nil), b[at:at+int(n)]...)
		for i := range f.payload {
			f.payload[i] ^= f.key[i&3]
		}
		out = append(out, f)
		b = b[at+int(n):]
	}
	return out
}

func TestDial_FramesUseFreshMasks(t *testing.T) {
	c, s, err := dialScripted(t, nil, validReply)
	if err != nil {
		t.Fatal(err)
	}
	sizes := []int{0, 4, 125, 126, 4000, 5000}
	var want [][]byte
	for i := range 96 {
		data := bytes.Repeat([]byte{byte(i)}, sizes[i%len(sizes)])
		want = append(want, data)
		if i%3 == 0 {
			err = c.WritePrepared(mustPrepared(t, KindBinary, data))
		} else {
			err = c.WriteMessage(KindBinary, data)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Ping([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	c.Abort()
	frames := readMaskedFrames(t, <-s.received)
	keys := map[[4]byte]bool{}
	for i, f := range frames {
		keys[f.key] = true
		if i < len(want) && !bytes.Equal(f.payload, want[i]) {
			t.Error(test.DiffMessage(len(f.payload), len(want[i]), "D5: frame "+itoa(i)+" unmasks to the data"))
		}
	}
	if len(frames) != len(want)+1 || len(keys) != len(frames) {
		t.Error(test.DiffMessage(len(keys), len(want)+1, "D5, T11: a fresh mask key per frame"))
	}
}

func mustPrepared(t *testing.T, kind Kind, data []byte) *PreparedMessage {
	t.Helper()
	m, err := NewPreparedMessage(kind, data)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestDial_DoesNotModifyData(t *testing.T) {
	c, s, err := dialScripted(t, nil, validReply)
	if err != nil {
		t.Fatal(err)
	}
	var sent [][]byte
	for _, n := range []int{3, 125, 126, 4096, 9000} {
		data := asciiPayload(newTestRand(), n)
		keep := append([]byte(nil), data...)
		if err := c.WriteMessage(KindText, data); err != nil {
			t.Fatal(err)
		}
		w, _ := c.NextWriter(KindBinary)
		_, _ = w.Write(data)
		_ = w.Close()
		if !bytes.Equal(data, keep) {
			t.Error(test.DiffMessage(len(data), len(keep), "D5: the caller's data is unchanged after a masked write of "+itoa(n)))
		}
		sent = append(sent, keep)
	}
	c.Abort()
	frames := readMaskedFrames(t, <-s.received)
	for i, want := range sent {
		if 3*i >= len(frames) || !bytes.Equal(frames[3*i].payload, want) {
			t.Error(test.DiffMessage(i, "the payload sent", "D5: masked frame payload"))
		}
	}
}

func TestDial_NilContext(t *testing.T) {
	var ctx context.Context
	called := false
	c, err := Dial(ctx, "ws://example.test/", &DialOptions{NetDial: func(context.Context, string, string) (net.Conn, error) {
		called = true
		return nil, errors.New("no")
	}})
	expectRefusedDial(t, "D0, T26: a nil ctx", c, err, nil, ErrInvalidArgument, "websocket: invalid argument: nil context")
	if called {
		t.Error(test.DiffMessage(called, false, "D0: no connection before the checks"))
	}
}

func refusedBeforeDial(t *testing.T, rule, rawURL, text string) {
	t.Helper()
	called := false
	c, err := Dial(context.Background(), rawURL, &DialOptions{NetDial: func(context.Context, string, string) (net.Conn, error) {
		called = true
		return nil, errors.New("no")
	}})
	expectRefusedDial(t, rule, c, err, nil, ErrInvalidArgument, text)
	if called {
		t.Error(test.DiffMessage(called, false, rule+": no connection"))
	}
}

func TestDial_RefusesUserinfo(t *testing.T) {
	refusedBeforeDial(t, "D0: a user and a password", "ws://user:zz-LEAK@example.test/", "websocket: invalid argument: url holds userinfo")
	refusedBeforeDial(t, "D0: a user", "wss://user@example.test/", "websocket: invalid argument: url holds userinfo")
}

func TestDial_RefusesOtherScheme(t *testing.T) {
	for _, u := range []string{"http://example.test/", "https://example.test/", "example.test/", "WSS2://example.test/", "ftp://example.test/"} {
		refusedBeforeDial(t, "D0: the scheme of "+u, u, "websocket: invalid argument: url scheme is not ws or wss")
	}
	for _, u := range []string{"ws:opaque", "ws:///path", "ws://:80/", "ws://exa mple.test/", "ws://example.test/\x7f"} {
		refusedBeforeDial(t, "D0: an unusable URL "+u, u, "")
	}
}

func TestDial_RefusesFragment(t *testing.T) {
	refusedBeforeDial(t, "D0: a fragment", "ws://example.test/chat#zz-LEAK", "websocket: invalid argument: url holds a fragment")
	refusedBeforeDial(t, "D0: an empty fragment", "ws://example.test/chat#", "websocket: invalid argument: url holds a fragment")
}

func TestDial_DoesNotFollowRedirect(t *testing.T) {
	var hits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	t.Cleanup(target.Close)
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/ws", http.StatusFound)
	}))
	t.Cleanup(redirect.Close)
	c, err := Dial(context.Background(), "ws"+strings.TrimPrefix(redirect.URL, "http")+"/ws", nil)
	expectRefusedDial(t, "D0, D2: a 302", c, err, nil, ErrBadHandshake, badHandshakeDetail("response status is not 101"))
	if got := hits.Load(); got != 0 {
		t.Error(test.DiffMessage(got, 0, "D0, T12: the redirect target is never contacted"))
	}
}

func TestDial_IgnoresProxyEnvironment(t *testing.T) {
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		t.Setenv(name, "http://127.0.0.1:1")
	}
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	dial, s := scriptedDial(validReply)
	var address string
	c, err := Dial(context.Background(), "ws://target.example:8080/x", &DialOptions{NetDial: func(ctx context.Context, network, addr string) (net.Conn, error) {
		address = addr
		return dial(ctx, network, addr)
	}})
	if err != nil {
		t.Fatal(err)
	}
	c.Abort()
	if r := <-s.requests; address != "target.example:8080" || r.RequestURI != "/x" {
		t.Error(test.DiffMessage(address+" "+r.RequestURI, "target.example:8080 /x", "D0, T12: the URL's host is dialed directly, never a proxy"))
	}
}

func TestDial_DefaultPorts(t *testing.T) {
	for scheme, want := range map[string]string{"ws": "example.test:80", "wss": "example.test:443"} {
		var address string
		_, _ = Dial(context.Background(), scheme+"://example.test/", &DialOptions{NetDial: func(_ context.Context, _, addr string) (net.Conn, error) {
			address = addr
			return nil, errors.New("no")
		}})
		if address != want {
			t.Error(test.DiffMessage(address, want, "D1: the default port of "+scheme))
		}
	}
}

func TestDial_VerifiesCertificate(t *testing.T) {
	names := make(chan string, 4)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS12, GetConfigForClient: func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
		names <- hi.ServerName
		return nil, nil
	}}
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
	c, err := Dial(context.Background(), "wss://localhost:"+port+"/ws", nil)
	expectRefusedDial(t, "D0: a certificate no root trusts", c, err, nil, ErrDial, "websocket: dial failed")
	select {
	case name := <-names:
		if name != "localhost" {
			t.Error(test.DiffMessage(name, "localhost", "D0: ServerName from the URL's host"))
		}
	default:
		t.Error(test.DiffMessage("no ClientHello", "a ClientHello", "D0: the TLS handshake ran"))
	}
}

func TestDial_TalksOverTLS(t *testing.T) {
	srv, _ := startEchoUpgrader(t, &UpgradeOptions{CheckOrigin: allowAll}, func(s *httptest.Server) { s.StartTLS() })
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "127.0.0.1"}
	c, err := Dial(context.Background(), "wss://"+addrOf(srv)+"/", &DialOptions{NetDial: func(ctx context.Context, network, address string) (net.Conn, error) {
		d := tls.Dialer{Config: cfg}
		return d.DialContext(ctx, network, address)
	}})
	if err != nil {
		t.Fatal(err)
	}
	expectEcho(t, "D1: a round trip over TLS", c, KindText, bytes.Repeat([]byte("t"), 9000))
	if err := c.Close(CloseNormal, ""); err != nil {
		t.Error(test.DiffMessage(err, nil, "C6: close over TLS"))
	}
}

func TestDial_RefusesHandshakeHeaderOption(t *testing.T) {
	for _, name := range []string{"Sec-WebSocket-Key", "sec-websocket-protocol", "Host", "Upgrade", "Connection", "Sec-WebSocket-Extensions", "Content-Length"} {
		_, err := Dial(context.Background(), "ws://example.test/", &DialOptions{Header: http.Header{name: {"zz-LEAK"}}})
		checkErr(t, "D1: "+name+" in DialOptions.Header", err, ErrInvalidOptions, "websocket: invalid options: DialOptions.Header holds a header of the handshake")
	}
	_, err := Dial(context.Background(), "ws://example.test/", &DialOptions{Header: http.Header{"Bad Name": {"x"}}})
	checkErr(t, "D1: an invalid header name", err, ErrInvalidOptions, "websocket: invalid options: DialOptions.Header holds an invalid header name")
}

func TestDial_RefusesInvalidOptions(t *testing.T) {
	cases := map[string]*DialOptions{
		"websocket: invalid options: DialOptions.HandshakeTimeout is negative":   {HandshakeTimeout: -1},
		"websocket: invalid options: DialOptions.Subprotocols[1] is not a token": {Subprotocols: []string{"a", "zz LEAK"}},
		"websocket: invalid options: DialOptions.Subprotocols[1] is a duplicate": {Subprotocols: []string{"a", "a"}},
		"websocket: invalid options: DialOptions.Conn.ReadTimeout is negative":   {Conn: &ConnOptions{ReadTimeout: -1}},
	}
	for text, opts := range cases {
		opts.NetDial = func(context.Context, string, string) (net.Conn, error) {
			t.Error(test.DiffMessage("dialed", "no dial", "D0: options are checked before the dial"))
			return nil, errors.New("no")
		}
		_, err := Dial(context.Background(), "ws://example.test/", opts)
		checkErr(t, "D1: "+text, err, ErrInvalidOptions, text)
	}
}

func TestDial_HeaderValueCannotSplitRequest(t *testing.T) {
	dial, s := scriptedDial(validReply)
	c, err := Dial(context.Background(), "ws://example.test/", &DialOptions{NetDial: dial, Header: http.Header{"X-Note": {"a\r\nX-Injected: 1"}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Abort()
	if r := <-s.requests; r.Header.Get("X-Injected") != "" {
		t.Error(test.DiffMessage(r.Header, "no injected header", "D1: a CR or LF in a value cannot start a new line"))
	}
}

func TestDial_ErrorHoldsNoURL(t *testing.T) {
	m := leakMarker
	leakyDial := func(_ context.Context, _, addr string) (net.Conn, error) {
		return nil, errors.New("dial " + addr + " failed")
	}
	silent, _ := scriptedDial(func(*http.Request) []byte { return nil })
	bad, _ := scriptedDial(func(*http.Request) []byte { return []byte("HTTP/1.1 302 Found\r\nLocation: /" + m + "\r\n\r\n") })
	cases := []struct {
		url  string
		opts *DialOptions
	}{
		{"ws://" + m + ".example/" + m + "?q=" + m, &DialOptions{NetDial: leakyDial}},
		{"ws://" + m + ".example/" + m + "?q=" + m, &DialOptions{NetDial: silent, HandshakeTimeout: 20 * time.Millisecond}},
		{"ws://" + m + ".example/" + m + "?q=" + m, &DialOptions{NetDial: bad}},
		{"http://" + m + ".example/", nil},
		{"ws://" + m + ":" + m + "@x.example/", nil},
		{"ws://x.example/#" + m, nil},
		{"ws://x.example/%zz" + m, nil},
	}
	for _, tc := range cases {
		_, err := Dial(context.Background(), tc.url, tc.opts)
		if err == nil {
			t.Fatal(test.DiffMessage(err, "an error", "D0: the test's dial fails"))
		}
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			if got := fmt.Sprintf(verb, err); strings.Contains(got, m) {
				t.Error(test.DiffMessage(got, "no marker", "D0, E1: a dial error printed with "+verb))
			}
		}
	}
}

func TestDial_CloseEndsConnection(t *testing.T) {
	srv, ends := startEchoUpgrader(t, &UpgradeOptions{CheckOrigin: allowAll}, nil)
	c, err := Dial(context.Background(), "ws://"+addrOf(srv)+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	expectEcho(t, "D1: a round trip", c, KindText, []byte("x"))
	if err := c.Close(CloseNormal, ""); err != nil {
		t.Fatal(err)
	}
	var ce *CloseError
	if end := <-ends; !errors.As(end, &ce) || ce.Code() != CloseNormal {
		t.Error(test.DiffMessage(end, "closed by peer: 1000", "C5: the server reads the client's close"))
	}
	if err := c.WriteMessage(KindText, []byte("late")); !errors.Is(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "X1: a write after the client's close"))
	}
}

const fuzzAcceptMarker = "@ACCEPT@"

var fuzzAcceptStandIn = strings.Repeat("A", 28)

type fuzzVerdict struct {
	decided bool
	ok      bool
}

func refDialVerdict(raw []byte) fuzzVerdict {
	end := bytes.Index(raw, []byte("\r\n\r\n"))
	if end < 0 {
		return fuzzVerdict{}
	}
	head := raw[:end+4]
	if bytes.Contains(bytes.ReplaceAll(head, []byte("\r\n"), nil), []byte("\n")) {
		return fuzzVerdict{}
	}
	if len(head) > maxResponseHead {
		return fuzzVerdict{decided: true}
	}
	lines := strings.Split(string(head[:end]), "\r\n")
	status := lines[0]
	if len(status) < 12 || !strings.HasPrefix(status, "HTTP/1.1 ") || !refDigits(status[9:12]) ||
		len(status) > 12 && (status[12] != ' ' || !refVisible(status[13:])) {
		return fuzzVerdict{}
	}
	values := map[string][]string{}
	for _, line := range lines[1:] {
		name, value, found := strings.Cut(line, ":")
		if !found || !refIsToken(name) || !refVisible(value) {
			return fuzzVerdict{}
		}
		key := strings.ToLower(name)
		if key == "content-length" || key == "transfer-encoding" || key == "trailer" {
			return fuzzVerdict{}
		}
		values[key] = append(values[key], strings.Trim(value, " \t"))
	}
	if status[9:12] != "101" {
		return fuzzVerdict{decided: true}
	}
	if up := values["upgrade"]; len(up) != 1 || !strings.EqualFold(up[0], "websocket") {
		return fuzzVerdict{decided: true}
	}
	hasUpgrade := false
	for _, v := range values["connection"] {
		if strings.Contains(v, `"`) {
			return fuzzVerdict{decided: true}
		}
		for e := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.Trim(e, " \t"), "upgrade") {
				hasUpgrade = true
			}
		}
	}
	if !hasUpgrade || values["sec-websocket-extensions"] != nil {
		return fuzzVerdict{decided: true}
	}
	if acc := values["sec-websocket-accept"]; len(acc) != 1 || acc[0] != fuzzAcceptStandIn {
		return fuzzVerdict{decided: true}
	}
	if p, ok := values["sec-websocket-protocol"]; ok && (len(p) != 1 || p[0] != "chat") {
		return fuzzVerdict{decided: true}
	}
	return fuzzVerdict{decided: true, ok: true}
}

func refDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s[0] != '0'
}

func refVisible(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c != '\t' && (c < 0x20 || c > 0x7e) {
			return false
		}
	}
	return true
}

func FuzzDialResponse(f *testing.F) {
	valid := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + fuzzAcceptMarker + "\r\n"
	seeds := []string{
		valid + "\r\n",
		valid + "Sec-WebSocket-Protocol: chat\r\n\r\n",
		valid + "Sec-WebSocket-Protocol: other\r\n\r\n",
		valid + "Sec-WebSocket-Extensions: permessage-deflate\r\n\r\n",
		valid + "\r\n\x81\x02hi",
		"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
		"HTTP/1.1 302 Found\r\nLocation: http://other.example/\r\n\r\n",
		"HTTP/1.0 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + fuzzAcceptMarker + "\r\n\r\n",
		strings.Replace(valid, "Upgrade: websocket", "Upgrade: h2c", 1) + "\r\n",
		strings.Replace(valid, "Connection: Upgrade", "Connection: keep-alive", 1) + "\r\n",
		strings.Replace(valid, fuzzAcceptMarker, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", 1) + "\r\n",
		valid + "Sec-WebSocket-Accept: " + fuzzAcceptMarker + "\r\n\r\n",
		valid + "X-Pad: " + strings.Repeat("p", maxResponseHead) + "\r\n\r\n",
		"HTTP/1.1 101\r\nupgrade: WebSocket\r\nconnection: keep-alive, upgrade\r\nsec-websocket-accept: " + fuzzAcceptMarker + "\r\n\r\n",
		"\r\n\r\n",
		"HTTP/1.1 101 Switching Protocols\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		verdict := refDialVerdict(bytes.ReplaceAll(raw, []byte(fuzzAcceptMarker), []byte(fuzzAcceptStandIn)))
		dial := func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				br := bufio.NewReader(server)
				r, err := http.ReadRequest(br)
				if err == nil {
					_, _ = server.Write(bytes.ReplaceAll(raw, []byte(fuzzAcceptMarker), []byte(acceptFor(r))))
				}
				_ = server.Close()
			}()
			return client, nil
		}
		c, err := Dial(context.Background(), "ws://example.test/", &DialOptions{
			NetDial: dial, Subprotocols: []string{"chat"}, HandshakeTimeout: 2 * time.Second,
		})
		if c != nil {
			c.Abort()
		}
		if (err == nil) != (c != nil) {
			t.Fatal(test.DiffMessage(err, "a connection or an error", "D2: Dial's result"))
		}
		if errors.Is(err, ErrTimeout) {
			t.Fatal(test.DiffMessage(err, "a decision before the timeout", "D2: a response the fake transport sent whole"))
		}
		if !verdict.decided {
			return
		}
		if verdict.ok != (err == nil) {
			t.Fatal(test.DiffMessage(err, verdict.ok, "D2: the reference's verdict on this response"))
		}
	})
}
