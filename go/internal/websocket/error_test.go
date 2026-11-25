package websocket

import (
	"bytes"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"go.owlkei.com/sdk/internal/test"
)

func TestConn_Errors_LeakNothing(t *testing.T) {
	m := leakMarker
	small := connOpts(func(o *ConnOptions) { o.MaxMessageBytes = 4 })
	scenarios := []struct {
		name   string
		opts   *ConnOptions
		frames [][]byte
		pongs  bool
	}{
		{"text", nil, [][]byte{clientFrame(testOpText, true, []byte(m))}, false},
		{"binary", nil, [][]byte{clientFrame(testOpBinary, true, []byte(m))}, false},
		{"ping", nil, [][]byte{clientFrame(testOpPing, true, []byte(m))}, true},
		{"pong", nil, [][]byte{clientFrame(testOpPong, true, []byte(m))}, false},
		{"close reason", nil, [][]byte{peerClose(1000, m)}, false},
		{"close invalid code", nil, [][]byte{peerClose(1005, m)}, false},
		{"close reason not UTF-8", nil, [][]byte{peerClose(1000, m+"\xff")}, false},
		{"text not UTF-8", nil, [][]byte{clientFrame(testOpText, true, []byte(m+"\xff"))}, false},
		{"unmasked frame", nil, [][]byte{serverFrame(testOpText, true, []byte(m))}, false},
		{"frame over limit", small, [][]byte{clientFrame(testOpBinary, true, []byte(m))}, false},
		{"reserved bit", nil, [][]byte{encodeTestFrame(testFrame{Fin: true, Rsv: 0x40, Opcode: testOpText, Masked: true, Payload: []byte(m)}, testMaskKey)}, false},
		{"reserved opcode", nil, [][]byte{clientFrame(3, true, []byte(m))}, false},
		{"fragmented ping", nil, [][]byte{clientFrame(testOpPing, false, []byte(m))}, false},
		{"continuation without message", nil, [][]byte{clientFrame(testOpCont, true, []byte(m))}, false},
	}
	for _, s := range scenarios {
		c, rc := newRecServerConn(s.opts, s.frames...)
		for range 4 {
			_, _, err := c.ReadMessage(nil)
			if err == nil {
				continue
			}
			for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
				if got := fmt.Sprintf(verb, err); strings.Contains(got, m) {
					t.Error(test.DiffMessage(got, "no marker", "E1, T15: "+s.name+" error printed with "+verb))
				}
			}
		}
		if !s.pongs && containsLeak(rc.written()) {
			t.Error(test.DiffMessage(rc.written(), "no marker", "E3, T15: "+s.name+": bytes the package wrote"))
		}
	}
}

func TestErrors_SentinelTexts(t *testing.T) {
	cases := map[error]string{
		ErrInvalidOptions:     "websocket: invalid options",
		ErrBadHandshake:       "websocket: bad handshake",
		ErrUnsupportedVersion: "websocket: unsupported version",
		ErrOriginNotAllowed:   "websocket: origin not allowed",
		ErrHijack:             "websocket: connection cannot be hijacked",
		ErrDial:               "websocket: dial failed",
		ErrProtocol:           "websocket: protocol error",
		ErrInvalidUTF8:        "websocket: invalid UTF-8",
		ErrMessageTooBig:      "websocket: message too big",
		ErrTimeout:            "websocket: timeout",
		ErrAbnormalClosure:    "websocket: connection lost",
		ErrClosed:             "websocket: connection closed",
		ErrWriteInProgress:    "websocket: a message is being written",
		ErrInvalidArgument:    "websocket: invalid argument",
	}
	for err, want := range cases {
		if err.Error() != want {
			t.Error(test.DiffMessage(err.Error(), want, "E1: sentinel text"))
		}
	}
}

func TestErrors_DetailMatchesClass(t *testing.T) {
	cases := []struct {
		name  string
		frame []byte
		class error
		text  string
	}{
		{"protocol", encodeTestFrame(testFrame{Fin: true, Rsv: 0x20, Opcode: testOpText, Masked: true}, testMaskKey), ErrProtocol, "websocket: protocol error: reserved bit set"},
		{"utf8", clientFrame(testOpText, true, []byte{0xff}), ErrInvalidUTF8, "websocket: invalid UTF-8"},
		{"too big", rawHeader(0x82, true, 2, 5000), ErrMessageTooBig, "websocket: message too big: frame over the limit"},
	}
	classes := []error{ErrProtocol, ErrInvalidUTF8, ErrMessageTooBig, ErrTimeout, ErrClosed, ErrAbnormalClosure}
	for _, cs := range cases {
		c, _ := newRecServerConn(connOpts(func(o *ConnOptions) { o.MaxMessageBytes = 1000 }), cs.frame)
		_, _, err := c.ReadMessage(nil)
		checkErr(t, "E1: "+cs.name, err, cs.class, cs.text)
		for _, other := range classes {
			if other != cs.class && isErr(err, other) {
				t.Error(test.DiffMessage(err, "not "+other.Error(), "E1: "+cs.name+" matches one class only"))
			}
		}
	}
	a, _ := newRecServerConn(nil, clientFrame(testOpText, true, []byte("a\xff"+"AAAA")))
	b, _ := newRecServerConn(nil, clientFrame(testOpText, true, []byte("zz\xc0\x80")))
	_, _, errA := a.ReadMessage(nil)
	_, _, errB := b.ReadMessage(nil)
	if errA == nil || errB == nil || errA.Error() != errB.Error() {
		t.Error(test.DiffMessage(errA, errB, "E1: the text of an error does not depend on the peer's bytes"))
	}
}

func TestConn_Errors_WrapNoTransportError(t *testing.T) {
	c, rc := newRecServerConn(nil)
	rc.writeErr = errBoom{}
	err := c.WriteMessage(KindText, []byte("x"))
	if err == nil || isErr(err, errBoom{}) || strings.Contains(fmt.Sprintf("%v %+v %#v", err, err, err), "boom") {
		t.Error(test.DiffMessage(err, "no transport error", "E1, Q10: a transport error is hidden"))
	}
	c, _ = newRecServerConn(nil)
	_, _, err = c.ReadMessage(nil)
	if err == nil || strings.Contains(fmt.Sprintf("%#v", err), "EOF") {
		t.Error(test.DiffMessage(err, "ErrAbnormalClosure without the cause", "E1, Q10"))
	}
}

func TestPackage_ImportsNoLoggerOrMathRand(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	banned := map[string]bool{
		"log":                                   true,
		"log/slog":                              true,
		"math/rand":                             true,
		"math/rand/v2":                          true,
		"go.owlkei.com/owlkei/log":              true,
		"go.owlkei.com/owlkei/internal/logpipe": true,
	}
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), f, bytes.NewReader(src), parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for _, imp := range parsed.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			if banned[path] {
				t.Error(test.DiffMessage(path, "no logger and no math/rand", "E4, T11: import in "+f))
			}
		}
	}
	if checked == 0 {
		t.Error(test.DiffMessage(checked, "at least one non-test file", "E4: the package has code to check"))
	}
}
