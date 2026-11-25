package websocket

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.owlkei.com/sdk/internal/test"
)

const (
	testOpCont   byte = 0
	testOpText   byte = 1
	testOpBinary byte = 2
	testOpClose  byte = 8
	testOpPing   byte = 9
	testOpPong   byte = 10
)

const leakMarker = "zz-LEAK"

var testMaskKey = [4]byte{0x37, 0xfa, 0x21, 0x3d}

type testFrame struct {
	Fin     bool
	Rsv     byte
	Opcode  byte
	Masked  bool
	Payload []byte
}

func appendTestLength(dst []byte, maskBit byte, n int) []byte {
	switch {
	case n <= 125:
		return append(dst, maskBit|byte(n))
	case n <= 65535:
		return append(dst, maskBit|126, byte(n>>8), byte(n))
	default:
		dst = append(dst, maskBit|127)
		return binary.BigEndian.AppendUint64(dst, uint64(n))
	}
}

func encodeTestFrame(f testFrame, key [4]byte) []byte {
	b0 := f.Opcode | f.Rsv
	if f.Fin {
		b0 |= 0x80
	}
	out := []byte{b0}
	var maskBit byte
	if f.Masked {
		maskBit = 0x80
	}
	out = appendTestLength(out, maskBit, len(f.Payload))
	if !f.Masked {
		return append(out, f.Payload...)
	}
	out = append(out, key[:]...)
	for i, c := range f.Payload {
		out = append(out, c^key[i&3])
	}
	return out
}

func clientFrame(opcode byte, fin bool, payload []byte) []byte {
	return encodeTestFrame(testFrame{Fin: fin, Opcode: opcode, Masked: true, Payload: payload}, testMaskKey)
}

func serverFrame(opcode byte, fin bool, payload []byte) []byte {
	return encodeTestFrame(testFrame{Fin: fin, Opcode: opcode, Payload: payload}, testMaskKey)
}

func rawHeader(b0 byte, masked bool, encoding int, n uint64) []byte {
	var maskBit byte
	if masked {
		maskBit = 0x80
	}
	var out []byte
	switch encoding {
	case 0:
		out = []byte{b0, maskBit | byte(n)}
	case 2:
		out = []byte{b0, maskBit | 126, byte(n >> 8), byte(n)}
	default:
		out = []byte{b0, maskBit | 127}
		out = binary.BigEndian.AppendUint64(out, n)
	}
	if masked {
		out = append(out, testMaskKey[:]...)
	}
	return out
}

func closePayload(code uint16, reason string) []byte {
	return append([]byte{byte(code >> 8), byte(code)}, reason...)
}

func failWire(code CloseCode) []byte {
	return []byte{0x88, 0x02, byte(code >> 8), byte(code)}
}

func parseTestFrames(b []byte, masked bool) ([]testFrame, error) {
	var out []testFrame
	for len(b) > 0 {
		if len(b) < 2 {
			return out, errors.New("truncated header")
		}
		f := testFrame{Fin: b[0]&0x80 != 0, Rsv: b[0] & 0x70, Opcode: b[0] & 0x0f, Masked: b[1]&0x80 != 0}
		if f.Masked != masked {
			return out, errors.New("unexpected mask bit")
		}
		n := uint64(b[1] & 0x7f)
		b = b[2:]
		switch n {
		case 126:
			if len(b) < 2 {
				return out, errors.New("truncated length")
			}
			n = uint64(binary.BigEndian.Uint16(b))
			b = b[2:]
		case 127:
			if len(b) < 8 {
				return out, errors.New("truncated length")
			}
			n = binary.BigEndian.Uint64(b)
			b = b[8:]
		}
		var key [4]byte
		if masked {
			if len(b) < 4 {
				return out, errors.New("truncated key")
			}
			copy(key[:], b)
			b = b[4:]
		}
		if uint64(len(b)) < n {
			return out, errors.New("truncated payload")
		}
		f.Payload = append([]byte(nil), b[:n]...)
		b = b[n:]
		if masked {
			for i := range f.Payload {
				f.Payload[i] ^= key[i&3]
			}
		}
		out = append(out, f)
	}
	return out, nil
}

type testAddr struct{}

func (testAddr) Network() string { return "test" }
func (testAddr) String() string  { return "test" }

type recConn struct {
	mu         sync.Mutex
	chunks     [][]byte
	loop       []byte
	loopPos    int
	block      bool
	discard    bool
	out        bytes.Buffer
	writes     int
	writeSizes []int
	closeCalls int
	rdl, wdl   time.Time
	writeErr   error
	blockWrite chan struct{}
	entered    chan struct{}
	hook       func(p []byte)
	failOnRead func()
	onReadDL   func()
	closed     chan struct{}
	closeOnce  sync.Once
}

func newRecConn(chunks ...[]byte) *recConn {
	return &recConn{chunks: chunks, closed: make(chan struct{})}
}

func newLoopConn(loop []byte) *recConn {
	r := newRecConn()
	r.loop = loop
	r.discard = true
	return r
}

func (r *recConn) Read(p []byte) (int, error) {
	r.mu.Lock()
	select {
	case <-r.closed:
		r.mu.Unlock()
		return 0, net.ErrClosed
	default:
	}
	if r.failOnRead != nil {
		fail := r.failOnRead
		r.mu.Unlock()
		fail()
		return 0, io.EOF
	}
	for len(r.chunks) > 0 && len(r.chunks[0]) == 0 {
		r.chunks = r.chunks[1:]
	}
	if len(r.chunks) > 0 {
		n := copy(p, r.chunks[0])
		r.chunks[0] = r.chunks[0][n:]
		r.mu.Unlock()
		return n, nil
	}
	if r.loop != nil {
		n := 0
		for n < len(p) {
			c := copy(p[n:], r.loop[r.loopPos:])
			r.loopPos = (r.loopPos + c) % len(r.loop)
			n += c
		}
		r.mu.Unlock()
		return n, nil
	}
	block := r.block
	r.mu.Unlock()
	if block {
		<-r.closed
		return 0, net.ErrClosed
	}
	return 0, io.EOF
}

func (r *recConn) Write(p []byte) (int, error) {
	r.mu.Lock()
	if r.blockWrite != nil {
		gate := r.blockWrite
		entered := r.entered
		r.mu.Unlock()
		if entered != nil {
			select {
			case entered <- struct{}{}:
			default:
			}
		}
		select {
		case <-gate:
		case <-r.closed:
			return 0, net.ErrClosed
		}
		r.mu.Lock()
	}
	select {
	case <-r.closed:
		r.mu.Unlock()
		return 0, net.ErrClosed
	default:
	}
	if r.writeErr != nil {
		err := r.writeErr
		r.mu.Unlock()
		return 0, err
	}
	r.writes++
	if !r.discard {
		r.writeSizes = append(r.writeSizes, len(p))
		r.out.Write(p)
	}
	hook := r.hook
	r.mu.Unlock()
	if hook != nil {
		hook(p)
	}
	return len(p), nil
}

func (r *recConn) Close() error {
	r.mu.Lock()
	r.closeCalls++
	r.mu.Unlock()
	r.closeOnce.Do(func() { close(r.closed) })
	return nil
}

func (r *recConn) LocalAddr() net.Addr  { return testAddr{} }
func (r *recConn) RemoteAddr() net.Addr { return testAddr{} }

func (r *recConn) SetDeadline(t time.Time) error {
	r.mu.Lock()
	r.rdl, r.wdl = t, t
	r.mu.Unlock()
	return nil
}

func (r *recConn) SetReadDeadline(t time.Time) error {
	r.mu.Lock()
	r.rdl = t
	hook := r.onReadDL
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (r *recConn) SetWriteDeadline(t time.Time) error {
	r.mu.Lock()
	r.wdl = t
	r.mu.Unlock()
	return nil
}

func (r *recConn) written() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]byte(nil), r.out.Bytes()...)
}

func (r *recConn) closes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeCalls
}

func (r *recConn) writeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writes
}

func (r *recConn) deadlines() (time.Time, time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rdl, r.wdl
}

type hijackWriter struct {
	header   http.Header
	conn     net.Conn
	br       *bufio.Reader
	wrote    bool
	hijacked bool
	status   int
	body     bytes.Buffer
}

func newHijackWriter(conn net.Conn) *hijackWriter {
	return &hijackWriter{header: http.Header{}, conn: conn, br: bufio.NewReaderSize(conn, 4096)}
}

func (w *hijackWriter) Header() http.Header { return w.header }

func (w *hijackWriter) Write(b []byte) (int, error) {
	w.wrote = true
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(b)
}

func (w *hijackWriter) WriteHeader(code int) {
	w.wrote = true
	w.status = code
}

func (w *hijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacked = true
	return w.conn, bufio.NewReadWriter(w.br, bufio.NewWriterSize(w.conn, 4096)), nil
}

func allowAll(*http.Request) bool { return true }

func mustUpgrader(t testing.TB, opts *UpgradeOptions) *Upgrader {
	t.Helper()
	u, err := NewUpgrader(opts)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func openUpgrader(t testing.TB) *Upgrader {
	t.Helper()
	return mustUpgrader(t, &UpgradeOptions{CheckOrigin: allowAll})
}

func validRequest() *http.Request {
	return validRequestAt("/chat?q=1")
}

func validRequestAt(target string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	return r
}

func upgradeOverRec(t testing.TB, u *Upgrader, r *http.Request, chunks ...[]byte) (*Conn, *hijackWriter, *recConn, error) {
	t.Helper()
	rc := newRecConn(chunks...)
	w := newHijackWriter(rc)
	c, err := u.Upgrade(w, r)
	return c, w, rc, err
}

func newRecServerConn(opts *ConnOptions, chunks ...[]byte) (*Conn, *recConn) {
	rc := newRecConn(chunks...)
	return newConn(rc, bufio.NewReaderSize(rc, 4096), false, opts), rc
}

func newRecClientConn(opts *ConnOptions, chunks ...[]byte) (*Conn, *recConn) {
	rc := newRecConn(chunks...)
	return newConn(rc, bufio.NewReaderSize(rc, 4096), true, opts), rc
}

func newPipeConn(t testing.TB, isClient bool, opts *ConnOptions) (*Conn, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	return newConn(a, bufio.NewReaderSize(a, 4096), isClient, opts), b
}

func connOpts(mutate func(*ConnOptions)) *ConnOptions {
	o := &ConnOptions{}
	mutate(o)
	return o
}

func collectAll(r io.Reader) <-chan []byte {
	ch := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(r)
		ch <- b
	}()
	return ch
}

func feedSlowly(peer net.Conn, data []byte, gap time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range data {
			if _, err := peer.Write(data[i : i+1]); err != nil {
				return
			}
			time.Sleep(gap)
		}
	}()
	return done
}

func feedGenerated(peer net.Conn, gen func(i int) []byte, gap time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			if _, err := peer.Write(gen(i)); err != nil {
				return
			}
			time.Sleep(gap)
		}
	}()
	return done
}

func elapsed(f func()) time.Duration {
	start := time.Now()
	f()
	return time.Since(start)
}

func newTestRand() *rand.Rand {
	return rand.New(rand.NewPCG(1, 2))
}

func asciiPayload(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + r.IntN(26))
	}
	return b
}

func randomBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.IntN(256))
	}
	return b
}

func repeatByte(c byte, n int) []byte {
	return bytes.Repeat([]byte{c}, n)
}

func totalAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc
}

func checkErr(t *testing.T, rule string, err error, class error, text string) {
	t.Helper()
	if err == nil {
		t.Error(test.DiffMessage(err, class, rule+": an error"))
		return
	}
	if class != nil && !errors.Is(err, class) {
		t.Error(test.DiffMessage(err, class, rule+": error class"))
	}
	if text != "" && err.Error() != text {
		t.Error(test.DiffMessage(err.Error(), text, rule+": error detail"))
	}
}

func expectFail(t *testing.T, rule string, rc *recConn, err error, class error, code CloseCode, text string) {
	t.Helper()
	checkErr(t, rule, err, class, text)
	if got := rc.written(); !bytes.Equal(got, failWire(code)) {
		t.Error(test.DiffMessage(got, failWire(code), rule+": close frame on the wire"))
	}
	if got := rc.closes(); got != 1 {
		t.Error(test.DiffMessage(got, 1, rule+": transport closed once"))
	}
}

func expectBytes(t *testing.T, rule string, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Error(test.DiffMessage(got, want, rule))
	}
}

func refusalBody(status int) string {
	return http.StatusText(status) + "\n"
}

func containsLeak(b []byte) bool {
	return strings.Contains(string(b), leakMarker)
}

func itoa(n int) string {
	return strconv.Itoa(n)
}

func isErr(err, target error) bool {
	return err != nil && target != nil && errors.Is(err, target)
}
