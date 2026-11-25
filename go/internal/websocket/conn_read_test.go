package websocket

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"runtime"
	"slices"
	"testing"
	"time"

	"go.owlkei.com/sdk/internal/test"
)

func TestConn_ReadMessage_RoundTrip(t *testing.T) {
	r := newTestRand()
	lengths := []int{0, 1, 125, 126, 127, 4095, 4096, 4097, 65535, 65536, 70000}
	for iter := range 300 {
		isClient := iter%2 == 1
		op := testOpBinary
		want := KindBinary
		n := lengths[r.IntN(len(lengths))]
		var payload []byte
		if r.IntN(2) == 0 {
			op, want = testOpText, KindText
			payload = asciiPayload(r, n)
		} else {
			payload = randomBytes(r, n)
		}
		parts := 1 + r.IntN(6)
		cuts := []int{0, n}
		for range parts - 1 {
			cuts = append(cuts, r.IntN(n+1))
		}
		slices.Sort(cuts)
		var key [4]byte
		copy(key[:], randomBytes(r, 4))
		var stream []byte
		for i := 0; i+1 < len(cuts); i++ {
			opcode := testOpCont
			if i == 0 {
				opcode = op
			}
			f := testFrame{Fin: i+2 == len(cuts), Opcode: opcode, Masked: !isClient, Payload: payload[cuts[i]:cuts[i+1]]}
			stream = append(stream, encodeTestFrame(f, key)...)
		}
		var chunks [][]byte
		for pos := 0; pos < len(stream); {
			size := 1 + r.IntN(1+r.IntN(8192))
			end := min(pos+size, len(stream))
			chunks = append(chunks, stream[pos:end])
			pos = end
		}
		opts := connOpts(func(o *ConnOptions) {
			o.MaxMessageBytes = 1 << 20
			o.MaxFragments = 64
		})
		var c *Conn
		if isClient {
			c, _ = newRecClientConn(opts, chunks...)
		} else {
			c, _ = newRecServerConn(opts, chunks...)
		}
		kind, data, err := c.ReadMessage(nil)
		if err != nil || kind != want || !bytes.Equal(data, payload) {
			t.Fatal(test.DiffMessage(err, nil, "F5: round trip failed, iteration "+itoa(iter)+" client="+boolText(isClient)+" length="+itoa(n)+" fragments="+itoa(len(cuts)-1)+" chunks="+itoa(len(chunks))))
		}
	}
}

func boolText(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func TestConn_Read_SingleFrameMessage(t *testing.T) {
	c, _ := newRecServerConn(nil, clientFrame(testOpText, true, []byte("hello")), clientFrame(testOpBinary, true, []byte{1, 2, 3}))
	kind, data, err := c.ReadMessage(nil)
	if err != nil || kind != KindText || string(data) != "hello" {
		t.Error(test.DiffMessage(string(data), "hello", "M1: a text frame with FIN is one text message"))
	}
	kind, data, err = c.ReadMessage(nil)
	if err != nil || kind != KindBinary || !bytes.Equal(data, []byte{1, 2, 3}) {
		t.Error(test.DiffMessage(data, []byte{1, 2, 3}, "M1: a binary frame with FIN is one binary message"))
	}
}

func TestConn_Read_FragmentedMessage(t *testing.T) {
	c, _ := newRecServerConn(nil,
		clientFrame(testOpText, false, []byte("he")),
		clientFrame(testOpCont, false, []byte("l")),
		clientFrame(testOpCont, true, []byte("lo")),
	)
	kind, data, err := c.ReadMessage(nil)
	if err != nil || kind != KindText || string(data) != "hello" {
		t.Error(test.DiffMessage(string(data), "hello", "M2: the message is the payloads in order, kind of the first frame"))
	}
}

func TestConn_Read_EmptyMessage(t *testing.T) {
	c, _ := newRecServerConn(nil, clientFrame(testOpBinary, true, nil))
	kind, data, err := c.ReadMessage(nil)
	if err != nil || kind != KindBinary || len(data) != 0 {
		t.Error(test.DiffMessage(err, nil, "M2: an empty message is legal"))
	}
}

func TestConn_Read_EmptyFragments(t *testing.T) {
	c, _ := newRecServerConn(nil,
		clientFrame(testOpText, false, nil),
		clientFrame(testOpCont, false, nil),
		clientFrame(testOpCont, false, []byte("ab")),
		clientFrame(testOpCont, true, nil),
	)
	kind, data, err := c.ReadMessage(nil)
	if err != nil || kind != KindText || string(data) != "ab" {
		t.Error(test.DiffMessage(string(data), "ab", "M2: empty frames are legal anywhere in a message"))
	}
}

func TestConn_Read_ContinuationWithoutMessageFails(t *testing.T) {
	for _, fin := range []bool{true, false} {
		c, rc := newRecServerConn(nil, clientFrame(testOpCont, fin, []byte("x")))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "M3: continuation without a message, FIN "+boolText(fin), rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: continuation without a message")
	}
}

func TestConn_Read_DataFrameInsideMessageFails(t *testing.T) {
	for _, second := range []byte{testOpText, testOpBinary} {
		c, rc := newRecServerConn(nil, clientFrame(testOpText, false, []byte("a")), clientFrame(second, true, []byte("b")))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "M4: data frame "+itoa(int(second))+" inside a fragmented message", rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: data frame inside a fragmented message")
	}
}

func TestConn_Read_PingBetweenFragments(t *testing.T) {
	c, rc := newRecServerConn(nil,
		clientFrame(testOpText, false, []byte("a")),
		clientFrame(testOpPing, true, []byte("p1")),
		clientFrame(testOpCont, true, []byte("b")),
	)
	_, data, err := c.ReadMessage(nil)
	if err != nil || string(data) != "ab" {
		t.Error(test.DiffMessage(string(data), "ab", "M5: the message is unchanged by a ping between fragments"))
	}
	expectBytes(t, "M5: the pong written", rc.written(), serverFrame(testOpPong, true, []byte("p1")))
}

func TestConn_Read_PongBetweenFragments(t *testing.T) {
	c, rc := newRecServerConn(nil,
		clientFrame(testOpBinary, false, []byte("a")),
		clientFrame(testOpPong, true, []byte("p")),
		clientFrame(testOpCont, true, []byte("b")),
	)
	_, data, err := c.ReadMessage(nil)
	if err != nil || string(data) != "ab" {
		t.Error(test.DiffMessage(string(data), "ab", "M5: a pong between fragments is dropped"))
	}
	if len(rc.written()) != 0 {
		t.Error(test.DiffMessage(rc.written(), []byte(nil), "M5, C2: nothing is written for a pong"))
	}
}

func TestConn_Read_CloseBetweenFragments(t *testing.T) {
	c, rc := newRecServerConn(nil,
		clientFrame(testOpText, false, []byte("a")),
		clientFrame(testOpClose, true, closePayload(1000, "")),
		clientFrame(testOpCont, true, []byte("b")),
	)
	_, _, err := c.ReadMessage(nil)
	var ce *CloseError
	if !errors.As(err, &ce) || ce.Code() != CloseNormal {
		t.Fatal(test.DiffMessage(err, "a *CloseError with 1000", "M5, C5: close between fragments"))
	}
	expectBytes(t, "C5: the echoed close frame", rc.written(), failWire(CloseNormal))
}

func TestConn_Read_MessageOverLimitAcrossFragments(t *testing.T) {
	opts := connOpts(func(o *ConnOptions) { o.MaxMessageBytes = 10 })
	second := rawHeader(0x80, true, 0, 6)
	c, rc := newRecServerConn(opts, clientFrame(testOpBinary, false, []byte("abcdef")), second)
	_, _, err := c.ReadMessage(nil)
	expectFail(t, "M6, T2: 6 + 6 bytes over a limit of 10, decided before the payload", rc, err, ErrMessageTooBig, CloseMessageTooBig, "websocket: message too big: message over the limit")
}

func TestConn_Read_MessageExactlyAtLimit(t *testing.T) {
	opts := connOpts(func(o *ConnOptions) { o.MaxMessageBytes = 10 })
	c, _ := newRecServerConn(opts, clientFrame(testOpBinary, false, []byte("abcdef")), clientFrame(testOpCont, true, []byte("ghij")))
	_, data, err := c.ReadMessage(nil)
	if err != nil || string(data) != "abcdefghij" {
		t.Error(test.DiffMessage(err, nil, "M6: a message of exactly the limit passes"))
	}
}

func TestConn_Read_TooManyFragments(t *testing.T) {
	opts := connOpts(func(o *ConnOptions) { o.MaxFragments = 3 })
	three := [][]byte{clientFrame(testOpText, false, []byte("a")), clientFrame(testOpCont, false, []byte("b")), clientFrame(testOpCont, true, []byte("c"))}
	c, _ := newRecServerConn(opts, three...)
	if _, data, err := c.ReadMessage(nil); err != nil || string(data) != "abc" {
		t.Error(test.DiffMessage(err, nil, "M7: exactly MaxFragments frames pass"))
	}
	four := [][]byte{clientFrame(testOpText, false, []byte("a")), clientFrame(testOpCont, false, []byte("b")), clientFrame(testOpCont, false, []byte("c")), rawHeader(0x80, true, 0, 5)}
	c, rc := newRecServerConn(opts, four...)
	_, _, err := c.ReadMessage(nil)
	expectFail(t, "M7: fragment number MaxFragments+1, before its payload", rc, err, ErrMessageTooBig, CloseMessageTooBig, "websocket: message too big: too many fragments")
}

func TestConn_Read_EmptyFragmentFloodFails(t *testing.T) {
	chunks := [][]byte{clientFrame(testOpText, false, nil)}
	for range 300 {
		chunks = append(chunks, clientFrame(testOpCont, false, nil))
	}
	c, rc := newRecServerConn(nil, chunks...)
	_, _, err := c.ReadMessage(nil)
	expectFail(t, "M7, T4: empty fragments count", rc, err, ErrMessageTooBig, CloseMessageTooBig, "websocket: message too big: too many fragments")
}

func TestConn_ReadMessage_ReusesCallerBuffer(t *testing.T) {
	c, _ := newRecServerConn(nil, clientFrame(testOpBinary, true, repeatByte('m', 50)))
	buf := make([]byte, 7, 100)
	_, data, err := c.ReadMessage(buf)
	if err != nil || len(data) != 50 {
		t.Fatal(test.DiffMessage(err, nil, "M8: read into a caller buffer"))
	}
	if &data[0] != &buf[:1][0] {
		t.Error(test.DiffMessage("a new array", "the caller's array", "M8: the result aliases buf when cap(buf) is enough"))
	}
}

func TestConn_ReadMessage_ZeroAllocsSteadyState(t *testing.T) {
	rc := newLoopConn(append(clientFrame(testOpBinary, true, repeatByte('a', 200)), clientFrame(testOpText, true, []byte("hello text"))...))
	c := newConn(rc, bufio.NewReaderSize(rc, 4096), false, nil)
	buf := make([]byte, 0, 512)
	allocs := testing.AllocsPerRun(200, func() {
		if _, _, err := c.ReadMessage(buf); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Error(test.DiffMessage(allocs, 0.0, "M8, Performance: allocations per ReadMessage into a large enough buffer"))
	}
}

func TestConn_ReadMessage_GrowsWithReceivedBytes(t *testing.T) {
	payload := repeatByte('g', 5000)
	c, _ := newRecServerConn(nil, clientFrame(testOpBinary, true, payload))
	_, data, err := c.ReadMessage(nil)
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatal(test.DiffMessage(err, nil, "M8: the message"))
	}
	if cap(data) > 2*len(payload)+2048 {
		t.Error(test.DiffMessage(cap(data), "at most about 2 x 5000", "M8: capacity grows with received bytes"))
	}
}

func TestConn_ReadMessage_DeclaredLengthAllocatesNothing(t *testing.T) {
	opts := connOpts(func(o *ConnOptions) { o.MaxMessageBytes = 1 << 20 })
	frame := append(rawHeader(0x82, true, 8, 1<<20), maskedPayload([]byte("0123456789"))...)
	c, _ := newRecServerConn(opts, frame)
	before := totalAlloc()
	_, _, err := c.ReadMessage(nil)
	after := totalAlloc()
	checkErr(t, "T3: a header declaring 1 MiB then 10 bytes", err, ErrAbnormalClosure, "")
	if after-before > 64*1024 {
		t.Error(test.DiffMessage(after-before, "under 64 KiB", "M8, T3: bytes allocated for a declared 1 MiB frame"))
	}
}

func TestConn_ReadMessage_ErrorReturnsEmptyBuffer(t *testing.T) {
	c, _ := newRecServerConn(nil, clientFrame(testOpText, false, []byte("ab")))
	buf := make([]byte, 5, 64)
	kind, data, err := c.ReadMessage(buf)
	if err == nil {
		t.Fatal("an error is expected")
	}
	if kind != 0 || len(data) != 0 || cap(data) != cap(buf) {
		t.Error(test.DiffMessage(len(data), 0, "M8: on error ReadMessage returns 0, buf[:0], err"))
	}
}

func TestConn_ReadMessage_GrowthSaturatesAtLimit(t *testing.T) {
	opts := connOpts(func(o *ConnOptions) { o.MaxMessageBytes = 5000 })
	c, _ := newRecServerConn(opts, clientFrame(testOpBinary, false, repeatByte('s', 3000)), clientFrame(testOpCont, true, repeatByte('s', 2000)))
	_, data, err := c.ReadMessage(nil)
	if err != nil || len(data) != 5000 {
		t.Fatal(test.DiffMessage(err, nil, "M8, T25: a message at the limit"))
	}
	if cap(data) > 5000+1024 {
		t.Error(test.DiffMessage(cap(data), "at most the limit rounded to a size class", "M8, T25: growth saturates at MaxMessageBytes"))
	}
}

func TestConn_NextReader_StreamsAcrossFragments(t *testing.T) {
	c, _ := newRecServerConn(nil,
		clientFrame(testOpText, false, []byte("ab")),
		clientFrame(testOpCont, false, []byte("cd")),
		clientFrame(testOpCont, true, []byte("ef")),
	)
	kind, r, err := c.NextReader()
	if err != nil || kind != KindText {
		t.Fatal(test.DiffMessage(err, nil, "M9: NextReader"))
	}
	data, err := io.ReadAll(r)
	if err != nil || string(data) != "abcdef" {
		t.Error(test.DiffMessage(string(data), "abcdef", "M9: the reader crosses fragment boundaries"))
	}
}

func TestConn_NextReader_EOFOnce(t *testing.T) {
	c, _ := newRecServerConn(nil, clientFrame(testOpBinary, true, []byte("xy")))
	_, r, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 16)
	total := 0
	var last error
	for range 5 {
		n, err := r.Read(p[total:])
		total += n
		last = err
		if err != nil {
			break
		}
	}
	if string(p[:total]) != "xy" || last != io.EOF {
		t.Error(test.DiffMessage(last, io.EOF, "M9: io.EOF after the final fragment, with the full content "+string(p[:total])))
	}
}

func TestConn_NextReader_ReturnsAvailableBytes(t *testing.T) {
	c, _ := newRecServerConn(nil, clientFrame(testOpBinary, false, []byte("ab")), clientFrame(testOpCont, true, []byte("cd")))
	_, r, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 100)
	n, err := r.Read(p)
	if err != nil || string(p[:n]) != "ab" {
		t.Error(test.DiffMessage(string(p[:n]), "ab", "M9, F7: a read never crosses the end of the frame and never waits to fill p"))
	}
	n, err = r.Read(p)
	if err != nil || string(p[:n]) != "cd" {
		t.Error(test.DiffMessage(string(p[:n]), "cd", "M9: the next fragment"))
	}
}

func TestConn_NextReader_DiscardsUnfinishedMessage(t *testing.T) {
	c, _ := newRecServerConn(nil,
		clientFrame(testOpText, false, []byte("abc")),
		clientFrame(testOpCont, true, []byte("def")),
		clientFrame(testOpBinary, true, []byte("zz")),
		clientFrame(testOpText, false, []byte("q")),
		clientFrame(testOpCont, true, []byte("r")),
		clientFrame(testOpText, true, []byte("last")),
	)
	_, r, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 1)
	if _, err := r.Read(one); err != nil {
		t.Fatal(err)
	}
	kind, r2, err := c.NextReader()
	if err != nil || kind != KindBinary {
		t.Fatal(test.DiffMessage(err, nil, "M9: NextReader discards the rest of an unfinished message"))
	}
	if data, _ := io.ReadAll(r2); string(data) != "zz" {
		t.Error(test.DiffMessage(string(data), "zz", "M9: the next message"))
	}
	_, r3, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r3.Read(one); err != nil {
		t.Fatal(err)
	}
	_, data, err := c.ReadMessage(nil)
	if err != nil || string(data) != "last" {
		t.Error(test.DiffMessage(string(data), "last", "M9: ReadMessage discards the rest of an unfinished streamed message"))
	}
}

func TestConn_NextReader_LimitsStillApply(t *testing.T) {
	opts := connOpts(func(o *ConnOptions) { o.MaxMessageBytes = 10 })
	c, rc := newRecServerConn(opts, clientFrame(testOpBinary, false, []byte("abcdef")), rawHeader(0x80, true, 0, 6))
	_, r, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(r)
	checkErr(t, "M9, M6: limits apply to the streaming reader", err, ErrMessageTooBig, "websocket: message too big: message over the limit")
	if got := rc.written(); !bytes.Equal(got, failWire(CloseMessageTooBig)) {
		t.Error(test.DiffMessage(got, failWire(CloseMessageTooBig), "M9: close frame 1009"))
	}
}

func TestConn_NextReader_StaleHandleReturnsErrClosed(t *testing.T) {
	c, _ := newRecServerConn(nil, clientFrame(testOpText, true, []byte("first")), clientFrame(testOpText, true, []byte("second")))
	_, r1, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	if data, _ := io.ReadAll(r1); string(data) != "first" {
		t.Fatal(test.DiffMessage(string(data), "first", "M9: the first message"))
	}
	_, r2, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	p := []byte("....")
	n, err := r1.Read(p)
	if n != 0 || !isErr(err, ErrClosed) || string(p) != "...." {
		t.Error(test.DiffMessage(err, ErrClosed, "T23, M9: a stale reader returns ErrClosed and reads nothing"))
	}
	if data, _ := io.ReadAll(r2); string(data) != "second" {
		t.Error(test.DiffMessage(string(data), "second", "T23: the stale read did not consume the current message"))
	}
}

func TestConn_NextReader_LargeBufferStopsAtFrameEnd(t *testing.T) {
	both := append(clientFrame(testOpBinary, true, []byte("abc")), clientFrame(testOpBinary, true, []byte("def"))...)
	c, _ := newRecServerConn(nil, both)
	_, r, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 4096)
	n, err := r.Read(p)
	if err != nil || string(p[:n]) != "abc" {
		t.Fatal(test.DiffMessage(string(p[:n]), "abc", "F7: a read into a large buffer stops at the end of the frame"))
	}
	if _, err := r.Read(p); err != io.EOF {
		t.Error(test.DiffMessage(err, io.EOF, "F7: end of message"))
	}
	_, data, err := c.ReadMessage(nil)
	if err != nil || string(data) != "def" {
		t.Error(test.DiffMessage(string(data), "def", "F7: the next frame is parsed as a frame"))
	}
}

func TestConn_NextReader_InvalidUTF8FailsWhileStreaming(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpText, false, []byte("ab")), clientFrame(testOpCont, true, []byte{0xff}))
	_, r, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	if string(data) != "ab" {
		t.Error(test.DiffMessage(string(data), "ab", "M9: the valid bytes are returned before the error"))
	}
	checkErr(t, "M9, U1: validation while streaming", err, ErrInvalidUTF8, "websocket: invalid UTF-8")
	if got := rc.written(); !bytes.Equal(got, failWire(CloseInvalidPayload)) {
		t.Error(test.DiffMessage(got, failWire(CloseInvalidPayload), "U1: close frame 1007"))
	}
}

func TestConn_Read_TwoFramesInOneSegmentLargeBuffer(t *testing.T) {
	both := append(clientFrame(testOpText, false, []byte("AA")), clientFrame(testOpCont, true, []byte("BB"))...)
	both = append(both, clientFrame(testOpBinary, true, []byte("CC"))...)
	c, _ := newRecServerConn(nil, both)
	_, data, err := c.ReadMessage(make([]byte, 0, 4096))
	if err != nil || string(data) != "AABB" {
		t.Fatal(test.DiffMessage(string(data), "AABB", "F7: the first message, buffer larger than both frames"))
	}
	_, data, err = c.ReadMessage(make([]byte, 0, 4096))
	if err != nil || string(data) != "CC" {
		t.Error(test.DiffMessage(string(data), "CC", "F7: the second message, its header never unmasked as payload"))
	}
}

func TestConn_Read_CloseAfterDataInOneSegmentLargeBuffer(t *testing.T) {
	both := append(clientFrame(testOpBinary, true, []byte("data")), clientFrame(testOpClose, true, closePayload(1000, ""))...)
	c, rc := newRecServerConn(nil, both)
	_, data, err := c.ReadMessage(make([]byte, 0, 4096))
	if err != nil || string(data) != "data" {
		t.Fatal(test.DiffMessage(string(data), "data", "F7: the data frame"))
	}
	_, _, err = c.ReadMessage(make([]byte, 0, 4096))
	var ce *CloseError
	if !errors.As(err, &ce) || ce.Code() != CloseNormal {
		t.Error(test.DiffMessage(err, "a *CloseError with 1000", "F7: a close in the same segment is never swallowed"))
	}
	expectBytes(t, "C5: the echo", rc.written(), failWire(CloseNormal))
}

func TestConn_Read_PingAfterDataInOneSegmentLargeBuffer(t *testing.T) {
	all := append(clientFrame(testOpBinary, true, []byte("one")), clientFrame(testOpPing, true, []byte("hey"))...)
	all = append(all, clientFrame(testOpBinary, true, []byte("two"))...)
	c, rc := newRecServerConn(nil, all)
	_, data, err := c.ReadMessage(make([]byte, 0, 4096))
	if err != nil || string(data) != "one" {
		t.Fatal(test.DiffMessage(string(data), "one", "F7: the first message"))
	}
	_, data, err = c.ReadMessage(make([]byte, 0, 4096))
	if err != nil || string(data) != "two" {
		t.Error(test.DiffMessage(string(data), "two", "F7: the message after a ping"))
	}
	expectBytes(t, "C1: the pong", rc.written(), serverFrame(testOpPong, true, []byte("hey")))
}

func TestConn_ReadsOnlyThroughBufferedReader(t *testing.T) {
	rc := newRecConn()
	rc.failOnRead = func() { t.Error("F8: the connection read the net.Conn directly") }
	stream := append(clientFrame(testOpText, true, []byte("buffered")), clientFrame(testOpBinary, true, repeatByte('x', 6000))...)
	br := bufio.NewReaderSize(bytes.NewReader(stream), 4096)
	c := newConn(rc, br, false, bigOpts())
	if _, data, err := c.ReadMessage(nil); err != nil || string(data) != "buffered" {
		t.Error(test.DiffMessage(err, nil, "F8: first message through the bufio.Reader"))
	}
	if _, data, err := c.ReadMessage(nil); err != nil || len(data) != 6000 {
		t.Error(test.DiffMessage(err, nil, "F8: a message larger than the buffer"))
	}
}

func TestConn_Read_InvalidUTF8Fails1007(t *testing.T) {
	for _, bad := range []string{"\xc0\x80", "\xe0\x80\x80", "\xed\xa0\x80", "\xf4\x90\x80\x80", "\xff", "\x80", "ok\xc1\xbf"} {
		c, rc := newRecServerConn(nil, clientFrame(testOpText, true, []byte(bad)))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "U1, T17: invalid text of "+itoa(len(bad))+" bytes", rc, err, ErrInvalidUTF8, CloseInvalidPayload, "websocket: invalid UTF-8")
	}
}

func TestConn_Read_InvalidUTF8FailsFast(t *testing.T) {
	header := rawHeader(0x81, true, 0, 100)
	prefix := maskedPayload([]byte("ok\xed\xa0"))
	c, rc := newRecServerConn(nil, append(header, prefix...))
	_, _, err := c.ReadMessage(nil)
	expectFail(t, "U2: a bad byte fails before the frame or the message ends", rc, err, ErrInvalidUTF8, CloseInvalidPayload, "websocket: invalid UTF-8")
	first := []byte("ok\xed")
	c, rc = newRecServerConn(nil, append(rawHeader(0x81, true, 0, 100), maskedPayload(first)...), []byte{'\xa0' ^ testMaskKey[3]})
	_, _, err = c.ReadMessage(nil)
	expectFail(t, "U2: the bad byte arrives alone in a later read", rc, err, ErrInvalidUTF8, CloseInvalidPayload, "websocket: invalid UTF-8")
}

func TestConn_Read_InvalidUTF8TruncatedMessage(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpText, true, []byte("ab\xc3")))
	_, _, err := c.ReadMessage(nil)
	expectFail(t, "U1: a message ending inside a code point", rc, err, ErrInvalidUTF8, CloseInvalidPayload, "websocket: invalid UTF-8")
	c, rc = newRecServerConn(nil, clientFrame(testOpText, false, []byte("\xe1\xbb")), clientFrame(testOpCont, true, nil))
	_, _, err = c.ReadMessage(nil)
	expectFail(t, "U1: ending inside a code point after an empty last fragment", rc, err, ErrInvalidUTF8, CloseInvalidPayload, "websocket: invalid UTF-8")
}

func TestConn_Read_CodePointSplitAcrossFragments(t *testing.T) {
	text := []byte("aé日😀")
	for i := 0; i <= len(text); i++ {
		for j := i; j <= len(text); j++ {
			c, _ := newRecServerConn(nil,
				clientFrame(testOpText, false, text[:i]),
				clientFrame(testOpCont, false, text[i:j]),
				clientFrame(testOpCont, true, text[j:]),
			)
			_, data, err := c.ReadMessage(nil)
			if err != nil || !bytes.Equal(data, text) {
				t.Fatal(test.DiffMessage(err, nil, "U3: code point split across fragments at "+itoa(i)+" and "+itoa(j)))
			}
		}
	}
	whole := clientFrame(testOpText, true, text)
	for i := 1; i < len(whole); i++ {
		c, _ := newRecServerConn(nil, whole[:i], whole[i:])
		_, data, err := c.ReadMessage(nil)
		if err != nil || !bytes.Equal(data, text) {
			t.Fatal(test.DiffMessage(err, nil, "U3: split across TCP reads at "+itoa(i)))
		}
	}
}

func TestConn_Read_BinaryNotValidated(t *testing.T) {
	bad := []byte{0xff, 0xfe, 0xc0, 0x80, 0xed, 0xa0, 0x80}
	c, _ := newRecServerConn(nil, clientFrame(testOpBinary, true, bad))
	kind, data, err := c.ReadMessage(nil)
	if err != nil || kind != KindBinary || !bytes.Equal(data, bad) {
		t.Error(test.DiffMessage(err, nil, "U1: binary messages are not validated"))
	}
}

func expectRemaining(t *testing.T, c *Conn, wantLeft int, wantFinal bool, desc string) {
	t.Helper()
	left, isFinal := c.Remaining()
	if left != wantLeft || isFinal != wantFinal {
		t.Error(test.DiffMessage([]any{left, isFinal}, []any{wantLeft, wantFinal}, "RB1: "+desc))
	}
}

func TestConn_Remaining_SingleFrame(t *testing.T) {
	c, _ := newRecServerConn(nil, clientFrame(testOpBinary, true, repeatByte('a', 20000)))
	_, r, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	expectRemaining(t, c, 20000, true, "the header of a final frame announces its whole payload")
	if _, err := io.ReadFull(r, make([]byte, 512)); err != nil {
		t.Fatal(err)
	}
	expectRemaining(t, c, 19488, true, "after 512 bytes, the rest of the frame")
}

func TestConn_Remaining_Fragmented(t *testing.T) {
	c, _ := newRecServerConn(nil,
		clientFrame(testOpBinary, false, repeatByte('a', 1000)),
		clientFrame(testOpCont, true, repeatByte('b', 500)),
	)
	_, r, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	expectRemaining(t, c, 1000, false, "the first frame announces 1000 bytes and is not the last")
	if _, err := io.ReadFull(r, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	expectRemaining(t, c, 900, false, "the rest of the first frame, the total is not known yet")
	if _, err := io.ReadFull(r, make([]byte, 900)); err != nil {
		t.Fatal(err)
	}
	expectRemaining(t, c, 0, false, "the first frame is read, the message is open")
	if _, err := io.ReadFull(r, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	expectRemaining(t, c, 499, true, "after the final header, the rest of the message")
}

func TestConn_Remaining_AfterLastByte(t *testing.T) {
	c, _ := newRecServerConn(nil, clientFrame(testOpText, true, repeatByte('a', 600)))
	_, r, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(r, make([]byte, 600)); err != nil {
		t.Fatal(err)
	}
	expectRemaining(t, c, 0, true, "every byte of the message was returned")
}

func TestConn_Remaining_ControlFrameBetweenFragments(t *testing.T) {
	c, _ := newRecServerConn(nil,
		clientFrame(testOpBinary, false, repeatByte('a', 100)),
		clientFrame(testOpPing, true, []byte("zz")),
		clientFrame(testOpCont, true, repeatByte('b', 200)),
	)
	_, r, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(r, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	expectRemaining(t, c, 0, false, "the first frame is read, a ping is still to come")
	if _, err := io.ReadFull(r, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	expectRemaining(t, c, 199, true, "the ping adds nothing to the message's rest")
}

type readDuringClose struct {
	c       *Conn
	peer    net.Conn
	closed  chan error
	readErr chan error
}

func startReadDuringClose(t *testing.T, timeout time.Duration) *readDuringClose {
	t.Helper()
	c, peer := newPipeConn(t, false, connOpts(func(o *ConnOptions) { o.CloseTimeout = timeout }))
	r := &readDuringClose{c: c, peer: peer, closed: make(chan error, 1), readErr: make(chan error, 1)}
	go func() { r.closed <- c.Close(CloseNormal, "") }()
	for c.state.Load() != stateClosing {
		runtime.Gosched()
	}
	go func() {
		_, _, err := c.NextReader()
		r.readErr <- err
	}()
	select {
	case err := <-r.readErr:
		t.Fatal(test.DiffMessage(err, "no return before the close frame is on the wire", "C7: a read started while Close runs"))
	case <-time.After(50 * time.Millisecond):
	}
	return r
}

func TestConn_Read_StartedDuringCloseWaitsForPeerClose(t *testing.T) {
	r := startReadDuringClose(t, 5*time.Second)
	got := answerClose(r.peer, peerClose(1000, ""))
	expectBytes(t, "C7: the frame the peer received", <-got, failWire(CloseNormal))
	if err := <-r.readErr; !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "C7: the read returns ErrClosed after the peer's close"))
	}
	if err := <-r.closed; err != nil {
		t.Error(test.DiffMessage(err, nil, "C7: Close returns nil when the peer answered"))
	}
}

func TestConn_Read_StartedDuringClose_PeerSilentEndsAtCloseTimeout(t *testing.T) {
	r := startReadDuringClose(t, 100*time.Millisecond)
	got := answerClose(r.peer, nil)
	expectBytes(t, "C7: the frame the peer received", <-got, failWire(CloseNormal))
	if err := <-r.readErr; !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "C7: the read returns ErrClosed at CloseTimeout"))
	}
	checkErr(t, "C6: the peer never answered", <-r.closed, ErrTimeout, "websocket: timeout")
}

func TestConn_Read_StartedDuringClose_TransportFailure(t *testing.T) {
	r := startReadDuringClose(t, 5*time.Second)
	got := answerClose(r.peer, nil)
	expectBytes(t, "C7: the frame the peer received", <-got, failWire(CloseNormal))
	_ = r.peer.Close()
	if err := <-r.readErr; !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "C7: the read returns ErrClosed when the transport fails"))
	}
	checkErr(t, "C6: the transport ended first", <-r.closed, ErrAbnormalClosure, "websocket: connection lost")
}
