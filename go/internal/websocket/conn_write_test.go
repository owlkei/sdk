package websocket

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"go.owlkei.com/sdk/internal/test"
)

func onlyFrames(t *testing.T, rule string, rc *recConn, masked bool) []testFrame {
	t.Helper()
	frames, err := parseTestFrames(rc.written(), masked)
	if err != nil {
		t.Fatal(test.DiffMessage(err, nil, rule+": the bytes written parse as frames"))
	}
	return frames
}

func TestConn_WriteMessage_RoundTrip(t *testing.T) {
	r := newTestRand()
	lengths := []int{0, 1, 2, 124, 125, 126, 127, 4095, 4096, 4097, 65535, 65536, 70000}
	opts := connOpts(func(o *ConnOptions) { o.MaxMessageBytes = 1 << 20 })
	for iter := range 200 {
		isClient := iter%2 == 1
		kind := KindBinary
		n := lengths[r.IntN(len(lengths))]
		var payload []byte
		if r.IntN(2) == 0 {
			kind = KindText
			payload = asciiPayload(r, n)
		} else {
			payload = randomBytes(r, n)
		}
		var writer *Conn
		var rc *recConn
		if isClient {
			writer, rc = newRecClientConn(opts)
		} else {
			writer, rc = newRecServerConn(opts)
		}
		if err := writer.WriteMessage(kind, payload); err != nil {
			t.Fatal(test.DiffMessage(err, nil, "W1: write, iteration "+itoa(iter)))
		}
		frames, err := parseTestFrames(rc.written(), isClient)
		if err != nil || len(frames) != 1 {
			t.Fatal(test.DiffMessage(err, nil, "W1: one frame per message, iteration "+itoa(iter)))
		}
		f := frames[0]
		if !f.Fin || f.Rsv != 0 || f.Opcode != byte(kind) || !bytes.Equal(f.Payload, payload) {
			t.Fatal(test.DiffMessage(len(f.Payload), n, "W1: FIN, opcode and payload, iteration "+itoa(iter)+" length "+itoa(n)))
		}
		var reader *Conn
		if isClient {
			reader, _ = newRecServerConn(opts, rc.written())
		} else {
			reader, _ = newRecClientConn(opts, rc.written())
		}
		gotKind, got, err := reader.ReadMessage(nil)
		if err != nil || gotKind != kind || !bytes.Equal(got, payload) {
			t.Fatal(test.DiffMessage(err, nil, "W1: the package reader parses what the package writer wrote, iteration "+itoa(iter)))
		}
	}
}

func TestConn_WriteMessage_MinimalLengthEncoding(t *testing.T) {
	cases := []struct {
		n      int
		header []byte
	}{
		{0, []byte{0x82, 0x00}},
		{125, []byte{0x82, 0x7d}},
		{126, []byte{0x82, 0x7e, 0x00, 0x7e}},
		{65535, []byte{0x82, 0x7e, 0xff, 0xff}},
		{65536, []byte{0x82, 0x7f, 0, 0, 0, 0, 0, 1, 0, 0}},
	}
	for _, c := range cases {
		conn, rc := newRecServerConn(nil)
		if err := conn.WriteMessage(KindBinary, make([]byte, c.n)); err != nil {
			t.Fatal(err)
		}
		got := rc.written()
		if len(got) != len(c.header)+c.n || !bytes.Equal(got[:len(c.header)], c.header) {
			t.Error(test.DiffMessage(got[:min(len(got), 10)], c.header, "W1: header of a server frame with "+itoa(c.n)+" payload bytes"))
		}
	}
}

func TestConn_WriteMessage_ClientFramesAreMasked(t *testing.T) {
	c, rc := newRecClientConn(nil)
	if err := c.WriteMessage(KindText, []byte("masked hello")); err != nil {
		t.Fatal(err)
	}
	frames := onlyFrames(t, "W1", rc, true)
	if len(frames) != 1 || string(frames[0].Payload) != "masked hello" {
		t.Error(test.DiffMessage(frames, "one masked frame", "W1, D5: a client frame is masked"))
	}
	if bytes.Contains(rc.written(), []byte("masked hello")) {
		t.Error(test.DiffMessage("plain payload on the wire", "masked payload", "W1: the payload is masked"))
	}
}

func TestConn_WriteMessage_InvalidKind(t *testing.T) {
	c, rc := newRecServerConn(nil)
	for _, k := range []Kind{0, 3, 8, 9, 255} {
		err := c.WriteMessage(k, []byte("x"))
		checkErr(t, "W1: kind "+itoa(int(k)), err, ErrInvalidArgument, "websocket: invalid argument: kind")
	}
	if len(rc.written()) != 0 {
		t.Error(test.DiffMessage(rc.written(), []byte(nil), "W1: nothing is written for an invalid kind"))
	}
	if err := c.WriteMessage(KindText, []byte("ok")); err != nil {
		t.Error(test.DiffMessage(err, nil, "W1: the connection stays usable"))
	}
}

func TestConn_WriteMessage_DoesNotModifyData(t *testing.T) {
	data := []byte("do not touch this payload, it is longer than sixteen bytes")
	orig := append([]byte(nil), data...)
	for _, isClient := range []bool{false, true} {
		var c *Conn
		if isClient {
			c, _ = newRecClientConn(nil)
		} else {
			c, _ = newRecServerConn(nil)
		}
		if err := c.WriteMessage(KindBinary, data); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, orig) {
			t.Error(test.DiffMessage(data, orig, "W1: data is never modified, client="+boolText(isClient)))
		}
	}
}

func TestConn_WriteMessage_BlockedPeerCutAtWriteTimeout(t *testing.T) {
	c, _ := newPipeConn(t, false, connOpts(func(o *ConnOptions) { o.WriteTimeout = 50 * time.Millisecond }))
	var err error
	took := elapsed(func() { err = c.WriteMessage(KindBinary, make([]byte, 100)) })
	checkErr(t, "W6, T8: a peer that never reads", err, ErrTimeout, "websocket: timeout")
	if took < 40*time.Millisecond || took > 5*time.Second {
		t.Error(test.DiffMessage(took, "about 50ms", "W6: the write is cut at WriteTimeout"))
	}
	if err := c.WriteMessage(KindBinary, nil); !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "W6, X1: the connection is closed after a timeout"))
	}
}

func TestConn_WriteMessage_WriteErrorAborts(t *testing.T) {
	c, rc := newRecServerConn(nil)
	rc.writeErr = errBoom{}
	err := c.WriteMessage(KindText, []byte("x"))
	checkErr(t, "W6: a transport error", err, ErrAbnormalClosure, "websocket: connection lost")
	if isErr(err, rc.writeErr) {
		t.Error(test.DiffMessage(err, "no wrapped transport error", "E1: the error exposes no transport error"))
	}
	if rc.closes() != 1 {
		t.Error(test.DiffMessage(rc.closes(), 1, "W6: the connection is aborted"))
	}
	if err := c.WriteMessage(KindText, []byte("x")); !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "X1"))
	}
}

type errBoom struct{}

func (errBoom) Error() string { return "zz-LEAK boom" }

func TestConn_WriteMessage_NonVectoredOneWrite(t *testing.T) {
	for _, n := range []int{0, 16, 125, 126, 1024, 4000} {
		c, rc := newRecServerConn(nil)
		if err := c.WriteMessage(KindBinary, repeatByte('v', n)); err != nil {
			t.Fatal(err)
		}
		if got := rc.writeCount(); got != 1 {
			t.Error(test.DiffMessage(got, 1, "P8: Write calls of a "+itoa(n)+"-byte frame on a non-vectored transport"))
		}
		frames := onlyFrames(t, "P8", rc, false)
		if len(frames) != 1 || len(frames[0].Payload) != n {
			t.Error(test.DiffMessage(len(frames), 1, "P8: the frame parses"))
		}
	}
	c, rc := newRecServerConn(nil)
	if err := c.WriteMessage(KindBinary, repeatByte('v', 8192)); err != nil {
		t.Fatal(err)
	}
	if got := rc.writeCount(); got != 2 {
		t.Error(test.DiffMessage(got, 2, "P8: a payload over the scratch size leaves in two writes"))
	}
}

func TestConn_WriteMessage_PooledScratchCarriesNoLeftover(t *testing.T) {
	sizes := []int{4000, 5, 3000, 0, 7, 4000, 1}
	for _, isClient := range []bool{false, true} {
		var c *Conn
		var rc *recConn
		if isClient {
			c, rc = newRecClientConn(nil)
		} else {
			c, rc = newRecServerConn(nil)
		}
		for i, n := range sizes {
			if err := c.WriteMessage(KindBinary, repeatByte(byte('A'+i), n)); err != nil {
				t.Fatal(err)
			}
		}
		frames := onlyFrames(t, "W9", rc, isClient)
		if len(frames) != len(sizes) {
			t.Fatal(test.DiffMessage(len(frames), len(sizes), "W9: frames written"))
		}
		for i, f := range frames {
			if !bytes.Equal(f.Payload, repeatByte(byte('A'+i), sizes[i])) {
				t.Error(test.DiffMessage(len(f.Payload), sizes[i], "W9: frame "+itoa(i)+" holds only its own bytes, client="+boolText(isClient)))
			}
		}
	}
	other, rc2 := newRecServerConn(nil)
	big, _ := newRecServerConn(nil)
	if err := big.WriteMessage(KindBinary, repeatByte('L', 4000)); err != nil {
		t.Fatal(err)
	}
	if err := other.WriteMessage(KindBinary, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	expectBytes(t, "W9: a small frame after a large one on another connection", rc2.written(), serverFrame(testOpBinary, true, []byte("hello")))
}

func TestConn_NextWriter_OneFramePerWrite(t *testing.T) {
	c, rc := newRecServerConn(nil)
	w, err := c.NextWriter(KindText)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := w.Write([]byte("ab")); n != 2 || err != nil {
		t.Fatal(test.DiffMessage(err, nil, "W2: first Write"))
	}
	want := serverFrame(testOpText, false, []byte("ab"))
	expectBytes(t, "W2: the first fragment is on the wire when Write returns", rc.written(), want)
	if n, err := w.Write([]byte("cde")); n != 3 || err != nil {
		t.Fatal(test.DiffMessage(err, nil, "W2: second Write"))
	}
	want = append(want, serverFrame(testOpCont, false, []byte("cde"))...)
	expectBytes(t, "W2: the second fragment", rc.written(), want)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	want = append(want, serverFrame(testOpCont, true, nil)...)
	expectBytes(t, "W2: Close writes the final empty frame", rc.written(), want)
}

func TestConn_NextWriter_EmptyWriteWritesNothing(t *testing.T) {
	c, rc := newRecServerConn(nil)
	w, err := c.NextWriter(KindBinary)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := w.Write(nil); n != 0 || err != nil {
		t.Error(test.DiffMessage(err, nil, "W2: an empty Write"))
	}
	if len(rc.written()) != 0 {
		t.Error(test.DiffMessage(rc.written(), []byte(nil), "W2: an empty Write writes nothing"))
	}
}

func TestConn_NextWriter_NoWriteSendsSingleFrame(t *testing.T) {
	c, rc := newRecServerConn(nil)
	w, err := c.NextWriter(KindBinary)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	expectBytes(t, "W2: Close without a Write sends one FIN frame with the message opcode", rc.written(), serverFrame(testOpBinary, true, nil))
}

func TestConn_NextWriter_ExcludesDataWrites(t *testing.T) {
	c, rc := newRecServerConn(nil)
	w, err := c.NextWriter(KindText)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("a")); err != nil {
		t.Fatal(err)
	}
	before := rc.written()
	pm, err := NewPreparedMessage(KindText, []byte("p"))
	if err != nil {
		t.Fatal(err)
	}
	checkErr(t, "W3: WriteMessage", c.WriteMessage(KindText, []byte("x")), ErrWriteInProgress, "")
	checkErr(t, "W3: WritePrepared", c.WritePrepared(pm), ErrWriteInProgress, "")
	_, err = c.NextWriter(KindText)
	checkErr(t, "W3: NextWriter", err, ErrWriteInProgress, "")
	expectBytes(t, "W3: nothing written by the refused calls", rc.written(), before)
	if err := c.Ping([]byte("pp")); err != nil {
		t.Fatal(test.DiffMessage(err, nil, "W3: Ping between fragments"))
	}
	if _, err := w.Write([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	frames := onlyFrames(t, "W3", rc, false)
	ops := []byte{}
	for _, f := range frames {
		ops = append(ops, f.Opcode)
	}
	if !bytes.Equal(ops, []byte{testOpText, testOpPing, testOpCont, testOpCont}) {
		t.Error(test.DiffMessage(ops, []byte{testOpText, testOpPing, testOpCont, testOpCont}, "W3: the ping sits between the fragments"))
	}
	if err := c.WriteMessage(KindText, []byte("after")); err != nil {
		t.Error(test.DiffMessage(err, nil, "W3: data writes work again after Close"))
	}
}

func TestConn_NextWriter_UseAfterClose(t *testing.T) {
	c, rc := newRecServerConn(nil)
	w, err := c.NextWriter(KindText)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	before := rc.written()
	_, err = w.Write([]byte("late"))
	checkErr(t, "W2: Write after Close", err, ErrClosed, "")
	checkErr(t, "W2: Close after Close", w.Close(), ErrClosed, "")
	expectBytes(t, "W2: nothing written after Close", rc.written(), before)
}

func TestConn_NextWriter_StaleHandleReturnsErrClosed(t *testing.T) {
	c, rc := newRecServerConn(nil)
	w1, err := c.NextWriter(KindText)
	if err != nil {
		t.Fatal(err)
	}
	if err := w1.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := c.NextWriter(KindBinary)
	if err != nil {
		t.Fatal(err)
	}
	before := rc.written()
	n, err := w1.Write([]byte("zz"))
	if n != 0 {
		t.Error(test.DiffMessage(n, 0, "T23: a stale writer writes no byte"))
	}
	checkErr(t, "T23: a stale Write", err, ErrClosed, "")
	checkErr(t, "T23: a stale Close", w1.Close(), ErrClosed, "")
	expectBytes(t, "T23: nothing is written by a stale handle", rc.written(), before)
	if _, err := w2.Write([]byte("fresh")); err != nil {
		t.Error(test.DiffMessage(err, nil, "T23: the current handle still works"))
	}
	if err := w2.Close(); err != nil {
		t.Error(err)
	}
}

func TestConn_NextWriter_WritesNothingAfterClose(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpClose, true, closePayload(1000, "")))
	w, err := c.NextWriter(KindText)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(CloseNormal, ""); err != nil {
		t.Fatal(test.DiffMessage(err, nil, "W3: Close"))
	}
	before := rc.written()
	_, err = w.Write([]byte("b"))
	checkErr(t, "W3, C10: Write after the close frame", err, ErrClosed, "")
	checkErr(t, "W3, C10: Close of the writer after the close frame", w.Close(), ErrClosed, "")
	expectBytes(t, "T22: no frame after the close frame", rc.written(), before)
	frames := onlyFrames(t, "T22", rc, false)
	if len(frames) == 0 || frames[len(frames)-1].Opcode != testOpClose {
		t.Error(test.DiffMessage(frames, "the close frame last", "T22"))
	}
}

func TestConn_WriteQueuedBehindClose_ReturnsErrClosed(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpClose, true, closePayload(1000, "")))
	result := make(chan error, 1)
	var once sync.Once
	rc.hook = func(p []byte) {
		if p[0] == 0x88 {
			once.Do(func() {
				go func() { result <- c.WriteMessage(KindText, []byte("late")) }()
			})
		}
	}
	if err := c.Close(CloseNormal, ""); err != nil {
		t.Fatal(test.DiffMessage(err, nil, "C6: Close"))
	}
	err := <-result
	checkErr(t, "C10, T22: a write racing the close frame", err, ErrClosed, "")
	frames := onlyFrames(t, "C10", rc, false)
	closes := 0
	for i, f := range frames {
		if f.Opcode == testOpClose {
			closes++
			if i != len(frames)-1 {
				t.Error(test.DiffMessage(i, len(frames)-1, "C10: a frame follows the close frame"))
			}
		}
	}
	if closes != 1 {
		t.Error(test.DiffMessage(closes, 1, "C10: one close frame"))
	}
}

func TestConn_PingAfterCloseFrame_ReturnsErrClosed(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpClose, true, closePayload(1000, "")))
	if err := c.Close(CloseNormal, ""); err != nil {
		t.Fatal(err)
	}
	before := rc.written()
	checkErr(t, "C10: Ping after the close frame", c.Ping([]byte("x")), ErrClosed, "")
	expectBytes(t, "C10: nothing written", rc.written(), before)
}

func TestConn_ConcurrentPings_NoRace(t *testing.T) {
	c, rc := newRecServerConn(nil)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for range 100 {
				if err := c.Ping([]byte{byte('a' + g), byte('a' + g)}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	frames := onlyFrames(t, "W5", rc, false)
	if len(frames) != 800 {
		t.Fatal(test.DiffMessage(len(frames), 800, "W5: pings written"))
	}
	for _, f := range frames {
		if f.Opcode != testOpPing || len(f.Payload) != 2 || f.Payload[0] != f.Payload[1] {
			t.Fatal(test.DiffMessage(f, "an intact ping", "W5: interleaved bytes"))
		}
	}
}

func TestConn_ConcurrentWriteMessage_FramesIntact(t *testing.T) {
	c, rc := newRecServerConn(nil)
	sizes := []int{10, 200, 5000, 20000}
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Go(func() {
			for i := range 40 {
				n := sizes[(g+i)%len(sizes)]
				if err := c.WriteMessage(KindBinary, repeatByte(byte('A'+g), n)); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	frames := onlyFrames(t, "W5", rc, false)
	if len(frames) != 160 {
		t.Fatal(test.DiffMessage(len(frames), 160, "W5: frames written"))
	}
	for _, f := range frames {
		if len(f.Payload) == 0 || !bytes.Equal(f.Payload, repeatByte(f.Payload[0], len(f.Payload))) {
			t.Fatal(test.DiffMessage(len(f.Payload), "a uniform payload", "W5: frames never interleave"))
		}
	}
}

func TestConn_PingDuringStreamedMessage_ArrivesBetweenFragments(t *testing.T) {
	c, rc := newRecServerConn(nil)
	w, err := c.NextWriter(KindText)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("aa")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Ping([]byte("mid")) }()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("bb")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	want := serverFrame(testOpText, false, []byte("aa"))
	want = append(want, serverFrame(testOpPing, true, []byte("mid"))...)
	want = append(want, serverFrame(testOpCont, false, []byte("bb"))...)
	want = append(want, serverFrame(testOpCont, true, nil)...)
	expectBytes(t, "W3, W5: a ping written during a fragmented message arrives between its fragments", rc.written(), want)
}

func TestConn_PongDuringSmallWrites_FramesIntact(t *testing.T) {
	var input [][]byte
	for i := range 150 {
		input = append(input, clientFrame(testOpPing, true, []byte{'p', byte('0' + i/100), byte('0' + i/10%10), byte('0' + i%10)}))
	}
	input = append(input, clientFrame(testOpClose, true, closePayload(1000, "")))
	c, rc := newRecServerConn(connOpts(func(o *ConnOptions) { o.MaxPingsPerSecond = 100000 }), input...)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			if _, _, err := c.ReadMessage(nil); err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range 150 {
			if err := c.WriteMessage(KindBinary, repeatByte('W', 20)); err != nil {
				return
			}
		}
	}()
	wg.Wait()
	frames := onlyFrames(t, "W8", rc, false)
	closes := 0
	for _, f := range frames {
		switch f.Opcode {
		case testOpPong:
			if len(f.Payload) != 4 || f.Payload[0] != 'p' {
				t.Fatal(test.DiffMessage(f.Payload, "p000..p149", "W8: a pong holds only its ping"))
			}
		case testOpBinary:
			if !bytes.Equal(f.Payload, repeatByte('W', 20)) {
				t.Fatal(test.DiffMessage(f.Payload, repeatByte('W', 20), "W8: small data frames share no byte with a pong"))
			}
		case testOpClose:
			closes++
		default:
			t.Fatal(test.DiffMessage(f.Opcode, "pong, binary or close", "W8"))
		}
	}
	if closes > 1 {
		t.Error(test.DiffMessage(closes, 1, "C10: at most one close frame"))
	}
}

func TestConn_WritePrepared_ConcurrentFanOut(t *testing.T) {
	data := repeatByte('f', 3000)
	pm, err := NewPreparedMessage(KindBinary, data)
	if err != nil {
		t.Fatal(err)
	}
	const conns = 16
	var wg sync.WaitGroup
	recs := make([]*recConn, conns)
	for i := range conns {
		c, rc := newRecServerConn(nil)
		recs[i] = rc
		wg.Go(func() {
			for range 5 {
				if err := c.WritePrepared(pm); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	one := serverFrame(testOpBinary, true, data)
	want := bytes.Repeat(one, 5)
	for i, rc := range recs {
		if !bytes.Equal(rc.written(), want) {
			t.Error(test.DiffMessage(len(rc.written()), len(want), "P5: fan-out bytes of connection "+itoa(i)))
		}
	}
}

func TestConn_DataWriteAllowedLocked_RefusesOnceClosing(t *testing.T) {
	c, _ := newRecServerConn(nil)
	c.state.Store(stateClosing)
	c.writeMu.Lock()
	err := c.dataWriteAllowedLocked()
	c.writeMu.Unlock()
	checkErr(t, "C10: a data write that got the lock after the close began", err, ErrClosed, "")
}

func TestConn_WriterUsableLocked_RefusesOnceClosing(t *testing.T) {
	c, _ := newRecServerConn(nil)
	w, err := c.NextWriter(KindText)
	if err != nil {
		t.Fatal(err)
	}
	c.state.Store(stateClosing)
	c.writeMu.Lock()
	err = c.writerUsableLocked(w.(*messageWriter).gen)
	c.writeMu.Unlock()
	checkErr(t, "C10: an open writer that got the lock after the close began", err, ErrClosed, "")
}

func TestConn_StreamedWriteAfterCloseFrame_WritesNothing(t *testing.T) {
	c, rc := newRecServerConn(nil)
	rc.block = true
	w, err := c.NextWriter(KindText)
	if err != nil {
		t.Fatal(err)
	}
	sent := make(chan struct{})
	var once sync.Once
	rc.hook = func(p []byte) {
		if p[0] == 0x88 {
			once.Do(func() { close(sent) })
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close(CloseNormal, "") }()
	<-sent
	_, err = w.Write([]byte("late"))
	checkErr(t, "C10, T22: a streamed Write while Close waits for the peer", err, ErrClosed, "")
	expectBytes(t, "C10, T22: nothing follows the close frame", rc.written(), []byte{0x88, 0x02, 0x03, 0xe8})
	c.Abort()
	checkErr(t, "C9: Close ends when the connection is aborted", <-closed, ErrClosed, "")
}

func TestConn_PingLocked_RefusesOnceClosing(t *testing.T) {
	c, rc := newRecServerConn(nil)
	c.state.Store(stateClosing)
	c.writeMu.Lock()
	err := c.pingLocked([]byte("x"))
	c.writeMu.Unlock()
	checkErr(t, "C10: a ping that got the lock after the close began", err, ErrClosed, "")
	expectBytes(t, "C10: nothing written", rc.written(), nil)
}

func TestConn_WritePongLocked_WritesNothingOnceClosing(t *testing.T) {
	c, rc := newRecServerConn(nil)
	c.state.Store(stateClosing)
	c.writeMu.Lock()
	err := c.writePongLocked([]byte("x"), [4]byte{}, false)
	c.writeMu.Unlock()
	if err != nil {
		t.Error(test.DiffMessage(err, nil, "C10: a pong dropped after the close began"))
	}
	expectBytes(t, "C10: no pong after the close began", rc.written(), nil)
}

func TestConn_WriteMessage_TCPEqualsNonVectored(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := ln.Accept()
		accepted <- conn
	}()
	peer, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.Close() }()
	server := <-accepted
	if server == nil {
		t.Fatal("accept failed")
	}
	defer func() { _ = server.Close() }()
	c := newConn(server, bufio.NewReader(server), false, nil)
	if !c.isVectored {
		t.Fatal("P8: a TCP connection is vectored")
	}
	sizes := []int{0, 125, 126, 1024, scratchSize - maxHeader - 1, scratchSize - maxHeader, scratchSize - maxHeader + 1, scratchSize, 8192, 70000}
	r := newTestRand()
	for _, n := range sizes {
		payload := randomBytes(r, n)
		if err := c.WriteMessage(KindBinary, payload); err != nil {
			t.Fatal(err)
		}
		ref, rc := newRecServerConn(nil)
		if err := ref.WriteMessage(KindBinary, payload); err != nil {
			t.Fatal(err)
		}
		want := rc.written()
		got := make([]byte, len(want))
		_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(peer, got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Error(test.DiffMessage(len(got), len(want), "P8: the TCP bytes of a "+itoa(n)+"-byte message equal the non-vectored ones"))
		}
	}
}
