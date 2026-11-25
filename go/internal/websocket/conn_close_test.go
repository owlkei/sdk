package websocket

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.owlkei.com/sdk/internal/test"
)

func answerClose(peer net.Conn, reply []byte) <-chan []byte {
	ch := make(chan []byte, 1)
	go func() {
		hdr := make([]byte, 2)
		if _, err := io.ReadFull(peer, hdr); err != nil {
			ch <- nil
			return
		}
		rest := make([]byte, hdr[1]&0x7f)
		if _, err := io.ReadFull(peer, rest); err != nil {
			ch <- nil
			return
		}
		if reply != nil {
			_, _ = peer.Write(reply)
		}
		ch <- append(hdr, rest...)
	}()
	return ch
}

func peerClose(code uint16, reason string) []byte {
	return clientFrame(testOpClose, true, closePayload(code, reason))
}

func closeErrorOf(t *testing.T, err error) *CloseError {
	t.Helper()
	var ce *CloseError
	if !errors.As(err, &ce) {
		t.Fatal(test.DiffMessage(err, "a *CloseError", "C5: the read returns the peer's close"))
	}
	return ce
}

func TestConn_Read_PingAnsweredWithSamePayload(t *testing.T) {
	for _, n := range []int{0, 1, 125} {
		payload := repeatByte('P', n)
		c, rc := newRecServerConn(nil, clientFrame(testOpPing, true, payload), clientFrame(testOpText, true, []byte("x")))
		_, data, err := c.ReadMessage(nil)
		if err != nil || string(data) != "x" {
			t.Fatal(test.DiffMessage(err, nil, "C1: the message after a ping of "+itoa(n)+" bytes"))
		}
		expectBytes(t, "C1: the pong holds the ping payload ("+itoa(n)+" bytes)", rc.written(), serverFrame(testOpPong, true, payload))
	}
	c, rc := newRecClientConn(nil, serverFrame(testOpPing, true, []byte("cp")), serverFrame(testOpText, true, []byte("x")))
	if _, _, err := c.ReadMessage(nil); err != nil {
		t.Fatal(err)
	}
	frames, err := parseTestFrames(rc.written(), true)
	if err != nil || len(frames) != 1 || frames[0].Opcode != testOpPong || string(frames[0].Payload) != "cp" {
		t.Error(test.DiffMessage(frames, "one masked pong", "C1, D6: a client answers with a masked pong"))
	}
}

func TestConn_Read_PongIgnored(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpPong, true, []byte("unsolicited")), clientFrame(testOpText, true, []byte("x")))
	_, data, err := c.ReadMessage(nil)
	if err != nil || string(data) != "x" {
		t.Error(test.DiffMessage(err, nil, "C2: a pong is dropped"))
	}
	if len(rc.written()) != 0 {
		t.Error(test.DiffMessage(rc.written(), []byte(nil), "C2: nothing answers a pong"))
	}
}

func TestConn_Ping_OverLimit(t *testing.T) {
	c, rc := newRecServerConn(nil)
	err := c.Ping(repeatByte('x', 126))
	checkErr(t, "C3: a ping of 126 bytes", err, ErrInvalidArgument, "websocket: invalid argument: ping payload over 125 bytes")
	if len(rc.written()) != 0 {
		t.Error(test.DiffMessage(rc.written(), []byte(nil), "C3: nothing written"))
	}
	for _, n := range []int{0, 125} {
		if err := c.Ping(repeatByte('y', n)); err != nil {
			t.Error(test.DiffMessage(err, nil, "C3: a ping of "+itoa(n)+" bytes"))
		}
	}
	frames := onlyFrames(t, "C3", rc, false)
	if len(frames) != 2 || frames[0].Opcode != testOpPing || len(frames[1].Payload) != 125 {
		t.Error(test.DiffMessage(frames, "two ping frames", "C3"))
	}
}

func TestConn_Read_EmptyClosePayload(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpClose, true, nil))
	_, _, err := c.ReadMessage(nil)
	ce := closeErrorOf(t, err)
	if ce.Code() != CloseNoStatus || ce.Reason() != "" {
		t.Error(test.DiffMessage(ce.Code(), CloseNoStatus, "C4: an empty close payload is 1005 with no reason"))
	}
	checkErr(t, "C5", err, nil, "websocket: closed by peer: 1005")
	expectBytes(t, "C5: the echo of 1005 is an empty close frame", rc.written(), []byte{0x88, 0x00})
}

func TestConn_Read_OneByteClosePayloadFails(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpClose, true, []byte{0x03}))
	_, _, err := c.ReadMessage(nil)
	expectFail(t, "C4: a close payload of one byte", rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: close payload of one byte")
}

func TestConn_Read_InvalidCloseCodes(t *testing.T) {
	codes := []uint16{0, 999, 1004, 1005, 1006, 1014, 1015, 1016, 1100, 2000, 2999, 5000, 65535}
	for _, code := range codes {
		c, rc := newRecServerConn(nil, peerClose(code, ""))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "C4, T20: close code "+itoa(int(code)), rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: invalid close code")
	}
}

func TestConn_Read_ValidCloseCodes(t *testing.T) {
	codes := []uint16{1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 3000, 3999, 4000, 4999}
	for _, code := range codes {
		c, rc := newRecServerConn(nil, peerClose(code, "bye"))
		_, _, err := c.ReadMessage(nil)
		ce := closeErrorOf(t, err)
		if ce.Code() != CloseCode(code) || ce.Reason() != "bye" {
			t.Error(test.DiffMessage(ce.Code(), code, "C4: valid close code "+itoa(int(code))))
		}
		expectBytes(t, "C5: the echo holds the code and no reason", rc.written(), failWire(CloseCode(code)))
		if rc.closes() != 1 {
			t.Error(test.DiffMessage(rc.closes(), 1, "C5: the server closes the transport"))
		}
	}
}

func TestConn_Read_CloseReasonInvalidUTF8(t *testing.T) {
	c, rc := newRecServerConn(nil, peerClose(1000, "\xff\xfe"))
	_, _, err := c.ReadMessage(nil)
	expectFail(t, "C4, T17: a close reason that is not UTF-8", rc, err, ErrInvalidUTF8, CloseInvalidPayload, "")
	c, _ = newRecServerConn(nil, peerClose(1000, "tạm biệt"))
	_, _, err = c.ReadMessage(nil)
	if ce := closeErrorOf(t, err); ce.Reason() != "tạm biệt" {
		t.Error(test.DiffMessage(ce.Reason(), "tạm biệt", "C4: a UTF-8 reason is accepted"))
	}
}

func TestConn_Read_PeerCloseReturnsCloseError(t *testing.T) {
	c, rc := newRecServerConn(nil, peerClose(3000, "bye"), clientFrame(testOpText, true, []byte("after")))
	_, _, err := c.ReadMessage(nil)
	ce := closeErrorOf(t, err)
	if ce.Code() != 3000 || ce.Reason() != "bye" {
		t.Error(test.DiffMessage(ce.Code(), 3000, "C5: code and reason"))
	}
	checkErr(t, "C5", err, nil, "websocket: closed by peer: 3000")
	if rc.closes() != 1 {
		t.Error(test.DiffMessage(rc.closes(), 1, "C5: the transport is closed"))
	}
	if _, _, err := c.ReadMessage(nil); !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "C5, X1: frames after the peer's close are never read"))
	}
}

func TestConn_Close_NeverEchoesPeerReason(t *testing.T) {
	c, rc := newRecServerConn(nil, peerClose(1000, leakMarker))
	_, _, err := c.ReadMessage(nil)
	_ = closeErrorOf(t, err)
	expectBytes(t, "C5, E3, T15: the echo holds the code only", rc.written(), failWire(CloseNormal))
	if containsLeak(rc.written()) {
		t.Error(test.DiffMessage("the reason on the wire", "no reason", "T15"))
	}
}

func TestConn_Read_ServerClosesTCPAfterPeerClose(t *testing.T) {
	c, rc := newRecServerConn(nil, peerClose(1000, ""))
	if _, _, err := c.ReadMessage(nil); err == nil {
		t.Fatal("a close error is expected")
	}
	if rc.closes() != 1 || len(rc.written()) == 0 {
		t.Error(test.DiffMessage(rc.closes(), 1, "C5: the echo is written, then the server closes the TCP connection at once"))
	}
}

func TestConn_Read_ClientWaitsForServerToCloseTCP(t *testing.T) {
	c, peer := newPipeConn(t, true, connOpts(func(o *ConnOptions) { o.CloseTimeout = 50 * time.Millisecond }))
	echo := make(chan []byte, 1)
	go func() {
		_, _ = peer.Write(serverFrame(testOpClose, true, closePayload(1000, "")))
		buf := make([]byte, 8)
		_, _ = io.ReadFull(peer, buf)
		echo <- buf
	}()
	var err error
	took := elapsed(func() { _, _, err = c.ReadMessage(nil) })
	ce := closeErrorOf(t, err)
	if ce.Code() != CloseNormal {
		t.Error(test.DiffMessage(ce.Code(), CloseNormal, "C5"))
	}
	if got := <-echo; len(got) != 8 || got[0] != 0x88 || got[1] != 0x82 {
		t.Error(test.DiffMessage(got, "a masked close frame", "C5, D6: the client echoes masked"))
	}
	if took < 40*time.Millisecond || took > 5*time.Second {
		t.Error(test.DiffMessage(took, "about CloseTimeout", "C5: the client waits for the server to close, at most CloseTimeout"))
	}
	if _, _, err := c.ReadMessage(nil); !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "X1"))
	}
}

func TestConn_Close_SendsCodeAndReason(t *testing.T) {
	c, rc := newRecServerConn(nil, peerClose(1001, ""))
	if err := c.Close(CloseGoingAway, "bye"); err != nil {
		t.Fatal(test.DiffMessage(err, nil, "C6"))
	}
	expectBytes(t, "C6: the close frame", rc.written(), serverFrame(testOpClose, true, closePayload(1001, "bye")))
	if rc.closes() != 1 {
		t.Error(test.DiffMessage(rc.closes(), 1, "C6, L7: the transport is closed once"))
	}
}

func TestConn_Close_Sends1012And1013(t *testing.T) {
	for _, code := range []CloseCode{CloseServiceRestart, CloseTryAgainLater} {
		c, rc := newRecServerConn(nil, peerClose(uint16(code), ""))
		if err := c.Close(code, ""); err != nil {
			t.Fatal(test.DiffMessage(err, nil, "C14: Close accepts "+itoa(int(code))))
		}
		expectBytes(t, "C14: the close frame holds "+itoa(int(code))+", not 1011", rc.written(), serverFrame(testOpClose, true, closePayload(uint16(code), "")))
	}
}

func TestConn_Close_ServiceRestartAndTryAgainLaterValues(t *testing.T) {
	if CloseServiceRestart != 1012 || CloseTryAgainLater != 1013 {
		t.Error(test.DiffMessage([]CloseCode{CloseServiceRestart, CloseTryAgainLater}, []CloseCode{1012, 1013}, "C14: the constants"))
	}
}

func TestConn_Read_Accepts1012And1013FromPeer(t *testing.T) {
	for _, code := range []uint16{1012, 1013} {
		c, rc := newRecServerConn(nil, peerClose(code, ""))
		_, _, err := c.ReadMessage(nil)
		ce := closeErrorOf(t, err)
		if ce.Code() != CloseCode(code) {
			t.Error(test.DiffMessage(ce.Code(), code, "C14: a peer close with "+itoa(int(code))+" is a close, not a protocol error"))
		}
		expectBytes(t, "C14: the echo holds the peer's code", rc.written(), failWire(CloseCode(code)))
	}
}

func TestConn_Close_ClientMasksCloseFrame(t *testing.T) {
	c, rc := newRecClientConn(nil, serverFrame(testOpClose, true, closePayload(1000, "")))
	if err := c.Close(CloseNormal, "r"); err != nil {
		t.Fatal(err)
	}
	frames, err := parseTestFrames(rc.written(), true)
	if err != nil || len(frames) != 1 || frames[0].Opcode != testOpClose || !bytes.Equal(frames[0].Payload, closePayload(1000, "r")) {
		t.Error(test.DiffMessage(frames, "one masked close frame", "C6, D6"))
	}
}

func TestConn_Close_WaitsForPeerClose(t *testing.T) {
	c, peer := newPipeConn(t, false, nil)
	got := answerClose(peer, peerClose(1000, ""))
	if err := c.Close(CloseNormal, ""); err != nil {
		t.Fatal(test.DiffMessage(err, nil, "C6: Close returns nil when the peer's close arrived"))
	}
	expectBytes(t, "C6: the frame the peer received", <-got, failWire(CloseNormal))
}

func TestConn_Close_PeerSilentCutAtCloseTimeout(t *testing.T) {
	c, peer := newPipeConn(t, false, connOpts(func(o *ConnOptions) { o.CloseTimeout = 50 * time.Millisecond }))
	got := answerClose(peer, nil)
	var err error
	took := elapsed(func() { err = c.Close(CloseNormal, "") })
	checkErr(t, "C6, T20: the peer never answers", err, ErrTimeout, "websocket: timeout")
	if took < 40*time.Millisecond || took > 5*time.Second {
		t.Error(test.DiffMessage(took, "about CloseTimeout", "C6: the wait ends at CloseTimeout"))
	}
	<-got
	if _, _, err := c.ReadMessage(nil); !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "X1"))
	}
}

func TestConn_Close_TransportFailsFirst(t *testing.T) {
	c, rc := newRecServerConn(nil)
	err := c.Close(CloseNormal, "")
	checkErr(t, "C6: the transport ends before the peer's close", err, ErrAbnormalClosure, "websocket: connection lost")
	if rc.closes() != 1 {
		t.Error(test.DiffMessage(rc.closes(), 1, "L7"))
	}
}

func TestConn_Close_DiscardsDataWhileClosing(t *testing.T) {
	c, rc := newRecServerConn(nil,
		clientFrame(testOpText, true, []byte("data")),
		clientFrame(testOpPing, true, []byte("ping")),
		clientFrame(testOpBinary, false, []byte("frag")),
		clientFrame(testOpCont, true, []byte("ment")),
		peerClose(1000, ""),
	)
	if err := c.Close(CloseNormal, ""); err != nil {
		t.Fatal(test.DiffMessage(err, nil, "C6"))
	}
	expectBytes(t, "C6: pings are unanswered while closing, nothing but the close frame is written", rc.written(), failWire(CloseNormal))
}

func TestConn_Close_InvalidCodeSends1011(t *testing.T) {
	for _, code := range []CloseCode{0, 999, 1004, 1006, 1014, 2999, 5000} {
		c, rc := newRecServerConn(nil, peerClose(1011, ""))
		err := c.Close(code, "reason")
		checkErr(t, "C6: code "+itoa(int(code)), err, ErrInvalidArgument, "websocket: invalid argument: close code")
		expectBytes(t, "C6: an invalid code sends 1011 and no reason", rc.written(), failWire(CloseInternalError))
		if rc.closes() != 1 {
			t.Error(test.DiffMessage(rc.closes(), 1, "C6: the handshake ran and the transport is closed"))
		}
	}
}

func TestConn_Close_InvalidReasonSends1011(t *testing.T) {
	for name, reason := range map[string]string{"124 bytes": strings.Repeat("r", 124), "not UTF-8": "\xff\xfe"} {
		c, rc := newRecServerConn(nil, peerClose(1011, ""))
		err := c.Close(CloseNormal, reason)
		checkErr(t, "C6: reason "+name, err, ErrInvalidArgument, "websocket: invalid argument: close reason")
		expectBytes(t, "C6: "+name+" sends 1011", rc.written(), failWire(CloseInternalError))
	}
	nc, _ := newRecServerConn(nil, peerClose(1005, ""))
	checkErr(t, "C6: CloseNoStatus with a reason", nc.Close(CloseNoStatus, "why"), ErrInvalidArgument, "websocket: invalid argument: close reason")
	c, rc := newRecServerConn(nil, peerClose(1000, ""))
	if err := c.Close(CloseNormal, strings.Repeat("r", 123)); err != nil {
		t.Error(test.DiffMessage(err, nil, "C6: a reason of 123 bytes is valid"))
	}
	expectBytes(t, "C6: 123 bytes of reason", rc.written(), serverFrame(testOpClose, true, closePayload(1000, strings.Repeat("r", 123))))
}

func TestConn_Close_NoStatusSendsEmptyPayload(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpClose, true, nil))
	if err := c.Close(CloseNoStatus, ""); err != nil {
		t.Fatal(test.DiffMessage(err, nil, "C6"))
	}
	expectBytes(t, "C6: CloseNoStatus is an empty payload", rc.written(), []byte{0x88, 0x00})
}

func TestConn_Close_Twice(t *testing.T) {
	c, rc := newRecServerConn(nil, peerClose(1000, ""))
	if err := c.Close(CloseNormal, ""); err != nil {
		t.Fatal(err)
	}
	before := rc.written()
	checkErr(t, "C8: a second Close", c.Close(CloseNormal, ""), ErrClosed, "")
	expectBytes(t, "C8: nothing written", rc.written(), before)
}

func TestConn_CloseDuringBlockedRead(t *testing.T) {
	c, peer := newPipeConn(t, false, connOpts(func(o *ConnOptions) { o.CloseTimeout = 2 * time.Second }))
	started := make(chan struct{})
	readErr := make(chan error, 1)
	go func() {
		close(started)
		_, _, err := c.ReadMessage(nil)
		readErr <- err
	}()
	<-started
	got := answerClose(peer, peerClose(1000, ""))
	if err := c.Close(CloseNormal, ""); err != nil {
		t.Error(test.DiffMessage(err, nil, "C7: Close returns nil when the peer answers"))
	}
	if err := <-readErr; !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "C7: the blocked read returns ErrClosed"))
	}
	<-got
}

func TestConn_CloseDuringBlockedRead_PeerSilent(t *testing.T) {
	c, peer := newPipeConn(t, false, connOpts(func(o *ConnOptions) { o.CloseTimeout = 50 * time.Millisecond }))
	started := make(chan struct{})
	readErr := make(chan error, 1)
	go func() {
		close(started)
		_, _, err := c.ReadMessage(nil)
		readErr <- err
	}()
	<-started
	got := answerClose(peer, nil)
	var err error
	took := elapsed(func() { err = c.Close(CloseNormal, "") })
	checkErr(t, "C7: a silent peer", err, ErrTimeout, "")
	if err := <-readErr; !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "C7: the blocked read returns ErrClosed no later than CloseTimeout"))
	}
	if took > 5*time.Second {
		t.Error(test.DiffMessage(took, "under WriteTimeout + CloseTimeout", "C7"))
	}
	<-got
}

func TestConn_Abort_Idempotent(t *testing.T) {
	c, rc := newRecServerConn(nil)
	c.Abort()
	c.Abort()
	if rc.closes() != 1 {
		t.Error(test.DiffMessage(rc.closes(), 1, "C9, L7: Abort closes the transport exactly once"))
	}
	if len(rc.written()) != 0 {
		t.Error(test.DiffMessage(rc.written(), []byte(nil), "C9: an abort writes no close frame"))
	}
	if _, _, err := c.ReadMessage(nil); !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "C9"))
	}
}

func TestConn_AbortDuringBlockedReadAndWrite(t *testing.T) {
	c, _ := newPipeConn(t, false, connOpts(func(o *ConnOptions) {
		o.WriteTimeout = time.Minute
		o.IdleTimeout = time.Minute
		o.ReadTimeout = time.Minute
	}))
	started := make(chan struct{}, 2)
	readErr := make(chan error, 1)
	writeErr := make(chan error, 1)
	go func() {
		started <- struct{}{}
		_, _, err := c.ReadMessage(nil)
		readErr <- err
	}()
	go func() {
		started <- struct{}{}
		writeErr <- c.WriteMessage(KindBinary, make([]byte, 1<<20))
	}()
	<-started
	<-started
	c.Abort()
	if err := <-readErr; !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "C9: a blocked read returns ErrClosed"))
	}
	if err := <-writeErr; !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "C9: a blocked write returns ErrClosed"))
	}
}

func checkEverythingClosed(t *testing.T, rule string, c *Conn, rc *recConn, r io.Reader, w io.WriteCloser) {
	t.Helper()
	before := rc.written()
	pm, err := NewPreparedMessage(KindBinary, []byte("p"))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = c.ReadMessage(nil)
	checkErr(t, rule+": ReadMessage", err, ErrClosed, "")
	_, _, err = c.NextReader()
	checkErr(t, rule+": NextReader", err, ErrClosed, "")
	checkErr(t, rule+": WriteMessage", c.WriteMessage(KindText, []byte("x")), ErrClosed, "")
	_, err = c.NextWriter(KindText)
	checkErr(t, rule+": NextWriter", err, ErrClosed, "")
	checkErr(t, rule+": WritePrepared", c.WritePrepared(pm), ErrClosed, "")
	checkErr(t, rule+": Ping", c.Ping([]byte("x")), ErrClosed, "")
	checkErr(t, rule+": Close", c.Close(CloseNormal, ""), ErrClosed, "")
	if r != nil {
		_, err = r.Read(make([]byte, 4))
		checkErr(t, rule+": reader Read", err, ErrClosed, "")
	}
	if w != nil {
		_, err = w.Write([]byte("x"))
		checkErr(t, rule+": writer Write", err, ErrClosed, "")
		checkErr(t, rule+": writer Close", w.Close(), ErrClosed, "")
	}
	expectBytes(t, rule+": a closed connection touches nothing", rc.written(), before)
}

func TestConn_AfterClose_EverythingReturnsErrClosed(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpText, false, []byte("hi")), encodeTestFrame(testFrame{Fin: true, Rsv: 0x40, Opcode: testOpText, Masked: true}, testMaskKey))
	w, err := c.NextWriter(KindText)
	if err != nil {
		t.Fatal(err)
	}
	_, r, err := c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(r)
	checkErr(t, "X1: the call that observed the cause returns the cause", err, ErrProtocol, "")
	checkEverythingClosed(t, "X1 after a fail", c, rc, r, w)

	c, rc = newRecServerConn(nil, clientFrame(testOpText, true, []byte("hi")))
	w, err = c.NextWriter(KindText)
	if err != nil {
		t.Fatal(err)
	}
	_, r, err = c.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	c.Abort()
	checkEverythingClosed(t, "X1 after Abort", c, rc, r, w)

	c, rc = newRecServerConn(nil, peerClose(1000, ""))
	if err := c.Close(CloseNormal, ""); err != nil {
		t.Fatal(err)
	}
	checkEverythingClosed(t, "X1 after Close", c, rc, nil, nil)
}

func TestConn_Close_ClosesNetConnOnce(t *testing.T) {
	c, rc := newRecServerConn(nil, peerClose(1000, ""))
	if err := c.Close(CloseNormal, ""); err != nil {
		t.Fatal(err)
	}
	c.Abort()
	c.Abort()
	if rc.closes() != 1 {
		t.Error(test.DiffMessage(rc.closes(), 1, "L7: Close then Abort"))
	}
	c, rc = newRecServerConn(nil, encodeTestFrame(testFrame{Fin: true, Rsv: 0x40, Opcode: testOpText, Masked: true}, testMaskKey))
	_, _, _ = c.ReadMessage(nil)
	c.Abort()
	if rc.closes() != 1 {
		t.Error(test.DiffMessage(rc.closes(), 1, "L7: a fail then Abort"))
	}
}

func TestConn_Close_SimultaneousWithPeerCloseSendsOneCloseFrame(t *testing.T) {
	for iter := range 40 {
		a, b := net.Pipe()
		c := newConn(a, bufio.NewReaderSize(a, 4096), false, connOpts(func(o *ConnOptions) {
			o.CloseTimeout = 5 * time.Second
			o.WriteTimeout = 5 * time.Second
		}))
		collected := collectAll(b)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			_, _ = b.Write(peerClose(1000, ""))
		}()
		go func() {
			defer wg.Done()
			_, _, _ = c.ReadMessage(nil)
		}()
		go func() {
			defer wg.Done()
			_ = c.Close(CloseNormal, "")
		}()
		wg.Wait()
		_ = a.Close()
		frames, err := parseTestFrames(<-collected, false)
		_ = b.Close()
		if err != nil {
			t.Fatal(test.DiffMessage(err, nil, "C10: iteration "+itoa(iter)+": frames parse"))
		}
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
			t.Fatal(test.DiffMessage(closes, 1, "C10, T22: close frames sent, iteration "+itoa(iter)))
		}
	}
}

func TestConn_Read_FailBehindBlockedWriteAbortsWithinCloseTimeout(t *testing.T) {
	bad := encodeTestFrame(testFrame{Fin: true, Rsv: 0x40, Opcode: testOpText, Masked: true}, testMaskKey)
	rc := newRecConn(bad)
	rc.blockWrite = make(chan struct{})
	rc.entered = make(chan struct{}, 1)
	c := newConn(rc, bufio.NewReaderSize(rc, 4096), false, connOpts(func(o *ConnOptions) { o.CloseTimeout = 100 * time.Millisecond }))
	writeErr := make(chan error, 1)
	go func() { writeErr <- c.WriteMessage(KindBinary, []byte("blocked")) }()
	<-rc.entered
	_, _, err := c.ReadMessage(nil)
	checkErr(t, "T22: a fail never waits behind a blocked write", err, ErrProtocol, "websocket: protocol error: reserved bit set")
	if rc.closes() != 1 {
		t.Error(test.DiffMessage(rc.closes(), 1, "T22: the transport is closed"))
	}
	if len(rc.written()) != 0 {
		t.Error(test.DiffMessage(rc.written(), []byte(nil), "T22: no close frame when another write holds the lock"))
	}
	if err := <-writeErr; err == nil {
		t.Error(test.DiffMessage(err, "an error", "T22: the blocked write is freed by the close"))
	}
}

func TestConn_Abort_TLSNeverBlocks(t *testing.T) {
	u := mustUpgrader(t, &UpgradeOptions{CheckOrigin: allowAll, Conn: connOpts(func(o *ConnOptions) { o.WriteTimeout = time.Minute })})
	results := make(chan *Conn, 1)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := u.Upgrade(w, r)
		if err != nil {
			results <- nil
			return
		}
		results <- c
	}))
	srv.StartTLS()
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	tc, err := tls.Dial("tcp", srv.Listener.Addr().String(), &tls.Config{RootCAs: pool, ServerName: "example.com"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tc.Close() })
	req := "GET /ws HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := tc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(tc)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	c := <-results
	if c == nil {
		t.Fatal("the TLS handshake was refused")
	}
	writeErr := make(chan error, 1)
	go func() { writeErr <- c.WriteMessage(KindBinary, make([]byte, 64<<20)) }()
	if _, err := br.ReadByte(); err != nil {
		t.Fatal(err)
	}
	took := elapsed(c.Abort)
	if took > 2*time.Second {
		t.Error(test.DiffMessage(took, "milliseconds", "C9, T24: Abort on TLS never waits for close_notify (5 s)"))
	}
	if err := <-writeErr; !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "C9: the blocked write returns ErrClosed"))
	}
}

func TestCloseError_FormatHidesReason(t *testing.T) {
	c, _ := newRecServerConn(nil, peerClose(3000, leakMarker+" reason"))
	_, _, err := c.ReadMessage(nil)
	ce := closeErrorOf(t, err)
	if ce.Reason() != leakMarker+" reason" {
		t.Error(test.DiffMessage(ce.Reason(), leakMarker+" reason", "E2: Reason returns the peer's text"))
	}
	want := "websocket: closed by peer: 3000"
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		got := fmt.Sprintf(verb, ce)
		if strings.Contains(got, leakMarker) || !strings.Contains(got, want) {
			t.Error(test.DiffMessage(got, want, "E2: verb "+verb+" prints Error() only"))
		}
		viaErr := fmt.Sprintf(verb, err)
		if strings.Contains(viaErr, leakMarker) {
			t.Error(test.DiffMessage(viaErr, want, "E2: verb "+verb+" on the returned error"))
		}
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		if got := fmt.Sprintf(verb, ce); got != want {
			t.Error(test.DiffMessage(got, want, "E2: verb "+verb+" equals Error()"))
		}
	}
}

func TestConn_Close_PeerOneByteCloseReturnsErrProtocol(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpClose, true, []byte{0x03}))
	err := c.Close(CloseNormal, "")
	checkErr(t, "C6: the peer answers with a close payload of one byte", err, ErrProtocol, "websocket: protocol error: close payload of one byte")
	expectBytes(t, "C10: only Close's own frame is sent", rc.written(), []byte{0x88, 0x02, 0x03, 0xe8})
	if rc.closes() != 1 {
		t.Error(test.DiffMessage(rc.closes(), 1, "C6: the transport is closed once"))
	}
}
