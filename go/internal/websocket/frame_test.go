package websocket

import (
	"bytes"
	"errors"
	"slices"
	"testing"
	"unicode/utf8"

	"go.owlkei.com/sdk/internal/test"
)

func bigOpts() *ConnOptions {
	return connOpts(func(o *ConnOptions) { o.MaxMessageBytes = 1 << 20 })
}

func TestConn_Read_HeaderSplitAtEveryByte(t *testing.T) {
	payload := repeatByte('x', 300)
	frame := clientFrame(testOpBinary, true, payload)
	for i := 1; i < len(frame); i++ {
		c, _ := newRecServerConn(bigOpts(), frame[:i], frame[i:])
		kind, data, err := c.ReadMessage(nil)
		if err != nil || kind != KindBinary || !bytes.Equal(data, payload) {
			t.Fatal(test.DiffMessage(err, nil, "F1: header split after byte "+itoa(i)))
		}
	}
	chunks := make([][]byte, len(frame))
	for i := range frame {
		chunks[i] = frame[i : i+1]
	}
	c, _ := newRecServerConn(bigOpts(), chunks...)
	kind, data, err := c.ReadMessage(nil)
	if err != nil || kind != KindBinary || !bytes.Equal(data, payload) {
		t.Error(test.DiffMessage(err, nil, "F1: a frame arriving one byte per read"))
	}
}

func readSized(t *testing.T, rule string, size int, opcode byte) {
	t.Helper()
	payload := repeatByte('p', size)
	c, _ := newRecServerConn(bigOpts(), clientFrame(opcode, true, payload))
	kind, data, err := c.ReadMessage(nil)
	if err != nil {
		t.Error(test.DiffMessage(err, nil, rule+": error for length "+itoa(size)))
		return
	}
	if kind != Kind(opcode) || !bytes.Equal(data, payload) {
		t.Error(test.DiffMessage(len(data), size, rule+": payload length"))
	}
}

func TestConn_Read_Length7Bit(t *testing.T) {
	for _, n := range []int{0, 1, 124, 125} {
		readSized(t, "F1: 7-bit length", n, testOpBinary)
	}
}

func TestConn_Read_Length16Bit(t *testing.T) {
	for _, n := range []int{126, 127, 1000, 65535} {
		readSized(t, "F1: 16-bit length", n, testOpBinary)
	}
}

func TestConn_Read_Length64Bit(t *testing.T) {
	for _, n := range []int{65536, 65537, 100000} {
		readSized(t, "F1: 64-bit length", n, testOpBinary)
	}
}

func TestConn_Read_ReservedBitsFail(t *testing.T) {
	for _, rsv := range []byte{0x40, 0x20, 0x10} {
		frame := encodeTestFrame(testFrame{Fin: true, Rsv: rsv, Opcode: testOpText, Masked: true, Payload: []byte("hi")}, testMaskKey)
		c, rc := newRecServerConn(nil, frame)
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "F2: RSV bit "+itoa(int(rsv)), rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: reserved bit set")
	}
}

func TestConn_Read_ReservedOpcodesFail(t *testing.T) {
	ops := []byte{3, 4, 5, 6, 7, 0xB, 0xC, 0xD, 0xE, 0xF}
	for _, op := range ops {
		c, rc := newRecServerConn(nil, clientFrame(op, true, nil))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "F2: reserved opcode "+itoa(int(op)), rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: reserved opcode")
	}
}

func TestConn_Read_UnmaskedClientFrameFails(t *testing.T) {
	c, rc := newRecServerConn(nil, serverFrame(testOpText, true, []byte("hi")))
	_, _, err := c.ReadMessage(nil)
	expectFail(t, "F2: unmasked client frame", rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: unmasked client frame")
}

func TestConn_Read_MaskedServerFrameFails(t *testing.T) {
	c, rc := newRecClientConn(nil, clientFrame(testOpText, true, []byte("hi")))
	_, _, err := c.ReadMessage(nil)
	checkErr(t, "F2: masked server frame", err, ErrProtocol, "websocket: protocol error: masked server frame")
	frames, perr := parseTestFrames(rc.written(), true)
	if perr != nil || len(frames) != 1 || frames[0].Opcode != testOpClose || !bytes.Equal(frames[0].Payload, []byte{0x03, 0xEA}) {
		t.Error(test.DiffMessage(frames, "one masked close frame with 1002", "F2: the client's close frame"))
	}
}

func TestConn_Read_NonMinimal16BitLengthFails(t *testing.T) {
	for _, n := range []uint64{0, 100, 125} {
		c, rc := newRecServerConn(nil, rawHeader(0x82, true, 2, n))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "F2: 16-bit length "+itoa(int(n)), rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: length is not minimal")
	}
}

func TestConn_Read_NonMinimal64BitLengthFails(t *testing.T) {
	for _, n := range []uint64{0, 125, 126, 65535} {
		c, rc := newRecServerConn(nil, rawHeader(0x82, true, 8, n))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "F2: 64-bit length "+itoa(int(n)), rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: length is not minimal")
	}
}

func TestConn_Read_LengthHighBitFails(t *testing.T) {
	for _, n := range []uint64{1 << 63, 1<<63 + 1, 1<<64 - 1} {
		c, rc := newRecServerConn(bigOpts(), rawHeader(0x82, true, 8, n))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "F2, T14: 64-bit length with the high bit set", rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: length out of range")
	}
}

func TestConn_Read_FragmentedControlFrameFails(t *testing.T) {
	for _, op := range []byte{testOpClose, testOpPing, testOpPong} {
		c, rc := newRecServerConn(nil, clientFrame(op, false, nil))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "F2: control frame "+itoa(int(op))+" without FIN", rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: fragmented control frame")
	}
}

func TestConn_Read_LongControlFrameFails(t *testing.T) {
	for _, op := range []byte{testOpClose, testOpPing, testOpPong} {
		c, rc := newRecServerConn(nil, rawHeader(0x80|op, true, 2, 126))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "F2: control frame "+itoa(int(op))+" of 126 bytes", rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: control frame too long")
	}
}

func TestConn_Read_ControlFrameOf125BytesPasses(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpPing, true, repeatByte('q', 125)), clientFrame(testOpText, true, []byte("ok")))
	_, data, err := c.ReadMessage(nil)
	if err != nil || string(data) != "ok" {
		t.Error(test.DiffMessage(err, nil, "F2: a 125-byte ping is legal"))
	}
	expectBytes(t, "C1: the pong of 125 bytes", rc.written(), serverFrame(testOpPong, true, repeatByte('q', 125)))
}

func TestConn_Read_FailsFromHeaderAlone(t *testing.T) {
	headers := map[string][]byte{
		"reserved bit":        {0xC1, 0x85, 1, 2, 3, 4},
		"reserved opcode":     {0x83, 0x85, 1, 2, 3, 4},
		"unmasked":            {0x81, 0x05},
		"non minimal 16":      rawHeader(0x82, true, 2, 5),
		"non minimal 64":      rawHeader(0x82, true, 8, 5),
		"high bit":            rawHeader(0x82, true, 8, 1<<63),
		"fragmented control":  {0x09, 0x85, 1, 2, 3, 4},
		"long control":        rawHeader(0x89, true, 2, 200),
		"continuation no msg": {0x80, 0x85, 1, 2, 3, 4},
	}
	for name, h := range headers {
		c, rc := newRecServerConn(nil, h)
		_, _, err := c.ReadMessage(nil)
		if err == nil || !isErr(err, ErrProtocol) {
			t.Error(test.DiffMessage(err, ErrProtocol, "F2: "+name+" is decided from the header alone, never from a missing payload"))
		}
		if got := rc.written(); !bytes.Equal(got, failWire(CloseProtocolError)) {
			t.Error(test.DiffMessage(got, failWire(CloseProtocolError), "F2: "+name+": close frame"))
		}
	}
}

func TestConn_Read_FrameOverLimitFailsBeforePayload(t *testing.T) {
	opts := connOpts(func(o *ConnOptions) {
		o.MaxMessageBytes = 1000
		o.MaxFrameBytes = 100
	})
	c, rc := newRecServerConn(opts, rawHeader(0x82, true, 0, 101))
	_, _, err := c.ReadMessage(nil)
	expectFail(t, "F3: frame of 101 over a limit of 100", rc, err, ErrMessageTooBig, CloseMessageTooBig, "websocket: message too big: frame over the limit")
	c, _ = newRecServerConn(opts, clientFrame(testOpBinary, true, repeatByte('a', 100)))
	if _, data, err := c.ReadMessage(nil); err != nil || len(data) != 100 {
		t.Error(test.DiffMessage(err, nil, "F3: a frame exactly at the limit passes"))
	}
}

func TestConn_Read_BoundaryLengths(t *testing.T) {
	small := []uint64{0, 125, 126, 65535, 65536}
	for _, n := range small {
		enc := 8
		switch {
		case n <= 125:
			enc = 0
		case n <= 65535:
			enc = 2
		}
		payload := bytes.Repeat([]byte{'b'}, int(n))
		okFrame := append(rawHeader(0x82, true, enc, n), maskedPayload(payload)...)
		c, _ := newRecServerConn(connOpts(func(o *ConnOptions) { o.MaxMessageBytes = int(n) + 1 }), okFrame)
		if _, data, err := c.ReadMessage(nil); err != nil || len(data) != int(n) {
			t.Error(test.DiffMessage(err, nil, "F4: length "+itoa(int(n))+" accepted under a sufficient limit"))
		}
		if n == 0 {
			continue
		}
		over := connOpts(func(o *ConnOptions) { o.MaxMessageBytes = int(n) - 1 })
		c, rc := newRecServerConn(over, rawHeader(0x82, true, enc, n))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "F4, T2: length "+itoa(int(n))+" over its limit", rc, err, ErrMessageTooBig, CloseMessageTooBig, "")
	}
	for _, n := range []uint64{1 << 32, 1<<63 - 1} {
		c, rc := newRecServerConn(connOpts(func(o *ConnOptions) { o.MaxMessageBytes = 1 << 30 }), rawHeader(0x82, true, 8, n))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "F4, T14: huge length", rc, err, ErrMessageTooBig, CloseMessageTooBig, "websocket: message too big: frame over the limit")
	}
	for _, n := range []uint64{1 << 63, 1<<64 - 1} {
		c, rc := newRecServerConn(bigOpts(), rawHeader(0x82, true, 8, n))
		_, _, err := c.ReadMessage(nil)
		expectFail(t, "F4: length with the high bit set", rc, err, ErrProtocol, CloseProtocolError, "websocket: protocol error: length out of range")
	}
}

func maskedPayload(p []byte) []byte {
	out := make([]byte, len(p))
	for i, c := range p {
		out[i] = c ^ testMaskKey[i&3]
	}
	return out
}

func TestConn_Read_EOFBetweenFrames(t *testing.T) {
	c, rc := newRecServerConn(nil, clientFrame(testOpText, true, []byte("one")))
	if _, data, err := c.ReadMessage(nil); err != nil || string(data) != "one" {
		t.Fatal(test.DiffMessage(err, nil, "F6: the first message"))
	}
	_, _, err := c.ReadMessage(nil)
	checkErr(t, "F6: EOF between frames", err, ErrAbnormalClosure, "websocket: connection lost")
	if len(rc.written()) != 0 {
		t.Error(test.DiffMessage(rc.written(), []byte(nil), "F6: an abort writes no close frame"))
	}
	if rc.closes() != 1 {
		t.Error(test.DiffMessage(rc.closes(), 1, "F6: transport closed once"))
	}
	if _, _, err := c.ReadMessage(nil); !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "X1: after the abort"))
	}
}

func TestConn_Read_EOFInsideFrame(t *testing.T) {
	frame := clientFrame(testOpBinary, true, repeatByte('z', 50))
	for _, cut := range []int{1, 3, 7, 20, len(frame) - 1} {
		c, rc := newRecServerConn(nil, frame[:cut])
		_, _, err := c.ReadMessage(nil)
		checkErr(t, "F6: EOF after "+itoa(cut)+" bytes", err, ErrAbnormalClosure, "websocket: connection lost")
		if len(rc.written()) != 0 {
			t.Error(test.DiffMessage(rc.written(), []byte(nil), "F6: no close frame on a lost connection"))
		}
	}
}

type refOutcome struct {
	msgs []string
	end  string
	alt  string
}

func refReserved(op byte) bool {
	return op >= 3 && op <= 7 || op >= 0xB
}

func refValidCloseCode(c uint16) bool {
	return c >= 1000 && c <= 1003 || c >= 1007 && c <= 1013 || c >= 3000 && c <= 4999
}

func refParseStream(stream []byte, maxMsg, maxFrag int) refOutcome {
	var out refOutcome
	var cur []byte
	open := false
	var kind byte
	frags := 0
	pos := 0
	fail := func(end, alt string) refOutcome {
		out.end, out.alt = end, alt
		return out
	}
	for {
		rest := stream[pos:]
		if len(rest) == 0 {
			return fail("eof", "")
		}
		if len(rest) < 2 {
			return fail("eof", "")
		}
		b0, b1 := rest[0], rest[1]
		fin := b0&0x80 != 0
		op := b0 & 0x0f
		masked := b1&0x80 != 0
		l7 := uint64(b1 & 0x7f)
		ext := 0
		switch l7 {
		case 126:
			ext = 2
		case 127:
			ext = 8
		}
		headerLen := 2 + ext + 4
		altEOF := ""
		if len(rest) < headerLen {
			altEOF = "eof"
		}
		control := op >= 8
		if b0&0x70 != 0 || refReserved(op) || !masked {
			return fail("fail1002", altEOF)
		}
		if control && (!fin || l7 > 125) {
			return fail("fail1002", altEOF)
		}
		if len(rest) < headerLen {
			return fail("eof", "")
		}
		length := l7
		switch l7 {
		case 126:
			length = uint64(rest[2])<<8 | uint64(rest[3])
			if length < 126 {
				return fail("fail1002", "")
			}
		case 127:
			length = 0
			for i := range 8 {
				length = length<<8 | uint64(rest[2+i])
			}
			if length>>63 != 0 || length < 65536 {
				return fail("fail1002", "")
			}
		}
		key := rest[headerLen-4 : headerLen]
		avail := uint64(len(rest) - headerLen)
		take := min(length, avail)
		payload := make([]byte, take)
		for i := range payload {
			payload[i] = rest[headerLen+i] ^ key[i&3]
		}
		if control {
			if avail < length {
				return fail("eof", "")
			}
			pos += headerLen + int(length)
			switch op {
			case testOpClose:
				switch length {
				case 0:
					out.end = "close1005"
					return out
				case 1:
					return fail("fail1002", "")
				}
				code := uint16(payload[0])<<8 | uint16(payload[1])
				if !refValidCloseCode(code) {
					alt := ""
					if !utf8.Valid(payload[2:]) {
						alt = "fail1007"
					}
					return fail("fail1002", alt)
				}
				if !utf8.Valid(payload[2:]) {
					return fail("fail1007", "")
				}
				out.end = "close" + itoa(int(code))
				return out
			}
			continue
		}
		stateBad := op == 0 && !open || op != 0 && open
		tooBig := length > uint64(maxMsg) || length > uint64(maxMsg-len(cur)) || frags+1 > maxFrag
		if stateBad {
			alt := ""
			if tooBig {
				alt = "fail1009"
			}
			return fail("fail1002", alt)
		}
		if tooBig {
			return fail("fail1009", "")
		}
		if op != 0 {
			open = true
			kind = op
			cur = cur[:0]
			frags = 0
		}
		frags++
		cur = append(cur, payload...)
		if kind == testOpText && !viableUTF8Prefix(cur) {
			return fail("fail1007", "")
		}
		if avail < length {
			return fail("eof", "")
		}
		pos += headerLen + int(length)
		if fin {
			if kind == testOpText && !utf8.Valid(cur) {
				return fail("fail1007", "")
			}
			out.msgs = append(out.msgs, string(rune('0'+kind))+string(cur))
			open = false
			cur = nil
			frags = 0
		}
	}
}

func frameReaderOutcome(stream []byte) ([]string, string) {
	opts := &ConnOptions{MaxMessageBytes: 1024, MaxFrameBytes: 1024, MaxFragments: 8, MaxPingsPerSecond: 1 << 30}
	c, _ := newRecServerConn(opts, stream)
	var msgs []string
	for range len(stream) + 2 {
		kind, data, err := c.ReadMessage(nil)
		if err == nil {
			msgs = append(msgs, string(rune('0'+kind))+string(data))
			continue
		}
		var ce *CloseError
		switch {
		case errors.As(err, &ce):
			return msgs, "close" + itoa(int(ce.Code()))
		case errors.Is(err, ErrAbnormalClosure):
			return msgs, "eof"
		case errors.Is(err, ErrProtocol):
			return msgs, "fail1002"
		case errors.Is(err, ErrInvalidUTF8):
			return msgs, "fail1007"
		case errors.Is(err, ErrMessageTooBig):
			return msgs, "fail1009"
		}
		return msgs, "other"
	}
	return msgs, "no end"
}

func checkFrameReaderStream(t testing.TB, stream []byte) {
	t.Helper()
	want := refParseStream(stream, 1024, 8)
	msgs, end := frameReaderOutcome(stream)
	if !slices.Equal(msgs, want.msgs) {
		t.Fatal(test.DiffMessage(msgs, want.msgs, "T21: messages read from the stream"))
	}
	if end != want.end && end != want.alt {
		t.Fatal(test.DiffMessage(end, want.end+" or "+want.alt, "T21: how the stream ends"))
	}
}

func frameReaderSeeds() [][]byte {
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	return [][]byte{
		nil,
		clientFrame(testOpText, true, []byte("hello")),
		cat(clientFrame(testOpText, false, []byte("he")), clientFrame(testOpPing, true, []byte("p")), clientFrame(testOpCont, true, []byte("llo"))),
		cat(clientFrame(testOpBinary, true, []byte{1, 2}), clientFrame(testOpClose, true, closePayload(1000, "bye"))),
		clientFrame(testOpClose, true, nil),
		clientFrame(testOpClose, true, []byte{3}),
		clientFrame(testOpClose, true, closePayload(1005, "")),
		clientFrame(testOpClose, true, closePayload(1000, "\xff")),
		clientFrame(testOpText, true, []byte("\xed\xa0\x80")),
		clientFrame(testOpText, true, []byte("ab\xc3")),
		cat(clientFrame(testOpText, false, []byte("\xc3")), clientFrame(testOpCont, true, []byte("\xa9"))),
		serverFrame(testOpText, true, []byte("unmasked")),
		encodeTestFrame(testFrame{Fin: true, Rsv: 0x40, Opcode: testOpText, Masked: true}, testMaskKey),
		clientFrame(3, true, nil),
		clientFrame(testOpCont, true, []byte("x")),
		cat(clientFrame(testOpText, false, []byte("a")), clientFrame(testOpText, true, []byte("b"))),
		clientFrame(testOpPing, false, nil),
		rawHeader(0x89, true, 2, 126),
		rawHeader(0x82, true, 2, 100),
		rawHeader(0x82, true, 8, 100),
		rawHeader(0x82, true, 8, 1<<63),
		rawHeader(0x82, true, 8, 1<<32),
		rawHeader(0x82, true, 2, 2000),
		cat(rawHeader(0x82, true, 0, 50), []byte("short")),
		clientFrame(testOpBinary, true, repeatByte('x', 1024)),
		clientFrame(testOpBinary, true, repeatByte('x', 1025)),
		cat(clientFrame(testOpBinary, false, repeatByte('x', 1000)), clientFrame(testOpCont, true, repeatByte('x', 25))),
		cat(clientFrame(testOpText, false, nil), bytes.Repeat(clientFrame(testOpCont, false, nil), 8), clientFrame(testOpCont, true, nil)),
		{0x81},
		{0x81, 0x85, 1, 2},
	}
}

func FuzzFrameReader(f *testing.F) {
	for _, s := range frameReaderSeeds() {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, stream []byte) {
		checkFrameReaderStream(t, stream)
	})
}

func TestConn_Read_StreamsMatchReferenceParser(t *testing.T) {
	for _, s := range frameReaderSeeds() {
		checkFrameReaderStream(t, s)
	}
	r := newTestRand()
	seeds := frameReaderSeeds()
	for range 3000 {
		s := append([]byte(nil), seeds[r.IntN(len(seeds))]...)
		if len(s) > 0 {
			switch r.IntN(3) {
			case 0:
				s[r.IntN(len(s))] = byte(r.IntN(256))
			case 1:
				s = s[:r.IntN(len(s)+1)]
			default:
				s = append(s, seeds[r.IntN(len(seeds))]...)
			}
		}
		checkFrameReaderStream(t, s)
	}
}
