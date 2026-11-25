package websocket

import (
	"bytes"
	"testing"

	"go.owlkei.com/sdk/internal/test"
)

func TestNewPreparedMessage_InvalidKind(t *testing.T) {
	for _, k := range []Kind{0, 3, 9} {
		pm, err := NewPreparedMessage(k, []byte("x"))
		checkErr(t, "W4: kind "+itoa(int(k)), err, ErrInvalidArgument, "")
		if pm != nil {
			t.Error(test.DiffMessage(pm, nil, "W4: no message for an invalid kind"))
		}
	}
}

func TestNewPreparedMessage_InvalidUTF8Text(t *testing.T) {
	for _, bad := range []string{"\xff", "ok\xc0\x80", "\xed\xa0\x80", "ab\xe1\xbb"} {
		pm, err := NewPreparedMessage(KindText, []byte(bad))
		checkErr(t, "W4: invalid text", err, ErrInvalidUTF8, "websocket: invalid UTF-8")
		if pm != nil {
			t.Error(test.DiffMessage(pm, nil, "W4: no message for invalid text"))
		}
	}
	if pm, err := NewPreparedMessage(KindText, []byte("Tiếng Việt")); err != nil || pm == nil {
		t.Error(test.DiffMessage(err, nil, "W4: valid text"))
	}
}

func TestNewPreparedMessage_BinaryNotValidated(t *testing.T) {
	pm, err := NewPreparedMessage(KindBinary, []byte{0xff, 0xfe, 0xc0})
	if err != nil || pm == nil {
		t.Error(test.DiffMessage(err, nil, "W4: binary is not validated"))
	}
}

func TestConn_WritePrepared_SameBytesAsWriteMessage(t *testing.T) {
	r := newTestRand()
	for _, n := range []int{0, 1, 125, 126, 4000, 65535, 70000} {
		for _, kind := range []Kind{KindText, KindBinary} {
			data := asciiPayload(r, n)
			pm, err := NewPreparedMessage(kind, data)
			if err != nil {
				t.Fatal(err)
			}
			a, rcA := newRecServerConn(nil)
			b, rcB := newRecServerConn(nil)
			if err := a.WriteMessage(kind, data); err != nil {
				t.Fatal(err)
			}
			if err := b.WritePrepared(pm); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(rcA.written(), rcB.written()) {
				t.Error(test.DiffMessage(len(rcB.written()), len(rcA.written()), "W4, P5: WritePrepared writes the bytes of WriteMessage, length "+itoa(n)))
			}
			expectBytes(t, "W4: the bytes equal the independent encoder", rcB.written(), serverFrame(byte(kind), true, data))
		}
	}
}

func TestConn_WritePrepared_CopiesData(t *testing.T) {
	data := []byte("original payload")
	pm, err := NewPreparedMessage(KindBinary, data)
	if err != nil {
		t.Fatal(err)
	}
	copy(data, "MUTATED!!!!!!!!!")
	c, rc := newRecServerConn(nil)
	if err := c.WritePrepared(pm); err != nil {
		t.Fatal(err)
	}
	expectBytes(t, "W4: the message holds its own copy", rc.written(), serverFrame(testOpBinary, true, []byte("original payload")))
}

func TestConn_WritePrepared_ClientMasks(t *testing.T) {
	pm, err := NewPreparedMessage(KindText, []byte("prepared for a client"))
	if err != nil {
		t.Fatal(err)
	}
	c, rc := newRecClientConn(nil)
	if err := c.WritePrepared(pm); err != nil {
		t.Fatal(err)
	}
	frames, perr := parseTestFrames(rc.written(), true)
	if perr != nil || len(frames) != 1 || string(frames[0].Payload) != "prepared for a client" || frames[0].Opcode != testOpText {
		t.Error(test.DiffMessage(perr, nil, "W4: a client sends a prepared message masked"))
	}
	if bytes.Contains(rc.written(), []byte("prepared for a client")) {
		t.Error(test.DiffMessage("plain payload", "masked payload", "W4: client masking"))
	}
}

func TestConn_WritePrepared_NilAndZero(t *testing.T) {
	c, rc := newRecServerConn(nil)
	checkErr(t, "W4, T26: nil", c.WritePrepared(nil), ErrInvalidArgument, "websocket: invalid argument: prepared message")
	checkErr(t, "W4, T26: zero value", c.WritePrepared(&PreparedMessage{}), ErrInvalidArgument, "websocket: invalid argument: prepared message")
	if len(rc.written()) != 0 {
		t.Error(test.DiffMessage(rc.written(), []byte(nil), "W4: nothing written"))
	}
	if err := c.WriteMessage(KindText, []byte("still ok")); err != nil {
		t.Error(test.DiffMessage(err, nil, "W4: the connection stays usable"))
	}
}
