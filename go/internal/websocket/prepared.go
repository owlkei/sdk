package websocket

import "unicode/utf8"

// PreparedMessage is a message encoded once as a server frame, to be written to any number of
// connections with WritePrepared. It is immutable and safe from any goroutine.
type PreparedMessage struct {
	kind  Kind
	frame []byte
	size  int
}

// NewPreparedMessage copies data once into an encoded frame of the given kind. A KindText
// payload is validated here: invalid UTF-8 returns ErrInvalidUTF8 and no message. Another kind
// than KindText or KindBinary returns ErrInvalidArgument.
func NewPreparedMessage(kind Kind, data []byte) (*PreparedMessage, error) {
	if kind != KindText && kind != KindBinary {
		return nil, errInvalidKind
	}
	if kind == KindText && !utf8.Valid(data) {
		return nil, ErrInvalidUTF8
	}
	var head [maxHeader]byte
	size := encodeHeader(head[:], finBit|byte(kind), len(data), false)
	frame := make([]byte, size+len(data))
	copy(frame, head[:size])
	copy(frame[size:], data)
	return &PreparedMessage{kind: kind, frame: frame, size: size}, nil
}

// WritePrepared writes m. On a server connection it writes the encoded frame as it is, the
// bytes WriteMessage would write; on a client connection the payload is sent masked with a fresh
// key. A nil or zero m returns ErrInvalidArgument. The rules of WriteMessage apply otherwise.
func (c *Conn) WritePrepared(m *PreparedMessage) error {
	if m == nil || m.frame == nil {
		return errInvalidPrepared
	}
	if c.state.Load() != stateOpen {
		return ErrClosed
	}
	c.writeMu.Lock()
	err := c.dataWriteAllowedLocked()
	if err == nil {
		if c.isClient {
			err = c.writeFrameLocked(finBit|byte(m.kind), m.frame[m.size:])
		} else {
			c.armWrite(c.opts.WriteTimeout)
			err = c.writeDone(c.write(m.frame))
		}
	}
	c.writeMu.Unlock()
	return err
}
