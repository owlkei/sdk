package websocket

import (
	"encoding/binary"
	"time"
)

const (
	opContinuation byte = 0x0
	opBinary       byte = 0x2
	opClose        byte = 0x8
	opPing         byte = 0x9
	opPong         byte = 0xa

	finBit     byte = 0x80
	maskBit    byte = 0x80
	maxControl      = 125
	maxHeader       = 14
)

type frameHeader struct {
	fin    bool
	op     byte
	masked bool
	key    [4]byte
	length int
	size   int
}

func (c *Conn) readHeader() (frameHeader, error) {
	var h frameHeader
	if !c.msgOpen {
		if c.br.Buffered() == 0 {
			c.arm(time.Now().Add(c.opts.IdleTimeout))
			if _, err := c.br.Peek(1); err != nil {
				return h, c.readFailed(err)
			}
		}
		c.msgDeadline = time.Now().Add(c.opts.ReadTimeout)
	}
	b, err := c.peek(2)
	if err != nil {
		return h, err
	}
	b0, b1 := b[0], b[1]
	h.fin = b0&finBit != 0
	h.op = b0 & 0x0f
	h.masked = b1&maskBit != 0
	if b0&0x70 != 0 {
		return h, c.fail(errReservedBit, CloseProtocolError)
	}
	if h.op > opBinary && h.op < opClose || h.op > opPong {
		return h, c.fail(errReservedOpcode, CloseProtocolError)
	}
	if !c.isClient && !h.masked {
		return h, c.fail(errUnmaskedFrame, CloseProtocolError)
	}
	if c.isClient && h.masked {
		return h, c.fail(errMaskedFrame, CloseProtocolError)
	}
	l7 := b1 & 0x7f
	if h.op >= opClose {
		if !h.fin {
			return h, c.fail(errFragmentedControl, CloseProtocolError)
		}
		if l7 > maxControl {
			return h, c.fail(errControlTooLong, CloseProtocolError)
		}
	}
	h.size = 2
	switch l7 {
	case 126:
		h.size += 2
	case 127:
		h.size += 8
	}
	if h.masked {
		h.size += 4
	}
	if b, err = c.peek(h.size); err != nil {
		return h, err
	}
	length := uint64(l7)
	switch l7 {
	case 126:
		length = uint64(binary.BigEndian.Uint16(b[2:]))
		if length < 126 {
			return h, c.fail(errNotMinimal, CloseProtocolError)
		}
	case 127:
		length = binary.BigEndian.Uint64(b[2:])
		if length>>63 != 0 {
			return h, c.fail(errLengthRange, CloseProtocolError)
		}
		if length < 1<<16 {
			return h, c.fail(errNotMinimal, CloseProtocolError)
		}
	}
	if length > maxMessageBytesCap {
		return h, c.fail(errFrameTooBig, CloseMessageTooBig)
	}
	h.length = int(length)
	if h.op < opClose && h.length > c.opts.MaxFrameBytes {
		return h, c.fail(errFrameTooBig, CloseMessageTooBig)
	}
	if h.masked {
		copy(h.key[:], b[h.size-4:h.size])
	}
	return h, nil
}

func (c *Conn) peek(n int) ([]byte, error) {
	if c.br.Buffered() < n {
		c.arm(c.msgDeadline)
	}
	b, err := c.br.Peek(n)
	if err != nil {
		return nil, c.readFailed(err)
	}
	return b, nil
}

func (c *Conn) readFailed(err error) error {
	switch c.state.Load() {
	case stateClosed:
		return ErrClosed
	case stateClosing:
		c.endErr = ErrAbnormalClosure
		if isTimeout(err) {
			c.endErr = ErrTimeout
		}
		return ErrClosed
	}
	c.shutdown()
	if isTimeout(err) {
		return ErrTimeout
	}
	return ErrAbnormalClosure
}
