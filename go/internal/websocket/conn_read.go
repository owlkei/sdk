package websocket

import "io"

type messageReader struct {
	c   *Conn
	gen uint64
}

// ReadMessage blocks until one whole message has arrived, appends it to buf[:0] and returns its
// kind and the result. When cap(buf) is large enough the result aliases buf and nothing is
// allocated; otherwise the buffer grows with the bytes received, never past MaxMessageBytes.
// Pings are answered and pongs dropped on the way. On error it returns 0, buf[:0] and the error:
// a *CloseError when the peer closed, ErrClosed once the connection is closed.
func (c *Conn) ReadMessage(buf []byte) (Kind, []byte, error) {
	buf = buf[:0]
	c.readMu.Lock()
	kind, err := c.beginMessageLocked()
	for err == nil {
		if c.frameLeft == 0 {
			if !c.msgOpen {
				break
			}
			err = c.nextDataFrame()
			continue
		}
		if len(buf) == cap(buf) {
			buf = c.grow(buf)
		}
		var n int
		n, err = c.readPayloadLocked(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
	}
	c.readMu.Unlock()
	if err != nil {
		return 0, buf[:0], err
	}
	return kind, buf, nil
}

// NextReader blocks until the first frame of the next message and returns its kind and a reader
// of that message. Read returns the bytes available (at least one, or an error), crosses fragment
// boundaries and returns io.EOF after the last one; a text message is validated as it streams, so
// an error may follow bytes already returned. The reader belongs to that message only: after the
// next NextReader or ReadMessage, which discard what is left of it, its Read returns ErrClosed.
func (c *Conn) NextReader() (Kind, io.Reader, error) {
	c.readMu.Lock()
	kind, err := c.beginMessageLocked()
	gen := c.readGen
	c.readMu.Unlock()
	if err != nil {
		return 0, nil, err
	}
	return kind, &messageReader{c: c, gen: gen}, nil
}

// Remaining reports, for the message being read through NextReader, the payload bytes its frame
// headers have announced that Read has not returned, and whether the final frame's header has been
// read (then n is the whole rest of the message). Call it on the reading goroutine between Reads.
func (c *Conn) Remaining() (n int, isFinal bool) {
	c.readMu.Lock()
	n, isFinal = c.frameLeft, c.frameFin || !c.msgOpen
	c.readMu.Unlock()
	return n, isFinal
}

func (r *messageReader) Read(p []byte) (int, error) {
	c := r.c
	c.readMu.Lock()
	if c.readGen != r.gen || c.state.Load() == stateClosed {
		c.readMu.Unlock()
		return 0, ErrClosed
	}
	var n int
	var err error
	for len(p) > 0 && err == nil && c.frameLeft == 0 {
		if !c.msgOpen {
			err = io.EOF
			break
		}
		err = c.nextDataFrame()
	}
	if err == nil && len(p) > 0 {
		n, err = c.readPayloadLocked(p)
	}
	c.readMu.Unlock()
	return n, err
}

func (c *Conn) beginMessageLocked() (Kind, error) {
	c.readGen++
	if state := c.state.Load(); state != stateOpen {
		if state == stateClosing && !c.peerClosed && c.endErr == nil && c.discardLeftLocked() == nil {
			_ = c.nextDataFrame()
		}
		return 0, ErrClosed
	}
	for c.frameLeft > 0 || c.msgOpen {
		var err error
		if c.frameLeft > 0 {
			err = c.skipPayloadLocked()
		} else {
			err = c.nextDataFrame()
		}
		if err != nil {
			return 0, err
		}
	}
	if err := c.nextDataFrame(); err != nil {
		return 0, err
	}
	return c.msgKind, nil
}

func (c *Conn) nextDataFrame() error {
	for {
		h, err := c.readHeader()
		if err != nil {
			return err
		}
		if h.op >= opClose {
			if err := c.handleControl(h); err != nil {
				return err
			}
			continue
		}
		if c.state.Load() != stateOpen {
			if err := c.discardFrame(h); err != nil {
				return err
			}
			continue
		}
		if h.op == opContinuation && !c.msgOpen {
			return c.fail(errContinuation, CloseProtocolError)
		}
		if h.op != opContinuation && c.msgOpen {
			return c.fail(errDataInMessage, CloseProtocolError)
		}
		if h.op != opContinuation {
			c.msgOpen = true
			c.msgKind = Kind(h.op)
			c.msgBytes = 0
			c.msgFrags = 0
			c.utf8 = utf8State{}
		}
		if h.length > c.opts.MaxMessageBytes-c.msgBytes {
			return c.fail(errMessageTooBig, CloseMessageTooBig)
		}
		if c.msgFrags >= c.opts.MaxFragments {
			return c.fail(errTooManyFragments, CloseMessageTooBig)
		}
		c.msgFrags++
		c.msgBytes += h.length
		_, _ = c.br.Discard(h.size)
		c.frameLeft = h.length
		c.frameKey = h.key
		c.framePos = 0
		c.frameFin = h.fin
		c.frameMasked = h.masked
		if c.frameLeft == 0 {
			return c.frameEndLocked()
		}
		return nil
	}
}

func (c *Conn) readPayloadLocked(p []byte) (int, error) {
	if len(p) > c.frameLeft {
		p = p[:c.frameLeft]
	}
	if c.br.Buffered() == 0 {
		c.arm(c.msgDeadline)
	}
	n, err := c.br.Read(p)
	if n == 0 {
		return 0, c.readFailed(err)
	}
	p = p[:n]
	if c.frameMasked {
		c.framePos = maskBytes(p, c.frameKey, c.framePos)
	}
	c.frameLeft -= n
	if c.msgKind == KindText && !c.utf8.validate(p) {
		return 0, c.fail(ErrInvalidUTF8, CloseInvalidPayload)
	}
	if c.frameLeft == 0 {
		if err := c.frameEndLocked(); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (c *Conn) skipPayloadLocked() error {
	var tmp [256]byte
	for c.frameLeft > 0 {
		if c.br.Buffered() == 0 {
			c.arm(c.msgDeadline)
		}
		if _, err := c.br.Peek(1); err != nil {
			return c.readFailed(err)
		}
		n := min(c.br.Buffered(), c.frameLeft, len(tmp))
		b, _ := c.br.Peek(n)
		if c.msgKind == KindText {
			chunk := tmp[:copy(tmp[:], b)]
			if c.frameMasked {
				c.framePos = maskBytes(chunk, c.frameKey, c.framePos)
			}
			if !c.utf8.validate(chunk) {
				return c.fail(ErrInvalidUTF8, CloseInvalidPayload)
			}
		}
		_, _ = c.br.Discard(n)
		c.frameLeft -= n
	}
	return c.frameEndLocked()
}

func (c *Conn) discardFrame(h frameHeader) error {
	_, _ = c.br.Discard(h.size)
	c.frameLeft = h.length
	return c.discardLeftLocked()
}

func (c *Conn) discardLeftLocked() error {
	for c.frameLeft > 0 {
		if c.br.Buffered() == 0 {
			c.arm(c.msgDeadline)
		}
		n, err := c.br.Discard(c.frameLeft)
		c.frameLeft -= n
		if err != nil {
			return c.readFailed(err)
		}
	}
	return nil
}

func (c *Conn) frameEndLocked() error {
	if !c.frameFin {
		return nil
	}
	c.msgOpen = false
	if c.msgKind == KindText && !c.utf8.isComplete() {
		return c.fail(ErrInvalidUTF8, CloseInvalidPayload)
	}
	return nil
}

func (c *Conn) grow(buf []byte) []byte {
	limit := c.opts.MaxMessageBytes
	size := 4096
	if cur := cap(buf); cur >= size/2 {
		size = limit
		if cur < limit/2 {
			size = 2 * cur
		}
	}
	size = min(size, limit)
	grown := make([]byte, len(buf), size)
	copy(grown, buf)
	return grown
}
