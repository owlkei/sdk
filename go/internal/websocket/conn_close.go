package websocket

import (
	"encoding/binary"
	"time"
	"unicode/utf8"
)

const maxCloseReason = maxControl - 2

func (c *Conn) handleControl(h frameHeader) error {
	b, err := c.peek(h.size + h.length)
	if err != nil {
		return err
	}
	payload := b[h.size:]
	switch h.op {
	case opPing:
		if c.state.Load() == stateOpen && c.allowPong() {
			c.writeMu.Lock()
			err = c.writePongLocked(payload, h.key, h.masked)
			c.writeMu.Unlock()
		}
		_, _ = c.br.Discard(h.size + h.length)
		return err
	case opPong:
		_, _ = c.br.Discard(h.size + h.length)
		return nil
	}
	var p [maxControl]byte
	n := copy(p[:], payload)
	_, _ = c.br.Discard(h.size + h.length)
	if h.masked {
		maskBytes(p[:n], h.key, 0)
	}
	code := CloseNoStatus
	switch {
	case n == 1:
		return c.fail(errCloseOneByte, CloseProtocolError)
	case n >= 2:
		code = CloseCode(binary.BigEndian.Uint16(p[:2]))
		if !isValidCloseCode(code) {
			return c.fail(errInvalidCloseCode, CloseProtocolError)
		}
		if !utf8.Valid(p[2:n]) {
			return c.fail(ErrInvalidUTF8, CloseInvalidPayload)
		}
	}
	return c.onPeerClose(code, p[2:max(n, 2)])
}

func (c *Conn) allowPong() bool {
	now := time.Now()
	if c.pingWindow.IsZero() || now.Sub(c.pingWindow) >= time.Second {
		c.pingWindow = now
		c.pingCount = 0
	}
	if c.pingCount >= c.opts.MaxPingsPerSecond {
		return false
	}
	c.pingCount++
	return true
}

func (c *Conn) onPeerClose(code CloseCode, reason []byte) error {
	if !c.state.CompareAndSwap(stateOpen, stateClosing) {
		c.peerClosed = true
		return ErrClosed
	}
	ce := &CloseError{code: code, reason: string(reason)}
	c.writeMu.Lock()
	_ = c.writeCloseLocked(code, "", c.opts.WriteTimeout)
	c.writeMu.Unlock()
	if c.isClient {
		c.awaitTransportEnd(time.Now().Add(c.opts.CloseTimeout))
	}
	c.shutdown()
	return ce
}

func (c *Conn) awaitTransportEnd(deadline time.Time) {
	_ = c.netConn.SetReadDeadline(deadline)
	for {
		if _, err := c.br.Peek(1); err != nil {
			return
		}
		_, _ = c.br.Discard(c.br.Buffered())
	}
}

func (c *Conn) fail(err error, code CloseCode) error {
	c.endErr = err
	if c.state.CompareAndSwap(stateOpen, stateClosing) && c.writeMu.TryLock() {
		_ = c.writeCloseLocked(code, "", c.opts.CloseTimeout)
		c.writeMu.Unlock()
	}
	c.shutdown()
	return err
}

func (c *Conn) shutdown() {
	if c.state.Swap(stateClosed) != stateClosed {
		_ = c.closer.Close()
	}
}

// Close runs the closing handshake: it writes one close frame with code and reason, waits at
// most CloseTimeout for the peer's close frame (frames arriving meanwhile are discarded, pings
// unanswered), then closes the transport. A read blocked in another goroutine, or started while
// Close runs, returns ErrClosed once the peer's close arrives, the transport fails or CloseTimeout
// passes.
//
// It returns nil when the peer's close arrived, ErrTimeout when it did not, ErrAbnormalClosure
// when the transport failed first, ErrProtocol or ErrInvalidUTF8 when the peer's close frame was
// invalid (a payload of one byte, a code outside the valid set, a reason that is not UTF-8), and
// ErrClosed when the connection was already closing or closed. It blocks at most WriteTimeout
// (waiting for a data frame being written) + WriteTimeout (its own close frame) + CloseTimeout,
// 25 s with the defaults. Valid arguments are a code of 1000-1003, 1007-1013 or 3000-4999 with a UTF-8 reason of
// at most 123 bytes, or CloseNoStatus with an empty reason. Other arguments still close the
// connection, with code 1011 and no reason, and return ErrInvalidArgument.
func (c *Conn) Close(code CloseCode, reason string) error {
	argErr := checkCloseArgs(code, reason)
	if argErr != nil {
		code, reason = CloseInternalError, ""
	}
	if !c.state.CompareAndSwap(stateOpen, stateClosing) {
		return ErrClosed
	}
	c.writeMu.Lock()
	err := c.writeCloseLocked(code, reason, c.opts.WriteTimeout)
	c.writeMu.Unlock()
	if err != nil {
		c.shutdown()
		return err
	}
	deadline := time.Now().Add(c.opts.CloseTimeout)
	c.closeDeadline.Store(deadline.UnixNano())
	_ = c.netConn.SetReadDeadline(deadline)
	c.readMu.Lock()
	err = c.finishCloseLocked(deadline)
	c.readMu.Unlock()
	c.shutdown()
	if argErr != nil {
		return argErr
	}
	return err
}

func (c *Conn) finishCloseLocked(deadline time.Time) error {
	if !c.peerClosed && c.endErr == nil && c.state.Load() == stateClosing {
		if c.discardLeftLocked() == nil {
			_ = c.nextDataFrame()
		}
	}
	if c.peerClosed {
		if c.isClient {
			c.awaitTransportEnd(deadline)
		}
		return nil
	}
	if c.endErr != nil {
		return c.endErr
	}
	return ErrClosed
}

func checkCloseArgs(code CloseCode, reason string) error {
	if code == CloseNoStatus {
		if reason != "" {
			return errCloseReason
		}
		return nil
	}
	if !isValidCloseCode(code) {
		return errCloseCode
	}
	if len(reason) > maxCloseReason || !utf8.ValidString(reason) {
		return errCloseReason
	}
	return nil
}

func isValidCloseCode(code CloseCode) bool {
	return code >= 1000 && code <= 1003 || code >= 1007 && code <= 1013 || code >= 3000 && code <= 4999
}

// Abort closes the transport at once, with no close frame. It is safe from any goroutine, never
// blocks and can be called any number of times; blocked reads and writes return ErrClosed. On
// TLS it closes the underlying connection directly, so no close_notify is sent.
func (c *Conn) Abort() {
	c.shutdown()
}
