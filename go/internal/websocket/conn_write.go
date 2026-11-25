package websocket

import (
	"crypto/rand"
	"encoding/binary"
	"io"
	"sync"
	"time"
)

const scratchSize = 4096

var scratchPool = sync.Pool{New: func() any { return new([scratchSize]byte) }}

type messageWriter struct {
	c   *Conn
	gen uint64
}

// WriteMessage writes data as one frame of the given kind (KindText or KindBinary), masked on a
// client connection. data is never modified. Text is not validated: sending valid UTF-8 is the
// caller's duty. It returns ErrWriteInProgress while a NextWriter message is open, ErrTimeout
// when the frame is not written within WriteTimeout and ErrAbnormalClosure on a transport error;
// both close the connection.
func (c *Conn) WriteMessage(kind Kind, data []byte) error {
	if kind != KindText && kind != KindBinary {
		return errInvalidKind
	}
	if c.state.Load() != stateOpen {
		return ErrClosed
	}
	c.writeMu.Lock()
	err := c.dataWriteAllowedLocked()
	if err == nil {
		err = c.writeFrameLocked(finBit|byte(kind), data)
	}
	c.writeMu.Unlock()
	return err
}

// NextWriter opens a streamed message of the given kind and returns its writer. Each Write of a
// non-empty slice writes one frame and returns after it is written; Close writes the final frame.
// While the writer is open, other data writes return ErrWriteInProgress; Ping and Close may still
// be called from any goroutine and their frames go between the fragments. The writer belongs to
// that message only: after its Close, or once the connection is closing, it returns ErrClosed.
func (c *Conn) NextWriter(kind Kind) (io.WriteCloser, error) {
	if kind != KindText && kind != KindBinary {
		return nil, errInvalidKind
	}
	if c.state.Load() != stateOpen {
		return nil, ErrClosed
	}
	c.writeMu.Lock()
	err := c.dataWriteAllowedLocked()
	if err == nil {
		c.writerOpen = true
		c.writerBegun = false
		c.writerKind = kind
		c.writeGen++
	}
	gen := c.writeGen
	c.writeMu.Unlock()
	if err != nil {
		return nil, err
	}
	return &messageWriter{c: c, gen: gen}, nil
}

func (w *messageWriter) Write(p []byte) (int, error) {
	c := w.c
	c.writeMu.Lock()
	err := c.writerUsableLocked(w.gen)
	if err == nil && len(p) > 0 {
		op := opContinuation
		if !c.writerBegun {
			op = byte(c.writerKind)
		}
		c.writerBegun = true
		err = c.writeFrameLocked(op, p)
	}
	c.writeMu.Unlock()
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *messageWriter) Close() error {
	c := w.c
	c.writeMu.Lock()
	err := c.writerUsableLocked(w.gen)
	if err == nil {
		op := opContinuation
		if !c.writerBegun {
			op = byte(c.writerKind)
		}
		c.writerOpen = false
		err = c.writeFrameLocked(finBit|op, nil)
	}
	c.writeMu.Unlock()
	return err
}

func (c *Conn) writerUsableLocked(gen uint64) error {
	if gen != c.writeGen || !c.writerOpen || c.state.Load() != stateOpen {
		return ErrClosed
	}
	return nil
}

func (c *Conn) dataWriteAllowedLocked() error {
	if c.state.Load() != stateOpen {
		return ErrClosed
	}
	if c.writerOpen {
		return ErrWriteInProgress
	}
	return nil
}

// Ping writes a ping frame holding payload (at most 125 bytes) from any goroutine, also between
// the fragments of a streamed message. The peer's pong is read and dropped by the reader.
func (c *Conn) Ping(payload []byte) error {
	if len(payload) > maxControl {
		return errPingTooLong
	}
	if c.state.Load() != stateOpen {
		return ErrClosed
	}
	c.writeMu.Lock()
	err := c.pingLocked(payload)
	c.writeMu.Unlock()
	return err
}

func (c *Conn) pingLocked(payload []byte) error {
	if c.state.Load() != stateOpen {
		return ErrClosed
	}
	return c.writeFrameLocked(finBit|opPing, payload)
}

func (c *Conn) putHeader(b []byte, b0 byte, n int) int {
	size := encodeHeader(b, b0, n, c.isClient)
	if c.isClient {
		_, _ = rand.Read(c.maskKey[:])
		copy(b[size:], c.maskKey[:])
		size += 4
	}
	return size
}

func (c *Conn) writeFrameLocked(b0 byte, p []byte) error {
	c.armWrite(c.opts.WriteTimeout)
	if len(p) <= maxControl {
		h := c.putHeader(c.wbuf[:], b0, len(p))
		n := copy(c.wbuf[h:], p)
		if c.isClient {
			maskBytes(c.wbuf[h:h+n], c.maskKey, 0)
		}
		return c.writeDone(c.write(c.wbuf[:h+n]))
	}
	if c.isVectored && !c.isClient && len(p) > scratchSize-maxHeader {
		h := c.putHeader(c.wbuf[:], b0, len(p))
		c.bufs[0], c.bufs[1] = c.wbuf[:h], p
		c.vec = c.bufs[:]
		_, err := c.vec.WriteTo(c.netConn)
		c.bufs[0], c.bufs[1] = nil, nil
		c.vec = nil
		return c.writeDone(err)
	}
	s := scratchPool.Get().(*[scratchSize]byte)
	h := c.putHeader(s[:], b0, len(p))
	var err error
	switch {
	case c.isClient:
		n := copy(s[h:], p)
		pos := maskBytes(s[h:h+n], c.maskKey, 0)
		err = c.write(s[:h+n])
		for p = p[n:]; err == nil && len(p) > 0; p = p[n:] {
			n = copy(s[:], p)
			pos = maskBytes(s[:n], c.maskKey, pos)
			err = c.write(s[:n])
		}
	case h+len(p) <= scratchSize:
		n := copy(s[h:], p)
		err = c.write(s[:h+n])
	default:
		if err = c.write(s[:h]); err == nil {
			err = c.write(p)
		}
	}
	scratchPool.Put(s)
	return c.writeDone(err)
}

func (c *Conn) writePongLocked(payload []byte, key [4]byte, masked bool) error {
	if c.state.Load() != stateOpen {
		return nil
	}
	c.armWrite(c.opts.WriteTimeout)
	h := c.putHeader(c.wbuf[:], finBit|opPong, len(payload))
	n := copy(c.wbuf[h:], payload)
	q := c.wbuf[h : h+n]
	if masked {
		maskBytes(q, key, 0)
	}
	if c.isClient {
		maskBytes(q, c.maskKey, 0)
	}
	return c.writeDone(c.write(c.wbuf[:h+n]))
}

func (c *Conn) writeCloseLocked(code CloseCode, reason string, timeout time.Duration) error {
	c.armWrite(timeout)
	n := 0
	if code != CloseNoStatus {
		n = 2 + len(reason)
	}
	h := c.putHeader(c.wbuf[:], finBit|opClose, n)
	if n > 0 {
		binary.BigEndian.PutUint16(c.wbuf[h:], uint16(code))
		copy(c.wbuf[h+2:], reason)
	}
	if c.isClient {
		maskBytes(c.wbuf[h:h+n], c.maskKey, 0)
	}
	return c.writeDone(c.write(c.wbuf[:h+n]))
}

func (c *Conn) armWrite(timeout time.Duration) {
	_ = c.netConn.SetWriteDeadline(time.Now().Add(timeout))
}

func (c *Conn) write(b []byte) error {
	_, err := c.netConn.Write(b)
	return err
}

func (c *Conn) writeDone(err error) error {
	if err == nil {
		return nil
	}
	if c.state.Load() == stateClosed {
		return ErrClosed
	}
	c.shutdown()
	if isTimeout(err) {
		return ErrTimeout
	}
	return ErrAbnormalClosure
}
