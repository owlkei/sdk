package websocket

import (
	"bufio"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Kind is the type of a data message; its values are the RFC 6455 opcodes.
type Kind uint8

const (
	// KindText is a UTF-8 text message.
	KindText Kind = 1
	// KindBinary is a binary message.
	KindBinary Kind = 2
)

// CloseCode is a close status code of RFC 6455 § 7.4.
type CloseCode uint16

const (
	// CloseNormal is a normal closure.
	CloseNormal CloseCode = 1000
	// CloseGoingAway is an endpoint going away, such as a server shutting down.
	CloseGoingAway CloseCode = 1001
	// CloseProtocolError is a protocol error.
	CloseProtocolError CloseCode = 1002
	// CloseUnsupportedData is data of a kind the endpoint does not accept.
	CloseUnsupportedData CloseCode = 1003
	// CloseNoStatus is a close frame without a code. Close sends it as an empty close frame.
	CloseNoStatus CloseCode = 1005
	// CloseInvalidPayload is a message whose data does not match its kind, such as invalid UTF-8.
	CloseInvalidPayload CloseCode = 1007
	// ClosePolicyViolation is a message that violates the endpoint's policy.
	ClosePolicyViolation CloseCode = 1008
	// CloseMessageTooBig is a message over the endpoint's limit.
	CloseMessageTooBig CloseCode = 1009
	// CloseMandatoryExtension is a client closing because the server did not negotiate an extension.
	CloseMandatoryExtension CloseCode = 1010
	// CloseInternalError is an unexpected condition on the endpoint.
	CloseInternalError CloseCode = 1011
	// CloseServiceRestart is a server restarting; the client may reconnect.
	CloseServiceRestart CloseCode = 1012
	// CloseTryAgainLater is a server that is overloaded for now; the client may reconnect later.
	CloseTryAgainLater CloseCode = 1013
)

const (
	stateOpen uint32 = iota
	stateClosing
	stateClosed
)

// Conn is a WebSocket connection. It starts no goroutine: the caller drives reads and writes.
//
// One goroutine reads (ReadMessage, NextReader and its reader); a second reader is serialized but
// which one gets which message is undefined. One goroutine writes data (WriteMessage,
// WritePrepared, NextWriter and its writer); concurrent data writes are serialized per frame and
// stay well-formed, and data writes during a streamed message return ErrWriteInProgress. Ping,
// Close, Abort and Subprotocol are safe from any goroutine at any time.
type Conn struct {
	netConn     net.Conn
	closer      net.Conn
	br          *bufio.Reader
	isClient    bool
	isVectored  bool
	opts        ConnOptions
	subprotocol string

	state         atomic.Uint32
	closeDeadline atomic.Int64

	readMu      sync.Mutex
	frameLeft   int
	frameKey    [4]byte
	framePos    int
	frameFin    bool
	frameMasked bool
	msgOpen     bool
	msgKind     Kind
	msgBytes    int
	msgFrags    int
	utf8        utf8State
	msgDeadline time.Time
	armed       time.Time
	readGen     uint64
	pingWindow  time.Time
	pingCount   int
	peerClosed  bool
	endErr      error

	writeMu     sync.Mutex
	wbuf        [144]byte
	bufs        [2][]byte
	vec         net.Buffers
	maskKey     [4]byte
	writerOpen  bool
	writerBegun bool
	writerKind  Kind
	writeGen    uint64
}

func newConn(netConn net.Conn, br *bufio.Reader, isClient bool, opts *ConnOptions) *Conn {
	o, err := loadConnOptions(opts)
	if err != nil {
		o, _ = loadConnOptions(nil)
	}
	c := &Conn{netConn: netConn, closer: netConn, br: br, isClient: isClient, opts: o}
	switch t := netConn.(type) {
	case *net.TCPConn, *net.UnixConn:
		c.isVectored = true
	case *tls.Conn:
		c.closer = t.NetConn()
	}
	return c
}

// Subprotocol returns the subprotocol chosen in the handshake, or "" when none was.
func (c *Conn) Subprotocol() string { return c.subprotocol }

func (c *Conn) arm(t time.Time) {
	if c.state.Load() == stateOpen {
		if t.Equal(c.armed) {
			return
		}
		_ = c.netConn.SetReadDeadline(t)
		c.armed = t
		if c.state.Load() == stateOpen {
			return
		}
	}
	c.armed = time.Time{}
	if d := c.closeDeadline.Load(); d != 0 {
		_ = c.netConn.SetReadDeadline(time.Unix(0, d))
	}
}
