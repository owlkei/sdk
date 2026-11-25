package websocket

import (
	"errors"
	"fmt"
	"io"
	"strconv"
)

var (
	// ErrInvalidOptions reports an invalid option given to NewUpgrader or Dial. Its text names the
	// option, never its value.
	ErrInvalidOptions = errors.New("websocket: invalid options")
	// ErrBadHandshake reports a malformed opening handshake.
	ErrBadHandshake = errors.New("websocket: bad handshake")
	// ErrUnsupportedVersion reports a handshake that does not ask for version 13.
	ErrUnsupportedVersion = errors.New("websocket: unsupported version")
	// ErrOriginNotAllowed reports a handshake refused by the origin decision.
	ErrOriginNotAllowed = errors.New("websocket: origin not allowed")
	// ErrHijack reports a ResponseWriter that cannot hand over its connection.
	ErrHijack = errors.New("websocket: connection cannot be hijacked")
	// ErrDial reports a failed connect or TLS handshake of the client.
	ErrDial = errors.New("websocket: dial failed")
	// ErrProtocol reports a frame that breaks RFC 6455 (close code 1002).
	ErrProtocol = errors.New("websocket: protocol error")
	// ErrInvalidUTF8 reports a text message or close reason that is not UTF-8 (close code 1007).
	ErrInvalidUTF8 = errors.New("websocket: invalid UTF-8")
	// ErrMessageTooBig reports a frame or message over a limit (close code 1009).
	ErrMessageTooBig = errors.New("websocket: message too big")
	// ErrTimeout reports a read, write or close handshake that did not end within its deadline.
	ErrTimeout = errors.New("websocket: timeout")
	// ErrAbnormalClosure reports a transport that ended or failed without a close handshake.
	ErrAbnormalClosure = errors.New("websocket: connection lost")
	// ErrClosed reports a call on a connection that is closed or closing.
	ErrClosed = errors.New("websocket: connection closed")
	// ErrWriteInProgress reports a data write while a streamed message is open.
	ErrWriteInProgress = errors.New("websocket: a message is being written")
	// ErrInvalidArgument reports an argument the call cannot use.
	ErrInvalidArgument = errors.New("websocket: invalid argument")
)

var (
	errReservedBit       = newDetailError(ErrProtocol, "reserved bit set")
	errReservedOpcode    = newDetailError(ErrProtocol, "reserved opcode")
	errUnmaskedFrame     = newDetailError(ErrProtocol, "unmasked client frame")
	errMaskedFrame       = newDetailError(ErrProtocol, "masked server frame")
	errNotMinimal        = newDetailError(ErrProtocol, "length is not minimal")
	errLengthRange       = newDetailError(ErrProtocol, "length out of range")
	errFragmentedControl = newDetailError(ErrProtocol, "fragmented control frame")
	errControlTooLong    = newDetailError(ErrProtocol, "control frame too long")
	errContinuation      = newDetailError(ErrProtocol, "continuation without a message")
	errDataInMessage     = newDetailError(ErrProtocol, "data frame inside a fragmented message")
	errCloseOneByte      = newDetailError(ErrProtocol, "close payload of one byte")
	errInvalidCloseCode  = newDetailError(ErrProtocol, "invalid close code")
	errFrameTooBig       = newDetailError(ErrMessageTooBig, "frame over the limit")
	errMessageTooBig     = newDetailError(ErrMessageTooBig, "message over the limit")
	errTooManyFragments  = newDetailError(ErrMessageTooBig, "too many fragments")
	errNilArgument       = newDetailError(ErrInvalidArgument, "nil request or response")
	errInvalidKind       = newDetailError(ErrInvalidArgument, "kind")
	errInvalidPrepared   = newDetailError(ErrInvalidArgument, "prepared message")
	errPingTooLong       = newDetailError(ErrInvalidArgument, "ping payload over 125 bytes")
	errCloseCode         = newDetailError(ErrInvalidArgument, "close code")
	errCloseReason       = newDetailError(ErrInvalidArgument, "close reason")
	errHTTPVersion       = newDetailError(ErrBadHandshake, "HTTP/1.1 is required")
	errMethod            = newDetailError(ErrBadHandshake, "method is not GET")
	errRequestBody       = newDetailError(ErrBadHandshake, "request has a body")
	errConnectionHeader  = newDetailError(ErrBadHandshake, "Connection is not Upgrade")
	errUpgradeHeader     = newDetailError(ErrBadHandshake, "Upgrade is not websocket")
	errKeyHeader         = newDetailError(ErrBadHandshake, "invalid Sec-WebSocket-Key")
	errProtocolHeader    = newDetailError(ErrBadHandshake, "invalid Sec-WebSocket-Protocol")
	errExtensionsHeader  = newDetailError(ErrBadHandshake, "invalid Sec-WebSocket-Extensions")
	errTwoOrigins        = newDetailError(ErrBadHandshake, "more than one Origin")
	errNoOriginCheck     = newDetailError(ErrOriginNotAllowed, "UpgradeOptions.CheckOrigin is not set")
	errOriginPanic       = newDetailError(ErrOriginNotAllowed, "CheckOrigin panicked")
)

type detailError struct {
	text  string
	class error
}

func newDetailError(class error, detail string) error {
	return &detailError{text: class.Error() + ": " + detail, class: class}
}

func (e *detailError) Error() string { return e.text }

func (e *detailError) Unwrap() error { return e.class }

// CloseError is the close frame the peer sent: its code and its reason. Reason is the peer's
// text, checked as UTF-8 only: it is untrusted and must not be logged or shown.
type CloseError struct {
	code   CloseCode
	reason string
}

// Error returns "websocket: closed by peer: <code>"; it never holds the reason.
func (e *CloseError) Error() string {
	return "websocket: closed by peer: " + strconv.Itoa(int(e.code))
}

// Code returns the peer's close code, or CloseNoStatus when its close frame held none.
func (e *CloseError) Code() CloseCode { return e.code }

// Reason returns the peer's close reason. It is untrusted input: never log it or show it.
func (e *CloseError) Reason() string { return e.reason }

// Format prints Error() under every fmt verb, so no verb can print the reason.
func (e *CloseError) Format(s fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = io.WriteString(s, strconv.Quote(e.Error()))
		return
	}
	_, _ = io.WriteString(s, e.Error())
}
