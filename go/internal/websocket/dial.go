package websocket

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxResponseHead = 16 * 1024

var (
	reservedRequestHeaders = map[string]bool{
		"Host":                     true,
		"Upgrade":                  true,
		"Connection":               true,
		"Sec-Websocket-Key":        true,
		"Sec-Websocket-Version":    true,
		"Sec-Websocket-Protocol":   true,
		"Sec-Websocket-Extensions": true,
		"Sec-Websocket-Accept":     true,
		"Content-Length":           true,
		"Transfer-Encoding":        true,
		"Trailer":                  true,
	}

	errNilContext       = newDetailError(ErrInvalidArgument, "nil context")
	errDialURL          = newDetailError(ErrInvalidArgument, "url")
	errDialScheme       = newDetailError(ErrInvalidArgument, "url scheme is not ws or wss")
	errDialUserinfo     = newDetailError(ErrInvalidArgument, "url holds userinfo")
	errDialFragment     = newDetailError(ErrInvalidArgument, "url holds a fragment")
	errResponseHead     = newDetailError(ErrBadHandshake, "malformed response")
	errResponseTooLong  = newDetailError(ErrBadHandshake, "response head over 16 KiB")
	errResponseStatus   = newDetailError(ErrBadHandshake, "response status is not 101")
	errResponseUpgrade  = newDetailError(ErrBadHandshake, "response Upgrade is not websocket")
	errResponseConn     = newDetailError(ErrBadHandshake, "response Connection is not Upgrade")
	errResponseAccept   = newDetailError(ErrBadHandshake, "invalid Sec-WebSocket-Accept")
	errResponseProtocol = newDetailError(ErrBadHandshake, "Sec-WebSocket-Protocol was not offered")
	errResponseExt      = newDetailError(ErrBadHandshake, "response names an extension")
)

// DialOptions configures Dial. A nil *DialOptions is valid.
type DialOptions struct {
	// Header holds extra request headers (Origin, Cookie, Authorization). A header of the
	// handshake itself (Host, Upgrade, Connection, Sec-WebSocket-*) is refused. Default: none.
	Header http.Header
	// Subprotocols lists the subprotocols offered, most preferred first. Default: none.
	Subprotocols []string
	// HandshakeTimeout bounds the dial and the handshake together. Default: 5s.
	HandshakeTimeout time.Duration
	// NetDial opens the transport. For a wss URL it returns a connection that is already TLS.
	// Default: net.Dialer for ws, tls.Dialer for wss, verifying the certificate against the host.
	NetDial func(ctx context.Context, network, address string) (net.Conn, error)
	// Conn holds the limits and deadlines of the connection. Default: nil (ConnOptions defaults).
	Conn *ConnOptions
}

// Dial opens a client connection to a ws:// or wss:// URL (RFC 6455 § 4.1). It follows no
// redirect, reads no proxy environment variable and, for wss, always verifies the server
// certificate against the URL's host. Every frame it writes is masked with a fresh key from
// crypto/rand. The dial and the handshake together end within HandshakeTimeout and ctx.
//
// It returns ErrInvalidArgument for a nil ctx or an unusable URL (userinfo, a fragment, another
// scheme), ErrInvalidOptions for an invalid option, ErrDial when the connection or the TLS
// handshake fails, ErrTimeout when the deadline passes and ErrBadHandshake for any response that
// is not a valid 101 (a redirect, an extension, a subprotocol that was not offered, a head over
// 16 KiB). No error holds the URL or a byte of the response.
func Dial(ctx context.Context, rawURL string, opts *DialOptions) (*Conn, error) {
	if ctx == nil {
		return nil, errNilContext
	}
	u, err := parseDialURL(rawURL)
	if err != nil {
		return nil, err
	}
	var o DialOptions
	if opts != nil {
		o = *opts
	}
	connOpts, err := loadDialOptions(&o)
	if err != nil {
		return nil, err
	}
	timeout := o.HandshakeTimeout
	if timeout == 0 {
		timeout = defaultHandshakeTimeout
	}
	dctx, cancel := context.WithTimeout(ctx, timeout)
	netConn, err := dialTransport(dctx, u, o.NetDial)
	if err != nil {
		timedOut := dctx.Err() != nil
		cancel()
		if timedOut {
			return nil, ErrTimeout
		}
		return nil, ErrDial
	}
	deadline, _ := dctx.Deadline()
	_ = netConn.SetDeadline(deadline)
	stop := context.AfterFunc(dctx, func() { _ = netConn.SetDeadline(time.Unix(1, 0)) })
	c, err := clientHandshake(netConn, u, &o, &connOpts)
	interrupted := !stop()
	cancel()
	if err == nil && interrupted {
		err = ErrTimeout
	}
	if err != nil {
		abortTransport(netConn)
		if interrupted || isTimeout(err) {
			return nil, ErrTimeout
		}
		return nil, err
	}
	_ = netConn.SetDeadline(time.Time{})
	return c, nil
}

func parseDialURL(rawURL string) (*url.URL, error) {
	u, err := url.Parse(rawURL)
	switch {
	case err != nil:
		return nil, errDialURL
	case u.Scheme != "ws" && u.Scheme != "wss":
		return nil, errDialScheme
	case u.User != nil:
		return nil, errDialUserinfo
	case u.Fragment != "" || strings.IndexByte(rawURL, '#') >= 0:
		return nil, errDialFragment
	case u.Opaque != "" || u.Hostname() == "":
		return nil, errDialURL
	}
	return u, nil
}

func loadDialOptions(o *DialOptions) (ConnOptions, error) {
	if o.HandshakeTimeout < 0 {
		return ConnOptions{}, invalidOption("DialOptions", "HandshakeTimeout", "is negative")
	}
	if err := checkSubprotocols("DialOptions", o.Subprotocols); err != nil {
		return ConnOptions{}, err
	}
	for name := range o.Header {
		switch {
		case !isToken(name):
			return ConnOptions{}, invalidOption("DialOptions", "Header", "holds an invalid header name")
		case reservedRequestHeaders[http.CanonicalHeaderKey(name)]:
			return ConnOptions{}, invalidOption("DialOptions", "Header", "holds a header of the handshake")
		}
	}
	return loadConnOptionsAt(o.Conn, "DialOptions.Conn")
}

func dialTransport(ctx context.Context, u *url.URL, netDial func(context.Context, string, string) (net.Conn, error)) (net.Conn, error) {
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "wss" {
			port = "443"
		}
	}
	address := net.JoinHostPort(u.Hostname(), port)
	if netDial != nil {
		conn, err := netDial(ctx, "tcp", address)
		if err == nil && conn == nil {
			err = ErrDial
		}
		return conn, err
	}
	if u.Scheme == "ws" {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", address)
	}
	d := tls.Dialer{Config: &tls.Config{
		ServerName: u.Hostname(),
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"http/1.1"},
	}}
	return d.DialContext(ctx, "tcp", address)
}

func clientHandshake(netConn net.Conn, u *url.URL, o *DialOptions, connOpts *ConnOptions) (*Conn, error) {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	var key [24]byte
	base64.StdEncoding.Encode(key[:], raw[:])
	if err := writeDialRequest(netConn, u, o, string(key[:])); err != nil {
		return nil, err
	}
	br := bufio.NewReaderSize(netConn, scratchSize)
	head, err := readResponseHead(br)
	if err != nil {
		return nil, err
	}
	protocol, err := checkDialResponse(head, string(key[:]), o.Subprotocols)
	if err != nil {
		return nil, err
	}
	c := newConn(netConn, br, true, connOpts)
	c.subprotocol = protocol
	return c, nil
}

func writeDialRequest(netConn net.Conn, u *url.URL, o *DialOptions, key string) error {
	var b bytes.Buffer
	b.WriteString("GET ")
	b.WriteString(u.RequestURI())
	b.WriteString(" HTTP/1.1\r\nHost: ")
	b.WriteString(u.Host)
	b.WriteString("\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ")
	b.WriteString(key)
	b.WriteString("\r\n")
	if len(o.Subprotocols) > 0 {
		b.WriteString("Sec-WebSocket-Protocol: ")
		b.WriteString(strings.Join(o.Subprotocols, ", "))
		b.WriteString("\r\n")
	}
	if len(o.Header) > 0 {
		_ = o.Header.WriteSubset(&b, reservedRequestHeaders)
	}
	b.WriteString("\r\n")
	_, err := netConn.Write(b.Bytes())
	if err != nil && !isTimeout(err) {
		return ErrAbnormalClosure
	}
	return err
}

func readResponseHead(br *bufio.Reader) ([]byte, error) {
	var head []byte
	lineStart := 0
	for {
		chunk, err := br.ReadSlice('\n')
		if len(head)+len(chunk) > maxResponseHead {
			return nil, errResponseTooLong
		}
		head = append(head, chunk...)
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case err != nil:
			if isTimeout(err) {
				return nil, err
			}
			return nil, errResponseHead
		}
		line := head[lineStart:]
		if len(line) <= 2 && (string(line) == "\r\n" || string(line) == "\n") {
			return head, nil
		}
		lineStart = len(head)
	}
}

func checkDialResponse(head []byte, key string, offered []string) (string, error) {
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(head)), nil)
	if err != nil {
		return "", errResponseHead
	}
	_ = resp.Body.Close()
	h := resp.Header
	switch {
	case resp.ProtoMajor != 1 || resp.ProtoMinor != 1 || resp.StatusCode != http.StatusSwitchingProtocols:
		return "", errResponseStatus
	case !isSingleValue(h["Upgrade"], "websocket"):
		return "", errResponseUpgrade
	case !hasConnectionUpgrade(h["Connection"]):
		return "", errResponseConn
	case h["Sec-Websocket-Extensions"] != nil:
		return "", errResponseExt
	}
	var want [28]byte
	if accept := h["Sec-Websocket-Accept"]; len(accept) != 1 || accept[0] != string(appendAccept(want[:0], key)) {
		return "", errResponseAccept
	}
	protocols := h["Sec-Websocket-Protocol"]
	if protocols == nil {
		return "", nil
	}
	if len(protocols) == 1 {
		for _, p := range offered {
			if protocols[0] == p {
				return p, nil
			}
		}
	}
	return "", errResponseProtocol
}
