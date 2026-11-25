package websocket

import (
	"bufio"
	"encoding/base64"
	"io"
	"maps"
	"net/http"
	"time"
)

const (
	acceptGUID          = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	maxSubprotocols     = 16
	maxSubprotocolBytes = 64
	maxListHeaderBytes  = 1024
	maxListValues       = 16
	switchingHead       = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
)

var (
	strictBase64 = base64.StdEncoding.Strict()

	reservedResponseHeaders = map[string]bool{
		"Upgrade":                  true,
		"Connection":               true,
		"Sec-Websocket-Accept":     true,
		"Sec-Websocket-Protocol":   true,
		"Sec-Websocket-Extensions": true,
		"Content-Length":           true,
		"Content-Type":             true,
		"Content-Encoding":         true,
		"Transfer-Encoding":        true,
		"Trailer":                  true,
	}

	zeroUpgrader Upgrader
)

// Upgrader turns HTTP/1.1 requests into WebSocket connections (RFC 6455 § 4.2). It is immutable
// after NewUpgrader and safe from any goroutine. A zero or nil Upgrader behaves as
// NewUpgrader(nil): it checks the request, then refuses it with 403.
type Upgrader struct {
	checkOrigin      func(r *http.Request) bool
	subprotocols     []string
	handshakeTimeout time.Duration
	conn             ConnOptions
}

// NewUpgrader checks and copies opts once. A nil opts is valid. An invalid option returns
// ErrInvalidOptions with a text naming the option, never its value.
func NewUpgrader(opts *UpgradeOptions) (*Upgrader, error) {
	u := &Upgrader{handshakeTimeout: defaultHandshakeTimeout}
	var o UpgradeOptions
	if opts != nil {
		o = *opts
	}
	if o.HandshakeTimeout < 0 {
		return nil, invalidOption("UpgradeOptions", "HandshakeTimeout", "is negative")
	}
	if o.HandshakeTimeout > 0 {
		u.handshakeTimeout = o.HandshakeTimeout
	}
	if err := checkSubprotocols("UpgradeOptions", o.Subprotocols); err != nil {
		return nil, err
	}
	conn, err := loadConnOptionsAt(o.Conn, "UpgradeOptions.Conn")
	if err != nil {
		return nil, err
	}
	u.checkOrigin = o.CheckOrigin
	u.subprotocols = append([]string(nil), o.Subprotocols...)
	u.conn = conn
	return u, nil
}

// Upgrade checks the opening handshake of r, hijacks the connection and writes the 101 response.
// Call it before anything is written to w. Headers already set in w.Header() are sent in the 101,
// except the handshake's own and the content headers, whatever the case of their names. A refused handshake answers a fixed
// text/plain status (400, 403, 426 with Sec-WebSocket-Version: 13, or 500 when w cannot be
// hijacked), returns nil and the error, and writes nothing more. The subprotocol chosen is the
// first entry of UpgradeOptions.Subprotocols the client offered; none is chosen when there is no
// common value, and Subprotocol returns "". No extension is ever negotiated.
//
// Upgrade cannot bound the request read itself: the http.Server needs a ReadHeaderTimeout.
func (u *Upgrader) Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if w == nil || r == nil {
		return nil, errNilArgument
	}
	if u == nil {
		u = &zeroUpgrader
	}
	if status, err := checkHandshake(r); err != nil {
		return nil, refuseHandshake(w, r, status, err)
	}
	if u.checkOrigin == nil {
		return nil, refuseHandshake(w, r, http.StatusForbidden, errNoOriginCheck)
	}
	allowed, panicked := callCheckOrigin(u.checkOrigin, r)
	switch {
	case panicked:
		return nil, refuseHandshake(w, r, http.StatusForbidden, errOriginPanic)
	case !allowed:
		return nil, refuseHandshake(w, r, http.StatusForbidden, ErrOriginNotAllowed)
	}
	protocol := u.pickSubprotocol(r.Header["Sec-Websocket-Protocol"])
	header := w.Header()
	netConn, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return nil, refuseHandshake(w, r, http.StatusInternalServerError, ErrHijack)
	}
	if brw == nil {
		brw = bufio.NewReadWriter(bufio.NewReader(netConn), bufio.NewWriter(netConn))
	}
	timeout := u.handshakeTimeout
	if timeout == 0 {
		timeout = defaultHandshakeTimeout
	}
	head := brw.AvailableBuffer()
	head = append(head, switchingHead...)
	head = appendAccept(head, r.Header["Sec-Websocket-Key"][0])
	head = append(head, "\r\n"...)
	if protocol != "" {
		head = append(head, "Sec-WebSocket-Protocol: "...)
		head = append(head, protocol...)
		head = append(head, "\r\n"...)
	}
	_ = netConn.SetWriteDeadline(time.Now().Add(timeout))
	_, err = brw.Write(head)
	if err == nil && len(header) > 0 {
		err = header.WriteSubset(brw, reservedNames(header))
	}
	if err == nil {
		_, err = brw.WriteString("\r\n")
	}
	if err == nil {
		err = brw.Flush()
	}
	if err != nil {
		abortTransport(netConn)
		if isTimeout(err) {
			return nil, ErrTimeout
		}
		return nil, ErrAbnormalClosure
	}
	_ = netConn.SetDeadline(time.Time{})
	c := newConn(netConn, bufio.NewReaderSize(brw.Reader, scratchSize), false, &u.conn)
	c.subprotocol = protocol
	return c, nil
}

func reservedNames(header http.Header) map[string]bool {
	names := reservedResponseHeaders
	for k := range header {
		if names[k] || !reservedResponseHeaders[http.CanonicalHeaderKey(k)] {
			continue
		}
		if len(names) == len(reservedResponseHeaders) {
			names = maps.Clone(reservedResponseHeaders)
		}
		names[k] = true
	}
	return names
}

func checkHandshake(r *http.Request) (int, error) {
	switch {
	case r.ProtoMajor != 1 || r.ProtoMinor != 1:
		return http.StatusBadRequest, errHTTPVersion
	case r.Method != http.MethodGet:
		return http.StatusBadRequest, errMethod
	case r.ContentLength != 0 || len(r.TransferEncoding) != 0:
		return http.StatusBadRequest, errRequestBody
	case !hasConnectionUpgrade(r.Header["Connection"]):
		return http.StatusBadRequest, errConnectionHeader
	case !isSingleValue(r.Header["Upgrade"], "websocket"):
		return http.StatusBadRequest, errUpgradeHeader
	}
	if v := r.Header["Sec-Websocket-Version"]; len(v) != 1 || v[0] != "13" {
		return http.StatusUpgradeRequired, ErrUnsupportedVersion
	}
	switch {
	case !isValidKey(r.Header["Sec-Websocket-Key"]):
		return http.StatusBadRequest, errKeyHeader
	case !isValidProtocolList(r.Header["Sec-Websocket-Protocol"]):
		return http.StatusBadRequest, errProtocolHeader
	case !isValidExtensionList(r.Header["Sec-Websocket-Extensions"]):
		return http.StatusBadRequest, errExtensionsHeader
	case len(r.Header["Origin"]) > 1:
		return http.StatusBadRequest, errTwoOrigins
	}
	return 0, nil
}

func isValidKey(values []string) bool {
	if len(values) != 1 || len(values[0]) != 24 {
		return false
	}
	var raw [18]byte
	n, err := strictBase64.Decode(raw[:], []byte(values[0]))
	return err == nil && n == 16
}

func isValidProtocolList(values []string) bool {
	if values == nil {
		return true
	}
	if totalLength(values) > maxListHeaderBytes || hasQuote(values) {
		return false
	}
	count, valid := 0, true
	forEachElement(values, func(e string) {
		count++
		valid = valid && isToken(e)
	})
	return valid && count > 0 && count <= maxListValues
}

func isValidExtensionList(values []string) bool {
	if totalLength(values) > maxListHeaderBytes {
		return false
	}
	count := 0
	forEachElement(values, func(string) { count++ })
	return count <= maxListValues
}

func (u *Upgrader) pickSubprotocol(offered []string) string {
	for _, p := range u.subprotocols {
		found := false
		forEachElement(offered, func(e string) {
			if e == p {
				found = true
			}
		})
		if found {
			return p
		}
	}
	return ""
}

func totalLength(values []string) int {
	n := 0
	for _, v := range values {
		n += len(v)
	}
	return n
}

func callCheckOrigin(check func(*http.Request) bool, r *http.Request) (allowed, panicked bool) {
	defer func() {
		if recover() != nil {
			allowed, panicked = false, true
		}
	}()
	return check(r), false
}

func refuseHandshake(w http.ResponseWriter, r *http.Request, status int, err error) error {
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	if r.ProtoMajor == 1 {
		h.Set("Connection", "close")
	}
	if status == http.StatusUpgradeRequired {
		h.Set("Sec-WebSocket-Version", "13")
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, http.StatusText(status)+"\n")
	return err
}
