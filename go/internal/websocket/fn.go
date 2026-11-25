package websocket

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
)

func invalidOption(path, field, problem string) error {
	return fmt.Errorf("%w: %s.%s %s", ErrInvalidOptions, path, field, problem)
}

func checkSubprotocols(path string, list []string) error {
	if len(list) > maxSubprotocols {
		return invalidOption(path, "Subprotocols", "has more than 16 entries")
	}
	for i, p := range list {
		field := "Subprotocols[" + strconv.Itoa(i) + "]"
		switch {
		case len(p) > maxSubprotocolBytes:
			return invalidOption(path, field, "is longer than 64 bytes")
		case !isToken(p):
			return invalidOption(path, field, "is not a token")
		}
		if slices.Contains(list[:i], p) {
			return invalidOption(path, field, "is a duplicate")
		}
	}
	return nil
}

func isTimeout(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func encodeHeader(b []byte, b0 byte, n int, masked bool) int {
	b[0] = b0
	var mb byte
	if masked {
		mb = maskBit
	}
	if n > 0xffff {
		b[1] = mb | 127
		binary.BigEndian.PutUint64(b[2:], uint64(n))
		return 10
	}
	if n > maxControl {
		b[1] = mb | 126
		binary.BigEndian.PutUint16(b[2:], uint16(n))
		return 4
	}
	if n < 0 {
		n = 0
	}
	b[1] = mb | byte(n)
	return 2
}

func hasConnectionUpgrade(values []string) bool {
	if hasQuote(values) {
		return false
	}
	found := false
	forEachElement(values, func(e string) {
		if strings.EqualFold(e, "upgrade") {
			found = true
		}
	})
	return found
}

func isSingleValue(values []string, want string) bool {
	return len(values) == 1 && strings.EqualFold(trimSpaceTab(values[0]), want)
}

func forEachElement(values []string, fn func(string)) {
	for _, v := range values {
		for {
			i := strings.IndexByte(v, ',')
			e := v
			if i >= 0 {
				e = v[:i]
			}
			if e = trimSpaceTab(e); e != "" {
				fn(e)
			}
			if i < 0 {
				break
			}
			v = v[i+1:]
		}
	}
}

func trimSpaceTab(s string) string {
	return strings.Trim(s, " \t")
}

func hasQuote(values []string) bool {
	for _, v := range values {
		if strings.IndexByte(v, '"') >= 0 {
			return true
		}
	}
	return false
}

func isToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}

func appendAccept(dst []byte, key string) []byte {
	var in [24 + len(acceptGUID)]byte
	copy(in[:], key)
	copy(in[24:], acceptGUID)
	sum := sha1.Sum(in[:])
	var out [28]byte
	base64.StdEncoding.Encode(out[:], sum[:])
	return append(dst, out[:]...)
}

func abortTransport(netConn net.Conn) {
	if tc, ok := netConn.(interface{ NetConn() net.Conn }); ok {
		_ = tc.NetConn().Close()
		return
	}
	_ = netConn.Close()
}
