package websocket

import (
	"net/http"
	"time"
)

const (
	defaultMaxMessageBytes   = 64 * 1024
	defaultMaxFragments      = 128
	defaultMaxPingsPerSecond = 32
	defaultIdleTimeout       = 60 * time.Second
	defaultReadTimeout       = 10 * time.Second
	defaultWriteTimeout      = 10 * time.Second
	defaultCloseTimeout      = 5 * time.Second
	defaultHandshakeTimeout  = 5 * time.Second
	maxMessageBytesCap       = 1 << 30
)

// UpgradeOptions configures an Upgrader. A nil *UpgradeOptions is valid and refuses every
// handshake, since CheckOrigin is then not set.
type UpgradeOptions struct {
	// CheckOrigin decides whether the request's origin may connect. It is called once per
	// well-formed handshake, also when the request has no Origin header. Default: nil, which
	// refuses every handshake with 403.
	CheckOrigin func(r *http.Request) bool
	// Subprotocols lists the supported subprotocols, most preferred first. Default: none.
	Subprotocols []string
	// HandshakeTimeout bounds writing the 101 response. Default: 5s.
	HandshakeTimeout time.Duration
	// Conn holds the limits and deadlines of every connection. Default: nil (ConnOptions defaults).
	Conn *ConnOptions
}

// ConnOptions holds the limits and deadlines of a connection. A zero field takes its default;
// no field can turn a limit or a deadline off.
type ConnOptions struct {
	// MaxMessageBytes is the largest message accepted, across fragments. Default: 64 KiB. At most 1 GiB.
	MaxMessageBytes int
	// MaxFrameBytes is the largest data frame accepted. Default: MaxMessageBytes.
	MaxFrameBytes int
	// MaxFragments is the largest number of frames in one message. Default: 128.
	MaxFragments int
	// MaxPingsPerSecond is the number of pings answered per second; the rest are dropped. Default: 32.
	MaxPingsPerSecond int
	// IdleTimeout bounds the wait for the first byte of a frame between messages. Default: 60s.
	IdleTimeout time.Duration
	// ReadTimeout bounds a message from its first byte to its last. Default: 10s.
	ReadTimeout time.Duration
	// WriteTimeout bounds the write of one frame. Default: 10s.
	WriteTimeout time.Duration
	// CloseTimeout bounds the wait for the peer's close frame. Default: 5s.
	CloseTimeout time.Duration
}

func loadConnOptions(opts *ConnOptions) (ConnOptions, error) {
	return loadConnOptionsAt(opts, "ConnOptions")
}

func loadConnOptionsAt(opts *ConnOptions, path string) (ConnOptions, error) {
	var o ConnOptions
	if opts != nil {
		o = *opts
	}
	var err error
	o.MaxMessageBytes, err = defaultInt(o.MaxMessageBytes, defaultMaxMessageBytes, path, "MaxMessageBytes", err)
	o.MaxFrameBytes, err = defaultInt(o.MaxFrameBytes, o.MaxMessageBytes, path, "MaxFrameBytes", err)
	o.MaxFragments, err = defaultInt(o.MaxFragments, defaultMaxFragments, path, "MaxFragments", err)
	o.MaxPingsPerSecond, err = defaultInt(o.MaxPingsPerSecond, defaultMaxPingsPerSecond, path, "MaxPingsPerSecond", err)
	o.IdleTimeout, err = defaultDuration(o.IdleTimeout, defaultIdleTimeout, path, "IdleTimeout", err)
	o.ReadTimeout, err = defaultDuration(o.ReadTimeout, defaultReadTimeout, path, "ReadTimeout", err)
	o.WriteTimeout, err = defaultDuration(o.WriteTimeout, defaultWriteTimeout, path, "WriteTimeout", err)
	o.CloseTimeout, err = defaultDuration(o.CloseTimeout, defaultCloseTimeout, path, "CloseTimeout", err)
	switch {
	case err != nil:
		return ConnOptions{}, err
	case o.MaxMessageBytes > maxMessageBytesCap:
		return ConnOptions{}, invalidOption(path, "MaxMessageBytes", "is over 1 GiB")
	case o.MaxFrameBytes > o.MaxMessageBytes:
		return ConnOptions{}, invalidOption(path, "MaxFrameBytes", "is over MaxMessageBytes")
	}
	return o, nil
}

func defaultInt(value, def int, path, field string, err error) (int, error) {
	switch {
	case err != nil:
		return value, err
	case value < 0:
		return value, invalidOption(path, field, "is negative")
	case value == 0:
		return def, nil
	}
	return value, nil
}

func defaultDuration(value, def time.Duration, path, field string, err error) (time.Duration, error) {
	switch {
	case err != nil:
		return value, err
	case value < 0:
		return value, invalidOption(path, field, "is negative")
	case value == 0:
		return def, nil
	}
	return value, nil
}
