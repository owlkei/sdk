package ws

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
)

const (
	maxEnvelopeBytes = 1024
	headerBytes      = 4
	maxTopicBytes    = 255
	maxSegments      = 16
	maxIDBytes       = 64
)

type envelope struct {
	Type       string          `json:"type"`
	ID         string          `json:"id,omitempty"`
	Topic      string          `json:"topic,omitempty"`
	RequestID  string          `json:"requestId,omitempty"`
	RetryAfter *int            `json:"retryAfter,omitempty"`
	Error      json.RawMessage `json:"error,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
}

type incoming struct {
	envelope envelope
	binary   []byte
	isBinary bool
}

var errInvalidFrame = errors.New("ws: invalid frame")

// IsTopic reports whether topic is a valid topic of the wire protocol v1: 1 to 16 segments of
// [A-Za-z0-9_~-] joined by dots, 255 bytes at most.
func IsTopic(topic string) bool {
	if len(topic) == 0 || len(topic) > maxTopicBytes {
		return false
	}
	segments, size := 1, 0
	for i := 0; i < len(topic); i++ {
		b := topic[i]
		switch {
		case b == '.':
			if size == 0 {
				return false
			}
			segments++
			size = 0
		case isTopicByte(b):
			size++
		default:
			return false
		}
	}
	return size > 0 && segments <= maxSegments
}

func isTopicByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '~' || b == '-'
}

func isID(id string) bool {
	if len(id) == 0 || len(id) > maxIDBytes {
		return false
	}
	for i := 0; i < len(id); i++ {
		if b := id[i]; b != '.' && (b == '~' || !isTopicByte(b)) {
			return false
		}
	}
	return true
}

func encodeText(e envelope) ([]byte, error) {
	return json.Marshal(e)
}

func encodeBinary(e envelope, data []byte) ([]byte, error) {
	e.Data = nil
	head, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	if len(head) < 2 || len(head) > maxEnvelopeBytes {
		return nil, errors.New("ws: the envelope of a binary frame must be 2 to 1024 bytes")
	}
	frame := make([]byte, headerBytes+len(head)+len(data))
	binary.BigEndian.PutUint32(frame, uint32(len(head))) //nolint:gosec // at most 1024, checked above
	copy(frame[headerBytes:], head)
	copy(frame[headerBytes+len(head):], data)
	return frame, nil
}

func decodeText(frame []byte) (incoming, error) {
	e, err := decodeEnvelope(frame)
	return incoming{envelope: e}, err
}

func decodeBinary(frame []byte) (incoming, error) {
	if len(frame) < headerBytes {
		return incoming{}, errInvalidFrame
	}
	n := binary.BigEndian.Uint32(frame)
	if n < 2 || n > maxEnvelopeBytes || int(n) > len(frame)-headerBytes {
		return incoming{}, errInvalidFrame
	}
	head := frame[headerBytes : headerBytes+int(n)]
	e, err := decodeEnvelope(head)
	if err != nil {
		return incoming{}, err
	}
	if hasMember(head, "data") {
		return incoming{}, errInvalidFrame
	}
	return incoming{envelope: e, binary: frame[headerBytes+int(n):], isBinary: true}, nil
}

func decodeEnvelope(frame []byte) (envelope, error) {
	var e envelope
	if err := json.Unmarshal(frame, &e); err != nil || e.Type == "" {
		return envelope{}, errInvalidFrame
	}
	return e, nil
}

func hasMember(object []byte, name string) bool {
	var members map[string]json.RawMessage
	if json.Unmarshal(object, &members) != nil {
		return false
	}
	_, ok := members[name]
	return ok
}

func isJSONNull(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
