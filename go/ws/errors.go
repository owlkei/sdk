package ws

import (
	"encoding/json"
	"errors"
	"strconv"
)

// ErrClosed is returned by every call after Close, and by calls pending when Close ran.
var ErrClosed = errors.New("ws: the client is closed")

// ErrTimeout is returned when the server does not answer within Options.RequestTimeout.
var ErrTimeout = errors.New("ws: no answer within the request timeout")

// ErrInvalidTopic is returned, before anything is sent, for a topic outside the protocol's set.
var ErrInvalidTopic = errors.New("ws: invalid topic")

// ErrAmbiguousBytes is returned by Publish for a bare []byte: use Binary for a binary frame, or a
// string for JSON text.
var ErrAmbiguousBytes = errors.New("ws: []byte is ambiguous: use ws.Binary for a binary frame or a string for JSON text")

// FieldError is one field's failure in a validation error (422).
type FieldError struct {
	Rule    string `json:"rule"`
	Param   string `json:"param,omitempty"`
	Message string `json:"message"`
}

// Error is the server's refusal of a message: Code is an HTTP status code (403, 404, 405, 409, 415,
// 422, 429, 500, 503…), RequestID finds the server's log lines, RetryAfter is in seconds (0 when
// the server gave none).
type Error struct {
	Code       int                   `json:"code"`
	Status     string                `json:"error"`
	Message    string                `json:"message"`
	Fields     map[string]FieldError `json:"fields,omitempty"`
	RequestID  string                `json:"requestId,omitempty"`
	RetryAfter int                   `json:"-"`
}

func (e *Error) Error() string {
	return "ws: " + strconv.Itoa(e.Code) + " " + e.Message
}

// ConnectionError is returned when the connection ends before the server answers. CloseCode is the
// WebSocket close code, 1006 when the connection was lost without one.
type ConnectionError struct {
	CloseCode int
}

func (e *ConnectionError) Error() string {
	return "ws: the connection closed (" + strconv.Itoa(e.CloseCode) + ")"
}

func errorOf(e envelope) *Error {
	body := &Error{}
	if json.Unmarshal(e.Error, body) != nil || body.Code == 0 {
		body = &Error{Code: 500, Status: "Internal Server Error", Message: "Internal Server Error"}
	}
	if e.RequestID != "" {
		body.RequestID = e.RequestID
	}
	if e.RetryAfter != nil {
		body.RetryAfter = *e.RetryAfter
	}
	return body
}
