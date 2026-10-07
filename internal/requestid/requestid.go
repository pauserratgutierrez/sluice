// Package requestid carries the ID of the HTTP request that work is done for,
// so Sluice's log lines, and the calls it makes to the shape issuer and to
// channel hooks, can be matched with the lines a gateway and an application
// log under the same ID.
//
// The ID is a log label and nothing else. Unless a gateway in front of Sluice
// overwrites it, a client can send any value, so it never decides anything.
package requestid

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
)

// DefaultHeader is the header the ID is read from and sent in unless
// SLUICE_REQUEST_ID_HEADER names another.
const DefaultHeader = "X-Request-ID"

// MaxLen is the longest ID taken from a request. The ID goes into every log
// line about the request, so a longer one, or one with a character outside
// visible ASCII, is replaced with a new one.
const MaxLen = 128

// ID is a request ID and the header it travels in.
type ID struct {
	Header string
	Value  string
}

type ctxKey struct{}

// Read returns the ID r carries in header, or a new one when it carries none
// or one unfit for a log line.
func Read(r *http.Request, header string) ID {
	v := r.Header.Get(header)
	if !valid(v) {
		v = New()
	}
	return ID{Header: header, Value: v}
}

// valid reports whether v is 1 to MaxLen visible ASCII characters.
func valid(v string) bool {
	if v == "" || len(v) > MaxLen {
		return false
	}
	for i := range len(v) {
		if v[i] < 0x21 || v[i] > 0x7e {
			return false
		}
	}
	return true
}

// New returns a random (version 4) UUID.
func New() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // RFC 9562 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// With returns ctx carrying id.
func With(ctx context.Context, id ID) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// From returns the ID ctx carries. It is zero for work no request started:
// fan-out of the replication stream, the catalog tick, revocations.
func From(ctx context.Context) ID {
	id, _ := ctx.Value(ctxKey{}).(ID)
	return id
}

// Set adds the ID to an outbound request's headers. A zero ID adds nothing.
func (id ID) Set(h http.Header) {
	if id.Header != "" && id.Value != "" {
		h.Set(id.Header, id.Value)
	}
}
