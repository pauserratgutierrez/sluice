package oracle

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// closingAfterOne answers one request per connection and keeps it open, then
// closes it when the next request on it has been read, without answering: an
// endpoint closing an idle keep-alive connection just as a request goes out
// on it (Node's and Bun's keep-alive timeouts, 5 and 10 s, against Go's 90).
func closingAfterOne(t *testing.T) (url string, seenKey *atomic.Bool) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	seenKey = &atomic.Bool{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				for n := 0; ; n++ {
					req, err := http.ReadRequest(r)
					if err != nil {
						return
					}
					if req.Header.Get("Idempotency-Key") != "" {
						seenKey.Store(true)
					}
					_, _ = req.Body.Read(make([]byte, 1<<16))
					if n == 1 {
						return // the second request on a connection: close, no answer
					}
					body := `{"allow":false}`
					fmt.Fprintf(c, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
				}
			}(conn)
		}
	}()
	return "http://" + ln.Addr().String(), seenKey
}

// A pooled connection the issuer closed as the request went out on it is a
// retry on a new connection, not an issuer_unavailable.
func TestIssuerRetriesARequestOnAConnectionTheIssuerClosed(t *testing.T) {
	url, seenKey := closingAfterOne(t)
	iss, err := NewIssuerWith(IssuerOptions{
		URL: url, Bearer: "issuer-secret", Timeout: 2 * time.Second,
		Lookup: lookup(docsRel(), membersRel()), Columns: grantAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := Request{Relation: docsRel()}
	for i := range 3 {
		_, err := iss.Resolve(context.Background(), req)
		// allow:false is a denial; a failed call would be ErrUnavailable.
		var unavailable *ErrUnavailable
		if errors.As(err, &unavailable) {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if !seenKey.Load() {
		t.Fatal("issuer calls must carry an Idempotency-Key")
	}
	if tr, ok := iss.client.Transport.(*http.Transport); !ok || tr.IdleConnTimeout != DefaultIdleTimeout {
		t.Fatalf("idle timeout = %v, want %v", tr.IdleConnTimeout, DefaultIdleTimeout)
	}
}
