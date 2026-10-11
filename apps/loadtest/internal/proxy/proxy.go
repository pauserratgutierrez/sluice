// Package proxy is a TCP proxy between the runner and Sluice that can drop
// every connection on command, the way a reverse proxy does when it reloads
// or restarts: the streams end without a server_shutdown event, and Sluice,
// still running, keeps its resume buffer.
//
//	POST /cut?down=0s   close every connection; with down > 0 also refuse new
//	                    ones for that long (a restart rather than a reload)
//	GET  /stats         connections open and accepted
package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func Main() error {
	p := &proxy{
		listen:   envOr("PROXY_LISTEN", "0.0.0.0:4000"),
		upstream: envOr("PROXY_UPSTREAM", "sluice-rooms:4000"),
		conns:    map[*pair]struct{}{},
	}
	if err := p.start(); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.HandleFunc("POST /cut", p.handleCut)
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"open": p.open(), "accepted": p.accepted.Load(), "cuts": p.cuts.Load()})
	})
	control := envOr("PROXY_CONTROL", "0.0.0.0:9000")
	fmt.Printf("proxy: %s -> %s, control on %s\n", p.listen, p.upstream, control)
	return (&http.Server{Addr: control, Handler: mux, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe()
}

type pair struct{ client, upstream net.Conn }

func (c *pair) close() {
	_ = c.client.Close()
	if c.upstream != nil {
		_ = c.upstream.Close()
	}
}

type proxy struct {
	listen, upstream string

	mu    sync.Mutex
	ln    net.Listener
	conns map[*pair]struct{}

	accepted atomic.Int64
	cuts     atomic.Int64
}

func (p *proxy) start() error {
	ln, err := net.Listen("tcp", p.listen)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	go p.accept(ln)
	return nil
}

func (p *proxy) accept(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		p.accepted.Add(1)
		go p.serve(c)
	}
}

func (p *proxy) serve(c net.Conn) {
	pc := &pair{client: c}
	p.mu.Lock()
	p.conns[pc] = struct{}{}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.conns, pc)
		p.mu.Unlock()
		pc.close()
	}()
	up, err := net.DialTimeout("tcp", p.upstream, 5*time.Second)
	if err != nil {
		return
	}
	p.mu.Lock()
	if _, ok := p.conns[pc]; !ok {
		// Cut while dialling.
		p.mu.Unlock()
		_ = up.Close()
		return
	}
	pc.upstream = up
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, c); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
	<-done
}

func (p *proxy) open() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

// cut closes every connection. With down > 0 the listener is closed first and
// reopened after down, so connection attempts meanwhile are refused.
func (p *proxy) cut(down time.Duration) (int, error) {
	p.cuts.Add(1)
	p.mu.Lock()
	if down > 0 && p.ln != nil {
		_ = p.ln.Close()
		p.ln = nil
	}
	victims := make([]*pair, 0, len(p.conns))
	for c := range p.conns {
		victims = append(victims, c)
	}
	clear(p.conns)
	p.mu.Unlock()
	for _, c := range victims {
		c.close()
	}
	if down > 0 {
		time.Sleep(down)
		if err := p.start(); err != nil {
			return len(victims), err
		}
	}
	return len(victims), nil
}

func (p *proxy) handleCut(w http.ResponseWriter, r *http.Request) {
	var down time.Duration
	if v := r.URL.Query().Get("down"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			http.Error(w, "down must be a duration", http.StatusBadRequest)
			return
		}
		down = d
	}
	at := time.Now()
	n, err := p.cut(down)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"closed": n, "at": at.UnixNano(), "down_ms": down.Milliseconds()})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
