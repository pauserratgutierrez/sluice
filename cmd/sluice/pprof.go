//go:build pprof

// Profiling builds only (go build -tags pprof): with SLUICE_PPROF_ADDR set,
// Go's pprof handlers listen on that address, apart from the public one. A
// release build does not contain this file.
//
// The block and mutex profiles sample from the start, at
// SLUICE_PPROF_BLOCK_RATE (one blocking event per that many nanoseconds
// blocked; default 100000) and SLUICE_PPROF_MUTEX_FRACTION (one contended
// lock in that many; default 100). 0 turns either off.

package main

import (
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"time"
)

func init() {
	addr := os.Getenv("SLUICE_PPROF_ADDR")
	if addr == "" {
		return
	}
	runtime.SetBlockProfileRate(envIntOr("SLUICE_PPROF_BLOCK_RATE", 100_000))
	runtime.SetMutexProfileFraction(envIntOr("SLUICE_PPROF_MUTEX_FRACTION", 100))
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	go func() {
		srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		if err := srv.ListenAndServe(); err != nil {
			slog.Error("pprof listener", "addr", addr, "err", err)
		}
	}()
}

func envIntOr(k string, d int) int {
	if n, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return n
	}
	return d
}
