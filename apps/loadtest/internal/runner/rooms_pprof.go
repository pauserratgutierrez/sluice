package runner

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// captureProfiles takes, LOAD_PPROF_AT into the steady phase, a CPU profile
// of LOAD_PPROF_SECONDS from a Sluice built with pprof (scripts/build-pprof.sh)
// and then its heap and goroutines, and keeps them next to the report.
func (e *engine) captureProfiles(ctx context.Context, steadyAt time.Time) {
	// Heap snapshots at fixed points of the steady phase, to compare what the
	// GC finds live as the run goes on.
	for _, at := range e.rc.PprofHeapAt {
		go func(at time.Duration) {
			if !sleepCtx(ctx, at-time.Since(steadyAt)) {
				return
			}
			e.snapshot(ctx, "heap", "heap-"+at.String())
		}(at)
	}
	if !sleepCtx(ctx, e.rc.PprofAt-time.Since(steadyAt)) {
		return
	}
	base := fmt.Sprintf("%s-%s-%s", e.cfg.Scenario, sanitize(e.rc.Label), e.started.UTC().Format("20060102T150405Z"))
	client := &http.Client{Timeout: e.rc.PprofSeconds + time.Minute}
	for _, p := range []struct{ path, ext string }{
		{fmt.Sprintf("profile?seconds=%d", int(e.rc.PprofSeconds.Seconds())), "cpu"},
		{"heap", "heap"},
		{"goroutine", "goroutine"},
		{"block", "block"},
		{"mutex", "mutex"},
		{"trace?seconds=5", "trace"},
	} {
		at := time.Since(e.started).Seconds()
		name := filepath.Join(e.cfg.ResultsDir, base+"."+p.ext+".pprof")
		if err := fetchTo(ctx, client, strings.TrimRight(e.rc.PprofURL, "/")+"/"+p.path, name); err != nil {
			e.log.Warn("profile", "kind", p.ext, "err", err)
			e.count("pprof_failed:" + p.ext)
			continue
		}
		e.profMu.Lock()
		e.profiles = append(e.profiles, Profile{Kind: p.ext, File: filepath.Base(name), AtS: at, Phase: e.phaseName()})
		e.profMu.Unlock()
		e.log.Info("profile", "kind", p.ext, "file", filepath.Base(name))
	}
}

// Profile is a pprof profile taken during the run.
type Profile struct {
	Kind  string  `json:"kind"`
	File  string  `json:"file"`
	AtS   float64 `json:"at_s"`
	Phase string  `json:"phase"`
}

func fetchTo(ctx context.Context, c *http.Client, url, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: status %d", url, resp.StatusCode)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// snapshot saves one profile that needs no sampling time (heap, goroutine).
func (e *engine) snapshot(ctx context.Context, path, ext string) {
	base := fmt.Sprintf("%s-%s-%s", e.cfg.Scenario, sanitize(e.rc.Label), e.started.UTC().Format("20060102T150405Z"))
	name := filepath.Join(e.cfg.ResultsDir, base+"."+ext+".pprof")
	at := time.Since(e.started).Seconds()
	client := &http.Client{Timeout: time.Minute}
	if err := fetchTo(ctx, client, strings.TrimRight(e.rc.PprofURL, "/")+"/"+path, name); err != nil {
		e.log.Warn("profile", "kind", ext, "err", err)
		e.count("pprof_failed:" + ext)
		return
	}
	e.profMu.Lock()
	e.profiles = append(e.profiles, Profile{Kind: ext, File: filepath.Base(name), AtS: at, Phase: e.phaseName()})
	e.profMu.Unlock()
	e.log.Info("profile", "kind", ext, "file", filepath.Base(name))
}
