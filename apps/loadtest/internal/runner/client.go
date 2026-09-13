package runner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/sse"
)

type readySub struct {
	Sub    string `json:"sub"`
	OK     bool   `json:"ok"`
	Tier   string `json:"tier"`
	Oracle string `json:"oracle"`
	Error  *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type readyEvent struct {
	StreamID      string     `json:"stream_id"`
	Subscriptions []readySub `json:"subscriptions"`
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   15 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     false,
			MaxIdleConns:          4096,
			MaxConnsPerHost:       0,
			IdleConnTimeout:       90 * time.Second,
			DisableCompression:    true,
			ResponseHeaderTimeout: 120 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

func waitHealthy(ctx context.Context, url string) error {
	client := &http.Client{Timeout: 3 * time.Second}
	var last error
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			last = fmt.Errorf("health %s: status %d", url, resp.StatusCode)
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			if last == nil {
				last = ctx.Err()
			}
			return fmt.Errorf("wait %s: %w", url, last)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func openStream(ctx context.Context, client *http.Client, sluiceURL, token, body string) (*sse.Stream, *readyEvent, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sluiceURL+"/stream", strings.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		resp.Body.Close()
		return nil, nil, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	st := sse.Start(resp.Body)
	ev, err := st.Next(30 * time.Second)
	if err != nil {
		st.Close()
		return nil, nil, fmt.Errorf("ready: %w", err)
	}
	if ev.Name != "ready" {
		st.Close()
		return nil, nil, fmt.Errorf("first event %q, want ready", ev.Name)
	}
	var ready readyEvent
	if err := json.Unmarshal(ev.Data, &ready); err != nil {
		st.Close()
		return nil, nil, fmt.Errorf("ready json: %w", err)
	}
	return st, &ready, nil
}

func postJSON(ctx context.Context, client *http.Client, url, token, body string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

func fetchMetrics(ctx context.Context, client *http.Client, url string) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	return parseProm(string(b)), nil
}

func parseProm(text string) map[string]float64 {
	out := map[string]float64{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		var v float64
		if _, err := fmt.Sscanf(strings.TrimSpace(rest), "%f", &v); err != nil {
			continue
		}
		out[name] = v
	}
	return out
}

func promSum(m map[string]float64, prefix string) float64 {
	var sum float64
	for k, v := range m {
		if k == prefix || strings.HasPrefix(k, prefix+"{") {
			sum += v
		}
	}
	return sum
}

func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
