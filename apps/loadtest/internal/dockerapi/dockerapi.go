// Package dockerapi is the little of the Docker Engine API the runner uses,
// over the daemon's unix socket: finding the containers of its own compose
// project, restarting Sluice, and reading each container's CPU and memory.
//
// The socket is root-equivalent on the host; it is mounted into the runner
// of the production-shaped profiles only, and only the calls below are made.
package dockerapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type Client struct {
	http *http.Client
}

// New returns a client for the socket, or nil when it is not there.
func New(socket string) *Client {
	if socket == "" {
		socket = "/var/run/docker.sock"
	}
	if _, err := os.Stat(socket); err != nil {
		return nil
	}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
		MaxIdleConnsPerHost: 8,
	}
	return &Client{http: &http.Client{Transport: tr, Timeout: 90 * time.Second}}
}

type Container struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	Labels map[string]string `json:"Labels"`
	State  string            `json:"State"`
}

// Service is the compose service a container runs.
func (c Container) Service() string { return c.Labels["com.docker.compose.service"] }

func (c *Client) do(ctx context.Context, method, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("docker %s %s: %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Project is the compose project of the container this process runs in,
// found from its hostname (Docker sets it to the short container ID).
func (c *Client) Project(ctx context.Context) (string, error) {
	host, err := os.Hostname()
	if err != nil {
		return "", err
	}
	var info struct {
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
	}
	if err := c.do(ctx, http.MethodGet, "/containers/"+url.PathEscape(host)+"/json", &info); err != nil {
		return "", err
	}
	p := info.Config.Labels["com.docker.compose.project"]
	if p == "" {
		return "", fmt.Errorf("container %s has no compose project label", host)
	}
	return p, nil
}

// ProjectContainers lists the running containers of a compose project.
func (c *Client) ProjectContainers(ctx context.Context, project string) ([]Container, error) {
	f, _ := json.Marshal(map[string][]string{"label": {"com.docker.compose.project=" + project}})
	var out []Container
	err := c.do(ctx, http.MethodGet, "/containers/json?filters="+url.QueryEscape(string(f)), &out)
	return out, err
}

// Restart stops a container (SIGTERM, then SIGKILL after timeout) and starts it.
func (c *Client) Restart(ctx context.Context, id string, timeout time.Duration) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/containers/%s/restart?t=%d", url.PathEscape(id), int(timeout.Seconds())), nil)
}

// Usage is one reading of a container's cumulative CPU time and its memory.
type Usage struct {
	At         time.Time
	CPUSeconds float64
	// MemBytes is usage minus inactive file cache, as `docker stats` shows it.
	MemBytes int64
	Limit    int64
}

// Stats reads a container's usage once, without waiting for a second sample.
func (c *Client) Stats(ctx context.Context, id string) (Usage, error) {
	var s struct {
		CPU struct {
			Usage struct {
				Total uint64 `json:"total_usage"`
			} `json:"cpu_usage"`
		} `json:"cpu_stats"`
		Memory struct {
			Usage uint64            `json:"usage"`
			Limit uint64            `json:"limit"`
			Stats map[string]uint64 `json:"stats"`
		} `json:"memory_stats"`
	}
	if err := c.do(ctx, http.MethodGet, "/containers/"+url.PathEscape(id)+"/stats?stream=false&one-shot=true", &s); err != nil {
		return Usage{}, err
	}
	mem := s.Memory.Usage
	inactive := s.Memory.Stats["inactive_file"]
	if inactive == 0 {
		inactive = s.Memory.Stats["total_inactive_file"]
	}
	if inactive < mem {
		mem -= inactive
	}
	return Usage{
		At:         time.Now(),
		CPUSeconds: float64(s.CPU.Usage.Total) / 1e9,
		MemBytes:   int64(mem),
		Limit:      int64(s.Memory.Limit),
	}, nil
}
