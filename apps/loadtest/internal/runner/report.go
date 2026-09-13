package runner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/host"
)

type Report struct {
	Target          string         `json:"target"`
	Scenario        string         `json:"scenario"`
	Host            host.Resources `json:"host"`
	Cap             int            `json:"stream_cap"`
	MaxUsers        int            `json:"max_concurrent_users"`
	MaxConnsPerUser int            `json:"max_conns_per_user"`
	StartedAt       time.Time      `json:"started_at"`
	FinishedAt      time.Time      `json:"finished_at"`
	Steps           []Step         `json:"steps"`
}

func writeReport(dir, scenario string, r Report) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	stamp := r.FinishedAt.UTC().Format("20060102T150405Z")
	base := fmt.Sprintf("%s-%s", scenario, stamp)
	jsonPath := filepath.Join(dir, base+".json")
	mdPath := filepath.Join(dir, base+".md")
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(jsonPath, append(raw, '\n'), 0o644); err != nil {
		return "", err
	}
	if err := os.WriteFile(mdPath, []byte(markdown(r)), 0o644); err != nil {
		return "", err
	}
	latest := filepath.Join(dir, scenario+"-latest.json")
	_ = os.WriteFile(latest, append(raw, '\n'), 0o644)
	return jsonPath, nil
}

func markdown(r Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Sluice load-test — %s\n\n", r.Scenario)
	fmt.Fprintf(&b, "- Target: `%s`\n", r.Target)
	fmt.Fprintf(&b, "- Host: %.0f CPUs, %d MiB visible\n", r.Host.CPUs, r.Host.MemoryBytes/1024/1024)
	fmt.Fprintf(&b, "- Stream cap (safety): %d\n", r.Cap)
	fmt.Fprintf(&b, "- Started: %s\n", r.StartedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "- Finished: %s\n\n", r.FinishedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "## Capacity\n\n")
	fmt.Fprintf(&b, "| Axis | Max that passed |\n| --- | ---: |\n")
	fmt.Fprintf(&b, "| Concurrent users (1 conn/user) | %d |\n", r.MaxUsers)
	fmt.Fprintf(&b, "| Connections per user (1 user) | %d |\n\n", r.MaxConnsPerUser)
	fmt.Fprintf(&b, "## Steps\n\n")
	fmt.Fprintf(&b, "| Step | Users | Conns/user | Opened | Delivery | events/s | p95 | Pass |\n")
	fmt.Fprintf(&b, "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |\n")
	for _, s := range r.Steps {
		fmt.Fprintf(&b, "| %s | %d | %d | %d/%d | %.1f%% | %.0f | %s | %t |\n",
			s.Name, s.Users, s.ConnsPerUser, s.Opened, s.Streams, s.DeliveryPct, s.EventsPerSec, s.LatencyP95, s.Pass)
	}
	b.WriteByte('\n')
	return b.String()
}

func printSummary(r Report) {
	fmt.Println()
	fmt.Printf("== %s capacity against %s ==\n", r.Scenario, r.Target)
	fmt.Printf("  max concurrent users (1 conn/user): %d\n", r.MaxUsers)
	fmt.Printf("  max conns per user (1 user):        %d\n", r.MaxConnsPerUser)
	fmt.Printf("  duration: %s\n", r.FinishedAt.Sub(r.StartedAt).Round(time.Second))
}
