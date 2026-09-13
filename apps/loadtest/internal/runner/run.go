package runner

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/authn"
	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/host"
	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/hunt"
)

func Main() error {
	cfg := LoadConfig()
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	res := host.Detect()
	if cfg.Cap <= 0 {
		cfg.Cap = host.StreamCap(res)
	}
	if cfg.Start > cfg.Cap {
		cfg.Start = max(cfg.Cap/4, 25)
	}

	log.Info("loadtest start",
		"scenario", cfg.Scenario,
		"target", cfg.TargetImage,
		"sluice", cfg.SluiceURL,
		"cpus", res.CPUs,
		"memory_mb", res.MemoryBytes/1024/1024,
		"cap", cfg.Cap,
		"hunt", cfg.Hunt,
		"axis", cfg.Axis,
	)

	health := strings.TrimSuffix(cfg.SluiceURL, "/sluice/v1") + "/healthz"
	waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer waitCancel()
	if err := waitHealthy(waitCtx, health); err != nil {
		return err
	}

	signer, err := authn.Load(cfg.PrivateJWK)
	if err != nil {
		return err
	}
	pool, err := connectDB(ctx, cfg.DBURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := requireSchema(ctx, pool); err != nil {
		return err
	}

	a := &app{
		cfg:      cfg,
		signer:   signer,
		pool:     pool,
		http:     newHTTPClient(),
		scenario: selectScenario(cfg.Scenario, cfg.Channel),
	}

	rep := Report{
		Target:    cfg.TargetImage,
		Scenario:  cfg.Scenario,
		Host:      res,
		Cap:       cfg.Cap,
		StartedAt: time.Now().UTC(),
	}

	wuUsers, wuConns, wuChanges := a.warmup()
	warmUsers, err := mintUsers(signer, wuUsers, cfg.TokenTTL)
	if err != nil {
		return err
	}
	log.Info("warmup", "users", wuUsers, "conns", wuConns, "changes", wuChanges)
	warm, err := a.runStep(ctx, "warmup", warmUsers, wuConns, wuChanges)
	if err != nil {
		return err
	}
	printStep(log, warm)
	rep.Steps = append(rep.Steps, warm)
	if !warm.Pass {
		rep.FinishedAt = time.Now().UTC()
		if _, err := writeReport(cfg.ResultsDir, cfg.Scenario, rep); err != nil {
			log.Error("write report", "err", err)
		}
		return fmt.Errorf("warmup failed: %s", warm.FailReason)
	}

	axes := axesFor(cfg)
	for _, axis := range axes {
		if ctx.Err() != nil {
			break
		}
		log.Info("axis", "name", axis)
		switch axis {
		case "users":
			maxN, steps, err := a.runAxis(ctx, log, "users", cfg, func(n int, users []User) (Step, error) {
				return a.runStep(ctx, fmt.Sprintf("users-%d", n), users[:n], 1, cfg.Changes)
			})
			if err != nil {
				return err
			}
			rep.Steps = append(rep.Steps, steps...)
			rep.MaxUsers = maxN
		case "conns":
			users, err := mintUsers(signer, 1, cfg.TokenTTL)
			if err != nil {
				return err
			}
			maxN, steps, err := a.runAxis(ctx, log, "conns", cfg, func(n int, _ []User) (Step, error) {
				return a.runStep(ctx, fmt.Sprintf("conns-%d", n), users, n, cfg.Changes)
			})
			if err != nil {
				return err
			}
			rep.Steps = append(rep.Steps, steps...)
			rep.MaxConnsPerUser = maxN
		}
	}

	rep.FinishedAt = time.Now().UTC()
	path, err := writeReport(cfg.ResultsDir, cfg.Scenario, rep)
	if err != nil {
		return err
	}
	log.Info("wrote report", "path", path)
	printSummary(rep)
	return nil
}

func (a *app) runAxis(ctx context.Context, log *slog.Logger, axis string, cfg Config, run func(n int, users []User) (Step, error)) (int, []Step, error) {
	var steps []Step
	probe := func(n int) (bool, error) {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		var users []User
		var err error
		if axis == "users" {
			users, err = mintUsers(a.signer, n, cfg.TokenTTL)
			if err != nil {
				return false, err
			}
		}
		log.Info("probe", "axis", axis, "n", n)
		st, err := run(n, users)
		if err != nil {
			return false, err
		}
		printStep(log, st)
		steps = append(steps, st)
		return st.Pass, nil
	}

	if !cfg.Hunt {
		maxPass := 0
		for _, n := range cfg.Ladder {
			if n > cfg.Cap {
				n = cfg.Cap
			}
			pass, err := probe(n)
			if err != nil {
				return maxPass, steps, err
			}
			if pass {
				maxPass = n
			}
		}
		return maxPass, steps, nil
	}

	maxN, _, err := hunt.Search(hunt.Config{
		Start:       cfg.Start,
		Cap:         cfg.Cap,
		Granularity: cfg.Granularity,
	}, probe)
	return maxN, steps, err
}

func selectScenario(name, channel string) scenario {
	if channel == "" {
		channel = "room:load"
	}
	room := fmt.Sprintf(`{"sub":"room","channel":%q}`, channel)
	roomP := fmt.Sprintf(`{"sub":"room","channel":%q,"presence":true}`, channel)
	switch name {
	case "rls-b":
		return scenario{
			Name:       "rls-b",
			Kind:       kindPosts,
			Plane:      planeChange,
			ExpectTier: "B",
			SubscribeJSON: func(u User) string {
				return fmt.Sprintf(`{"subscriptions":[{"sub":"p","shape":{"schema":"public","table":"posts","filter":"owner_id=eq.%s"}}]}`, u.ID)
			},
			Write: func(ctx context.Context, pool *pgxpool.Pool, u User, title string) error {
				return insertPost(ctx, pool, u.ID, title)
			},
		}
	case "rls-c":
		return scenario{
			Name:       "rls-c",
			Kind:       kindInvoices,
			Plane:      planeChange,
			ExpectTier: "C",
			SubscribeJSON: func(u User) string {
				return fmt.Sprintf(`{"subscriptions":[{"sub":"i","shape":{"schema":"public","table":"invoices","filter":"team_id=eq.%d"}}]}`, u.TeamID)
			},
			Write: func(ctx context.Context, pool *pgxpool.Pool, u User, title string) error {
				return insertInvoice(ctx, pool, u.TeamID, title)
			},
		}
	case "issuer", "kick":
		sc := scenario{
			Name:         name,
			Kind:         kindProjectDocs,
			Plane:        planeChange,
			ExpectOracle: "issuer",
			SubscribeJSON: func(u User) string {
				return fmt.Sprintf(`{"subscriptions":[{"sub":"d","shape":{"schema":"public","table":"project_docs","filter":"project_id=eq.%s"}}]}`, u.ProjectID)
			},
			Write: func(ctx context.Context, pool *pgxpool.Pool, u User, title string) error {
				return insertProjectDoc(ctx, pool, u.ProjectID, title)
			},
		}
		if name == "kick" {
			sc.Plane = planeKick
		}
		return sc
	case "broadcast":
		return scenario{
			Name:    "broadcast",
			Kind:    kindChannel,
			Plane:   planeBroadcast,
			Channel: channel,
			SubscribeJSON: func(User) string {
				return `{"subscriptions":[` + room + `]}`
			},
		}
	case "presence":
		return scenario{
			Name:    "presence",
			Kind:    kindChannel,
			Plane:   planePresence,
			Channel: channel,
			SubscribeJSON: func(User) string {
				return `{"subscriptions":[` + roomP + `]}`
			},
		}
	case "mixed":
		return scenario{
			Name:       "mixed",
			Kind:       kindNotes,
			Plane:      planeMixed,
			Channel:    channel,
			ExpectTier: "A",
			SubscribeJSON: func(u User) string {
				return fmt.Sprintf(`{"subscriptions":[{"sub":"n","shape":{"schema":"public","table":"notes","filter":"owner_id=eq.%s"}},%s]}`, u.ID, room)
			},
			Write: func(ctx context.Context, pool *pgxpool.Pool, u User, title string) error {
				return insertNote(ctx, pool, u.ID, title)
			},
		}
	default: // rls-a
		return scenario{
			Name:       "rls-a",
			Kind:       kindNotes,
			Plane:      planeChange,
			ExpectTier: "A",
			SubscribeJSON: func(u User) string {
				return fmt.Sprintf(`{"subscriptions":[{"sub":"n","shape":{"schema":"public","table":"notes","filter":"owner_id=eq.%s"}}]}`, u.ID)
			},
			Write: func(ctx context.Context, pool *pgxpool.Pool, u User, title string) error {
				return insertNote(ctx, pool, u.ID, title)
			},
		}
	}
}

func (a *app) warmup() (users, conns, changes int) {
	switch a.scenario.Plane {
	case planePresence, planeKick:
		return 10, 1, 5
	case planeBroadcast, planeMixed:
		return 8, 1, 5
	default:
		return 1, 25, 5
	}
}

func axesFor(cfg Config) []string {
	switch cfg.Axis {
	case "users":
		return []string{"users"}
	case "conns":
		return []string{"conns"}
	}
	switch cfg.Scenario {
	case "presence", "kick":
		return []string{"users"}
	default:
		return []string{"users", "conns"}
	}
}

func printStep(log *slog.Logger, st Step) {
	log.Info("step",
		"name", st.Name,
		"pass", st.Pass,
		"opened", fmt.Sprintf("%d/%d", st.Opened, st.Streams),
		"delivery", fmt.Sprintf("%.1f%%", st.DeliveryPct),
		"events_s", fmt.Sprintf("%.0f", st.EventsPerSec),
		"p95", st.LatencyP95,
		"client_bcast", st.Broadcasts,
		"db_bcast", st.DBBroadcasts,
		"presence", st.PresenceReady,
		"kicked", st.Kicked,
		"tier", st.ObservedTier,
		"oracle", st.ObservedOracle,
		"reason", st.FailReason,
	)
}
