// Command sluice is a realtime data-streaming server for PostgreSQL.
//
// It requires nothing installed in the database: no extensions, no tables, no
// functions, no schemas. Four server settings, two roles and a publication.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pauserratgutierrez/sluice/internal/auth"
	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/metrics"
	"github.com/pauserratgutierrez/sluice/internal/reader"
	"github.com/pauserratgutierrez/sluice/internal/server"
)

var version = "dev"

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe the local server and exit")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("sluice", version)
		return
	}
	if *healthcheck {
		os.Exit(runHealthcheck())
	}

	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sluice:", err)
		os.Exit(1)
	}
}

func runHealthcheck() int {
	addr := os.Getenv("SLUICE_LISTEN_ADDR")
	if addr == "" {
		addr = "127.0.0.1:4000"
	}
	if h, p, err := net.SplitHostPort(addr); err == nil {
		if h == "" || h == "0.0.0.0" || h == "::" {
			addr = net.JoinHostPort("127.0.0.1", p)
		}
	}
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get("http://" + addr + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg)
	log.Info("starting", "version", version, "node_id", cfg.NodeID)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---- database pool --------------------------------------------------
	poolCfg, err := pgxpool.ParseConfig(cfg.AuthzURL)
	if err != nil {
		return fmt.Errorf("parse SLUICE_DB_AUTHZ_URL: %w", err)
	}
	poolCfg.MaxConns = cfg.PoolMax
	poolCfg.MinConns = cfg.PoolMin
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	if cfg.ParanoidPool {
		// Belt and braces. All impersonation is already transaction-scoped, so
		// PostgreSQL unwinds it at COMMIT/ROLLBACK regardless; this only guards
		// against a future code path forgetting that.
		poolCfg.AfterRelease = func(c *pgx.Conn) bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := c.Exec(ctx, "DISCARD ALL")
			return err == nil
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("connect authz pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	// ---- startup validation ---------------------------------------------
	// Two of these checks catch configurations that break the APPLICATION's own
	// writes, not just replication, so they are fatal rather than advisory.
	if err := validate(ctx, pool, cfg, log); err != nil {
		return err
	}

	// ---- catalog ---------------------------------------------------------
	cat := catalog.New(pool)
	if err := cat.Refresh(ctx, cfg.Publication); err != nil {
		return fmt.Errorf("load catalog: %w", err)
	}
	log.Info("catalog loaded", "relations", len(cat.All()), "publication", cfg.Publication)

	// ---- auth ------------------------------------------------------------
	verifier := auth.NewVerifier(cfg.JWKSURL, cfg.JWTAlg, cfg.JWTIssuer, cfg.JWTAudience,
		cfg.JWTLeeway, cfg.JWKSRefresh, cfg.AllowedRoles)
	if err := verifier.Refresh(ctx); err != nil {
		var warn *auth.WarnSymmetricKey
		if !errors.As(err, &warn) {
			return fmt.Errorf("load JWKS from %s: %w", cfg.JWKSURL, err)
		}
		log.Warn("JWKS hygiene", "warning", warn.Error())
	}
	log.Info("JWKS loaded", "url", cfg.JWKSURL, "alg", cfg.JWTAlg)

	revoker := auth.NewRevoker(0)

	// ---- hub, authorizer, server ----------------------------------------
	h := hub.New(cfg.StreamQueue, cfg.RingEvents, cfg.RingMaxAge, cfg.PresenceBcast)
	go h.Presence().Run()
	defer h.Presence().Stop()

	az := authz.New(pool, cat, authz.Options{
		Lease:           cfg.AuthzLease,
		TierC:           cfg.TierC,
		MaxProbesPerSec: cfg.TierCMaxProbes,
		VerifySamples:   cfg.TierBVerify,
	})
	// A compiled predicate disagreeing with PostgreSQL is the most serious thing
	// that can go wrong in Sluice, so it is logged at ERROR and counted, not
	// buried in a debug line.
	az.OnDowngrade = func(rel *catalog.Relation, predicateSQL string, goSaid, pgSaid bool) {
		metrics.AuthzDowngrades.WithLabelValues(rel.Schema, rel.Name).Inc()
		log.Error("compiled predicate disagreed with PostgreSQL; downgrading to per-change impersonation",
			"relation", rel.FullName(),
			"predicate", predicateSQL,
			"sluice_said", goSaid,
			"postgres_said", pgSaid)
	}

	srv := server.New(ctx, server.Options{
		Config: cfg, Logger: log, Pool: pool,
		Catalog: cat, Authz: az, Hub: h,
		Verify: verifier, Revoker: revoker,
	})

	rd := reader.New(cfg, pool, log, srv)
	srv.SetReader(rd)

	if created, err := rd.EnsureSlot(ctx); err != nil {
		return err
	} else if created {
		log.Info("created replication slot", "slot", cfg.SlotName)
	} else {
		log.Info("using existing replication slot", "slot", cfg.SlotName)
	}

	// ---- background loops -----------------------------------------------
	// The shared heartbeat wheel: one timer for every stream on the node.
	go srv.Run(ctx)

	go func() {
		t := time.NewTicker(cfg.CatalogRefresh)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			// A policy change produces no Relation message, so the periodic
			// refresh is the only way to notice one -- and a policy change must be
			// able to REVOKE access, not just grant it. Loading it into the cache
			// is only half of that: RefreshLeases is what acts on it, by comparing
			// the catalog's authorization version against the one the live
			// decisions were resolved at. So these two calls belong together, in
			// this order.
			if err := cat.Refresh(ctx, cfg.Publication); err != nil {
				log.Warn("catalog refresh failed", "err", err)
			}
			srv.RefreshLeases(ctx)
			verifier.EnsureFresh(ctx)
			revoker.Sweep()
		}
	}()

	// ---- reader ----------------------------------------------------------
	readerDone := make(chan error, 1)
	go func() {
		// A single advisory lock elects one reader per slot. Without it, N
		// processes would each hold a slot, and every slot independently retains
		// WAL -- multiplying the disk-exhaustion risk rather than sharing load.
		if ok, err := acquireLeadership(ctx, pool, cfg.SlotName, log); err != nil {
			readerDone <- err
			return
		} else if !ok {
			log.Info("another node holds the reader lock; serving streams only")
			metrics.ReaderIsLeader.Set(0)
			<-ctx.Done()
			readerDone <- nil
			return
		}
		metrics.ReaderIsLeader.Set(1)
		readerDone <- rd.Run(ctx)
	}()

	// ---- HTTP ------------------------------------------------------------
	httpSrv := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: srv.Handler(),
		// No write timeout: SSE streams are long-lived by definition, and each
		// write is bounded individually with SetWriteDeadline instead.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       0,
	}
	httpDone := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.ListenAddr, "prefix", cfg.PathPrefix)
		err := httpSrv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		httpDone <- err
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-readerDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("reader stopped", "err", err)
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Shutdown)
			defer cancel()
			_ = httpSrv.Shutdown(shutdownCtx)
			return err
		}
	case err := <-httpDone:
		if err != nil {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Shutdown)
	defer cancel()
	return httpSrv.Shutdown(shutdownCtx)
}

// acquireLeadership takes a session-scoped advisory lock keyed by the slot name.
func acquireLeadership(ctx context.Context, pool *pgxpool.Pool, slot string, log *slog.Logger) (bool, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return false, err
	}
	// Deliberately not released: the lock must be held for the reader's whole
	// lifetime, and the connection dying is exactly the signal that should free it.
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, "sluice:"+slot).Scan(&ok); err != nil {
		conn.Release()
		return false, err
	}
	if !ok {
		conn.Release()
	}
	return ok, nil
}

func newLogger(cfg *config.Config) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(cfg.LogFormat, "text") {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}
