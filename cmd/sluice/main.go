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
	"sync/atomic"
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
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/reader"
	"github.com/pauserratgutierrez/sluice/internal/server"
)

var version = "dev"

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe the local server's liveness (/healthz) and exit")
	readycheck := flag.Bool("readycheck", false, "probe the local server's readiness (/readyz) and exit")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("sluice", version)
		return
	}
	if *healthcheck {
		os.Exit(probe("/healthz"))
	}
	if *readycheck {
		os.Exit(probe("/readyz"))
	}

	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sluice:", err)
		os.Exit(1)
	}
}

// probe GETs a local health endpoint and returns the exit code for it.
func probe(path string) int {
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
	resp, err := c.Get("http://" + addr + path)
	if err != nil {
		fmt.Fprintln(os.Stderr, path+":", err)
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
	applyMemoryLimit(log)

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
	startupWarnings, err := validate(ctx, pool, cfg, log)
	if err != nil {
		return err
	}

	// ---- catalog ---------------------------------------------------------
	// Without a role claim there are no JWT roles whose memberships matter.
	var roles []string
	if cfg.JWTRequireRole {
		roles = cfg.AllowedRoles
	}
	cat := catalog.New(pool, roles...)
	if err := cat.Refresh(ctx, cfg.Publication); err != nil {
		return fmt.Errorf("load catalog: %w", err)
	}
	log.Info("catalog loaded", "relations", len(cat.All()), "publication", cfg.Publication)

	// ---- auth ------------------------------------------------------------
	verifier := auth.NewVerifier(cfg.JWKSURL, cfg.JWTAlg, cfg.JWTIssuer, cfg.JWTAudience,
		cfg.JWTLeeway, cfg.JWKSRefresh, cfg.AllowedRoles)
	verifier.SetClaimRules(cfg.JWTSessionClaim, cfg.JWTRequireRole)
	if err := verifier.Refresh(ctx); err != nil {
		var warn *auth.WarnSymmetricKey
		if !errors.As(err, &warn) {
			return fmt.Errorf("load JWKS from %s: %w", cfg.JWKSURL, err)
		}
		log.Warn("JWKS hygiene", "warning", warn.Error())
	}
	log.Info("JWKS loaded", "url", cfg.JWKSURL, "alg", cfg.JWTAlg,
		"session_claim", cfg.JWTSessionClaim, "require_role", cfg.JWTRequireRole)

	revoker := auth.NewRevoker(0)

	// ---- hub, authorizer, server ----------------------------------------
	h := hub.New(cfg.StreamQueue, cfg.RingEvents, cfg.RingMaxBytes, cfg.RingMaxAge, cfg.PresenceBcast)
	go h.Presence().Run()
	defer h.Presence().Stop()

	az := authz.New(pool, cat, authz.Options{
		Lease:           cfg.AuthzLease,
		TierC:           cfg.TierC,
		MaxProbesPerSec: cfg.TierCMaxProbes,
		VerifySamples:   cfg.TierBVerify,
		ProbeTimeout:    cfg.TierCTimeout,
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

	orc, err := oracle.New(cfg, az, cat)
	if err != nil {
		return fmt.Errorf("shape oracle: %w", err)
	}
	log.Info("shape oracle", "oracle", orc.Name())

	srv := server.New(ctx, server.Options{
		Config: cfg, Logger: log, Pool: pool,
		Catalog: cat, Authz: az, Oracle: orc, Hub: h,
		Verify: verifier, Revoker: revoker,
		StartupWarnings: startupWarnings,
	})

	rd := reader.New(cfg, pool, log, srv)
	srv.SetReader(rd)

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
			srv.RefreshHealth(ctx)
			verifier.EnsureFresh(ctx)
			revoker.Sweep()
		}
	}()

	// ---- reader ----------------------------------------------------------
	// One process per slot reads it. Others stand by: they retry the lock and
	// report not ready (so a load balancer sends them no streams) until the
	// reader's process goes away and they take over.
	readerDone := make(chan error, 1)
	// The pool waits on Close for every connection it lent out, and the lock's
	// is never given back while the process runs; without this release, a
	// shutdown blocked there until the process was killed. Closing the pool
	// then closes the connection, which releases the lock.
	var lockConn atomic.Pointer[pgxpool.Conn]
	defer func() {
		if c := lockConn.Load(); c != nil {
			c.Release()
		}
	}()
	go func() {
		metrics.ReaderIsLeader.Set(0)
		for standing := false; ; standing = true {
			conn, err := acquireLeadership(ctx, pool, cfg.SlotName)
			if err != nil && ctx.Err() == nil {
				log.Warn("could not try the reader lock", "err", err)
			}
			if conn != nil {
				lockConn.Store(conn)
				break
			}
			if !standing {
				log.Info("another process reads this slot; standing by", "slot", cfg.SlotName)
			}
			select {
			case <-ctx.Done():
				readerDone <- nil
				return
			case <-time.After(5 * time.Second):
			}
		}
		metrics.ReaderIsLeader.Set(1)
		log.Info("holding the reader lock", "slot", cfg.SlotName)
		readerDone <- rd.Run(ctx)
	}()

	// ---- HTTP ------------------------------------------------------------
	httpSrv := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: srv.Handler(),
		// No write timeout: SSE streams are long-lived by definition, and each
		// write is bounded individually with SetWriteDeadline instead.
		ReadHeaderTimeout: 10 * time.Second,
		// Applies only between requests on a kept-alive connection, never to a
		// stream in progress. Longer than a reverse proxy's own idle timeout
		// (Caddy's and nginx's are shorter), so the proxy closes first and
		// never reuses a connection Sluice is closing.
		IdleTimeout: 5 * time.Minute,
	}
	// Shutdown waits for handlers to return; Drain is what ends the streams.
	httpSrv.RegisterOnShutdown(srv.Drain)
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

// acquireLeadership tries a session-scoped advisory lock keyed by the slot name,
// and returns the connection holding it, or nil when another process holds it.
//
// The caller keeps that connection for the reader's whole lifetime and
// releases it only as the pool closes: the lock must last as long as the
// reader, and the connection dying is exactly the signal that should free it.
// It permanently takes one of SLUICE_DB_POOL_MAX_CONNS. The slot itself is the
// hard guarantee -- PostgreSQL lets only one connection stream it -- and the
// lock keeps a standby from contending for it.
func acquireLeadership(ctx context.Context, pool *pgxpool.Pool, slot string) (*pgxpool.Conn, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, "sluice:"+slot).Scan(&ok); err != nil {
		conn.Release()
		return nil, err
	}
	if !ok {
		conn.Release()
		return nil, nil
	}
	return conn, nil
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
