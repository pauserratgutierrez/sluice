package main

import (
	"context"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	poolOnce sync.Once
	pool     *pgxpool.Pool
	poolErr  error
)

// execViaPgx runs DML as a superuser, deliberately bypassing Sluice.
//
// The test must never use the system under test to arrange its own preconditions,
// otherwise a bug that swallows writes would make the assertions pass.
func execViaPgx(ctx context.Context, sql string) error {
	poolOnce.Do(func() {
		url := os.Getenv("SMOKE_DB_URL")
		if url == "" {
			url = "postgres://postgres:" + os.Getenv("POSTGRES_PASSWORD") + "@db:5432/postgres"
		}
		pool, poolErr = pgxpool.New(ctx, url)
	})
	if poolErr != nil {
		return poolErr
	}
	_, err := pool.Exec(ctx, sql)
	return err
}

// countAll counts rows as a superuser, i.e. with RLS bypassed. Used to prove that
// the rows Sluice withheld genuinely exist rather than never having been written.
func countAll(ctx context.Context, table string) (int, error) {
	if err := execViaPgx(ctx, "select 1"); err != nil {
		return 0, err
	}
	var n int
	err := pool.QueryRow(ctx, "select count(*) from "+table).Scan(&n)
	return n, err
}

// queryOne runs a single-value query as a superuser.
func queryOne(ctx context.Context, sql string, arg any, dest any) error {
	if err := execViaPgx(ctx, "select 1"); err != nil {
		return err
	}
	return pool.QueryRow(ctx, sql, arg).Scan(dest)
}
