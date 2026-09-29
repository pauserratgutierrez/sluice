package main

import (
	"context"
	"fmt"
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

// insertReturningID runs an INSERT ... RETURNING id as a superuser.
func insertReturningID(ctx context.Context, sql string) (int64, error) {
	if err := execViaPgx(ctx, "select 1"); err != nil {
		return 0, err
	}
	var id int64
	err := pool.QueryRow(ctx, sql).Scan(&id)
	return id, err
}

// toJSONB returns PostgreSQL's own JSON encoding of one row, in UTC.
func toJSONB(ctx context.Context, table string, id int64) ([]byte, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL TimeZone = 'UTC'`); err != nil {
		return nil, err
	}
	var raw []byte
	err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT to_jsonb(r)::text FROM %s r WHERE id = $1`, table), id).Scan(&raw)
	return raw, err
}

// queryOne runs a single-value query as a superuser.
func queryOne(ctx context.Context, sql string, arg any, dest any) error {
	if err := execViaPgx(ctx, "select 1"); err != nil {
		return err
	}
	return pool.QueryRow(ctx, sql, arg).Scan(dest)
}
