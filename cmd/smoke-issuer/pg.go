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
