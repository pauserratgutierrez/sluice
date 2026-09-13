package runner

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func connectDB(ctx context.Context, url string) (*pgxpool.Pool, error) {
	if url == "" {
		pass := os.Getenv("POSTGRES_PASSWORD")
		url = "postgres://postgres:" + pass + "@db:5432/postgres"
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 32
	cfg.MinConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func requireSchema(ctx context.Context, pool *pgxpool.Pool) error {
	var name *string
	if err := pool.QueryRow(ctx, `select to_regclass('public.notes')::text`).Scan(&name); err != nil {
		return err
	}
	if name == nil {
		return fmt.Errorf("schema missing (public.notes); docker compose down -v and up again")
	}
	return nil
}

func truncateApp(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		truncate table
			public.notes,
			public.posts,
			public.invoices,
			public.team_members,
			public.project_docs,
			public.project_members
		restart identity`)
	return err
}

func seedTeamRows(ctx context.Context, pool *pgxpool.Pool, users []User) error {
	if len(users) == 0 {
		return nil
	}
	rows := make([][]any, len(users))
	for i, u := range users {
		rows[i] = []any{u.ID, u.TeamID}
	}
	_, err := pool.CopyFrom(ctx,
		pgx.Identifier{"public", "team_members"},
		[]string{"user_id", "team_id"},
		pgx.CopyFromRows(rows),
	)
	return err
}

func seedProjects(ctx context.Context, pool *pgxpool.Pool, users []User) error {
	if len(users) == 0 {
		return nil
	}
	rows := make([][]any, len(users))
	for i, u := range users {
		rows[i] = []any{u.ProjectID, u.ID}
	}
	_, err := pool.CopyFrom(ctx,
		pgx.Identifier{"public", "project_members"},
		[]string{"project_id", "user_id"},
		pgx.CopyFromRows(rows),
	)
	return err
}

func insertNote(ctx context.Context, pool *pgxpool.Pool, ownerID, title string) error {
	_, err := pool.Exec(ctx, `insert into public.notes (owner_id, title) values ($1, $2)`, ownerID, title)
	return err
}

func insertPost(ctx context.Context, pool *pgxpool.Pool, ownerID, title string) error {
	_, err := pool.Exec(ctx, `insert into public.posts (owner_id, visibility, title) values ($1, 'private', $2)`, ownerID, title)
	return err
}

func insertInvoice(ctx context.Context, pool *pgxpool.Pool, team int, title string) error {
	_, err := pool.Exec(ctx, `insert into public.invoices (team_id, amount, note) values ($1, 10.00, $2)`, team, title)
	return err
}

func insertProjectDoc(ctx context.Context, pool *pgxpool.Pool, projectID, title string) error {
	_, err := pool.Exec(ctx, `insert into public.project_docs (project_id, title) values ($1, $2)`, projectID, title)
	return err
}

func emitBroadcast(ctx context.Context, pool *pgxpool.Pool, channel, payload string) error {
	_, err := pool.Exec(ctx,
		`select pg_logical_emit_message(true, $1, $2)`,
		"sluice:"+channel, payload)
	return err
}

func deleteHold(ctx context.Context, pool *pgxpool.Pool, userID string) error {
	_, err := pool.Exec(ctx, `delete from public.project_members where user_id = $1::uuid`, userID)
	return err
}
