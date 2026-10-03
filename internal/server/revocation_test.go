package server

import (
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/auth"
	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
)

const bannedUser = "6f1c1a52-6d4e-4a3e-9d0e-2b8f2f6f0a11"

func revocationServer(t *testing.T) (*Server, *pgoutput.Relation) {
	t.Helper()
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	s.cfg.RevocationEnabled = true
	s.cfg.SessionsTable = "auth.sessions"
	s.cfg.UsersTable = "auth.users"
	s.revoker = auth.NewRevoker(0)
	users := walRelation(t, 300, "auth", "users",
		walColumn{1, "id", 2950}, walColumn{0, "banned_until", 1184})
	s.OnRelation(nil, users)
	return s, users
}

// updateUser replays an UPDATE of auth.users; a zero bannedUntil is NULL.
func updateUser(t *testing.T, s *Server, users *pgoutput.Relation, bannedUntil time.Time) {
	t.Helper()
	banned := pgoutput.Column{Kind: pgoutput.ColNull}
	if !bannedUntil.IsZero() {
		banned = pgoutput.Column{Kind: pgoutput.ColText,
			Data: []byte(bannedUntil.UTC().Format("2006-01-02 15:04:05.999999-07"))}
	}
	m := &pgoutput.Message{Type: pgoutput.MsgUpdate, RelationOID: users.OID, New: &pgoutput.Tuple{
		Columns: []pgoutput.Column{{Kind: pgoutput.ColText, Data: []byte(bannedUser)}, banned},
	}}
	if err := s.OnChange(m, users, 0x10, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// The auth service leaves banned_until set when a timed ban runs out. An
// unrelated UPDATE of that user afterwards (a sign-in) must not ban them again.
func TestExpiredBanIsNotABan(t *testing.T) {
	s, users := revocationServer(t)
	id := authz.Identity{Sub: bannedUser, Role: "authenticated"}
	st := s.hub.Open("n1.1", id)

	updateUser(t, s, users, time.Now().Add(-time.Hour))

	if code := st.CloseCode(); code != "" {
		t.Fatalf("stream closed with %q by a ban that had already run out", code)
	}
	if s.revoker.Revoked(id) {
		t.Fatal("an expired ban still refuses the user's tokens")
	}
}

func TestBanLastsUntilItIsLifted(t *testing.T) {
	s, users := revocationServer(t)
	id := authz.Identity{Sub: bannedUser, Role: "authenticated"}
	st := s.hub.Open("n1.1", id)

	updateUser(t, s, users, time.Now().Add(time.Hour))
	if code := st.CloseCode(); code != "user_banned" {
		t.Fatalf("stream closed with %q, want user_banned", code)
	}
	if !s.revoker.Revoked(id) {
		t.Fatal("a ban in force must refuse the user's tokens")
	}

	updateUser(t, s, users, time.Time{})
	if s.revoker.Revoked(id) {
		t.Fatal("a lifted ban still refuses the user's tokens")
	}
}

func TestBanEndsWithItsTime(t *testing.T) {
	r := auth.NewRevoker(0)
	id := authz.Identity{Sub: bannedUser}
	r.BanUser(bannedUser, time.Now().Add(50*time.Millisecond))
	if !r.Revoked(id) {
		t.Fatal("ban not in force")
	}
	time.Sleep(80 * time.Millisecond)
	if r.Revoked(id) {
		t.Fatal("a ban is enforced past its banned_until")
	}
}

func TestBanExpiry(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		in     expr.Value
		active bool
	}{
		{expr.Null, false},
		{expr.Text("2026-10-03 11:59:59.5+00"), false},
		{expr.Text("2026-10-03 12:00:00+00"), false},
		{expr.Text("2026-10-03 14:30:00+02"), true},
		{expr.Text("infinity"), true},
		{expr.Text("-infinity"), false},
		{expr.Text("not a time"), true},
	} {
		if _, active := banExpiry(c.in, now); active != c.active {
			t.Errorf("banExpiry(%q) active = %v, want %v", c.in.String(), active, c.active)
		}
	}
}
