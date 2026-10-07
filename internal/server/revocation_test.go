package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/pauserratgutierrez/sluice/internal/auth"
	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/hub"
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
	s.cfg.UsersBanColumn = "banned_until"
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
	st := s.hub.Open("n1.1", "", id)

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
	st := s.hub.Open("n1.1", "", id)

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

// sidVerifier accepts tokens shaped like the identity service's: no `role`, the
// session in `sid`. It returns a signer for such tokens.
func sidVerifier(t *testing.T) (*auth.Verifier, func(sub, sid string) string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	b64 := base64.RawURLEncoding.EncodeToString
	jwks, _ := json.Marshal(map[string]any{"keys": []map[string]string{{
		"kty": "EC", "kid": "k", "crv": "P-256",
		"x": b64(key.X.FillBytes(make([]byte, 32))), "y": b64(key.Y.FillBytes(make([]byte, 32))),
	}}})
	keys := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(jwks) }))
	t.Cleanup(keys.Close)

	v := auth.NewVerifier(keys.URL, "ES256", "", "", time.Second, time.Minute, nil)
	v.SetClaimRules("sid", false)
	if err := v.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	return v, func(sub, sid string) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
			"sub": sub, "sid": sid, "exp": time.Now().Add(time.Hour).Unix(),
		})
		tok.Header["kid"] = "k"
		signed, err := tok.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
}

// Sign-out DELETEs the session row. The stream whose token named that row in
// `sid` closes with session_revoked; the same user's other session does not,
// and the signed-out token can no longer open a stream.
func TestSessionRevocationBySidClaim(t *testing.T) {
	const (
		user      = "0199b1c2-0000-7000-8000-000000000001"
		signedOut = "0199b1c2-7a3e-7c11-9f00-6e5d4c3b2a10"
		other     = "0199b1c2-7a3e-7c11-9f00-6e5d4c3b2a11"
	)
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	s.cfg.RevocationEnabled = true
	s.cfg.SessionsTable = "auth.session"
	s.revoker = auth.NewRevoker(0)
	verify, sign := sidVerifier(t)
	s.verify = verify

	open := func(sid string) (*hub.Stream, string) {
		tok := sign(user, sid)
		id, err := s.verifyToken(context.Background(), "Bearer "+tok)
		if err != nil {
			t.Fatal(err)
		}
		return s.hub.Open("n1."+sid, "", id), tok
	}
	gone, tok := open(signedOut)
	kept, _ := open(other)

	sessions := walRelation(t, 400, "auth", "session",
		walColumn{1, "id", 2950}, walColumn{0, "user_id", 2950})
	s.OnRelation(nil, sessions)
	m := &pgoutput.Message{Type: pgoutput.MsgDelete, RelationOID: sessions.OID, Old: &pgoutput.Tuple{
		Columns: []pgoutput.Column{
			{Kind: pgoutput.ColText, Data: []byte(signedOut)},
			{Kind: pgoutput.ColNull},
		},
	}}
	if err := s.OnChange(m, sessions, 0x10, time.Now()); err != nil {
		t.Fatal(err)
	}

	if code := gone.CloseCode(); code != "session_revoked" {
		t.Fatalf("signed-out stream closed with %q, want session_revoked", code)
	}
	var sawError bool
	for _, ev := range gone.Take(nil) {
		if e, ok := ev.Data.(event.Error); ok && e.Code == "session_revoked" {
			sawError = true
		}
	}
	if !sawError {
		t.Fatal("the signed-out stream got no session_revoked error event")
	}
	if code := kept.CloseCode(); code != "" {
		t.Fatalf("the user's other session closed with %q", code)
	}
	if _, err := s.verifyToken(context.Background(), "Bearer "+tok); !errors.Is(err, errSessionRevoked) {
		t.Fatalf("reconnect with the signed-out token: err = %v, want errSessionRevoked", err)
	}
}
