package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func b64u(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// jwksServer serves one P-256 key as "ec" and one RSA key as "rsa", counting
// fetches.
func jwksServer(t *testing.T, ec *ecdsa.PublicKey, rs *rsa.PublicKey) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	body, _ := json.Marshal(map[string]any{"keys": []map[string]string{
		{"kty": "EC", "kid": "ec", "crv": "P-256", "x": b64u(ec.X.FillBytes(make([]byte, 32))), "y": b64u(ec.Y.FillBytes(make([]byte, 32)))},
		{"kty": "RSA", "kid": "rsa", "n": b64u(rs.N.Bytes()), "e": b64u(big.NewInt(int64(rs.E)).Bytes())},
	}})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func sign(t *testing.T, key *ecdsa.PrivateKey, kid string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"role": "authenticated", "sub": "u1", "aud": "authenticated",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	tok.Header["kid"] = kid
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestVerifierLoadsOnlyKeysForThePinnedAlgorithm(t *testing.T) {
	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	rs, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv, _ := jwksServer(t, &ec.PublicKey, &rs.PublicKey)

	v := NewVerifier(srv.URL, "ES256", "", "authenticated", time.Second, time.Minute, []string{"authenticated"})
	if err := v.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := v.keys["rsa"]; ok {
		t.Error("an RSA key was loaded although ES256 is pinned")
	}
	if _, ok := v.keys["ec"]; !ok {
		t.Fatal("the P-256 key was not loaded")
	}
	if _, err := v.Verify(context.Background(), "Bearer "+sign(t, ec, "ec")); err != nil {
		t.Fatalf("a valid token was rejected: %v", err)
	}
}

// An unknown kid may mean a key rotation, so it triggers a refetch -- but at
// most one per interval, or any unauthenticated caller could make Sluice hit
// the JWKS endpoint once per request.
func TestUnknownKidRefetchIsThrottled(t *testing.T) {
	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	rs, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv, hits := jwksServer(t, &ec.PublicKey, &rs.PublicKey)

	v := NewVerifier(srv.URL, "ES256", "", "authenticated", time.Second, time.Minute, []string{"authenticated"})
	if err := v.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := hits.Load()
	for i := 0; i < 20; i++ {
		if _, err := v.Verify(context.Background(), sign(t, ec, "nope")); err == nil {
			t.Fatal("a token with an unknown kid was accepted")
		}
	}
	if got := hits.Load() - before; got != 1 {
		t.Fatalf("20 unknown-kid tokens caused %d JWKS fetches, want 1", got)
	}
}

// signClaims signs arbitrary claims with the "ec" key.
func signClaims(t *testing.T, key *ecdsa.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()
	claims["exp"] = time.Now().Add(time.Hour).Unix()
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	tok.Header["kid"] = "ec"
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRoleIsOptionalOnlyWhenNotRequired(t *testing.T) {
	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	rs, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv, _ := jwksServer(t, &ec.PublicKey, &rs.PublicKey)
	ctx := context.Background()
	noRole := signClaims(t, ec, jwt.MapClaims{"sub": "u1", "aud": "authenticated"})

	v := NewVerifier(srv.URL, "ES256", "", "authenticated", time.Second, time.Minute, []string{"authenticated"})
	if err := v.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(ctx, noRole); err == nil {
		t.Fatal("a token without role was accepted while the role is required")
	}

	v.SetClaimRules("sid", false)
	id, err := v.Verify(ctx, noRole)
	if err != nil {
		t.Fatalf("a token without role was rejected while the role is optional: %v", err)
	}
	if id.Role != "" {
		t.Fatalf("Role = %q, want empty", id.Role)
	}

	// The claim is ignored, not just tolerated: a token cannot claim its way
	// into service_role.
	id, err = v.Verify(ctx, signClaims(t, ec, jwt.MapClaims{"sub": "u1", "role": "service_role"}))
	if err != nil {
		t.Fatal(err)
	}
	if id.Role != "" {
		t.Fatalf("Role = %q from an ignored role claim, want empty", id.Role)
	}
}

func TestSessionClaimIsConfigurableAndLowercased(t *testing.T) {
	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	rs, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv, _ := jwksServer(t, &ec.PublicKey, &rs.PublicKey)
	ctx := context.Background()
	const sid = "0199B1C2-7A3E-7C11-9F00-6E5D4C3B2A10"
	tok := signClaims(t, ec, jwt.MapClaims{
		"sub": "u1", "role": "authenticated", "sid": sid, "session_id": "other",
	})

	v := NewVerifier(srv.URL, "ES256", "", "", time.Second, time.Minute, []string{"authenticated"})
	if err := v.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := v.Verify(ctx, tok)
	if err != nil {
		t.Fatal(err)
	}
	if id.SessionID != "other" {
		t.Fatalf("default SessionID = %q, want the session_id claim", id.SessionID)
	}

	v.SetClaimRules("sid", true)
	if id, err = v.Verify(ctx, tok); err != nil {
		t.Fatal(err)
	}
	if want := strings.ToLower(sid); id.SessionID != want {
		t.Fatalf("SessionID = %q, want %q", id.SessionID, want)
	}
}
