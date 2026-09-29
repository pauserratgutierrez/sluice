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
