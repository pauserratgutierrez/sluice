// Package auth verifies access tokens and tracks session revocation.
//
// Two decisions here are security-load-bearing:
//
//  1. The signing algorithm is PINNED. A JWKS is public, so if a `kid` could
//     select a symmetric key the verification set would become a signing oracle.
//     Sluice accepts exactly one algorithm and rejects everything else before it
//     even looks up a key.
//
//  2. Revocation is PUSHED, not polled. A GoTrue access token lives an hour by
//     default, and a server whose entire job is holding long-lived connections
//     cannot keep streaming to a signed-out user for that long. Verified against
//     supabase/auth v2.195.0: the `session_id` claim IS `auth.sessions.id`, and
//     sign-out DELETEs the row. So Sluice watches auth.sessions on the
//     replication slot it already consumes and kills the affected streams in
//     milliseconds, with no polling and no extra query. Supabase's own docs
//     recommend exactly this check.
package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/expr"
)

// Verifier validates access tokens against a cached JWKS.
type Verifier struct {
	url      string
	alg      string
	issuer   string
	audience string
	leeway   time.Duration
	refresh  time.Duration
	allowed  map[string]bool

	client *http.Client

	mu        sync.RWMutex
	keys      map[string]any // kid -> *ecdsa.PublicKey or *rsa.PublicKey
	fetchedAt time.Time
}

func NewVerifier(url, alg, issuer, audience string, leeway, refresh time.Duration, allowedRoles []string) *Verifier {
	allowed := make(map[string]bool, len(allowedRoles))
	for _, r := range allowedRoles {
		allowed[r] = true
	}
	return &Verifier{
		url: url, alg: alg, issuer: issuer, audience: audience,
		leeway: leeway, refresh: refresh, allowed: allowed,
		client: &http.Client{Timeout: 5 * time.Second},
		keys:   map[string]any{},
	}
}

// Refresh fetches the JWKS.
//
// Exposed so that an admin endpoint can force it: Supabase's own docs warn that
// multi-level caching can leave a third-party component trusting a revoked
// signing key for up to ~20 minutes, and tell you to build a cache-busting
// mechanism. This is it.
func (v *Verifier) Refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.url, nil)
	if err != nil {
		return err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return fmt.Errorf("auth: fetch JWKS: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("auth: fetch JWKS: unexpected status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}

	var set struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			Alg string `json:"alg"`
			Crv string `json:"crv"`
			X   string `json:"x"`
			Y   string `json:"y"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(body, &set); err != nil {
		return fmt.Errorf("auth: parse JWKS: %w", err)
	}

	keys := map[string]any{}
	symmetric := 0
	for _, k := range set.Keys {
		switch k.Kty {
		case "EC":
			if k.Crv != "P-256" {
				continue
			}
			x, err1 := b64(k.X)
			y, err2 := b64(k.Y)
			if err1 != nil || err2 != nil {
				continue
			}
			keys[k.Kid] = &ecdsa.PublicKey{
				Curve: elliptic.P256(),
				X:     new(big.Int).SetBytes(x),
				Y:     new(big.Int).SetBytes(y),
			}
		case "RSA":
			n, err1 := b64(k.N)
			e, err2 := b64(k.E)
			if err1 != nil || err2 != nil {
				continue
			}
			keys[k.Kid] = &rsa.PublicKey{
				N: new(big.Int).SetBytes(n),
				E: int(new(big.Int).SetBytes(e).Int64()),
			}
		case "oct":
			// A symmetric key must never appear in a public JWKS. Count it so
			// startup validation can warn, but never load it.
			symmetric++
		}
	}
	if len(keys) == 0 {
		return fmt.Errorf("auth: JWKS at %s contains no usable %s key", v.url, v.alg)
	}

	v.mu.Lock()
	v.keys, v.fetchedAt = keys, time.Now()
	v.mu.Unlock()

	if symmetric > 0 {
		return &WarnSymmetricKey{Count: symmetric}
	}
	return nil
}

// WarnSymmetricKey is returned by Refresh when the JWKS exposes an `oct` key.
// Non-fatal, because the key is never loaded, but worth shouting about.
type WarnSymmetricKey struct{ Count int }

func (e *WarnSymmetricKey) Error() string {
	return fmt.Sprintf("JWKS exposes %d symmetric key(s); they were ignored, but a public "+
		"JWKS should never contain one", e.Count)
}

func b64(s string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(s) }

// EnsureFresh refetches if the cache is older than the refresh interval.
func (v *Verifier) EnsureFresh(ctx context.Context) {
	v.mu.RLock()
	age := time.Since(v.fetchedAt)
	v.mu.RUnlock()
	if age > v.refresh {
		_ = v.Refresh(ctx)
	}
}

var ErrNoToken = errors.New("auth: no bearer token")

// Verify parses and validates a token, returning the caller's identity.
func (v *Verifier) Verify(ctx context.Context, bearer string) (authz.Identity, error) {
	tok := strings.TrimSpace(strings.TrimPrefix(bearer, "Bearer "))
	if tok == "" || tok == bearer && !strings.HasPrefix(bearer, "Bearer") {
		if tok == "" {
			return authz.Identity{}, ErrNoToken
		}
	}
	// Opaque publishable/secret keys are not JWTs. Reject them explicitly rather
	// than producing a confusing parse error.
	if strings.HasPrefix(tok, "sb_") {
		return authz.Identity{}, fmt.Errorf("auth: opaque sb_* keys are not accepted; send a JWT")
	}

	parser := jwt.NewParser(
		// Pinning the algorithm is what stops `kid`-driven algorithm confusion.
		jwt.WithValidMethods([]string{v.alg}),
		jwt.WithLeeway(v.leeway),
		jwt.WithExpirationRequired(),
	)

	claims := jwt.MapClaims{}
	_, err := parser.ParseWithClaims(tok, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		v.mu.RLock()
		key, ok := v.keys[kid]
		v.mu.RUnlock()
		if ok {
			return key, nil
		}
		// Unknown kid: could be a rotation that happened since the last fetch.
		if err := v.Refresh(ctx); err != nil {
			var warn *WarnSymmetricKey
			if !errors.As(err, &warn) {
				return nil, err
			}
		}
		v.mu.RLock()
		key, ok = v.keys[kid]
		v.mu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("no key for kid %q", kid)
		}
		return key, nil
	})
	if err != nil {
		return authz.Identity{}, fmt.Errorf("auth: %w", err)
	}

	role, _ := claims["role"].(string)
	if role == "" {
		return authz.Identity{}, fmt.Errorf("auth: token has no role claim")
	}
	if !v.allowed[role] {
		return authz.Identity{}, fmt.Errorf("auth: role %q is not permitted", role)
	}
	if v.issuer != "" {
		if iss, _ := claims["iss"].(string); iss != v.issuer {
			return authz.Identity{}, fmt.Errorf("auth: unexpected issuer")
		}
	}
	// Audience is only checked for user tokens: apikey tokens carry no `aud`.
	if v.audience != "" {
		if _, hasSub := claims["sub"]; hasSub {
			if !audienceMatches(claims["aud"], v.audience) {
				return authz.Identity{}, fmt.Errorf("auth: unexpected audience")
			}
		}
	}

	raw, err := json.Marshal(claims)
	if err != nil {
		return authz.Identity{}, err
	}
	sub, _ := claims["sub"].(string)
	sid, _ := claims["session_id"].(string)

	return authz.Identity{
		Role:      role,
		Sub:       strings.ToLower(sub),
		SessionID: sid,
		Claims:    expr.Claims(claims),
		ClaimsRaw: string(raw),
	}, nil
}

func audienceMatches(aud any, want string) bool {
	switch a := aud.(type) {
	case string:
		return a == want
	case []any:
		for _, x := range a {
			if s, ok := x.(string); ok && s == want {
				return true
			}
		}
	case nil:
		return true // no aud claim: nothing to contradict
	}
	return false
}

// Expiry returns the token expiry recorded in the claims, so the server can
// close a stream when it lapses rather than serving a dead identity.
func Expiry(id authz.Identity) time.Time {
	switch v := id.Claims["exp"].(type) {
	case float64:
		return time.Unix(int64(v), 0)
	case int64:
		return time.Unix(v, 0)
	}
	return time.Time{}
}

// ---------------------------------------------------------------------------
// Revocation
// ---------------------------------------------------------------------------

// Revoker tracks revoked sessions and banned users, fed by the replication
// stream rather than by polling the auth service.
type Revoker struct {
	mu       sync.RWMutex
	sessions map[string]time.Time // revoked session_id -> when
	users    map[string]time.Time // banned user_id -> when
	ttl      time.Duration
}

func NewRevoker(ttl time.Duration) *Revoker {
	if ttl <= 0 {
		// A revocation only needs to be remembered until every token that could
		// carry it has expired. Two hours comfortably covers the default
		// one-hour GOTRUE_JWT_EXP plus clock skew.
		ttl = 2 * time.Hour
	}
	return &Revoker{
		sessions: map[string]time.Time{},
		users:    map[string]time.Time{},
		ttl:      ttl,
	}
}

// RevokeSession records that a session row was deleted.
func (r *Revoker) RevokeSession(id string) {
	if id == "" {
		return
	}
	r.mu.Lock()
	r.sessions[id] = time.Now()
	r.mu.Unlock()
}

// BanUser records that a user was banned.
func (r *Revoker) BanUser(id string) {
	if id == "" {
		return
	}
	r.mu.Lock()
	r.users[strings.ToLower(id)] = time.Now()
	r.mu.Unlock()
}

// Revoked reports whether an identity has been invalidated.
func (r *Revoker) Revoked(id authz.Identity) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if id.SessionID != "" {
		if _, ok := r.sessions[id.SessionID]; ok {
			return true
		}
	}
	if id.Sub != "" {
		if _, ok := r.users[id.Sub]; ok {
			return true
		}
	}
	return false
}

// Sweep drops entries older than the TTL so the maps do not grow without bound.
func (r *Revoker) Sweep() {
	cutoff := time.Now().Add(-r.ttl)
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, t := range r.sessions {
		if t.Before(cutoff) {
			delete(r.sessions, k)
		}
	}
	for k, t := range r.users {
		if t.Before(cutoff) {
			delete(r.users, k)
		}
	}
}
