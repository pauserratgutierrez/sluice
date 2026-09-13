package authn

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/keys"
)

type Signer struct {
	key *ecdsa.PrivateKey
	kid string
	aud string
}

func Load(path string) (*Signer, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("authn: read private jwk: %w", err)
	}
	var j keys.JWK
	if err := json.Unmarshal(raw, &j); err != nil {
		return nil, fmt.Errorf("authn: parse private jwk: %w", err)
	}
	key, err := ecdsaFrom(j)
	if err != nil {
		return nil, err
	}
	return &Signer{key: key, kid: j.Kid, aud: "authenticated"}, nil
}

func (s *Signer) User(sub, sessionID string, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"role":       "authenticated",
		"sub":        sub,
		"aud":        s.aud,
		"session_id": sessionID,
		"iat":        now.Unix(),
		"exp":        now.Add(ttl).Unix(),
	}
	return s.sign(claims)
}

func (s *Signer) Public() *ecdsa.PublicKey { return &s.key.PublicKey }

func (s *Signer) sign(claims jwt.MapClaims) (string, error) {
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	tok.Header["kid"] = s.kid
	return tok.SignedString(s.key)
}

func ecdsaFrom(j keys.JWK) (*ecdsa.PrivateKey, error) {
	if j.Kty != "EC" || j.Crv != "P-256" || j.D == "" {
		return nil, fmt.Errorf("authn: private jwk must be EC P-256 with d")
	}
	xb, err := b64(j.X)
	if err != nil {
		return nil, err
	}
	yb, err := b64(j.Y)
	if err != nil {
		return nil, err
	}
	db, err := b64(j.D)
	if err != nil {
		return nil, err
	}
	return &ecdsa.PrivateKey{
		PublicKey: ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(xb), Y: new(big.Int).SetBytes(yb)},
		D:         new(big.Int).SetBytes(db),
	}, nil
}

func b64(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}
