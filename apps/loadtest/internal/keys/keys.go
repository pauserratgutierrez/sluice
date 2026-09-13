package keys

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	PrivateFile = "private.jwk"
	JWKSFile    = "jwks.json"
)

type JWK struct {
	Kty string `json:"kty"`
	Kid string `json:"kid,omitempty"`
	Use string `json:"use,omitempty"`
	Alg string `json:"alg"`
	Crv string `json:"crv,omitempty"`
	X   string `json:"x,omitempty"`
	Y   string `json:"y,omitempty"`
	D   string `json:"d,omitempty"`
}

type JWKS struct {
	Keys []JWK `json:"keys"`
}

// Main writes an ES256 P-256 key pair into KEYS_DIR (default /keys).
// Idempotent: existing files are left alone so a compose restart does not
// rotate the key out from under a still-running Sluice.
func Main() error {
	dir := envOr("KEYS_DIR", "/keys")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	privPath := filepath.Join(dir, PrivateFile)
	jwksPath := filepath.Join(dir, JWKSFile)
	if fileExists(privPath) && fileExists(jwksPath) {
		fmt.Println("keys: existing key pair reused")
		return nil
	}
	priv, pub, err := Generate()
	if err != nil {
		return err
	}
	if err := os.WriteFile(privPath, mustJSON(priv), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(jwksPath, mustJSON(JWKS{Keys: []JWK{pub}}), 0o644); err != nil {
		return err
	}
	fmt.Println("keys: wrote ES256 key pair to", dir)
	return nil
}

func Generate() (private, public JWK, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return JWK{}, JWK{}, err
	}
	kid, err := kidFor(key)
	if err != nil {
		return JWK{}, JWK{}, err
	}
	xb := make([]byte, 32)
	yb := make([]byte, 32)
	db := make([]byte, 32)
	key.X.FillBytes(xb)
	key.Y.FillBytes(yb)
	key.D.FillBytes(db)
	enc := base64.RawURLEncoding
	public = JWK{
		Kty: "EC", Kid: kid, Use: "sig", Alg: "ES256", Crv: "P-256",
		X: enc.EncodeToString(xb), Y: enc.EncodeToString(yb),
	}
	private = public
	private.D = enc.EncodeToString(db)
	return private, public, nil
}

func kidFor(key *ecdsa.PrivateKey) (string, error) {
	xb := make([]byte, 32)
	yb := make([]byte, 32)
	key.X.FillBytes(xb)
	key.Y.FillBytes(yb)
	sum := sha256.Sum256(append(xb, yb...))
	return hex.EncodeToString(sum[:8]), nil
}

func fileExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.Size() > 0
}

func mustJSON(v any) []byte {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	b = append(b, '\n')
	return b
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
