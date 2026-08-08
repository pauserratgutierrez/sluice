// Command keygen mints the secrets the test harness needs and prints them as
// .env lines.
//
// The JWT material is an ES256 (NIST P-256) key pair packaged as two JWK sets:
// a private one for GoTrue to sign with, and a public one for verifiers. This
// mirrors what supabase-headless does, so a JWT minted here is
// indistinguishable from one minted by a real deployment.
//
//	go run ./cmd/keygen              # print .env lines
//	go run ./cmd/keygen -json        # print a JSON object instead
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"os"
	"time"
)

type jwk struct {
	Kty    string   `json:"kty"`
	Kid    string   `json:"kid,omitempty"`
	Use    string   `json:"use,omitempty"`
	Alg    string   `json:"alg"`
	KeyOps []string `json:"key_ops,omitempty"`
	Crv    string   `json:"crv,omitempty"`
	X      string   `json:"x,omitempty"`
	Y      string   `json:"y,omitempty"`
	D      string   `json:"d,omitempty"`
	K      string   `json:"k,omitempty"`
	Ext    bool     `json:"ext,omitempty"`
}

type jwks struct {
	Keys []jwk `json:"keys"`
}

func main() {
	asJSON := flag.Bool("json", false, "emit a JSON object instead of .env lines")
	signRole := flag.String("sign", "", "mint one API key for this role using the existing "+
		"JWT_KEYS in the environment, instead of generating a new key pair")
	flag.Parse()

	// Minting a token against the EXISTING key is the common case in practice:
	// rotating the key pair would invalidate every password in .env and force a
	// database rebuild, which is far too blunt just to get a service_role token.
	if *signRole != "" {
		tok, err := signWithExistingKey(*signRole)
		if err != nil {
			fatal("%v", err)
		}
		fmt.Println(tok)
		return
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		fatal("generate P-256 key: %v", err)
	}

	kid, err := uuidV4()
	if err != nil {
		fatal("generate kid: %v", err)
	}

	// P-256 coordinates are fixed-width 32 bytes. FillBytes is what makes them
	// so; using Bytes() would drop leading zeros and produce a JWK that some
	// verifiers reject.
	xb := make([]byte, 32)
	yb := make([]byte, 32)
	db := make([]byte, 32)
	priv.X.FillBytes(xb)
	priv.Y.FillBytes(yb)
	priv.D.FillBytes(db)

	b64 := base64.RawURLEncoding.EncodeToString

	jwtSecret, err := randomBase64(30)
	if err != nil {
		fatal("generate jwt secret: %v", err)
	}

	// Private set: the EC signing key plus the legacy symmetric key, matching
	// the shape GoTrue expects in GOTRUE_JWT_KEYS.
	privateSet := []jwk{
		{
			Kty: "EC", Kid: kid, Use: "sig", Alg: "ES256", Ext: true,
			KeyOps: []string{"sign", "verify"},
			Crv:    "P-256", X: b64(xb), Y: b64(yb), D: b64(db),
		},
		{Kty: "oct", Alg: "HS256", K: b64([]byte(jwtSecret))},
	}

	// Public set: verification only, and NO symmetric key. Leaking the oct key
	// through a JWKS endpoint would let anyone mint tokens.
	publicSet := jwks{Keys: []jwk{{
		Kty: "EC", Kid: kid, Use: "sig", Alg: "ES256", Ext: true,
		KeyOps: []string{"verify"},
		Crv:    "P-256", X: b64(xb), Y: b64(yb),
	}}}

	privateJSON, _ := json.Marshal(privateSet)
	publicJSON, _ := json.Marshal(publicSet)

	// Long-lived API keys, ES256-signed by the same key GoTrue uses. These carry
	// only {role, iss, iat, exp} -- no `sub` and no `session_id` -- which is why
	// Sluice's claim handling must tolerate both token shapes.
	anonKey, err := signES256(priv, kid, map[string]any{
		"role": "anon", "iss": "sluice", "iat": now(), "exp": now() + 5*365*24*3600,
	})
	if err != nil {
		fatal("sign anon key: %v", err)
	}
	serviceKey, err := signES256(priv, kid, map[string]any{
		"role": "service_role", "iss": "sluice", "iat": now(), "exp": now() + 5*365*24*3600,
	})
	if err != nil {
		fatal("sign service_role key: %v", err)
	}

	out := map[string]string{
		"ANON_KEY":              anonKey,
		"SERVICE_ROLE_KEY":      serviceKey,
		"POSTGRES_PASSWORD":     mustHex(32),
		"SLUICE_REPL_PASSWORD":  mustHex(32),
		"SLUICE_AUTHZ_PASSWORD": mustHex(32),
		"AUTH_DB_PASSWORD":      mustHex(32),
		"PGRST_AUTH_PASSWORD":   mustHex(32),
		"JWT_SECRET":            jwtSecret,
		"JWT_KEYS":              string(privateJSON),
		"JWT_JWKS":              string(publicJSON),
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
		return
	}

	// Stable order so a diff against an existing .env is readable.
	order := []string{
		"POSTGRES_PASSWORD", "SLUICE_REPL_PASSWORD", "SLUICE_AUTHZ_PASSWORD",
		"AUTH_DB_PASSWORD", "PGRST_AUTH_PASSWORD",
		"JWT_SECRET", "JWT_KEYS", "JWT_JWKS",
		"ANON_KEY", "SERVICE_ROLE_KEY",
	}
	for _, k := range order {
		fmt.Printf("%s=%s\n", k, out[k])
	}
}

func now() int64 { return time.Now().Unix() }

// signWithExistingKey mints an API key using the EC private JWK already in
// JWT_KEYS, so an existing deployment can issue one without rotating anything.
func signWithExistingKey(role string) (string, error) {
	raw := os.Getenv("JWT_KEYS")
	if raw == "" {
		return "", fmt.Errorf("JWT_KEYS is not set in the environment")
	}
	var set []jwk
	if err := json.Unmarshal([]byte(raw), &set); err != nil {
		return "", fmt.Errorf("parse JWT_KEYS: %w", err)
	}
	for _, k := range set {
		if k.Kty != "EC" || k.D == "" || k.Crv != "P-256" {
			continue
		}
		d, err := base64.RawURLEncoding.DecodeString(k.D)
		if err != nil {
			return "", fmt.Errorf("decode private scalar: %w", err)
		}
		x, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil {
			return "", fmt.Errorf("decode x: %w", err)
		}
		y, err := base64.RawURLEncoding.DecodeString(k.Y)
		if err != nil {
			return "", fmt.Errorf("decode y: %w", err)
		}
		priv := &ecdsa.PrivateKey{
			PublicKey: ecdsa.PublicKey{
				Curve: elliptic.P256(),
				X:     new(big.Int).SetBytes(x),
				Y:     new(big.Int).SetBytes(y),
			},
			D: new(big.Int).SetBytes(d),
		}
		return signES256(priv, k.Kid, map[string]any{
			"role": role, "iss": "sluice",
			"iat": now(), "exp": now() + 5*365*24*3600,
		})
	}
	return "", fmt.Errorf("JWT_KEYS contains no EC P-256 private key")
}

// signES256 produces a compact JWS. The signature is the raw r||s pair, each
// left-padded to 32 bytes -- ASN.1 DER, which crypto/ecdsa.Sign returns, is NOT
// what JWS expects and is the classic way to produce a token nothing can verify.
func signES256(priv *ecdsa.PrivateKey, kid string, claims map[string]any) (string, error) {
	header := map[string]any{"alg": "ES256", "typ": "JWT", "kid": kid}
	hb, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	cb, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding.EncodeToString
	signing := enc(hb) + "." + enc(cb)

	sum := sha256.Sum256([]byte(signing))
	r, s, err := ecdsa.Sign(rand.Reader, priv, sum[:])
	if err != nil {
		return "", err
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return signing + "." + enc(sig), nil
}

func randomBase64(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func mustHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		fatal("read random: %v", err)
	}
	return hex.EncodeToString(b)
}

func uuidV4() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "keygen: "+format+"\n", args...)
	os.Exit(1)
}
