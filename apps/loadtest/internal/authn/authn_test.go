package authn

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/keys"
)

func TestMintUserToken(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("KEYS_DIR", dir)
	if err := keys.Main(); err != nil {
		t.Fatal(err)
	}
	s, err := Load(filepath.Join(dir, keys.PrivateFile))
	if err != nil {
		t.Fatal(err)
	}
	tok, err := s.User("11111111-1111-4111-8111-111111111111", "sess-1", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := jwt.Parse(tok, func(t *jwt.Token) (any, error) {
		return s.Public(), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	claims := parsed.Claims.(jwt.MapClaims)
	if claims["role"] != "authenticated" {
		t.Fatalf("role=%v", claims["role"])
	}
	if _, err := os.Stat(filepath.Join(dir, keys.JWKSFile)); err != nil {
		t.Fatal(err)
	}
}
