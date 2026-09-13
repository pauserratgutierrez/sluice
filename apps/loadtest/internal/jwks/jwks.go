package jwks

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/keys"
)

func Main() error {
	// Mint in-process so this service is long-lived. A separate keys oneshot
	// would exit 0 and trip `docker compose --abort-on-container-exit`.
	if err := keys.Main(); err != nil {
		return err
	}
	dir := envOr("KEYS_DIR", "/keys")
	path := filepath.Join(dir, keys.JWKSFile)
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("jwks: read %s: %w", path, err)
	}

	mux := http.NewServeMux()
	serve := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(raw)
	}
	mux.HandleFunc("GET /.well-known/jwks.json", serve)
	mux.HandleFunc("GET /jwks.json", serve)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	addr := envOr("JWKS_LISTEN", "0.0.0.0:8080")
	fmt.Println("jwks: listening on", addr)
	s := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s.ListenAndServe()
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
