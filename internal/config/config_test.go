package config

import "testing"

func baseValid() *Config {
	return &Config{
		ReplURL:         "postgres://sluice_repl@db/postgres?replication=database",
		AuthzURL:        "postgres://sluice_authz@db/postgres",
		JWKSURL:         "http://auth/.well-known/jwks.json",
		JWTAlg:          "ES256",
		ProtoVersion:    4,
		TierC:           "allow",
		TierCTimeout:    1_000_000_000,
		ReplicaIdentity: "warn",
		DegradedDeletes: "withhold",
		Heartbeat:       20_000_000_000,
		PresenceWindow:  30_000_000_000,
		MessagePrefix:   "sluice:",
		ShapeOracle:     "rls",
	}
}

// A hook namespace must work in rls mode with neither the issuer bearer nor the
// hook bearer set: the hook bearer is optional and independent of the oracle.
func TestLoadHookBearerIsOptionalAndSeparate(t *testing.T) {
	t.Setenv("SLUICE_DB_REPL_URL", "postgres://r@db/postgres?replication=database")
	t.Setenv("SLUICE_DB_AUTHZ_URL", "postgres://a@db/postgres")
	t.Setenv("SLUICE_JWKS_URL", "http://auth/.well-known/jwks.json")
	t.Setenv("SLUICE_CHANNELS", "billing:hook:http://api:8080/authz")
	t.Setenv("SLUICE_ISSUER_BEARER", "issuer-secret")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.HookBearer != "" {
		t.Fatalf("HookBearer = %q, must not fall back to SLUICE_ISSUER_BEARER", c.HookBearer)
	}

	t.Setenv("SLUICE_CHANNEL_HOOK_BEARER", "hook-secret")
	if c, err = Load(); err != nil {
		t.Fatal(err)
	}
	if c.HookBearer != "hook-secret" {
		t.Fatalf("HookBearer = %q, want hook-secret", c.HookBearer)
	}
}

func TestValidateRLSDefault(t *testing.T) {
	c := baseValid()
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	if c.IssuerMode() {
		t.Fatal("rls must not report issuer mode")
	}
}

func TestValidateIssuerRequiresURLAndBearer(t *testing.T) {
	c := baseValid()
	c.ShapeOracle = "issuer"
	if err := c.validate(); err == nil {
		t.Fatal("issuer without URL and bearer must be rejected")
	}
	c.IssuerURL = "http://api/sluice/shapes"
	if err := c.validate(); err == nil {
		t.Fatal("issuer without bearer must be rejected")
	}
	c.IssuerBearer = "secret"
	c.IssuerTimeout = 0
	if err := c.validate(); err == nil {
		t.Fatal("issuer with zero timeout must be rejected")
	}
	c.IssuerTimeout = 2_000_000_000
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	if !c.IssuerMode() {
		t.Fatal("issuer must report issuer mode")
	}
}

func TestValidateRejectsUnknownOracle(t *testing.T) {
	c := baseValid()
	c.ShapeOracle = "both"
	if err := c.validate(); err == nil {
		t.Fatal("AND/OR of oracles is not a mode")
	}
}
