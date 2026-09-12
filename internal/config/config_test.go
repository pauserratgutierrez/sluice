package config

import "testing"

func baseValid() *Config {
	return &Config{
		ReplURL:         "postgres://sluice_repl@db/postgres?replication=database",
		AuthzURL:        "postgres://sluice_authz@db/postgres",
		JWKSURL:         "http://auth/.well-known/jwks.json",
		JWTAlg:          "ES256",
		Streaming:       "off",
		ProtoVersion:    4,
		TierC:           "allow",
		ReplicaIdentity: "warn",
		DegradedDeletes: "withhold",
		Heartbeat:       20_000_000_000,
		MessagePrefix:   "sluice:",
		ShapeOracle:     "rls",
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
