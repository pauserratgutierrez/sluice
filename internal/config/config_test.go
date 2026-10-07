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
		JWTRequireRole:  true,
		RequestIDHeader: "X-Request-ID",
	}
}

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("SLUICE_DB_REPL_URL", "postgres://r@db/postgres?replication=database")
	t.Setenv("SLUICE_DB_AUTHZ_URL", "postgres://a@db/postgres")
	t.Setenv("SLUICE_JWKS_URL", "http://auth/.well-known/jwks.json")
}

// A hook namespace must work in rls mode with neither the issuer bearer nor the
// hook bearer set: the hook bearer is optional and independent of the oracle.
func TestLoadHookBearerIsOptionalAndSeparate(t *testing.T) {
	setRequired(t)
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

// The defaults keep a GoTrue deployment's tokens working unchanged; user-level
// revocation is opt-in.
func TestLoadTokenAndRevocationDefaults(t *testing.T) {
	setRequired(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.JWTSessionClaim != "session_id" {
		t.Errorf("JWTSessionClaim = %q, want session_id", c.JWTSessionClaim)
	}
	if !c.JWTRequireRole {
		t.Error("JWTRequireRole must default to true")
	}
	if c.UsersTable != "" {
		t.Errorf("UsersTable = %q, want unset", c.UsersTable)
	}
	if c.UsersBanColumn != "banned_until" {
		t.Errorf("UsersBanColumn = %q, want banned_until", c.UsersBanColumn)
	}
}

func TestLoadTokenAndRevocationOverrides(t *testing.T) {
	setRequired(t)
	t.Setenv("SLUICE_SHAPE_ORACLE", "issuer")
	t.Setenv("SLUICE_ISSUER_URL", "http://api/sluice/shapes")
	t.Setenv("SLUICE_ISSUER_BEARER", "secret")
	t.Setenv("SLUICE_JWT_SESSION_CLAIM", "sid")
	t.Setenv("SLUICE_JWT_REQUIRE_ROLE", "false")
	t.Setenv("SLUICE_REVOCATION_USERS_TABLE", "auth.user")
	t.Setenv("SLUICE_REVOCATION_USERS_BAN_COLUMN", "suspended_until")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.JWTSessionClaim != "sid" || c.JWTRequireRole ||
		c.UsersTable != "auth.user" || c.UsersBanColumn != "suspended_until" {
		t.Fatalf("got session claim %q, require role %v, users table %q, ban column %q",
			c.JWTSessionClaim, c.JWTRequireRole, c.UsersTable, c.UsersBanColumn)
	}
}

// RLS mode impersonates the token's role, so it cannot run without one.
func TestValidateOptionalRoleNeedsIssuer(t *testing.T) {
	c := baseValid()
	c.JWTRequireRole = false
	if err := c.validate(); err == nil {
		t.Fatal("SLUICE_JWT_REQUIRE_ROLE=false must be rejected in rls mode")
	}
	c.ShapeOracle = "issuer"
	c.IssuerURL = "http://api/sluice/shapes"
	c.IssuerBearer = "secret"
	c.IssuerTimeout = 2_000_000_000
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRingDisabledNeedsSnapshotsOff(t *testing.T) {
	c := baseValid()
	c.SnapshotEnabled = true
	if err := c.validate(); err == nil {
		t.Fatal("snapshots replay the resume buffer; disabling it must require snapshots off")
	}
	c.SnapshotEnabled = false
	if err := c.validate(); err != nil {
		t.Fatalf("a disabled buffer without snapshots is valid: %v", err)
	}
	c.RingEvents = -1
	if err := c.validate(); err == nil {
		t.Fatal("a negative SLUICE_RING_EVENTS must be rejected")
	}
	c.RingEvents = 4096
	if err := c.validate(); err == nil {
		t.Fatal("an enabled buffer needs a positive SLUICE_RING_MAX_BYTES")
	}
	c.RingMaxBytes = 64 << 20
	c.SnapshotEnabled = true
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateSessionLookupNeedsRevocation(t *testing.T) {
	c := baseValid()
	c.SessionLookup = true
	if err := c.validate(); err == nil {
		t.Fatal("the session lookup without revocation must be rejected")
	}
	c.RevocationEnabled = true
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadRequestIDHeader(t *testing.T) {
	setRequired(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.RequestIDHeader != "X-Request-ID" {
		t.Errorf("RequestIDHeader = %q, want X-Request-ID", c.RequestIDHeader)
	}
	t.Setenv("SLUICE_REQUEST_ID_HEADER", "X-Correlation-ID")
	if c, err = Load(); err != nil {
		t.Fatal(err)
	}
	if c.RequestIDHeader != "X-Correlation-ID" {
		t.Errorf("RequestIDHeader = %q, want X-Correlation-ID", c.RequestIDHeader)
	}
}

// The ID is logged and forwarded, so it can never be read from a credential.
func TestValidateRequestIDHeader(t *testing.T) {
	c := baseValid()
	for _, h := range []string{"Authorization", "cookie", "Proxy-Authorization", "X Request ID", "X-Request-ID:"} {
		c.RequestIDHeader = h
		if err := c.validate(); err == nil {
			t.Errorf("SLUICE_REQUEST_ID_HEADER=%q was accepted", h)
		}
	}
	for _, h := range []string{"X-Request-ID", "x-correlation-id", "Request-Id"} {
		c.RequestIDHeader = h
		if err := c.validate(); err != nil {
			t.Errorf("SLUICE_REQUEST_ID_HEADER=%q: %v", h, err)
		}
	}
}

func TestRevocationTables(t *testing.T) {
	c := baseValid()
	c.SessionsTable = "identity.session"
	if got := c.RevocationTables(); len(got) != 0 {
		t.Fatalf("without revocation no table is revocation-only, got %v", got)
	}
	c.RevocationEnabled = true
	if got := c.RevocationTables(); len(got) != 1 || got[0] != "identity.session" {
		t.Fatalf("got %v, want the sessions table", got)
	}
	c.UsersTable = "auth.users"
	if got := c.RevocationTables(); len(got) != 2 || got[1] != "auth.users" {
		t.Fatalf("got %v, want the sessions and users tables", got)
	}
}
