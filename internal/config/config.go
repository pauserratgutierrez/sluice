// Package config parses Sluice's configuration from the environment.
//
// Every default is chosen to be safe rather than fast. Where a default differs
// from what a naive reading would suggest -- notably STREAMING=off and
// BINARY=false -- the reason is recorded next to it, because both look like
// pessimisations and are not.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type ChannelMode string

const (
	// ChannelPublic: any authenticated stream may subscribe and publish.
	ChannelPublic ChannelMode = "public"
	// ChannelOwner: the channel name must end in the caller's `sub`, e.g.
	// `notify:7f3a...`. Authorization by naming convention, O(1), no I/O. This
	// is Centrifugo's `#user_id` idea.
	ChannelOwner ChannelMode = "owner"
	// ChannelHook: delegate to an HTTP endpoint. The escape hatch for arbitrary
	// business rules, so Sluice never has to learn an application's auth model.
	ChannelHook ChannelMode = "hook"
)

type Channel struct {
	Namespace string
	Mode      ChannelMode
	HookURL   string
}

type Config struct {
	ListenAddr string
	PathPrefix string
	LogLevel   string
	LogFormat  string
	NodeID     string
	Shutdown   time.Duration

	ReplURL      string
	AuthzURL     string
	PoolMax      int32
	PoolMin      int32
	ParanoidPool bool

	SlotName      string
	Publication   string
	ProtoVersion  int
	Streaming     string
	Binary        bool
	Messages      bool
	StatusEvery   time.Duration
	DispatchQueue int
	MessagePrefix string

	RingEvents int
	RingMaxAge time.Duration

	JWKSURL      string
	JWKSRefresh  time.Duration
	JWTAlg       string
	JWTIssuer    string
	JWTAudience  string
	JWTLeeway    time.Duration
	AnonRole     string
	AllowedRoles []string

	AuthzLease      time.Duration
	CatalogRefresh  time.Duration
	TierC           string // allow | deny
	TierCMaxProbes  int
	TierBVerify     int // cross-checks per Tier B subscription; 0 disables
	UnindexedMax    int
	ReplicaIdentity string // warn | strict
	DegradedDeletes string // withhold | deliver

	// ShapeOracle is the process-wide judge of shapes: "rls" (default) or "issuer".
	// There is no AND/OR of the two.
	ShapeOracle   string
	IssuerURL     string
	IssuerBearer  string
	IssuerTimeout time.Duration

	SnapshotEnabled  bool
	SnapshotMaxConc  int
	SnapshotMaxRows  int
	SnapshotPageSize int

	Heartbeat          time.Duration
	StreamQueue        int
	WriteTimeout       time.Duration
	MaxStreams         int
	MaxSubsPerStream   int
	MaxShapesPerStream int
	MaxPayloadBytes    int
	MaxChangeBytes     int
	SubscribeRate      int
	PublishRate        int
	PresenceRate       int
	PresenceWindow     time.Duration
	PresenceBcast      time.Duration
	PresenceMaxKeys    int

	Channels    []Channel
	HookTTL     time.Duration
	HookTimeout time.Duration

	RevocationEnabled bool
	SessionsTable     string
	UsersTable        string
	VerifyOnSubscribe bool

	MetricsEnabled     bool
	DiagnosticsEnabled bool
}

// Load reads configuration from the environment and validates it.
func Load() (*Config, error) {
	c := &Config{
		ListenAddr: env("SLUICE_LISTEN_ADDR", "0.0.0.0:4000"),
		PathPrefix: strings.TrimSuffix(env("SLUICE_PATH_PREFIX", "/sluice/v1"), "/"),
		LogLevel:   env("SLUICE_LOG_LEVEL", "info"),
		LogFormat:  env("SLUICE_LOG_FORMAT", "json"),
		NodeID:     env("SLUICE_NODE_ID", defaultNodeID()),
		Shutdown:   envDur("SLUICE_SHUTDOWN_GRACE", 15*time.Second),

		ReplURL:      env("SLUICE_DB_REPL_URL", ""),
		AuthzURL:     env("SLUICE_DB_AUTHZ_URL", ""),
		PoolMax:      int32(envInt("SLUICE_DB_POOL_MAX_CONNS", 8)),
		PoolMin:      int32(envInt("SLUICE_DB_POOL_MIN_CONNS", 2)),
		ParanoidPool: envBool("SLUICE_PARANOID_POOL_RESET", false),

		SlotName:    env("SLUICE_SLOT_NAME", "sluice"),
		Publication: env("SLUICE_PUBLICATION", "sluice"),

		// proto_version 4 declares capability. Verified empirically: the
		// negotiated version ALONE changes nothing -- requesting 1, 4, or 4 with
		// streaming=parallel produced byte-identical output (same MD5, same 528
		// bytes). The OPTIONS determine the message set. Asking for 4 costs
		// nothing and makes enabling streaming later a flag rather than a
		// protocol change. It is a per-connection option, not slot state, so it
		// can be changed at any time by reconnecting.
		ProtoVersion: envInt("SLUICE_PROTO_VERSION", 4),

		// streaming=off is the important default. With it, EVERYTHING the reader
		// receives is already committed and durable, so the reader forwards
		// immediately with zero buffering. With streaming=on you receive
		// UNCOMMITTED changes and must buffer whole transactions in the Go heap
		// until Stream Commit -- moving an unbounded, OOM-prone buffer out of
		// PostgreSQL (where it is disk-backed and observable via
		// pg_stat_replication_slots) and into the process that holds every
		// client connection.
		Streaming: env("SLUICE_STREAMING", "off"),

		// binary=false because, measured, the binary format was LARGER (112 vs
		// 88 bytes for a representative row) and would require per-type decoders
		// for numeric's four-word struct, timestamptz as int64 microseconds, the
		// full array header, and jsonb's version byte -- plus the text path
		// anyway, since pgoutput falls back to 't' per column for types without
		// typsend. Sluice emits JSON, and text is closer to JSON than binary is.
		Binary: envBool("SLUICE_BINARY", false),

		Messages:      envBool("SLUICE_MESSAGES", true),
		StatusEvery:   envDur("SLUICE_STATUS_INTERVAL", 10*time.Second),
		DispatchQueue: envInt("SLUICE_DISPATCH_QUEUE", 8192),
		MessagePrefix: env("SLUICE_MESSAGE_PREFIX", "sluice:"),

		RingEvents: envInt("SLUICE_RING_EVENTS", 4096),
		RingMaxAge: envDur("SLUICE_RING_MAX_AGE", 60*time.Second),

		JWKSURL:      env("SLUICE_JWKS_URL", ""),
		JWKSRefresh:  envDur("SLUICE_JWKS_REFRESH", 5*time.Minute),
		JWTAlg:       env("SLUICE_JWT_ALG", "ES256"),
		JWTIssuer:    env("SLUICE_JWT_ISSUER", ""),
		JWTAudience:  env("SLUICE_JWT_AUDIENCE", "authenticated"),
		JWTLeeway:    envDur("SLUICE_JWT_LEEWAY", 10*time.Second),
		AnonRole:     env("SLUICE_ANON_ROLE", "anon"),
		AllowedRoles: envList("SLUICE_ALLOWED_ROLES", []string{"anon", "authenticated", "service_role"}),

		AuthzLease:     envDur("SLUICE_AUTHZ_LEASE", 60*time.Second),
		CatalogRefresh: envDur("SLUICE_CATALOG_REFRESH", 30*time.Second),
		ShapeOracle:    strings.ToLower(env("SLUICE_SHAPE_ORACLE", "rls")),
		IssuerURL:      env("SLUICE_ISSUER_URL", ""),
		IssuerBearer:   env("SLUICE_ISSUER_BEARER", ""),
		IssuerTimeout:  envDur("SLUICE_ISSUER_TIMEOUT", 2*time.Second),
		TierC:          env("SLUICE_TIER_C", "allow"),
		TierCMaxProbes: envInt("SLUICE_TIER_C_MAX_PROBES_PER_SECOND", 2000),
		// Tier B reimplements PostgreSQL's evaluation semantics in Go, so the
		// first few decisions on each subscription are cross-checked against
		// PostgreSQL evaluating the same predicate on the same tuple. A
		// disagreement downgrades to Tier C. Cheap (bounded per subscription)
		// and the only real defence against a semantic difference that parsing
		// correctly does not rule out.
		TierBVerify:     envInt("SLUICE_TIER_B_VERIFY", 5),
		UnindexedMax:    envInt("SLUICE_UNINDEXED_SHAPES_MAX", 200),
		ReplicaIdentity: env("SLUICE_REPLICA_IDENTITY", "warn"),
		DegradedDeletes: env("SLUICE_DEGRADED_DELETES", "withhold"),

		SnapshotEnabled:  envBool("SLUICE_SNAPSHOT_ENABLED", true),
		SnapshotMaxConc:  envInt("SLUICE_SNAPSHOT_MAX_CONCURRENT", 4),
		SnapshotMaxRows:  envInt("SLUICE_SNAPSHOT_MAX_ROWS", 50000),
		SnapshotPageSize: envInt("SLUICE_SNAPSHOT_PAGE_SIZE", 1000),

		Heartbeat:          envDur("SLUICE_HEARTBEAT", 20*time.Second),
		StreamQueue:        envInt("SLUICE_STREAM_QUEUE", 256),
		WriteTimeout:       envDur("SLUICE_WRITE_TIMEOUT", 10*time.Second),
		MaxStreams:         envInt("SLUICE_MAX_STREAMS", 50000),
		MaxSubsPerStream:   envInt("SLUICE_MAX_SUBS_PER_STREAM", 100),
		MaxShapesPerStream: envInt("SLUICE_MAX_SHAPES_PER_STREAM", 20),
		MaxPayloadBytes:    envInt("SLUICE_MAX_PAYLOAD_BYTES", 256*1024),
		MaxChangeBytes:     envInt("SLUICE_MAX_CHANGE_BYTES", 1024*1024),
		SubscribeRate:      envInt("SLUICE_SUBSCRIBE_RATE", 20),
		PublishRate:        envInt("SLUICE_PUBLISH_RATE", 100),
		PresenceRate:       envInt("SLUICE_PRESENCE_RATE", 5),
		PresenceWindow:     envDur("SLUICE_PRESENCE_WINDOW", 30*time.Second),
		PresenceBcast:      envDur("SLUICE_PRESENCE_BROADCAST", 1500*time.Millisecond),
		PresenceMaxKeys:    envInt("SLUICE_PRESENCE_MAX_KEYS", 10),

		HookTTL:     envDur("SLUICE_CHANNEL_HOOK_TTL", 60*time.Second),
		HookTimeout: envDur("SLUICE_CHANNEL_HOOK_TIMEOUT", 2*time.Second),

		RevocationEnabled: envBool("SLUICE_REVOCATION_ENABLED", false),
		SessionsTable:     env("SLUICE_REVOCATION_SESSIONS_TABLE", "auth.sessions"),
		UsersTable:        env("SLUICE_REVOCATION_USERS_TABLE", "auth.users"),
		VerifyOnSubscribe: envBool("SLUICE_REVOCATION_VERIFY_ON_SUBSCRIBE", true),

		MetricsEnabled:     envBool("SLUICE_METRICS_ENABLED", true),
		DiagnosticsEnabled: envBool("SLUICE_DIAGNOSTICS_ENABLED", true),
	}

	chans, err := parseChannels(env("SLUICE_CHANNELS", "room:public"))
	if err != nil {
		return nil, err
	}
	c.Channels = chans

	return c, c.validate()
}

func (c *Config) validate() error {
	if c.ReplURL == "" {
		return fmt.Errorf("SLUICE_DB_REPL_URL is required")
	}
	if !strings.Contains(c.ReplURL, "replication=database") {
		return fmt.Errorf("SLUICE_DB_REPL_URL must include replication=database; " +
			"logical replication commands are only available over a replication connection")
	}
	if c.AuthzURL == "" {
		return fmt.Errorf("SLUICE_DB_AUTHZ_URL is required")
	}
	if strings.Contains(c.AuthzURL, "replication=") {
		return fmt.Errorf("SLUICE_DB_AUTHZ_URL must not be a replication connection")
	}
	if c.JWKSURL == "" {
		return fmt.Errorf("SLUICE_JWKS_URL is required")
	}
	switch c.JWTAlg {
	case "ES256", "RS256":
	default:
		// HS256 is deliberately not offered. Accepting a symmetric algorithm
		// alongside a public JWKS is how a verification key becomes a signing
		// oracle.
		return fmt.Errorf("SLUICE_JWT_ALG must be ES256 or RS256, got %q", c.JWTAlg)
	}

	switch c.Streaming {
	case "off", "on", "parallel":
	default:
		return fmt.Errorf("SLUICE_STREAMING must be off, on or parallel")
	}
	// Mirror PostgreSQL's own gating so a bad combination fails at startup with
	// a clear message rather than at START_REPLICATION with a server error.
	minVersion := 1
	if c.Streaming == "on" {
		minVersion = 2
	}
	if c.Streaming == "parallel" {
		minVersion = 4
	}
	if c.ProtoVersion < minVersion {
		return fmt.Errorf("SLUICE_PROTO_VERSION=%d does not support streaming=%s, need %d or higher",
			c.ProtoVersion, c.Streaming, minVersion)
	}
	if c.ProtoVersion < 1 || c.ProtoVersion > 4 {
		return fmt.Errorf("SLUICE_PROTO_VERSION must be 1..4 (PostgreSQL 18 supports at most 4), got %d",
			c.ProtoVersion)
	}
	if c.Binary {
		// Not fatal, but the decoder only handles the text path today, so be
		// explicit rather than mysteriously mangling values.
		return fmt.Errorf("SLUICE_BINARY=true is not supported: the decoder handles the " +
			"text format only, and binary was measured to be larger for typical rows")
	}

	switch c.TierC {
	case "allow", "deny":
	default:
		return fmt.Errorf("SLUICE_TIER_C must be allow or deny")
	}
	switch c.ReplicaIdentity {
	case "warn", "strict":
	default:
		return fmt.Errorf("SLUICE_REPLICA_IDENTITY must be warn or strict")
	}
	switch c.DegradedDeletes {
	case "withhold", "deliver":
	default:
		return fmt.Errorf("SLUICE_DEGRADED_DELETES must be withhold or deliver")
	}
	if c.Heartbeat < 5*time.Second {
		return fmt.Errorf("SLUICE_HEARTBEAT below 5s is counterproductive: frequent " +
			"keepalives wake the cellular modem and reset the LTE RRC inactivity timer")
	}
	if c.MessagePrefix == "" {
		return fmt.Errorf("SLUICE_MESSAGE_PREFIX must not be empty; an empty prefix would " +
			"expose every pg_logical_emit_message in the database as a channel")
	}
	switch c.ShapeOracle {
	case "rls":
	case "issuer":
		if c.IssuerURL == "" {
			return fmt.Errorf("SLUICE_ISSUER_URL is required when SLUICE_SHAPE_ORACLE=issuer")
		}
		if c.IssuerBearer == "" {
			return fmt.Errorf("SLUICE_ISSUER_BEARER is required when SLUICE_SHAPE_ORACLE=issuer")
		}
		if c.IssuerTimeout <= 0 {
			return fmt.Errorf("SLUICE_ISSUER_TIMEOUT must be positive")
		}
	default:
		return fmt.Errorf("SLUICE_SHAPE_ORACLE must be rls or issuer, got %q", c.ShapeOracle)
	}
	return nil
}

// IssuerMode reports whether this process uses the HTTP issuer as the shape oracle.
func (c *Config) IssuerMode() bool { return c.ShapeOracle == "issuer" }

// Channel returns the configured policy for a channel name's namespace.
func (c *Config) Channel(name string) (Channel, bool) {
	ns := name
	if i := strings.IndexByte(name, ':'); i >= 0 {
		ns = name[:i]
	}
	for _, ch := range c.Channels {
		if ch.Namespace == ns {
			return ch, true
		}
	}
	return Channel{}, false
}

func parseChannels(s string) ([]Channel, error) {
	var out []Channel
	for _, spec := range strings.Split(s, ",") {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		parts := strings.SplitN(spec, ":", 3)
		if len(parts) < 2 {
			return nil, fmt.Errorf("SLUICE_CHANNELS entry %q must be namespace:mode[:hook_url]", spec)
		}
		ch := Channel{Namespace: parts[0], Mode: ChannelMode(parts[1])}
		switch ch.Mode {
		case ChannelPublic, ChannelOwner:
		case ChannelHook:
			if len(parts) != 3 || parts[2] == "" {
				return nil, fmt.Errorf("SLUICE_CHANNELS entry %q with mode hook needs a URL", spec)
			}
			ch.HookURL = parts[2]
		default:
			return nil, fmt.Errorf("SLUICE_CHANNELS entry %q has unknown mode %q", spec, parts[1])
		}
		out = append(out, ch)
	}
	return out, nil
}

func defaultNodeID() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "sluice"
	}
	return h
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(k string, def bool) bool {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envList(k string, def []string) []string {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}
