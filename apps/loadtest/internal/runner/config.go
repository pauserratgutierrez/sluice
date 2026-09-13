package runner

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Scenario string
	Axis     string // all | users | conns
	Hunt     bool

	SluiceURL    string
	MetricsURL   string
	DBURL        string
	IssuerURL    string
	IssuerBearer string
	PrivateJWK   string
	ResultsDir   string
	TargetImage  string

	Start       int
	Cap         int
	Granularity int
	Changes     int
	Wave        int
	OpenMin     float64
	DeliveryMin float64
	Ladder      []int

	OpenTimeout     time.Duration
	DeliveryTimeout time.Duration
	PresenceWait    time.Duration
	TokenTTL        time.Duration
	Channel         string
}

func LoadConfig() Config {
	c := Config{
		Scenario:        envOr("LOAD_SCENARIO", "rls-a"),
		Axis:            envOr("LOAD_AXIS", "all"),
		SluiceURL:       strings.TrimRight(envOr("SLUICE_URL", "http://sluice-rls:4000/sluice/v1"), "/"),
		DBURL:           envOr("LOAD_DB_URL", ""),
		IssuerURL:       strings.TrimRight(envOr("ISSUER_URL", "http://issuer:8080"), "/"),
		IssuerBearer:    envOr("SLUICE_ISSUER_BEARER", ""),
		PrivateJWK:      envOr("LOAD_PRIVATE_JWK", "/keys/private.jwk"),
		ResultsDir:      envOr("LOAD_RESULTS", "/results"),
		TargetImage:     envOr("SLUICE_IMAGE", "ghcr.io/pauserratgutierrez/sluice:0.1.4"),
		Start:           envInt("LOAD_START", 100),
		Cap:             envInt("LOAD_MAX_STREAMS", 0),
		Granularity:     envInt("LOAD_GRANULARITY", 50),
		Changes:         envInt("LOAD_CHANGES", 20),
		Wave:            envInt("LOAD_WAVE", 256),
		OpenMin:         envFloat("LOAD_OPEN_MIN", 0.95),
		DeliveryMin:     envFloat("LOAD_DELIVERY_MIN", 0.90),
		OpenTimeout:     envDur("LOAD_OPEN_TIMEOUT", 3*time.Minute),
		DeliveryTimeout: envDur("LOAD_DELIVERY_TIMEOUT", 90*time.Second),
		PresenceWait:    envDur("LOAD_PRESENCE_WAIT", 8*time.Second),
		TokenTTL:        24 * time.Hour,
		Channel:         envOr("LOAD_CHANNEL", "room:load"),
	}
	c.MetricsURL = envOr("SLUICE_METRICS_URL", c.SluiceURL+"/metrics")
	c.Hunt = parseHunt(c.Scenario)
	if v := os.Getenv("LOAD_HUNT"); v != "" {
		c.Hunt = v == "1" || strings.EqualFold(v, "true") || (strings.EqualFold(v, "auto") && c.Hunt)
	}
	if s := os.Getenv("LOAD_LADDER"); s != "" {
		for _, p := range strings.Split(s, ",") {
			if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil && n > 0 {
				c.Ladder = append(c.Ladder, n)
			}
		}
	}
	if len(c.Ladder) == 0 {
		switch c.Scenario {
		case "rls-b", "broadcast", "mixed":
			c.Ladder = []int{100, 400, 800, 1500}
		case "rls-c", "kick":
			c.Ladder = []int{25, 50, 100, 200}
		case "presence":
			c.Ladder = []int{50, 100, 200, 400, 800}
		}
	}
	return c
}

func parseHunt(scenario string) bool {
	return scenario == "rls-a" || scenario == "issuer"
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return d
}

func envFloat(k string, d float64) float64 {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return n
		}
	}
	return d
}

func envDur(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		n, err := time.ParseDuration(v)
		if err == nil {
			return n
		}
	}
	return d
}
