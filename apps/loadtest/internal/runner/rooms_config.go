package runner

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// roomClass is a group of identical rooms: Count rooms of Members members, of
// whom Streams hold an open stream and Typing (on average) keep typing.
type roomClass struct {
	Count   int     `json:"count"`
	Members int     `json:"members"`
	Streams int     `json:"streams"`
	Typing  float64 `json:"typing"`
}

// roomsConfig drives the production-shaped profiles: rooms, bigtxn,
// reconnect and calibrate.
type roomsConfig struct {
	Classes        []roomClass   `json:"classes"`
	Shapes         int           `json:"shapes"`
	MsgEvery       time.Duration `json:"msg_every"`
	// MemberUpdates makes each message also update every member's
	// last_message_at (1 + N rows); without it a message is its one row.
	MemberUpdates  bool          `json:"member_updates"`
	TypingEvery    time.Duration `json:"typing_every"`
	TypingEnabled  bool          `json:"typing_enabled"`
	Duration       time.Duration `json:"duration"`
	Arrival        time.Duration `json:"arrival"`
	ArrivalTraffic bool          `json:"arrival_traffic"`
	Wave           int           `json:"wave"`
	OpenTimeout    time.Duration `json:"open_timeout"`
	TokenEvery     time.Duration `json:"token_every"`
	// Churn is the share of open streams ended and reopened every minute.
	Churn           float64         `json:"churn,omitempty"`
	TokenTTL        time.Duration   `json:"token_ttl"`
	ReplicaIdentity string          `json:"replica_identity"`
	SSEBuffer       int             `json:"sse_buffer"`
	SampleEvery     time.Duration   `json:"sample_every"`
	StatsEvery      time.Duration   `json:"stats_every"`
	Drain           time.Duration   `json:"drain"`
	Backoff         []time.Duration `json:"backoff"`
	Seed            uint64          `json:"seed"`
	Label           string          `json:"label"`

	// Pass criteria.
	P95Max      time.Duration `json:"p95_max"`
	DeliveryMin float64       `json:"delivery_min"`
	SlotLagMax  int64         `json:"slot_lag_max_bytes"`

	// bigtxn
	BigRows     []int         `json:"bigtxn_rows,omitempty"`
	BigStreamed float64       `json:"bigtxn_streamed,omitempty"`
	BigAfter    time.Duration `json:"bigtxn_after,omitempty"`
	BigEvery    time.Duration `json:"bigtxn_every,omitempty"`
	BigRepeat   int           `json:"bigtxn_repeat,omitempty"`
	BigBaseline time.Duration `json:"bigtxn_baseline,omitempty"`
	BigTimeout  time.Duration `json:"bigtxn_timeout,omitempty"`

	// reconnect
	ReconnectMode    string        `json:"reconnect_mode,omitempty"` // restart | cut
	ReconnectAfter   time.Duration `json:"reconnect_after,omitempty"`
	ReconnectEvery   time.Duration `json:"reconnect_every,omitempty"`
	ReconnectCount   int           `json:"reconnect_count,omitempty"`
	ReconnectTimeout time.Duration `json:"reconnect_timeout,omitempty"`
	ProxyDown        time.Duration `json:"proxy_down,omitempty"`
	ProxyControl     string        `json:"proxy_control,omitempty"`
	SluiceService    string        `json:"sluice_service,omitempty"`
	StopTimeout      time.Duration `json:"stop_timeout,omitempty"`

	// calibrate
	CalWindows int           `json:"calibrate_windows,omitempty"`
	CalWindow  time.Duration `json:"calibrate_window,omitempty"`

	// ramp: rooms join in steps of about RampStep streams; each step is
	// judged on a window of RampHold after it settles.
	RampStep      int           `json:"ramp_step_streams,omitempty"`
	RampArrival   time.Duration `json:"ramp_arrival,omitempty"`
	RampSettle    time.Duration `json:"ramp_settle,omitempty"`
	RampHold      time.Duration `json:"ramp_hold,omitempty"`
	RampAfterFail int           `json:"ramp_after_fail,omitempty"`

	// The resume-buffer probe: every ProbeEvery, a short stream per age in
	// ProbeAges asks to resume each table from that far back.
	ProbeEvery time.Duration   `json:"probe_every,omitempty"`
	ProbeAges  []time.Duration `json:"probe_ages,omitempty"`
	RingMemory float64         `json:"ring_memory,omitempty"`
	RingMaxAge time.Duration   `json:"ring_max_age,omitempty"`
	// ProbeDerivedWindow is how far back each table's change rate is averaged
	// for a probe round.
	ProbeDerivedWindow time.Duration `json:"probe_derived_window,omitempty"`

	DockerSocket string `json:"-"`

	// Profiles of a Sluice built with pprof: when, how long, and where.
	PprofAt      time.Duration   `json:"pprof_at,omitempty"`
	PprofSeconds time.Duration   `json:"pprof_seconds,omitempty"`
	PprofURL     string          `json:"pprof_url,omitempty"`
	PprofHeapAt  []time.Duration `json:"pprof_heap_at,omitempty"`
}

func (c roomsConfig) streams() int {
	n := 0
	for _, k := range c.Classes {
		n += k.Count * k.Streams
	}
	return n
}

func (c roomsConfig) rooms() int {
	n := 0
	for _, k := range c.Classes {
		n += k.Count
	}
	return n
}

func (c roomsConfig) members() int {
	n := 0
	for _, k := range c.Classes {
		n += k.Count * k.Members
	}
	return n
}

// expectedRates is what the configuration should produce in steady state,
// with every stream open: changes decoded per second, and events delivered
// per second by kind.
func (c roomsConfig) expectedRates() (changes, messages, members, typing float64) {
	if c.MsgEvery <= 0 {
		return 0, 0, 0, 0
	}
	per := c.MsgEvery.Seconds()
	for _, k := range c.Classes {
		r := float64(k.Count)
		changes += r / per
		messages += r * float64(k.Streams) / per
		if c.MemberUpdates {
			changes += r * float64(k.Members) / per
			if c.Shapes >= 2 {
				members += r * float64(k.Streams) / per
			}
		}
		if c.TypingEnabled && c.TypingEvery > 0 && k.Streams > 1 {
			typing += float64(typistsUpTo(k.Typing, k.Count)) * float64(k.Streams-1) / c.TypingEvery.Seconds()
		}
	}
	return changes, messages, members, typing
}

func loadRoomsConfig(scenario string) (roomsConfig, error) {
	c := roomsConfig{
		Shapes:          envInt("LOAD_SHAPES", 7),
		MsgEvery:        envDur("LOAD_MSG_EVERY", 15*time.Second),
		MemberUpdates:   envBool("LOAD_MEMBER_UPDATES", true),
		TypingEvery:     envDur("LOAD_TYPING_EVERY", 2*time.Second),
		TypingEnabled:   envBool("LOAD_TYPING_ENABLED", true),
		Duration:        envDur("LOAD_DURATION", 10*time.Minute),
		Arrival:         envDur("LOAD_ARRIVAL", 0),
		ArrivalTraffic:  envBool("LOAD_ARRIVAL_TRAFFIC", true),
		Wave:            envInt("LOAD_WAVE", 256),
		OpenTimeout:     envDur("LOAD_OPEN_TIMEOUT", 3*time.Minute),
		TokenEvery:      envDur("LOAD_TOKEN_EVERY", 15*time.Minute),
		Churn:           envFloat("LOAD_CHURN", 0),
		TokenTTL:        envDur("LOAD_TOKEN_TTL", time.Hour),
		ReplicaIdentity: strings.ToLower(envOr("LOAD_REPLICA_IDENTITY", "full")),
		SSEBuffer:       envInt("LOAD_SSE_BUFFER", 16),
		SampleEvery:     envDur("LOAD_SAMPLE_EVERY", 5*time.Second),
		StatsEvery:      envDur("LOAD_STATS_EVERY", time.Minute),
		Drain:           envDur("LOAD_DRAIN", 5*time.Second),
		Seed:            uint64(envInt("LOAD_SEED", 0)),
		Label:           os.Getenv("LOAD_LABEL"),

		P95Max:      envDur("LOAD_P95_MAX", 500*time.Millisecond),
		DeliveryMin: envFloat("LOAD_DELIVERY_MIN", 0.99),
		SlotLagMax:  int64(envInt("LOAD_SLOT_LAG_MAX_MB", 64)) << 20,

		DockerSocket: envOr("LOAD_DOCKER_SOCKET", "/var/run/docker.sock"),

		ProbeEvery: envDur("LOAD_PROBE_EVERY", 2*time.Minute),
		RingMemory: envFloat("SLUICE_RING_MEMORY", 0.1),
		RingMaxAge: envDur("SLUICE_RING_MAX_AGE", time.Minute),

		ProbeDerivedWindow: time.Minute,

		PprofAt:      envDur("LOAD_PPROF_AT", 0),
		PprofSeconds: envDur("LOAD_PPROF_SECONDS", time.Minute),
		PprofURL:     envOr("LOAD_PPROF_URL", "http://sluice-rooms:6060/debug/pprof"),
	}
	ages, err := parseDurations(envOr("LOAD_PROBE_AGES", "500ms,1s,2s,3s,5s,7500ms,10s,15s,20s,30s,45s,60s,90s"))
	if err != nil {
		return c, fmt.Errorf("LOAD_PROBE_AGES: %w", err)
	}
	c.ProbeAges = ages
	if c.PprofAt > 0 {
		// What the GC finds live early and late in the steady phase.
		if c.PprofHeapAt, err = parseDurations(envOr("LOAD_PPROF_HEAP_AT", "2m,9m")); err != nil {
			return c, fmt.Errorf("LOAD_PPROF_HEAP_AT: %w", err)
		}
	}
	switch c.ReplicaIdentity {
	case "full", "pk":
	default:
		return c, fmt.Errorf("LOAD_REPLICA_IDENTITY must be full or pk, got %q", c.ReplicaIdentity)
	}
	if c.Shapes < 1 || c.Shapes > 20 {
		return c, fmt.Errorf("LOAD_SHAPES must be 1..20")
	}
	b, err := parseDurations(envOr("LOAD_BACKOFF", "500ms,1s,2s,5s,10s"))
	if err != nil {
		return c, fmt.Errorf("LOAD_BACKOFF: %w", err)
	}
	c.Backoff = b

	classes, err := parseRoomClasses(os.Getenv("LOAD_ROOM_MIX"))
	if err != nil {
		return c, err
	}
	if len(classes) == 0 {
		k := roomClass{
			Count:   envInt("LOAD_ROOMS", 10),
			Members: envInt("LOAD_ROOM_MEMBERS", 80),
			Streams: envInt("LOAD_ROOM_STREAMS", 20),
			Typing:  envFloat("LOAD_TYPING", 1),
		}
		// LOAD_STREAMS sizes the room count for a total, when LOAD_ROOMS is
		// not given: the reconnect profile is specified by streams.
		if n := envInt("LOAD_STREAMS", 0); n > 0 && os.Getenv("LOAD_ROOMS") == "" && k.Streams > 0 {
			k.Count = int(math.Ceil(float64(n) / float64(k.Streams)))
		}
		classes = []roomClass{k}
	}
	for _, k := range classes {
		if k.Count < 0 || k.Members < 1 || k.Streams < 0 || k.Streams > k.Members || k.Typing < 0 {
			return c, fmt.Errorf("room class %+v: need members >= 1 and 0 <= streams <= members", k)
		}
	}
	c.Classes = classes

	switch scenario {
	case "bigtxn":
		c.BigRows = parseInts(envOr("LOAD_BIGTXN_ROWS", "1500,10000,100000"))
		c.BigStreamed = envFloat("LOAD_BIGTXN_STREAMED", 0.25)
		c.BigAfter = envDur("LOAD_BIGTXN_AFTER", time.Minute)
		c.BigEvery = envDur("LOAD_BIGTXN_EVERY", 2*time.Minute)
		c.BigRepeat = max(1, envInt("LOAD_BIGTXN_REPEAT", 3))
		c.BigBaseline = envDur("LOAD_BIGTXN_BASELINE", 30*time.Second)
		c.BigTimeout = envDur("LOAD_BIGTXN_TIMEOUT", 5*time.Minute)
		if c.Shapes < 3 {
			return c, fmt.Errorf("bigtxn needs LOAD_SHAPES >= 3: the third shape is the notifications one")
		}
		if os.Getenv("LOAD_DURATION") == "" {
			// Long enough for every occurrence.
			n := len(c.BigRows) * c.BigRepeat
			c.Duration = c.BigAfter + time.Duration(n)*c.BigEvery
		}
	case "reconnect":
		c.ReconnectMode = strings.ToLower(envOr("LOAD_RECONNECT_MODE", "cut"))
		c.ReconnectAfter = envDur("LOAD_RECONNECT_AFTER", time.Minute)
		c.ReconnectEvery = envDur("LOAD_RECONNECT_EVERY", 2*time.Minute)
		c.ReconnectCount = max(1, envInt("LOAD_RECONNECT_COUNT", 3))
		c.ReconnectTimeout = envDur("LOAD_RECONNECT_TIMEOUT", 3*time.Minute)
		c.ProxyDown = envDur("LOAD_PROXY_DOWN", 0)
		c.ProxyControl = strings.TrimRight(envOr("LOAD_PROXY_CONTROL", "http://proxy:9000"), "/")
		c.SluiceService = envOr("LOAD_SLUICE_SERVICE", "sluice-rooms")
		c.StopTimeout = envDur("LOAD_STOP_TIMEOUT", 30*time.Second)
		switch c.ReconnectMode {
		case "restart", "cut":
		default:
			return c, fmt.Errorf("LOAD_RECONNECT_MODE must be restart or cut, got %q", c.ReconnectMode)
		}
		if os.Getenv("LOAD_DURATION") == "" {
			c.Duration = c.ReconnectAfter + time.Duration(c.ReconnectCount)*c.ReconnectEvery
		}
	case "ramp":
		c.RampStep = max(1, envInt("LOAD_RAMP_STEP", 1000))
		c.RampArrival = envDur("LOAD_RAMP_ARRIVAL", 20*time.Second)
		c.RampSettle = envDur("LOAD_RAMP_SETTLE", 15*time.Second)
		c.RampHold = envDur("LOAD_RAMP_HOLD", 90*time.Second)
		c.RampAfterFail = envInt("LOAD_RAMP_AFTER_FAIL", 0)
		c.ProbeEvery = envDur("LOAD_PROBE_EVERY", 0)
	case "calibrate":
		// A fixed load, so the CPU it costs compares between machines. Only
		// the window length and count may change.
		c.Classes = []roomClass{{Count: 40, Members: 100, Streams: 25, Typing: 2}}
		c.Shapes = 7
		c.MsgEvery = 5 * time.Second
		c.TypingEvery = 2 * time.Second
		c.TypingEnabled = true
		c.Arrival = 0
		c.ArrivalTraffic = false
		c.TokenEvery = time.Hour
		c.ReplicaIdentity = "full"
		c.Seed = 1
		c.CalWindows = max(1, envInt("LOAD_CALIBRATE_WINDOWS", 3))
		c.CalWindow = envDur("LOAD_CALIBRATE_WINDOW", time.Minute)
		c.ProbeEvery = 0
		c.Duration = 10*time.Second + time.Duration(c.CalWindows)*c.CalWindow
	}
	return c, nil
}

// parseRoomClasses reads "count:members:streams[:typing],..." .
func parseRoomClasses(s string) ([]roomClass, error) {
	var out []roomClass
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		f := strings.Split(part, ":")
		if len(f) < 3 || len(f) > 4 {
			return nil, fmt.Errorf("LOAD_ROOM_MIX entry %q: want count:members:streams[:typing]", part)
		}
		var k roomClass
		var err error
		if k.Count, err = strconv.Atoi(f[0]); err != nil {
			return nil, fmt.Errorf("LOAD_ROOM_MIX entry %q: %w", part, err)
		}
		if k.Members, err = strconv.Atoi(f[1]); err != nil {
			return nil, fmt.Errorf("LOAD_ROOM_MIX entry %q: %w", part, err)
		}
		if k.Streams, err = strconv.Atoi(f[2]); err != nil {
			return nil, fmt.Errorf("LOAD_ROOM_MIX entry %q: %w", part, err)
		}
		if len(f) == 4 {
			if k.Typing, err = strconv.ParseFloat(f[3], 64); err != nil {
				return nil, fmt.Errorf("LOAD_ROOM_MIX entry %q: %w", part, err)
			}
		}
		out = append(out, k)
	}
	return out, nil
}

func parseDurations(s string) ([]time.Duration, error) {
	var out []time.Duration
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		d, err := time.ParseDuration(p)
		if err != nil || d < 0 {
			return nil, fmt.Errorf("%q is not a duration", p)
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty")
	}
	return out, nil
}

func parseInts(s string) []int {
	var out []int
	for _, p := range strings.Split(s, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	return out
}

func envBool(k string, d bool) bool {
	if v := os.Getenv(k); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return d
}

// typistsUpTo is how many typists the first n rooms of a class with an
// average of k per room have between them: floor(k·n), so a class of R rooms
// has floor(k·R) and they are spread evenly.
func typistsUpTo(k float64, n int) int {
	return int(math.Floor(k*float64(n) + 1e-9))
}
