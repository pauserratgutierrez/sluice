// Package issuer is the load test's shape issuer and channel hook: the HTTP
// endpoints an application's backend serves to Sluice in issuer mode.
//
// It answers from memberships the runner seeds, after a configurable latency
// and behind a configurable concurrency limit, so a run sees the backend a
// product would put there rather than an instant map lookup.
package issuer

import (
	"crypto/hmac"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/procstat"
)

func Main() error {
	bearer := os.Getenv("SLUICE_ISSUER_BEARER")
	if bearer == "" {
		bearer = os.Getenv("ISSUER_BEARER")
	}
	if bearer == "" {
		return fmt.Errorf("issuer: SLUICE_ISSUER_BEARER is required")
	}
	lat, err := ParseLatency(os.Getenv("LOAD_ISSUER_LATENCY"), envFloat("LOAD_ISSUER_JITTER", 0.2))
	if err != nil {
		return err
	}
	s := newServer(bearer, lat, envInt("LOAD_ISSUER_CONCURRENCY", 0), envInt("LOAD_ISSUER_QUEUE_MAX", 0),
		envInt("LOAD_HOOK_TTL", 0))
	switch order := envOr("LOAD_HOLD_ORDER", "user-first"); order {
	case "user-first":
	case "room-first":
		s.roomFirst = true
	default:
		return fmt.Errorf("issuer: LOAD_HOLD_ORDER is %q, want user-first or room-first", order)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /shapes", s.shapes)
	mux.HandleFunc("POST /hook", s.hook)
	mux.HandleFunc("POST /seed", s.seed)
	mux.HandleFunc("POST /reset", s.reset)
	mux.HandleFunc("GET /stats", s.statsHandler)

	addr := envOr("ISSUER_LISTEN", "0.0.0.0:8080")
	fmt.Printf("issuer: listening on %s (latency %s, concurrency %d, queue max %d, hook ttl %d)\n",
		addr, lat, s.limit, s.queueMax, s.hookTTL)
	hs := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return hs.ListenAndServe()
}

type server struct {
	bearer string
	lat    Latency
	// limit is how many requests are served at once; 0 is unlimited. Past it
	// they queue, up to queueMax (0: no bound), and are refused with 503 and
	// Retry-After beyond that, the way a backend sheds load.
	limit    int
	queueMax int
	gate     chan struct{}
	hookTTL  int
	// roomFirst writes membership holds as room_id=eq.R,user_id=eq.U. Sluice
	// 0.8.1 indexes a hold by the first of its equally good columns (0.8.0 by
	// either, at random), so this order puts every member of a room in one
	// list; the default, user first, gives each member a list of their own.
	roomFirst bool

	mu      sync.RWMutex
	members map[string]struct{}       // project memberships (issuer, kick)
	rooms   map[int64]map[string]bool // room -> members (rooms family)
	userRm  map[string]int64          // user -> the first room they joined

	stats *stats
}

func newServer(bearer string, lat Latency, limit, queueMax, hookTTL int) *server {
	s := &server{
		bearer: bearer, lat: lat, limit: limit, queueMax: queueMax, hookTTL: hookTTL,
		members: map[string]struct{}{},
		rooms:   map[int64]map[string]bool{},
		userRm:  map[string]int64{},
		stats:   newStats(),
	}
	if limit > 0 {
		s.gate = make(chan struct{}, limit)
	}
	return s
}

type issuerRequest struct {
	Action   string `json:"action"`
	Identity struct {
		Sub string `json:"sub"`
	} `json:"identity"`
	Requested struct {
		Schema string `json:"schema"`
		Table  string `json:"table"`
		Filter string `json:"filter"`
	} `json:"requested"`
}

type hookRequest struct {
	Action  string `json:"action"`
	Channel string `json:"channel"`
	Sub     string `json:"sub"`
}

type member struct {
	ProjectID string `json:"project_id"`
	UserID    string `json:"user_id"`
}

type roomMember struct {
	RoomID int64  `json:"room_id"`
	UserID string `json:"user_id"`
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) reset(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	s.mu.Lock()
	s.members = map[string]struct{}{}
	s.rooms = map[int64]map[string]bool{}
	s.userRm = map[string]int64{}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) seed(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	var body struct {
		Members []member     `json:"members"`
		Rooms   []roomMember `json:"rooms"`
	}
	if err := readJSON(r, &body); err != nil {
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	for _, m := range body.Members {
		s.members[key(m.ProjectID, m.UserID)] = struct{}{}
	}
	for _, m := range body.Rooms {
		u := strings.ToLower(m.UserID)
		set := s.rooms[m.RoomID]
		if set == nil {
			set = map[string]bool{}
			s.rooms[m.RoomID] = set
		}
		set[u] = true
		if _, ok := s.userRm[u]; !ok {
			s.userRm[u] = m.RoomID
		}
	}
	n := len(s.members) + len(s.userRm)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "members": n})
}

// admit waits for a slot under the concurrency limit. It returns false, having
// answered, when the queue is full or the caller went away.
func (s *server) admit(w http.ResponseWriter, r *http.Request) (release func(), ok bool) {
	if s.gate == nil {
		s.stats.enter(0)
		return func() { s.stats.leave() }, true
	}
	select {
	case s.gate <- struct{}{}:
		s.stats.enter(0)
		return func() { <-s.gate; s.stats.leave() }, true
	default:
	}
	q := s.stats.queued.Add(1)
	defer s.stats.queued.Add(-1)
	s.stats.peakQueue(q)
	if s.queueMax > 0 && int(q) > s.queueMax {
		s.stats.count(kindRejected)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return nil, false
	}
	start := time.Now()
	select {
	case s.gate <- struct{}{}:
		s.stats.enter(time.Since(start))
		return func() { <-s.gate; s.stats.leave() }, true
	case <-r.Context().Done():
		s.stats.count(kindAbandoned)
		return nil, false
	}
}

func (s *server) shapes(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	var req issuerRequest
	if err := readJSON(r, &req); err != nil {
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}
	if req.Action == "refresh" {
		s.stats.count(kindRefresh)
	} else {
		s.stats.count(kindSubscribe)
	}
	release, ok := s.admit(w, r)
	if !ok {
		return
	}
	defer release()
	s.lat.Sleep()
	writeJSON(w, http.StatusOK, s.decide(req))
}

func (s *server) decide(req issuerRequest) map[string]any {
	deny := map[string]any{"allow": false}
	sub := strings.ToLower(strings.TrimSpace(req.Identity.Sub))
	if sub == "" {
		return deny
	}
	switch req.Requested.Table {
	case "project_docs":
		projectID := parseEq(req.Requested.Filter, "project_id")
		if projectID == "" {
			return deny
		}
		s.mu.RLock()
		_, ok := s.members[key(projectID, sub)]
		s.mu.RUnlock()
		if !ok {
			return deny
		}
		return grant("project_docs", "project_id=eq."+projectID, []string{"id", "project_id", "title", "body"},
			"project_members", "project_id=eq."+projectID+",user_id=eq."+sub)
	case "room_messages":
		room, err := strconv.ParseInt(parseEq(req.Requested.Filter, "room_id"), 10, 64)
		if err != nil {
			return deny
		}
		s.mu.RLock()
		ok := s.rooms[room][sub]
		s.mu.RUnlock()
		if !ok {
			return deny
		}
		return grant("room_messages", fmt.Sprintf("room_id=eq.%d", room), nil,
			"room_members", s.memberHold(room, sub))
	case "room_members", "notifications":
		// A member's own rows: their memberships, their notifications. The
		// grant is held by their membership of the first room they joined.
		if strings.ToLower(parseEq(req.Requested.Filter, "user_id")) != sub {
			return deny
		}
		s.mu.RLock()
		room, ok := s.userRm[sub]
		s.mu.RUnlock()
		if !ok {
			return deny
		}
		return grant(req.Requested.Table, "user_id=eq."+sub, nil,
			"room_members", s.memberHold(room, sub))
	}
	return deny
}

// memberHold is the hold filter for sub's membership of room.
func (s *server) memberHold(room int64, sub string) string {
	if s.roomFirst {
		return fmt.Sprintf("room_id=eq.%d,user_id=eq.%s", room, sub)
	}
	return fmt.Sprintf("user_id=eq.%s,room_id=eq.%d", sub, room)
}

func grant(table, filter string, cols []string, holdTable, holdFilter string) map[string]any {
	shape := map[string]any{"schema": "public", "table": table, "filter": filter}
	if cols != nil {
		shape["columns"] = cols
	}
	return map[string]any{
		"allow": true,
		"shape": shape,
		"holds": []map[string]any{{"schema": "public", "table": holdTable, "filter": holdFilter}},
	}
}

// hook decides channel joins for `room:<id>`: members only.
func (s *server) hook(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	var req hookRequest
	if err := readJSON(r, &req); err != nil {
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}
	s.stats.count(kindHook)
	release, ok := s.admit(w, r)
	if !ok {
		return
	}
	defer release()
	s.lat.Sleep()
	name, ok := strings.CutPrefix(req.Channel, "room:")
	room, err := strconv.ParseInt(name, 10, 64)
	sub := strings.ToLower(req.Sub)
	allow := false
	if ok && err == nil {
		s.mu.RLock()
		allow = s.rooms[room][sub]
		s.mu.RUnlock()
	}
	writeJSON(w, http.StatusOK, map[string]any{"allow": allow, "ttl": s.hookTTL})
}

func (s *server) statsHandler(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	writeJSON(w, http.StatusOK, s.stats.snapshot(since))
}

func (s *server) auth(w http.ResponseWriter, r *http.Request) bool {
	got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if !hmac.Equal([]byte(got), []byte(s.bearer)) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

// Latency is how long the issuer takes to answer: uniform in [Min, Max].
type Latency struct{ Min, Max time.Duration }

// ParseLatency reads "10ms" (that, ± jitter as a fraction) or "5ms-20ms" (a
// uniform range). Empty or "0" is no latency.
func ParseLatency(s string, jitter float64) (Latency, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return Latency{}, nil
	}
	if lo, hi, ok := strings.Cut(s, "-"); ok {
		a, err1 := time.ParseDuration(strings.TrimSpace(lo))
		b, err2 := time.ParseDuration(strings.TrimSpace(hi))
		if err1 != nil || err2 != nil || a < 0 || b < a {
			return Latency{}, fmt.Errorf("LOAD_ISSUER_LATENCY %q: want a duration or min-max", s)
		}
		return Latency{a, b}, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return Latency{}, fmt.Errorf("LOAD_ISSUER_LATENCY %q: want a duration or min-max", s)
	}
	j := time.Duration(float64(d) * max(0, min(jitter, 1)))
	return Latency{d - j, d + j}, nil
}

func (l Latency) Sleep() {
	if l.Max <= 0 {
		return
	}
	d := l.Min
	if l.Max > l.Min {
		d += rand.N(l.Max - l.Min)
	}
	time.Sleep(d)
}

func (l Latency) String() string {
	if l.Max <= 0 {
		return "0"
	}
	return fmt.Sprintf("%s-%s", l.Min, l.Max)
}

// ---------------------------------------------------------------------------
// Statistics: totals, and calls per second for the last few hours, which is
// what a run reads its peak rate from.
// ---------------------------------------------------------------------------

const (
	kindSubscribe = iota
	kindRefresh
	kindHook
	kindRejected
	kindAbandoned
	nKinds
)

var kindNames = [nKinds]string{"subscribe", "refresh", "hook", "rejected", "abandoned"}

const statsWindow = 4 * 3600

type second struct {
	at int64
	n  [nKinds]int64
}

type stats struct {
	mu       sync.Mutex
	secs     []second
	total    [nKinds]int64
	inflight atomic.Int64
	queued   atomic.Int64
	peakIn   atomic.Int64
	peakQ    atomic.Int64
	waitNs   atomic.Int64
	waitMax  atomic.Int64
	served   atomic.Int64
	start    time.Time
}

func newStats() *stats {
	return &stats{secs: make([]second, statsWindow), start: time.Now()}
}

func (st *stats) count(kind int) {
	now := time.Now().Unix()
	st.mu.Lock()
	b := &st.secs[now%statsWindow]
	if b.at != now {
		*b = second{at: now}
	}
	b.n[kind]++
	st.total[kind]++
	st.mu.Unlock()
}

func (st *stats) enter(wait time.Duration) {
	n := st.inflight.Add(1)
	for {
		p := st.peakIn.Load()
		if n <= p || st.peakIn.CompareAndSwap(p, n) {
			break
		}
	}
	st.served.Add(1)
	st.waitNs.Add(int64(wait))
	for {
		p := st.waitMax.Load()
		if int64(wait) <= p || st.waitMax.CompareAndSwap(p, int64(wait)) {
			break
		}
	}
}

func (st *stats) leave() { st.inflight.Add(-1) }

func (st *stats) peakQueue(q int64) {
	for {
		p := st.peakQ.Load()
		if q <= p || st.peakQ.CompareAndSwap(p, q) {
			break
		}
	}
}

// Snapshot is what GET /stats returns.
type Snapshot struct {
	Now          int64            `json:"now"`
	Totals       map[string]int64 `json:"totals"`
	Seconds      []SecondCounts   `json:"seconds,omitempty"`
	Inflight     int64            `json:"inflight"`
	Queued       int64            `json:"queued"`
	PeakInflight int64            `json:"peak_inflight"`
	PeakQueued   int64            `json:"peak_queued"`
	WaitAvgMs    float64          `json:"wait_avg_ms"`
	WaitMaxMs    float64          `json:"wait_max_ms"`
	CPUSeconds   float64          `json:"cpu_seconds"`
	RSSBytes     int64            `json:"rss_bytes"`
}

// SecondCounts is one second's calls, by kind.
type SecondCounts struct {
	At int64            `json:"at"`
	N  map[string]int64 `json:"n"`
}

func (st *stats) snapshot(since int64) Snapshot {
	now := time.Now().Unix()
	out := Snapshot{
		Now:          now,
		Totals:       map[string]int64{},
		Inflight:     st.inflight.Load(),
		Queued:       st.queued.Load(),
		PeakInflight: st.peakIn.Load(),
		PeakQueued:   st.peakQ.Load(),
		WaitMaxMs:    float64(st.waitMax.Load()) / 1e6,
	}
	if n := st.served.Load(); n > 0 {
		out.WaitAvgMs = float64(st.waitNs.Load()) / float64(n) / 1e6
	}
	out.CPUSeconds, out.RSSBytes = procstat.Self()
	st.mu.Lock()
	for k := range nKinds {
		out.Totals[kindNames[k]] = st.total[k]
	}
	if since > 0 {
		from := max(since, now-statsWindow+1)
		for t := from; t <= now; t++ {
			b := st.secs[t%statsWindow]
			if b.at != t {
				continue
			}
			sc := SecondCounts{At: t, N: map[string]int64{}}
			for k := range nKinds {
				if b.n[k] > 0 {
					sc.N[kindNames[k]] = b.n[k]
				}
			}
			out.Seconds = append(out.Seconds, sc)
		}
	}
	st.mu.Unlock()
	return out
}

func parseEq(filter, col string) string {
	prefix := col + "=eq."
	for _, part := range strings.Split(filter, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, prefix) {
			return strings.TrimSpace(part[len(prefix):])
		}
	}
	return ""
}

func key(projectID, userID string) string {
	return projectID + "\x00" + strings.ToLower(userID)
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return d
}

func envFloat(k string, d float64) float64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return d
}
