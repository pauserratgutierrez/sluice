package runner

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/authn"
	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/dockerapi"
)

// The rooms family: production-shaped load against an issuer-mode Sluice.
//
//	rooms      R rooms of N members, M of them with a stream of S shapes and
//	           the room's hook channel; a message every LOAD_MSG_EVERY per
//	           room (1 INSERT + N UPDATEs in one transaction) and typing
//	           broadcasts; /token renewals; hook re-checks.
//	bigtxn     rooms, plus commits of thousands of notification rows.
//	reconnect  rooms, plus every stream dropped at once, by restarting
//	           Sluice or by cutting a proxy in front of it.
//	calibrate  a fixed rooms load that reports Sluice's CPU per 1000 events.

func isRoomsScenario(s string) bool {
	switch s {
	case "rooms", "bigtxn", "reconnect", "calibrate":
		return true
	}
	return false
}

type room struct {
	id      int64
	class   int
	members []string
	clients []*rclient
	seq     atomic.Int64
	open    atomic.Int64 // its clients with a stream open
}

type engine struct {
	cfg    Config
	rc     roomsConfig
	log    *slog.Logger
	signer *authn.Signer
	pool   *pgxpool.Pool
	docker *dockerapi.Client
	seed   uint64

	streamHTTP *http.Client
	ctlHTTP    *http.Client

	rooms   []*room
	clients []*rclient

	clientsCtx    context.Context
	clientsCancel context.CancelFunc
	trafficCtx    context.Context
	trafficCancel context.CancelFunc
	trafficOn     chan struct{}
	trafficOnce   sync.Once

	waveSem chan struct{}

	lat    recorders
	recv   [nLat]atomic.Int64
	expect [nLat]atomic.Int64
	meter  atomic.Int64 // events delivered since the last second tick

	liveClients  atomic.Int64
	firstReady   atomic.Int64
	openAttempts atomic.Int64
	openFails    atomic.Int64
	messages     atomic.Int64
	publishes    atomic.Int64

	ctr counters

	phase    atomic.Pointer[string]
	sampler  *sampler
	secMu    sync.Mutex
	secs     []int32 // delivered events per second since the run started
	marks    []mark
	wave     atomic.Pointer[waveStats]
	big      atomic.Pointer[bigWatch]
	started  time.Time
	arrivalT time.Duration

	windows []window
	bigs    []BigTxnResult
	pgInfo  map[string]string
	waves   []WaveResult
	cal     *CalibrationResult
}

type counters struct {
	mu sync.Mutex
	m  map[string]int64
}

func (c *counters) add(k string) { c.addN(k, 1) }

func (c *counters) addN(k string, n int64) {
	c.mu.Lock()
	if c.m == nil {
		c.m = map[string]int64{}
	}
	c.m[k] += n
	c.mu.Unlock()
}

func (c *counters) snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(c.m))
	for k, v := range c.m {
		out[k] = v
	}
	return out
}

func (e *engine) count(k string)           { e.ctr.add(k) }
func (e *engine) countN(k string, n int64) { e.ctr.addN(k, n) }
func (e *engine) openFailed(code string) {
	e.openFails.Add(1)
	e.count("open_failed:" + code)
	if w := e.wave.Load(); w != nil {
		w.counters.add("open_failed:" + code)
	}
}

func (e *engine) phaseName() string {
	if p := e.phase.Load(); p != nil {
		return *p
	}
	return ""
}

func (e *engine) setPhase(name string, set *LatSet) {
	e.phase.Store(&name)
	e.lat.phase.Store(set)
}

func (e *engine) delivered() int64 {
	var n int64
	for _, k := range deliveryKinds {
		n += e.recv[k].Load()
	}
	return n
}

func (e *engine) onReady(c *rclient, first bool) {
	if first {
		e.firstReady.Add(1)
	}
}

// waveStats follows one reconnect wave.
type waveStats struct {
	t0       time.Time
	attempts atomic.Int64
	down     atomic.Int64
	backN    atomic.Int64
	mu       sync.Mutex
	backAt   []time.Duration // from t0 to each stream's ready
	downtime []time.Duration // from each stream's end to its ready
	counters counters
}

func (w *waveStats) back(now, downAt time.Time) {
	w.mu.Lock()
	w.backAt = append(w.backAt, now.Sub(w.t0))
	w.downtime = append(w.downtime, now.Sub(downAt))
	w.mu.Unlock()
	w.backN.Add(1)
}

type bigWatch struct {
	got  atomic.Int64
	last atomic.Int64
}

// mark is the state of the run at one instant, so that any two marks bound
// a phase or a measurement window.
type mark struct {
	name   string
	at     time.Time
	point  Point
	idx    int // the point's index in the timeline
	recv   [nLat]int64
	expect [nLat]int64
	opens  int64
	fails  int64
	ctr    map[string]int64
}

func (e *engine) mark(ctx context.Context, name string) mark {
	p, idx := e.sampler.sample(ctx, name)
	m := mark{name: name, at: time.Now(), point: p, idx: idx, opens: e.openAttempts.Load(), fails: e.openFails.Load(), ctr: e.ctr.snapshot()}
	for k := range nLat {
		m.recv[k] = e.recv[k].Load()
		m.expect[k] = e.expect[k].Load()
	}
	e.marks = append(e.marks, m)
	return m
}

func runRooms(ctx context.Context, cfg Config, log *slog.Logger) error {
	rc, err := loadRoomsConfig(cfg.Scenario)
	if err != nil {
		return err
	}
	base := strings.TrimSuffix(cfg.MetricsURL, "/metrics")
	waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Minute)
	defer waitCancel()
	if err := waitHealthy(waitCtx, base+"/healthz"); err != nil {
		return err
	}
	if err := waitHealthy(waitCtx, cfg.IssuerURL+"/healthz"); err != nil {
		return err
	}
	signer, err := authn.Load(cfg.PrivateJWK)
	if err != nil {
		return err
	}
	pool, err := connectDB(ctx, cfg.DBURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	var name *string
	if err := pool.QueryRow(ctx, `select to_regclass('public.room_messages')::text`).Scan(&name); err != nil {
		return err
	}
	if name == nil {
		return fmt.Errorf("rooms schema missing (public.room_messages): the volume predates sql/02-rooms.sql; docker compose down -v and up again")
	}

	e := &engine{
		cfg: cfg, rc: rc, log: log, signer: signer, pool: pool,
		docker:     dockerapi.New(rc.DockerSocket),
		streamHTTP: newStreamClient(),
		ctlHTTP:    newControlClient(),
		trafficOn:  make(chan struct{}),
		started:    time.Now(),
	}
	e.seed = rc.Seed
	if e.seed == 0 {
		e.seed = rand.Uint64()
	}
	e.sampler = newSampler(e)
	e.clientsCtx, e.clientsCancel = context.WithCancel(ctx)
	defer e.clientsCancel()
	e.trafficCtx, e.trafficCancel = context.WithCancel(ctx)
	defer e.trafficCancel()

	exp := expectedOf(rc)
	log.Info("rooms start", "scenario", cfg.Scenario, "label", rc.Label, "target", cfg.TargetImage,
		"rooms", exp.Rooms, "members", exp.Members, "streams", exp.Streams, "shapes", rc.Shapes,
		"changes_per_s", fmt.Sprintf("%.0f", exp.ChangesPerSec), "events_per_s", fmt.Sprintf("%.0f", exp.EventsPerSec),
		"duration", rc.Duration, "arrival", rc.Arrival, "replica_identity", rc.ReplicaIdentity, "docker", e.docker != nil)

	e.pgInfo = postgresInfo(ctx, pool)
	setupStart := time.Now()
	if err := e.setup(ctx); err != nil {
		return err
	}
	log.Info("setup done", "took", time.Since(setupStart).Round(time.Millisecond))

	go e.sampler.run(e.clientsCtx, rc.SampleEvery)
	go e.secondTicker(e.clientsCtx)
	go e.windowTicker(e.clientsCtx)

	// Arrival.
	arrival := &LatSet{}
	e.setPhase("arrival", arrival)
	mStart := e.mark(ctx, "start")
	if rc.Arrival <= 0 {
		e.waveSem = make(chan struct{}, max(1, rc.Wave))
	}
	if rc.ArrivalTraffic {
		e.startTraffic()
	}
	e.startClients()
	ok := e.waitArrival(ctx)
	e.arrivalT = time.Since(mStart.at)
	log.Info("arrival done", "opened", e.firstReady.Load(), "of", len(e.clients), "took", e.arrivalT.Round(time.Millisecond), "complete", ok)

	steady := &LatSet{}
	e.setPhase("steady", steady)
	mSteady := e.mark(ctx, "steady")
	e.startTraffic()

	switch cfg.Scenario {
	case "bigtxn":
		e.runBigTxns(ctx)
	case "reconnect":
		e.runReconnects(ctx)
	case "calibrate":
		e.runCalibration(ctx)
	}
	if rest := rc.Duration - time.Since(mSteady.at); rest > 0 {
		sleepCtx(ctx, rest)
	}

	// Stop writing, let what is in flight arrive, then close.
	e.trafficCancel()
	sleepCtx(ctx, rc.Drain)
	mEnd := e.mark(ctx, "end")
	e.setPhase("end", nil)
	e.clientsCancel()

	rep := e.report(mStart, mSteady, mEnd, arrival, steady, ok)
	path, err := writeRoomsReport(cfg.ResultsDir, rep)
	if err != nil {
		return err
	}
	log.Info("wrote report", "path", path, "pass", rep.Verdict.Pass, "reasons", strings.Join(rep.Verdict.Reasons, "; "))
	printRoomsSummary(rep)
	return nil
}

func newStreamClient() *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          0,
		DisableKeepAlives:     true, // one connection per stream, closed with it
		DisableCompression:    true,
		ResponseHeaderTimeout: 2 * time.Minute,
	}}
}

// newControlClient keeps enough idle connections for /publish, /token and
// /subscribe at the rates of a large run: the default two per host would
// open and close one per request and fill the port range with TIME_WAIT.
func newControlClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        2048,
		MaxIdleConnsPerHost: 2048,
		IdleConnTimeout:     2 * time.Minute,
		DisableCompression:  true,
	}}
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

func (e *engine) setup(ctx context.Context) error {
	rc := e.rc
	if envBool("LOAD_DROP_IDLE_SLOTS", true) {
		// An inactive slot left by another profile retains every byte of WAL
		// this run writes.
		rows, err := e.pool.Query(ctx, `select slot_name from pg_replication_slots
			where not active and slot_name like 'load\_%' and slot_name <> $1`, e.cfg.SlotName)
		if err == nil {
			names, _ := pgx.CollectRows(rows, pgx.RowTo[string])
			for _, n := range names {
				if _, err := e.pool.Exec(ctx, `select pg_drop_replication_slot($1)`, n); err == nil {
					e.log.Warn("dropped an inactive replication slot of another profile", "slot", n)
				}
			}
		}
	}
	ri := "FULL"
	if rc.ReplicaIdentity == "pk" {
		ri = "DEFAULT"
	}
	if _, err := e.pool.Exec(ctx, `alter table public.room_members replica identity `+ri); err != nil {
		return fmt.Errorf("replica identity: %w", err)
	}
	if _, err := e.pool.Exec(ctx, `truncate table public.room_messages, public.room_members, public.notifications restart identity`); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}

	rng := rand.New(rand.NewPCG(e.seed, e.seed^0x9e3779b97f4a7c15))
	var id int64
	var rows [][]any
	var seed []map[string]any
	for ci, k := range rc.Classes {
		for range k.Count {
			id++
			r := &room{id: id, class: ci}
			// Typists spread so the average per room is k.Typing.
			typists := int(math.Floor(k.Typing*float64(id))) - int(math.Floor(k.Typing*float64(id-1)))
			if !rc.TypingEnabled {
				typists = 0
			}
			for j := range k.Members {
				u := uuidFrom(rng)
				r.members = append(r.members, u)
				rows = append(rows, []any{id, u})
				seed = append(seed, map[string]any{"room_id": id, "user_id": u})
				if j < k.Streams {
					c := &rclient{
						e: e, room: r, idx: j, userID: u, sessID: uuidFrom(rng),
						typist: j < typists,
						subs:   roomSubs(id, u, rc.Shapes),
						resume: map[string]string{}, retries: map[string]*time.Timer{},
					}
					r.clients = append(r.clients, c)
					e.clients = append(e.clients, c)
				}
			}
			e.rooms = append(e.rooms, r)
		}
	}
	if _, err := e.pool.CopyFrom(ctx, pgx.Identifier{"public", "room_members"}, []string{"room_id", "user_id"},
		pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("seed room_members: %w", err)
	}
	if _, err := e.pool.Exec(ctx, `analyze public.room_members`); err != nil {
		return err
	}

	if err := postJSON(ctx, e.ctlHTTP, e.cfg.IssuerURL+"/reset", e.cfg.IssuerBearer, "{}"); err != nil {
		return fmt.Errorf("issuer reset: %w", err)
	}
	for i := 0; i < len(seed); i += 5000 {
		b, _ := json.Marshal(map[string]any{"rooms": seed[i:min(i+5000, len(seed))]})
		if err := postJSON(ctx, e.ctlHTTP, e.cfg.IssuerURL+"/seed", e.cfg.IssuerBearer, string(b)); err != nil {
			return fmt.Errorf("issuer seed: %w", err)
		}
	}

	// Tokens, on every core.
	var wg sync.WaitGroup
	var mintErr atomic.Value
	chunk := (len(e.clients) + runtime.NumCPU() - 1) / max(1, runtime.NumCPU())
	for i := 0; i < len(e.clients); i += max(1, chunk) {
		part := e.clients[i:min(i+max(1, chunk), len(e.clients))]
		wg.Go(func() {
			for _, c := range part {
				tok, err := e.signer.User(c.userID, c.sessID, e.rc.TokenTTL)
				if err != nil {
					mintErr.Store(err)
					return
				}
				c.token.Store(&tok)
			}
		})
	}
	wg.Wait()
	if err, ok := mintErr.Load().(error); ok {
		return err
	}
	return nil
}

func uuidFrom(rng *rand.Rand) string {
	var b [16]byte
	for i := 0; i < 16; i += 8 {
		v := rng.Uint64()
		for j := range 8 {
			b[i+j] = byte(v >> (8 * j))
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// ---------------------------------------------------------------------------
// Clients and traffic
// ---------------------------------------------------------------------------

// startClients opens every stream: spread at random over LOAD_ARRIVAL, or
// all at once LOAD_WAVE at a time.
func (e *engine) startClients() {
	order := rand.Perm(len(e.clients))
	for _, i := range order {
		c := e.clients[i]
		var delay time.Duration
		if e.rc.Arrival > 0 {
			delay = time.Duration(rand.Int64N(int64(e.rc.Arrival)))
		}
		time.AfterFunc(delay, func() {
			if e.clientsCtx.Err() != nil {
				return
			}
			e.scheduleToken(c, time.Duration(rand.Int64N(int64(max(e.rc.TokenEvery, time.Second)))))
			if c.typist {
				go e.typist(c)
			}
			c.run(e.clientsCtx)
		})
	}
}

func (e *engine) scheduleToken(c *rclient, after time.Duration) {
	if e.rc.TokenEvery <= 0 {
		return
	}
	time.AfterFunc(after, func() {
		if e.clientsCtx.Err() != nil {
			return
		}
		c.refreshToken(e.clientsCtx)
		e.scheduleToken(c, e.rc.TokenEvery)
	})
}

// waitArrival waits until every stream has opened once, or the arrival
// window plus LOAD_OPEN_TIMEOUT has passed.
func (e *engine) waitArrival(ctx context.Context) bool {
	deadline := time.Now().Add(e.rc.Arrival + e.rc.OpenTimeout)
	for time.Now().Before(deadline) {
		if int(e.firstReady.Load()) >= len(e.clients) {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
	return int(e.firstReady.Load()) >= len(e.clients)
}

func (e *engine) startTraffic() {
	e.trafficOnce.Do(func() {
		close(e.trafficOn)
		if e.rc.MsgEvery <= 0 {
			return
		}
		for _, r := range e.rooms {
			go e.roomWriter(r)
		}
	})
}

// roomWriter commits one message every LOAD_MSG_EVERY, from a random phase:
// one INSERT and an UPDATE of every member's last_message_at, as one
// transaction.
func (e *engine) roomWriter(r *room) {
	ctx := e.trafficCtx
	if !sleepCtx(ctx, time.Duration(rand.Int64N(int64(e.rc.MsgEvery)))) {
		return
	}
	t := time.NewTicker(e.rc.MsgEvery)
	defer t.Stop()
	for {
		seq := r.seq.Add(1)
		author := r.members[rand.IntN(len(r.members))]
		open := r.open.Load()
		at := time.Now()
		b := &pgx.Batch{}
		b.Queue(`insert into public.room_messages (room_id, seq, author_id, body, created_at) values ($1, $2, $3, $4, $5)`,
			r.id, seq, author, "message body of a typical length for a chat, about sixty bytes", at)
		b.Queue(`update public.room_members set last_message_at = $2 where room_id = $1`, r.id, at)
		err := e.pool.SendBatch(ctx, b).Close()
		e.lat.Record(latTxn, time.Since(at))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			e.count("message_failed")
		} else {
			e.messages.Add(1)
			e.expect[latMessage].Add(open)
			if e.rc.Shapes >= 2 {
				e.expect[latMember].Add(open)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (e *engine) typist(c *rclient) {
	select {
	case <-e.trafficOn:
	case <-e.clientsCtx.Done():
		return
	}
	ctx := e.trafficCtx
	if e.rc.TypingEvery <= 0 || !sleepCtx(ctx, time.Duration(rand.Int64N(int64(e.rc.TypingEvery)))) {
		return
	}
	t := time.NewTicker(e.rc.TypingEvery)
	defer t.Stop()
	for {
		c.typing(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// secondTicker keeps the delivered events of every second, for peak rates.
func (e *engine) secondTicker(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n := e.meter.Swap(0)
			e.secMu.Lock()
			e.secs = append(e.secs, int32(n))
			e.secMu.Unlock()
		}
	}
}

// windowTicker closes a latency window every LOAD_STATS_EVERY, so the report
// shows whether latency drifted over the run.
type window struct {
	T       float64            `json:"t"`
	Phase   string             `json:"phase"`
	Latency map[string]Summary `json:"latency"`
}

var windowsMu sync.Mutex

func (e *engine) windowTicker(ctx context.Context) {
	cur := &LatSet{}
	e.lat.window.Store(cur)
	t := time.NewTicker(e.rc.StatsEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			next := &LatSet{}
			e.lat.window.Store(next)
			w := window{T: time.Since(e.started).Seconds(), Phase: e.phaseName(), Latency: cur.Summaries()}
			windowsMu.Lock()
			e.windows = append(e.windows, w)
			windowsMu.Unlock()
			e.log.Info("window", "t", fmt.Sprintf("%.0fs", w.T), "phase", w.Phase, "live", e.liveClients.Load(),
				"message_p95", fmtMs(w.Latency["message"].P95), "member_p95", fmtMs(w.Latency["member"].P95),
				"typing_p95", fmtMs(w.Latency["typing"].P95))
			cur = next
		}
	}
}

func fmtMs(v float64) string { return fmt.Sprintf("%.1fms", v) }

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// ---------------------------------------------------------------------------
// PostgreSQL views of the slot
// ---------------------------------------------------------------------------

type slotRow struct {
	current                    uint64
	slotLag, retained, sendLag int64
	sent                       uint64
}

func (e *engine) slotRow(ctx context.Context) (slotRow, error) {
	var r slotRow
	var cur, sent *string
	var lag, ret, send *int64
	err := e.pool.QueryRow(ctx, `
		select pg_current_wal_lsn()::text,
		       pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)::bigint,
		       pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint,
		       pg_wal_lsn_diff(pg_current_wal_lsn(), r.sent_lsn)::bigint,
		       r.sent_lsn::text
		  from pg_replication_slots s
		  left join pg_stat_replication r on r.pid = s.active_pid
		 where s.slot_name = $1`, e.cfg.SlotName).Scan(&cur, &lag, &ret, &send, &sent)
	if err != nil {
		return r, err
	}
	if cur != nil {
		r.current, _ = parseLSN(*cur)
	}
	if sent != nil {
		r.sent, _ = parseLSN(*sent)
	}
	if lag != nil {
		r.slotLag = *lag
	}
	if ret != nil {
		r.retained = *ret
	}
	if send != nil {
		r.sendLag = *send
	}
	return r, nil
}

// ---------------------------------------------------------------------------
// bigtxn
// ---------------------------------------------------------------------------

func (e *engine) runBigTxns(ctx context.Context) {
	if !sleepCtx(ctx, e.rc.BigAfter) {
		return
	}
	n := 0
	for _, rows := range e.rc.BigRows {
		for range e.rc.BigRepeat {
			start := time.Now()
			n++
			res := e.bigTxn(ctx, n, rows)
			e.bigs = append(e.bigs, res)
			e.log.Info("bigtxn", "n", n, "rows", rows, "streamed", res.Streamed,
				"delivered", fmt.Sprintf("%d/%d", res.Delivered, res.Expected),
				"txn_ms", res.TxnMs, "sent_ms", res.SentMs, "dispatched_ms", res.DispatchedMs,
				"last_delivery_ms", res.LastDeliveryMs, "backlog_max_mib", fmt.Sprintf("%.1f", float64(res.BacklogMaxBytes)/mib),
				"others_p95_before", fmtMs(res.Baseline["message"].P95), "others_p95_during", fmtMs(res.During["message"].P95))
			if ctx.Err() != nil {
				return
			}
			sleepCtx(ctx, e.rc.BigEvery-time.Since(start))
		}
	}
}

func (e *engine) bigTxn(ctx context.Context, n, rows int) BigTxnResult {
	res := BigTxnResult{N: n, Rows: rows}
	var live []*rclient
	for _, c := range e.clients {
		if c.live.Load() {
			live = append(live, c)
		}
	}
	rand.Shuffle(len(live), func(i, j int) { live[i], live[j] = live[j], live[i] })
	streamed := min(int(math.Round(float64(rows)*e.rc.BigStreamed)), len(live), rows)
	ids := make([]string, 0, rows)
	for _, c := range live[:streamed] {
		ids = append(ids, c.userID)
	}
	for len(ids) < rows {
		ids = append(ids, newUUID())
	}
	res.Streamed = streamed

	baseline := &LatSet{}
	e.lat.extra.Store(baseline)
	e.mark(ctx, fmt.Sprintf("bigtxn-%d-baseline", n))
	sleepCtx(ctx, e.rc.BigBaseline)

	during := &LatSet{}
	e.lat.extra.Store(during)
	w := &bigWatch{}
	e.big.Store(w)
	mA := e.mark(ctx, fmt.Sprintf("bigtxn-%d", n))
	var expected int64
	for _, c := range live[:streamed] {
		if c.live.Load() {
			expected++
		}
	}
	t0 := time.Now()
	_, err := e.pool.Exec(ctx, `insert into public.notifications (user_id, kind, body, created_at)
		select u::uuid, 'post', $2, $3 from unnest($1::text[]) as u`, ids, "someone you follow published something new", t0)
	t1 := time.Now()
	if err != nil {
		res.Error = err.Error()
	}
	var lsnText string
	_ = e.pool.QueryRow(ctx, `select pg_current_wal_lsn()::text`).Scan(&lsnText)
	target, _ := parseLSN(lsnText)
	res.Expected = expected
	res.TxnMs = ms(t1.Sub(t0))

	deadline := t1.Add(e.rc.BigTimeout)
	var sentAt, confAt time.Time
	for time.Now().Before(deadline) && ctx.Err() == nil {
		conf := e.sampler.readyzLSN(ctx)
		row, _ := e.slotRow(ctx)
		now := time.Now()
		if conf > 0 && row.current >= conf {
			res.BacklogMaxBytes = max(res.BacklogMaxBytes, int64(row.current-conf))
		}
		if sentAt.IsZero() && row.sent >= target {
			sentAt = now
		}
		if confAt.IsZero() && conf >= target {
			confAt = now
		}
		if !confAt.IsZero() && w.got.Load() >= expected {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The window the other streams are measured in is as long as the baseline,
	// so the two compare: what queued behind the commit arrives within it.
	sleepCtx(ctx, e.rc.BigBaseline-time.Since(mA.at))
	mB := e.mark(ctx, fmt.Sprintf("bigtxn-%d-end", n))
	e.big.Store(nil)
	e.lat.extra.Store(nil)

	res.Delivered = w.got.Load()
	if !sentAt.IsZero() {
		res.SentMs = ms(sentAt.Sub(t1))
	}
	if !confAt.IsZero() {
		res.DispatchedMs = ms(confAt.Sub(t1))
	}
	if l := w.last.Load(); l > 0 {
		res.LastDeliveryMs = ms(time.Unix(0, l).Sub(t1))
	}
	res.Baseline = baseline.Summaries()
	res.During = during.Summaries()
	res.SluiceCPUSeconds = mB.point.SluiceCPU - mA.point.SluiceCPU
	if dc := mB.point.DispatchCount - mA.point.DispatchCount; dc > 0 {
		res.DispatchAvgUs = (mB.point.DispatchSum - mA.point.DispatchSum) / dc * 1e6
	}
	pts := e.sampler.snapshotPoints()[mA.idx : mB.idx+1]
	for _, p := range pts {
		res.RSSPeakMiB = max(res.RSSPeakMiB, p.RSSMiB)
		res.GCPauseMaxMs = max(res.GCPauseMaxMs, p.GCPauseMaxMs)
	}
	res.Lagging = mB.point.Lagging - mA.point.Lagging
	return res
}

func ms(d time.Duration) float64 { return math.Round(float64(d)/float64(time.Microsecond)) / 1000 }

// ---------------------------------------------------------------------------
// reconnect
// ---------------------------------------------------------------------------

func (e *engine) runReconnects(ctx context.Context) {
	if !sleepCtx(ctx, e.rc.ReconnectAfter) {
		return
	}
	for i := range e.rc.ReconnectCount {
		start := time.Now()
		res := e.reconnectWave(ctx, i+1)
		e.waves = append(e.waves, res)
		e.log.Info("reconnect", "n", i+1, "mode", res.Mode, "down", res.Down, "back", res.Back,
			"all_back_s", res.AllBackS, "p99_back_s", res.P99BackS, "issuer_peak_per_s", res.IssuerPeakPerSec,
			"issuer_unavailable", res.IssuerUnavailable, "fully_resumed", res.FullyResumed, "not_fully_resumed", res.NotFullyResumed)
		if ctx.Err() != nil {
			return
		}
		sleepCtx(ctx, max(e.rc.ReconnectEvery-time.Since(start), 15*time.Second))
	}
}

func (e *engine) reconnectWave(ctx context.Context, n int) WaveResult {
	res := WaveResult{N: n, Mode: e.rc.ReconnectMode}
	live := e.liveClients.Load()
	res.LiveBefore = live
	during := &LatSet{}
	e.lat.extra.Store(during)
	mA := e.mark(ctx, fmt.Sprintf("reconnect-%d", n))
	w := &waveStats{t0: time.Now()}
	e.wave.Store(w)

	// The restart call returns once the container is up again, which the
	// streams do not wait for.
	type restartOut struct {
		took time.Duration
		err  error
	}
	restarted := make(chan restartOut, 1)
	switch e.rc.ReconnectMode {
	case "restart":
		go func() {
			t := time.Now()
			err := e.restartSluice(ctx)
			restarted <- restartOut{time.Since(t), err}
		}()
	case "cut":
		u := fmt.Sprintf("%s/cut?down=%s", e.rc.ProxyControl, e.rc.ProxyDown)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
		resp, err := e.ctlHTTP.Do(req)
		if err != nil {
			res.Error = err.Error()
		} else {
			var out struct {
				Closed int `json:"closed"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&out)
			resp.Body.Close()
			res.ProxyClosed = out.Closed
		}
	}

	deadline := w.t0.Add(e.rc.ReconnectTimeout)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		down, back := w.down.Load(), w.backN.Load()
		if down >= live && back >= down {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if e.rc.ReconnectMode == "restart" {
		select {
		case out := <-restarted:
			res.RestartMs = ms(out.took)
			if out.err != nil {
				res.Error = out.err.Error()
				e.log.Error("restart sluice", "err", out.err)
			}
		case <-time.After(e.rc.StopTimeout + time.Minute):
			res.Error = "the restart did not return"
		}
	}
	// Results settle: a resubscribe after issuer_unavailable lands later.
	sleepCtx(ctx, 2*time.Second)
	mB := e.mark(ctx, fmt.Sprintf("reconnect-%d-end", n))
	e.wave.Store(nil)
	e.lat.extra.Store(nil)

	w.mu.Lock()
	backAt := append([]time.Duration(nil), w.backAt...)
	downtime := append([]time.Duration(nil), w.downtime...)
	w.mu.Unlock()
	slices.Sort(backAt)
	slices.Sort(downtime)
	res.Down = w.down.Load()
	res.Back = int64(len(backAt))
	res.Attempts = w.attempts.Load()
	q := func(v []time.Duration, p float64) float64 {
		if len(v) == 0 {
			return 0
		}
		return math.Round(v[int(float64(len(v)-1)*p)].Seconds()*1000) / 1000
	}
	res.P50BackS, res.P90BackS, res.P99BackS = q(backAt, .5), q(backAt, .9), q(backAt, .99)
	if len(backAt) > 0 {
		res.AllBackS = math.Round(backAt[len(backAt)-1].Seconds()*1000) / 1000
	}
	res.DowntimeP50S, res.DowntimeP95S, res.DowntimeP99S = q(downtime, .5), q(downtime, .95), q(downtime, .99)
	res.Counters = w.counters.snapshot()
	res.FullyResumed = res.Counters["fully_resumed"]
	res.NotFullyResumed = res.Counters["not_fully_resumed"]
	for k, v := range res.Counters {
		if strings.HasSuffix(k, ":issuer_unavailable") {
			res.IssuerUnavailable += v
		}
	}
	res.GapsAfterResume = mB.ctr["msg_gap_after_resume"] - mA.ctr["msg_gap_after_resume"]
	res.GapsUnresumed = mB.ctr["msg_gap_unresumed"] - mA.ctr["msg_gap_unresumed"]
	res.Latency = during.Summaries()
	res.SluiceCPUSeconds = mB.point.SluiceCPU - mA.point.SluiceCPU
	res.Lagging = mB.point.Lagging - mA.point.Lagging
	for _, p := range e.sampler.snapshotPoints()[mA.idx : mB.idx+1] {
		res.RSSPeakMiB = max(res.RSSPeakMiB, p.RSSMiB)
		res.GCPauseMaxMs = max(res.GCPauseMaxMs, p.GCPauseMaxMs)
		res.RunnerCoresPeak = max(res.RunnerCoresPeak, p.RunnerCores)
	}
	// The issuer's calls per second over the wave, read after it so the last
	// seconds are complete.
	if snap, err := fetchIssuerStats(ctx, e.sampler.http, e.cfg.IssuerURL, w.t0.Unix()); err == nil {
		res.IssuerPeakPerSec, res.IssuerPeakByKind, res.IssuerCalls = issuerPeak(snap.Seconds, w.t0.Unix(), mB.at.Unix())
	}
	return res
}

func (e *engine) restartSluice(ctx context.Context) error {
	if e.docker == nil {
		return fmt.Errorf("the restart mode needs the Docker socket at %s", e.rc.DockerSocket)
	}
	project, err := e.docker.Project(ctx)
	if err != nil {
		return err
	}
	cs, err := e.docker.ProjectContainers(ctx, project)
	if err != nil {
		return err
	}
	for _, c := range cs {
		if c.Service() == e.rc.SluiceService {
			return e.docker.Restart(ctx, c.ID, e.rc.StopTimeout)
		}
	}
	return fmt.Errorf("no running container for service %s in project %s", e.rc.SluiceService, project)
}

// ---------------------------------------------------------------------------
// calibrate
// ---------------------------------------------------------------------------

func (e *engine) runCalibration(ctx context.Context) {
	sleepCtx(ctx, 10*time.Second)
	res := &CalibrationResult{}
	for i := range e.rc.CalWindows {
		a := e.mark(ctx, fmt.Sprintf("calibrate-%d", i+1))
		if !sleepCtx(ctx, e.rc.CalWindow) {
			return
		}
		b := e.mark(ctx, fmt.Sprintf("calibrate-%d-end", i+1))
		ev := b.point.Events - a.point.Events
		ch := b.point.Changes - a.point.Changes
		cpu := b.point.SluiceCPU - a.point.SluiceCPU
		if ev > 0 && ch > 0 {
			res.Windows = append(res.Windows, CalWindow{
				Events: ev, Changes: ch, CPUSeconds: cpu,
				CPUMsPer1kEvents: cpu * 1e6 / float64(ev), CPUMsPer1kChanges: cpu * 1e6 / ch,
				EventsPerSec: float64(ev) / b.at.Sub(a.at).Seconds(),
			})
		}
	}
	var perEvent, perChange []float64
	for _, w := range res.Windows {
		perEvent = append(perEvent, w.CPUMsPer1kEvents)
		perChange = append(perChange, w.CPUMsPer1kChanges)
	}
	res.CPUMsPer1kEvents = median(perEvent)
	res.CPUMsPer1kChanges = median(perChange)
	e.cal = res
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	slices.Sort(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// postgresInfo records the server version and the settings that change what
// a run measures.
func postgresInfo(ctx context.Context, pool *pgxpool.Pool) map[string]string {
	out := map[string]string{}
	var v string
	if pool.QueryRow(ctx, `select version()`).Scan(&v) == nil {
		out["version"] = v
	}
	for _, name := range []string{"synchronous_commit", "shared_buffers", "logical_decoding_work_mem",
		"max_slot_wal_keep_size", "wal_sender_timeout", "wal_writer_delay", "max_wal_senders"} {
		var s string
		if pool.QueryRow(ctx, `select current_setting($1)`, name).Scan(&s) == nil {
			out[name] = s
		}
	}
	return out
}
