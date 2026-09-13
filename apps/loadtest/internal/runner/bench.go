package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/authn"
	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/sse"
)

type User struct {
	ID        string
	SessionID string
	Token     string
	ProjectID string
	TeamID    int
}

type kind int

const (
	kindNotes kind = iota
	kindPosts
	kindInvoices
	kindProjectDocs
	kindChannel
)

type plane int

const (
	planeChange plane = iota
	planeBroadcast
	planePresence
	planeMixed
	planeKick
)

type scenario struct {
	Name          string
	Kind          kind
	Plane         plane
	Channel       string
	ExpectTier    string
	ExpectOracle  string
	SubscribeJSON func(u User) string
	Write         func(ctx context.Context, pool *pgxpool.Pool, u User, title string) error
}

type Step struct {
	Name           string  `json:"name"`
	Users          int     `json:"users"`
	ConnsPerUser   int     `json:"conns_per_user"`
	Streams        int     `json:"streams"`
	Changes        int     `json:"changes"`
	Opened         int     `json:"opened"`
	FailedOpen     int     `json:"failed_open"`
	Denied         int     `json:"denied"`
	ObservedTier   string  `json:"observed_tier,omitempty"`
	ObservedOracle string  `json:"observed_oracle,omitempty"`
	Writes         int     `json:"writes"`
	WriteErrors    int     `json:"write_errors"`
	Received       int64   `json:"received"`
	Expected       int64   `json:"expected"`
	DeliveryPct    float64 `json:"delivery_pct"`
	OpenOKPct      float64 `json:"open_ok_pct"`
	OpenElapsed    string  `json:"open_elapsed"`
	FanoutElapsed  string  `json:"fanout_elapsed"`
	EventsPerSec   float64 `json:"events_per_sec"`
	LatencyP50     string  `json:"latency_p50,omitempty"`
	LatencyP95     string  `json:"latency_p95,omitempty"`
	LatencyP99     string  `json:"latency_p99,omitempty"`
	Lagging        float64 `json:"stream_lagging_delta"`
	StreamsGauge   float64 `json:"sluice_streams"`
	TierCProbes    float64 `json:"tier_c_probes_delta"`
	DispatchAvg    string  `json:"dispatch_avg,omitempty"`
	ChangesRecv    int64   `json:"changes_received,omitempty"`
	Broadcasts     int64   `json:"broadcasts_client,omitempty"`
	DBBroadcasts   int64   `json:"broadcasts_database,omitempty"`
	PresenceReady  int     `json:"presence_converged,omitempty"`
	PresenceLeave  int     `json:"presence_after_leave,omitempty"`
	Kicked         int64   `json:"kicked,omitempty"`
	Pass           bool    `json:"pass"`
	FailReason     string  `json:"fail_reason,omitempty"`
}

type live struct {
	user     User
	sse      *sse.Stream
	streamID string
	mu       sync.Mutex
	roster   map[string]struct{}
}

type app struct {
	cfg      Config
	signer   *authn.Signer
	pool     *pgxpool.Pool
	http     *http.Client
	scenario scenario
}

func mintUsers(signer *authn.Signer, n int, ttl time.Duration) ([]User, error) {
	out := make([]User, n)
	for i := 0; i < n; i++ {
		id := newUUID()
		sess := newUUID()
		tok, err := signer.User(id, sess, ttl)
		if err != nil {
			return nil, err
		}
		out[i] = User{
			ID:        id,
			SessionID: sess,
			Token:     tok,
			ProjectID: "p-" + id,
			TeamID:    i + 1,
		}
	}
	return out, nil
}

func (a *app) prepareUsers(ctx context.Context, users []User) error {
	switch a.scenario.Kind {
	case kindInvoices:
		if err := seedTeamRows(ctx, a.pool, users); err != nil {
			return fmt.Errorf("seed team_members: %w", err)
		}
	case kindProjectDocs:
		if err := seedProjects(ctx, a.pool, users); err != nil {
			return fmt.Errorf("seed project_members: %w", err)
		}
		if err := a.seedIssuer(ctx, users); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) seedIssuer(ctx context.Context, users []User) error {
	if a.cfg.IssuerBearer == "" {
		return fmt.Errorf("SLUICE_ISSUER_BEARER is required for issuer tests")
	}
	if err := a.issuerPOST(ctx, "/reset", map[string]any{}); err != nil {
		return err
	}
	const chunk = 1000
	for i := 0; i < len(users); i += chunk {
		j := i + chunk
		if j > len(users) {
			j = len(users)
		}
		members := make([]map[string]string, 0, j-i)
		for _, u := range users[i:j] {
			members = append(members, map[string]string{
				"project_id": u.ProjectID,
				"user_id":    u.ID,
			})
		}
		if err := a.issuerPOST(ctx, "/seed", map[string]any{"members": members}); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) issuerPOST(ctx context.Context, path string, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	if err := postJSON(ctx, a.http, a.cfg.IssuerURL+path, a.cfg.IssuerBearer, string(raw)); err != nil {
		return fmt.Errorf("issuer %s: %w", path, err)
	}
	return nil
}

func (a *app) runStep(ctx context.Context, name string, users []User, connsPerUser, changes int) (Step, error) {
	st := Step{
		Name:         name,
		Users:        len(users),
		ConnsPerUser: connsPerUser,
		Streams:      len(users) * connsPerUser,
		Changes:      changes,
	}
	if st.Streams == 0 {
		st.FailReason = "no streams requested"
		return st, nil
	}

	if err := truncateApp(ctx, a.pool); err != nil {
		return st, fmt.Errorf("truncate: %w", err)
	}
	if err := a.prepareUsers(ctx, users); err != nil {
		return st, err
	}

	before, _ := fetchMetrics(ctx, a.http, a.cfg.MetricsURL)

	var (
		mu          sync.Mutex
		lives       []*live
		opened      atomic.Int64
		failed      atomic.Int64
		denied      atomic.Int64
		changesN    atomic.Int64
		clientBcast atomic.Int64
		dbBcast     atomic.Int64
		kicks       atomic.Int64
		lagged      atomic.Int64
		tier        atomic.Value
		oracle      atomic.Value
		latMu       sync.Mutex
		lats        []time.Duration
		writeAt     sync.Map
		readWG      sync.WaitGroup
		firstErr    atomic.Value
	)
	stop := make(chan struct{})
	sem := make(chan struct{}, max(1, a.cfg.Wave))

	noteLatency := func(seq int64) {
		if v, ok := writeAt.Load(seq); ok {
			d := time.Since(v.(time.Time))
			latMu.Lock()
			if len(lats) < 100_000 {
				lats = append(lats, d)
			}
			latMu.Unlock()
		}
	}

	openStart := time.Now()
	openCtx, openCancel := context.WithTimeout(ctx, a.cfg.OpenTimeout)
	defer openCancel()

	var openWG sync.WaitGroup
	for _, u := range users {
		body := a.scenario.SubscribeJSON(u)
		for c := 0; c < connsPerUser; c++ {
			openWG.Add(1)
			go func(u User, body string) {
				defer openWG.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				s, ready, err := openStream(openCtx, a.http, a.cfg.SluiceURL, u.Token, body)
				if err != nil {
					failed.Add(1)
					firstErr.CompareAndSwap(nil, err.Error())
					return
				}
				if !subsAccepted(ready) {
					denied.Add(1)
					s.Close()
					return
				}
				for _, sub := range ready.Subscriptions {
					if sub.Tier != "" {
						_ = tier.CompareAndSwap(nil, sub.Tier)
					}
					if sub.Oracle != "" {
						_ = oracle.CompareAndSwap(nil, sub.Oracle)
					}
				}
				l := &live{
					user:     u,
					sse:      s,
					streamID: ready.StreamID,
					roster:   map[string]struct{}{},
				}
				opened.Add(1)
				mu.Lock()
				lives = append(lives, l)
				mu.Unlock()

				readWG.Add(1)
				go func(l *live) {
					defer readWG.Done()
					for {
						select {
						case <-stop:
							return
						case ev, ok := <-l.sse.Events:
							if !ok {
								return
							}
							switch ev.Name {
							case "change":
								changesN.Add(1)
								if seq, ok := titleSeq(ev.Data); ok {
									noteLatency(seq)
								}
							case "broadcast":
								origin, seq, seqOK := broadcastMeta(ev.Data)
								if origin == "database" {
									dbBcast.Add(1)
								} else {
									clientBcast.Add(1)
								}
								if seqOK {
									noteLatency(seq)
								}
							case "presence":
								l.mu.Lock()
								applyPresence(l.roster, ev.Data)
								l.mu.Unlock()
							case "error":
								switch errorCode(ev.Data) {
								case "stream_lagging":
									lagged.Add(1)
								case "shape_not_authorized":
									kicks.Add(1)
								}
							}
						}
					}
				}(l)
			}(u, body)
		}
	}
	openWG.Wait()
	st.OpenElapsed = time.Since(openStart).Round(time.Millisecond).String()
	st.Opened = int(opened.Load())
	st.FailedOpen = int(failed.Load())
	st.Denied = int(denied.Load())
	if v, ok := tier.Load().(string); ok {
		st.ObservedTier = v
	}
	if v, ok := oracle.Load().(string); ok {
		st.ObservedOracle = v
	}
	if st.Streams > 0 {
		st.OpenOKPct = 100 * float64(st.Opened) / float64(st.Streams)
	}

	cleanup := func() {
		close(stop)
		for _, l := range snapshotLives(lives, &mu) {
			l.sse.Close()
		}
		done := make(chan struct{})
		go func() {
			readWG.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(45 * time.Second):
		}
	}

	if st.Opened == 0 {
		cleanup()
		st.FailReason = "no streams opened"
		if v, ok := firstErr.Load().(string); ok && v != "" {
			st.FailReason = "no streams opened: " + v
		}
		return st, nil
	}
	if err := a.checkClassification(&st); err != nil {
		cleanup()
		st.FailReason = err.Error()
		return st, nil
	}

	drv := drive{
		st:          &st,
		users:       users,
		lives:       snapshotLives(lives, &mu),
		changesN:    &changesN,
		clientBcast: &clientBcast,
		dbBcast:     &dbBcast,
		kicks:       &kicks,
		writeAt:     &writeAt,
		latMu:       &latMu,
		lats:        &lats,
		started:     time.Now(),
	}
	switch a.scenario.Plane {
	case planeBroadcast:
		a.driveBroadcast(ctx, drv)
	case planePresence:
		a.drivePresence(ctx, drv)
	case planeMixed:
		a.driveMixed(ctx, drv)
	case planeKick:
		a.driveKick(ctx, drv)
	default:
		a.driveChange(ctx, drv)
	}

	cleanup()

	after, _ := fetchMetrics(ctx, a.http, a.cfg.MetricsURL)
	st.StreamsGauge = after["sluice_streams"]
	st.Lagging = float64(lagged.Load())
	if d := labeledDelta(before, after, "sluice_stream_closed_total", "stream_lagging"); d > 0 {
		st.Lagging += d
	}
	st.TierCProbes = promSum(after, "sluice_authz_tier_c_probes_total") - promSum(before, "sluice_authz_tier_c_probes_total")
	sum, okSum := after["sluice_change_dispatch_seconds_sum"]
	cnt := after["sluice_change_dispatch_seconds_count"]
	if okSum && cnt > 0 {
		dCount := cnt - before["sluice_change_dispatch_seconds_count"]
		dSum := sum - before["sluice_change_dispatch_seconds_sum"]
		if dCount > 0 {
			st.DispatchAvg = time.Duration(float64(time.Second) * dSum / dCount).Round(time.Microsecond).String()
		}
	}

	openOK := float64(st.Opened) / float64(st.Streams)
	deliverOK := 1.0
	if st.Expected > 0 {
		deliverOK = float64(st.Received) / float64(st.Expected)
	}
	switch {
	case openOK < a.cfg.OpenMin:
		st.FailReason = fmt.Sprintf("opened %.1f%% < %.0f%%", 100*openOK, 100*a.cfg.OpenMin)
	case st.FailReason != "":
	case deliverOK < a.cfg.DeliveryMin:
		st.FailReason = fmt.Sprintf("delivered %.1f%% < %.0f%%", 100*deliverOK, 100*a.cfg.DeliveryMin)
	case st.Lagging > float64(st.Opened)/20 && st.Lagging >= 5:
		st.FailReason = fmt.Sprintf("stream_lagging delta %.0f", st.Lagging)
	default:
		st.Pass = true
	}
	return st, nil
}

func (a *app) checkClassification(st *Step) error {
	if want := a.scenario.ExpectTier; want != "" && st.ObservedTier != "" && st.ObservedTier != want {
		return fmt.Errorf("expected tier %s, ready reported %s", want, st.ObservedTier)
	}
	if want := a.scenario.ExpectOracle; want != "" && st.ObservedOracle != "" && st.ObservedOracle != want {
		return fmt.Errorf("expected oracle %s, ready reported %s", want, st.ObservedOracle)
	}
	return nil
}

func snapshotLives(lives []*live, mu *sync.Mutex) []*live {
	mu.Lock()
	defer mu.Unlock()
	out := make([]*live, len(lives))
	copy(out, lives)
	return out
}

func subsAccepted(ready *readyEvent) bool {
	if ready == nil || len(ready.Subscriptions) == 0 {
		return false
	}
	for _, s := range ready.Subscriptions {
		if !s.OK {
			return false
		}
	}
	return true
}

func titleSeq(data []byte) (int64, bool) {
	var ev struct {
		Record map[string]any `json:"record"`
	}
	if json.Unmarshal(data, &ev) != nil || ev.Record == nil {
		return 0, false
	}
	title, _ := ev.Record["title"].(string)
	if title == "" {
		title, _ = ev.Record["note"].(string)
	}
	if !strings.HasPrefix(title, "m-") {
		return 0, false
	}
	n, err := strconv.ParseInt(title[2:], 10, 64)
	return n, err == nil
}

func percentiles(v []time.Duration) (p50, p95, p99 string) {
	if len(v) == 0 {
		return "", "", ""
	}
	slices.Sort(v)
	at := func(p float64) string {
		i := int(float64(len(v)-1) * p)
		return v[i].Round(time.Millisecond).String()
	}
	return at(0.50), at(0.95), at(0.99)
}

func labeledDelta(before, after map[string]float64, metric, needle string) float64 {
	var b, a float64
	found := false
	for k, v := range after {
		if strings.HasPrefix(k, metric+"{") && strings.Contains(k, needle) {
			a += v
			found = true
		}
	}
	if !found {
		return -1
	}
	for k, v := range before {
		if strings.HasPrefix(k, metric+"{") && strings.Contains(k, needle) {
			b += v
		}
	}
	return a - b
}
