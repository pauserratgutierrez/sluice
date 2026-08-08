// Package metrics exposes Sluice's Prometheus collectors.
//
// The design principle is that the expensive path must be LOUD. Supabase's
// central ergonomic failure is that you can write a policy that costs 100x and
// never be told; TierCProbes and AuthzCompileFailures exist so that cannot
// happen here. Alert on TierCProbes.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ---- replication -----------------------------------------------------
	WALLsn = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sluice_wal_lsn",
		Help: "Log sequence number by kind (received, confirmed).",
	}, []string{"kind"})

	WALLagBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sluice_wal_lag_bytes",
		Help: "Bytes between the received and confirmed LSN.",
	})

	SlotRetainedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sluice_slot_retained_bytes",
		Help: "WAL retained by the replication slot. Watch this: an unconsumed slot can fill the disk.",
	})

	WALMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_wal_messages_total",
		Help: "Decoded pgoutput messages by type.",
	}, []string{"type"})

	ReaderReconnects = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_reader_reconnects_total",
		Help: "Replication reconnections by reason.",
	}, []string{"reason"})

	ReaderIsLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sluice_reader_is_leader",
		Help: "1 when this process holds the single-reader advisory lock.",
	})

	// ---- dispatch --------------------------------------------------------
	Changes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_changes_total",
		Help: "Row changes decoded, by relation and operation.",
	}, []string{"schema", "table", "op"})

	DispatchSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sluice_change_dispatch_seconds",
		Help:    "Time to route, authorize and enqueue one change.",
		Buckets: []float64{.00001, .00005, .0001, .0005, .001, .005, .01, .05, .1, .5},
	}, []string{"schema", "table"})

	RoutingCandidates = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sluice_routing_candidates",
		Help:    "Subscriptions considered per change. Should stay small; growth means shapes are not indexed.",
		Buckets: []float64{1, 2, 5, 10, 25, 50, 100, 500, 1000, 5000},
	}, []string{"schema", "table"})

	ToastUnchanged = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_toast_unchanged_total",
		Help: "Columns delivered as unchanged-TOAST placeholders.",
	}, []string{"schema", "table", "column"})

	// ---- authorization ---------------------------------------------------
	Subscriptions = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sluice_subscriptions",
		Help: "Active subscriptions by relation, tier and whether they are indexed.",
	}, []string{"schema", "table", "tier", "indexed"})

	AuthzResolutions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_authz_resolutions_total",
		Help: "Subscribe-time authorization resolutions by tier and result.",
	}, []string{"tier", "result"})

	AuthzResolveSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sluice_authz_resolve_seconds",
		Help:    "Time to resolve a subscription's authorization.",
		Buckets: prometheus.DefBuckets,
	}, []string{"tier"})

	// TierCProbes is the metric to alert on. Every increment is an impersonated
	// query on the per-change path, which is the thing Sluice exists to avoid.
	TierCProbes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_authz_tier_c_probes_total",
		Help: "Impersonated per-change authorization probes. ALERT ON THIS.",
	}, []string{"schema", "table"})

	TierCWithheld = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_authz_tier_c_withheld_total",
		Help: "Changes withheld from Tier C subscribers because the probe budget was exhausted.",
	}, []string{"schema", "table"})

	AuthzCompileFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_authz_compile_failures_total",
		Help: "Predicates that could not be compiled in process, by reason.",
	}, []string{"schema", "table", "reason"})

	// AuthzDowngrades counts Tier B subscriptions demoted to Tier C because the
	// in-process evaluator disagreed with PostgreSQL on the same tuple. This
	// should be zero forever; a non-zero value is a compiler bug, not a
	// configuration issue. Page on it.
	AuthzDowngrades = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_authz_downgrades_total",
		Help: "Tier B decisions demoted after disagreeing with PostgreSQL. Should always be 0.",
	}, []string{"schema", "table"})

	AuthzUnknown = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_authz_unknown_total",
		Help: "Visibility decisions that could not be made because the WAL lacked a needed value.",
	}, []string{"schema", "table", "op"})

	AuthzLeaseRefreshes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_authz_lease_refreshes_total",
		Help: "Lease re-evaluations of volatile predicates, by outcome.",
	}, []string{"result"})

	// ---- streams ---------------------------------------------------------
	Streams = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sluice_streams",
		Help: "Open SSE streams.",
	})

	StreamDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_stream_dropped_events_total",
		Help: "Events dropped by plane and reason.",
	}, []string{"plane", "reason"})

	StreamClosed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_stream_closed_total",
		Help: "Streams closed by reason.",
	}, []string{"reason"})

	// ---- signalling and snapshots ---------------------------------------
	BroadcastPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_broadcast_published_total",
		Help: "Broadcasts published by namespace and origin.",
	}, []string{"namespace", "origin"})

	PresenceMembers = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sluice_presence_members",
		Help: "Presence members per channel.",
	}, []string{"channel"})

	SnapshotSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sluice_snapshot_seconds",
		Help:    "Time to serve an initial snapshot.",
		Buckets: prometheus.DefBuckets,
	}, []string{"schema", "table"})

	SnapshotRows = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_snapshot_rows_total",
		Help: "Rows emitted by initial snapshots.",
	}, []string{"schema", "table"})

	Revocations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_revocations_total",
		Help: "Streams terminated by pushed revocation, by source.",
	}, []string{"source"})

	// ---- configuration health -------------------------------------------
	ConfigWarnings = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sluice_config_warnings",
		Help: "Active configuration warnings by code. Non-zero means /diagnostics has something to say.",
	}, []string{"code"})
)
