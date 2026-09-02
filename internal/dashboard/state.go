package dashboard

import (
	"time"

	"github.com/offloadintelligence/offload-ingest/pkg/dds"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
	"github.com/offloadintelligence/offload-ingest/pkg/licensing"
	"github.com/offloadintelligence/offload-ingest/pkg/metrics"
)

// State is the whole dashboard payload.
//
// Every number the page shows is here, and the page computes nothing the API
// does not expose. That is deliberate: a support engineer curling /api/state
// must see exactly what the operator on the venue LAN is looking at.
type State struct {
	Now      time.Time        `json:"now"`
	Product  dds.Product      `json:"product"`
	Mode     string           `json:"mode"`
	Health   dds.Health       `json:"health"`
	Status   string           `json:"status"`
	License  licensing.Status `json:"license"`
	Warnings []Warning        `json:"warnings"`

	Throughput Signal        `json:"throughput"`
	Latency    Signal        `json:"latency"`
	Drift      DriftView     `json:"drift"`
	Errors     ErrorView     `json:"errors"`
	Partitions PartitionView `json:"partitions"`
	Host       HostView      `json:"host"`
	Flink      FlinkView     `json:"flink"`

	Drops     []DropRow     `json:"drops"`
	Providers []ProviderRow `json:"providers"`
	Budgets   []BudgetRow   `json:"budgets"`
}

// Warning is a single operator-facing message.
type Warning struct {
	Health dds.Health `json:"health"`
	Text   string     `json:"text"`
}

// Signal is the standard card payload: a value, a health lamp, and an hour of
// history for the sparkline. Every Golden Signal card renders from one of these,
// which is what keeps the cards visually identical across products.
type Signal struct {
	Value  float64    `json:"value"`
	Unit   string     `json:"unit"`
	Sub    string     `json:"sub"`
	Health dds.Health `json:"health"`
	Alert  bool       `json:"alert"`
	Series []float64  `json:"series"`
}

// DriftView is the three-part real-time fidelity measurement.
//
// Three numbers rather than one because the obvious single definition —
// now minus the payload timestamp — measures match elapsed time with this
// provider, not staleness. See pkg/ingest/drift.go.
type DriftView struct {
	IngestAgeSeconds float64    `json:"ingest_age_seconds"`
	SkewSeconds      float64    `json:"provider_skew_seconds"`
	SkewKnown        bool       `json:"skew_known"`
	MatchLagSeconds  float64    `json:"live_match_lag_seconds"`
	LagSamples       int        `json:"lag_samples"`
	Health           dds.Health `json:"health"`
	Alert            bool       `json:"alert"`
	Series           []float64  `json:"series"`
}

// ErrorView is the error rate split by attribution.
type ErrorView struct {
	Rate      float64    `json:"rate"`
	Total     int64      `json:"total"`
	Class4xx  int64      `json:"class_4xx"`
	Class5xx  int64      `json:"class_5xx"`
	Transport int64      `json:"transport"`
	Throttles int64      `json:"throttles"`
	Health    dds.Health `json:"health"`
	Alert     bool       `json:"alert"`
	Series    []float64  `json:"series"`
}

// PartitionView reports Kafka balance.
type PartitionView struct {
	Skew      float64 `json:"skew"`
	Hottest   int     `json:"hottest_partition"`
	Count     int     `json:"partition_count"`
	Projected bool    `json:"projected"`
	// Insufficient is true when too few messages have been written for the
	// skew figure to mean anything yet.
	Insufficient bool                        `json:"insufficient"`
	Rows         []metrics.PartitionSnapshot `json:"rows"`
	Health       dds.Health                  `json:"health"`
	Alert        bool                        `json:"alert"`
}

// HostView is the edge appliance's resources.
type HostView struct {
	Available  bool       `json:"available"`
	CPUPercent float64    `json:"cpu_percent"`
	MemUsed    float64    `json:"memory_used_bytes"`
	MemTotal   float64    `json:"memory_total_bytes"`
	MemPercent float64    `json:"memory_percent"`
	Load1      float64    `json:"load1"`
	ProcessRSS float64    `json:"process_memory_bytes"`
	Goroutines float64    `json:"goroutines"`
	CPUHealth  dds.Health `json:"cpu_health"`
	MemHealth  dds.Health `json:"memory_health"`
	Alert      bool       `json:"alert"`
	CPUSeries  []float64  `json:"cpu_series"`
	MemSeries  []float64  `json:"memory_series"`
}

// FlinkView is the downstream state buffer.
//
// Configured is false by default, and the card then explains that the metric
// belongs to the Flink product rather than showing a confident zero for
// something this process cannot see.
type FlinkView struct {
	Configured    bool       `json:"configured"`
	Reachable     bool       `json:"reachable"`
	StateBytes    float64    `json:"state_bytes"`
	CheckpointAge float64    `json:"checkpoint_age_seconds"`
	TTLSeconds    float64    `json:"ttl_seconds"`
	FillRatio     float64    `json:"fill_ratio"`
	Health        dds.Health `json:"health"`
	Alert         bool       `json:"alert"`
	Series        []float64  `json:"series"`
	Note          string     `json:"note"`
}

// DropRow is one sport's scope-enforcement summary.
//
// Published is the denominator alongside Dropped, so an operator can tell a
// sport dropping 5 of 1000 records from one dropping 5 of 5 — the same count,
// completely different situations.
type DropRow struct {
	Sport     string           `json:"sport"`
	Dropped   int64            `json:"dropped"`
	Published int64            `json:"published"`
	Rate      float64          `json:"rate"`
	Reasons   map[string]int64 `json:"reasons"`
	Mismatch  bool             `json:"mismatch"`
	// Inconclusive is true when too few records have been seen for the rate to
	// mean anything yet.
	Inconclusive bool       `json:"inconclusive"`
	Health       dds.Health `json:"health"`
}

// ProviderRow is one sport in the sidebar.
type ProviderRow struct {
	Sport    string     `json:"sport"`
	Provider string     `json:"provider"`
	State    string     `json:"state"`
	Health   dds.Health `json:"health"`
	Messages int64      `json:"messages"`
	Errors   int64      `json:"errors"`
	Note     string     `json:"note"`
	// Live is false for a sport this build cannot ingest in the current mode,
	// which is rendered muted rather than hidden — an operator needs to see
	// that cricket exists and is simulation-only, not wonder where it went.
	Live bool `json:"live"`
}

// BudgetRow joins our allocation with the provider's own reported headroom.
type BudgetRow struct {
	Vertical        string     `json:"vertical"`
	Budget          int        `json:"budget"`
	UsedToday       int        `json:"used_today"`
	UsedMonth       int        `json:"used_month"`
	Pressure        float64    `json:"pressure"`
	Deferring       bool       `json:"deferring"`
	Weight          float64    `json:"weight"`
	PerMinute       int        `json:"per_minute"`
	State           string     `json:"state"`
	IntervalSeconds float64    `json:"interval_seconds"`
	TargetSeconds   float64    `json:"target_seconds"`
	Reason          string     `json:"reason"`
	APIDayRemaining int        `json:"api_day_remaining"`
	APIDayLimit     int        `json:"api_day_limit"`
	HasAPIView      bool       `json:"has_api_view"`
	Health          dds.Health `json:"health"`
}

// Provider is what the dashboard needs from the running pipeline.
//
// An interface rather than the concrete types, so the page can be served in
// simulation mode where there is no limiter and no licence-driven budget at
// all. A nil-returning implementation is a valid one.
type Provider interface {
	LicenseStatus() licensing.Status
	Mode() ingest.Mode
	Registry() *metrics.Registry
	Budgets() []ingest.Budget
	Usage() []ingest.Stat
	Plans() []ingest.Plan
	Weights() []ingest.Snapshot
	Quotas() map[apisports.Vertical]apisports.Quota
	// SportCatalog is every sport this build knows about, licensed or not, so
	// the sidebar can show the full estate.
	SportCatalog() []ingest.SportStatus
}

// catalogRows maps the runtime's facts onto the sidebar view.
func catalogRows(in []ingest.SportStatus) []ProviderRow {
	out := make([]ProviderRow, 0, len(in))
	for _, s := range in {
		out = append(out, ProviderRow{
			Sport: s.Sport, Provider: s.Provider, State: s.State,
			Health: dds.Health(s.Health), Messages: s.Messages,
			Errors: s.Errors, Note: s.Note, Live: s.Live,
		})
	}
	return out
}
