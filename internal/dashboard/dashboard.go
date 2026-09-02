// Package dashboard serves the Offload Ingest operator view.
//
// The audience is one person: whoever is standing in the venue wondering why a
// screen has gone stale. They need the Golden Signals fast — throughput,
// latency, real-time fidelity, error rate — plus what the licence allows, how
// much API headroom is left, and whether this is live data or simulation.
//
// Layout, palette and card anatomy come from pkg/dds and are shared with the
// other Offload products. This package supplies only the numbers.
package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"time"

	"github.com/offloadintelligence/offload-ingest/pkg/dds"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
	"github.com/offloadintelligence/offload-ingest/pkg/metrics"
)

// ProductName is what the shared header renders.
const ProductName = "Offload Ingest"

// Server is the dashboard HTTP server.
type Server struct {
	provider Provider
	log      *slog.Logger
	srv      *http.Server
	now      func() time.Time
	version  string
	page     string
}

// Config configures the dashboard.
type Config struct {
	Addr     string
	Provider Provider
	Version  string
	Logger   *slog.Logger
	Now      func() time.Time
}

// New builds the server.
func New(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Version == "" {
		cfg.Version = "dev"
	}
	s := &Server{
		provider: cfg.Provider, log: cfg.Logger, now: cfg.Now,
		version: cfg.Version,
	}
	// The page is rendered once at construction: it is a static shell whose
	// numbers all arrive from /api/state, so re-rendering it per request would
	// be work with no output difference.
	s.page = renderPage(cfg.Version)

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/state", s.handleState)
	mux.HandleFunc("/healthz", s.handleHealth)
	s.srv = &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Start binds and serves in the background, returning the bound address.
//
// The listener is created synchronously so a port clash is reported to the
// caller rather than appearing in a log a second later, and so a test can ask
// for :0 and learn which port it got.
func (s *Server) Start() (string, error) {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return "", fmt.Errorf("dashboard: cannot bind %s: %w", s.srv.Addr, err)
	}
	go func() {
		if err := s.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.log.Error("dashboard: server stopped", "err", err)
		}
	}()
	return ln.Addr().String(), nil
}

// Shutdown stops the server.
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

// Handler exposes the mux, for tests and for embedding.
func (s *Server) Handler() http.Handler { return s.srv.Handler }

// State assembles the current view.
func (s *Server) State() State {
	now := s.now()
	st := State{
		Now: now,
		Product: dds.Product{
			Name: ProductName, Version: s.version, DDSVersion: dds.Version,
		},
		Health: dds.HealthUnknown,
		Status: "no pipeline attached",
	}
	if s.provider == nil {
		return st
	}

	st.Mode = string(s.provider.Mode())
	st.License = s.provider.LicenseStatus()
	reg := s.provider.Registry()
	if reg == nil {
		return st
	}
	snap := reg.Snapshot()

	st.Throughput = throughputSignal(reg, snap)
	st.Latency = latencySignal(reg)
	st.Drift = driftView(reg)
	st.Errors = errorView(reg, snap)
	st.Partitions = partitionView(reg)
	st.Host = hostView(reg)
	st.Flink = flinkView(reg)
	st.Drops = dropRows(reg, snap)
	st.Providers = catalogRows(s.provider.SportCatalog())
	st.Budgets = budgetRows(s.provider)
	st.Warnings = warnings(st, now)
	st.Health, st.Status = overall(st)
	return st
}

// throughputSignal is messages per minute with an hour of history.
func throughputSignal(reg *metrics.Registry, snap metrics.Snapshot) Signal {
	series := reg.MessageRate.Totals()
	last := 0.0
	if n := len(series); n > 0 {
		last = series[n-1]
	}
	return Signal{
		Value: last, Unit: "msg/min", Series: series,
		Sub:    fmt.Sprintf("%s published all time", humanCount(snap.Messages)),
		Health: dds.HealthOK,
	}
}

// latencySignal is the poll-to-Kafka delta.
func latencySignal(reg *metrics.Registry) Signal {
	p95 := reg.PublishLatency.Quantile(0.95)
	health := dds.ClassifyLatency(p95)
	return Signal{
		Value: p95, Unit: "ms p95",
		Sub:    fmt.Sprintf("mean %.0f ms over %s samples", reg.PublishLatency.Mean(), humanCount(int64(reg.PublishLatency.Count()))),
		Health: health,
		Alert:  p95 >= dds.LatencyAlertMS,
		Series: reg.MessageRate.Means(),
	}
}

func driftView(reg *metrics.Registry) DriftView {
	age := reg.IngestAge.Quantile(0.95)
	// Staleness bands: a bulk sweep lands within a second or two of the
	// response, so a minute of age already means the pipeline is backing up.
	health := dds.ClassifyRatio(age, 60, 300)
	return DriftView{
		IngestAgeSeconds: age,
		SkewSeconds:      reg.ProviderSkew.Value(),
		SkewKnown:        reg.ProviderSkew.Value() != 0,
		MatchLagSeconds:  reg.LiveMatchLag.Quantile(0.5),
		LagSamples:       int(reg.LiveMatchLag.Count()),
		Health:           health,
		Alert:            health == dds.HealthBad,
	}
}

func errorView(reg *metrics.Registry, snap metrics.Snapshot) ErrorView {
	rate := reg.ErrorRate()
	var c4, c5, transport int64
	for _, sp := range snap.Sports {
		c4 += sp.Errors4xx
		c5 += sp.Errors5xx
		transport += sp.ErrorsTransport
	}
	health := dds.ClassifyErrorRate(rate)
	return ErrorView{
		Rate: rate, Total: snap.Errors,
		Class4xx: c4, Class5xx: c5, Transport: transport,
		Throttles: snap.Throttles,
		Health:    health,
		Alert:     rate >= dds.ErrorRateAlert,
		Series:    reg.ErrorSeries.Totals(),
	}
}

func partitionView(reg *metrics.Registry) PartitionView {
	rows := reg.Partitions()
	v := PartitionView{Rows: rows, Count: len(rows), Skew: reg.PartitionSkew()}
	var hottest int64 = -1
	for _, r := range rows {
		if r.Writes > hottest {
			hottest, v.Hottest = r.Writes, r.Partition
		}
		if r.Topic == "(discarded)" {
			v.Projected = true
		}
	}
	// A partition carrying 50% more than the mean is worth flagging; double the
	// mean is a hot partition by any reading.
	v.Health = dds.ClassifyRatio(v.Skew, 0.5, 1.0)

	// Skew is meaningless on a small sample. Sixteen messages across five
	// partitions is uneven by arithmetic, not by keying, and alerting on it
	// would have every fresh start come up amber for its first minute. The
	// floor is ten writes per partition before the figure is trusted.
	var total int64
	for _, r := range rows {
		total += r.Writes
	}
	if len(rows) < 2 || total < int64(10*len(rows)) {
		v.Health = dds.HealthUnknown
		v.Insufficient = true
	}
	v.Alert = v.Health == dds.HealthBad
	return v
}

func hostView(reg *metrics.Registry) HostView {
	h := reg.Host
	v := HostView{
		Available:  h.Available.Value(),
		CPUPercent: h.CPUPercent.Value(),
		MemUsed:    h.MemUsed.Value(),
		MemTotal:   h.MemTotal.Value(),
		MemPercent: h.MemPercent.Value(),
		Load1:      h.LoadAvg1.Value(),
		ProcessRSS: h.ProcessRSS.Value(),
		Goroutines: h.Goroutines.Value(),
		CPUSeries:  h.CPUSeries.Totals(),
		MemSeries:  h.MemSeries.Totals(),
	}
	if !v.Available {
		v.CPUHealth, v.MemHealth = dds.HealthUnknown, dds.HealthUnknown
		return v
	}
	// An edge box running the whole pipeline should sit well under capacity;
	// sustained 80% means there is no headroom for a Saturday card.
	v.CPUHealth = dds.ClassifyRatio(v.CPUPercent, 70, 88)
	v.MemHealth = dds.ClassifyRatio(v.MemPercent, 75, 90)
	v.Alert = v.CPUHealth == dds.HealthBad || v.MemHealth == dds.HealthBad
	return v
}

func flinkView(reg *metrics.Registry) FlinkView {
	f := reg.Flink
	v := FlinkView{
		Configured:    f.Configured.Value(),
		Reachable:     f.Reachable.Value(),
		StateBytes:    f.StateBytes.Value(),
		CheckpointAge: f.CheckpointAge.Value(),
		TTLSeconds:    f.TTLSeconds.Value(),
		Series:        f.StateSeries.Totals(),
	}
	switch {
	case !v.Configured:
		v.Health = dds.HealthUnknown
		v.Note = "Not configured. Flink runs as a separate process, so this " +
			"metric belongs to the Flink product's own dashboard. Set -flink-addr " +
			"to scrape it here instead."
	case !v.Reachable:
		v.Health = dds.HealthBad
		v.Alert = true
		v.Note = "Configured but the JobManager did not answer the last scrape."
	default:
		// The checkpoint age against the retention window is the signal that
		// matters: a state buffer that stops checkpointing is the one that
		// eventually falls over.
		if v.TTLSeconds > 0 {
			v.FillRatio = v.CheckpointAge / v.TTLSeconds
		}
		v.Health = dds.ClassifyRatio(v.FillRatio, 0.6, 0.85)
		v.Alert = v.Health == dds.HealthBad
		v.Note = fmt.Sprintf("retention %s", time.Duration(v.TTLSeconds)*time.Second)
	}
	return v
}

// dropRows summarises scope enforcement per sport.
func dropRows(reg *metrics.Registry, snap metrics.Snapshot) []DropRow {
	byReason := map[string]map[string]int64{}
	for _, d := range reg.Drops() {
		if byReason[d.Sport] == nil {
			byReason[d.Sport] = map[string]int64{}
		}
		byReason[d.Sport][d.Reason] = d.Count
	}
	var out []DropRow
	for _, sp := range snap.Sports {
		if sp.Dropped == 0 {
			continue
		}
		row := DropRow{
			Sport: sp.Sport, Dropped: sp.Dropped, Published: sp.Messages,
			Rate: sp.DropRate, Reasons: byReason[sp.Sport],
		}
		row.Health = dds.HealthOK
		// A rate is only evidence on a large enough sample; see DropSampleFloor.
		if row.Dropped+row.Published < metrics.DropSampleFloor {
			row.Inconclusive = true
		} else if row.Rate >= metrics.DropRateWarn {
			row.Health = dds.HealthWarn
			row.Mismatch = true
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Rate > out[j].Rate })
	return out
}

func budgetRows(p Provider) []BudgetRow {
	usage := map[apisports.Vertical]ingest.Stat{}
	for _, u := range p.Usage() {
		usage[u.Vertical] = u
	}
	plans := map[apisports.Vertical]ingest.Plan{}
	for _, pl := range p.Plans() {
		plans[pl.Vertical] = pl
	}
	quotas := p.Quotas()

	var out []BudgetRow
	for _, b := range p.Budgets() {
		row := BudgetRow{
			Vertical: string(b.Vertical), Budget: b.Requests,
			Weight: b.Weight, PerMinute: b.PerMinute,
		}
		if u, ok := usage[b.Vertical]; ok {
			row.UsedToday, row.UsedMonth = u.Today, u.Month
			row.Pressure, row.Deferring = u.Pressure, u.Deferring
		}
		if pl, ok := plans[b.Vertical]; ok {
			row.State = pl.StateName
			row.IntervalSeconds, row.TargetSeconds = pl.IntervalSeconds, pl.TargetSeconds
			row.Reason = pl.Reason
		}
		if q, ok := quotas[b.Vertical]; ok && q.Present {
			row.HasAPIView = true
			row.APIDayRemaining, row.APIDayLimit = q.DayRemaining, q.DayLimit
		}
		row.Health = dds.ClassifyRatio(row.Pressure, ingest.WarnAt, ingest.DeferAt)
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Vertical < out[j].Vertical })
	return out
}

// warnings are the things worth saying out loud on the page.
func warnings(st State, now time.Time) []Warning {
	var out []Warning
	add := func(h dds.Health, format string, args ...any) {
		out = append(out, Warning{Health: h, Text: fmt.Sprintf(format, args...)})
	}

	if !st.License.Valid && st.License.LicenseID != "" {
		add(dds.HealthBad, "LICENCE INVALID — %s", st.License.Error)
	} else if st.License.InGrace {
		add(dds.HealthWarn,
			"Licence EXPIRED and running on the offline grace period — shuts down %s (%d days left)",
			st.License.Deadline.Format("2 Jan 2006 15:04 MST"),
			st.License.DaysRemaining(now))
	}
	if st.Mode == string(ingest.ModeSimulation) {
		add(dds.HealthWarn,
			"SIMULATION MODE — payloads are generated locally; no upstream data is being ingested")
	}
	if st.Latency.Alert {
		add(dds.HealthBad, "Poll-to-Kafka p95 is %.0f ms, above the %d ms threshold",
			st.Latency.Value, dds.LatencyAlertMS)
	}
	if st.Errors.Alert {
		add(dds.HealthBad, "Error rate is %.1f%%, above the %.0f%% threshold",
			st.Errors.Rate*100, dds.ErrorRateAlert*100)
	}
	if st.Partitions.Alert {
		add(dds.HealthWarn,
			"Kafka partition %d is carrying %.0f%% more than the mean — hot partition",
			st.Partitions.Hottest, st.Partitions.Skew*100)
	}
	if st.Host.Alert {
		add(dds.HealthWarn, "Edge host under pressure: CPU %.0f%%, memory %.0f%%",
			st.Host.CPUPercent, st.Host.MemPercent)
	}
	if st.Flink.Configured && !st.Flink.Reachable {
		add(dds.HealthBad, "Flink JobManager is not answering; state-buffer size is unknown")
	}
	for _, b := range st.Budgets {
		if b.Deferring {
			add(dds.HealthWarn,
				"%s has spent %d%% of its daily budget; low-priority polling has stood down",
				b.Vertical, int(b.Pressure*100))
		}
	}
	// A licence mismatch: the feed and the licence disagree about what this
	// venue bought. Reported per sport, because one misconfigured sport is a
	// very different situation from the whole estate being wrong.
	for _, d := range st.Drops {
		// Mismatch already accounts for both the rate and the sample floor;
		// re-deriving the condition here is how the two drift apart.
		if !d.Mismatch {
			continue
		}
		add(dds.HealthWarn,
			"LICENCE MISMATCH — %.0f%% of the %s feed is being dropped as out of scope (%d records). "+
				"The licensed leagues and the provider's card disagree.",
			d.Rate*100, d.Sport, d.Dropped)
	}
	if st.Errors.Throttles > 0 {
		add(dds.HealthWarn,
			"%d rate-limit rejections (429) since start — the limiter has backed off",
			st.Errors.Throttles)
	}
	return out
}

// overall reduces the page to the single lamp in the header.
//
// Worst-wins: the header must never read green while a card is red, because
// the header is the only thing visible from across a room.
func overall(st State) (dds.Health, string) {
	if !st.License.Valid && st.License.LicenseID != "" {
		return dds.HealthBad, "licence invalid"
	}
	worst, reason := dds.HealthOK, "nominal"
	demote := func(h dds.Health, why string) {
		switch {
		case h == dds.HealthBad:
			worst, reason = dds.HealthBad, why
		case h == dds.HealthWarn && worst != dds.HealthBad:
			worst, reason = dds.HealthWarn, why
		}
	}
	demote(st.Latency.Health, "latency")
	for _, d := range st.Drops {
		if d.Mismatch {
			demote(dds.HealthWarn, "licence mismatch")
		}
	}
	demote(st.Errors.Health, "errors")
	demote(st.Drift.Health, "drift")
	if st.Partitions.Health != dds.HealthUnknown {
		demote(st.Partitions.Health, "partition skew")
	}
	if st.Host.Available {
		demote(st.Host.CPUHealth, "host cpu")
		demote(st.Host.MemHealth, "host memory")
	}
	if st.Flink.Configured {
		demote(st.Flink.Health, "flink state")
	}
	if st.License.InGrace {
		demote(dds.HealthWarn, "licence in grace")
	}
	return worst, reason
}

func humanCount(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// --- handlers ---------------------------------------------------------------

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(s.State()); err != nil {
		s.log.Error("dashboard: encoding state", "err", err)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	st := s.State()
	// Health follows the licence: an unlicensed process is not healthy, whatever
	// else is working, because it is about to stop.
	if s.provider != nil && !st.License.Valid && st.License.LicenseID != "" {
		http.Error(w, "license invalid", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "ok")
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(s.page))
}
