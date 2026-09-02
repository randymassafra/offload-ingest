package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
	"github.com/offloadintelligence/offload-ingest/internal/producer"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
	"github.com/offloadintelligence/offload-ingest/pkg/licensing"
	"github.com/offloadintelligence/offload-ingest/pkg/metrics"
)

// Runtime assembles a licensed ingest pipeline.
//
// It is the one place the pieces meet, and the order is the interesting part:
//
//	licence  ->  tier  ->  limiter  ->  client  ->  streamer
//
// Nothing downstream can widen what the licence granted, because each stage only
// ever receives what the previous one produced. The client cannot call a
// vertical the limiter does not know about; the limiter only knows about
// verticals the entitlement resolved; the entitlement comes from verified
// claims. There is no path from a config file to more throughput.
type Runtime struct {
	validator *licensing.Validator
	limiter   *Limiter
	weights   *CrowdWeights
	registry  *metrics.Registry
	streamer  DataStreamer
	prod      *ProductionStreamer
	mode      Mode
	log       *slog.Logger
	now       func() time.Time
	flinkAddr string
	flinkTTL  time.Duration
}

// RuntimeConfig configures a Runtime.
type RuntimeConfig struct {
	// Mode selects simulation or production; empty reads OFFLOAD_MODE.
	Mode string
	// APIKey for API-Sports; empty reads APISPORTS_KEY.
	APIKey string
	// LicensePath overrides the licence location.
	LicensePath string
	// Sports and Kinds bound simulation mode. Production takes its sports from
	// the licence, never from a flag.
	Sports []generators.Sport
	Kinds  []generators.FeedKind
	Seed   int64
	Batch  int

	// BaseWeights overrides the venue's crowd-interest profile.
	BaseWeights map[apisports.Vertical]float64

	// FlinkAddr optionally enables the downstream state-buffer scraper. Empty
	// leaves it off, which is the recommended architecture — see flink.go.
	FlinkAddr string
	// FlinkTTL is the configured state retention; defaults to 10 hours.
	FlinkTTL time.Duration

	Logger   *slog.Logger
	Now      func() time.Time
	Shutdown func(int)
}

// NewRuntime builds and licenses a pipeline.
func NewRuntime(cfg RuntimeConfig) (*Runtime, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Mode == "" {
		cfg.Mode = os.Getenv("OFFLOAD_MODE")
	}
	mode, err := ParseMode(cfg.Mode)
	if err != nil {
		return nil, err
	}

	validator, err := licensing.New(licensing.Config{
		Path: cfg.LicensePath, Logger: cfg.Logger,
		Now: cfg.Now, Shutdown: cfg.Shutdown,
	})
	if err != nil {
		return nil, fmt.Errorf("ingest: licence check failed: %w", err)
	}
	claims := validator.Claims()
	tier := validator.Tier()
	cfg.Logger.Info("ingest: licensed",
		"tenant", claims.TenantID, "tier", tier.String(),
		"sports", strings.Join(claims.Sports, ","), "mode", mode)

	r := &Runtime{
		validator: validator, registry: metrics.NewRegistry(cfg.Now),
		weights: NewCrowdWeights(cfg.BaseWeights), mode: mode,
		log: cfg.Logger, now: cfg.Now,
		flinkAddr: cfg.FlinkAddr, flinkTTL: cfg.FlinkTTL,
	}
	r.weights.SetClock(cfg.Now)

	if mode == ModeSimulation {
		// Simulation is still gated by the licence — a venue may not load-test
		// a sport it did not buy — but it spends no upstream quota, so no
		// limiter or client is built.
		sports := cfg.Sports
		if len(sports) == 0 {
			sports = licensedSports(claims)
		} else {
			sports = filterLicensed(sports, claims)
		}
		if len(sports) == 0 {
			return nil, fmt.Errorf("ingest: the licence entitles none of the requested sports")
		}
		kinds := cfg.Kinds
		if len(kinds) == 0 {
			kinds = generators.AllKinds
		}
		sim, err := NewSimulationStreamer(sports, kinds, cfg.Seed, cfg.Batch)
		if err != nil {
			return nil, err
		}
		r.streamer = sim
		return r, nil
	}

	// Production.
	key := cfg.APIKey
	if key == "" {
		key = os.Getenv("APISPORTS_KEY")
	}
	if strings.TrimSpace(key) == "" {
		return nil, fmt.Errorf("ingest: production mode needs APISPORTS_KEY")
	}

	bindings := apisports.Entitled(claims.Sports, claims.Regions)
	if len(bindings) == 0 {
		return nil, fmt.Errorf(
			"ingest: the licence entitles no sport API-Sports can serve (licensed: %s)",
			strings.Join(claims.Sports, ","))
	}
	verticals := apisports.VerticalsFor(bindings)

	limiter, err := NewLimiter(LimiterConfig{
		Tier: tier, Verticals: verticals, Weights: r.weights, Now: cfg.Now,
	})
	if err != nil {
		return nil, err
	}
	limiter.Usage().SetLogger(cfg.Logger)
	r.limiter = limiter

	client, err := apisports.New(apisports.Config{
		APIKey: key, Limiter: limiter,
		Observer: &observerFanout{limiter: limiter, registry: r.registry},
		Logger:   cfg.Logger, Now: cfg.Now,
	})
	if err != nil {
		return nil, err
	}
	prod, err := NewProductionStreamer(ProductionConfig{
		Client: client, Limiter: limiter, Weights: r.weights,
		Registry: r.registry, Bindings: bindings, Logger: cfg.Logger, Now: cfg.Now,
	})
	if err != nil {
		return nil, err
	}
	r.prod, r.streamer = prod, prod

	// What the plan actually buys, said once at startup so nobody has to work
	// it out from a dashboard later.
	for _, v := range verticals {
		cadence := AffordableLiveCadence(tier.RequestsPerDay, 3)
		cfg.Logger.Info("ingest: vertical budget",
			"vertical", v, "requests_per_day", tier.RequestsPerDay,
			"sustainable_live_cadence_over_3h", cadence.Round(time.Second).String())
	}
	return r, nil
}

// licensedSports maps the licence's sport claims onto generator sports.
func licensedSports(c licensing.Claims) []generators.Sport {
	var out []generators.Sport
	for _, s := range c.Sports {
		if sp, err := generators.ParseSport(s); err == nil {
			out = append(out, sp)
		}
	}
	return out
}

func filterLicensed(want []generators.Sport, c licensing.Claims) []generators.Sport {
	var out []generators.Sport
	for _, s := range want {
		if c.AllowsSport(string(s)) {
			out = append(out, s)
		}
	}
	return out
}

// Watch starts the background collectors: the licence ticker, the host resource
// sampler, and the Flink scraper if one was configured.
func (r *Runtime) Watch(ctx context.Context) {
	r.validator.Watch(ctx)
	r.registry.StartHostCollector(ctx, nil, 5*time.Second)
	StartFlinkCollector(ctx, FlinkConfig{
		Addr: r.flinkAddr, TTL: r.flinkTTL, Logger: r.log,
	}, r.registry)
}

// Streamer is the data source.
func (r *Runtime) Streamer() DataStreamer { return r.streamer }

// Registry is the metrics registry.
func (r *Runtime) Registry() *metrics.Registry { return r.registry }

// Close releases resources.
func (r *Runtime) Close() error { return r.streamer.Close() }

// --- dashboard.Provider ----------------------------------------------------

func (r *Runtime) LicenseStatus() licensing.Status { return r.validator.Status() }
func (r *Runtime) Mode() Mode                      { return r.mode }
func (r *Runtime) Metrics() metrics.Snapshot       { return r.registry.Snapshot() }

// SportStatus is one sport's row in the dashboard sidebar.
//
// Declared here rather than imported from internal/dashboard because pkg must
// not depend on internal: the runtime owns the facts, and the dashboard maps
// them onto its own view type.
type SportStatus struct {
	Sport    string
	Provider string
	State    string
	Health   string
	Messages int64
	Errors   int64
	Note     string
	Live     bool
}

// SportCatalog is every sport this build knows about, licensed or not.
//
// The full estate, not just what is running: an operator needs to see that
// cricket exists and is simulation-only in this mode, rather than wondering
// where it went. A sport the licence does not cover is listed and muted.
func (r *Runtime) SportCatalog() []SportStatus {
	claims := r.validator.Claims()
	snap := r.registry.Snapshot()
	perSport := map[string]metrics.SportSnapshot{}
	for _, s := range snap.Sports {
		perSport[s.Sport] = s
	}

	states := map[apisports.Vertical]string{}
	for _, p := range r.Plans() {
		states[p.Vertical] = p.StateName
	}

	out := make([]SportStatus, 0, len(generators.AllSports))
	for _, sport := range generators.AllSports {
		row := SportStatus{Sport: string(sport), Health: "unknown"}

		// Which provider serves it, and whether production can reach it.
		for _, ep := range generators.EndpointsFor(sport) {
			row.Provider = string(ep.Provider)
			break
		}
		licensed := claims.AllowsSport(string(sport))
		servedLive := r.mode == ModeProduction && apisports.Serves(apisports.Sport(sport))

		switch {
		case !licensed:
			row.Note = "not covered by this licence"
		case r.mode == ModeSimulation:
			row.Note = "simulation — generated locally"
			row.Live = true
			row.Health = "ok"
		case servedLive:
			row.Live = true
		default:
			row.Note = row.Provider + " — not ingested in production mode"
		}

		// Live sports resolve their vertical for state and counters.
		if servedLive && licensed {
			if v, err := apisports.ParseVertical(string(sport)); err == nil {
				row.State = states[v]
				// A vertical the subscription does not reach is called out as
				// such rather than left looking merely idle, which is what it
				// would otherwise resemble on the sidebar. Checked after the
				// scheduler state so it is not overwritten by it.
				if r.prod != nil {
					if reason, restricted := r.prod.PlanRestrictions()[v]; restricted {
						row.Live, row.Health = false, "warn"
						row.State, row.Note = "not on plan", reason
					}
				}
				if m, ok := perSport[string(v)]; ok {
					row.Messages, row.Errors = m.Messages, m.Errors
					switch {
					case m.Requests > 0 && float64(m.Errors)/float64(m.Requests) >= 0.05:
						row.Health = "bad"
					case m.Errors > 0:
						row.Health = "warn"
					case m.Requests > 0:
						row.Health = "ok"
					}
				}
			}
		}
		out = append(out, row)
	}
	return out
}

func (r *Runtime) Budgets() []Budget {
	if r.limiter == nil {
		return nil
	}
	return r.limiter.Budgets()
}

func (r *Runtime) Usage() []Stat {
	if r.limiter == nil {
		return nil
	}
	return r.limiter.Usage().Stats()
}

func (r *Runtime) Plans() []Plan {
	if r.prod == nil {
		return nil
	}
	return r.prod.Plans()
}

func (r *Runtime) Weights() []Snapshot {
	if r.limiter == nil {
		return nil
	}
	var vs []apisports.Vertical
	for _, b := range r.limiter.Budgets() {
		vs = append(vs, b.Vertical)
	}
	return r.weights.Snapshot(vs)
}

func (r *Runtime) Quotas() map[apisports.Vertical]apisports.Quota {
	out := map[apisports.Vertical]apisports.Quota{}
	if r.limiter == nil {
		return out
	}
	for _, b := range r.limiter.Budgets() {
		if q, ok := r.limiter.Quota(b.Vertical); ok {
			out[b.Vertical] = q
		}
	}
	return out
}

// CrowdWeights exposes the allocator so a venue's engagement telemetry can be
// fed in from outside.
func (r *Runtime) CrowdWeights() *CrowdWeights { return r.weights }

// observerFanout sends provider telemetry to both the limiter (which steers on
// it) and the metrics registry (which displays it).
type observerFanout struct {
	limiter  *Limiter
	registry *metrics.Registry
}

func (o *observerFanout) ObserveQuota(v apisports.Vertical, q apisports.Quota) {
	o.limiter.ObserveQuota(v, q)
	if q.Present {
		o.registry.Sport(string(v)).QuotaRemaining.Set(float64(q.DayRemaining))
	}
}

func (o *observerFanout) ObserveRequest(v apisports.Vertical, status int, latency time.Duration, err error) {
	o.limiter.ObserveRequest(v, status, latency, err)
	sm := o.registry.Sport(string(v))
	ms := float64(latency.Microseconds()) / 1000
	sm.LastLatency.Set(ms)
	sm.Tokens.Set(o.limiter.Tokens(v))
	o.registry.RequestLatency.Observe(ms)

	// A transport failure is booked as status 0 so it lands in the transport
	// class rather than being attributed to the provider.
	if err != nil {
		o.registry.RecordStatus(string(v), 0)
		return
	}
	o.registry.RecordStatus(string(v), status)
}

func (o *observerFanout) ObserveThrottle(v apisports.Vertical, retryAfter time.Duration) {
	o.limiter.ObserveThrottle(v, retryAfter)
	o.registry.Throttles.Inc()
	o.registry.Retries.Inc()
	o.registry.Sport(string(v)).Throttles.Inc()
}

// publishObserver adapts producer telemetry into the metrics registry.
//
// It lives here rather than in internal/producer so that the sink keeps no
// dependency on the metrics package — the lowest layer of the pipeline should
// not know what is being graphed above it.
type publishObserver struct{ registry *metrics.Registry }

// NewPublishObserver returns a producer.Observer backed by a registry.
func NewPublishObserver(reg *metrics.Registry) producer.Observer {
	return &publishObserver{registry: reg}
}

func (p *publishObserver) ObservePublish(m generators.Message, topic string, partition, bytes int, latency time.Duration) {
	ms := float64(latency.Microseconds()) / 1000
	p.registry.PublishLatency.Observe(ms)
	p.registry.Sport(string(m.Sport)).Latency.Observe(ms)
	p.registry.MessageRate.Observe()
	p.registry.Sport(string(m.Sport)).MessageRate.Observe()
	if partition >= 0 {
		p.registry.RecordPartition(topic, partition)
	}
}

func (p *publishObserver) ObservePublishError(m generators.Message, err error) {
	p.registry.Errors.Inc()
	p.registry.ErrorSeries.Observe()
	p.registry.Sport(string(m.Sport)).Errors.Inc()
}

// RecordDrift books a sweep's fidelity measurement.
func (r *Runtime) RecordDrift(v apisports.Vertical, d Drift) {
	r.registry.IngestAge.Observe(d.IngestAge)
	r.registry.Sport(string(v)).IngestAge.Add(d.IngestAge)
	if d.SkewKnown {
		r.registry.ProviderSkew.Set(d.ProviderSkew)
	}
	if d.LagSamples > 0 {
		r.registry.LiveMatchLag.Observe(d.LiveMatchLag)
	}
}
