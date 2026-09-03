package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/scope"
	"github.com/offloadintelligence/offload-ingest/pkg/metrics"
)

// Mode selects where messages come from.
type Mode string

const (
	// ModeSimulation drives the local generators. No upstream is contacted and
	// no quota is spent, which is what makes round-the-clock load testing
	// possible on a 100/day plan.
	ModeSimulation Mode = "simulation"
	// ModeProduction delegates entirely to API-Sports.
	ModeProduction Mode = "production"
)

// ParseMode reads a mode, defaulting to simulation.
//
// Simulation is the default on purpose. An operator who mis-types the variable
// gets a load test, not an unplanned run against a live metered API — the
// failure mode should waste nobody's quota.
func ParseMode(s string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "simulation", "sim", "mock":
		return ModeSimulation, nil
	case "production", "prod", "live":
		return ModeProduction, nil
	default:
		return "", fmt.Errorf("ingest: unknown OFFLOAD_MODE %q (want simulation or production)", s)
	}
}

// DataStreamer is the seam between simulation and production.
//
// Both sides emit generators.Message, so everything downstream — the Kafka
// producer, the partitioning, the headers, the webhook emitters — is identical
// whichever is in use. That is the point: a venue's production pipeline is the
// one that was load-tested, not a different code path that happens to look
// similar.
type DataStreamer interface {
	// Next returns the messages available now. It blocks until at least one is
	// ready or the context ends. A production streamer paces itself against
	// the licence budget; a simulation one is bounded only by the clock.
	Next(ctx context.Context) ([]generators.Message, error)
	// Sports lists what this streamer covers.
	Sports() []generators.Sport
	// Mode reports which side of the seam this is.
	Mode() Mode
	// Close releases resources.
	Close() error
}

// SimulationStreamer drives the local generators.
type SimulationStreamer struct {
	mu    sync.Mutex
	feeds []generators.Feed
	batch int
}

// NewSimulationStreamer builds a streamer over the generator catalog.
func NewSimulationStreamer(sports []generators.Sport, kinds []generators.FeedKind, seed int64, batch int) (*SimulationStreamer, error) {
	feeds, err := generators.NewAll(sports, kinds, seed)
	if err != nil {
		return nil, err
	}
	if len(feeds) == 0 {
		return nil, fmt.Errorf("ingest: no generator feeds for the requested sports")
	}
	if batch <= 0 {
		batch = 1
	}
	return &SimulationStreamer{feeds: feeds, batch: batch}, nil
}

// Next advances every feed once per call.
func (s *SimulationStreamer) Next(ctx context.Context) ([]generators.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]generators.Message, 0, len(s.feeds)*s.batch)
	for _, f := range s.feeds {
		for i := 0; i < s.batch; i++ {
			out = append(out, f.Next())
		}
	}
	return out, nil
}

// Sports lists the simulated sports.
func (s *SimulationStreamer) Sports() []generators.Sport {
	seen := map[generators.Sport]bool{}
	var out []generators.Sport
	for _, f := range s.feeds {
		sp := f.Endpoint().Sport
		if !seen[sp] {
			seen[sp] = true
			out = append(out, sp)
		}
	}
	return out
}

func (s *SimulationStreamer) Mode() Mode   { return ModeSimulation }
func (s *SimulationStreamer) Close() error { return nil }

// ProductionStreamer delegates entirely to API-Sports.
//
// It owns the adaptive loop: each vertical is swept on its own schedule, the
// schedule is recomputed from the sweep's own state tally, and the limiter
// decides whether the sweep happens at all.
type ProductionStreamer struct {
	sweeper   *Sweeper
	scheduler *Scheduler
	limiter   *Limiter
	weights   *CrowdWeights
	registry  *metrics.Registry
	log       *slog.Logger
	now       func() time.Time

	// bindings maps each vertical back to the pipeline sports it serves, so a
	// message carries the pipeline's own sport token rather than the provider's
	// host name.
	bindings map[apisports.Vertical][]apisports.Binding

	mu         sync.Mutex
	nextDue    map[apisports.Vertical]time.Time
	sequence   int64
	restricted map[apisports.Vertical]string
}

// ProductionConfig configures a ProductionStreamer.
type ProductionConfig struct {
	Client   *apisports.Client
	Limiter  *Limiter
	Weights  *CrowdWeights
	Registry *metrics.Registry
	Bindings []apisports.Binding
	Logger   *slog.Logger
	Now      func() time.Time
}

// NewProductionStreamer builds the live streamer.
func NewProductionStreamer(cfg ProductionConfig) (*ProductionStreamer, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("ingest: production mode needs an API-Sports client")
	}
	if cfg.Limiter == nil {
		return nil, fmt.Errorf("ingest: production mode needs a limiter")
	}
	if len(cfg.Bindings) == 0 {
		return nil, fmt.Errorf("ingest: the licence entitles no API-Sports sports")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Registry == nil {
		cfg.Registry = metrics.NewRegistry(cfg.Now)
	}

	byVertical := map[apisports.Vertical][]apisports.Binding{}
	for _, b := range cfg.Bindings {
		byVertical[b.Vertical] = append(byVertical[b.Vertical], b)
	}
	sched := NewScheduler(cfg.Limiter, cfg.Weights, cfg.Now)
	due := map[apisports.Vertical]time.Time{}
	for v := range byVertical {
		sched.Register(v)
		// Everything is due immediately on the first pass, so a cold start
		// learns the real state of every vertical before it starts pacing.
		due[v] = cfg.Now()
	}
	return &ProductionStreamer{
		sweeper: NewSweeper(cfg.Client, cfg.Now), scheduler: sched,
		limiter: cfg.Limiter, weights: cfg.Weights, registry: cfg.Registry,
		log: cfg.Logger, now: cfg.Now, bindings: byVertical, nextDue: due,
	}, nil
}

// Next sweeps whichever verticals are due and returns their messages.
//
// If nothing is due it sleeps until the earliest one is, rather than spinning.
// On a budget-limited plan that sleep is frequently minutes long, and burning
// CPU to rediscover that every millisecond would be the wrong kind of busy.
func (p *ProductionStreamer) Next(ctx context.Context) ([]generators.Message, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		now := p.now()
		var ready []apisports.Vertical
		earliest := time.Time{}

		p.mu.Lock()
		for v, due := range p.nextDue {
			if !due.After(now) {
				ready = append(ready, v)
			} else if earliest.IsZero() || due.Before(earliest) {
				earliest = due
			}
		}
		p.mu.Unlock()

		if len(ready) == 0 {
			wait := time.Until(earliest)
			if earliest.IsZero() || wait <= 0 {
				wait = time.Second
			}
			if wait > 30*time.Second {
				// Wake periodically even on a long sleep so a weight or licence
				// change is picked up promptly.
				wait = 30 * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
			continue
		}

		// Sweeps are returned one vertical at a time rather than accumulated
		// across every ready vertical.
		//
		// At a cold start all verticals fall due together, and batching them
		// meant the first sweep's records waited for the ninth sweep's rate
		// limiter before being published — measured at over two seconds of
		// poll-to-Kafka latency, all of it self-inflicted. Returning as soon as
		// one vertical is ready keeps that delta down to the sweep itself.
		for _, v := range ready {
			msgs, err := p.sweepOne(ctx, v)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				var apiErr *apisports.APIError
				if errors.As(err, &apiErr) && apiErr.IsPlanRestriction() {
					// The plan does not cover this vertical's window. That will
					// still be true in ten seconds and in ten minutes, so the
					// vertical stands down for the rest of the day instead of
					// spending budget rediscovering it.
					//
					// Deliberately NOT counted as an error: a licensing
					// boundary is a configuration fact, not a failure, and
					// booking it as one turned the dashboard's error rate red
					// on a pipeline that was working correctly.
					p.standDown(v)
					p.planRestricted(v, err.Error())
					p.log.Warn("ingest: vertical is outside the plan; standing down until tomorrow",
						"vertical", v, "err", err)
					continue
				}
				// A genuine failure. Classified so a 4xx, a 5xx and a transport
				// fault stay distinguishable on the dashboard.
				p.registry.RecordStatus(string(v), statusOf(err))
				p.log.Warn("ingest: sweep failed", "vertical", v, "err", err)
				p.reschedule(v)
				continue
			}
			if len(msgs) > 0 {
				return msgs, nil
			}
			// An empty card is a valid answer; move to the next ready vertical.
		}
		// Every ready vertical produced nothing — an out-of-season card, or a
		// deferred budget. Loop round rather than returning an empty batch.
	}
}

// sweepOne performs one vertical's bulk fetch and turns it into messages.
func (p *ProductionStreamer) sweepOne(ctx context.Context, v apisports.Vertical) ([]generators.Message, error) {
	sweep, err := p.sweeper.Sweep(ctx, v)
	if err != nil {
		return nil, err
	}
	p.registry.Sweeps.Inc()
	p.registry.Requests.Inc()
	p.registry.RequestRate.Mark()
	p.registry.Sport(string(v)).Requests.Inc()
	// Recorded here rather than after toMessages, and deliberately: the
	// provider answered. An out-of-season vertical returns an empty card
	// forever, and booking that as a failed poll would make the readiness
	// probe unable to tell a quiet sport from an unreachable one.
	p.registry.RecordPoll(string(v))

	// Real-time fidelity, measured from the same response — no extra request.
	p.registry.IngestAge.Observe(sweep.Drift.IngestAge)
	p.registry.Sport(string(v)).IngestAge.Add(sweep.Drift.IngestAge)
	if sweep.Drift.SkewKnown {
		p.registry.ProviderSkew.Set(sweep.Drift.ProviderSkew)
	}
	if sweep.Drift.LagSamples > 0 {
		p.registry.LiveMatchLag.Observe(sweep.Drift.LiveMatchLag)
	}

	// The sweep tells the scheduler and the crowd allocator what is happening,
	// so neither needs a request of its own.
	p.scheduler.SetState(v, sweep.State)
	if p.weights != nil {
		p.weights.ObserveLive(v, sweep.Live)
	}
	p.reschedule(v)

	msgs := p.toMessages(v, sweep)
	p.registry.Messages.Add(int64(len(msgs)))
	p.registry.Sport(string(v)).Messages.Add(int64(len(msgs)))
	if len(msgs) > 0 {
		p.registry.RecordData(string(v))
	}
	return msgs, nil
}

// statusOf maps a sweep failure onto an HTTP status class for accounting.
//
// A body-level APIError arrived on a 200 but is the provider rejecting our
// request, so it books as a 4xx. Anything else never produced a usable
// response and books as transport, which keeps "our network is broken" from
// being reported as "the provider is down".
func statusOf(err error) int {
	var apiErr *apisports.APIError
	if errors.As(err, &apiErr) {
		return 400
	}
	var throttled *apisports.ThrottleError
	if errors.As(err, &throttled) {
		return 429
	}
	return 0
}

// planRestricted records that a vertical is outside the subscription.
func (p *ProductionStreamer) planRestricted(v apisports.Vertical, reason string) {
	p.mu.Lock()
	if p.restricted == nil {
		p.restricted = map[apisports.Vertical]string{}
	}
	p.restricted[v] = reason
	p.mu.Unlock()
}

// PlanRestrictions reports the verticals standing down because the
// subscription does not cover them, so the dashboard can say so plainly
// instead of leaving them looking merely idle.
func (p *ProductionStreamer) PlanRestrictions() map[apisports.Vertical]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[apisports.Vertical]string, len(p.restricted))
	for k, v := range p.restricted {
		out[k] = v
	}
	return out
}

// standDown parks a vertical until the next UTC day, when the plan's rolling
// window will have moved and its quota resets.
func (p *ProductionStreamer) standDown(v apisports.Vertical) {
	now := p.now().UTC()
	tomorrow := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).
		Add(24 * time.Hour)
	p.mu.Lock()
	p.nextDue[v] = tomorrow
	p.mu.Unlock()
}

// reschedule sets the vertical's next due time from the current plan.
func (p *ProductionStreamer) reschedule(v apisports.Vertical) {
	plan := p.scheduler.PlanFor(v)
	p.mu.Lock()
	p.nextDue[v] = p.now().Add(plan.Interval)
	p.mu.Unlock()

	if p.weights != nil {
		if sm := p.registry.Sport(string(v)); sm != nil {
			sm.CrowdWeight.Set(plan.Share)
			sm.Tokens.Set(p.limiter.Tokens(v))
		}
	}
}

// toMessages turns raw provider documents into pipeline messages.
//
// The payload is the provider's own JSON, unmodified — the same contract the
// pipeline has always had. Routing travels beside it, not inside it.
func (p *ProductionStreamer) toMessages(v apisports.Vertical, s *Sweep) []generators.Message {
	spec, _ := apisports.SpecFor(v)
	binds := p.bindings[v]
	if len(binds) == 0 {
		return nil
	}
	// One vertical can serve several pipeline sports (NFL and NCAAF share a
	// host). Without a league id on the row there is no way to tell them apart,
	// so the row is attributed to the first binding and the league is carried
	// in the payload for a downstream consumer to filter on.
	sport := generators.Sport(binds[0].Sport)

	out := make([]generators.Message, 0, len(s.Fixtures))
	for _, row := range s.Fixtures {
		p.mu.Lock()
		p.sequence++
		seq := p.sequence
		p.mu.Unlock()

		var payload any
		if err := json.Unmarshal(row, &payload); err != nil {
			continue
		}
		// Envelope normalisation. Reading the scope identity here — once, where
		// the record enters the pipeline — is what lets every stage downstream
		// see one consistent field regardless of which provider shape it came
		// from, without anything writing into the payload.
		id := scope.Normalize(payload)
		out = append(out, generators.Message{
			Sport:              sport,
			Kind:               generators.FeedBoxScore,
			Endpoint:           spec.BulkPath,
			Model:              string(v),
			FixtureID:          fixtureID(row),
			Sequence:           seq,
			Emitted:            s.FetchedAt,
			NormalizedLeagueID: id.LeagueID,
			ProviderOrgID:      id.OrgID,
			LeagueName:         id.LeagueName,
			Payload:            payload,
		})
	}
	return out
}

// idHolder covers the places the verticals put a fixture id.
type idHolder struct {
	ID      json.Number `json:"id"`
	Fixture struct {
		ID json.Number `json:"id"`
	} `json:"fixture"`
	Game struct {
		ID json.Number `json:"id"`
	} `json:"game"`
}

// fixtureID extracts the provider's fixture identifier, which becomes the Kafka
// partition key so every update for one game stays ordered on one partition.
func fixtureID(row json.RawMessage) string {
	var h idHolder
	if err := json.Unmarshal(row, &h); err != nil {
		return "unknown"
	}
	for _, c := range []json.Number{h.Fixture.ID, h.Game.ID, h.ID} {
		if s := strings.TrimSpace(c.String()); s != "" && s != "0" {
			return s
		}
	}
	return "unknown"
}

// Sports lists the pipeline sports this streamer produces.
func (p *ProductionStreamer) Sports() []generators.Sport {
	seen := map[generators.Sport]bool{}
	var out []generators.Sport
	for _, binds := range p.bindings {
		for _, b := range binds {
			s := generators.Sport(b.Sport)
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
	}
	return out
}

func (p *ProductionStreamer) Mode() Mode   { return ModeProduction }
func (p *ProductionStreamer) Close() error { return nil }

// Plans exposes the scheduler's current decisions, for the dashboard.
func (p *ProductionStreamer) Plans() []Plan { return p.scheduler.Plans() }
