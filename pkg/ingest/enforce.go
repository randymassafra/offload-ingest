package ingest

import (
	"context"
	"log/slog"

	"github.com/offloadintelligence/offload-ingest/config"
	"github.com/offloadintelligence/offload-ingest/internal/generators"
	"github.com/offloadintelligence/offload-ingest/internal/producer"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/scope"
	"github.com/offloadintelligence/offload-ingest/pkg/metrics"
)

// ScopeValidator builds the licence's league entitlements from the runtime's
// resolved bindings.
//
// The bindings already carry the league ids the licence unlocks — they were
// resolved from apisports.Entitled at startup and used to shape requests. This
// turns the same set into the post-fetch check, so the two controls cannot
// disagree about what a venue bought.
//
// Returns nil in simulation, where there is no upstream to be out of scope of.
// A nil validator authorises everything, so callers need no special case.
func (r *Runtime) ScopeValidator() *scope.Validator {
	if r.mode != ModeProduction {
		return nil
	}
	claims := r.validator.Claims()
	bindings := apisports.Entitled(claims.Sports, claims.Regions)
	if len(bindings) == 0 {
		return nil
	}
	authorized, unconstrained := config.AuthorizedScopesFor(bindings, claims.Regions)
	return scope.New(authorized, unconstrained...)
}

// ScopedPublisher wraps a sink and refuses records outside the licence.
//
// This is the authoritative enforcement point: it sits between the streamer and
// the Kafka producer, so nothing reaches a topic without passing. The providers
// stay dumb — they return what they fetched and know nothing about
// entitlements, which keeps the rule in one place instead of four.
//
// Wrapping the Publisher rather than adding a check to each call site means a
// future emitter cannot forget to enforce: the only way to reach Kafka is
// through this.
type ScopedPublisher struct {
	sink      producer.Publisher
	validator *scope.Validator
	registry  *metrics.Registry
	log       *slog.Logger
	// warned tracks which sports have already logged a drop, so a persistent
	// mismatch produces one line rather than one per record for the rest of
	// the evening.
	warned map[string]bool
}

// NewScopedPublisher wraps a sink with scope enforcement.
func NewScopedPublisher(sink producer.Publisher, v *scope.Validator, reg *metrics.Registry, log *slog.Logger) *ScopedPublisher {
	if log == nil {
		log = slog.Default()
	}
	return &ScopedPublisher{
		sink: sink, validator: v, registry: reg, log: log,
		warned: map[string]bool{},
	}
}

// Publish forwards only the records the licence authorises.
//
// Dropped records are counted before anything else happens to them. The whole
// argument for dropping rather than failing the sweep is that one out-of-scope
// fixture should not take a sport off the air — but that trade is only
// defensible if the drop is visible, so the metric is not optional.
func (p *ScopedPublisher) Publish(ctx context.Context, msgs ...generators.Message) error {
	if p.validator == nil {
		return p.sink.Publish(ctx, msgs...)
	}
	allowed := make([]generators.Message, 0, len(msgs))
	for _, m := range msgs {
		sport := string(m.Sport)
		// The validator reads the normalised envelope, not the payload: the
		// identity was resolved once upstream and the payload is untouched.
		res := p.validator.Check(scope.Envelope{
			Sport:      sport,
			LeagueID:   m.NormalizedLeagueID,
			OrgID:      m.ProviderOrgID,
			LeagueName: m.LeagueName,
			FixtureID:  m.FixtureID,
		})
		if res.Authorized {
			allowed = append(allowed, m)
			continue
		}
		if p.registry != nil {
			p.registry.RecordDrop(sport, string(res.Reason))
		}
		if !p.warned[sport] {
			p.warned[sport] = true
			p.log.Warn("ingest: dropping out-of-scope record",
				"sport", sport, "reason", string(res.Reason),
				"detail", res.Detail, "fixture", m.FixtureID)
		}
	}
	if len(allowed) == 0 {
		return nil
	}
	return p.sink.Publish(ctx, allowed...)
}

// Close closes the wrapped sink.
func (p *ScopedPublisher) Close() error { return p.sink.Close() }

// Stats forwards the wrapped sink's counters when it reports any, so wrapping
// does not blind the progress reporter.
func (p *ScopedPublisher) Stats() producer.Stats {
	if s, ok := p.sink.(producer.StatsReporter); ok {
		return s.Stats()
	}
	return producer.Stats{}
}
