package ingest

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/scope"
	"github.com/offloadintelligence/offload-ingest/pkg/metrics"
)

// recordingSink captures what actually reached the producer.
type recordingSink struct {
	mu   sync.Mutex
	msgs []generators.Message
}

func (s *recordingSink) Publish(_ context.Context, msgs ...generators.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, msgs...)
	return nil
}
func (s *recordingSink) Close() error { return nil }
func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs)
}

func msg(t *testing.T, sport, fixture, payload string) generators.Message {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(payload), &v); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	// The envelope is normalised where a record enters the pipeline; the test
	// helper does the same so it exercises the real path.
	id := scope.Normalize(v)
	return generators.Message{
		Sport: generators.Sport(sport), FixtureID: fixture,
		Emitted:            time.Now(),
		NormalizedLeagueID: id.LeagueID,
		ProviderOrgID:      id.OrgID,
		LeagueName:         id.LeagueName,
		Payload:            v,
	}
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestOutOfScopeRecordsNeverReachKafka is the guarantee the whole control
// exists to provide.
func TestOutOfScopeRecordsNeverReachKafka(t *testing.T) {
	sink := &recordingSink{}
	reg := metrics.NewRegistry(time.Now)
	v := scope.New([]scope.AuthorizedScope{
		{Sport: "soccer", ID: 39, Source: "sport", Name: "Premier League"},
	})
	p := NewScopedPublisher(sink, v, reg, quiet())

	err := p.Publish(context.Background(),
		msg(t, "soccer", "1", `{"league":{"id":39,"name":"Premier League"}}`),
		msg(t, "soccer", "2", `{"league":{"id":135,"name":"Serie A"}}`),
		msg(t, "soccer", "3", `{"league":{"id":39,"name":"Premier League"}}`),
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := sink.count(); got != 2 {
		t.Fatalf("%d records reached the sink, want 2", got)
	}
	for _, m := range sink.msgs {
		if m.FixtureID == "2" {
			t.Error("the Serie A record was published")
		}
	}
}

// TestDropsAreCounted. Dropping rather than failing the sweep is only
// defensible because the drop is visible; silent filtering is its own failure.
func TestDropsAreCounted(t *testing.T) {
	sink := &recordingSink{}
	reg := metrics.NewRegistry(time.Now)
	v := scope.New([]scope.AuthorizedScope{{Sport: "soccer", ID: 39, Source: "sport"}})
	p := NewScopedPublisher(sink, v, reg, quiet())

	p.Publish(context.Background(),
		msg(t, "soccer", "1", `{"league":{"id":135}}`),
		msg(t, "soccer", "2", `{"league":{"id":140}}`),
		msg(t, "soccer", "3", `{"id":7}`), // no league at all
	)

	drops := reg.Drops()
	if len(drops) == 0 {
		t.Fatal("nothing was counted")
	}
	byReason := map[string]int64{}
	for _, d := range drops {
		if d.Sport != "soccer" {
			t.Errorf("unexpected sport %q", d.Sport)
		}
		byReason[d.Reason] = d.Count
	}
	if byReason["out_of_scope"] != 2 {
		t.Errorf("out_of_scope = %d, want 2", byReason["out_of_scope"])
	}
	// The modelling gap is counted separately from the licence mismatch.
	if byReason["unidentified"] != 1 {
		t.Errorf("unidentified = %d, want 1", byReason["unidentified"])
	}
}

// TestDropRateUsesPublishedPlusDropped. A sport whose entire feed is refused
// must not divide by zero and report a healthy 0%.
func TestDropRateUsesPublishedPlusDropped(t *testing.T) {
	sink := &recordingSink{}
	reg := metrics.NewRegistry(time.Now)
	v := scope.New([]scope.AuthorizedScope{{Sport: "soccer", ID: 39, Source: "sport"}})
	p := NewScopedPublisher(sink, v, reg, quiet())

	// Everything refused.
	for i := 0; i < 10; i++ {
		p.Publish(context.Background(), msg(t, "soccer", "x", `{"league":{"id":135}}`))
	}
	if rate := reg.DropRate("soccer"); rate != 1.0 {
		t.Errorf("drop rate = %v, want 1.0 when the whole feed is refused", rate)
	}

	// A healthy mix: 1 dropped in 40 is 2.5%, comfortably below the threshold.
	reg2 := metrics.NewRegistry(time.Now)
	p2 := NewScopedPublisher(&recordingSink{}, v, reg2, quiet())
	for i := 0; i < 39; i++ {
		p2.Publish(context.Background(), msg(t, "soccer", "x", `{"league":{"id":39}}`))
		reg2.Sport("soccer").Messages.Inc()
	}
	p2.Publish(context.Background(), msg(t, "soccer", "y", `{"league":{"id":135}}`))
	if rate := reg2.DropRate("soccer"); rate >= metrics.DropRateWarn {
		t.Errorf("drop rate = %v, want it below the %v warning threshold", rate, metrics.DropRateWarn)
	}
}

// TestDropRateThresholdIsInclusive pins the boundary, because "exceeds 5%" and
// "is at least 5%" differ by exactly the case an operator will hit first: one
// drop in twenty.
func TestDropRateThresholdIsInclusive(t *testing.T) {
	reg := metrics.NewRegistry(time.Now)
	v := scope.New([]scope.AuthorizedScope{{Sport: "soccer", ID: 39, Source: "sport"}})
	p := NewScopedPublisher(&recordingSink{}, v, reg, quiet())

	for i := 0; i < 19; i++ {
		p.Publish(context.Background(), msg(t, "soccer", "x", `{"league":{"id":39}}`))
		reg.Sport("soccer").Messages.Inc()
	}
	p.Publish(context.Background(), msg(t, "soccer", "y", `{"league":{"id":135}}`))

	rate := reg.DropRate("soccer")
	if rate != 0.05 {
		t.Fatalf("drop rate = %v, want exactly 0.05 (1 in 20)", rate)
	}
	if rate < metrics.DropRateWarn {
		t.Error("exactly 5% should warn; the threshold is inclusive")
	}
}

// TestNilValidatorPassesEverythingThrough keeps simulation working, where there
// is no upstream to be out of scope of.
func TestNilValidatorPassesEverythingThrough(t *testing.T) {
	sink := &recordingSink{}
	p := NewScopedPublisher(sink, nil, metrics.NewRegistry(time.Now), quiet())
	p.Publish(context.Background(),
		msg(t, "soccer", "1", `{"league":{"id":135}}`),
		msg(t, "cricket", "2", `{"anything":true}`),
	)
	if got := sink.count(); got != 2 {
		t.Errorf("%d records reached the sink, want both", got)
	}
}

// TestABatchThatIsEntirelyRefusedIsNotAnError: dropping is the designed
// outcome, so refusing every record in a batch must not look like a failure.
func TestABatchThatIsEntirelyRefusedIsNotAnError(t *testing.T) {
	sink := &recordingSink{}
	v := scope.New([]scope.AuthorizedScope{{Sport: "soccer", ID: 39, Source: "sport"}})
	p := NewScopedPublisher(sink, v, metrics.NewRegistry(time.Now), quiet())

	if err := p.Publish(context.Background(),
		msg(t, "soccer", "1", `{"league":{"id":135}}`)); err != nil {
		t.Errorf("an entirely-refused batch returned an error: %v", err)
	}
	if sink.count() != 0 {
		t.Error("something reached the sink")
	}
}

// TestScopeValidatorIsNilInSimulation.
func TestScopeValidatorIsNilInSimulation(t *testing.T) {
	r := &Runtime{mode: ModeSimulation}
	if v := r.ScopeValidator(); v != nil {
		t.Error("simulation should have no scope validator")
	}
}
