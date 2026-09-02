package ingest

import (
	"context"
	"encoding/json"
	golfprovider "github.com/offloadintelligence/offload-ingest/internal/provider/golf"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
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

// --- multi-source fan-in ------------------------------------------------------

// slowSource blocks until its interval elapses, like the API-Sports streamer,
// which blocks internally until one of its verticals is due.
type slowSource struct {
	delay time.Duration
	sport string
	calls int32
}

func (s *slowSource) Next(ctx context.Context) ([]generators.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(s.delay):
	}
	return []generators.Message{{Sport: generators.Sport(s.sport), FixtureID: "x"}}, nil
}
func (s *slowSource) Sports() []generators.Sport {
	return []generators.Sport{generators.Sport(s.sport)}
}
func (s *slowSource) Mode() Mode   { return ModeProduction }
func (s *slowSource) Close() error { return nil }

// TestFanInDoesNotStarveASlowerSource is the regression guard for the bug that
// made golf produce nothing at all.
//
// The first version polled sources in sequence. ProductionStreamer.Next blocks
// internally until a vertical is due, so a sequential loop never returned
// control and every source behind the first was starved. Golf was enabled,
// correctly configured, and silent for a whole run.
func TestFanInDoesNotStarveASlowerSource(t *testing.T) {
	blocking := &slowSource{delay: 2 * time.Second, sport: "soccer"}
	quick := &slowSource{delay: 10 * time.Millisecond, sport: "golf"}
	m := NewMultiStreamer(quiet(), blocking, quick)
	defer m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// The quick source must produce well before the blocking one would.
	start := time.Now()
	msgs, err := m.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatal("no messages")
	}
	if msgs[0].Sport != "golf" {
		t.Errorf("first batch came from %q; the quick source should not wait on the slow one", msgs[0].Sport)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %s; the quick source was blocked behind the slow one", elapsed)
	}
}

// TestFanInSurfacesEverySource: both sources must eventually be represented.
func TestFanInSurfacesEverySource(t *testing.T) {
	a := &slowSource{delay: 10 * time.Millisecond, sport: "soccer"}
	b := &slowSource{delay: 10 * time.Millisecond, sport: "golf"}
	m := NewMultiStreamer(quiet(), a, b)
	defer m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	seen := map[generators.Sport]bool{}
	for i := 0; i < 40 && len(seen) < 2; i++ {
		msgs, err := m.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		for _, msg := range msgs {
			seen[msg.Sport] = true
		}
	}
	if !seen["soccer"] || !seen["golf"] {
		t.Errorf("saw %v, want both sources represented", seen)
	}
}

// TestFanInIgnoresNilSources so a caller can pass an optional streamer without
// guarding it.
func TestFanInIgnoresNilSources(t *testing.T) {
	m := NewMultiStreamer(quiet(), nil, &slowSource{delay: time.Millisecond, sport: "golf"}, nil)
	defer m.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := m.Next(ctx); err != nil {
		t.Errorf("Next: %v", err)
	}
	if got := m.Sports(); len(got) != 1 {
		t.Errorf("Sports = %v, want just the one live source", got)
	}
}

// TestFanInStopsItsGoroutinesOnClose keeps a restart in the same process from
// leaking one worker per source.
func TestFanInStopsItsGoroutinesOnClose(t *testing.T) {
	before := runtime.NumGoroutine()
	m := NewMultiStreamer(quiet(),
		&slowSource{delay: 50 * time.Millisecond, sport: "soccer"},
		&slowSource{delay: 50 * time.Millisecond, sport: "golf"})

	ctx, cancel := context.WithCancel(context.Background())
	m.Next(ctx)
	cancel()
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("goroutines leaked: %d before, %d after Close", before, runtime.NumGoroutine())
}

func TestFanInWithNoSourcesIsAnError(t *testing.T) {
	if _, err := NewMultiStreamer(quiet()).Next(context.Background()); err == nil {
		t.Error("want an error with no sources configured")
	}
}

// --- golf cadence -------------------------------------------------------------

// TestCadenceFollowsTournamentState pins the three-state ladder.
func TestCadenceFollowsTournamentState(t *testing.T) {
	for _, tc := range []struct {
		name       string
		inProgress bool
		round      int
		wantMode   string
		wantTTL    time.Duration
	}{
		{"final round", true, 4, "final-round", golfprovider.FinalRoundCacheTTL},
		{"fifth round playoff", true, 5, "final-round", golfprovider.FinalRoundCacheTTL},
		{"second round", true, 2, "live", golfprovider.LiveCacheTTL},
		{"first round", true, 1, "live", golfprovider.LiveCacheTTL},
		{"completed", false, 4, "static", golfprovider.CacheTTL},
		{"not started", false, 0, "static", golfprovider.CacheTTL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := cadenceFor(tc.inProgress, tc.round)
			if got.Mode != tc.wantMode {
				t.Errorf("mode = %q, want %q", got.Mode, tc.wantMode)
			}
			if got.TTL != tc.wantTTL {
				t.Errorf("TTL = %s, want %s", got.TTL, tc.wantTTL)
			}
			// The poll interval must sit just under the cache lifetime, or the
			// loop either spins on an unexpired cache or lets it go stale.
			if got.Interval >= got.TTL {
				t.Errorf("interval %s is not shorter than the TTL %s", got.Interval, got.TTL)
			}
			if got.Reason == "" {
				t.Error("a cadence must explain itself for the switch log")
			}
		})
	}
}

// TestFinalRoundIsFasterThanLiveIsFasterThanStatic — the ordering is the whole
// point of the feature.
func TestCadenceOrdering(t *testing.T) {
	final := cadenceFor(true, 4)
	live := cadenceFor(true, 2)
	static := cadenceFor(false, 0)
	if !(final.Interval < live.Interval && live.Interval < static.Interval) {
		t.Errorf("intervals out of order: final=%s live=%s static=%s",
			final.Interval, live.Interval, static.Interval)
	}
}

// TestThrottleOverridesTheCadence is the guardrail seen from the streamer's
// side: however live the tournament, a throttled provider must be polled at the
// resting rate or the loop spins against a cache that is not expiring.
func TestThrottleOverridesTheCadence(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	client := golfprovider.New("k").Configure(
		golfprovider.WithBaseURL(srv.URL),
		golfprovider.WithCachePath(filepath.Join(t.TempDir(), "c.json")),
		golfprovider.WithClock(func() time.Time { return clock }),
	)
	reg := metrics.NewRegistry(func() time.Time { return clock })
	g, err := NewGolfStreamer(GolfConfig{
		Client: client, Registry: reg, Logger: quiet(),
		Now: func() time.Time { return clock }, TournID: "033", Year: "2023",
	})
	if err != nil {
		t.Fatalf("NewGolfStreamer: %v", err)
	}

	// Trip the floor.
	client.Leaderboard(context.Background(), golfprovider.LeaderboardRequest{
		OrgID: "1", TournID: "033", Year: "2023",
	})
	if throttled, _ := client.Throttled(); !throttled {
		t.Fatal("the floor did not engage")
	}

	// Even mid-final-round, the cadence must be the resting one.
	g.applyCadence(&golfprovider.Leaderboard{RoundID: 4})
	got := g.Cadence()
	if got.Mode != "throttled" {
		t.Errorf("mode = %q, want throttled", got.Mode)
	}
	if got.Interval != GolfPollInterval {
		t.Errorf("interval = %s, want the resting %s", got.Interval, GolfPollInterval)
	}
	if !reg.Golf.Throttled.Value() {
		t.Error("the throttled flag was not reported to metrics")
	}
	if got := reg.Golf.CadenceMinutes.Value(); got != GolfPollInterval.Minutes() {
		t.Errorf("cadence gauge = %v, want %v", got, GolfPollInterval.Minutes())
	}
}

// TestCadenceGaugeTracksTheMode, so the dashboard shows the real polling rate.
func TestCadenceGaugeTracksTheMode(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	reg := metrics.NewRegistry(func() time.Time { return clock })
	client := golfprovider.New("k").Configure(
		golfprovider.WithCachePath(filepath.Join(t.TempDir(), "c.json")),
		golfprovider.WithClock(func() time.Time { return clock }),
	)
	g, _ := NewGolfStreamer(GolfConfig{
		Client: client, Registry: reg, Logger: quiet(),
		Now: func() time.Time { return clock }, TournID: "033", Year: "2023",
	})

	// Not in progress: resting.
	g.applyCadence(&golfprovider.Leaderboard{RoundID: 4})
	if got := reg.Golf.CadenceMinutes.Value(); got != GolfPollInterval.Minutes() {
		t.Errorf("static gauge = %v, want %v", got, GolfPollInterval.Minutes())
	}
	if client.EffectiveTTL() != golfprovider.CacheTTL {
		t.Errorf("provider TTL = %s, want the resting %s", client.EffectiveTTL(), golfprovider.CacheTTL)
	}

	// Put the tournament in progress and re-evaluate.
	var e golfprovider.ScheduleEntry
	e.Date.Start = golfprovider.MongoDate{Time: clock.Add(-48 * time.Hour)}
	e.Date.End = golfprovider.MongoDate{Time: clock.Add(24 * time.Hour)}
	g.mu.Lock()
	g.entry = e
	g.mu.Unlock()

	g.applyCadence(&golfprovider.Leaderboard{RoundID: 4})
	if g.Cadence().Mode != "final-round" {
		t.Fatalf("mode = %q, want final-round", g.Cadence().Mode)
	}
	if client.EffectiveTTL() != golfprovider.FinalRoundCacheTTL {
		t.Errorf("provider TTL = %s, want %s", client.EffectiveTTL(), golfprovider.FinalRoundCacheTTL)
	}
	if got := reg.Golf.CadenceMinutes.Value(); got >= GolfPollInterval.Minutes() {
		t.Errorf("gauge = %v, should have dropped below the resting rate", got)
	}
}
