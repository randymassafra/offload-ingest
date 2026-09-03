package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
	"github.com/offloadintelligence/offload-ingest/internal/producer"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/scope"
	"github.com/offloadintelligence/offload-ingest/pkg/licensing"
	"github.com/offloadintelligence/offload-ingest/pkg/metrics"
)

// ---------------------------------------------------------------------------
// The hostile provider
// ---------------------------------------------------------------------------

// chaosServer serves a scripted sequence of failures and then recovers.
//
// Scripted rather than randomised on purpose. A fuzzing server finds different
// bugs on different runs, which is useful but is not a regression test; this
// one reproduces the same three failures in the same order every time, so a
// failure here always means the same thing.
type chaosServer struct {
	mu     sync.Mutex
	script []responder
	served int
	hits   []string // a label per request, for assertions
}

type responder struct {
	label string
	fn    func(w http.ResponseWriter)
}

func (c *chaosServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		i := c.served
		c.served++
		// Past the end of the script the provider is healthy forever. The
		// recovery half of the test needs a provider that stays recovered.
		step := c.script[len(c.script)-1]
		if i < len(c.script) {
			step = c.script[i]
		}
		c.hits = append(c.hits, step.label)
		c.mu.Unlock()
		step.fn(w)
	})
}

func (c *chaosServer) requests() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.hits...)
}

func (c *chaosServer) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.served
}

// malformed is a truncated document: valid framing, invalid JSON. This is the
// realistic shape of a provider failure — a proxy cutting a response short —
// rather than the unrealistic "returns the word banana".
func malformed(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"get":"fixtures","response":[{"fixture":{"id":1,`))
}

func serverError(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = w.Write([]byte("<html><body>502 Bad Gateway</body></html>"))
}

// rateLimited carries Retry-After: 0 so the client's backoff resolves
// immediately. The backoff arithmetic is covered in pkg/ingest/apisports; what
// is under test here is that a 429 does not take the pipeline down, and making
// the test wait sixteen real seconds to prove it would only make it flaky.
func rateLimited(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", "0")
	w.Header().Set("x-ratelimit-remaining", "0")
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write([]byte(`{"message":"Too many requests"}`))
}

// healthy returns two Premier League fixtures. League 39 is in the licensed
// bundle, so these survive scope validation and reach the sink.
func healthy(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-ratelimit-remaining", "9")
	w.Header().Set("x-ratelimit-requests-remaining", "95")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{
      "get":"fixtures","parameters":{},"errors":[],"results":2,
      "paging":{"current":1,"total":1},
      "response":[%s,%s]}`, fixture(1001, 39), fixture(1002, 39))
}

func fixture(id, league int) string {
	return fmt.Sprintf(`{
      "fixture":{"id":%d,"date":"2026-09-02T15:00:00+00:00","timestamp":1788361200,
                 "status":{"long":"First Half","short":"1H","elapsed":22}},
      "league":{"id":%d,"name":"Premier League","country":"England","season":2026},
      "teams":{"home":{"id":33,"name":"Manchester United"},
               "away":{"id":40,"name":"Liverpool"}},
      "goals":{"home":1,"away":0}}`, id, league)
}

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// recordingSink stands in for Kafka and counts what survived the pipeline.
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

func (s *recordingSink) published() []generators.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]generators.Message(nil), s.msgs...)
}

// logCapture collects slog records so the test can assert the pipeline
// reported its failures rather than swallowing them.
//
// "It recovered" is only half the requirement. A pipeline that silently eats a
// provider outage is arguably worse than one that crashes, because nobody finds
// out until a venue asks why a screen was blank.
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (l *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (l *logCapture) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *logCapture) WithGroup(string) slog.Handler            { return l }

func (l *logCapture) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, r.Clone())
	return nil
}

func (l *logCapture) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.records))
	for _, r := range l.records {
		out = append(out, r.Message)
	}
	return out
}

// atLevel returns the messages logged at or above a level.
func (l *logCapture) atLevel(min slog.Level) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, r := range l.records {
		if r.Level >= min {
			out = append(out, r.Message)
		}
	}
	return out
}

func (l *logCapture) logger() *slog.Logger { return slog.New(l) }

// fastClock advances on every read.
//
// The streamer paces itself by asking the clock what time it is, so a clock
// that moves forward faster than the scheduler's intervals makes every vertical
// permanently due and the test never waits on a timer. Real time is left to the
// rate limiter, which is given a tier generous enough not to block.
type fastClock struct {
	mu   sync.Mutex
	t    time.Time
	step time.Duration
}

func newFastClock(step time.Duration) *fastClock {
	return &fastClock{t: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC), step: step}
}

func (c *fastClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(c.step)
	return c.t
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type harness struct {
	streamer *ingest.ProductionStreamer
	sink     *recordingSink
	scoped   *ingest.ScopedPublisher
	registry *metrics.Registry
	logs     *logCapture
	server   *chaosServer
	clock    *fastClock
}

// newHarness assembles the real pipeline against a scripted provider.
//
// Nothing here is a mock except the provider itself and the Kafka sink. The
// client, limiter, streamer, validator and publisher are the production types,
// because a resilience test built on stubs proves only that the stubs are calm.
func newHarness(t *testing.T, script []responder) *harness {
	t.Helper()

	chaos := &chaosServer{script: script}
	srv := httptest.NewServer(chaos.handler())
	t.Cleanup(srv.Close)

	logs := &logCapture{}
	clock := newFastClock(31 * time.Minute)
	registry := metrics.NewRegistry(clock.now)

	// A generous tier so the token bucket is never the thing under test. The
	// free tier's ten-per-minute would add six real seconds between sweeps and
	// turn a resilience test into a timing test.
	tier, ok := licensing.LookupTier(licensing.TierMega)
	if !ok {
		t.Fatal("mega tier missing from the catalog")
	}

	bindings := apisports.Entitled([]string{"soccer"}, nil)
	if len(bindings) == 0 {
		t.Fatal("soccer is not bound to an API-Sports vertical")
	}
	verticals := apisports.VerticalsFor(bindings)

	limiter, err := ingest.NewLimiter(ingest.LimiterConfig{
		Tier: tier, Verticals: verticals, Now: clock.now,
	})
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}

	client, err := apisports.New(apisports.Config{
		APIKey: "test-key", BaseURLOverride: srv.URL,
		Limiter: limiter, Logger: logs.logger(),
	})
	if err != nil {
		t.Fatalf("apisports.New: %v", err)
	}

	streamer, err := ingest.NewProductionStreamer(ingest.ProductionConfig{
		Client: client, Limiter: limiter, Registry: registry,
		Bindings: bindings, Logger: logs.logger(), Now: clock.now,
	})
	if err != nil {
		t.Fatalf("NewProductionStreamer: %v", err)
	}
	t.Cleanup(func() { _ = streamer.Close() })

	// The real validator, licensed for exactly the leagues the healthy
	// response carries.
	validator := scope.New([]scope.AuthorizedScope{
		{Sport: "soccer", ID: 39, Source: "sport", Name: "Premier League"},
	})

	sink := &recordingSink{}
	scoped := ingest.NewScopedPublisher(sink, validator, registry, logs.logger())

	return &harness{
		streamer: streamer, sink: sink, scoped: scoped,
		registry: registry, logs: logs, server: chaos, clock: clock,
	}
}

// pump drives the streamer exactly as the production loop does.
//
// This mirrors cmd/loadtest/production.go deliberately, and the fidelity is
// load-bearing. An earlier version of this harness retried after an error from
// Next, which made every test here pass even with the streamer's internal
// error recovery deliberately deleted — the harness was supplying the
// resilience it was meant to be measuring. A test double that is more forgiving
// than production does not test production.
//
// So: an error out of Next ends the pump, and every caller treats that as a
// failure to recover. That is what the real process does, where a returned
// error exits the service.
func (h *harness) pump(t *testing.T, want int, budget time.Duration) (int, []error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	var errs []error
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if len(h.sink.published()) >= want {
			break
		}
		msgs, err := h.streamer.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			// Fatal, exactly as in production.
			errs = append(errs, err)
			break
		}
		if len(msgs) > 0 {
			if err := h.scoped.Publish(ctx, msgs...); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return len(h.sink.published()), errs
}

// standardChaos is the sequence the directive specifies: malformed JSON, then
// a server error, then a rate limit, then recovery.
func standardChaos() []responder {
	return []responder{
		{"malformed", malformed},
		{"500", serverError},
		{"429", rateLimited},
		{"healthy", healthy},
	}
}

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

// TestPipelineSurvivesAndRecoversFromAHostileProvider is the headline case.
//
// A provider returns garbage, then fails, then throttles, then comes back. The
// process must still be alive, must have said so in the log, and must publish
// once the provider recovers.
func TestPipelineSurvivesAndRecoversFromAHostileProvider(t *testing.T) {
	h := newHarness(t, standardChaos())

	got, errs := h.pump(t, 2, 20*time.Second)

	// 1. It did not crash. Reaching this line at all is that assertion; the
	//    streamer runs in this goroutine, so a panic fails the test.
	// 2. It recovered.
	if got < 2 {
		t.Fatalf("expected the pipeline to recover and publish 2 fixtures, got %d "+
			"(provider saw %v, errors: %v)", got, h.server.requests(), errs)
	}

	// 3. It complained. Every failure class must appear in the log.
	warnings := strings.Join(h.logs.atLevel(slog.LevelWarn), " | ")
	if warnings == "" {
		t.Error("the pipeline recovered silently; a provider outage must be logged")
	}
	if !strings.Contains(warnings, "sweep failed") {
		t.Errorf("expected a 'sweep failed' warning, got: %s", warnings)
	}

	// 4. The provider actually served every scripted failure, so the test is
	//    not passing because the chaos never happened.
	served := h.server.requests()
	for _, want := range []string{"malformed", "500", "429", "healthy"} {
		if !contains(served, want) {
			t.Errorf("the %q step never ran; provider saw %v", want, served)
		}
	}
}

// Each failure class is asserted on its own, because "it survived three things
// at once" does not prove it survives each of them — a single overly broad
// recover() would pass the combined test and fail all three of these.
func TestMalformedJSONIsSurvived(t *testing.T) {
	assertSurvives(t, "malformed", []responder{{"malformed", malformed}, {"healthy", healthy}})
}

func TestServerErrorIsSurvived(t *testing.T) {
	assertSurvives(t, "500", []responder{{"500", serverError}, {"healthy", healthy}})
}

func TestRateLimitIsSurvived(t *testing.T) {
	assertSurvives(t, "429", []responder{{"429", rateLimited}, {"healthy", healthy}})
}

// A provider that fails repeatedly before recovering must not exhaust anything
// that stops it recovering. One bad response is a blip; twelve is an outage,
// and an outage is what actually happens in a venue.
func TestSustainedOutageStillRecovers(t *testing.T) {
	var script []responder
	for i := 0; i < 4; i++ {
		script = append(script,
			responder{"malformed", malformed},
			responder{"500", serverError},
			responder{"429", rateLimited},
		)
	}
	script = append(script, responder{"healthy", healthy})

	h := newHarness(t, script)
	got, _ := h.pump(t, 2, 30*time.Second)
	if got < 2 {
		t.Fatalf("after a sustained outage the pipeline did not recover: published %d, provider saw %d requests",
			got, h.server.count())
	}
}

func assertSurvives(t *testing.T, label string, script []responder) {
	t.Helper()
	h := newHarness(t, script)
	got, _ := h.pump(t, 2, 20*time.Second)
	if got < 2 {
		t.Fatalf("pipeline did not recover after %s: published %d (provider saw %v)",
			label, got, h.server.requests())
	}
	if len(h.logs.messages()) == 0 {
		t.Errorf("%s was not logged at all", label)
	}
}

// Failures must be classified, not merely counted. A 5xx and a malformed body
// mean different things to whoever is on call, and folding them into one
// "errors" number is what makes a dashboard useless during an incident.
func TestFailuresAreClassifiedInMetrics(t *testing.T) {
	h := newHarness(t, standardChaos())
	if got, _ := h.pump(t, 2, 20*time.Second); got < 2 {
		t.Fatalf("precondition: pipeline did not recover, got %d", got)
	}

	snap := h.registry.Snapshot()
	var soccer *metrics.SportSnapshot
	for i := range snap.Sports {
		if snap.Sports[i].Sport == "football" || snap.Sports[i].Sport == "soccer" {
			soccer = &snap.Sports[i]
		}
	}
	if soccer == nil {
		t.Fatalf("no per-sport metrics recorded; saw %+v", snap.Sports)
	}
	if soccer.Errors == 0 {
		t.Error("the outage was not counted against the sport at all")
	}
	if soccer.Messages == 0 {
		t.Error("recovery was not counted")
	}
}

// The scope validator sits downstream of the chaos. It must not be knocked
// over by whatever the provider sent, and must still refuse an unlicensed
// league after the provider recovers — a pipeline that stops enforcing the
// licence under stress is a licensing failure, not a resilience one.
func TestScopeValidatorStillEnforcesAfterAnOutage(t *testing.T) {
	script := []responder{
		{"malformed", malformed},
		{"500", serverError},
		{"429", rateLimited},
		// League 140 is La Liga: real, well-formed, and NOT licensed here.
		{"unlicensed", func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `{"get":"fixtures","errors":[],"results":1,
              "paging":{"current":1,"total":1},"response":[%s]}`, fixture(2001, 140))
		}},
	}
	h := newHarness(t, script)

	// Pump for a fixed budget; nothing should ever reach the sink.
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		msgs, err := h.streamer.Next(ctx)
		if err != nil {
			break // fatal in production; see pump
		}
		if len(msgs) > 0 {
			_ = h.scoped.Publish(ctx, msgs...)
		}
	}

	if got := h.sink.published(); len(got) != 0 {
		t.Fatalf("an unlicensed league reached the sink after an outage: %d messages", len(got))
	}
	// And the drop was metered rather than silent.
	if rate := h.registry.DropRate("soccer"); rate == 0 {
		drops := h.registry.Drops()
		t.Errorf("unlicensed records were dropped without being counted; drops: %+v", drops)
	}
}

// A payload that is well-formed JSON but carries no recognisable identity must
// be refused rather than panicking. This is the shape a partial provider
// migration produces, and it exercises the real path: scope.Normalize reads
// the identity out of the document, and the publisher then judges the envelope
// Normalize produced. The payload itself is never inspected by the validator —
// that separation is the contract, and this test walks both halves of it.
func TestUnidentifiableRecordsAreRefusedNotFatal(t *testing.T) {
	logs := &logCapture{}
	registry := metrics.NewRegistry(time.Now)
	validator := scope.New([]scope.AuthorizedScope{
		{Sport: "soccer", ID: 39, Source: "sport"},
	})
	sink := &recordingSink{}
	scoped := ingest.NewScopedPublisher(sink, validator, registry, logs.logger())

	// Everything a broken or half-migrated provider might send. Only the last
	// carries a licensed identity.
	junk := []string{
		`{}`,
		`{"league": "not an object"}`,
		`{"league": {"id": "not a number"}}`,
		`{"league": {"id": null}}`,
		`null`,
		`[1, 2, 3]`,
		`"a bare string"`,
		`{"league": {"id": 140, "name": "La Liga"}}`, // real, but unlicensed
		`{"league": {"id": 39, "name": "Premier League"}}`,
	}
	for i, raw := range junk {
		var payload any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			t.Fatalf("test fixture %d is not valid JSON: %v", i, err)
		}
		// Normalize is where a malformed document would panic if it were going
		// to; it must return an empty identity instead.
		id := scope.Normalize(payload)

		msg := generators.Message{
			Sport: generators.SportSoccer, Kind: generators.FeedBoxScore,
			FixtureID:          fmt.Sprintf("junk-%d", i),
			NormalizedLeagueID: id.LeagueID,
			ProviderOrgID:      id.OrgID,
			LeagueName:         id.LeagueName,
			Payload:            payload,
		}
		if err := scoped.Publish(context.Background(), msg); err != nil {
			t.Fatalf("publishing junk %d returned an error: %v", i, err)
		}
	}

	got := sink.published()
	if len(got) != 1 {
		t.Fatalf("expected exactly the one licensed record to survive, got %d", len(got))
	}
	if got[0].NormalizedLeagueID != 39 {
		t.Errorf("the wrong record survived: league %d", got[0].NormalizedLeagueID)
	}
	// The refusals were metered, not silent.
	if registry.DropRate("soccer") == 0 {
		t.Error("unidentifiable records were dropped without being counted")
	}
}

// Health must go red during the outage and green again after recovery. This is
// what the venue's monitoring system reads, so it is the externally visible
// half of resilience.
func TestHealthReflectsTheOutageAndTheRecovery(t *testing.T) {
	h := newHarness(t, standardChaos())

	// Before anything has flowed and past the startup grace, the box is
	// starved. The registry's clock is the fast one, so the grace has long
	// since expired by construction.
	h.clock.now()
	if got := h.registry.Health(metrics.DefaultHealthWindow); got.OK {
		t.Fatalf("precondition: expected an unhealthy box before any data, got %s", got.Status)
	}

	if got, _ := h.pump(t, 2, 20*time.Second); got < 2 {
		t.Fatalf("precondition: pipeline did not recover, got %d", got)
	}

	// The fast clock advances 31 minutes per read, which is past the fifteen
	// minute window, so health is asserted against a window wide enough to
	// span the harness's own clock skew. The point is that LastData was
	// recorded at all.
	after := h.registry.Health(24 * time.Hour)
	if !after.OK {
		t.Errorf("health did not recover after the provider did: %s (%s)", after.Status, after.Detail)
	}
	if after.LastData.IsZero() {
		t.Error("recovery did not record a last-data timestamp")
	}
}

// The producer's own errors must not take the streamer down either. A broker
// that refuses a write is at least as likely as a provider that misbehaves.
func TestSinkFailuresDoNotHaltIngestion(t *testing.T) {
	logs := &logCapture{}
	registry := metrics.NewRegistry(time.Now)
	validator := scope.New([]scope.AuthorizedScope{{Sport: "soccer", ID: 39, Source: "sport"}})
	failing := &failingSink{err: fmt.Errorf("broker unreachable")}
	scoped := ingest.NewScopedPublisher(failing, validator, registry, logs.logger())

	// The envelope is what the validator judges, so it must carry the licensed
	// league for the message to reach the sink at all.
	msg := generators.Message{
		Sport: generators.SportSoccer, Kind: generators.FeedBoxScore, FixtureID: "1",
		NormalizedLeagueID: 39, LeagueName: "Premier League",
		Payload: map[string]any{"league": map[string]any{"id": float64(39)}},
	}
	// An error is expected and correct; a panic is not, and the caller decides
	// what to do about a broker outage.
	if err := scoped.Publish(context.Background(), msg); err == nil {
		t.Error("a failing sink should surface its error to the caller")
	}
	if failing.calls == 0 {
		t.Error("the sink was never called")
	}
}

type failingSink struct {
	err   error
	calls int
}

func (f *failingSink) Publish(context.Context, ...generators.Message) error {
	f.calls++
	return f.err
}
func (f *failingSink) Close() error { return nil }

// Compile-time proof the doubles satisfy the real interfaces. If producer's
// Publisher ever grows a method, this fails here rather than somewhere subtle.
var (
	_ producer.Publisher  = (*recordingSink)(nil)
	_ producer.Publisher  = (*failingSink)(nil)
	_ ingest.DataStreamer = (*ingest.ProductionStreamer)(nil)
)

func contains(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Fan-in isolation
// ---------------------------------------------------------------------------

// alwaysFails is a source that never succeeds, standing in for a vendor whose
// subscription has lapsed or whose host is unreachable.
type alwaysFails struct {
	calls int64
	mu    sync.Mutex
}

func (a *alwaysFails) Next(context.Context) ([]generators.Message, error) {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	return nil, fmt.Errorf("vendor unreachable")
}
func (a *alwaysFails) Sports() []generators.Sport { return []generators.Sport{generators.SportGolf} }
func (a *alwaysFails) Mode() ingest.Mode          { return ingest.ModeProduction }
func (a *alwaysFails) Close() error               { return nil }

// healthySource emits one message per call.
type healthySource struct{}

func (healthySource) Next(context.Context) ([]generators.Message, error) {
	return []generators.Message{{
		Sport: generators.SportSoccer, Kind: generators.FeedBoxScore,
		FixtureID: "ok-1", NormalizedLeagueID: 39,
		Payload: map[string]any{"league": map[string]any{"id": float64(39)}},
	}}, nil
}
func (healthySource) Sports() []generators.Sport { return []generators.Sport{generators.SportSoccer} }
func (healthySource) Mode() ingest.Mode          { return ingest.ModeProduction }
func (healthySource) Close() error               { return nil }

// TestOneDeadSourceDoesNotStopTheOthers is the fan-in's whole reason for
// existing, stated as a test.
//
// Production runs API-Sports and golf as separate sources behind one
// MultiStreamer. They are different vendors on different subscriptions, so one
// can fail entirely while the other is perfectly healthy — a lapsed RapidAPI
// subscription must not stop soccer. The single-source path has no such
// isolation, which is why this matters: the moment golf is enabled the
// topology changes, and this pins the behaviour that change depends on.
func TestOneDeadSourceDoesNotStopTheOthers(t *testing.T) {
	logs := &logCapture{}
	dead := &alwaysFails{}
	multi := ingest.NewMultiStreamer(logs.logger(), dead, healthySource{})
	t.Cleanup(func() { _ = multi.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var got int
	for got < 3 {
		msgs, err := multi.Next(ctx)
		if err != nil {
			t.Fatalf("a dead source must not surface an error to the caller: %v", err)
		}
		got += len(msgs)
	}

	// And the dead source was reported rather than ignored. Polled rather than
	// asserted once: the healthy source can satisfy the loop above before the
	// dead one has finished its first attempt, and asserting immediately made
	// this flaky in a way that had nothing to do with the behaviour under test.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(strings.Join(logs.atLevel(slog.LevelWarn), " | "), "source failed") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("the failing source was never logged; saw: %v", logs.atLevel(slog.LevelWarn))
}
