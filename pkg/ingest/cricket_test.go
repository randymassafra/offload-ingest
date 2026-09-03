package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cricketprovider "github.com/offloadintelligence/offload-ingest/internal/provider/cricket"
	"github.com/offloadintelligence/offload-ingest/pkg/metrics"
)

// Compile-time proof the streamer fits the pipeline. If DataStreamer grows a
// method this fails here rather than at the MultiStreamer call site.
var _ DataStreamer = (*CricketStreamer)(nil)

// ---------------------------------------------------------------------------
// A fake Cricbuzz
// ---------------------------------------------------------------------------

type fakeCricbuzz struct {
	mu sync.Mutex
	// state is what the live list reports for the match below.
	state string
	// live controls whether the match appears in the live list at all.
	present bool
	matchID int

	listCalls int
	cardCalls int
	// cardStatus, when non-zero, is returned instead of a scorecard.
	cardStatus int
}

func (f *fakeCricbuzz) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()

		switch {
		case strings.HasPrefix(r.URL.Path, "/matches/v1/"):
			f.listCalls++
			if !f.present {
				_, _ = io.WriteString(w, `{"typeMatches":[]}`)
				return
			}
			_, _ = fmt.Fprintf(w, `{"typeMatches":[{"matchType":"International",
              "seriesMatches":[
                {"seriesAdWrapper":null},
                {"seriesAdWrapper":{"seriesId":3866,"seriesName":"Ireland tour of USA",
                  "matches":[{"matchInfo":{
                    "matchId":%d,"seriesId":3866,"seriesName":"Ireland tour of USA",
                    "matchDesc":"2nd T20I","matchFormat":"T20",
                    "startDate":"1788361200000","endDate":"1788371200000",
                    "state":%q,"status":"Live",
                    "team1":{"teamId":1,"teamName":"Ireland","teamSName":"IRE"},
                    "team2":{"teamId":2,"teamName":"USA","teamSName":"USA"}}}]}}]}]}`,
				f.matchID, f.state)

		case strings.HasSuffix(r.URL.Path, "/hscard"):
			f.cardCalls++
			if f.cardStatus != 0 {
				w.WriteHeader(f.cardStatus)
				return
			}
			// ismatchcomplete tracks the advertised state, as the real API
			// does: a completed match's card says so.
			complete := f.state == "Complete"
			_, _ = fmt.Fprintf(w, `{"scorecard":[{"inningsid":1,"score":150,"wickets":10,
              "overs":18.5,"batteamname":"Ireland",
              "batsman":[{"id":1114,"runs":5,"balls":5}],
              "bowler":[{"id":2001,"wickets":2}]}],
              "ismatchcomplete":%t,"status":"Ireland need 151"}`, complete)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

func (f *fakeCricbuzz) setState(s string) {
	f.mu.Lock()
	f.state = s
	f.mu.Unlock()
}

func (f *fakeCricbuzz) setPresent(v bool) {
	f.mu.Lock()
	f.present = v
	f.mu.Unlock()
}

func (f *fakeCricbuzz) counts() (list, card int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls, f.cardCalls
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// newCricketHarness wires the real streamer to a fake upstream.
func newCricketHarness(t *testing.T, state string) (*CricketStreamer, *fakeCricbuzz, *metrics.Registry, func(time.Duration)) {
	t.Helper()

	fake := &fakeCricbuzz{state: state, present: true, matchID: 40381}
	srv := httptest.NewServer(fake.handler())
	t.Cleanup(srv.Close)

	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		now = now.Add(d)
		mu.Unlock()
	}

	client := cricketprovider.New("test-key").Configure(
		cricketprovider.WithBaseURL(srv.URL),
		cricketprovider.WithCachePath(filepath.Join(t.TempDir(), "cache.json")),
		cricketprovider.WithClock(clock),
	)
	reg := metrics.NewRegistry(clock)
	s, err := NewCricketStreamer(CricketConfig{
		Client: client, Registry: reg, Logger: quietLogger(), Now: clock,
	})
	if err != nil {
		t.Fatalf("NewCricketStreamer: %v", err)
	}
	return s, fake, reg, advance
}

// ---------------------------------------------------------------------------
// The happy path
// ---------------------------------------------------------------------------

func TestCricketPublishesAScorecard(t *testing.T) {
	s, _, reg, _ := newCricketHarness(t, "In Progress")

	msgs, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Sport != "cricket" {
		t.Errorf("sport = %q", m.Sport)
	}
	if m.FixtureID != "40381" {
		t.Errorf("fixture = %q, want the match id", m.FixtureID)
	}
	if m.Model != "Scorecard" {
		t.Errorf("model = %q", m.Model)
	}
	// Health must see both a poll and data.
	if h := reg.Health(time.Hour); !h.OK {
		t.Errorf("health did not go green after a publish: %s", h.Detail)
	}
}

// A Cricbuzz scorecard carries no identity of its own, so the series must be
// supplied from discovery. Without this the Kafka routing headers would be
// empty and a downstream consumer could not tell one series from another.
func TestSeriesIdentityComesFromDiscovery(t *testing.T) {
	s, _, _, _ := newCricketHarness(t, "In Progress")

	msgs, err := s.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := msgs[0]
	if m.NormalizedLeagueID != 3866 {
		t.Errorf("league id = %d, want the series id 3866 from the live list", m.NormalizedLeagueID)
	}
	if m.LeagueName != "Ireland tour of USA" {
		t.Errorf("league name = %q", m.LeagueName)
	}
}

// The payload must be the provider's document, not an envelope of ours.
func TestPayloadIsTheProviderDocument(t *testing.T) {
	s, _, _, _ := newCricketHarness(t, "In Progress")
	msgs, err := s.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(msgs[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc["scorecard"]; !ok {
		t.Errorf("payload is not a scorecard document: %v", keysOfMap(doc))
	}
	for _, ours := range []string{"sport", "sequence", "normalized_league_id", "league_name"} {
		if _, leaked := doc[ours]; leaked {
			t.Errorf("envelope field %q leaked into the payload", ours)
		}
	}
}

func keysOfMap(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// ---------------------------------------------------------------------------
// Cadence
// ---------------------------------------------------------------------------

func TestCadenceFollowsMatchState(t *testing.T) {
	for _, tc := range []struct {
		state string
		mode  string
	}{
		{"In Progress", "live"},
		{"Innings Break", "break"},
		{"Stumps", "break"},
		{"Rain", "break"},
		{"Preview", "static"},
	} {
		s, _, _, _ := newCricketHarness(t, tc.state)
		if _, err := s.Next(context.Background()); err != nil {
			t.Fatalf("state %q: %v", tc.state, err)
		}
		if got := s.Cadence().Mode; got != tc.mode {
			t.Errorf("state %q produced cadence %q, want %q", tc.state, got, tc.mode)
		}
	}
}

// The break tier is the reason cricket needs four cadences where golf needs
// three. Innings breaks are long, and polling them at the live rate spends the
// day's budget on a document that cannot change.
func TestBreakIsSlowerThanLiveButFasterThanResting(t *testing.T) {
	live, _, _, _ := newCricketHarness(t, "In Progress")
	brk, _, _, _ := newCricketHarness(t, "Innings Break")
	idle, _, _, _ := newCricketHarness(t, "Preview")

	for _, s := range []*CricketStreamer{live, brk, idle} {
		if _, err := s.Next(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	l, b, i := live.Cadence().Interval, brk.Cadence().Interval, idle.Cadence().Interval
	if !(l < b && b < i) {
		t.Errorf("expected live < break < static, got %v, %v, %v", l, b, i)
	}
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

// Nothing in play is a quiet afternoon, not a failure. It must not error, must
// not publish, and must record a successful poll so the readiness probe can
// tell quiet from broken.
func TestNoLiveMatchIsQuietNotAnError(t *testing.T) {
	s, fake, reg, _ := newCricketHarness(t, "In Progress")
	fake.setPresent(false)

	msgs, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("an empty schedule must not be an error: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("published %d messages with nothing in play", len(msgs))
	}
	if s.Cadence().Mode != "static" {
		t.Errorf("cadence = %q, want static when nothing is on", s.Cadence().Mode)
	}
	h := reg.Health(time.Hour)
	if h.LastPoll.IsZero() {
		t.Error("a quiet poll was not recorded; health cannot distinguish quiet from broken")
	}
	if !h.LastData.IsZero() {
		t.Error("data was recorded when nothing was published")
	}
}

// A running match is retained rather than re-discovered, so the common case
// costs one request per poll instead of two.
func TestARunningMatchIsNotRediscovered(t *testing.T) {
	s, fake, _, advance := newCricketHarness(t, "In Progress")

	for i := 0; i < 3; i++ {
		if _, err := s.Next(context.Background()); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
		advance(time.Hour) // clear both the cadence gate and the cache TTL
	}
	list, card := fake.counts()
	if list != 1 {
		t.Errorf("discovery ran %d times over 3 polls; a running match should be retained", list)
	}
	if card != 3 {
		t.Errorf("scorecard fetched %d times, want 3", card)
	}
}

// The one that matters most. A completed match must be released, or the
// streamer polls a finished scorecard for the rest of the deployment — a feed
// that looks healthy on every metric and has been serving Tuesday's match
// since Tuesday.
func TestACompletedMatchIsReleasedAndRediscovered(t *testing.T) {
	s, fake, _, advance := newCricketHarness(t, "In Progress")

	if _, err := s.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	advance(time.Hour)

	fake.setState("Complete")
	if _, err := s.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	advance(time.Hour)

	// The next poll must go looking again rather than re-reading the finished
	// match it already had.
	before, _ := fake.counts()
	if _, err := s.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, _ := fake.counts()
	if after <= before {
		t.Error("a completed match was not released; discovery never ran again")
	}
}

// Through a rain delay the appliance should keep publishing the last state
// rather than going silent — a screen showing a stalled score is better than a
// blank one.
func TestAStartedButNotLiveMatchIsStillFollowed(t *testing.T) {
	s, _, _, _ := newCricketHarness(t, "Rain")

	msgs, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("a rain delay must not stop the feed: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages during a rain delay, want 1", len(msgs))
	}
}

// ---------------------------------------------------------------------------
// Pacing and failure
// ---------------------------------------------------------------------------

// Next must return promptly when nothing is due. The composite streamer polls
// its sources, and a source that blocked for its full interval would hold the
// whole fan-in hostage to the slowest cadence in the set.
func TestNextReturnsImmediatelyWhenNotDue(t *testing.T) {
	s, _, _, _ := newCricketHarness(t, "In Progress")
	if _, err := s.Next(context.Background()); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	msgs, err := s.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Error("published twice inside one interval")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Next blocked for %v when not due; it must return immediately", elapsed)
	}
}

// A 429 must publish the hard floor even though the fetch failed, because that
// error path skips applyCadence entirely. Reporting the floor only on success
// would leave the readiness probe green during exactly the outage it exists to
// report.
func TestRateLimitIsReportedOnTheFailurePath(t *testing.T) {
	s, fake, reg, _ := newCricketHarness(t, "In Progress")
	fake.mu.Lock()
	fake.cardStatus = http.StatusTooManyRequests
	fake.mu.Unlock()

	if _, err := s.Next(context.Background()); err == nil {
		t.Fatal("a 429 with no cache should surface an error")
	}
	h := reg.Health(time.Hour)
	if !h.RateLimited {
		t.Fatal("the hard floor was not published to the registry")
	}
	if len(h.RateLimitedProviders) == 0 || h.RateLimitedProviders[0] != "cricbuzz" {
		t.Errorf("rate-limited providers = %v, want [cricbuzz]", h.RateLimitedProviders)
	}
	if h.Status != metrics.StatusRateLimited {
		t.Errorf("health status = %q, want %q", h.Status, metrics.StatusRateLimited)
	}
}

func TestUpstreamFailureIsAnErrorNotAPanic(t *testing.T) {
	s, fake, _, _ := newCricketHarness(t, "In Progress")
	fake.mu.Lock()
	fake.cardStatus = http.StatusInternalServerError
	fake.mu.Unlock()

	if _, err := s.Next(context.Background()); err == nil {
		t.Error("a 500 should surface an error")
	}
}

func TestContextCancellationIsHonoured(t *testing.T) {
	s, _, _, _ := newCricketHarness(t, "In Progress")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Next(ctx); err == nil {
		t.Error("a cancelled context should abort")
	}
}

func TestStreamerRequiresAClient(t *testing.T) {
	if _, err := NewCricketStreamer(CricketConfig{}); err == nil {
		t.Error("a streamer with no client should be refused at construction")
	}
}

func TestSportsAndMode(t *testing.T) {
	s, _, _, _ := newCricketHarness(t, "In Progress")
	if got := s.Sports(); len(got) != 1 || got[0] != "cricket" {
		t.Errorf("Sports() = %v", got)
	}
	if s.Mode() != ModeProduction {
		t.Errorf("Mode() = %v, want production", s.Mode())
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close() = %v", err)
	}
}
