package cricket

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func capture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "fixtures", "cricbuzz", name))
	if err != nil {
		t.Skipf("no capture on disk; run `make capture` to enable this check: %v", err)
	}
	return b
}

// ---------------------------------------------------------------------------
// The captures. These are the tests that matter.
// ---------------------------------------------------------------------------

// TestCapturedMatchListDecodes runs the real /matches/v1/live response through
// the model.
//
// It asserts ids are non-zero rather than merely that the document parsed,
// because a structural mistake in the four-level nesting walk — the wrong
// wrapper, a missed level — yields an empty or zero-filled result rather than
// an error. "It decoded" is not evidence that it decoded the right thing.
func TestCapturedMatchListDecodes(t *testing.T) {
	var list MatchList
	if err := json.Unmarshal(capture(t, "matches_live.json"), &list); err != nil {
		t.Fatalf("the captured match list does not decode: %v", err)
	}

	matches := list.Matches()
	if len(matches) == 0 {
		t.Fatal("flattening produced no matches; the four-level nesting walk is wrong")
	}
	for i, m := range matches {
		if m.MatchID == 0 {
			t.Errorf("match %d decoded with id 0; the nesting walk found the wrong object", i)
		}
		if m.State == "" {
			t.Errorf("match %d (%d) decoded with no state", i, m.MatchID)
		}
	}
}

// TestCapturedScorecardDecodes is the same check for the lowercase half.
func TestCapturedScorecardDecodes(t *testing.T) {
	sc, err := decodeScorecard(capture(t, "scorecard.json"))
	if err != nil {
		t.Fatalf("the captured scorecard does not decode: %v", err)
	}
	if len(sc.Scorecard) == 0 {
		t.Fatal("no innings decoded from a real scorecard")
	}
	first := sc.Scorecard[0]
	if len(first.Batsman) == 0 {
		t.Error("innings 1 has no batsmen; a nested field is not being read")
	}
	if first.Batteamname == "" {
		t.Error("innings 1 has no batting team name")
	}
}

// Go's decoder is case-insensitive, so the casing split cannot break ingest.
// This pins that fact, because the package comment relies on it and because
// the intuitive assumption is the opposite — which is exactly the assumption
// an earlier draft of this file was written on.
//
// What the tags actually control is the casing we EMIT. Cricket re-marshals
// through these types rather than forwarding provider bytes, and a non-Go
// consumer matching field names exactly would break on a normalised document.
func TestCasingIsADownstreamConcernNotADecodingOne(t *testing.T) {
	// Decoding: either tag reads either document.
	var lower struct {
		MatchID int `json:"matchid"`
	}
	if err := json.Unmarshal([]byte(`{"matchId":154524}`), &lower); err != nil {
		t.Fatal(err)
	}
	if lower.MatchID != 154524 {
		t.Errorf("a lowercase tag failed to read a camelCase document: got %d", lower.MatchID)
	}

	// Emitting: the tag decides, and this is what a downstream consumer sees.
	out, err := json.Marshal(MatchInfo{MatchID: 154524})
	if err != nil {
		t.Fatal(err)
	}
	if !contains(string(out), `"matchId"`) {
		t.Errorf("MatchInfo emitted %s; the list family must stay camelCase on the wire", out)
	}
	if contains(string(out), `"matchid"`) {
		t.Errorf("MatchInfo emitted lowercase; that is the match-centre convention, not this one")
	}
}

// ---------------------------------------------------------------------------
// Flattening
// ---------------------------------------------------------------------------

// An advertising entry carries no seriesAdWrapper. Skipping it is the whole
// reason Matches exists rather than each caller writing the walk.
func TestAdSlotsAreSkippedNotPanicked(t *testing.T) {
	list := MatchList{TypeMatches: []TypeMatch{{
		MatchType: "League",
		SeriesMatches: []SeriesMatch{
			{SeriesAdWrapper: nil}, // an ad slot
			{SeriesAdWrapper: &SeriesAdWrapper{
				SeriesID: 1, Matches: []Match{{MatchInfo: MatchInfo{MatchID: 7, State: "In Progress"}}},
			}},
		},
	}}}

	got := list.Matches()
	if len(got) != 1 || got[0].MatchID != 7 {
		t.Fatalf("Matches() = %+v, want the one real fixture", got)
	}
	if ids := list.LiveMatchIDs(); len(ids) != 1 || ids[0] != 7 {
		t.Errorf("LiveMatchIDs() = %v, want [7]", ids)
	}
}

func TestNilListIsSafe(t *testing.T) {
	var list *MatchList
	if got := list.Matches(); got != nil {
		t.Errorf("a nil list should flatten to nil, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// State vocabulary
// ---------------------------------------------------------------------------

// Cricket has more non-playing states than most sports, and conflating them
// costs quota: a rain delay polled every thirty seconds is a document that
// will not move. All three states below appear in the live capture.
func TestMatchStateVocabulary(t *testing.T) {
	for _, tc := range []struct {
		state    string
		started  bool
		live     bool
		complete bool
	}{
		{"In Progress", true, true, false},
		{"Innings Break", true, false, false},
		{"Stumps", true, false, false},
		{"Delay", true, false, false},
		{"Rain", true, false, false},
		{"Preview", false, false, false},
		{"Complete", false, false, true},
		{"Abandon", false, false, true},
		{"No Result", false, false, true},
		{"", false, false, false},
	} {
		m := MatchInfo{State: tc.state}
		if got := m.Started(); got != tc.started {
			t.Errorf("state %q: Started() = %v, want %v", tc.state, got, tc.started)
		}
		if got := m.Live(); got != tc.live {
			t.Errorf("state %q: Live() = %v, want %v", tc.state, got, tc.live)
		}
		if got := m.Complete(); got != tc.complete {
			t.Errorf("state %q: Complete() = %v, want %v", tc.state, got, tc.complete)
		}
	}
}

// startDate is epoch MILLISECONDS in a quoted string. Reading it as seconds
// yields a date tens of thousands of years out that still sorts and compares
// without complaint — the same trap golf's MongoDate carries.
func TestStartDateIsMillisecondsNotSeconds(t *testing.T) {
	m := MatchInfo{StartDate: "1788361200000"}
	got, ok := m.StartsAt()
	if !ok {
		t.Fatal("a valid startDate did not parse")
	}
	if got.Year() != 2026 {
		t.Errorf("startDate parsed to year %d; milliseconds were probably read as seconds", got.Year())
	}
}

func TestAbsentDatesReportNotOK(t *testing.T) {
	for _, raw := range []string{"", "0", "not a number"} {
		if _, ok := (MatchInfo{StartDate: raw}).StartsAt(); ok {
			t.Errorf("startDate %q should not parse", raw)
		}
	}
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

func TestRapidAPIHeadersAreSent(t *testing.T) {
	var gotKey, gotHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("x-rapidapi-key")
		gotHost = r.Header.Get("x-rapidapi-host")
		w.Write([]byte(`{"typeMatches":[]}`))
	}))
	defer srv.Close()

	c := New("test-key").Configure(WithBaseURL(srv.URL))
	if _, err := c.LiveMatches(context.Background()); err != nil {
		t.Fatalf("LiveMatches: %v", err)
	}
	if gotKey != "test-key" {
		t.Errorf("x-rapidapi-key = %q", gotKey)
	}
	if gotHost != Host {
		t.Errorf("x-rapidapi-host = %q, want %q", gotHost, Host)
	}
}

func TestMissingKeyIsReportedBeforeAnyRequest(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	defer srv.Close()

	c := New("  ").Configure(WithBaseURL(srv.URL))
	if _, err := c.LiveMatches(context.Background()); err == nil {
		t.Fatal("an empty credential should be an error")
	}
	if called {
		t.Error("a request was sent with no credential; the check must come first")
	}
}

// A 429 starts the hard floor. Losing the subscription costs far more than a
// day of stale cricket, so the resting lifetime is forced back on.
func TestRateLimitStartsTheHardFloor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	c := New("k").Configure(WithBaseURL(srv.URL), WithClock(func() time.Time { return now }))
	c.SetCacheTTL(LiveCacheTTL)

	if throttled, _ := c.Throttled(); throttled {
		t.Fatal("precondition: should not start throttled")
	}
	if _, err := c.LiveMatches(context.Background()); err == nil {
		t.Fatal("a 429 should be an error")
	}
	throttled, until := c.Throttled()
	if !throttled {
		t.Fatal("a 429 did not start the hard floor")
	}
	if want := now.Add(ThrottlePenalty); !until.Equal(want) {
		t.Errorf("floor lifts at %v, want %v", until, want)
	}
	// And the floor outranks a live TTL that was set moments earlier.
	if got := c.EffectiveTTL(); got != CacheTTL {
		t.Errorf("EffectiveTTL() = %v while throttled, want the resting %v", got, CacheTTL)
	}
}

func TestCredentialRejectionIsDistinguishable(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(code)
		}))
		_, err := New("k").Configure(WithBaseURL(srv.URL)).LiveMatches(context.Background())
		srv.Close()
		if err == nil {
			t.Fatalf("HTTP %d should be an error", code)
		}
		if got := err.Error(); !contains(got, "credential rejected") {
			t.Errorf("HTTP %d reported as %q; it should name the credential", code, got)
		}
	}
}

func TestScorecardRejectsAnUnusableMatchID(t *testing.T) {
	c := New("k")
	for _, id := range []int{0, -1} {
		if _, _, err := c.Scorecard(context.Background(), ScorecardRequest{MatchID: id}); err == nil {
			t.Errorf("matchId %d should be refused before a request is built", id)
		}
	}
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

func TestCacheIsServedWhileFresh(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Write([]byte(`{"scorecard":[{"inningsid":1,"batteamname":"Ireland"}]}`))
	}))
	defer srv.Close()

	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	c := New("k").Configure(
		WithBaseURL(srv.URL),
		WithCachePath(filepath.Join(t.TempDir(), "cache.json")),
		WithClock(func() time.Time { return now }),
	)

	req := ScorecardRequest{MatchID: 40381}
	if _, _, err := c.Scorecard(context.Background(), req); err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	_, meta, err := c.Scorecard(context.Background(), req)
	if err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	if !meta.FromCache {
		t.Error("the second call was not served from cache")
	}
	if hits != 1 {
		t.Errorf("upstream was called %d times; the cache spent quota it should have saved", hits)
	}
}

// A cache holding a different match must never be served. A scorecard is not
// self-identifying enough for anyone downstream to notice the substitution.
func TestCacheForADifferentMatchIsNotServed(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Write([]byte(`{"scorecard":[{"inningsid":1}]}`))
	}))
	defer srv.Close()

	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	c := New("k").Configure(
		WithBaseURL(srv.URL),
		WithCachePath(filepath.Join(t.TempDir(), "cache.json")),
		WithClock(func() time.Time { return now }),
	)

	if _, _, err := c.Scorecard(context.Background(), ScorecardRequest{MatchID: 1}); err != nil {
		t.Fatal(err)
	}
	_, meta, err := c.Scorecard(context.Background(), ScorecardRequest{MatchID: 2})
	if err != nil {
		t.Fatal(err)
	}
	if meta.FromCache {
		t.Error("a cache for match 1 was served for match 2")
	}
	if hits != 2 {
		t.Errorf("upstream called %d times, want 2", hits)
	}
}

// When the upstream dies, a stale scorecard beats an error: a card from four
// minutes ago is worth incomparably more to a screen behind a bar than nothing.
func TestStaleCacheIsServedWhenUpstreamFails(t *testing.T) {
	fail := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte(`{"scorecard":[{"inningsid":1,"batteamname":"Ireland"}]}`))
	}))
	defer srv.Close()

	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	c := New("k").Configure(
		WithBaseURL(srv.URL),
		WithCachePath(filepath.Join(t.TempDir(), "cache.json")),
		WithClock(clock),
	)

	req := ScorecardRequest{MatchID: 40381}
	if _, _, err := c.Scorecard(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	fail = true
	now = now.Add(10 * time.Hour) // well past any TTL
	sc, meta, err := c.Scorecard(context.Background(), req)
	if err != nil {
		t.Fatalf("a stale cache should have rescued this: %v", err)
	}
	if !meta.Stale || !meta.FromCache {
		t.Errorf("expected a stale cache read, got %+v", meta)
	}
	if meta.Err == nil {
		t.Error("a stale read must report why it was stale")
	}
	if sc == nil || len(sc.Scorecard) == 0 {
		t.Error("the stale document did not decode")
	}
}

func TestCorruptCacheIsRecoveredFrom(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"scorecard":[{"inningsid":1}]}`))
	}))
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "cache.json")
	if err := os.WriteFile(path, []byte("{ this is not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := New("k").Configure(WithBaseURL(srv.URL), WithCachePath(path))

	if _, _, err := c.Scorecard(context.Background(), ScorecardRequest{MatchID: 1}); err != nil {
		t.Fatalf("a corrupt cache should be refetched past, not fatal: %v", err)
	}
}

func TestContextCancellationIsHonoured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := New("k").Configure(WithBaseURL(srv.URL))
	if _, err := c.LiveMatches(ctx); err == nil {
		t.Error("a cancelled context should abort the request")
	}
}

func TestTimeoutIsFiveSeconds(t *testing.T) {
	if got := New("k").http.Timeout; got != Timeout {
		t.Errorf("timeout = %v, want %v", got, Timeout)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) &&
		(haystack == needle || len(needle) == 0 ||
			indexOf(haystack, needle) >= 0)
}

func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}
