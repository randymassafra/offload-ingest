package golf

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// sampleLeaderboard is the shape the live API actually returns, extended-JSON
// integers included. Taken from a real /leaderboard response.
const sampleLeaderboard = `{
  "orgId": "1", "year": "2023", "tournId": "033",
  "status": "Official", "roundId": {"$numberInt": "4"},
  "roundStatus": "Official", "lastUpdated": "2023-05-21T23:12:00Z",
  "cutLines": [{"cutCount": {"$numberInt": "70"}, "cutScore": "+5"}],
  "leaderboardRows": [
    {
      "lastName": "Koepka", "firstName": "Brooks", "playerId": "36689",
      "isAmateur": false, "courseId": "514", "status": "complete",
      "position": "1", "total": "-9", "currentRoundScore": "-3",
      "totalStrokesFromCompletedRounds": "271",
      "currentHole": {"$numberInt": "18"},
      "startingHole": {"$numberInt": "1"},
      "roundComplete": true,
      "rounds": [
        {"scoreToPar": "+2", "roundId": {"$numberInt": "1"}, "strokes": {"$numberInt": "72"}, "courseId": "514", "courseName": "Oak Hill"},
        {"scoreToPar": "E",  "roundId": {"$numberInt": "2"}, "strokes": {"$numberInt": "70"}, "courseId": "514", "courseName": "Oak Hill"}
      ]
    }
  ]
}`

func testServer(t *testing.T, body string, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls != nil {
			calls.Add(1)
		}
		if got := r.Header.Get("x-rapidapi-key"); got != "test-key" {
			t.Errorf("key header = %q", got)
		}
		if got := r.Header.Get("x-rapidapi-host"); got != Host {
			t.Errorf("host header = %q", got)
		}
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestClient(t *testing.T, srv *httptest.Server, now func() time.Time) *Client {
	t.Helper()
	return New("test-key").Configure(
		WithBaseURL(srv.URL),
		WithCachePath(filepath.Join(t.TempDir(), "golf_cache.json")),
		WithClock(now),
	)
}

var req = LeaderboardRequest{OrgID: "1", TournID: "033", Year: "2023"}

// TestExtendedJSONIntegersDecode is the trap this provider exists around: the
// upstream serves MongoDB extended JSON, so integers arrive wrapped and a plain
// int field fails to unmarshal outright.
func TestExtendedJSONIntegersDecode(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, testServer(t, sampleLeaderboard, &calls), time.Now)

	lb, _, err := c.Leaderboard(context.Background(), req)
	if err != nil {
		t.Fatalf("Leaderboard: %v", err)
	}
	if lb.RoundID.Int() != 4 {
		t.Errorf("roundId = %d, want 4", lb.RoundID.Int())
	}
	if len(lb.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(lb.Rows))
	}
	row := lb.Rows[0]
	if row.CurrentHole.Int() != 18 || row.StartingHole.Int() != 1 {
		t.Errorf("holes = %d/%d, want 18/1", row.CurrentHole.Int(), row.StartingHole.Int())
	}
	if len(row.Rounds) != 2 || row.Rounds[0].Strokes.Int() != 72 {
		t.Errorf("round strokes not decoded: %+v", row.Rounds)
	}
	if row.Name() != "Brooks Koepka" {
		t.Errorf("name = %q", row.Name())
	}
	if len(lb.CutLines) != 1 || lb.CutLines[0].CutCount.Int() != 70 {
		t.Errorf("cut line not decoded: %+v", lb.CutLines)
	}
}

// TestMongoIntAcceptsEveryObservedForm. Being liberal costs nothing; being
// strict would mean one unwrapped field breaks a whole leaderboard.
func TestMongoIntAcceptsEveryObservedForm(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int
	}{
		{`{"$numberInt":"18"}`, 18},
		{`{"$numberLong":"9000"}`, 9000},
		{`{"$numberDouble":"3.7"}`, 3},
		{`42`, 42},
		{`"42"`, 42},
		{`null`, 0},
		{`{}`, 0},
		{`""`, 0},
	} {
		var m MongoInt
		if err := json.Unmarshal([]byte(tc.raw), &m); err != nil {
			t.Errorf("%s: %v", tc.raw, err)
			continue
		}
		if m.Int() != tc.want {
			t.Errorf("%s = %d, want %d", tc.raw, m.Int(), tc.want)
		}
	}
	var m MongoInt
	if err := json.Unmarshal([]byte(`"not-a-number"`), &m); err == nil {
		t.Error("a non-numeric string should be an error, not a silent zero")
	}
}

// TestMongoIntMarshalsAsAPlainNumber: the extended form is a MongoDB
// serialisation artifact, and propagating it would push the upstream's accident
// into our own schema.
func TestMongoIntMarshalsAsAPlainNumber(t *testing.T) {
	b, err := json.Marshal(struct {
		Hole MongoInt `json:"hole"`
	}{Hole: 18})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `{"hole":18}` {
		t.Errorf("marshalled as %s, want a plain number", b)
	}
}

func TestParScore(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int
		ok   bool
	}{
		{"-9", -9, true}, {"+2", 2, true}, {"E", 0, true}, {"e", 0, true},
		{"0", 0, true}, {"", 0, false}, {"WD", 0, false}, {"CUT", 0, false},
	} {
		got, ok := ParScore(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Errorf("ParScore(%q) = %d,%v want %d,%v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

// --- cache behaviour ---------------------------------------------------------

// TestCacheIsUsedWhenFresh is the cache-first requirement: a second call inside
// the TTL must not touch the network.
func TestCacheIsUsedWhenFresh(t *testing.T) {
	var calls atomic.Int32
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	c := newTestClient(t, testServer(t, sampleLeaderboard, &calls), func() time.Time { return clock })

	if _, meta, err := c.Leaderboard(context.Background(), req); err != nil {
		t.Fatalf("first call: %v", err)
	} else if meta.FromCache {
		t.Error("the first call should have hit the network")
	}
	if calls.Load() != 1 {
		t.Fatalf("made %d calls, want 1", calls.Load())
	}

	// Well inside the hour.
	clock = clock.Add(59 * time.Minute)
	_, meta, err := c.Leaderboard(context.Background(), req)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if !meta.FromCache {
		t.Error("a fresh cache should have been used")
	}
	if calls.Load() != 1 {
		t.Errorf("made %d calls; the cache was not honoured", calls.Load())
	}
	if meta.Age < 58*time.Minute {
		t.Errorf("reported age %s, want ~59m", meta.Age)
	}
}

// TestCacheExpiresAfterAnHour pins the TTL boundary.
func TestCacheExpiresAfterAnHour(t *testing.T) {
	var calls atomic.Int32
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	c := newTestClient(t, testServer(t, sampleLeaderboard, &calls), func() time.Time { return clock })

	c.Leaderboard(context.Background(), req)
	clock = clock.Add(CacheTTL + time.Second)

	_, meta, err := c.Leaderboard(context.Background(), req)
	if err != nil {
		t.Fatalf("refetch: %v", err)
	}
	if meta.FromCache {
		t.Error("a stale cache should have been refetched")
	}
	if calls.Load() != 2 {
		t.Errorf("made %d calls, want 2", calls.Load())
	}
}

func TestCacheFileIsWritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "golf_cache.json")
	c := New("test-key").Configure(
		WithBaseURL(testServer(t, sampleLeaderboard, nil).URL),
		WithCachePath(path),
	)
	if _, _, err := c.Leaderboard(context.Background(), req); err != nil {
		t.Fatalf("Leaderboard: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cache file: %v", err)
	}
	var env cacheEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("cache is not readable: %v", err)
	}
	if env.Key == "" || env.FetchedAt.IsZero() || len(env.Document) == 0 {
		t.Errorf("cache envelope is incomplete: %+v", env)
	}
	// The temporary file must not survive.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("the temporary write file was left behind")
	}
}

// TestCacheForADifferentTournamentIsNotServed. Serving one tournament's
// leaderboard for another is silently and completely wrong, so the cache
// carries the request it was fetched for.
func TestCacheForADifferentTournamentIsNotServed(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, testServer(t, sampleLeaderboard, &calls), time.Now)

	c.Leaderboard(context.Background(), req)
	other := LeaderboardRequest{OrgID: "1", TournID: "999", Year: "2023"}
	if _, meta, err := c.Leaderboard(context.Background(), other); err != nil {
		t.Fatalf("second tournament: %v", err)
	} else if meta.FromCache {
		t.Error("a cache for a different tournament was served")
	}
	if calls.Load() != 2 {
		t.Errorf("made %d calls, want 2", calls.Load())
	}
}

// TestStaleCacheServedWhenUpstreamFails. A leaderboard an hour old still beats
// a blank panel behind a bar.
func TestStaleCacheServedWhenUpstreamFails(t *testing.T) {
	var fail atomic.Bool
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, sampleLeaderboard)
	}))
	defer srv.Close()

	c := New("test-key").Configure(
		WithBaseURL(srv.URL),
		WithCachePath(filepath.Join(t.TempDir(), "c.json")),
		WithClock(func() time.Time { return clock }),
	)
	if _, _, err := c.Leaderboard(context.Background(), req); err != nil {
		t.Fatalf("priming: %v", err)
	}

	fail.Store(true)
	clock = clock.Add(3 * time.Hour) // well past the TTL

	lb, meta, err := c.Leaderboard(context.Background(), req)
	if err != nil {
		t.Fatalf("want the stale cache, got an error: %v", err)
	}
	if !meta.Stale || !meta.FromCache {
		t.Errorf("meta should report a stale cache read: %+v", meta)
	}
	if meta.Err == nil {
		t.Error("the upstream failure should be reported alongside the stale data")
	}
	if len(lb.Rows) != 1 {
		t.Error("the stale document did not decode")
	}
}

// TestUpstreamFailureWithNoCacheIsAnError: degrading is only acceptable when
// there is something to degrade to.
func TestUpstreamFailureWithNoCacheIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	c := New("test-key").Configure(
		WithBaseURL(srv.URL),
		WithCachePath(filepath.Join(t.TempDir(), "c.json")),
	)
	if _, _, err := c.Leaderboard(context.Background(), req); err == nil {
		t.Error("want an error when neither the upstream nor a cache is available")
	}
}

// TestCorruptCacheIsRecoveredFrom. A truncated file must not wedge the provider
// permanently.
func TestCorruptCacheIsRecoveredFrom(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	c := New("test-key").Configure(
		WithBaseURL(testServer(t, sampleLeaderboard, nil).URL),
		WithCachePath(path),
	)
	if _, _, err := c.Leaderboard(context.Background(), req); err != nil {
		t.Fatalf("a corrupt cache should be refetched, got: %v", err)
	}
}

// --- errors ------------------------------------------------------------------

func TestCredentialErrorsAreDistinguishable(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   string
	}{
		{http.StatusUnauthorized, "credential rejected"},
		{http.StatusForbidden, "credential rejected"},
		{http.StatusTooManyRequests, "rate limited"},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
		}))
		c := New("test-key").Configure(
			WithBaseURL(srv.URL),
			WithCachePath(filepath.Join(t.TempDir(), "c.json")),
		)
		_, _, err := c.Leaderboard(context.Background(), req)
		srv.Close()
		if err == nil || !contains(err.Error(), tc.want) {
			t.Errorf("HTTP %d gave %v, want it to mention %q", tc.status, err, tc.want)
		}
	}
}

func TestMissingKeyIsReported(t *testing.T) {
	c := New("").Configure(WithCachePath(filepath.Join(t.TempDir(), "c.json")))
	_, _, err := c.Leaderboard(context.Background(), req)
	if err == nil || !contains(err.Error(), "no API key") {
		t.Errorf("got %v, want a missing-key error", err)
	}
}

func TestRequiredParametersAreValidated(t *testing.T) {
	c := New("k")
	if _, _, err := c.Leaderboard(context.Background(), LeaderboardRequest{OrgID: "1"}); err == nil {
		t.Error("tournId and year should be required")
	}
}

func TestTimeoutIsFiveSeconds(t *testing.T) {
	if Timeout != 5*time.Second {
		t.Errorf("Timeout = %s, the directive specifies 5s", Timeout)
	}
	if New("k").http.Timeout != 5*time.Second {
		t.Error("the HTTP client does not carry the 5s timeout")
	}
}

func TestContextCancellationIsHonoured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	c := New("k").Configure(
		WithBaseURL(srv.URL),
		WithCachePath(filepath.Join(t.TempDir(), "c.json")),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, _, err := c.Leaderboard(ctx, req); err == nil {
		t.Error("want an error on a cancelled context")
	}
}

func contains(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) &&
		func() bool {
			for i := 0; i+len(needle) <= len(haystack); i++ {
				if haystack[i:i+len(needle)] == needle {
					return true
				}
			}
			return false
		}()
}

// --- schedule dates and tournament selection ---------------------------------

// TestMongoDateAcceptsEveryObservedForm. The same endpoint has been seen
// returning both the extended-JSON epoch and a bare zone-less timestamp,
// apparently depending on how the request is made.
func TestMongoDateAcceptsEveryObservedForm(t *testing.T) {
	want := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	for _, raw := range []string{
		`{"$date":{"$numberLong":"1735776000000"}}`,
		`{"$date":"2025-01-02T00:00:00"}`,
		`"2025-01-02T00:00:00Z"`,
		`"2025-01-02"`,
		`"1735776000000"`,
		`1735776000000`,
	} {
		var d MongoDate
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			t.Errorf("%s: %v", raw, err)
			continue
		}
		if !d.Time.Equal(want) {
			t.Errorf("%s = %s, want %s", raw, d.Time, want)
		}
	}

	var zero MongoDate
	for _, raw := range []string{`null`, `{}`, `""`} {
		if err := json.Unmarshal([]byte(raw), &zero); err != nil || !zero.IsZero() {
			t.Errorf("%s should decode to the zero time, got %s (%v)", raw, zero.Time, err)
		}
	}
}

// TestMongoDateIsMillisecondsNotSeconds. Reading the epoch as seconds yields a
// date in the year 56000, which sorts and compares without erroring — so a
// window check would simply never match and golf would go silent.
func TestMongoDateIsMillisecondsNotSeconds(t *testing.T) {
	var d MongoDate
	if err := json.Unmarshal([]byte(`{"$date":{"$numberLong":"1735776000000"}}`), &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if y := d.Time.Year(); y != 2025 {
		t.Errorf("year = %d, want 2025 — the epoch is milliseconds, not seconds", y)
	}
}

func scheduleFor(t *testing.T) *Schedule {
	t.Helper()
	mk := func(id, name string, start, end time.Time) ScheduleEntry {
		var e ScheduleEntry
		e.TournID, e.Name = id, name
		e.Date.Start = MongoDate{start}
		e.Date.End = MongoDate{end}
		return e
	}
	d := func(m, day int) time.Time { return time.Date(2026, time.Month(m), day, 0, 0, 0, 0, time.UTC) }
	return &Schedule{Schedule: []ScheduleEntry{
		mk("001", "January Open", d(1, 8), d(1, 11)),
		mk("002", "August Championship", d(8, 27), d(8, 30)),
		mk("003", "December Invitational", d(12, 12), d(12, 15)),
	}}
}

// TestCurrentPrefersTheTournamentInProgress.
func TestCurrentPrefersTheTournamentInProgress(t *testing.T) {
	s := scheduleFor(t)
	// Saturday of the August event.
	pick, ok := s.Current(time.Date(2026, 8, 29, 18, 0, 0, 0, time.UTC))
	if !ok || pick.TournID != "002" {
		t.Fatalf("got %+v (ok=%v), want the in-progress August event", pick.TournID, ok)
	}
	if !pick.InProgress(time.Date(2026, 8, 29, 18, 0, 0, 0, time.UTC)) {
		t.Error("InProgress disagrees with Current")
	}
}

// TestFinalRoundEveningStillCountsAsInProgress. The window runs to the end of
// the final day: a leaderboard is at its most interesting on Sunday evening,
// and an exclusive comparison would drop the tournament exactly then.
func TestFinalRoundEveningStillCountsAsInProgress(t *testing.T) {
	s := scheduleFor(t)
	sundayEvening := time.Date(2026, 8, 30, 22, 0, 0, 0, time.UTC)
	pick, ok := s.Current(sundayEvening)
	if !ok || pick.TournID != "002" {
		t.Errorf("Sunday evening resolved to %q, want the event still in progress", pick.TournID)
	}
}

// TestCurrentFallsBackToTheMostRecentlyCompleted is the fix for the original
// bug: taking the last schedule entry picked an unplayed December event for
// which the provider has no leaderboard at all.
func TestCurrentFallsBackToTheMostRecentlyCompleted(t *testing.T) {
	s := scheduleFor(t)
	// September: between the August and December events.
	pick, ok := s.Current(time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatal("no tournament resolved between events")
	}
	if pick.TournID != "002" {
		t.Errorf("resolved %q, want the most recently completed event, not the future one", pick.TournID)
	}
	if pick.TournID == "003" {
		t.Error("resolved the unplayed December event — the original bug")
	}
}

// TestCurrentReportsNothingBeforeTheSeasonOpens, so the caller can fall back to
// the previous season rather than requesting a leaderboard that cannot exist.
func TestCurrentReportsNothingBeforeTheSeasonOpens(t *testing.T) {
	s := scheduleFor(t)
	if _, ok := s.Current(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); ok {
		t.Error("a tournament was resolved before any had been played")
	}
}

func TestEntryWithNoDatesIsNeverSelected(t *testing.T) {
	var e ScheduleEntry
	now := time.Now()
	if e.InProgress(now) || e.Completed(now) {
		t.Error("an entry with no dates should be neither in progress nor completed")
	}
}

// --- dynamic cadence and the hard floor ---------------------------------------

// TestEffectiveTTLFollowsTheRequest.
func TestEffectiveTTLFollowsTheRequest(t *testing.T) {
	c := New("k")
	if got := c.EffectiveTTL(); got != CacheTTL {
		t.Errorf("default TTL = %s, want %s", got, CacheTTL)
	}
	c.SetCacheTTL(LiveCacheTTL)
	if got := c.EffectiveTTL(); got != LiveCacheTTL {
		t.Errorf("TTL = %s, want %s", got, LiveCacheTTL)
	}
	// A nonsensical value falls back to the resting lifetime rather than
	// producing an every-request cache.
	c.SetCacheTTL(0)
	if got := c.EffectiveTTL(); got != CacheTTL {
		t.Errorf("zero TTL = %s, want the resting %s", got, CacheTTL)
	}
}

// TestA429EngagesTheHardFloor is the guardrail: RapidAPI suspends accounts that
// keep hammering a throttled endpoint, so a 429 must force the resting lifetime
// back on regardless of what the caller asks for.
func TestA429EngagesTheHardFloor(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := New("k").Configure(
		WithBaseURL(srv.URL),
		WithCachePath(filepath.Join(t.TempDir(), "c.json")),
		WithClock(func() time.Time { return clock }),
	)
	c.SetCacheTTL(FinalRoundCacheTTL)
	if got := c.EffectiveTTL(); got != FinalRoundCacheTTL {
		t.Fatalf("before the 429 the TTL should be %s, got %s", FinalRoundCacheTTL, got)
	}

	_, _, err := c.Leaderboard(context.Background(), req)
	if err == nil {
		t.Fatal("want an error on a 429")
	}
	if !contains(err.Error(), "429") || !contains(err.Error(), "forced to") {
		t.Errorf("the error should say the floor engaged: %v", err)
	}

	throttled, until := c.Throttled()
	if !throttled {
		t.Fatal("the hard floor did not engage")
	}
	if want := clock.Add(ThrottlePenalty); !until.Equal(want) {
		t.Errorf("floor lifts at %s, want %s", until, want)
	}

	// The floor overrides any request while it holds.
	c.SetCacheTTL(FinalRoundCacheTTL)
	if got := c.EffectiveTTL(); got != CacheTTL {
		t.Errorf("TTL = %s while throttled, want the resting %s", got, CacheTTL)
	}
}

// TestTheHardFloorLiftsAfterTwentyFourHours.
func TestTheHardFloorLiftsAfterTwentyFourHours(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := New("k").Configure(
		WithBaseURL(srv.URL),
		WithCachePath(filepath.Join(t.TempDir(), "c.json")),
		WithClock(func() time.Time { return clock }),
	)
	c.Leaderboard(context.Background(), req)
	c.SetCacheTTL(LiveCacheTTL)

	// One minute short of a day: still held.
	clock = clock.Add(ThrottlePenalty - time.Minute)
	if got := c.EffectiveTTL(); got != CacheTTL {
		t.Errorf("TTL = %s just before the floor lifts, want %s", got, CacheTTL)
	}
	// Past it: the requested lifetime is honoured again.
	clock = clock.Add(2 * time.Minute)
	if got := c.EffectiveTTL(); got != LiveCacheTTL {
		t.Errorf("TTL = %s after the floor lifts, want the requested %s", got, LiveCacheTTL)
	}
	if throttled, _ := c.Throttled(); throttled {
		t.Error("still reporting throttled after the penalty expired")
	}
}

// TestLiveTTLIsHonouredByTheCache: a shorter lifetime must actually cause a
// refetch, or the cadence switch would be decorative.
func TestLiveTTLIsHonouredByTheCache(t *testing.T) {
	var calls atomic.Int32
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	c := newTestClient(t, testServer(t, sampleLeaderboard, &calls), func() time.Time { return clock })
	c.SetCacheTTL(LiveCacheTTL)

	c.Leaderboard(context.Background(), req)
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
	// Inside the live TTL: cached.
	clock = clock.Add(4 * time.Minute)
	if _, meta, _ := c.Leaderboard(context.Background(), req); !meta.FromCache {
		t.Error("a 4-minute-old document should still be cached at a 5-minute TTL")
	}
	// Past it: refetched, where the resting hour would still have been cached.
	clock = clock.Add(2 * time.Minute)
	if _, meta, _ := c.Leaderboard(context.Background(), req); meta.FromCache {
		t.Error("a 6-minute-old document should be refetched at a 5-minute TTL")
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2", calls.Load())
	}
}

// TestCapturedLeaderboardDecodes runs the real recorded response through the
// model.
//
// This is the test the package was missing, and its absence cost a production
// outage's worth of feed: thru was modelled as a number, the provider sends
// "F" for a finished round, and because a leaderboard is decoded as one
// document that single value failed the whole unmarshal. Every field was
// individually plausible; only the capture disproved the model.
//
// It is deliberately assertion-light. The point is not what the values are —
// they change every tournament — but that a document the provider actually
// sent survives the round trip at all.
func TestCapturedLeaderboardDecodes(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "fixtures", "golfdata", "leaderboard.json"))
	if err != nil {
		t.Skipf("no capture to check against: %v", err)
	}

	var lb Leaderboard
	if err := json.Unmarshal(raw, &lb); err != nil {
		t.Fatalf("the captured leaderboard does not decode: %v", err)
	}
	if len(lb.Rows) == 0 {
		t.Fatal("decoded a leaderboard with no rows; the capture is not exercising the model")
	}
	if lb.TournID == "" {
		t.Error("tournId is empty after decoding a real document")
	}
	if lb.LastUpdated.IsZero() {
		t.Error("lastUpdated is zero; the extended-JSON date did not decode")
	}
}

// TestCapturedLeaderboardKeepsEveryField re-marshals the capture and reports
// any field the model silently discarded.
//
// Golf is the one provider whose payload is rebuilt from typed structs rather
// than passed through as bytes, so an unmodelled field does not reach Kafka at
// all. That is invisible without a check like this one: the feed keeps
// flowing, just thinner.
//
// Extended-JSON wrappers are expected to disappear — unwrapping them is
// deliberate, documented on MongoInt.MarshalJSON — so they are exempt.
func TestCapturedLeaderboardKeepsEveryField(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "fixtures", "golfdata", "leaderboard.json"))
	if err != nil {
		t.Skipf("no capture to check against: %v", err)
	}

	var lb Leaderboard
	if err := json.Unmarshal(raw, &lb); err != nil {
		t.Fatalf("decoding the capture: %v", err)
	}
	out, err := json.Marshal(lb)
	if err != nil {
		t.Fatalf("re-marshalling: %v", err)
	}

	unwrapped := map[string]bool{"$numberInt": true, "$numberLong": true, "$numberDouble": true, "$date": true}
	for field := range fieldNames(t, raw) {
		if unwrapped[field] {
			continue
		}
		if !fieldNames(t, out)[field] {
			t.Errorf("field %q is in the provider's document but not in ours; it will not reach Kafka", field)
		}
	}
}

// fieldNames collects every object key appearing anywhere in a document.
func fieldNames(t *testing.T, raw []byte) map[string]bool {
	t.Helper()
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("collecting field names: %v", err)
	}
	out := map[string]bool{}
	var walk func(any)
	walk = func(v any) {
		switch node := v.(type) {
		case map[string]any:
			for k, child := range node {
				out[k] = true
				walk(child)
			}
		case []any:
			for _, child := range node {
				walk(child)
			}
		}
	}
	walk(doc)
	return out
}

// TestThruAcceptsTheProvidersVocabulary pins the three forms observed on the
// wire. "F" is the one that broke the feed.
func TestThruAcceptsTheProvidersVocabulary(t *testing.T) {
	for _, tc := range []struct {
		thru     string
		holes    int
		numeric  bool
		finished bool
	}{
		{"F", 0, false, true},
		{"-", 0, false, false},
		{"12", 12, true, false},
		{"", 0, false, false},
	} {
		var row Row
		if err := json.Unmarshal([]byte(`{"thru":`+strconv.Quote(tc.thru)+`}`), &row); err != nil {
			t.Fatalf("thru %q did not decode: %v", tc.thru, err)
		}
		holes, ok := row.HolesThru()
		if ok != tc.numeric || holes != tc.holes {
			t.Errorf("thru %q: HolesThru() = %d, %v; want %d, %v", tc.thru, holes, ok, tc.holes, tc.numeric)
		}
		if got := row.Finished(); got != tc.finished {
			t.Errorf("thru %q: Finished() = %v; want %v", tc.thru, got, tc.finished)
		}
	}
}
