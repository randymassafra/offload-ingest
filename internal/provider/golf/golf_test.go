package golf

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
