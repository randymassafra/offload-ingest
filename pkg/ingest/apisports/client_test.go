package apisports

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	c, err := New(Config{
		APIKey: "test-key", BaseURLOverride: srv.URL, Logger: quiet(),
		Rand: rand.New(rand.NewSource(1)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestGetDecodesTheEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("x-apisports-key"); got != "test-key" {
			t.Errorf("key header = %q", got)
		}
		w.Header().Set("x-ratelimit-limit", "10")
		w.Header().Set("x-ratelimit-remaining", "7")
		w.Header().Set("x-ratelimit-requests-limit", "100")
		w.Header().Set("x-ratelimit-requests-remaining", "42")
		fmt.Fprint(w, `{"get":"fixtures","errors":[],"results":2,"response":[{"id":1},{"id":2}]}`)
	}))
	defer srv.Close()

	obs := &recordingObserver{}
	c, _ := New(Config{APIKey: "test-key", BaseURLOverride: srv.URL, Logger: quiet(), Observer: obs})
	env, err := c.Get(context.Background(), VerticalFootball, "/fixtures", map[string]string{"live": "all"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if env.Results != 2 {
		t.Errorf("results = %d, want 2", env.Results)
	}
	if q := obs.lastQuota.Load(); q == nil {
		t.Fatal("no quota was observed")
	} else if q.(*Quota).DayRemaining != 42 {
		t.Errorf("day remaining = %d, want 42", q.(*Quota).DayRemaining)
	}
}

// TestErrorsInsideA200AreSurfaced is the trap this API sets: a rejected request
// returns HTTP 200 with the reason in the body. A client that trusts the status
// code reports success and an empty result forever.
func TestErrorsInsideA200AreSurfaced(t *testing.T) {
	for _, body := range []string{
		`{"get":"games","errors":{"live":"The Live field do not exist."},"results":0,"response":[]}`,
		`{"get":"games","errors":{"token":"Error/Missing application key."},"results":0,"response":[]}`,
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, body)
		}))
		c := testClient(t, srv)
		_, err := c.Get(context.Background(), VerticalBasketball, "/games", nil)
		srv.Close()

		var apiErr *APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("body %s gave err %v, want an APIError", body, err)
		}
	}
}

// TestEmptyErrorArrayIsSuccess: the success case sends "errors": [], which must
// not be mistaken for a failure.
func TestEmptyErrorArrayIsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"get":"games","errors":[],"results":0,"response":[]}`)
	}))
	defer srv.Close()
	if _, err := testClient(t, srv).Get(context.Background(), VerticalAFL, "/games", nil); err != nil {
		t.Errorf("an empty error array was treated as a failure: %v", err)
	}
}

// TestThrottleRetriesWithBackoff is the 429 requirement.
func TestThrottleRetriesWithBackoff(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, `{"get":"fixtures","errors":[],"results":1,"response":[{"id":9}]}`)
	}))
	defer srv.Close()

	obs := &recordingObserver{}
	c, _ := New(Config{
		APIKey: "k", BaseURLOverride: srv.URL, Logger: quiet(),
		Observer: obs, Rand: rand.New(rand.NewSource(1)),
	})
	env, err := c.Get(context.Background(), VerticalFootball, "/fixtures", nil)
	if err != nil {
		t.Fatalf("Get after retries: %v", err)
	}
	if env.Results != 1 {
		t.Errorf("results = %d, want 1", env.Results)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("made %d calls, want 3 (two throttled, one good)", got)
	}
	if obs.throttles.Load() != 2 {
		t.Errorf("observed %d throttles, want 2", obs.throttles.Load())
	}
}

// TestThrottleGivesUpAfterMaxRetries: the retry loop is a safety net, not an
// infinite one.
func TestThrottleGivesUpAfterMaxRetries(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c, _ := New(Config{
		APIKey: "k", BaseURLOverride: srv.URL, Logger: quiet(),
		MaxRetries: 2, Rand: rand.New(rand.NewSource(1)),
	})
	_, err := c.Get(context.Background(), VerticalFootball, "/fixtures", nil)
	var te *ThrottleError
	if !errors.As(err, &te) {
		t.Fatalf("got %v, want a ThrottleError", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("made %d calls, want 3 (initial + 2 retries)", got)
	}
}

// TestRetriesRespectTheLimiter: a retry spends quota exactly like a first try,
// so it must go through the same gate.
func TestRetriesRespectTheLimiter(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, `{"errors":[],"results":0,"response":[]}`)
	}))
	defer srv.Close()

	lim := &countingLimiter{}
	c, _ := New(Config{
		APIKey: "k", BaseURLOverride: srv.URL, Logger: quiet(),
		Limiter: lim, Rand: rand.New(rand.NewSource(1)),
	})
	if _, err := c.Get(context.Background(), VerticalFootball, "/fixtures", nil); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if lim.waits.Load() != 2 {
		t.Errorf("limiter consulted %d times, want 2 (the retry must not bypass it)",
			lim.waits.Load())
	}
}

func TestContextCancellationStopsRetrying(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	c := testClient(t, srv)
	start := time.Now()
	if _, err := c.Get(ctx, VerticalFootball, "/fixtures", nil); err == nil {
		t.Fatal("want an error on a cancelled context")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("took %s; cancellation should cut the backoff short", elapsed)
	}
}

func TestNonOKStatusIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `upstream exploded`)
	}))
	defer srv.Close()
	_, err := testClient(t, srv).Get(context.Background(), VerticalFootball, "/fixtures", nil)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("got %v, want an error mentioning HTTP 500", err)
	}
}

func TestNewRejectsAnEmptyKey(t *testing.T) {
	if _, err := New(Config{APIKey: "   "}); err == nil {
		t.Error("an empty API key should be rejected")
	}
}

// --- vertical and bundle mapping -------------------------------------------

// TestEveryVerticalHasAVerifiedHost guards the table that was probed live.
func TestEveryVerticalHasAVerifiedHost(t *testing.T) {
	if n := len(Verticals()); n != 12 {
		t.Errorf("catalog has %d verticals, want the 12 probed live", n)
	}
	for _, v := range Verticals() {
		spec, ok := SpecFor(v)
		if !ok {
			t.Fatalf("no spec for %s", v)
		}
		if !strings.HasSuffix(spec.Host, ".api-sports.io") {
			t.Errorf("%s host %q is not an api-sports.io host", v, spec.Host)
		}
		if !spec.Verified {
			t.Errorf("%s is not marked verified", v)
		}
		if spec.BulkPath == "" || !strings.HasPrefix(spec.BulkPath, "/") {
			t.Errorf("%s has a bad bulk path %q", v, spec.BulkPath)
		}
	}
}

// TestBulkModeMatchesWhatTheAPIAccepts pins the finding that `live=all` is not
// universal: most verticals reject it and must be swept by date.
func TestBulkModeMatchesWhatTheAPIAccepts(t *testing.T) {
	live := map[Vertical]bool{
		VerticalFootball: true, VerticalAmericanFootball: true, VerticalNBA: true,
	}
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	for _, v := range Verticals() {
		spec, _ := SpecFor(v)
		q := spec.BulkQuery(now)
		if live[v] {
			if q["live"] != "all" {
				t.Errorf("%s should sweep with live=all, got %v", v, q)
			}
			continue
		}
		if spec.Mode == BulkSeason {
			if q["season"] == "" {
				t.Errorf("%s should sweep by season, got %v", v, q)
			}
			continue
		}
		if q["date"] != "2026-09-01" {
			t.Errorf("%s should sweep by date, got %v", v, q)
		}
		if _, hasLive := q["live"]; hasLive {
			t.Errorf("%s must not send live=all; the API rejects it", v)
		}
	}
}

// TestSportsAPISportsCannotServe records the coverage gap that kept three
// providers in the pipeline.
func TestSportsAPISportsCannotServe(t *testing.T) {
	// NASCAR was retired; API-Sports sells no motorsport product but F1.
	for _, s := range []Sport{"cricket", "tennis", "golf"} {
		if Serves(s) {
			t.Errorf("%s is mapped to API-Sports, which has no host for it", s)
		}
	}
	for _, s := range []Sport{"nfl", "ncaaf", "ncaab", "nba", "soccer", "afl", "rugby", "ufc", "mma", "f1"} {
		if !Serves(s) {
			t.Errorf("%s should be served by API-Sports", s)
		}
	}
}

// TestRegionsCannotWidenSportEntitlement is an entitlement rule worth pinning:
// a broad region claim must never grant a sport the licence did not sell.
func TestRegionsCannotWidenSportEntitlement(t *testing.T) {
	got := Entitled([]string{"nfl"}, []string{"global"})
	if len(got) != 1 || got[0].Sport != "nfl" {
		t.Errorf("region 'global' widened a single-sport licence to %v", got)
	}

	// And a region narrows: a US bundle must not unlock European soccer.
	got = Entitled([]string{"nfl", "soccer"}, []string{"us"})
	for _, b := range got {
		if b.Sport == "soccer" {
			t.Error("the US bundle unlocked soccer")
		}
	}

	// No sports claim entitles nothing, however broad the regions.
	if got := Entitled(nil, []string{"global"}); len(got) != 0 {
		t.Errorf("a licence with no sports entitled %v", got)
	}
}

func TestEntitledDropsUnservedSports(t *testing.T) {
	got := Entitled([]string{"nfl", "cricket", "tennis", "golf"}, nil)
	if len(got) != 1 || got[0].Sport != "nfl" {
		t.Errorf("got %v, want only nfl — the rest are other providers'", got)
	}
}

func TestParseVerticalAliases(t *testing.T) {
	for in, want := range map[string]Vertical{
		"soccer": VerticalFootball, "epl": VerticalFootball,
		"nfl": VerticalAmericanFootball, "ncaaf": VerticalAmericanFootball,
		"ncaab": VerticalBasketball, "nba": VerticalNBA,
		"f1": VerticalFormula1, "ufc": VerticalMMA, "afl": VerticalAFL,
	} {
		got, err := ParseVertical(in)
		if err != nil || got != want {
			t.Errorf("ParseVertical(%q) = %s, %v; want %s", in, got, err, want)
		}
	}
	if _, err := ParseVertical("quidditch"); err == nil {
		t.Error("want an error for an unknown sport")
	}
}

// --- helpers ---------------------------------------------------------------

type recordingObserver struct {
	lastQuota atomic.Value
	throttles atomic.Int32
	requests  atomic.Int32
}

func (o *recordingObserver) ObserveQuota(_ Vertical, q Quota) { o.lastQuota.Store(&q) }
func (o *recordingObserver) ObserveRequest(Vertical, int, time.Duration, error) {
	o.requests.Add(1)
}
func (o *recordingObserver) ObserveThrottle(Vertical, time.Duration) { o.throttles.Add(1) }

type countingLimiter struct{ waits atomic.Int32 }

func (l *countingLimiter) Wait(context.Context, Vertical) error {
	l.waits.Add(1)
	return nil
}
