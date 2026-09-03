package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// clock is a manual time source. Health is entirely about elapsed time, so
// every test here drives it rather than sleeping.
type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }

func newTestRegistry() (*Registry, *clock) {
	c := &clock{t: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)}
	return NewRegistry(c.now), c
}

// past the startup grace, so a test is measuring starvation rather than boot.
func (c *clock) settle() { c.add(startupGrace + time.Minute) }

func TestHealthyWhenASportProducedDataInsideTheWindow(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()
	r.RecordData("soccer")
	c.add(5 * time.Minute)

	h := r.Health(DefaultHealthWindow)
	if !h.OK {
		t.Fatalf("expected healthy, got %s: %s", h.Status, h.Detail)
	}
	if h.Status != StatusOK {
		t.Errorf("status = %q, want %q", h.Status, StatusOK)
	}
	if h.LastDataSport != "soccer" {
		t.Errorf("last data sport = %q, want soccer", h.LastDataSport)
	}
}

// One sport in season is enough. This is the "at least one" clause, and it
// matters: a venue licensed for ten sports has most of them out of season for
// most of the year, and requiring all ten would report the normal case as an
// outage.
func TestOneFreshSportIsEnoughWhileOthersAreStale(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()
	r.RecordData("afl")
	c.add(3 * time.Hour)
	r.RecordData("soccer")

	h := r.Health(DefaultHealthWindow)
	if !h.OK {
		t.Fatalf("one fresh sport should be enough; got %s: %s", h.Status, h.Detail)
	}
	if h.LastDataSport != "soccer" {
		t.Errorf("last data sport = %q, want the freshest (soccer)", h.LastDataSport)
	}
}

func TestDataStarvedOnceTheWindowLapses(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()
	r.RecordData("soccer")
	c.add(DefaultHealthWindow + time.Minute)

	h := r.Health(DefaultHealthWindow)
	if h.OK {
		t.Fatal("expected unhealthy after the window lapsed")
	}
	if h.Status != StatusDataStarved {
		t.Errorf("status = %q, want %q", h.Status, StatusDataStarved)
	}
}

// The boundary is inclusive: data exactly at the window edge is still fresh.
// An exclusive comparison makes a probe flap for a whole scrape interval on a
// feed whose cadence happens to equal the window.
func TestTheWindowBoundaryIsInclusive(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()
	r.RecordData("soccer")
	c.add(DefaultHealthWindow)

	if h := r.Health(DefaultHealthWindow); !h.OK {
		t.Errorf("data exactly at the window edge should be healthy, got %s", h.Status)
	}
}

func TestRateLimitedIsUnhealthyEvenWithFreshData(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()
	r.RecordData("golf")
	r.MarkRateLimited("livegolf", true)

	h := r.Health(DefaultHealthWindow)
	if h.OK {
		t.Fatal("a hard floor must fail the probe even when data is fresh")
	}
	if h.Status != StatusRateLimited {
		t.Errorf("status = %q, want %q", h.Status, StatusRateLimited)
	}
	if len(h.RateLimitedProviders) != 1 || h.RateLimitedProviders[0] != "livegolf" {
		t.Errorf("providers = %v, want [livegolf]", h.RateLimitedProviders)
	}
}

// The floor outranks staleness when both are true, because it names a cause
// and a remedy where "data_starved" only names a symptom.
func TestRateLimitOutranksStarvation(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()
	r.RecordData("golf")
	c.add(2 * time.Hour)
	r.MarkRateLimited("livegolf", true)

	if h := r.Health(DefaultHealthWindow); h.Status != StatusRateLimited {
		t.Errorf("status = %q, want %q when both conditions hold", h.Status, StatusRateLimited)
	}
}

func TestClearingTheFloorRestoresHealth(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()
	r.RecordData("golf")
	r.MarkRateLimited("livegolf", true)
	if h := r.Health(0); h.OK {
		t.Fatal("precondition: should be unhealthy while throttled")
	}

	r.MarkRateLimited("livegolf", false)
	h := r.Health(0)
	if !h.OK {
		t.Fatalf("clearing the floor should restore health, got %s: %s", h.Status, h.Detail)
	}
	if h.RateLimited {
		t.Error("RateLimited should be false once the floor lifts")
	}
}

// A box that has just booted has not had a chance to poll anything. Reporting
// that as starved makes every rolling restart look like an estate-wide outage.
func TestStartupGraceIsHealthyWithNoDataYet(t *testing.T) {
	r, c := newTestRegistry()
	c.add(10 * time.Second)

	h := r.Health(DefaultHealthWindow)
	if !h.OK {
		t.Fatalf("a freshly booted box should be healthy, got %s: %s", h.Status, h.Detail)
	}
	if h.Status != StatusStarting {
		t.Errorf("status = %q, want %q", h.Status, StatusStarting)
	}
}

func TestGraceExpiresIntoStarvation(t *testing.T) {
	r, c := newTestRegistry()
	c.add(startupGrace + time.Second)

	h := r.Health(DefaultHealthWindow)
	if h.OK {
		t.Fatal("the grace period must expire")
	}
	if h.Status != StatusDataStarved {
		t.Errorf("status = %q, want %q", h.Status, StatusDataStarved)
	}
}

// A successful poll that carried no records does NOT make the box healthy —
// the licence is being paid for data, not for reachability. But it must be
// reported, because it is the only evidence that separates "quiet" from
// "broken", and an operator woken at 04:00 needs exactly that distinction.
func TestAnEmptyPollIsReportedButDoesNotConferHealth(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()
	r.RecordPoll("afl")

	h := r.Health(DefaultHealthWindow)
	if h.OK {
		t.Error("a poll with no records must not count as data")
	}
	if h.LastPollSport != "afl" {
		t.Errorf("last poll sport = %q, want afl", h.LastPollSport)
	}
	if h.PollAgeSeconds < 0 {
		t.Error("a successful poll should be reported with an age")
	}
	if h.DataAgeSeconds >= 0 {
		t.Error("data age should be negative when no data has ever arrived")
	}
}

func TestHandlerReturns200WhenHealthy(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()
	r.RecordData("soccer")

	rec := httptest.NewRecorder()
	r.HealthHandler(DefaultHealthWindow).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("code = %d, want 200", rec.Code)
	}
	var h Health
	if err := json.Unmarshal(rec.Body.Bytes(), &h); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if !h.OK {
		t.Error("body disagrees with the status code")
	}
}

func TestHandlerReturns503WhenStarved(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()

	rec := httptest.NewRecorder()
	r.HealthHandler(DefaultHealthWindow).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("code = %d, want 503", rec.Code)
	}
	// The body must still explain itself. A 503 with no reason forces whoever
	// was paged into a second request against a box that is already unwell.
	var h Health
	if err := json.Unmarshal(rec.Body.Bytes(), &h); err != nil {
		t.Fatalf("a 503 must still carry a JSON body: %v", err)
	}
	if h.Status != StatusDataStarved || h.Detail == "" {
		t.Errorf("503 body did not explain itself: %+v", h)
	}
}

func TestHandlerIsNotCacheable(t *testing.T) {
	r, _ := newTestRegistry()
	rec := httptest.NewRecorder()
	r.HealthHandler(0).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store; a cached probe reports history", got)
	}
}

func TestHeadRequestReturnsStatusWithoutABody(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()
	r.RecordData("soccer")

	rec := httptest.NewRecorder()
	r.HealthHandler(0).ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("code = %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("HEAD should carry no body, got %d bytes", rec.Body.Len())
	}
}

// Per-sport freshness is in the payload so a 503 says which feed died.
func TestPerSportFreshnessIsReported(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()
	r.RecordData("soccer")
	r.RecordPoll("afl")

	h := r.Health(DefaultHealthWindow)
	seen := map[string]bool{}
	for _, s := range h.Sports {
		seen[s.Sport] = true
	}
	if !seen["soccer"] || !seen["afl"] {
		t.Errorf("both sports should appear in the breakdown, got %v", h.Sports)
	}
}

// The probe is read by a scraper while the ingest path is writing. This is the
// case the mutex split exists for.
func TestHealthIsSafeUnderConcurrentRecording(t *testing.T) {
	r, _ := newTestRegistry()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			r.RecordData("soccer")
			r.RecordPoll("afl")
			r.MarkRateLimited("livegolf", i%2 == 0)
		}
	}()
	for i := 0; i < 500; i++ {
		_ = r.Health(DefaultHealthWindow)
	}
	<-done
}

// The probe must be reachable on the metrics listener, not only constructible.
// A handler that exists but is not routed is the failure this catches.
func TestHealthIsMountedOnTheMetricsListener(t *testing.T) {
	r, c := newTestRegistry()
	c.settle()
	r.RecordData("soccer")

	_, srv, err := r.ServeMetrics(":0", "offload-ingest")
	if err != nil {
		t.Fatalf("ServeMetrics: %v", err)
	}

	for _, tc := range []struct {
		path string
		want int
	}{
		{"/health", http.StatusOK},
		{"/metrics", http.StatusOK},
	} {
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != tc.want {
			t.Errorf("%s = %d, want %d", tc.path, rec.Code, tc.want)
		}
	}

	// And it reports the unhealthy case through the same route.
	c.add(2 * time.Hour)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("stale /health = %d, want 503", rec.Code)
	}
}
