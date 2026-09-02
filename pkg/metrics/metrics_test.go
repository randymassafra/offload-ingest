package metrics

import (
	"sync"
	"testing"
	"time"
)

// TestRateWindowExpiresOldBuckets is the reason this is a sliding window rather
// than a since-start average: an operator looking at the dashboard wants the
// rate NOW, and a mean since boot smooths away exactly the burst that tripped a
// 429.
func TestRateWindowExpiresOldBuckets(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	w := NewRateWindow(func() time.Time { return clock })

	for i := 0; i < 60; i++ {
		w.Mark()
	}
	if got := w.PerMinute(); got != 60 {
		t.Fatalf("PerMinute = %d, want 60", got)
	}
	if got := w.PerSecond(); got != 1 {
		t.Errorf("PerSecond = %v, want 1", got)
	}

	// Half a minute on: the marks are still inside the window.
	clock = clock.Add(30 * time.Second)
	if got := w.PerMinute(); got != 60 {
		t.Errorf("PerMinute = %d after 30s, want the marks still counted", got)
	}

	// Past the window: they must age out completely.
	clock = clock.Add(31 * time.Second)
	if got := w.PerMinute(); got != 0 {
		t.Errorf("PerMinute = %d after the window passed, want 0", got)
	}
}

// TestRateWindowHandlesLongIdleGaps guards the wraparound arithmetic: a feed
// polling every 30 minutes must not have a stale bucket resurface as traffic.
func TestRateWindowHandlesLongIdleGaps(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	w := NewRateWindow(func() time.Time { return clock })
	for i := 0; i < 10; i++ {
		w.Mark()
	}
	clock = clock.Add(45 * time.Minute)
	if got := w.PerMinute(); got != 0 {
		t.Errorf("PerMinute = %d after a 45-minute idle, want 0", got)
	}
	w.Mark()
	if got := w.PerMinute(); got != 1 {
		t.Errorf("PerMinute = %d after one fresh mark, want 1", got)
	}
}

func TestRateWindowPartialExpiry(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	w := NewRateWindow(func() time.Time { return clock })

	w.Mark() // second 0
	clock = clock.Add(40 * time.Second)
	w.Mark() // second 40
	if got := w.PerMinute(); got != 2 {
		t.Fatalf("PerMinute = %d, want 2", got)
	}
	// Second 65: the first mark has aged out, the second has not.
	clock = clock.Add(25 * time.Second)
	if got := w.PerMinute(); got != 1 {
		t.Errorf("PerMinute = %d, want 1 (only the newer mark still counts)", got)
	}
}

func TestCounterAndGauge(t *testing.T) {
	var c Counter
	c.Inc()
	c.Add(4)
	if c.Value() != 5 {
		t.Errorf("counter = %d, want 5", c.Value())
	}
	var g Gauge
	g.Set(2.5)
	if g.Value() != 2.5 {
		t.Errorf("gauge = %v, want 2.5", g.Value())
	}
	g.Set(-1)
	if g.Value() != -1 {
		t.Errorf("gauge should move in both directions, got %v", g.Value())
	}
}

// TestSportIsCreatedOnceUnderConcurrency: every poller goroutine calls Sport()
// for its vertical, and two of them racing must not end up with two registries
// that each count half the traffic.
func TestSportIsCreatedOnceUnderConcurrency(t *testing.T) {
	r := NewRegistry(time.Now)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Sport("football").Requests.Inc()
		}()
	}
	wg.Wait()
	if got := r.Sport("football").Requests.Value(); got != 50 {
		t.Errorf("counted %d of 50 increments; the per-sport registry raced", got)
	}
}

func TestSnapshotRanksBusiestFirst(t *testing.T) {
	r := NewRegistry(time.Now)
	r.Sport("handball").Requests.Add(2)
	r.Sport("football").Requests.Add(90)
	r.Sport("rugby").Requests.Add(30)
	r.Requests.Add(122)

	snap := r.Snapshot()
	if len(snap.Sports) != 3 {
		t.Fatalf("got %d sports, want 3", len(snap.Sports))
	}
	if snap.Sports[0].Sport != "football" {
		t.Errorf("busiest sport is %q, want football", snap.Sports[0].Sport)
	}
	if snap.Sports[2].Sport != "handball" {
		t.Errorf("quietest sport is %q, want handball", snap.Sports[2].Sport)
	}
	if snap.Requests != 122 {
		t.Errorf("total requests = %d, want 122", snap.Requests)
	}
}

func TestUptimeUsesTheInjectedClock(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	r := NewRegistry(func() time.Time { return clock })
	clock = clock.Add(90 * time.Second)
	if got := r.Uptime(); got != 90*time.Second {
		t.Errorf("uptime = %s, want 90s", got)
	}
}
