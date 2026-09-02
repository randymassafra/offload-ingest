package poller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

// recorder is a Publisher that counts everything it is handed.
type recorder struct {
	mu     sync.Mutex
	events []generators.Message
	fail   error
}

func (r *recorder) Publish(_ context.Context, msgs ...generators.Message) error {
	if r.fail != nil {
		return r.fail
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, msgs...)
	return nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

func TestPoolPublishesExpectedVolume(t *testing.T) {
	sink := &recorder{}
	pool, err := New(Config{
		Sports:        generators.AllSports,
		Workers:       8,
		Interval:      5 * time.Millisecond,
		Jitter:        0,
		EventsPerPoll: 3,
		MaxPolls:      5,
		Seed:          1,
	}, sink)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := 8 * 5 * 3
	if got := sink.count(); got != want {
		t.Errorf("published %d events, want %d", got, want)
	}
	stats := pool.Stats()
	if stats.Polls != 8*5 {
		t.Errorf("polls = %d, want 40", stats.Polls)
	}
	if stats.Errors != 0 {
		t.Errorf("errors = %d, want 0", stats.Errors)
	}
}

// TestWorkersOwnDistinctFeeds is the property that makes per-fixture ordering
// hold: two workers must never share a feed instance, because feeds are
// stateful and a shared one would interleave two workers' sequence numbers.
func TestWorkersOwnDistinctFeeds(t *testing.T) {
	f, err := NewMockFetcher(generators.AllSports, generators.AllKinds, 13, 5)
	if err != nil {
		t.Fatalf("NewMockFetcher: %v", err)
	}
	seen := map[generators.Feed]bool{}
	for i := 0; i < 13; i++ {
		feed := f.Feed(i)
		if seen[feed] {
			t.Fatalf("worker %d shares a feed instance with an earlier worker", i)
		}
		seen[feed] = true
	}
}

// TestSequenceIsPerFeedMonotonic guards the ordering contract a Flink job
// relies on to detect gaps.
func TestSequenceIsPerFeedMonotonic(t *testing.T) {
	f, err := NewMockFetcher([]generators.Sport{generators.SportNFL}, []generators.FeedKind{generators.FeedBoxScore}, 1, 9)
	if err != nil {
		t.Fatalf("NewMockFetcher: %v", err)
	}
	// The sequence is per FIXTURE, not per feed: it restarts at 1 when a
	// fixture ends and the next one begins. That is the contract a consumer
	// needs — a gap within one fixture is a lost message, whereas a reset
	// alongside a new fixture id is a new game. Asserting an unbroken run
	// across a rollover would only pass on feeds whose fixtures happen to
	// outlast the test.
	var last int64
	var lastFixture string
	var rollovers int
	for i := 0; i < 200; i++ {
		msgs, err := f.Fetch(context.Background(), 0, 1)
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		m := msgs[0]
		switch {
		case m.FixtureID != lastFixture:
			if lastFixture != "" {
				rollovers++
			}
			if m.Sequence != 1 {
				t.Fatalf("new fixture %s started at sequence %d, want 1",
					m.FixtureID, m.Sequence)
			}
		case m.Sequence != last+1:
			t.Fatalf("sequence jumped %d -> %d within fixture %s",
				last, m.Sequence, m.FixtureID)
		}
		last, lastFixture = m.Sequence, m.FixtureID
	}
	if rollovers == 0 {
		t.Log("note: no rollover occurred in 200 ticks; the reset path went untested")
	}
}

// TestMessagesCarryRouting checks that every published message can be routed
// and keyed without inspecting its payload.
func TestMessagesCarryRouting(t *testing.T) {
	sink := &recorder{}
	pool, err := New(Config{
		Workers: 6, Interval: time.Millisecond, EventsPerPoll: 1, MaxPolls: 2, Seed: 4,
	}, sink)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := pool.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, m := range sink.events {
		switch {
		case m.Sport == "":
			t.Fatal("message with no sport")
		case m.Kind == "":
			t.Fatal("message with no feed kind")
		case m.Endpoint == "":
			t.Fatal("message with no endpoint")
		case len(m.Key()) == 0:
			t.Fatal("message with an empty partition key")
		case m.Payload == nil:
			t.Fatal("message with a nil payload")
		}
	}
}

func TestPoolRecordsPublishErrors(t *testing.T) {
	var mu sync.Mutex
	var observed int

	sink := &recorder{fail: errors.New("broker down")}
	pool, err := New(Config{
		Workers:       2,
		Interval:      time.Millisecond,
		EventsPerPoll: 1,
		MaxPolls:      3,
		Seed:          2,
		OnError: func(int, error) {
			mu.Lock()
			observed++
			mu.Unlock()
		},
	}, sink)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := pool.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := pool.Stats().Errors; got != 6 {
		t.Errorf("errors = %d, want 6", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if observed != 6 {
		t.Errorf("OnError called %d times, want 6", observed)
	}
}

func TestPoolStopsOnContextCancel(t *testing.T) {
	pool, err := New(Config{
		Workers:       4,
		Interval:      time.Millisecond,
		EventsPerPoll: 1,
		Seed:          3,
	}, &recorder{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = pool.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestConfigValidation(t *testing.T) {
	cases := map[string]Config{
		"zero workers":  {Workers: 0, Interval: time.Second, EventsPerPoll: 1},
		"zero interval": {Workers: 1, Interval: 0, EventsPerPoll: 1},
		"zero events":   {Workers: 1, Interval: time.Second, EventsPerPoll: 0},
		"bad sport":     {Workers: 1, Interval: time.Second, EventsPerPoll: 1, Sports: []generators.Sport{"chess"}},
		"bad kind":      {Workers: 1, Interval: time.Second, EventsPerPoll: 1, Kinds: []generators.FeedKind{"boxscores"}},
	}
	for name, cfg := range cases {
		if _, err := New(cfg, &recorder{}); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
