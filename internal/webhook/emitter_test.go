package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

type recorder struct {
	mu     sync.Mutex
	events []generators.Message
	calls  int
}

func (r *recorder) Publish(_ context.Context, msgs ...generators.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.events = append(r.events, msgs...)
	return nil
}

func (r *recorder) snapshot() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events), r.calls
}

func TestEmitterProducesBurstsWithinJitterBounds(t *testing.T) {
	sink := &recorder{}
	em, err := New(Config{
		Emitters:    4,
		BurstSize:   10,
		BurstJitter: 3,
		Interval:    time.Millisecond,
		Uniform:     true,
		MaxBursts:   5,
		Seed:        11,
	}, sink)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := em.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	stats := em.Stats()
	if stats.Bursts != 20 {
		t.Errorf("bursts = %d, want 20", stats.Bursts)
	}
	// With InBurstDelay unset each burst is one publish call.
	events, calls := sink.snapshot()
	if calls != 20 {
		t.Errorf("publish calls = %d, want 20 (one per burst)", calls)
	}
	if lo, hi := 20*7, 20*13; events < lo || events > hi {
		t.Errorf("events = %d, want within [%d,%d]", events, lo, hi)
	}
	if stats.PeakBurst > 13 {
		t.Errorf("peak burst = %d, exceeds size+jitter", stats.PeakBurst)
	}
}

// TestEmitterDefaultsToPushKinds checks that the burst emitter carries the
// feed kinds that genuinely arrive by push, not the polled box-score snapshot.
func TestEmitterDefaultsToPushKinds(t *testing.T) {
	sink := &recorder{}
	em, err := New(Config{Emitters: 6, BurstSize: 5, Interval: time.Millisecond, Uniform: true, MaxBursts: 1, Seed: 12}, sink)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := em.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, m := range sink.events {
		if m.Kind != generators.FeedTelemetry && m.Kind != generators.FeedPlayByPlay {
			t.Fatalf("burst carried %q, want telemetry or playbyplay", m.Kind)
		}
	}
	for _, ep := range em.Endpoints() {
		if ep.Path == "" {
			t.Fatal("emitter endpoint has no path")
		}
	}
}

func TestEmitterDripModePublishesOneAtATime(t *testing.T) {
	sink := &recorder{}
	em, _ := New(Config{
		Emitters: 1, BurstSize: 6, Interval: time.Millisecond, Uniform: true,
		InBurstDelay: time.Microsecond, MaxBursts: 1, Seed: 13,
	}, sink)
	if err := em.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	events, calls := sink.snapshot()
	if calls != events {
		t.Errorf("drip mode: %d calls for %d events, want one call per event", calls, events)
	}
}

func TestEmitterStopsOnContextCancel(t *testing.T) {
	em, _ := New(Config{Emitters: 3, BurstSize: 2, Interval: time.Millisecond, Seed: 14}, &recorder{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = em.Run(ctx) }()

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
		"zero emitters":  {Emitters: 0, BurstSize: 5, Interval: time.Second},
		"zero burst":     {Emitters: 1, BurstSize: 0, Interval: time.Second},
		"jitter too big": {Emitters: 1, BurstSize: 5, BurstJitter: 5, Interval: time.Second},
		"zero interval":  {Emitters: 1, BurstSize: 5, Interval: 0},
		"bad sport":      {Emitters: 1, BurstSize: 5, Interval: time.Second, Sports: []generators.Sport{"darts"}},
		"bad kind":       {Emitters: 1, BurstSize: 5, Interval: time.Second, Kinds: []generators.FeedKind{"boxscores"}},
	}
	for name, cfg := range cases {
		if _, err := New(cfg, &recorder{}); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestReceiverAcceptsBatchAndSingleEvent(t *testing.T) {
	sink := &recorder{}
	recv, err := NewReceiver(ReceiverConfig{Addr: ":0", Secret: "s3cret"}, sink)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	srv := httptest.NewServer(recv.Handler())
	defer srv.Close()

	g, err := generators.New(generators.SportNBA, generators.FeedBoxScore, 1)
	if err != nil {
		t.Fatalf("generators.New: %v", err)
	}
	batch := []any{g.Next().Payload, g.Next().Payload}
	body, _ := json.Marshal(batch)

	post := func(payload []byte, secret string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/webhook", bytes.NewReader(payload))
		if secret != "" {
			req.Header.Set("X-Offload-Signature", secret)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		return resp
	}

	resp := post(body, "s3cret")
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("batch: status = %d, want 202", resp.StatusCode)
	}

	single, _ := json.Marshal(g.Next().Payload)
	resp = post(single, "s3cret")
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("single: status = %d, want 202", resp.StatusCode)
	}

	resp = post(body, "wrong")
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad signature: status = %d, want 401", resp.StatusCode)
	}

	if got := recv.Stats().Events; got != 3 {
		t.Errorf("received %d events, want 3", got)
	}
	if events, _ := sink.snapshot(); events != 3 {
		t.Errorf("forwarded %d events, want 3", events)
	}
}
