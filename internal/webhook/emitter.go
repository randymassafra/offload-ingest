// Package webhook models the event-driven half of the load profile: providers
// that stay silent and then push a tight burst of events when something
// happens on the field. Bursts are what expose batching and backpressure bugs
// that steady polling never reaches.
package webhook

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

// Publisher is the sink bursts are written to; producer.Kafka satisfies it.
type Publisher interface {
	Publish(ctx context.Context, msgs ...generators.Message) error
}

// Config describes the burst profile.
type Config struct {
	// Sports to emit for. Empty means all 13.
	Sports []generators.Sport
	// Kinds selects which feed kinds to burst. Empty means telemetry and
	// play-by-play — the two that genuinely arrive in bursts. Box scores are a
	// polled snapshot and do not belong on a push channel.
	Kinds []generators.FeedKind
	// Emitters is the number of independent burst sources running concurrently.
	Emitters int
	// BurstSize is the number of events in a burst; the actual size is drawn
	// uniformly from [BurstSize-BurstJitter, BurstSize+BurstJitter].
	BurstSize   int
	BurstJitter int
	// Interval is the mean gap between bursts. Real webhook traffic is bursty
	// in time as well as volume, so gaps are drawn from an exponential
	// distribution around this mean unless Uniform is set.
	Interval time.Duration
	Uniform  bool
	// InBurstDelay spaces events inside a burst. Zero publishes the whole burst
	// in a single call, which is the harshest case for the consumer.
	InBurstDelay time.Duration
	// MaxBursts stops each emitter after N bursts. Zero means run until cancelled.
	MaxBursts int64
	// PublishTimeout bounds a single publish call.
	PublishTimeout time.Duration
	// Seed makes a run reproducible.
	Seed int64
	// OnError is called for every failed publish. Defaults to a no-op.
	OnError func(emitter int, err error)
}

// DefaultConfig returns a spiky profile: short bursts, seconds apart.
func DefaultConfig() Config {
	return Config{
		Sports:         generators.AllSports,
		Kinds:          []generators.FeedKind{generators.FeedTelemetry, generators.FeedPlayByPlay},
		Emitters:       13,
		BurstSize:      25,
		BurstJitter:    10,
		Interval:       3 * time.Second,
		PublishTimeout: 10 * time.Second,
		Seed:           time.Now().UnixNano(),
	}
}

func (c *Config) validate() error {
	switch {
	case c.Emitters <= 0:
		return errors.New("webhook: emitters must be > 0")
	case c.BurstSize <= 0:
		return errors.New("webhook: burst size must be > 0")
	case c.BurstJitter < 0:
		return errors.New("webhook: burst jitter must be >= 0")
	case c.BurstJitter >= c.BurstSize:
		return errors.New("webhook: burst jitter must be smaller than burst size")
	case c.Interval <= 0:
		return errors.New("webhook: interval must be > 0")
	}
	if len(c.Sports) == 0 {
		c.Sports = generators.AllSports
	}
	for _, s := range c.Sports {
		if !s.Valid() {
			return fmt.Errorf("webhook: unknown sport %q", s)
		}
	}
	if len(c.Kinds) == 0 {
		c.Kinds = []generators.FeedKind{generators.FeedTelemetry, generators.FeedPlayByPlay}
	}
	for _, k := range c.Kinds {
		if !k.Valid() {
			return fmt.Errorf("webhook: unknown feed kind %q", k)
		}
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 10 * time.Second
	}
	if c.Seed == 0 {
		c.Seed = time.Now().UnixNano()
	}
	if c.OnError == nil {
		c.OnError = func(int, error) {}
	}
	return nil
}

// Stats is a snapshot of emitter activity.
type Stats struct {
	Bursts       int64         `json:"bursts"`
	Events       int64         `json:"events"`
	Errors       int64         `json:"errors"`
	Emitters     int           `json:"emitters"`
	PeakBurst    int64         `json:"peak_burst"`
	PublishAvgMs float64       `json:"publish_latency_avg_ms"`
	Elapsed      time.Duration `json:"-"`
}

// EventsPerSecond is the observed throughput over the emitter's lifetime.
func (s Stats) EventsPerSecond() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.Events) / s.Elapsed.Seconds()
}

// Emitter drives a set of burst sources, one feed each.
type Emitter struct {
	cfg   Config
	sink  Publisher
	feeds []generators.Feed

	bursts    atomic.Int64
	events    atomic.Int64
	errs      atomic.Int64
	peak      atomic.Int64
	publishNs atomic.Int64
	publishes atomic.Int64

	started time.Time
	wg      sync.WaitGroup
	once    sync.Once
}

// New builds an emitter with one feed per burst source, assigned round-robin
// across the (sport, kind) endpoints that exist for the requested selection.
func New(cfg Config, sink Publisher) (*Emitter, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	catalog, err := generators.NewAll(cfg.Sports, cfg.Kinds, cfg.Seed)
	if err != nil {
		return nil, err
	}
	feeds := make([]generators.Feed, 0, cfg.Emitters)
	for i := 0; i < cfg.Emitters; i++ {
		ep := catalog[i%len(catalog)].Endpoint()
		f, err := generators.NewNamed(ep.Sport, ep.Kind, ep.Name, cfg.Seed+int64(i)*15485863)
		if err != nil {
			return nil, err
		}
		feeds = append(feeds, f)
	}
	return &Emitter{cfg: cfg, sink: sink, feeds: feeds}, nil
}

// Run starts every burst source and blocks until ctx is cancelled or all
// sources have hit MaxBursts.
func (e *Emitter) Run(ctx context.Context) error {
	e.started = time.Now()
	for i := 0; i < e.cfg.Emitters; i++ {
		e.wg.Add(1)
		go e.source(ctx, i)
	}
	e.wg.Wait()
	return nil
}

// Start runs the emitter in the background and returns a stop function.
func (e *Emitter) Start(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.Run(ctx)
	}()
	return func() {
		e.once.Do(cancel)
		<-done
	}
}

func (e *Emitter) source(ctx context.Context, id int) {
	defer e.wg.Done()

	rnd := rand.New(rand.NewSource(e.cfg.Seed + int64(id)*31))
	// Spread the first burst so all sources do not fire together at startup.
	if !sleepCtx(ctx, e.nextGap(rnd)) {
		return
	}

	var n int64
	for {
		if ctx.Err() != nil {
			return
		}
		e.burst(ctx, id, rnd)
		n++
		if e.cfg.MaxBursts > 0 && n >= e.cfg.MaxBursts {
			return
		}
		if !sleepCtx(ctx, e.nextGap(rnd)) {
			return
		}
	}
}

// burst generates and publishes one burst of events for source id.
func (e *Emitter) burst(ctx context.Context, id int, rnd *rand.Rand) {
	size := e.cfg.BurstSize
	if e.cfg.BurstJitter > 0 {
		size += rnd.Intn(2*e.cfg.BurstJitter+1) - e.cfg.BurstJitter
	}
	if size < 1 {
		size = 1
	}

	f := e.feeds[id]
	msgs := make([]generators.Message, 0, size)
	for i := 0; i < size; i++ {
		msgs = append(msgs, f.Next())
	}

	e.bursts.Add(1)
	if int64(size) > e.peak.Load() {
		e.peak.Store(int64(size))
	}

	if e.cfg.InBurstDelay <= 0 {
		e.publish(ctx, id, msgs)
		return
	}
	// Drip mode: publish one at a time so the burst arrives as a rapid stream
	// rather than a single oversized batch.
	for _, m := range msgs {
		e.publish(ctx, id, []generators.Message{m})
		if !sleepCtx(ctx, e.cfg.InBurstDelay) {
			return
		}
	}
}

func (e *Emitter) publish(ctx context.Context, id int, msgs []generators.Message) {
	pubCtx, cancel := context.WithTimeout(ctx, e.cfg.PublishTimeout)
	defer cancel()

	start := time.Now()
	err := e.sink.Publish(pubCtx, msgs...)
	e.publishNs.Add(int64(time.Since(start)))
	e.publishes.Add(1)

	if err != nil {
		e.errs.Add(1)
		e.cfg.OnError(id, err)
		return
	}
	e.events.Add(int64(len(msgs)))
}

// Endpoints lists the provider endpoint each burst source is imitating.
func (e *Emitter) Endpoints() []generators.Endpoint {
	out := make([]generators.Endpoint, 0, len(e.feeds))
	for _, f := range e.feeds {
		out = append(out, f.Endpoint())
	}
	return out
}

// nextGap draws the delay before the next burst. Exponential gaps around the
// configured mean reproduce the clustering seen in real webhook traffic.
func (e *Emitter) nextGap(rnd *rand.Rand) time.Duration {
	if e.cfg.Uniform {
		return e.cfg.Interval
	}
	d := time.Duration(rnd.ExpFloat64() * float64(e.cfg.Interval))
	// Clamp the tail so a single unlucky draw does not idle a source for minutes.
	if max := 5 * e.cfg.Interval; d > max {
		d = max
	}
	return d
}

// Stats returns a snapshot; safe to call while the emitter is running.
func (e *Emitter) Stats() Stats {
	var avg float64
	if n := e.publishes.Load(); n > 0 {
		avg = float64(e.publishNs.Load()) / float64(n) / float64(time.Millisecond)
	}
	elapsed := time.Duration(0)
	if !e.started.IsZero() {
		elapsed = time.Since(e.started)
	}
	return Stats{
		Bursts:       e.bursts.Load(),
		Events:       e.events.Load(),
		Errors:       e.errs.Load(),
		Emitters:     e.cfg.Emitters,
		PeakBurst:    e.peak.Load(),
		PublishAvgMs: avg,
		Elapsed:      elapsed,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
