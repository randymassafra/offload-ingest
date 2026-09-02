package poller

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

// Stats is a snapshot of pool activity.
type Stats struct {
	Polls        int64         `json:"polls"`
	Events       int64         `json:"events"`
	Errors       int64         `json:"errors"`
	Workers      int           `json:"workers"`
	LatencyAvgMs float64       `json:"poll_latency_avg_ms"`
	Elapsed      time.Duration `json:"-"`
}

// EventsPerSecond is the observed throughput over the pool's lifetime.
func (s Stats) EventsPerSecond() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.Events) / s.Elapsed.Seconds()
}

// Pool is a fixed set of goroutines each polling one fixture on an interval.
type Pool struct {
	cfg     Config
	sink    Publisher
	fetcher Fetcher

	polls     atomic.Int64
	events    atomic.Int64
	errs      atomic.Int64
	latencyNs atomic.Int64

	started time.Time
	wg      sync.WaitGroup
	once    sync.Once
}

// New builds a pool. When cfg.Fetcher is nil a generator-backed mock provider
// is created, one generator per worker.
func New(cfg Config, sink Publisher) (*Pool, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	fetcher := cfg.Fetcher
	if fetcher == nil {
		mock, err := NewMockFetcher(cfg.Sports, cfg.Kinds, cfg.Workers, cfg.Seed)
		if err != nil {
			return nil, err
		}
		fetcher = mock
	}
	return &Pool{cfg: cfg, sink: sink, fetcher: fetcher}, nil
}

// Run starts every worker and blocks until ctx is cancelled or all workers have
// hit MaxPolls. It is safe to call Run once per Pool.
func (p *Pool) Run(ctx context.Context) error {
	p.started = time.Now()
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
	p.wg.Wait()
	return p.fetcher.Close()
}

// Start runs the pool in the background and returns a stop function that
// cancels it and waits for the workers to drain.
func (p *Pool) Start(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = p.Run(ctx)
	}()
	return func() {
		p.once.Do(cancel)
		<-done
	}
}

// worker owns one fixture for the life of the run.
func (p *Pool) worker(ctx context.Context, id int) {
	defer p.wg.Done()

	rnd := rand.New(rand.NewSource(p.cfg.Seed + int64(id)))
	// Stagger the first poll across the interval so N workers do not all fire
	// on the same tick at startup.
	if !sleepCtx(ctx, time.Duration(rnd.Int63n(int64(p.cfg.Interval)+1))) {
		return
	}

	var polls int64
	for {
		if ctx.Err() != nil {
			return
		}
		p.poll(ctx, id)
		polls++
		if p.cfg.MaxPolls > 0 && polls >= p.cfg.MaxPolls {
			return
		}

		wait := p.cfg.Interval
		if p.cfg.Jitter > 0 {
			wait += time.Duration(rnd.Int63n(int64(p.cfg.Jitter) + 1))
		}
		if !sleepCtx(ctx, wait) {
			return
		}
	}
}

// poll performs one fetch-and-publish cycle, bounded by the configured timeout.
func (p *Pool) poll(ctx context.Context, id int) {
	callCtx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()

	start := time.Now()
	msgs, err := p.fetcher.Fetch(callCtx, id, p.cfg.EventsPerPoll)
	p.latencyNs.Add(int64(time.Since(start)))
	p.polls.Add(1)

	if err != nil {
		p.errs.Add(1)
		p.cfg.OnError(id, err)
		return
	}
	if len(msgs) == 0 {
		return
	}
	if err := p.sink.Publish(callCtx, msgs...); err != nil {
		p.errs.Add(1)
		p.cfg.OnError(id, err)
		return
	}
	p.events.Add(int64(len(msgs)))
}

// Stats returns a snapshot; safe to call while the pool is running.
func (p *Pool) Stats() Stats {
	polls := p.polls.Load()
	var avg float64
	if polls > 0 {
		avg = float64(p.latencyNs.Load()) / float64(polls) / float64(time.Millisecond)
	}
	elapsed := time.Duration(0)
	if !p.started.IsZero() {
		elapsed = time.Since(p.started)
	}
	return Stats{
		Polls:        polls,
		Events:       p.events.Load(),
		Errors:       p.errs.Load(),
		Workers:      p.cfg.Workers,
		LatencyAvgMs: avg,
		Elapsed:      elapsed,
	}
}

// Sports returns the sports this pool is polling.
func (p *Pool) Sports() []generators.Sport { return p.cfg.Sports }

// Endpoints lists the provider endpoint each worker is imitating.
func (p *Pool) Endpoints() []generators.Endpoint {
	out := make([]generators.Endpoint, 0, p.cfg.Workers)
	for w := 0; w < p.cfg.Workers; w++ {
		out = append(out, p.fetcher.Endpoint(w))
	}
	return out
}

// sleepCtx waits for d, returning false if ctx was cancelled first.
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
