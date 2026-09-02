package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

// MultiStreamer merges several sources into one DataStreamer.
//
// # Why compose rather than special-case
//
// Golf comes from a different vendor on a different cadence, but everything
// downstream of the streamer — scope enforcement, the metrics, the Kafka
// producer — must treat it identically to an API-Sports sweep. Composing at the
// DataStreamer seam gets that for free: the publish path never learns there is
// more than one source, so a record cannot reach a topic by a route that skips
// the licence check.
//
// The alternative, running golf as its own goroutine publishing straight to the
// sink, would work today and would quietly rot: the next person adding a
// provider copies that goroutine, and one of the copies eventually forgets to
// wrap the sink.
//
// # Why the sources run concurrently
//
// Polling them in turn does not work, and the first version of this did exactly
// that. ProductionStreamer.Next blocks internally until one of its verticals is
// due — up to thirty seconds — so a sequential loop starves every source behind
// it. Golf was wired in, enabled, and produced nothing at all for the length of
// a run because the API-Sports streamer never returned control.
//
// Each source therefore gets its own goroutine feeding a shared channel. A
// source blocking on its own schedule now blocks only itself, which is what
// "different cadence" has to mean in practice.
type MultiStreamer struct {
	sources []DataStreamer
	log     *slog.Logger

	start sync.Once
	out   chan []generators.Message
	// stop cancels the fan-in goroutines when the streamer is closed, so a
	// restart in the same process does not leak one worker per source.
	stop context.CancelFunc
	wg   sync.WaitGroup
}

// NewMultiStreamer composes sources. Nil sources are ignored so a caller can
// pass an optional streamer without guarding it.
func NewMultiStreamer(log *slog.Logger, sources ...DataStreamer) *MultiStreamer {
	if log == nil {
		log = slog.Default()
	}
	live := make([]DataStreamer, 0, len(sources))
	for _, s := range sources {
		if s != nil {
			live = append(live, s)
		}
	}
	return &MultiStreamer{
		sources: live, log: log,
		// Buffered by source count so a slow consumer cannot deadlock a
		// producer mid-batch.
		out: make(chan []generators.Message, len(live)),
	}
}

// Next returns the next batch any source produces.
func (m *MultiStreamer) Next(ctx context.Context) ([]generators.Message, error) {
	if len(m.sources) == 0 {
		return nil, errors.New("ingest: no streamers configured")
	}
	m.start.Do(func() { m.launch(ctx) })

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msgs, ok := <-m.out:
		if !ok {
			return nil, context.Canceled
		}
		return msgs, nil
	}
}

// launch starts one fan-in goroutine per source.
func (m *MultiStreamer) launch(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	m.stop = cancel

	for _, src := range m.sources {
		source := src
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			// A source that keeps failing is backed off rather than spun on:
			// an unreachable vendor should not burn a core reporting so.
			var consecutiveErrors int
			for {
				if ctx.Err() != nil {
					return
				}
				msgs, err := source.Next(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					consecutiveErrors++
					m.log.Warn("ingest: source failed",
						"sports", source.Sports(), "err", err,
						"consecutive", consecutiveErrors)
					if !sleepCtx(ctx, backoffFor(consecutiveErrors)) {
						return
					}
					continue
				}
				consecutiveErrors = 0
				if len(msgs) == 0 {
					// Nothing due. Yield briefly so a source that returns
					// immediately does not spin.
					if !sleepCtx(ctx, time.Second) {
						return
					}
					continue
				}
				select {
				case <-ctx.Done():
					return
				case m.out <- msgs:
				}
			}
		}()
	}
}

// backoffFor grows the pause between failed polls, capped so a source that
// recovers is picked up again within a minute.
func backoffFor(consecutive int) time.Duration {
	d := time.Duration(consecutive) * 5 * time.Second
	if d > time.Minute {
		d = time.Minute
	}
	return d
}

// sleepCtx waits, reporting false when the context ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// Sports is the union across sources.
func (m *MultiStreamer) Sports() []generators.Sport {
	seen := map[generators.Sport]bool{}
	var out []generators.Sport
	for _, s := range m.sources {
		for _, sp := range s.Sports() {
			if !seen[sp] {
				seen[sp] = true
				out = append(out, sp)
			}
		}
	}
	return out
}

// Mode reports production when any source is live.
func (m *MultiStreamer) Mode() Mode {
	for _, s := range m.sources {
		if s.Mode() == ModeProduction {
			return ModeProduction
		}
	}
	return ModeSimulation
}

// Close stops the fan-in and closes every source.
//
// The first error is returned while the rest are still closed: a failure to
// close one source must not leak the others.
func (m *MultiStreamer) Close() error {
	if m.stop != nil {
		m.stop()
	}
	m.wg.Wait()
	var first error
	for _, s := range m.sources {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
