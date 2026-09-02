// Package poller drives concurrent REST polling workers. Each worker owns a
// fixture, polls its provider endpoint on an interval, and publishes whatever
// the poll returned. It is the steady-state half of the load profile: the
// webhook package supplies the bursty half.
package poller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

// Publisher is the sink workers write to; producer.Kafka satisfies it.
type Publisher interface {
	Publish(ctx context.Context, msgs ...generators.Message) error
}

// Config controls the polling load profile.
type Config struct {
	// Sports to poll. Empty means all 13.
	Sports []generators.Sport
	// Kinds selects which feed kinds to poll. Empty means all four. The poller
	// is the right emitter for the slow, heavy kinds — box scores, player stat
	// arrays, play-by-play — while the webhook emitter carries telemetry.
	Kinds []generators.FeedKind
	// Workers is the number of concurrent polling goroutines. Each one owns a
	// distinct fixture, so this is also the partition-key cardinality.
	Workers int
	// Interval is the base poll period; Jitter is added uniformly at random to
	// keep workers from synchronising into a thundering herd.
	Interval time.Duration
	Jitter   time.Duration
	// Timeout bounds a single poll (HTTP request or mock fetch).
	Timeout time.Duration
	// EventsPerPoll is how many payloads a single poll returns. SportsDataIO
	// endpoints return one document per call, so 1 is the faithful setting;
	// raise it to model a client sweeping several fixtures per tick.
	EventsPerPoll int
	// MaxPolls stops each worker after N polls. Zero means run until cancelled.
	MaxPolls int64
	// Seed makes a run reproducible.
	Seed int64
	// Fetcher supplies events. Defaults to a generator-backed mock provider.
	Fetcher Fetcher
	// OnError is called for every failed poll. Defaults to a no-op.
	OnError func(worker int, err error)
}

// DefaultConfig returns a moderate steady-state profile.
func DefaultConfig() Config {
	return Config{
		Sports:        generators.AllSports,
		Kinds:         []generators.FeedKind{generators.FeedBoxScore, generators.FeedPlayerStats, generators.FeedPlayByPlay},
		Workers:       26,
		Interval:      2 * time.Second,
		Jitter:        500 * time.Millisecond,
		Timeout:       5 * time.Second,
		EventsPerPoll: 1,
		Seed:          time.Now().UnixNano(),
	}
}

func (c *Config) validate() error {
	switch {
	case c.Workers <= 0:
		return errors.New("poller: workers must be > 0")
	case c.Interval <= 0:
		return errors.New("poller: interval must be > 0")
	case c.Jitter < 0:
		return errors.New("poller: jitter must be >= 0")
	case c.EventsPerPoll <= 0:
		return errors.New("poller: events per poll must be > 0")
	}
	if len(c.Sports) == 0 {
		c.Sports = generators.AllSports
	}
	for _, s := range c.Sports {
		if !s.Valid() {
			return fmt.Errorf("poller: unknown sport %q", s)
		}
	}
	if len(c.Kinds) == 0 {
		c.Kinds = generators.AllKinds
	}
	for _, k := range c.Kinds {
		if !k.Valid() {
			return fmt.Errorf("poller: unknown feed kind %q", k)
		}
	}
	if c.Timeout <= 0 {
		c.Timeout = 5 * time.Second
	}
	if c.Seed == 0 {
		c.Seed = time.Now().UnixNano()
	}
	if c.OnError == nil {
		c.OnError = func(int, error) {}
	}
	return nil
}
