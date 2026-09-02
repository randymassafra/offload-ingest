// Package producer publishes generated payloads to Kafka.
//
// The message value is the provider's payload verbatim — no envelope of ours
// wrapped around it. A Flink job reading this topic deserializes exactly the
// bytes it would deserialize from the real provider. Everything the pipeline
// needs for routing travels in Kafka headers instead, where it does not
// contaminate the schema:
//
//	sport      nfl, epl, f1, ...
//	feed       boxscore | playbyplay | playerstats | telemetry
//	endpoint   the provider route the payload imitates
//	model      the provider model name, e.g. "Leaderboard"
//	sequence   monotonic per fixture, for gap detection downstream
//	verified   "true" unless the schema is modeled rather than observed
//
// The partition key is the provider's fixture id, so every message for one
// game lands on one partition in order.
package producer

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

// Publisher is the sink every emitter writes to.
type Publisher interface {
	// Publish writes messages in order. It must be safe for concurrent use.
	Publish(ctx context.Context, msgs ...generators.Message) error
	// Close flushes any buffered batches and releases resources.
	Close() error
}

// Stats is a snapshot of publisher counters.
type Stats struct {
	Published int64 `json:"published"`
	Bytes     int64 `json:"bytes"`
	Errors    int64 `json:"errors"`
}

// counters is embedded by every Publisher implementation.
type counters struct {
	published atomic.Int64
	bytes     atomic.Int64
	errors    atomic.Int64
}

func (c *counters) Stats() Stats {
	return Stats{
		Published: c.published.Load(),
		Bytes:     c.bytes.Load(),
		Errors:    c.errors.Load(),
	}
}

// StatsReporter is implemented by publishers that expose counters.
type StatsReporter interface{ Stats() Stats }

// Observer receives per-message publication telemetry.
//
// An interface, and optional, so that internal/producer keeps no dependency on
// the metrics package: the sink is the lowest layer in the pipeline and should
// not know what is being graphed above it. The runtime supplies an adapter.
type Observer interface {
	// ObservePublish is called once per message accepted by the broker. The
	// latency is the poll-to-Kafka delta — how long the record took to travel
	// from the provider response to the topic — which is the DDS's mandated
	// latency signal. Partition is -1 when the sink cannot attribute one.
	ObservePublish(m generators.Message, topic string, partition, bytes int, latency time.Duration)
	// ObservePublishError is called once per message that failed to publish.
	ObservePublishError(m generators.Message, err error)
}

// PollToPublish is the age of a message at the moment it is handed to a sink.
//
// Message.Emitted is stamped when the provider response was received, and is
// carried unchanged through the streamer, so this really is end-to-end rather
// than a measure of the last hop. A zero or future Emitted yields zero rather
// than a negative latency, which would poison a histogram.
func PollToPublish(m generators.Message, now time.Time) time.Duration {
	if m.Emitted.IsZero() {
		return 0
	}
	d := now.Sub(m.Emitted)
	if d < 0 {
		return 0
	}
	return d
}
