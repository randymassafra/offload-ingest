package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"sync"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

// Discard encodes, counts and throws away. Use it to measure how fast the
// generators and emitters run with the broker taken out of the picture — it
// still pays the JSON marshalling cost, which for a 168 KB play-by-play
// payload is most of the work.
type Discard struct {
	counters
	observer  Observer
	projected int
}

func NewDiscard() *Discard { return &Discard{} }

func (d *Discard) Publish(_ context.Context, msgs ...generators.Message) error {
	now := time.Now()
	for _, m := range msgs {
		b, err := json.Marshal(m.Payload)
		if err != nil {
			d.errors.Add(1)
			if d.observer != nil {
				d.observer.ObservePublishError(m, err)
			}
			continue
		}
		d.published.Add(1)
		d.bytes.Add(int64(len(b)))
		if d.observer != nil {
			d.observer.ObservePublish(m, "(discarded)", d.projectedPartition(m),
				len(b), PollToPublish(m, now))
		}
	}
	return nil
}

// SetObserver attaches publication telemetry.
func (d *Discard) SetObserver(o Observer) { d.observer = o }

// SetProjectedPartitions enables partition-skew projection in dry runs.
//
// Without a broker there is no real partition, but the question the skew card
// answers — "will keying on fixture id make one partition hot?" — is decidable
// offline, because the balancer is a pure function of the key. Projecting it
// against an assumed partition count lets a venue find a hot-partition problem
// before deploying rather than after. The dashboard labels the figure projected
// so it is never mistaken for a broker measurement.
func (d *Discard) SetProjectedPartitions(n int) { d.projected = n }

// projectedPartition reproduces kafka-go's Hash balancer: FNV-1a over the key,
// modulo the partition count. Returns -1 when projection is disabled, which the
// dashboard renders as "unknown" rather than as partition zero.
func (d *Discard) projectedPartition(m generators.Message) int {
	if d.projected <= 0 {
		return -1
	}
	h := fnv.New32a()
	h.Write(m.Key())
	return int(h.Sum32() % uint32(d.projected))
}

func (d *Discard) Close() error { return nil }

// Writer publishes newline-delimited provider payloads to w. Handy for
// eyeballing the wire shape before pointing the load test at a real cluster,
// and for diffing a generated payload against a captured provider response.
type Writer struct {
	counters
	mu       sync.Mutex
	w        io.Writer
	envelope bool
}

// NewWriter writes the bare payload, exactly as it would reach Kafka.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// NewEnvelopeWriter wraps each payload with its routing metadata, which is
// easier to read when several feeds are interleaved on one stream.
func NewEnvelopeWriter(w io.Writer) *Writer { return &Writer{w: w, envelope: true} }

func (jw *Writer) Publish(_ context.Context, msgs ...generators.Message) error {
	jw.mu.Lock()
	defer jw.mu.Unlock()
	for _, m := range msgs {
		var (
			b   []byte
			err error
		)
		if jw.envelope {
			b, err = json.Marshal(m)
		} else {
			b, err = json.Marshal(m.Payload)
		}
		if err != nil {
			jw.errors.Add(1)
			return fmt.Errorf("producer: encode %s/%s: %w", m.Sport, m.Kind, err)
		}
		if _, err := jw.w.Write(append(b, '\n')); err != nil {
			jw.errors.Add(1)
			return fmt.Errorf("producer: write: %w", err)
		}
		jw.published.Add(1)
		jw.bytes.Add(int64(len(b)))
	}
	return nil
}

func (jw *Writer) Close() error { return nil }

// Multi fans one stream out to several publishers.
type Multi struct{ sinks []Publisher }

func NewMulti(sinks ...Publisher) *Multi { return &Multi{sinks: sinks} }

func (m *Multi) Publish(ctx context.Context, msgs ...generators.Message) error {
	for _, s := range m.sinks {
		if err := s.Publish(ctx, msgs...); err != nil {
			return err
		}
	}
	return nil
}

func (m *Multi) Close() error {
	var firstErr error
	for _, s := range m.sinks {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
