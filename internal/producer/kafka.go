package producer

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

// Config describes how the load generator connects to Kafka.
type Config struct {
	Brokers []string
	// Topic is the destination. TopicPerSport and TopicPerFeed treat it as a
	// prefix and append the sport and/or feed kind, e.g. "ingest.nfl.boxscore".
	// Splitting by feed kind is usually what you want: a 168 KB play-by-play
	// payload and a 186 byte telemetry row have nothing in common downstream.
	Topic         string
	TopicPerSport bool
	TopicPerFeed  bool

	ClientID     string
	Async        bool          // fire-and-forget; much higher throughput, no per-write error
	BatchSize    int           // messages per batch
	BatchBytes   int64         // max batch size in bytes
	BatchTimeout time.Duration // linger before flushing a partial batch
	WriteTimeout time.Duration
	RequiredAcks int    // 0 none, 1 leader, -1 all
	Compression  string // none | gzip | snappy | lz4 | zstd

	// Optional auth. Leave empty for a plaintext local broker.
	SASLMechanism string // plain | scram-sha-256 | scram-sha-512
	SASLUsername  string
	SASLPassword  string
	TLSEnabled    bool
	TLSSkipVerify bool
}

// DefaultConfig returns settings tuned for a high-throughput load test against
// a local broker: async batching, lz4 compression, leader acks only.
func DefaultConfig() Config {
	return Config{
		Brokers:      []string{"localhost:9092"},
		Topic:        "ingest.events",
		ClientID:     "offload-ingest-loadtest",
		Async:        true,
		BatchSize:    500,
		BatchBytes:   1 << 20,
		BatchTimeout: 25 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: int(kafka.RequireOne),
		Compression:  "lz4",
	}
}

// Kafka is a Publisher backed by a kafka-go Writer. One writer multiplexes all
// topics; per-fixture keys preserve ordering within a partition.
// Kafka publishes to a broker.
type Kafka struct {
	counters
	cfg    Config
	writer *kafka.Writer
	// observer is optional telemetry; nil in tests and in bare runs.
	observer Observer
	// inflight maps a queued message back to the pipeline message, so the
	// async completion callback can attribute a partition and a latency to it.
	// Keyed by the Kafka message's value pointer identity via index, which is
	// why the slice order is preserved end to end.
	mu      sync.Mutex
	pending map[string]pendingMsg
}

// pendingMsg carries what the completion callback needs but kafka.Message
// cannot: the originating pipeline message and when it was queued.
type pendingMsg struct {
	msg    generators.Message
	queued time.Time
}

// SetObserver attaches publication telemetry.
func (k *Kafka) SetObserver(o Observer) { k.observer = o }

// NewKafka builds a producer and validates the configuration. It does not dial
// the broker: kafka-go connects lazily on first write.
func NewKafka(cfg Config) (*Kafka, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("producer: at least one broker is required")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, errors.New("producer: topic is required")
	}
	transport, err := buildTransport(cfg)
	if err != nil {
		return nil, err
	}
	codec, err := compressionFor(cfg.Compression)
	if err != nil {
		return nil, err
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Balancer:     &kafka.Hash{}, // hash on Key => one partition per fixture
		Async:        cfg.Async,
		BatchSize:    cfg.BatchSize,
		BatchBytes:   cfg.BatchBytes,
		BatchTimeout: cfg.BatchTimeout,
		WriteTimeout: cfg.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(cfg.RequiredAcks),
		Compression:  codec,
		Transport:    transport,
		// AllowAutoTopicCreation keeps first-run load tests from failing against
		// a broker that has not been pre-provisioned.
		AllowAutoTopicCreation: true,
	}
	if !cfg.TopicPerSport {
		w.Topic = cfg.Topic
	}

	k := &Kafka{cfg: cfg, writer: w, pending: map[string]pendingMsg{}}
	if cfg.Async {
		// In async mode errors surface only through this callback, and so does
		// the partition the broker actually chose.
		w.Completion = func(msgs []kafka.Message, err error) {
			if err != nil {
				k.errors.Add(int64(len(msgs)))
				k.observeBatch(msgs, err)
				return
			}
			k.published.Add(int64(len(msgs)))
			for _, m := range msgs {
				k.bytes.Add(int64(len(m.Value)))
			}
			k.observeBatch(msgs, nil)
		}
	}
	return k, nil
}

// pendingKey identifies a queued message. Topic plus key plus sequence is
// unique per fixture update, which is exactly the granularity needed to match a
// completion callback back to the pipeline message that produced it.
func pendingKey(topic string, key []byte, seq int64) string {
	return topic + "\x00" + string(key) + "\x00" + strconv.FormatInt(seq, 10)
}

// trackPending records what the completion callback will need.
//
// kafka.Message has nowhere to carry our own identifiers — Headers are the wire
// contract and must not be polluted with bookkeeping — so the mapping is held
// here and consumed once.
func (k *Kafka) trackPending(msgs []generators.Message, out []kafka.Message) {
	now := time.Now()
	k.mu.Lock()
	defer k.mu.Unlock()
	// A bounded map: if completions stop arriving (a broker that never acks)
	// this must not grow without limit.
	if len(k.pending) > 50_000 {
		k.pending = map[string]pendingMsg{}
	}
	for i, m := range msgs {
		if i >= len(out) {
			break
		}
		k.pending[pendingKey(out[i].Topic, out[i].Key, m.Sequence)] = pendingMsg{msg: m, queued: now}
	}
}

// observeBatch reports each completed message to the observer.
func (k *Kafka) observeBatch(msgs []kafka.Message, writeErr error) {
	if k.observer == nil {
		return
	}
	now := time.Now()
	for _, km := range msgs {
		seq := sequenceOf(km)
		key := pendingKey(km.Topic, km.Key, seq)

		k.mu.Lock()
		p, ok := k.pending[key]
		if ok {
			delete(k.pending, key)
		}
		k.mu.Unlock()
		if !ok {
			continue
		}
		if writeErr != nil {
			k.observer.ObservePublishError(p.msg, writeErr)
			continue
		}
		k.observer.ObservePublish(p.msg, km.Topic, km.Partition,
			len(km.Value), PollToPublish(p.msg, now))
	}
}

// sequenceOf reads the sequence back out of the message headers, which is the
// only field that survives the round trip through kafka-go intact.
func sequenceOf(m kafka.Message) int64 {
	for _, h := range m.Headers {
		if h.Key == "sequence" {
			if v, err := strconv.ParseInt(string(h.Value), 10, 64); err == nil {
				return v
			}
		}
	}
	return 0
}

// TopicFor returns the destination topic for a message.
func (k *Kafka) TopicFor(m generators.Message) string {
	topic := k.cfg.Topic
	if k.cfg.TopicPerSport {
		topic = fmt.Sprintf("%s.%s", topic, m.Sport)
	}
	if k.cfg.TopicPerFeed {
		topic = fmt.Sprintf("%s.%s", topic, m.Kind)
	}
	return topic
}

// Publish encodes and writes messages. In async mode it returns as soon as the
// messages are queued; counters are updated from the completion callback.
func (k *Kafka) Publish(ctx context.Context, msgs ...generators.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]kafka.Message, 0, len(msgs))
	for _, m := range msgs {
		// The value is the provider's payload alone.
		value, err := json.Marshal(m.Payload)
		if err != nil {
			k.errors.Add(1)
			if k.observer != nil {
				k.observer.ObservePublishError(m, err)
			}
			return fmt.Errorf("producer: encode %s/%s %s: %w", m.Sport, m.Kind, m.FixtureID, err)
		}
		out = append(out, kafka.Message{
			Topic:   k.TopicFor(m),
			Key:     m.Key(),
			Value:   value,
			Time:    m.Emitted,
			Headers: headersFor(m),
		})
	}
	if k.observer != nil {
		k.trackPending(msgs, out)
	}

	if err := k.writer.WriteMessages(ctx, out...); err != nil {
		if !k.cfg.Async {
			k.errors.Add(int64(len(out)))
			if k.observer != nil {
				for _, m := range msgs {
					k.observer.ObservePublishError(m, err)
				}
			}
		}
		return fmt.Errorf("producer: write %d messages: %w", len(out), err)
	}
	if !k.cfg.Async {
		k.published.Add(int64(len(out)))
		for _, m := range out {
			k.bytes.Add(int64(len(m.Value)))
		}
		// In synchronous mode WriteMessages returns the messages with their
		// assigned partitions filled in, so attribution is exact.
		if k.observer != nil {
			k.observeBatch(out, nil)
		}
	}
	return nil
}

// headersFor builds the routing headers. Keeping this metadata out of the value
// is what lets a consumer deserialize the payload with a generated
// SportsDataIO schema and nothing else.
func headersFor(m generators.Message) []kafka.Header {
	return []kafka.Header{
		{Key: "sport", Value: []byte(m.Sport)},
		{Key: "feed", Value: []byte(m.Kind)},
		{Key: "endpoint", Value: []byte(m.Endpoint)},
		{Key: "model", Value: []byte(m.Model)},
		{Key: "sequence", Value: []byte(strconv.FormatInt(m.Sequence, 10))},
		{Key: "fixture", Value: []byte(m.FixtureID)},
	}
}

// Close flushes pending batches and closes the underlying writer.
func (k *Kafka) Close() error { return k.writer.Close() }

// WriterStats exposes kafka-go's own instrumentation for reporting.
func (k *Kafka) WriterStats() kafka.WriterStats { return k.writer.Stats() }

func buildTransport(cfg Config) (*kafka.Transport, error) {
	t := &kafka.Transport{
		ClientID:    cfg.ClientID,
		DialTimeout: 10 * time.Second,
	}
	if cfg.TLSEnabled {
		t.TLS = &tls.Config{InsecureSkipVerify: cfg.TLSSkipVerify} //nolint:gosec // opt-in for self-signed test brokers
	}
	switch strings.ToLower(strings.TrimSpace(cfg.SASLMechanism)) {
	case "", "none":
	case "plain":
		t.SASL = plain.Mechanism{Username: cfg.SASLUsername, Password: cfg.SASLPassword}
	case "scram-sha-256":
		m, err := scram.Mechanism(scram.SHA256, cfg.SASLUsername, cfg.SASLPassword)
		if err != nil {
			return nil, fmt.Errorf("producer: sasl: %w", err)
		}
		t.SASL = m
	case "scram-sha-512":
		m, err := scram.Mechanism(scram.SHA512, cfg.SASLUsername, cfg.SASLPassword)
		if err != nil {
			return nil, fmt.Errorf("producer: sasl: %w", err)
		}
		t.SASL = m
	default:
		return nil, fmt.Errorf("producer: unsupported sasl mechanism %q", cfg.SASLMechanism)
	}
	return t, nil
}

func compressionFor(name string) (compress.Compression, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "none":
		return 0, nil
	case "gzip":
		return compress.Gzip, nil
	case "snappy":
		return compress.Snappy, nil
	case "lz4":
		return compress.Lz4, nil
	case "zstd":
		return compress.Zstd, nil
	}
	return 0, fmt.Errorf("producer: unsupported compression %q", name)
}
