package producer

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

func sample(t *testing.T, sport generators.Sport, kind generators.FeedKind, n int) []generators.Message {
	t.Helper()
	f, err := generators.New(sport, kind, 1)
	if err != nil {
		t.Fatalf("generators.New(%s,%s): %v", sport, kind, err)
	}
	out := make([]generators.Message, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, f.Next())
	}
	return out
}

func TestDiscardCountsMessages(t *testing.T) {
	d := NewDiscard()
	if err := d.Publish(context.Background(), sample(t, generators.SportNFL, generators.FeedBoxScore, 4)...); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	s := d.Stats()
	if s.Published != 4 {
		t.Errorf("published = %d, want 4", s.Published)
	}
	if s.Bytes == 0 {
		t.Error("bytes = 0, want the encoded size")
	}
}

// TestWriterEmitsBarePayload is the contract that makes these payloads useful:
// the message body must be the SportsDataIO document alone, with none of our
// routing metadata mixed in, or a Flink job cannot use a generated schema.
func TestWriterEmitsBarePayload(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.Publish(context.Background(), sample(t, generators.SportNBA, generators.FeedBoxScore, 1)...); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var doc map[string]any
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, leaked := range []string{"sport", "kind", "endpoint", "fixture_id", "sequence", "payload"} {
		if _, found := doc[leaked]; found {
			t.Errorf("routing field %q leaked into the payload", leaked)
		}
	}
	// The payload is the provider's own document. API-Sports roots a game on a
	// flat id/status/teams shape, so those keys are what a consumer binds to.
	for _, want := range []string{"id", "status", "teams"} {
		if _, ok := doc[want]; !ok {
			t.Errorf("box score has no %q field; keys = %v", want, keysOf(doc))
		}
	}
}

func TestEnvelopeWriterIncludesRouting(t *testing.T) {
	var buf bytes.Buffer
	w := NewEnvelopeWriter(&buf)
	if err := w.Publish(context.Background(), sample(t, generators.SportNASCAR, generators.FeedTelemetry, 1)...); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	var env map[string]any
	if err := json.Unmarshal(buf.Bytes(), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, want := range []string{"sport", "kind", "endpoint", "model", "fixture_id", "payload"} {
		if _, ok := env[want]; !ok {
			t.Errorf("envelope missing %q", want)
		}
	}
}

func TestMultiFansOut(t *testing.T) {
	a, b := NewDiscard(), NewDiscard()
	m := NewMulti(a, b)
	if err := m.Publish(context.Background(), sample(t, generators.SportGolf, generators.FeedTelemetry, 2)...); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if a.Stats().Published != 2 || b.Stats().Published != 2 {
		t.Errorf("fan-out counts = %d/%d, want 2/2", a.Stats().Published, b.Stats().Published)
	}
	if err := m.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestTopicRouting(t *testing.T) {
	msg := sample(t, generators.SportNBA, generators.FeedTelemetry, 1)[0]

	cases := []struct {
		name     string
		perSport bool
		perFeed  bool
		want     string
	}{
		{"flat", false, false, "ingest.events"},
		{"per sport", true, false, "ingest.events.nba"},
		{"per feed", false, true, "ingest.events.telemetry"},
		{"per sport and feed", true, true, "ingest.events.nba.telemetry"},
	}
	for _, tc := range cases {
		cfg := DefaultConfig()
		cfg.Topic = "ingest.events"
		cfg.TopicPerSport, cfg.TopicPerFeed = tc.perSport, tc.perFeed
		k, err := NewKafka(cfg)
		if err != nil {
			t.Fatalf("%s: NewKafka: %v", tc.name, err)
		}
		if got := k.TopicFor(msg); got != tc.want {
			t.Errorf("%s: TopicFor = %q, want %q", tc.name, got, tc.want)
		}
		k.Close()
	}
}

func TestHeadersCarryRouting(t *testing.T) {
	msg := sample(t, generators.SportSoccer, generators.FeedBoxScore, 1)[0]
	got := map[string]string{}
	for _, h := range headersFor(msg) {
		got[h.Key] = string(h.Value)
	}
	if got["sport"] != "soccer" {
		t.Errorf("sport header = %q", got["sport"])
	}
	if got["feed"] != "boxscore" {
		t.Errorf("feed header = %q", got["feed"])
	}
	if got["endpoint"] == "" || got["model"] == "" || got["fixture"] == "" || got["sequence"] == "" {
		t.Errorf("incomplete headers: %v", got)
	}
}

func TestKafkaConfigValidation(t *testing.T) {
	base := DefaultConfig()

	noBrokers := base
	noBrokers.Brokers = nil
	if _, err := NewKafka(noBrokers); err == nil {
		t.Error("expected an error with no brokers")
	}

	noTopic := base
	noTopic.Topic = "  "
	if _, err := NewKafka(noTopic); err == nil {
		t.Error("expected an error with an empty topic")
	}

	badCodec := base
	badCodec.Compression = "brotli"
	if _, err := NewKafka(badCodec); err == nil {
		t.Error("expected an error for an unsupported compression codec")
	}

	badSASL := base
	badSASL.SASLMechanism = "kerberos"
	if _, err := NewKafka(badSASL); err == nil {
		t.Error("expected an error for an unsupported SASL mechanism")
	}
}

// TestPartitionKeyIsTheFixture is the ordering guarantee: every message for one
// fixture must hash to the same partition.
func TestPartitionKeyIsTheFixture(t *testing.T) {
	msgs := sample(t, generators.SportCricket, generators.FeedTelemetry, 10)
	want := string(msgs[0].Key())
	if want == "" {
		t.Fatal("empty partition key")
	}
	for _, m := range msgs[1:] {
		if string(m.Key()) != want {
			t.Fatalf("key drifted: %q != %q", m.Key(), want)
		}
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
