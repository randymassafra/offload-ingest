package generators

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Captured feeds replay real provider responses saved to disk instead of
// simulating them.
//
// This is the escape hatch for the sports whose schema we could not verify.
// Tennis is the immediate case: SportsDataIO sells the feed but publishes no
// reachable schema, so the generated models in internal/sdio/tennis.go are an
// educated guess. Rather than leave the load test running against a guess,
// capture one real response per endpoint and drop it in a directory:
//
//	fixtures/
//	  tennis.boxscore.json      a single document, or an array of documents
//	  tennis.telemetry.json
//	  cricket.boxscore.json
//
// Point the load test at it with -captured-dir and those endpoints switch from
// simulated to replayed. The payloads are then authentic by construction: they
// are bytes the provider actually sent.
//
// A capture is registered over the top of the generated feed for the same
// (sport, kind), so partial coverage works — capture tennis today, cricket when
// the provider is chosen, and everything else keeps simulating.

// capturedFeed replays a fixed set of documents in order, cycling forever.
type capturedFeed struct {
	ep        Endpoint
	docs      []json.RawMessage
	fixtureID string
	cursor    int
	seq       int64
}

func (c *capturedFeed) Endpoint() Endpoint { return c.ep }
func (c *capturedFeed) FixtureID() string  { return c.fixtureID }

// Done is always false: a capture is a loop, not a fixture that ends.
func (c *capturedFeed) Done() bool { return false }

func (c *capturedFeed) Reset() {
	c.cursor = 0
	c.seq = 0
}

func (c *capturedFeed) Next() Message {
	doc := c.docs[c.cursor%len(c.docs)]
	c.cursor++
	c.seq++

	// The payload is handed on as a raw document. Round-tripping it through a
	// model would defeat the point: the whole value of a capture is that the
	// bytes are the provider's, not ours.
	var payload any
	if err := json.Unmarshal(doc, &payload); err != nil {
		payload = map[string]any{"error": "unreadable capture"}
	}
	return Message{
		Sport:      c.ep.Sport,
		Kind:       c.ep.Kind,
		Endpoint:   c.ep.Path,
		Projection: c.ep.Projection,
		Model:      c.ep.Model,
		FixtureID:  c.fixtureID,
		Sequence:   c.seq,
		Emitted:    now().UTC(),
		Payload:    payload,
	}
}

// Capture is one loaded fixture file.
type Capture struct {
	Sport Sport
	Kind  FeedKind
	File  string
	Docs  int
}

// LoadCaptures reads every <sport>.<kind>.json in dir and registers a replay
// feed for it, replacing the simulated feed for that pair. It returns what it
// loaded so the caller can report it.
//
// A file may hold a single JSON document or an array of them; an array is
// replayed in order and then repeats, which is how a captured sequence of
// polls becomes a repeatable load profile.
func LoadCaptures(dir string) ([]Capture, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("generators: read capture dir: %w", err)
	}

	var loaded []Capture
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".json")
		parts := strings.Split(name, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("generators: capture %q must be named <sport>.<kind>.json", e.Name())
		}
		sport, err := ParseSport(parts[0])
		if err != nil {
			return nil, fmt.Errorf("generators: capture %q: %w", e.Name(), err)
		}
		kind, err := ParseKind(parts[1])
		if err != nil {
			return nil, fmt.Errorf("generators: capture %q: %w", e.Name(), err)
		}

		path := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("generators: read %s: %w", path, err)
		}
		docs, err := splitDocuments(raw)
		if err != nil {
			return nil, fmt.Errorf("generators: parse %s: %w", path, err)
		}
		if len(docs) == 0 {
			return nil, fmt.Errorf("generators: %s contains no documents", path)
		}

		registerCapture(sport, kind, path, docs)
		loaded = append(loaded, Capture{Sport: sport, Kind: kind, File: path, Docs: len(docs)})
	}

	sort.Slice(loaded, func(i, j int) bool {
		if loaded[i].Sport != loaded[j].Sport {
			return loaded[i].Sport < loaded[j].Sport
		}
		return loaded[i].Kind < loaded[j].Kind
	})
	return loaded, nil
}

// splitDocuments accepts either a single JSON document or an array of them.
// A top-level array is ambiguous — it could be one response that happens to be
// an array, such as PlayerGame[] — so an array of arrays or of objects that
// each look like a whole response is treated as a sequence, and anything else
// is treated as one document.
func splitDocuments(raw []byte) ([]json.RawMessage, error) {
	var probe any
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, err
	}
	arr, isArray := probe.([]any)
	if !isArray {
		return []json.RawMessage{json.RawMessage(raw)}, nil
	}
	// An empty array is a valid single response.
	if len(arr) == 0 {
		return []json.RawMessage{json.RawMessage(raw)}, nil
	}
	// A sequence of captures is an array whose elements are themselves arrays.
	if _, nested := arr[0].([]any); nested {
		var seq []json.RawMessage
		if err := json.Unmarshal(raw, &seq); err != nil {
			return nil, err
		}
		return seq, nil
	}
	// Otherwise this is one array-shaped response, replayed whole.
	return []json.RawMessage{json.RawMessage(raw)}, nil
}

// registerCapture replaces the registered feed for a (sport, kind) pair.
func registerCapture(sport Sport, kind FeedKind, file string, docs []json.RawMessage) {
	registryMu.Lock()
	defer registryMu.Unlock()

	ep := Endpoint{Sport: sport, Kind: kind, Model: "captured", Path: file}
	if existing, ok := lookup(sport, kind, ""); ok {
		// Keep the real route, name and projection; only the source changes.
		ep.Path = existing.ep.Path
		ep.Name = existing.ep.Name
		ep.Projection = existing.ep.Projection
		ep.Model = existing.ep.Model
	}
	// A replay is authentic by construction: these are bytes the provider
	// actually sent, whatever the generated model for that sport claimed.
	ep.Provenance = ProvenanceCaptured
	ep.Replayed = true

	fixture := fmt.Sprintf("%s-%s-capture", sport, kind)
	entry := registration{
		ep: ep,
		newFeed: func(seed int64) Feed {
			return &capturedFeed{ep: ep, docs: docs, fixtureID: fixture}
		},
	}
	// Replace the first endpoint of this kind, or append if the sport has none.
	for k, existing := range registry[sport] {
		if existing.ep.Kind == kind {
			registry[sport][k] = entry
			return
		}
	}
	registry[sport] = append(registry[sport], entry)
}
