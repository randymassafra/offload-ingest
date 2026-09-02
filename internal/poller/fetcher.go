package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

// Fetcher retrieves the next payload for a worker's assigned endpoint. The mock
// implementation drives generators locally; the HTTP implementation points the
// same worker pool at a live SportsDataIO endpoint or a recorded fixture server.
type Fetcher interface {
	// Fetch returns up to n payloads for the worker's current fixture.
	Fetch(ctx context.Context, worker int, n int) ([]generators.Message, error)
	// Endpoint describes what the worker is polling, for logs and stats.
	Endpoint(worker int) generators.Endpoint
	// Close releases any per-worker resources.
	Close() error
}

// MockFetcher runs one generator feed per worker in-process. It is the default:
// it removes the provider from the loop so the test measures the ingest path.
type MockFetcher struct {
	feeds []generators.Feed
}

// NewMockFetcher assigns (sport, kind) endpoints to workers round-robin, so a
// run with more workers than endpoints covers every endpoint at least once and
// a run with fewer still spreads across sports.
func NewMockFetcher(sports []generators.Sport, kinds []generators.FeedKind, workers int, seed int64) (*MockFetcher, error) {
	catalog, err := generators.NewAll(sports, kinds, seed)
	if err != nil {
		return nil, err
	}
	feeds := make([]generators.Feed, 0, workers)
	for w := 0; w < workers; w++ {
		// Each worker gets its own feed instance — never a shared one — because
		// feeds are stateful and not safe for concurrent use.
		ep := catalog[w%len(catalog)].Endpoint()
		f, err := generators.NewNamed(ep.Sport, ep.Kind, ep.Name, seed+int64(w)*7919)
		if err != nil {
			return nil, err
		}
		feeds = append(feeds, f)
	}
	return &MockFetcher{feeds: feeds}, nil
}

// Fetch advances worker w's feed n times. Each worker touches only its own
// feed, so no locking is needed even though workers run concurrently.
func (m *MockFetcher) Fetch(_ context.Context, worker, n int) ([]generators.Message, error) {
	if worker < 0 || worker >= len(m.feeds) {
		return nil, fmt.Errorf("poller: worker %d out of range", worker)
	}
	f := m.feeds[worker]
	out := make([]generators.Message, 0, n)
	for k := 0; k < n; k++ {
		out = append(out, f.Next())
	}
	return out, nil
}

func (m *MockFetcher) Endpoint(worker int) generators.Endpoint {
	if worker < 0 || worker >= len(m.feeds) {
		return generators.Endpoint{}
	}
	return m.feeds[worker].Endpoint()
}

func (m *MockFetcher) Close() error { return nil }

// Feed exposes worker w's feed, for tests.
func (m *MockFetcher) Feed(worker int) generators.Feed { return m.feeds[worker] }

// HTTPFetcher polls real SportsDataIO endpoints. Supply fully-formed URLs with
// the route parameters already substituted; the API key travels in the
// Ocp-Apim-Subscription-Key header, which is how SportsDataIO authenticates.
type HTTPFetcher struct {
	client    *http.Client
	endpoints []string
	apiKey    string
	sport     generators.Sport
	kind      generators.FeedKind
}

// NewHTTPFetcher builds a fetcher over the given URLs, with a connection pool
// sized for the worker count so polls do not serialise on socket setup.
func NewHTTPFetcher(endpoints []string, workers int, timeout time.Duration, apiKey string, sport generators.Sport, kind generators.FeedKind) (*HTTPFetcher, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("poller: at least one endpoint is required")
	}
	for _, e := range endpoints {
		if _, err := url.ParseRequestURI(e); err != nil {
			return nil, fmt.Errorf("poller: bad endpoint %q: %w", e, err)
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = workers * 2
	transport.MaxIdleConnsPerHost = workers * 2
	transport.MaxConnsPerHost = workers * 2

	return &HTTPFetcher{
		client:    &http.Client{Timeout: timeout, Transport: transport},
		endpoints: endpoints,
		apiKey:    apiKey,
		sport:     sport,
		kind:      kind,
	}, nil
}

func (h *HTTPFetcher) Fetch(ctx context.Context, worker, n int) ([]generators.Message, error) {
	endpoint := h.endpoints[worker%len(h.endpoints)]
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if h.apiKey != "" {
		req.Header.Set("Ocp-Apim-Subscription-Key", h.apiKey)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("poller: GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Drain a little of the body so the connection can be reused.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("poller: GET %s: %s: %s", endpoint, resp.Status, strings.TrimSpace(string(body)))
	}

	// The provider's response is forwarded as an opaque document: whatever
	// shape it has is by definition the authentic shape.
	var payload any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("poller: decode %s: %w", endpoint, err)
	}
	return []generators.Message{{
		Sport:     h.sport,
		Kind:      h.kind,
		Endpoint:  endpoint,
		Model:     "live",
		FixtureID: fixtureFromURL(endpoint),
		Sequence:  1,
		Emitted:   time.Now().UTC(),
		Payload:   payload,
	}}, nil
}

// fixtureFromURL uses the last path segment as the partition key, which for
// SportsDataIO routes is the game, race or tournament id.
func fixtureFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) == 0 {
		return raw
	}
	return parts[len(parts)-1]
}

func (h *HTTPFetcher) Endpoint(int) generators.Endpoint {
	// Polling the real API is the strongest evidence there is: the payload
	// is the provider's own bytes.
	return generators.Endpoint{
		Sport: h.sport, Kind: h.kind, Path: h.endpoints[0],
		Model: "live", Provenance: generators.ProvenanceCaptured, Replayed: true,
	}
}

func (h *HTTPFetcher) Close() error {
	h.client.CloseIdleConnections()
	return nil
}
