package golf

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Host is the RapidAPI host serving this provider.
const Host = "live-golf-data.p.rapidapi.com"

// Timeout bounds a single upstream call.
//
// Five seconds. A leaderboard is a foreground read for a screen behind a bar,
// and a request that has not answered in five seconds has already missed the
// refresh it was fetched for — waiting longer only delays the fall back to
// cache.
const Timeout = 5 * time.Second

// CacheTTL is how long a cached leaderboard stays usable.
const CacheTTL = time.Hour

// DefaultCachePath is where the leaderboard cache lives.
//
// Note this sits under testdata/, which the Go toolchain ignores when building
// and which is gitignored, so a stale cache can never be compiled in or
// committed. It is an unusual home for a runtime cache — testdata is
// conventionally test fixtures — so the path is a field on the client and a
// deployment should point it somewhere writable and durable.
const DefaultCachePath = "testdata/golf_cache.json"

// Client is the live-golf-data provider.
type Client struct {
	apiKey string
	http   *http.Client
	// baseURL is overridable so tests can point at an httptest server without
	// reaching the real upstream.
	baseURL   string
	cachePath string
	ttl       time.Duration
	now       func() time.Time

	// mu guards the cache file against two concurrent refreshes writing over
	// each other. The provider is safe for concurrent use.
	mu sync.Mutex
}

// New builds a golf client.
//
// The key is threaded in rather than read from the environment: this package
// never touches os.Getenv, so it can be constructed in a test, pointed at a
// fake server, and given a key that does not exist. Configuration is loaded
// once at startup by the config package and passed down.
//
// An empty key is not rejected here. A constructor that cannot fail is easier
// to compose, and the caller — which knows whether golf is entitled at all —
// validates the credential before building this. See cmd/loadtest.
func New(apiKey string) *Client {
	return &Client{
		apiKey:    apiKey,
		http:      &http.Client{Timeout: Timeout},
		baseURL:   "https://" + Host,
		cachePath: DefaultCachePath,
		ttl:       CacheTTL,
		now:       time.Now,
	}
}

// Option configures a Client.
type Option func(*Client)

// WithCachePath moves the cache file.
func WithCachePath(path string) Option { return func(c *Client) { c.cachePath = path } }

// WithCacheTTL changes how long a cached document stays usable.
func WithCacheTTL(d time.Duration) Option { return func(c *Client) { c.ttl = d } }

// WithHTTPClient replaces the transport.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// WithBaseURL points the client at a different origin, for tests.
func WithBaseURL(u string) Option { return func(c *Client) { c.baseURL = strings.TrimRight(u, "/") } }

// WithClock injects a clock, for tests.
func WithClock(now func() time.Time) Option { return func(c *Client) { c.now = now } }

// Configure applies options to an existing client.
func (c *Client) Configure(opts ...Option) *Client {
	for _, o := range opts {
		o(c)
	}
	return c
}

// LeaderboardRequest identifies a tournament.
type LeaderboardRequest struct {
	OrgID   string // "1" PGA Tour, "2" LIV
	TournID string
	Year    string
}

func (r LeaderboardRequest) values() url.Values {
	v := url.Values{}
	set := func(k, val, fallback string) {
		if strings.TrimSpace(val) == "" {
			val = fallback
		}
		v.Set(k, val)
	}
	set("orgId", r.OrgID, "1")
	set("tournId", r.TournID, "")
	set("year", r.Year, "")
	return v
}

// cacheKey identifies a cached document. Included in the cache file so a cache
// fetched for one tournament is never served for another — the failure that
// would otherwise be silent and completely wrong.
func (r LeaderboardRequest) cacheKey() string {
	v := r.values()
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+v.Get(k))
	}
	return strings.Join(parts, "&")
}

// cacheEnvelope is the on-disk format.
//
// The document is wrapped rather than stored bare so the cache carries its own
// validity metadata. Relying on the file's mtime instead would break the moment
// anything touches the file — a backup restore, a container image build, a
// `cp -p` — and would give no way to tell which tournament it holds.
type cacheEnvelope struct {
	Key       string          `json:"key"`
	FetchedAt time.Time       `json:"fetched_at"`
	Source    string          `json:"source"`
	Document  json.RawMessage `json:"document"`
}

// Leaderboard returns a tournament leaderboard, preferring a fresh cache.
//
// Cache-first, then network:
//
//  1. If the cache exists, matches this request and is younger than the TTL,
//     it is returned without contacting the upstream.
//  2. Otherwise the upstream is called and the result written to the cache.
//  3. If the upstream fails and a STALE cache exists, the stale document is
//     returned with the error described in the log rather than nothing at all.
//
// Step 3 is the one worth arguing for: a leaderboard an hour old is still worth
// showing on a screen, and the alternative — a blank panel because RapidAPI had
// a bad minute — is strictly worse for the venue. The staleness is reported so
// a caller can label it.
func (c *Client) Leaderboard(ctx context.Context, req LeaderboardRequest) (*Leaderboard, Meta, error) {
	if strings.TrimSpace(req.TournID) == "" || strings.TrimSpace(req.Year) == "" {
		return nil, Meta{}, fmt.Errorf("golf: tournId and year are required")
	}
	key := req.cacheKey()

	c.mu.Lock()
	defer c.mu.Unlock()

	cached, cacheErr := c.readCache(key)
	if cacheErr == nil && c.now().Sub(cached.FetchedAt) < c.ttl {
		lb, err := decodeLeaderboard(cached.Document)
		if err == nil {
			return lb, Meta{
				FromCache: true,
				Age:       c.now().Sub(cached.FetchedAt),
				FetchedAt: cached.FetchedAt,
			}, nil
		}
		// A corrupt cache is not fatal: fall through and refetch.
	}

	raw, err := c.fetch(ctx, "/leaderboard", req.values())
	if err != nil {
		// The upstream is unreachable. A stale cache beats nothing.
		if cacheErr == nil {
			if lb, decErr := decodeLeaderboard(cached.Document); decErr == nil {
				return lb, Meta{
					FromCache: true, Stale: true,
					Age:       c.now().Sub(cached.FetchedAt),
					FetchedAt: cached.FetchedAt,
					Err:       err,
				}, nil
			}
		}
		return nil, Meta{Err: err}, err
	}

	lb, err := decodeLeaderboard(raw)
	if err != nil {
		return nil, Meta{Err: err}, err
	}
	fetchedAt := c.now()
	if writeErr := c.writeCache(key, fetchedAt, raw); writeErr != nil {
		// A cache that cannot be written is a degraded mode, not a failure:
		// the caller has the data it asked for.
		return lb, Meta{FetchedAt: fetchedAt, CacheErr: writeErr}, nil
	}
	return lb, Meta{FetchedAt: fetchedAt}, nil
}

// Meta describes where a response came from.
type Meta struct {
	// FromCache is true when the document was served from disk.
	FromCache bool
	// Stale is true when the cache was past its TTL and served only because the
	// upstream could not be reached.
	Stale     bool
	Age       time.Duration
	FetchedAt time.Time
	// Err is the upstream error that forced a stale read, if any.
	Err error
	// CacheErr is a non-fatal failure to persist a fresh document.
	CacheErr error
}

// Schedule returns a season's tournament schedule. Not cached: it is read once
// at startup to resolve a tournament id, not polled.
func (c *Client) Schedule(ctx context.Context, orgID, year string) (*Schedule, error) {
	v := url.Values{}
	if orgID == "" {
		orgID = "1"
	}
	v.Set("orgId", orgID)
	v.Set("year", year)

	raw, err := c.fetch(ctx, "/schedule", v)
	if err != nil {
		return nil, err
	}
	var s Schedule
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("golf: decoding schedule: %w", err)
	}
	return &s, nil
}

// Schedule is the /schedule response.
type Schedule struct {
	OrgID    string          `json:"orgId"`
	Year     string          `json:"year"`
	Schedule []ScheduleEntry `json:"schedule"`
}

// ScheduleEntry is one tournament.
type ScheduleEntry struct {
	TournID string   `json:"tournId"`
	Name    string   `json:"name"`
	Purse   MongoInt `json:"purse"`
	Date    struct {
		Start      MongoDate `json:"start"`
		End        MongoDate `json:"end"`
		WeekNumber string    `json:"weekNumber"`
	} `json:"date"`
	Format string `json:"format"`
}

// InProgress reports whether the tournament's window contains t.
//
// The end date is the final round's date, so the window runs to the end of that
// whole day — a leaderboard is at its most interesting on Sunday evening, and
// an exclusive comparison would drop the tournament exactly then.
func (e ScheduleEntry) InProgress(t time.Time) bool {
	if e.Date.Start.IsZero() || e.Date.End.IsZero() {
		return false
	}
	return !t.Before(e.Date.Start.Time) && t.Before(e.Date.End.Time.Add(24*time.Hour))
}

// Completed reports whether the tournament finished before t.
func (e ScheduleEntry) Completed(t time.Time) bool {
	if e.Date.End.IsZero() {
		return false
	}
	return t.After(e.Date.End.Time.Add(24 * time.Hour))
}

// Current picks the tournament to poll for a given moment.
//
// The window is tried first: a tournament in progress is what a venue wants on
// a screen. Failing that, the most recently COMPLETED tournament is used.
//
// That fallback is the correction to the original heuristic, which took the
// last entry in the schedule. Part-way through a season that is an event which
// has not been played, and the provider has no leaderboard for it at all:
//
//	{"detail":"leaderboards not found for query {'tournId': '551', ...}"}
//
// Falling back to a completed tournament means the feed always has a real
// leaderboard to publish between events, rather than erroring until the next
// tee-off.
func (s *Schedule) Current(t time.Time) (ScheduleEntry, bool) {
	for _, e := range s.Schedule {
		if e.InProgress(t) {
			return e, true
		}
	}
	var best ScheduleEntry
	var found bool
	for _, e := range s.Schedule {
		if !e.Completed(t) {
			continue
		}
		if !found || e.Date.End.Time.After(best.Date.End.Time) {
			best, found = e, true
		}
	}
	return best, found
}

// fetch performs one authenticated request.
func (c *Client) fetch(ctx context.Context, path string, params url.Values) (json.RawMessage, error) {
	if strings.TrimSpace(c.apiKey) == "" {
		return nil, fmt.Errorf("golf: no API key; the provider was built without a credential")
	}
	target := c.baseURL + path
	if len(params) > 0 {
		target += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-rapidapi-key", c.apiKey)
	req.Header.Set("x-rapidapi-host", Host)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("golf: %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("golf: reading %s: %w", path, err)
	}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, fmt.Errorf("golf: %s: rate limited by RapidAPI (429)", path)
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		return nil, fmt.Errorf("golf: %s: credential rejected (HTTP %d)", path, resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("golf: %s: HTTP %d: %s", path, resp.StatusCode, trim(body))
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("golf: %s: empty response", path)
	}
	return json.RawMessage(body), nil
}

func decodeLeaderboard(raw json.RawMessage) (*Leaderboard, error) {
	var lb Leaderboard
	if err := json.Unmarshal(raw, &lb); err != nil {
		return nil, fmt.Errorf("golf: decoding leaderboard: %w", err)
	}
	return &lb, nil
}

// readCache loads the cache and checks it is for this request.
func (c *Client) readCache(key string) (*cacheEnvelope, error) {
	b, err := os.ReadFile(c.cachePath)
	if err != nil {
		return nil, err
	}
	var env cacheEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("golf: cache %s is not readable: %w", c.cachePath, err)
	}
	if env.Key != key {
		// The cache holds a different tournament. Serving it would be silently
		// and completely wrong.
		return nil, fmt.Errorf("golf: cache holds %q, wanted %q", env.Key, key)
	}
	if len(env.Document) == 0 {
		return nil, fmt.Errorf("golf: cache %s is empty", c.cachePath)
	}
	return &env, nil
}

// writeCache persists a document.
//
// Written to a temporary file and renamed, so a process killed mid-write leaves
// the previous cache intact rather than a truncated file that fails to parse on
// every subsequent start.
func (c *Client) writeCache(key string, at time.Time, doc json.RawMessage) error {
	if err := os.MkdirAll(filepath.Dir(c.cachePath), 0o755); err != nil {
		return fmt.Errorf("golf: creating cache directory: %w", err)
	}
	body, err := json.MarshalIndent(cacheEnvelope{
		Key: key, FetchedAt: at, Source: Host, Document: doc,
	}, "", "  ")
	if err != nil {
		return err
	}
	tmp := c.cachePath + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return fmt.Errorf("golf: writing cache: %w", err)
	}
	if err := os.Rename(tmp, c.cachePath); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("golf: replacing cache: %w", err)
	}
	return nil
}

// CachePath reports where this client caches.
func (c *Client) CachePath() string { return c.cachePath }

func trim(b []byte) string {
	s := strings.Join(strings.Fields(string(b)), " ")
	if len(s) > 160 {
		s = s[:160] + "…"
	}
	return s
}
