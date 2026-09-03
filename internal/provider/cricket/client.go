// Package cricket is the client for Cricbuzz, the cricket feed's upstream.
//
// Cricket is one of three sports API-Sports has no host for, so it needs its
// own provider. This one is reached through RapidAPI, the same way golf is, and
// deliberately mirrors internal/provider/golf: the two are onboarded the same
// way, authenticate the same way and fail the same way, so an operator who has
// debugged one has debugged both.
//
// # Two casing conventions in one API
//
// Verified against captures fetched by cmd/schematool, not transcribed.
//
// The match-list endpoints use camelCase:
//
//	/matches/v1/live    {"typeMatches":[{"matchType":"League","seriesMatches":[...
//	                     ... "matchInfo":{"matchId":154524,"state":"Delay"}
//
// The match-centre endpoints use flat lowercase:
//
//	/mcenter/v1/{id}        {"matchid":40381,"seriesid":3866,"matchdesc":"2nd T20I"
//	/mcenter/v1/{id}/hscard {"scorecard":[{"inningsid":1,"batsman":[...
//
// So `matchId` and `matchid` are the same field on two endpoints of one API.
//
// This does NOT break decoding here, and it is worth being precise about why,
// because the obvious assumption is wrong: encoding/json matches field names
// case-insensitively, so a `json:"matchid"` tag reads `"matchId"` perfectly
// well and vice versa. Getting the tags wrong would cost nothing at ingest.
//
// It costs something on the way OUT. Struct tags decide the casing we emit,
// and for cricket the pipeline re-marshals through these types rather than
// forwarding provider bytes — the same trade golf makes. A Kafka consumer is
// not necessarily Go, and a Flink job in Java or Python matching field names
// exactly would break on a document whose casing we had quietly normalised.
// So the tags below mirror the wire per endpoint family: camelCase here,
// lowercase in internal/cricbuzz. Neither may be "tidied" to match the other,
// and the reason is the downstream consumer rather than this decoder.
//
// # Discovery, not configuration
//
// Golf resolves a tournament from a season schedule. Cricket has no equivalent:
// matches are discovered from /matches/v1/live, which returns whatever is in
// play right now across every series. LiveMatchIDs flattens that four-level
// nesting into the ids a caller actually needs.
package cricket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/cricbuzz"
)

// Host is the RapidAPI host this provider is reached through.
//
// A constant rather than configuration because it is not a deployment choice:
// it identifies which product the subscription is for, and pointing it
// elsewhere would send a Cricbuzz-shaped request to something that is not
// Cricbuzz. RAPIDAPI_CRICKET_HOST exists in config for the schema tooling,
// which probes hosts deliberately; the ingest path does not.
const Host = "cricbuzz-cricket.p.rapidapi.com"

// Timeout bounds a single request.
//
// Five seconds, matching golf. An edge appliance polling on a live cadence
// would rather miss one poll than have a slow response hold a worker while the
// next three fall due behind it.
const Timeout = 5 * time.Second

const (
	// CacheTTL is the resting lifetime, used when no match is in play.
	//
	// Longer than golf's hour because cricket's quiet periods are longer: a
	// Test match has a full night between days, and there are stretches of the
	// calendar with nothing live at all.
	CacheTTL = 2 * time.Hour
	// LiveCacheTTL applies while a match is in progress. A scorecard changes
	// every ball, so this is the cadence that matters.
	LiveCacheTTL = 30 * time.Second
	// InningsBreakCacheTTL applies between innings, when the score is settled
	// but the match has not ended. Nothing changes for ten minutes or so, and
	// polling every thirty seconds through it is quota spent on a document
	// known not to have moved.
	InningsBreakCacheTTL = 3 * time.Minute
)

// ThrottlePenalty is how long a 429 forces the resting lifetime back on.
//
// A full day, exactly as in golf and for the same reason: RapidAPI suspends
// accounts that keep hammering a throttled endpoint, and losing the
// subscription costs far more than a day of stale cricket.
const ThrottlePenalty = 24 * time.Hour

// DefaultCachePath is where the last scorecard is persisted.
const DefaultCachePath = "testdata/cricket_cache.json"

// Client is the Cricbuzz provider.
type Client struct {
	apiKey  string
	http    *http.Client
	baseURL string
	now     func() time.Time

	// mu guards the cache file and the throttle state together. One mutex
	// because a throttle changes the effective TTL, and reading a TTL while
	// another goroutine is recording a 429 would serve a document under a
	// lifetime that no longer applies.
	//
	// Never held across a call to a method that takes it again — see the note
	// on effectiveTTLLocked.
	mu sync.Mutex

	cachePath string
	ttl       time.Duration

	// throttledUntil is when the hard floor lifts. Zero when not throttled.
	throttledUntil time.Time
}

// New builds a client from an API key.
//
// The signature matches golf's exactly: a single credential in, a usable client
// out. Everything else is an Option, so a caller that needs nothing special
// writes one line and a test writes three.
func New(apiKey string) *Client {
	return &Client{
		apiKey:    strings.TrimSpace(apiKey),
		http:      &http.Client{Timeout: Timeout},
		baseURL:   "https://" + Host,
		now:       time.Now,
		cachePath: DefaultCachePath,
		ttl:       CacheTTL,
	}
}

// Option configures a Client.
type Option func(*Client)

// WithCachePath overrides where the scorecard cache is written.
func WithCachePath(path string) Option { return func(c *Client) { c.cachePath = path } }

// WithCacheTTL overrides the resting cache lifetime.
func WithCacheTTL(d time.Duration) Option { return func(c *Client) { c.ttl = d } }

// WithHTTPClient replaces the underlying transport.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// WithBaseURL points the client at a test server.
func WithBaseURL(u string) Option {
	return func(c *Client) { c.baseURL = strings.TrimRight(u, "/") }
}

// WithClock injects a time source.
func WithClock(now func() time.Time) Option { return func(c *Client) { c.now = now } }

// Configure applies options and returns the client, for chaining off New.
func (c *Client) Configure(opts ...Option) *Client {
	for _, o := range opts {
		o(c)
	}
	return c
}

// SetCacheTTL adjusts the resting lifetime at runtime.
//
// The streamer calls this as a match's state changes. A throttle overrides it:
// see effectiveTTLLocked.
func (c *Client) SetCacheTTL(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	c.ttl = d
	c.mu.Unlock()
}

// EffectiveTTL is the lifetime actually in force, throttle included.
func (c *Client) EffectiveTTL() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.effectiveTTLLocked()
}

// effectiveTTLLocked assumes mu is held.
//
// Split from EffectiveTTL because Scorecard already holds the mutex when it
// needs this value, and calling the exported form there would deadlock on the
// same goroutine. Go's sync.Mutex is not reentrant, and this is the shape of
// that mistake.
func (c *Client) effectiveTTLLocked() time.Duration {
	if !c.throttledUntil.IsZero() && c.now().Before(c.throttledUntil) {
		return CacheTTL
	}
	return c.ttl
}

// Throttled reports whether the 429 hard floor is in force, and until when.
func (c *Client) Throttled() (bool, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.throttledUntil.IsZero() || !c.now().Before(c.throttledUntil) {
		return false, time.Time{}
	}
	return true, c.throttledUntil
}

// recordThrottle starts the hard floor and reports when it lifts.
func (c *Client) recordThrottle() (bool, time.Time) {
	until := c.now().Add(ThrottlePenalty)
	c.throttledUntil = until
	return true, until
}

// CachePath reports where this client caches.
func (c *Client) CachePath() string { return c.cachePath }

// ---------------------------------------------------------------------------
// Match discovery
// ---------------------------------------------------------------------------

// MatchList is the /matches/v1/{live,recent} response.
//
// CAMELCASE. See the package comment: these tags differ from the scorecard
// models in internal/cricbuzz by design, because the API itself differs.
type MatchList struct {
	TypeMatches         []TypeMatch `json:"typeMatches"`
	ResponseLastUpdated string      `json:"responseLastUpdated"`
}

// TypeMatch groups by competition class — "International", "League", "Domestic".
type TypeMatch struct {
	MatchType     string        `json:"matchType"`
	SeriesMatches []SeriesMatch `json:"seriesMatches"`
}

// SeriesMatch wraps a series. The wrapper exists because the same array also
// carries advertising entries, which have no seriesAdWrapper and are skipped.
type SeriesMatch struct {
	SeriesAdWrapper *SeriesAdWrapper `json:"seriesAdWrapper"`
}

// SeriesAdWrapper is one series and the matches in it.
type SeriesAdWrapper struct {
	SeriesID   int     `json:"seriesId"`
	SeriesName string  `json:"seriesName"`
	Matches    []Match `json:"matches"`
}

// Match wraps one fixture.
type Match struct {
	MatchInfo MatchInfo `json:"matchInfo"`
}

// MatchInfo is a fixture header as the LIST endpoints render it.
//
// Distinct from cricbuzz.Scorecard's view of the same match, and distinct again
// from /mcenter/v1/{id}, which returns the same facts in lowercase. Three
// spellings of one fixture is the API's design, not ours.
type MatchInfo struct {
	MatchID     int    `json:"matchId"`
	SeriesID    int    `json:"seriesId"`
	SeriesName  string `json:"seriesName"`
	MatchDesc   string `json:"matchDesc"`
	MatchFormat string `json:"matchFormat"`
	// StartDate and EndDate are epoch MILLISECONDS in a quoted string. Reading
	// them as seconds silently yields a date in the far future that sorts and
	// compares without complaint — the same trap golf's MongoDate carries.
	StartDate string `json:"startDate"`
	EndDate   string `json:"endDate"`
	// State is the short machine token: "In Progress", "Complete", "Preview",
	// "Delay", "Stumps", "Innings Break". Status is the human sentence.
	State  string `json:"state"`
	Status string `json:"status"`
	Team1  Team   `json:"team1"`
	Team2  Team   `json:"team2"`
}

// Team identifies a side.
type Team struct {
	TeamID    int    `json:"teamId"`
	TeamName  string `json:"teamName"`
	TeamSName string `json:"teamSName"`
}

// Started reports whether play has begun and not finished.
//
// Matched on the state token rather than on timestamps: a match can be past its
// start time and not started (rain), and cricket has more of those states than
// most sports.
func (m MatchInfo) Started() bool {
	switch strings.ToLower(strings.TrimSpace(m.State)) {
	case "in progress", "innings break", "stumps", "tea", "lunch", "drinks", "rain", "delay":
		return true
	}
	return false
}

// Live reports whether the ball is actually in play, which is narrower than
// Started: a rain delay is started but not live, and polling it every thirty
// seconds spends quota on a document that will not move.
func (m MatchInfo) Live() bool {
	return strings.EqualFold(strings.TrimSpace(m.State), "in progress")
}

// Complete reports whether the match has finished.
func (m MatchInfo) Complete() bool {
	switch strings.ToLower(strings.TrimSpace(m.State)) {
	case "complete", "abandon", "abandoned", "cancelled", "canceled", "no result":
		return true
	}
	return false
}

// StartsAt parses StartDate, which is epoch milliseconds in a quoted string.
func (m MatchInfo) StartsAt() (time.Time, bool) { return epochMillis(m.StartDate) }

// EndsAt parses EndDate.
func (m MatchInfo) EndsAt() (time.Time, bool) { return epochMillis(m.EndDate) }

func epochMillis(s string) (time.Time, bool) {
	ms, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || ms == 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(ms).UTC(), true
}

// Matches flattens the four levels of nesting into the fixtures themselves.
//
// typeMatches -> seriesMatches -> seriesAdWrapper -> matches -> matchInfo. Every
// caller wants the last of those, and every caller writing the walk itself is
// four chances to forget the nil check on seriesAdWrapper — which is absent on
// the advertising entries the same array carries.
func (l *MatchList) Matches() []MatchInfo {
	if l == nil {
		return nil
	}
	var out []MatchInfo
	for _, t := range l.TypeMatches {
		for _, s := range t.SeriesMatches {
			if s.SeriesAdWrapper == nil {
				continue // an ad slot, not a series
			}
			for _, m := range s.SeriesAdWrapper.Matches {
				out = append(out, m.MatchInfo)
			}
		}
	}
	return out
}

// LiveMatchIDs returns the ids of matches currently in play, in list order.
func (l *MatchList) LiveMatchIDs() []int {
	var out []int
	for _, m := range l.Matches() {
		if m.Live() {
			out = append(out, m.MatchID)
		}
	}
	return out
}

// LiveMatches fetches everything in play right now.
//
// Not cached. It is the discovery call the poll cadence is built from, so
// serving it from a stale cache would pin the streamer to a match that finished
// hours ago.
func (c *Client) LiveMatches(ctx context.Context) (*MatchList, error) {
	return c.matchList(ctx, "/matches/v1/live")
}

// RecentMatches fetches recently completed matches. Useful for backfilling a
// final scorecard after an appliance was offline at the close of play.
func (c *Client) RecentMatches(ctx context.Context) (*MatchList, error) {
	return c.matchList(ctx, "/matches/v1/recent")
}

func (c *Client) matchList(ctx context.Context, path string) (*MatchList, error) {
	raw, err := c.fetch(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	var list MatchList
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("cricket: decoding %s: %w", path, err)
	}
	return &list, nil
}

// ---------------------------------------------------------------------------
// Scorecard
// ---------------------------------------------------------------------------

// ScorecardRequest identifies one match.
type ScorecardRequest struct {
	MatchID int
}

func (r ScorecardRequest) cacheKey() string {
	return "hscard/" + strconv.Itoa(r.MatchID)
}

// Meta describes where a response came from.
//
// Identical in shape to golf's, deliberately: the streamer logic that reads it
// is the same logic, and two providers describing the same states with
// different field names would be gratuitous.
type Meta struct {
	// FromCache is true when the document was served from disk.
	FromCache bool
	// Stale is true when the cache was past its TTL and served only because
	// the upstream could not be reached.
	Stale     bool
	Age       time.Duration
	FetchedAt time.Time
	// Err is the upstream error that forced a stale read, if any.
	Err error
	// CacheErr is a non-fatal failure to persist a fresh document.
	CacheErr error
}

// Scorecard fetches a match's full innings-by-innings card.
//
// This is the box score: /mcenter/v1/{id}/hscard, the closest thing the API has
// to one. Cache-first, with a stale read on upstream failure, exactly as golf
// does — a scorecard from four minutes ago is worth incomparably more to a
// screen behind a bar than an error.
func (c *Client) Scorecard(ctx context.Context, req ScorecardRequest) (*cricbuzz.Scorecard, Meta, error) {
	if req.MatchID <= 0 {
		return nil, Meta{}, fmt.Errorf("cricket: a positive matchId is required, got %d", req.MatchID)
	}
	key := req.cacheKey()

	c.mu.Lock()
	defer c.mu.Unlock()

	ttl := c.effectiveTTLLocked()
	cached, cacheErr := c.readCache(key)
	if cacheErr == nil && c.now().Sub(cached.FetchedAt) < ttl {
		if sc, err := decodeScorecard(cached.Document); err == nil {
			return sc, Meta{
				FromCache: true,
				Age:       c.now().Sub(cached.FetchedAt),
				FetchedAt: cached.FetchedAt,
			}, nil
		}
		// A corrupt cache is not fatal: fall through and refetch.
	}

	path := "/mcenter/v1/" + strconv.Itoa(req.MatchID) + "/hscard"
	raw, err := c.fetch(ctx, path, nil)
	if err != nil {
		// The upstream is unreachable. A stale cache beats nothing.
		if cacheErr == nil {
			if sc, decErr := decodeScorecard(cached.Document); decErr == nil {
				return sc, Meta{
					FromCache: true, Stale: true,
					Age:       c.now().Sub(cached.FetchedAt),
					FetchedAt: cached.FetchedAt,
					Err:       err,
				}, nil
			}
		}
		return nil, Meta{Err: err}, err
	}

	sc, err := decodeScorecard(raw)
	if err != nil {
		return nil, Meta{Err: err}, err
	}
	fetchedAt := c.now()
	if writeErr := c.writeCache(key, fetchedAt, raw); writeErr != nil {
		// A cache that cannot be written is a degraded mode, not a failure:
		// the caller has the data it asked for.
		return sc, Meta{FetchedAt: fetchedAt, CacheErr: writeErr}, nil
	}
	return sc, Meta{FetchedAt: fetchedAt}, nil
}

// MatchInfoFor fetches one match's header from /mcenter/v1/{id}.
//
// Returned as a json.RawMessage rather than a struct. The endpoint is lowercase
// where the list endpoints are camelCase, so it needs a third model of the same
// fixture; nothing in the ingest path reads it today, and adding a model that
// no caller uses is how a package accumulates a wrong one. Decode it at the
// call site when a caller needs it.
func (c *Client) MatchInfoFor(ctx context.Context, matchID int) (json.RawMessage, error) {
	if matchID <= 0 {
		return nil, fmt.Errorf("cricket: a positive matchId is required, got %d", matchID)
	}
	return c.fetch(ctx, "/mcenter/v1/"+strconv.Itoa(matchID), nil)
}

func decodeScorecard(raw json.RawMessage) (*cricbuzz.Scorecard, error) {
	var sc cricbuzz.Scorecard
	if err := json.Unmarshal(raw, &sc); err != nil {
		return nil, fmt.Errorf("cricket: decoding scorecard: %w", err)
	}
	return &sc, nil
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

// fetch performs one authenticated GET.
//
// The header set is the RapidAPI pattern from cmd/schematool/routes.go, which
// is the same one golf uses: every RapidAPI-hosted provider authenticates
// identically, so onboarding another really is a host name.
func (c *Client) fetch(ctx context.Context, path string, params url.Values) (json.RawMessage, error) {
	if strings.TrimSpace(c.apiKey) == "" {
		return nil, fmt.Errorf("cricket: no API key; the provider was built without a credential")
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
		return nil, fmt.Errorf("cricket: %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("cricket: reading %s: %w", path, err)
	}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		// The hard floor. Forcing the resting lifetime back on for a full day
		// protects the subscription: RapidAPI suspends accounts that keep
		// hammering a throttled endpoint.
		_, until := c.recordThrottle()
		return nil, fmt.Errorf(
			"cricket: %s: rate limited by RapidAPI (429); cache lifetime forced to %s until %s",
			path, CacheTTL, until.Format(time.RFC3339))
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		return nil, fmt.Errorf("cricket: %s: credential rejected (HTTP %d)", path, resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("cricket: %s: HTTP %d: %s", path, resp.StatusCode, trim(body))
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("cricket: %s: empty response", path)
	}
	return json.RawMessage(body), nil
}

func trim(b []byte) string {
	s := strings.Join(strings.Fields(string(b)), " ")
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

type cacheEnvelope struct {
	Key       string          `json:"key"`
	FetchedAt time.Time       `json:"fetched_at"`
	Source    string          `json:"source"`
	Document  json.RawMessage `json:"document"`
}

// readCache loads the cache and checks it is for this match.
func (c *Client) readCache(key string) (*cacheEnvelope, error) {
	b, err := os.ReadFile(c.cachePath)
	if err != nil {
		return nil, err
	}
	var env cacheEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("cricket: cache %s is not readable: %w", c.cachePath, err)
	}
	if env.Key != key {
		// The cache holds a different match. Serving it would be silently and
		// completely wrong — a scorecard is not self-identifying enough for
		// anyone downstream to notice.
		return nil, fmt.Errorf("cricket: cache holds %q, wanted %q", env.Key, key)
	}
	if len(env.Document) == 0 {
		return nil, fmt.Errorf("cricket: cache %s is empty", c.cachePath)
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
		return fmt.Errorf("cricket: creating cache directory: %w", err)
	}
	body, err := json.MarshalIndent(cacheEnvelope{
		Key: key, FetchedAt: at, Source: Host, Document: doc,
	}, "", "  ")
	if err != nil {
		return err
	}
	tmp := c.cachePath + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return fmt.Errorf("cricket: writing cache: %w", err)
	}
	if err := os.Rename(tmp, c.cachePath); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("cricket: replacing cache: %w", err)
	}
	return nil
}
