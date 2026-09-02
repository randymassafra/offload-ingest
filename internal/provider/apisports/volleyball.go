// Package apisports is the volleyball provider.
//
// # Why this is a wrapper and not a client
//
// The pipeline already has a complete, live-verified API-Sports client at
// pkg/ingest/apisports. It covers all twelve verticals — volleyball among them
// — and carries a lot of behaviour that was expensive to get right:
//
//   - the 200-with-errors trap, where a rejected request returns HTTP 200 with
//     the reason inside the body, so a status-code check reports success
//   - the rate-limit headers, whose names invert the obvious reading
//   - 429 backoff with full jitter, and licence-driven per-host budgeting
//
// Writing a second volleyball client would duplicate every one of those, and
// the copy would drift. So this package supplies the constructor and the
// normalisation the directive asks for, and delegates transport to the client
// that is already tested against the live API. That is what "follows the same
// normalization patterns as our other API-Sports integrations" means in
// practice: not a similar implementation, the same one.
package apisports

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	ingestapi "github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
)

// Vertical is the API-Sports host this provider polls.
const Vertical = ingestapi.VerticalVolleyball

// Client is the volleyball provider.
type Client struct {
	api *ingestapi.Client
	now func() time.Time
}

// New builds a volleyball client.
//
// The key is threaded in rather than read from the environment; this package
// never touches os.Getenv. An empty key is not rejected here — the caller knows
// whether volleyball is entitled and validates the credential before building
// this. See cmd/loadtest.
func New(apiKey string) *Client {
	// The underlying constructor rejects an empty key, which would force this
	// signature to return an error. Keeping the signature the directive asks
	// for, the failure is deferred to the first call, where it is reported as a
	// normal request error rather than a panic.
	api, err := ingestapi.New(ingestapi.Config{APIKey: apiKey})
	if err != nil {
		return &Client{now: time.Now}
	}
	return &Client{api: api, now: time.Now}
}

// Option configures a Client.
type Option func(*Client)

// WithClient replaces the underlying API-Sports client, which is how a caller
// shares one rate limiter and one metrics registry across every vertical
// instead of letting each provider meter itself.
func WithClient(api *ingestapi.Client) Option { return func(c *Client) { c.api = api } }

// WithClock injects a clock, for tests.
func WithClock(now func() time.Time) Option { return func(c *Client) { c.now = now } }

// Configure applies options.
func (c *Client) Configure(opts ...Option) *Client {
	for _, o := range opts {
		o(c)
	}
	return c
}

// Games returns the day's volleyball card.
//
// Volleyball is swept by date, not by live=all: that parameter exists on only
// three of the twelve API-Sports hosts and this is not one of them — it answers
// "The Live field do not exist." One request returns the whole day and the
// caller filters, which is also what keeps the request budget survivable.
func (c *Client) Games(ctx context.Context, day time.Time) ([]Game, error) {
	if c.api == nil {
		return nil, fmt.Errorf("apisports: volleyball client has no credential")
	}
	if day.IsZero() {
		day = c.now()
	}
	spec, ok := ingestapi.SpecFor(Vertical)
	if !ok {
		return nil, fmt.Errorf("apisports: no spec for %s", Vertical)
	}

	env, err := c.api.Get(ctx, Vertical, spec.BulkPath, spec.BulkQuery(day))
	if err != nil {
		return nil, fmt.Errorf("apisports: volleyball sweep: %w", err)
	}
	var games []Game
	if len(env.Response) > 0 {
		if err := json.Unmarshal(env.Response, &games); err != nil {
			return nil, fmt.Errorf("apisports: decoding volleyball games: %w", err)
		}
	}
	return games, nil
}

// Live returns only the games currently in play.
func (c *Client) Live(ctx context.Context, day time.Time) ([]Game, error) {
	all, err := c.Games(ctx, day)
	if err != nil {
		return nil, err
	}
	out := make([]Game, 0, len(all))
	for _, g := range all {
		if g.InPlay() {
			out = append(out, g)
		}
	}
	return out, nil
}

// --- wire model --------------------------------------------------------------

// Game is one volleyball fixture, in the flat "games family" shape every
// API-Sports vertical except football uses.
//
// Generated from a live capture (fixtures/apisports/volleyball.json), not
// transcribed: scores here are plain set counts, where basketball reports
// per-quarter columns and baseball a per-inning map. The families differ per
// vertical and are reproduced rather than normalised into one shape.
type Game struct {
	ID        int            `json:"id"`
	Date      string         `json:"date"`
	Time      string         `json:"time"`
	Timestamp int64          `json:"timestamp"`
	Timezone  string         `json:"timezone"`
	Week      string         `json:"week"`
	Status    Status         `json:"status"`
	Country   Country        `json:"country"`
	League    League         `json:"league"`
	Teams     Teams          `json:"teams"`
	Scores    Scores         `json:"scores"`
	Periods   map[string]any `json:"periods"`
}

// Status is the fixture's state.
type Status struct {
	Long  string `json:"long"`
	Short string `json:"short"`
	Timer any    `json:"timer"`
}

// Country is the competition's country.
type Country struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Code string `json:"code"`
	Flag string `json:"flag"`
}

// League is the competition.
//
// Season is an integer on every vertical checked (volleyball, basketball,
// baseball, hockey, handball, football) — 2026, not "2026". Modelling it as a
// string looked reasonable and failed against the real capture immediately,
// which is the argument for testing providers against captured documents rather
// than hand-written ones.
type League struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Season int    `json:"season"`
	Logo   string `json:"logo"`
}

// Teams is the pair.
type Teams struct {
	Home Team `json:"home"`
	Away Team `json:"away"`
}

// Team is one side.
type Team struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Logo string `json:"logo"`
}

// Scores is the set tally.
type Scores struct {
	Home int `json:"home"`
	Away int `json:"away"`
}

// inPlayStatuses are the short codes meaning a match is live.
//
// Volleyball is played in sets, so the in-play codes are S1..S5 rather than the
// halves and quarters the other verticals use. An unrecognised status reads as
// not-live, which is the safe direction on a metered budget.
var inPlayStatuses = map[string]bool{
	"S1": true, "S2": true, "S3": true, "S4": true, "S5": true, "LIVE": true,
}

// InPlay reports whether the match is currently being played.
func (g Game) InPlay() bool {
	return inPlayStatuses[strings.ToUpper(strings.TrimSpace(g.Status.Short))]
}

// Finished reports whether the match has ended.
func (g Game) Finished() bool {
	s := strings.ToUpper(strings.TrimSpace(g.Status.Short))
	return s == "FT" || s == "AW" || s == "POST" || s == "CANC"
}

// Kickoff is the scheduled start.
func (g Game) Kickoff() time.Time {
	if g.Timestamp == 0 {
		return time.Time{}
	}
	return time.Unix(g.Timestamp, 0).UTC()
}

// Fixture renders the pairing for logs.
func (g Game) Fixture() string { return g.Teams.Home.Name + " v " + g.Teams.Away.Name }
