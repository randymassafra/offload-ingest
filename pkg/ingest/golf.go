package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
	golfprovider "github.com/offloadintelligence/offload-ingest/internal/provider/golf"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/scope"
	"github.com/offloadintelligence/offload-ingest/pkg/metrics"
)

// Golf ingestion.
//
// Golf does not come from API-Sports — that provider has no golf host — so it
// cannot ride the bulk sweeper. It gets its own streamer, which is then merged
// with the API-Sports one so both reach Kafka through the same scope-enforcing
// publisher. The enforcement path is shared; only the fetching differs.
//
// # Cadence
//
// The provider caches a leaderboard for an hour, so polling faster than that
// returns the same document from disk and spends nothing. The poll interval is
// therefore driven by the cache TTL rather than by the licence budget: this
// upstream is a separate RapidAPI subscription and is not metered against the
// API-Sports per-host quota that the limiter manages.
//
// A live final round genuinely wants fresher data than an hour. That is a TTL
// decision, not a scheduling one — lower GolfCacheTTL and this loop follows it.

// GolfPollInterval is the resting interval between leaderboard reads.
//
// Slightly under the resting cache lifetime so a poll lands just after the
// cached copy expires rather than serving a nearly-full-hour-old document for
// another cycle. The live intervals below follow the same rule.
const GolfPollInterval = 55 * time.Minute

// GolfCadence is how often the leaderboard is being polled, and why.
//
// Named distinctly from the scheduler's Cadence, which describes the
// API-Sports sweep rate: the two are different mechanisms answering to
// different constraints — one a licence budget, the other a cache lifetime on a
// separate subscription — and sharing a name would invite treating them as one.
type GolfCadence struct {
	// Interval between polls.
	Interval time.Duration
	// TTL requested of the provider's cache. Polling faster than the cache
	// lifetime returns identical bytes from disk, so the two move together.
	TTL time.Duration
	// Mode is the operator-facing label: "live", "final-round" or "static".
	Mode string
	// Reason explains the choice, for the log line on a switch.
	Reason string
}

// cadenceFor decides how hard to poll, from what the tournament is doing.
//
// Three states, because the audience and the rate of change differ by an order
// of magnitude between them:
//
//	final round   positions move constantly and the audience is largest
//	in progress   worth following, but a five-minute-old board is defensible
//	otherwise     the document does not change at all
//
// roundID identifies the round in play; 4 is the final round of a standard
// stroke-play event. It is read from the leaderboard we just fetched, so
// detecting the final round costs no extra request.
func cadenceFor(inProgress bool, roundID int) GolfCadence {
	switch {
	case inProgress && roundID >= 4:
		return GolfCadence{
			Interval: 50 * time.Second, TTL: golfprovider.FinalRoundCacheTTL,
			Mode:   "final-round",
			Reason: "final round in play",
		}
	case inProgress:
		return GolfCadence{
			Interval: 4*time.Minute + 30*time.Second, TTL: golfprovider.LiveCacheTTL,
			Mode:   "live",
			Reason: "tournament in progress",
		}
	default:
		return GolfCadence{
			Interval: GolfPollInterval, TTL: golfprovider.CacheTTL,
			Mode:   "static",
			Reason: "no tournament in play; the leaderboard is not changing",
		}
	}
}

// GolfStreamer polls a tournament leaderboard.
type GolfStreamer struct {
	client   *golfprovider.Client
	registry *metrics.Registry
	log      *slog.Logger
	now      func() time.Time

	orgID   string
	tournID string
	year    string

	mu       sync.Mutex
	nextDue  time.Time
	sequence int64
	resolved bool
	// entry is the resolved tournament, kept so the cadence can be recomputed
	// each cycle from its window without re-fetching the schedule.
	entry golfprovider.ScheduleEntry
	// cadence is the mode currently in force, so a switch can be logged once
	// rather than on every poll.
	cadence GolfCadence
}

// GolfConfig configures the golf streamer.
type GolfConfig struct {
	Client   *golfprovider.Client
	Registry *metrics.Registry
	Logger   *slog.Logger
	Now      func() time.Time
	// OrgID is the tour: "1" PGA, "2" LIV. Defaults to the PGA Tour.
	OrgID string
	// TournID and Year pin a specific tournament. Left empty, the streamer
	// resolves the most recent one from the schedule.
	TournID string
	Year    string
}

// NewGolfStreamer builds the golf streamer.
func NewGolfStreamer(cfg GolfConfig) (*GolfStreamer, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("ingest: golf streamer needs a client")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Registry == nil {
		cfg.Registry = metrics.NewRegistry(cfg.Now)
	}
	if cfg.OrgID == "" {
		cfg.OrgID = "1"
	}
	if cfg.Year == "" {
		cfg.Year = strconv.Itoa(cfg.Now().Year())
	}
	return &GolfStreamer{
		client: cfg.Client, registry: cfg.Registry, log: cfg.Logger,
		now: cfg.Now, orgID: cfg.OrgID, tournID: cfg.TournID, year: cfg.Year,
		nextDue: cfg.Now(),
	}, nil
}

// Next returns the current leaderboard when one is due.
//
// Returns an empty slice rather than blocking for the best part of an hour: the
// composite streamer polls its sources and would otherwise be held hostage by
// the slowest cadence in the set.
func (g *GolfStreamer) Next(ctx context.Context) ([]generators.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	if g.now().Before(g.nextDue) {
		g.mu.Unlock()
		return nil, nil
	}
	// Provisionally reschedule at the current cadence. It is recomputed below
	// once the leaderboard says which round is in play.
	interval := g.cadence.Interval
	if interval <= 0 {
		interval = GolfPollInterval
	}
	g.nextDue = g.now().Add(interval)
	g.mu.Unlock()

	if err := g.resolveTournament(ctx); err != nil {
		return nil, err
	}

	lb, meta, err := g.client.Leaderboard(ctx, golfprovider.LeaderboardRequest{
		OrgID: g.orgID, TournID: g.tournID, Year: g.year,
	})
	if err != nil {
		return nil, fmt.Errorf("ingest: golf leaderboard: %w", err)
	}
	g.registry.Requests.Inc()
	g.registry.Sport("golf").Requests.Inc()
	g.applyCadence(lb)
	if meta.Stale {
		g.log.Warn("ingest: serving a stale golf leaderboard",
			"age", meta.Age.Round(time.Minute), "err", meta.Err)
	}

	// The payload is the provider's document, marshalled back to the generic
	// form the pipeline carries. Nothing is added to it — the scope identity
	// goes on the envelope.
	raw, err := json.Marshal(lb)
	if err != nil {
		return nil, fmt.Errorf("ingest: encoding golf leaderboard: %w", err)
	}
	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}

	// Envelope normalisation, exactly as the API-Sports path does it: read the
	// scope identity once, here, and attach it beside the payload.
	id := scope.Normalize(payload)

	g.mu.Lock()
	g.sequence++
	seq := g.sequence
	g.mu.Unlock()

	msg := generators.Message{
		Sport:              generators.SportGolf,
		Kind:               generators.FeedBoxScore,
		Endpoint:           "/leaderboard",
		Model:              "Leaderboard",
		FixtureID:          g.tournID,
		Sequence:           seq,
		Emitted:            g.now(),
		NormalizedLeagueID: id.LeagueID,
		ProviderOrgID:      id.OrgID,
		LeagueName:         id.LeagueName,
		Payload:            payload,
	}
	g.registry.Messages.Inc()
	g.registry.Sport("golf").Messages.Inc()
	g.log.Debug("golf leaderboard",
		"tournament", g.tournID, "tour", id.OrgID,
		"players", len(lb.Rows), "status", lb.Status, "cached", meta.FromCache)
	return []generators.Message{msg}, nil
}

// resolveTournament finds a tournament to poll when none was pinned.
//
// Done once and cached on the streamer: the schedule is a season-long document
// that does not change during a run, and re-fetching it every cycle would spend
// a request to learn nothing.
func (g *GolfStreamer) resolveTournament(ctx context.Context) error {
	g.mu.Lock()
	if g.resolved || g.tournID != "" {
		g.mu.Unlock()
		return nil
	}
	g.mu.Unlock()

	sched, err := g.client.Schedule(ctx, g.orgID, g.year)
	if err != nil {
		return fmt.Errorf("ingest: golf schedule: %w", err)
	}
	if len(sched.Schedule) == 0 {
		return fmt.Errorf("ingest: golf schedule for %s is empty", g.year)
	}
	now := g.now()
	pick, ok := sched.Current(now)
	if !ok {
		// Every tournament in this season is still ahead of us — early January,
		// or a season that has not opened. Fall back to the previous season,
		// which has a full set of played events.
		prev := strconv.Itoa(now.Year() - 1)
		if prev != g.year {
			g.log.Info("golf: no played tournament this season, falling back",
				"from", g.year, "to", prev)
			g.year = prev
			g.resolved = false
			return g.resolveTournament(ctx)
		}
		return fmt.Errorf("ingest: no played golf tournament found for %s", g.year)
	}

	state := "completed"
	if pick.InProgress(now) {
		state = "in progress"
	}
	g.mu.Lock()
	g.tournID, g.resolved, g.entry = pick.TournID, true, pick
	g.mu.Unlock()
	g.log.Info("golf tournament resolved",
		"tournament", pick.Name, "id", pick.TournID, "year", g.year,
		"state", state,
		"window", pick.Date.Start.Time.Format("2006-01-02")+" to "+
			pick.Date.End.Time.Format("2006-01-02"))
	return nil
}

// Sports reports what this streamer produces.
func (g *GolfStreamer) Sports() []generators.Sport {
	return []generators.Sport{generators.SportGolf}
}

// Mode reports production; golf only streams live.
func (g *GolfStreamer) Mode() Mode { return ModeProduction }

// Close releases resources.
func (g *GolfStreamer) Close() error { return nil }

// applyCadence recomputes the polling rate from the tournament's state and the
// round just read, and pushes the resulting TTL down to the provider.
//
// Called after every successful fetch, because the state that decides it —
// whether the window is open, and which round is in play — is carried on the
// document we just received. Nothing here costs a request.
func (g *GolfStreamer) applyCadence(lb *golfprovider.Leaderboard) {
	now := g.now()

	g.mu.Lock()
	inProgress := g.entry.InProgress(now)
	previous := g.cadence
	g.mu.Unlock()

	next := cadenceFor(inProgress, lb.RoundID.Int())

	// The hard floor overrides everything. A provider that has been throttled
	// reports the resting lifetime however short a TTL is requested, so the
	// poll interval has to follow it or the loop would spin against a cache
	// that is not expiring.
	if throttled, until := g.client.Throttled(); throttled {
		next = GolfCadence{
			Interval: GolfPollInterval, TTL: golfprovider.CacheTTL,
			Mode: "throttled",
			Reason: "RapidAPI returned 429; the hard floor holds until " +
				until.Format(time.RFC3339),
		}
	}

	g.client.SetCacheTTL(next.TTL)

	g.mu.Lock()
	g.cadence = next
	// Bring the next poll forward if the new cadence is faster than the one
	// scheduled a moment ago; a switch into the final round should not wait out
	// a static-mode interval.
	if due := now.Add(next.Interval); due.Before(g.nextDue) {
		g.nextDue = due
	}
	g.mu.Unlock()

	g.registry.Golf.CadenceMinutes.Set(next.Interval.Minutes())
	g.registry.Golf.Throttled.Set(next.Mode == "throttled")

	if previous.Mode != next.Mode {
		level := "switched to"
		if previous.Mode == "" {
			level = "starting in"
		}
		g.log.Info("golf cadence "+level+" "+next.Mode+" mode",
			"from", previous.Mode, "to", next.Mode,
			"poll_every", next.Interval.String(), "cache_ttl", next.TTL.String(),
			"round", lb.RoundID.Int(), "reason", next.Reason)
	}
}

// Cadence reports the polling mode currently in force, for the dashboard.
func (g *GolfStreamer) Cadence() GolfCadence {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cadence
}
