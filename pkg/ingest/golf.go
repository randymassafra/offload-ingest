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

// GolfPollInterval is how often the leaderboard is re-read.
//
// Slightly under the cache TTL so a poll lands just after the cached copy
// expires rather than serving a nearly-full-hour-old document for another
// cycle.
const GolfPollInterval = 55 * time.Minute

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
	g.nextDue = g.now().Add(GolfPollInterval)
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
	g.tournID, g.resolved = pick.TournID, true
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
