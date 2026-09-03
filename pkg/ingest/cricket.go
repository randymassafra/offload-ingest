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
	cricketprovider "github.com/offloadintelligence/offload-ingest/internal/provider/cricket"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/scope"
	"github.com/offloadintelligence/offload-ingest/pkg/metrics"
)

// Cricket ingestion.
//
// Cricket does not come from API-Sports — that provider has no cricket host —
// so it cannot ride the bulk sweeper. It gets its own streamer, which is then
// merged with the API-Sports one so both reach Kafka through the same
// scope-enforcing publisher. The enforcement path is shared; only the fetching
// differs. This is the same arrangement golf has, deliberately.
//
// # Cadence
//
// As with golf, the poll interval is driven by the provider's cache lifetime
// rather than by the licence budget: Cricbuzz is a separate RapidAPI
// subscription and is not metered against the API-Sports per-host quota the
// limiter manages.
//
// Cricket needs one more cadence tier than golf, and the reason is the sport
// rather than the API. A cricket match spends a great deal of its wall-clock
// duration not being played — innings breaks, drinks, lunch, tea, stumps, rain
// — and during those the scorecard is fixed. Golf's three tiers would have to
// treat "match in progress" as a single rate and would spend the live budget
// polling a document that cannot move for the ten minutes between innings.
//
// # Discovery
//
// Golf resolves a tournament from a season schedule and holds it. Cricket has
// no equivalent: matches are discovered from /matches/v1/live, which returns
// whatever is in play right now across every series worldwide. The streamer
// therefore re-resolves whenever the match it was following finishes, rather
// than resolving once at startup.

// CricketPollInterval is the resting interval between scorecard reads.
//
// Slightly under the provider's resting cache lifetime so a poll lands just
// after the cached copy expires rather than serving an almost-expired document
// for another full cycle. The live intervals below follow the same rule.
const CricketPollInterval = 110 * time.Minute

// CricketCadence is how often the scorecard is being polled, and why.
//
// Named distinctly from the scheduler's Cadence for the same reason
// GolfCadence is: the two are different mechanisms answering to different
// constraints — one a licence budget, the other a cache lifetime on a separate
// subscription — and sharing a name would invite treating them as one.
type CricketCadence struct {
	// Interval between polls.
	Interval time.Duration
	// TTL requested of the provider's cache. Polling faster than the cache
	// lifetime returns identical bytes from disk, so the two move together.
	TTL time.Duration
	// Mode is the operator-facing label: "live", "break", "static" or
	// "throttled".
	Mode string
	// Reason explains the choice, for the log line on a switch.
	Reason string
}

// cricketCadenceFor decides how hard to poll, from the match state.
//
// Four states, because the rate of change differs by orders of magnitude
// between them:
//
//	live      the score changes every ball
//	break     the match is on but the score is fixed — innings break, rain,
//	          stumps, an interval. Nothing moves, sometimes for hours.
//	static    nothing is in play anywhere; there is no document to follow
//
// The break tier is the one that matters commercially. Cricket's non-playing
// intervals are long enough that treating them as live would spend most of a
// day's requests re-reading a document known not to have changed.
func cricketCadenceFor(m cricketprovider.MatchInfo, found bool) CricketCadence {
	switch {
	case !found:
		return CricketCadence{
			Interval: CricketPollInterval, TTL: cricketprovider.CacheTTL,
			Mode:   "static",
			Reason: "no match in play; resting until one starts",
		}
	case m.Live():
		return CricketCadence{
			Interval: 25 * time.Second, TTL: cricketprovider.LiveCacheTTL,
			Mode:   "live",
			Reason: "ball in play; the scorecard changes every delivery",
		}
	case m.Started():
		return CricketCadence{
			Interval: 150 * time.Second, TTL: cricketprovider.InningsBreakCacheTTL,
			Mode: "break",
			Reason: "match is on but not in play (" + m.State +
				"); the score is fixed until it resumes",
		}
	default:
		return CricketCadence{
			Interval: CricketPollInterval, TTL: cricketprovider.CacheTTL,
			Mode:   "static",
			Reason: "match state " + strconv.Quote(m.State) + " is not in play",
		}
	}
}

// CricketStreamer polls a match scorecard.
type CricketStreamer struct {
	client   *cricketprovider.Client
	registry *metrics.Registry
	log      *slog.Logger
	now      func() time.Time

	mu       sync.Mutex
	nextDue  time.Time
	sequence int64

	// pinned is a match id supplied by configuration. When set, discovery is
	// skipped entirely and this match is followed to its end and beyond.
	pinned int

	// match is the fixture currently being followed, and found says whether
	// there is one. Kept so the cadence can be recomputed each cycle from the
	// state without a second discovery call.
	match cricketprovider.MatchInfo
	found bool

	// cadence is the mode currently in force, so a switch can be logged once
	// rather than on every poll.
	cadence CricketCadence
}

// CricketConfig configures the cricket streamer.
type CricketConfig struct {
	Client   *cricketprovider.Client
	Registry *metrics.Registry
	Logger   *slog.Logger
	Now      func() time.Time
	// MatchID pins a specific match. Left zero, the streamer discovers whatever
	// is live and re-discovers when that match ends.
	MatchID int
}

// NewCricketStreamer builds the cricket streamer.
func NewCricketStreamer(cfg CricketConfig) (*CricketStreamer, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("ingest: cricket streamer needs a client")
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
	return &CricketStreamer{
		client: cfg.Client, registry: cfg.Registry, log: cfg.Logger,
		now: cfg.Now, pinned: cfg.MatchID, nextDue: cfg.Now(),
	}, nil
}

// Next returns the current scorecard when one is due.
//
// Returns an empty slice rather than blocking for the best part of two hours:
// the composite streamer polls its sources and would otherwise be held hostage
// by the slowest cadence in the set.
func (c *CricketStreamer) Next(ctx context.Context) ([]generators.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.now().Before(c.nextDue) {
		c.mu.Unlock()
		return nil, nil
	}
	// Provisionally reschedule at the current cadence. It is recomputed below
	// once the match state is known.
	interval := c.cadence.Interval
	if interval <= 0 {
		interval = CricketPollInterval
	}
	c.nextDue = c.now().Add(interval)
	c.mu.Unlock()

	matchID, info, ok, err := c.resolveMatch(ctx)
	if err != nil {
		c.syncThrottleState()
		return nil, fmt.Errorf("ingest: cricket discovery: %w", err)
	}
	if !ok {
		// Nothing in play anywhere. Not an error, and not a starved feed
		// either — it is a quiet afternoon. The cadence drops to resting so the
		// loop is not asking every twenty-five seconds whether cricket has
		// started yet.
		c.applyCadence(cricketprovider.MatchInfo{}, false)
		// Recorded as a successful poll with no data. That is what separates a
		// quiet feed from a broken one on the readiness probe; see
		// pkg/metrics/health.go.
		c.registry.RecordPoll("cricket")
		return nil, nil
	}

	card, meta, err := c.client.Scorecard(ctx, cricketprovider.ScorecardRequest{MatchID: matchID})
	if err != nil {
		// The throttle state has to be published here as well as in
		// applyCadence. A 429 with no cache to fall back on returns an error,
		// which skips applyCadence entirely — so reporting the hard floor only
		// from the success path would leave the readiness probe green during
		// precisely the outage it exists to report. Golf learned this the same
		// way; see GolfStreamer.Next.
		c.syncThrottleState()
		return nil, fmt.Errorf("ingest: cricket scorecard %d: %w", matchID, err)
	}
	c.registry.Requests.Inc()
	c.registry.Sport("cricket").Requests.Inc()
	c.applyCadence(info, true)
	if meta.Stale {
		c.log.Warn("ingest: serving a stale cricket scorecard",
			"match", matchID, "age", meta.Age.Round(time.Second), "err", meta.Err)
	}

	// The payload is the provider's document, marshalled back to the generic
	// form the pipeline carries. Nothing is added to it — the scope identity
	// goes on the envelope.
	raw, err := json.Marshal(card)
	if err != nil {
		return nil, fmt.Errorf("ingest: encoding cricket scorecard: %w", err)
	}
	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}

	// Envelope normalisation. Attempted first, exactly as the other two paths
	// do it, but a Cricbuzz scorecard carries NO identity of its own: its top
	// level is scorecard[], status, ismatchcomplete, appindex and a timestamp.
	// There is no series id, no match id, not even a team id above the innings.
	//
	// So the identity is supplied from discovery, which is where cricket's
	// identity actually lives. This is not a shortcut around Normalize — there
	// is nothing in the document for it to find, and a series id invented from
	// the payload would be a fiction.
	id := scope.Normalize(payload)
	if id.LeagueID == 0 {
		id.LeagueID = info.SeriesID
	}
	if id.LeagueName == "" {
		id.LeagueName = info.SeriesName
	}

	c.mu.Lock()
	c.sequence++
	seq := c.sequence
	c.mu.Unlock()

	msg := generators.Message{
		Sport:              generators.SportCricket,
		Kind:               generators.FeedBoxScore,
		Endpoint:           "/mcenter/v1/{matchid}/hscard",
		Model:              "Scorecard",
		FixtureID:          strconv.Itoa(matchID),
		Sequence:           seq,
		Emitted:            c.now(),
		NormalizedLeagueID: id.LeagueID,
		ProviderOrgID:      id.OrgID,
		LeagueName:         id.LeagueName,
		Payload:            payload,
	}
	c.registry.Messages.Inc()
	c.registry.Sport("cricket").Messages.Inc()
	c.registry.RecordPoll("cricket")
	c.registry.RecordData("cricket")

	// Release a finished match so the next poll goes looking for another.
	//
	// The completion signal is taken from the scorecard rather than from the
	// retained MatchInfo, and that distinction is the whole of this block. The
	// retained fixture is a snapshot from whenever discovery last ran; its
	// State says "In Progress" for as long as it is held, however long ago
	// play actually ended. Trusting it meant the streamer re-read a finished
	// scorecard forever — a feed green on every metric, still serving
	// Tuesday's match on Friday.
	//
	// ismatchcomplete rides on the document already fetched, so noticing costs
	// no request.
	if card.Ismatchcomplete {
		c.release()
		c.log.Info("cricket match complete; releasing it",
			"match", matchID, "series", info.SeriesName, "status", card.Status)
	}

	c.log.Debug("cricket scorecard",
		"match", matchID, "series", info.SeriesName, "state", info.State,
		"innings", len(card.Scorecard), "complete", card.Ismatchcomplete,
		"cached", meta.FromCache)
	return []generators.Message{msg}, nil
}

// Sports reports what this streamer covers.
func (c *CricketStreamer) Sports() []generators.Sport {
	return []generators.Sport{generators.SportCricket}
}

// Mode reports production; cricket only streams live.
func (c *CricketStreamer) Mode() Mode { return ModeProduction }

// Close releases resources.
func (c *CricketStreamer) Close() error { return nil }

// resolveMatch decides which match to poll.
//
// Re-resolved rather than cached for the life of the streamer, which is the
// substantive difference from golf. A golf tournament runs for four days and
// the streamer follows one to its end. Cricket matches finish and are replaced
// continuously, and a streamer that resolved once at startup would poll a
// completed scorecard for the rest of the deployment — a feed that looks
// perfectly healthy on every metric and has been serving a finished match since
// Tuesday.
//
// The reported match is retained while it is still running, so the common case
// costs no discovery request.
func (c *CricketStreamer) resolveMatch(ctx context.Context) (int, cricketprovider.MatchInfo, bool, error) {
	// A pinned match skips discovery entirely and is followed regardless of
	// state, so an operator can point the appliance at one fixture.
	if c.pinned > 0 {
		c.mu.Lock()
		info, found := c.match, c.found
		c.mu.Unlock()
		if found {
			return c.pinned, info, true, nil
		}
		// The pinned match's state is still unknown; discover it once so the
		// cadence has something to work from.
		info, found, err := c.lookup(ctx, c.pinned)
		if err != nil {
			return 0, cricketprovider.MatchInfo{}, false, err
		}
		if found {
			c.mu.Lock()
			c.match, c.found = info, true
			c.mu.Unlock()
		}
		return c.pinned, info, true, nil
	}

	c.mu.Lock()
	current, have := c.match, c.found
	c.mu.Unlock()
	if have && !current.Complete() {
		return current.MatchID, current, true, nil
	}

	list, err := c.client.LiveMatches(ctx)
	if err != nil {
		return 0, cricketprovider.MatchInfo{}, false, err
	}
	// In-play first; a match merely started (rain, stumps) is the fallback, so
	// an appliance still has something to publish through a rain delay rather
	// than going silent.
	var fallback cricketprovider.MatchInfo
	var haveFallback bool
	for _, m := range list.Matches() {
		if m.Live() {
			c.adopt(m)
			return m.MatchID, m, true, nil
		}
		if !haveFallback && m.Started() {
			fallback, haveFallback = m, true
		}
	}
	if haveFallback {
		c.adopt(fallback)
		return fallback.MatchID, fallback, true, nil
	}

	c.mu.Lock()
	c.match, c.found = cricketprovider.MatchInfo{}, false
	c.mu.Unlock()
	return 0, cricketprovider.MatchInfo{}, false, nil
}

// lookup finds one match in the live list by id.
func (c *CricketStreamer) lookup(ctx context.Context, id int) (cricketprovider.MatchInfo, bool, error) {
	list, err := c.client.LiveMatches(ctx)
	if err != nil {
		return cricketprovider.MatchInfo{}, false, err
	}
	for _, m := range list.Matches() {
		if m.MatchID == id {
			return m, true, nil
		}
	}
	return cricketprovider.MatchInfo{}, false, nil
}

// adopt records the match now being followed, logging a change of fixture.
func (c *CricketStreamer) adopt(m cricketprovider.MatchInfo) {
	c.mu.Lock()
	previous := c.match
	c.match, c.found = m, true
	c.mu.Unlock()

	if previous.MatchID != m.MatchID {
		c.log.Info("cricket following a new match",
			"match", m.MatchID, "series", m.SeriesName, "desc", m.MatchDesc,
			"format", m.MatchFormat, "state", m.State,
			"previous", previous.MatchID)
	}
}

// release forgets the match being followed, so the next poll re-discovers.
func (c *CricketStreamer) release() {
	c.mu.Lock()
	c.match, c.found = cricketprovider.MatchInfo{}, false
	c.mu.Unlock()
}

// applyCadence recomputes the polling rate from the match state and pushes the
// resulting TTL down to the provider.
//
// Called after every successful fetch, because the state that decides it is
// carried on the document just received. Nothing here costs a request.
func (c *CricketStreamer) applyCadence(m cricketprovider.MatchInfo, found bool) {
	c.mu.Lock()
	previous := c.cadence
	c.mu.Unlock()

	next := cricketCadenceFor(m, found)

	// The hard floor overrides everything. A provider that has been throttled
	// reports the resting lifetime however short a TTL is requested, so the
	// poll interval has to follow it or the loop would spin against a cache
	// that is not expiring.
	if throttled, until := c.client.Throttled(); throttled {
		next = CricketCadence{
			Interval: CricketPollInterval, TTL: cricketprovider.CacheTTL,
			Mode: "throttled",
			Reason: "RapidAPI returned 429; the hard floor holds until " +
				until.Format(time.RFC3339),
		}
	}

	c.client.SetCacheTTL(next.TTL)

	c.mu.Lock()
	c.cadence = next
	// Bring the next poll forward if the new cadence is faster than the one
	// scheduled a moment ago; a match resuming after an innings break should
	// not wait out a break-mode interval.
	if due := c.now().Add(next.Interval); due.Before(c.nextDue) {
		c.nextDue = due
	}
	c.mu.Unlock()

	c.syncThrottleState()

	if previous.Mode != next.Mode {
		verb := "switched to"
		if previous.Mode == "" {
			verb = "starting in"
		}
		c.log.Info("cricket cadence "+verb+" "+next.Mode+" mode",
			"from", previous.Mode, "to", next.Mode,
			"poll_every", next.Interval.String(), "cache_ttl", next.TTL.String(),
			"state", m.State, "reason", next.Reason)
	}
}

// Cadence reports the polling mode currently in force.
func (c *CricketStreamer) Cadence() CricketCadence {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cadence
}

// syncThrottleState publishes the cricket hard floor to the metrics registry.
//
// Reads the client rather than a cadence mode so that both the success and the
// failure path report the same thing, and keyed by provider because a floor is
// charged against a subscription.
func (c *CricketStreamer) syncThrottleState() {
	throttled, _ := c.client.Throttled()
	c.registry.MarkRateLimited(cricketProviderName, throttled)
}

// cricketProviderName is how the Cricbuzz subscription appears in health and
// provider-mode output. It matches ProviderCricbuzz in runtime.go.
const cricketProviderName = ProviderCricbuzz
