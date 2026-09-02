package generators

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	golfprovider "github.com/offloadintelligence/offload-ingest/internal/provider/golf"
)

// Golf comes from live-golf-data, via RapidAPI.
//
// API-Sports has no golf host — verified by probing every plausible name — so
// golf is one of the three sports reached through a RapidAPI vendor rather than
// the primary provider.
//
// This generator emits the live-golf-data document shape, not SportsDataIO's.
// That matters more than it sounds: the simulation exists to load-test the real
// pipeline, and a simulation emitting a retired vendor's schema would be
// testing a shape nothing will ever publish. The models are shared with the
// live client in internal/provider/golf, so the two cannot drift.
//
// The provider's quirks are reproduced rather than cleaned up:
//
//   - Integers arrive as MongoDB extended JSON, {"$numberInt": "18"}. MongoInt
//     marshals back to a plain number so the upstream's serialisation accident
//     does not propagate into our schema.
//   - Scores are signed strings — "-9", "+2", "E" for even. "E" has no integer
//     spelling, which is why they stay strings.
func init() {
	mk := func(rnd *rand.Rand) *golfSim { s := &golfSim{base: newBase(rnd)}; s.reset(); return s }

	register(Endpoint{
		Sport: SportGolf, Kind: FeedBoxScore,
		Provider: ProviderLiveGolf, Provenance: ProvenanceCaptured,
		Path: "/leaderboard", Model: "Leaderboard",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.leaderboard(), "", true }
	})

	register(Endpoint{
		Sport: SportGolf, Kind: FeedPlayerStats,
		Provider: ProviderLiveGolf, Provenance: ProvenanceCaptured,
		Path: "/leaderboard", Projection: "leaderboardRows[]", Model: "Row[]",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.rows(), "", true }
	})

	// The finest grain this provider offers is a round, not a hole: unlike the
	// feed it replaced, live-golf-data carries no hole-by-hole scoring. The
	// telemetry feed therefore emits the row that just changed — a player's
	// position update — which is the highest-frequency real signal available.
	register(Endpoint{
		Sport: SportGolf, Kind: FeedTelemetry,
		Provider: ProviderLiveGolf, Provenance: ProvenanceCaptured,
		Path: "/leaderboard", Projection: "leaderboardRows[]", Model: "Row",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.lastMove() }
	})
}

// golfFieldSize is how many players a simulated tournament carries.
//
// Thirty, matching the captured TOUR Championship field. A full-field major
// would be 150+, but the payload-size range that stresses a consumer is already
// covered by the bulk soccer and basketball sweeps.
const golfFieldSize = 30

// golfSim plays a four-round tournament, shot by shot at round granularity.
type golfSim struct {
	base
	orgID   string
	tournID string
	year    string
	course  string

	players []player
	// scores[playerIndex][roundIndex] is strokes; 0 means unplayed.
	scores [][]int
	par    int
	round  int // 1-4
	// moved is the player whose row changed on the last tick, or -1.
	moved int
	// status walks Scheduled -> In Progress -> Official.
	status string
}

var golfCourses = []string{
	"East Lake Golf Club", "Augusta National", "Pebble Beach", "TPC Sawgrass",
	"Torrey Pines", "Bay Hill", "Muirfield Village",
}

func (g *golfSim) reset() {
	g.newFixture("GOLF", 4, 0, 30*time.Second)
	// Tournament ids are three digits, zero-padded, as the provider sends them.
	g.tournID = fmt.Sprintf("%03d", 1+g.rnd.Intn(600))
	g.orgID = "1" // PGA Tour; "2" is LIV
	g.year = fmt.Sprintf("%d", seasonFor(now()))
	g.course = pick(g.rnd, golfCourses)
	g.par = 70 + g.rnd.Intn(3)
	g.round = 1
	g.moved = -1
	g.status = "In Progress"

	g.players = roster(700, golfFieldSize, []string{"G"}, g.rnd)
	g.scores = make([][]int, golfFieldSize)
	for i := range g.scores {
		g.scores[i] = make([]int, 4)
	}
}

func (g *golfSim) advance() {
	if g.over {
		return
	}
	// One player completes a round per tick, so a consumer sees the leaderboard
	// reorder continuously rather than in four jumps.
	idx := -1
	for i := 0; i < golfFieldSize; i++ {
		if g.scores[i][g.round-1] == 0 {
			idx = i
			break
		}
	}
	if idx < 0 {
		// The round is complete.
		if g.round >= 4 {
			g.status = "Official"
			g.over = true
			return
		}
		g.round++
		g.period = g.round
		return
	}

	// A round is par-ish with a spread: the field's best days are about six
	// under and the worst about eight over.
	g.scores[idx][g.round-1] = g.par - 6 + g.rnd.Intn(15)
	g.moved = idx
}

// totalStrokes is a player's strokes across completed rounds.
func (g *golfSim) totalStrokes(i int) int {
	total := 0
	for _, s := range g.scores[i] {
		total += s
	}
	return total
}

// roundsPlayed counts a player's completed rounds.
func (g *golfSim) roundsPlayed(i int) int {
	n := 0
	for _, s := range g.scores[i] {
		if s > 0 {
			n++
		}
	}
	return n
}

// toPar renders a score relative to par the way the provider does: a signed
// string, with "E" for level.
func toPar(strokes, par, rounds int) string {
	if rounds == 0 {
		return "E"
	}
	diff := strokes - par*rounds
	switch {
	case diff == 0:
		return "E"
	case diff > 0:
		return fmt.Sprintf("+%d", diff)
	default:
		return fmt.Sprintf("%d", diff)
	}
}

// standings returns player indices ordered by score, best first.
func (g *golfSim) standings() []int {
	order := make([]int, golfFieldSize)
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		ia, ib := order[a], order[b]
		ra, rb := g.roundsPlayed(ia), g.roundsPlayed(ib)
		// A player who has played fewer rounds cannot be ranked against one who
		// has played more on raw strokes; more rounds ranks first.
		if ra != rb {
			return ra > rb
		}
		return g.totalStrokes(ia) < g.totalStrokes(ib)
	})
	return order
}

// row builds one leaderboard row.
func (g *golfSim) row(i, position int) golfprovider.Row {
	p := g.players[i]
	played := g.roundsPlayed(i)
	strokes := g.totalStrokes(i)

	rounds := make([]golfprovider.Round, 0, played)
	for r := 0; r < 4; r++ {
		if g.scores[i][r] == 0 {
			continue
		}
		rounds = append(rounds, golfprovider.Round{
			RoundID:    golfprovider.MongoInt(r + 1),
			Strokes:    golfprovider.MongoInt(g.scores[i][r]),
			ScoreToPar: toPar(g.scores[i][r], g.par, 1),
			CourseID:   "514",
			CourseName: g.course,
		})
	}

	status := "active"
	if g.over {
		status = "complete"
	}
	// Position is shared when scores tie, which the provider renders as "T2".
	pos := fmt.Sprintf("%d", position)
	if position > 1 && g.rnd.Intn(3) == 0 {
		pos = "T" + pos
	}

	// A player's progress through the round drives both currentHole and thru,
	// so they are derived together rather than rolled independently — a row
	// reading "thru 7" while sitting on the 15th tee is not a document the
	// provider would ever send.
	done := g.scores[i][g.round-1] != 0
	hole := 18
	if !done {
		hole = 1 + g.rnd.Intn(18)
	}

	return golfprovider.Row{
		PlayerID:                        fmt.Sprintf("%d", p.ID),
		FirstName:                       firstOf(p.Name),
		LastName:                        lastOf(p.Name),
		IsAmateur:                       false,
		CourseID:                        "514",
		Status:                          status,
		Position:                        pos,
		Total:                           toPar(strokes, g.par, played),
		CurrentRoundScore:               toPar(g.scores[i][g.round-1], g.par, 1),
		TotalStrokesFromCompletedRounds: fmt.Sprintf("%d", strokes),
		CurrentHole:                     golfprovider.MongoInt(hole),
		StartingHole:                    golfprovider.MongoInt(1),
		CurrentRound:                    golfprovider.MongoInt(g.round),
		Thru:                            thru(done, hole),
		RoundComplete:                   done,
		Rounds:                          rounds,
		// A display string, as the provider sends it — "1:26pm".
		TeeTime:          g.start.Format("3:04pm"),
		TeeTimeTimestamp: golfprovider.MongoDate{Time: g.start.UTC()},
	}
}

// rows is the whole leaderboard, in position order.
func (g *golfSim) rows() []golfprovider.Row {
	order := g.standings()
	out := make([]golfprovider.Row, 0, len(order))
	for pos, idx := range order {
		out = append(out, g.row(idx, pos+1))
	}
	return out
}

// leaderboard is the whole document.
func (g *golfSim) leaderboard() golfprovider.Leaderboard {
	cut := golfprovider.CutLine{
		CutCount: golfprovider.MongoInt(golfFieldSize),
		CutScore: toPar(g.par*2+4, g.par, 2),
	}
	return golfprovider.Leaderboard{
		OrgID:       g.orgID,
		Year:        g.year,
		TournID:     g.tournID,
		Status:      g.status,
		RoundID:     golfprovider.MongoInt(g.round),
		RoundStatus: g.status,
		LastUpdated: golfprovider.MongoDate{Time: now().UTC()},
		Timestamp:   golfprovider.MongoDate{Time: now().UTC()},
		CutLines:    []golfprovider.CutLine{cut},
		Rows:        g.rows(),
	}
}

// lastMove is the row that changed on the last tick.
//
// Returns ok=false on a tick where nobody completed a round, so the feed
// advances instead of republishing: identical consecutive records on a Kafka
// topic are indistinguishable from a replay.
func (g *golfSim) lastMove() (any, string, bool) {
	if g.moved < 0 {
		return nil, "", false
	}
	idx := g.moved
	g.moved = -1
	for pos, i := range g.standings() {
		if i == idx {
			return g.row(idx, pos+1), "", true
		}
	}
	return nil, "", false
}

func (g *golfSim) fixtureID() string { return g.tournID }

// thru renders the provider's vocabulary for progress through a round.
//
// It is a string because the provider sends "F" for a finished round and "-"
// for one not yet started, neither of which has an integer spelling. Modelling
// it as a number is what broke the live feed; see golf.Row.Thru.
func thru(done bool, hole int) string {
	switch {
	case done:
		return "F"
	case hole <= 1:
		return "-"
	default:
		return fmt.Sprintf("%d", hole)
	}
}
