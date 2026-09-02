package generators

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/sdio"
)

func init() {
	mk := func(rnd *rand.Rand) *golfSim { s := &golfSim{base: newBase(rnd)}; s.reset(); return s }

	register(Endpoint{
		Sport: SportGolf, Kind: FeedBoxScore, Provenance: ProvenanceCaptured,
		Path: "/golf/v2/json/Leaderboard/{tournamentid}", Model: "Leaderboard",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.leaderboard(), "", true }
	})

	// The hole-by-hole record is the high-frequency stream underneath the
	// leaderboard: one small object per player per hole, versus a whole-field
	// snapshot that runs to tens of kilobytes.
	register(Endpoint{
		Sport: SportGolf, Kind: FeedTelemetry, Provenance: ProvenanceCaptured,
		Path: "/golf/v2/json/Leaderboard/{tournamentid}", Projection: "Players[].Rounds[].Holes[]", Model: "PlayerHole",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.lastHole(), "", true }
	})

	register(Endpoint{
		Sport: SportGolf, Kind: FeedPlayerStats, Provenance: ProvenanceCaptured,
		Path: "/golf/v2/json/Player/{playerid}", Model: "PlayerTournament[]",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.players(), "", true }
	})
}

const (
	golfFieldSize = 8
	golfRounds    = 4
	golfHoles     = 18
)

// golfSim simulates a four-round stroke-play tournament, one hole at a time.
type golfSim struct {
	base
	tournamentID int
	venue        string
	pars         [golfHoles]int
	field        []competitor

	// per-player progress
	round   []int
	hole    []int
	toPar   []int
	strokes []int
	holes   [][]sdio.GolfPlayerHole
	// completed holds each player's finished rounds, so the leaderboard reports
	// a full tournament history rather than only the round in play.
	completed [][]sdio.GolfPlayerRound
	// oddsToWin, salaries and the status flags are set before the first tee
	// shot and do not move during play.
	oddsToWin []float64
	salaries  []dfsSalaries
	withdrawn []bool
	alternate []bool

	cursor      int
	lastHoleRec sdio.GolfPlayerHole
}

func (g *golfSim) reset() {
	g.field = pickN(g.rnd, golfPlayers, golfFieldSize)
	g.venue = pick(g.rnd, golfVenues)
	g.newFixture("PGA", golfRounds, 0, 0)
	g.tournamentID = g.gameID

	for k := range g.pars {
		g.pars[k] = pick(g.rnd, []int{3, 4, 4, 4, 5})
	}
	g.round = make([]int, golfFieldSize)
	g.hole = make([]int, golfFieldSize)
	g.toPar = make([]int, golfFieldSize)
	g.strokes = make([]int, golfFieldSize)
	g.holes = make([][]sdio.GolfPlayerHole, golfFieldSize)
	g.completed = make([][]sdio.GolfPlayerRound, golfFieldSize)
	g.oddsToWin = make([]float64, golfFieldSize)
	g.salaries = make([]dfsSalaries, golfFieldSize)
	g.withdrawn = make([]bool, golfFieldSize)
	g.alternate = make([]bool, golfFieldSize)
	for k := range g.round {
		g.round[k] = 1
		g.holes[k] = []sdio.GolfPlayerHole{}
		g.completed[k] = []sdio.GolfPlayerRound{}
		// Market prices span the field, shortest for the favourite.
		g.oddsToWin[k] = round1(float64(600 + k*450 + g.rnd.Intn(400)))
		g.salaries[k] = dfsSalaries{
			draftKings:   11000 - k*700 + g.rnd.Intn(500),
			fanDuel:      12000 - k*750 + g.rnd.Intn(500),
			fantasyDraft: 10500 - k*650 + g.rnd.Intn(500),
			yahoo:        40 - k*2,
		}
		// A small share of the field withdraws or is an alternate, as on tour.
		g.alternate[k] = g.chance(0.06)
		g.withdrawn[k] = g.chance(0.03)
	}
	g.cursor = 0
	g.period = 1
}

// golfOutcomes covers the full scoring range the wire model reports, including
// the rare results that give the DoubleEagle, HoleInOne, TripleBogey and
// WorseThanTripleBogey columns something to carry. Their probabilities are low
// enough to stay plausible over a four-round tournament.
var golfOutcomes = []struct {
	name  string
	delta int
	prob  float64
}{
	{"albatross", -3, 0.002},
	{"eagle", -2, 0.020},
	{"birdie", -1, 0.200},
	{"par", 0, 0.560},
	{"bogey", 1, 0.160},
	{"double", 2, 0.040},
	{"triple", 3, 0.014},
	{"worse", 4, 0.004},
}

// advance plays one hole for the next player in the rotation.
func (g *golfSim) advance() {
	if g.over {
		return
	}
	k := g.cursor
	g.cursor = (g.cursor + 1) % golfFieldSize

	holeIdx := g.hole[k]
	par := g.pars[holeIdx]
	outcome := g.rollOutcome()
	strokes := par + outcome.delta

	g.toPar[k] += outcome.delta
	g.strokes[k] += strokes

	rec := sdio.GolfPlayerHole{
		PlayerRoundID:        g.playerRoundID(k),
		Number:               holeIdx + 1,
		Par:                  par,
		Score:                i(strokes),
		ToPar:                i(outcome.delta),
		HoleInOne:            strokes == 1,
		DoubleEagle:          outcome.delta == -3,
		Eagle:                outcome.delta == -2,
		Birdie:               outcome.delta == -1,
		IsPar:                outcome.delta == 0,
		Bogey:                outcome.delta == 1,
		DoubleBogey:          outcome.delta == 2,
		WorseThanDoubleBogey: outcome.delta > 2,
	}
	g.holes[k] = append(g.holes[k], rec)
	g.lastHoleRec = rec

	g.hole[k]++
	if g.hole[k] >= golfHoles {
		// Archive the finished round before starting the next one, so the
		// leaderboard can report every round the player has completed.
		g.completed[k] = append(g.completed[k], g.buildRound(k))
		g.hole[k], g.strokes[k] = 0, 0
		g.holes[k] = []sdio.GolfPlayerHole{}
		g.round[k]++
		if g.round[k] > golfRounds {
			g.over = allRoundsDone(g.round)
		}
	}
	g.period = minRound(g.round)
}

func allRoundsDone(rounds []int) bool {
	for _, r := range rounds {
		if r <= golfRounds {
			return false
		}
	}
	return true
}

func minRound(rounds []int) int {
	m := golfRounds
	for _, r := range rounds {
		if r < m {
			m = r
		}
	}
	return clamp(m, 1, golfRounds)
}

func (g *golfSim) rollOutcome() struct {
	name  string
	delta int
	prob  float64
} {
	r, acc := g.rnd.Float64(), 0.0
	for _, o := range golfOutcomes {
		acc += o.prob
		if r < acc {
			return o
		}
	}
	return golfOutcomes[2]
}

func (g *golfSim) playerRoundID(k int) int {
	return g.tournamentID*1000 + g.field[k].ID*10 + clamp(g.round[k], 1, golfRounds)
}

func (g *golfSim) playerTournamentID(k int) int {
	return g.tournamentID*100 + g.field[k].ID
}

// --- renderers -------------------------------------------------------------

func (g *golfSim) lastHole() sdio.GolfPlayerHole { return g.lastHoleRec }

func (g *golfSim) tournament() sdio.GolfTournament {
	rounds := make([]sdio.GolfRound, 0, golfRounds)
	for r := 1; r <= golfRounds; r++ {
		rounds = append(rounds, sdio.GolfRound{
			TournamentID: g.tournamentID,
			RoundID:      g.tournamentID*10 + r,
			Number:       r,
			Day:          s(sdioDay(g.start)),
			IsRoundOver:  minRound(g.round) > r,
		})
	}
	totalPar := 0
	for _, p := range g.pars {
		totalPar += p
	}
	return sdio.GolfTournament{
		TournamentID:           g.tournamentID,
		Name:                   s(fmt.Sprintf("%s Invitational", g.venue)),
		StartDate:              s(sdioDay(g.start)),
		EndDate:                s(sdioDay(g.start.AddDate(0, 0, 3))),
		IsOver:                 g.over,
		IsInProgress:           !g.over,
		Venue:                  s(g.venue),
		Location:               s(fmt.Sprintf("%s, %s", golfCity(g.venue), golfState(g.venue))),
		Par:                    i(totalPar),
		Yards:                  i(6800 + g.rnd.Intn(600)),
		Purse:                  f(float64(8000000 + g.rnd.Intn(12000000))),
		StartDateTime:          s(sdioDateTime(g.start)),
		City:                   s(golfCity(g.venue)),
		State:                  s(golfState(g.venue)),
		ZipCode:                s(fmt.Sprintf("%05d", 10000+g.rnd.Intn(80000))),
		Country:                s("USA"),
		TimeZone:               s("EST"),
		Covered:                true,
		SportRadarTournamentID: s(fmt.Sprintf("sr:tournament:%d", 40000+g.tournamentID%9999)),
		Format:                 s("StrokePlay"),
		Rounds:                 rounds,
		OddsCoverage:           s("Full"),
	}
}

// players renders the field, ranked by score to par with ties sharing a rank.
func (g *golfSim) players() []sdio.GolfPlayerTournament {
	out := make([]sdio.GolfPlayerTournament, 0, golfFieldSize)
	for k, p := range g.field {
		rounds := g.playerRounds(k)
		pt := sdio.GolfPlayerTournament{
			PlayerTournamentID: g.playerTournamentID(k),
			PlayerID:           p.ID,
			TournamentID:       g.tournamentID,
			Name:               s(p.Name),
			Country:            s(p.Country),
			TotalScore:         f(float64(g.toPar[k])),
			TotalStrokes:       f(float64(g.totalStrokes(k))),
			TotalThrough:       i(g.hole[k]),
			TeeTime:            s(sdioDateTime(g.start.Add(time.Duration(k*11) * time.Minute))),
			TournamentStatus:   s(g.playerStatus(k)),
			Rounds:             rounds,

			DraftKingsSalary:   i(g.salaries[k].draftKings),
			FanDuelSalary:      i(g.salaries[k].fanDuel),
			FantasyDraftSalary: i(g.salaries[k].fantasyDraft),

			OddsToWin:            f(g.oddsToWin[k]),
			OddsToWinDescription: s(fmt.Sprintf("+%d", int(g.oddsToWin[k]))),

			IsAlternate:         g.alternate[k],
			IsWithdrawn:         g.withdrawn[k],
			MadeCutDidNotFinish: g.withdrawn[k] && g.round[k] > 2,
		}
		g.foldTournamentCounts(k, rounds, &pt)

		// The cut falls after round two; before that the column reports 0, as
		// it does on a live leaderboard.
		if g.round[k] > 2 {
			pt.MadeCut = boolFloat(!g.withdrawn[k])
		}
		if g.over {
			pt.Win = boolFloat(g.toPar[k] == g.bestScore())
		}

		pt.FantasyPoints = round2(-float64(g.toPar[k])*3 + pt.Birdies*2 + pt.Eagles*5 +
			pt.DoubleEagles*8 + pt.HoleInOnes*10 - pt.Bogeys - pt.DoubleBogeys*2)
		pt.FantasyPointsDraftKings = pt.FantasyPoints
		pt.FantasyPointsFanDuel = pt.FantasyPoints
		pt.FantasyPointsYahoo = pt.FantasyPoints
		pt.FantasyPointsFantasyDraft = pt.FantasyPoints
		out = append(out, pt)
	}
	sortBy(out, func(a, b sdio.GolfPlayerTournament) bool {
		if *a.TotalScore != *b.TotalScore {
			return *a.TotalScore < *b.TotalScore
		}
		return a.PlayerID < b.PlayerID
	})
	rank := 0
	var prev float64
	for k := range out {
		if k == 0 || *out[k].TotalScore != prev {
			rank = k + 1
			prev = *out[k].TotalScore
		}
		out[k].Rank = i(rank)
		out[k].Earnings = f(round2(float64(2000000) / float64(rank)))
		out[k].FedExPoints = i(clamp(600/rank, 5, 600))
	}
	return out
}

// totalStrokes is the whole tournament, not just the round in progress.
func (g *golfSim) totalStrokes(k int) int {
	total := g.strokes[k]
	for _, r := range g.completed[k] {
		if r.Score != nil {
			total += *r.Score
		}
	}
	return total
}

func (g *golfSim) bestScore() int {
	best := g.toPar[0]
	for _, v := range g.toPar[1:] {
		if v < best {
			best = v
		}
	}
	return best
}

func (g *golfSim) playerStatus(k int) string {
	switch {
	case g.withdrawn[k]:
		return "Withdrawn"
	case g.round[k] > golfRounds:
		return "Complete"
	default:
		return "InProgress"
	}
}

// playerRounds returns every completed round plus the one in progress.
func (g *golfSim) playerRounds(k int) []sdio.GolfPlayerRound {
	out := append([]sdio.GolfPlayerRound{}, g.completed[k]...)
	if len(g.holes[k]) > 0 {
		out = append(out, g.buildRound(k))
	}
	return out
}

// buildRound summarises one round from its holes, including the streak columns
// SportsDataIO derives rather than reports directly.
func (g *golfSim) buildRound(k int) sdio.GolfPlayerRound {
	holes := g.holes[k]
	pr := sdio.GolfPlayerRound{
		PlayerRoundID:      g.playerRoundID(k),
		PlayerTournamentID: g.playerTournamentID(k),
		Number:             clamp(g.round[k], 1, golfRounds),
		Day:                s(sdioDay(g.start.AddDate(0, 0, g.round[k]-1))),
		Score:              i(g.strokes[k]),
		Holes:              append([]sdio.GolfPlayerHole{}, holes...),
		TeeTime:            s(sdioDateTime(g.start.Add(time.Duration(k*11) * time.Minute))),
		// Roughly half the field starts on the tenth tee in the early rounds.
		BackNineStart: k%2 == 1 && g.round[k] <= 2,
	}

	par := 0
	streak, longest, bounceBack := 0, 0, 0
	prevOverPar := false
	for _, h := range holes {
		par += h.Par
		switch {
		case h.DoubleEagle:
			pr.DoubleEagles++
		case h.Eagle:
			pr.Eagles++
		case h.Birdie:
			pr.Birdies++
		case h.IsPar:
			pr.Pars++
		case h.Bogey:
			pr.Bogeys++
		case h.DoubleBogey:
			pr.DoubleBogeys++
		}
		if h.HoleInOne {
			pr.HoleInOnes++
		}
		if h.ToPar != nil {
			switch {
			case *h.ToPar == 3:
				pr.TripleBogeys++
			case *h.ToPar > 3:
				pr.WorseThanTripleBogey++
			}
			if *h.ToPar > 2 {
				pr.WorseThanDoubleBogey++
			}
		}

		// A birdie-or-better streak, and a bounce-back: a birdie immediately
		// following a hole played over par.
		betterThanPar := h.Birdie || h.Eagle || h.DoubleEagle
		if betterThanPar {
			streak++
			if streak > longest {
				longest = streak
			}
			if prevOverPar {
				bounceBack++
			}
		} else {
			streak = 0
		}
		prevOverPar = h.Bogey || h.DoubleBogey || h.WorseThanDoubleBogey
	}

	pr.Par = i(par)
	pr.BogeyFree = pr.Bogeys == 0 && pr.DoubleBogeys == 0 && pr.WorseThanDoubleBogey == 0
	pr.LongestBirdieOrBetterStreak = float64(longest)
	pr.ConsecutiveBirdieOrBetterCount = float64(streak)
	pr.BounceBackCount = float64(bounceBack)
	pr.IncludesStreakOfThreeBirdiesOrBetter = longest >= 3
	pr.IncludesStreakOfFourBirdiesOrBetter = longest >= 4
	pr.IncludesStreakOfFiveBirdiesOrBetter = longest >= 5
	pr.IncludesStreakOfSixBirdiesOrBetter = longest >= 6
	pr.IncludesFiveOrMoreBirdiesOrBetter = pr.Birdies+pr.Eagles+pr.DoubleEagles >= 5
	return pr
}

// foldTournamentCounts aggregates the round summaries into the tournament-level
// counters, which SportsDataIO publishes as decimals.
func (g *golfSim) foldTournamentCounts(k int, rounds []sdio.GolfPlayerRound, pt *sdio.GolfPlayerTournament) {
	for _, r := range rounds {
		pt.DoubleEagles += float64(r.DoubleEagles)
		pt.Eagles += float64(r.Eagles)
		pt.Birdies += float64(r.Birdies)
		pt.Pars += float64(r.Pars)
		pt.Bogeys += float64(r.Bogeys)
		pt.DoubleBogeys += float64(r.DoubleBogeys)
		pt.WorseThanDoubleBogey += float64(r.WorseThanDoubleBogey)
		pt.HoleInOnes += float64(r.HoleInOnes)
		pt.TripleBogeys += float64(r.TripleBogeys)
		pt.WorseThanTripleBogey += float64(r.WorseThanTripleBogey)

		pt.BounceBackCount += r.BounceBackCount
		pt.ConsecutiveBirdieOrBetterCount = r.ConsecutiveBirdieOrBetterCount
		if r.IncludesStreakOfThreeBirdiesOrBetter {
			pt.StreaksOfThreeBirdiesOrBetter++
		}
		if r.IncludesStreakOfFourBirdiesOrBetter {
			pt.StreaksOfFourBirdiesOrBetter++
		}
		if r.IncludesStreakOfFiveBirdiesOrBetter {
			pt.StreaksOfFiveBirdiesOrBetter++
		}
		if r.IncludesStreakOfSixBirdiesOrBetter {
			pt.StreaksOfSixBirdiesOrBetter++
		}
		if r.IncludesFiveOrMoreBirdiesOrBetter {
			pt.RoundsWithFiveOrMoreBirdiesOrBetter++
		}
		if r.BogeyFree {
			pt.BogeyFreeRounds++
		}
		if r.Score != nil && *r.Score < 70 && *r.Score > 0 {
			pt.RoundsUnderSeventy++
		}
	}
}

func (g *golfSim) leaderboard() sdio.GolfLeaderboard {
	return sdio.GolfLeaderboard{Tournament: g.tournament(), Players: g.players()}
}

func (g *golfSim) fixtureID() string { return fmt.Sprintf("%d", g.tournamentID) }

// golfCity and golfState fill the venue's location columns. The real feed
// carries a genuine address; these are consistent per venue so a consumer
// grouping by location gets stable keys.
func golfCity(venue string) string {
	switch venue {
	case "Augusta National":
		return "Augusta"
	case "St Andrews":
		return "St Andrews"
	case "Pebble Beach":
		return "Pebble Beach"
	default:
		return "Ponte Vedra Beach"
	}
}

func golfState(venue string) string {
	switch venue {
	case "Augusta National":
		return "GA"
	case "St Andrews":
		return "Fife"
	case "Pebble Beach":
		return "CA"
	default:
		return "FL"
	}
}
