package generators

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/allscores"
)

// Tennis comes from AllScores, not SportsDataIO.
//
// SportsDataIO sells a tennis feed but this key is not scoped to it — every
// tennis route returns "401 Unauthorized Endpoint" — so the models here were
// previously invented. AllScores serves real tennis data and these are
// generated from captures of it.
//
// One thing it does NOT serve, despite advertising it: point-by-point. The
// match document sets hasPointByPoint: true, but no route returns that data.
// The telemetry feed therefore carries set-score updates — a real, changing
// projection of the match document — rather than pretending to per-point data.
func init() {
	mk := func(rnd *rand.Rand) *tennisSim { s := &tennisSim{base: newBase(rnd)}; s.reset(); return s }

	register(Endpoint{
		Sport: SportTennis, Kind: FeedBoxScore,
		Provider: ProviderAllScores, Provenance: ProvenanceCaptured,
		Path: "/api/allscores/game-details", Model: "GameDetails",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.gameDetails(), "", true }
	})

	register(Endpoint{
		Sport: SportTennis, Kind: FeedPlayerStats,
		Provider: ProviderAllScores, Provenance: ProvenanceCaptured,
		Path: "/api/allscores/game-details", Projection: "game.homeCompetitor | game.awayCompetitor",
		Model: "Competitor[]",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.competitors(), "", true }
	})

	register(Endpoint{
		Sport: SportTennis, Kind: FeedTelemetry,
		Provider: ProviderAllScores, Provenance: ProvenanceCaptured,
		Path: "/api/allscores/game-details", Projection: "game.stages[]", Model: "Stage",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.currentStage(), "", true }
	})
}

// tennisSim simulates a match point by point internally and renders it in the
// AllScores shape: sets as stages, players as competitors.
type tennisSim struct {
	base
	gameID        int
	competitionID int
	competition   string
	stageName     string
	venue         allscores.Venue

	a, b     competitor
	aRank    int
	bRank    int
	bestOf   int
	setsWon  [2]int
	games    [2]int
	points   [2]int
	server   int
	tiebreak bool

	// sets holds the completed and in-progress sets, in AllScores' stage shape.
	sets     []allscores.Stage
	setStart time.Time
	winner   int
}

var tennisRounds = []string{"Round of 128", "Round of 64", "Round of 32", "Round of 16", "Quarterfinal", "Semifinal", "Final"}
var tennisCompetitions = []string{"US Open - Men", "US Open - Women", "ATP French Open", "Wimbledon", "Australian Open"}
var tennisCourts = []string{"Arthur Ashe Stadium", "Centre Court", "Philippe-Chatrier", "Rod Laver Arena"}

func (t *tennisSim) reset() {
	t.a, t.b = pickPair(t.rnd, tennisPlayers)
	t.newFixture("TEN", 5, 0, 0)
	t.gameID = 4800000 + t.rnd.Intn(99999)
	t.competitionID = 8700 + t.rnd.Intn(300)
	t.competition = pick(t.rnd, tennisCompetitions)
	t.stageName = pick(t.rnd, tennisRounds)
	t.venue = allscores.Venue{
		ID:        1000 + t.rnd.Intn(500),
		Name:      pick(t.rnd, tennisCourts),
		ShortName: "",
	}
	t.aRank = 1 + t.rnd.Intn(400)
	t.bRank = 1 + t.rnd.Intn(400)

	t.bestOf = pick(t.rnd, []int{3, 3, 5})
	t.periods = t.bestOf
	t.setsWon, t.games, t.points = [2]int{}, [2]int{}, [2]int{}
	t.server = t.rnd.Intn(2)
	t.tiebreak = false
	t.winner = -1
	t.period = 1
	t.setStart = now()
	t.sets = []allscores.Stage{t.newStage(1)}
}

// newStage opens a set. AllScores numbers set stages from 27 and uses a
// separate stage id 35 for the match-level set tally.
func (t *tennisSim) newStage(n int) allscores.Stage {
	return allscores.Stage{
		ID:        26 + n,
		Name:      fmt.Sprintf("Set %d", n),
		ShortName: fmt.Sprintf("S%d", n),
		// A set that has not been played reports -1, not 0.
		HomeCompetitorScore: -1,
		AwayCompetitorScore: -1,
	}
}

func (t *tennisSim) advance() {
	if t.over {
		return
	}

	// The server wins the point more often than not; that bias is what makes
	// breaks rare enough to be interesting downstream.
	winner := 1 - t.server
	if t.chance(0.63) {
		winner = t.server
	}
	switch r := t.rnd.Float64(); {
	case r < 0.08:
		winner = t.server // ace
	case r < 0.13:
		winner = 1 - t.server // double fault
	}
	t.award(winner)
	t.syncStage()
}

func (t *tennisSim) award(w int) {
	l := 1 - w
	t.points[w]++

	if t.tiebreak {
		if t.points[w] >= 7 && t.points[w]-t.points[l] >= 2 {
			t.winGame(w)
		}
		return
	}
	if t.points[w] >= 4 && t.points[w]-t.points[l] >= 2 {
		t.winGame(w)
		return
	}
	// Deuce: both back to 40 whenever the trailing side draws level past 40.
	if t.points[w] > 4 && t.points[l] > 3 {
		t.points[w], t.points[l] = 3, 3
	}
}

func (t *tennisSim) winGame(w int) {
	t.games[w]++
	t.points = [2]int{}
	t.server = 1 - t.server
	wasTiebreak := t.tiebreak
	t.tiebreak = false

	l := 1 - w
	if (t.games[w] >= 6 && t.games[w]-t.games[l] >= 2) || t.games[w] == 7 {
		t.closeSet(w, wasTiebreak)
		return
	}
	if t.games[0] == 6 && t.games[1] == 6 {
		t.tiebreak = true
	}
}

// closeSet finalises the stage for the set just won and opens the next one.
func (t *tennisSim) closeSet(w int, wasTiebreak bool) {
	cur := &t.sets[len(t.sets)-1]
	cur.HomeCompetitorScore = t.games[0]
	cur.AwayCompetitorScore = t.games[1]
	cur.IsEnded = true
	cur.Time = fmt.Sprintf("%d'", 30+t.rnd.Intn(45))
	if wasTiebreak {
		// The tiebreak score rides on the extra-score columns.
		cur.HomeCompetitorExtraScore = 7
		cur.AwayCompetitorExtraScore = 5
		if w == 1 {
			cur.HomeCompetitorExtraScore, cur.AwayCompetitorExtraScore = 5, 7
		}
	}

	t.setsWon[w]++
	t.games = [2]int{}
	if t.setsWon[w] > t.bestOf/2 {
		t.winner = w
		t.over = true
		return
	}
	t.period++
	t.sets = append(t.sets, t.newStage(t.period))
}

// syncStage keeps the in-progress set's games in step with the simulation.
func (t *tennisSim) syncStage() {
	cur := &t.sets[len(t.sets)-1]
	if cur.IsEnded {
		return
	}
	cur.HomeCompetitorScore = t.games[0]
	cur.AwayCompetitorScore = t.games[1]
	if t.tiebreak {
		cur.HomeCompetitorExtraScore = t.points[0]
		cur.AwayCompetitorExtraScore = t.points[1]
	}
}

// --- renderers -------------------------------------------------------------

// stages returns the set stages plus the match-level "Sets" tally that
// AllScores appends as stage 35.
func (t *tennisSim) stages() []allscores.Stage {
	out := append([]allscores.Stage{}, t.sets...)
	total := allscores.Stage{
		ID:                  35,
		Name:                "Sets",
		ShortName:           "Sets",
		HomeCompetitorScore: t.setsWon[0],
		AwayCompetitorScore: t.setsWon[1],
		Time:                t.elapsedDisplay(),
		IsCurrent:           true,
		IsEnded:             t.over,
	}
	return append(out, total)
}

func (t *tennisSim) elapsedDisplay() string {
	mins := 20*len(t.sets) + t.games[0] + t.games[1]
	return fmt.Sprintf("%02d:%02d", mins/60, mins%60)
}

// currentStage is the set in progress, the record the telemetry feed carries.
func (t *tennisSim) currentStage() allscores.Stage {
	return t.sets[len(t.sets)-1]
}

func (t *tennisSim) competitor(side int) allscores.Competitor {
	p, rank := t.a, t.aRank
	if side == 1 {
		p, rank = t.b, t.bRank
	}
	tour := "ATP"
	if t.competition == "US Open - Women" {
		tour = "WTA"
	}
	recent := make([]int, 0, 8)
	for k := 0; k < 8; k++ {
		recent = append(recent, t.gameID-(k+1)*137)
	}
	return allscores.Competitor{
		ID:                p.ID,
		CountryID:         5,
		SportID:           3,
		Name:              p.Name,
		LongName:          p.Name,
		Score:             t.setsWon[side],
		IsWinner:          t.over && t.winner == side,
		Type:              4,
		RecentMatches:     recent,
		NameForURL:        urlName(p.Name),
		Rankings:          []allscores.Ranking{{Name: tour, Position: rank}},
		ImageVersion:      1,
		Color:             "#7f97ab",
		MainCompetitionID: t.competitionID,
		CreatedAt:         sdioDateTime(t.start.AddDate(-3, 0, 0)),
	}
}

func (t *tennisSim) competitors() []allscores.Competitor {
	return []allscores.Competitor{t.competitor(0), t.competitor(1)}
}

func (t *tennisSim) gameModel() allscores.Game {
	status, short, group := "Scheduled", "Sched", 1
	if t.over {
		status, short, group = "Ended", "Ended", 3
	} else if len(t.sets) > 0 {
		status, short, group = "Set "+fmt.Sprint(t.period), "S"+fmt.Sprint(t.period), 2
	}
	win := ""
	if t.over {
		win = t.a.Name
		if t.winner == 1 {
			win = t.b.Name
		}
	}
	return allscores.Game{
		ID:                           t.gameID,
		SportID:                      3,
		CompetitionID:                t.competitionID,
		StatusID:                     statusID(t.over),
		SeasonNum:                    1,
		StageNum:                     t.period,
		StageName:                    t.stageName,
		CompetitionDisplayName:       t.competition,
		StartTime:                    sdioDateTime(t.start),
		StatusGroup:                  group,
		StatusText:                   status,
		ShortStatusText:              short,
		GameTimeAndStatusDisplayType: 1,
		JustEnded:                    false,
		GameTime:                     -1,
		GameTimeDisplay:              "",
		WinDescription:               win,
		HomeCompetitor:               t.competitor(0),
		AwayCompetitor:               t.competitor(1),
		Stages:                       t.stages(),
		Venue:                        t.venue,
		LineTypesIds:                 []int{1, 2},

		HasBets:          true,
		HasBetsTeaser:    true,
		HasBrackets:      true,
		HasRecentMatches: true,
		HasStandings:     true,
		HasStats:         true,
		HasTrends:        true,
		// Reproduced from the wire even though no route serves it.
		HasPointByPoint: true,
	}
}

func (t *tennisSim) gameDetails() allscores.GameDetails {
	return allscores.GameDetails{
		LastUpdateID:      int(now().Unix()),
		RequestedUpdateID: -1,
		TTL:               15,
		Game:              t.gameModel(),
	}
}

func (t *tennisSim) fixtureID() string { return fmt.Sprintf("%d", t.gameID) }

func statusID(over bool) int {
	if over {
		return 3
	}
	return 2
}

// urlName renders a player's name the way AllScores slugs it.
func urlName(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'A' && r <= 'Z':
			out = append(out, r+32)
		case r == ' ':
			out = append(out, '-')
		case r == '\'':
		default:
			out = append(out, r)
		}
	}
	return string(out)
}
