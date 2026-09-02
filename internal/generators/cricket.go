package generators

import (
	"fmt"
	"math/rand"

	"github.com/offloadintelligence/offload-ingest/internal/cricbuzz"
)

// Cricket comes from Cricbuzz, not SportsDataIO.
//
// SportsDataIO offers no cricket at all — every prefix returns 404 — so this is
// the pipeline's second provider, with a completely different shape: lowercase
// field names, display-formatted numbers arriving as strings, and a scorecard
// of innings rather than flat per-player arrays. The models in
// internal/cricbuzz are generated from captured responses.
func init() {
	mk := func(rnd *rand.Rand) *cricketSim { s := &cricketSim{base: newBase(rnd)}; s.reset(); return s }

	register(Endpoint{
		Sport: SportCricket, Kind: FeedBoxScore,
		Provider: ProviderCricbuzz, Provenance: ProvenanceCaptured,
		Path: "/mcenter/v1/{matchid}/hscard", Model: "Scorecard",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.scorecard(), "", true }
	})

	register(Endpoint{
		Sport: SportCricket, Kind: FeedPlayerStats,
		Provider: ProviderCricbuzz, Provenance: ProvenanceCaptured,
		Path: "/mcenter/v1/{matchid}/hscard", Projection: "scorecard[].batsman[]", Model: "Batsman[]",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.batsmen(), "", true }
	})

	register(Endpoint{
		Sport: SportCricket, Kind: FeedTelemetry,
		Provider: ProviderCricbuzz, Provenance: ProvenanceCaptured,
		Path: "/mcenter/v1/{matchid}/hscard", Projection: "scorecard[].fow.fow[]", Model: "FallOfWicket",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) {
			if !s.wicketFell {
				return nil, "", false
			}
			return s.lastWicket, "", true
		}
	})
}

const (
	cricketOvers   = 20 // T20 format
	cricketWickets = 10
	cricketSquad   = 11
)

// cricketSim simulates a T20 innings ball by ball and renders it in Cricbuzz's
// shape.
type cricketSim struct {
	base
	homeTeam, awayTeam   competitor
	venue                string
	homeSquad, awaySquad []player

	innings   int
	overCount int
	ball      int
	batting   int // 0 home, 1 away

	// Per-innings accumulators, indexed by batting side.
	runs               [2]int
	wickets            [2]int
	extras             [2]cricbuzz.Extras
	batting1, batting2 map[int]*cricbuzz.Batsman
	bowling1, bowling2 map[int]*cricbuzz.Bowler
	fow1, fow2         []cricbuzz.FallOfWicket

	striker, nonStriker, bowler int
	overRuns                    int
	overBowler                  int
	lastWicket                  cricbuzz.FallOfWicket
	// wicketFell says a wicket was taken on the tick just simulated. Without
	// it the telemetry feed republishes the previous dismissal on every ball,
	// and identical consecutive records on a Kafka topic are indistinguishable
	// from a replay — the same defect the soccer incident feed once had.
	wicketFell bool
	statusText string

	// Partnerships are their own collection on the wire: one record per pair of
	// batsmen at the crease, closed out by a wicket. They are accumulated here
	// rather than derived at render time because a partnership's totals depend
	// on when it started.
	part1, part2 []cricbuzz.Partnership
	current      *cricbuzz.Partnership
}

var cricketRoles = []string{"Batter", "Batter", "Batter", "AllRounder", "WicketKeeper", "Bowler", "Bowler"}

func (c *cricketSim) reset() {
	c.wicketFell = false
	c.homeTeam, c.awayTeam = pickPair(c.rnd, cricketTeams)
	c.venue = pick(c.rnd, cricketVenues)
	c.newFixture("CRK", 2, 0, 0)

	c.homeSquad = roster(c.homeTeam.ID, cricketSquad, cricketRoles, c.rnd)
	c.awaySquad = roster(c.awayTeam.ID, cricketSquad, cricketRoles, c.rnd)

	c.innings, c.overCount, c.ball = 1, 0, 0
	c.runs, c.wickets = [2]int{}, [2]int{}
	c.extras = [2]cricbuzz.Extras{}
	c.batting = c.rnd.Intn(2)
	c.striker, c.nonStriker, c.bowler = 0, 1, 0
	c.overRuns, c.overBowler = 0, 0
	c.batting1 = map[int]*cricbuzz.Batsman{}
	c.batting2 = map[int]*cricbuzz.Batsman{}
	c.bowling1 = map[int]*cricbuzz.Bowler{}
	c.bowling2 = map[int]*cricbuzz.Bowler{}
	c.fow1, c.fow2 = []cricbuzz.FallOfWicket{}, []cricbuzz.FallOfWicket{}
	c.part1, c.part2 = []cricbuzz.Partnership{}, []cricbuzz.Partnership{}
	c.current = nil
	c.statusText = "Match in progress"
	c.period = 1
}

// partnerships returns the accumulator for the innings in progress.
func (c *cricketSim) partnerships() *[]cricbuzz.Partnership {
	if c.innings == 1 {
		return &c.part1
	}
	return &c.part2
}

// openPartnership starts a stand between the two batsmen at the crease.
func (c *cricketSim) openPartnership(bat1, bat2 player, team competitor) {
	c.current = &cricbuzz.Partnership{
		ID:       c.id(),
		Bat1id:   bat1.ID,
		Bat1name: bat1.Name,
		Bat2id:   bat2.ID,
		Bat2name: bat2.Name,
		Teamid:   team.ID,
		Teamname: team.Name,
	}
}

// creditPartnership attributes a delivery to the stand in progress.
func (c *cricketSim) creditPartnership(strikerIsBat1 bool, runs int, legal bool) {
	if c.current == nil {
		return
	}
	p := c.current
	if legal {
		p.Totalballs++
		if strikerIsBat1 {
			p.Bat1balls++
		} else {
			p.Bat2balls++
		}
	}
	p.Totalruns += runs
	if strikerIsBat1 {
		p.Bat1runs += runs
		tallyStroke(&p.Bat1ones, &p.Bat1twos, &p.Bat1threes, &p.Bat1fours, &p.Bat1sixes,
			&p.Bat1boundaries, &p.Bat1sixers, runs)
	} else {
		p.Bat2runs += runs
		tallyStroke(&p.Bat2ones, &p.Bat2twos, &p.Bat2threes, &p.Bat2fours, &p.Bat2sixes,
			&p.Bat2boundaries, &p.Bat2sixers, runs)
	}
}

// tallyStroke buckets a scoring shot the way the wire model does: singles
// through sixes, plus the boundary counters that duplicate fours and sixes.
func tallyStroke(ones, twos, threes, fours, sixes, boundaries, sixers *int, runs int) {
	switch runs {
	case 1:
		*ones++
	case 2:
		*twos++
	case 3:
		*threes++
	case 4:
		*fours++
		*boundaries++
	case 6:
		*sixes++
		*sixers++
	}
}

// closePartnership files the stand and clears it, ready for the next pair.
func (c *cricketSim) closePartnership() {
	if c.current == nil {
		return
	}
	list := c.partnerships()
	*list = append(*list, *c.current)
	c.current = nil
}

// squads returns the batting and bowling sides for the innings in progress.
func (c *cricketSim) squads() (bat, bowl []player, batTeam, bowlTeam competitor) {
	if c.batting == 0 {
		return c.homeSquad, c.awaySquad, c.homeTeam, c.awayTeam
	}
	return c.awaySquad, c.homeSquad, c.awayTeam, c.homeTeam
}

// books returns the accumulators for the innings in progress.
func (c *cricketSim) books() (map[int]*cricbuzz.Batsman, map[int]*cricbuzz.Bowler, *[]cricbuzz.FallOfWicket) {
	if c.innings == 1 {
		return c.batting1, c.bowling1, &c.fow1
	}
	return c.batting2, c.bowling2, &c.fow2
}

func (c *cricketSim) advance() {
	if c.base.over {
		return
	}
	// Cleared each ball: only the tick that actually took a wicket publishes one.
	c.wicketFell = false
	batSquad, bowlSquad, batTeam, _ := c.squads()
	bats, bowls, fow := c.books()

	striker := batSquad[c.striker%len(batSquad)]
	nonStriker := batSquad[c.nonStriker%len(batSquad)]
	bowler := bowlSquad[6+c.bowler%5]

	if c.current == nil {
		c.openPartnership(striker, nonStriker, batTeam)
	}
	strikerIsBat1 := c.current.Bat1id == striker.ID

	runs, extra, wicket := 0, "", false
	switch r := c.rnd.Float64(); {
	case r < 0.05:
		wicket = true
	case r < 0.10:
		runs = 6
	case r < 0.22:
		runs = 4
	case r < 0.34:
		runs = 2
	case r < 0.62:
		runs = 1
	case r < 0.68:
		runs, extra = 1, pick(c.rnd, []string{"wides", "noballs", "legbyes", "byes"})
	}

	c.runs[c.batting] += runs
	c.overRuns += runs
	if extra != "" {
		c.addExtra(extra, runs)
	}
	c.creditPartnership(strikerIsBat1, runs, extra == "")

	bat := c.batsman(bats, striker)
	if extra == "" {
		bat.Balls++
		bat.Runs += runs
		if runs == 4 {
			bat.Fours++
		}
		if runs == 6 {
			bat.Sixes++
		}
		// Strike rate is a display string on this API, not a number.
		bat.Strkrate = fmt.Sprintf("%.2f", 100*float64(bat.Runs)/float64(maxInt(bat.Balls, 1)))
	}

	bwl := c.bowlerRec(bowls, bowler)
	bwl.Runs += runs
	if extra == "" {
		bwl.Balls++
		bwl.Overs = fmt.Sprintf("%d.%d", bwl.Balls/6, bwl.Balls%6)
		bwl.Economy = fmt.Sprintf("%.2f", float64(bwl.Runs)/maxFloat(float64(bwl.Balls)/6, 0.1))
		if runs == 0 {
			bwl.Dots++
		}
	}

	if wicket {
		c.wickets[c.batting]++
		bat.Outdec = pick(c.rnd, []string{"b " + bowler.Name, "c & b " + bowler.Name, "lbw b " + bowler.Name, "run out", "st b " + bowler.Name})
		if bat.Outdec != "run out" {
			bwl.Wickets++
		}
		w := cricbuzz.FallOfWicket{
			Batsmanid:   striker.ID,
			Batsmanname: striker.Name,
			Overnbr:     round1(float64(c.overCount) + float64(c.ball+1)/10),
			Runs:        c.runs[c.batting],
			Ballnbr:     c.overCount*6 + c.ball + 1,
		}
		*fow = append(*fow, w)
		c.lastWicket, c.wicketFell = w, true
		// A wicket ends the stand.
		c.closePartnership()
		c.striker = c.wickets[c.batting] + 1
	} else if runs%2 == 1 {
		c.striker, c.nonStriker = c.nonStriker, c.striker
	}

	c.advanceBall()
}

func (c *cricketSim) addExtra(kind string, runs int) {
	e := &c.extras[c.batting]
	switch kind {
	case "wides":
		e.Wides += runs
	case "noballs":
		e.Noballs += runs
	case "legbyes":
		e.Legbyes += runs
	case "byes":
		e.Byes += runs
	}
	e.Total += runs
}

func (c *cricketSim) batsman(m map[int]*cricbuzz.Batsman, p player) *cricbuzz.Batsman {
	if b, ok := m[p.ID]; ok {
		return b
	}
	b := &cricbuzz.Batsman{
		ID: p.ID, Name: p.Name, Nickname: shortName(p.Name),
		Iskeeper: p.Position == "WicketKeeper", Iscaptain: p.Number == 1,
		Strkrate: "0.00", Outdec: "batting",
	}
	m[p.ID] = b
	return b
}

func (c *cricketSim) bowlerRec(m map[int]*cricbuzz.Bowler, p player) *cricbuzz.Bowler {
	if b, ok := m[p.ID]; ok {
		return b
	}
	b := &cricbuzz.Bowler{
		ID: p.ID, Name: p.Name, Nickname: shortName(p.Name),
		Overs: "0.0", Economy: "0.00",
	}
	m[p.ID] = b
	return b
}

// advanceBall rolls the ball counter into overs, credits maidens, and rolls the
// innings over when the allocation or the wickets run out.
func (c *cricketSim) advanceBall() {
	c.ball++
	if c.ball >= 6 {
		if c.overRuns == 0 {
			_, bowlSquad, _, _ := c.squads()
			_, bowls, _ := c.books()
			c.bowlerRec(bowls, bowlSquad[6+c.overBowler%5]).Maidens++
		}
		c.ball, c.overRuns = 0, 0
		c.overCount++
		c.bowler++
		c.overBowler = c.bowler
		c.striker, c.nonStriker = c.nonStriker, c.striker
	}
	if c.overCount < cricketOvers && c.wickets[c.batting] < cricketWickets {
		return
	}
	if c.innings == 1 {
		// The innings closes the stand in progress before the sides swap.
		c.closePartnership()
		c.innings, c.period = 2, 2
		c.overCount, c.ball = 0, 0
		c.batting = 1 - c.batting
		c.striker, c.nonStriker, c.bowler = 0, 1, 0
		return
	}
	c.base.over = true
	c.statusText = c.result()
}

func (c *cricketSim) result() string {
	home, away := c.runs[0], c.runs[1]
	switch {
	case home > away:
		return fmt.Sprintf("%s won by %d runs", c.homeTeam.Name, home-away)
	case away > home:
		return fmt.Sprintf("%s won by %d wickets", c.awayTeam.Name, cricketWickets-c.wickets[1])
	default:
		return "Match tied"
	}
}

// --- renderers -------------------------------------------------------------

func (c *cricketSim) inningsRec(n int) cricbuzz.Innings {
	bats, bowls, fow, parts := c.batting1, c.bowling1, c.fow1, c.part1
	side := c.batting
	if n == 2 {
		bats, bowls, fow, parts = c.batting2, c.bowling2, c.fow2, c.part2
	}
	// The stand in progress belongs to the innings being played, and is shown
	// alongside the completed ones.
	if c.innings == n && c.current != nil {
		parts = append(append([]cricbuzz.Partnership{}, parts...), *c.current)
	}
	if c.innings != n {
		side = 1 - c.batting
	}
	batTeam := c.homeTeam
	if side == 1 {
		batTeam = c.awayTeam
	}

	balls := c.overCount*6 + c.ball
	inn := cricbuzz.Innings{
		Inningsid:    c.gameID*10 + n,
		Batsman:      derefBatsmen(bats),
		Bowler:       derefBowlers(bowls),
		Fow:          cricbuzz.Fow{Fow: append([]cricbuzz.FallOfWicket{}, fow...)},
		Extras:       c.extras[side],
		Score:        c.runs[side],
		Wickets:      c.wickets[side],
		Overs:        round1(float64(c.overCount) + float64(c.ball)/10),
		Batteamname:  batTeam.Name,
		Batteamsname: batTeam.Key,
		Ballnbr:      balls,
		Partnership:  cricbuzz.PartnershipList{Partnership: append([]cricbuzz.Partnership{}, parts...)},
	}
	if balls > 0 {
		inn.Runrate = round2(float64(c.runs[side]) / (float64(balls) / 6))
		inn.Rpb = round2(float64(c.runs[side]) / float64(balls))
	}
	return inn
}

func (c *cricketSim) scorecard() cricbuzz.Scorecard {
	innings := []cricbuzz.Innings{c.inningsRec(1)}
	if c.innings == 2 {
		innings = append(innings, c.inningsRec(2))
	}
	return cricbuzz.Scorecard{
		Scorecard:           innings,
		Ismatchcomplete:     c.base.over,
		Status:              c.statusText,
		Responselastupdated: int(now().Unix()),
		Appindex: &cricbuzz.AppIndex{
			Seotitle: fmt.Sprintf("Cricket scorecard - %s vs %s | Cricbuzz.com", c.homeTeam.Key, c.awayTeam.Key),
			Weburl:   fmt.Sprintf("http://www.cricbuzz.com/live-cricket-scorecard/%d", c.gameID),
		},
	}
}

// batsmen renders the current innings' batting card, the projection the player
// stats feed carries.
func (c *cricketSim) batsmen() []cricbuzz.Batsman {
	bats := c.batting1
	if c.innings == 2 {
		bats = c.batting2
	}
	return derefBatsmen(bats)
}

func derefBatsmen(m map[int]*cricbuzz.Batsman) []cricbuzz.Batsman {
	out := make([]cricbuzz.Batsman, 0, len(m))
	for _, b := range sortedByKey(m) {
		out = append(out, *b)
	}
	return out
}

func derefBowlers(m map[int]*cricbuzz.Bowler) []cricbuzz.Bowler {
	out := make([]cricbuzz.Bowler, 0, len(m))
	for _, b := range sortedByKey(m) {
		out = append(out, *b)
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
