package generators

import (
	"fmt"
	"math/rand"

	"github.com/offloadintelligence/offload-ingest/internal/sdio"
)

func init() {
	mk := func(rnd *rand.Rand) *nascarSim { s := &nascarSim{base: newBase(rnd)}; s.reset(); return s }

	register(Endpoint{
		Sport: SportNASCAR, Kind: FeedBoxScore, Provenance: ProvenanceCaptured,
		Path: "/nascar/v2/json/RaceResult/{raceid}", Model: "RaceResult",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.raceResult(), "", true }
	})

	// The per-driver row is the highest-frequency record in the pipeline:
	// forty of them updating every lap for three hours.
	register(Endpoint{
		Sport: SportNASCAR, Kind: FeedTelemetry, Provenance: ProvenanceCaptured,
		Path: "/nascar/v2/json/RaceResult/{raceid}", Projection: "DriverRaces[]", Model: "DriverRace",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.lastDriverRace(), "", true }
	})

	// The season schedule and the driver directory are reference documents:
	// large, slow-moving, and not tied to one race. They are what makes the
	// NASCARRace and NASCARDriver models reachable rather than dead code.
	register(Endpoint{
		Sport: SportNASCAR, Kind: FeedReference, Name: "schedule", Provenance: ProvenanceCaptured,
		Path: "/nascar/v2/json/races/{season}", Model: "Race[]",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.schedule(), "", true }
	})

	// The driver directory is the second reference document. Both are the same
	// kind, distinguished by name — which is why the registry allows more than
	// one endpoint per (sport, kind).
	register(Endpoint{
		Sport: SportNASCAR, Kind: FeedReference, Name: "drivers", Provenance: ProvenanceCaptured,
		Path: "/nascar/v2/json/drivers", Model: "Driver[]",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.drivers(), "", true }
	})

	register(Endpoint{
		Sport: SportNASCAR, Kind: FeedPlayerStats, Provenance: ProvenanceCaptured,
		Path: "/nascar/v2/json/RaceResult/{raceid}", Projection: "DriverRaces[]", Model: "DriverRace[]",
	}, func(rnd *rand.Rand) (sim, renderer) {
		s := mk(rnd)
		return s, func() (any, string, bool) { return s.driverRaces(), "", true }
	})
}

const nascarFieldSize = 12

// nascarSim simulates a Cup Series race, advancing one driver's lap per tick.
type nascarSim struct {
	base
	raceID     int
	seriesID   int
	seriesName string
	track      string
	name       string
	laps       int
	// round is this race\'s position in the season calendar, used by the
	// schedule feed to mark earlier races complete and later ones scheduled.
	round int

	grid       []competitor
	numbers    []string
	makes      []string
	salaries   []int
	qualifying []float64

	lapsDone []int
	lapsLed  []int
	fastest  []int
	startPos []int
	pits     []int
	elapsed  []float64

	cursor  int
	lastRow sdio.NASCARDriverRace
}

func (r *nascarSim) reset() {
	r.grid = pickN(r.rnd, nascarDrivers, nascarFieldSize)
	r.track = pick(r.rnd, nascarTracks)
	r.newFixture("NAS", 1, 0, 0)
	r.raceID = r.gameID
	r.seriesID = 1
	r.seriesName = "NASCAR Cup"
	r.name = fmt.Sprintf("%s 400", r.track)
	r.laps = 200 + r.rnd.Intn(200)
	r.round = r.rnd.Intn(30)

	n := nascarFieldSize
	r.numbers = make([]string, n)
	r.makes = make([]string, n)
	r.salaries = make([]int, n)
	r.qualifying = make([]float64, n)
	r.lapsDone = make([]int, n)
	r.lapsLed = make([]int, n)
	r.fastest = make([]int, n)
	r.startPos = make([]int, n)
	r.pits = make([]int, n)
	r.elapsed = make([]float64, n)
	r.cursor = 0
	r.period = 1

	// The starting grid is settled in qualifying and does not change.
	order := r.rnd.Perm(n)
	for k := range r.grid {
		r.numbers[k] = nascarNumbers[r.grid[k].ID]
		r.makes[k] = nascarMakes[r.grid[k].ID]
		r.salaries[k] = 6000 + r.rnd.Intn(5000)
		r.qualifying[k] = round1(105 + r.rnd.Float64()*15)
		r.startPos[order[k]] = k + 1
	}
}

// advance runs one lap for the next car in the rotation.
func (r *nascarSim) advance() {
	if r.over {
		return
	}
	k := r.cursor
	r.cursor++
	if r.cursor >= nascarFieldSize {
		r.cursor = 0
		r.period++
		if r.period > r.laps {
			r.period = r.laps
			r.over = true
		}
	}

	lap := round2(28 + r.rnd.Float64()*4)
	if r.chance(0.02) { // green-flag stop
		lap = round2(lap + 35 + r.rnd.Float64()*10)
		r.pits[k]++
	}
	r.elapsed[k] = round2(r.elapsed[k] + lap)
	r.lapsDone[k]++
	if r.positionOf(k) == 1 {
		r.lapsLed[k]++
	}
	if r.chance(0.08) {
		r.fastest[k]++
	}
	r.lastRow = r.driverRace(k)
}

// positionOf ranks a car by laps completed then elapsed time, the way a timing
// tower orders the field.
func (r *nascarSim) positionOf(k int) int {
	pos := 1
	for j := range r.grid {
		if j == k {
			continue
		}
		if r.lapsDone[j] > r.lapsDone[k] ||
			(r.lapsDone[j] == r.lapsDone[k] && r.elapsed[j] < r.elapsed[k]) {
			pos++
		}
	}
	return pos
}

// nascarPoints is the Cup Series finishing scale for the top ten.
var nascarPoints = []int{40, 35, 34, 33, 32, 31, 30, 29, 28, 27}

func (r *nascarSim) pointsFor(pos int) float64 {
	if pos >= 1 && pos <= len(nascarPoints) {
		return float64(nascarPoints[pos-1])
	}
	if pos <= 36 {
		return float64(37 - pos)
	}
	return 1
}

// driverRace builds one timing row. Every counter is a decimal on the wire,
// because the same model carries season averages elsewhere.
func (r *nascarSim) driverRace(k int) sdio.NASCARDriverRace {
	d := r.grid[k]
	pos := r.positionOf(k)
	points := r.pointsFor(pos)
	fantasy := round1(points + float64(r.lapsLed[k])*0.25 + float64(r.fastest[k])*0.5 +
		float64(r.startPos[k]-pos))

	return sdio.NASCARDriverRace{
		StatID:           r.raceID*100 + k,
		DriverID:         d.ID,
		Season:           r.season,
		Name:             d.Name,
		Number:           i(numberOf(r.numbers[k])),
		NumberDisplay:    s(r.numbers[k]),
		Manufacturer:     r.makes[k],
		DraftKingsSalary: r.salaries[k],
		RaceID:           r.raceID,
		Day:              sdioDateTime(r.start),
		DateTime:         sdioDateTime(r.start),
		Updated:          sdioDateTime(now()),
		Created:          sdioDateTime(r.start),

		FantasyPoints:           fantasy,
		FantasyPointsDraftKings: round1(fantasy * 1.5),
		QualifyingSpeed:         f(r.qualifying[k]),
		PoleFinalPosition:       float64(r.startPos[k]),
		StartPosition:           float64(r.startPos[k]),
		FinalPosition:           float64(pos),
		PositionDifferential:    float64(r.startPos[k] - pos),
		Laps:                    float64(r.lapsDone[k]),
		LapsLed:                 float64(r.lapsLed[k]),
		FastestLaps:             float64(r.fastest[k]),
		Points:                  points,
		Bonus:                   0,
		Penalty:                 0,
		Wins:                    boolFloat(r.over && pos == 1),
		Poles:                   boolFloat(r.startPos[k] == 1),
		CurrentPosition:         float64(pos),
	}
}

// numberOf parses a car number, which the feed carries both as an integer and
// as a display string (some entries are "07" or similar).
func numberOf(display string) int {
	n := 0
	for _, c := range display {
		if c < '0' || c > '9' {
			return n
		}
		n = n*10 + int(c-'0')
	}
	return n
}

// --- renderers -------------------------------------------------------------

func (r *nascarSim) lastDriverRace() sdio.NASCARDriverRace { return r.lastRow }

func (r *nascarSim) driverRaces() []sdio.NASCARDriverRace {
	out := make([]sdio.NASCARDriverRace, 0, nascarFieldSize)
	for k := range r.grid {
		out = append(out, r.driverRace(k))
	}
	sortBy(out, func(a, b sdio.NASCARDriverRace) bool { return a.FinalPosition < b.FinalPosition })
	return out
}

func (r *nascarSim) raceModel() sdio.NASCARRace {
	race := sdio.NASCARRace{
		RaceID:        r.raceID,
		SeriesID:      r.seriesID,
		SeriesName:    r.seriesName,
		Season:        r.season,
		Name:          r.name,
		Day:           sdioDateTime(r.start),
		DateTime:      sdioDateTime(r.start),
		Track:         r.track,
		Broadcast:     pick(r.rnd, []string{"FOX", "NBC", "USA", "FS1"}),
		IsInProgress:  !r.over,
		IsOver:        r.over,
		Updated:       sdioDateTime(now()),
		Created:       sdioDateTime(r.start),
		Canceled:      false,
		ScheduledLaps: r.laps,
		ActualLaps:    r.period,
	}
	rows := r.driverRaces()
	if len(rows) > 0 {
		if r.over {
			race.WinnerID = rows[0].DriverID
		}
		for k := range r.grid {
			if r.startPos[k] == 1 {
				race.PoleWinnerID = r.grid[k].ID
			}
		}
	}
	return race
}

func (r *nascarSim) raceResult() sdio.NASCARRaceResult {
	return sdio.NASCARRaceResult{Race: r.raceModel(), DriverRaces: r.driverRaces()}
}

func (r *nascarSim) fixtureID() string { return fmt.Sprintf("%d", r.raceID) }

// schedule renders the season calendar: every race in the series, with the
// simulated one in progress and the rest scheduled or complete. This is the
// /races/{season} document, verified against captures of both 2025 and 2026.
func (r *nascarSim) schedule() []sdio.NASCARRace {
	const seasonRaces = 36
	out := make([]sdio.NASCARRace, 0, seasonRaces)
	for k := 0; k < seasonRaces; k++ {
		race := sdio.NASCARRace{
			RaceID:        r.raceID - r.round + k,
			SeriesID:      r.seriesID,
			SeriesName:    r.seriesName,
			Season:        r.season,
			Name:          fmt.Sprintf("%s 400", nascarTracks[k%len(nascarTracks)]),
			Day:           sdioDateTime(r.start.AddDate(0, 0, (k-r.round)*7)),
			DateTime:      sdioDateTime(r.start.AddDate(0, 0, (k-r.round)*7)),
			Track:         nascarTracks[k%len(nascarTracks)],
			Broadcast:     nascarBroadcasts[k%len(nascarBroadcasts)],
			Updated:       sdioDateTime(now()),
			Created:       sdioDateTime(r.start.AddDate(0, -6, 0)),
			ScheduledLaps: 200 + (k*7)%200,
		}
		switch {
		case k < r.round: // already run
			race.IsOver = true
			race.ActualLaps = race.ScheduledLaps
			race.WinnerID = nascarDrivers[k%len(nascarDrivers)].ID
			race.PoleWinnerID = nascarDrivers[(k+3)%len(nascarDrivers)].ID
		case k == r.round: // the race being simulated
			race.RaceID = r.raceID
			race.Name = r.name
			race.Track = r.track
			race.IsInProgress = !r.over
			race.IsOver = r.over
			race.ScheduledLaps = r.laps
			race.ActualLaps = r.period
		}
		out = append(out, race)
	}
	return out
}

// drivers renders the driver directory: the /drivers document, which carries
// every driver the provider knows, not just the current field. Most profile
// columns are null for the long tail of the list, which the capture confirms
// and which this reproduces rather than filling in.
func (r *nascarSim) drivers() []sdio.NASCARDriver {
	out := make([]sdio.NASCARDriver, 0, len(nascarDrivers))
	for k, d := range nascarDrivers {
		drv := sdio.NASCARDriver{
			DriverID:      d.ID,
			FirstName:     firstOf(d.Name),
			LastName:      lastOf(d.Name),
			Number:        i(numberOf(nascarNumbers[d.ID])),
			NumberDisplay: s(nascarNumbers[d.ID]),
			PhotoUrl:      (fmt.Sprintf("https://s3-us-west-2.amazonaws.com/static.fantasydata.com/headshots/nas/low-res/%d.png", d.ID)),
			Updated:       sdioDateTime(now()),
			Created:       s(sdioDateTime(r.start.AddDate(-3, 0, 0))),
		}
		// The active field carries a full profile; the rest are sparse, as on
		// the wire.
		if k < nascarFieldSize {
			drv.Team = s(nascarTeams[d.ID])
			drv.Manufacturer = s(nascarMakes[d.ID])
			drv.Gender = s("Male")
			drv.Engine = s(nascarMakes[d.ID])
			drv.Chassis = s("Next Gen")
			drv.CrewChief = s(fmt.Sprintf("%s %s", pick(r.rnd, firstNames), pick(r.rnd, lastNames)))
			drv.Sponsors = s(pick(r.rnd, []string{"Mobil 1", "GEICO", "Freeway Insurance", "Bass Pro Shops"}))
			drv.BirthPlace = s("United States")
			drv.Height = i(68 + k%8)
			drv.Weight = i(160 + k%30)
		}
		out = append(out, drv)
	}
	sortBy(out, func(a, b sdio.NASCARDriver) bool { return a.DriverID < b.DriverID })
	return out
}
