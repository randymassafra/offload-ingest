package generators

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// API-Sports simulation.
//
// This replaced the per-provider generators for every sport API-Sports serves.
// The reason is shape parity, and it is the whole point of the consolidation:
// a load test is only worth running if the bytes it pushes through Kafka are
// the bytes production will push. When production moved to API-Sports, a
// simulation still emitting SportsDataIO-shaped documents would have been
// testing a pipeline nobody was going to run.
//
// So rather than hand-writing a simulation per sport, this loads a REAL
// captured API-Sports response and evolves it: the clock advances, scores
// change, statuses walk from "not started" through to "finished", and the
// fixture rolls over. The document shape is authentic by construction because
// it came off the wire.
//
// Two document families, both reproduced exactly as captured:
//
//	fixture family  soccer — nested fixture{}, league{}, teams{}, goals{}, score{}
//	games family    everything else — flat id/date/status/league/teams/scores
//
// A vertical with no usable capture (out of season on the day we captured, or
// gated by the plan) falls back to a synthesised document in its family's
// shape. Those are marked ProvenanceModeled, not captured — the catalog says
// which is which rather than letting a synthesised shape pass as evidence.

// apiSportsCaptureDir is where the capturer writes.
//
// Resolved by walking up from the working directory, because a Go test runs in
// its own package directory: a plain relative path finds the captures when the
// binary is launched from the module root and silently finds nothing under
// `go test ./internal/generators/`. That failure is invisible — every feed just
// quietly registers as modeled and synthesises — so the path is searched rather
// than assumed.
var apiSportsCaptureDir = findCaptureDir()

func findCaptureDir() string {
	rel := filepath.Join("fixtures", "apisports")
	dir, err := os.Getwd()
	if err != nil {
		return rel
	}
	for i := 0; i < 6; i++ {
		candidate := filepath.Join(dir, rel)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return rel
}

// apiSportsBinding ties a pipeline sport to its vertical capture and family.
type apiSportsBinding struct {
	sport    Sport
	vertical string
	family   docFamily
	// path is the collection endpoint, matching what the production sweeper
	// calls, so the catalog and the live client never disagree.
	path string
	// bulkParam documents how the vertical is swept, which differs per host.
	bulkParam string
}

type docFamily int

const (
	familyGames docFamily = iota
	familyFixture
	familyFight
	familyRace
)

// apiSportsCatalog is the sport catalog for the primary provider.
//
// Endpoint paths and bulk parameters were probed live: only football,
// american-football and NBA accept live=all; the rest are swept by date, and
// Formula 1 by season.
var apiSportsCatalog = []apiSportsBinding{
	{SportSoccer, "football", familyFixture, "/fixtures", "live=all"},
	{SportNFL, "american-football", familyGames, "/games", "live=all"},
	{SportNCAAF, "american-football", familyGames, "/games", "live=all"},
	{SportNBA, "nba", familyGames, "/games", "live=all"},
	{SportNCAAB, "basketball", familyGames, "/games", "date"},
	{SportAFL, "afl", familyGames, "/games", "date"},
	{SportRugby, "rugby", familyGames, "/games", "date"},
	{SportUFC, "mma", familyFight, "/fights", "date"},
	{SportMMA, "mma", familyFight, "/fights", "date"},
	{SportF1, "f1", familyRace, "/races", "season"},
}

// apiSportsHosts maps a vertical to its host, for the endpoint catalog.
var apiSportsHosts = map[string]string{
	"football":          "v3.football.api-sports.io",
	"american-football": "v1.american-football.api-sports.io",
	"nba":               "v2.nba.api-sports.io",
	"basketball":        "v1.basketball.api-sports.io",
	"afl":               "v1.afl.api-sports.io",
	"rugby":             "v1.rugby.api-sports.io",
	"mma":               "v1.mma.api-sports.io",
	"f1":                "v1.formula-1.api-sports.io",
}

// captureFile is the capture a vertical replays. Formula 1's capture is filed
// under the provider's own host name rather than the pipeline's sport token.
func (b apiSportsBinding) captureFile() string {
	name := b.vertical
	if name == "f1" {
		name = "formula-1"
	}
	return filepath.Join(apiSportsCaptureDir, name+".json")
}

func init() {
	for _, b := range apiSportsCatalog {
		binding := b
		docs, provenance := loadAPISportsDocs(binding)

		mk := func(rnd *rand.Rand) *apiSportsSim {
			s := &apiSportsSim{base: newBase(rnd), binding: binding, seed: docs}
			s.reset()
			return s
		}
		register(Endpoint{
			Sport: binding.sport, Kind: FeedBoxScore,
			Provider: ProviderAPISports, Provenance: provenance,
			Path:  binding.path,
			Model: apiSportsModel(binding.family),
		}, func(rnd *rand.Rand) (sim, renderer) {
			s := mk(rnd)
			return s, func() (any, string, bool) { return s.document(), "", true }
		})

		// The live sweep is the production access pattern: one call returns the
		// whole card. The telemetry feed reproduces it so a load test exercises
		// the same payload size production will see, which is the number that
		// actually stresses a consumer.
		register(Endpoint{
			Sport: binding.sport, Kind: FeedTelemetry,
			Provider: ProviderAPISports, Provenance: provenance,
			Path: binding.path, Projection: "response[]",
			Model: apiSportsModel(binding.family) + "[]",
		}, func(rnd *rand.Rand) (sim, renderer) {
			s := mk(rnd)
			return s, func() (any, string, bool) { return s.envelope(), "", true }
		})
	}
}

func apiSportsModel(f docFamily) string {
	switch f {
	case familyFixture:
		return "Fixture"
	case familyFight:
		return "Fight"
	case familyRace:
		return "Race"
	default:
		return "Game"
	}
}

// apiSportsDocCache avoids re-reading the same capture once per registered feed.
var (
	apiSportsDocMu    sync.Mutex
	apiSportsDocCache = map[string][]map[string]any{}
)

// loadAPISportsDocs reads a vertical's capture, returning the documents and the
// provenance they justify.
func loadAPISportsDocs(b apiSportsBinding) ([]map[string]any, Provenance) {
	path := b.captureFile()

	apiSportsDocMu.Lock()
	cached, seen := apiSportsDocCache[path]
	apiSportsDocMu.Unlock()

	if !seen {
		cached = readAPISportsCapture(path)
		apiSportsDocMu.Lock()
		apiSportsDocCache[path] = cached
		apiSportsDocMu.Unlock()
	}
	if len(cached) > 0 {
		return cached, ProvenanceCaptured
	}
	// No usable capture: synthesise, and say so.
	return nil, ProvenanceModeled
}

// readAPISportsCapture pulls the response array out of a captured envelope.
func readAPISportsCapture(path string) []map[string]any {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var env struct {
		Response json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(raw, &env); err != nil || len(env.Response) == 0 {
		return nil
	}
	var rows []map[string]any
	if err := json.Unmarshal(env.Response, &rows); err != nil {
		return nil
	}
	// Cap the working set. A 180-race Formula 1 capture is more variety than a
	// fixture pool needs, and holding all of it per feed instance would cost
	// memory for no extra coverage.
	const maxSeed = 24
	if len(rows) > maxSeed {
		rows = rows[:maxSeed]
	}
	return rows
}

// apiSportsSim evolves one captured document through a match.
type apiSportsSim struct {
	base
	binding apiSportsBinding
	seed    []map[string]any

	doc      map[string]any
	fixture  string
	homeGoal int
	awayGoal int
	elapsed  int
	statusIx int
}

// statusLadder is the sequence a fixture walks, per family.
//
// The tokens are the provider's own, taken from the captures: a consumer
// matching on "1H" or "FT" sees exactly what it would see in production.
type statusStep struct {
	short, long string
	live        bool
}

var soccerLadder = []statusStep{
	{"NS", "Not Started", false},
	{"1H", "First Half", true},
	{"HT", "Halftime", false},
	{"2H", "Second Half", true},
	{"FT", "Match Finished", false},
}

var quarterLadder = []statusStep{
	{"NS", "Not Started", false},
	{"Q1", "Quarter 1", true},
	{"Q2", "Quarter 2", true},
	{"HT", "Halftime", false},
	{"Q3", "Quarter 3", true},
	{"Q4", "Quarter 4", true},
	{"FT", "Game Finished", false},
}

var boutLadder = []statusStep{
	{"NS", "Not Started", false},
	{"LIVE", "In Progress", true},
	{"FT", "Finished", false},
}

func (s *apiSportsSim) ladder() []statusStep {
	switch s.binding.family {
	case familyFixture:
		return soccerLadder
	case familyFight, familyRace:
		return boutLadder
	default:
		return quarterLadder
	}
}

func (s *apiSportsSim) reset() {
	s.newFixture("APS", len(s.ladder()), 15*time.Minute, 30*time.Second)
	s.homeGoal, s.awayGoal, s.elapsed, s.statusIx = 0, 0, 0, 0

	if len(s.seed) > 0 {
		// Deep-copy a captured document so each fixture mutates its own.
		src := s.seed[s.rnd.Intn(len(s.seed))]
		s.doc = deepCopyMap(src)
	} else {
		s.doc = s.synthesise()
	}
	s.fixture = s.readFixtureID()
	s.applyState()
}

// advance moves the fixture on one tick.
func (s *apiSportsSim) advance() {
	if s.over {
		return
	}
	ladder := s.ladder()
	step := ladder[s.statusIx]

	if step.live {
		s.elapsed++
		// Scoring rate is family-shaped: a soccer goal is rare, a basketball
		// possession is not. These are not precise models, only enough that a
		// consumer sees a score that moves plausibly.
		p := 0.06
		if s.binding.family == familyGames {
			p = 0.55
		}
		if s.chance(p) {
			points := 1
			if s.binding.family == familyGames {
				points = pick(s.rnd, []int{1, 2, 2, 3})
			}
			if s.chance(0.5) {
				s.homeGoal += points
			} else {
				s.awayGoal += points
			}
		}
	}

	// Advance the ladder roughly every few ticks, so a consumer sees each state.
	if s.chance(0.18) {
		s.statusIx++
		if s.statusIx >= len(ladder) {
			s.over = true
			s.statusIx = len(ladder) - 1
		}
		s.period = s.statusIx + 1
	}
	s.applyState()
}

// applyState writes the simulation's state back into the document, at the paths
// the provider actually uses for that family.
func (s *apiSportsSim) applyState() {
	step := s.ladder()[s.statusIx]

	switch s.binding.family {
	case familyFixture:
		fixture, _ := s.doc["fixture"].(map[string]any)
		if fixture == nil {
			fixture = map[string]any{}
			s.doc["fixture"] = fixture
		}
		fixture["id"] = s.gameID
		fixture["date"] = now().UTC().Format(time.RFC3339)
		fixture["timestamp"] = now().Unix()
		status := map[string]any{"short": step.short, "long": step.long}
		// elapsed is null before kick-off and after the final whistle, which is
		// how the provider sends it — not zero.
		if step.live {
			status["elapsed"] = s.elapsed
		} else {
			status["elapsed"] = nil
		}
		status["extra"] = nil
		fixture["status"] = status
		s.doc["goals"] = map[string]any{"home": s.homeGoal, "away": s.awayGoal}

	case familyFight, familyRace:
		s.doc["id"] = s.gameID
		s.doc["date"] = now().UTC().Format(time.RFC3339)
		s.doc["timestamp"] = now().Unix()
		s.doc["status"] = map[string]any{"short": step.short, "long": step.long}

	default:
		s.doc["id"] = s.gameID
		s.doc["date"] = now().UTC().Format(time.RFC3339)
		s.doc["timestamp"] = now().Unix()
		s.doc["time"] = now().UTC().Format("15:04")
		timer := any(nil)
		if step.live {
			timer = s.elapsed
		}
		s.doc["status"] = map[string]any{
			"short": step.short, "long": step.long, "timer": timer,
		}
		s.writeGamesScores()
	}
}

// writeGamesScores updates the flat family's scores, preserving whichever score
// sub-shape the captured vertical uses.
//
// The shape genuinely differs — basketball reports per-quarter columns, hockey
// a plain integer, baseball a per-inning map — so the totals are written into
// the structure that is already there rather than replacing it with one guess.
func (s *apiSportsSim) writeGamesScores() {
	scores, _ := s.doc["scores"].(map[string]any)
	if scores == nil {
		s.doc["scores"] = map[string]any{"home": s.homeGoal, "away": s.awayGoal}
		return
	}
	for side, value := range map[string]int{"home": s.homeGoal, "away": s.awayGoal} {
		switch existing := scores[side].(type) {
		case map[string]any:
			existing["total"] = value
			scores[side] = existing
		default:
			scores[side] = value
		}
	}
}

// synthesise builds a document for a vertical with no usable capture.
//
// It follows the family's shape exactly as observed on the verticals that did
// return data, because the families are consistent across hosts — but it is
// still synthetic, which is why these feeds are registered as modeled.
func (s *apiSportsSim) synthesise() map[string]any {
	home, away := pickPair(s.rnd, apiSportsTeamsFor(s.binding.sport))
	league := apiSportsLeagueFor(s.binding.sport)

	team := func(c competitor) map[string]any {
		return map[string]any{
			"id": c.ID, "name": c.Name,
			"logo": fmt.Sprintf("https://media.api-sports.io/%s/teams/%d.png",
				s.binding.vertical, c.ID),
		}
	}

	switch s.binding.family {
	case familyFixture:
		return map[string]any{
			"fixture": map[string]any{
				"id": s.gameID, "referee": nil, "timezone": "UTC",
				"date": now().UTC().Format(time.RFC3339), "timestamp": now().Unix(),
				"periods": map[string]any{"first": now().Unix(), "second": nil},
				"venue":   map[string]any{"id": nil, "name": "Stadium", "city": home.Country},
				"status":  map[string]any{"long": "Not Started", "short": "NS", "elapsed": nil, "extra": nil},
			},
			"league": league,
			"teams": map[string]any{
				"home": mergeMap(team(home), map[string]any{"winner": nil}),
				"away": mergeMap(team(away), map[string]any{"winner": nil}),
			},
			"goals": map[string]any{"home": 0, "away": 0},
			"score": map[string]any{
				"halftime":  map[string]any{"home": nil, "away": nil},
				"fulltime":  map[string]any{"home": nil, "away": nil},
				"extratime": map[string]any{"home": nil, "away": nil},
				"penalty":   map[string]any{"home": nil, "away": nil},
			},
			"events": []any{},
		}

	case familyFight:
		return map[string]any{
			"id": s.gameID, "date": now().UTC().Format(time.RFC3339),
			"timestamp": now().Unix(), "timezone": "UTC",
			"slug":     strings.ToLower(home.Key + "-vs-" + away.Key),
			"category": "Lightweight", "is_main": true,
			"status": map[string]any{"long": "Not Started", "short": "NS"},
			"fighters": map[string]any{
				"first":  map[string]any{"id": home.ID, "name": home.Name, "winner": nil},
				"second": map[string]any{"id": away.ID, "name": away.Name, "winner": nil},
			},
		}

	case familyRace:
		return map[string]any{
			"id": s.gameID, "competition": league,
			"circuit": map[string]any{"id": 1, "name": "Circuit", "image": nil},
			"season":  seasonFor(now()), "type": "Race",
			"laps":        map[string]any{"current": nil, "total": 58},
			"fastest_lap": map[string]any{"driver": map[string]any{"id": nil}, "time": nil},
			"distance":    "305.270 km", "timezone": "UTC",
			"date":    now().UTC().Format(time.RFC3339),
			"weather": nil,
			"status":  map[string]any{"long": "Not Started", "short": "NS"},
		}

	default:
		return map[string]any{
			"id": s.gameID, "date": now().UTC().Format(time.RFC3339),
			"time": now().UTC().Format("15:04"), "timestamp": now().Unix(),
			"timezone": "UTC", "stage": nil, "week": fmt.Sprint(1 + s.rnd.Intn(18)),
			"status":  map[string]any{"long": "Not Started", "short": "NS", "timer": nil},
			"league":  league,
			"country": map[string]any{"id": 1, "name": home.Country, "code": home.Country, "flag": nil},
			"teams":   map[string]any{"home": team(home), "away": team(away)},
			"scores": map[string]any{
				"home": map[string]any{"quarter_1": nil, "quarter_2": nil, "quarter_3": nil, "quarter_4": nil, "over_time": nil, "total": 0},
				"away": map[string]any{"quarter_1": nil, "quarter_2": nil, "quarter_3": nil, "quarter_4": nil, "over_time": nil, "total": 0},
			},
		}
	}
}

// document is the single-fixture payload.
func (s *apiSportsSim) document() map[string]any { return s.doc }

// envelope wraps the fixture in the provider's response envelope, which is what
// a bulk sweep actually returns.
func (s *apiSportsSim) envelope() map[string]any {
	return map[string]any{
		"get":        strings.TrimPrefix(s.binding.path, "/"),
		"parameters": map[string]any{s.bulkKey(): s.bulkValue()},
		"errors":     []any{},
		"results":    1,
		"paging":     map[string]any{"current": 1, "total": 1},
		"response":   []any{s.doc},
	}
}

func (s *apiSportsSim) bulkKey() string {
	if k, _, ok := strings.Cut(s.binding.bulkParam, "="); ok {
		return k
	}
	return s.binding.bulkParam
}

func (s *apiSportsSim) bulkValue() string {
	if _, v, ok := strings.Cut(s.binding.bulkParam, "="); ok {
		return v
	}
	if s.binding.bulkParam == "season" {
		return fmt.Sprint(seasonFor(now()))
	}
	return now().UTC().Format("2006-01-02")
}

// readFixtureID pulls the provider's id out of whichever family this is, so the
// Kafka partition key matches what production would use.
func (s *apiSportsSim) readFixtureID() string {
	if fixture, ok := s.doc["fixture"].(map[string]any); ok {
		if id, ok := fixture["id"]; ok {
			return fmt.Sprint(id)
		}
	}
	if id, ok := s.doc["id"]; ok {
		return fmt.Sprint(id)
	}
	return fmt.Sprint(s.gameID)
}

func (s *apiSportsSim) fixtureID() string { return fmt.Sprint(s.gameID) }

// deepCopyMap clones a decoded JSON document so two feed instances replaying the
// same captured fixture cannot write through to each other.
func deepCopyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = deepCopyValue(v)
	}
	return out
}

func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return deepCopyMap(t)
	case []any:
		out := make([]any, len(t))
		for i, el := range t {
			out[i] = deepCopyValue(el)
		}
		return out
	default:
		return v
	}
}

func mergeMap(a, b map[string]any) map[string]any {
	out := make(map[string]any, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// apiSportsCaptureHasResults reports whether a capture carried real fixtures.
//
// Used by the provenance test: an empty card is a valid response and proves the
// route works, but it proves nothing about the document shape, so it must not
// let a feed claim captured evidence.
func apiSportsCaptureHasResults(path string) bool {
	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var env struct {
		Results int `json:"results"`
	}
	return json.Unmarshal(raw, &env) == nil && env.Results > 0
}
