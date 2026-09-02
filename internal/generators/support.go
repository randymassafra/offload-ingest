package generators

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"
)

// base carries the state every sport simulation needs: the RNG, the identity
// of the fixture, an id allocator and a wall clock.
type base struct {
	rnd    *rand.Rand
	season int

	// gameID is the provider's integer identifier for the fixture, and gameKey
	// its string form for the APIs (NFL) that use one.
	gameID   int
	gameKey  string
	globalID int

	// nextID hands out monotonically increasing surrogate keys for the child
	// records (plays, goals, stat lines) of the current fixture.
	nextID int

	start   time.Time
	period  int
	periods int
	elapsed time.Duration
	tick    time.Duration
	length  time.Duration // regulation length of one period, 0 if not clock-based
	over    bool
}

func newBase(rnd *rand.Rand) base {
	return base{rnd: rnd, season: seasonFor(time.Now())}
}

// seasonFor picks a plausible season year for a date.
func seasonFor(t time.Time) int {
	if t.Month() < time.July {
		return t.Year()
	}
	return t.Year() + 1
}

// newFixture assigns a fresh set of provider identifiers and resets the clock.
func (b *base) newFixture(prefix string, periods int, length, tick time.Duration) {
	b.gameID = 10000 + b.rnd.Intn(89999)
	b.globalID = 90000000 + b.gameID
	b.gameKey = fmt.Sprintf("%d%s%d", b.season, prefix, b.gameID)
	b.nextID = 1
	b.start = now().UTC()
	b.period = 1
	b.periods = periods
	b.length = length
	b.tick = tick
	b.elapsed = 0
	b.over = false
}

// id allocates the next surrogate key for a child record.
func (b *base) id() int {
	b.nextID++
	return b.gameID*1000 + b.nextID
}

func (b *base) done() bool        { return b.over }
func (b *base) fixtureID() string { return fmt.Sprintf("%d", b.gameID) }

// advanceClock moves the game clock forward one tick, rolling periods over and
// marking the fixture final after the last one.
func (b *base) advanceClock() {
	if b.over || b.length == 0 {
		return
	}
	b.elapsed += b.tick/2 + time.Duration(b.rnd.Int63n(int64(b.tick)+1))
	for b.elapsed >= b.length {
		b.elapsed -= b.length
		b.period++
		if b.period > b.periods {
			b.period = b.periods
			b.elapsed = b.length
			b.over = true
			return
		}
	}
}

// remaining is the time left in the current period.
func (b *base) remaining() time.Duration {
	r := b.length - b.elapsed
	if r < 0 {
		return 0
	}
	return r
}

// remainingParts splits the time left into the minutes/seconds pair the
// the wire models carry.
func (b *base) remainingParts() (min, sec int) {
	r := b.remaining()
	return int(r.Minutes()), int(r.Seconds()) % 60
}

// clockString formats the time left as "11:23", the wire format for
// TimeRemaining on the NFL models.
func (b *base) clockString() string {
	m, s := b.remainingParts()
	return fmt.Sprintf("%d:%02d", m, s)
}

// status maps the simulation state onto the Status vocabulary the feeds use.
func (b *base) status() string {
	switch {
	case b.over:
		return statusFinalText
	case b.period > 1 || b.elapsed > 0:
		return statusInProgressText
	default:
		return statusScheduledText
	}
}

// chance reports true with probability p.
func (b *base) chance(p float64) bool { return b.rnd.Float64() < p }

// --- wire formatting -------------------------------------------------------

// easternDateTime renders a US Eastern timestamp in the API's zone-less format.
func easternDateTime(t time.Time) string {
	return t.In(easternZone).Format(feedDateTimeLayout)
}

// easternDay renders a day in the API's format.
func easternDay(t time.Time) string {
	return t.In(easternZone).Format(feedDateLayout)
}

// utcTimestamp renders a UTC timestamp for the *Utc / *UTC fields.
func utcTimestamp(t time.Time) string {
	return t.UTC().Format(feedDateTimeUTCLayout)
}

// s and i are shorthand for the nullable scalars the wire models use
// everywhere. Passing a value through them documents "present, not null".
func s(v string) *string   { return &v }
func i(v int) *int         { return &v }
func f(v float64) *float64 { return &v }
func bo(v bool) *bool      { return &v }

// round1 and round2 keep generated decimals plausible on the wire.
func round1(v float64) float64 { return float64(int(v*10+0.5)) / 10 }
func round2(v float64) float64 { return float64(int(v*100+0.5)) / 100 }

// pick returns a random element of opts.
func pick[T any](rnd *rand.Rand, opts []T) T { return opts[rnd.Intn(len(opts))] }

// pickPair returns two distinct entries from pool.
func pickPair(rnd *rand.Rand, pool []competitor) (competitor, competitor) {
	a := rnd.Intn(len(pool))
	b := rnd.Intn(len(pool))
	for b == a {
		b = rnd.Intn(len(pool))
	}
	return pool[a], pool[b]
}

// pickN returns n distinct entries from pool.
func pickN(rnd *rand.Rand, pool []competitor, n int) []competitor {
	if n > len(pool) {
		n = len(pool)
	}
	out := make([]competitor, 0, n)
	for _, idx := range rnd.Perm(len(pool))[:n] {
		out = append(out, pool[idx])
	}
	return out
}

// roster builds n synthetic players for a team, with stable ids derived from
// the team id so a consumer sees a consistent squad across messages.
func roster(teamID int, n int, positions []string, rnd *rand.Rand) []player {
	out := make([]player, 0, n)
	for k := 0; k < n; k++ {
		out = append(out, player{
			ID:       teamID*100 + k + 1,
			Name:     fmt.Sprintf("%s %s", pick(rnd, firstNames), pick(rnd, lastNames)),
			Position: positions[k%len(positions)],
			Number:   k + 1,
		})
	}
	return out
}

// player is a synthetic squad member.
type player struct {
	ID       int
	Name     string
	Position string
	Number   int
}

var firstNames = []string{
	"James", "Marcus", "Andre", "Tyler", "Jordan", "Elias", "Nico", "Rowan",
	"Kai", "Diego", "Omar", "Luca", "Finn", "Ezra", "Malik", "Theo",
}

var lastNames = []string{
	"Harper", "Okafor", "Lindqvist", "Marchetti", "Byrne", "Sato", "Delacroix",
	"Ngata", "Kowalski", "Vargas", "Ferreira", "Haddad", "Nakamura", "Olsen",
	"Petrov", "Adeyemi",
}

// clamp bounds v to [lo, hi].
func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// territory names whose half of the field the ball is on, matching the
// YardLineTerritory field on the gridiron models.
func territory(yardLine int, offense, defense string) string {
	if yardLine <= 50 {
		return offense
	}
	return defense
}

// ordinal renders 1..4 as "1st".."4th" for DownAndDistance.
func ordinal(n int) string {
	switch n {
	case 1:
		return "1st"
	case 2:
		return "2nd"
	case 3:
		return "3rd"
	default:
		return fmt.Sprintf("%dth", n)
	}
}

// positionCategory maps a position onto the OFF/DEF/ST grouping.
func positionCategory(pos string) string {
	switch pos {
	case "QB", "RB", "WR", "TE", "OL":
		return "OFF"
	case "K", "P":
		return "ST"
	default:
		return "DEF"
	}
}

// sortBy is a generic stable sort, used to give every rendered array a
// deterministic order. Go randomises map iteration, so without this a replayed
// run with the same seed would not produce identical bytes.
func sortBy[T any](rows []T, less func(a, b T) bool) {
	sort.SliceStable(rows, func(x, y int) bool { return less(rows[x], rows[y]) })
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// sortedByKey returns a map's values ordered by key.
//
// This is not cosmetic. Go randomises map iteration order, and float addition
// is not associative, so summing player stats in map order makes a team total
// differ in its last bits from run to run. That breaks byte-for-byte replay
// with a fixed seed, which is the property that lets a downstream regression be
// blamed on the consumer rather than on this generator.
func sortedByKey[V any](m map[int]V) []V {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	out := make([]V, 0, len(keys))
	for _, k := range keys {
		out = append(out, m[k])
	}
	return out
}

// maxf returns the larger of two stat values, for the "long" columns that
// track a game's best single play.
func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// shortName is the display abbreviation a scorecard uses: first initial plus
// surname, so "Marcus Harper" becomes "M Harper".
func shortName(name string) string {
	first, last, ok := strings.Cut(name, " ")
	if !ok || first == "" {
		return name
	}
	return first[:1] + " " + last
}

// firstOf and lastOf split a full name the way the provider stores it, as
// separate first and last columns.
func firstOf(name string) string {
	first, _, ok := strings.Cut(name, " ")
	if !ok {
		return name
	}
	return first
}

func lastOf(name string) string {
	_, last, ok := strings.Cut(name, " ")
	if !ok {
		return ""
	}
	return last
}

// Date and status conventions, previously imported from internal/sdio.
//
// They moved here when that package was deleted with the SportsDataIO
// retirement. They are kept rather than rewritten because the simulations that
// use them — tennis via AllScores, cricket via Cricbuzz — were verified against
// captured responses with these values in place, and changing them now would
// invalidate that verification for no gain.
//
// Worth flagging for whoever touches this next: the zone-less US Eastern layout
// is a SportsDataIO convention, and neither remaining provider uses it. The
// schema comparison checks JSON paths rather than value formats, so it would
// not catch the mismatch. Correcting it per provider is real work with real
// value; it is simply not part of a vendor retirement.
const (
	statusScheduledText  = "Scheduled"
	statusInProgressText = "InProgress"
	statusFinalText      = "Final"

	feedDateTimeLayout    = "2006-01-02T15:04:05"
	feedDateLayout        = "2006-01-02"
	feedDateTimeUTCLayout = "2006-01-02T15:04:05Z"
)

// easternZone is US Eastern, with a fixed-offset fallback for a container that
// ships without a timezone database — which would otherwise silently shift
// every timestamp by five hours.
var easternZone = func() *time.Location {
	if loc, err := time.LoadLocation("America/New_York"); err == nil {
		return loc
	}
	return time.FixedZone("EST", -5*60*60)
}()
