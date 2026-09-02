// Package apisports is the client for api-sports.io, the pipeline's primary
// provider.
//
// # One API, twelve hosts
//
// API-Sports is not a single service. Each sport is a separate host with its
// own version prefix — v3.football.api-sports.io, v1.basketball.api-sports.io,
// v2.nba.api-sports.io — and, critically, its own independently metered quota.
// One key authenticates all of them. A venue on the free plan therefore has 100
// requests/day *per sport it polls*, not 100 in total, and getting throttled on
// football says nothing about the basketball budget.
//
// That shapes everything downstream: the limiter, the usage tracker and the
// budget allocator all key on the vertical, never on the account.
//
// # Verified, not assumed
//
// The vertical table below was probed against the live API on 2026-09-01 with a
// free key. Every host listed returned HTTP 200 on /status. Four sports this
// pipeline carries have NO host at api-sports.io under any spelling tried —
// cricket, tennis, golf and NASCAR — so they keep their own providers; see the
// provider map in the README. Motorsport at API-Sports is Formula 1.
//
// # Response envelope
//
// Every endpoint returns the same envelope, which is why one client covers
// twelve sports:
//
//	{"get": "...", "parameters": {...}, "errors": [], "results": 8, "response": [...]}
//
// `errors` is the trap: a request that fails validation still returns HTTP 200,
// with the reason inside the body. A client that only checks the status code
// reports success on an empty result forever. See Do.
package apisports

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Vertical is one API-Sports sport host.
type Vertical string

const (
	VerticalFootball         Vertical = "football"   // association football
	VerticalBasketball       Vertical = "basketball" // incl. NCAA
	VerticalBaseball         Vertical = "baseball"
	VerticalAmericanFootball Vertical = "american-football" // NFL + NCAA
	VerticalHockey           Vertical = "hockey"
	VerticalRugby            Vertical = "rugby"
	VerticalVolleyball       Vertical = "volleyball"
	VerticalHandball         Vertical = "handball"
	VerticalFormula1         Vertical = "formula-1"
	VerticalMMA              Vertical = "mma"
	VerticalAFL              Vertical = "afl"
	VerticalNBA              Vertical = "nba"
)

// BulkMode says how a vertical exposes "everything happening now".
//
// This is not uniform, and discovering that changed the scheduler's design.
// Football, American football and NBA accept `live=all` and return only
// in-progress fixtures. Every other vertical rejects the parameter outright
// ("The Live field do not exist.") and must be polled by date, returning the
// whole day's card to be filtered client-side.
//
// Both are still ONE request per sport per poll — which is the point. The
// per-game loop this replaced cost one request per fixture, and on a 100/day
// budget that is the difference between covering a sport and covering four
// games of it.
type BulkMode int

const (
	// BulkLive supports ?live=all and returns in-progress fixtures only.
	BulkLive BulkMode = iota
	// BulkDate supports ?date=YYYY-MM-DD and returns the day's whole card.
	BulkDate
	// BulkSeason is polled by season; used for motorsport, where a race
	// weekend is a schedule entry rather than a daily fixture list.
	BulkSeason
)

// Spec describes one vertical: where it lives and how to sweep it.
type Spec struct {
	Vertical Vertical
	// Host is the fully-qualified API-Sports host, version prefix included.
	Host string
	// BulkPath is the collection endpoint — /fixtures for football, /games for
	// most others, /fights for MMA, /races for Formula 1.
	BulkPath string
	// Mode says which bulk parameter the vertical accepts.
	Mode BulkMode
	// Verified records that this host answered a live /status call.
	Verified bool
}

// specs is the vertical catalog.
//
// Host, path and mode for every row were confirmed against the live API on
// 2026-09-01: /status returned 200 on all twelve, and the bulk parameter was
// probed per vertical rather than assumed uniform.
var specs = map[Vertical]Spec{
	VerticalFootball:         {VerticalFootball, "v3.football.api-sports.io", "/fixtures", BulkLive, true},
	VerticalAmericanFootball: {VerticalAmericanFootball, "v1.american-football.api-sports.io", "/games", BulkLive, true},
	VerticalNBA:              {VerticalNBA, "v2.nba.api-sports.io", "/games", BulkLive, true},

	VerticalBasketball: {VerticalBasketball, "v1.basketball.api-sports.io", "/games", BulkDate, true},
	VerticalBaseball:   {VerticalBaseball, "v1.baseball.api-sports.io", "/games", BulkDate, true},
	VerticalHockey:     {VerticalHockey, "v1.hockey.api-sports.io", "/games", BulkDate, true},
	VerticalRugby:      {VerticalRugby, "v1.rugby.api-sports.io", "/games", BulkDate, true},
	VerticalAFL:        {VerticalAFL, "v1.afl.api-sports.io", "/games", BulkDate, true},
	VerticalVolleyball: {VerticalVolleyball, "v1.volleyball.api-sports.io", "/games", BulkDate, true},
	VerticalHandball:   {VerticalHandball, "v1.handball.api-sports.io", "/games", BulkDate, true},
	VerticalMMA:        {VerticalMMA, "v1.mma.api-sports.io", "/fights", BulkDate, true},

	VerticalFormula1: {VerticalFormula1, "v1.formula-1.api-sports.io", "/races", BulkSeason, true},
}

// SpecFor returns the vertical's spec.
func SpecFor(v Vertical) (Spec, bool) {
	s, ok := specs[v]
	return s, ok
}

// Verticals lists every supported vertical, sorted for stable output.
func Verticals() []Vertical {
	out := make([]Vertical, 0, len(specs))
	for v := range specs {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// BaseURL is the https root for a vertical.
func (s Spec) BaseURL() string { return "https://" + s.Host }

// BulkQuery builds the parameters for a bulk sweep at a point in time.
//
// One request per sport, whichever mode the vertical uses.
func (s Spec) BulkQuery(now time.Time) map[string]string {
	switch s.Mode {
	case BulkLive:
		return map[string]string{"live": "all"}
	case BulkSeason:
		return map[string]string{"season": fmt.Sprintf("%d", now.Year())}
	default:
		return map[string]string{"date": now.Format("2006-01-02")}
	}
}

// String renders the vertical for logs.
func (v Vertical) String() string { return string(v) }

// ParseVertical resolves a name, tolerating the spellings that appear in
// licences and CLI flags.
func ParseVertical(name string) (Vertical, error) {
	n := strings.ToLower(strings.TrimSpace(name))
	n = strings.NewReplacer("_", "-", " ", "-").Replace(n)
	switch n {
	case "soccer", "football", "epl":
		return VerticalFootball, nil
	case "american-football", "americanfootball", "nfl", "ncaaf":
		return VerticalAmericanFootball, nil
	case "nba":
		return VerticalNBA, nil
	case "basketball", "ncaab", "cbb":
		return VerticalBasketball, nil
	case "f1", "formula1", "formula-1":
		return VerticalFormula1, nil
	case "ufc", "mma":
		return VerticalMMA, nil
	}
	if _, ok := specs[Vertical(n)]; ok {
		return Vertical(n), nil
	}
	return "", fmt.Errorf("apisports: no vertical for %q", name)
}
