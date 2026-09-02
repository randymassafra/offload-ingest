package apisports

import (
	"fmt"
	"sort"
	"strings"
)

// Sport is the pipeline's own sport token, the one that appears in a licence's
// `sports` claim and on every Kafka message.
//
// It is deliberately NOT the same vocabulary as Vertical. Three of the
// pipeline's sports — nfl, ncaaf, and to the provider's mind nba — share one
// API-Sports host and are told apart by a league id, and one pipeline sport can
// span many leagues. Collapsing the two vocabularies would either lose that
// distinction or leak provider structure into the licence.
type Sport string

// Binding maps one pipeline sport onto the API-Sports vertical and leagues that
// serve it.
type Binding struct {
	Sport    Sport
	Vertical Vertical
	// Leagues restricts a sweep to specific league ids. Empty means the whole
	// vertical, which is right for a host that serves exactly one competition
	// (AFL) and wrong for one that serves 427 (basketball).
	Leagues []int
	// LeagueNames documents the ids, because a bare list of integers is
	// unreviewable and these were resolved from the live /leagues endpoint.
	LeagueNames []string
}

// bindings is the sport catalog.
//
// Every league id below was read from the live /leagues endpoint on 2026-09-01,
// not guessed. The ones that matter most for a US venue — NFL 1, NCAA football
// 2, NCAA basketball 116, NBA 12 — are the ids the provider actually returns.
var bindings = map[Sport]Binding{
	"nfl":   {"nfl", VerticalAmericanFootball, []int{1}, []string{"NFL"}},
	"ncaaf": {"ncaaf", VerticalAmericanFootball, []int{2}, []string{"NCAA"}},
	"ncaab": {"ncaab", VerticalBasketball, []int{116}, []string{"NCAA"}},

	// NBA has a dedicated v2 host as well as being league 12 on the basketball
	// host. The dedicated host is used: it carries richer per-game detail and,
	// because quota is metered per host, polling it leaves the basketball
	// budget entirely free for NCAA.
	"nba": {"nba", VerticalNBA, nil, []string{"NBA"}},

	"soccer": {"soccer", VerticalFootball,
		[]int{39, 140, 135, 78, 61, 2, 253, 88, 94},
		[]string{"Premier League", "La Liga", "Serie A", "Bundesliga", "Ligue 1",
			"UEFA Champions League", "MLS", "Eredivisie", "Primeira Liga"}},

	"afl": {"afl", VerticalAFL, []int{1}, []string{"AFL Premiership"}},

	"rugby": {"rugby", VerticalRugby,
		[]int{13, 16, 51, 71, 76, 85},
		[]string{"Premiership Rugby", "Top 14", "Six Nations", "Super Rugby",
			"United Rugby Championship", "Rugby Championship"}},

	// UFC and MMA share one host, exactly as they shared one SportsDataIO
	// product. They stay separate pipeline sports so a licence can entitle one
	// without the other.
	"ufc": {"ufc", VerticalMMA, nil, []string{"UFC"}},
	"mma": {"mma", VerticalMMA, nil, nil},

	"f1": {"f1", VerticalFormula1, nil, []string{"Formula 1"}},
}

// BindingFor resolves a pipeline sport to its API-Sports binding.
func BindingFor(sport Sport) (Binding, bool) {
	b, ok := bindings[Sport(strings.ToLower(strings.TrimSpace(string(sport))))]
	return b, ok
}

// Sports lists every sport API-Sports can serve, sorted.
func Sports() []Sport {
	out := make([]Sport, 0, len(bindings))
	for s := range bindings {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Serves reports whether API-Sports covers a pipeline sport at all.
//
// Three of the pipeline's sports it does not: cricket, tennis and golf
// have no host there under any spelling. They keep their own providers, and
// this function is how the router knows not to look.
func Serves(sport Sport) bool {
	_, ok := BindingFor(sport)
	return ok
}

// Region is a licensed content bundle. A venue buys coverage by region, and the
// licence carries the tokens; this is where a token becomes a set of sports.
type Region string

const (
	RegionUS     Region = "us"
	RegionEU     Region = "eu"
	RegionAPAC   Region = "apac"
	RegionGlobal Region = "global"
)

// regionSports maps a bundle token onto the sports it unlocks.
//
// `global` is not a wildcard over everything the provider has; it is the union
// of the sold bundles. A wildcard would silently start polling a vertical the
// venue never paid for the day one is added to the catalog.
var regionSports = map[Region][]Sport{
	RegionUS:   {"nfl", "ncaaf", "ncaab", "nba", "ufc", "mma"},
	RegionEU:   {"soccer", "rugby", "f1"},
	RegionAPAC: {"afl", "rugby"},
}

func init() {
	seen := map[Sport]bool{}
	var all []Sport
	for _, region := range []Region{RegionUS, RegionEU, RegionAPAC} {
		for _, s := range regionSports[region] {
			if !seen[s] {
				seen[s] = true
				all = append(all, s)
			}
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	regionSports[RegionGlobal] = all
}

// SportsForRegion returns the sports a bundle token unlocks.
func SportsForRegion(r Region) ([]Sport, error) {
	s, ok := regionSports[Region(strings.ToLower(strings.TrimSpace(string(r))))]
	if !ok {
		return nil, fmt.Errorf("apisports: unknown region bundle %q", r)
	}
	return append([]Sport{}, s...), nil
}

// Regions lists the known bundle tokens.
func Regions() []Region {
	return []Region{RegionUS, RegionEU, RegionAPAC, RegionGlobal}
}

// Entitled resolves what a licence actually unlocks on API-Sports.
//
// Both claims are applied, and the SPORT claim is authoritative: a region
// widens nothing that the sports list does not already grant. A licence saying
// region "global" and sports ["nfl"] gets the NFL, not the world. Regions exist
// to describe a package; they must never be a second, looser path to content.
//
// Sports API-Sports cannot serve are dropped here rather than erroring, because
// a licence legitimately covers cricket and tennis — they are simply routed to
// a different provider.
func Entitled(licensedSports, licensedRegions []string) []Binding {
	allowed := map[Sport]bool{}
	for _, s := range licensedSports {
		allowed[Sport(strings.ToLower(strings.TrimSpace(s)))] = true
	}

	// A licence with regions but no usable sport claim gets nothing: an
	// omission must not widen entitlement.
	var out []Binding
	for _, sport := range Sports() {
		if !allowed[sport] {
			continue
		}
		if len(licensedRegions) > 0 && !inAnyRegion(sport, licensedRegions) {
			continue
		}
		if b, ok := BindingFor(sport); ok {
			out = append(out, b)
		}
	}
	return out
}

func inAnyRegion(sport Sport, regions []string) bool {
	for _, r := range regions {
		sports, err := SportsForRegion(Region(r))
		if err != nil {
			continue
		}
		for _, s := range sports {
			if s == sport {
				return true
			}
		}
	}
	return false
}

// VerticalsFor reduces bindings to the distinct hosts they touch, which is the
// unit the quota is metered in.
func VerticalsFor(bs []Binding) []Vertical {
	seen := map[Vertical]bool{}
	var out []Vertical
	for _, b := range bs {
		if !seen[b.Vertical] {
			seen[b.Vertical] = true
			out = append(out, b.Vertical)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
