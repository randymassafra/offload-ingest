package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

// binding ties one registered endpoint to the captured response that proves it,
// and says how to project the capture down to what the feed actually emits.
type binding struct {
	sport generators.Sport
	ref   string // endpoint Ref(): "kind" or "kind:name"
	file  string // path under the fixtures root
	// unwrap navigates into the captured document before comparing. Several
	// endpoints return a single-element array wrapping the real payload, and
	// several feeds carry a projection out of a larger document.
	unwrap func(any) any
}

func firstElem(doc any) any {
	if arr, ok := doc.([]any); ok && len(arr) == 1 {
		return arr[0]
	}
	return doc
}

// field navigates into a named field, unwrapping a single-element array first.
func field(name string) func(any) any {
	return func(doc any) any {
		if obj, ok := firstElem(doc).(map[string]any); ok {
			return obj[name]
		}
		return doc
	}
}

// mongo collapses MongoDB extended-JSON wrappers before applying inner.
//
// live-golf-data serves documents straight out of MongoDB, so an integer
// arrives as {"$numberInt":"18"} and a date as {"$date":{"$numberLong":"..."}}.
// The golf provider unwraps both on the way out — deliberately, and documented
// on golf.MongoInt.MarshalJSON — so comparing our payload against the raw
// capture reports $numberInt and $date as permanently missing fields.
//
// That is a false gap, and a permanent one is worse than none: three feeds
// stuck at 75% teach a reader to skip the golf rows, which is exactly where a
// real schema change would then hide. Collapsing the wrappers here compares
// the capture as we normalise it, so a GAP on golf means what it means
// everywhere else.
func mongo(inner func(any) any) func(any) any {
	var collapse func(any) any
	collapse = func(v any) any {
		switch node := v.(type) {
		case map[string]any:
			// A wrapper is an object whose only key is an extended-JSON tag.
			if len(node) == 1 {
				for _, tag := range []string{"$numberInt", "$numberLong", "$numberDouble", "$date"} {
					if inner, ok := node[tag]; ok {
						return collapse(inner)
					}
				}
			}
			out := make(map[string]any, len(node))
			for k, child := range node {
				out[k] = collapse(child)
			}
			return out
		case []any:
			out := make([]any, len(node))
			for i, child := range node {
				out[i] = collapse(child)
			}
			return out
		}
		return v
	}
	return func(doc any) any {
		doc = collapse(doc)
		if inner != nil {
			doc = inner(doc)
		}
		return doc
	}
}

// elemOf takes the first element of a named array, for feeds that carry one
// record rather than the whole collection.
func elemOf(name string) func(any) any {
	return func(doc any) any {
		if obj, ok := firstElem(doc).(map[string]any); ok {
			if arr, ok := obj[name].([]any); ok && len(arr) > 0 {
				return arr[0]
			}
		}
		return doc
	}
}

// nested walks a chain of array-or-object steps, taking the first element of
// each array. Used for feeds that carry a record buried inside a document, such
// as a golf hole inside the leaderboard.
func nested(names ...string) func(any) any {
	return func(doc any) any {
		cur := firstElem(doc)
		for _, n := range names {
			obj, ok := cur.(map[string]any)
			if !ok {
				return cur
			}
			cur = obj[n]
			if arr, ok := cur.([]any); ok {
				if len(arr) == 0 {
					return cur
				}
				cur = arr[0]
			}
		}
		return cur
	}
}

// nestedArray descends like nested but stops at the final collection instead of
// taking its first element, for feeds that emit the whole array.
func nestedArray(names ...string) func(any) any {
	return func(doc any) any {
		cur := firstElem(doc)
		for i, n := range names {
			obj, ok := cur.(map[string]any)
			if !ok {
				return cur
			}
			cur = obj[n]
			// Descend into intermediate arrays, but leave the last one intact.
			if arr, ok := cur.([]any); ok && i < len(names)-1 {
				if len(arr) == 0 {
					return cur
				}
				cur = arr[0]
			}
		}
		return cur
	}
}

// mergeArrays folds one representative element from each named array into a
// single object. The soccer timeline feed emits Goals, Bookings and Lineups on
// the same endpoint, one record per message, so the shape it must cover is the
// union of all three record types — not a list containing them.
func mergeArrays(names ...string) func(any) any {
	return func(doc any) any {
		obj, ok := firstElem(doc).(map[string]any)
		if !ok {
			return doc
		}
		merged := map[string]any{}
		for _, n := range names {
			arr, ok := obj[n].([]any)
			if !ok || len(arr) == 0 {
				continue
			}
			rec, ok := arr[0].(map[string]any)
			if !ok {
				continue
			}
			for k, v := range rec {
				if _, exists := merged[k]; !exists || v != nil {
					merged[k] = v
				}
			}
		}
		return merged
	}
}

// bindings maps every endpoint that has a capture to prove it. An endpoint
// absent from this table is reported as unverified rather than skipped
// silently.
// bindings maps every endpoint that has a capture to prove it. An endpoint
// absent from this table is reported as unverified rather than skipped
// silently.
//
// The table shrank sharply when the pipeline consolidated on API-Sports: the
// primary provider returns one document family per vertical, so a sport needs
// one binding rather than one per feed kind.
var bindings = []binding{
	// API-Sports. The box score is a single fixture out of the bulk response;
	// the telemetry feed carries the whole envelope, which is what the sweeper
	// actually receives.
	{"soccer", "boxscore", "apisports/football.json", elemOf("response")},
	{"soccer", "telemetry", "apisports/football.json", nil},
	{"ncaab", "boxscore", "apisports/basketball.json", elemOf("response")},
	{"ncaab", "telemetry", "apisports/basketball.json", nil},
	{"f1", "boxscore", "apisports/formula-1.json", elemOf("response")},
	{"f1", "telemetry", "apisports/formula-1.json", nil},

	// Golf comes from live-golf-data via RapidAPI. API-Sports has no golf host.
	{"golf", "boxscore", "golfdata/leaderboard.json", mongo(nil)},
	{"golf", "playerstats", "golfdata/leaderboard.json", mongo(field("leaderboardRows"))},
	{"golf", "telemetry", "golfdata/leaderboard.json", mongo(elemOf("leaderboardRows"))},

	// Cricket comes from Cricbuzz, a different provider with a different
	// shape — the captures sit under their own root.
	{"cricket", "boxscore", "cricbuzz/scorecard.json", nil},
	{"cricket", "playerstats", "cricbuzz/scorecard.json", nestedArray("scorecard", "batsman")},
	{"cricket", "telemetry", "cricbuzz/scorecard.json", nested("scorecard", "fow", "fow")},

	// Tennis comes from AllScores.
	{"tennis", "boxscore", "allscores/tennis_game_details.json", allscoresGame()},
	{"tennis", "playerstats", "allscores/tennis_game_details.json", competitorPair()},
	{"tennis", "telemetry", "allscores/tennis_game_details.json", nested("game", "stages")},
}

// allscoresGame narrows a captured AllScores match envelope to what the feed
// actually carries. It serves both tennis and soccer — the two sports share
// the game-details document.
//
// Two deliberate exclusions, both recorded rather than silent:
//
//   - sports / countries / competitions are reference lookup tables bundled
//     into every response; the feed carries the match document.
//   - game.promotedPredictions is a bookmaker promo. Betting content is out of
//     scope for this pipeline, so it is dropped from the model and from this
//     side of the comparison too — otherwise it would show as a permanent gap.
func allscoresGame() func(any) any {
	return func(doc any) any {
		obj, ok := firstElem(doc).(map[string]any)
		if !ok {
			return doc
		}
		out := map[string]any{}
		for _, n := range []string{"lastUpdateId", "requestedUpdateId", "ttl"} {
			if v, ok := obj[n]; ok {
				out[n] = v
			}
		}
		game, ok := obj["game"].(map[string]any)
		if !ok {
			out["game"] = obj["game"]
			return out
		}
		trimmed := map[string]any{}
		for k, v := range game {
			if k == "promotedPredictions" {
				continue
			}
			if k == "topPerformers" {
				v = stripRelatedLines(v)
			}
			trimmed[k] = v
		}
		out["game"] = trimmed
		return out
	}
}

// competitorPair returns the two competitors as the array the player-stats
// feed emits.
func competitorPair() func(any) any {
	return func(doc any) any {
		g, ok := navigate(doc, "game").(map[string]any)
		if !ok {
			return doc
		}
		return []any{g["homeCompetitor"], g["awayCompetitor"]}
	}
}

// load reads and parses a capture.
func load(root, rel string) (any, error) {
	raw, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		return nil, err
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", rel, err)
	}
	return doc, nil
}

// bindingFor returns the capture bound to an endpoint, if any.
func bindingFor(sport generators.Sport, ref string) (binding, bool) {
	for _, b := range bindings {
		if b.sport == sport && b.ref == ref {
			return b, true
		}
	}
	return binding{}, false
}

// stripRelatedLines drops the bookmaker odds the provider hangs off each top
// performer. Same reasoning as promotedPredictions: betting content is out of
// scope, so it is excluded from the model and from this side of the comparison
// rather than showing up forever as a gap.
func stripRelatedLines(v any) any {
	tp, ok := v.(map[string]any)
	if !ok {
		return v
	}
	cats, ok := tp["categories"].([]any)
	if !ok {
		return v
	}
	outCats := make([]any, 0, len(cats))
	for _, c := range cats {
		cm, ok := c.(map[string]any)
		if !ok {
			outCats = append(outCats, c)
			continue
		}
		trimmed := map[string]any{}
		for k, cv := range cm {
			if k == "homePlayer" || k == "awayPlayer" {
				if pm, ok := cv.(map[string]any); ok {
					player := map[string]any{}
					for pk, pv := range pm {
						if pk == "relatedLines" {
							continue
						}
						player[pk] = pv
					}
					cv = player
				}
			}
			trimmed[k] = cv
		}
		outCats = append(outCats, trimmed)
	}
	return map[string]any{"categories": outCats}
}

// soccerLineupMembers folds both teamsheets into one list, because the player
// feed emits every squad member from both sides on the same endpoint.
func soccerLineupMembers() func(any) any {
	return func(doc any) any {
		obj, ok := firstElem(doc).(map[string]any)
		if !ok {
			return doc
		}
		game, ok := obj["game"].(map[string]any)
		if !ok {
			return doc
		}
		out := []any{}
		for _, side := range []string{"homeCompetitor", "awayCompetitor"} {
			c, ok := game[side].(map[string]any)
			if !ok {
				continue
			}
			lu, ok := c["lineups"].(map[string]any)
			if !ok {
				continue
			}
			if members, ok := lu["members"].([]any); ok {
				out = append(out, members...)
			}
		}
		return out
	}
}

// scoreboard narrows the captured multi-league board to what the feed carries.
//
// DELIBERATELY EXCLUDED: bookmakers, and the odds block on each game. Betting
// content is out of scope for this pipeline, so it is dropped from the model
// and from this side of the comparison too — see stripRelatedLines.
func scoreboard() func(any) any {
	return func(doc any) any {
		obj, ok := firstElem(doc).(map[string]any)
		if !ok {
			return doc
		}
		out := map[string]any{}
		for k, v := range obj {
			if k == "bookmakers" {
				continue
			}
			if k == "games" {
				v = stripOdds(v)
			}
			out[k] = v
		}
		return out
	}
}

func stripOdds(v any) any {
	games, ok := v.([]any)
	if !ok {
		return v
	}
	out := make([]any, 0, len(games))
	for _, g := range games {
		gm, ok := g.(map[string]any)
		if !ok {
			out = append(out, g)
			continue
		}
		trimmed := map[string]any{}
		for k, gv := range gm {
			if k == "odds" {
				continue
			}
			trimmed[k] = gv
		}
		out = append(out, trimmed)
	}
	return out
}

// unionOf walks to an array and folds every element into one object.
//
// A feed that emits one record per message off a heterogeneous array — the
// soccer timeline sends goals, cards, substitutions and woodwork down the same
// endpoint — must cover the union of those record shapes, not the first one it
// happens to find. Taking element zero would compare against whichever incident
// opened the captured match and call every other field a gap.
func unionOf(names ...string) func(any) any {
	return func(doc any) any {
		cur := firstElem(doc)
		for _, n := range names {
			obj, ok := cur.(map[string]any)
			if !ok {
				return cur
			}
			cur = obj[n]
		}
		arr, ok := cur.([]any)
		if !ok {
			return cur
		}
		merged := map[string]any{}
		for _, el := range arr {
			rec, ok := el.(map[string]any)
			if !ok {
				continue
			}
			deepMerge(merged, rec)
		}
		return merged
	}
}

// deepMerge folds src into dst, descending into nested objects rather than
// letting the first record's sub-document win. A soccer timeline's eventType is
// the case that forces this: only a goal carries subTypeName, so a shallow
// merge that took the first event's eventType whole would hide it.
func deepMerge(dst, src map[string]any) {
	for k, v := range src {
		old, seen := dst[k]
		if !seen || old == nil {
			dst[k] = v
			continue
		}
		om, ok1 := old.(map[string]any)
		nm, ok2 := v.(map[string]any)
		if ok1 && ok2 {
			deepMerge(om, nm)
		}
	}
}
