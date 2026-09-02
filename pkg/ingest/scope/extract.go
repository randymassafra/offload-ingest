package scope

import (
	"encoding/json"
	"strconv"
	"strings"
)

// Normalize reads the scope-bearing identity out of a raw provider payload and
// returns it as envelope metadata.
//
// This is the normalisation step: it runs ONCE, where a provider's record
// enters the pipeline, and its output is attached to the message envelope. The
// payload itself is never modified — the Kafka value stays the provider's
// document byte for byte, which the schema comparison enforces.
//
// The providers do not agree on where this lives, and the whole point of doing
// the check in a shared layer is that the providers stay dumb about it:
//
//	API-Sports, football family   fixture{}, then league.id at the top level
//	API-Sports, games family      league.id, country.name at the top level
//	live-golf-data                orgId ("1" PGA, "2" LIV), no league at all
//	Cricbuzz / AllScores          neither; those sports carry no league
//	                              constraint, so extraction is never reached
//
// Values are read defensively. A league id has been seen as a JSON number
// everywhere so far, but the golf provider taught us that an upstream will
// happily send a number as a string — or wrapped in MongoDB extended JSON — so
// every numeric read here accepts all three forms. Being liberal costs nothing;
// being strict would turn one upstream formatting change into a feed that drops
// every record as unidentified.
func Normalize(payload any) Identity {
	doc, ok := asMap(payload)
	if !ok {
		return Identity{}
	}

	var id Identity

	// Golf: a leaderboard is scoped by tour, not league.
	if org := readString(doc["orgId"]); org != "" {
		id.OrgID = org
		id.Found = true
	}

	// API-Sports: league lives at the top level in both document families.
	// The football family nests fixture{} but keeps league{} beside it, so one
	// lookup covers both.
	if league, ok := asMap(doc["league"]); ok {
		if n, ok := readInt(league["id"]); ok {
			id.LeagueID = n
			id.Found = true
		}
		id.LeagueName = readString(league["name"])
		// The football family carries the country inside league{}; the games
		// family has it as a sibling.
		id.Country = readString(league["country"])
	}
	if id.Country == "" {
		if country, ok := asMap(doc["country"]); ok {
			id.Country = readString(country["name"])
		}
	}

	// Some documents wrap the fixture one level down. Checked last so a
	// top-level league always wins.
	if !id.Found {
		for _, key := range []string{"fixture", "game", "response"} {
			nested, ok := asMap(doc[key])
			if !ok {
				continue
			}
			if league, ok := asMap(nested["league"]); ok {
				if n, ok := readInt(league["id"]); ok {
					id.LeagueID = n
					id.LeagueName = readString(league["name"])
					id.Found = true
					break
				}
			}
		}
	}
	return id
}

// asMap accepts a decoded JSON object in either of the two forms a payload
// reaches this package in: a map from json.Unmarshal into any, or a typed
// struct that has to be round-tripped.
func asMap(v any) (map[string]any, bool) {
	switch t := v.(type) {
	case nil:
		return nil, false
	case map[string]any:
		return t, true
	case json.RawMessage:
		var m map[string]any
		if err := json.Unmarshal(t, &m); err != nil {
			return nil, false
		}
		return m, true
	}
	// A typed payload. Round-tripping is not free, but it happens once per
	// record on a feed that publishes tens per minute, and the alternative is
	// an interface every provider model would have to implement — which is
	// exactly the coupling this layer exists to avoid.
	b, err := json.Marshal(v)
	if err != nil {
		return nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, false
	}
	return m, true
}

// readInt accepts a number, a numeric string, or MongoDB extended JSON.
func readInt(v any) (int, bool) {
	switch t := v.(type) {
	case float64:
		return int(t), true
	case int:
		return t, true
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return int(n), true
		}
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(t)); err == nil {
			return n, true
		}
	case map[string]any:
		// {"$numberInt": "18"} and friends — see internal/provider/golf.
		for _, key := range []string{"$numberInt", "$numberLong", "$numberDouble"} {
			if raw, ok := t[key].(string); ok {
				if f, err := strconv.ParseFloat(raw, 64); err == nil {
					return int(f), true
				}
			}
		}
	}
	return 0, false
}

// readString accepts a string or a number rendered as one.
func readString(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		return strconv.Itoa(int(t))
	case json.Number:
		return t.String()
	}
	return ""
}
