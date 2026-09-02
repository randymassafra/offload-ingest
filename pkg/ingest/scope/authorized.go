package scope

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// AuthorizedScope is one competition a licence unlocks.
//
// # Why the sport qualifier is not optional
//
// API-Sports scopes league ids PER HOST, not globally. Checking the licensed
// bindings turns up direct collisions:
//
//	id 1 — NFL (american-football) and AFL Premiership (afl)
//	id 2 — NCAA football (american-football) and UEFA Champions League (football)
//
// A flat []int of authorised ids would therefore authorise an AFL fixture
// against an NFL entitlement and a Champions League fixture against an NCAA
// football one — returning IsAuthorized for content the venue never bought,
// which is the exact failure this control exists to prevent. The Sport field is
// what makes the list safe to flatten.
//
// Golf is carried in the same shape: its tours are numbered ("1" PGA, "2" LIV)
// and qualified by Sport "golf", so tour 1 never collides with NFL league 1.
type AuthorizedScope struct {
	// Sport is the pipeline's sport token, lowercase.
	Sport string `json:"sport"`
	// ID is the provider's league id, or tour id for a tour-scoped sport.
	ID int `json:"id"`
	// Source records which claim granted this entry — "sport" when the licence
	// named the sport directly, "region:eu" when a regional bundle did.
	//
	// Kept because an operator asking "why is this venue receiving La Liga"
	// needs to know whether it came from the sports list or a bundle, and
	// because a licence audit that cannot answer that is not an audit.
	Source string `json:"source"`
	// Name documents the id. A drop reported as "league 481 is not licensed"
	// is unactionable; one naming the competition is not.
	Name string `json:"name,omitempty"`
}

// String renders the entry for a log line or an audit.
func (a AuthorizedScope) String() string {
	label := a.Name
	if label == "" {
		label = "league " + strconv.Itoa(a.ID)
	}
	return fmt.Sprintf("%s/%s (%s, via %s)", a.Sport, label, strconv.Itoa(a.ID), a.Source)
}

// Envelope is the normalised, scope-bearing metadata the pipeline attaches to
// every record.
//
// The validator takes this rather than the raw payload. Normalisation happens
// once, where a provider's data enters the pipeline, and the payload itself is
// never touched — the Kafka value stays the provider's document verbatim, which
// is a contract the schema comparison enforces.
type Envelope struct {
	// Sport is the pipeline sport token.
	Sport string
	// LeagueID is the normalised competition id, 0 when the payload carried
	// none.
	LeagueID int
	// OrgID is the provider's tour identifier, for tour-scoped sports.
	OrgID string
	// LeagueName is carried for the operator-facing drop message.
	LeagueName string
	// FixtureID identifies the record, for logs.
	FixtureID string
}

// identified reports whether the envelope carries anything to match on.
func (e Envelope) identified() bool { return e.LeagueID != 0 || strings.TrimSpace(e.OrgID) != "" }

// scopeID resolves the id to match against the authorised list.
//
// A tour-scoped sport carries its tour in OrgID as a string; it is numeric on
// every provider seen, and the Sport qualifier keeps it from colliding with a
// league id of the same value on another sport.
func (e Envelope) scopeID() (int, bool) {
	if e.LeagueID != 0 {
		return e.LeagueID, true
	}
	if org := strings.TrimSpace(e.OrgID); org != "" {
		if n, err := strconv.Atoi(org); err == nil {
			return n, true
		}
	}
	return 0, false
}

// describe renders the envelope for a drop message.
func (e Envelope) describe() string {
	switch {
	case e.LeagueName != "" && e.LeagueID != 0:
		return fmt.Sprintf("%s (league %d)", e.LeagueName, e.LeagueID)
	case e.LeagueID != 0:
		return fmt.Sprintf("league %d", e.LeagueID)
	case e.OrgID != "":
		return "tour " + e.OrgID
	default:
		return "unidentified"
	}
}

// Aggregate flattens per-sport entitlements into one authorised list,
// de-duplicating entries granted by both a sport claim and a regional bundle.
//
// Order is stable so two runs of the same licence produce the same list, which
// matters for anything that logs or diffs it.
func Aggregate(scopes ...[]AuthorizedScope) []AuthorizedScope {
	seen := map[string]bool{}
	var out []AuthorizedScope
	for _, group := range scopes {
		for _, s := range group {
			s.Sport = strings.ToLower(strings.TrimSpace(s.Sport))
			key := s.Sport + "/" + strconv.Itoa(s.ID)
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sport != out[j].Sport {
			return out[i].Sport < out[j].Sport
		}
		return out[i].ID < out[j].ID
	})
	return out
}
