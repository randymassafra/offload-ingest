// Package scope enforces a licence's league and region entitlement against the
// payloads that actually come back from a provider.
//
// # Why this is a separate control
//
// The pipeline already narrows what it ASKS for: apisports.Entitled resolves a
// licence's sport and region claims into the set of provider verticals that get
// polled at all, once, at startup. That is pre-fetch gating on our own side.
//
// This package is the post-fetch half. A bulk sweep returns whatever the
// provider decides to put in the response — the volleyball card for a given day
// includes every competition that host covers, not only the ones a venue paid
// for — so the request being in scope does not make the response in scope. Both
// controls are needed and neither implies the other.
//
// # Where it lives
//
// pkg/ingest/scope, not pkg/dds and not internal/pipeline.
//
// pkg/dds is the Dashboard Design System, shared with three other products and
// committed in TODO.md to having no dependency outside the standard library so
// it can be lifted into its own repository. A validator that imports licence
// and provider types would break that on the first commit.
//
// internal/pipeline would work, but pkg/ingest is where the runtime and the
// streamer already live, and a pkg/ package that imports internal/ becomes
// un-importable by any other module — which is the direction this suite is
// heading. Keeping the validator under pkg/ keeps that door open.
//
// # Fail direction
//
// A sport with no league constraint authorises everything: the sport itself was
// already checked upstream, and most verticals serve exactly one competition.
// A sport WITH a constraint whose payload carries no identifiable league is
// reported separately from one that carries a league outside the set — the
// first is a modelling gap on our side, the second is a licence mismatch, and
// collapsing them would make an extraction bug look like a compliance problem.
package scope

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Reason says why a record was refused.
type Reason string

const (
	// ReasonAuthorized is not a refusal; it is the zero reason.
	ReasonAuthorized Reason = ""
	// ReasonOutOfScope means the payload named a league or tour the licence
	// does not cover. This is a licence mismatch.
	ReasonOutOfScope Reason = "out_of_scope"
	// ReasonUnidentified means the sport is constrained but no league or tour
	// could be read from the payload. This is a modelling gap, not a licence
	// problem, and is counted separately so the two are never confused.
	ReasonUnidentified Reason = "unidentified"
	// ReasonUnlicensedSport means the payload arrived for a sport the licence
	// does not cover at all — a routing bug, since the sweeper should never
	// have polled it.
	ReasonUnlicensedSport Reason = "unlicensed_sport"
)

// Authorization is what a licence permits for one sport.
type Authorization struct {
	// Sport is the pipeline's sport token.
	Sport string
	// LeagueIDs are the provider league ids the licence covers. Empty means
	// the whole sport is authorised, which is correct for a vertical that
	// serves one competition.
	LeagueIDs []int
	// OrgIDs are tour identifiers, used where a provider scopes by tour rather
	// than league — golf's PGA ("1") and LIV ("2").
	OrgIDs []string
	// LeagueNames documents the ids for the operator-facing message. A drop
	// reported as "league 481 is not licensed" is unactionable; one naming the
	// competition is not.
	LeagueNames []string
}

// Result is the outcome of one check.
type Result struct {
	// Authorized is the answer.
	Authorized bool
	// Reason is empty when authorised.
	Reason Reason
	// Detail is a human-readable explanation, safe to log and to show an
	// operator. It never contains a credential.
	Detail string
	// Matched is the authorised entry that permitted the record, which records
	// whether a sport claim or a regional bundle granted it.
	Matched AuthorizedScope
}

// Err renders a refusal as an error, for callers that want one.
func (r Result) Err() error {
	if r.Authorized {
		return nil
	}
	return fmt.Errorf("scope: %s: %s", r.Reason, r.Detail)
}

// Identity is the scope-bearing part of a provider payload.
type Identity struct {
	LeagueID   int
	LeagueName string
	Country    string
	// OrgID is golf's tour identifier.
	OrgID string
	// Found is false when nothing scope-bearing could be read.
	Found bool
}

// String renders the identity for a log line.
func (i Identity) String() string {
	switch {
	case i.OrgID != "":
		return "tour " + i.OrgID
	case i.LeagueName != "":
		return fmt.Sprintf("%s (league %d)", i.LeagueName, i.LeagueID)
	case i.LeagueID != 0:
		return fmt.Sprintf("league %d", i.LeagueID)
	default:
		return "unidentified"
	}
}

// Validator checks records against a licence's entitlements.
//
// Safe for concurrent use: the authorised list is built once and never mutated,
// so every publish path can share one validator.
type Validator struct {
	// byScope is the flattened authorised list, indexed for lookup. The key is
	// sport/id, which is what makes a colliding league id across two sports
	// impossible to confuse.
	byScope map[string]AuthorizedScope
	// unconstrained holds sports the licence covers with no league restriction
	// — a vertical that serves exactly one competition, where the sport claim
	// alone is the entitlement.
	unconstrained map[string]bool
	// licensed is every sport the licence covers, constrained or not.
	licensed map[string]bool
	// names indexes a sport's licensed competition names for drop messages.
	names map[string][]string
}

// New builds a validator from a flattened authorised list.
//
// Sports named in `unconstrained` are authorised wholesale: the sport claim is
// the entitlement and no league check applies. That is correct for AFL, the NBA
// host and MMA, each of which serves a single competition.
func New(authorized []AuthorizedScope, unconstrained ...string) *Validator {
	v := &Validator{
		byScope:       make(map[string]AuthorizedScope, len(authorized)),
		unconstrained: map[string]bool{},
		licensed:      map[string]bool{},
		names:         map[string][]string{},
	}
	for _, a := range authorized {
		sport := normalize(a.Sport)
		a.Sport = sport
		v.byScope[key(sport, a.ID)] = a
		v.licensed[sport] = true
		if a.Name != "" {
			v.names[sport] = append(v.names[sport], a.Name)
		}
	}
	for _, s := range unconstrained {
		s = normalize(s)
		v.unconstrained[s] = true
		v.licensed[s] = true
	}
	return v
}

func key(sport string, id int) string { return sport + "/" + strconv.Itoa(id) }

// Authorized returns the flattened list, for audit and for the dashboard.
func (v *Validator) Authorized() []AuthorizedScope {
	if v == nil {
		return nil
	}
	out := make([]AuthorizedScope, 0, len(v.byScope))
	for _, a := range v.byScope {
		out = append(out, a)
	}
	return Aggregate(out)
}

// Sports lists every licensed sport, sorted.
func (v *Validator) Sports() []string {
	if v == nil {
		return nil
	}
	out := make([]string, 0, len(v.licensed))
	for s := range v.licensed {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// Check validates one normalised envelope against the licence.
//
// A nil validator authorises everything. That is deliberate: simulation runs
// and unlicensed development builds have no entitlement to check against, and a
// nil-check at every call site would be noise.
func (v *Validator) Check(e Envelope) Result {
	if v == nil || len(v.licensed) == 0 {
		return Result{Authorized: true}
	}
	sport := normalize(e.Sport)
	if !v.licensed[sport] {
		return Result{
			Reason: ReasonUnlicensedSport,
			Detail: fmt.Sprintf("%s is not covered by this licence", e.Sport),
		}
	}
	// The sport is entitled and the vertical serves one competition.
	if v.unconstrained[sport] {
		return Result{Authorized: true}
	}

	id, ok := e.scopeID()
	if !ok {
		return Result{
			Reason: ReasonUnidentified,
			Detail: fmt.Sprintf("%s is league-restricted but the record carries no league or tour", e.Sport),
		}
	}
	if a, found := v.byScope[key(sport, id)]; found {
		return Result{Authorized: true, Matched: a}
	}
	return Result{
		Reason: ReasonOutOfScope,
		Detail: fmt.Sprintf("%s %s is not licensed (licensed: %s)",
			e.Sport, e.describe(), v.describeLicensed(sport)),
	}
}

// describeLicensed renders a sport's licensed competitions for an operator.
func (v *Validator) describeLicensed(sport string) string {
	if names := v.names[sport]; len(names) > 0 {
		return strings.Join(names, ", ")
	}
	var ids []string
	for k, a := range v.byScope {
		if strings.HasPrefix(k, sport+"/") {
			ids = append(ids, strconv.Itoa(a.ID))
		}
	}
	sort.Strings(ids)
	return strings.Join(ids, ", ")
}

// Authorize is the free-function form: given the authorised list and a
// normalised envelope, report whether the record may be processed.
//
// Provided because a caller holding a bare list should not have to build a
// Validator to ask one question. An empty list authorises everything, matching
// the nil-validator behaviour.
func Authorize(authorized []AuthorizedScope, e Envelope) (bool, error) {
	if len(authorized) == 0 {
		return true, nil
	}
	sport := normalize(e.Sport)
	id, ok := e.scopeID()
	if !ok {
		return false, fmt.Errorf("scope: %s: record carries no league identity", ReasonUnidentified)
	}
	for _, a := range authorized {
		if normalize(a.Sport) == sport && a.ID == id {
			return true, nil
		}
	}
	return false, fmt.Errorf("scope: %s: %s %s is not in the authorised list",
		ReasonOutOfScope, e.Sport, e.describe())
}

func normalize(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
