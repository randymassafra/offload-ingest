package scope

import (
	"encoding/json"
	"strings"
	"testing"
)

// decode turns a JSON literal into the shape a payload reaches Normalize in.
func decode(t *testing.T, s string) any {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		t.Fatalf("fixture is not valid JSON: %v", err)
	}
	return v
}

// env normalises a payload into the envelope the validator reads.
func env(t *testing.T, sport, payload string) Envelope {
	t.Helper()
	id := Normalize(decode(t, payload))
	return Envelope{
		Sport: sport, LeagueID: id.LeagueID,
		OrgID: id.OrgID, LeagueName: id.LeagueName,
	}
}

// Soccer, licensed for the Premier League (39) and La Liga (140) only.
func soccerValidator() *Validator {
	return New([]AuthorizedScope{
		{Sport: "soccer", ID: 39, Source: "sport", Name: "Premier League"},
		{Sport: "soccer", ID: 140, Source: "sport", Name: "La Liga"},
	})
}

// TestOutOfScopeLeagueIsRefused is the control this package exists for: a bulk
// sweep returns the provider's whole card, and the card contains competitions
// the venue did not buy.
func TestOutOfScopeLeagueIsRefused(t *testing.T) {
	v := soccerValidator()

	// Premier League — licensed.
	res := v.Check(env(t, "soccer", `{"league":{"id":39,"name":"Premier League"}}`))
	if !res.Authorized {
		t.Errorf("Premier League was refused: %s", res.Detail)
	}

	// Serie A — not licensed.
	res = v.Check(env(t, "soccer", `{"league":{"id":135,"name":"Serie A"}}`))
	if res.Authorized {
		t.Fatal("Serie A was authorised on a Premier League / La Liga licence")
	}
	if res.Reason != ReasonOutOfScope {
		t.Errorf("reason = %q, want %q", res.Reason, ReasonOutOfScope)
	}
	// The message must be actionable: a bare id is unreviewable.
	if !strings.Contains(res.Detail, "Serie A") {
		t.Errorf("detail does not name the competition: %s", res.Detail)
	}
	if !strings.Contains(res.Detail, "Premier League") {
		t.Errorf("detail does not say what IS licensed: %s", res.Detail)
	}
}

// TestUnconstrainedSportAuthorizesEverything. Most verticals serve exactly one
// competition, and the sport itself was already checked upstream.
func TestUnconstrainedSportAuthorizesEverything(t *testing.T) {
	v := New(nil, "afl") // licensed, no league restriction
	res := v.Check(env(t, "afl", `{"league":{"id":999,"name":"Anything"}}`))
	if !res.Authorized {
		t.Errorf("an unconstrained sport refused a record: %s", res.Detail)
	}
	// Even with no identifiable league at all.
	if res := v.Check(env(t, "afl", `{"id":1}`)); !res.Authorized {
		t.Errorf("an unconstrained sport should not require an identity: %s", res.Detail)
	}
}

// TestUnidentifiedIsDistinctFromOutOfScope. One is a modelling gap on our side,
// the other is a licence mismatch. Collapsing them would make an extraction bug
// look like a compliance problem and send someone to the wrong place.
func TestUnidentifiedIsDistinctFromOutOfScope(t *testing.T) {
	v := soccerValidator()
	res := v.Check(env(t, "soccer", `{"id":123,"teams":{"home":{"name":"A"}}}`))
	if res.Authorized {
		t.Fatal("a constrained sport with no league should not be authorised")
	}
	if res.Reason != ReasonUnidentified {
		t.Errorf("reason = %q, want %q", res.Reason, ReasonUnidentified)
	}
}

// TestUnlicensedSportIsRefused catches a routing bug: the sweeper should never
// have polled a sport the licence does not cover.
func TestUnlicensedSportIsRefused(t *testing.T) {
	res := soccerValidator().Check(env(t, "cricket", `{"league":{"id":39}}`))
	if res.Authorized {
		t.Fatal("an unlicensed sport was authorised")
	}
	if res.Reason != ReasonUnlicensedSport {
		t.Errorf("reason = %q, want %q", res.Reason, ReasonUnlicensedSport)
	}
}

// TestNilValidatorAuthorizesEverything. Simulation has no upstream to be out of
// scope of, and a nil-check at every call site would be noise.
func TestNilValidatorAuthorizesEverything(t *testing.T) {
	var v *Validator
	if res := v.Check(env(t, "soccer", `{"league":{"id":135}}`)); !res.Authorized {
		t.Error("a nil validator must authorise everything")
	}
	if res := New(nil).Check(Envelope{Sport: "soccer"}); !res.Authorized {
		t.Error("an empty validator must authorise everything")
	}
}

// --- extraction --------------------------------------------------------------

// TestExtractHandlesEveryProviderShape. The providers do not agree on where the
// league lives, which is the whole reason this check is in a shared layer.
func TestExtractHandlesEveryProviderShape(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		league  int
		org     string
		country string
	}{
		{"api-sports games family",
			`{"id":1,"league":{"id":116,"name":"NCAA","season":2026},"country":{"name":"USA"}}`,
			116, "", "USA"},
		{"api-sports football family",
			`{"fixture":{"id":9,"status":{"short":"1H"}},"league":{"id":39,"name":"Premier League","country":"England"}}`,
			39, "", "England"},
		{"golf leaderboard",
			`{"orgId":"1","tournId":"033","year":"2023"}`,
			0, "1", ""},
		{"nested fixture",
			`{"fixture":{"league":{"id":140,"name":"La Liga"}}}`,
			140, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := Normalize(decode(t, tc.payload))
			if !id.Found {
				t.Fatalf("nothing extracted from %s", tc.payload)
			}
			if id.LeagueID != tc.league {
				t.Errorf("league = %d, want %d", id.LeagueID, tc.league)
			}
			if id.OrgID != tc.org {
				t.Errorf("org = %q, want %q", id.OrgID, tc.org)
			}
			if tc.country != "" && id.Country != tc.country {
				t.Errorf("country = %q, want %q", id.Country, tc.country)
			}
		})
	}
}

// TestExtractToleratesNumericFormatting. The golf provider taught us an upstream
// will send a number as a string or wrapped in MongoDB extended JSON; being
// strict here would turn a formatting change into a feed that drops everything.
func TestExtractToleratesNumericFormatting(t *testing.T) {
	for _, payload := range []string{
		`{"league":{"id":39}}`,
		`{"league":{"id":"39"}}`,
		`{"league":{"id":{"$numberInt":"39"}}}`,
	} {
		id := Normalize(decode(t, payload))
		if !id.Found || id.LeagueID != 39 {
			t.Errorf("%s gave league %d (found=%v), want 39", payload, id.LeagueID, id.Found)
		}
	}
}

func TestExtractOnJunkIsNotFound(t *testing.T) {
	for _, payload := range []any{nil, "a string", 42, []any{1, 2}} {
		if id := Normalize(payload); id.Found {
			t.Errorf("%v should not yield an identity", payload)
		}
	}
}

// TestExtractAcceptsATypedPayload, since production messages carry decoded
// structs as often as maps.
func TestExtractAcceptsATypedPayload(t *testing.T) {
	type league struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	type game struct {
		ID     int    `json:"id"`
		League league `json:"league"`
	}
	id := Normalize(game{ID: 7, League: league{ID: 253, Name: "MLS"}})
	if !id.Found || id.LeagueID != 253 || id.LeagueName != "MLS" {
		t.Errorf("typed payload gave %+v", id)
	}
}

// --- golf tours ---------------------------------------------------------------

// TestGolfIsScopedByTour: golf has no leagues, it has tours — PGA is "1" and
// LIV is "2", and a PGA-only licence must not receive a LIV leaderboard.
func TestGolfIsScopedByTour(t *testing.T) {
	v := New([]AuthorizedScope{{Sport: "golf", ID: 1, Source: "sport", Name: "PGA Tour"}})

	if res := v.Check(env(t, "golf", `{"orgId":"1","tournId":"033"}`)); !res.Authorized {
		t.Errorf("the PGA tour was refused on a PGA licence: %s", res.Detail)
	}
	res := v.Check(env(t, "golf", `{"orgId":"2","tournId":"007"}`))
	if res.Authorized {
		t.Fatal("LIV was authorised on a PGA-only licence")
	}
	if res.Reason != ReasonOutOfScope {
		t.Errorf("reason = %q", res.Reason)
	}
}

// --- the free-function form ---------------------------------------------------

func TestAuthorizeFreeFunction(t *testing.T) {
	list := []AuthorizedScope{
		{Sport: "soccer", ID: 39}, {Sport: "soccer", ID: 140},
	}
	ok, err := Authorize(list, env(t, "soccer", `{"league":{"id":39}}`))
	if !ok || err != nil {
		t.Errorf("licensed league refused: %v", err)
	}
	ok, err = Authorize(list, env(t, "soccer", `{"league":{"id":135}}`))
	if ok || err == nil {
		t.Error("unlicensed league authorised")
	}
	// An empty authorised set means unconstrained, not "deny all".
	if ok, _ := Authorize(nil, env(t, "soccer", `{"league":{"id":999}}`)); !ok {
		t.Error("an empty list should authorise everything")
	}
}

func TestResultErr(t *testing.T) {
	if err := (Result{Authorized: true}).Err(); err != nil {
		t.Errorf("an authorised result should have no error: %v", err)
	}
	err := (Result{Reason: ReasonOutOfScope, Detail: "nope"}).Err()
	if err == nil || !strings.Contains(err.Error(), "out_of_scope") {
		t.Errorf("got %v", err)
	}
}

// --- the qualified form ------------------------------------------------------

// TestSportQualifierPreventsCrossSportAuthorization is the reason the
// authorised list is qualified rather than a flat []int.
//
// API-Sports scopes league ids per host, and the licensed bindings collide
// directly: id 1 is the NFL on the american-football host and the AFL
// Premiership on the afl host; id 2 is NCAA football and the UEFA Champions
// League. A flat list would authorise each against the other's entitlement.
func TestSportQualifierPreventsCrossSportAuthorization(t *testing.T) {
	// A venue licensed for the NFL (league 1) and NCAA football (league 2).
	v := New([]AuthorizedScope{
		{Sport: "nfl", ID: 1, Source: "sport", Name: "NFL"},
		{Sport: "ncaaf", ID: 2, Source: "sport", Name: "NCAA"},
	})

	if res := v.Check(Envelope{Sport: "nfl", LeagueID: 1}); !res.Authorized {
		t.Errorf("the NFL was refused on an NFL licence: %s", res.Detail)
	}

	// The AFL Premiership is also league 1 — on a different host. It must not
	// be authorised by the NFL entitlement.
	res := v.Check(Envelope{Sport: "afl", LeagueID: 1, LeagueName: "AFL Premiership"})
	if res.Authorized {
		t.Fatal("AFL league 1 was authorised by the NFL's league 1 entitlement")
	}

	// The Champions League is league 2 on the football host; NCAA football is
	// league 2 on the american-football host.
	res = v.Check(Envelope{Sport: "soccer", LeagueID: 2, LeagueName: "UEFA Champions League"})
	if res.Authorized {
		t.Fatal("the Champions League was authorised by the NCAA's league 2 entitlement")
	}
}

// TestAggregateDeduplicatesAndOrders. A competition granted by both the sport
// claim and a regional bundle must appear once, and two runs of one licence
// must produce the same list.
func TestAggregateDeduplicatesAndOrders(t *testing.T) {
	got := Aggregate(
		[]AuthorizedScope{{Sport: "soccer", ID: 140}, {Sport: "nfl", ID: 1}},
		[]AuthorizedScope{{Sport: "soccer", ID: 39}, {Sport: "soccer", ID: 140}},
	)
	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3 after de-duplication: %v", len(got), got)
	}
	if got[0].Sport != "nfl" || got[1].ID != 39 || got[2].ID != 140 {
		t.Errorf("not in stable sport-then-id order: %v", got)
	}
}

// TestClaimSourceIsRecorded. An operator asking why a venue receives La Liga
// needs to know whether the sports list or a bundle granted it.
func TestClaimSourceIsRecorded(t *testing.T) {
	v := New([]AuthorizedScope{
		{Sport: "soccer", ID: 39, Source: "region:eu", Name: "Premier League"},
	})
	res := v.Check(Envelope{Sport: "soccer", LeagueID: 39})
	if !res.Authorized {
		t.Fatalf("refused: %s", res.Detail)
	}
	if res.Matched.Source != "region:eu" {
		t.Errorf("claim source = %q, want it preserved through the check", res.Matched.Source)
	}
}

// TestEnvelopeCarriesTourScopedSports: golf has tours, not leagues, and the
// sport qualifier keeps tour 1 from colliding with NFL league 1.
func TestEnvelopeCarriesTourScopedSports(t *testing.T) {
	v := New([]AuthorizedScope{
		{Sport: "golf", ID: 1, Source: "sport", Name: "PGA Tour"},
		{Sport: "nfl", ID: 1, Source: "sport", Name: "NFL"},
	})
	if res := v.Check(Envelope{Sport: "golf", OrgID: "1"}); !res.Authorized {
		t.Errorf("PGA refused: %s", res.Detail)
	}
	if res := v.Check(Envelope{Sport: "golf", OrgID: "2"}); res.Authorized {
		t.Error("LIV was authorised on a PGA-only licence")
	}
}
