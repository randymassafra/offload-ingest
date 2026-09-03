package generators

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fixtureRoot holds the captured provider responses the provenance check reads.
// fixtureRoot holds the captured provider responses, one directory per
// provider. The layout is provider-scoped because the pipeline is not
// single-vendor: each provider's captures sit under its own directory.
const fixtureRoot = "../../fixtures"

// capturedSports reports which sports this repository holds evidence for,
// across every provider.
func capturedSports(t *testing.T) map[Sport]bool {
	t.Helper()
	out := map[Sport]bool{}

	// Golf: live-golf-data, one capture holding the whole document.
	if entries, err := os.ReadDir(filepath.Join(fixtureRoot, "golfdata")); err == nil {
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".json") {
				out[SportGolf] = true
				break
			}
		}
	}

	// Cricbuzz serves exactly one sport here, so any capture under its
	// directory is evidence for that sport.
	for provider, sport := range map[Provider]Sport{
		ProviderCricbuzz: SportCricket,
	} {
		files, err := os.ReadDir(filepath.Join(fixtureRoot, string(provider)))
		if err != nil {
			continue
		}
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".json") {
				out[sport] = true
				break
			}
		}
	}

	// API-Sports: a capture is evidence only if the response actually carried
	// fixtures. An out-of-season vertical returns a valid but empty card, and
	// an empty card proves the endpoint works — not the document shape. Those
	// feeds are registered modeled, and this is what keeps that honest.
	if files, err := os.ReadDir(filepath.Join(fixtureRoot, string(ProviderAPISports))); err == nil {
		for _, f := range files {
			name := f.Name()
			if !strings.HasSuffix(name, ".json") {
				continue
			}
			vertical := strings.TrimSuffix(name, ".json")
			if !apiSportsCaptureHasResults(filepath.Join(fixtureRoot, string(ProviderAPISports), name)) {
				continue
			}
			for _, b := range apiSportsCatalog {
				v := b.vertical
				if v == "f1" {
					v = "formula-1"
				}
				if v == vertical {
					out[b.sport] = true
				}
			}
		}
	}

	// AllScores serves two sports, so a capture there proves only the sport
	// named in its filename. Crediting the whole provider would let a tennis
	// capture stand in as evidence for soccer.
	if files, err := os.ReadDir(filepath.Join(fixtureRoot, string(ProviderAllScores))); err == nil {
		for _, f := range files {
			name := f.Name()
			if !strings.HasSuffix(name, ".json") {
				continue
			}
			prefix, _, ok := strings.Cut(name, "_")
			if !ok {
				continue
			}
			if sport, err := ParseSport(prefix); err == nil {
				out[sport] = true
			}
		}
	}
	return out
}

// TestEveryEndpointRenders is the smoke test for the whole catalog: every
// registered (sport, kind) must produce a marshalable payload from a cold start
// and after a full fixture has run to completion and rolled over.
func TestEveryEndpointRenders(t *testing.T) {
	for _, ep := range Endpoints() {
		f, err := NewNamed(ep.Sport, ep.Kind, ep.Name, 11)
		if err != nil {
			t.Fatalf("%s: %v", ep, err)
		}
		for j := 0; j < 250; j++ {
			m := f.Next()
			b, err := json.Marshal(m.Payload)
			if err != nil {
				t.Fatalf("%s at %d: marshal: %v", ep, j, err)
			}
			if len(b) < 2 {
				t.Fatalf("%s at %d: empty payload", ep, j)
			}
		}
	}
}

// TestAllSportsCovered guards against a sport losing its registration.
func TestAllSportsCovered(t *testing.T) {
	for _, s := range AllSports {
		kinds := KindsFor(s)
		if len(kinds) == 0 {
			t.Errorf("%s has no registered feeds", s)
		}
		// Every sport must expose a whole-event snapshot. The other kinds vary
		// by provider, and on API-Sports that variation is a budget decision
		// rather than a gap: per-player statistics need a /players call per
		// fixture, which is exactly the per-game loop the bulk sweep replaced.
		// A venue on 100 requests a day cannot afford both, so those sports
		// carry boxscore and telemetry only.
		var hasBox bool
		for _, k := range kinds {
			hasBox = hasBox || k == FeedBoxScore
		}
		if !hasBox {
			t.Errorf("%s has no boxscore feed", s)
		}
	}
}

// TestPerPlayerStatsOnlyWhereAffordable pins the trade-off above, so that a
// later change adding per-fixture player calls to an API-Sports sport has to
// argue with a test rather than quietly multiply the request bill.
func TestPerPlayerStatsOnlyWhereAffordable(t *testing.T) {
	for _, ep := range Endpoints() {
		if ep.Kind != FeedPlayerStats {
			continue
		}
		if ep.Provider == ProviderAPISports {
			t.Errorf("%s/%s is a per-player feed on API-Sports; that costs one "+
				"request per fixture and defeats the bulk sweep", ep.Sport, ep.Ref())
		}
	}
}

// TestMessageRouting checks that a consumer can route and key any message
// without opening the payload.
func TestMessageRouting(t *testing.T) {
	for _, ep := range Endpoints() {
		f, _ := NewNamed(ep.Sport, ep.Kind, ep.Name, 3)
		m := f.Next()
		switch {
		case m.Sport != ep.Sport:
			t.Errorf("%s: sport = %s", ep, m.Sport)
		case m.Kind != ep.Kind:
			t.Errorf("%s: kind = %s", ep, m.Kind)
		case m.Endpoint != ep.Path:
			t.Errorf("%s: endpoint = %s", ep, m.Endpoint)
		case m.Model == "":
			t.Errorf("%s: empty model", ep)
		case m.FixtureID == "":
			t.Errorf("%s: empty fixture id", ep)
		case len(m.Key()) == 0:
			t.Errorf("%s: empty partition key", ep)
		case m.Emitted.IsZero():
			t.Errorf("%s: zero emitted timestamp", ep)
		}
	}
}

// TestSequenceIsMonotonicPerFixture is the ordering guarantee downstream gap
// detection depends on. The sequence is scoped to the fixture, matching the
// provider's own per-game numbering, so it restarts at 1 on rollover — but it
// must never skip or repeat within one fixture.
func TestSequenceIsMonotonicPerFixture(t *testing.T) {
	for _, ep := range Endpoints() {
		f, _ := NewNamed(ep.Sport, ep.Kind, ep.Name, 7)
		var last int64
		fixture := f.FixtureID()
		for j := 0; j < 400; j++ {
			m := f.Next()
			if m.FixtureID != fixture {
				// Rolled into a new fixture: the counter restarts at 1.
				if m.Sequence != 1 {
					t.Fatalf("%s: new fixture %s started at sequence %d, want 1", ep, m.FixtureID, m.Sequence)
				}
				fixture, last = m.FixtureID, m.Sequence
				continue
			}
			if m.Sequence != last+1 {
				t.Fatalf("%s: sequence jumped %d -> %d within fixture %s", ep, last, m.Sequence, fixture)
			}
			last = m.Sequence
		}
	}
}

// TestFixtureRollover checks that a finished fixture restarts cleanly rather
// than going silent — a round-the-clock load test must never dry up.
func TestFixtureRollover(t *testing.T) {
	for _, ep := range Endpoints() {
		f, _ := NewNamed(ep.Sport, ep.Kind, ep.Name, 5)
		first := f.FixtureID()
		var rolled bool
		for j := 0; j < 4000 && !rolled; j++ {
			f.Next()
			if f.FixtureID() != first {
				rolled = true
			}
		}
		if !rolled {
			t.Errorf("%s: fixture %s never rolled over in 4000 ticks", ep, first)
		}
	}
}

// TestDeterministicSeed keeps load-test runs reproducible: with the clock
// pinned, the same seed must replay the same payloads byte for byte. That is
// what makes a regression in a Flink job attributable to the job rather than
// to the generator.
//
// The clock has to be pinned for this to mean anything. Timestamps come from
// the wall clock and are formatted to second precision, so two feeds advanced
// in lockstep produce different bytes whenever their renders straddle a second
// boundary — rare, but real, and it made this test flaky roughly one run in
// three under the race detector before the clock was fixed here. The
// simulation is deterministic; the timestamps deliberately are not, because
// downstream event-time windowing needs real wall-clock values.
func TestDeterministicSeed(t *testing.T) {
	restore := now
	fixed := time.Date(2026, 3, 14, 15, 9, 26, 0, time.UTC)
	now = func() time.Time { return fixed }
	defer func() { now = restore }()

	for _, ep := range Endpoints() {
		a, _ := NewNamed(ep.Sport, ep.Kind, ep.Name, 99)
		b, _ := NewNamed(ep.Sport, ep.Kind, ep.Name, 99)
		for j := 0; j < 60; j++ {
			ma, mb := a.Next(), b.Next()
			ja, _ := json.Marshal(ma.Payload)
			jb, _ := json.Marshal(mb.Payload)
			if string(ja) != string(jb) {
				t.Fatalf("%s: payload diverged at tick %d\n a=%.400s\n b=%.400s", ep, j, ja, jb)
			}
			if ma.FixtureID != mb.FixtureID {
				t.Fatalf("%s: fixture diverged at tick %d", ep, j)
			}
		}
	}
}

// TestPayloadSizesSpanTheRange is the point of splitting the feed kinds: a
// telemetry row and a play-by-play document must differ by orders of
// magnitude, because that spread is what stresses batching and backpressure.
func TestPayloadSizesSpanTheRange(t *testing.T) {
	var smallest, largest int
	smallest = 1 << 30
	for _, ep := range Endpoints() {
		f, _ := NewNamed(ep.Sport, ep.Kind, ep.Name, 13)
		for j := 0; j < 150; j++ {
			f.Next()
		}
		b, _ := json.Marshal(f.Next().Payload)
		if len(b) < smallest {
			smallest = len(b)
		}
		if len(b) > largest {
			largest = len(b)
		}
	}
	// The spread is the point: a consumer has to cope with both a terse status
	// record and a whole leaderboard in the same topic. The floor moved up when
	// the pipeline consolidated on API-Sports, because a bulk sweep's smallest
	// unit is a whole fixture rather than a single incident.
	if smallest > 4000 {
		t.Errorf("smallest payload is %d B, expected a small record somewhere", smallest)
	}
	// The ceiling moved from 20 KB when NASCAR was retired: its race result,
	// carrying forty driver rows of thirty fields each, was the largest
	// document the catalog produced. The golf leaderboard is now the biggest at
	// roughly 19 KB. The point of the assertion is unchanged — one topic has to
	// carry both a terse status record and a whole leaderboard — only the
	// number it is calibrated against.
	if largest < 15000 {
		t.Errorf("largest payload is %d B, expected a large document somewhere", largest)
	}
	t.Logf("payload range: %d B to %d B", smallest, largest)
}

// TestAPISportsDocumentFamilies pins the two document shapes the provider
// actually returns, because simulation and production must be byte-compatible.
//
// Soccer uses a nested fixture{} envelope; every other vertical is flat. A
// consumer written against one and fed the other breaks, so the split is
// reproduced rather than normalised away.
func TestAPISportsDocumentFamilies(t *testing.T) {
	f, err := New(SportSoccer, FeedBoxScore, 5)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	doc := marshalMap(t, f.Next().Payload)
	fixture, ok := doc["fixture"].(map[string]any)
	if !ok {
		t.Fatalf("soccer has no fixture{} root; keys = %v", keysOf(doc))
	}
	for _, k := range []string{"id", "status", "date"} {
		if _, ok := fixture[k]; !ok {
			t.Errorf("soccer fixture has no %s", k)
		}
	}
	if _, ok := doc["goals"]; !ok {
		t.Error("soccer document has no goals block")
	}

	// The flat family.
	for _, sport := range []Sport{SportNCAAB, SportNFL, SportAFL} {
		f, err := New(sport, FeedBoxScore, 5)
		if err != nil {
			t.Fatalf("New(%s): %v", sport, err)
		}
		doc := marshalMap(t, f.Next().Payload)
		if _, nested := doc["fixture"]; nested {
			t.Errorf("%s should be flat, not nested in fixture{}", sport)
		}
		for _, k := range []string{"id", "status", "teams"} {
			if _, ok := doc[k]; !ok {
				t.Errorf("%s document has no %s; keys = %v", sport, k, keysOf(doc))
			}
		}
	}
}

// TestStatusUsesProviderTokens: a consumer matching on "1H" or "FT" must see
// exactly what production sends.
func TestStatusUsesProviderTokens(t *testing.T) {
	f, _ := New(SportSoccer, FeedBoxScore, 9)
	seen := map[string]bool{}
	for i := 0; i < 4000; i++ {
		doc := marshalMap(t, f.Next().Payload)
		fixture, _ := doc["fixture"].(map[string]any)
		if fixture == nil {
			continue
		}
		status, _ := fixture["status"].(map[string]any)
		if status == nil {
			continue
		}
		if short, ok := status["short"].(string); ok {
			seen[short] = true
		}
	}
	for _, want := range []string{"NS", "1H", "HT", "2H", "FT"} {
		if !seen[want] {
			t.Errorf("status %q never appeared; the ladder should walk through it", want)
		}
	}
}

// TestElapsedIsNullWhenNotPlaying reproduces a provider detail that is easy to
// normalise away: elapsed is null before kick-off and after the whistle, not 0.
// A consumer computing an average would silently include those zeroes.
func TestElapsedIsNullWhenNotPlaying(t *testing.T) {
	f, _ := New(SportSoccer, FeedBoxScore, 4)
	for i := 0; i < 3000; i++ {
		doc := marshalMap(t, f.Next().Payload)
		fixture, _ := doc["fixture"].(map[string]any)
		if fixture == nil {
			continue
		}
		status, _ := fixture["status"].(map[string]any)
		if status == nil {
			continue
		}
		short, _ := status["short"].(string)
		elapsed, present := status["elapsed"]
		if !present {
			t.Fatalf("status has no elapsed key at all; the provider always sends it")
		}
		switch short {
		case "NS", "HT", "FT":
			if elapsed != nil {
				t.Fatalf("status %s carries elapsed %v, want null", short, elapsed)
			}
		}
	}
}

// TestSoccerSpansMultipleLeagues is the guard on why soccer keeps moving.
//
// SportsDataIO's trial licensed one competition. AllScores fixed that, and
// API-Sports goes further — its football host carries over a thousand leagues.
// Whatever the provider, the invariant is the same: soccer must never collapse
// back to a single-competition feed.
func TestSoccerSpansMultipleLeagues(t *testing.T) {
	// The soccer simulator evolves a real captured sweep rather than inventing
	// documents, so the breadth this test measures is the breadth of the
	// capture. Without fixtures/ on disk it visits exactly one competition and
	// fails — which is a missing input, not a regression.
	//
	// Guarded because /fixtures/ is gitignored: on a fresh clone, and therefore
	// on every CI run, this test had nothing to work with. It was the one
	// capture-dependent test that failed instead of skipping, and it would have
	// made the first CI run red for a reason unrelated to the commit.
	if len(capturedSports(t)) == 0 {
		t.Skip("no captures on disk; run `make capture` to enable this check")
	}

	f, err := New(SportSoccer, FeedBoxScore, 3)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	leagues := map[string]bool{}
	for j := 0; j < 4000 && len(leagues) < 3; j++ {
		doc := marshalMap(t, f.Next().Payload)
		league, _ := doc["league"].(map[string]any)
		if league == nil {
			continue
		}
		leagues[fmt.Sprint(league["id"])] = true
	}
	if len(leagues) < 3 {
		t.Errorf("soccer visited %d competitions over 4000 ticks, want several", len(leagues))
	}
}

// TestCombatSportsShareOneSchema pins the fact that SportsDataIO ships one MMA
// API: UFC is a league inside it, distinguished only by LeagueId.
func TestCombatSportsShareOneSchema(t *testing.T) {
	ufc, _ := New(SportUFC, FeedBoxScore, 1)
	mma, _ := New(SportMMA, FeedBoxScore, 1)

	a := marshalMap(t, ufc.Next().Payload)
	b := marshalMap(t, mma.Next().Payload)

	if !sameKeys(a, b) {
		t.Errorf("UFC and MMA documents have different shapes:\n ufc=%v\n mma=%v", keysOf(a), keysOf(b))
	}
	// UFC is a promotion inside the MMA vertical, exactly as it was a league
	// inside the SportsDataIO MMA product. They share a host and a route and
	// stay separate pipeline sports only so a licence can entitle one and not
	// the other.
	if ep := ufc.Endpoint(); ep.Path != "/fights" {
		t.Errorf("UFC endpoint is %q, expected the shared /fights route", ep.Path)
	}
	if a, b := ufc.Endpoint().Path, mma.Endpoint().Path; a != b {
		t.Errorf("UFC and MMA use different routes: %q vs %q", a, b)
	}
}

// TestProvenanceMatchesEvidence keeps the catalog's schema claims tied to what
// is actually on disk. An endpoint may only claim ProvenanceCaptured if this
// repository holds a captured response for its sport; anything else is a claim
// we cannot back up, and the flag drifting away from the evidence is exactly
// how the earlier Verified boolean became misleading.
func TestProvenanceMatchesEvidence(t *testing.T) {
	captured := capturedSports(t)
	if len(captured) == 0 {
		t.Skip("no captures on disk; run `make capture` to enable this check")
	}

	// UFC is a league inside the MMA API — same /mma/v3 routes, same models,
	// distinguished only by LeagueId — so the MMA captures are its evidence.
	evidenceFor := func(s Sport) Sport {
		if s == SportUFC {
			return SportMMA
		}
		return s
	}

	for _, ep := range Endpoints() {
		switch ep.Provenance {
		case ProvenanceCaptured:
			if !captured[evidenceFor(ep.Sport)] {
				t.Errorf("%s claims %s but no capture exists for %s",
					ep.Ref(), ep.Provenance, ep.Sport)
			}
		case ProvenanceModeled:
			// A sport we hold captures for should not still be claiming that
			// nothing authoritative exists.
			if captured[evidenceFor(ep.Sport)] {
				t.Errorf("%s/%s claims %s but captures exist for that sport",
					ep.Sport, ep.Ref(), ep.Provenance)
			}
		case ProvenanceOpenAPI, ProvenanceDataDict, ProvenanceInferred:
			// Weaker tiers are legitimate; they simply are not proof.
		default:
			t.Errorf("%s/%s has no provenance set", ep.Sport, ep.Ref())
		}
	}
}

// TestUnreachableSportsAreNotClaimedAsProven pins the sports no provider wired
// up here can serve. None may claim captured evidence.
//
// The list keeps shrinking, which is the point of the pipeline being
// multi-provider: cricket left it when Cricbuzz was added, tennis when
// AllScores was, and rugby when Rugby Live Data was. Only Australian Rules
// remains — no provider wired up here carries it, and a search of the AllScores
// catalog for "AFL", "Aussie" and "Australian Football" found nothing.
func TestUnreachableSportsAreNotClaimedAsProven(t *testing.T) {
	for _, sport := range []Sport{SportAFL} {
		for _, ep := range EndpointsFor(sport) {
			if ep.Provenance.Proven() {
				t.Errorf("%s/%s claims proof, but the endpoint is unreachable",
					sport, ep.Ref())
			}
			if ep.Provenance != ProvenanceModeled {
				t.Errorf("%s/%s = %s, want modeled", sport, ep.Ref(), ep.Provenance)
			}
		}
	}
}

func TestNewAllSkipsMissingCombinations(t *testing.T) {
	// Golf has no play-by-play; asking for a kind across all sports must return
	// the sports that do have it rather than failing.
	feeds, err := NewAll(AllSports, []FeedKind{FeedTelemetry}, 1)
	if err != nil {
		t.Fatalf("NewAll: %v", err)
	}
	if len(feeds) == 0 {
		t.Fatal("NewAll returned nothing")
	}

	// A selection with no matches at all is an error, not an empty slice.
	if _, err := NewAll([]Sport{SportGolf}, []FeedKind{FeedPlayByPlay}, 1); err == nil {
		t.Error("expected an error for a selection with no registered feeds")
	}
}

func TestParseSportList(t *testing.T) {
	all, err := ParseSportList("all")
	if err != nil || len(all) != len(AllSports) {
		t.Fatalf(`ParseSportList("all") = %v, %v`, all, err)
	}
	got, err := ParseSportList("nfl, NBA ,golf,nfl")
	if err != nil {
		t.Fatalf("ParseSportList: %v", err)
	}
	want := []Sport{SportNFL, SportNBA, SportGolf}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v (duplicates should collapse)", got, want)
	}
	if _, err := ParseSportList("quidditch"); err == nil {
		t.Error("expected an error for an unknown sport")
	}
}

func TestParseKindList(t *testing.T) {
	all, err := ParseKindList("all")
	if err != nil || len(all) != len(AllKinds) {
		t.Fatalf(`ParseKindList("all") = %v, %v`, all, err)
	}
	got, err := ParseKindList("pbp, TELEMETRY")
	if err != nil {
		t.Fatalf("ParseKindList: %v", err)
	}
	if len(got) != 2 || got[0] != FeedPlayByPlay || got[1] != FeedTelemetry {
		t.Errorf("got %v, want [playbyplay telemetry]", got)
	}
	if _, err := ParseKindList("scoreboard"); err == nil {
		t.Error("expected an error for an unknown feed kind")
	}
}

// --- helpers ---------------------------------------------------------------

func marshalMap(t *testing.T, payload any) map[string]any {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func sameKeys(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// TestTimelineFeedsNeverDuplicate is the regression guard for a feed that used
// to republish the previous record on ticks that produced no incident. Every
// record a push feed emits must be new: identical consecutive payloads on a
// Kafka topic are indistinguishable from a replay.
//
// It was written against soccer, which has since moved to a bulk provider;
// cricket's fall-of-wicket feed is the surviving per-incident stream and
// carries the same hazard.
func TestTimelineFeedsNeverDuplicate(t *testing.T) {
	f, err := New(SportCricket, FeedTelemetry, 17)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	seen := map[string]int{}
	var prev string
	for j := 0; j < 600; j++ {
		m := f.Next()
		b, err := json.Marshal(m.Payload)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		body := string(b)
		if body == prev {
			t.Fatalf("tick %d republished an identical record: %.200s", j, body)
		}
		prev = body
		// Keyed by fixture so a rollover legitimately reuses incident ids.
		key := m.FixtureID + "|" + body
		if first, dup := seen[key]; dup {
			t.Fatalf("tick %d re-emitted the record first sent at tick %d: %.200s", j, first, body)
		}
		seen[key] = j
	}
}

// TestEveryTickProducesAMessage checks the flip side: an event-driven feed that
// skips empty ticks must still return a message on every Next, or a poller
// worker would silently stall. The sequence is per fixture, so it restarts at 1
// on rollover — but it must never stall or skip.
func TestEveryTickProducesAMessage(t *testing.T) {
	for _, ep := range Endpoints() {
		f, _ := NewNamed(ep.Sport, ep.Kind, ep.Name, 23)
		var last int64
		fixture := f.FixtureID()
		for j := 0; j < 200; j++ {
			m := f.Next()
			if m.Payload == nil {
				t.Fatalf("%s: nil payload at tick %d", ep, j)
			}
			if m.FixtureID != fixture {
				fixture, last = m.FixtureID, 0
			}
			if m.Sequence != last+1 {
				t.Fatalf("%s: sequence %d at tick %d, want %d", ep, m.Sequence, j, last+1)
			}
			last = m.Sequence
		}
	}
}

// TestNamedEndpointsAreDistinct guards the registry's support for several
// endpoints of one kind.
//
// Before the registry keyed on name, a second endpoint of the same kind
// silently shadowed the first and one of the two models became unreachable.
// The regression was originally caught through NASCAR, which published both a
// season schedule and a driver directory as reference documents. NASCAR has
// since been retired and no shipping sport currently registers a named
// endpoint, so the mechanism is exercised directly here instead — deleting the
// test along with the sport would have quietly dropped coverage of a real bug
// while leaving the code that had it in place.
func TestNamedEndpointsAreDistinct(t *testing.T) {
	var refs []Endpoint
	for _, ep := range EndpointsFor(testNamedSport) {
		if ep.Kind == FeedReference {
			refs = append(refs, ep)
		}
	}
	if len(refs) < 2 {
		t.Fatalf("expected two named reference endpoints, got %d", len(refs))
	}

	seenRef := map[string]bool{}
	for _, ep := range refs {
		if ep.Name == "" {
			t.Errorf("%s has two reference endpoints but one is unnamed", ep.Sport)
		}
		if seenRef[ep.Ref()] {
			t.Errorf("%s is registered twice; one shadows the other", ep.Ref())
		}
		seenRef[ep.Ref()] = true

		// Each must be independently reachable by name.
		f, err := NewNamed(ep.Sport, ep.Kind, ep.Name, 5)
		if err != nil {
			t.Fatalf("NewNamed(%s): %v", ep.Ref(), err)
		}
		if got := f.Endpoint().Ref(); got != ep.Ref() {
			t.Errorf("NewNamed(%s) returned %s; the name did not route", ep.Ref(), got)
		}
	}
}

func TestNewAllReachesEveryEndpoint(t *testing.T) {
	feeds, err := NewAll(nil, nil, 1)
	if err != nil {
		t.Fatalf("NewAll: %v", err)
	}
	// Counted against the endpoints of shipping sports only. Endpoints()
	// also returns the registry fixture in registry_named_test.go, which is
	// deliberately absent from AllSports and so is never built by NewAll.
	var shipping int
	for _, ep := range Endpoints() {
		if ep.Sport != testNamedSport {
			shipping++
		}
	}
	if len(feeds) != shipping {
		t.Errorf("NewAll built %d feeds for %d endpoints", len(feeds), shipping)
	}
	seen := map[string]bool{}
	for _, f := range feeds {
		ep := f.Endpoint()
		key := string(ep.Sport) + "/" + ep.Ref()
		if seen[key] {
			t.Errorf("%s built twice", key)
		}
		seen[key] = true
	}
}

// TestAPISportsSportsAreOnTheirProvider guards the consolidation. A leftover
// SportsDataIO route for a sport API-Sports serves would look fine in the
// catalog and fail in production, because that key no longer covers it.
func TestAPISportsSportsAreOnTheirProvider(t *testing.T) {
	onAPISports := []Sport{
		SportNFL, SportNCAAF, SportNCAAB, SportNBA, SportSoccer,
		SportAFL, SportRugby, SportUFC, SportMMA, SportF1,
	}
	for _, sport := range onAPISports {
		eps := EndpointsFor(sport)
		if len(eps) == 0 {
			t.Errorf("%s has no endpoints", sport)
		}
		for _, ep := range eps {
			if ep.Provider != ProviderAPISports {
				t.Errorf("%s/%s is on provider %q, want %q",
					sport, ep.Ref(), ep.Provider, ProviderAPISports)
			}
		}
	}
}

// TestSportsAPISportsCannotServeKeepTheirProvider is the other half: the four
// sports it has no host for must NOT have been moved onto it.
func TestSportsAPISportsCannotServeKeepTheirProvider(t *testing.T) {
	for sport, want := range map[Sport]Provider{
		SportCricket: ProviderCricbuzz,
		SportTennis:  ProviderAllScores,
		SportGolf:    ProviderLiveGolf,
	} {
		eps := EndpointsFor(sport)
		if len(eps) == 0 {
			t.Errorf("%s lost its endpoints", sport)
		}
		for _, ep := range eps {
			if ep.Provider != want {
				t.Errorf("%s/%s is on %q, want %q — API-Sports has no host for it",
					sport, ep.Ref(), ep.Provider, want)
			}
		}
	}
}
