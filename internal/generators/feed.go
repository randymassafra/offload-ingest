// Package generators produces mock SportsDataIO-shaped payloads for every sport
// the ingest pipeline carries.
//
// The package is deliberately thin: all wire structs live in internal/sdio, and
// everything here is simulation — advancing a fixture and rendering the SDIO
// model that a given endpoint would return at that moment. Swapping in a
// corrected schema is an edit to internal/sdio alone.
//
// # Feed kinds
//
// A sport is not one stream. SportsDataIO splits a live event across endpoints
// with wildly different shapes, sizes and update rates, and the pipeline has to
// survive all of them at once. The four kinds are modelled separately:
//
//	FeedBoxScore    A whole-event snapshot: game, line score, both team stat
//	                lines and every player stat line. Large (tens of KB),
//	                slow-moving, polled. This is what blows up a consumer's
//	                deserialization budget.
//	FeedPlayByPlay  The event timeline: an ordered array of plays or incidents.
//	                Medium size, append-only, strictly ordered per fixture.
//	FeedPlayerStats An array of per-player stat lines with no game wrapper.
//	                Wide, flat, numeric — the shape a Flink aggregation joins on.
//	FeedTelemetry   The high-frequency tail: one NASCAR timing row, one golf
//	                hole, one tennis point, one cricket delivery. Small, fast,
//	                bursty. This is what the webhook emitters push.
//	FeedReference   Schedules and directories. Very large, very slow-moving,
//	                and not tied to a single fixture.
//
// Not every sport offers every kind, which is itself authentic: SportsDataIO
// has no soccer play-by-play endpoint and no golf play-by-play at all.
package generators

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Sport identifies a feed.
type Sport string

// The thirteen sports carried by the ingest pipeline.
const (
	SportNFL     Sport = "nfl"
	SportNCAAF   Sport = "ncaaf"
	SportNCAAB   Sport = "ncaab"
	SportNBA     Sport = "nba"
	SportSoccer  Sport = "soccer"
	SportAFL     Sport = "afl"
	SportRugby   Sport = "rugby"
	SportCricket Sport = "cricket"
	SportTennis  Sport = "tennis"
	SportGolf    Sport = "golf"
	SportUFC     Sport = "ufc"
	SportMMA     Sport = "mma"
	SportNASCAR  Sport = "nascar"
	SportF1      Sport = "f1"
)

// AllSports lists every sport in a stable order.
var AllSports = []Sport{
	SportNFL, SportNCAAF, SportNCAAB, SportNBA, SportSoccer,
	SportAFL, SportRugby, SportCricket, SportTennis, SportGolf,
	SportUFC, SportMMA, SportNASCAR, SportF1,
}

// Valid reports whether s is a known sport.
func (s Sport) Valid() bool {
	for _, known := range AllSports {
		if known == s {
			return true
		}
	}
	return false
}

func (s Sport) String() string { return string(s) }

// ParseSport resolves a case-insensitive sport code or common alias.
func ParseSport(raw string) (Sport, error) {
	switch normalize(raw) {
	case "nfl":
		return SportNFL, nil
	case "ncaaf", "cfb", "collegefootball":
		return SportNCAAF, nil
	case "ncaab", "cbb", "collegebasketball":
		return SportNCAAB, nil
	case "nba":
		return SportNBA, nil
	case "epl", "premierleague", "soccer", "football":
		return SportSoccer, nil
	case "afl", "aussierules":
		return SportAFL, nil
	case "rugby", "nrl":
		return SportRugby, nil
	case "cricket", "t20":
		return SportCricket, nil
	case "tennis", "atp", "wta":
		return SportTennis, nil
	case "golf", "pga":
		return SportGolf, nil
	case "ufc":
		return SportUFC, nil
	case "mma", "bellator", "pfl":
		return SportMMA, nil
	case "nascar", "cup", "cupseries", "motorsport", "motorsports":
		return SportNASCAR, nil
	}
	return "", fmt.Errorf("generators: unknown sport %q", raw)
}

// FeedKind is the class of endpoint a feed imitates.
type FeedKind string

const (
	FeedBoxScore    FeedKind = "boxscore"
	FeedPlayByPlay  FeedKind = "playbyplay"
	FeedPlayerStats FeedKind = "playerstats"
	FeedTelemetry   FeedKind = "telemetry"
	// FeedReference is slow-moving reference data: schedules, rosters, driver
	// and team directories. It is polled rarely but the documents are large,
	// which is a different stress on the pipeline from either a box score or a
	// telemetry row.
	FeedReference FeedKind = "reference"
)

// AllKinds lists every feed kind in increasing update frequency.
var AllKinds = []FeedKind{FeedReference, FeedBoxScore, FeedPlayerStats, FeedPlayByPlay, FeedTelemetry}

// Valid reports whether k is a known feed kind.
func (k FeedKind) Valid() bool {
	for _, known := range AllKinds {
		if known == k {
			return true
		}
	}
	return false
}

func (k FeedKind) String() string { return string(k) }

// ParseKind resolves a case-insensitive feed kind or alias.
func ParseKind(raw string) (FeedKind, error) {
	switch normalize(raw) {
	case "boxscore", "box", "leaderboard":
		return FeedBoxScore, nil
	case "playbyplay", "pbp", "timeline":
		return FeedPlayByPlay, nil
	case "playerstats", "players", "stats":
		return FeedPlayerStats, nil
	case "telemetry", "tele", "highfrequency", "hf":
		return FeedTelemetry, nil
	case "reference", "ref", "schedule", "roster", "directory":
		return FeedReference, nil
	}
	return "", fmt.Errorf("generators: unknown feed kind %q", raw)
}

// Provider identifies which upstream API a feed imitates.
//
// The pipeline consolidated on API-Sports, which serves ten of the fourteen
// sports from twelve independently-metered hosts. It does not serve the other
// four at all: cricket, tennis, golf and NASCAR have no host there under any
// spelling, verified by probing every plausible name. Those keep the providers
// that do serve them, so the catalog stays honest about which vendor's schema a
// topic carries, and the wire models live in a package per provider rather than
// being forced into one house style.
type Provider string

const (
	// ProviderAPISports is the primary provider: soccer, NFL, NCAA football and
	// basketball, NBA, AFL, rugby, UFC, MMA and Formula 1.
	ProviderAPISports Provider = "apisports"
	// ProviderCricbuzz serves cricket, which API-Sports does not carry.
	ProviderCricbuzz Provider = "cricbuzz"
	// ProviderAllScores serves tennis, which API-Sports does not carry.
	ProviderAllScores Provider = "allscores"
	// ProviderSportsDataIO is retained ONLY for golf and NASCAR — the two
	// sports no other provider here serves. It was the pipeline's original
	// primary and now covers two feeds; everything else moved to API-Sports.
	ProviderSportsDataIO Provider = "sportsdataio"
	// ProviderNone marks a feed with no upstream at all.
	ProviderNone Provider = "none"
)

// AllProviders lists the upstreams in a stable order, primary first.
var AllProviders = []Provider{
	ProviderAPISports, ProviderCricbuzz, ProviderAllScores, ProviderSportsDataIO,
}

func (p Provider) String() string { return string(p) }

// Endpoint describes the upstream endpoint a feed imitates.
//
// Path is the documented route template, verbatim, with nothing appended.
// Several feeds consume only part of a response — the per-driver timing rows
// inside an F1 race document, the incident arrays inside a soccer box score —
// and those are described by Projection rather than by inventing a route
// suffix. A projection is a JSON path into the response Path returns; empty
// means the feed carries the whole body.
//
// This distinction matters downstream: Path is what a consumer would call, and
// Projection is what it would then select. Encoding the second as a fake URL
// fragment made the catalog look like it covered routes that do not exist.
type Endpoint struct {
	Sport Sport    `json:"sport"`
	Kind  FeedKind `json:"kind"`
	// Provider is the upstream API this endpoint belongs to. Empty defaults to
	// SportsDataIO, which is what most of the catalog imitates.
	Provider Provider `json:"provider"`
	// Name distinguishes endpoints that share a sport and kind — a schedule and
	// a driver directory are both reference documents, for instance. It is
	// empty for the sole endpoint of its kind.
	Name       string `json:"name,omitempty"`
	Path       string `json:"path"`
	Projection string `json:"projection,omitempty"`
	Model      string `json:"model"`
	// Provenance records how strongly this endpoint's schema is evidenced.
	Provenance Provenance `json:"provenance"`
	// Replayed is true when the payloads come from a saved provider response
	// rather than being simulated. See internal/generators/captured.go.
	Replayed bool `json:"replayed,omitempty"`
}

// Provenance records the strength of evidence behind an endpoint's schema.
//
// This replaced a plain Verified boolean, which conflated three very different
// claims: "the provider documents this shape", "the provider's spec declares
// this shape", and "we diffed this shape against bytes the provider actually
// sent". Only the last is proof, and the captures under fixtures/sportsdataio
// showed the difference matters — the NCAA data-dictionary pages described
// 37-49%% of what the API really returns, and the NFL OpenAPI spec declares 14
// TeamGame columns the live endpoint never sends.
type Provenance string

const (
	// ProvenanceCaptured: the model was diffed against a real response, either
	// directly or as a projection of one. This is the only tier that is proof.
	ProvenanceCaptured Provenance = "captured"
	// ProvenanceOpenAPI: taken from the published OpenAPI specification, but
	// not yet checked against live bytes. The spec can over-declare.
	ProvenanceOpenAPI Provenance = "openapi"
	// ProvenanceDataDict: taken from a public data-dictionary page. Weaker
	// still — these pages proved materially incomplete for the NCAA feeds.
	ProvenanceDataDict Provenance = "datadict"
	// ProvenanceInferred: modelled on a sibling API's captured shape because
	// this endpoint could not be reached. Plausible, not evidenced.
	ProvenanceInferred Provenance = "inferred"
	// ProvenanceModeled: no authoritative source at all. The provider either
	// does not document the feed or does not offer the sport.
	ProvenanceModeled Provenance = "modeled"
)

// Proven reports whether the schema was checked against real provider bytes.
// Everything else is a claim of varying strength, not evidence.
func (p Provenance) Proven() bool { return p == ProvenanceCaptured }

// Ref is the endpoint's stable identifier within its sport.
func (e Endpoint) Ref() string {
	if e.Name == "" {
		return string(e.Kind)
	}
	return string(e.Kind) + ":" + e.Name
}

func (e Endpoint) String() string {
	if e.Projection != "" {
		return fmt.Sprintf("%s/%s %s [%s] -> %s", e.Sport, e.Kind, e.Path, e.Projection, e.Model)
	}
	return fmt.Sprintf("%s/%s %s -> %s", e.Sport, e.Kind, e.Path, e.Model)
}

// Whole reports whether the feed carries an entire endpoint response rather
// than a projection out of one.
func (e Endpoint) Whole() bool { return e.Projection == "" }

// Message is one rendered payload plus the routing metadata the producer needs.
// Payload is an internal/sdio value; it is what gets marshalled as the Kafka
// message body, with no envelope of ours wrapped around it.
type Message struct {
	Sport      Sport     `json:"sport"`
	Kind       FeedKind  `json:"kind"`
	Endpoint   string    `json:"endpoint"`
	Projection string    `json:"projection,omitempty"`
	Model      string    `json:"model"`
	FixtureID  string    `json:"fixture_id"`
	Sequence   int64     `json:"sequence"`
	Emitted    time.Time `json:"emitted"`

	// NormalizedLeagueID and ProviderOrgID are the scope-bearing identity,
	// normalised out of the payload once when the record enters the pipeline.
	//
	// They live on the ENVELOPE, never in the payload. The Kafka value is the
	// provider's document verbatim — a contract the schema comparison enforces
	// at 100% path coverage — so a synthetic field injected into it would fail
	// both that check and TestWriterEmitsBarePayload. Routing metadata travels
	// beside the payload in headers, and this is routing metadata: it decides
	// whether the record may be published at all.
	//
	// Zero means the payload carried no league, which is normal for a provider
	// that scopes by tour or for a sport with a single competition.
	NormalizedLeagueID int    `json:"normalized_league_id,omitempty"`
	ProviderOrgID      string `json:"provider_org_id,omitempty"`
	// LeagueName documents the id for logs and drop messages.
	LeagueName string `json:"league_name,omitempty"`

	Payload any `json:"payload"`
}

// Key is the Kafka partition key: the provider's own fixture identifier, so
// every message for one game lands on one partition in order.
func (m Message) Key() []byte { return []byte(m.FixtureID) }

func normalize(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	return strings.NewReplacer(" ", "", "-", "", "_", "", ".", "").Replace(s)
}

// ParseSportList resolves a comma-separated list; "all" expands to every sport.
func ParseSportList(csv string) ([]Sport, error) {
	csv = strings.TrimSpace(csv)
	if csv == "" || normalize(csv) == "all" {
		return append([]Sport(nil), AllSports...), nil
	}
	out := make([]Sport, 0)
	seen := map[Sport]bool{}
	for _, p := range strings.Split(csv, ",") {
		if strings.TrimSpace(p) == "" {
			continue
		}
		s, err := ParseSport(p)
		if err != nil {
			return nil, err
		}
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("generators: empty sport list %q", csv)
	}
	return out, nil
}

// ParseKindList resolves a comma-separated list; "all" expands to every kind.
func ParseKindList(csv string) ([]FeedKind, error) {
	csv = strings.TrimSpace(csv)
	if csv == "" || normalize(csv) == "all" {
		return append([]FeedKind(nil), AllKinds...), nil
	}
	out := make([]FeedKind, 0)
	seen := map[FeedKind]bool{}
	for _, p := range strings.Split(csv, ",") {
		if strings.TrimSpace(p) == "" {
			continue
		}
		k, err := ParseKind(p)
		if err != nil {
			return nil, err
		}
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("generators: empty feed kind list %q", csv)
	}
	return out, nil
}

// sortEndpoints orders endpoints for stable CLI output.
func sortEndpoints(eps []Endpoint) {
	sort.Slice(eps, func(i, j int) bool {
		if eps[i].Sport != eps[j].Sport {
			return eps[i].Sport < eps[j].Sport
		}
		if eps[i].Kind != eps[j].Kind {
			return eps[i].Kind < eps[j].Kind
		}
		return eps[i].Name < eps[j].Name
	})
}
