package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/config"
	"github.com/offloadintelligence/offload-ingest/internal/generators"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
)

// runRoutes calls every registered route against the live API.
//
// This is the check the schema comparison cannot make. Captures are fetched by
// hand with a correct URL, so a payload can match perfectly while the route
// recorded in the catalog is wrong — which is exactly what happened: eight
// endpoints sat at 100% shape coverage while returning 404.
func runRoutes(fixtureRoot string) error {
	if _, err := config.LoadEnv(""); err != nil {
		return err
	}
	key := config.APIKey()
	if key == "" {
		return fmt.Errorf("SPORTS_DATA_IO_API_KEY is not set; route validation needs a live key")
	}

	params, err := buildParams(fixtureRoot)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}

	fmt.Printf("%-8s %-20s %-10s %-6s %s\n", "SPORT", "FEED", "SCHEMA", "HTTP", "NOTE")
	var broken, skipped, expected int
	seen := map[string]bool{}

	for _, ep := range generators.Endpoints() {
		// Several feeds project out of one document and share its route; call
		// each distinct route once.
		routeKey := string(ep.Sport) + " " + ep.Path
		if seen[routeKey] {
			continue
		}
		seen[routeKey] = true

		url, missing := resolve(ep, params)
		if len(missing) > 0 {
			skipped++
			fmt.Printf("%-8s %-20s %-10s %-6s no value for {%s}\n",
				ep.Sport, ep.Ref(), ep.Provenance, "SKIP", strings.Join(missing, ","))
			continue
		}

		base, header, ok := upstream(ep, key)
		if !ok {
			skipped++
			fmt.Printf("%-8s %-20s %-10s %-6s no credentials for provider %q\n",
				ep.Sport, ep.Ref(), ep.Provenance, "SKIP", ep.Provider)
			continue
		}
		target := base + url
		if ep.Provider == generators.ProviderAPISports {
			// The bulk sweep's parameters live in the query string, so the
			// route is not callable without them: /games alone returns an
			// errors block rather than a card.
			if v, err := apisports.ParseVertical(string(ep.Sport)); err == nil {
				if spec, ok := apisports.SpecFor(v); ok {
					target = spec.BaseURL() + spec.BulkPath + "?" +
						encodeParams(spec.BulkQuery(time.Now()))
				}
			}
		}
		code, body := call(client, target, header)
		note := "ok"
		flag := ""
		switch {
		case code == 200 && envelopeError(body) != "":
			// API-Sports answers a rejected request with 200 and the reason in
			// the body, so the status code alone would call this route healthy.
			note, flag = envelopeError(body), "   (plan or parameter)"
		case code == 200:
		case code == 401:
			note, flag = trim(body), "   (not licensed)"
		case code == 404:
			// A feed marked "inferred" is one we already know cannot be
			// reached on this subscription — that is what the tier means. It
			// is reported, but it does not fail the check; only an unexpected
			// 404 does, which is the case this tool exists to catch.
			if ep.Provenance == generators.ProvenanceInferred {
				note, flag = trim(body), "   (known unreachable)"
				expected++
			} else {
				note, flag = trim(body), "   <-- BROKEN ROUTE"
				broken++
			}
		default:
			note = trim(body)
		}
		fmt.Printf("%-8s %-20s %-10s %-6d %s%s\n", ep.Sport, ep.Ref(), ep.Provenance, code, note, flag)
	}

	fmt.Printf("\n%d routes checked, %d skipped for want of a parameter.\n",
		len(seen)-skipped, skipped)
	fmt.Printf("%d unexpected 404s; %d known-unreachable (inferred).\n", broken, expected)
	if broken > 0 {
		return fmt.Errorf("%d route(s) do not resolve", broken)
	}
	return nil
}

// upstream maps a provider onto its base URL and auth headers. Adding a
// provider is a matter of extending this switch: everything else in the tool
// works off the endpoint's Provider field.
func upstream(ep generators.Endpoint, sdioKey string) (string, map[string]string, bool) {
	switch ep.Provider {
	case generators.ProviderSportsDataIO:
		if sdioKey == "" {
			return "", nil, false
		}
		return "https://api.sportsdata.io",
			map[string]string{"Ocp-Apim-Subscription-Key": sdioKey}, true
	case generators.ProviderCricbuzz:
		return rapidAPI(os.Getenv("RAPIDAPI_CRICKET_HOST"))
	case generators.ProviderAllScores:
		return rapidAPI(os.Getenv("RAPIDAPI_ALLSCORES_HOST"))
	case generators.ProviderAPISports:
		// The primary provider is twelve hosts, not one, each independently
		// metered. The host is resolved from the SPORT rather than the path,
		// because several sports share a path (/games) on different hosts and
		// two share a host (NFL and NCAAF) on the same path.
		key := os.Getenv("APISPORTS_KEY")
		if key == "" {
			return "", nil, false
		}
		v, err := apisports.ParseVertical(string(ep.Sport))
		if err != nil {
			return "", nil, false
		}
		spec, ok := apisports.SpecFor(v)
		if !ok {
			return "", nil, false
		}
		return spec.BaseURL(), map[string]string{"x-apisports-key": key}, true
	default:
		// ProviderNone: the sport has no upstream, so there is no route to call.
		return "", nil, false
	}
}

// rapidAPI builds the base URL and headers for any RapidAPI-hosted provider.
// They all authenticate the same way, so onboarding another is a host name.
func rapidAPI(host string) (string, map[string]string, bool) {
	key := os.Getenv("RAPIDAPI_KEY")
	if key == "" || host == "" {
		return "", nil, false
	}
	return "https://" + host, map[string]string{
		"x-rapidapi-key":  key,
		"x-rapidapi-host": host,
		"Content-Type":    "application/json",
	}, true
}

func call(c *http.Client, url string, header map[string]string) (int, []byte) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, []byte(err.Error())
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, []byte(err.Error())
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
	return resp.StatusCode, body
}

func trim(b []byte) string {
	s := strings.Join(strings.Fields(string(b)), " ")
	if len(s) > 70 {
		s = s[:70]
	}
	return s
}

// resolve substitutes {placeholders}, preferring a sport-specific value.
func resolve(ep generators.Endpoint, params map[string]string) (string, []string) {
	out := ep.Path
	var missing []string
	for strings.Contains(out, "{") {
		i := strings.Index(out, "{")
		j := strings.Index(out[i:], "}")
		if j < 0 {
			break
		}
		name := out[i+1 : i+j]
		val, ok := params[string(ep.Sport)+"."+name]
		if !ok {
			val, ok = params[name]
		}
		if !ok {
			missing = append(missing, name)
			val = "MISSING"
		}
		out = out[:i] + val + out[i+j+1:]
	}
	return out, missing
}

// buildParams sources real route parameters from the captures, so the routes
// are exercised with values the provider will actually recognise.
func buildParams(root string) (map[string]string, error) {
	p := map[string]string{"season": "2025", "week": "1"}

	type pick struct {
		file, key, param string
		done             string // only take records where this field is truthy
	}
	picks := []pick{
		{"sportsdataio/nfl/scores_by_week.json", "HomeTeam", "nfl.hometeam", ""},
		{"sportsdataio/nba/games_by_date.json", "GameID", "nba.gameid", "Status"},
		{"sportsdataio/ncaab/games_by_date.json", "GameID", "ncaab.gameid", "Status"},
		{"sportsdataio/ncaaf/games_by_week.json", "GameID", "ncaaf.gameid", "Status"},
		{"sportsdataio/golf/tournaments.json", "TournamentID", "tournamentid", "IsOver"},
		{"sportsdataio/golf/players.json", "PlayerID", "playerid", ""},
		{"sportsdataio/mma/schedule.json", "EventId", "eventid", "Status"},
		{"sportsdataio/mma/fighters.json", "FighterId", "fighterid", ""},
		{"sportsdataio/nascar/races.json", "RaceID", "raceid", "IsOver"},
	}
	for _, pk := range picks {
		doc, err := load(root, pk.file)
		if err != nil {
			continue // a missing capture just means that route is skipped
		}
		arr, ok := doc.([]any)
		if !ok {
			continue
		}
		for _, item := range arr {
			obj, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if pk.done != "" && !isFinal(obj[pk.done]) {
				continue
			}
			if v, ok := obj[pk.key]; ok && v != nil {
				p[pk.param] = scalar(v)
				break
			}
		}
	}
	p["nba.date"] = "2026-JAN-15"
	p["ncaab.date"] = "2026-JAN-15"

	// Cricbuzz nests its match ids several levels down, so the id is taken
	// from the captured match-info document rather than a flat list.
	if doc, err := load(root, "cricbuzz/match_info.json"); err == nil {
		if obj, ok := doc.(map[string]any); ok {
			if v, ok := obj["matchid"]; ok && v != nil {
				p["cricket.matchid"] = scalar(v)
			}
		}
	}
	// AllScores and Rugby Live wrap their payloads, so their ids come from
	// inside the captured document too.
	if doc, err := load(root, "allscores/tennis_game_details.json"); err == nil {
		if g, ok := navigate(doc, "game").(map[string]any); ok {
			p["tennis.gameId"] = scalar(g["id"])
		}
	}
	if doc, err := load(root, "allscores/soccer_game_details.json"); err == nil {
		if g, ok := navigate(doc, "game").(map[string]any); ok {
			p["soccer.gameId"] = scalar(g["id"])
		}
	}
	if doc, err := load(root, "rugbylive/match.json"); err == nil {
		if m, ok := navigate(doc, "results.match").(map[string]any); ok {
			p["rugby.match_id"] = scalar(m["id"])
		}
	}
	return p, nil
}

// isFinal accepts either a "Final" status string or a boolean completion flag.
func isFinal(v any) bool {
	switch t := v.(type) {
	case string:
		return strings.EqualFold(t, "final")
	case bool:
		return t
	}
	return false
}

func scalar(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return fmt.Sprintf("%d", int64(t))
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}
