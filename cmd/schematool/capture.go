package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/offloadintelligence/offload-ingest/config"
)

// runCapture refreshes the provider responses under the fixtures root.
//
// The comparison and route checks are only as good as what they compare
// against, so capturing has to be a first-class, repeatable step rather than an
// ad-hoc script. Everything here is discovery-driven: it fetches a schedule,
// picks a completed fixture out of it, and follows that id into the box score,
// so a refresh does not depend on hand-maintained game ids going stale.
func runCapture(root string, providers string) error {
	// Every credential this run might use, read once. Nothing below reaches
	// for os.Getenv on its own.
	cfg, err := config.Load("")
	if err != nil {
		return err
	}

	want := map[string]bool{}
	for _, p := range strings.Split(providers, ",") {
		if p = strings.TrimSpace(p); p != "" {
			want[p] = true
		}
	}
	if len(want) == 0 || want["all"] {
		want = map[string]bool{
			"cricbuzz": true, "allscores": true,
			"apisports": true, "golfdata": true,
		}
	}

	c := &capturer{root: root, client: &http.Client{Timeout: 60 * time.Second}}

	// Golf: live-golf-data via RapidAPI. API-Sports has no golf host.
	if want["golfdata"] && cfg.GolfAPIKey != "" {
		const host = "live-golf-data.p.rapidapi.com"
		c.header = map[string]string{
			"x-rapidapi-key": cfg.GolfAPIKey, "x-rapidapi-host": host,
		}
		c.captureGolf(host)
	}
	// RapidAPI-hosted providers all authenticate identically, so each one is a
	// host name and a capture function.
	for _, p := range []struct {
		name    string
		hostEnv string
		host    string
		fn      func(string)
	}{
		{"cricbuzz", "RAPIDAPI_CRICKET_HOST", cfg.RapidAPICricketHost, c.captureCricbuzz},
		{"allscores", "RAPIDAPI_ALLSCORES_HOST", cfg.RapidAPIAllScoresHost, c.captureAllScores},
	} {
		if !want[p.name] {
			continue
		}
		if cfg.RapidAPIKey == "" || p.host == "" {
			fmt.Fprintf(os.Stderr, "skipping %s: RAPIDAPI_KEY / %s not set\n", p.name, p.hostEnv)
			continue
		}
		c.header = map[string]string{
			"x-rapidapi-key":  cfg.RapidAPIKey,
			"x-rapidapi-host": p.host,
			"Content-Type":    "application/json",
		}
		p.fn(p.host)
	}

	if want["apisports"] {
		if cfg.APISportsKey == "" {
			fmt.Fprintln(os.Stderr, "skipping apisports: APISPORTS_KEY not set")
		} else {
			c.header = map[string]string{"x-apisports-key": cfg.APISportsKey}
			c.captureAPISports()
		}
	}

	fmt.Printf("\n%d captured, %d failed.\n", c.ok, c.failed)
	if c.failed > 0 {
		return fmt.Errorf("%d capture(s) failed", c.failed)
	}
	return nil
}

type capturer struct {
	root   string
	client *http.Client
	header map[string]string
	ok     int
	failed int
}

// get fetches a URL and returns the decoded document.
func (c *capturer) get(url string) (any, []byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, err
	}
	for k, v := range c.header {
		req.Header.Set(k, v)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, body, fmt.Errorf("HTTP %d: %s", resp.StatusCode, trim(body))
	}
	var doc any
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, body, fmt.Errorf("decode: %w", err)
	}
	return doc, body, nil
}

// save fetches a URL and writes it under fixtures/<rel>. It returns the decoded
// document so a caller can follow ids out of it into the next request.
func (c *capturer) save(rel, url string) any {
	doc, _, err := c.get(url)
	if err != nil {
		c.failed++
		fmt.Printf("FAIL %-40s %v\n", rel, err)
		return nil
	}
	path := filepath.Join(c.root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		c.failed++
		fmt.Printf("FAIL %-40s %v\n", rel, err)
		return nil
	}
	pretty, err := json.MarshalIndent(doc, "", " ")
	if err != nil {
		c.failed++
		fmt.Printf("FAIL %-40s %v\n", rel, err)
		return nil
	}
	if err := os.WriteFile(path, pretty, 0o644); err != nil {
		c.failed++
		fmt.Printf("FAIL %-40s %v\n", rel, err)
		return nil
	}
	c.ok++
	fmt.Printf("ok   %-40s %8d B  %s\n", rel, len(pretty), describe(doc))
	return doc
}

func describe(doc any) string {
	switch t := doc.(type) {
	case []any:
		return fmt.Sprintf("%d record(s)", len(t))
	case map[string]any:
		return fmt.Sprintf("%d field(s)", len(t))
	}
	return ""
}

// pickID walks a list and returns the first record's field value, preferring
// records whose completion flag is set so the capture contains a finished
// fixture with fully populated stats.
func pickID(doc any, idField, doneField string) (string, bool) {
	arr, ok := doc.([]any)
	if !ok {
		return "", false
	}
	var fallback string
	for _, item := range arr {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		v, ok := obj[idField]
		if !ok || v == nil {
			continue
		}
		id := scalar(v)
		if doneField == "" || isFinal(obj[doneField]) {
			return id, true
		}
		if fallback == "" {
			fallback = id
		}
	}
	return fallback, fallback != ""
}

// captureCricbuzz refreshes the cricket captures.
//
// API-Sports has no cricket host, so this is a second
// provider with an entirely different schema: lowercase keys, a scorecard of
// innings each carrying batsman, bowler, fall-of-wicket and partnership arrays.
func (c *capturer) captureCricbuzz(host string) {
	fmt.Println("\n== Cricbuzz (RapidAPI) ==")
	base := "https://" + host

	recent := c.save("cricbuzz/matches_recent.json", base+"/matches/v1/recent")
	c.save("cricbuzz/matches_live.json", base+"/matches/v1/live")

	id := firstCricbuzzMatchID(recent)
	if id == "" {
		// Fall back to a known completed match so a refresh still produces the
		// per-match documents even when the recent list is empty.
		id = "40381"
	}
	c.save("cricbuzz/match_info.json", base+"/mcenter/v1/"+id)
	c.save("cricbuzz/scorecard.json", base+"/mcenter/v1/"+id+"/hscard")
	c.save("cricbuzz/commentary.json", base+"/mcenter/v1/"+id+"/comm")
}

// firstCricbuzzMatchID digs a match id out of the nested recent-matches
// document, whose shape is typeMatches[] > seriesMatches[] > seriesAdWrapper >
// matches[] > matchInfo.matchId.
func firstCricbuzzMatchID(doc any) string {
	obj, ok := doc.(map[string]any)
	if !ok {
		return ""
	}
	types, _ := obj["typeMatches"].([]any)
	for _, t := range types {
		tm, ok := t.(map[string]any)
		if !ok {
			continue
		}
		series, _ := tm["seriesMatches"].([]any)
		for _, s := range series {
			sm, ok := s.(map[string]any)
			if !ok {
				continue
			}
			wrap, ok := sm["seriesAdWrapper"].(map[string]any)
			if !ok {
				continue
			}
			matches, _ := wrap["matches"].([]any)
			for _, m := range matches {
				mm, ok := m.(map[string]any)
				if !ok {
					continue
				}
				info, ok := mm["matchInfo"].(map[string]any)
				if !ok {
					continue
				}
				if v, ok := info["matchId"]; ok && v != nil {
					return scalar(v)
				}
			}
		}
	}
	return ""
}

// captureAllScores refreshes the tennis captures.
//
// API-Sports has no tennis host, so tennis
// comes from here instead. The capture walks a date window of matches, prefers
// a completed singles tie from a major, and follows its id into the match
// document.
func (c *capturer) captureAllScores(host string) {
	fmt.Println("\n== AllScores (RapidAPI) ==")
	base := "https://" + host + "/api/allscores"
	const tz = "America%2FChicago"

	c.save("allscores/sports.json", base+"/sports?langId=1&timezone="+tz+"&withCount=true")

	// A recent window, formatted the way this API expects (dd/MM/yyyy).
	//
	// Two things this endpoint does that its parameters do not suggest: it
	// honours only the end of the range, returning that single day's card; and
	// it takes dd/MM/yyyy, silently falling back to the next matchday for any
	// other layout. So the window ends yesterday — ending it today would
	// return a card of unplayed fixtures, and an unplayed fixture carries no
	// lineups, no statistics and no shot chart, which is the whole point of
	// following one into game-details.
	end := time.Now().AddDate(0, 0, -1)
	start := end.AddDate(0, 0, -4)
	// The date must be escaped AFTER formatting, never inside the layout:
	// Go's reference layout treats the "2" in a literal "%2F" as the day of
	// the month, so "02%2F01%2F2006" renders as "29%29F08%29F2026" and the API
	// quietly ignores the range and returns the next matchday instead.
	day := func(t time.Time) string { return url.QueryEscape(t.Format("02/01/2006")) }
	window := fmt.Sprintf("startDate=%s&endDate=%s", day(start), day(end))

	games := c.save("allscores/tennis_games.json",
		fmt.Sprintf("%s/games-scores?sport=3&%s&langId=1&timezone=%s", base, window, tz))
	if id := pickTennisGame(games); id != "" {
		c.save("allscores/tennis_game_details.json",
			fmt.Sprintf("%s/game-details?gameId=%s&langId=1&timezone=%s", base, id, tz))
	}

	// Soccer (sport 1). One call returns the whole multi-league board, which is
	// both the scoreboard feed's capture and the source of a match id.
	soccer := c.save("allscores/soccer_games.json",
		fmt.Sprintf("%s/games-scores?sport=1&%s&langId=1&timezone=%s", base, window, tz))
	if id := pickSoccerGame(soccer); id != "" {
		c.save("allscores/soccer_game_details.json",
			fmt.Sprintf("%s/game-details?gameId=%s&langId=1&timezone=%s", base, id, tz))
	}
}

// allScoresEnded reports whether a fixture has been played to a result.
//
// AllScores does not use "Final": a completed match is "Ended",
// or one of the variants a knockout tie can end on. Reusing isFinal here made
// both pickers match nothing, so every capture run stopped at the fixture list
// and never followed a game into its match document.
func allScoresEnded(v any) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	switch strings.ToLower(s) {
	case "ended", "after et", "after penalties", "final result only":
		return true
	}
	return false
}

// pickSoccerGame prefers a finished tie from one of the leagues the generator
// covers, because only a completed match carries confirmed lineups, the full
// 43-statistic player rows and a populated shot chart.
func pickSoccerGame(doc any) string {
	obj, ok := doc.(map[string]any)
	if !ok {
		return ""
	}
	// The competitions the soccer generator models, by AllScores id.
	wanted := map[float64]bool{7: true, 11: true, 17: true, 25: true, 35: true,
		57: true, 78: true, 104: true, 113: true, 141: true}

	games, _ := obj["games"].([]any)
	var fallback string
	for _, g := range games {
		gm, ok := g.(map[string]any)
		if !ok || !allScoresEnded(gm["statusText"]) {
			continue
		}
		id := scalar(gm["id"])
		if cid, _ := gm["competitionId"].(float64); wanted[cid] {
			// hasLineups is what separates a document with player rows from a
			// bare result, and it is the whole point of this capture.
			if lu, _ := gm["hasLineups"].(bool); lu {
				return id
			}
		}
		if fallback == "" {
			fallback = id
		}
	}
	return fallback
}

// pickTennisGame prefers a completed singles match from a major, because a
// finished tie carries the full set-by-set breakdown including tiebreaks.
func pickTennisGame(doc any) string {
	obj, ok := doc.(map[string]any)
	if !ok {
		return ""
	}
	// Competition names live in a sibling lookup table.
	names := map[float64]string{}
	if comps, ok := obj["competitions"].([]any); ok {
		for _, c := range comps {
			if cm, ok := c.(map[string]any); ok {
				if id, ok := cm["id"].(float64); ok {
					names[id], _ = cm["name"].(string)
				}
			}
		}
	}
	games, _ := obj["games"].([]any)
	var fallback string
	for _, g := range games {
		gm, ok := g.(map[string]any)
		if !ok || !allScoresEnded(gm["statusText"]) {
			continue
		}
		id := scalar(gm["id"])
		cid, _ := gm["competitionId"].(float64)
		name := names[cid]
		// Singles at a major: the richest document this feed produces.
		if strings.Contains(name, "Open") && !strings.Contains(name, "Doubles") {
			return id
		}
		if fallback == "" {
			fallback = id
		}
	}
	return fallback
}

// captureRugbyLive refreshes the rugby captures.
//
// Only recent seasons carry the full document: a 2008 fixture returns empty
// teamsheets and events, so the capture deliberately walks a team's fixture
// history newest-first and takes the most recent completed match.
func (c *capturer) captureRugbyLive(host string) {
	fmt.Println("\n== Rugby Live Data (RapidAPI) ==")
	base := "https://" + host

	c.save("rugbylive/competitions.json", base+"/competitions")

	// Castres Olympique: a TOP 14 side with a long, well-populated history.
	const teamID = "6167"
	fixtures := c.save("rugbylive/fixtures.json", base+"/fixtures-results-by-team/"+teamID)
	if id := latestCompletedRugbyMatch(fixtures); id != "" {
		c.save("rugbylive/match.json", base+"/match/"+id)
	}
}

// latestCompletedRugbyMatch returns the most recent finished fixture, which is
// the one whose match document carries teamsheets, statistics and events.
func latestCompletedRugbyMatch(doc any) string {
	obj, ok := doc.(map[string]any)
	if !ok {
		return ""
	}
	rows, _ := obj["results"].([]any)
	var bestID, bestDate string
	for _, r := range rows {
		row, ok := r.(map[string]any)
		if !ok {
			continue
		}
		status, _ := row["status"].(string)
		if !strings.EqualFold(status, "Result") {
			continue
		}
		date, _ := row["date"].(string)
		if date > bestDate {
			bestDate, bestID = date, scalar(row["id"])
		}
	}
	return bestID
}

// captureGolf refreshes the golf capture.
//
// One call: the leaderboard is the whole document, and the player rows and
// round records are projections of it rather than separate endpoints.
func (c *capturer) captureGolf(host string) {
	fmt.Println("\n== live-golf-data (RapidAPI) ==")
	base := "https://" + host
	// A completed tournament, so the capture carries full four-round scoring
	// rather than a card of empty rows.
	c.save("golfdata/leaderboard.json",
		base+"/leaderboard?orgId=1&tournId=060&year=2026")
}
