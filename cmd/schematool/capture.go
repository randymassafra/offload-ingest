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

	"github.com/offloadintelligence/offload-ingest/internal/config"
)

// runCapture refreshes the provider responses under the fixtures root.
//
// The comparison and route checks are only as good as what they compare
// against, so capturing has to be a first-class, repeatable step rather than an
// ad-hoc script. Everything here is discovery-driven: it fetches a schedule,
// picks a completed fixture out of it, and follows that id into the box score,
// so a refresh does not depend on hand-maintained game ids going stale.
func runCapture(root string, providers string) error {
	if _, err := config.LoadEnv(""); err != nil {
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
			"sportsdataio": true, "cricbuzz": true, "allscores": true,
			"rugbylive": true, "apisports": true,
		}
	}

	c := &capturer{root: root, client: &http.Client{Timeout: 60 * time.Second}}

	if want["sportsdataio"] {
		key := config.APIKey()
		if key == "" {
			return fmt.Errorf("SPORTS_DATA_IO_API_KEY is not set")
		}
		c.header = map[string]string{"Ocp-Apim-Subscription-Key": key}
		c.captureSportsDataIO()
	}
	// RapidAPI-hosted providers all authenticate identically, so each one is a
	// host name and a capture function.
	for _, p := range []struct {
		name    string
		hostEnv string
		fn      func(string)
	}{
		{"cricbuzz", "RAPIDAPI_CRICKET_HOST", c.captureCricbuzz},
		{"allscores", "RAPIDAPI_ALLSCORES_HOST", c.captureAllScores},
		{"rugbylive", "RAPIDAPI_RUGBY_HOST", c.captureRugbyLive},
	} {
		if !want[p.name] {
			continue
		}
		key, host := os.Getenv("RAPIDAPI_KEY"), os.Getenv(p.hostEnv)
		if key == "" || host == "" {
			fmt.Fprintf(os.Stderr, "skipping %s: RAPIDAPI_KEY / %s not set\n", p.name, p.hostEnv)
			continue
		}
		c.header = map[string]string{
			"x-rapidapi-key":  key,
			"x-rapidapi-host": host,
			"Content-Type":    "application/json",
		}
		p.fn(host)
	}

	if want["apisports"] {
		key := os.Getenv("APISPORTS_KEY")
		if key == "" {
			fmt.Fprintln(os.Stderr, "skipping apisports: APISPORTS_KEY not set")
		} else {
			c.header = map[string]string{"x-apisports-key": key}
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

const sdio = "https://api.sportsdata.io"

func (c *capturer) captureSportsDataIO() {
	fmt.Println("== SportsDataIO ==")

	// The trial key is authorised for 2025 onwards; 2025 is the most recent
	// season with completed fixtures across every sport.
	const season, week = "2025", "1"

	// NFL
	c.save("sportsdataio/nfl/teams.json", sdio+"/v3/nfl/scores/json/Teams")
	c.save("sportsdataio/nfl/players.json", sdio+"/v3/nfl/scores/json/PlayersByAvailable")
	scores := c.save("sportsdataio/nfl/scores_by_week.json",
		sdio+"/v3/nfl/scores/json/ScoresByWeek/"+season+"/"+week)
	if ht, ok := pickID(scores, "HomeTeam", "IsClosed"); ok {
		c.save("sportsdataio/nfl/boxscore_v3.json",
			sdio+"/v3/nfl/stats/json/BoxScoreV3/"+season+"/"+week+"/"+ht)
		c.save("sportsdataio/nfl/playbyplay.json",
			sdio+"/v3/nfl/pbp/json/PlayByPlay/"+season+"/"+week+"/"+ht)
	}
	c.save("sportsdataio/nfl/player_game_stats.json",
		sdio+"/v3/nfl/stats/json/PlayerGameStatsByWeek/"+season+"/"+week)
	c.save("sportsdataio/nfl/team_game_stats.json",
		sdio+"/v3/nfl/scores/json/TeamGameStats/"+season+"/"+week)

	// NBA
	const nbaDate = "2026-JAN-15"
	c.save("sportsdataio/nba/teams.json", sdio+"/v3/nba/scores/json/teams")
	c.save("sportsdataio/nba/players.json", sdio+"/v3/nba/scores/json/Players")
	games := c.save("sportsdataio/nba/games_by_date.json",
		sdio+"/v3/nba/scores/json/GamesByDate/"+nbaDate)
	if id, ok := pickID(games, "GameID", "Status"); ok {
		c.save("sportsdataio/nba/boxscore.json", sdio+"/v3/nba/stats/json/BoxScore/"+id)
		c.save("sportsdataio/nba/playbyplay.json", sdio+"/v3/nba/pbp/json/PlayByPlay/"+id)
	}
	c.save("sportsdataio/nba/player_game_stats.json",
		sdio+"/v3/nba/stats/json/PlayerGameStatsByDate/"+nbaDate)
	c.save("sportsdataio/nba/team_game_stats.json",
		sdio+"/v3/nba/scores/json/TeamGameStatsByDate/"+nbaDate)

	// NCAA Football
	c.save("sportsdataio/ncaaf/teams.json", sdio+"/v3/cfb/scores/json/Teams")
	c.save("sportsdataio/ncaaf/players.json", sdio+"/v3/cfb/scores/json/Players")
	cfb := c.save("sportsdataio/ncaaf/games_by_week.json",
		sdio+"/v3/cfb/scores/json/GamesByWeek/"+season+"/"+week)
	if id, ok := pickID(cfb, "GameID", "Status"); ok {
		c.save("sportsdataio/ncaaf/boxscore.json", sdio+"/v3/cfb/stats/json/BoxScore/"+id)
	}
	c.save("sportsdataio/ncaaf/player_game_stats.json",
		sdio+"/v3/cfb/stats/json/PlayerGameStatsByWeek/"+season+"/"+week)

	// NCAA Basketball
	const cbbDate = "2026-JAN-15"
	c.save("sportsdataio/ncaab/teams.json", sdio+"/v3/cbb/scores/json/teams")
	cbb := c.save("sportsdataio/ncaab/games_by_date.json",
		sdio+"/v3/cbb/scores/json/GamesByDate/"+cbbDate)
	if id, ok := pickID(cbb, "GameID", "Status"); ok {
		c.save("sportsdataio/ncaab/boxscore.json", sdio+"/v3/cbb/stats/json/BoxScore/"+id)
	}
	c.save("sportsdataio/ncaab/player_game_stats.json",
		sdio+"/v3/cbb/stats/json/PlayerGameStatsByDate/"+cbbDate)

	// Soccer is no longer a SportsDataIO sport. The trial key licensed exactly
	// one competition — the UEFA Champions League, id 3 — and every other
	// competition returned an empty array, so the sport is served by AllScores
	// instead. See captureAllScores.

	// Golf
	c.save("sportsdataio/golf/players.json", sdio+"/golf/v2/json/Players")
	tours := c.save("sportsdataio/golf/tournaments.json", sdio+"/golf/v2/json/Tournaments")
	if id, ok := pickID(tours, "TournamentID", "IsOver"); ok {
		c.save("sportsdataio/golf/leaderboard.json", sdio+"/golf/v2/json/Leaderboard/"+id)
	}

	// MMA / UFC — one API, UFC is a league inside it.
	c.save("sportsdataio/mma/leagues.json", sdio+"/v3/mma/scores/json/Leagues")
	c.save("sportsdataio/mma/fighters.json", sdio+"/v3/mma/scores/json/Fighters")
	sched := c.save("sportsdataio/mma/schedule.json", sdio+"/v3/mma/scores/json/Schedule/UFC/"+season)
	if id, ok := pickID(sched, "EventId", "Status"); ok {
		c.save("sportsdataio/mma/event.json", sdio+"/v3/mma/scores/json/Event/"+id)
	}

	// NASCAR. Both seasons are captured: the models must hold across them, and
	// a diff of the two is what proves the schema is stable season to season.
	c.save("sportsdataio/nascar/series.json", sdio+"/nascar/v2/json/series")
	c.save("sportsdataio/nascar/drivers.json", sdio+"/nascar/v2/json/drivers")
	for _, s := range []string{"2025", "2026"} {
		suffix := ""
		if s != "2025" {
			suffix = "_" + s
		}
		races := c.save("sportsdataio/nascar/races"+suffix+".json", sdio+"/nascar/v2/json/races/"+s)
		if id, ok := pickID(races, "RaceID", "IsOver"); ok {
			c.save("sportsdataio/nascar/race_result"+suffix+".json",
				sdio+"/nascar/v2/json/RaceResult/"+id)
		}
	}
}

// captureCricbuzz refreshes the cricket captures.
//
// SportsDataIO does not offer cricket — every route 404s — so this is a second
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
// SportsDataIO sells a tennis feed but this key is not scoped to it, so tennis
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
// AllScores does not use SportsDataIO's "Final": a completed match is "Ended",
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
