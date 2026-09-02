package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
)

// captureAPISports refreshes the primary provider's captures.
//
// One bulk call per vertical — the same call the production sweeper makes — so
// what lands on disk is exactly the document the pipeline ingests. That is the
// point: the simulation generators replay these, so simulation and production
// carry identical shapes by construction rather than by a promise in a comment.
//
// Two provider behaviours this has to work around, both discovered by running
// it rather than by reading the docs:
//
//   - A rejected request returns HTTP 200 with the reason in `errors`. An
//     earlier version of this function happily wrote those error envelopes to
//     disk and reported "ok", which would have made every downstream shape
//     check compare against a 250-byte error document.
//   - The free plan only serves a ±1 day window ("Free plans do not have access
//     to this date, try from 2026-08-31 to 2026-09-02") and, for Formula 1,
//     only seasons 2022-2024. So walking back through a week of dates to find
//     fixtures does not work on free; it just collects paywall messages.
func (c *capturer) captureAPISports() {
	fmt.Println("\n== API-Sports ==")
	now := time.Now()

	for _, v := range apisports.Verticals() {
		spec, _ := apisports.SpecFor(v)

		// Candidate parameter sets, best first. The free plan's window is
		// yesterday..tomorrow, so that is as far as the fallbacks reach.
		candidates := []map[string]string{spec.BulkQuery(now)}
		switch spec.Mode {
		case apisports.BulkDate:
			candidates = append(candidates,
				spec.BulkQuery(now.AddDate(0, 0, -1)),
				spec.BulkQuery(now.AddDate(0, 0, 1)))
		case apisports.BulkSeason:
			// Free plans are capped at 2024 for motorsport; try the current
			// season first so a paid key still captures live data.
			for _, season := range []string{"2024", "2023"} {
				candidates = append(candidates, map[string]string{"season": season})
			}
		case apisports.BulkLive:
			// live=all is always in-window, but returns nothing out of season;
			// fall back to today's card to capture the document shape.
			candidates = append(candidates, map[string]string{"date": now.Format("2006-01-02")})
		}

		name := fmt.Sprintf("apisports/%s.json", v)
		if !c.captureFirstUsable(name, spec, candidates) {
			fmt.Printf("warn %-38s no usable response (out of season, or the plan does not cover it)\n", name)
		}
	}
}

// captureFirstUsable tries each candidate and keeps the first response that is
// both error-free and non-empty, writing nothing otherwise.
func (c *capturer) captureFirstUsable(name string, spec apisports.Spec, candidates []map[string]string) bool {
	var fallback []byte // an error-free but empty response, kept only as a last resort
	for _, params := range candidates {
		url := spec.BaseURL() + spec.BulkPath + "?" + encodeParams(params)
		body, err := c.fetch(url)
		if err != nil {
			continue
		}
		if reason := envelopeError(body); reason != "" {
			fmt.Printf("skip %-38s %s\n", name, reason)
			continue
		}
		if envelopeResults(body) > 0 {
			return c.writeCapture(name, body)
		}
		if fallback == nil {
			fallback = body
		}
	}
	if fallback != nil {
		// Shape-less but honest: the endpoint works and is simply empty today.
		fmt.Printf("warn %-38s empty card (endpoint valid, nothing scheduled)\n", name)
		return c.writeCapture(name, fallback)
	}
	return false
}

// fetch performs one authenticated GET, returning the raw body.
func (c *capturer) fetch(url string) ([]byte, error) {
	_, body, err := c.get(url)
	return body, err
}

// writeCapture stores a response and counts it.
func (c *capturer) writeCapture(name string, body []byte) bool {
	path := filepath.Join(c.root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		c.failed++
		return false
	}
	var pretty map[string]any
	out := body
	if json.Unmarshal(body, &pretty) == nil {
		if b, err := json.MarshalIndent(pretty, "", "  "); err == nil {
			out = b
		}
	}
	if err := os.WriteFile(path, out, 0o644); err != nil {
		fmt.Printf("FAIL %-38s %v\n", name, err)
		c.failed++
		return false
	}
	fmt.Printf("ok   %-38s %7d B  %d result(s)\n", name, len(out), envelopeResults(body))
	c.ok++
	return true
}

// envelopeError returns the provider's reason when a 200 carries one.
func envelopeError(body []byte) string {
	var env struct {
		Errors json.RawMessage `json:"errors"`
	}
	if json.Unmarshal(body, &env) != nil || len(env.Errors) == 0 {
		return ""
	}
	var asObject map[string]any
	if json.Unmarshal(env.Errors, &asObject) == nil && len(asObject) > 0 {
		for k, v := range asObject {
			return fmt.Sprintf("%s: %v", k, v)
		}
	}
	return ""
}

func envelopeResults(body []byte) int {
	var env struct {
		Results int `json:"results"`
	}
	if json.Unmarshal(body, &env) != nil {
		return 0
	}
	return env.Results
}

// encodeParams renders a sorted query string. Values here are dates and fixed
// tokens, so no escaping is needed; sorting keeps the URL deterministic.
func encodeParams(params map[string]string) string {
	out := ""
	for _, k := range sortedParamKeys(params) {
		if out != "" {
			out += "&"
		}
		out += k + "=" + params[k]
	}
	return out
}

func sortedParamKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
