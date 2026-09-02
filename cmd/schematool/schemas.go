package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

// ticksPerFeed is how far each feed is advanced before comparing. A single
// payload understates the schema: arrays such as ScoringPlays are empty early
// in a game and only reveal their inner fields once something has happened, so
// the paths are unioned across a whole simulated fixture.
const ticksPerFeed = 1500

func runSchemas(fixtureRoot string, verbose, fullPaths bool) error {
	fmt.Printf("%-8s %-20s %11s %9s %7s   %s\n",
		"SPORT", "FEED", "REAL PATHS", "MATCHED", "COVER", "NOTES")

	type result struct {
		ep             generators.Endpoint
		total, matched int
		missing, extra []string
		bound          bool
	}
	var results []result

	for _, ep := range generators.Endpoints() {
		b, ok := bindingFor(ep.Sport, ep.Ref())
		if !ok {
			results = append(results, result{ep: ep})
			continue
		}
		doc, err := load(fixtureRoot, b.file)
		if err != nil {
			return err
		}
		if b.unwrap != nil {
			doc = b.unwrap(doc)
		}
		want := pathsOf(doc)

		// The generator runs in-process. The Python version shelled out to the
		// loadtest binary and parsed its stdout; calling the registry directly
		// is both faster and immune to output-format drift.
		feed, err := generators.NewNamed(ep.Sport, ep.Kind, ep.Name, 13)
		if err != nil {
			return fmt.Errorf("%s/%s: %w", ep.Sport, ep.Ref(), err)
		}
		got := pathSet{}
		for i := 0; i < ticksPerFeed; i++ {
			raw, err := json.Marshal(feed.Next().Payload)
			if err != nil {
				return fmt.Errorf("%s/%s: marshal: %w", ep.Sport, ep.Ref(), err)
			}
			ps, err := pathsOfJSON(raw)
			if err != nil {
				return err
			}
			got.union(ps)
		}

		missing, extra := want.diff(got)
		results = append(results, result{
			ep: ep, total: len(want), matched: want.overlap(got),
			missing: missing, extra: extra, bound: true,
		})
	}

	verified, unbound := 0, 0
	for _, r := range results {
		if !r.bound {
			unbound++
			fmt.Printf("%-8s %-20s %11s %9s %7s   no capture bound (%s)\n",
				r.ep.Sport, r.ep.Ref(), "-", "-", "-", r.ep.Provenance)
			continue
		}
		pct := 0
		if r.total > 0 {
			pct = 100 * r.matched / r.total
		}
		flag := ""
		if pct < 100 {
			flag = "   <-- GAP"
		} else {
			verified++
		}
		fmt.Printf("%-8s %-20s %11d %9d %6d%%   %d missing / %d extra%s\n",
			r.ep.Sport, r.ep.Ref(), r.total, r.matched, pct,
			len(r.missing), len(r.extra), flag)
	}

	fmt.Printf("\n%d feeds compared against captures, %d at full coverage; %d have no capture bound.\n",
		len(results)-unbound, verified, unbound)

	if verbose {
		fmt.Println("\n================ FIELD-LEVEL DIFF ================")
		for _, r := range results {
			if !r.bound || (len(r.missing) == 0 && len(r.extra) == 0) {
				continue
			}
			fmt.Printf("\n### %s/%s\n", r.ep.Sport, r.ep.Ref())
			if len(r.missing) > 0 {
				fmt.Printf("  MISSING from ours (%d):\n    %s\n", len(r.missing), render(r.missing, fullPaths))
			}
			if len(r.extra) > 0 {
				fmt.Printf("  EXTRA in ours (%d):\n    %s\n", len(r.extra), render(r.extra, fullPaths))
			}
		}
	}
	return nil
}

// render formats a diff. Leaf names read well for a quick scan, but they
// collapse two different paths that end in the same field — "id" under an
// injury and "id" under a suspension look identical — so -paths prints the
// whole path when the short form stops being enough to act on.
func render(paths []string, full bool) string {
	if !full {
		return joinLeaves(paths, 40)
	}
	return strings.Join(paths, "\n    ")
}
