package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// maxArraySamples bounds how many elements of an array contribute paths. It
// only needs to be large enough to reach records that appear late in a
// collection; the whole array is not walked because a 1,700-element player list
// would cost a lot to learn nothing new.
//
// It has been raised twice, both times because a real field was being reported
// as spurious: from 3 to 30 for AllScores' trailing match-level set tally, and
// from 30 to 100 for the multi-league scoreboard, where stoppage time and the
// group labels appear only on the fifty-third fixture of sixty-five.
const maxArraySamples = 100

// pathSet is the set of JSON paths present in a document, which is how two
// payloads are compared for shape rather than for content.
type pathSet map[string]bool

// walk records every path in doc. Arrays collapse to a single "[]" segment and
// only the first few elements are inspected: the goal is the shape a consumer
// would bind to, not an exhaustive enumeration of a long list.
func walk(doc any, prefix string, out pathSet, depth int) {
	const maxDepth = 6
	if depth > maxDepth {
		return
	}
	switch t := doc.(type) {
	case map[string]any:
		for k, v := range t {
			p := k
			if prefix != "" {
				p = prefix + "." + k
			}
			out[p] = true
			walk(v, p, out, depth+1)
		}
	case []any:
		p := prefix + "[]"
		out[p] = true
		// Sample enough elements to reach records that only appear late in a
		// collection. AllScores appends a match-level "Sets" tally after the
		// per-set entries, so a cap of three missed it entirely and reported a
		// field we emit as spurious.
		for i, item := range t {
			if i >= maxArraySamples {
				break
			}
			walk(item, p, out, depth+1)
		}
	}
}

func pathsOf(doc any) pathSet {
	out := pathSet{}
	walk(doc, "", out, 0)
	return out
}

// pathsOfJSON parses raw JSON and returns its path set.
func pathsOfJSON(raw []byte) (pathSet, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	return pathsOf(doc), nil
}

func (p pathSet) union(other pathSet) {
	for k := range other {
		p[k] = true
	}
}

// diff returns the paths present in want but not in got, and vice versa.
func (p pathSet) diff(other pathSet) (missing, extra []string) {
	for k := range p {
		if !other[k] {
			missing = append(missing, k)
		}
	}
	for k := range other {
		if !p[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return missing, extra
}

func (p pathSet) overlap(other pathSet) int {
	n := 0
	for k := range p {
		if other[k] {
			n++
		}
	}
	return n
}

// leaf trims a path to its final segment, for compact reporting.
func leaf(path string) string {
	if i := strings.LastIndex(path, "."); i >= 0 {
		return path[i+1:]
	}
	return path
}

func joinLeaves(paths []string, limit int) string {
	seen := map[string]bool{}
	var out []string
	for _, p := range paths {
		l := leaf(p)
		if seen[l] {
			continue
		}
		seen[l] = true
		out = append(out, l)
		if len(out) >= limit {
			out = append(out, fmt.Sprintf("… (+%d more)", len(paths)-len(out)))
			break
		}
	}
	return strings.Join(out, ", ")
}
