package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
)

// Sweep is the result of one bulk poll of a vertical.
type Sweep struct {
	Vertical apisports.Vertical
	// Fixtures are the raw provider documents, untouched. The pipeline's
	// contract is that Kafka carries the provider's own JSON, so nothing is
	// reshaped on the way through.
	Fixtures []json.RawMessage
	// Live, Break, Upcoming and Finished are the state tally, derived from the
	// same response — so knowing what to do next costs no extra request.
	Live, Break, Upcoming, Finished int
	State                           MatchState
	FetchedAt                       time.Time
	// Drift is the real-time fidelity measurement for this sweep. Computed
	// here because this is the only place that has the response headers, the
	// fetch time and the fixtures together.
	Drift Drift
}

// Sweeper performs bulk sweeps and derives state from them.
//
// This replaces the per-fixture polling the pipeline used to do. The difference
// is not marginal: a Saturday card of 40 soccer fixtures cost 40 requests per
// cycle before and costs 1 now. On a 100/day budget that is the difference
// between two and a half cycles a day and a hundred.
type Sweeper struct {
	client *apisports.Client
	now    func() time.Time
}

// NewSweeper builds a sweeper.
func NewSweeper(c *apisports.Client, now func() time.Time) *Sweeper {
	if now == nil {
		now = time.Now
	}
	return &Sweeper{client: c, now: now}
}

// Sweep performs one bulk fetch for a vertical.
func (s *Sweeper) Sweep(ctx context.Context, v apisports.Vertical) (*Sweep, error) {
	spec, ok := apisports.SpecFor(v)
	if !ok {
		return nil, fmt.Errorf("ingest: unknown vertical %q", v)
	}
	now := s.now()
	env, err := s.client.Get(ctx, v, spec.BulkPath, spec.BulkQuery(now))
	if err != nil {
		return nil, err
	}

	var rows []json.RawMessage
	if len(env.Response) > 0 {
		if err := json.Unmarshal(env.Response, &rows); err != nil {
			// A vertical that returns an object rather than an array is still
			// a valid single-document answer; carry it as one row.
			rows = []json.RawMessage{env.Response}
		}
	}

	fetchedAt := now
	if !env.ReceivedAt.IsZero() {
		fetchedAt = env.ReceivedAt
	}
	out := &Sweep{Vertical: v, Fixtures: rows, FetchedAt: fetchedAt}
	out.Drift = MeasureDrift(rows, fetchedAt, env.Header, s.now())
	for _, row := range rows {
		switch classify(row) {
		case StateLive:
			out.Live++
		case StateBreak:
			out.Break++
		case StatePregame:
			out.Upcoming++
		case StateFinal:
			out.Finished++
		}
	}
	out.State = deriveState(out)
	return out, nil
}

// deriveState reduces a fixture tally to the vertical's polling state.
//
// Live beats everything: if anything at all is in play, the vertical polls at
// live cadence. A break only counts when nothing is live, because a venue with
// one game at half-time and another in the third quarter is not on a break.
func deriveState(s *Sweep) MatchState {
	switch {
	case s.Live > 0:
		return StateLive
	case s.Break > 0:
		return StateBreak
	case s.Upcoming > 0:
		return StatePregame
	case s.Finished > 0:
		return StateFinal
	default:
		return StateIdle
	}
}

// statusHolder is the shape common to every vertical's status block. The
// verticals differ in almost everything else, but all of them report status as
// either a nested object with short/long, or a bare string.
type statusHolder struct {
	Fixture struct {
		Status struct {
			Short string `json:"short"`
			Long  string `json:"long"`
		} `json:"status"`
	} `json:"fixture"`
	Status json.RawMessage `json:"status"`
	Game   struct {
		Status struct {
			Short string `json:"short"`
			Long  string `json:"long"`
		} `json:"status"`
	} `json:"game"`
}

// classify maps a provider status onto a polling state.
//
// The status vocabulary is per-vertical and not documented in one place, so
// this works from the tokens rather than an exhaustive enum: an unknown status
// falls through to idle instead of being mistaken for live, which is the safe
// direction on a metered budget.
func classify(row json.RawMessage) MatchState {
	var h statusHolder
	if err := json.Unmarshal(row, &h); err != nil {
		return StateIdle
	}
	short, long := h.Fixture.Status.Short, h.Fixture.Status.Long
	if short == "" && long == "" {
		short, long = h.Game.Status.Short, h.Game.Status.Long
	}
	if short == "" && long == "" && len(h.Status) > 0 {
		// The bare-string form, and the flat-object form.
		var asString string
		if err := json.Unmarshal(h.Status, &asString); err == nil {
			short = asString
		} else {
			var obj struct {
				Short string `json:"short"`
				Long  string `json:"long"`
			}
			if err := json.Unmarshal(h.Status, &obj); err == nil {
				short, long = obj.Short, obj.Long
			}
		}
	}
	return classifyStatus(short, long)
}

// classifyStatus is the token table, kept separate so it is directly testable.
func classifyStatus(short, long string) MatchState {
	s := strings.ToUpper(strings.TrimSpace(short))
	l := strings.ToLower(strings.TrimSpace(long))

	switch s {
	// Breaks. Checked before live, because "HT" would otherwise match nothing
	// and fall through to idle.
	case "HT", "BT", "INT", "BREAK", "AWH", "END_OF_PERIOD":
		return StateBreak
	// In play. The per-vertical spellings: halves, quarters, periods, innings,
	// overtime, penalties.
	case "1H", "2H", "3H", "4H", "ET", "P", "PEN", "LIVE", "IN_PLAY",
		"Q1", "Q2", "Q3", "Q4", "OT", "P1", "P2", "P3", "IN", "SUSP", "S1", "S2", "S3", "S4", "S5":
		return StateLive
	// Finished.
	case "FT", "AET", "PENS", "AOT", "AP", "END", "FIN", "FINISHED", "AFTER_OVER_TIME":
		return StateFinal
	// Not started.
	case "NS", "TBD", "SCHEDULED", "POSTPONED", "PST", "CANC", "ABD", "AWD", "WO":
		return StatePregame
	}

	// Fall back to the long form, which several verticals populate more
	// reliably than the short one.
	switch {
	case strings.Contains(l, "half time"), strings.Contains(l, "halftime"),
		strings.Contains(l, "break"), strings.Contains(l, "intermission"):
		return StateBreak
	case strings.Contains(l, "in play"), strings.Contains(l, "live"),
		strings.Contains(l, "quarter"), strings.Contains(l, "period"),
		strings.Contains(l, "half"), strings.Contains(l, "overtime"),
		strings.Contains(l, "inning"), strings.Contains(l, "set "):
		return StateLive
	case strings.Contains(l, "finished"), strings.Contains(l, "final"),
		strings.Contains(l, "ended"), strings.Contains(l, "after "):
		return StateFinal
	case strings.Contains(l, "not started"), strings.Contains(l, "scheduled"),
		strings.Contains(l, "postponed"), strings.Contains(l, "cancelled"):
		return StatePregame
	}
	return StateIdle
}
