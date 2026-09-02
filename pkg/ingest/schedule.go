package ingest

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
)

// MatchState is what a vertical is currently doing, which is the dominant input
// to how often it is worth polling.
type MatchState int

const (
	// StateIdle: nothing scheduled soon. The cheapest state.
	StateIdle MatchState = iota
	// StatePregame: fixtures scheduled but not started.
	StatePregame
	// StateLive: at least one fixture in play.
	StateLive
	// StateBreak: half-time, an intermission, a between-innings gap.
	StateBreak
	// StateFinal: everything today has finished.
	StateFinal
)

func (s MatchState) String() string {
	switch s {
	case StatePregame:
		return "pregame"
	case StateLive:
		return "live"
	case StateBreak:
		return "break"
	case StateFinal:
		return "final"
	default:
		return "idle"
	}
}

// Cadence is the target polling interval for a state, before any budget or
// crowd adjustment.
//
// These are the targets the product asks for. Whether a venue can afford them
// is a separate question, answered by Plan below — and on the free tier the
// answer is usually no. Keeping the target and the affordable rate as distinct
// numbers is deliberate: the dashboard shows both, so an operator can see that
// a sport is polling every four minutes because of the plan, not because of a
// bug.
type Cadence struct {
	Target time.Duration
	// Floor is the fastest this state is ever polled, whatever the budget says.
	Floor time.Duration
}

// cadences are the state-driven targets.
var cadences = map[MatchState]Cadence{
	StateLive:    {Target: 8 * time.Second, Floor: 5 * time.Second},
	StateBreak:   {Target: 150 * time.Second, Floor: 60 * time.Second},
	StatePregame: {Target: 12 * time.Minute, Floor: 5 * time.Minute},
	StateFinal:   {Target: 15 * time.Minute, Floor: 10 * time.Minute},
	StateIdle:    {Target: 30 * time.Minute, Floor: 15 * time.Minute},
}

// CadenceFor returns the target cadence for a state.
func CadenceFor(s MatchState) Cadence { return cadences[s] }

// Plan is a vertical's computed polling decision.
type Plan struct {
	Vertical  apisports.Vertical `json:"vertical"`
	State     MatchState         `json:"-"`
	StateName string             `json:"state"`
	// Target is the cadence the state alone would ask for.
	Target time.Duration `json:"-"`
	// Interval is what the venue can actually afford, after budget and crowd
	// weighting. This is the number the scheduler sleeps on.
	Interval        time.Duration `json:"-"`
	TargetSeconds   float64       `json:"target_seconds"`
	IntervalSeconds float64       `json:"interval_seconds"`
	// Share is the vertical's crowd-weighted share of the budget.
	Share float64 `json:"share"`
	// Budget is its daily request allowance.
	Budget int `json:"budget"`
	// Deferred is true when the vertical has stood down to protect the reserve.
	Deferred bool `json:"deferred"`
	// Reason explains the interval, for the dashboard and logs.
	Reason string `json:"reason"`
}

// Scheduler turns licence, budget, crowd weights and match state into a
// per-vertical polling plan.
//
// The central calculation is embarrassingly simple and easy to get wrong: a
// vertical with B requests left and S seconds remaining in the day can afford
// one request every S/B seconds. Everything else — crowd weights, states,
// targets — decides how B is shared and what cadence to *aim* for; this line
// decides what is actually sustainable.
type Scheduler struct {
	limiter *Limiter
	weights *CrowdWeights
	now     func() time.Time

	states map[apisports.Vertical]MatchState
}

// NewScheduler builds a scheduler over a limiter.
func NewScheduler(l *Limiter, w *CrowdWeights, now func() time.Time) *Scheduler {
	if now == nil {
		now = time.Now
	}
	return &Scheduler{
		limiter: l, weights: w, now: now,
		states: map[apisports.Vertical]MatchState{},
	}
}

// SetState records what a vertical is doing. The scheduler learns this from the
// bulk sweep it just performed, so it costs no extra requests.
func (s *Scheduler) SetState(v apisports.Vertical, st MatchState) {
	s.states[v] = st
}

// State returns a vertical's last known state.
func (s *Scheduler) State(v apisports.Vertical) MatchState { return s.states[v] }

// PlanFor computes the polling decision for one vertical.
func (s *Scheduler) PlanFor(v apisports.Vertical) Plan {
	now := s.now()
	state := s.states[v]
	cad := cadences[state]

	var budget Budget
	for _, b := range s.limiter.Budgets() {
		if b.Vertical == v {
			budget = b
			break
		}
	}
	usage := s.limiter.Usage()
	pressure := usage.Pressure(v)

	plan := Plan{
		Vertical: v, State: state, StateName: state.String(),
		Target: cad.Target, Share: budget.Weight, Budget: budget.Requests,
	}

	// Requests still available to this vertical today.
	remaining := budget.Requests - int(math.Round(pressure*float64(budget.Requests)))
	if remaining < 0 {
		remaining = 0
	}
	secondsLeft := time.Until(startOfDay(now).Add(24 * time.Hour)).Seconds()
	if d := startOfDay(now).Add(24 * time.Hour).Sub(now).Seconds(); d > 0 {
		secondsLeft = d
	}
	if secondsLeft < 1 {
		secondsLeft = 1
	}

	switch {
	case remaining <= 0:
		plan.Interval = 30 * time.Minute
		plan.Deferred = true
		plan.Reason = "daily budget spent; holding until the day rolls over"
	case usage.ShouldDefer(v) && state != StateLive:
		// Past the defer threshold, only live action is still worth spending on.
		plan.Interval = 20 * time.Minute
		plan.Deferred = true
		plan.Reason = fmt.Sprintf("budget %d%% spent; non-live polling stood down", int(pressure*100))
	default:
		// The affordable cadence: spread what is left over what remains of the
		// day. This is what makes a free-tier venue survive to closing time.
		affordable := time.Duration(secondsLeft/float64(remaining)) * time.Second
		plan.Interval = cad.Target
		plan.Reason = "target cadence for " + state.String()
		if affordable > cad.Target {
			plan.Interval = affordable
			plan.Reason = fmt.Sprintf(
				"budget-limited: %d requests left for %s of the day",
				remaining, roundDur(time.Duration(secondsLeft)*time.Second))
		}
		// A crowd favourite may spend faster than its even share, up to the
		// state's floor — this is where audience interest actually buys
		// something rather than just re-ranking a list.
		if boost := s.crowdBoost(v); boost > 1 {
			sped := time.Duration(float64(plan.Interval) / boost)
			if sped < cad.Floor {
				sped = cad.Floor
			}
			if sped < plan.Interval {
				plan.Interval = sped
				plan.Reason += "; crowd-boosted"
			}
		}
	}

	if plan.Interval < cad.Floor {
		plan.Interval = cad.Floor
	}
	plan.TargetSeconds = plan.Target.Seconds()
	plan.IntervalSeconds = plan.Interval.Seconds()
	return plan
}

// crowdBoost is how much faster than its even share a vertical may poll.
//
// Capped at 2x. Without a cap, a single dominant sport on a venue with one
// screen would starve every other vertical to a 30-minute cadence, and the
// system would miss the moment a second sport becomes interesting — which is
// exactly when the allocator needs to react.
func (s *Scheduler) crowdBoost(v apisports.Vertical) float64 {
	if s.weights == nil {
		return 1
	}
	verticals := make([]apisports.Vertical, 0, len(s.states))
	for vv := range s.states {
		verticals = append(verticals, vv)
	}
	if len(verticals) < 2 {
		return 1
	}
	shares := s.weights.Shares(verticals)
	even := 1.0 / float64(len(verticals))
	if even <= 0 {
		return 1
	}
	boost := shares[v] / even
	if boost > 2 {
		return 2
	}
	if boost < 1 {
		return 1
	}
	return boost
}

// Plans returns the decision for every licensed vertical, busiest first.
func (s *Scheduler) Plans() []Plan {
	out := make([]Plan, 0, len(s.states))
	for v := range s.states {
		out = append(out, s.PlanFor(v))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Interval != out[j].Interval {
			return out[i].Interval < out[j].Interval
		}
		return out[i].Vertical < out[j].Vertical
	})
	return out
}

// Register makes a vertical known to the scheduler at its idle state.
func (s *Scheduler) Register(v apisports.Vertical) {
	if _, ok := s.states[v]; !ok {
		s.states[v] = StateIdle
	}
}

// AffordableLiveCadence reports the fastest live cadence a tier can sustain for
// n verticals across a whole day, which is what the README and the dashboard
// use to tell an operator what their plan actually buys.
//
// It is a blunt, honest number: total daily budget divided across the verticals
// and spread over 24 hours. A venue only cares about a few hours of that, so
// concentrating the budget does better — but as a plan-sizing figure it is the
// one that stops someone expecting 5-second polling on 100 requests a day.
func AffordableLiveCadence(dailyPerHost int, hoursOfPlay float64) time.Duration {
	if dailyPerHost <= 0 || hoursOfPlay <= 0 {
		return 0
	}
	usable := float64(dailyPerHost) * safetyMargin
	return time.Duration((hoursOfPlay * 3600 / usable) * float64(time.Second))
}

func roundDur(d time.Duration) time.Duration { return d.Round(time.Minute) }
