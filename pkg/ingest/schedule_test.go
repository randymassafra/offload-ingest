package ingest

import (
	"testing"
	"time"

	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
	"github.com/offloadintelligence/offload-ingest/pkg/licensing"
)

// TestStateDrivesCadence pins the requested polling ladder: live fast, breaks
// medium, pre-game and final slow.
func TestStateDrivesCadence(t *testing.T) {
	for _, tc := range []struct {
		state   MatchState
		atLeast time.Duration
		atMost  time.Duration
	}{
		{StateLive, 5 * time.Second, 10 * time.Second},
		{StateBreak, 2 * time.Minute, 3 * time.Minute},
		{StatePregame, 10 * time.Minute, 15 * time.Minute},
		{StateFinal, 10 * time.Minute, 15 * time.Minute},
	} {
		got := CadenceFor(tc.state).Target
		if got < tc.atLeast || got > tc.atMost {
			t.Errorf("%s target = %s, want between %s and %s",
				tc.state, got, tc.atLeast, tc.atMost)
		}
	}
}

// TestFreeTierCannotAffordLiveCadence is the finding that shaped the scheduler.
//
// The brief asks for 5-10 second polling during live action. On the free plan
// that is arithmetically impossible: 100 requests/day against a three-hour
// window is one request every ~2 minutes. The scheduler must therefore report
// the affordable interval rather than the target, and must NOT quietly poll at
// the target and exhaust the venue's day before kick-off.
func TestFreeTierCannotAffordLiveCadence(t *testing.T) {
	tier, _ := licensing.Tier{Name: licensing.TierFree}.Resolve()
	v := apisports.VerticalFootball
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	l, err := NewLimiter(LimiterConfig{
		Tier: tier, Verticals: []apisports.Vertical{v},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	s := NewScheduler(l, NewCrowdWeights(nil), func() time.Time { return now })
	s.SetState(v, StateLive)

	plan := s.PlanFor(v)
	if plan.Target > 10*time.Second {
		t.Errorf("live target = %s, want the 5-10s the brief asks for", plan.Target)
	}
	if plan.Interval <= plan.Target {
		t.Fatalf("interval %s should exceed the %s target on a 100/day plan",
			plan.Interval, plan.Target)
	}
	if plan.Interval < time.Minute {
		t.Errorf("interval = %s; a 100/day budget cannot sustain sub-minute polling", plan.Interval)
	}
	if plan.Reason == "" {
		t.Error("a budget-limited plan must explain itself")
	}
	t.Logf("free tier live plan: target %s, affordable %s (%s)",
		plan.Target, plan.Interval.Round(time.Second), plan.Reason)
}

// TestGenerousTierReachesTheTarget is the other half: when the budget can fund
// the requested cadence, the scheduler must actually use it.
func TestGenerousTierReachesTheTarget(t *testing.T) {
	tier, _ := licensing.Tier{
		Name: licensing.TierCustom, RequestsPerMinute: 1200, RequestsPerDay: 500_000,
	}.Resolve()
	v := apisports.VerticalFootball
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	l, _ := NewLimiter(LimiterConfig{
		Tier: tier, Verticals: []apisports.Vertical{v},
		Now: func() time.Time { return now },
	})
	s := NewScheduler(l, NewCrowdWeights(nil), func() time.Time { return now })
	s.SetState(v, StateLive)

	plan := s.PlanFor(v)
	if plan.Interval > 10*time.Second {
		t.Errorf("interval = %s on a custom tier; the target should be reachable", plan.Interval)
	}
}

// TestDeferralProtectsTheReserve: past the defer threshold, everything except
// live action stands down.
func TestDeferralProtectsTheReserve(t *testing.T) {
	tier, _ := licensing.Tier{
		Name: licensing.TierCustom, RequestsPerMinute: 600, RequestsPerDay: 100,
	}.Resolve()
	v := apisports.VerticalFootball
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	l, _ := NewLimiter(LimiterConfig{
		Tier: tier, Verticals: []apisports.Vertical{v},
		Now: func() time.Time { return now },
	})
	s := NewScheduler(l, NewCrowdWeights(nil), func() time.Time { return now })
	s.SetState(v, StatePregame)

	budget := l.Budgets()[0].Requests
	for i := 0; i < int(float64(budget)*DeferAt)+1; i++ {
		l.Usage().Reserve(v)
	}
	plan := s.PlanFor(v)
	if !plan.Deferred {
		t.Errorf("pre-game polling should defer at %.0f%% spend, got %s",
			DeferAt*100, plan.Reason)
	}
}

// TestCrowdWeightsShiftTheBudget is the allocator's whole purpose: the sport
// the room is watching gets more of a scarce budget.
func TestCrowdWeightsShiftTheBudget(t *testing.T) {
	tier, _ := licensing.Tier{Name: licensing.TierFree}.Resolve()
	nfl, handball := apisports.VerticalAmericanFootball, apisports.VerticalHandball
	w := NewCrowdWeights(nil)
	l, _ := NewLimiter(LimiterConfig{
		Tier: tier, Verticals: []apisports.Vertical{nfl, handball}, Weights: w,
	})

	// Nothing live anywhere: shares track the configured base interest.
	base := sharesOf(l)
	if base[nfl] <= base[handball] {
		t.Errorf("NFL share %.3f should exceed handball's %.3f from base interest",
			base[nfl], base[handball])
	}

	// Eight NFL games kick off and the room fills up.
	w.ObserveLive(nfl, 8)
	w.ObserveEngagement(nfl, 0.95)
	w.ObserveLive(handball, 1)
	l.Rebalance()

	after := sharesOf(l)
	if after[nfl] <= base[nfl] {
		t.Errorf("NFL share did not rise with live games and engagement: %.3f -> %.3f",
			base[nfl], after[nfl])
	}
	if after[handball] >= base[handball] {
		t.Errorf("handball share did not yield: %.3f -> %.3f", base[handball], after[handball])
	}
	t.Logf("shares: nfl %.3f -> %.3f, handball %.3f -> %.3f",
		base[nfl], after[nfl], base[handball], after[handball])
}

func sharesOf(l *Limiter) map[apisports.Vertical]float64 {
	out := map[apisports.Vertical]float64{}
	for _, b := range l.Budgets() {
		out[b.Vertical] = b.Weight
	}
	return out
}

// TestStaleEngagementIsIgnored: a crowd reading from two hours ago is worse
// than no reading, because it is confidently wrong.
func TestStaleEngagementIsIgnored(t *testing.T) {
	clock := time.Date(2026, 9, 1, 20, 0, 0, 0, time.UTC)
	w := NewCrowdWeights(nil)
	w.SetClock(func() time.Time { return clock })
	v := apisports.VerticalFootball

	w.ObserveLive(v, 2)
	w.ObserveEngagement(v, 1.0)
	hot := w.Score(v)

	clock = clock.Add(engagementTTL + time.Minute)
	stale := w.Score(v)
	if stale >= hot {
		t.Errorf("a stale engagement reading still counted: %.3f then %.3f", hot, stale)
	}
}

func TestSharesAlwaysSumToOne(t *testing.T) {
	w := NewCrowdWeights(nil)
	vs := []apisports.Vertical{
		apisports.VerticalFootball, apisports.VerticalNBA,
		apisports.VerticalAFL, apisports.VerticalHandball,
	}
	w.ObserveLive(apisports.VerticalFootball, 12)
	total := 0.0
	for _, s := range w.Shares(vs) {
		total += s
	}
	if total < 0.999 || total > 1.001 {
		t.Errorf("shares sum to %v, want 1", total)
	}
}

// TestAffordableLiveCadenceIsHonest is the number the README quotes.
func TestAffordableLiveCadenceIsHonest(t *testing.T) {
	got := AffordableLiveCadence(100, 3)
	if got < 100*time.Second || got > 130*time.Second {
		t.Errorf("free-tier 3h cadence = %s, want roughly two minutes", got)
	}
	t.Logf("free tier, 3 hours of play: one poll every %s", got.Round(time.Second))
}

// --- state classification ---------------------------------------------------

// TestClassifyStatusAcrossVerticals covers the status vocabularies actually
// seen across the twelve hosts. Getting one wrong is expensive in both
// directions: a live game read as final goes stale, and a finished game read as
// live burns the budget all night.
func TestClassifyStatusAcrossVerticals(t *testing.T) {
	for _, tc := range []struct {
		short, long string
		want        MatchState
	}{
		{"1H", "First Half", StateLive},
		{"2H", "Second Half", StateLive},
		{"HT", "Halftime", StateBreak},
		{"BT", "Break Time", StateBreak},
		{"Q3", "Quarter 3", StateLive},
		{"P2", "Period 2", StateLive},
		{"OT", "Over Time", StateLive},
		{"FT", "Match Finished", StateFinal},
		{"AET", "After Extra Time", StateFinal},
		{"NS", "Not Started", StatePregame},
		{"PST", "Postponed", StatePregame},
		{"", "In Play", StateLive},
		{"", "Half Time", StateBreak},
		{"", "Game Finished", StateFinal},
		{"", "Not Started", StatePregame},
		// The safe direction: something unrecognised must not read as live.
		{"WHAT", "who knows", StateIdle},
		{"", "", StateIdle},
	} {
		if got := classifyStatus(tc.short, tc.long); got != tc.want {
			t.Errorf("classifyStatus(%q,%q) = %s, want %s", tc.short, tc.long, got, tc.want)
		}
	}
}

// TestLiveBeatsBreak: one game at half-time while another is in play is not a
// break for the vertical.
func TestDeriveStatePrefersLive(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sweep Sweep
		want  MatchState
	}{
		{"live wins", Sweep{Live: 1, Break: 4, Upcoming: 9, Finished: 3}, StateLive},
		{"break when nothing live", Sweep{Break: 2, Upcoming: 5}, StateBreak},
		{"pregame", Sweep{Upcoming: 5}, StatePregame},
		{"final", Sweep{Finished: 7}, StateFinal},
		{"empty card", Sweep{}, StateIdle},
	} {
		if got := deriveState(&tc.sweep); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}
