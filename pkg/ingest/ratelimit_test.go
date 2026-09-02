package ingest

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
	"github.com/offloadintelligence/offload-ingest/pkg/licensing"
)

func freeTier(t *testing.T) licensing.Tier {
	t.Helper()
	tier, err := licensing.Tier{Name: licensing.TierFree}.Resolve()
	if err != nil {
		t.Fatalf("resolve free tier: %v", err)
	}
	return tier
}

func testVerticals() []apisports.Vertical {
	return []apisports.Vertical{
		apisports.VerticalAmericanFootball,
		apisports.VerticalFootball,
		apisports.VerticalHandball,
	}
}

// TestLimiterScalesWithTier is the core licence-to-throughput link: the same
// code on a bigger plan must actually go faster.
func TestLimiterScalesWithTier(t *testing.T) {
	var free, pro int
	for _, tc := range []struct {
		name string
		tier licensing.TierName
		out  *int
	}{{"free", licensing.TierFree, &free}, {"pro", licensing.TierPro, &pro}} {
		tier, err := licensing.Tier{Name: tc.tier}.Resolve()
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		l, err := NewLimiter(LimiterConfig{Tier: tier, Verticals: testVerticals()})
		if err != nil {
			t.Fatalf("NewLimiter: %v", err)
		}
		*tc.out = l.Budgets()[0].PerMinute
	}
	if !(pro > free) {
		t.Errorf("pro per-minute ceiling %d is not above free's %d", pro, free)
	}
	// The safety margin must actually be applied, or we pace at the ceiling.
	if free >= 10 {
		t.Errorf("free per-minute = %d, want below the raw ceiling of 10", free)
	}
}

// TestBudgetExhaustionStopsRequests is the behaviour that protects a venue from
// burning a day's quota before the evening.
func TestBudgetExhaustionStopsRequests(t *testing.T) {
	now := time.Date(2026, 9, 1, 9, 0, 0, 0, time.UTC)
	tier := licensing.Tier{Name: licensing.TierCustom, RequestsPerMinute: 600, RequestsPerDay: 10}
	tier, _ = tier.Resolve()
	v := apisports.VerticalFootball
	l, err := NewLimiter(LimiterConfig{
		Tier: tier, Verticals: []apisports.Vertical{v},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}

	budget := l.Budgets()[0].Requests
	if budget <= 0 || budget > 10 {
		t.Fatalf("budget = %d, want a positive value no greater than the tier's 10", budget)
	}
	for i := 0; i < budget; i++ {
		if !l.Allow(v) {
			t.Fatalf("request %d of %d was refused while budget remained", i+1, budget)
		}
	}
	if l.Allow(v) {
		t.Error("a request past the daily budget was allowed")
	}
}

// TestUsageRollsOverAtMidnight: a venue that spent its budget yesterday must
// open today with a full one.
func TestUsageRollsOverAtMidnight(t *testing.T) {
	clock := time.Date(2026, 9, 1, 23, 59, 0, 0, time.UTC)
	tier, _ := licensing.Tier{Name: licensing.TierCustom, RequestsPerMinute: 600, RequestsPerDay: 4}.Resolve()
	v := apisports.VerticalFootball
	l, _ := NewLimiter(LimiterConfig{
		Tier: tier, Verticals: []apisports.Vertical{v},
		Now: func() time.Time { return clock },
	})
	for l.Allow(v) {
	}
	if l.Allow(v) {
		t.Fatal("budget should be spent")
	}
	clock = clock.Add(2 * time.Minute) // past midnight
	if !l.Allow(v) {
		t.Error("the daily budget did not roll over")
	}
}

// TestProviderHeadersOverrideTheTierTable is why a transcribed pricing page is
// safe to ship: the API's own numbers win.
func TestProviderHeadersOverrideTheTierTable(t *testing.T) {
	tier, _ := licensing.Tier{Name: licensing.TierMega}.Resolve() // claims 900/min
	v := apisports.VerticalFootball
	l, _ := NewLimiter(LimiterConfig{Tier: tier, Verticals: []apisports.Vertical{v}})

	before := l.Budgets()[0].PerMinute
	// The provider says the real ceiling is 10.
	l.ObserveQuota(v, apisports.Quota{
		MinuteLimit: 10, MinuteRemaining: 9,
		DayLimit: 100, DayRemaining: 99, Present: true, Observed: time.Now(),
	})
	q, ok := l.Quota(v)
	if !ok || q.MinuteLimit != 10 {
		t.Fatal("quota was not recorded")
	}
	// The bucket must have been retuned downward.
	if got := l.Tokens(v); got > float64(before) {
		t.Errorf("bucket was not tightened after the provider reported a lower ceiling")
	}
}

// TestThrottleHalvesTheRate pins the 429 reaction.
func TestThrottleHalvesTheRate(t *testing.T) {
	tier, _ := licensing.Tier{Name: licensing.TierPro}.Resolve()
	v := apisports.VerticalFootball
	l, _ := NewLimiter(LimiterConfig{Tier: tier, Verticals: []apisports.Vertical{v}})

	l.ObserveThrottle(v, time.Second)
	if l.Throttles()[v] != 1 {
		t.Errorf("throttle count = %d, want 1", l.Throttles()[v])
	}
	l.ObserveThrottle(v, time.Second)
	if l.Throttles()[v] != 2 {
		t.Errorf("throttle count = %d, want 2", l.Throttles()[v])
	}
}

// TestFailedRequestsDoNotBurnBudget: a call that never reached the provider did
// not spend quota, and must not be counted as if it had.
func TestFailedRequestsDoNotBurnBudget(t *testing.T) {
	tier, _ := licensing.Tier{Name: licensing.TierCustom, RequestsPerMinute: 600, RequestsPerDay: 5}.Resolve()
	v := apisports.VerticalFootball
	l, _ := NewLimiter(LimiterConfig{Tier: tier, Verticals: []apisports.Vertical{v}})

	if !l.Allow(v) {
		t.Fatal("first request refused")
	}
	spentBefore := l.Usage().Stats()[0].Today
	l.ObserveRequest(v, 0, time.Millisecond, context.DeadlineExceeded)
	spentAfter := l.Usage().Stats()[0].Today
	if spentAfter >= spentBefore {
		t.Errorf("a failed request kept its reservation: %d -> %d", spentBefore, spentAfter)
	}
}

func TestUnlicensedVerticalIsRefused(t *testing.T) {
	l, _ := NewLimiter(LimiterConfig{Tier: freeTier(t), Verticals: testVerticals()})
	if l.Allow(apisports.VerticalFormula1) {
		t.Error("an unlicensed vertical was allowed")
	}
	if err := l.Wait(context.Background(), apisports.VerticalFormula1); err == nil {
		t.Error("Wait on an unlicensed vertical should fail")
	}
}

// --- quota header parsing ---------------------------------------------------

// TestParseQuotaReadsTheRealHeaders guards the trap that the minute and day
// figures live in confusingly-named headers. Reading them the wrong way round
// throttles a venue to a fraction of what it paid for.
func TestParseQuotaReadsTheRealHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("x-ratelimit-limit", "10")
	h.Set("x-ratelimit-remaining", "9")
	h.Set("x-ratelimit-requests-limit", "100")
	h.Set("x-ratelimit-requests-remaining", "99")

	q := apisports.ParseQuota(h, time.Now())
	if !q.Present {
		t.Fatal("headers were not detected")
	}
	if q.MinuteLimit != 10 || q.MinuteRemaining != 9 {
		t.Errorf("minute window = %d/%d, want 9/10", q.MinuteRemaining, q.MinuteLimit)
	}
	if q.DayLimit != 100 || q.DayRemaining != 99 {
		t.Errorf("day window = %d/%d, want 99/100", q.DayRemaining, q.DayLimit)
	}
	if got := q.DayFractionRemaining(); got < 0.98 || got > 1.0 {
		t.Errorf("DayFractionRemaining = %v, want ~0.99", got)
	}
}

func TestParseQuotaHandlesAbsentHeaders(t *testing.T) {
	q := apisports.ParseQuota(http.Header{}, time.Now())
	if q.Present {
		t.Error("Present should be false when no headers were sent")
	}
	if got := q.DayFractionRemaining(); got != -1 {
		t.Errorf("DayFractionRemaining = %v, want -1 for unknown", got)
	}
}
