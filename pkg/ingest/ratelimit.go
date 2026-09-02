// Package ingest turns a licence into a polling plan and enforces it.
//
// The scarce resource in this system is not CPU, memory or Kafka throughput —
// it is the upstream request budget. On the free API-Sports plan a venue gets
// 100 requests per day per sport host. Polling one sport every ten seconds for
// a three-hour game would cost 1,080 requests, or roughly eleven days of that
// sport's budget for a single evening.
//
// So the budget is treated as the primary constraint and everything else is
// derived from it:
//
//	licence tier  ->  per-vertical budget  ->  crowd-weighted share
//	              ->  achievable poll cadence
//
// rather than the other way round. A scheduler that picks a cadence first and
// hopes the budget covers it will exhaust the day's quota before the evening
// kick-off and go dark exactly when the venue cares most.
package ingest

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
	"github.com/offloadintelligence/offload-ingest/pkg/licensing"
)

// safetyMargin is the fraction of a tier's stated ceiling the limiter will
// actually use.
//
// Ten percent held back, for two reasons. The provider's minute window and
// ours are not aligned — our token bucket refills continuously while theirs
// resets on a boundary we cannot see — so pacing at exactly the ceiling
// produces occasional 429s through nothing but phase difference. And the daily
// counter has to survive retries and the odd manual call without the scheduler
// having already spent every last request.
const safetyMargin = 0.90

// Budget is one vertical's allowance for a day.
type Budget struct {
	Vertical apisports.Vertical
	// Requests is the vertical's share of the day, after crowd weighting.
	Requests int
	// Weight is the crowd-interest weight that produced the share.
	Weight float64
	// PerMinute is the burst ceiling, which the provider meters per host.
	PerMinute int
}

// Limiter enforces a licence's throughput across verticals.
//
// One token bucket per vertical, because that is how the provider meters. A
// single shared bucket would be wrong in both directions: it would let football
// exhaust basketball's untouched quota, and it would throttle basketball for
// football's spending.
type Limiter struct {
	mu sync.RWMutex

	tier    licensing.Tier
	buckets map[apisports.Vertical]*rate.Limiter
	budgets map[apisports.Vertical]*Budget
	usage   *UsageTracker
	weights *CrowdWeights
	now     func() time.Time

	// observed holds the provider's own view, from response headers. It
	// overrides the tier's assumed ceilings: the API is the authority on what
	// it will accept, and a tier table transcribed from a pricing page is not.
	observed map[apisports.Vertical]apisports.Quota

	// throttles counts 429s per vertical, for the dashboard.
	throttles map[apisports.Vertical]int
}

// LimiterConfig configures a Limiter.
type LimiterConfig struct {
	Tier      licensing.Tier
	Verticals []apisports.Vertical
	Weights   *CrowdWeights
	Usage     *UsageTracker
	Now       func() time.Time
}

// NewLimiter builds a limiter sized by the licence tier.
func NewLimiter(cfg LimiterConfig) (*Limiter, error) {
	if cfg.Tier.RequestsPerMinute <= 0 || cfg.Tier.RequestsPerDay <= 0 {
		return nil, fmt.Errorf("ingest: tier %q has no usable ceilings", cfg.Tier.Name)
	}
	if len(cfg.Verticals) == 0 {
		return nil, fmt.Errorf("ingest: no verticals to limit")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Weights == nil {
		cfg.Weights = NewCrowdWeights(nil)
	}
	if cfg.Usage == nil {
		cfg.Usage = NewUsageTracker(cfg.Tier, cfg.Verticals, cfg.Now)
	}

	l := &Limiter{
		tier:      cfg.Tier,
		buckets:   map[apisports.Vertical]*rate.Limiter{},
		budgets:   map[apisports.Vertical]*Budget{},
		usage:     cfg.Usage,
		weights:   cfg.Weights,
		now:       cfg.Now,
		observed:  map[apisports.Vertical]apisports.Quota{},
		throttles: map[apisports.Vertical]int{},
	}
	for _, v := range cfg.Verticals {
		// Buckets are NOT pre-created here. A rate.Limiter carries its token
		// count across SetLimit/SetBurst, so a placeholder built with burst 1
		// would leave the real bucket holding a single token however large the
		// tier — the first request would succeed and the second would block.
		// Rebalance constructs each bucket once, correctly sized.
		l.budgets[v] = &Budget{Vertical: v}
	}
	l.Rebalance()
	return l, nil
}

// Rebalance recomputes every vertical's share from the current crowd weights.
//
// Called on a weight change and whenever the day rolls over. The per-minute
// bucket is NOT divided between verticals — the provider meters the minute
// window per host too, so each vertical gets the full per-minute ceiling. Only
// the daily allowance is a shared, weighted resource... and even that is
// per-host, so "sharing" here means shaping cadence, not splitting a pool.
func (l *Limiter) Rebalance() {
	l.mu.Lock()
	defer l.mu.Unlock()

	perMinute := int(math.Floor(float64(l.tier.RequestsPerMinute) * safetyMargin))
	if perMinute < 1 {
		perMinute = 1
	}
	perDay := int(math.Floor(float64(l.tier.RequestsPerDay) * safetyMargin))
	if perDay < 1 {
		perDay = 1
	}

	verticals := make([]apisports.Vertical, 0, len(l.budgets))
	for v := range l.budgets {
		verticals = append(verticals, v)
	}
	sort.Slice(verticals, func(i, j int) bool { return verticals[i] < verticals[j] })

	shares := l.weights.Shares(verticals)
	for _, v := range verticals {
		b := l.budgets[v]
		b.Weight = shares[v]
		b.PerMinute = perMinute
		// Each host has its own daily allowance, so the weight scales how much
		// of that host's OWN day this vertical plans to spend, not a slice of a
		// shared pool. A low-interest sport deliberately leaves quota unspent
		// so the scheduler can hold it back for a surge.
		b.Requests = int(math.Floor(float64(perDay) * shares[v] * float64(len(verticals))))
		if b.Requests > perDay {
			b.Requests = perDay
		}
		if b.Requests < 1 {
			b.Requests = 1
		}

		// The bucket paces the minute window. Burst is capped at the ceiling so
		// a long idle period cannot bank a burst the provider would reject.
		limit := rate.Limit(float64(perMinute) / 60.0)
		if cur, ok := l.buckets[v]; ok {
			cur.SetLimit(limit)
			cur.SetBurst(maxInt(1, perMinute/2))
		} else {
			l.buckets[v] = rate.NewLimiter(limit, maxInt(1, perMinute/2))
		}
	}
	l.usage.SetBudgets(l.budgets)
}

// Wait blocks until the vertical may issue a request.
//
// It enforces three gates in order, cheapest first: the daily budget, the
// provider's own reported headroom, then the token bucket. Checking the budget
// before blocking on the bucket means an exhausted vertical fails fast instead
// of parking a goroutine on a bucket it must not drain.
func (l *Limiter) Wait(ctx context.Context, v apisports.Vertical) error {
	l.mu.RLock()
	bucket, ok := l.buckets[v]
	quota := l.observed[v]
	l.mu.RUnlock()
	if !ok {
		return fmt.Errorf("ingest: vertical %s is not licensed for polling", v)
	}

	if err := l.usage.Reserve(v); err != nil {
		return err
	}
	// If the provider says the daily allowance is gone, believe it over our own
	// accounting — our counter can drift, theirs is the one that returns 429.
	if quota.Present && quota.DayLimit > 0 && quota.DayRemaining <= 0 {
		l.usage.Release(v)
		return &ExhaustedError{Vertical: v, Scope: "day (provider-reported)"}
	}
	if err := bucket.Wait(ctx); err != nil {
		l.usage.Release(v)
		return err
	}
	return nil
}

// Allow is the non-blocking form, for callers that would rather skip a poll
// than queue behind the bucket.
func (l *Limiter) Allow(v apisports.Vertical) bool {
	l.mu.RLock()
	bucket, ok := l.buckets[v]
	l.mu.RUnlock()
	if !ok {
		return false
	}
	if err := l.usage.Reserve(v); err != nil {
		return false
	}
	if !bucket.Allow() {
		l.usage.Release(v)
		return false
	}
	return true
}

// ExhaustedError says a budget is spent.
type ExhaustedError struct {
	Vertical apisports.Vertical
	Scope    string
}

func (e *ExhaustedError) Error() string {
	return fmt.Sprintf("ingest: %s budget exhausted for %s", e.Vertical, e.Scope)
}

// ObserveQuota implements apisports.Observer: it feeds the provider's own
// numbers back into the limiter.
//
// This is the correction loop that makes a transcribed tier table safe. If the
// pricing page said 300/min and the API says 100, the bucket is retuned to 100
// on the first response rather than generating 429s until someone notices.
func (l *Limiter) ObserveQuota(v apisports.Vertical, q apisports.Quota) {
	if !q.Present {
		return
	}
	l.mu.Lock()
	l.observed[v] = q
	bucket, ok := l.buckets[v]
	tierMinute := l.tier.RequestsPerMinute
	l.mu.Unlock()
	if !ok {
		return
	}

	if q.MinuteLimit > 0 && q.MinuteLimit < tierMinute {
		effective := int(math.Floor(float64(q.MinuteLimit) * safetyMargin))
		if effective < 1 {
			effective = 1
		}
		bucket.SetLimit(rate.Limit(float64(effective) / 60.0))
		bucket.SetBurst(maxInt(1, effective/2))
	}
	l.usage.ObserveRemaining(v, q)
}

// ObserveRequest implements apisports.Observer.
func (l *Limiter) ObserveRequest(v apisports.Vertical, status int, _ time.Duration, err error) {
	// A request that never reached the provider did not spend quota, so the
	// reservation is handed back rather than silently burned.
	if err != nil || status == 0 {
		l.usage.Release(v)
	}
}

// ObserveThrottle implements apisports.Observer.
//
// A 429 means our pacing was wrong, so the bucket is halved immediately rather
// than waiting for the next header to suggest it. Recovery comes from the next
// successful response's headers, which is the honest signal.
func (l *Limiter) ObserveThrottle(v apisports.Vertical, _ time.Duration) {
	l.mu.Lock()
	l.throttles[v]++
	bucket, ok := l.buckets[v]
	l.mu.Unlock()
	if !ok {
		return
	}
	if cur := float64(bucket.Limit()); cur > 0 {
		next := cur / 2
		// Never below one request per two minutes; at that point the scheduler
		// should be dropping polls, not queueing them.
		if min := 1.0 / 120.0; next < min {
			next = min
		}
		bucket.SetLimit(rate.Limit(next))
	}
}

// Throttles returns the 429 count per vertical.
func (l *Limiter) Throttles() map[apisports.Vertical]int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make(map[apisports.Vertical]int, len(l.throttles))
	for k, v := range l.throttles {
		out[k] = v
	}
	return out
}

// Budgets returns a snapshot of the per-vertical allocation.
func (l *Limiter) Budgets() []Budget {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]Budget, 0, len(l.budgets))
	for _, b := range l.budgets {
		out = append(out, *b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Vertical < out[j].Vertical })
	return out
}

// Tokens reports the bucket's current fill, for the dashboard.
func (l *Limiter) Tokens(v apisports.Vertical) float64 {
	l.mu.RLock()
	bucket, ok := l.buckets[v]
	l.mu.RUnlock()
	if !ok {
		return 0
	}
	return bucket.TokensAt(l.now())
}

// Quota returns the last provider-reported quota for a vertical.
func (l *Limiter) Quota(v apisports.Vertical) (apisports.Quota, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	q, ok := l.observed[v]
	return q, ok
}

// Usage exposes the tracker.
func (l *Limiter) Usage() *UsageTracker { return l.usage }

// Tier returns the licensed tier.
func (l *Limiter) Tier() licensing.Tier { return l.tier }

// SetWeights swaps the crowd weights and rebalances.
func (l *Limiter) SetWeights(w *CrowdWeights) {
	l.mu.Lock()
	l.weights = w
	l.mu.Unlock()
	l.Rebalance()
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
