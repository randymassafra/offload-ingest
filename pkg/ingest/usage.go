package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
	"github.com/offloadintelligence/offload-ingest/pkg/licensing"
)

// Threshold fractions at which the tracker changes behaviour.
//
// Two distinct ideas, deliberately separated. WarnAt is informational — the
// venue's operator should know the day is going faster than planned. DeferAt is
// operational — past it, the scheduler stops polling anything low-priority so
// whatever budget remains is held for the sport the room is actually watching.
//
// The reserve exists because quota exhaustion is not a smooth degradation: at
// zero, the feed simply stops. Holding back the last tenth means a surge — a
// game going to overtime, a fight card overrunning — still has something to
// spend rather than arriving at an empty bucket.
const (
	WarnAt  = 0.75
	DeferAt = 0.90
)

// UsageTracker meters what has actually been spent against the licence's
// volume allocation, per vertical, per day and per month.
//
// It counts reservations rather than completions, and hands a reservation back
// when a call never reached the provider. Counting completions would let a
// burst of in-flight requests overshoot the ceiling before any of them
// returned — which on a 100/day budget is most of the day.
type UsageTracker struct {
	mu sync.Mutex

	tier      licensing.Tier
	verticals []apisports.Vertical
	now       func() time.Time
	log       *slog.Logger

	day      map[apisports.Vertical]int
	month    map[apisports.Vertical]int
	budgets  map[apisports.Vertical]int
	dayStart time.Time
	monStart time.Time

	// warned tracks which thresholds have already been logged, so crossing one
	// produces a single line rather than one per request for the rest of the
	// day.
	warned map[apisports.Vertical]float64
}

// NewUsageTracker builds a tracker for a tier.
func NewUsageTracker(tier licensing.Tier, verticals []apisports.Vertical, now func() time.Time) *UsageTracker {
	if now == nil {
		now = time.Now
	}
	t := now()
	return &UsageTracker{
		tier: tier, verticals: append([]apisports.Vertical{}, verticals...),
		now: now, log: slog.Default(),
		day:      map[apisports.Vertical]int{},
		month:    map[apisports.Vertical]int{},
		budgets:  map[apisports.Vertical]int{},
		warned:   map[apisports.Vertical]float64{},
		dayStart: startOfDay(t), monStart: startOfMonth(t),
	}
}

// SetLogger swaps the logger.
func (u *UsageTracker) SetLogger(l *slog.Logger) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.log = l
}

// SetBudgets records the per-vertical daily allocation.
func (u *UsageTracker) SetBudgets(budgets map[apisports.Vertical]*Budget) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for v, b := range budgets {
		u.budgets[v] = b.Requests
	}
}

// Reserve claims one request against a vertical's allowance.
func (u *UsageTracker) Reserve(v apisports.Vertical) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rollLocked()

	budget := u.budgetLocked(v)
	if u.day[v] >= budget {
		return &ExhaustedError{Vertical: v, Scope: fmt.Sprintf("day (%d/%d)", u.day[v], budget)}
	}
	if m := u.tier.RequestsPerMonth; m > 0 {
		if u.totalMonthLocked() >= m {
			return &ExhaustedError{Vertical: v, Scope: fmt.Sprintf("month (%d/%d)", u.totalMonthLocked(), m)}
		}
	}
	u.day[v]++
	u.month[v]++
	u.warnLocked(v, budget)
	return nil
}

// Release hands back a reservation for a call that never reached the provider.
func (u *UsageTracker) Release(v apisports.Vertical) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.day[v] > 0 {
		u.day[v]--
	}
	if u.month[v] > 0 {
		u.month[v]--
	}
}

// ObserveRemaining reconciles our count against the provider's.
//
// Theirs wins. Our counter resets when the process restarts and knows nothing
// about requests made by another process on the same key; the provider's figure
// is the one that decides whether the next call returns data or a 429.
func (u *UsageTracker) ObserveRemaining(v apisports.Vertical, q apisports.Quota) {
	if !q.Present || q.DayLimit <= 0 {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rollLocked()
	spent := q.DayLimit - q.DayRemaining
	if spent > u.day[v] {
		u.log.Debug("ingest: reconciling usage upward from provider headers",
			"vertical", v, "ours", u.day[v], "provider", spent)
		u.day[v] = spent
	}
}

// Pressure is how much of a vertical's daily budget is spent, in [0,1].
func (u *UsageTracker) Pressure(v apisports.Vertical) float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rollLocked()
	budget := u.budgetLocked(v)
	if budget <= 0 {
		return 1
	}
	p := float64(u.day[v]) / float64(budget)
	if p > 1 {
		return 1
	}
	return p
}

// ShouldDefer reports whether low-priority polling should stand down for this
// vertical, because the day's allowance is nearly gone.
func (u *UsageTracker) ShouldDefer(v apisports.Vertical) bool {
	return u.Pressure(v) >= DeferAt
}

// Stat is one vertical's usage, for the dashboard.
type Stat struct {
	Vertical  apisports.Vertical `json:"vertical"`
	Today     int                `json:"today"`
	Budget    int                `json:"budget"`
	Month     int                `json:"month"`
	Pressure  float64            `json:"pressure"`
	Deferring bool               `json:"deferring"`
}

// Stats snapshots per-vertical usage.
func (u *UsageTracker) Stats() []Stat {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rollLocked()

	out := make([]Stat, 0, len(u.verticals))
	for _, v := range u.verticals {
		budget := u.budgetLocked(v)
		p := 0.0
		if budget > 0 {
			p = float64(u.day[v]) / float64(budget)
			if p > 1 {
				p = 1
			}
		}
		out = append(out, Stat{
			Vertical: v, Today: u.day[v], Budget: budget, Month: u.month[v],
			Pressure: p, Deferring: p >= DeferAt,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Vertical < out[j].Vertical })
	return out
}

// MonthTotal is every vertical's month-to-date spend.
func (u *UsageTracker) MonthTotal() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.totalMonthLocked()
}

func (u *UsageTracker) totalMonthLocked() int {
	total := 0
	for _, n := range u.month {
		total += n
	}
	return total
}

func (u *UsageTracker) budgetLocked(v apisports.Vertical) int {
	if b, ok := u.budgets[v]; ok && b > 0 {
		return b
	}
	return u.tier.RequestsPerDay
}

// rollLocked resets the counters when the day or month turns over.
func (u *UsageTracker) rollLocked() {
	now := u.now()
	if d := startOfDay(now); d.After(u.dayStart) {
		u.dayStart = d
		u.day = map[apisports.Vertical]int{}
		u.warned = map[apisports.Vertical]float64{}
	}
	if m := startOfMonth(now); m.After(u.monStart) {
		u.monStart = m
		u.month = map[apisports.Vertical]int{}
	}
}

// warnLocked logs once per threshold crossing.
func (u *UsageTracker) warnLocked(v apisports.Vertical, budget int) {
	if budget <= 0 {
		return
	}
	p := float64(u.day[v]) / float64(budget)
	for _, threshold := range []float64{WarnAt, DeferAt} {
		if p >= threshold && u.warned[v] < threshold {
			u.warned[v] = threshold
			level := slog.LevelWarn
			msg := "ingest: vertical is nearing its daily request budget"
			if threshold >= DeferAt {
				msg = "ingest: vertical has passed its defer threshold; low-priority polling stands down"
			}
			u.log.Log(context.Background(), level, msg,
				"vertical", v, "used", u.day[v], "budget", budget,
				"pct", int(p*100), "tier", string(u.tier.Name))
		}
	}
}

func startOfDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func startOfMonth(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}
