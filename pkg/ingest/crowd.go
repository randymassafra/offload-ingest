package ingest

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
)

// CrowdWeights distributes a scarce request budget by how much the venue's
// audience actually cares about each sport right now.
//
// The premise: a sports bar with forty people watching the NFL and nobody
// watching handball should not spend the same number of upstream requests on
// each. On a free tier that difference is the whole game — there is only enough
// budget to poll one or two sports at a useful cadence, so the allocator has to
// decide which, continuously.
//
// A weight is built from three signals, each defensible on its own:
//
//	base      what this venue generally cares about, from configuration
//	live      whether anything is actually in progress right now
//	engagement a real-time audience signal, if the venue feeds one in
//
// The last is optional. A venue with no engagement telemetry still gets sensible
// behaviour from base × live, which is the common case; the hook exists so a
// venue that does have screen-occupancy or till data can drive allocation with
// it rather than with a guess.
type CrowdWeights struct {
	mu sync.RWMutex

	base       map[apisports.Vertical]float64
	live       map[apisports.Vertical]int
	engagement map[apisports.Vertical]float64
	updated    map[apisports.Vertical]time.Time
	now        func() time.Time
}

// DefaultBaseWeights is the starting interest profile.
//
// These are a venue-agnostic prior, not a claim about the sports themselves:
// they reflect how much of a typical multi-screen venue's attention each
// vertical commands, and they are meant to be overridden per site. A venue in
// Melbourne should raise AFL and drop American football; the point of the
// config is that nobody has to edit code to do it.
var DefaultBaseWeights = map[apisports.Vertical]float64{
	apisports.VerticalAmericanFootball: 1.00,
	apisports.VerticalNBA:              0.90,
	apisports.VerticalFootball:         0.85,
	apisports.VerticalBasketball:       0.55,
	apisports.VerticalMMA:              0.50,
	apisports.VerticalBaseball:         0.45,
	apisports.VerticalHockey:           0.45,
	apisports.VerticalAFL:              0.40,
	apisports.VerticalRugby:            0.35,
	apisports.VerticalFormula1:         0.30,
	apisports.VerticalVolleyball:       0.15,
	apisports.VerticalHandball:         0.15,
}

// engagementTTL is how long an audience reading stays trusted.
//
// Twenty minutes. A crowd signal is a perishable fact: the room that was packed
// for the early game may be empty by the late one, and continuing to weight on
// a stale reading is worse than falling back to the venue's base profile,
// because it is confidently wrong rather than merely generic.
const engagementTTL = 20 * time.Minute

// NewCrowdWeights builds an allocator. A nil base map uses the defaults.
func NewCrowdWeights(base map[apisports.Vertical]float64) *CrowdWeights {
	if base == nil {
		base = DefaultBaseWeights
	}
	cp := make(map[apisports.Vertical]float64, len(base))
	for k, v := range base {
		cp[k] = v
	}
	return &CrowdWeights{
		base:       cp,
		live:       map[apisports.Vertical]int{},
		engagement: map[apisports.Vertical]float64{},
		updated:    map[apisports.Vertical]time.Time{},
		now:        time.Now,
	}
}

// SetClock injects a clock, for tests.
func (c *CrowdWeights) SetClock(now func() time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// SetBase replaces a vertical's configured interest.
func (c *CrowdWeights) SetBase(v apisports.Vertical, w float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.base[v] = clampWeight(w)
}

// ObserveLive records how many fixtures are in progress, which the scheduler
// learns for free from each bulk sweep.
func (c *CrowdWeights) ObserveLive(v apisports.Vertical, liveFixtures int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if liveFixtures < 0 {
		liveFixtures = 0
	}
	c.live[v] = liveFixtures
}

// ObserveEngagement records a real-time audience signal in [0,1].
func (c *CrowdWeights) ObserveEngagement(v apisports.Vertical, score float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.engagement[v] = clampWeight(score)
	c.updated[v] = c.now()
}

// Score is one vertical's composite interest, before normalisation.
func (c *CrowdWeights) Score(v apisports.Vertical) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.score(v)
}

func (c *CrowdWeights) score(v apisports.Vertical) float64 {
	base, ok := c.base[v]
	if !ok {
		// A vertical nobody configured still deserves a floor, or a newly
		// licensed sport would never be polled and never become interesting.
		base = 0.25
	}

	// Live action multiplies interest, with diminishing returns: the jump from
	// nothing to one live game is the big one, and the tenth simultaneous game
	// does not make the room ten times as engaged. log1p gives that shape
	// without a magic table.
	live := c.live[v]
	liveFactor := 1.0
	if live > 0 {
		liveFactor = 1.0 + math.Log1p(float64(live))
	} else {
		// Nothing in progress: keep a small share so the sport is still polled
		// often enough to notice a fixture starting.
		liveFactor = 0.25
	}

	engagementFactor := 1.0
	if score, ok := c.engagement[v]; ok {
		if at, ok := c.updated[v]; ok && c.now().Sub(at) <= engagementTTL {
			// Range 0.5..1.5, so a real signal can meaningfully move the
			// allocation without ever zeroing a sport out entirely.
			engagementFactor = 0.5 + score
		}
	}
	return base * liveFactor * engagementFactor
}

// Shares returns each vertical's normalised share, summing to 1.
func (c *CrowdWeights) Shares(verticals []apisports.Vertical) map[apisports.Vertical]float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make(map[apisports.Vertical]float64, len(verticals))
	if len(verticals) == 0 {
		return out
	}
	total := 0.0
	for _, v := range verticals {
		s := c.score(v)
		out[v] = s
		total += s
	}
	if total <= 0 {
		// Degenerate but possible; split evenly rather than dividing by zero.
		even := 1.0 / float64(len(verticals))
		for _, v := range verticals {
			out[v] = even
		}
		return out
	}
	for v := range out {
		out[v] /= total
	}
	return out
}

// Snapshot is the dashboard's view of the allocator.
type Snapshot struct {
	Vertical   apisports.Vertical `json:"vertical"`
	Base       float64            `json:"base"`
	LiveGames  int                `json:"live_games"`
	Engagement float64            `json:"engagement"`
	Score      float64            `json:"score"`
	Share      float64            `json:"share"`
}

// Snapshot returns per-vertical weights for display.
func (c *CrowdWeights) Snapshot(verticals []apisports.Vertical) []Snapshot {
	shares := c.Shares(verticals)
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]Snapshot, 0, len(verticals))
	for _, v := range verticals {
		s := Snapshot{
			Vertical: v, Base: c.base[v], LiveGames: c.live[v],
			Score: c.score(v), Share: shares[v],
		}
		if e, ok := c.engagement[v]; ok {
			if at, ok := c.updated[v]; ok && c.now().Sub(at) <= engagementTTL {
				s.Engagement = e
			}
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Share > out[j].Share })
	return out
}

func clampWeight(w float64) float64 {
	if math.IsNaN(w) || w < 0 {
		return 0
	}
	if w > 1 {
		return 1
	}
	return w
}
