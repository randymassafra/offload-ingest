package metrics

import (
	"sort"
	"sync"
)

// LatencyBuckets are the Prometheus histogram boundaries, in milliseconds.
//
// Chosen around the two numbers that matter rather than as a neat power series:
// the DDS alerts at 2,000ms, so there are boundaries either side of it, and the
// interesting detail for a healthy poll-to-Kafka path is under 500ms.
var LatencyBuckets = []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000, 10000}

// Histogram accumulates observations into fixed buckets.
//
// It keeps an exact count, sum and bucket distribution — everything Prometheus
// needs for a histogram, plus enough to answer "what is p95 right now" for the
// dashboard without a scrape round trip.
type Histogram struct {
	mu     sync.Mutex
	bounds []float64
	counts []uint64 // one per bound, plus a final +Inf slot
	sum    float64
	total  uint64
	// recent is a small reservoir used for the live quantile the dashboard
	// shows. Bucket boundaries alone give a coarse interpolation; a few hundred
	// raw samples give an honest p95 for a card refreshed every two seconds.
	recent []float64
	cursor int
}

const recentSamples = 512

// NewHistogram builds a histogram over the given upper bounds.
func NewHistogram(bounds []float64) *Histogram {
	b := append([]float64{}, bounds...)
	sort.Float64s(b)
	return &Histogram{
		bounds: b,
		counts: make([]uint64, len(b)+1),
		recent: make([]float64, 0, recentSamples),
	}
}

// Observe records one value.
func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += v
	h.total++
	i := sort.SearchFloat64s(h.bounds, v)
	// SearchFloat64s returns the first index whose bound is >= v, which is the
	// bucket v belongs to; len(bounds) means it exceeded every bound (+Inf).
	if i >= len(h.counts) {
		i = len(h.counts) - 1
	}
	h.counts[i]++

	if len(h.recent) < recentSamples {
		h.recent = append(h.recent, v)
		return
	}
	h.recent[h.cursor] = v
	h.cursor = (h.cursor + 1) % recentSamples
}

// Count is the number of observations.
func (h *Histogram) Count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.total
}

// Sum is the total of all observations.
func (h *Histogram) Sum() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sum
}

// Mean is the arithmetic mean, or 0 with no observations.
func (h *Histogram) Mean() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return 0
	}
	return h.sum / float64(h.total)
}

// Quantile estimates a quantile from the recent reservoir.
//
// Reported from raw samples rather than interpolated from bucket boundaries,
// because a p95 that can only ever land on 1000 or 2000 is not much of a signal
// when the alert threshold sits at 2000.
func (h *Histogram) Quantile(q float64) float64 {
	h.mu.Lock()
	sample := append([]float64{}, h.recent...)
	h.mu.Unlock()
	if len(sample) == 0 {
		return 0
	}
	sort.Float64s(sample)
	idx := int(q * float64(len(sample)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sample) {
		idx = len(sample) - 1
	}
	return sample[idx]
}

// Cumulative returns the Prometheus-style cumulative bucket counts alongside
// their upper bounds. The final entry is +Inf and equals the total count.
func (h *Histogram) Cumulative() (bounds []float64, counts []uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	bounds = append([]float64{}, h.bounds...)
	counts = make([]uint64, len(h.counts))
	var running uint64
	for i, c := range h.counts {
		running += c
		counts[i] = running
	}
	return bounds, counts
}
