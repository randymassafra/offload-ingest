package metrics

import (
	"sync"
	"time"
)

// SeriesBuckets is how many points a one-hour sparkline carries.
//
// Sixty one-minute buckets. The DDS mandates a one-hour window on every card,
// and a minute is the coarsest resolution that still shows a dip an operator
// could act on. Storing seconds instead would be 3,600 points per series to
// draw a 240-pixel-wide line — most of them landing on the same pixel.
const SeriesBuckets = 60

// SeriesInterval is the width of one bucket.
const SeriesInterval = time.Minute

// TimeSeries is a one-hour ring of per-minute aggregates.
//
// It records both a sum and a count so the same structure serves a rate card
// (events per minute) and an average card (mean latency in that minute).
// Keeping two series in step by hand is how they drift apart.
type TimeSeries struct {
	mu     sync.Mutex
	sum    [SeriesBuckets]float64
	count  [SeriesBuckets]int64
	filled [SeriesBuckets]bool
	// minute is the absolute minute index of the newest bucket, so a gap of
	// any length can be zeroed correctly rather than wrapping onto live data.
	minute int64
	now    func() time.Time
}

// NewTimeSeries builds a series.
func NewTimeSeries(now func() time.Time) *TimeSeries {
	if now == nil {
		now = time.Now
	}
	return &TimeSeries{now: now, minute: -1}
}

// Add records one observation into the current minute.
func (s *TimeSeries) Add(v float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.advance()
	s.sum[i] += v
	s.count[i]++
	s.filled[i] = true
}

// Observe is Add(1)-style event counting, for rate series.
func (s *TimeSeries) Observe() { s.Add(1) }

// advance rolls the ring forward to the current minute, clearing any buckets
// that were skipped, and returns the live index.
func (s *TimeSeries) advance() int {
	m := s.now().Unix() / 60
	if s.minute < 0 {
		s.minute = m
		return int(m % SeriesBuckets)
	}
	if m == s.minute {
		return int(m % SeriesBuckets)
	}
	gap := m - s.minute
	if gap >= SeriesBuckets {
		s.sum, s.count, s.filled = [SeriesBuckets]float64{}, [SeriesBuckets]int64{}, [SeriesBuckets]bool{}
	} else {
		for k := int64(1); k <= gap; k++ {
			i := int((s.minute + k) % SeriesBuckets)
			s.sum[i], s.count[i], s.filled[i] = 0, 0, false
		}
	}
	s.minute = m
	return int(m % SeriesBuckets)
}

// Totals returns the last hour oldest-first, as per-minute sums.
//
// Oldest-first because that is the order a sparkline draws in; returning ring
// order and expecting every caller to rotate it is a bug waiting to happen.
func (s *TimeSeries) Totals() []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance()
	out := make([]float64, 0, SeriesBuckets)
	for k := SeriesBuckets - 1; k >= 0; k-- {
		i := int((s.minute - int64(k) + SeriesBuckets*2) % SeriesBuckets)
		out = append(out, s.sum[i])
	}
	return out
}

// Means returns the last hour oldest-first as per-minute averages. A minute
// with no observations reports 0 rather than a stale carry-forward.
func (s *TimeSeries) Means() []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance()
	out := make([]float64, 0, SeriesBuckets)
	for k := SeriesBuckets - 1; k >= 0; k-- {
		i := int((s.minute - int64(k) + SeriesBuckets*2) % SeriesBuckets)
		if s.count[i] == 0 {
			out = append(out, 0)
			continue
		}
		out = append(out, s.sum[i]/float64(s.count[i]))
	}
	return out
}

// LastMean is the mean over the most recent minute that recorded anything.
//
// Falling back to the previous minute matters for a feed that polls every few
// minutes: reading only the live bucket would show "—" most of the time on a
// card that is working perfectly.
func (s *TimeSeries) LastMean() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance()
	for k := 0; k < SeriesBuckets; k++ {
		i := int((s.minute - int64(k) + SeriesBuckets*2) % SeriesBuckets)
		if s.count[i] > 0 {
			return s.sum[i] / float64(s.count[i])
		}
	}
	return 0
}

// HourTotal is the sum across the whole window.
func (s *TimeSeries) HourTotal() float64 {
	total := 0.0
	for _, v := range s.Totals() {
		total += v
	}
	return total
}

// HourMean is the mean across every observation in the window.
func (s *TimeSeries) HourMean() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance()
	var sum float64
	var n int64
	for i := 0; i < SeriesBuckets; i++ {
		sum += s.sum[i]
		n += s.count[i]
	}
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}
