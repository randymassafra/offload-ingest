// Package metrics is the in-process telemetry the dashboard reads.
//
// It is deliberately not Prometheus. A venue appliance is a single box that an
// operator looks at through a browser when something seems wrong, not a fleet
// with a scrape pipeline in front of it. Pulling in a metrics SDK and a
// registry to serve one local page would add a dependency and an exposition
// format nobody here consumes. The Snapshot type below is the whole contract:
// the dashboard renders it, and anything that wants Prometheus can translate it
// in one place.
//
// Everything is safe for concurrent use — collectors are written from every
// poller goroutine and read from the HTTP handler.
package metrics

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Counter is a monotonic count.
type Counter struct{ n atomic.Int64 }

func (c *Counter) Inc()         { c.n.Add(1) }
func (c *Counter) Add(d int64)  { c.n.Add(d) }
func (c *Counter) Value() int64 { return c.n.Load() }

// Gauge is a value that moves in both directions.
type Gauge struct {
	mu sync.RWMutex
	v  float64
}

func (g *Gauge) Set(v float64) {
	g.mu.Lock()
	g.v = v
	g.mu.Unlock()
}

func (g *Gauge) Value() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.v
}

// RateWindow measures requests per second over a sliding window.
//
// A one-minute window of one-second buckets. Averaging since process start
// would smooth away exactly the thing an operator is looking for — the burst
// that tripped a 429 — while a shorter window on a feed that legitimately polls
// once every ten minutes would read a flat zero and look broken.
type RateWindow struct {
	mu      sync.Mutex
	buckets [60]int64
	last    int64 // unix second of the newest bucket
	now     func() time.Time
}

// NewRateWindow builds a window.
func NewRateWindow(now func() time.Time) *RateWindow {
	if now == nil {
		now = time.Now
	}
	return &RateWindow{now: now}
}

// Mark records one event.
func (r *RateWindow) Mark() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.advance()
	r.buckets[r.last%60]++
}

// advance zeroes the buckets that have expired since the last write.
func (r *RateWindow) advance() {
	sec := r.now().Unix()
	if r.last == 0 {
		r.last = sec
		return
	}
	if sec == r.last {
		return
	}
	gap := sec - r.last
	if gap >= 60 {
		r.buckets = [60]int64{}
	} else {
		for i := int64(1); i <= gap; i++ {
			r.buckets[(r.last+i)%60] = 0
		}
	}
	r.last = sec
}

// PerSecond is the mean rate over the window.
func (r *RateWindow) PerSecond() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.advance()
	var total int64
	for _, b := range r.buckets {
		total += b
	}
	return float64(total) / 60.0
}

// PerMinute is the count over the window.
func (r *RateWindow) PerMinute() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.advance()
	var total int64
	for _, b := range r.buckets {
		total += b
	}
	return total
}

// Registry holds every metric the process reports.
type Registry struct {
	Requests    *Counter
	Messages    *Counter
	Errors      *Counter
	Throttles   *Counter
	Retries     *Counter
	Sweeps      *Counter
	RequestRate *RateWindow

	// --- Golden Signals -----------------------------------------------------

	// PublishLatency is the poll-to-Kafka delta: the age of a record when it is
	// handed to the producer. This is the number that decides whether a screen
	// behind a bar is showing a stale score, and it is measured end-to-end
	// rather than as an upstream round trip.
	PublishLatency *Histogram
	// RequestLatency is the upstream HTTP round trip, kept separate because the
	// two answer different questions: one blames the provider, the other blames
	// us.
	RequestLatency *Histogram

	// IngestAge is the always-valid staleness signal: how old a record is when
	// it reaches the pipeline. In seconds.
	IngestAge *Histogram
	// ProviderSkew is the provider's clock minus ours, in seconds, from the
	// HTTP Date header. Signed: a negative value means their clock is behind.
	ProviderSkew *Gauge
	// LiveMatchLag estimates how far behind live play the provider's data is,
	// in seconds. Only first-half fixtures contribute — see the drift note in
	// pkg/ingest.
	LiveMatchLag *Histogram

	// MessageRate and ErrorRate carry one hour of per-minute history for the
	// header cards.
	MessageRate *TimeSeries
	ErrorSeries *TimeSeries

	// Partitions counts Kafka writes per partition, for hot-partition
	// detection. Keyed by "topic/partition".
	partitions map[string]*Counter

	// dropped counts records refused before publication, keyed by
	// "sport/reason". Scope enforcement is only safe because it is metered:
	// silently discarding half a feed is its own failure mode, and an operator
	// seeing a thin card has to be able to tell a quiet day from a licence
	// mismatch.
	dropped map[string]*Counter

	// Host is the edge appliance's resource state.
	Host *HostMetrics

	// Golf is the golf feed's polling state. It is called out separately
	// because golf is the one provider whose rate is driven by a cache
	// lifetime on a separate subscription rather than by the licence budget the
	// limiter manages — so an operator cannot infer it from the tier.
	Golf *GolfMetrics

	// Flink is the downstream state-buffer gauge. Populated only when a Flink
	// endpoint is configured; see pkg/ingest/flink.go for why it is optional.
	Flink *FlinkMetrics

	mu       sync.RWMutex
	perSport map[string]*SportMetrics
	started  time.Time
	now      func() time.Time
}

// HostMetrics is the Minisforum edge box's resource usage.
type HostMetrics struct {
	CPUPercent *Gauge
	MemUsed    *Gauge
	MemTotal   *Gauge
	MemPercent *Gauge
	LoadAvg1   *Gauge
	ProcessRSS *Gauge
	Goroutines *Gauge
	CPUSeries  *TimeSeries
	MemSeries  *TimeSeries
	// Available is false when no host sampler could be started, so the card
	// says "unavailable" instead of reporting a confident zero.
	Available *Flag
}

// GolfMetrics tracks the golf feed's polling cadence.
type GolfMetrics struct {
	// CadenceMinutes is the current interval between leaderboard polls.
	CadenceMinutes *Gauge
	// Throttled is true while the 429 hard floor is in force.
	Throttled *Flag
}

// FlinkMetrics tracks the downstream state buffer.
type FlinkMetrics struct {
	StateBytes    *Gauge
	StateSeries   *TimeSeries
	CheckpointAge *Gauge
	TTLSeconds    *Gauge
	Reachable     *Flag
	// Configured is false when no Flink endpoint was supplied at all, which is
	// the default and is not an error.
	Configured *Flag
}

// Flag is a concurrency-safe boolean.
type Flag struct {
	mu sync.RWMutex
	v  bool
}

func (f *Flag) Set(v bool) {
	f.mu.Lock()
	f.v = v
	f.mu.Unlock()
}

func (f *Flag) Value() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.v
}

// SportMetrics is one sport's counters.
//
// Errors are split by status class because 4xx and 5xx mean opposite things: a
// 4xx is our request being wrong, a 5xx is the provider failing. Folding them
// into one counter — which this registry used to do — makes an outage and a
// bad parameter look identical on the dashboard.
type SportMetrics struct {
	Requests  *Counter
	Messages  *Counter
	Errors    *Counter
	Throttles *Counter
	Errors4xx *Counter
	Errors5xx *Counter
	// Dropped counts records refused by scope enforcement.
	Dropped *Counter
	// ErrorsTransport counts failures that never reached the provider at all —
	// DNS, TCP, TLS, timeouts. Not attributable to them.
	ErrorsTransport *Counter

	// LastLatency is the most recent round trip, in milliseconds.
	LastLatency *Gauge
	// Tokens is the limiter's current bucket fill.
	Tokens *Gauge
	// CrowdWeight is the allocator's current share for this sport.
	CrowdWeight *Gauge
	// QuotaRemaining is the provider's own reported daily headroom.
	QuotaRemaining *Gauge

	// MessageRate is one hour of per-minute throughput for the sparkline.
	MessageRate *TimeSeries
	// Latency is this sport's poll-to-publish distribution.
	Latency *Histogram
	// IngestAge is this sport's staleness distribution, in seconds.
	IngestAge *TimeSeries
}

// NewRegistry builds a registry.
func NewRegistry(now func() time.Time) *Registry {
	if now == nil {
		now = time.Now
	}
	return &Registry{
		Requests: &Counter{}, Messages: &Counter{}, Errors: &Counter{},
		Throttles: &Counter{}, Retries: &Counter{}, Sweeps: &Counter{},
		RequestRate: NewRateWindow(now),

		PublishLatency: NewHistogram(LatencyBuckets),
		RequestLatency: NewHistogram(LatencyBuckets),
		IngestAge:      NewHistogram([]float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600}),
		ProviderSkew:   &Gauge{},
		LiveMatchLag:   NewHistogram([]float64{5, 15, 30, 60, 120, 300, 600}),

		MessageRate: NewTimeSeries(now),
		ErrorSeries: NewTimeSeries(now),
		partitions:  map[string]*Counter{},

		Host: &HostMetrics{
			CPUPercent: &Gauge{}, MemUsed: &Gauge{}, MemTotal: &Gauge{},
			MemPercent: &Gauge{}, LoadAvg1: &Gauge{}, ProcessRSS: &Gauge{},
			Goroutines: &Gauge{}, Available: &Flag{},
			CPUSeries: NewTimeSeries(now), MemSeries: NewTimeSeries(now),
		},
		Golf: &GolfMetrics{CadenceMinutes: &Gauge{}, Throttled: &Flag{}},
		Flink: &FlinkMetrics{
			StateBytes: &Gauge{}, CheckpointAge: &Gauge{}, TTLSeconds: &Gauge{},
			Reachable: &Flag{}, Configured: &Flag{},
			StateSeries: NewTimeSeries(now),
		},

		perSport: map[string]*SportMetrics{},
		dropped:  map[string]*Counter{},
		started:  now(), now: now,
	}
}

// Sport returns (creating if needed) the metrics for one sport or vertical.
func (r *Registry) Sport(name string) *SportMetrics {
	r.mu.RLock()
	m, ok := r.perSport[name]
	r.mu.RUnlock()
	if ok {
		return m
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.perSport[name]; ok {
		return m
	}
	m = &SportMetrics{
		Requests: &Counter{}, Messages: &Counter{}, Errors: &Counter{},
		Throttles: &Counter{}, Errors4xx: &Counter{}, Errors5xx: &Counter{},
		ErrorsTransport: &Counter{}, Dropped: &Counter{},
		LastLatency: &Gauge{}, Tokens: &Gauge{}, CrowdWeight: &Gauge{},
		QuotaRemaining: &Gauge{},
		MessageRate:    NewTimeSeries(r.now),
		Latency:        NewHistogram(LatencyBuckets),
		IngestAge:      NewTimeSeries(r.now),
	}
	r.perSport[name] = m
	return m
}

// Uptime is how long the process has been running.
func (r *Registry) Uptime() time.Duration { return r.now().Sub(r.started) }

// SportSnapshot is one sport's metrics, flattened for JSON.
type SportSnapshot struct {
	Sport           string    `json:"sport"`
	Requests        int64     `json:"requests"`
	Messages        int64     `json:"messages"`
	Errors          int64     `json:"errors"`
	Errors4xx       int64     `json:"errors_4xx"`
	Errors5xx       int64     `json:"errors_5xx"`
	ErrorsTransport int64     `json:"errors_transport"`
	Throttles       int64     `json:"throttles"`
	Dropped         int64     `json:"dropped"`
	DropRate        float64   `json:"drop_rate"`
	LatencyMS       float64   `json:"latency_ms"`
	LatencyP95MS    float64   `json:"latency_p95_ms"`
	Tokens          float64   `json:"tokens"`
	CrowdWeight     float64   `json:"crowd_weight"`
	QuotaRemaining  float64   `json:"quota_remaining"`
	MessageSeries   []float64 `json:"message_series"`
}

// Snapshot is the whole registry at an instant.
type Snapshot struct {
	UptimeSeconds  float64         `json:"uptime_seconds"`
	Requests       int64           `json:"requests"`
	Messages       int64           `json:"messages"`
	Errors         int64           `json:"errors"`
	Throttles      int64           `json:"throttles_429"`
	Retries        int64           `json:"retries"`
	Sweeps         int64           `json:"sweeps"`
	RequestsPerSec float64         `json:"requests_per_second"`
	RequestsPerMin int64           `json:"requests_per_minute"`
	Sports         []SportSnapshot `json:"sports"`
}

// Snapshot collects the current values.
func (r *Registry) Snapshot() Snapshot {
	r.mu.RLock()
	names := make([]string, 0, len(r.perSport))
	for n := range r.perSport {
		names = append(names, n)
	}
	sports := make([]SportSnapshot, 0, len(names))
	for _, n := range names {
		m := r.perSport[n]
		dropped := m.Dropped.Value()
		dropRate := 0.0
		if total := m.Messages.Value() + dropped; total > 0 {
			dropRate = float64(dropped) / float64(total)
		}
		sports = append(sports, SportSnapshot{
			Sport: n, Requests: m.Requests.Value(), Messages: m.Messages.Value(),
			Errors: m.Errors.Value(), Errors4xx: m.Errors4xx.Value(),
			Errors5xx: m.Errors5xx.Value(), ErrorsTransport: m.ErrorsTransport.Value(),
			Throttles: m.Throttles.Value(), Dropped: dropped, DropRate: dropRate,
			LatencyMS: m.LastLatency.Value(), LatencyP95MS: m.Latency.Quantile(0.95),
			Tokens: m.Tokens.Value(), CrowdWeight: m.CrowdWeight.Value(),
			QuotaRemaining: m.QuotaRemaining.Value(),
			MessageSeries:  m.MessageRate.Totals(),
		})
	}
	r.mu.RUnlock()

	// Busiest first: the sport an operator is most likely asking about.
	for i := 1; i < len(sports); i++ {
		for j := i; j > 0 && sports[j].Requests > sports[j-1].Requests; j-- {
			sports[j], sports[j-1] = sports[j-1], sports[j]
		}
	}
	return Snapshot{
		UptimeSeconds:  r.Uptime().Seconds(),
		Requests:       r.Requests.Value(),
		Messages:       r.Messages.Value(),
		Errors:         r.Errors.Value(),
		Throttles:      r.Throttles.Value(),
		Retries:        r.Retries.Value(),
		Sweeps:         r.Sweeps.Value(),
		RequestsPerSec: r.RequestRate.PerSecond(),
		RequestsPerMin: r.RequestRate.PerMinute(),
		Sports:         sports,
	}
}

// --- error classification ---------------------------------------------------

// RecordStatus books an upstream response against the right error class.
//
// Status 0 means the request never reached the provider, which is a transport
// failure and must not be attributed to them: an expired TLS certificate on our
// side would otherwise show up on the dashboard as the provider being down.
// 429 is excluded from the error rate entirely — being throttled is the rate
// limiter working, not the feed failing.
func (r *Registry) RecordStatus(sport string, status int) {
	sm := r.Sport(sport)
	switch {
	case status == 0:
		sm.ErrorsTransport.Inc()
		sm.Errors.Inc()
		r.Errors.Inc()
		r.ErrorSeries.Observe()
	case status == 429:
		// Counted as a throttle elsewhere, never as an error.
	case status >= 500:
		sm.Errors5xx.Inc()
		sm.Errors.Inc()
		r.Errors.Inc()
		r.ErrorSeries.Observe()
	case status >= 400:
		sm.Errors4xx.Inc()
		sm.Errors.Inc()
		r.Errors.Inc()
		r.ErrorSeries.Observe()
	}
}

// ErrorRate is the fraction of requests that failed over the whole run.
func (r *Registry) ErrorRate() float64 {
	req := r.Requests.Value()
	if req == 0 {
		return 0
	}
	return float64(r.Errors.Value()) / float64(req)
}

// --- Kafka partitions -------------------------------------------------------

// RecordPartition books one write to a topic partition.
func (r *Registry) RecordPartition(topic string, partition int) {
	key := topic + "/" + itoa(partition)
	r.mu.RLock()
	c, ok := r.partitions[key]
	r.mu.RUnlock()
	if !ok {
		r.mu.Lock()
		if c, ok = r.partitions[key]; !ok {
			c = &Counter{}
			r.partitions[key] = c
		}
		r.mu.Unlock()
	}
	c.Inc()
}

// PartitionSnapshot is one partition's write count.
type PartitionSnapshot struct {
	Topic     string  `json:"topic"`
	Partition int     `json:"partition"`
	Writes    int64   `json:"writes"`
	Share     float64 `json:"share"`
}

// Partitions returns per-partition write counts and their share of the total.
func (r *Registry) Partitions() []PartitionSnapshot {
	r.mu.RLock()
	keys := make([]string, 0, len(r.partitions))
	for k := range r.partitions {
		keys = append(keys, k)
	}
	counts := make(map[string]int64, len(keys))
	for _, k := range keys {
		counts[k] = r.partitions[k].Value()
	}
	r.mu.RUnlock()

	var total int64
	for _, v := range counts {
		total += v
	}
	out := make([]PartitionSnapshot, 0, len(keys))
	for _, k := range keys {
		topic, part := splitPartitionKey(k)
		share := 0.0
		if total > 0 {
			share = float64(counts[k]) / float64(total)
		}
		out = append(out, PartitionSnapshot{
			Topic: topic, Partition: part, Writes: counts[k], Share: share,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

// PartitionSkew is the spread between the busiest and the mean partition,
// expressed as a ratio: 0 is perfectly even, 1 means the hottest partition
// carries twice the mean.
//
// Max-over-mean rather than a standard deviation because the question an
// operator is asking is "is one partition hot", and a single hot partition
// barely moves a standard deviation across sixteen of them.
func (r *Registry) PartitionSkew() float64 {
	parts := r.Partitions()
	if len(parts) < 2 {
		return 0
	}
	var total int64
	var max int64
	for _, p := range parts {
		total += p.Writes
		if p.Writes > max {
			max = p.Writes
		}
	}
	if total == 0 {
		return 0
	}
	mean := float64(total) / float64(len(parts))
	if mean == 0 {
		return 0
	}
	return (float64(max) - mean) / mean
}

func splitPartitionKey(k string) (string, int) {
	i := strings.LastIndexByte(k, '/')
	if i < 0 {
		return k, 0
	}
	n, err := strconv.Atoi(k[i+1:])
	if err != nil {
		return k[:i], 0
	}
	return k[:i], n
}

func itoa(n int) string { return strconv.Itoa(n) }

// --- dropped records ---------------------------------------------------------

// DropRateWarn is the share of a sport's feed that may be dropped before the
// dashboard raises a licence-mismatch warning.
//
// Five percent. Below that, an occasional out-of-scope fixture is normal — a
// bulk sweep returns the provider's whole card and some of it will always sit
// outside a venue's package. Above it, the licence and the feed disagree about
// what the venue bought, and somebody needs to look.
const DropRateWarn = 0.05

// DropSampleFloor is how many records a sport must produce before its drop rate
// is treated as meaningful.
//
// A rate is not evidence on a small sample. The first live run of scope
// enforcement dropped 7 of 7 basketball records and 4 of 4 soccer records and
// reported both as a 100% licence mismatch — but the cause was simply that
// nothing the venue licensed was playing in that window, which is an ordinary
// Tuesday rather than a misconfigured licence. Warning on it would teach an
// operator to ignore the warning, which is worse than not having it.
//
// Twenty is enough that a sport genuinely serving the wrong leagues will cross
// it within a couple of sweeps, while a quiet card stays quiet.
const DropSampleFloor = 20

// RecordDrop books one record refused before publication.
func (r *Registry) RecordDrop(sport, reason string) {
	key := sport + "/" + reason
	r.mu.RLock()
	c, ok := r.dropped[key]
	r.mu.RUnlock()
	if !ok {
		r.mu.Lock()
		if c, ok = r.dropped[key]; !ok {
			c = &Counter{}
			r.dropped[key] = c
		}
		r.mu.Unlock()
	}
	c.Inc()
	r.Sport(sport).Dropped.Inc()
}

// DropSnapshot is one sport-and-reason drop count.
type DropSnapshot struct {
	Sport  string `json:"sport"`
	Reason string `json:"reason"`
	Count  int64  `json:"count"`
}

// Drops returns every drop counter, sorted.
func (r *Registry) Drops() []DropSnapshot {
	r.mu.RLock()
	out := make([]DropSnapshot, 0, len(r.dropped))
	for key, c := range r.dropped {
		sport, reason, _ := strings.Cut(key, "/")
		out = append(out, DropSnapshot{Sport: sport, Reason: reason, Count: c.Value()})
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sport != out[j].Sport {
			return out[i].Sport < out[j].Sport
		}
		return out[i].Reason < out[j].Reason
	})
	return out
}

// DropRate is a sport's dropped share of everything it produced.
//
// The denominator is published plus dropped, not published alone: a sport whose
// entire feed is being refused would otherwise divide by zero and report a
// healthy 0%, which is the exact opposite of the truth.
func (r *Registry) DropRate(sport string) float64 {
	m := r.Sport(sport)
	dropped := m.Dropped.Value()
	total := m.Messages.Value() + dropped
	if total == 0 {
		return 0
	}
	return float64(dropped) / float64(total)
}
