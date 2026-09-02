package metrics

import (
	"bytes"
	"math"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func exposition(t *testing.T, r *Registry) string {
	t.Helper()
	var b bytes.Buffer
	if err := r.WritePrometheus(&b, "Offload Ingest"); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	return b.String()
}

// TestExpositionParses is the check that matters for a hand-written encoder:
// every line must be a comment or a well-formed sample, HELP and TYPE must
// precede their metric, and no metric may be declared twice — a duplicate HELP
// makes Prometheus reject the whole scrape rather than skip one line.
func TestExpositionParses(t *testing.T) {
	r := NewRegistry(time.Now)
	r.Requests.Add(10)
	r.RecordStatus("football", 500)
	r.RecordStatus("football", 404)
	r.PublishLatency.Observe(120)
	r.RecordPartition("ingest", 3)
	r.Host.Available.Set(true)
	r.Host.CPUPercent.Set(12.5)
	r.Flink.Configured.Set(true)
	r.Flink.StateBytes.Set(1 << 20)

	out := exposition(t, r)
	declared := map[string]string{}
	seenSample := map[string]bool{}

	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "# HELP ") {
			name := strings.Fields(line)[2]
			if _, dup := declared[name]; dup {
				t.Errorf("duplicate HELP for %s — Prometheus rejects the whole scrape", name)
			}
			declared[name] = ""
			continue
		}
		if strings.HasPrefix(line, "# TYPE ") {
			f := strings.Fields(line)
			declared[f[2]] = f[3]
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		// A sample: name[{labels}] value. Split on the LAST space — a label
		// value may legitimately contain one, e.g. product="Offload Ingest".
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 {
			t.Errorf("malformed sample line: %q", line)
			continue
		}
		name, value := line[:sp], line[sp+1:]
		if _, err := strconv.ParseFloat(value, 64); err != nil &&
			value != "NaN" && value != "+Inf" && value != "-Inf" {
			t.Errorf("sample %q has a non-numeric value %q", name, value)
		}
		base := name
		if i := strings.IndexByte(base, '{'); i >= 0 {
			if !strings.HasSuffix(name, "}") {
				t.Errorf("unbalanced label set: %q", line)
			}
			base = base[:i]
		}
		seenSample[base] = true
		// Histogram families expose _bucket/_sum/_count under the declared name.
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			base = strings.TrimSuffix(base, suffix)
		}
		if _, ok := declared[base]; !ok {
			t.Errorf("sample %q has no preceding HELP/TYPE", base)
		}
	}
	if len(declared) == 0 {
		t.Fatal("no metrics were exposed")
	}
	if !strings.HasPrefix(out, "# HELP") {
		t.Error("exposition should open with a HELP line")
	}
}

// TestEveryMetricIsNamespaced keeps four products' metrics distinguishable in
// one Prometheus server.
func TestEveryMetricIsNamespaced(t *testing.T) {
	r := NewRegistry(time.Now)
	for _, line := range strings.Split(exposition(t, r), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, Namespace+"_") {
			t.Errorf("sample is not namespaced: %q", line)
		}
	}
}

// TestErrorsAreExposedByClass. The dashboard and the scrape must agree that a
// 4xx and a 5xx are different failures.
func TestErrorsAreExposedByClass(t *testing.T) {
	r := NewRegistry(time.Now)
	r.RecordStatus("football", 404)
	r.RecordStatus("football", 503)
	r.RecordStatus("football", 0)
	out := exposition(t, r)
	for _, want := range []string{
		`sport_errors_total{sport="football",class="4xx"} 1`,
		`sport_errors_total{sport="football",class="5xx"} 1`,
		`sport_errors_total{sport="football",class="transport"} 1`,
	} {
		if !strings.Contains(out, Namespace+"_"+want) {
			t.Errorf("exposition is missing %s", want)
		}
	}
}

// TestHistogramIsCumulative pins the Prometheus contract that bucket counts
// accumulate and the +Inf bucket equals the total.
func TestHistogramIsCumulative(t *testing.T) {
	h := NewHistogram([]float64{10, 100, 1000})
	for _, v := range []float64{5, 50, 500, 5000} {
		h.Observe(v)
	}
	bounds, counts := h.Cumulative()
	if len(counts) != len(bounds)+1 {
		t.Fatalf("got %d counts for %d bounds", len(counts), len(bounds))
	}
	want := []uint64{1, 2, 3, 4}
	for i, w := range want {
		if counts[i] != w {
			t.Errorf("cumulative bucket %d = %d, want %d", i, counts[i], w)
		}
	}
	if counts[len(counts)-1] != h.Count() {
		t.Errorf("+Inf bucket %d != total %d", counts[len(counts)-1], h.Count())
	}
	if h.Sum() != 5555 {
		t.Errorf("sum = %v, want 5555", h.Sum())
	}
}

func TestHistogramQuantile(t *testing.T) {
	h := NewHistogram(LatencyBuckets)
	for i := 1; i <= 100; i++ {
		h.Observe(float64(i))
	}
	if p50 := h.Quantile(0.5); p50 < 45 || p50 > 55 {
		t.Errorf("p50 = %v, want ~50", p50)
	}
	if p95 := h.Quantile(0.95); p95 < 90 || p95 > 100 {
		t.Errorf("p95 = %v, want ~95", p95)
	}
	if empty := NewHistogram(LatencyBuckets).Quantile(0.95); empty != 0 {
		t.Errorf("empty histogram quantile = %v, want 0", empty)
	}
}

// TestLabelsAreEscaped: a topic name with a quote would otherwise produce a
// line the scraper rejects.
func TestLabelsAreEscaped(t *testing.T) {
	r := NewRegistry(time.Now)
	r.RecordPartition(`weird"topic`, 1)
	out := exposition(t, r)
	if !strings.Contains(out, `topic="weird\"topic"`) {
		t.Errorf("label was not escaped:\n%s", out)
	}
}

func TestFormatFloatUsesExpositionTokens(t *testing.T) {
	for _, tc := range []struct {
		in   float64
		want string
	}{
		{0, "0"}, {1.5, "1.5"}, {math.Inf(1), "+Inf"},
		{math.Inf(-1), "-Inf"}, {math.NaN(), "NaN"},
	} {
		if got := formatFloat(tc.in); got != tc.want {
			t.Errorf("formatFloat(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestFlinkIsAbsentUntilConfigured. A gauge that is always zero because nothing
// is being scraped graphs as a healthy flat line, which is worse than an absent
// series that a scraper can alert on.
func TestFlinkIsAbsentUntilConfigured(t *testing.T) {
	r := NewRegistry(time.Now)
	if strings.Contains(exposition(t, r), "flink_state_bytes") {
		t.Error("Flink metrics exposed while unconfigured")
	}
	r.Flink.Configured.Set(true)
	if !strings.Contains(exposition(t, r), "flink_state_bytes") {
		t.Error("Flink metrics missing once configured")
	}
}

// TestHostIsAbsentWhenUnreadable, for the same reason.
func TestHostIsAbsentWhenUnreadable(t *testing.T) {
	r := NewRegistry(time.Now)
	out := exposition(t, r)
	if strings.Contains(out, "host_cpu_percent") {
		t.Error("host CPU exposed while the sampler reported nothing")
	}
	// Process-level figures are always available and must always be exposed:
	// "our process is leaking" is a distinct failure from "the box is full".
	if !strings.Contains(out, "process_goroutines") {
		t.Error("process metrics should always be exposed")
	}
}

func TestHandlerServesTheExpositionContentType(t *testing.T) {
	r := NewRegistry(time.Now)
	rec := httptest.NewRecorder()
	r.PrometheusHandler("Offload Ingest").ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	res := rec.Result()
	if res.StatusCode != 200 {
		t.Fatalf("status = %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content type = %q", ct)
	}
}

// --- partitions -------------------------------------------------------------

func TestPartitionSkewDetectsAHotPartition(t *testing.T) {
	r := NewRegistry(time.Now)
	for p := 0; p < 4; p++ {
		for i := 0; i < 100; i++ {
			r.RecordPartition("ingest", p)
		}
	}
	if skew := r.PartitionSkew(); skew > 0.01 {
		t.Errorf("even distribution reported skew %v, want ~0", skew)
	}
	for i := 0; i < 400; i++ {
		r.RecordPartition("ingest", 1)
	}
	if skew := r.PartitionSkew(); skew < 0.5 {
		t.Errorf("hot partition reported skew %v, want it well above 0.5", skew)
	}
	var shares float64
	for _, p := range r.Partitions() {
		shares += p.Share
	}
	if shares < 0.999 || shares > 1.001 {
		t.Errorf("partition shares sum to %v, want 1", shares)
	}
}

func TestPartitionSkewIsZeroBelowTwoPartitions(t *testing.T) {
	r := NewRegistry(time.Now)
	r.RecordPartition("ingest", 0)
	if skew := r.PartitionSkew(); skew != 0 {
		t.Errorf("skew = %v with one partition, want 0", skew)
	}
}

// --- time series ------------------------------------------------------------

func TestTimeSeriesHoldsOneHour(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	ts := NewTimeSeries(func() time.Time { return clock })
	for i := 0; i < SeriesBuckets; i++ {
		if i > 0 {
			clock = clock.Add(time.Minute)
		}
		ts.Add(float64(i))
	}
	totals := ts.Totals()
	if len(totals) != SeriesBuckets {
		t.Fatalf("got %d points, want %d", len(totals), SeriesBuckets)
	}
	// Oldest first, and the last point is the minute currently being filled.
	if totals[len(totals)-1] != float64(SeriesBuckets-1) {
		t.Errorf("newest point = %v, want %v", totals[len(totals)-1], SeriesBuckets-1)
	}
	if totals[0] != 0 {
		t.Errorf("oldest point = %v, want the first write to have aged to the window edge", totals[0])
	}
}

func TestTimeSeriesExpiresBeyondTheWindow(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	ts := NewTimeSeries(func() time.Time { return clock })
	ts.Add(100)
	clock = clock.Add(2 * time.Hour)
	for _, v := range ts.Totals() {
		if v != 0 {
			t.Fatalf("a point survived two hours: %v", v)
		}
	}
}

// TestTimeSeriesClearsSkippedMinutes guards the ring arithmetic: a feed that
// polls every ten minutes must not have an hour-old value wrap onto a live
// bucket and read as current traffic.
func TestTimeSeriesClearsSkippedMinutes(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	ts := NewTimeSeries(func() time.Time { return clock })
	ts.Add(999)
	clock = clock.Add(SeriesBuckets * time.Minute) // exactly one full lap
	ts.Add(1)
	totals := ts.Totals()
	if totals[len(totals)-1] != 1 {
		t.Errorf("newest bucket = %v, want 1", totals[len(totals)-1])
	}
	var sum float64
	for _, v := range totals {
		sum += v
	}
	if sum != 1 {
		t.Errorf("series totals %v; the stale value wrapped onto a live bucket", sum)
	}
}

func TestTimeSeriesMeans(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	ts := NewTimeSeries(func() time.Time { return clock })
	ts.Add(10)
	ts.Add(20)
	if got := ts.LastMean(); got != 15 {
		t.Errorf("LastMean = %v, want 15", got)
	}
	// A minute with no observations must report zero, not carry the last value
	// forward — a flat line from a carry-forward hides a stalled feed.
	clock = clock.Add(time.Minute)
	means := ts.Means()
	if means[len(means)-1] != 0 {
		t.Errorf("empty minute = %v, want 0", means[len(means)-1])
	}
	if got := ts.LastMean(); got != 15 {
		t.Errorf("LastMean should fall back to the last populated minute, got %v", got)
	}
}
