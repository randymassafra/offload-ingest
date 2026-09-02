package dashboard

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/offloadintelligence/offload-ingest/pkg/dds"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
	"github.com/offloadintelligence/offload-ingest/pkg/licensing"
	"github.com/offloadintelligence/offload-ingest/pkg/metrics"
)

var testClock = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

// fakeProvider stands in for a running pipeline.
type fakeProvider struct {
	status  licensing.Status
	mode    ingest.Mode
	reg     *metrics.Registry
	budgets []ingest.Budget
	usage   []ingest.Stat
	plans   []ingest.Plan
	quotas  map[apisports.Vertical]apisports.Quota
	catalog []ingest.SportStatus
}

func (f *fakeProvider) LicenseStatus() licensing.Status { return f.status }
func (f *fakeProvider) Mode() ingest.Mode               { return f.mode }
func (f *fakeProvider) Registry() *metrics.Registry     { return f.reg }
func (f *fakeProvider) Budgets() []ingest.Budget        { return f.budgets }
func (f *fakeProvider) Usage() []ingest.Stat            { return f.usage }
func (f *fakeProvider) Plans() []ingest.Plan            { return f.plans }
func (f *fakeProvider) Weights() []ingest.Snapshot      { return nil }
func (f *fakeProvider) SportCatalog() []ingest.SportStatus {
	return f.catalog
}
func (f *fakeProvider) Quotas() map[apisports.Vertical]apisports.Quota { return f.quotas }

func healthy() *fakeProvider {
	tier, _ := licensing.Tier{Name: licensing.TierFree}.Resolve()
	v := apisports.VerticalFootball
	reg := metrics.NewRegistry(func() time.Time { return testClock })
	reg.Requests.Add(100)
	reg.Messages.Add(400)
	reg.PublishLatency.Observe(120)
	reg.MessageRate.Add(40)

	return &fakeProvider{
		mode: ingest.ModeProduction,
		reg:  reg,
		status: licensing.Status{
			Valid: true, LicenseID: "lic-1", TenantID: "acme-arena",
			VenueName: "Acme Arena", Tier: tier, Sports: []string{"soccer"},
			ExpiresAt: testClock.AddDate(1, 0, 0), Deadline: testClock.AddDate(1, 0, 7),
		},
		budgets: []ingest.Budget{{Vertical: v, Requests: 90, Weight: 0.5, PerMinute: 9}},
		usage:   []ingest.Stat{{Vertical: v, Today: 12, Budget: 90, Pressure: 0.13}},
		plans:   []ingest.Plan{{Vertical: v, StateName: "live", IntervalSeconds: 30, TargetSeconds: 8}},
		quotas: map[apisports.Vertical]apisports.Quota{
			v: {MinuteLimit: 10, MinuteRemaining: 8, DayLimit: 100, DayRemaining: 88, Present: true},
		},
		catalog: []ingest.SportStatus{
			{Sport: "soccer", Provider: "apisports", State: "live", Health: "ok", Live: true},
			{Sport: "cricket", Provider: "cricbuzz", Health: "unknown", Note: "simulation only"},
		},
	}
}

func serve(t *testing.T, p Provider) *Server {
	t.Helper()
	return New(Config{Provider: p, Version: "test", Now: func() time.Time { return testClock }})
}

func get(t *testing.T, s *Server, path string) (*http.Response, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	res := rec.Result()
	body, _ := io.ReadAll(res.Body)
	return res, string(body)
}

func state(t *testing.T, s *Server) State {
	t.Helper()
	_, body := get(t, s, "/api/state")
	var st State
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	return st
}

// TestStateCarriesEveryGoldenSignal is the DDS contract: the required metrics
// must all be present on the endpoint, not merely drawn on the page.
func TestStateCarriesEveryGoldenSignal(t *testing.T) {
	_, body := get(t, serve(t, healthy()), "/api/state")
	for _, field := range []string{
		"throughput", "latency", "drift", "errors", "partitions", "host",
		"flink", "providers", "budgets",
		"ingest_age_seconds", "provider_skew_seconds", "live_match_lag_seconds",
		"class_4xx", "class_5xx", "transport",
		"cpu_percent", "memory_percent", "state_bytes",
	} {
		if !strings.Contains(body, `"`+field+`"`) {
			t.Errorf("state is missing the %q field required by the DDS", field)
		}
	}
}

// TestEveryCardSignalCarriesAnHourOfHistory: the DDS mandates a sparkline on
// every metric card, so every Signal must expose a series the page can draw.
func TestEveryCardSignalCarriesAnHourOfHistory(t *testing.T) {
	st := state(t, serve(t, healthy()))
	for name, series := range map[string][]float64{
		"throughput": st.Throughput.Series,
		"errors":     st.Errors.Series,
		"host cpu":   st.Host.CPUSeries,
		"host mem":   st.Host.MemSeries,
	} {
		if len(series) != metrics.SeriesBuckets {
			t.Errorf("%s series has %d points, want %d (one hour of minutes)",
				name, len(series), metrics.SeriesBuckets)
		}
	}
}

// TestLatencyThresholdTriggersAnAlert pins the DDS rule that a card pulses
// above 2s.
func TestLatencyThresholdTriggersAnAlert(t *testing.T) {
	p := healthy()
	p.reg.PublishLatency.Observe(4000)
	for i := 0; i < 40; i++ {
		p.reg.PublishLatency.Observe(3500)
	}
	st := state(t, serve(t, p))
	if !st.Latency.Alert {
		t.Errorf("latency p95 %.0fms should alert above %dms", st.Latency.Value, dds.LatencyAlertMS)
	}
	if st.Latency.Health != dds.HealthBad {
		t.Errorf("latency health = %s, want bad", st.Latency.Health)
	}
	if st.Health != dds.HealthBad {
		t.Errorf("header health = %s; a red card must not leave the header green", st.Health)
	}
}

// TestErrorRateThresholdTriggersAnAlert pins the 5% rule, and that a 429 is not
// counted as an error — being throttled is the limiter working.
func TestErrorRateThresholdTriggersAnAlert(t *testing.T) {
	p := healthy()
	for i := 0; i < 10; i++ {
		p.reg.RecordStatus("football", 500)
	}
	st := state(t, serve(t, p))
	if st.Errors.Rate < dds.ErrorRateAlert {
		t.Fatalf("error rate = %v, expected it above the threshold", st.Errors.Rate)
	}
	if !st.Errors.Alert {
		t.Error("error card should alert above 5%")
	}
	if st.Errors.Class5xx != 10 {
		t.Errorf("5xx count = %d, want 10", st.Errors.Class5xx)
	}

	before := st.Errors.Total
	for i := 0; i < 50; i++ {
		p.reg.RecordStatus("football", 429)
	}
	if after := state(t, serve(t, p)).Errors.Total; after != before {
		t.Errorf("429s were counted as errors: %d -> %d", before, after)
	}
}

// TestErrorsAreAttributedByClass: a 4xx is our fault and a 5xx is theirs.
// Folding them together makes an outage and a bad parameter look identical.
func TestErrorsAreAttributedByClass(t *testing.T) {
	p := healthy()
	p.reg.RecordStatus("football", 404)
	p.reg.RecordStatus("football", 503)
	p.reg.RecordStatus("football", 0) // never reached the provider
	st := state(t, serve(t, p))
	if st.Errors.Class4xx != 1 || st.Errors.Class5xx != 1 || st.Errors.Transport != 1 {
		t.Errorf("classes = 4xx:%d 5xx:%d transport:%d, want 1/1/1",
			st.Errors.Class4xx, st.Errors.Class5xx, st.Errors.Transport)
	}
}

// TestPartitionSkewDetectsAHotPartition is the hot-partition signal.
func TestPartitionSkewDetectsAHotPartition(t *testing.T) {
	p := healthy()
	// Even distribution first.
	for i := 0; i < 4; i++ {
		for n := 0; n < 25; n++ {
			p.reg.RecordPartition("ingest", i)
		}
	}
	if st := state(t, serve(t, p)); st.Partitions.Skew > 0.1 {
		t.Errorf("even distribution reported skew %v", st.Partitions.Skew)
	}
	// Now overload one.
	for n := 0; n < 400; n++ {
		p.reg.RecordPartition("ingest", 2)
	}
	st := state(t, serve(t, p))
	if st.Partitions.Hottest != 2 {
		t.Errorf("hottest partition = %d, want 2", st.Partitions.Hottest)
	}
	if !st.Partitions.Alert {
		t.Errorf("skew %v should have alerted", st.Partitions.Skew)
	}
}

// TestFlinkCardIsHonestWhenUnconfigured. This metric belongs to another
// process; showing a confident zero for it would be worse than showing nothing.
func TestFlinkCardIsHonestWhenUnconfigured(t *testing.T) {
	st := state(t, serve(t, healthy()))
	if st.Flink.Configured {
		t.Fatal("Flink should be unconfigured by default")
	}
	if st.Flink.Health != dds.HealthUnknown {
		t.Errorf("health = %s, want unknown when nothing is being scraped", st.Flink.Health)
	}
	if !strings.Contains(st.Flink.Note, "Flink product") {
		t.Errorf("note should say where the metric belongs; got %q", st.Flink.Note)
	}
	if st.Flink.Alert {
		t.Error("an unconfigured card must not pulse")
	}
}

func TestFlinkCardAlertsWhenConfiguredButUnreachable(t *testing.T) {
	p := healthy()
	p.reg.Flink.Configured.Set(true)
	p.reg.Flink.Reachable.Set(false)
	st := state(t, serve(t, p))
	if !st.Flink.Alert || st.Flink.Health != dds.HealthBad {
		t.Errorf("configured-but-unreachable should alert; got health=%s alert=%v",
			st.Flink.Health, st.Flink.Alert)
	}
}

// TestSidebarListsEverySport: the estate, not just what is running.
func TestSidebarListsEverySport(t *testing.T) {
	st := state(t, serve(t, healthy()))
	if len(st.Providers) != 2 {
		t.Fatalf("sidebar has %d rows, want 2", len(st.Providers))
	}
	var live, muted int
	for _, p := range st.Providers {
		if p.Live {
			live++
		} else {
			muted++
		}
	}
	if live != 1 || muted != 1 {
		t.Errorf("live=%d muted=%d, want 1/1", live, muted)
	}
}

// TestSimulationModeIsAlwaysAnnounced: an operator must never mistake generated
// payloads for real ones. That confusion is the expensive kind.
func TestSimulationModeIsAlwaysAnnounced(t *testing.T) {
	p := healthy()
	p.mode = ingest.ModeSimulation
	_, body := get(t, serve(t, p), "/api/state")
	if !strings.Contains(body, "SIMULATION MODE") {
		t.Error("simulation mode is not called out in the warnings")
	}
}

func TestGraceIsWarnedAbout(t *testing.T) {
	p := healthy()
	p.status.InGrace = true
	p.status.ExpiresAt = testClock.Add(-48 * time.Hour)
	p.status.Deadline = testClock.Add(5 * 24 * time.Hour)
	_, body := get(t, serve(t, p), "/api/state")
	if !strings.Contains(body, "grace period") {
		t.Errorf("grace not surfaced; body was: %s", body)
	}
}

// TestHealthFollowsTheLicence: an unlicensed process is about to stop, so it
// must not report itself healthy to whatever is watching it.
func TestHealthFollowsTheLicence(t *testing.T) {
	p := healthy()
	s := serve(t, p)
	if res, _ := get(t, s, "/healthz"); res.StatusCode != http.StatusOK {
		t.Errorf("healthy process reported %d", res.StatusCode)
	}

	p.status.Valid = false
	p.status.Error = "signature does not verify"
	res, _ := get(t, s, "/healthz")
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("unlicensed process reported %d, want 503", res.StatusCode)
	}
	_, body := get(t, s, "/api/state")
	if !strings.Contains(body, "LICENCE INVALID") {
		t.Error("an invalid licence is not surfaced on the state endpoint")
	}
}

// TestPageIsSelfContained. A venue appliance may have no outbound internet
// beyond the API host, and a page that renders blank because a CDN is
// unreachable fails exactly when someone needs it.
func TestPageIsSelfContained(t *testing.T) {
	res, body := get(t, serve(t, healthy()), "/")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("content type = %q", ct)
	}
	for _, forbidden := range []string{
		"<script src=", "<link rel=\"stylesheet\"", "@import", "//cdn", "fonts.googleapis",
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("page references an external resource (%q)", forbidden)
		}
	}
	if !strings.Contains(body, "/api/state") {
		t.Error("page does not poll the state endpoint")
	}
}

// TestPageUsesTheMandatedPalette guards against a product drifting off the
// design system.
func TestPageUsesTheMandatedPalette(t *testing.T) {
	_, body := get(t, serve(t, healthy()), "/")
	for _, token := range []string{dds.Background, dds.Highlight, dds.Label} {
		if !strings.Contains(body, token) {
			t.Errorf("page does not use the mandated colour %s", token)
		}
	}
	if !strings.Contains(body, "repeat(12,") {
		t.Error("page does not use the 12-column grid")
	}
	if !strings.Contains(body, "prefers-reduced-motion") {
		t.Error("the alert pulse has no reduced-motion guard")
	}
}

func TestNilProviderDoesNotPanic(t *testing.T) {
	s := New(Config{})
	if res, _ := get(t, s, "/api/state"); res.StatusCode != http.StatusOK {
		t.Errorf("status = %d with no provider", res.StatusCode)
	}
	if res, _ := get(t, s, "/healthz"); res.StatusCode != http.StatusOK {
		t.Errorf("healthz = %d with no provider", res.StatusCode)
	}
	if res, _ := get(t, s, "/"); res.StatusCode != http.StatusOK {
		t.Errorf("index = %d with no provider", res.StatusCode)
	}
}

func TestUnknownPathIs404(t *testing.T) {
	if res, _ := get(t, serve(t, healthy()), "/nope"); res.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", res.StatusCode)
	}
}

func TestStartBindsAndReportsTheAddress(t *testing.T) {
	s := New(Config{Addr: "127.0.0.1:0", Provider: healthy()})
	addr, err := s.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Errorf("bound address = %q", addr)
	}
	res, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Errorf("status = %d", res.StatusCode)
	}
}

// --- scope enforcement -------------------------------------------------------

// TestLicenceMismatchWarnsOnlyWithEnoughVolume. The first live run of scope
// enforcement dropped 7 of 7 basketball records and reported a 100% licence
// mismatch — but the cause was that nothing licensed was playing, not a bad
// licence. A warning that fires on an ordinary quiet card teaches operators to
// ignore it.
func TestLicenceMismatchWarnsOnlyWithEnoughVolume(t *testing.T) {
	p := healthy()
	// A small sample, entirely dropped: suspicious, but not evidence.
	for i := 0; i < 7; i++ {
		p.reg.RecordDrop("ncaab", "out_of_scope")
	}
	st := state(t, serve(t, p))
	if len(st.Drops) != 1 {
		t.Fatalf("got %d drop rows, want 1", len(st.Drops))
	}
	row := st.Drops[0]
	if !row.Inconclusive {
		t.Error("7 records is below the sample floor and should be inconclusive")
	}
	if row.Mismatch {
		t.Error("a licence mismatch was declared on 7 records")
	}
	for _, w := range st.Warnings {
		if strings.Contains(w.Text, "LICENCE MISMATCH") {
			t.Errorf("warned on an insufficient sample: %s", w.Text)
		}
	}
}

// TestLicenceMismatchWarnsOnceTheSampleIsBigEnough is the other half: a sport
// genuinely serving the wrong leagues must be reported.
func TestLicenceMismatchWarnsOnceTheSampleIsBigEnough(t *testing.T) {
	p := healthy()
	for i := 0; i < metrics.DropSampleFloor+5; i++ {
		p.reg.RecordDrop("soccer", "out_of_scope")
	}
	st := state(t, serve(t, p))
	if len(st.Drops) == 0 {
		t.Fatal("no drop rows")
	}
	row := st.Drops[0]
	if row.Inconclusive {
		t.Error("a sample above the floor should not be inconclusive")
	}
	if !row.Mismatch {
		t.Errorf("a 100%% drop rate on %d records should be a mismatch", row.Dropped)
	}
	var warned bool
	for _, w := range st.Warnings {
		if strings.Contains(w.Text, "LICENCE MISMATCH") {
			warned = true
		}
	}
	if !warned {
		t.Error("no licence-mismatch warning was raised")
	}
	if st.Health != dds.HealthWarn && st.Health != dds.HealthBad {
		t.Errorf("header health = %s; a mismatch must demote it", st.Health)
	}
}

// TestDropReasonsAreSeparated. A modelling gap on our side and a licence
// mismatch need different people to look at them.
func TestDropReasonsAreSeparated(t *testing.T) {
	p := healthy()
	for i := 0; i < 20; i++ {
		p.reg.RecordDrop("soccer", "out_of_scope")
	}
	for i := 0; i < 5; i++ {
		p.reg.RecordDrop("soccer", "unidentified")
	}
	st := state(t, serve(t, p))
	if len(st.Drops) == 0 {
		t.Fatal("no drop rows")
	}
	r := st.Drops[0].Reasons
	if r["out_of_scope"] != 20 || r["unidentified"] != 5 {
		t.Errorf("reasons = %v, want 20 out_of_scope and 5 unidentified", r)
	}
}
