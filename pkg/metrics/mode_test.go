package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// scrape renders the exposition output and returns the provider_mode lines.
func scrapeProviderModes(t *testing.T, r *Registry) map[string]string {
	t.Helper()
	rec := httptest.NewRecorder()
	r.PrometheusHandler("offload-ingest").ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/metrics", nil))

	out := map[string]string{}
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if !strings.HasPrefix(line, "offload_ingest_provider_mode{") {
			continue
		}
		// offload_ingest_provider_mode{provider="apisports"} 1
		name := line[strings.Index(line, `provider="`)+len(`provider="`):]
		name = name[:strings.Index(name, `"`)]
		out[name] = strings.TrimSpace(line[strings.LastIndex(line, "}")+1:])
	}
	return out
}

func TestLiveProviderReportsOne(t *testing.T) {
	r := NewRegistry(nil)
	r.SetProviderMode("apisports", true, "")

	if got := scrapeProviderModes(t, r)["apisports"]; got != "1" {
		t.Errorf("a live provider exported %q, want \"1\"", got)
	}
}

func TestSimulatedProviderReportsZero(t *testing.T) {
	r := NewRegistry(nil)
	r.SetProviderMode("livegolf", false, "GOLF_API_KEY is not set")

	if got := scrapeProviderModes(t, r)["livegolf"]; got != "0" {
		t.Errorf("a simulated provider exported %q, want \"0\"", got)
	}
}

// The mixed case is the one the metric exists for: some feeds live, some not,
// in the same process. A single global mode gauge could not express it.
func TestProvidersAreReportedIndependently(t *testing.T) {
	r := NewRegistry(nil)
	r.SetProviderMode("apisports", true, "")
	r.SetProviderMode("livegolf", false, "GOLF_API_KEY is not set")
	r.SetProviderMode("cricbuzz", false, "no production streamer")
	r.SetProviderMode("allscores", false, "no production streamer")

	got := scrapeProviderModes(t, r)
	want := map[string]string{
		"apisports": "1", "livegolf": "0", "cricbuzz": "0", "allscores": "0",
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("provider %q = %q, want %q", name, got[name], w)
		}
	}
	if n := r.LiveProviders(); n != 1 {
		t.Errorf("LiveProviders() = %d, want 1", n)
	}
}

// Zero must always be exported. The golf and flink blocks are suppressed when
// unconfigured because a gauge pinned at zero graphs as a healthy flat line,
// but here zero is the actionable value — suppressing it would hide the exact
// state an operator is looking for.
func TestZeroIsExportedNotSuppressed(t *testing.T) {
	r := NewRegistry(nil)
	r.SetProviderMode("apisports", false, "process is in simulation mode")

	got := scrapeProviderModes(t, r)
	if _, present := got["apisports"]; !present {
		t.Fatal("a simulated provider produced no series at all; zero must be exported")
	}
	if got["apisports"] != "0" {
		t.Errorf("= %q, want \"0\"", got["apisports"])
	}
}

// Nothing is exported before the runtime has registered anything, so a
// half-constructed process does not publish a confident zero for providers it
// has not yet decided about.
func TestNoSeriesBeforeRegistration(t *testing.T) {
	r := NewRegistry(nil)
	if got := scrapeProviderModes(t, r); len(got) != 0 {
		t.Errorf("expected no provider_mode series before registration, got %v", got)
	}
}

func TestModeCanBeUpdated(t *testing.T) {
	r := NewRegistry(nil)
	r.SetProviderMode("apisports", false, "process is in simulation mode")
	r.SetProviderMode("apisports", true, "")

	if got := scrapeProviderModes(t, r)["apisports"]; got != "1" {
		t.Errorf("= %q, want \"1\" after being set live", got)
	}
	if reason := r.ProviderModes()["apisports"].Reason; reason != "" {
		t.Errorf("a live provider kept a reason: %q; the field explains zeroes only", reason)
	}
}

// A zero must carry its reason. "cricbuzz is 0" is a symptom; "cricbuzz is 0
// because it has no production streamer" is an answer, and the difference is
// whether someone has to read the runtime assembly to act on it.
func TestZeroCarriesAReason(t *testing.T) {
	r := NewRegistry(nil)
	r.SetProviderMode("cricbuzz", false, "no production streamer")

	if got := r.ProviderModes()["cricbuzz"].Reason; got == "" {
		t.Error("a simulated provider must record why it is simulated")
	}
}

func TestProviderNamesAreSorted(t *testing.T) {
	r := NewRegistry(nil)
	for _, n := range []string{"livegolf", "apisports", "cricbuzz"} {
		r.SetProviderMode(n, false, "x")
	}
	got := r.ProviderNames()
	want := []string{"apisports", "cricbuzz", "livegolf"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ProviderNames() = %v, want %v; unstable output churns diffs", got, want)
		}
	}
}

func TestModeIsSafeUnderConcurrentScrape(t *testing.T) {
	r := NewRegistry(nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 300; i++ {
			r.SetProviderMode("apisports", i%2 == 0, "flapping")
			r.SetProviderMode("livegolf", i%3 == 0, "flapping")
		}
	}()
	for i := 0; i < 300; i++ {
		_ = r.ProviderModes()
		_ = r.LiveProviders()
		_ = r.ProviderNames()
	}
	<-done
}
