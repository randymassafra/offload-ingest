package ingest

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

var kickoff = time.Date(2026, 9, 1, 22, 30, 0, 0, time.UTC)

func fixtureJSON(t *testing.T, status string, elapsed any, ts int64) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"fixture": map[string]any{
			"id": 1, "timestamp": ts,
			"status": map[string]any{"short": status, "elapsed": elapsed},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// TestMatchLagMeasuresProviderDelay is the metric that replaced the literal
// "now minus payload timestamp" definition.
func TestMatchLagMeasuresProviderDelay(t *testing.T) {
	// Kick-off + 47 minutes of wall clock, provider still reporting minute 44:
	// three minutes behind.
	now := kickoff.Add(47 * time.Minute)
	lag, ok := MatchLag(fixtureJSON(t, "1H", 44, kickoff.Unix()), now)
	if !ok {
		t.Fatal("a first-half fixture should yield a lag")
	}
	if lag < 170 || lag > 190 {
		t.Errorf("lag = %vs, want ~180s", lag)
	}
}

// TestMatchLagDeclinesOutsideTheFirstHalf is the caveat made explicit: elapsed
// does not count the half-time interval, so a second-half fixture would read
// roughly fifteen minutes of false lag. A metric that declines to answer beats
// one that answers wrongly.
func TestMatchLagDeclinesOutsideTheFirstHalf(t *testing.T) {
	now := kickoff.Add(75 * time.Minute)
	for _, status := range []string{"2H", "HT", "FT", "NS", "ET", ""} {
		if _, ok := MatchLag(fixtureJSON(t, status, 60, kickoff.Unix()), now); ok {
			t.Errorf("status %q should not contribute to lag", status)
		}
	}
}

// TestMatchLagDeclinesWithoutAnElapsedMinute: the provider sends elapsed as
// null before kick-off and after the whistle.
func TestMatchLagDeclinesWithoutAnElapsedMinute(t *testing.T) {
	now := kickoff.Add(10 * time.Minute)
	if _, ok := MatchLag(fixtureJSON(t, "1H", nil, kickoff.Unix()), now); ok {
		t.Error("a null elapsed should not produce a lag")
	}
	if _, ok := MatchLag(fixtureJSON(t, "1H", 5, 0), now); ok {
		t.Error("a missing kickoff timestamp should not produce a lag")
	}
}

// TestMatchLagRejectsImplausibleValues keeps a mis-read status or a stale clock
// out of the median.
func TestMatchLagRejectsImplausibleValues(t *testing.T) {
	// A fixture that kicks off tomorrow.
	future := kickoff.Add(-2 * time.Hour)
	if _, ok := MatchLag(fixtureJSON(t, "1H", 5, kickoff.Unix()), future); ok {
		t.Error("a fixture that has not kicked off should not produce a lag")
	}
	// Wall clock hours past a reported minute 2 — the comparison has broken.
	if _, ok := MatchLag(fixtureJSON(t, "1H", 2, kickoff.Unix()), kickoff.Add(3*time.Hour)); ok {
		t.Error("an implausible lag should be discarded, not reported")
	}
}

// TestMatchLagNeverNegative: a rounded elapsed minute can put the provider
// nominally ahead of the wall clock.
func TestMatchLagNeverNegative(t *testing.T) {
	lag, ok := MatchLag(fixtureJSON(t, "1H", 10, kickoff.Unix()), kickoff.Add(9*time.Minute))
	if !ok {
		t.Fatal("want a lag")
	}
	if lag < 0 {
		t.Errorf("lag = %v, want it clamped at 0", lag)
	}
}

// TestProviderSkewReadsTheDateHeader. This is the only clock signal the API
// offers, and a drifting appliance clock silently corrupts every other
// time-based metric and every Flink event-time window.
func TestProviderSkewReadsTheDateHeader(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	h := http.Header{}
	h.Set("Date", now.Add(4*time.Second).UTC().Format(http.TimeFormat))

	skew, ok := ParseProviderSkew(h, now)
	if !ok {
		t.Fatal("Date header was not parsed")
	}
	if skew < 3 || skew > 5 {
		t.Errorf("skew = %vs, want ~4s", skew)
	}

	// A behind clock reports negative, not absolute.
	h.Set("Date", now.Add(-30*time.Second).UTC().Format(http.TimeFormat))
	if skew, _ := ParseProviderSkew(h, now); skew > -25 {
		t.Errorf("skew = %vs, want about -30s", skew)
	}
	if _, ok := ParseProviderSkew(http.Header{}, now); ok {
		t.Error("an absent Date header should report unknown, not zero skew")
	}
}

// TestMeasureDriftCombinesTheThreeComponents.
func TestMeasureDriftCombinesTheThreeComponents(t *testing.T) {
	now := kickoff.Add(47 * time.Minute)
	fetched := now.Add(-3 * time.Second)
	h := http.Header{}
	h.Set("Date", fetched.Add(2*time.Second).UTC().Format(http.TimeFormat))

	rows := []json.RawMessage{
		fixtureJSON(t, "1H", 44, kickoff.Unix()), // ~180s lag
		fixtureJSON(t, "1H", 45, kickoff.Unix()), // ~120s lag
		fixtureJSON(t, "FT", 90, kickoff.Unix()), // ignored
	}
	d := MeasureDrift(rows, fetched, h, now)

	if d.IngestAge < 2.5 || d.IngestAge > 3.5 {
		t.Errorf("ingest age = %vs, want ~3s", d.IngestAge)
	}
	if !d.SkewKnown || d.ProviderSkew < 1 || d.ProviderSkew > 3 {
		t.Errorf("skew = %vs (known=%v), want ~2s", d.ProviderSkew, d.SkewKnown)
	}
	if d.LagSamples != 2 {
		t.Errorf("lag samples = %d, want 2 — the finished fixture must not count", d.LagSamples)
	}
	if d.LiveMatchLag < 140 || d.LiveMatchLag > 160 {
		t.Errorf("median lag = %vs, want ~150s", d.LiveMatchLag)
	}
}

// TestMeasureDriftUsesAMedian: one fixture with a stale clock must not drag the
// figure for a card of healthy ones.
func TestMeasureDriftUsesAMedian(t *testing.T) {
	now := kickoff.Add(20 * time.Minute)
	rows := []json.RawMessage{
		fixtureJSON(t, "1H", 20, kickoff.Unix()),
		fixtureJSON(t, "1H", 20, kickoff.Unix()),
		fixtureJSON(t, "1H", 20, kickoff.Unix()),
		fixtureJSON(t, "1H", 1, kickoff.Unix()), // an outlier, ~19 minutes behind
	}
	d := MeasureDrift(rows, now, http.Header{}, now)
	if d.LiveMatchLag > 300 {
		t.Errorf("median lag = %vs; one outlier dragged the figure", d.LiveMatchLag)
	}
}

// TestIngestAgeIsNeverNegative guards against a clock stepping backwards.
func TestIngestAgeIsNeverNegative(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	d := MeasureDrift(nil, now.Add(5*time.Second), http.Header{}, now)
	if d.IngestAge < 0 {
		t.Errorf("ingest age = %v, want it clamped at 0", d.IngestAge)
	}
}

// TestDriftOnAFlatGamesFamilyDocument covers the non-soccer verticals, whose
// status lives at the top level rather than under fixture{}.
func TestDriftOnAFlatGamesFamilyDocument(t *testing.T) {
	now := kickoff.Add(30 * time.Minute)
	row, _ := json.Marshal(map[string]any{
		"id": 7, "timestamp": kickoff.Unix(),
		"status": map[string]any{"short": "Q1", "timer": 28},
	})
	lag, ok := MatchLag(row, now)
	if !ok {
		t.Fatal("a flat first-quarter document should yield a lag")
	}
	if lag < 100 || lag > 140 {
		t.Errorf("lag = %vs, want ~120s", lag)
	}
}
