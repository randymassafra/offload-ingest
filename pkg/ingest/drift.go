package ingest

import (
	"encoding/json"
	"net/http"
	"time"
)

// Real-time fidelity, measured honestly.
//
// # Why this is three numbers and not one
//
// The obvious definition — "current system time minus API payload timestamp" —
// does not measure data freshness with this provider, and shipping it would put
// an alarming number on a healthy dashboard.
//
// API-Sports sends `timestamp` as the fixture's SCHEDULED KICKOFF, not as an
// update time. Checked against a live payload: a match with timestamp 22:30 and
// status 1H/elapsed 44 yields now−timestamp = 44 minutes. That is match elapsed
// time. For a finished fixture it is hours, and for tomorrow's card it is
// negative. The provider sends no per-record update stamp, so the literal
// metric is not implementable as intended.
//
// What is measurable, each with a single clear meaning:
//
//	IngestAge     now − our fetch time. Always valid. The true staleness signal:
//	              how long a record has been sitting in our pipeline.
//	ProviderSkew  provider clock − our clock, from the HTTP Date header. Catches
//	              a drifting appliance clock, which silently corrupts every
//	              other time-based metric and every Flink event-time window.
//	LiveMatchLag  (wall clock − kickoff) − reported elapsed. How far behind live
//	              play the provider's own data is. First-half fixtures only.
//
// The last one carries a real caveat and is scoped accordingly: `elapsed` does
// not count the half-time interval, so a second-half fixture would read ~15
// minutes of false lag. Rather than apply a fudge factor for a break whose
// actual length varies, only first-half fixtures contribute — a smaller, honest
// sample beats a larger one that needs an asterisk.

// Drift is one sweep's fidelity measurement.
type Drift struct {
	// IngestAge is how old the data was when we received it, in seconds. With
	// a bulk sweep this is near zero by construction; it grows when the
	// pipeline backs up.
	IngestAge float64
	// ProviderSkew is the provider's clock minus ours, in seconds.
	ProviderSkew float64
	// SkewKnown is false when the response carried no usable Date header.
	SkewKnown bool
	// LiveMatchLag is the median lag across first-half fixtures, in seconds.
	LiveMatchLag float64
	// LagSamples is how many fixtures contributed. Zero means no first-half
	// fixture was in the sweep, which is normal and not a fault.
	LagSamples int
}

// ParseProviderSkew reads the HTTP Date header and returns the provider's clock
// offset from ours.
func ParseProviderSkew(h http.Header, now time.Time) (float64, bool) {
	raw := h.Get("Date")
	if raw == "" {
		return 0, false
	}
	when, err := http.ParseTime(raw)
	if err != nil {
		return 0, false
	}
	return when.Sub(now).Seconds(), true
}

// liveLagHolder covers where the two document families put kickoff and elapsed.
type liveLagHolder struct {
	Fixture struct {
		Timestamp int64 `json:"timestamp"`
		Status    struct {
			Short   string `json:"short"`
			Elapsed *int   `json:"elapsed"`
		} `json:"status"`
	} `json:"fixture"`
	Timestamp int64 `json:"timestamp"`
	Status    struct {
		Short   string `json:"short"`
		Elapsed *int   `json:"elapsed"`
		Timer   *int   `json:"timer"`
	} `json:"status"`
}

// firstHalfStatuses are the only states where wall-clock-since-kickoff and the
// provider's elapsed minute are directly comparable, because no interval has
// intervened yet.
var firstHalfStatuses = map[string]bool{"1H": true, "Q1": true, "P1": true}

// MatchLag computes one fixture's provider lag in seconds.
//
// Returns ok=false for anything that is not a first-half fixture with both a
// kickoff timestamp and a reported elapsed minute — which is most of a card,
// and is the point: a metric that declines to answer is better than one that
// answers wrongly.
func MatchLag(row json.RawMessage, now time.Time) (float64, bool) {
	var h liveLagHolder
	if err := json.Unmarshal(row, &h); err != nil {
		return 0, false
	}
	ts, status, elapsed := h.Fixture.Timestamp, h.Fixture.Status.Short, h.Fixture.Status.Elapsed
	if ts == 0 {
		ts, status, elapsed = h.Timestamp, h.Status.Short, h.Status.Elapsed
		if elapsed == nil {
			elapsed = h.Status.Timer
		}
	}
	if ts == 0 || elapsed == nil || !firstHalfStatuses[status] {
		return 0, false
	}
	sinceKickoff := now.Sub(time.Unix(ts, 0)).Seconds()
	// A fixture whose clock has not started, or whose kickoff is in the future,
	// tells us nothing about lag.
	if sinceKickoff <= 0 {
		return 0, false
	}
	lag := sinceKickoff - float64(*elapsed)*60
	// Negative lag means the provider is ahead of the wall clock, which happens
	// with a rounded elapsed minute; clamp rather than report a negative delay.
	if lag < 0 {
		lag = 0
	}
	// Beyond a half's length the comparison has broken down — a fixture that
	// went to a long stoppage, or a status we mis-read. Discard rather than
	// pollute the median.
	if lag > 45*60 {
		return 0, false
	}
	return lag, true
}

// MeasureDrift computes the fidelity components for one sweep.
func MeasureDrift(rows []json.RawMessage, fetchedAt time.Time, header http.Header, now time.Time) Drift {
	d := Drift{IngestAge: now.Sub(fetchedAt).Seconds()}
	if d.IngestAge < 0 {
		d.IngestAge = 0
	}
	if skew, ok := ParseProviderSkew(header, fetchedAt); ok {
		d.ProviderSkew, d.SkewKnown = skew, true
	}

	var lags []float64
	for _, row := range rows {
		if lag, ok := MatchLag(row, now); ok {
			lags = append(lags, lag)
		}
	}
	if len(lags) == 0 {
		return d
	}
	// Median, not mean: one fixture with a stale clock should not drag the
	// figure for a card of forty healthy ones.
	d.LiveMatchLag = median(lags)
	d.LagSamples = len(lags)
	return d
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64{}, v...)
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
	mid := len(s) / 2
	if len(s)%2 == 1 {
		return s[mid]
	}
	return (s[mid-1] + s[mid]) / 2
}
